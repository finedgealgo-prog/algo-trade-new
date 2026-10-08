"""
leg_followup.py
──────────────────
When a leg exits (SL_HIT or TP_HIT), reads its LegReentrySL/LegReentryTP
config (whichever matches the exit reason) and arms/triggers the matching
followup — master-doc §32 (Lazy Leg) + §38-45 (Re-entry/Re-cost).

Dispatch by ReentryType (field names/values ported exactly from
shared/features/leg_builder.py's ReentryType enum):
  NextLeg               -> lazy_engine.arm_lazy_watcher, using the referenced
                           IdleLegConfigs entry's OWN strike selection +
                           momentum config (frozen once, master-doc §32).
  Immediate/Reverse     -> reentry_engine.trigger_immediate_reentry, then
                           immediately resolves a fresh strike and enters —
                           no waiting condition ("ASAP", master-doc §38).
  AtCost/Reverse        -> recost_engine.arm_recost_watcher — reuses the
                           EXITED leg's own token/strike (never re-selected).
  LikeOriginal/Reverse  -> reentry_engine.trigger_like_original_reentry, then
                           arms a lazy-style watcher: a FRESH strike
                           selection, waiting on the ORIGINAL leg's own
                           LegMomentum config before entering.

"*Reverse" variants flip the position (is_sell = not original.is_sell) —
ported from leg_builder.py's ReentryType.{IMMEDIATE,LIKE_ORIGINAL}_REV.

Every followup is gated by execution.entry_guard.entry_allowed() first
(master-doc §34/§35/§36) — nothing arms/enters if the strategy or its
broker has already exited.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from conditions import lazy_engine, recost_engine, reentry_engine
from execution.entry_guard import entry_allowed
from orders.order_engine import OrderEngine, order_venue
from persistence.checkpoint import checkpoint_strategy, get_checkpoint_writer
from risk import sl_tp_engine
from runtime.leg_runtime import LegRuntime
from selection import expiry_resolver, strike_resolver
from services.strategy_activation import _leg_qty, _reentry_count
from shared.brokers.delta import client as delta_client
from shared.logging.logger import get_logger
from shared.market.instrument_master import InstrumentMaster
from shared.market.ltp_cache import LtpCache
from token_router import TokenRouter

log = get_logger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

_REVERSE_TYPES = {"ReentryType.ImmediateReverse", "ReentryType.LikeOriginalReverse", "ReentryType.AtCostReverse"}


def _reentry_config_for(leg: LegRuntime, exit_reason: str) -> dict:
    field = "LegReentrySL" if exit_reason == "SL_HIT" else "LegReentryTP"
    return leg.leg_cfg.get(field) or {}


def handle_leg_exit(
    router: TokenRouter, order_engine: OrderEngine,
    instrument_master: InstrumentMaster, ltp_cache: LtpCache,
    leg: LegRuntime, exit_reason: str,
) -> None:
    strategy = router.strategies.get(leg.strategy_id)
    if strategy is None:
        return
    broker = router.brokers.get(leg.broker_scope_id) if leg.broker_scope_id else None
    if not entry_allowed(strategy, broker):
        log.info("[Followup] leg=%s skipped — strategy/broker not ACTIVE", leg.leg_id)
        return

    reentry_cfg = _reentry_config_for(leg, exit_reason)
    reentry_type = str(reentry_cfg.get("Type") or "")
    if not reentry_type or "None" in reentry_type:
        return

    is_reverse = reentry_type in _REVERSE_TYPES
    followup_is_sell = (not leg.is_sell) if is_reverse else leg.is_sell

    if "NextLeg" in reentry_type:
        _arm_next_leg(router, instrument_master, ltp_cache, leg, strategy, reentry_cfg)
    elif "AtCost" in reentry_type:
        _arm_recost(router, leg, _reentry_count_from(reentry_cfg))
    elif "Immediate" in reentry_type:
        _trigger_immediate(router, order_engine, instrument_master, ltp_cache, leg, strategy, followup_is_sell)
    elif "LikeOriginal" in reentry_type:
        _trigger_like_original(router, instrument_master, ltp_cache, leg, strategy, followup_is_sell)


def _resolve_fresh_strike(instrument_master, ltp_cache, strategy, leg_cfg: dict, option_type: str, is_sell: bool):
    underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper()
    expiry_kind = str(leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
    is_crypto = strike_resolver.is_crypto_underlying(underlying)
    if is_crypto:
        # Mirrors strategy_activation.py's own is_crypto branch — instrument_
        # master has zero crypto data (NSE-only, by design; see strike_
        # resolver.build_chain_rows's docstring), so expiries_for(underlying,
        # ...) always comes back empty for BTCUSD/ETHUSD and every crypto
        # followup (Immediate/NextLeg/LikeOriginal) silently died here,
        # forever, regardless of how many legs the strategy actually
        # configured (confirmed live: "strike not resolved" on every
        # Immediate re-entry attempt for a BTCUSD strategy).
        from shared.market.delta_instrument_cache import crypto_expiries
        trade_date = datetime.now(_IST).strftime("%Y-%m-%d")
        expiry = delta_client.resolve_delta_expiry(trade_date, expiry_kind, crypto_expiries(underlying))
        spot = ltp_cache.get_spot(underlying) or ltp_cache.get_ltp(underlying) or 0.0
    else:
        expiries = instrument_master.expiries_for(underlying, option_type)
        expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
        spot = ltp_cache.get_spot(underlying) or 0.0
    if not expiry:
        return None, None
    selection = strike_resolver.resolve_strike(
        instrument_master, ltp_cache, underlying, expiry, option_type,
        str(leg_cfg.get("EntryType") or "EntryType.EntryByStrikeType"),
        leg_cfg.get("StrikeParameter"), spot, is_sell,
    )
    if is_crypto and selection is not None:
        # Same "subscribe the instant it's chosen, don't wait for the
        # periodic sweep" guarantee strategy_activation.py's fresh-entry
        # path has — covers every caller of this function (Immediate
        # re-entry, NextLeg lazy watcher, LikeOriginal watcher) in one
        # place, since they all funnel through here.
        from shared.market.delta_instrument_cache import subscribe_tokens_background
        subscribe_tokens_background([selection.token])
    return selection, expiry


def _arm_next_leg(router: TokenRouter, instrument_master, ltp_cache, leg: LegRuntime, strategy, reentry_cfg: dict) -> None:
    lazy_ref = str((reentry_cfg.get("Value") or {}).get("NextLegRef") or "")
    idle_configs = strategy.strategy_cfg.get("IdleLegConfigs") or {}
    lazy_cfg = idle_configs.get(lazy_ref)
    if not lazy_cfg:
        log.warning("[Followup] NextLeg ref=%s not found in IdleLegConfigs for strategy=%s", lazy_ref, strategy.strategy_id)
        return

    option_type = "PE" if "PE" in str(lazy_cfg.get("InstrumentKind") or "") else "CE"
    is_sell = "sell" in str(lazy_cfg.get("PositionType") or "").lower()
    selection, expiry = _resolve_fresh_strike(instrument_master, ltp_cache, strategy, lazy_cfg, option_type, is_sell)
    if selection is None:
        log.warning("[Followup] NextLeg ref=%s strike not resolved", lazy_ref)
        return

    underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper()
    if strike_resolver.is_crypto_underlying(underlying):
        lot_size = 1  # Delta contracts are sized directly — see strategy_activation.py's own is_crypto branch
    else:
        meta = instrument_master.get(selection.token)
        lot_size = (meta.lot_size if meta else None) or 75
    qty = _leg_qty(lazy_cfg, lot_size, strategy.qty_multiplier)

    momentum_cfg = lazy_cfg.get("LegMomentum") or {}
    watcher = lazy_engine.arm_lazy_watcher(
        lazy_id=f"lazy_{uuid.uuid4().hex[:10]}", strategy_id=strategy.strategy_id, parent_leg_id=leg.leg_id,
        token=selection.token, strike=selection.strike, option_type=option_type,
        momentum_type=str(momentum_cfg.get("Type") or ""), momentum_value=float(momentum_cfg.get("Value") or 0),
        reference_ltp=selection.ltp,
        qty=qty, is_sell=is_sell, leg_cfg=lazy_cfg, broker_scope_id=leg.broker_scope_id, user_id=leg.user_id, expiry=expiry,
    )
    if watcher is None:
        log.warning("[Followup] NextLeg ref=%s momentum config invalid, not armed", lazy_ref)
        return
    router.register_lazy_watcher(watcher)
    log.info("[Followup] lazy watcher armed ref=%s token=%s reference_ltp=%s", lazy_ref, selection.token, selection.ltp)


def _reentry_count_from(reentry_cfg: dict) -> int:
    value = reentry_cfg.get("Value")
    if isinstance(value, dict):
        return int(value.get("ReentryCount") or 0)
    return 0


def _underlying_spot(router: TokenRouter, strategy) -> float:
    underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper() if strategy is not None else ""
    return float(router.spot_prices.get(underlying) or 0.0)


def _arm_recost(router: TokenRouter, leg: LegRuntime, configured_max: int) -> None:
    # The AtCost budget is only known from the exit's own LegReentrySL/TP
    # config — nothing sets recost_max at entry, so without this it stayed
    # 0 and arm_recost_watcher refused every AtCost re-entry. A child leg
    # inherits its parent's recost_max/used, so this only fills it once per
    # lineage and never resets a budget already in use.
    if leg.recost_max <= 0:
        leg.recost_max = configured_max
    watcher = recost_engine.arm_recost_watcher(f"recost_{uuid.uuid4().hex[:10]}", leg)
    if watcher is None:
        log.info("[Followup] leg=%s re-cost budget exhausted or no entry price, not armed", leg.leg_id)
        return
    leg.recost_used += 1
    router.register_recost_watcher(watcher)
    checkpoint_strategy(router, leg.strategy_id)  # recost_used changed
    log.info("[Followup] recost watcher armed leg=%s token=%s reference_price=%s", leg.leg_id, watcher.token, watcher.reference_price)


def _trigger_immediate(router: TokenRouter, order_engine: OrderEngine, instrument_master, ltp_cache, leg: LegRuntime, strategy, is_sell: bool) -> None:
    spec = reentry_engine.trigger_immediate_reentry(leg)
    if spec is None:
        return
    checkpoint_strategy(router, leg.strategy_id)  # reentry_used changed
    selection, expiry = _resolve_fresh_strike(instrument_master, ltp_cache, strategy, leg.leg_cfg, leg.option_type, is_sell)
    if selection is None:
        log.warning("[Followup] Immediate re-entry leg=%s strike not resolved", leg.leg_id)
        return
    _enter_fresh_leg(router, order_engine, leg, strategy, selection, expiry, is_sell, spec.reentry_used, spec.reentry_max)


def _trigger_like_original(router: TokenRouter, instrument_master, ltp_cache, leg: LegRuntime, strategy, is_sell: bool) -> None:
    """RE-MOMENTUM (master-doc §38): fresh strike selection (unlike Lazy's
    frozen one — see leg_runtime.py's docstring on §39), but waits on the
    ORIGINAL leg's own LegMomentum config before actually entering. Reuses
    lazy_engine's watcher machinery — the wait/trigger math is identical,
    only the strike-selection source differs (this leg's own config, not an
    IdleLegConfigs lazy leg)."""
    momentum_cfg = leg.leg_cfg.get("LegMomentum") or {}
    momentum_type = str(momentum_cfg.get("Type") or "")
    momentum_value = float(momentum_cfg.get("Value") or 0)
    spec = reentry_engine.trigger_like_original_reentry(leg, momentum_type, momentum_value)
    if spec is None:
        return

    selection, expiry = _resolve_fresh_strike(instrument_master, ltp_cache, strategy, leg.leg_cfg, leg.option_type, is_sell)
    if selection is None:
        log.warning("[Followup] LikeOriginal leg=%s fresh strike not resolved", leg.leg_id)
        return

    watcher = lazy_engine.arm_lazy_watcher(
        lazy_id=f"reentry_{uuid.uuid4().hex[:10]}", strategy_id=strategy.strategy_id, parent_leg_id=leg.leg_id,
        token=selection.token, strike=selection.strike, option_type=selection.option_type,
        momentum_type=momentum_type, momentum_value=momentum_value, reference_ltp=selection.ltp,
        # Same leg lineage as the original (fresh strike/contract only) —
        # qty/leg_cfg/broker_scope_id/user_id all carry over unchanged,
        # same as _enter_fresh_leg's Immediate/AtCost siblings do.
        qty=leg.qty, is_sell=is_sell, leg_cfg=leg.leg_cfg, broker_scope_id=leg.broker_scope_id, user_id=leg.user_id, expiry=expiry,
    )
    if watcher is None:
        log.warning("[Followup] LikeOriginal leg=%s momentum config invalid, not armed", leg.leg_id)
        return
    watcher.reentry_used = spec.reentry_used
    watcher.reentry_max = spec.reentry_max
    router.register_lazy_watcher(watcher)
    log.info("[Followup] LikeOriginal watcher armed leg=%s token=%s reference_ltp=%s", leg.leg_id, selection.token, selection.ltp)


def _enter_fresh_leg(router: TokenRouter, order_engine: OrderEngine, parent_leg: LegRuntime, strategy, selection, expiry: str, is_sell: bool, used: int, max_count: int) -> None:
    ts = datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{ts}] [ENTRY] strategy={strategy.strategy_id} leg={parent_leg.leg_id}_re{used} token={selection.token} ltp={selection.ltp}")
    order_id = order_engine.broker.place_order(
        tradingsymbol=selection.symbol or selection.token, **order_venue(selection.symbol or selection.token),
        transaction_type="SELL" if is_sell else "BUY", quantity=parent_leg.qty,
        order_type="MARKET", fill_price=selection.ltp,
    )
    new_leg = LegRuntime(
        leg_id=f"{parent_leg.leg_id}_re{used}", strategy_id=strategy.strategy_id, user_id=parent_leg.user_id,
        token=selection.token, is_sell=is_sell, option_type=selection.option_type, qty=parent_leg.qty,
        entry_price=selection.ltp, entry_spot=_underlying_spot(router, strategy), leg_cfg=parent_leg.leg_cfg, current_price=selection.ltp,
        strike=selection.strike, expiry=expiry,
        broker_scope_id=parent_leg.broker_scope_id, reentry_max=max_count, reentry_used=used,
        recost_max=parent_leg.recost_max, recost_used=parent_leg.recost_used,
    )
    sl_tp_engine.initialize_sl_tp(new_leg)
    router.register_leg(new_leg)
    order_engine.submit_live_entry(new_leg)  # real Delta order for a "live"-mode crypto leg
    checkpoint_strategy(router, new_leg.strategy_id)
    log.info("[Followup] fresh entry leg=%s token=%s price=%s order_id=%s", new_leg.leg_id, selection.token, selection.ltp, order_id)


def complete_lazy_entry(router: TokenRouter, order_engine: OrderEngine, lazy_id: str, price: float) -> None:
    """LAZY_TRIGGERED completion (main.py's own trigger listener calls this
    the moment token_router marks a lazy watcher TRIGGERED) — the actual
    entry order + LegRuntime that arm_lazy_watcher's freeze made possible,
    closing the exact gap orders/order_engine.py's own docstring flagged:
    "NOT wired yet: LAZY_TRIGGERED / RECOST_TRIGGERED -> entry order."
    Before this, a Lazy Leg (NextLeg) or LikeOriginal-momentum watcher armed
    correctly, detected its trigger correctly, and then did nothing else —
    confirmed, no entry ever followed.

    Covers BOTH callers of arm_lazy_watcher above (NextLeg's own
    IdleLegConfigs leg, and LikeOriginal's re-momentum wait) identically —
    only WHERE the frozen token/qty/leg_cfg came from differs, and that's
    already baked into the LazyRuntime by arm time (see its own docstring),
    nothing here needs to know which case this was."""
    lazy = router.lazy_watchers.get(lazy_id)
    if lazy is None or lazy.status != "TRIGGERED":
        return
    strategy = router.strategies.get(lazy.strategy_id)
    broker = router.brokers.get(lazy.broker_scope_id) if lazy.broker_scope_id else None
    if strategy is None or not entry_allowed(strategy, broker):
        router.unregister_lazy_watcher(lazy_id)
        log.info("[Followup] lazy=%s dropped at trigger — strategy/broker no longer ACTIVE", lazy_id)
        return

    order_id = order_engine.broker.place_order(
        tradingsymbol=lazy.token, **order_venue(lazy.token),
        transaction_type="SELL" if lazy.is_sell else "BUY", quantity=lazy.qty,
        order_type="MARKET", fill_price=price,
    )
    if lazy.reentry_max >= 0:
        # LikeOriginal re-entry: same lineage as the exited leg, so its
        # already-consumed budget carries over (a fresh 0 here re-armed an
        # unlimited chain of re-entries for any ReentryCount).
        reentry_max, reentry_used = lazy.reentry_max, lazy.reentry_used
    else:
        reentry_max, reentry_used = _reentry_count(lazy.leg_cfg, "LegReentrySL") or _reentry_count(lazy.leg_cfg, "LegReentryTP"), 0
    new_leg = LegRuntime(
        leg_id=f"{lazy.parent_leg_id}_lazy{uuid.uuid4().hex[:6]}", strategy_id=lazy.strategy_id, user_id=lazy.user_id,
        token=lazy.token, is_sell=lazy.is_sell, option_type=lazy.option_type, qty=lazy.qty,
        entry_price=price, entry_spot=_underlying_spot(router, strategy), leg_cfg=lazy.leg_cfg, current_price=price,
        strike=lazy.strike, expiry=lazy.expiry,
        broker_scope_id=lazy.broker_scope_id,
        reentry_max=reentry_max, reentry_used=reentry_used,
    )
    sl_tp_engine.initialize_sl_tp(new_leg)
    router.register_leg(new_leg)
    order_engine.submit_live_entry(new_leg)  # real Delta order for a "live"-mode crypto leg
    router.unregister_lazy_watcher(lazy_id)
    checkpoint_strategy(router, lazy.strategy_id)
    log.info("[Followup] lazy entry completed leg=%s token=%s price=%s order_id=%s", new_leg.leg_id, lazy.token, price, order_id)


def complete_recost_entry(router: TokenRouter, order_engine: OrderEngine, recost_id: str, price: float) -> None:
    """RECOST_TRIGGERED completion — same previously-unwired gap as
    complete_lazy_entry above, for the AtCost re-entry path. Re-uses the
    ORIGINAL (already-EXITED) parent leg's own token/qty/leg_cfg/
    broker_scope_id/user_id verbatim (never re-selected — see
    RecostRuntime's own docstring), so unlike LazyRuntime nothing extra had
    to be frozen at arm time; it's all still sitting on the parent
    LegRuntime, which stays in router.legs (terminal status, not popped)
    until its owning strategy fully finalizes — see finalize_if_no_open_
    legs's own "has_pending_watcher" check for why arming a recost watcher
    keeps that finalize from happening out from under this."""
    recost = router.recost_watchers.get(recost_id)
    if recost is None or recost.status != "TRIGGERED":
        return
    parent_leg = router.legs.get(recost.parent_leg_id)
    strategy = router.strategies.get(recost.strategy_id)
    broker = router.brokers.get(parent_leg.broker_scope_id) if parent_leg and parent_leg.broker_scope_id else None
    if parent_leg is None or strategy is None or not entry_allowed(strategy, broker):
        router.unregister_recost_watcher(recost_id)
        log.info("[Followup] recost=%s dropped at trigger — parent leg/strategy/broker gone or not ACTIVE", recost_id)
        return

    order_id = order_engine.broker.place_order(
        tradingsymbol=parent_leg.token, **order_venue(parent_leg.token),
        transaction_type="SELL" if parent_leg.is_sell else "BUY", quantity=parent_leg.qty,
        order_type="MARKET", fill_price=price,
    )
    new_leg = LegRuntime(
        leg_id=f"{parent_leg.leg_id}_recost{parent_leg.recost_used}", strategy_id=strategy.strategy_id, user_id=parent_leg.user_id,
        token=parent_leg.token, is_sell=parent_leg.is_sell, option_type=parent_leg.option_type, qty=parent_leg.qty,
        entry_price=price, entry_spot=_underlying_spot(router, strategy), leg_cfg=parent_leg.leg_cfg, current_price=price,
        strike=parent_leg.strike, expiry=parent_leg.expiry,
        broker_scope_id=parent_leg.broker_scope_id,
        recost_max=parent_leg.recost_max, recost_used=parent_leg.recost_used,
        reentry_max=parent_leg.reentry_max, reentry_used=parent_leg.reentry_used,
    )
    sl_tp_engine.initialize_sl_tp(new_leg)
    router.register_leg(new_leg)
    order_engine.submit_live_entry(new_leg)  # real Delta order for a "live"-mode crypto leg
    router.unregister_recost_watcher(recost_id)
    checkpoint_strategy(router, strategy.strategy_id)
    log.info("[Followup] recost entry completed leg=%s token=%s price=%s order_id=%s", new_leg.leg_id, parent_leg.token, price, order_id)
