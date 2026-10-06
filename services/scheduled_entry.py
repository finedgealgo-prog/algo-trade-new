"""
scheduled_entry.py
─────────────────────
Consumes SCHEDULED_ENTRY_READY trigger events (emitted by token_router's
`_check_pending_entries`, conditions/schedule_engine.py's TimeIndicator gate)
and actually enters the leg — resolve strike, place virtual entry order,
register LegRuntime. Same strike-resolution path activation itself uses
(strike_resolver via ltp_cache/instrument_master, no Mongo).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from conditions import lazy_engine, range_breakout_engine
from execution.entry_guard import entry_allowed
from orders.order_engine import OrderEngine, order_venue
from persistence.checkpoint import checkpoint_strategy
from risk import sl_tp_engine
from runtime.leg_runtime import LegRuntime
from selection import expiry_resolver, strike_resolver
from services.strategy_activation import cleanup_if_no_legs
from shared.brokers.delta import client as delta_client
from shared.logging.logger import get_logger
from shared.market.instrument_master import InstrumentMaster
from shared.market.ltp_cache import LtpCache
from token_router import TokenRouter, TriggerEvent

log = get_logger(__name__)
_IST = timezone(timedelta(hours=5, minutes=30))
def _dbg(msg: str) -> None:
    log.debug("%s", msg)


def _reentry_count(leg_cfg: dict, field: str) -> int:
    cfg = leg_cfg.get(field) or {}
    value = cfg.get("Value")
    return int(value.get("ReentryCount") or 0) if isinstance(value, dict) else 0


def _resolve_strike_cached(router: TokenRouter, instrument_master: InstrumentMaster, ltp_cache: LtpCache, underlying: str, expiry: str, option_type: str, entry_type: str, strike_param, spot: float, is_sell: bool):
    """Mass-same-second entry concept (entry-#CLAUDE CODE IMPLEMENTATION
    PROMPT.md §11-17): many strategies due in the SAME release batch often
    want the identical selection (e.g. 50 strategies all wanting "NIFTY
    current-week ATM CE") — resolve it once per batch, reuse the result,
    instead of every strategy independently re-running strike_resolver.
    `is_sell` is part of the signature because it genuinely changes the
    result for delta-range selection (never share across a semantically
    different request just to save CPU — doc §17).
    router.selection_cache is reset once per release batch by
    TokenRouter._check_pending_entries, so a stale/different-snapshot
    selection can never leak into a later batch."""
    signature = (underlying, expiry, option_type, entry_type, str(strike_param), is_sell)
    if signature in router.selection_cache:
        return router.selection_cache[signature]
    selection = strike_resolver.resolve_strike(
        instrument_master, ltp_cache, underlying, expiry, option_type,
        entry_type, strike_param, spot, is_sell,
    )
    router.selection_cache[signature] = selection
    return selection


def _arm_gated_entry(router: TokenRouter, pending, strategy, leg_cfg: dict, underlying: str, option_type: str, is_sell: bool, qty: int, selection, expiry: str) -> bool:
    """Returns True when this pending leg was handed to a range-breakout or
    momentum watcher (or dropped for an invalid gate config) instead of
    entering now."""
    rb_type, _condition, _start, _end = range_breakout_engine.parse_leg_range_breakout(leg_cfg)
    momentum_cfg = leg_cfg.get("LegMomentum") or {}
    momentum_type = str(momentum_cfg.get("Type") or "")
    momentum_value = float(momentum_cfg.get("Value") or 0)
    has_momentum = bool(momentum_type) and "None" not in momentum_type and momentum_value > 0
    if rb_type == "None" and not has_momentum:
        return False

    leg_ref = str(leg_cfg.get("id") or pending.pending_id)
    if rb_type != "None":
        from services import range_breakout_service
        watcher = range_breakout_service.arm_range_watcher(
            strategy_id=pending.strategy_id, leg_ref=leg_ref, underlying=underlying,
            leg_cfg=leg_cfg, option_type=option_type, is_sell=is_sell, qty=qty,
            broker_scope_id=pending.broker_scope_id, user_id=strategy.user_id,
        )
        if watcher is not None:
            router.register_range_watcher(watcher)
            log.info("[ScheduledEntry] pending=%s armed on LegRangeBreakout type=%s", pending.pending_id, rb_type)
        else:
            log.warning("[ScheduledEntry] pending=%s LegRangeBreakout config invalid, leg skipped", pending.pending_id)
    else:
        watcher = lazy_engine.arm_lazy_watcher(
            lazy_id=f"lazy_init_{uuid.uuid4().hex[:10]}", strategy_id=pending.strategy_id,
            parent_leg_id=f"{pending.strategy_id}_{leg_ref}",
            token=selection.token, strike=selection.strike, option_type=option_type,
            momentum_type=momentum_type, momentum_value=momentum_value, reference_ltp=selection.ltp,
            qty=qty, is_sell=is_sell, leg_cfg=leg_cfg, broker_scope_id=pending.broker_scope_id,
            user_id=strategy.user_id, expiry=expiry,
        )
        if watcher is not None:
            router.register_lazy_watcher(watcher)
            log.info("[ScheduledEntry] pending=%s armed on LegMomentum type=%s value=%s reference=%s", pending.pending_id, momentum_type, momentum_value, selection.ltp)
        else:
            log.warning("[ScheduledEntry] pending=%s LegMomentum config invalid, leg skipped", pending.pending_id)

    router.unregister_pending_entry(pending.pending_id)
    cleanup_if_no_legs(router, pending.strategy_id)
    checkpoint_strategy(router, pending.strategy_id)
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return True  # no event loop (sync caller) — nothing to push to
    try:
        from api.live_broadcast import broadcast_strategy_snapshots
        asyncio.create_task(broadcast_strategy_snapshots(router, [pending.strategy_id]))
    except Exception:
        log.exception("[ScheduledEntry] broadcast scheduling failed for strategy=%s", pending.strategy_id)
    return True


def handle_scheduled_entry_ready(router: TokenRouter, order_engine: OrderEngine, instrument_master: InstrumentMaster, ltp_cache: LtpCache, event: TriggerEvent) -> None:
    _dbg(f"[SCHED] event fired reason={event.reason}")
    pending = router.pending_entries.get(event.reason)
    if pending is None or pending.status != "TRIGGERED":
        _dbg(f"[SCHED] SKIP reason={event.reason} pending_found={pending is not None} status={getattr(pending, 'status', None)}")
        return

    strategy = router.strategies.get(pending.strategy_id)
    if strategy is None:
        _dbg(f"[SCHED] SKIP pending={pending.pending_id} strategy_missing strategy_id={pending.strategy_id}")
        router.unregister_pending_entry(pending.pending_id)
        return
    broker = router.brokers.get(pending.broker_scope_id) if pending.broker_scope_id else None
    if not entry_allowed(strategy, broker):
        _dbg(f"[SCHED] SKIP pending={pending.pending_id} entry_not_allowed strategy_status={strategy.status} broker={pending.broker_scope_id} broker_found={broker is not None}")
        log.info("[ScheduledEntry] pending=%s skipped — strategy/broker not ACTIVE", pending.pending_id)
        router.unregister_pending_entry(pending.pending_id)
        cleanup_if_no_legs(router, pending.strategy_id)
        return
    _dbg(f"[SCHED] pending={pending.pending_id} strategy={pending.strategy_id} leg_cfg={pending.leg_cfg}")

    leg_cfg = pending.leg_cfg
    underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper()
    option_type = "PE" if "PE" in str(leg_cfg.get("InstrumentKind") or "") else "CE"
    is_sell = "sell" in str(leg_cfg.get("PositionType") or "").lower()
    expiry_kind = str(leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")

    # Mirrors strategy_activation.py's/leg_followup.py's own is_crypto
    # branch — instrument_master has zero crypto data (NSE-only, by
    # design), so expiries_for(underlying, ...) always came back empty for
    # BTCUSD/ETHUSD and every scheduled (time-gated EntryIndicators) crypto
    # entry silently died here with "no expiry available", forever —
    # confirmed live: a real BTCUSD strategy scheduled for a future time
    # fired exactly on time (token_router's pending-entry release itself
    # works) but then failed this expiry lookup every single attempt, got
    # cleaned up as a zero-leg zombie, and never actually entered — looking
    # indistinguishable from "the scheduled entry never fired" even though
    # the trigger mechanism was never the problem.
    is_crypto = strike_resolver.is_crypto_underlying(underlying)
    if is_crypto:
        from shared.market.delta_instrument_cache import crypto_expiries
        trade_date = datetime.now(_IST).strftime("%Y-%m-%d")
        expiry = delta_client.resolve_delta_expiry(trade_date, expiry_kind, crypto_expiries(underlying))
        spot = ltp_cache.get_spot(underlying) or ltp_cache.get_ltp(underlying) or 0.0
    else:
        expiries = instrument_master.expiries_for(underlying, option_type)
        expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
        spot = ltp_cache.get_spot(underlying) or 0.0
    _dbg(f"[SCHED] pending={pending.pending_id} is_crypto={is_crypto} underlying={underlying} option_type={option_type} expiry_kind={expiry_kind} expiry={expiry!r} spot={spot}")
    if not expiry:
        _dbg(f"[SCHED] DROP pending={pending.pending_id} reason=no_expiry")
        log.warning("[ScheduledEntry] pending=%s no expiry available", pending.pending_id)
        router.unregister_pending_entry(pending.pending_id)
        cleanup_if_no_legs(router, pending.strategy_id)
        return
    entry_type = str(leg_cfg.get("EntryType") or "EntryType.EntryByStrikeType")
    strike_param = leg_cfg.get("StrikeParameter")
    selection = _resolve_strike_cached(
        router, instrument_master, ltp_cache, underlying, expiry, option_type,
        entry_type, strike_param, spot, is_sell,
    )
    _dbg(f"[SCHED] pending={pending.pending_id} entry_type={entry_type} strike_param={strike_param!r} selection={selection}")
    if selection is None:
        _dbg(f"[SCHED] DROP pending={pending.pending_id} reason=strike_not_resolved")
        log.warning("[ScheduledEntry] pending=%s strike not resolved", pending.pending_id)
        router.unregister_pending_entry(pending.pending_id)
        cleanup_if_no_legs(router, pending.strategy_id)
        return

    if is_crypto:
        # Same "subscribe the instant it's chosen" guarantee strategy_
        # activation.py's fresh-entry path and leg_followup.py's re-entry
        # path already have — see delta_instrument_cache.py's
        # subscribe_tokens_now docstring.
        from shared.market.delta_instrument_cache import subscribe_tokens_background
        subscribe_tokens_background([selection.token])
        lot_size = 1  # Delta contracts are sized directly, no separate lot multiple (see client.py's DELTA_CONTRACT_VALUE scaling)
    else:
        meta = instrument_master.get(selection.token)
        lot_size = (meta.lot_size if meta else None) or 75
    lots = int((leg_cfg.get("LotConfig") or {}).get("Value") or 1)
    qty = max(1, lots) * max(1, strategy.qty_multiplier) * max(1, lot_size)

    # Same LegRangeBreakout / LegMomentum gates the immediate-activation path
    # applies (strategy_activation._complete_leg_entry) — a time-gated leg
    # with either configured must arm its watcher at the scheduled time, not
    # enter straight away.
    if _arm_gated_entry(router, pending, strategy, leg_cfg, underlying, option_type, is_sell, qty, selection, expiry):
        return

    order_id = order_engine.broker.place_order(
        tradingsymbol=selection.symbol or selection.token, **order_venue(selection.symbol or selection.token),
        transaction_type="SELL" if is_sell else "BUY", quantity=qty,
        order_type="MARKET", fill_price=selection.ltp,
    )

    leg = LegRuntime(
        leg_id=f"sched_{uuid.uuid4().hex[:10]}", strategy_id=pending.strategy_id, user_id=strategy.user_id,
        token=selection.token, is_sell=is_sell, option_type=option_type, qty=qty,
        entry_price=selection.ltp, entry_spot=spot, leg_cfg=leg_cfg, current_price=selection.ltp,
        strike=selection.strike, expiry=expiry,
        broker_scope_id=pending.broker_scope_id,
        reentry_max=_reentry_count(leg_cfg, "LegReentrySL") or _reentry_count(leg_cfg, "LegReentryTP"),
    )
    sl_tp_engine.initialize_sl_tp(leg)
    router.register_leg(leg)
    # Unregister BEFORE the checkpoint so the snapshot never holds both the
    # entered leg and its pending entry (a restart would enter it twice).
    router.unregister_pending_entry(pending.pending_id)
    checkpoint_strategy(router, pending.strategy_id)
    _dbg(f"[SCHED] ENTERED leg={leg.leg_id} token={selection.token} price={selection.ltp} order_id={order_id}")
    log.info("[ScheduledEntry] entered leg=%s token=%s price=%s order_id=%s", leg.leg_id, selection.token, selection.ltp, order_id)

    # A scheduled entry firing is otherwise invisible to an already-open
    # /algo/ws/execute-orders connection until the NEXT real tick touches
    # this leg's token (broadcast_tick_update's touched_strategy_ids) — same
    # gap strategy.py's /{strategy_id}/activate already had and was fixed
    # for (see that route's own comment). This handler runs synchronously
    # from within token_router's tick dispatch (itself inside the running
    # asyncio loop), so a plain create_task here is always safe — no
    # "no running loop" fallback needed, unlike a call site that might run
    # outside asyncio entirely.
    try:
        import asyncio

        from api.live_broadcast import broadcast_strategy_snapshots
        asyncio.create_task(broadcast_strategy_snapshots(router, [pending.strategy_id]))
    except Exception:
        log.exception("[ScheduledEntry] broadcast_strategy_snapshots scheduling failed for strategy=%s", pending.strategy_id)
