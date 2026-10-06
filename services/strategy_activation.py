"""
strategy_activation.py
─────────────────────────
Bridges a saved_strategies Mongo doc into live runtime: resolves each leg's
strike (selection/strike_resolver.py + expiry_resolver.py), places a virtual
ENTRY order, and registers the resulting LegRuntime/StrategyRuntime into
token_router — so it starts receiving ticks immediately.

Strategy/leg config shape matches shared/features/leg_builder.py's build_leg()
output exactly (PositionType/InstrumentKind/ExpiryKind/EntryType/
StrikeParameter/LotConfig/LegStopLoss/LegTarget/LegTrailSL/LegReentrySL/
LegReentryTP) — confirmed there is NO transformation layer between that and
what's persisted in `saved_strategies.full_config.strategy` in the old
system, so a saved strategy needs no translation to be read here.

EntryIndicators (time-gated entry, conditions/schedule_engine.py's
TimeIndicator support only — full technical-indicator trees are NOT ported,
see that module's docstring): if the strategy's EntryIndicators specifies a
TimeIndicator that hasn't been reached yet, every leg is registered as a
PendingEntryRuntime instead of entered immediately — token_router checks
these once per tick and services/scheduled_entry.py performs the actual
entry once the time arrives.

NOT ported yet (explicit gaps, not oversights):
  - ExitIndicators (technical-indicator gated exit).
  - IdleLegConfigs (Lazy Legs) are not armed at activation itself — they DO
    get armed at leg-exit time via services/leg_followup.py's NextLeg
    handling; there's just no "arm immediately on activation" path (the old
    system doesn't have that either — Lazy Legs are reentry-triggered, not
    activation-triggered).
  - Multi-broker-account resolution — one broker_scope_id is passed in by
    the caller (the API route), not derived from the strategy's own
    `broker` field the way the old system's `broker_configuration` lookup does.
  - `algo_trades` write here is a minimal activation record for tracking/
    compat, not the old system's full document shape — full checkpoint/
    recovery (so this survives a restart) is Phase 8.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from bson import ObjectId

from conditions import lazy_engine, range_breakout_engine, schedule_engine
from services import broker_scope, daily_portfolio, strategy_finalize
from orders.order_engine import OrderEngine, order_venue
from persistence.checkpoint import checkpoint_strategy, get_checkpoint_writer
from persistence.serializers import _today_ist_date
from risk import overall_risk, sl_tp_engine
from runtime.leg_runtime import LegRuntime
from runtime.pending_entry_runtime import PendingEntryRuntime
from runtime.strategy_runtime import StrategyRuntime
from selection import expiry_resolver, strike_resolver
from shared.brokers.delta import client as delta_client
from shared.market.delta_instrument_cache import crypto_expiries
from shared.config.settings import get_settings
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger
from shared.market.instrument_master import InstrumentMaster
from shared.market.ltp_cache import LtpCache
from token_router import TokenRouter

_IST = timezone(timedelta(hours=5, minutes=30))

# Bounded wait for a synchronous, immediate (non-scheduled) activation's
# strike resolution — see _ensure_live_prices below. 2.0s was too short for
# a chain that's never been subscribed before this call — confirmed live,
# repeatedly, against real NIFTY/BANKNIFTY monthly chains: Dhan's own
# first-tick latency for a chain nobody has watched yet is NOT a fixed
# number — one cold chain had real LTP at ~15-20s, another otherwise
# identical cold chain still had none at 25s and only showed data on a
# check ~40s after its own subscribe request. There is no bound here that
# is both "always long enough" and "not an obviously bad user-facing wait"
# — 40s is a deliberately generous upper bound for the worst case actually
# observed, accepting that a genuinely cold far-expiry activation can
# legitimately take that long, ONCE. Every subsequent strategy on the SAME
# (underlying, expiry, option_type) combo resolves instantly afterward —
# DhanFeed._subscribed persists across activations (confirmed live) — and
# shared/market/dhan_chain_prewarm.py's current-cycle prewarm avoids this
# cold-start entirely for near expiries, so this long a wait is only ever
# paid on a genuinely far (monthly+) expiry's first-ever activation.
_SUBSCRIBE_WAIT_SECONDS = 40.0
_SUBSCRIBE_POLL_INTERVAL_SECONDS = 0.1


def _trace(strategy_id: str, event: str, **fields: Any) -> None:
    if log.isEnabledFor(logging.DEBUG):
        log.debug("[Activation] strategy=%s event=%s fields=%s", strategy_id, event, fields)


async def _ensure_live_prices(instrument_master: InstrumentMaster, ltp_cache: LtpCache, expiry_option_pairs: set[tuple[str, str]], underlying: str, strategy_id: str = "") -> None:
    """Bridges a real gap: strike_resolver needs a live LTP (in ltp_cache)
    to pick a strike, but DhanFeed only ever ticks a token someone
    explicitly subscribed — and nothing subscribes a fresh strategy's
    candidate strikes ahead of time for an IMMEDIATE (button-click-now)
    activation. The old system's live_entry_monitor.py solves this with a
    5-MINUTE lead time (pre-subscribe well before a scheduled entry_time —
    see that module's own docstring); that lead time doesn't exist for a
    synchronous "/strategy/{id}/activate" call, so this subscribes every
    candidate strike for this activation's (expiry, option_type) combos
    right now and waits briefly (bounded, not indefinite) for DhanFeed to
    start delivering ticks, instead of the leg loop below immediately
    finding nothing and giving up.

    No-ops harmlessly for an underlying instrument_master has no data for
    (e.g. a crypto underlying — that path already has its own separate live
    tick mechanism, see api/live_broadcast.py's legacy-token Delta
    subscribe) since by_underlying_expiry_type then returns nothing to
    subscribe.

    Grouped by each token's OWN ws_segment ("NSE_FNO" vs "BSE_FNO"), not a
    single hardcoded "NSE_FNO" for everything — SENSEX/BANKEX are BSE-listed
    (active_option_tokens' own exchange/ws_segment fields already say so),
    and subscribing a BSE token under the NSE_FNO segment silently asks
    Dhan for a different (nonexistent, from this segment's point of view)
    instrument — confirmed live: a SENSEX activation's whole chain sat at
    zero LTP/bid/ask/OI forever, no error either side, subscribed_count
    still went up (Dhan accepted the request, just for the wrong segment).
    NIFTY/BANKNIFTY/FINNIFTY/MIDCPNIFTY are all NSE-listed and unaffected —
    this was SENSEX(+BANKEX)-specific."""
    tokens_by_segment: dict[str, list[str]] = {}
    for expiry, option_type in expiry_option_pairs:
        for meta in instrument_master.by_underlying_expiry_type(underlying, expiry, option_type):
            seg = meta.ws_segment or "NSE_FNO"
            tokens_by_segment.setdefault(seg, []).append(meta.token)
    if not tokens_by_segment:
        _trace(strategy_id, "prewarm_skipped_no_tokens", underlying=underlying, pairs=len(expiry_option_pairs))
        return

    tokens = [t for toks in tokens_by_segment.values() for t in toks]
    _trace(strategy_id, "prewarm_subscribe_sending", token_count=len(tokens), segments=list(tokens_by_segment.keys()))
    try:
        settings = get_settings()
        async with httpx.AsyncClient(timeout=5.0) as client:
            for seg, seg_tokens in tokens_by_segment.items():
                response = await client.post(f"{settings.websocket_base_url}/ticker/subscribe", json={"tokens": seg_tokens, "exchange": seg})
                response.raise_for_status()
    except Exception:
        get_logger(__name__).exception("[Activation] ticker/subscribe request failed for %s expiry/option combos", len(expiry_option_pairs))
        _trace(strategy_id, "prewarm_subscribe_failed", token_count=len(tokens))
        return
    _trace(strategy_id, "prewarm_subscribe_sent", token_count=len(tokens))

    deadline = time.monotonic() + _SUBSCRIBE_WAIT_SECONDS
    while time.monotonic() < deadline:
        if any(ltp_cache.get_ltp(t) for t in tokens):
            _trace(strategy_id, "prewarm_ltp_ready", waited_ms=int((time.monotonic() - (deadline - _SUBSCRIBE_WAIT_SECONDS)) * 1000))
            return
        await asyncio.sleep(_SUBSCRIBE_POLL_INTERVAL_SECONDS)
    _trace(strategy_id, "prewarm_wait_timed_out", waited_seconds=_SUBSCRIBE_WAIT_SECONDS)

log = get_logger(__name__)


class ActivationError(Exception):
    pass


def cleanup_if_no_legs(router: TokenRouter, strategy_id: str) -> None:
    """A strategy is registered (and its checkpoint queued) BEFORE any leg
    is confirmed to actually enter — the immediate-entry path above handles
    its own all-legs-failed case inline, but the time-gated path
    (EntryIndicators not yet reached -> PendingEntryRuntime per leg) leaves
    the strategy registered while its legs enter one at a time, sometimes
    much later, via services/scheduled_entry.py / debug_scheduled_entry.py.
    If every one of those per-leg attempts ultimately fails to resolve a
    strike (e.g. no live price), nothing was cleaning up the now-permanently-
    empty parent strategy — it stayed ACTIVE with leg_ids=[] forever, and
    got resurrected as a zombie by persistence/recovery.py on every future
    restart. Called after each pending-entry resolution attempt (success or
    failure); a strategy that still has other pending entries in flight, or
    that already has at least one real leg, is left alone.

    The delete goes through the SAME ordered checkpoint queue the original
    upsert was queued on (queue_strategy_delete), not a direct synchronous
    Mongo call — a direct delete_one() here would race the still-pending
    async upsert and could find nothing to delete, letting that upsert land
    *after* this "cleanup" ran and resurrect the zombie anyway (this is
    exactly how the very zombies this function exists to prevent kept
    reappearing before this fix)."""
    strategy = router.strategies.get(strategy_id)
    if strategy is None or strategy.leg_ids:
        return
    still_pending = any(p.strategy_id == strategy_id for p in router.pending_entries.values())
    if still_pending:
        return
    has_watcher = any(w.strategy_id == strategy_id for w in router.lazy_watchers.values()) or \
        any(w.strategy_id == strategy_id for w in router.recost_watchers.values()) or \
        any(w.strategy_id == strategy_id for w in router.range_watchers.values())
    if has_watcher:
        return  # a momentum / range-breakout leg is still waiting to enter
    router.strategies.pop(strategy_id, None)
    broker = router.brokers.get(strategy.broker_scope_id)
    if broker is not None:
        broker.strategy_ids.discard(strategy_id)
    get_checkpoint_writer().queue_strategy_delete(strategy_id)
    log.info("[Activation] strategy=%s had no legs left to enter (all pending attempts failed) — removed", strategy_id)


def _is_sell(position_type: str) -> bool:
    return "sell" in str(position_type or "").lower()


def _leg_option_type(leg_cfg: dict) -> str:
    kind = str(leg_cfg.get("InstrumentKind") or "")
    # leg_builder.py's InstrumentKind.FUTURES ("LegType.Futures") — a plain
    # futures contract, no strike/CE/PE concept at all. "FUT" matches
    # dhan_token_sync.py's own tag for these rows in active_option_tokens
    # (instrument_master.py already loads them, unfiltered by option_type —
    # see selection/strike_resolver.py's own Futures branch for the rest).
    if "Futures" in kind:
        return "FUT"
    return "PE" if "PE" in kind else "CE"


def _leg_qty(leg_cfg: dict, lot_size: int, multiplier: int = 1) -> int:
    """Leg lots (LotConfig) × the strategy's Quantity Multiplier × lot size."""
    lots = int((leg_cfg.get("LotConfig") or {}).get("Value") or 1)
    return max(1, lots) * max(1, int(multiplier or 1)) * max(1, lot_size)


def qty_multiplier_from_doc(strategy_doc: dict) -> int:
    """Edit Setup's Quantity Multiplier (execution_config_base.Multiplier)."""
    base = strategy_doc.get("execution_config_base") or {}
    try:
        return max(1, int(float(base.get("Multiplier") or 1)))
    except (TypeError, ValueError):
        return 1


def _reentry_count(leg_cfg: dict, field: str) -> int:
    cfg = leg_cfg.get(field) or {}
    value = cfg.get("Value")
    if isinstance(value, dict):
        return int(value.get("ReentryCount") or 0)
    return 0


async def activate_strategy(
    router: TokenRouter, order_engine: OrderEngine,
    instrument_master: InstrumentMaster, ltp_cache: LtpCache, mongo: Mongo,
    strategy_doc: dict[str, Any], user_id: str, broker_scope_id: str,
    is_direct_strategy: bool = True,
    group_id: str = "", group_name: str = "", portfolio_id: str = "",
    qty_multiplier: int | None = None,
    slippage_pct: float = 0.0,
    skip_entry_time: bool = False,
) -> dict[str, Any]:
    """`qty_multiplier`: the activation screen's Qty Multiplier (portfolio
    row × portfolio qty) — when given it replaces the strategy's saved Edit
    Setup Multiplier, same precedence as the old system's activation.

    `skip_entry_time`: enter legs right now, ignoring the strategy's
    configured entry time — used by the crypto straddle spike alert
    (services/straddle_alert.py), where the alert trigger IS the entry signal."""
    _source_id = str(strategy_doc.get("_id") or "-")
    _trace("", "activate_strategy_called", source_strategy_id=_source_id, name=strategy_doc.get("name"), broker_scope_id=broker_scope_id, user_id=user_id)

    strategy_cfg = ((strategy_doc.get("full_config") or {}).get("strategy")) or {}
    underlying = delta_client.normalize_underlying(strategy_cfg.get("Ticker"))
    if not underlying:
        _trace("", "activate_rejected", source_strategy_id=_source_id, reason="no_ticker")
        raise ActivationError("Strategy has no Ticker/underlying configured")

    leg_configs = strategy_cfg.get("ListOfLegConfigs") or []
    if not leg_configs:
        _trace("", "activate_rejected", source_strategy_id=_source_id, reason="no_legs_configured")
        raise ActivationError("Strategy has no legs configured")
    _trace("", "leg_configs_loaded", source_strategy_id=_source_id, underlying=underlying, leg_count=len(leg_configs))

    if broker_scope_id and broker_scope_id not in router.brokers:
        try:
            settings_doc = await asyncio.to_thread(broker_scope.load_broker_settings, mongo, user_id, broker_scope_id)
            closed_mtm = await asyncio.to_thread(broker_scope.closed_day_mtm, mongo, broker_scope_id)
        except Exception:
            log.exception("[Activation] broker settings load failed broker=%s", broker_scope_id)
            settings_doc, closed_mtm = None, 0.0
        broker_scope.ensure_broker_runtime(router, broker_scope_id, user_id, settings_doc, closed_mtm)
    broker = router.brokers.get(broker_scope_id)
    if broker is not None and broker.status != "ACTIVE":
        _trace("", "activate_rejected", source_strategy_id=_source_id, reason="broker_not_active", broker_scope_id=broker_scope_id, broker_status=broker.status)
        raise ActivationError(f"Broker scope {broker_scope_id} is not ACTIVE — entries blocked")
    _trace("", "broker_confirmed_active", source_strategy_id=_source_id, broker_scope_id=broker_scope_id)

    strategy_id = f"exec_{uuid.uuid4().hex[:16]}"
    _trace(strategy_id, "strategy_id_generated", source_strategy_id=_source_id)
    # Daily rolling per-instrument portfolio (user's explicit ask — same
    # get-or-create logic the old system's own _resolve_daily_portfolio
    # already implements, ported into daily_portfolio.py, SAME
    # algo_trade_portfolio collection so an old-system BTCUSD activation
    # and an algo-2_0 BTCUSD activation on the same day share one id):
    # every strategy activated for this underlying today gets the SAME
    # trade_portfolio/trade_group_portfolio, a NEW instrument or a new IST
    # calendar day gets a fresh one. This is genuinely separate from
    # group_id/portfolio_id above (the user-chosen SAVED portfolio batch a
    # /portfolio/activate call groups by) — a direct single-strategy
    # activation has no group_id/portfolio_id at all, but still gets a
    # real trade_portfolio here, same as the old system's own single-
    # strategy shadow-portfolio path does.
    daily_trade_portfolio, daily_trade_group_portfolio = await asyncio.to_thread(
        daily_portfolio.resolve_daily_portfolio, mongo, "fast-forward", underlying,
    )
    _trace(strategy_id, "daily_portfolio_resolved", trade_portfolio=daily_trade_portfolio, trade_group_portfolio=daily_trade_group_portfolio)
    # strategy_group_id is the one genuinely per-strategy id here — fresh,
    # unique to EVERY activation (direct or portfolio member alike),
    # unlike group_id which every member of one portfolio batch shares.
    strategy_group_id = str(ObjectId())
    # Looked up ONCE here, cached on the runtime object for every future
    # checkpoint/broadcast to read — never re-queried per-tick. Same fields
    # the old system's own broker_details_map join reads (persistence/
    # serializers.py's load_legacy_execute_order_records). broker_scope_id
    # isn't always a real broker_configuration _id (a debug/test scope like
    # "test:foo" or a composite "user:broker" tag also passes through this
    # same code path), so an invalid ObjectId or a doc that isn't found
    # just leaves broker_details empty — FastForward2.tsx's getBrokerName()
    # already falls back through broker_label/broker for those cases; this
    # only needed to cover the REAL case it never did: a genuine broker_
    # configuration _id, which every actual (non-test) activation uses,
    # showed as "Unknown" in the UI even though the real broker was known.
    broker_details: dict[str, Any] = {}
    try:
        _broker_doc = await asyncio.to_thread(mongo.raw["broker_configuration"].find_one,
            {"_id": ObjectId(broker_scope_id)},
            {"broker_icon": 1, "broker_name": 1, "display_name": 1, "name": 1, "title": 1, "alias": 1, "broker_type": 1},
        )
        if _broker_doc:
            broker_details = {k: v for k, v in _broker_doc.items() if k != "_id"}
    except Exception:
        pass
    strategy_runtime = StrategyRuntime(
        strategy_id=strategy_id, user_id=user_id, name=str(strategy_doc.get("name") or strategy_id),
        is_direct_strategy=is_direct_strategy,
        group_id=group_id, group_name=group_name, portfolio_id=portfolio_id,
        trade_portfolio=daily_trade_portfolio, trade_group_portfolio=daily_trade_group_portfolio, strategy_group_id=strategy_group_id,
        broker_details=broker_details,
        source_strategy_id=str(strategy_doc.get("_id") or ""), full_strategy_cfg=strategy_cfg,
        # Ticker/IdleLegConfigs kept alongside the risk-relevant keys (not
        # just OverallSL/OverallTgt/LockAndTrail) because
        # services/leg_followup.py needs them at exit time to arm Lazy Leg
        # (NextLeg) / re-entry / re-cost followups — strategy_risk.py itself
        # only reads the risk keys and ignores the rest.
        strategy_cfg={
            **{k: strategy_cfg[k] for k in ("OverallSL", "OverallTgt", "OverallTrailSL", "LockAndTrail", "IdleLegConfigs") if k in strategy_cfg},
            "Ticker": underlying,  # normalized ("DELTA_BTCUSD" -> "BTCUSD")
        },
        broker_scope_id=broker_scope_id, activation_in_progress=True,
        activated_date=schedule_engine.trading_day(strike_resolver.is_crypto_underlying(underlying)), qty_multiplier=max(1, int(qty_multiplier)) if qty_multiplier else qty_multiplier_from_doc(strategy_doc),
        slippage_pct=max(0.0, float(slippage_pct or 0)),
    )
    router.register_strategy(strategy_runtime)
    _trace(strategy_id, "strategy_registered_in_router")
    # Set right before handing leg-entry off to _complete_leg_entry below —
    # tells the finally block not to settle activation_in_progress/finalize
    # here, since the background task now owns that (see its own finally).
    handed_off_to_background = False
    try:
        checkpoint_strategy(router, strategy_id)
        _trace(strategy_id, "checkpoint_queued_initial")

        entry_indicators = strategy_cfg.get("EntryIndicators") or {}
        is_crypto_strategy = strike_resolver.is_crypto_underlying(underlying)
        if not skip_entry_time and not schedule_engine.is_time_reached(entry_indicators, is_crypto=is_crypto_strategy):
            target_time = schedule_engine.resolve_target_time_ist(entry_indicators, is_crypto=is_crypto_strategy)
            _trace(strategy_id, "entry_time_not_reached_yet", target_time_ist=target_time)
            # FastForward2.tsx/LiveTrade.tsx's CountdownOrMtm renders "Entry in
            # HH:MM:SS" from `rec.entry_time` (a UTC "YYYY-MM-DD HH:MM:SS"
            # string, see that component's own toUtcMs comment) instead of MTM
            # while still waiting — the same countdown UI the old system's
            # page already has, ported here since nothing set this field
            # before (a scheduled/time-gated activation showed ₹0.00 MTM
            # instead of a countdown). checkpoint_strategy() re-queued right
            # after so Mongo/the socket snapshot both carry it immediately, not
            # just whenever the next periodic checkpoint cycle happens to fire.
            if target_time is not None:
                strategy_runtime.entry_time = target_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                checkpoint_strategy(router, strategy_id)
            pending_ids: list[str] = []
            for leg_cfg in leg_configs:
                pending = PendingEntryRuntime(
                    pending_id=f"pend_{uuid.uuid4().hex[:10]}", strategy_id=strategy_id,
                    broker_scope_id=broker_scope_id, leg_cfg=leg_cfg, target_time_ist=target_time,
                )
                router.register_pending_entry(pending)
                pending_ids.append(pending.pending_id)
                _trace(strategy_id, "leg_registered_as_pending", pending_id=pending.pending_id, leg_cfg_id=leg_cfg.get("id"))
            log.info("[Activation] strategy=%s scheduled for %s IST — %d legs pending", strategy_id, target_time, len(pending_ids))
            # register_strategy() + checkpoint_strategy() above already wrote
            # this strategy into algo_trades (0 legs yet) — no separate insert
            # needed (this used to be a second, conflicting direct write to the
            # same _id before the algo_trades consolidation).
            _trace(strategy_id, "activate_strategy_returning", mode="pending_scheduled", pending_count=len(pending_ids))
            return {"strategy_id": strategy_id, "leg_ids": [], "pending_entry_ids": pending_ids, "scheduled_for": target_time.isoformat()}
        _trace(strategy_id, "entry_time_reached_entering_now")

        # Real leg entry (chain subscribe-and-wait + per-leg strike
        # resolution below) is the genuinely slow part — bounded at up to
        # _SUBSCRIBE_WAIT_SECONDS=40s per cold chain (see _ensure_live_
        # prices's own docstring), and typically ~1-3s even on a warm one.
        # Awaiting this inline used to make the WHOLE HTTP request wait for
        # it, and portfolio.py's /activate loop calls this once per member
        # SEQUENTIALLY — user's explicit report: a 3-strategy portfolio
        # activation took 3+ seconds end-to-end, and a 10-strategy group
        # took 10-20s, because each member's real entry was awaited one at
        # a time before the response could return. The strategy is already
        # registered + checkpointed (ACTIVE) above at this point — that's
        # the fast "mark it active" half the user wants back immediately;
        # actual leg entry now runs as a background task instead, and
        # reaches the frontend the normal way once it lands (a tick-driven
        # broadcast, or the explicit push _complete_leg_entry itself sends
        # the moment legs are in) — same "socket emit happens outside this
        # function" model the rest of this module already documents.
        handed_off_to_background = True
        # Trigger-driven entry: only quotes from the trigger moment. The task
        # copies the context at creation, so its strike/price lookups see the
        # tighter limit; reset right after so the caller's context is untouched.
        quote_age_token = strike_resolver.crypto_quote_max_age.set(strike_resolver._TRIGGER_MAX_QUOTE_AGE_SECONDS) if skip_entry_time else None
        asyncio.create_task(
            _complete_leg_entry(router, order_engine, instrument_master, ltp_cache, strategy_runtime, strategy_id, leg_configs, underlying, user_id, broker_scope_id),
            name=f"activate_entry_{strategy_id}",
        )
        if quote_age_token is not None:
            strike_resolver.crypto_quote_max_age.reset(quote_age_token)
        _trace(strategy_id, "activate_strategy_returning", mode="entering_in_background")
        return {"strategy_id": strategy_id, "leg_ids": [], "status": "activating"}
    finally:
        # The pending-scheduled branch above (return at "pending_scheduled")
        # settles synchronously here same as before this split — resets
        # activation_in_progress, then finalize_if_no_open_legs no-ops
        # because it still has pending entries in flight. The background-
        # entry branch just above sets handed_off_to_background=True right
        # before scheduling the task, specifically so THIS finally leaves
        # activation_in_progress alone (still True) and does NOT finalize —
        # _complete_leg_entry's own finally is what settles both, once
        # entry actually finishes (this function has already returned by
        # then, so doing it here too would race the background task).
        if not handed_off_to_background:
            strategy_runtime.activation_in_progress = False
            strategy_finalize.finalize_and_publish(router, strategy_id)


async def _complete_leg_entry(
    router: TokenRouter, order_engine: OrderEngine, instrument_master: InstrumentMaster, ltp_cache: LtpCache,
    strategy_runtime: StrategyRuntime, strategy_id: str, leg_configs: list[dict[str, Any]], underlying: str,
    user_id: str, broker_scope_id: str,
) -> None:
    """Background half of activate_strategy's immediate-entry path — see
    the asyncio.create_task call site above for why this had to move off
    the HTTP request path. Logic below is otherwise unchanged from the old
    fully-synchronous version. Errors are logged, not raised (nothing is
    awaiting this task) — finalize_if_no_open_legs in the finally block
    already cleans up a strategy that ends up with zero entered legs,
    same as the old ActivationError("No legs could be entered") path did
    for the caller, just without an HTTP response to carry the message."""
    try:
        is_crypto = strike_resolver.is_crypto_underlying(underlying)
        spot = (ltp_cache.get_spot(underlying) or ltp_cache.get_ltp(underlying) or 0.0) if is_crypto else (ltp_cache.get_spot(underlying) or 0.0)
        leg_ids: list[str] = []
        skipped: list[dict[str, Any]] = []
        armed_momentum = 0  # legs armed on LegMomentum below, not entered yet — not a failure
        armed_range = 0  # legs armed on LegRangeBreakout below, not entered yet — not a failure
        _trace(strategy_id, "underlying_type_resolved", underlying=underlying, is_crypto=is_crypto, spot=spot)

        if is_crypto:
            # No WS-subscribe-and-wait for crypto — shared/brokers/delta/client.py's
            # REST chain fetch (see strike_resolver.build_chain_rows) is a single
            # bounded call per leg's own resolve_strike below, same as the old
            # system's real, working entry path. Nothing to pre-warm here.
            _trace(strategy_id, "prewarm_skipped_crypto")
        else:
            # Pre-pass: which (expiry, option_type) combos this activation's legs
            # actually need, resolved once up front so _ensure_live_prices can
            # subscribe+wait for all of them together (one bounded wait total, not
            # one per leg) before the real per-leg resolution loop below runs.
            expiry_option_pairs: set[tuple[str, str]] = set()
            for leg_cfg in leg_configs:
                option_type = _leg_option_type(leg_cfg)
                expiry_kind = str(leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
                expiries = instrument_master.expiries_for(underlying, option_type)
                expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
                if expiry:
                    expiry_option_pairs.add((expiry, option_type))
            _trace(strategy_id, "prewarm_prepass_done", pairs=sorted(expiry_option_pairs))
            await _ensure_live_prices(instrument_master, ltp_cache, expiry_option_pairs, underlying, strategy_id)
            spot = ltp_cache.get_spot(underlying) or spot
            _trace(strategy_id, "spot_after_prewarm", spot=spot)

        for leg_cfg in leg_configs:
            leg_id = f"{strategy_id}_{leg_cfg.get('id') or uuid.uuid4().hex[:6]}"
            option_type = _leg_option_type(leg_cfg)
            is_sell = _is_sell(leg_cfg.get("PositionType"))
            expiry_kind = str(leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
            entry_type = str(leg_cfg.get("EntryType") or "EntryType.EntryByStrikeType")
            strike_param = leg_cfg.get("StrikeParameter")
            _trace(strategy_id, "leg_processing_start", leg_id=leg_id, option_type=option_type, is_sell=is_sell, entry_type=entry_type, expiry_kind=expiry_kind)

            if is_crypto:
                trade_date = datetime.now(_IST).strftime("%Y-%m-%d")
                expiry = delta_client.resolve_delta_expiry(trade_date, expiry_kind, await asyncio.to_thread(crypto_expiries, underlying))
                not_found_reason = "no expiry available from Delta Exchange"
            else:
                expiries = instrument_master.expiries_for(underlying, option_type)
                expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
                not_found_reason = "no expiry available in instrument_master"
            if not expiry:
                _trace(strategy_id, "leg_skipped", leg_id=leg_id, reason=not_found_reason)
                skipped.append({"leg_id": leg_id, "reason": not_found_reason})
                continue
            _trace(strategy_id, "leg_expiry_resolved", leg_id=leg_id, expiry=expiry)

            selection = await asyncio.to_thread(
                strike_resolver.resolve_strike, instrument_master, ltp_cache, underlying, expiry, option_type,
                entry_type, strike_param, spot, is_sell,
            )
            if selection is None:
                _trace(strategy_id, "leg_skipped", leg_id=leg_id, reason="strike_not_resolved")
                skipped.append({"leg_id": leg_id, "reason": "strike not resolved (no live price / no match in range)"})
                continue
            _trace(strategy_id, "leg_strike_resolved", leg_id=leg_id, token=selection.token, strike=selection.strike, symbol=selection.symbol, ltp=selection.ltp)

            if is_crypto:
                lot_size = 1  # Delta contracts are sized directly, no separate lot multiple (see client.py's DELTA_CONTRACT_VALUE scaling)
                # Don't wait for delta_instrument_cache's periodic (30s) sweep to
                # pick this token up — subscribe it on DeltaFeed's mark_price
                # channel right now, so this leg's own LTP starts ticking the
                # instant it's entered, not up to 30s later (see shared/market/
                # delta_instrument_cache.py's subscribe_tokens_now docstring —
                # this is crypto's counterpart to the NSE branch's own
                # _ensure_live_prices call above, same "subscribe at entry
                # time" guarantee).
                from shared.market.delta_instrument_cache import subscribe_tokens_now
                await asyncio.to_thread(subscribe_tokens_now, [selection.token])
            else:
                meta = instrument_master.get(selection.token)
                lot_size = (meta.lot_size if meta else None) or 75
            qty = _leg_qty(leg_cfg, lot_size, strategy_runtime.qty_multiplier)
            _trace(strategy_id, "leg_qty_computed", leg_id=leg_id, qty=qty, lot_size=lot_size)

            # Network awaits allow broker exits/manual actions to run. Recheck
            # ownership and gates on the event-loop thread before committing.
            current_broker = router.brokers.get(broker_scope_id)
            if (router.strategies.get(strategy_id) is not strategy_runtime
                    or strategy_runtime.status != "ACTIVE"
                    or (current_broker is not None and current_broker.status != "ACTIVE")):
                cleanup_if_no_legs(router, strategy_id)
                raise ActivationError("Strategy or broker is no longer ACTIVE — entry cancelled")

            # LegRangeBreakout gate (ORB/BTST) — checked BEFORE LegMomentum,
            # since AlgoTest's own leg-builder docs (and shared/features/
            # range_breakout.py's own docstring) say the two are mutually
            # exclusive and RangeBreakout wins when both are configured.
            # This leg's already-resolved `selection` above is discarded
            # for this path (harmless wasted work, not incorrect) — a
            # range-breakout leg's real contract is chosen later, either
            # early during range-collection (Instrument/BTSTInstrument,
            # frozen once) or fresh at the breakout instant (Underlying/
            # BTSTUnderlying) — see services/range_breakout_service.py's
            # own docstring. Local import to avoid a real import cycle
            # (range_breakout_service imports _leg_qty/_reentry_count back
            # from this module).
            rb_type, _rb_condition, _rb_start, _rb_end = range_breakout_engine.parse_leg_range_breakout(leg_cfg)
            if rb_type != "None":
                from services import range_breakout_service
                range_watcher = range_breakout_service.arm_range_watcher(
                    strategy_id=strategy_id, leg_ref=str(leg_cfg.get("id") or leg_id), underlying=underlying,
                    leg_cfg=leg_cfg, option_type=option_type, is_sell=is_sell, qty=qty,
                    broker_scope_id=broker_scope_id, user_id=user_id,
                )
                if range_watcher is None:
                    _trace(strategy_id, "leg_skipped", leg_id=leg_id, reason="range_breakout_config_invalid")
                    skipped.append({"leg_id": leg_id, "reason": "LegRangeBreakout config invalid"})
                    continue
                router.register_range_watcher(range_watcher)
                armed_range += 1
                _trace(strategy_id, "leg_armed_on_range_breakout", leg_id=leg_id, rb_type=rb_type, day1=range_watcher.day1, day2=range_watcher.day2)
                log.info("[Activation] leg=%s armed on LegRangeBreakout type=%s day1=%s day2=%s", leg_id, rb_type, range_watcher.day1, range_watcher.day2)
                continue

            # LegMomentum gate (this leg's FIRST-EVER entry, not a re-entry —
            # see leg_followup.py's _trigger_like_original/_arm_next_leg for
            # the already-working re-entry-time momentum wait): user's
            # explicit report — a leg with LegMomentum configured (e.g.
            # "wait for a 36% move before entering") was entering immediately
            # alongside its non-momentum siblings, because nothing here ever
            # read LegMomentum at all before placing the entry order. Reuses
            # lazy_engine's existing arm/trigger machinery unchanged (same
            # "freeze strike/token once, wait for the frozen trigger_price,
            # never re-select" semantics conditions/lazy_engine.py already
            # implements for re-entries) rather than building a second,
            # parallel momentum-wait mechanism — this strike is ALREADY
            # resolved and frozen (selection, above) by the time this gate
            # runs, same as the old system's own real behavior (execution_
            # socket.py:5559-5745: strike is resolved early specifically so
            # momentum can arm on a fixed contract). main.py's
            # _on_lazy_or_recost_triggered completes the real entry once
            # price crosses the frozen target — this function's own job for
            # a momentum-gated leg ends at arming, same as it already does
            # for a time-gated (EntryIndicators) one above.
            momentum_cfg = leg_cfg.get("LegMomentum") or {}
            momentum_type = str(momentum_cfg.get("Type") or "")
            momentum_value = float(momentum_cfg.get("Value") or 0)
            if "None" not in momentum_type and momentum_value > 0:
                watcher = lazy_engine.arm_lazy_watcher(
                    lazy_id=f"lazy_init_{uuid.uuid4().hex[:10]}", strategy_id=strategy_id, parent_leg_id=leg_id,
                    token=selection.token, strike=selection.strike, option_type=option_type,
                    momentum_type=momentum_type, momentum_value=momentum_value, reference_ltp=selection.ltp,
                    qty=qty, is_sell=is_sell, leg_cfg=leg_cfg, broker_scope_id=broker_scope_id, user_id=user_id, expiry=expiry,
                )
                if watcher is None:
                    _trace(strategy_id, "leg_skipped", leg_id=leg_id, reason="momentum_config_invalid")
                    skipped.append({"leg_id": leg_id, "reason": "LegMomentum config invalid (bad Type/Value or zero reference price)"})
                    continue
                router.register_lazy_watcher(watcher)
                armed_momentum += 1
                _trace(strategy_id, "leg_armed_on_momentum", leg_id=leg_id, token=selection.token, momentum_type=momentum_type, momentum_value=momentum_value, reference_ltp=selection.ltp)
                log.info("[Activation] leg=%s armed on LegMomentum type=%s value=%s reference=%s token=%s", leg_id, momentum_type, momentum_value, selection.ltp, selection.token)
                continue

            order_id = order_engine.broker.place_order(
                tradingsymbol=selection.symbol or selection.token, **order_venue(selection.symbol or selection.token),
                transaction_type="SELL" if is_sell else "BUY",
                quantity=qty, order_type="MARKET",
                fill_price=selection.ltp,
            )
            _trace(strategy_id, "leg_order_placed", leg_id=leg_id, order_id=order_id, fill_price=selection.ltp)

            leg = LegRuntime(
                leg_id=leg_id, strategy_id=strategy_id, user_id=user_id, token=selection.token,
                is_sell=is_sell, option_type=option_type, qty=qty, entry_price=selection.ltp,
                entry_spot=spot, leg_cfg=leg_cfg, current_price=selection.ltp,
                strike=selection.strike, expiry=expiry,
                broker_scope_id=broker_scope_id,
                reentry_max=_reentry_count(leg_cfg, "LegReentrySL") or _reentry_count(leg_cfg, "LegReentryTP"),
            )
            sl_tp_engine.initialize_sl_tp(leg)
            router.register_leg(leg)
            leg_ids.append(leg_id)
            log.info(
                "[Activation] leg entered leg=%s token=%s strike=%s entry=%s order_id=%s",
                leg_id, selection.token, selection.strike, selection.ltp, order_id,
            )
            _trace(strategy_id, "leg_entered_registered_in_router", leg_id=leg_id, token=selection.token, entry_price=selection.ltp)

        if not leg_ids and not armed_momentum and not armed_range:
            _trace(strategy_id, "activation_failed_no_legs_entered", skipped=skipped)
            log.warning("[Activation] strategy=%s entered no legs, will be cleaned up: %s", strategy_id, skipped)
            return

        # A strategy with EVERY leg armed on LegMomentum/LegRangeBreakout
        # (leg_ids empty, armed_momentum/armed_range > 0) still needs this
        # same checkpoint+broadcast — same "0 legs yet, but genuinely still
        # activating" state a time-gated (EntryIndicators pending) strategy
        # already shows, not a failure. finalize_if_no_open_legs (this
        # function's own finally, below) won't finalize it either, since it
        # now has a pending lazy/range watcher — same has_pending_watcher
        # guard NextLeg/AtCost already rely on.
        checkpoint_strategy(router, strategy_id)  # one snapshot for all legs entered/armed above, not per-leg
        _trace(strategy_id, "checkpoint_queued_final", leg_count=len(leg_ids), armed_momentum=armed_momentum, armed_range=armed_range, skipped_count=len(skipped))

        # Explicit push the moment these legs actually land — the HTTP
        # response for this activation already went out (with leg_ids=[])
        # back when this was scheduled, so without this the frontend would
        # otherwise only learn real entry happened off the next tick on one
        # of these tokens (broadcast_tick_update), same gap every other
        # explicit-push call site in this codebase (main.py's trigger
        # listeners, ws_live.py's square_off) already exists to close.
        if strategy_runtime.user_id:
            from api.live_broadcast import broadcast_strategy_snapshots
            await broadcast_strategy_snapshots(router, [strategy_id])
        _trace(strategy_id, "background_entry_complete", leg_ids=leg_ids, skipped=skipped)
    except Exception:
        # Nothing is awaiting this task (see the create_task call site) —
        # an ActivationError raised above (e.g. "Strategy or broker is no
        # longer ACTIVE") or any other failure must be logged here or it's
        # lost entirely, not surfaced as an HTTP error the way it used to be.
        log.exception("[Activation] background leg entry failed strategy=%s", strategy_id)
    finally:
        strategy_runtime.activation_in_progress = False
        strategy_finalize.finalize_and_publish(router, strategy_id)


def parse_overall_reentry(strategy_full_cfg: dict, key: str) -> tuple[str, int]:
    """OverallReentrySL/OverallReentryTgt — exact port of shared/features/
    position_manager.py's _parse_overall_reentry. Lives on the UNTRIMMED
    full_strategy_cfg (not StrategyRuntime.strategy_cfg, which is
    deliberately trimmed to OverallSL/OverallTgt/LockAndTrail/Ticker/
    IdleLegConfigs only — see that field's own comment at construction)."""
    cfg = strategy_full_cfg.get(key) or {}
    stype = str(cfg.get("Type") or "")
    if not stype or "None" in stype:
        return "None", 0
    value = cfg.get("Value")
    count = 0
    if isinstance(value, dict):
        count = int(value.get("ReentryCount") or 0)
    elif value is not None:
        count = int(value or 0)
    return stype, count


async def handle_overall_reentry(
    router: TokenRouter, order_engine: OrderEngine, instrument_master: InstrumentMaster, ltp_cache: LtpCache,
    strategy_id: str, kind: str,
) -> None:
    """Whole-strategy reentry after an OverallSL/OverallTgt cycle hit —
    OverallReentrySL/OverallReentryTgt. Ported from shared/features/
    trading_core.py's real overall-SL/Target branches (~3040-3140): NOT a
    fresh strategy_id — the SAME strategy re-enters every one of its
    ORIGINAL ListOfLegConfigs legs (from strategy.full_strategy_cfg),
    peak_mtm resets to 0, and the effective threshold steps to the next
    cycle via overall_risk.resolve_overall_cycle_value — the SAME formula
    check_overall_sl_hit/check_overall_target_hit already apply going
    forward (sl_reentry_done/tgt_reentry_done on StrategyRuntime already
    existed and were already read by those two functions; nothing ever
    incremented them or acted on the hit before this).

    Only an "Immediate" reentry is implemented (enter ASAP, no waiting
    condition) — old system's requeue_all_legs_for_overall_reentry accepts
    a `reentry_type` that could ALSO be LikeOriginal/AtCost-style, but
    nothing here builds per-leg cost/momentum tracking at the OVERALL
    scope; a non-Immediate OverallReentrySL/Tgt.Type still gets an
    Immediate-style reentry (logged), not silently dropped — same
    "flagged gap, not a faked implementation" convention this module
    already follows elsewhere (see the module docstring's own list).

    Caller (main.py's _on_overall_reentry) has already set
    strategy.activation_in_progress = True SYNCHRONOUSLY, before this task
    was even scheduled — same "keep finalize_if_no_open_legs from popping
    this strategy out from under an in-flight background entry" guard
    activate_strategy's own background half (_complete_leg_entry) relies
    on; without it, _on_trigger_checkpoint's own finalize check (which
    runs right after, same event, same synchronous dispatch) would have
    already deleted this strategy by the time this coroutine gets its
    first chance to run."""
    strategy = router.strategies.get(strategy_id)
    if strategy is None:
        return
    try:
        strategy.status = "ACTIVE"
        strategy.peak_mtm = 0.0
        if kind == "SL":
            _, base_val = overall_risk.parse_overall_sl(strategy.strategy_cfg)
            strategy.sl_reentry_done += 1
            strategy.current_overall_sl_threshold = overall_risk.resolve_overall_cycle_value(base_val, strategy.sl_reentry_done)
            cycle_no = strategy.sl_reentry_done
        else:
            strategy.tgt_reentry_done += 1
            cycle_no = strategy.tgt_reentry_done

        underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper()
        leg_configs = strategy.full_strategy_cfg.get("ListOfLegConfigs") or []
        is_crypto = strike_resolver.is_crypto_underlying(underlying)
        spot = (ltp_cache.get_spot(underlying) or ltp_cache.get_ltp(underlying) or 0.0) if is_crypto else (ltp_cache.get_spot(underlying) or 0.0)

        if not is_crypto:
            expiry_option_pairs: set[tuple[str, str]] = set()
            for leg_cfg in leg_configs:
                option_type = _leg_option_type(leg_cfg)
                expiry_kind = str(leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
                expiries = instrument_master.expiries_for(underlying, option_type)
                expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
                if expiry:
                    expiry_option_pairs.add((expiry, option_type))
            await _ensure_live_prices(instrument_master, ltp_cache, expiry_option_pairs, underlying, strategy_id)
            spot = ltp_cache.get_spot(underlying) or spot

        leg_ids: list[str] = []
        for leg_cfg in leg_configs:
            leg_id = f"{strategy_id}_ore{cycle_no}_{leg_cfg.get('id') or uuid.uuid4().hex[:6]}"
            option_type = _leg_option_type(leg_cfg)
            is_sell = _is_sell(leg_cfg.get("PositionType"))
            expiry_kind = str(leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
            entry_type = str(leg_cfg.get("EntryType") or "EntryType.EntryByStrikeType")
            strike_param = leg_cfg.get("StrikeParameter")

            if is_crypto:
                trade_date = datetime.now(_IST).strftime("%Y-%m-%d")
                expiry = delta_client.resolve_delta_expiry(trade_date, expiry_kind, await asyncio.to_thread(crypto_expiries, underlying))
            else:
                expiries = instrument_master.expiries_for(underlying, option_type)
                expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
            if not expiry:
                log.warning("[OverallReentry] strategy=%s leg skipped — no expiry", strategy_id)
                continue

            selection = await asyncio.to_thread(
                strike_resolver.resolve_strike, instrument_master, ltp_cache, underlying, expiry, option_type,
                entry_type, strike_param, spot, is_sell,
            )
            if selection is None:
                log.warning("[OverallReentry] strategy=%s leg skipped — strike not resolved", strategy_id)
                continue

            if is_crypto:
                lot_size = 1
                from shared.market.delta_instrument_cache import subscribe_tokens_now
                await asyncio.to_thread(subscribe_tokens_now, [selection.token])
            else:
                meta = instrument_master.get(selection.token)
                lot_size = (meta.lot_size if meta else None) or 75
            qty = _leg_qty(leg_cfg, lot_size, strategy.qty_multiplier)

            if router.strategies.get(strategy_id) is not strategy or strategy.status != "ACTIVE":
                break  # squared off / broker locked mid-reentry — stop entering more legs

            order_id = order_engine.broker.place_order(
                tradingsymbol=selection.symbol or selection.token, **order_venue(selection.symbol or selection.token),
                transaction_type="SELL" if is_sell else "BUY",
                quantity=qty, order_type="MARKET", fill_price=selection.ltp,
            )
            leg = LegRuntime(
                leg_id=leg_id, strategy_id=strategy_id, user_id=strategy.user_id, token=selection.token,
                is_sell=is_sell, option_type=option_type, qty=qty, entry_price=selection.ltp,
                entry_spot=spot, leg_cfg=leg_cfg, current_price=selection.ltp,
                strike=selection.strike, expiry=expiry,
                broker_scope_id=strategy.broker_scope_id,
                reentry_max=_reentry_count(leg_cfg, "LegReentrySL") or _reentry_count(leg_cfg, "LegReentryTP"),
            )
            sl_tp_engine.initialize_sl_tp(leg)
            router.register_leg(leg)
            leg_ids.append(leg_id)
            log.info("[OverallReentry] strategy=%s leg entered leg=%s token=%s price=%s order_id=%s", strategy_id, leg_id, selection.token, selection.ltp, order_id)

        if not leg_ids:
            log.warning("[OverallReentry] strategy=%s no legs re-entered — will be cleaned up", strategy_id)
            return

        checkpoint_strategy(router, strategy_id)
        if strategy.user_id:
            from api.live_broadcast import broadcast_strategy_snapshots
            await broadcast_strategy_snapshots(router, [strategy_id])
        log.info("[OverallReentry] strategy=%s cycle=%s kind=%s complete legs=%s", strategy_id, cycle_no, kind, leg_ids)
    except Exception:
        log.exception("[OverallReentry] strategy=%s failed", strategy_id)
    finally:
        strategy.activation_in_progress = False
        strategy_finalize.finalize_and_publish(router, strategy_id)
