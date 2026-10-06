"""
range_breakout_service.py
──────────────────────────
Async half of LegRangeBreakout (ORB/BTST) — conditions/range_breakout_
engine.py holds the pure parsing/range/breakout-check functions this
module calls; this module owns everything with a side effect (resolving/
freezing an option contract, placing the entry order, checkpointing).

advance_all() is called periodically (main.py's own polling-loop
convention — see _daily_broker_reset_loop for the same "poll every N
seconds, not sleep-until-exact-instant" approach) instead of being driven
by token_router's per-tick dispatch: range-collection/day-rollover
transitions are WALL-CLOCK events (e.g. "has 09:30 arrived", "has the
calendar day rolled to day2"), not token-tick events — a watcher can need
to transition even on a token that never ticks at exactly that moment.
Breakout-price checks piggyback on the SAME poll instead of a separate
per-tick registration, trading a little latency (bounded by the poll
interval) for not needing a second dispatch path — same trade-off this
whole feature's old-system ancestor (leg_range_monitor.py) already made at
a 1-second poll.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from conditions import range_breakout_engine as rbe
from orders.order_engine import OrderEngine, order_venue
from persistence.checkpoint import checkpoint_strategy, get_checkpoint_writer
from risk import sl_tp_engine
from runtime.leg_runtime import LegRuntime
from runtime.range_watcher_runtime import RangeWatcherRuntime
from selection import expiry_resolver, strike_resolver
from services import strategy_finalize
from services.strategy_activation import _leg_qty, _reentry_count
from shared.brokers.delta import client as delta_client
from shared.market.delta_instrument_cache import crypto_expiries
from shared.logging.logger import get_logger
from shared.market.instrument_master import InstrumentMaster
from shared.market.ltp_cache import LtpCache
from token_router import TokenRouter

log = get_logger(__name__)


def arm_range_watcher(
    strategy_id: str, leg_ref: str, underlying: str, leg_cfg: dict[str, Any],
    option_type: str, is_sell: bool, qty: int, broker_scope_id: str, user_id: str,
) -> RangeWatcherRuntime | None:
    """Called from services/strategy_activation.py's entry loop in place of
    an immediate (or LegMomentum-gated) entry, when LegRangeBreakout is
    configured — see that call site's own comment for the "mutually
    exclusive with LegMomentum" ordering. day1/day2 resolved once, at arm
    time, exactly like every other watcher in this codebase freezes its
    starting state once and never re-derives it."""
    rb_type, condition, start_hhmm, end_hhmm = rbe.parse_leg_range_breakout(leg_cfg)
    if rb_type == "None":
        return None
    day1 = rbe.today_ist_date()
    day2 = _next_calendar_day(day1) if rbe.is_btst(rb_type) else day1
    return RangeWatcherRuntime(
        range_id=f"range_{uuid.uuid4().hex[:10]}", strategy_id=strategy_id, leg_ref=leg_ref,
        underlying=underlying, rb_type=rb_type, condition=condition,
        start_hhmm=start_hhmm, end_hhmm=end_hhmm, day1=day1, day2=day2,
        leg_cfg=leg_cfg, option_type=option_type, is_sell=is_sell, qty=qty,
        broker_scope_id=broker_scope_id, user_id=user_id,
    )


def _next_calendar_day(day: str) -> str:
    from datetime import datetime, timedelta
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


async def advance_all(router: TokenRouter, order_engine: OrderEngine, instrument_master: InstrumentMaster, ltp_cache: LtpCache) -> None:
    for range_id in list(router.range_watchers.keys()):
        watcher = router.range_watchers.get(range_id)
        if watcher is None:
            continue
        try:
            await _advance_one(router, order_engine, instrument_master, ltp_cache, watcher)
        except Exception:
            log.exception("[RangeBreakout] advance failed range=%s", range_id)


async def _advance_one(router: TokenRouter, order_engine: OrderEngine, instrument_master: InstrumentMaster, ltp_cache: LtpCache, watcher: RangeWatcherRuntime) -> None:
    strategy = router.strategies.get(watcher.strategy_id)
    if strategy is None or strategy.status != "ACTIVE":
        router.unregister_range_watcher(watcher.range_id)
        return

    today = rbe.today_ist_date()
    hhmm = rbe.now_ist_hhmm()
    same_day = watcher.day1 == watcher.day2

    if watcher.state == "CollectingDay1":
        if today < watcher.day1:
            return
        if today > watcher.day1:
            # Missed the whole day1 window (process downtime, etc.) — a
            # same-day range can't recover (its entire window is gone);
            # BTST just falls through to waiting for day2.
            watcher.state = "Cancelled" if same_day else "WaitingForNextSession"
            _finalize_if_cancelled(router, watcher)
            return
        if hhmm < watcher.start_hhmm:
            return
        if same_day and hhmm >= watcher.end_hhmm:
            await _freeze(watcher, router, instrument_master, ltp_cache)
            return
        if not same_day and hhmm >= rbe.MARKET_CLOSE_HHMM:
            watcher.state = "WaitingForNextSession"
            return
        price = await _get_price(watcher, instrument_master, ltp_cache)
        rbe.update_range(watcher, price)

    elif watcher.state == "WaitingForNextSession":
        if today >= watcher.day2:
            watcher.state = "CollectingDay2"

    elif watcher.state == "CollectingDay2":
        if today < watcher.day2 or hhmm < rbe.MARKET_OPEN_HHMM:
            return
        if hhmm >= watcher.end_hhmm:
            await _freeze(watcher, router, instrument_master, ltp_cache)
            return
        price = await _get_price(watcher, instrument_master, ltp_cache)
        rbe.update_range(watcher, price)

    elif watcher.state == "RangeFrozen":
        watcher.state = "WaitingForBreakout"

    elif watcher.state == "WaitingForBreakout":
        price = await _get_price(watcher, instrument_master, ltp_cache)
        if price and rbe.check_breakout(watcher, price):
            await _complete_range_entry(router, order_engine, instrument_master, ltp_cache, watcher, price)


def _finalize_if_cancelled(router: TokenRouter, watcher: RangeWatcherRuntime) -> None:
    if watcher.state != "Cancelled":
        return
    router.unregister_range_watcher(watcher.range_id)
    strategy_finalize.finalize_and_publish(router, watcher.strategy_id)


async def _freeze(watcher: RangeWatcherRuntime, router: TokenRouter, instrument_master: InstrumentMaster, ltp_cache: LtpCache) -> None:
    if watcher.range_high is None or watcher.range_low is None:
        log.warning("[RangeBreakout] range=%s no price data in window — cancelled", watcher.range_id)
        watcher.state = "Cancelled"
        _finalize_if_cancelled(router, watcher)
        return
    watcher.state = "RangeFrozen"
    log.info("[RangeBreakout] range=%s frozen high=%s low=%s", watcher.range_id, watcher.range_high, watcher.range_low)


async def _get_price(watcher: RangeWatcherRuntime, instrument_master: InstrumentMaster, ltp_cache: LtpCache) -> float:
    if rbe.is_watch_price_underlying(watcher.rb_type):
        is_crypto = strike_resolver.is_crypto_underlying(watcher.underlying)
        if is_crypto:
            return ltp_cache.get_spot(watcher.underlying) or ltp_cache.get_ltp(watcher.underlying) or 0.0
        return ltp_cache.get_spot(watcher.underlying) or 0.0

    # Instrument/BTSTInstrument — tracks THIS leg's own option premium,
    # contract chosen once (early) and frozen for the whole window, same
    # as the old system's _resolve_and_freeze_contract.
    if not watcher.token:
        await _resolve_and_freeze_contract(watcher, instrument_master, ltp_cache)
        return 0.0  # contract not ready yet this cycle — retry next
    return ltp_cache.get_ltp(watcher.token) or 0.0


async def _resolve_and_freeze_contract(watcher: RangeWatcherRuntime, instrument_master: InstrumentMaster, ltp_cache: LtpCache) -> None:
    underlying = watcher.underlying
    is_crypto = strike_resolver.is_crypto_underlying(underlying)
    expiry_kind = str(watcher.leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
    if is_crypto:
        from datetime import datetime, timedelta, timezone
        ist = timezone(timedelta(hours=5, minutes=30))
        trade_date = datetime.now(ist).strftime("%Y-%m-%d")
        expiry = delta_client.resolve_delta_expiry(trade_date, expiry_kind, await asyncio.to_thread(crypto_expiries, underlying))
        spot = ltp_cache.get_spot(underlying) or ltp_cache.get_ltp(underlying) or 0.0
    else:
        expiries = instrument_master.expiries_for(underlying, watcher.option_type)
        expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
        spot = ltp_cache.get_spot(underlying) or 0.0
    if not expiry or spot <= 0:
        return
    selection = await asyncio.to_thread(
        strike_resolver.resolve_strike, instrument_master, ltp_cache, underlying, expiry, watcher.option_type,
        str(watcher.leg_cfg.get("EntryType") or "EntryType.EntryByStrikeType"), watcher.leg_cfg.get("StrikeParameter"),
        spot, watcher.is_sell,
    )
    if selection is None:
        return
    watcher.token, watcher.symbol, watcher.strike, watcher.expiry = selection.token, selection.symbol, selection.strike, expiry
    if is_crypto:
        from shared.market.delta_instrument_cache import subscribe_tokens_now
        await asyncio.to_thread(subscribe_tokens_now, [selection.token])
    log.info("[RangeBreakout] range=%s contract frozen token=%s strike=%s expiry=%s", watcher.range_id, selection.token, selection.strike, expiry)


async def _complete_range_entry(router: TokenRouter, order_engine: OrderEngine, instrument_master: InstrumentMaster, ltp_cache: LtpCache, watcher: RangeWatcherRuntime, breakout_price: float) -> None:
    strategy = router.strategies.get(watcher.strategy_id)
    broker = router.brokers.get(watcher.broker_scope_id) if watcher.broker_scope_id else None
    if strategy is None or strategy.status != "ACTIVE" or (broker is not None and broker.status != "ACTIVE"):
        watcher.state = "Cancelled"
        _finalize_if_cancelled(router, watcher)
        return

    if rbe.is_watch_price_underlying(watcher.rb_type):
        # No contract was ever frozen for an Underlying-type watcher —
        # resolve one now, fresh, at the breakout instant (same as a
        # normal immediate entry — see strategy_activation.py's own loop).
        is_crypto = strike_resolver.is_crypto_underlying(watcher.underlying)
        expiry_kind = str(watcher.leg_cfg.get("ExpiryKind") or "ExpiryType.Weekly")
        if is_crypto:
            from datetime import datetime, timedelta, timezone
            ist = timezone(timedelta(hours=5, minutes=30))
            trade_date = datetime.now(ist).strftime("%Y-%m-%d")
            expiry = delta_client.resolve_delta_expiry(trade_date, expiry_kind, await asyncio.to_thread(crypto_expiries, watcher.underlying))
        else:
            expiries = instrument_master.expiries_for(watcher.underlying, watcher.option_type)
            expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
        if not expiry:
            return  # retry next cycle
        selection = await asyncio.to_thread(
            strike_resolver.resolve_strike, instrument_master, ltp_cache, watcher.underlying, expiry, watcher.option_type,
            str(watcher.leg_cfg.get("EntryType") or "EntryType.EntryByStrikeType"), watcher.leg_cfg.get("StrikeParameter"),
            breakout_price, watcher.is_sell,
        )
        if selection is None:
            return  # retry next cycle
        token, symbol, entry_price, option_type = selection.token, (selection.symbol or selection.token), selection.ltp, selection.option_type
        strike, leg_expiry = selection.strike, expiry
        if is_crypto:
            from shared.market.delta_instrument_cache import subscribe_tokens_now
            await asyncio.to_thread(subscribe_tokens_now, [selection.token])
    else:
        token, symbol, option_type = watcher.token, (watcher.symbol or watcher.token), watcher.option_type
        entry_price = ltp_cache.get_ltp(watcher.token) or breakout_price
        strike, leg_expiry = watcher.strike, watcher.expiry

    order_id = order_engine.broker.place_order(
        tradingsymbol=symbol, **order_venue(symbol),
        transaction_type="SELL" if watcher.is_sell else "BUY",
        quantity=watcher.qty, order_type="MARKET", fill_price=entry_price,
    )
    leg_id = f"{watcher.strategy_id}_{watcher.leg_ref}_rb{uuid.uuid4().hex[:6]}"
    leg = LegRuntime(
        leg_id=leg_id, strategy_id=watcher.strategy_id, user_id=watcher.user_id, token=token,
        is_sell=watcher.is_sell, option_type=option_type, qty=watcher.qty, entry_price=entry_price,
        entry_spot=ltp_cache.get_spot(watcher.underlying) or 0.0,
        strike=strike, expiry=leg_expiry,
        leg_cfg=watcher.leg_cfg, current_price=entry_price, broker_scope_id=watcher.broker_scope_id,
        reentry_max=_reentry_count(watcher.leg_cfg, "LegReentrySL") or _reentry_count(watcher.leg_cfg, "LegReentryTP"),
    )
    sl_tp_engine.initialize_sl_tp(leg)
    router.register_leg(leg)
    watcher.state = "Entered"
    router.unregister_range_watcher(watcher.range_id)
    checkpoint_strategy(router, watcher.strategy_id)
    log.info("[RangeBreakout] range=%s breakout leg entered leg=%s token=%s price=%s order_id=%s", watcher.range_id, leg_id, token, entry_price, order_id)

    if strategy.user_id:
        from api.live_broadcast import broadcast_strategy_snapshots
        await broadcast_strategy_snapshots(router, [watcher.strategy_id])
