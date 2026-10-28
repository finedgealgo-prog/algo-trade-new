"""
entry_resume.py
───────────────
Resume for a live entry the exchange rejected for a reason other than
margin (orders/live_executor.py leaves such a leg at ENTRY_FAILED: no
position, no SL/TP evaluated). Resuming places the entry order again for
those legs only — at the current LTP, with SL/Target re-anchored on it and
again on the real fill. A margin rejection squares the strategy off instead
and is never resumable.
"""

from __future__ import annotations

from datetime import datetime, timezone

from risk import mtm_engine, sl_tp_engine
from runtime.leg_runtime import ENTRY_FAILED
from shared.logging.logger import get_logger
from shared.market.ltp_cache import get_ltp_cache

log = get_logger(__name__)

# A resumed entry is priced off the LTP; refuse one older than this.
_MAX_LTP_AGE_SECONDS = 15


class ResumeError(Exception):
    pass


def failed_legs(router, strategy_id: str) -> list:
    strategy = router.strategies.get(strategy_id)
    if strategy is None:
        return []
    return [router.legs[lid] for lid in sorted(strategy.leg_ids) if lid in router.legs and router.legs[lid].status == ENTRY_FAILED]


def resume_failed_entries(router, order_engine, strategy_id: str, cache=None) -> list[str]:
    """Re-sends the entry for every ENTRY_FAILED leg of the strategy; returns
    their leg ids. Raises ResumeError when nothing can be resumed or a leg
    has no fresh price (no leg is touched in that case)."""
    strategy = router.strategies.get(strategy_id)
    if strategy is None:
        raise ResumeError("Strategy is not running")
    if strategy.status != "ACTIVE":
        raise ResumeError("Strategy is already squared off")
    broker = router.brokers.get(strategy.broker_scope_id) if strategy.broker_scope_id else None
    if broker is not None and broker.status != "ACTIVE":
        raise ResumeError("Broker is exiting — cannot place new entries")
    legs = failed_legs(router, strategy_id)
    if not legs:
        raise ResumeError("No failed leg to resume")
    cache = cache or get_ltp_cache()
    prices = {}
    for leg in legs:
        price = cache.get_ltp(leg.token)
        if not price or cache.is_stale(leg.token, _MAX_LTP_AGE_SECONDS):
            raise ResumeError(f"No live price for {leg.token} right now — try again in a moment")
        prices[leg.leg_id] = price

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for leg in legs:
        leg.entry_price = leg.current_price = prices[leg.leg_id]
        leg.entry_ts = now
        leg.entry_error = ""
        leg.status = "ACTIVE"
        mtm_engine.apply_leg_pnl_delta(leg)  # pnl 0 at the new entry (was 0 already)
        sl_tp_engine.initialize_sl_tp(leg)
        order_engine.submit_live_entry(leg)
        log.info("[EntryResume] strategy=%s leg=%s %s re-entry at %s", strategy_id, leg.leg_id, leg.token, leg.entry_price)
    return [leg.leg_id for leg in legs]
