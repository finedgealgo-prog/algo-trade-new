"""
range_breakout_engine.py
────────────────────────
Per-leg Range Breakout (ORB) / BTST Range Breakout — LegRangeBreakout,
ported from shared/features/range_breakout.py's parse_leg_range_breakout
(field parsing) + shared/features/leg_range_monitor.py's live state
machine (CollectingDay1 -> ... -> WaitingForBreakout -> Entered),
condensed into pure functions over RangeWatcherRuntime instead of a
once-a-second Mongo-polled doc — services/range_breakout_service.py is the
async half (advances the state machine, resolves/freezes a contract,
places the entry order) that calls these.

Mutually exclusive with LegMomentum — docs.algotest.in's own leg-builder
page + shared/features/range_breakout.py's own docstring: "Range Breakout
is mutually exclusive with Simple Momentum. When range breakout is
configured, LegMomentum is ignored." services/strategy_activation.py's
entry loop checks LegRangeBreakout BEFORE LegMomentum for exactly this
reason.

Same-day ORB: day1 == day2, range collected [start_hhmm, end_hhmm) on the
SAME calendar day, breakout scanned from end_hhmm onward that same day.

BTST ORB: range spans day1 (start_hhmm -> end of day) and day2 (market
open -> end_hhmm, exclusive), breakout scanned from end_hhmm onward on
day2. NOTE: day2 here is simply the next CALENDAR day (IST) — this module
has no trading-calendar/holiday awareness (none exists anywhere else in
algo-2_0 either), so a range window that would "really" skip a weekend/
holiday just waits quietly (WaitingForNextSession) for the next tick that
actually arrives, rather than computing the exact next trading session.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from runtime.range_watcher_runtime import RangeWatcherRuntime

_IST = timezone(timedelta(hours=5, minutes=30))

MARKET_OPEN_HHMM = "09:15"
MARKET_CLOSE_HHMM = "15:30"


def _hhmm(cfg: dict) -> str:
    return f"{int(cfg.get('Hour', 9)):02d}:{int(cfg.get('Minute', 15)):02d}"


def parse_leg_range_breakout(leg_cfg: dict) -> tuple[str, str, str, str]:
    """Returns (rb_type, condition, start_hhmm, end_hhmm). rb_type is
    "None" if LegRangeBreakout isn't configured (disabled-default shape,
    same as every other Leg* sub-config — see leg_builder.py)."""
    cfg = leg_cfg.get("LegRangeBreakout") or {}
    t = str(cfg.get("Type") or "None")
    if t == "None" or t.endswith(".None") or not t:
        return "None", "High", "09:15", "09:30"
    condition = "Low" if "Low" in str(cfg.get("Condition") or "High") else "High"
    start_hhmm = _hhmm(cfg.get("StartTime") or {})
    end_hhmm = _hhmm(cfg.get("EndTime") or {})
    if "BTSTUnderlying" in t:
        return "BTSTUnderlying", condition, start_hhmm, end_hhmm
    if "BTST" in t:
        return "BTSTInstrument", condition, start_hhmm, end_hhmm
    if "Underlying" in t:
        return "Underlying", condition, start_hhmm, end_hhmm
    return "Instrument", condition, start_hhmm, end_hhmm


def today_ist_date() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d")


def now_ist_hhmm() -> str:
    return datetime.now(_IST).strftime("%H:%M")


def is_watch_price_underlying(rb_type: str) -> bool:
    return "Underlying" in rb_type


def is_btst(rb_type: str) -> bool:
    return "BTST" in rb_type


def update_range(watcher: RangeWatcherRuntime, price: float | None) -> None:
    if not price or price <= 0:
        return
    watcher.range_high = price if watcher.range_high is None else max(watcher.range_high, price)
    watcher.range_low = price if watcher.range_low is None else min(watcher.range_low, price)


def check_breakout(watcher: RangeWatcherRuntime, price: float | None) -> bool:
    if watcher.range_high is None or watcher.range_low is None or not price or price <= 0:
        return False
    if watcher.condition == "High":
        return price > watcher.range_high
    return price < watcher.range_low
