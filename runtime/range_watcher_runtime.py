"""
range_watcher_runtime.py
────────────────────────
In-memory runtime object for one leg's LegRangeBreakout watcher — same-day
ORB and BTST (cross-day) ORB, per conditions/range_breakout_engine.py.
Mirrors LazyRuntime/RecostRuntime's "freeze what's needed, watch, trigger"
shape (runtime/lazy_runtime.py, runtime/recost_runtime.py) but adds the
day/time-window state machine BTST range-building needs — ported from
shared/features/leg_range_monitor.py's algo_leg_range_cycles doc shape
(CollectingDay1 -> WaitingForNextSession -> CollectingDay2 -> RangeFrozen
-> WaitingForBreakout -> Entered/Cancelled), just held in RAM and advanced
by a periodic loop instead of a once-a-second Mongo-polled doc.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RangeWatcherRuntime:
    range_id: str
    strategy_id: str
    leg_ref: str  # the ORIGINAL ListOfLegConfigs leg's own "id" (config-time — no real leg_id/LegRuntime exists until breakout)

    underlying: str
    rb_type: str  # "Instrument" | "Underlying" | "BTSTInstrument" | "BTSTUnderlying"
    condition: str  # "High" | "Low"
    start_hhmm: str
    end_hhmm: str
    day1: str  # "YYYY-MM-DD" IST — range-collection start day
    day2: str  # == day1 for same-day (non-BTST) types

    # Frozen once resolved — Instrument/BTSTInstrument only (tracked
    # instrument is this leg's OWN option premium, chosen early so the
    # SAME contract's price is watched for the whole range window).
    # Underlying/BTSTUnderlying types track spot and never need a
    # contract until the breakout entry itself.
    token: str = ""
    symbol: str = ""
    strike: float = 0.0
    expiry: str = ""

    range_high: float | None = None
    range_low: float | None = None

    # Everything needed to actually place the entry order + build a
    # LegRuntime once breakout fires — same "freeze at arm time" reasoning
    # as LazyRuntime's own qty/is_sell/leg_cfg/broker_scope_id/user_id.
    leg_cfg: dict[str, Any] = field(default_factory=dict)
    option_type: str = ""
    is_sell: bool = False
    qty: int = 0
    broker_scope_id: str = ""
    user_id: str = ""

    # CollectingDay1 -> WaitingForNextSession -> CollectingDay2 ->
    # RangeFrozen -> WaitingForBreakout -> Entered | Cancelled
    state: str = "CollectingDay1"
