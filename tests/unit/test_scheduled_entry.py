"""
test_scheduled_entry.py
──────────────────────────
Phase 7 (EntryIndicators scheduled entry) unit tests: TimeIndicator parsing,
is_time_reached gating, and token_router's per-tick pending-entry check
firing the SCHEDULED_ENTRY_READY trigger once the target time passes.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_scheduled_entry.py -v
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conditions import schedule_engine
from runtime.pending_entry_runtime import PendingEntryRuntime
from runtime.strategy_runtime import StrategyRuntime
from token_router import TokenRouter
from shared.market.ltp_cache import TickUpdate

_IST = timezone(timedelta(hours=5, minutes=30))


def test_extract_time_indicator():
    cfg = {"Value": {"IndicatorName": "IndicatorType.TimeIndicator", "Parameters": {"Hour": 9, "Minute": 20}}}
    assert schedule_engine.extract_time_indicator(cfg) == (9, 20)


def test_extract_time_indicator_returns_none_for_other_indicators():
    cfg = {"Value": {"IndicatorName": "IndicatorType.RsiIndicator", "Parameters": {}}}
    assert schedule_engine.extract_time_indicator(cfg) is None


def test_no_time_indicator_means_always_reached():
    assert schedule_engine.is_time_reached({}) is True
    assert schedule_engine.is_time_reached(None) is True


def test_time_not_yet_reached():
    now = datetime(2026, 9, 4, 9, 0, tzinfo=_IST)
    cfg = {"Value": {"IndicatorName": "IndicatorType.TimeIndicator", "Parameters": {"Hour": 9, "Minute": 20}}}
    assert schedule_engine.is_time_reached(cfg, now) is False


def test_time_reached_after_target():
    now = datetime(2026, 9, 4, 9, 25, tzinfo=_IST)
    cfg = {"Value": {"IndicatorName": "IndicatorType.TimeIndicator", "Parameters": {"Hour": 9, "Minute": 20}}}
    assert schedule_engine.is_time_reached(cfg, now) is True


# ── token_router per-tick check ─────────────────────────────────────────────

def test_pending_entry_fires_scheduled_ready_once_time_passes():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1")
    router.register_strategy(strategy)

    past_time = datetime.now(_IST) - timedelta(seconds=1)  # already due
    pending = PendingEntryRuntime(
        pending_id="pend1", strategy_id="s1", broker_scope_id="", leg_cfg={}, target_time_ist=past_time,
    )
    router.register_pending_entry(pending)

    # Any tick (even unrelated token) triggers the per-tick pending check.
    router.on_tick(TickUpdate(changed_ltp={"999": 1.0}))

    assert pending.status == "TRIGGERED"
    events = [e for e in router.trigger_log if e.event_type == "SCHEDULED_ENTRY_READY"]
    assert len(events) == 1
    assert events[0].reason == "pend1"

    # A second tick must NOT re-fire it (status is no longer WAITING_TIME).
    router.on_tick(TickUpdate(changed_ltp={"999": 2.0}))
    events = [e for e in router.trigger_log if e.event_type == "SCHEDULED_ENTRY_READY"]
    assert len(events) == 1


def test_pending_entry_not_fired_before_target_time():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1")
    router.register_strategy(strategy)

    future_time = datetime.now(_IST) + timedelta(hours=1)
    pending = PendingEntryRuntime(
        pending_id="pend1", strategy_id="s1", broker_scope_id="", leg_cfg={}, target_time_ist=future_time,
    )
    router.register_pending_entry(pending)

    router.on_tick(TickUpdate(changed_ltp={"999": 1.0}))

    assert pending.status == "WAITING_TIME"
    assert router.trigger_log == []
