"""
test_reentry_recost.py
─────────────────────────
Phase 4 (Re-entry / Re-cost) unit tests, per master-doc §38-45 + §80's
checklist (re-entry limit, re-cost first/second trigger, re-cost max limit).

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_reentry_recost.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conditions import recost_engine, reentry_engine
from token_router import TokenRouter
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from shared.market.ltp_cache import TickUpdate


def make_leg(**overrides) -> LegRuntime:
    defaults = dict(
        leg_id="leg1", strategy_id="s1", user_id="u1", token="12345",
        is_sell=True, option_type="CE", qty=75, entry_price=100.0,
        recost_max=2, reentry_max=3,
    )
    defaults.update(overrides)
    return LegRuntime(**defaults)


# ── Re-cost: SELL waits for price to FALL back to cost (master-doc §40) ────

def test_recost_sell_arms_with_down_direction():
    leg = make_leg(is_sell=True, entry_price=100.0)
    recost = recost_engine.arm_recost_watcher("rc1", leg)
    assert recost is not None
    assert recost.direction == "DOWN"
    assert recost.reference_price == 100.0


def test_recost_sell_not_triggered_above_cost():
    leg = make_leg(is_sell=True, entry_price=100.0)
    recost = recost_engine.arm_recost_watcher("rc1", leg)
    assert recost_engine.check_recost_trigger(recost, 105.0) is False


def test_recost_sell_triggered_when_price_falls_to_cost():
    # master-doc §40 worked example: SL hit at 131, waits for LTP <= 100
    leg = make_leg(is_sell=True, entry_price=100.0)
    recost = recost_engine.arm_recost_watcher("rc1", leg)
    ticks = [131, 125, 115, 105, 101, 99.80]
    triggered_at = None
    for px in ticks:
        if recost_engine.check_recost_trigger(recost, px):
            triggered_at = px
            break
    assert triggered_at == 99.80


# ── Re-cost: BUY waits for price to RISE back to cost (opposite direction) ──

def test_recost_buy_arms_with_up_direction():
    leg = make_leg(is_sell=False, entry_price=100.0)
    recost = recost_engine.arm_recost_watcher("rc1", leg)
    assert recost.direction == "UP"


def test_recost_buy_triggered_when_price_rises_to_cost():
    leg = make_leg(is_sell=False, entry_price=100.0)
    recost = recost_engine.arm_recost_watcher("rc1", leg)
    assert recost_engine.check_recost_trigger(recost, 95.0) is False
    assert recost_engine.check_recost_trigger(recost, 100.0) is True


# ── Re-cost limit (master-doc §43) ──────────────────────────────────────────

def test_recost_first_and_second_trigger_then_max_reached():
    leg = make_leg(recost_max=2, recost_used=0)
    rc1 = recost_engine.arm_recost_watcher("rc1", leg)
    assert rc1 is not None
    leg.recost_used += 1  # first re-cost consumed

    rc2 = recost_engine.arm_recost_watcher("rc2", leg)
    assert rc2 is not None
    leg.recost_used += 1  # second re-cost consumed

    rc3 = recost_engine.arm_recost_watcher("rc3", leg)
    assert rc3 is None  # 2 >= max=2, no more re-cost


# ── Re-cost duplicate-trigger safety via TokenRouter ────────────────────────

def test_token_router_recost_fires_once():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1")
    router.register_strategy(strategy)
    leg = make_leg(is_sell=True, entry_price=100.0, token="12345")
    recost = recost_engine.arm_recost_watcher("rc1", leg)
    router.register_recost_watcher(recost)

    for price in (99.80, 99.70, 99.60):  # all satisfy LTP <= 100
        router.on_tick(TickUpdate(changed_ltp={"12345": price}))

    events = [e for e in router.trigger_log if e.event_type == "RECOST_TRIGGERED"]
    assert len(events) == 1
    assert events[0].trigger_ltp == 99.80  # fired on first crossing tick only


# ── Re-entry eligibility / budget (master-doc §38/§44) ──────────────────────

def test_reentry_eligible_when_under_max():
    leg = make_leg(reentry_max=2, reentry_used=0)
    assert reentry_engine.is_reentry_eligible(leg) is True


def test_reentry_immediate_consumes_budget_at_trigger_time():
    leg = make_leg(reentry_max=2, reentry_used=0)
    spec1 = reentry_engine.trigger_immediate_reentry(leg)
    assert spec1 is not None
    assert leg.reentry_used == 1

    spec2 = reentry_engine.trigger_immediate_reentry(leg)
    assert spec2 is not None
    assert leg.reentry_used == 2

    spec3 = reentry_engine.trigger_immediate_reentry(leg)
    assert spec3 is None  # exhausted
    assert leg.reentry_used == 2  # not incremented past max


def test_reentry_like_original_carries_momentum_config():
    leg = make_leg(reentry_max=1, reentry_used=0)
    spec = reentry_engine.trigger_like_original_reentry(leg, "LegMomentumType.PercentageDown", 8)
    assert spec is not None
    assert spec.kind == "LIKE_ORIGINAL"
    assert spec.momentum_type == "LegMomentumType.PercentageDown"
    assert spec.momentum_value == 8


def test_recost_and_reentry_counters_are_independent():
    # master-doc §44: separate counters — exhausting recost must not affect reentry budget.
    leg = make_leg(recost_max=1, recost_used=1, reentry_max=2, reentry_used=0)
    assert recost_engine.arm_recost_watcher("rc1", leg) is None  # recost exhausted
    assert reentry_engine.is_reentry_eligible(leg) is True  # reentry untouched
