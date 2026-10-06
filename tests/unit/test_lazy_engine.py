"""
test_lazy_engine.py
──────────────────────
Phase 4 (Lazy Leg) unit tests, per master-doc §32-37 + §80's checklist
(Lazy UP, Lazy DOWN, exact crossing, price-gap crossing, frozen strike).

Formulas verified against the REAL live values from
shared/features/execution_socket.py's worked example doesn't exist in
comments, so these use the master-doc's own §33/§34 worked examples
(reference=100, 10% UP -> trigger=110; 10% DOWN -> trigger=90) as fixtures.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_lazy_engine.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conditions import lazy_engine
from token_router import TokenRouter
from runtime.strategy_runtime import StrategyRuntime
from shared.market.ltp_cache import TickUpdate


# ── master-doc §33: Lazy Simple Momentum UP ─────────────────────────────────

def test_momentum_up_percentage_target():
    # reference=100, 10% UP -> trigger=110 (master-doc §33 worked example)
    target = lazy_engine.resolve_momentum_target(100.0, "LegMomentumType.PercentageUp", 10)
    assert target == 110.0


def test_momentum_up_not_triggered_below_target():
    assert lazy_engine.is_momentum_triggered(109.0, 110.0, "LegMomentumType.PercentageUp") is False


def test_momentum_up_triggered_at_exact_crossing():
    assert lazy_engine.is_momentum_triggered(110.0, 110.0, "LegMomentumType.PercentageUp") is True


def test_momentum_up_triggered_with_price_gap():
    # ticks: 101,103,107,109,110.20 -> gap past 110 still triggers (crossing-safe, no exact-equality requirement)
    assert lazy_engine.is_momentum_triggered(110.20, 110.0, "LegMomentumType.PercentageUp") is True


# ── master-doc §34: Lazy Momentum DOWN ──────────────────────────────────────

def test_momentum_down_percentage_target():
    # reference=100, 10% DOWN -> trigger=90 (master-doc §34 worked example)
    target = lazy_engine.resolve_momentum_target(100.0, "LegMomentumType.PercentageDown", 10)
    assert target == 90.0


def test_momentum_down_triggered_at_or_below_target():
    assert lazy_engine.is_momentum_triggered(90.0, 90.0, "LegMomentumType.PercentageDown") is True
    assert lazy_engine.is_momentum_triggered(89.5, 90.0, "LegMomentumType.PercentageDown") is True
    assert lazy_engine.is_momentum_triggered(91.0, 90.0, "LegMomentumType.PercentageDown") is False


# ── Points mode ──────────────────────────────────────────────────────────

def test_momentum_points_mode_up():
    target = lazy_engine.resolve_momentum_target(100.0, "LegMomentumType.PointsUp", 15)
    assert target == 115.0


def test_momentum_points_mode_down():
    target = lazy_engine.resolve_momentum_target(100.0, "LegMomentumType.PointsDown", 15)
    assert target == 85.0


# ── master-doc §32: freeze on arm — strike/token/reference never re-derived ──

def test_arm_lazy_watcher_freezes_reference_and_trigger():
    lazy = lazy_engine.arm_lazy_watcher(
        lazy_id="lazy1", strategy_id="s1", parent_leg_id="leg1",
        token="56789", strike=25300, option_type="CE",
        momentum_type="LegMomentumType.PercentageUp", momentum_value=10, reference_ltp=100.0,
    )
    assert lazy is not None
    assert lazy.reference_ltp == 100.0
    assert lazy.trigger_price == 110.0
    assert lazy.token == "56789"
    assert lazy.strike == 25300
    assert lazy.status == "ARMED"


def test_arm_lazy_watcher_returns_none_for_zero_reference():
    lazy = lazy_engine.arm_lazy_watcher(
        lazy_id="lazy1", strategy_id="s1", parent_leg_id="leg1",
        token="56789", strike=25300, option_type="CE",
        momentum_type="LegMomentumType.PercentageUp", momentum_value=10, reference_ltp=0.0,
    )
    assert lazy is None


# ── TokenRouter integration: watcher dispatch + duplicate-trigger safety ────

def test_token_router_lazy_trigger_fires_once():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1")
    router.register_strategy(strategy)

    lazy = lazy_engine.arm_lazy_watcher(
        lazy_id="lazy1", strategy_id="s1", parent_leg_id="leg1",
        token="56789", strike=25300, option_type="CE",
        momentum_type="LegMomentumType.PercentageUp", momentum_value=10, reference_ltp=100.0,
    )
    router.register_lazy_watcher(lazy)

    # Repeated ticks all past the trigger (110) -> only first should fire.
    for price in (110.0, 110.2, 111.0):
        router.on_tick(TickUpdate(changed_ltp={"56789": price}))

    lazy_events = [e for e in router.trigger_log if e.event_type == "LAZY_TRIGGERED"]
    assert len(lazy_events) == 1
    assert lazy.status == "TRIGGERED"
    assert lazy_events[0].trigger_ltp == 110.0  # fired on the FIRST crossing tick, not a later one


def test_token_router_lazy_watcher_ignores_unrelated_token():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1")
    router.register_strategy(strategy)
    lazy = lazy_engine.arm_lazy_watcher(
        lazy_id="lazy1", strategy_id="s1", parent_leg_id="leg1",
        token="56789", strike=25300, option_type="CE",
        momentum_type="LegMomentumType.PercentageUp", momentum_value=10, reference_ltp=100.0,
    )
    router.register_lazy_watcher(lazy)

    router.on_tick(TickUpdate(changed_ltp={"999": 500.0}))
    assert lazy.status == "ARMED"
    assert router.trigger_log == []
