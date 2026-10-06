"""
test_unified_risk.py
───────────────────────
Unified Strategy+Broker Risk Engine unit tests, per that master-doc's §66
checklist: broker SL/target/lock/lock&trail, strategy hit doesn't affect
siblings, broker hit exits all its strategies, broker hit doesn't affect
another broker, broker>strategy priority, no duplicate leg exit, floor
monotonicity, delta propagation using the SAME delta.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_unified_risk.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk.evaluator import evaluate_risk
from risk.models import RiskConfig, RiskDecision, TrailingMode
from runtime.broker_runtime import BrokerRuntime
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from token_router import TokenRouter
from shared.market.ltp_cache import TickUpdate


def make_leg(**overrides) -> LegRuntime:
    defaults = dict(
        leg_id="leg1", strategy_id="s1", user_id="u1", token="111",
        is_sell=True, option_type="CE", qty=75, entry_price=100.0,
        leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 1000}},  # leg SL far away, won't fire
        broker_scope_id="user100:dhan01",
    )
    defaults.update(overrides)
    return LegRuntime(**defaults)


def make_strategy(**overrides) -> StrategyRuntime:
    defaults = dict(strategy_id="s1", user_id="u1", strategy_cfg={}, broker_scope_id="user100:dhan01")
    defaults.update(overrides)
    return StrategyRuntime(**defaults)


def make_broker(**overrides) -> BrokerRuntime:
    defaults = dict(broker_scope_id="user100:dhan01", user_id="u1")
    defaults.update(overrides)
    return BrokerRuntime(**defaults)


# ── §12/§13: plain SL/Target via the generic evaluator ──────────────────────

def test_evaluator_stop_loss_hit():
    config = RiskConfig(stop_loss_enabled=True, stop_loss_amount=500)
    result = evaluate_risk(-520, config)
    assert result.decision == RiskDecision.STOP_LOSS_HIT


def test_evaluator_target_hit():
    config = RiskConfig(target_enabled=True, target_amount=2190)
    result = evaluate_risk(2190, config)
    assert result.decision == RiskDecision.TARGET_HIT


# ── §14: LOCK mode — floor fixed once activated ─────────────────────────────

def test_evaluator_lock_not_activated_below_threshold():
    config = RiskConfig(trailing_mode=TrailingMode.LOCK, activation_profit=2000, lock_profit=1800)
    result = evaluate_risk(1500, config, peak_pnl=1500)
    assert result.decision == RiskDecision.NONE


def test_evaluator_lock_activates_and_holds_floor():
    config = RiskConfig(trailing_mode=TrailingMode.LOCK, activation_profit=2000, lock_profit=1800)
    result = evaluate_risk(2000, config, peak_pnl=2000)
    assert result.decision == RiskDecision.NONE
    assert result.new_floor == 1800


def test_evaluator_lock_hit_when_pnl_drops_to_floor():
    config = RiskConfig(trailing_mode=TrailingMode.LOCK, activation_profit=2000, lock_profit=1800)
    result = evaluate_risk(1800, config, peak_pnl=2200, current_floor=1800, trailing_activated=True)
    assert result.decision == RiskDecision.LOCK_HIT


# ── §15: LOCK_AND_TRAIL — master-doc's own worked example ───────────────────

def test_evaluator_lock_and_trail_worked_example():
    # Profit Reaches=2000, Lock=1800, step=200, trail_by=50
    config = RiskConfig(trailing_mode=TrailingMode.LOCK_AND_TRAIL, activation_profit=2000, lock_profit=1800, profit_step=200, trail_by=50)

    r1 = evaluate_risk(2000, config, peak_pnl=2000)
    assert r1.new_floor == 1800  # at 2000 -> floor 1800

    r2 = evaluate_risk(2200, config, peak_pnl=2200, current_floor=r1.new_floor, trailing_activated=True)
    assert r2.new_floor == 1850  # at 2200 -> floor 1850

    r3 = evaluate_risk(2400, config, peak_pnl=2400, current_floor=r2.new_floor, trailing_activated=True)
    assert r3.new_floor == 1900  # at 2400 -> floor 1900

    r4 = evaluate_risk(2600, config, peak_pnl=2600, current_floor=r3.new_floor, trailing_activated=True)
    assert r4.new_floor == 1950  # at 2600 -> floor 1950


# ── §16: floor must never move backward ──────────────────────────────────────

def test_evaluator_floor_never_moves_backward_when_pnl_drops():
    config = RiskConfig(trailing_mode=TrailingMode.LOCK_AND_TRAIL, activation_profit=2000, lock_profit=1800, profit_step=200, trail_by=50)
    # peak reached 2600 -> floor 1950; pnl now drops to 2300 (still peak=2600 remembered)
    result = evaluate_risk(2300, config, peak_pnl=2600, current_floor=1950, trailing_activated=True)
    assert result.new_floor == 1950  # NOT recalculated down to 1850
    assert result.decision == RiskDecision.NONE  # 2300 > 1950, no hit yet


# ── §17: TRAIL_STOP_LOSS — pure peak-distance mode ──────────────────────────

def test_evaluator_trail_stop_loss_worked_example():
    config = RiskConfig(trailing_mode=TrailingMode.TRAIL_STOP_LOSS, trailing_distance=500)
    result = evaluate_risk(3000, config, peak_pnl=3000)
    assert result.new_floor == 2500
    hit = evaluate_risk(2500, config, peak_pnl=3000, current_floor=2500, trailing_activated=True)
    assert hit.decision == RiskDecision.TRAILING_HIT


# ── §6: delta propagation uses the SAME delta for both scopes ──────────────

def test_token_router_propagates_same_delta_to_strategy_and_broker():
    router = TokenRouter()
    broker = make_broker()
    strategy = make_strategy()
    leg = make_leg(entry_price=100.0, qty=75, is_sell=True)
    router.register_broker(broker)
    router.register_strategy(strategy)
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"111": 90.0}))  # delta = (100-90)*75 = +750

    assert strategy.mtm == 750.0
    assert broker.mtm == 750.0  # SAME delta, not independently recomputed


# ── §22: broker hit does not affect another broker ──────────────────────────

def test_broker_hit_does_not_affect_sibling_broker():
    router = TokenRouter()
    dhan = make_broker(broker_scope_id="user100:dhan01", config=RiskConfig(stop_loss_enabled=True, stop_loss_amount=100))
    groww = make_broker(broker_scope_id="user100:groww01", config=RiskConfig(stop_loss_enabled=True, stop_loss_amount=100))
    s1 = make_strategy(strategy_id="s1", broker_scope_id="user100:dhan01")
    s2 = make_strategy(strategy_id="s2", broker_scope_id="user100:groww01", strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 100000}})
    leg1 = make_leg(leg_id="leg1", strategy_id="s1", token="111", broker_scope_id="user100:dhan01", entry_price=100.0, is_sell=True)
    leg2 = make_leg(leg_id="leg2", strategy_id="s2", token="222", broker_scope_id="user100:groww01", entry_price=100.0, is_sell=True)
    router.register_broker(dhan)
    router.register_broker(groww)
    router.register_strategy(s1)
    router.register_strategy(s2)
    router.register_leg(leg1)
    router.register_leg(leg2)

    # leg1 crosses dhan's SL (-150 <= -100); leg2 unaffected
    router.on_tick(TickUpdate(changed_ltp={"111": 102.0}))

    assert dhan.status == "EXIT_PENDING"
    assert groww.status == "ACTIVE"
    assert s2.status == "ACTIVE"


# ── §28/§63: broker > strategy priority, single leg claimed only once ──────

def test_broker_priority_claims_leg_before_strategy_and_no_double_claim():
    router = TokenRouter()
    broker = make_broker(config=RiskConfig(stop_loss_enabled=True, stop_loss_amount=100))
    strategy = make_strategy(strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 100}})  # also hits at same delta
    leg = make_leg(entry_price=100.0, qty=75, is_sell=True)
    router.register_broker(broker)
    router.register_strategy(strategy)
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"111": 102.0}))  # both broker and strategy SL would hit (-150)

    # §29 arbiter: "if broker_hit: trigger_broker_exit() elif strategy_hit: ..."
    # — broker exit is exclusive; strategy-level exit is never even evaluated
    # once its broker isn't ACTIVE. Strategy stays ACTIVE (its own status
    # transition is irrelevant now — entry_guard blocks it anyway via the
    # broker check), but its leg IS claimed, only once, via the broker path.
    assert broker.status == "EXIT_PENDING"
    assert strategy.status == "ACTIVE"
    assert leg.status == "EXIT_PENDING"  # claimed once (by broker)
    broker_events = [e for e in router.trigger_log if e.event_type.startswith("BROKER_")]
    strategy_events = [e for e in router.trigger_log if e.event_type.startswith("STRATEGY_")]
    assert len(broker_events) == 1  # exactly one broker trigger
    assert len(strategy_events) == 0  # strategy exit never triggered — exclusive priority


# ── §35: broker exit blocks strategy-level evaluation under it ─────────────

def test_broker_exit_pending_blocks_further_strategy_evaluation_same_tick():
    router = TokenRouter()
    broker = make_broker(status="EXIT_PENDING")  # already exited
    strategy = make_strategy(strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 100000}})
    router.register_broker(broker)
    router.register_strategy(strategy)

    router._evaluate_strategies({"s1"})
    assert strategy.status == "ACTIVE"  # skipped entirely — broker not active


# ── LOCK_AND_TRAIL wired end-to-end through StrategyRiskController ──────────

def test_strategy_lock_and_trail_triggers_exit():
    router = TokenRouter()
    broker = make_broker()
    strategy = make_strategy(strategy_cfg={
        "LockAndTrail": {"Type": "TrailingOption.LockAndTrail", "Value": {
            "ProfitReaches": 200, "LockProfit": 150, "IncreaseInProfitBy": 50, "TrailProfitBy": 10,
        }},
    })
    leg = make_leg(entry_price=100.0, qty=75, is_sell=False)  # BUY leg, profits as price rises
    router.register_broker(broker)
    router.register_strategy(strategy)
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"111": 103.0}))  # +225, past activation(200) -> floor=150+0*10=150... 225>150 no hit
    assert strategy.status == "ACTIVE"
    assert strategy.trailing_activated is True

    router.on_tick(TickUpdate(changed_ltp={"111": 102.0}))  # +150 == floor -> LOCK_HIT
    assert strategy.status == "EXIT_PENDING"
    lock_events = [e for e in router.trigger_log if e.event_type == "STRATEGY_LOCK_HIT"]
    assert len(lock_events) == 1



def test_peak_window_ignores_half_repriced_spike(monkeypatch):
    """A one-tick MTM spike (one leg repriced, the other not yet) must not
    activate the Lock; a level that holds >=3s must."""
    from risk import evaluator
    from risk.broker_risk import build_broker_risk_config, evaluate_broker_risk, risk_state
    from runtime.broker_runtime import BrokerRuntime

    now = [0.0]
    monkeypatch.setattr(evaluator, "clock", lambda: now[0])
    broker = BrokerRuntime(broker_scope_id="spike", user_id="u", config=build_broker_risk_config({
        "LockAndTrail": {"InstrumentMove": 100, "StopLossMove": 60},
        "OverallTrailSL": {"InstrumentMove": 5, "StopLossMove": 1},
    }))
    for t, mtm in ((0, -46), (1, -44), (2, 131), (3, -84)):
        now[0] = t
        assert evaluate_broker_risk(broker, mtm).decision == RiskDecision.NONE
    assert risk_state(broker)["lock_activated"] is False

    for t, mtm in ((10, 105), (11, 106), (12, 107), (13.5, 108)):
        now[0] = t
        evaluate_broker_risk(broker, mtm)
    assert risk_state(broker)["lock_activated"] is True
    assert risk_state(broker)["current_lock_floor"] == 61.0


def test_broker_settings_saved_in_profit_lock_immediately():
    """Lock&Trail 60->40, every 5 -> +2, saved while running MTM is +205:
    floor 40 + floor(145/5)*2 = 98 right on save — not after another +60."""
    from unittest import mock

    import services.broker_scope as broker_scope
    from risk.broker_risk import risk_state
    from runtime.broker_runtime import BrokerRuntime
    from runtime.strategy_runtime import StrategyRuntime

    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s", user_id="u", name="s", broker_scope_id="B")
    strategy.mtm = 205.0
    router.strategies["s"] = strategy
    router.register_broker(BrokerRuntime(broker_scope_id="B", user_id="u", mtm=-385.0))  # day total incl. -590 closed
    with mock.patch.object(broker_scope, "get_checkpoint_writer"), mock.patch.object(broker_scope, "schedule_publish_lock_state"):
        broker = broker_scope.apply_live_settings(router, "B", "u", {
            "status": 1, "StopLoss": 200,
            "LockAndTrail": {"InstrumentMove": 60, "StopLossMove": 40},
            "OverallTrailSL": {"InstrumentMove": 5, "StopLossMove": 2},
        })
    state = risk_state(broker)
    assert state["lock_activated"] is True
    assert state["current_lock_floor"] == 98.0
