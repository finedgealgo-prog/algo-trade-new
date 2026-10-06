"""
test_risk_engine.py
──────────────────────
Phase 3 unit tests, per master-doc §80's checklist. Ported test intent (not
code — the old system has no equivalent unit suite for this logic) from the
formulas in shared/features/position_manager.py / trading_core.py, now
verified against algo-2_0's own port in risk/.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_risk_engine.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk import mtm_engine, overall_risk, sl_tp_engine, trailing_engine
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from token_router import TokenRouter
from shared.market.ltp_cache import TickUpdate


def make_leg(**overrides) -> LegRuntime:
    defaults = dict(
        leg_id="leg1", strategy_id="strat1", user_id="u1", token="111",
        is_sell=True, option_type="CE", qty=75, entry_price=100.0,
        leg_cfg={
            "LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30},
            "LegTarget": {"Type": "LegTgtSLType.Points", "Value": 40},
        },
    )
    defaults.update(overrides)
    return LegRuntime(**defaults)


def make_strategy(**overrides) -> StrategyRuntime:
    defaults = dict(strategy_id="strat1", user_id="u1", strategy_cfg={})
    defaults.update(overrides)
    return StrategyRuntime(**defaults)


# ── SL ──────────────────────────────────────────────────────────────────────

def test_sell_sl_not_hit_below_threshold():
    leg = make_leg(is_sell=True, entry_price=100.0)
    leg.current_price = 125.0  # SL = 100+30 = 130, not yet hit
    hit, sl_price = sl_tp_engine.check_leg_sl(leg)
    assert sl_price == 130.0
    assert hit is False


def test_sell_sl_hit_when_price_rises_to_threshold():
    leg = make_leg(is_sell=True, entry_price=100.0)
    leg.current_price = 130.0  # SELL: LTP >= SL -> hit
    hit, sl_price = sl_tp_engine.check_leg_sl(leg)
    assert sl_price == 130.0
    assert hit is True


def test_buy_sl_hit_when_price_falls_to_threshold():
    leg = make_leg(is_sell=False, entry_price=100.0)
    leg.current_price = 70.0  # BUY: SL = 100-30 = 70, LTP <= SL -> hit
    hit, sl_price = sl_tp_engine.check_leg_sl(leg)
    assert sl_price == 70.0
    assert hit is True


def test_buy_sl_not_hit_above_threshold():
    leg = make_leg(is_sell=False, entry_price=100.0)
    leg.current_price = 75.0
    hit, _ = sl_tp_engine.check_leg_sl(leg)
    assert hit is False


# ── TP ──────────────────────────────────────────────────────────────────────

def test_sell_tp_hit_when_price_falls_to_threshold():
    leg = make_leg(is_sell=True, entry_price=100.0)
    leg.current_price = 60.0  # SELL: TP = 100-40 = 60, LTP <= TP -> hit
    hit, tp_price = sl_tp_engine.check_leg_target(leg)
    assert tp_price == 60.0
    assert hit is True


def test_buy_tp_hit_when_price_rises_to_threshold():
    leg = make_leg(is_sell=False, entry_price=100.0)
    leg.current_price = 140.0  # BUY: TP = 100+40 = 140, LTP >= TP -> hit
    hit, tp_price = sl_tp_engine.check_leg_target(leg)
    assert tp_price == 140.0
    assert hit is True


def test_buy_tp_not_hit_below_threshold():
    leg = make_leg(is_sell=False, entry_price=100.0)
    leg.current_price = 130.0
    hit, _ = sl_tp_engine.check_leg_target(leg)
    assert hit is False


# ── Trailing SL ────────────────────────────────────────────────────────────

def test_trailing_sell_moves_sl_down_as_price_falls():
    leg = make_leg(
        is_sell=True, entry_price=100.0,
        leg_cfg={
            "LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30},
            "LegTrailSL": {"Type": "TrailStopLossType.Points", "Value": {"InstrumentMove": 10, "StopLossMove": 5}},
        },
    )
    leg.current_sl_price = 130.0  # initial SL
    leg.current_price = 85.0  # fell 15 from entry -> 1 step of 10 -> SL moves 5
    changed = trailing_engine.apply_trailing(leg)
    assert changed is True
    assert leg.current_sl_price == 125.0  # 130 - 5


def test_trailing_buy_moves_sl_up_as_price_rises():
    leg = make_leg(
        is_sell=False, entry_price=100.0,
        leg_cfg={
            "LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30},
            "LegTrailSL": {"Type": "TrailStopLossType.Points", "Value": {"InstrumentMove": 10, "StopLossMove": 5}},
        },
    )
    leg.current_sl_price = 70.0
    leg.current_price = 121.0  # rose 21 -> 2 steps of 10 -> SL moves 10
    changed = trailing_engine.apply_trailing(leg)
    assert changed is True
    assert leg.current_sl_price == 80.0  # 70 + 10


def test_trailing_no_move_when_price_unfavorable():
    leg = make_leg(
        is_sell=True, entry_price=100.0,
        leg_cfg={
            "LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30},
            "LegTrailSL": {"Type": "TrailStopLossType.Points", "Value": {"InstrumentMove": 10, "StopLossMove": 5}},
        },
    )
    leg.current_sl_price = 130.0
    leg.current_price = 105.0  # moved against a sell position (up), no trail
    changed = trailing_engine.apply_trailing(leg)
    assert changed is False
    assert leg.current_sl_price == 130.0


# ── initialize_sl_tp — real bug caught via manual UI testing: without this,
# LegRuntime.current_sl_price/current_tp_price stay 0.0 from construction
# until an actual SL/TP hit sets them (token_router only writes them on a
# hit). Two silent failures result: (1) any caller/UI reading these fields
# for display sees nothing until a hit, and (2) trailing_engine.apply_trailing
# requires current_sl_price to already be non-zero, so trailing NEVER
# activates for a leg whose SL was never explicitly initialized at entry.

def test_initialize_sl_tp_sets_both_prices_at_entry():
    leg = make_leg(is_sell=True, entry_price=100.0)  # LegStopLoss=30pts, LegTarget=40pts (see make_leg defaults)
    assert leg.current_sl_price == 0.0
    assert leg.current_tp_price == 0.0
    sl_tp_engine.initialize_sl_tp(leg)
    assert leg.current_sl_price == 130.0
    assert leg.current_tp_price == 60.0


def test_trailing_engages_only_after_sl_initialized():
    leg = make_leg(
        is_sell=True, entry_price=100.0,
        leg_cfg={
            "LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30},
            "LegTrailSL": {"Type": "TrailStopLossType.Points", "Value": {"InstrumentMove": 10, "StopLossMove": 5}},
        },
    )
    leg.current_price = 85.0  # would be a favorable 1-step move once SL exists
    # Before initialize_sl_tp: trailing is a silent no-op (current_sl_price==0).
    assert trailing_engine.apply_trailing(leg) is False

    sl_tp_engine.initialize_sl_tp(leg)
    assert leg.current_sl_price == 130.0
    # Now it actually engages.
    assert trailing_engine.apply_trailing(leg) is True
    assert leg.current_sl_price == 125.0


# ── Incremental MTM ─────────────────────────────────────────────────────────

def test_incremental_mtm_applies_only_the_delta():
    leg = make_leg(is_sell=True, entry_price=100.0, qty=75)
    strategy = make_strategy()
    leg.current_price = 90.0
    mtm_engine.apply_leg_pnl(leg, strategy)
    assert strategy.mtm == (100.0 - 90.0) * 75  # 750

    leg.current_price = 80.0
    mtm_engine.apply_leg_pnl(leg, strategy)
    # New pnl = (100-80)*75 = 1500, delta from 750 applied incrementally
    assert strategy.mtm == 1500.0
    assert strategy.peak_mtm == 1500.0


def test_incremental_mtm_two_legs_sum_independently():
    leg1 = make_leg(leg_id="leg1", is_sell=True, entry_price=100.0, qty=75)
    leg2 = make_leg(leg_id="leg2", is_sell=False, entry_price=50.0, qty=75, token="222")
    strategy = make_strategy()
    leg1.current_price = 90.0  # +750
    leg2.current_price = 55.0  # +375
    mtm_engine.apply_leg_pnl(leg1, strategy)
    mtm_engine.apply_leg_pnl(leg2, strategy)
    assert strategy.mtm == 750.0 + 375.0


# ── Overall SL / Target ─────────────────────────────────────────────────────

def test_overall_sl_hit():
    strategy = make_strategy(strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 1000}})
    strategy.mtm = -1000.0
    assert overall_risk.check_overall_sl_hit(strategy) is True


def test_overall_sl_not_hit():
    strategy = make_strategy(strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 1000}})
    strategy.mtm = -500.0
    assert overall_risk.check_overall_sl_hit(strategy) is False


def test_overall_target_hit():
    strategy = make_strategy(strategy_cfg={"OverallTgt": {"Type": "MTM", "Value": 2000}})
    strategy.mtm = 2500.0
    assert overall_risk.check_overall_target_hit(strategy) is True


def test_overall_sl_cycle_adjusted_for_reentries():
    strategy = make_strategy(strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 1000}})
    strategy.sl_reentry_done = 1  # cycle 1 -> effective SL = 1000*2 = 2000
    strategy.mtm = -1500.0
    assert overall_risk.check_overall_sl_hit(strategy) is False  # not yet at -2000
    strategy.mtm = -2000.0
    assert overall_risk.check_overall_sl_hit(strategy) is True


# ── Duplicate-trigger protection (§46/§68) via TokenRouter ─────────────────

def test_token_router_fires_sl_trigger_only_once_across_repeated_ticks():
    router = TokenRouter()
    strategy = make_strategy(strategy_cfg={})
    leg = make_leg(is_sell=True, entry_price=100.0, token="111")
    router.register_strategy(strategy)
    router.register_leg(leg)

    # Three ticks all crossing/holding above SL=130 — only the first should trigger.
    for price in (130.0, 131.0, 132.0):
        router.on_tick(TickUpdate(changed_ltp={"111": price}))

    sl_events = [e for e in router.trigger_log if e.event_type == "SL_HIT"]
    assert len(sl_events) == 1
    assert leg.status == "SL_HIT"


def test_token_router_ignores_ticks_for_unmapped_tokens():
    router = TokenRouter()
    strategy = make_strategy()
    leg = make_leg(token="111")
    router.register_strategy(strategy)
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"999": 500.0}))  # unrelated token
    assert leg.current_price == 0.0
    assert router.trigger_log == []


def test_token_router_overall_sl_sets_strategy_exit_pending():
    router = TokenRouter()
    strategy = make_strategy(strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 100}})
    leg = make_leg(is_sell=True, entry_price=100.0, qty=75, token="111",
                    leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 1000}})  # leg SL far away
    router.register_strategy(strategy)
    router.register_leg(leg)

    # price rises 2 pts against a 75-qty short -> pnl = -150, past the -100 overall SL
    router.on_tick(TickUpdate(changed_ltp={"111": 102.0}))

    assert strategy.status == "EXIT_PENDING"
    # Renamed by the unified strategy+broker risk engine to STRATEGY_<decision>
    # (parallels BROKER_<decision>) — was OVERALL_SL_HIT before that refactor.
    assert any(e.event_type == "STRATEGY_STOP_LOSS_HIT" for e in router.trigger_log)
