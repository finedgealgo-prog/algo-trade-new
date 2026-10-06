"""
test_order_engine.py
───────────────────────
Phase 6 (Order Engine — virtual orders) unit tests: leg exit on SL/TP,
idempotency (no duplicate order for the same trigger), strategy-level and
broker-level batch exits, exit_side is opposite of entry position.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_order_engine.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orders.order_engine import OrderEngine
from risk.models import RiskConfig
from runtime.broker_runtime import BrokerRuntime
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from token_router import TokenRouter
from shared.market.ltp_cache import TickUpdate


def make_leg(**overrides) -> LegRuntime:
    defaults = dict(
        leg_id="leg1", strategy_id="s1", user_id="u1", token="111",
        is_sell=True, option_type="CE", qty=75, entry_price=100.0,
        leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30}},
        broker_scope_id="user100:dhan01",
    )
    defaults.update(overrides)
    return LegRuntime(**defaults)


def setup_router():
    router = TokenRouter()
    broker = BrokerRuntime(broker_scope_id="user100:dhan01", user_id="u1", config=RiskConfig())
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", broker_scope_id="user100:dhan01")
    router.register_broker(broker)
    router.register_strategy(strategy)
    return router, broker, strategy


# ── Leg SL/TP -> virtual exit order, instant fill ───────────────────────

def test_sl_hit_places_virtual_exit_order_and_fills():
    router, broker, strategy = setup_router()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    leg = make_leg()
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"111": 130.0}))  # crosses SL=130

    assert leg.status == "EXITED"
    assert len(engine.orders) == 1
    order = list(engine.orders.values())[0]
    assert order.status == "FILLED"
    assert order.fill_price == 130.0
    assert order.transaction_type == "BUY"  # closing a SELL position


def test_buy_leg_exit_side_is_sell():
    router, broker, strategy = setup_router()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    leg = make_leg(is_sell=False, entry_price=100.0, leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 30}})
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"111": 70.0}))  # BUY SL: LTP <= 70

    order = list(engine.orders.values())[0]
    assert order.transaction_type == "SELL"  # closing a BUY position


# ── Idempotency — same trigger never places a second order ─────────────

def test_duplicate_trigger_does_not_place_second_order():
    router, broker, strategy = setup_router()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    leg = make_leg()
    router.register_leg(leg)

    # Manually fire the same trigger twice (simulating a re-delivered event) —
    # token_router itself already prevents this via status flips, so this
    # directly tests order_engine's OWN idempotency layer.
    from token_router import TriggerEvent
    engine.on_trigger(TriggerEvent("SL_HIT", "s1", "leg1", 130.0, "SL"))
    engine.on_trigger(TriggerEvent("SL_HIT", "s1", "leg1", 131.0, "SL"))

    assert len(engine.orders) == 1


# ── Strategy-level batch exit ───────────────────────────────────────────

def test_strategy_exit_closes_all_its_legs():
    router, broker, strategy = setup_router()
    strategy.strategy_cfg = {"OverallSL": {"Type": "MTM", "Value": 100}}
    engine = OrderEngine(router, VirtualBrokerAdapter())
    leg1 = make_leg(leg_id="leg1", token="111", entry_price=100.0, qty=75, is_sell=True,
                     leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 1000}})
    leg2 = make_leg(leg_id="leg2", token="222", entry_price=50.0, qty=75, is_sell=True,
                     leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 1000}})
    router.register_leg(leg1)
    router.register_leg(leg2)

    # Both legs on the same strategy; tick moves leg1's token enough to blow the -100 overall SL.
    router.on_tick(TickUpdate(changed_ltp={"111": 102.0}))  # leg1 pnl = -150

    assert strategy.status == "EXIT_PENDING"
    assert leg1.status == "EXITED"
    assert leg2.status == "EXITED"  # claimed + exited even though ITS token never ticked
    assert len(engine.orders) == 2


# ── Broker-level batch exit ─────────────────────────────────────────────

def test_broker_exit_closes_legs_across_strategies():
    router, broker, strategy = setup_router()
    broker.config = RiskConfig(stop_loss_enabled=True, stop_loss_amount=100)
    strategy.strategy_cfg = {"OverallSL": {"Type": "MTM", "Value": 100000}}  # won't fire itself
    engine = OrderEngine(router, VirtualBrokerAdapter())
    leg = make_leg(entry_price=100.0, qty=75, is_sell=True,
                    leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 1000}})
    router.register_leg(leg)

    router.on_tick(TickUpdate(changed_ltp={"111": 102.0}))  # -150 <= broker's -100

    assert broker.status == "EXIT_PENDING"
    assert leg.status == "EXITED"
    assert len(engine.orders) == 1
