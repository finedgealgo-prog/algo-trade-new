"""
End-to-end (engine -> real-order executor) for a "live" strategy: every kind
of risk exit reaches Delta correctly — leg SL, strategy runtime-risk Lock &
Trail, broker SL — with each exited leg's resting Delta stop cancelled first,
and untouched legs left alone.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import persistence.checkpoint as checkpoint_module
from orders.live_executor import ExecutionResult, LiveOrderExecutor
from orders.order_engine import OrderEngine
from risk import sl_tp_engine
from risk.broker_risk import build_broker_risk_config
from risk.runtime_risk import RuntimeRisk
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from services import broker_scope, strategy_finalize
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from shared.market.ltp_cache import TickUpdate
from token_router import TokenRouter

CE, PE = "C-BTC-60000-011026", "P-BTC-56000-011026"


class _Writer:
    def queue_strategy(self, doc): pass
    def queue_broker(self, doc): pass
    def queue_strategy_delete(self, strategy_id): pass


@pytest.fixture(autouse=True)
def _no_persistence(monkeypatch):
    w = _Writer()
    monkeypatch.setattr(checkpoint_module, "get_checkpoint_writer", lambda: w)
    monkeypatch.setattr(broker_scope, "get_checkpoint_writer", lambda: w)
    monkeypatch.setattr(strategy_finalize, "get_checkpoint_writer", lambda: w)


class _Delta:
    """Records every real order call; both legs hold a short position."""

    def __init__(self):
        self.calls, self._next = [], 100
        self.pos = {CE: -10, PE: -10}

    def _id(self):
        self._next += 1
        return str(self._next)

    def place_stop_order(self, *, symbol, side, size, stop_price, limit_price, client_order_id):
        oid = self._id()
        self.calls.append(("stop", symbol, side, size, round(stop_price, 2), oid))
        return {"id": oid, "size": size, "unfilled_size": size, "state": "pending", "product_id": 1, "client_order_id": client_order_id}

    def edit_stop_order(self, order_id, product_id, symbol, side, stop_price, limit_price=0.0):
        self.calls.append(("edit", symbol, round(stop_price, 2)))
        return {}

    def cancel_order(self, order_id, product_id):
        self.calls.append(("cancel", order_id))
        return {"id": order_id, "size": 10, "unfilled_size": 10, "state": "cancelled", "product_id": product_id}

    def get_order(self, order_id):
        return {"id": order_id, "size": 10, "unfilled_size": 10, "state": "pending", "product_id": 1}

    def open_orders(self):
        return []

    def positions(self):
        return [{"product_symbol": s, "size": q} for s, q in self.pos.items() if q]

    def place_order(self, *, symbol, side, size, order_type, limit_price, reduce_only, client_order_id):
        self.calls.append(("order", symbol, side, size, order_type, reduce_only))
        if reduce_only:
            self.pos[symbol] = self.pos.get(symbol, 0) + (size if side == "buy" else -size)
        return {"id": self._id(), "size": size, "unfilled_size": 0, "state": "closed", "average_fill_price": 1.0, "product_id": 1}


def _setup(leg_sl_pct="50", strategy_cfg=None):
    router = TokenRouter()
    delta = _Delta()
    settings = SimpleNamespace(live_order_enabled=True, live_order_type="market", live_limit_buffer_pct=2.0,
                               live_max_slippage_pct=10.0, live_fill_timeout_seconds=0)
    ex = LiveOrderExecutor(router, settings=settings, adapter_resolver=lambda _id: delta,
                           sleep=lambda _s: None, notifier=lambda *a: None)
    engine = OrderEngine(router, VirtualBrokerAdapter(), live=ex)
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", activation_mode="live",
                               broker_scope_id="b1", strategy_cfg={"Ticker": "BTCUSD", **(strategy_cfg or {})})
    router.register_strategy(strategy)
    legs = []
    for leg_id, token, opt in (("L1", CE, "CE"), ("L2", PE, "PE")):
        leg = LegRuntime(leg_id=leg_id, strategy_id="s1", user_id="u1", token=token, is_sell=True, option_type=opt,
                         qty=10, entry_price=500.0, entry_spot=58000.0, current_price=500.0, broker_scope_id="b1",
                         leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Percentage", "Value": leg_sl_pct}})
        sl_tp_engine.initialize_sl_tp(leg)
        router.register_leg(leg)
        # A real entry fill: resting Delta stop goes on.
        ex._leg_order_type[leg_id] = "market"
        ex._entry_futures[leg_id] = None  # marks "entered for real" for submit_exit
        ex._entry_filled[leg_id] = 10
        ex._on_entry_done(leg, ExecutionResult(requested=10, filled=10, avg_price=500.0))
        ex._stop_futures[leg_id].result(timeout=5)
        legs.append(leg)
    ex._entry_futures.clear()
    ex._entry_futures.update({"L1": _Done(), "L2": _Done()})
    return router, engine, ex, delta, legs


class _Done:
    def done(self):
        return True


def _drain(ex):
    ex._pool.shutdown(wait=True)


def _orders(delta):
    return [c for c in delta.calls if c[0] == "order"]


def test_leg_sl_exits_only_that_leg_and_cancels_only_its_stop():
    router, engine, ex, delta, (ce, pe) = _setup()
    ce_stop = next(c[5] for c in delta.calls if c[0] == "stop" and c[1] == CE)
    pe_stop = next(c[5] for c in delta.calls if c[0] == "stop" and c[1] == PE)
    router.on_tick(TickUpdate(changed_ltp={CE: 760.0}))  # CE SL 750 hit
    _drain(ex)
    assert ("cancel", ce_stop) in delta.calls
    assert ("cancel", pe_stop) not in delta.calls
    assert _orders(delta) == [("order", CE, "buy", 10, "market_order", True)]
    assert pe.status == "ACTIVE" and "L2" in ex._stops


def _runtime_risk_listener(router, engine):
    """Same as main.py's _on_runtime_risk_hit."""
    def on_event(event):
        if event.event_type.startswith("RUNTIME_RISK_"):
            scope, _, target_id = event.reason.partition(":")
            ids = [target_id] if scope == "strategy" else [s for s, r in router.strategies.items() if r.group_id == target_id]
            for sid in ids:
                strategy_finalize.square_off_strategy(router, engine, sid, reason=event.event_type)
    router.add_trigger_listener(on_event)


def test_runtime_lock_and_trail_trails_then_exits_both_legs_for_real():
    router, engine, ex, delta, (ce, pe) = _setup(leg_sl_pct="500")
    _runtime_risk_listener(router, engine)
    settings = {"LockAndTrail": {"InstrumentMove": 300, "StopLossMove": 200},
                "OverallTrailSL": {"InstrumentMove": 100, "StopLossMove": 50}}
    rr = RuntimeRisk(scope="strategy", target_id="s1", user_id="u1", name="s", settings=settings,
                     config=build_broker_risk_config(settings))
    router.runtime_risks["strategy:s1"] = rr

    # Profit = 2 legs x (500 - px) x 10 x 0.085.
    router.on_tick(TickUpdate(changed_ltp={CE: 300.0, PE: 300.0}))  # +340 -> lock at 200
    router.on_tick(TickUpdate(changed_ltp={CE: 300.0, PE: 300.0}))
    assert rr.trailing_activated and rr.floor == 200
    router.on_tick(TickUpdate(changed_ltp={CE: 200.0, PE: 200.0}))  # +510 -> floor 200 + 2*50
    router.on_tick(TickUpdate(changed_ltp={CE: 200.0, PE: 200.0}))
    assert rr.floor == 300
    assert _orders(delta) == []  # nothing exits while above the floor
    router.on_tick(TickUpdate(changed_ltp={CE: 330.0, PE: 330.0}))  # +289 < 300 -> LOCK_HIT
    _drain(ex)

    assert rr.status == "HIT" and rr.hit_reason == "LOCK_HIT"
    assert sorted(_orders(delta)) == [("order", CE, "buy", 10, "market_order", True),
                                      ("order", PE, "buy", 10, "market_order", True)]
    assert sum(1 for c in delta.calls if c[0] == "cancel") == 2  # both stops off Delta
    assert ex._stops == {}


def test_broker_sl_exits_both_legs_for_real():
    router, engine, ex, delta, (ce, pe) = _setup(leg_sl_pct="500")
    broker_scope.ensure_broker_runtime(router, "b1", "u1", None, activation_mode="live")
    broker_scope.apply_live_settings(router, "b1", "u1", {"StopLoss": 100}, activation_mode="live")
    router.on_tick(TickUpdate(changed_ltp={CE: 600.0, PE: 600.0}))  # -170 <= -100
    _drain(ex)
    assert sorted(_orders(delta)) == [("order", CE, "buy", 10, "market_order", True),
                                      ("order", PE, "buy", 10, "market_order", True)]
    assert sum(1 for c in delta.calls if c[0] == "cancel") == 2


def test_strategy_square_off_after_one_leg_sl_only_touches_the_open_leg():
    router, engine, ex, delta, (ce, pe) = _setup()
    router.on_tick(TickUpdate(changed_ltp={CE: 760.0}))  # CE SL
    strategy_finalize.square_off_strategy(router, engine, "s1", reason="MANUAL_SQUARE_OFF")
    _drain(ex)
    assert sorted(_orders(delta)) == [("order", CE, "buy", 10, "market_order", True),
                                      ("order", PE, "buy", 10, "market_order", True)]  # one each, no double CE


def test_strategy_config_overall_sl_exits_both_legs_for_real():
    router, engine, ex, delta, (ce, pe) = _setup(leg_sl_pct="500", strategy_cfg={"OverallSL": {"Type": "OverallSLType.MTM", "Value": 100}})
    router.on_tick(TickUpdate(changed_ltp={CE: 600.0, PE: 600.0}))  # -170 <= -100
    _drain(ex)
    assert sorted(_orders(delta)) == [("order", CE, "buy", 10, "market_order", True),
                                      ("order", PE, "buy", 10, "market_order", True)]
    assert sum(1 for c in delta.calls if c[0] == "cancel") == 2
