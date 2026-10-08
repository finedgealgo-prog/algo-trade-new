"""
"live" activation mode (AlgoTrade2.tsx) vs "fast-forward" (FastForward2.tsx):
only live-mode crypto legs reach the real Delta account, the mode survives a
checkpoint round-trip, and broker risk settings are read for the broker's
own mode.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from orders.live_executor import LiveOrderExecutor
from persistence.serializers import broker_to_doc, doc_to_broker_kwargs, doc_to_strategy_kwargs, strategy_to_doc
from runtime.broker_runtime import BrokerRuntime
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from services import broker_scope
from services.strategy_activation import ActivationError, activate_strategy

TOKEN = "C-BTC-82000-180926"


def _leg(strategy_id: str, token: str = TOKEN) -> LegRuntime:
    return LegRuntime(leg_id=f"{strategy_id}-L1", strategy_id=strategy_id, user_id="u", option_type="CE", broker_scope_id="b1", token=token, is_sell=True, qty=1, entry_price=100.0)


def _executor(enabled: bool, *strategies: StrategyRuntime) -> LiveOrderExecutor:
    router = SimpleNamespace(strategies={s.strategy_id: s for s in strategies}, brokers={})
    return LiveOrderExecutor(router, settings=SimpleNamespace(live_order_enabled=enabled), adapter_resolver=lambda _id: None)


def test_only_live_mode_crypto_legs_get_real_orders():
    paper = StrategyRuntime(strategy_id="ff", user_id="u")
    real = StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live")
    ex = _executor(True, paper, real)
    assert paper.activation_mode == "fast-forward"
    assert not ex.applies_to(_leg("ff"))
    assert ex.applies_to(_leg("lv"))
    assert not ex.applies_to(_leg("lv", token="12345"))  # NSE leg
    assert not _executor(False, real).applies_to(_leg("lv"))  # master switch off


def test_paper_leg_never_submitted_to_real_account():
    paper = StrategyRuntime(strategy_id="ff", user_id="u")
    ex = _executor(True, paper)
    ex.submit_entry(_leg("ff"))
    ex.submit_exit(_leg("ff"), "SL")
    assert ex._entry_futures == {}


def test_activation_mode_survives_checkpoint_round_trip():
    strategy = StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live", broker_scope_id="b1")
    doc = strategy_to_doc(strategy, [])
    doc["_id"] = doc.get("_id") or strategy.strategy_id
    assert doc["activation_mode"] == "live"
    assert doc_to_strategy_kwargs(doc)["activation_mode"] == "live"

    broker = BrokerRuntime(broker_scope_id="b1", user_id="u", activation_mode="live")
    assert BrokerRuntime(**doc_to_broker_kwargs(broker_to_doc(broker))).activation_mode == "live"
    # Checkpoint written before the field existed -> fast-forward.
    old = broker_to_doc(BrokerRuntime(broker_scope_id="b2", user_id="u"))
    old.pop("activation_mode")
    assert BrokerRuntime(**doc_to_broker_kwargs(old)).activation_mode == "fast-forward"


def test_broker_settings_read_for_the_brokers_mode():
    seen = []

    class _Col:
        def find_one(self, query):
            seen.append(query)
            return None

    mongo = SimpleNamespace(raw={"algo_borker_stoploss_settings": _Col()})
    broker_scope.load_broker_settings(mongo, "u", "b1", "live")
    broker_scope.load_broker_settings(mongo, "u", "b2")
    assert seen[0]["activation_mode"] == "live"
    assert seen[-1]["activation_mode"] == "fast-forward"


def _activate(order_engine, mode, router=None):
    doc = {"_id": "s1", "name": "s", "full_config": {"strategy": {"Ticker": "DELTA_BTCUSD", "ListOfLegConfigs": [{"id": "1"}]}}}
    router = router or SimpleNamespace(brokers={}, strategies={})
    return asyncio.run(activate_strategy(router, order_engine, None, None, None, doc, "u", "b1", activation_mode=mode))


def test_live_activation_refused_when_real_orders_are_off():
    with pytest.raises(ActivationError, match="LIVE_ORDER_ENABLED"):
        _activate(SimpleNamespace(live=None), "live")
    off = SimpleNamespace(live=SimpleNamespace(enabled=False))
    with pytest.raises(ActivationError, match="LIVE_ORDER_ENABLED"):
        _activate(off, "live")


def test_live_activation_refused_when_delta_credentials_fail():
    live = SimpleNamespace(enabled=True, check_ready=lambda _id: (False, "Delta credentials check failed: 401"))
    with pytest.raises(ActivationError, match="401"):
        _activate(SimpleNamespace(live=live), "live")


def test_unknown_mode_and_mixed_broker_refused():
    with pytest.raises(ActivationError, match="Unsupported"):
        _activate(SimpleNamespace(live=None), "algo-backtest")
    ok = SimpleNamespace(enabled=True, check_ready=lambda _id: (True, ""))
    router = SimpleNamespace(brokers={"b1": BrokerRuntime(broker_scope_id="b1", user_id="u")}, strategies={})
    with pytest.raises(ActivationError, match="already running fast-forward"):
        _activate(SimpleNamespace(live=ok), "live", router)


def test_exit_without_a_real_entry_is_never_sent():
    # A live-mode leg with no real entry on record (entered before entries
    # were wired, or the entry was rejected) must not send a reduce_only
    # exit — it would close another position held in the same contract.
    real = StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live")
    router = SimpleNamespace(strategies={"lv": real}, brokers={})

    def _no_adapter(_id):
        raise AssertionError("exit must not reach Delta")

    ex = LiveOrderExecutor(router, settings=SimpleNamespace(live_order_enabled=True), adapter_resolver=_no_adapter)
    ex._run_exit(_leg("lv"), "buy", 100.0, "SL", None)
    ex.shutdown()


def test_exit_size_comes_from_recorded_entry_fill():
    class _Col:
        def find_one(self, query, sort=None):
            return {"filled": 3} if query["leg_id"] == "lv-L1" else None

    ex = LiveOrderExecutor(SimpleNamespace(strategies={}, brokers={}), SimpleNamespace(raw={"algo2_live_orders": _Col()}),
                           settings=SimpleNamespace(live_order_enabled=True), adapter_resolver=lambda _id: None)
    assert ex._recorded_entry_fill("lv-L1") == 3
    assert ex._recorded_entry_fill("other") == 0
    ex.shutdown()


class _FakeDelta:
    """Order book that fills a limit only at or past `fill_at` (sell: price
    <= fill_at, buy: price >= fill_at); market orders fill at `market_px`."""

    def __init__(self, fill_at: float, market_px: float):
        self.fill_at, self.market_px, self.orders = fill_at, market_px, []

    def place_order(self, *, symbol, side, size, order_type, limit_price, reduce_only, client_order_id):
        filled = order_type == "market_order" or (limit_price <= self.fill_at if side == "sell" else limit_price >= self.fill_at)
        px = self.market_px if order_type == "market_order" else limit_price
        o = {"id": str(len(self.orders) + 1), "size": size, "unfilled_size": 0 if filled else size,
             "state": "closed" if filled else "open", "average_fill_price": px if filled else 0, "product_id": 1}
        self.orders.append((order_type, side, round(limit_price, 4), reduce_only))
        return o

    def get_order(self, order_id):
        return {"id": order_id, "size": 1, "unfilled_size": 1, "state": "open", "product_id": 1}

    def cancel_order(self, order_id, product_id):
        return {"id": order_id, "size": 1, "unfilled_size": 1, "state": "cancelled", "product_id": 1}


def _live_ex(alerts=None):
    settings = SimpleNamespace(live_order_enabled=True, live_order_type="limit", live_limit_buffer_pct=2.0, live_max_slippage_pct=10.0, live_fill_timeout_seconds=0)
    return LiveOrderExecutor(SimpleNamespace(strategies={}, brokers={}), settings=settings, adapter_resolver=lambda _id: None,
                             sleep=lambda _s: None, notifier=lambda *a: (alerts.append(a) if alerts is not None else None))


def test_entry_never_market_orders_into_a_thin_book():
    # Book only bids 0.2 for an option whose LTP is 0.52.
    delta, ex = _FakeDelta(fill_at=0.2, market_px=0.2), _live_ex()
    result = ex._execute(delta, TOKEN, "sell", 1, 0.52, reduce_only=False)
    assert result.filled == 0
    assert [o[0] for o in delta.orders] == ["limit_order", "limit_order"]
    assert delta.orders[1][2] == round(0.52 * 0.90, 4)  # capped at 10% slippage
    assert "dropped" in result.error
    ex.shutdown()


def test_entry_fills_inside_the_slippage_cap():
    delta, ex = _FakeDelta(fill_at=0.48, market_px=0.2), _live_ex()
    result = ex._execute(delta, TOKEN, "sell", 1, 0.52, reduce_only=False)
    assert result.filled == 1 and result.avg_price == round(0.52 * 0.90, 4)
    assert all(o[0] == "limit_order" for o in delta.orders)
    ex.shutdown()


def test_exit_falls_back_to_market_so_the_position_closes():
    delta, ex = _FakeDelta(fill_at=0.9, market_px=0.8), _live_ex()  # asks only at 0.9+: no limit up to +10% fills
    result = ex._execute(delta, TOKEN, "buy", 1, 0.52, reduce_only=True)
    assert result.filled == 1 and result.avg_price == 0.8
    assert [o[0] for o in delta.orders] == ["limit_order", "limit_order", "market_order"]
    assert all(o[3] for o in delta.orders)  # every exit order is reduce_only
    ex.shutdown()


def test_sl_target_trail_computed_from_the_real_fill_price():
    from orders.live_executor import ExecutionResult

    real = StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live")
    ex = _live_ex()
    ex.router.strategies["lv"] = real
    leg = _leg("lv")
    leg.leg_cfg = {"LegStopLoss": {"Type": "LegTgtSLType.Percentage", "Value": "150"}}
    leg.entry_price, leg.current_price, leg.status = 0.52, 0.52, "ACTIVE"
    leg.awaiting_live_fill = True
    ex._on_entry_done(leg, ExecutionResult(requested=1, filled=1, avg_price=0.2))
    assert leg.entry_price == 0.2 and not leg.awaiting_live_fill
    assert leg.current_sl_price == pytest.approx(0.2 * 2.5)  # from the real fill, never the LTP
    assert leg.best_price == 0.2  # trail anchor too
    ex.shutdown()

def test_alerts_on_rejected_entry_and_failed_exit():
    alerts = []
    real = StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live")
    ex = _live_ex(alerts)
    ex.router.strategies["lv"] = real
    delta = _FakeDelta(fill_at=0.2, market_px=0.2)
    ex._resolve = lambda _id: delta
    ex._run_entry(_leg("lv"), "sell", 1, 0.52, None)
    assert alerts and alerts[-1][1] == "LIVE_ENTRY_REJECTED" and alerts[-1][0] == "u"

    class _Down(_FakeDelta):
        def place_order(self, **kw):
            raise RuntimeError("Delta 503")

    ex._adapters.clear()
    ex._resolve = lambda _id: _Down(0, 0)
    ex._entry_filled["lv-L1"] = 1
    ex._run_exit(_leg("lv"), "buy", 0.52, "SL_HIT", None)
    assert alerts[-1][1] == "LIVE_EXIT_FAILED" and "OPEN on Delta" in alerts[-1][2]
    ex.shutdown()


def test_market_order_type_is_one_market_order():
    delta = _FakeDelta(fill_at=0.2, market_px=0.21)
    settings = SimpleNamespace(live_order_enabled=True, live_order_type="market", live_limit_buffer_pct=2.0,
                               live_max_slippage_pct=10.0, live_fill_timeout_seconds=0)
    ex = LiveOrderExecutor(SimpleNamespace(strategies={}, brokers={}), settings=settings, adapter_resolver=lambda _id: None,
                           sleep=lambda _s: None, notifier=lambda *a: None)
    for reduce_only, side in ((False, "sell"), (True, "buy")):
        delta.orders.clear()
        result = ex._execute(delta, TOKEN, side, 2, 0.52, reduce_only=reduce_only)
        assert result.filled == 2 and result.avg_price == 0.21
        assert [o[0] for o in delta.orders] == ["market_order"] and delta.orders[0][3] == reduce_only
    ex.shutdown()


def test_square_off_targets_stay_inside_user_and_page_mode():
    from api.routers.ws_live import squared_off_targets

    def _s(sid, user="u", mode="live", group=""):
        return StrategyRuntime(strategy_id=sid, user_id=user, activation_mode=mode, group_id=group)

    router = SimpleNamespace(strategies={x.strategy_id: x for x in [
        _s("a", group="g1"), _s("b", group="g1"), _s("c"), _s("ff", mode="fast-forward"), _s("other", user="v"),
    ]})
    assert squared_off_targets(router, "u", "live") == ["a", "b", "c"]          # Square Off All
    assert squared_off_targets(router, "u", "live", group_id="g1") == ["a", "b"]  # portfolio group
    assert squared_off_targets(router, "u", "live", group_id="c") == ["c"]        # direct strategy card
    assert squared_off_targets(router, "u", "live", strategy_id="b") == ["b"]
    assert squared_off_targets(router, "u", "fast-forward") == ["ff"]


def test_strategy_order_type_overrides_server_default():
    from services.strategy_activation import live_order_type_from_doc

    assert live_order_type_from_doc({"execution_config_base": {"OrderType": "Limit"}}) == "limit"
    assert live_order_type_from_doc({"execution_config_base": {"OrderType": "Market"}}) == "market"
    assert live_order_type_from_doc({}) == ""

    lim = StrategyRuntime(strategy_id="lim", user_id="u", activation_mode="live", live_order_type="limit")
    dflt = StrategyRuntime(strategy_id="dflt", user_id="u", activation_mode="live")
    ex = _executor(True, lim, dflt)
    ex.settings.live_order_type = "market"
    assert ex._order_type_for(_leg("lim")) == "limit"
    assert ex._order_type_for(_leg("dflt")) == "market"

    doc = strategy_to_doc(lim, [])
    doc["_id"] = doc.get("_id") or "lim"
    assert doc_to_strategy_kwargs(doc)["live_order_type"] == "limit"
    ex.shutdown()


# ── protective stop-loss resting on Delta ────────────────────────────────────

class _StopDelta:
    def __init__(self):
        self.calls, self.stop_state = [], {"state": "pending", "unfilled_size": 1, "average_fill_price": 0}

    def place_stop_order(self, **kw):
        self.calls.append(("place_stop", kw["side"], kw["size"], round(kw["stop_price"], 4), kw["limit_price"]))
        return {"id": "900", "size": kw["size"], "unfilled_size": kw["size"], "state": "pending", "product_id": 7}

    def edit_stop_order(self, order_id, product_id, symbol, side, stop_price, limit_price=0.0):
        self.calls.append(("edit_stop", order_id, round(stop_price, 4)))
        return {}

    def cancel_order(self, order_id, product_id):
        self.calls.append(("cancel", order_id))
        if self.stop_state["state"] == "closed":
            raise RuntimeError("order already filled")
        return {"id": order_id, "size": 1, "unfilled_size": 1, "state": "cancelled", "product_id": product_id}

    def get_order(self, order_id):
        return {"id": order_id, "size": 1, "product_id": 7, **self.stop_state}

    def open_orders(self):
        return list(getattr(self, "resting", []))

    def positions(self):
        return getattr(self, "pos", [{"product_symbol": TOKEN, "size": -1}])

    def place_order(self, **kw):
        self.calls.append(("order", kw["order_type"], kw["side"], kw["size"], kw["reduce_only"]))
        return {"id": "1", "size": kw["size"], "unfilled_size": 0, "state": "closed", "average_fill_price": 0.3, "product_id": 7}


def _stop_setup(sl_cfg=None):
    alerts, broker_hits = [], []
    real = StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live")
    ex = _live_ex(alerts)
    ex.settings.live_order_type = "market"
    ex.router.strategies["lv"] = real
    delta = _StopDelta()
    ex._resolve = lambda _id: delta
    ex.on_broker_stop_filled = lambda leg_id, px: broker_hits.append((leg_id, px))
    leg = _leg("lv")
    leg.leg_cfg = {"LegStopLoss": sl_cfg or {"Type": "LegTgtSLType.Percentage", "Value": "50"}}
    leg.entry_price, leg.current_price, leg.status = 0.2, 0.2, "ACTIVE"
    return ex, delta, leg, alerts, broker_hits


def _enter(ex, leg):
    from orders.live_executor import ExecutionResult
    ex._entry_filled[leg.leg_id] = 1
    ex._on_entry_done(leg, ExecutionResult(requested=1, filled=1, avg_price=0.2))
    ex._stop_futures[leg.leg_id].result(timeout=5)


def test_entry_fill_places_reduce_only_stop_on_delta():
    ex, delta, leg, _, _ = _stop_setup()
    _enter(ex, leg)
    # Short option: SL 50% above 0.2 -> buy stop at 0.3; stop-limit 10% past it
    # (Delta rejects stop-market on options).
    assert delta.calls == [("place_stop", "buy", 1, 0.3, pytest.approx(0.33))]
    assert ex._stops[leg.leg_id]["order_id"] == "900"
    ex.shutdown()


def test_trailing_sl_moves_the_delta_stop():
    ex, delta, leg, _, _ = _stop_setup()
    _enter(ex, leg)
    leg.current_sl_price = 0.25
    ex.sync_stop(leg)
    for _ in range(50):
        if ("edit_stop", "900", 0.25) in delta.calls:
            break
        time.sleep(0.02)
    assert ("edit_stop", "900", 0.25) in delta.calls
    assert ex._stops[leg.leg_id]["stop_price"] == 0.25
    ex.shutdown()


def test_engine_exit_cancels_the_stop_before_its_own_order():
    ex, delta, leg, _, _ = _stop_setup()
    _enter(ex, leg)
    ex._run_exit(leg, "buy", 0.15, "TP_HIT", None)
    kinds = [c[0] for c in delta.calls]
    assert kinds == ["place_stop", "cancel", "order"]
    assert delta.calls[-1] == ("order", "market_order", "buy", 1, True)
    assert leg.leg_id not in ex._stops
    ex.shutdown()


def test_exit_after_stop_already_filled_sends_no_second_order():
    ex, delta, leg, _, _ = _stop_setup()
    _enter(ex, leg)
    delta.stop_state = {"state": "closed", "unfilled_size": 0, "average_fill_price": 0.31}
    ex._run_exit(leg, "buy", 0.3, "SL_HIT", None)
    assert [c[0] for c in delta.calls] == ["place_stop", "cancel"]  # no market order
    ex.shutdown()


def test_poller_turns_a_delta_stop_fill_into_an_engine_exit_without_orders():
    ex, delta, leg, alerts, broker_hits = _stop_setup()
    _enter(ex, leg)
    delta.stop_state = {"state": "closed", "unfilled_size": 0, "average_fill_price": 0.31}
    ex.check_stops_once()
    assert broker_hits == [(leg.leg_id, 0.31)]
    assert alerts[-1][1] == "LIVE_BROKER_SL_HIT"
    ex._run_exit(leg, "buy", 0.31, "BROKER_SL_HIT", None)  # what the engine exit then does
    assert [c[0] for c in delta.calls] == ["place_stop"]
    ex.shutdown()


def test_stop_cancelled_on_delta_by_someone_else_alerts():
    ex, delta, leg, alerts, broker_hits = _stop_setup()
    _enter(ex, leg)
    delta.stop_state = {"state": "cancelled", "unfilled_size": 1, "average_fill_price": 0}
    ex.router.legs = {leg.leg_id: leg}
    ex.check_stops_once()  # no loop in tests -> re-place runs inline
    assert broker_hits == [] and alerts[-1][1] == "LIVE_STOP_CANCELLED"
    ex._stop_futures[leg.leg_id].result(timeout=5)
    assert [c[0] for c in delta.calls] == ["place_stop", "place_stop"]  # put back
    ex.shutdown()


def test_spot_based_sl_is_not_placed_on_delta():
    ex, delta, leg, alerts, _ = _stop_setup({"Type": "LegTgtSLType.UnderlyingPoints", "Value": "500"})
    leg.current_sl_price = 0.3
    ex._schedule_stop(leg)
    ex.shutdown()
    assert delta.calls == [] and alerts and alerts[-1][1] == "LIVE_STOP_ENGINE_ONLY"


def test_recovered_live_leg_without_stop_gets_one():
    ex, delta, leg, _, _ = _stop_setup()
    leg.current_sl_price = 0.3
    ex.router.legs = {leg.leg_id: leg}
    ex._entry_filled[leg.leg_id] = 1
    assert ex.ensure_recovered_stops() == 1
    ex._stop_futures[leg.leg_id].result(timeout=5)
    assert delta.calls[0][:4] == ("place_stop", "buy", 1, 0.3)
    assert ex.ensure_recovered_stops() == 0  # already has one
    ex.shutdown()


def test_existing_stop_for_the_leg_is_adopted_not_duplicated():
    ex, delta, leg, _, _ = _stop_setup()
    delta.resting = [{"id": "777", "client_order_id": ex._stop_client_id(leg.leg_id), "stop_price": "0.3",
                      "size": 1, "unfilled_size": 1, "state": "pending", "product_id": 7}]
    _enter(ex, leg)
    assert delta.calls == []  # nothing new placed
    assert ex._stops[leg.leg_id]["order_id"] == "777"
    ex.shutdown()


def test_exit_when_position_already_closed_outside_engine_sends_nothing():
    ex, delta, leg, alerts, _ = _stop_setup()
    _enter(ex, leg)
    delta.pos = []  # user closed it on the Delta app
    ex._run_exit(leg, "buy", 0.3, "MANUAL_SQUARE_OFF", None)
    assert [c[0] for c in delta.calls] == ["place_stop", "cancel"]  # stop cancelled, no order
    assert alerts[-1][1] == "LIVE_ALREADY_FLAT"
    ex.shutdown()


def test_position_closed_on_delta_by_hand_is_detected_after_two_polls():
    ex, delta, leg, alerts, broker_hits = _stop_setup()
    _enter(ex, leg)
    delta.pos = []  # user closed it in the Delta app
    ex.check_stops_once()
    assert broker_hits == [] and leg.leg_id in ex._stops  # one flat poll: wait
    ex.check_stops_once()
    assert broker_hits == [(leg.leg_id, 0.0)]
    assert ("cancel", "900") in delta.calls and leg.leg_id not in ex._stops
    assert alerts[-1][1] == "LIVE_CLOSED_OUTSIDE"
    ex._run_exit(leg, "buy", 0.3, "BROKER_SL_HIT", None)  # the engine exit sends nothing
    assert not any(c[0] == "order" for c in delta.calls)
    ex.shutdown()


def test_flat_blip_then_position_back_does_nothing():
    ex, delta, leg, alerts, broker_hits = _stop_setup()
    _enter(ex, leg)
    delta.pos = []
    ex.check_stops_once()
    delta.pos = [{"product_symbol": TOKEN, "size": -1}]
    ex.check_stops_once()
    delta.pos = []
    ex.check_stops_once()
    assert broker_hits == [] and leg.leg_id in ex._stops
    ex.shutdown()


def test_engine_sl_waits_for_the_real_entry_fill():
    from orders.order_engine import OrderEngine
    from risk import sl_tp_engine
    from shared.brokers.virtual.adapter import VirtualBrokerAdapter
    from shared.market.ltp_cache import TickUpdate
    from token_router import TokenRouter

    router = TokenRouter()
    OrderEngine(router, VirtualBrokerAdapter())
    router.register_strategy(StrategyRuntime(strategy_id="lv", user_id="u", activation_mode="live", strategy_cfg={"Ticker": "BTCUSD"}))
    leg = _leg("lv")
    leg.leg_cfg = {"LegStopLoss": {"Type": "LegTgtSLType.Percentage", "Value": "50"}}
    leg.entry_price = leg.current_price = 100.0
    sl_tp_engine.initialize_sl_tp(leg)  # provisional SL 150
    leg.awaiting_live_fill = True
    router.register_leg(leg)
    router.on_tick(TickUpdate(changed_ltp={TOKEN: 160.0}))  # past the provisional SL
    assert leg.status == "ACTIVE"  # not exited: real entry price not known yet
    assert leg.current_price == 160.0  # price/MTM still tracked
    leg.awaiting_live_fill = False
    router.on_tick(TickUpdate(changed_ltp={TOKEN: 161.0}))
    assert leg.status != "ACTIVE"  # evaluated normally once filled
