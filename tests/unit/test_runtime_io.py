"""Regression coverage for spot dispatch and event-loop isolation (no services)."""
import asyncio
import threading
from types import SimpleNamespace

from api.ws_client_hub import ClientHub
from orders.order_engine import OrderEngine
from persistence.checkpoint import CheckpointWriter
from risk.sl_tp_engine import initialize_sl_tp
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from shared.market.ltp_cache import TickUpdate
from shared.market.socket_sender import SocketSender
from token_router import TokenRouter, TriggerEvent


def spot_router(option_type="CE", config_key="LegStopLoss"):
    router = TokenRouter()
    router.register_strategy(StrategyRuntime(strategy_id="s", user_id="u", strategy_cfg={"Ticker": "NIFTY"}))
    leg = LegRuntime(leg_id="l", strategy_id="s", user_id="u", token="111",
                     is_sell=True, option_type=option_type, qty=1, entry_price=100,
                     current_price=100, entry_spot=25000,
                     leg_cfg={config_key: {"Type": "UnderlyingPoints", "Value": 100}})
    initialize_sl_tp(leg)
    router.register_leg(leg)
    engine = OrderEngine(router, VirtualBrokerAdapter())
    return router, leg, engine


def test_spot_only_stoploss_and_target_for_calls_and_puts():
    for option, key, spot in [("CE", "LegStopLoss", 25200), ("PE", "LegStopLoss", 24800),
                               ("CE", "LegTarget", 24800), ("PE", "LegTarget", 25200)]:
        router, leg, engine = spot_router(option, key)
        router.on_tick(TickUpdate(spot={"BANKNIFTY": 50000}))
        assert leg.status == "ACTIVE"
        router.on_tick(TickUpdate(spot={"NIFTY": spot}))
        assert leg.status == "EXITED"
        assert len(engine.orders) == 1
        assert leg.current_spot == spot
        assert leg.current_pnl == 0
        router.unregister_leg(leg.leg_id)
        assert router.underlying_to_legs == {}


def test_combined_spot_and_premium_uses_new_premium_once():
    router, leg, engine = spot_router()
    router.on_tick(TickUpdate(changed_ltp={"111": 105}, spot={"NIFTY": 25200}))
    assert len(engine.orders) == 1
    assert next(iter(engine.orders.values())).fill_price == 105
    assert leg.current_pnl == -5
    engine.on_trigger(TriggerEvent("TP_HIT", "s", "l"))
    assert len(engine.orders) == 1


class FakeSocket:
    def __init__(self, blocked=False):
        self.messages = []
        self.closed = False
        self.gate = asyncio.Event()
        if not blocked:
            self.gate.set()

    async def send_text(self, message):
        await self.gate.wait()
        self.messages.append(message)

    async def close(self, **kwargs):
        self.closed = True


def test_slow_browser_does_not_block_other_clients_or_producer():
    async def run():
        hub = ClientHub("test")
        slow, fast = FakeSocket(True), FakeSocket()
        hub.register("slow", slow, "u1")
        hub.register("fast", fast, "u2")
        try:
            await asyncio.wait_for(hub.broadcast({"tick": 1}), .2)
            for _ in range(10):
                await asyncio.sleep(0)
            assert fast.messages and not slow.messages
            await hub.send_to_user("u2", {"private": 1})
            for _ in range(10):
                await asyncio.sleep(0)
            assert len(fast.messages) == 2
            assert hub._senders["slow"].queue.empty()
        finally:
            slow.gate.set()
            await hub.close()
    asyncio.run(run())


def test_sender_overflow_is_bounded_and_disconnects_even_before_task_starts():
    async def run():
        ws = FakeSocket(True)
        removed = []
        sender = SocketSender(ws, removed.append, capacity=2, timeout=.01)
        for n in range(100):
            sender.send(str(n))
        assert sender.queue.qsize() <= 2
        await asyncio.wait_for(sender.task, .2)
        assert ws.closed and removed == [sender]
    asyncio.run(run())


def test_checkpoint_does_not_block_and_delete_follows_inflight_upsert():
    async def run():
        started, release = threading.Event(), threading.Event()
        operations = []
        class Collection:
            def update_one(self, *args, **kwargs):
                started.set()
                assert release.wait(2)
                operations.append("upsert")
            def delete_one(self, *args, **kwargs):
                operations.append("delete")
        writer = CheckpointWriter(SimpleNamespace(raw={"algo_trades": Collection()}))
        writer.queue_strategy({"_id": "s"})
        writer.start()
        try:
            # If Mongo is on the event loop this heartbeat cannot run while
            # update_one waits for release, and its bounded wait fails.
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(.001)
            assert started.is_set()
            for n in range(100):
                writer.queue_strategy({"_id": "s", "mtm": n})
            writer.queue_strategy_delete("s")
            assert len(writer._pending) == 1
        finally:
            release.set()
            await writer.stop()
        assert operations == ["upsert", "delete"]
    asyncio.run(run())


def test_checkpoint_retries_failed_delete():
    async def run():
        calls = []
        class Collection:
            def delete_one(self, *args, **kwargs):
                calls.append("delete")
                if len(calls) == 1:
                    raise OSError("temporary failure")
        writer = CheckpointWriter(SimpleNamespace(raw={"algo_trades": Collection()}))
        writer.queue_strategy_delete("s")
        writer.start()
        await writer.stop(timeout=3)
        assert calls == ["delete", "delete"]
        assert writer.last_error == ""
    asyncio.run(run())


def test_activation_wait_allows_market_cache_to_update(monkeypatch):
    from services import strategy_activation as activation
    from shared.market.ltp_cache import LtpCache
    async def run():
        class HttpClient:
            def __init__(self, **kwargs): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def post(self, *args, **kwargs):
                return SimpleNamespace(raise_for_status=lambda: None)
        monkeypatch.setattr(activation.httpx, "AsyncClient", HttpClient)
        monkeypatch.setattr(activation, "_SUBSCRIBE_WAIT_SECONDS", .2)
        monkeypatch.setattr(activation, "_SUBSCRIBE_POLL_INTERVAL_SECONDS", .005)
        master = SimpleNamespace(by_underlying_expiry_type=lambda *a: [SimpleNamespace(token="111", ws_segment="NSE_FNO")])
        cache = LtpCache()
        async def tick():
            await asyncio.sleep(.01)
            cache.apply(TickUpdate(changed_ltp={"111": 100}))
        task = asyncio.create_task(tick())
        await activation._ensure_live_prices(master, cache, {("expiry", "CE")}, "NIFTY")
        assert task.done() and cache.get_ltp("111") == 100
    asyncio.run(run())


def test_async_activation_rechecks_broker_after_wait(monkeypatch):
    from runtime.broker_runtime import BrokerRuntime
    from services import strategy_activation as activation
    from shared.market.ltp_cache import LtpCache
    async def run():
        router = TokenRouter()
        broker = BrokerRuntime(broker_scope_id="b", user_id="u")
        router.register_broker(broker)
        engine = OrderEngine(router, VirtualBrokerAdapter())
        monkeypatch.setattr(activation, "checkpoint_strategy", lambda *args: None)
        monkeypatch.setattr(activation, "get_checkpoint_writer", lambda: SimpleNamespace(queue_strategy_delete=lambda *args: None))
        monkeypatch.setattr(activation.daily_portfolio, "resolve_daily_portfolio", lambda *args: ("tp1", "tgp1"))
        monkeypatch.setattr(activation.schedule_engine, "is_time_reached", lambda cfg, **kwargs: True)
        monkeypatch.setattr(activation.expiry_resolver, "resolve_expiry", lambda *args: "2026-09-17")
        monkeypatch.setattr(activation.strike_resolver, "resolve_strike", lambda *args: SimpleNamespace(token="111", symbol="TEST", ltp=100, strike=25000))
        async def wait_and_lock(*args):
            strategy_id = next(iter(router.strategies))
            assert not router.finalize_if_no_open_legs(strategy_id)
            await asyncio.sleep(0)
            broker.status = "LOCKED_FOR_DAY"
        monkeypatch.setattr(activation, "_ensure_live_prices", wait_and_lock)
        master = SimpleNamespace(expiries_for=lambda *args: ["2026-09-17"], get=lambda token: SimpleNamespace(lot_size=1))
        doc = {"full_config": {"strategy": {"Ticker": "NIFTY", "ListOfLegConfigs": [{"PositionType": "SELL", "InstrumentKind": "CE"}]}}}
        # activate_strategy() now hands real leg entry off to a background
        # task (asyncio.create_task in _complete_leg_entry) so the HTTP
        # request path doesn't block on it — see that call site's own
        # comment for why. It no longer raises ActivationError itself for
        # a failure discovered mid-entry; that surfaces (logged, not
        # raised) inside the background task instead, once it runs to
        # completion.
        result = await activation.activate_strategy(router, engine, master, LtpCache(), None, doc, "u", "b")
        assert result["status"] == "activating"
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*pending)
        assert not router.legs
        assert not engine.orders
        assert not router.strategies
        assert not broker.strategy_ids
    asyncio.run(run())


def test_internal_hub_preserves_tick_order_without_spawning_per_tick_tasks():
    import importlib.util
    import json
    from pathlib import Path
    path = Path(__file__).resolve().parents[3] / "algo.websocket/hub/internal_hub.py"
    spec = importlib.util.spec_from_file_location("review_internal_hub", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    async def run():
        class Socket(FakeSocket):
            async def accept(self): pass
        hub = module.InternalHub()
        ws = Socket()
        await hub.register(ws, "consumer")
        tasks_before = len(asyncio.all_tasks())
        for price in (100, 120, 90):
            hub.publish_tick({"token": "111", "ltp": price, "oi": 0})
        assert len(asyncio.all_tasks()) == tasks_before
        for _ in range(30):
            await asyncio.sleep(0)
        frames = [json.loads(raw)["data"] for raw in ws.messages]
        assert [frame["changed_ltp_map"]["111"] for frame in frames] == [100, 120, 90]
        assert frames[0]["changed_oi_map"]["111"] == 0
        await hub.close()
    asyncio.run(run())


def test_update_route_registers_authenticated_owner_for_position_updates(monkeypatch):
    from api.routers import ws_live
    from fastapi import WebSocketDisconnect
    async def run():
        hub = ClientHub("update-test")
        received = asyncio.Event()
        class Socket(FakeSocket):
            async def receive_text(self):
                await received.wait()
                raise WebSocketDisconnect()
            async def send_text(self, message):
                self.messages.append(message)
                received.set()
        async def authenticate(ws): return "owner"
        monkeypatch.setattr(hub, "authenticate", authenticate)
        monkeypatch.setattr(ws_live, "update_hub", hub)
        ws = Socket()
        connection = asyncio.create_task(ws_live.ws_update(ws))
        try:
            await asyncio.sleep(0)
            await hub.send_to_user("different-user", {"private": "other"})
            await hub.send_to_user("owner", {"private": "position"})
            await asyncio.wait_for(connection, .5)
            assert ws.messages == ['{"private": "position"}']
        finally:
            connection.cancel()
            await asyncio.gather(connection, return_exceptions=True)
            await hub.close()
    asyncio.run(run())
