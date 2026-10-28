"""Live entry rejections (AlgoTrade2): insufficient margin squares the whole
strategy off; any other error leaves the leg at ENTRY_FAILED with the error
shown, and Resume places only that leg's entry again."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from orders.live_executor import ExecutionResult, LiveOrderExecutor, is_margin_error
from orders.order_engine import OrderEngine
from persistence.serializers import _wire_to_leg_kwargs, doc_to_strategy_kwargs, strategy_to_doc
from runtime.leg_runtime import ENTRY_FAILED, LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from services import entry_resume, strategy_finalize
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from shared.market.ltp_cache import LtpCache, TickUpdate
from token_router import TokenRouter

CE, PE = "C-BTC-82000-291026", "P-BTC-78000-291026"


class _Live:
    """Stands in for LiveOrderExecutor on the OrderEngine: records orders."""
    def __init__(self):
        self.entries, self.exits = [], []

    def submit_entry(self, leg):
        self.entries.append(leg.leg_id)

    def submit_exit(self, leg, reason):
        self.exits.append((leg.leg_id, reason))


@pytest.fixture
def setup(monkeypatch):
    from persistence import checkpoint
    monkeypatch.setattr(strategy_finalize, "get_checkpoint_writer",
                        lambda: SimpleNamespace(queue_strategy=lambda d: None, queue_strategy_delete=lambda s: None))
    router = TokenRouter()
    live = _Live()
    engine = OrderEngine(router, VirtualBrokerAdapter(), live=live)
    strategy = StrategyRuntime(strategy_id="s1", user_id="u", activation_mode="live", strategy_cfg={"Ticker": "BTCUSD"}, broker_scope_id="b1")
    router.register_strategy(strategy)
    legs = []
    for i, tok in enumerate((CE, PE)):
        leg = LegRuntime(leg_id=f"L{i}", strategy_id="s1", user_id="u", token=tok, is_sell=True, option_type=tok[0],
                         qty=1, entry_price=100.0, current_price=100.0, broker_scope_id="b1")
        router.register_leg(leg)
        legs.append(leg)
    ex = LiveOrderExecutor(router, settings=SimpleNamespace(live_order_enabled=True), adapter_resolver=lambda _id: None,
                           sleep=lambda _s: None, notifier=lambda *a: None)
    changed = []
    ex.on_strategy_changed = lambda sid: (changed.append(sid), strategy_finalize.finalize_and_publish(router, sid))
    return SimpleNamespace(router=router, engine=engine, live=live, strategy=strategy, legs=legs, ex=ex, changed=changed)


def test_margin_error_is_recognised():
    assert is_margin_error("Delta POST /v2/orders: insufficient_margin {'available_balance': '1.2'}")
    assert is_margin_error("Insufficient Margin")
    assert not is_margin_error("Delta POST /v2/orders: invalid_contract")
    assert not is_margin_error("")


def test_margin_rejection_squares_off_the_whole_strategy(setup, monkeypatch):
    import main
    monkeypatch.setattr(main, "token_router", setup.router)
    monkeypatch.setattr(main, "order_engine", setup.engine)
    setup.ex.on_margin_rejected = main._on_live_margin_rejected
    ok, bad = setup.legs
    setup.ex._on_entry_done(ok, ExecutionResult(requested=1, filled=1, avg_price=100.0))
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="Delta POST /v2/orders: insufficient_margin"))
    assert ok.status == "EXITED" and bad.status == "EXITED"
    assert ("L0", "MARGIN_ERROR") in setup.live.exits     # the filled leg is closed on Delta
    assert all(lid != "L1" for lid, _ in setup.live.exits)  # nothing sent for the rejected one
    assert "s1" not in setup.router.strategies           # strategy squared off
    assert setup.strategy.entry_error["reason"] == "MARGIN"
    doc = strategy_to_doc(setup.strategy, setup.legs)
    assert doc["status"] == "StrategyStatus.SquaredOff" and doc["entry_error"]["reason"] == "MARGIN"
    assert doc["resume_available"] is False


def test_other_rejection_waits_for_resume_with_error_shown(setup):
    ok, bad = setup.legs
    setup.ex._on_entry_done(ok, ExecutionResult(requested=1, filled=1, avg_price=100.0))
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="Delta POST /v2/orders: invalid_contract"))
    assert ok.status == "ACTIVE" and bad.status == ENTRY_FAILED
    assert "s1" in setup.router.strategies  # not finalized: the failed leg waits
    assert setup.live.exits == []
    doc = strategy_to_doc(setup.strategy, setup.legs)
    assert doc["resume_available"] is True and doc["status"] == "StrategyStatus.Live_Running"
    assert doc["entry_error"]["reason"] == "ORDER_ERROR" and "invalid_contract" in doc["entry_error"]["message"]
    wire = {l["leg_id"]: l for l in doc["legs"]}
    assert wire["L1"]["status"] == 3 and wire["L1"]["entry_trade"] is None and "invalid_contract" in wire["L1"]["entry_error"]
    assert wire["L0"]["status"] == 1 and doc["open_legs_count"] == 1
    # no SL / MTM on the failed leg while ticks flow
    setup.router.on_tick(TickUpdate(changed_ltp={PE: 500.0}))
    assert bad.status == ENTRY_FAILED and bad.current_pnl == 0


def test_resume_places_only_the_failed_leg_again(setup):
    ok, bad = setup.legs
    bad.leg_cfg = {"LegStopLoss": {"Type": "LegTgtSLType.Percentage", "Value": "50"}}
    setup.ex._on_entry_done(ok, ExecutionResult(requested=1, filled=1, avg_price=100.0))
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="timeout"))
    cache = LtpCache()
    cache.apply(TickUpdate(changed_ltp={PE: 80.0}, now_ts=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))
    resumed = entry_resume.resume_failed_entries(setup.router, setup.engine, "s1", cache=cache)
    assert resumed == ["L1"] and setup.live.entries == ["L1"]  # only the failed leg
    assert bad.status == "ACTIVE" and bad.entry_price == 80.0 and bad.entry_error == ""
    assert bad.current_sl_price == pytest.approx(120.0)  # SL re-anchored on the new entry
    assert strategy_to_doc(setup.strategy, setup.legs)["resume_available"] is False
    with pytest.raises(entry_resume.ResumeError, match="No failed leg"):
        entry_resume.resume_failed_entries(setup.router, setup.engine, "s1", cache=cache)  # double click


def test_resume_refused_without_a_fresh_price(setup):
    _ok, bad = setup.legs
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="timeout"))
    cache = LtpCache()
    old = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cache.apply(TickUpdate(changed_ltp={PE: 80.0}, now_ts=old))
    with pytest.raises(entry_resume.ResumeError, match="No live price"):
        entry_resume.resume_failed_entries(setup.router, setup.engine, "s1", cache=cache)
    assert bad.status == ENTRY_FAILED and setup.live.entries == []


def test_square_off_with_a_failed_leg_sends_no_order_for_it(setup):
    ok, bad = setup.legs
    setup.ex._on_entry_done(ok, ExecutionResult(requested=1, filled=1, avg_price=100.0))
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="timeout"))
    strategy_finalize.square_off_strategy(setup.router, setup.engine, "s1", reason="MANUAL_SQUARE_OFF")
    assert setup.live.exits == [("L0", "MANUAL_SQUARE_OFF")]
    assert bad.status == "EXITED" and "s1" not in setup.router.strategies


def test_strategy_level_exit_closes_a_failed_leg_too(setup):
    ok, bad = setup.legs
    setup.ex._on_entry_done(ok, ExecutionResult(requested=1, filled=1, avg_price=100.0))
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="timeout"))
    setup.strategy.status = "EXIT_PENDING"
    setup.engine.manual_square_off("L0", reason="STRATEGY_STOP_LOSS_HIT")
    assert setup.router.finalize_if_no_open_legs("s1")
    assert bad.status == "EXITED"


def test_failed_leg_and_margin_reason_survive_a_restart(setup):
    _ok, bad = setup.legs
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="timeout"))
    setup.strategy.entry_error = {"reason": "MARGIN", "message": "m"}
    doc = strategy_to_doc(setup.strategy, setup.legs)
    leg_doc = next(l for l in doc["legs"] if l["leg_id"] == "L1")
    kwargs = _wire_to_leg_kwargs("s1", leg_doc)
    assert kwargs["status"] == ENTRY_FAILED and kwargs["entry_error"] == "timeout" and kwargs["entry_price"] == 100.0
    doc["_id"] = "s1"
    assert doc_to_strategy_kwargs(doc)["entry_error"] == {"reason": "MARGIN", "message": "m"}


def test_rejection_while_strategy_exiting_just_closes_the_leg(setup):
    _ok, bad = setup.legs
    setup.strategy.status = "EXIT_PENDING"
    setup.ex._on_entry_done(bad, ExecutionResult(requested=1, filled=0, error="timeout"))
    assert bad.status == "EXITED"


def test_closed_legs_survive_a_restart_in_the_record():
    """Restart recovery registers only open legs; the record must still list
    the legs that had closed before it, or the page loses their P&L."""
    from persistence import recovery
    from runtime.strategy_runtime import StrategyRuntime as SR
    from conditions.schedule_engine import trading_day

    today = trading_day(True)  # crypto session day
    strategy = SR(strategy_id="s9", user_id="u", strategy_cfg={"Ticker": "BTCUSD"}, activated_date=today)
    closed = LegRuntime(leg_id="old", strategy_id="s9", user_id="u", token=CE, is_sell=True, option_type="CE", qty=1, entry_price=100.0, current_price=150.0)
    closed.current_pnl, closed.status = -50.0, "EXITED"
    live = LegRuntime(leg_id="new", strategy_id="s9", user_id="u", token=PE, is_sell=True, option_type="PE", qty=1, entry_price=100.0, current_price=100.0)
    strategy.leg_ids = {"old", "new"}
    doc = strategy_to_doc(strategy, [closed, live])
    doc["_id"] = "s9"

    class _Col:
        def find(self, query, *a, **k):
            return [doc] if query.get("engine") else []

        def update_one(self, *a, **k):
            pass

    mongo = SimpleNamespace(raw={recovery.STRATEGY_COLLECTION: _Col(), recovery.BROKER_COLLECTION: _Col()})
    router = TokenRouter()
    recovery.recover(router, mongo)
    restored = router.strategies["s9"]
    assert "old" not in router.legs and "old" in restored.closed_legs
    rewritten = strategy_to_doc(restored, [router.legs["new"]])
    assert {l["leg_id"] for l in rewritten["legs"]} == {"old", "new"}
    assert next(l for l in rewritten["legs"] if l["leg_id"] == "old")["status"] == 2
