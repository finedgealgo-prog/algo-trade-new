"""
test_crypto_risk_fixes.py
────────────────────────────
Regression tests for the crypto-priority risk/entry fixes: ₹-scaled Delta
MTM, perpetual price as crypto spot, broker settings (UI shape, trail SL,
registration at activation), AtCost / LikeOriginal budgets, OverallTrailSL,
underlying trailing direction, strike-resolver spot guard, scheduled-entry
momentum gate and pending-work persistence.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_crypto_risk_fixes.py -v
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

import persistence.checkpoint as checkpoint_module
from orders.order_engine import OrderEngine, order_venue
from persistence import recovery
from persistence.serializers import pending_work_to_doc
from risk import overall_risk, sl_tp_engine
from risk.broker_risk import build_broker_risk_config
from risk.evaluator import evaluate_risk
from risk.models import RiskDecision, TrailingMode
from runtime.lazy_runtime import LazyRuntime
from runtime.leg_runtime import LegRuntime
from runtime.pending_entry_runtime import PendingEntryRuntime
from runtime.recost_runtime import RecostRuntime
from runtime.strategy_runtime import StrategyRuntime
from selection import strike_resolver
from selection.strike_resolver import StrikeSelection
from services import broker_scope, leg_followup, scheduled_entry
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from shared.market.ltp_cache import LtpCache, TickUpdate
from shared.market.tick_client import TickClient
from token_router import TokenRouter, TriggerEvent

_IST = timezone(timedelta(hours=5, minutes=30))
BTC_OPTION = "C-BTC-60000-011026"


class _FakeWriter:
    def __init__(self) -> None:
        self.strategies: list[dict] = []
        self.brokers: list[dict] = []

    def queue_strategy(self, doc):
        self.strategies.append(doc)

    def queue_broker(self, doc):
        self.brokers.append(doc)

    def queue_strategy_delete(self, strategy_id):
        pass


@pytest.fixture(autouse=True)
def fake_writer(monkeypatch):
    writer = _FakeWriter()
    monkeypatch.setattr(checkpoint_module, "get_checkpoint_writer", lambda: writer)
    monkeypatch.setattr(broker_scope, "get_checkpoint_writer", lambda: writer)
    return writer


def _crypto_setup(leg_cfg, strategy_cfg=None, is_sell=True, entry=500.0, spot=60000.0, option_type="CE"):
    router = TokenRouter()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", strategy_cfg={"Ticker": "BTCUSD", **(strategy_cfg or {})})
    router.register_strategy(strategy)
    leg = LegRuntime(
        leg_id="L1", strategy_id="s1", user_id="u1", token=BTC_OPTION, is_sell=is_sell, option_type=option_type,
        qty=1, entry_price=entry, entry_spot=spot, current_price=entry, leg_cfg=leg_cfg,
    )
    sl_tp_engine.initialize_sl_tp(leg)
    router.register_leg(leg)
    return router, engine, strategy, leg


# ── 1. crypto MTM in ₹ ──────────────────────────────────────────────────────

def test_crypto_mtm_is_contract_value_and_inr_scaled():
    router, _engine, strategy, leg = _crypto_setup({}, {"OverallSL": {"Type": "OverallSLType.MTM", "Value": 1000}})
    assert leg.pnl_multiplier == pytest.approx(0.001 * 85)
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 1600.0}))
    assert strategy.mtm == pytest.approx(-1100 * 0.085)
    assert strategy.status == "ACTIVE"  # ₹93.5 loss must not trip a ₹1000 OverallSL
    assert leg.status == "ACTIVE"


def test_nse_mtm_unchanged():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", strategy_cfg={"Ticker": "NIFTY"})
    router.register_strategy(strategy)
    leg = LegRuntime(leg_id="L1", strategy_id="s1", user_id="u1", token="123", is_sell=True, option_type="CE", qty=75, entry_price=100.0, current_price=100.0)
    router.register_leg(leg)
    router.on_tick(TickUpdate(changed_ltp={"123": 90.0}))
    assert strategy.mtm == 750.0


def test_order_venue_labels_crypto():
    assert order_venue(BTC_OPTION) == {"exchange": "DELTA", "product": "NRML"}
    assert order_venue("12345") == {"exchange": "NFO", "product": "MIS"}


# ── 2. crypto spot ──────────────────────────────────────────────────────────

def test_tick_client_mirrors_perpetual_into_spot():
    cache = LtpCache()
    client = TickClient("ws://unused", cache)
    seen = []
    client.add_listener(seen.append)
    raw = json.dumps({"type": "tick", "data": {"changed_ltp_map": {"BTCUSD": 61000.0}}})
    asyncio.run(client._on_raw(raw))
    assert cache.get_spot("BTCUSD") == 61000.0
    assert seen[0].spot == {"BTCUSD": 61000.0}


def test_crypto_underlying_sl_fires_on_spot():
    router, _engine, _strategy, leg = _crypto_setup({"LegStopLoss": {"Type": "LegTgtSLType.UnderlyingPoints", "Value": 500}})
    assert leg.current_sl_price == 60500.0
    router.on_tick(TickUpdate(changed_ltp={"BTCUSD": 61000.0}, spot={"BTCUSD": 61000.0}))
    assert leg.status == "EXITED"


# ── 3/4. broker settings ────────────────────────────────────────────────────

def test_broker_config_ui_lock_shape():
    cfg = build_broker_risk_config({"StopLoss": 5000, "LockAndTrail": {"InstrumentMove": 2000, "StopLossMove": 1000}})
    assert cfg.trailing_mode == TrailingMode.LOCK
    assert (cfg.activation_profit, cfg.lock_profit) == (2000, 1000)


def test_broker_config_ui_lock_and_trail_shape():
    cfg = build_broker_risk_config({
        "LockAndTrail": {"InstrumentMove": 2000, "StopLossMove": 1000},
        "OverallTrailSL": {"InstrumentMove": 500, "StopLossMove": 250},
    })
    assert cfg.trailing_mode == TrailingMode.LOCK_AND_TRAIL
    assert (cfg.profit_step, cfg.trail_by) == (500, 250)
    # peak 3000 -> 2 steps above 2000 -> floor 1000 + 2*250 = 1500
    result = evaluate_risk(1400, cfg, peak_pnl=3000)
    assert result.decision == RiskDecision.LOCK_HIT
    assert result.threshold == 1500


def test_broker_standalone_trail_sl_tightens_stop_loss():
    cfg = build_broker_risk_config({"StopLoss": 3700, "OverallTrailSL": {"InstrumentMove": 100, "StopLossMove": 50}})
    assert cfg.trailing_mode == TrailingMode.NONE
    # peak 200 -> SL 3700 - 2*50 = 3600
    assert evaluate_risk(-3650, cfg, peak_pnl=200).decision == RiskDecision.STOP_LOSS_HIT
    assert evaluate_risk(-3650, cfg, peak_pnl=0).decision == RiskDecision.NONE


def test_broker_config_strategy_shape_still_supported():
    cfg = build_broker_risk_config({"LockAndTrail": {"Type": "TrailingOption.Lock", "Value": {"ProfitReaches": 100, "LockProfit": 50}}})
    assert cfg.trailing_mode == TrailingMode.LOCK and cfg.activation_profit == 100


def test_ensure_broker_runtime_registers_saved_settings():
    router = TokenRouter()
    settings = {"StopLoss": 2000, "Target": 4000, "status": 1}
    broker = broker_scope.ensure_broker_runtime(router, "bconf1", "u1", settings)
    assert router.brokers["bconf1"] is broker
    assert broker.config.stop_loss_amount == 2000 and broker.config.target_amount == 4000
    broker.status = "LOCKED_FOR_DAY"
    assert broker_scope.ensure_broker_runtime(router, "bconf1", "u1", {"StopLoss": 1}) is broker
    assert broker.status == "LOCKED_FOR_DAY" and broker.config.stop_loss_amount == 2000


def test_ensure_broker_runtime_ignores_disabled_settings():
    broker = broker_scope.ensure_broker_runtime(TokenRouter(), "b2", "u1", {"StopLoss": 2000, "status": 0})
    assert broker.config.stop_loss_enabled is False


# ── 6/7. re-entry budgets ───────────────────────────────────────────────────

def test_atcost_arms_from_configured_count():
    cfg = {"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 50},
           "LegReentrySL": {"Type": "ReentryType.AtCost", "Value": {"ReentryCount": 2}}}
    router, engine, _strategy, leg = _crypto_setup(cfg)
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 560.0}))
    leg_followup.handle_leg_exit(router, engine, None, None, leg, "SL_HIT")
    assert len(router.recost_watchers) == 1
    assert (leg.recost_max, leg.recost_used) == (2, 1)


def test_like_original_budget_carries_across_generations(monkeypatch):
    cfg = {"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 50},
           "LegMomentum": {"Type": "MomentumType.PointsUp", "Value": 1},
           "LegReentrySL": {"Type": "ReentryType.LikeOriginal", "Value": {"ReentryCount": 1}}}
    router, engine, _strategy, leg = _crypto_setup(cfg)
    leg.reentry_max = 1
    market = {"price": 500.0}
    monkeypatch.setattr(leg_followup, "_resolve_fresh_strike", lambda *a, **k: (
        StrikeSelection(BTC_OPTION, 60000, BTC_OPTION, market["price"], "CE", "01-10-2026"), "01-10-2026"))

    def on_exit(event):
        exited = router.legs.get(event.leg_id)
        if event.event_type == "SL_HIT" and exited is not None and exited.status == "EXITED":
            leg_followup.handle_leg_exit(router, engine, None, None, exited, "SL_HIT")

    def on_lazy(event):
        if event.event_type == "LAZY_TRIGGERED":
            leg_followup.complete_lazy_entry(router, engine, event.reason.split(":", 1)[1], event.trigger_ltp)

    router.add_trigger_listener(on_exit)
    router.add_trigger_listener(on_lazy)
    re_entries = 0
    for _ in range(4):
        open_leg = next((l for l in router.legs.values() if l.status == "ACTIVE"), None)
        if open_leg is None:
            break
        market["price"] = open_leg.entry_price + 60  # SL hit
        router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: market["price"]}))
        market["price"] += 2  # momentum +1 crossed -> re-entry (if budget left)
        router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: market["price"]}))
        re_entries += any(l.status == "ACTIVE" for l in router.legs.values())
    assert re_entries == 1


def test_immediate_reentry_child_gets_entry_spot(monkeypatch):
    cfg = {"LegStopLoss": {"Type": "LegTgtSLType.UnderlyingPoints", "Value": 500},
           "LegReentrySL": {"Type": "ReentryType.Immediate", "Value": {"ReentryCount": 1}}}
    router, engine, _strategy, leg = _crypto_setup(cfg)
    leg.reentry_max = 1
    selection = StrikeSelection(BTC_OPTION, 60000, BTC_OPTION, 500.0, "CE", "01-10-2026")
    monkeypatch.setattr(leg_followup, "_resolve_fresh_strike", lambda *a, **k: (selection, "01-10-2026"))
    router.on_tick(TickUpdate(changed_ltp={"BTCUSD": 60600.0}, spot={"BTCUSD": 60600.0}))
    assert leg.status == "EXITED"
    leg_followup.handle_leg_exit(router, engine, None, None, leg, "SL_HIT")
    child = next(l for l in router.legs.values() if l.status == "ACTIVE")
    assert child.entry_spot == 60600.0
    assert child.current_sl_price == 61100.0


# ── 8. OverallTrailSL ───────────────────────────────────────────────────────

def test_overall_trail_sl_tightens_to_breakeven():
    strategy = StrategyRuntime(strategy_id="s", user_id="u", strategy_cfg={
        "OverallSL": {"Type": "MTM", "Value": 1000},
        "OverallTrailSL": {"Type": "TrailType.MTM", "Value": {"TrailForEvery": 500, "TrailBy": 500}},
    })
    strategy.peak_mtm = 1000  # 2 steps -> SL 0 (break-even)
    strategy.mtm = 10
    assert overall_risk.check_overall_sl_hit(strategy) is False
    strategy.mtm = 0
    assert overall_risk.check_overall_sl_hit(strategy) is True


def test_overall_trail_sl_read_from_full_config():
    strategy = StrategyRuntime(strategy_id="s", user_id="u", strategy_cfg={"OverallSL": {"Type": "MTM", "Value": 1000}},
                               full_strategy_cfg={"OverallTrailSL": {"Type": "TrailType.MTM", "Value": {"TrailForEvery": 500, "TrailBy": 250}}})
    strategy.peak_mtm = 500
    assert overall_risk.effective_overall_sl(strategy) == 750


# ── 14. underlying trailing direction ───────────────────────────────────────

def test_underlying_trailing_short_pe_tightens_toward_spot():
    cfg = {"LegStopLoss": {"Type": "LegTgtSLType.UnderlyingPoints", "Value": 500},
           "LegTrailSL": {"Type": "TrailStopLossType.Points", "Value": {"InstrumentMove": 100, "StopLossMove": 50}}}
    router, _engine, _strategy, leg = _crypto_setup(cfg, option_type="PE")
    assert leg.current_sl_price == 59500.0  # short PE: SL below spot
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 400.0, "BTCUSD": 60250.0}, spot={"BTCUSD": 60250.0}))
    assert leg.current_sl_price == 59600.0  # spot +250 = 2 steps -> SL up by 100


# ── 15. strike resolver guard ───────────────────────────────────────────────

def test_crypto_atm_without_spot_is_not_resolved(monkeypatch):
    rows = [{"strike": 50000.0, "token": "C-BTC-50000-011026", "symbol": "x", "ltp": 9000.0},
            {"strike": 60000.0, "token": "C-BTC-60000-011026", "symbol": "y", "ltp": 500.0}]
    monkeypatch.setattr(strike_resolver, "build_chain_rows", lambda *a, **k: rows)
    assert strike_resolver.resolve_strike(None, LtpCache(), "BTCUSD", "01-10-2026", "CE", "EntryType.EntryByStrikeType", "StrikeType.ATM", 0.0) is None
    chosen = strike_resolver.resolve_strike(None, LtpCache(), "BTCUSD", "01-10-2026", "CE", "EntryType.EntryByStrikeType", "StrikeType.ATM", 59900.0)
    assert chosen.strike == 60000.0


# ── 10. scheduled entry honours LegMomentum ─────────────────────────────────

def test_scheduled_entry_arms_momentum_instead_of_entering(monkeypatch):
    import shared.market.delta_instrument_cache as dic
    monkeypatch.setattr(dic, "crypto_expiries", lambda underlying: ["01-10-2026"])
    monkeypatch.setattr(dic, "subscribe_tokens_background", lambda tokens: None)
    selection = StrikeSelection(BTC_OPTION, 60000, BTC_OPTION, 500.0, "CE", "01-10-2026")
    monkeypatch.setattr(scheduled_entry, "_resolve_strike_cached", lambda *a, **k: selection)

    router = TokenRouter()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    router.register_strategy(StrategyRuntime(strategy_id="s1", user_id="u1", strategy_cfg={"Ticker": "BTCUSD"}))
    leg_cfg = {"id": "a", "InstrumentKind": "LegType.CE", "PositionType": "PositionType.Sell",
               "LegMomentum": {"Type": "MomentumType.PercentageUp", "Value": 10}}
    pending = PendingEntryRuntime(pending_id="p1", strategy_id="s1", broker_scope_id="", leg_cfg=leg_cfg,
                                  target_time_ist=datetime.now(_IST), status="TRIGGERED")
    router.register_pending_entry(pending)
    cache = LtpCache()
    cache.ltp_map["BTCUSD"] = 60000.0
    scheduled_entry.handle_scheduled_entry_ready(router, engine, None, cache, TriggerEvent("SCHEDULED_ENTRY_READY", "s1", reason="p1"))
    assert not router.legs
    assert len(router.lazy_watchers) == 1
    assert next(iter(router.lazy_watchers.values())).trigger_price == 550.0
    assert "s1" in router.strategies and not router.pending_entries


# ── 12. pending work survives restart ───────────────────────────────────────

def test_pending_work_roundtrip():
    target = datetime.now(_IST) + timedelta(hours=1)
    pending = PendingEntryRuntime(pending_id="p1", strategy_id="s1", broker_scope_id="b", leg_cfg={"x": 1}, target_time_ist=target)
    lazy = LazyRuntime(lazy_id="z1", strategy_id="s1", parent_leg_id="L0", token=BTC_OPTION, strike=60000, option_type="CE",
                       momentum_type="MomentumType.PointsUp", reference_ltp=500, trigger_price=510, reentry_used=1, reentry_max=2)
    recost = RecostRuntime(recost_id="r1", strategy_id="s1", parent_leg_id="L0", token=BTC_OPTION, reference_price=500,
                           direction="DOWN", used=1, max=2)
    doc = json.loads(json.dumps(pending_work_to_doc([pending], [lazy], [recost], [])))

    router = TokenRouter()
    router.register_strategy(StrategyRuntime(strategy_id="s1", user_id="u1"))
    restored = recovery._restore_pending_work(router, doc, doc["_runtime_recost_watchers"])
    assert restored == 3
    assert router.pending_entries["p1"].target_time_ist == target
    assert router.pending_entries["p1"].status == "WAITING_TIME"
    assert router.lazy_watchers["z1"].reentry_used == 1
    assert router.recost_watchers["r1"].reference_price == 500


# ── broker settings changed while a strategy is already running ────────────

def _running_strategy_without_broker():
    router = TokenRouter()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", strategy_cfg={"Ticker": "BTCUSD"}, broker_scope_id="bconf1")
    router.register_strategy(strategy)
    leg = LegRuntime(leg_id="L1", strategy_id="s1", user_id="u1", token=BTC_OPTION, is_sell=True, option_type="CE",
                     qty=10, entry_price=500.0, current_price=500.0, broker_scope_id="bconf1")
    sl_tp_engine.initialize_sl_tp(leg)
    router.register_leg(leg)
    return router, engine, strategy, leg


def test_broker_sl_saved_mid_run_applies_to_running_strategy():
    router, _engine, strategy, leg = _running_strategy_without_broker()
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 600.0}))  # -100 * 10 * 0.085 = -85
    assert "bconf1" not in router.brokers

    broker = broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 200, "status": 1})
    assert broker is router.brokers["bconf1"]
    assert broker.mtm == pytest.approx(strategy.mtm) and "L1" in broker.leg_ids

    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 700.0}))  # -170: not yet
    assert leg.status == "ACTIVE"
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 740.0}))  # -204: broker SL hit
    assert leg.status == "EXITED"
    assert broker.status == "EXIT_PENDING"


def test_broker_trailing_sl_saved_mid_run_uses_profit_already_made():
    router, _engine, _strategy, leg = _running_strategy_without_broker()
    broker = broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 100, "status": 1})
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 300.0}))  # +170 profit
    # trail SL set now: every ₹50 of peak profit tightens SL by ₹50 -> 100 - 3*50 = -50 (lock ₹50)
    broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 100, "OverallTrailSL": {"InstrumentMove": 50, "StopLossMove": 50}, "status": 1})
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 420.0}))  # +68: above +50 floor
    assert leg.status == "ACTIVE"
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 445.0}))  # +46.75: floor breached
    assert leg.status == "EXITED" and broker.status == "EXIT_PENDING"


def test_broker_lock_and_trail_saved_mid_run():
    router, _engine, _strategy, leg = _running_strategy_without_broker()
    broker_scope.apply_live_settings(router, "bconf1", "u1", {
        "LockAndTrail": {"InstrumentMove": 100, "StopLossMove": 50},
        "OverallTrailSL": {"InstrumentMove": 20, "StopLossMove": 10}, "status": 1,
    })
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 340.0}))  # +136 -> floor 50 + 1*10 = 60
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 420.0}))  # +68 -> still above 60
    assert leg.status == "ACTIVE"
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 440.0}))  # +51 -> below 60
    assert leg.status == "EXITED"


def test_broker_settings_change_does_not_unlock_a_locked_broker():
    router, _engine, _strategy, _leg = _running_strategy_without_broker()
    broker = broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 100, "status": 1})
    broker.status = "LOCKED_FOR_DAY"
    broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 500, "status": 1})
    assert broker.status == "LOCKED_FOR_DAY" and broker.config.stop_loss_amount == 500


# ── Delta expiry: Today / Tomorrow relative to the 17:30 IST expiry time ────

_DELTA_EXPIRIES = ["30-09-2026", "01-10-2026", "02-10-2026", "09-10-2026"]


@pytest.mark.parametrize("now_hm, kind, expected", [
    ((15, 40), "ExpiryType.Today", "30-09-2026"),     # today's contract still live
    ((15, 40), "ExpiryType.Tomorrow", "01-10-2026"),
    ((17, 30), "ExpiryType.Today", "01-10-2026"),     # today's expired at 17:30
    ((18, 0), "ExpiryType.Tomorrow", "02-10-2026"),
    ((0, 5), "ExpiryType.Tomorrow", "01-10-2026"),
])
def test_delta_today_tomorrow_follow_expiry_time(now_hm, kind, expected):
    from shared.brokers.delta import client as delta_client
    now = datetime(2026, 9, 30, *now_hm, tzinfo=_IST)
    assert delta_client.resolve_delta_expiry("2026-09-30", kind, _DELTA_EXPIRIES, now=now) == expected


# ── Edit Setup "Quantity Multiplier" × leg lots ─────────────────────────────

def test_leg_qty_applies_setup_multiplier():
    from services.strategy_activation import _leg_qty, qty_multiplier_from_doc
    assert qty_multiplier_from_doc({"execution_config_base": {"Multiplier": 21}}) == 21
    assert qty_multiplier_from_doc({}) == 1
    assert _leg_qty({"LotConfig": {"Value": 2}}, lot_size=1, multiplier=21) == 42   # crypto: 2 lots × 21 = 42 contracts
    assert _leg_qty({"LotConfig": {"Value": 1}}, lot_size=75, multiplier=3) == 225  # NSE: × lot size too


def test_activation_screen_multiplier_carried_from_prepare_to_activate():
    from api.routers import portfolio
    portfolio._remember_activation_multiplier("u1", "stratA", 100)   # row 10 × portfolio 10
    assert portfolio._take_activation_multiplier("u1", "stratA") == 100
    assert portfolio._take_activation_multiplier("u1", "stratA") is None  # consumed once
    portfolio._remember_activation_multiplier("u1", "stratB", 0)
    assert portfolio._take_activation_multiplier("u1", "stratB") is None


def test_broker_sl_hit_exits_then_broker_is_active_again():
    router, _engine, _strategy, leg = _running_strategy_without_broker()
    broker = broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 50, "status": 1})
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 600.0}))  # -85 -> broker SL
    assert leg.status == "EXITED" and broker.status == "EXIT_PENDING"
    broker_scope.reset_after_risk_exit(broker)  # what main.py's BROKER_ listener does next
    assert broker.status == "ACTIVE"
    assert broker.mtm == pytest.approx(-85.0)  # day total kept (matches the page's Total MTM)
    assert broker.config.stop_loss_enabled is False  # settings off until re-saved


# ── broker exit squares off every strategy and keeps it visible ─────────────

def test_broker_exit_squares_off_all_strategies_and_saves_squared_off(monkeypatch, fake_writer):
    from services import strategy_finalize
    fake_writer.deleted = []
    fake_writer.queue_strategy_delete = lambda sid: fake_writer.deleted.append(sid)
    monkeypatch.setattr(strategy_finalize, "get_checkpoint_writer", lambda: fake_writer)

    router, engine, strategy, leg = _running_strategy_without_broker()
    # a second strategy under the same broker that is still waiting to enter
    waiting = StrategyRuntime(strategy_id="s2", user_id="u1", strategy_cfg={"Ticker": "BTCUSD"}, broker_scope_id="bconf1")
    router.register_strategy(waiting)
    router.register_pending_entry(PendingEntryRuntime(pending_id="p2", strategy_id="s2", broker_scope_id="bconf1",
                                                      leg_cfg={}, target_time_ist=datetime.now(_IST) + timedelta(hours=1)))
    broker = broker_scope.apply_live_settings(router, "bconf1", "u1", {"StopLoss": 50, "status": 1})
    router.on_tick(TickUpdate(changed_ltp={BTC_OPTION: 600.0}))  # broker SL hit
    assert leg.status == "EXITED"

    for sid in {"s1", "s2"}:  # what main.py's BROKER_ listener does
        strategy_finalize.square_off_strategy(router, engine, sid, reason="BROKER_STOP_LOSS_HIT")

    assert "s1" not in router.strategies and "s2" not in router.strategies
    assert not router.pending_entries
    saved = {d["_id"]: d for d in fake_writer.strategies}
    assert saved["s1"]["status"] == "StrategyStatus.SquaredOff"
    assert saved["s1"]["active_on_server"] is False
    assert saved["s1"]["legs"][0]["status"] == 2
    assert saved["s2"]["status"] == "StrategyStatus.SquaredOff"  # was waiting to enter -> squared off too
    assert fake_writer.deleted == []


def test_broker_lock_and_trail_floor_matches_user_example():
    """ProfitReaches 100, LockProfit 70, every +10 profit trail +1 (FastForward2
    sends LockAndTrail {100, 70} + OverallTrailSL {10, 1}); SL 40."""
    from risk.broker_risk import evaluate_broker_risk, risk_state
    from runtime.broker_runtime import BrokerRuntime
    broker = BrokerRuntime(broker_scope_id="b", user_id="u", config=build_broker_risk_config({
        "StopLoss": 40, "LockAndTrail": {"InstrumentMove": 100, "StopLossMove": 70},
        "OverallTrailSL": {"InstrumentMove": 10, "StopLossMove": 1},
    }))
    seen = []
    for mtm in (50, 99, 100, 110, 115, 100, 90):
        broker.mtm = mtm
        decision = evaluate_broker_risk(broker).decision
        state = risk_state(broker)
        seen.append((mtm, state["lock_activated"], state["current_lock_floor"], decision.value))
    assert seen == [
        (50, False, 0.0, "NONE"), (99, False, 0.0, "NONE"),
        (100, True, 70.0, "NONE"), (110, True, 71.0, "NONE"), (115, True, 71.0, "NONE"),
        (100, True, 71.0, "NONE"), (90, True, 71.0, "NONE"),   # floor never comes back down
    ]
    broker.mtm = 71
    assert evaluate_broker_risk(broker).decision == RiskDecision.LOCK_HIT



def test_broker_lock_uses_day_total_and_target_is_display_only_with_lock():
    cfg = build_broker_risk_config({
        "StopLoss": 40, "Target": 90,
        "LockAndTrail": {"InstrumentMove": 100, "StopLossMove": 70},
        "OverallTrailSL": {"InstrumentMove": 10, "StopLossMove": 1},
    })
    assert cfg.target_enabled is False and cfg.target_amount == 90
    # day total 115 (65.47 already squared off + 50 running): lock active, floor 71, no exit
    result = evaluate_risk(115.0, cfg, peak_pnl=115.0)
    assert result.decision == RiskDecision.NONE
    assert result.trailing_activated and result.new_floor == 71


def test_startup_rebuilds_broker_config_from_saved_settings(monkeypatch):
    from risk.models import RiskConfig as RC
    from runtime.broker_runtime import BrokerRuntime
    router = TokenRouter()
    stale = BrokerRuntime(broker_scope_id="b", user_id="u", config=RC(target_enabled=True, target_amount=90))
    router.register_broker(stale)
    saved = {"StopLoss": 40, "Target": 90, "status": 1,
             "LockAndTrail": {"InstrumentMove": 100, "StopLossMove": 70}, "OverallTrailSL": {"InstrumentMove": 10, "StopLossMove": 1}}
    monkeypatch.setattr(broker_scope, "load_broker_settings", lambda *a: saved)
    monkeypatch.setattr(broker_scope, "closed_day_mtm", lambda *a: 65.0)
    broker_scope.rebase_day_mtm(router, None)
    assert stale.config.trailing_mode == TrailingMode.LOCK_AND_TRAIL
    assert stale.config.target_enabled is False
    assert stale.mtm == 65.0



def test_resaving_settings_after_lock_exit_starts_a_fresh_lock_cycle():
    """Cycle 1 peaked at 126 and locked out at 72; a new strategy is added and
    the same settings re-saved while the day total is 48 — that must NOT
    instantly lock out (the old 126 peak belongs to the finished cycle)."""
    from risk.broker_risk import evaluate_broker_risk
    from runtime.broker_runtime import BrokerRuntime
    settings = {"StopLoss": 40, "Target": 90, "status": 1,
                "LockAndTrail": {"InstrumentMove": 100, "StopLossMove": 70},
                "OverallTrailSL": {"InstrumentMove": 10, "StopLossMove": 1}}
    router = TokenRouter()
    broker = BrokerRuntime(broker_scope_id="b", user_id="u", config=build_broker_risk_config(settings), mtm=126.0, peak_pnl=126.0)
    router.register_broker(broker)
    broker.mtm = 72.0
    assert evaluate_broker_risk(broker).decision == RiskDecision.LOCK_HIT
    broker_scope.reset_after_risk_exit(broker)
    broker.mtm = 48.0  # new strategy running at a loss, day total 48
    broker_scope.apply_live_settings(router, "b", "u", settings)
    assert broker.trailing_activated is False and broker.current_floor == 0.0
    assert evaluate_broker_risk(broker).decision == RiskDecision.NONE


def test_algotest_delta_ticker_is_normalized():
    from shared.brokers.delta import client as delta_client
    assert delta_client.normalize_underlying("DELTA_BTCUSD") == "BTCUSD"
    assert delta_client.normalize_underlying("delta_ethusd") == "ETHUSD"
    assert delta_client.normalize_underlying("NIFTY") == "NIFTY"


# ── crypto entry time follows the 17:30 IST expiry session ──────────────────

def _time_cfg(h, m):
    return {"Value": [{"Value": {"IndicatorName": "IndicatorType.TimeIndicator", "Parameters": {"Hour": h, "Minute": m}}}]}


@pytest.mark.parametrize("now_hm, entry_hm, expected, reached", [
    # activated 30-Sep 18:30 IST (session 30-Sep 17:30 -> 01-Oct 17:30)
    ((18, 30), (18, 0), datetime(2026, 9, 30, 18, 0), True),     # already passed -> enter now
    ((18, 30), (20, 0), datetime(2026, 9, 30, 20, 0), False),    # later this evening
    ((18, 30), (4, 11), datetime(2026, 10, 1, 4, 11), False),    # next-day morning
    ((18, 30), (15, 57), datetime(2026, 10, 1, 15, 57), False),  # next day, before 17:30 expiry
    # activated 01-Oct 09:00 IST (still the session that began 30-Sep 17:30)
    ((9, 0), (20, 0), datetime(2026, 9, 30, 20, 0), True),       # passed last evening -> enter now
    ((9, 0), (15, 57), datetime(2026, 10, 1, 15, 57), False),
])
def test_crypto_entry_time_uses_expiry_session(now_hm, entry_hm, expected, reached):
    from conditions import schedule_engine
    day = 30 if now_hm[0] >= 17 else 1
    month = 9 if day == 30 else 10
    now = datetime(2026, month, day, *now_hm, tzinfo=_IST)
    target = schedule_engine.resolve_target_time_ist(_time_cfg(*entry_hm), now, is_crypto=True)
    assert target == expected.replace(tzinfo=_IST)
    assert schedule_engine.is_time_reached(_time_cfg(*entry_hm), now, is_crypto=True) is reached


def test_trading_day_crypto_session_vs_nse():
    from conditions import schedule_engine
    evening = datetime(2026, 9, 30, 20, 0, tzinfo=_IST)
    morning = datetime(2026, 10, 1, 4, 11, tzinfo=_IST)
    assert schedule_engine.trading_day(True, evening) == "2026-09-30"
    assert schedule_engine.trading_day(True, morning) == "2026-09-30"   # same session -> not swept at midnight
    assert schedule_engine.trading_day(False, morning) == "2026-10-01"
    # NSE behaviour unchanged
    assert schedule_engine.resolve_target_time_ist(_time_cfg(9, 20), morning) == datetime(2026, 10, 1, 9, 20, tzinfo=_IST)


# ── activation slippage % on entry/exit fills ───────────────────────────────

def test_activation_slippage_applies_to_entry_and_exit_fill():
    router = TokenRouter()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", strategy_cfg={"Ticker": "NIFTY"}, slippage_pct=5.0)
    router.register_strategy(strategy)
    sell = LegRuntime(leg_id="S", strategy_id="s1", user_id="u1", token="111", is_sell=True, option_type="CE", qty=1,
                      entry_price=100.0, current_price=100.0, leg_cfg={"LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": 10}})
    buy = LegRuntime(leg_id="B", strategy_id="s1", user_id="u1", token="222", is_sell=False, option_type="PE", qty=1,
                     entry_price=100.0, current_price=100.0)
    sl_tp_engine.initialize_sl_tp(sell)
    router.register_leg(sell)
    router.register_leg(buy)
    assert sell.entry_price == 95.0 and buy.entry_price == 105.0     # SELL fills lower, BUY higher
    assert sell.current_sl_price == 105.0                              # SL re-anchored on the fill
    engine.manual_square_off("B")
    assert buy.current_price == 95.0                                   # closing a BUY sells 5% lower
    engine.manual_square_off("S")
    assert sell.current_price == 105.0                                 # closing a SELL buys 5% higher
    assert strategy.mtm == pytest.approx((95.0 - 105.0) + (95.0 - 105.0))


def test_no_slippage_by_default_and_recovered_legs_not_slipped_twice():
    router = TokenRouter()
    strategy = StrategyRuntime(strategy_id="s1", user_id="u1", strategy_cfg={"Ticker": "NIFTY"}, slippage_pct=5.0)
    router.register_strategy(strategy)
    recovered = LegRuntime(leg_id="R", strategy_id="s1", user_id="u1", token="1", is_sell=True, option_type="CE", qty=1,
                           entry_price=95.0, current_price=100.0, slippage_pct=5.0, entry_slippage_applied=True)
    router.register_leg(recovered)
    assert recovered.entry_price == 95.0
    plain = StrategyRuntime(strategy_id="s2", user_id="u1", strategy_cfg={"Ticker": "NIFTY"})
    router.register_strategy(plain)
    leg = LegRuntime(leg_id="P", strategy_id="s2", user_id="u1", token="2", is_sell=True, option_type="CE", qty=1, entry_price=100.0, current_price=100.0)
    router.register_leg(leg)
    assert leg.entry_price == 100.0
