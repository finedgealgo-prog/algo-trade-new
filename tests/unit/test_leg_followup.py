"""
test_leg_followup.py
───────────────────────
Phase 7 (Lazy/Re-entry/Re-cost activation-time arming) unit tests: a SL/TP
exit correctly arms the matching followup based on LegReentrySL/LegReentryTP,
gated by entry_guard, using a fresh in-memory instrument_master/ltp_cache
(no Mongo needed — these are pure Python objects here).

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_leg_followup.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orders.order_engine import OrderEngine
from runtime.broker_runtime import BrokerRuntime
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime
from services import leg_followup
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from shared.market.instrument_master import InstrumentMaster, InstrumentMeta
from shared.market.ltp_cache import LtpCache
from token_router import TokenRouter


def make_instrument_master() -> InstrumentMaster:
    im = InstrumentMaster()
    for strike, token in ((24900, "T1"), (25000, "T2"), (25100, "T3")):
        im._add(InstrumentMeta(
            token=token, symbol=f"NIFTY-CE-{strike}", instrument="NIFTY", expiry="2026-09-10",
            strike=strike, option_type="CE", exchange="NSE", ws_segment="NSE_FNO", lot_size=75,
        ))
        pe_token = f"{token}P"
        im._add(InstrumentMeta(
            token=pe_token, symbol=f"NIFTY-PE-{strike}", instrument="NIFTY", expiry="2026-09-10",
            strike=strike, option_type="PE", exchange="NSE", ws_segment="NSE_FNO", lot_size=75,
        ))
    return im


def setup():
    router = TokenRouter()
    engine = OrderEngine(router, VirtualBrokerAdapter())
    im = make_instrument_master()
    cache = LtpCache()
    cache.ltp_map.update({"T1": 90.0, "T2": 100.0, "T3": 60.0, "T1P": 40.0, "T2P": 45.0, "T3P": 50.0})
    cache.spot_map["NIFTY"] = 25000.0

    broker = BrokerRuntime(broker_scope_id="user100:dhan01", user_id="u1")
    strategy = StrategyRuntime(
        strategy_id="s1", user_id="u1", broker_scope_id="user100:dhan01",
        strategy_cfg={"Ticker": "NIFTY", "IdleLegConfigs": {
            "lazy1": {
                "InstrumentKind": "LegType.PE", "PositionType": "PositionType.Sell",
                "ExpiryKind": "ExpiryType.Weekly", "EntryType": "EntryType.EntryByStrikeType",
                "StrikeParameter": "StrikeType.ATM", "LegMomentum": {"Type": "MomentumType.PercentageDown", "Value": 10},
            },
        }},
    )
    router.register_broker(broker)
    router.register_strategy(strategy)
    return router, engine, im, cache, broker, strategy


def make_leg(**overrides) -> LegRuntime:
    defaults = dict(
        leg_id="leg1", strategy_id="s1", user_id="u1", token="T2",
        is_sell=True, option_type="CE", qty=75, entry_price=100.0,
        leg_cfg={"ExpiryKind": "ExpiryType.Weekly", "EntryType": "EntryType.EntryByStrikeType",
                 "StrikeParameter": "StrikeType.ATM", "InstrumentKind": "LegType.CE"},
        broker_scope_id="user100:dhan01", current_price=100.0,
    )
    defaults.update(overrides)
    return LegRuntime(**defaults)


# ── NextLeg -> Lazy watcher armed with IdleLegConfigs' own strike/momentum ──

def test_next_leg_arms_lazy_watcher():
    router, engine, im, cache, broker, strategy = setup()
    leg = make_leg(leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "lazy1"}}})
    router.register_leg(leg)

    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")

    assert len(router.lazy_watchers) == 1
    watcher = list(router.lazy_watchers.values())[0]
    assert watcher.option_type == "PE"
    assert watcher.strike == 25000  # ATM
    assert watcher.reference_ltp == 45.0  # T2P ltp
    assert watcher.trigger_price == 40.5  # 10% down


def test_next_leg_missing_ref_does_not_crash():
    router, engine, im, cache, broker, strategy = setup()
    leg = make_leg(leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.NextLeg", "Value": {"NextLegRef": "does_not_exist"}}})
    router.register_leg(leg)
    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")
    assert len(router.lazy_watchers) == 0


# ── AtCost -> Re-cost watcher reusing the exited leg's own token/strike ────

def test_at_cost_arms_recost_watcher_same_token():
    router, engine, im, cache, broker, strategy = setup()
    leg = make_leg(recost_max=2, recost_used=0, leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.AtCost", "Value": {"ReentryCount": 2}}})
    router.register_leg(leg)

    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")

    assert len(router.recost_watchers) == 1
    watcher = list(router.recost_watchers.values())[0]
    assert watcher.token == "T2"  # SAME token, not re-selected
    assert watcher.reference_price == 100.0  # leg's own entry price
    assert leg.recost_used == 1


def test_at_cost_exhausted_budget_does_not_arm():
    router, engine, im, cache, broker, strategy = setup()
    leg = make_leg(recost_max=1, recost_used=1, leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.AtCost", "Value": {"ReentryCount": 1}}})
    router.register_leg(leg)
    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")
    assert len(router.recost_watchers) == 0


# ── Immediate -> fresh strike, enters right away, counter consumed ─────────

def test_immediate_enters_fresh_leg_immediately():
    router, engine, im, cache, broker, strategy = setup()
    leg = make_leg(reentry_max=2, reentry_used=0, leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.Immediate", "Value": {"ReentryCount": 2}}})
    router.register_leg(leg)

    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")

    assert leg.reentry_used == 1
    new_legs = [l for lid, l in router.legs.items() if lid != "leg1"]
    assert len(new_legs) == 1
    assert new_legs[0].status == "ACTIVE"
    assert new_legs[0].entry_price == 100.0  # ATM token T2's ltp


# ── entry_guard blocks followups once broker/strategy already exited ──────

def test_followup_blocked_when_broker_exited():
    router, engine, im, cache, broker, strategy = setup()
    broker.status = "EXIT_PENDING"
    leg = make_leg(leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.AtCost", "Value": {"ReentryCount": 2}}})
    router.register_leg(leg)

    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")
    assert len(router.recost_watchers) == 0


def test_followup_blocked_when_strategy_exited():
    router, engine, im, cache, broker, strategy = setup()
    strategy.status = "EXIT_PENDING"
    leg = make_leg(leg_cfg={**make_leg().leg_cfg, "LegReentrySL": {"Type": "ReentryType.AtCost", "Value": {"ReentryCount": 2}}})
    router.register_leg(leg)

    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")
    assert len(router.recost_watchers) == 0


# ── No reentry config -> no-op, no crash ────────────────────────────────────

def test_no_reentry_config_is_noop():
    router, engine, im, cache, broker, strategy = setup()
    leg = make_leg()  # no LegReentrySL/TP at all
    router.register_leg(leg)
    leg_followup.handle_leg_exit(router, engine, im, cache, leg, "SL_HIT")
    assert len(router.lazy_watchers) == 0
    assert len(router.recost_watchers) == 0
