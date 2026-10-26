"""
FastForward2 per-strategy / per-portfolio Risk Settings save
(api/routers/runtime_risk.py): Lock semantics, input validation, and what a
re-save carries over.
"""

from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api.routers import runtime_risk as route
from risk.models import RiskDecision


@pytest.fixture
def router(monkeypatch):
    members = [SimpleNamespace(strategy_id=f"s{i}", group_id="g1", mtm=1000.0, user_id="u", name=f"s{i}", group_name="grp") for i in range(3)]
    token_router = SimpleNamespace(strategies={m.strategy_id: m for m in members}, runtime_risks={})

    async def _publish(rr):
        return None

    monkeypatch.setitem(sys.modules, "main", SimpleNamespace(token_router=token_router, publish_runtime_risk=_publish))
    return token_router


def _save(**kw):
    payload = route.SaveRuntimeRiskRequest(scope="group", target_id="g1", **kw)
    return asyncio.run(route.save_runtime_risk(payload, user={"_id": "u"}))


def test_lock_holds_until_profit_falls_below_locked_amount(router):
    # Group MTM 3000; "if profit reaches 3000, lock 2000" must NOT exit now.
    _save(LockAndTrail={"InstrumentMove": 3000, "StopLossMove": 2000})
    rr = router.runtime_risks["group:g1"]
    for mtm in (3000, 3050, 2500, 2001):
        assert rr.evaluate(mtm).decision == RiskDecision.NONE
    assert rr.trailing_activated and rr.floor == 2000
    assert rr.evaluate(1999).decision == RiskDecision.LOCK_HIT


@pytest.mark.parametrize("lock", [
    {"InstrumentMove": 20000, "StopLossMove": 160000},
    {"InstrumentMove": 3000, "StopLossMove": 3000},
])
def test_lock_profit_not_below_trigger_is_rejected(router, lock):
    with pytest.raises(HTTPException) as exc:
        _save(LockAndTrail=lock)
    assert exc.value.status_code == 400
    assert "group:g1" not in router.runtime_risks


def test_resave_with_changed_lock_ignores_profit_reached_before_save(router):
    # 12:36 profit touches 7516 under the first lock (3000 -> 2300)...
    _save(LockAndTrail={"InstrumentMove": 3000, "StopLossMove": 2300}, OverallTrailSL={"InstrumentMove": 300, "StopLossMove": 30})
    rr = router.runtime_risks["group:g1"]
    rr.evaluate(7516)
    assert rr.trailing_activated

    # ...13:37 user sets "reaches 7500 -> lock 6600" while profit is 7440:
    # the earlier 7516 must not count, so the lock is not active yet.
    _save(LockAndTrail={"InstrumentMove": 7500, "StopLossMove": 6600}, OverallTrailSL={"InstrumentMove": 300, "StopLossMove": 30})
    rr = router.runtime_risks["group:g1"]
    assert rr.peak == 0 and rr.floor == 0 and not rr.trailing_activated
    assert rr.evaluate(7440).decision == RiskDecision.NONE
    assert not rr.trailing_activated
    # Even a drop below 6600 does not exit — the lock never activated.
    assert rr.evaluate(6000).decision == RiskDecision.NONE
    # Reaching 7500 after the save activates it; the floor is the new 6600.
    assert rr.evaluate(7500).decision == RiskDecision.NONE
    assert rr.trailing_activated and rr.floor == 6600
    assert rr.evaluate(6599).decision == RiskDecision.LOCK_HIT


def test_resave_with_same_lock_keeps_floor(router):
    lock = {"InstrumentMove": 3000, "StopLossMove": 2000}
    _save(LockAndTrail=lock)
    router.runtime_risks["group:g1"].evaluate(3200)
    _save(LockAndTrail=lock, StopLoss=5000)
    rr = router.runtime_risks["group:g1"]
    assert rr.trailing_activated and rr.floor == 2000


def test_lock_only_resave_ignores_profit_reached_before_save(router):
    _save(LockAndTrail={"InstrumentMove": 3000, "StopLossMove": 2300})
    router.runtime_risks["group:g1"].evaluate(7516)

    _save(LockAndTrail={"InstrumentMove": 7500, "StopLossMove": 6600})
    rr = router.runtime_risks["group:g1"]
    assert rr.peak == 0 and not rr.trailing_activated
    assert rr.evaluate(7440).decision == RiskDecision.NONE
    assert rr.evaluate(6000).decision == RiskDecision.NONE
    assert not rr.trailing_activated


@pytest.mark.parametrize("second", [
    {"StopLoss": 2000, "OverallTrailSL": {"InstrumentMove": 500, "StopLossMove": 100}},  # trail changed
    {"StopLoss": 1500, "OverallTrailSL": {"InstrumentMove": 300, "StopLossMove": 100}},  # only the SL changed
])
def test_trail_sl_resave_ignores_profit_reached_before_save(router, second):
    # Trail SL: SL 2000 shrinks by 100 per 300 of peak profit.
    _save(StopLoss=2000, OverallTrailSL={"InstrumentMove": 300, "StopLossMove": 100})
    rr = router.runtime_risks["group:g1"]
    rr.evaluate(3000)
    assert rr.state()["effective_sl"] == 2000 - 10 * 100

    _save(**second)
    rr = router.runtime_risks["group:g1"]
    assert rr.peak == 0
    # The earlier 3000 peak no longer tightens the new SL.
    assert rr.state()["effective_sl"] == second["StopLoss"]


def test_trail_sl_resave_unchanged_keeps_progress(router):
    trail = {"StopLoss": 2000, "OverallTrailSL": {"InstrumentMove": 300, "StopLossMove": 100}}
    _save(**trail)
    router.runtime_risks["group:g1"].evaluate(3000)
    _save(**trail)
    assert router.runtime_risks["group:g1"].peak == 3000
