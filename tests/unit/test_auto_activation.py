"""Portfolio Auto Activate — services/auto_activation.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from bson import ObjectId

from services import auto_activation as aa
from services.strategy_activation import ActivationError

IST = timezone(timedelta(hours=5, minutes=30))
THU_0531 = datetime(2026, 10, 22, 17, 31, tzinfo=IST)  # a Thursday, 5:31 PM
S1, S2, S3 = (str(ObjectId()) for _ in range(3))


def _portfolio(is_weekdays=True):
    return {
        "_id": ObjectId(), "name": "P", "is_weekdays": is_weekdays, "strategy_ids": [ObjectId(S1), ObjectId(S2), ObjectId(S3)],
        "strategies": [
            {"id": S1, "weekdays": ["Th"], "checked": True},
            {"id": S2, "weekdays": ["M", "T"], "checked": False},
            {"id": S3, "checked": True},  # no weekdays -> M-F default
        ],
    }


def test_todays_strategies_weekday_mode_matches_the_popup_rule():
    assert aa.todays_strategy_ids(_portfolio(), THU_0531) == [S1, S3]
    saturday = THU_0531 + timedelta(days=2)
    assert aa.todays_strategy_ids(_portfolio(), saturday) == []


def test_todays_strategies_dte_mode_uses_saved_checked():
    assert aa.todays_strategy_ids(_portfolio(is_weekdays=False), THU_0531) == [S1, S3]


def test_due_only_from_0531_and_once_per_day():
    cfg = {"enabled": True, "last_run_date": ""}
    assert not aa.due(cfg, THU_0531.replace(minute=30))
    assert aa.due(cfg, THU_0531)
    assert not aa.due({**cfg, "last_run_date": "2026-10-22"}, THU_0531)
    assert not aa.due({"enabled": False}, THU_0531)


class _Col:
    def __init__(self, docs=None):
        self.docs = {d["_id"]: d for d in (docs or [])}

    def find_one(self, q, *a, **k):
        d = self.docs.get(q["_id"])
        return dict(d) if d else None

    def insert_one(self, doc):
        self.inserted = getattr(self, "inserted", []) + [doc]

    def update_one(self, q, update, upsert=False):
        d = self.docs.get(q["_id"])
        ok = d is not None and all(
            (d.get(k) != v["$ne"]) if isinstance(v, dict) and "$ne" in v else d.get(k) == v
            for k, v in q.items() if k != "_id"
        )
        if ok:
            d.update(update["$set"])
        return SimpleNamespace(modified_count=1 if ok else 0)


def test_claim_is_exactly_once_per_day():
    col = _Col([{"_id": "p:live", "enabled": True, "last_run_date": "2026-10-21"}])
    mongo = SimpleNamespace(raw={aa.COLLECTION: col})
    assert aa.claim(mongo, "p:live", THU_0531)
    assert not aa.claim(mongo, "p:live", THU_0531)  # second tick / restart
    assert aa.claim(mongo, "p:live", THU_0531 + timedelta(days=1))


def test_run_one_activates_todays_strategies_with_chosen_settings():
    portfolio = _portfolio()
    pid = str(portfolio["_id"])
    strategies = {ObjectId(s): {"_id": ObjectId(s), "name": f"s-{s[-3:]}"} for s in (S1, S2, S3)}
    cfg_doc = {"_id": aa.config_id(pid, "live"), "portfolio_id": pid, "activation_mode": "live", "user_id": "u",
               "broker_scope_id": "b1", "slippage_pct": 0.5, "portfolio_qty": 2, "strategy_qty": {S1: 6}, "enabled": True}
    mongo = SimpleNamespace(raw={
        "saved_portfolios": _Col([portfolio]),
        "saved_strategies": _Col(list(strategies.values())),
        aa.COLLECTION: _Col([cfg_doc]),
        aa.RUNS_COLLECTION: _Col(),
    })
    calls, notes = [], []

    async def activate(doc, user_id, broker, **kw):
        calls.append((str(doc["_id"]), user_id, broker, kw))
        if str(doc["_id"]) == S3:
            raise ActivationError("broker not active")
        return {"strategy_id": "exec_1"}

    summary = asyncio.run(aa.run_one(
        cfg_doc, THU_0531, token_router=None, order_engine=None, mongo=mongo, activate=activate,
        resolve_group=lambda *a: ("grp", "P"), notify=lambda *a: notes.append(a),
    ))
    assert [c[0] for c in calls] == [S1, S3]  # S2 not scheduled on Thursday
    _, user, broker, kw = calls[0]
    assert (user, broker) == ("u", "b1")
    assert kw["qty_multiplier"] == 6 and kw["slippage_pct"] == 0.5 and kw["activation_mode"] == "live"
    assert kw["group_id"] == "grp" and kw["portfolio_id"] == pid and kw["is_direct_strategy"] is False
    assert calls[1][3]["qty_multiplier"] == 2  # no per-strategy qty -> portfolio qty
    assert summary["ok"] == 1 and summary["failed"] == 1 and summary["skipped"] == 1
    assert notes and "1 activated, 1 failed" in notes[0][2]
    assert mongo.raw[aa.COLLECTION].docs[cfg_doc["_id"]]["last_result"]["ok"] == 1
    run = mongo.raw[aa.RUNS_COLLECTION].inserted[0]  # per-user history row
    assert (run["user_id"], run["target_id"], run["run_date"], run["ok"], run["failed"]) == ("u", pid, "2026-10-22", 1, 1)


def test_strategy_runs_on_its_edit_setup_days():
    doc = {"is_weekdays": True, "weekdays": {"thursday": True, "monday": True}}
    assert aa.strategy_runs_today(doc, THU_0531)
    assert not aa.strategy_runs_today(doc, THU_0531 + timedelta(days=1))  # Friday
    assert aa.strategy_runs_today({"is_weekdays": True, "weekdays": {}}, THU_0531)  # default M-F
    assert not aa.strategy_runs_today({"is_weekdays": True, "weekdays": {}}, THU_0531 + timedelta(days=2))
    assert aa.strategy_runs_today({"is_weekdays": False}, THU_0531 + timedelta(days=2))  # DTE: every day


def test_strategy_target_activates_as_direct_strategy():
    sid = str(ObjectId())
    doc = {"_id": ObjectId(sid), "name": "live-1lot", "is_weekdays": True, "weekdays": {"thursday": True}}
    cfg = {"_id": aa.config_id(sid, "live", "strategy"), "target_type": "strategy", "strategy_id": sid,
           "activation_mode": "live", "user_id": "u", "broker_scope_id": "b1", "qty_multiplier": 10,
           "slippage_pct": 0.2, "enabled": True}
    mongo = SimpleNamespace(raw={"saved_strategies": _Col([doc]), aa.COLLECTION: _Col([cfg]), aa.RUNS_COLLECTION: _Col()})
    calls = []

    async def activate(d, user_id, broker, **kw):
        calls.append((str(d["_id"]), user_id, broker, kw))
        return {"strategy_id": "exec_9"}

    out = asyncio.run(aa.run_one(cfg, THU_0531, token_router=None, order_engine=None, mongo=mongo,
                                 activate=activate, resolve_group=None))
    assert out["ok"] == 1 and calls[0][:3] == (sid, "u", "b1")
    kw = calls[0][3]
    assert kw["is_direct_strategy"] is True and kw["qty_multiplier"] == 10 and kw["slippage_pct"] == 0.2 and kw["activation_mode"] == "live"
    assert out["results"][0]["strategy_id"] == "exec_9"

    calls.clear()  # Friday: not one of its days
    out = asyncio.run(aa.run_one(cfg, THU_0531 + timedelta(days=1), token_router=None, order_engine=None, mongo=mongo,
                                 activate=activate, resolve_group=None))
    assert calls == [] and out["skipped"] == 1


def test_runs_at_531_pm_not_am():
    cfg = {"enabled": True, "last_run_date": ""}
    assert not aa.due(cfg, THU_0531.replace(hour=5, minute=31))
    assert not aa.due(cfg, THU_0531.replace(hour=17, minute=30))
    assert aa.due(cfg, THU_0531)


def test_unticked_strategies_are_never_auto_activated():
    assert aa.todays_strategy_ids(_portfolio(), THU_0531, excluded=[S1]) == [S3]


def test_auto_run_skips_a_strategy_already_activated_by_hand():
    running = SimpleNamespace(strategies={"exec_x": SimpleNamespace(source_strategy_id=S1, activation_mode="live", status="ACTIVE")})
    assert aa.already_running(running, S1, "live")
    assert not aa.already_running(running, S1, "fast-forward")  # other page's mode
    assert not aa.already_running(running, S3, "live")
    done = SimpleNamespace(strategies={"exec_y": SimpleNamespace(source_strategy_id=S1, activation_mode="live", status="EXITED")})
    assert not aa.already_running(done, S1, "live")

    portfolio = _portfolio()
    pid = str(portfolio["_id"])
    cfg = {"_id": aa.config_id(pid, "live"), "portfolio_id": pid, "activation_mode": "live", "user_id": "u",
           "broker_scope_id": "b1", "enabled": True}
    mongo = SimpleNamespace(raw={
        "saved_portfolios": _Col([portfolio]),
        "saved_strategies": _Col([{"_id": ObjectId(s), "name": s} for s in (S1, S2, S3)]),
        aa.COLLECTION: _Col([cfg]), aa.RUNS_COLLECTION: _Col(),
    })
    calls = []

    async def activate(doc, *a, **kw):
        calls.append(str(doc["_id"]))
        return {"strategy_id": "exec_new"}

    asyncio.run(aa.run_one(cfg, THU_0531, token_router=running, order_engine=None, mongo=mongo,
                           activate=activate, resolve_group=lambda *a: ("g", "P")))
    assert calls == [S3]  # S1 already running by hand -> not activated twice
