"""FastForward2/AlgoTrade2 Archive button — POST /algo/execute-orders/archive."""

from __future__ import annotations

import sys
from types import SimpleNamespace

from api.routers import ws_live


class _Col:
    def __init__(self):
        self.calls = []

    def update_many(self, query, update):
        self.calls.append((query, update))
        return SimpleNamespace(modified_count=len(query["_id"]["$in"]))


def test_archive_skips_running_and_scopes_to_user(monkeypatch):
    col = _Col()
    monkeypatch.setitem(sys.modules, "main", SimpleNamespace(token_router=SimpleNamespace(strategies={"run": object()})))
    monkeypatch.setattr(ws_live, "get_mongo", lambda: SimpleNamespace(raw={"algo_trades": col}))
    out = ws_live.archive_strategies(ws_live.ArchiveRequest(strategy_ids=["done1", "run", " done2 "]), user={"_id": "u"})
    assert out["archived"] == 2 and out["skipped_running"] == ["run"]
    query, update = col.calls[0]
    assert query["_id"]["$in"] == ["done1", "done2"]
    assert query["user_id"] == "u" and query["active_on_server"] == {"$ne": True}
    assert update["$set"]["archived"] is True


def test_nothing_archivable_does_not_touch_db(monkeypatch):
    col = _Col()
    monkeypatch.setitem(sys.modules, "main", SimpleNamespace(token_router=SimpleNamespace(strategies={"run": object()})))
    monkeypatch.setattr(ws_live, "get_mongo", lambda: SimpleNamespace(raw={"algo_trades": col}))
    out = ws_live.archive_strategies(ws_live.ArchiveRequest(strategy_ids=["run"]), user={"_id": "u"})
    assert out["archived"] == 0 and col.calls == []
