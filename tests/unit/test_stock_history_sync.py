"""Daily stock history sync — services/stock_history_sync.py (copied from
algo.scanner's /scanner/sync_historical_data)."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from services import stock_history_sync as shs

IST = shs.IST


def _c(day, close, o=1.0):
    return {"date": f"{day} 00:00:00", "open": o, "high": close + 1, "low": o - 1, "close": close, "volume": 100}


def test_stock_rows_match_the_scanner_shape_with_previous_close():
    docs = shs.transform_stock_candles("TCS", "2953217", [_c("2026-10-26", 10), _c("2026-10-27", 12)], {"2026-10-27"})
    assert docs == [{
        "h_symbol": "TCS", "token": "2953217", "h_symbol_token": "2953217", "ch_timestamp": "2026-10-27",
        "ch_series": "EQ", "ch_opening_price": 1.0, "ch_high_price": 13.0, "ch_low_price": 0.0,
        "ch_closing_price": 12.0, "ch_volume": 100, "ch_previous_cls_price": 10.0,
    }]


def test_index_rows_carry_change_pct():
    docs = shs.transform_index_candles("nifty_50", "NIFTY 50", [_c("2026-10-26", 100), _c("2026-10-27", 110)], {"2026-10-27"})
    assert docs[0]["i_previous_close"] == 100.0 and docs[0]["i_ch"] == 10.0 and docs[0]["ih_timestamp"] == "2026-10-27"


def test_trading_dates_skip_weekends_and_holidays():
    days = shs.trading_dates(datetime(2026, 10, 19), datetime(2026, 10, 25), {"2026-10-20"})
    assert days == {"2026-10-19", "2026-10-21", "2026-10-22", "2026-10-23"}


class _Col:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find(self, query=None, projection=None):
        query = query or {}
        out = []
        for d in self.docs:
            ok = True
            for k, v in query.items():
                if isinstance(v, dict) and "$in" in v:
                    ok = ok and d.get(k) in v["$in"]
                elif not isinstance(v, dict):
                    ok = ok and d.get(k) == v
            if ok:
                out.append(dict(d))
        return out

    def find_one(self, query=None, projection=None):
        rows = self.find(query)
        return rows[0] if rows else None

    def distinct(self, field, query):
        (ts_field, rng), = query.items()
        return sorted({d[field] for d in self.docs if rng["$gte"] <= d.get(ts_field, "") <= rng["$lte"]})

    def insert_many(self, docs, ordered=False):
        self.docs.extend(docs)


class _Kite:
    def __init__(self):
        self.calls = []

    def instruments(self, exchange):
        return []


def _fake_env(monkeypatch, db, candles_by_token):
    mongo = SimpleNamespace(_db=db, close=lambda: None)
    monkeypatch.setattr(shs, "MongoData", lambda: mongo)
    kite = _Kite()
    monkeypatch.setattr(shs, "_build_kite_client_from_config", lambda _db: kite)

    def fetch(_kite, token, frm, to):
        kite.calls.append(token)
        return candles_by_token.get(int(token), [])

    monkeypatch.setattr(shs, "_fetch_kite_daily_candles", fetch)
    monkeypatch.setattr(shs, "KITE_INDEX_TOKENS", {"NIFTY": 256265})
    monkeypatch.setattr(shs, "DEFAULT_SCANNER_INDEXES", [
        {"lookup_symbol": "NIFTY", "raw_symbol": "NIFTY50", "normalized_symbol": "nifty_50", "fetch_mode": "token"},
    ])
    return kite


def _db(existing_stock_rows=()):
    return {
        "kite_market_config": _Col([{"broker": "kite", "enabled": True, "api_key": "k", "access_token": "t"}]),
        shs.MARKET_HOLIDAYS_COLLECTION: _Col([{"date": "2026-10-20"}]),
        shs.STOCKS_COLLECTION: _Col([{"symbol": "TCS", "kite_token": 2953217}, {"symbol": "INFY", "kite_token": 408065}, {"symbol": "NOTOK"}]),
        shs.INDEX_STOCKS_COLLECTION: _Col(),
        shs.HISTORY_COLLECTION: _Col(existing_stock_rows),
        shs.INDEX_HISTORY_COLLECTION: _Col(),
        shs.UNIVERSE_STOCK_LIST_COLLECTION: _Col(),
    }


def test_sync_writes_today_for_every_stock_and_index_and_skips_existing(monkeypatch):
    db = _db(existing_stock_rows=[{"h_symbol": "INFY", "ch_timestamp": "2026-10-27"}])  # 15:50 snapshot already wrote INFY
    day = [_c("2026-10-26", 10), _c("2026-10-27", 11)]
    kite = _fake_env(monkeypatch, db, {2953217: day, 408065: day, 256265: day})

    out = shs.sync_historical_data("2026-10-27", "2026-10-27")
    assert out["status"] == "success" and out["trading_days"] == ["2026-10-27"]
    assert out["stocks_processed"] == 1 and out["stocks_skipped_existing"] == 1 and out["indexes_processed"] == 1
    assert 408065 not in kite.calls  # existing -> no API call
    tcs = [d for d in db[shs.HISTORY_COLLECTION].docs if d["h_symbol"] == "TCS"]
    assert len(tcs) == 1 and tcs[0]["ch_closing_price"] == 11.0 and tcs[0]["ch_previous_cls_price"] == 10.0
    assert [d for d in db[shs.HISTORY_COLLECTION].docs if d["h_symbol"] == "INFY"] == [{"h_symbol": "INFY", "ch_timestamp": "2026-10-27"}]
    assert {"symbol": "NOTOK", "reason": "missing_kite_token"} in out["skipped"]

    again = shs.sync_historical_data("2026-10-27", "2026-10-27")  # second run same day: no duplicates
    assert again["stocks_processed"] == 0 and again["indexes_processed"] == 0
    assert len([d for d in db[shs.HISTORY_COLLECTION].docs if d["h_symbol"] == "TCS"]) == 1


def test_expired_kite_token_fails_loudly(monkeypatch):
    db = _db()
    _fake_env(monkeypatch, db, {})

    def boom(*a):
        raise Exception("TokenException: Incorrect `api_key` or `access_token`.")

    monkeypatch.setattr(shs, "_fetch_kite_daily_candles", boom)
    with pytest.raises(ValueError, match="Invalid Kite access token"):
        shs.sync_historical_data("2026-10-27", "2026-10-27")
    assert shs.get_status()["running"] is False


def test_daily_run_is_today_only_and_skips_holidays(monkeypatch):
    db = _db()
    seen = []
    monkeypatch.setattr(shs, "MongoData", lambda: SimpleNamespace(_db=db, close=lambda: None))
    monkeypatch.setattr(shs, "sync_historical_data", lambda s, e: seen.append((s, e)) or {"status": "success"})
    shs.run_today(datetime(2026, 10, 27, 16, 0, tzinfo=IST))
    assert seen == [("2026-10-27", "2026-10-27")]
    assert shs.run_today(datetime(2026, 10, 20, 16, 0, tzinfo=IST))["status"] == "skipped"  # holiday
    assert shs.run_today(datetime(2026, 10, 25, 16, 0, tzinfo=IST))["status"] == "skipped"  # Sunday
    assert len(seen) == 1


def test_routes_keep_the_scanner_paths():
    from api.routers.stock_history import router

    paths = {r.path for r in router.routes}
    assert {"/scanner/sync_historical_data", "/scanner/historical_data_sync_status", "/scanner/stop_historical_data_sync"} <= paths
