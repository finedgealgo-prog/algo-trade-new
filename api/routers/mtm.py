"""
routers/mtm.py
─────────────────
Path-compatible with the old algo.trade's GET /algo/mtm/window-config and
GET /algo/mtm/historical-data (api.py:7905-7962) — FastForward2.tsx (now
pointed at algo-2_0, port 8010) calls these to draw its MTM historical
chart. Minimal ports:

  - /mtm/window-config: the old route's config is effectively static (NSE/
    crypto default chart-window HH:MM, seeded once into Mongo and cached
    forever) — returned here as the same literal default, no Mongo
    round-trip or collection of its own needed.
  - /mtm/historical-data: crypto (Delta) legs + BTCUSD/ETHUSD spot, from
    TWO sources fetched in parallel and merged per minute:
      1. DB — stock_data.option_chain, the per-minute snapshots algo.
         simulator's crypto_live_option_chain_collector writes (`security_id`
         = leg token, `close` = Delta raw mark, `spot_price` on every row).
      2. Delta's public /v2/history/candles (1m) — option legs use the MARK
         series ("MARK:<symbol>"; options rarely trade, so trade candles are
         mostly empty), spot uses the spot-index symbol.
    DB value wins where it has the minute; Delta fills every gap (collector
    down / not yet written). All prices are Delta's raw per-1-coin quote —
    the same units as the live LTP feed and entry_trade.price.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query

from features.delta_exchange_client import DELTA_SPOT_INDEX_SYMBOL, _get as delta_get
from shared.db.mongo import get_mongo

log = logging.getLogger(__name__)

router = APIRouter(prefix="/algo/mtm", tags=["mtm"])

# Same literal default as shared/features/mtm_window_config.py's _DEFAULT_CONFIG
# (true UTC HH:MM — 03:46/10:10 = 09:16/15:40 IST for NSE; a ~24h rolling
# window anchored at 17:31 UTC for crypto, which trades 24/7).
_WINDOW_CONFIG = {
    "nse": {"start_hhmm": "03:46", "end_hhmm": "10:10"},
    "crypto": {"start_hhmm": "17:31", "start_day_offset": -1, "end_hhmm": "17:29", "end_day_offset": 0},
}


@router.get("/window-config")
def mtm_window_config() -> dict:
    return _WINDOW_CONFIG


_CRYPTO_OPTION_TOKEN = re.compile(r"^[CP]-(BTC|ETH)-\d+(\.\d+)?-\d{6}$")
_CRYPTO_SPOT_TOKEN = {"BTCUSD": "BTC", "ETHUSD": "ETH"}


def _to_epoch(raw: str, fallback: int) -> int:
    """'YYYY-MM-DDTHH:MM[:SS][Z]' or 'YYYY-MM-DD HH:MM:SS', taken as UTC."""
    raw = (raw or "").strip().replace(" ", "T").rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return int(datetime.strptime(raw[:19], fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return fallback


def _delta_series(symbol: str, start: int, end: int) -> dict | None:
    """Blocking — one Delta 1m candle request, shaped like the old
    mtm_historical_data output ({timestamp, open, high, low, close, volume},
    ascending, timestamps 'YYYY-MM-DDTHH:MM:00Z')."""
    try:
        data = delta_get("/v2/history/candles", {"symbol": symbol, "resolution": "1m", "start": start, "end": end})
    except Exception as exc:
        log.warning("[mtm] delta candles failed symbol=%s: %s", symbol, exc)
        return None
    rows = sorted((r for r in (data.get("result") or []) if r.get("time") is not None), key=lambda r: r["time"])
    if not rows:
        return None
    return {
        "timestamp": [datetime.fromtimestamp(int(r["time"]), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:00Z") for r in rows],
        "open": [float(r["open"]) for r in rows],
        "high": [float(r["high"]) for r in rows],
        "low": [float(r["low"]) for r in rows],
        "close": [float(r["close"]) for r in rows],
        "volume": [float(r.get("volume") or 0) for r in rows],
    }


_DB_INDEX_READY = False


def _db_minute_to_candle(ts: str) -> str:
    """The collector stamps a snapshot taken AT minute T's open (T:00) as T;
    a Delta 1m candle T closes at T+1:00. So collector T == Delta candle
    T-1's close (verified: same price) — relabel to T-1 so both sources
    mean "price at the end of this minute"."""
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:00Z")
    except ValueError:
        return ""
    return (dt - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:00Z")


def _db_series(option_tokens: list[str], spot_tokens: list[str], start: int, end: int) -> dict[str, dict[str, float]]:
    """Blocking — {token: {minute_iso: close}} from stock_data.option_chain.
    Spot (BTCUSD/ETHUSD) comes from the `spot_price` riding on the leg rows
    of the same underlying, so it needs at least one leg token."""
    global _DB_INDEX_READY
    coll = get_mongo().raw["option_chain"]
    if not _DB_INDEX_READY:
        try:
            coll.create_index([("security_id", 1), ("timestamp", 1)], name="security_id_timestamp", background=True)
        except Exception as exc:
            log.warning("[mtm] option_chain index create failed: %s", exc)
        _DB_INDEX_READY = True
    if not option_tokens:
        return {}
    iso = lambda t: datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:00Z")
    cursor = coll.find(
        # +60s: the snapshot labelled T+1 is candle T's close (see _db_minute_to_candle).
        {"security_id": {"$in": option_tokens}, "timestamp": {"$gte": iso(start + 60), "$lte": iso(end + 60)}},
        {"_id": 0, "security_id": 1, "timestamp": 1, "close": 1, "spot_price": 1, "underlying": 1},
    )
    out: dict[str, dict[str, float]] = {}
    for doc in cursor:
        ts = _db_minute_to_candle(str(doc.get("timestamp") or ""))
        if not ts:
            continue
        if doc.get("close"):
            out.setdefault(doc["security_id"], {})[ts] = float(doc["close"])
        spot_key = f"{doc.get('underlying')}USD"
        if spot_key in spot_tokens and doc.get("spot_price"):
            out.setdefault(spot_key, {}).setdefault(ts, float(doc["spot_price"]))
    return out


def _merge(db_points: dict[str, float] | None, api: dict | None) -> dict | None:
    """Per-minute union — DB value where present, Delta for the gaps.
    Close-only from the DB side (one snapshot per minute)."""
    points: dict[str, tuple[float, float, float, float]] = {}
    if api:
        for i, ts in enumerate(api["timestamp"]):
            points[ts] = (api["open"][i], api["high"][i], api["low"][i], api["close"][i])
    for ts, close in (db_points or {}).items():
        points[ts] = (close, close, close, close)
    if not points:
        return None
    keys = sorted(points)
    return {
        "timestamp": keys,
        "open": [points[k][0] for k in keys],
        "high": [points[k][1] for k in keys],
        "low": [points[k][2] for k in keys],
        "close": [points[k][3] for k in keys],
        "volume": [0.0] * len(keys),
    }


@router.get("/historical-data")
async def mtm_historical_data_range(tokens: str = Query(default=""), start_dt: str = Query(default=""), end_dt: str = Query(default="")) -> dict:
    now = int(datetime.now(timezone.utc).timestamp())
    end = min(_to_epoch(end_dt, now), now)
    start = _to_epoch(start_dt, end - 24 * 3600)
    if start >= end:
        return {}

    jobs: dict[str, str] = {}
    for token in {t.strip() for t in tokens.split(",") if t.strip()}:
        key = token.upper()
        if _CRYPTO_OPTION_TOKEN.match(key):
            jobs[token] = f"MARK:{key}"
        elif key in _CRYPTO_SPOT_TOKEN:
            jobs[token] = DELTA_SPOT_INDEX_SYMBOL[_CRYPTO_SPOT_TOKEN[key]]
    if not jobs:
        return {}

    option_tokens = [t for t in jobs if not jobs[t].startswith(".")]
    spot_tokens = [t for t in jobs if jobs[t].startswith(".")]
    db_task = asyncio.to_thread(_db_series, option_tokens, spot_tokens, start, end)
    api_tasks = [asyncio.to_thread(_delta_series, sym, start, end) for sym in jobs.values()]
    db_result, *api_results = await asyncio.gather(db_task, *api_tasks, return_exceptions=True)
    if isinstance(db_result, Exception):
        log.warning("[mtm] option_chain read failed: %s", db_result)
        db_result = {}

    out = {}
    for token, api in zip(jobs.keys(), api_results):
        merged = _merge(db_result.get(token), None if isinstance(api, Exception) else api)
        if merged:
            out[token] = merged
    return out
