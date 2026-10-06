"""
routers/crypto_straddle.py
──────────────────────────
Rolling ATM straddle chart for BTC/ETH (algo-admin's CryptoStraddleChart.tsx).

Reads stock_data.option_chain — the per-minute BTC/ETH chain snapshots
written by algo.simulator's crypto_live_option_chain_collector.py (every
expiry, every strike; `close` = Delta's raw per-1-BTC/ETH mark). algo-2_0
doesn't collect these itself; it only reads them.

  GET /crypto/straddle-chart/expiries?underlying=BTC[&date=YYYY-MM-DD]
  GET /crypto/straddle-chart?underlying=BTC&expiry=DD-MM-YYYY[&date=YYYY-MM-DD]

`date` is the collector's UTC date bucket, default today (UTC). The daily
crypto parquet export clears older days out of Mongo, so effectively only
today/yesterday are available here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from shared.db.mongo import get_mongo

router = APIRouter(prefix="/crypto/straddle-chart", tags=["crypto-straddle"])

SUPPORTED_UNDERLYINGS = ("BTC", "ETH")

# Only strikes within this fraction of spot are considered when picking each
# minute's ATM — keeps far-OTM strikes out of the $group stages (ATM is
# always well inside this band).
_ATM_BAND = 0.03


def _validate(underlying: str, date: str) -> tuple[str, str]:
    underlying = underlying.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'. Use one of {SUPPORTED_UNDERLYINGS}.")
    date = date.strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return underlying, date


def _expiry_sort_key(expiry: str) -> str:
    """DD-MM-YYYY → YYYY-MM-DD so expiries sort chronologically."""
    parts = expiry.split("-")
    return f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts) == 3 else expiry


@router.get("/expiries")
def straddle_expiries(
    underlying: str = Query(default="BTC"),
    date: str = Query(default=""),
) -> dict:
    underlying, date = _validate(underlying, date)
    expiries = get_mongo().raw["option_chain"].distinct("expiry", {"underlying": underlying, "date": date})
    # Drop contracts that already expired but still have rows from earlier in the day.
    expiries = sorted((e for e in expiries if e and _expiry_sort_key(e) >= date), key=_expiry_sort_key)
    return {"underlying": underlying, "date": date, "expiries": expiries}


@router.get("")
def straddle_chart(
    underlying: str = Query(default="BTC"),
    expiry: str = Query(...),
    date: str = Query(default=""),
) -> dict:
    """Per-minute ATM straddle series. Each minute's ATM is re-picked from
    that minute's own spot_price — the strike nearest spot that has both a
    CE and a PE quote — so the line follows spot like a rolling straddle."""
    underlying, date = _validate(underlying, date)
    expiry = expiry.strip()

    pipeline = [
        {"$match": {"underlying": underlying, "date": date, "expiry": expiry}},
        {"$project": {
            "_id": 0, "timestamp": 1, "strike": 1, "type": 1, "close": 1, "spot_price": 1,
            "dist": {"$abs": {"$subtract": ["$strike", "$spot_price"]}},
        }},
        {"$match": {"$expr": {"$lte": ["$dist", {"$multiply": ["$spot_price", _ATM_BAND]}]}}},
        {"$group": {
            "_id": {"ts": "$timestamp", "strike": "$strike"},
            "ce": {"$max": {"$cond": [{"$eq": ["$type", "CE"]}, "$close", None]}},
            "pe": {"$max": {"$cond": [{"$eq": ["$type", "PE"]}, "$close", None]}},
            "spot": {"$first": "$spot_price"},
            "dist": {"$first": "$dist"},
        }},
        {"$match": {"ce": {"$gt": 0}, "pe": {"$gt": 0}}},
        {"$sort": {"_id.ts": 1, "dist": 1}},
        {"$group": {
            "_id": "$_id.ts",
            "strike": {"$first": "$_id.strike"},
            "ce": {"$first": "$ce"},
            "pe": {"$first": "$pe"},
            "spot": {"$first": "$spot"},
        }},
        {"$sort": {"_id": 1}},
    ]
    rows = get_mongo().raw["option_chain"].aggregate(pipeline, allowDiskUse=True)

    points = []
    for r in rows:
        ce, pe, strike = float(r["ce"]), float(r["pe"]), float(r["strike"])
        points.append({
            "timestamp": r["_id"],
            "spot": round(float(r["spot"] or 0.0), 2),
            "strike": strike,
            "ce": round(ce, 2),
            "pe": round(pe, 2),
            "straddle": round(ce + pe, 2),
            # Put-call parity: F = K + C - P.
            "synthetic_future": round(strike + ce - pe, 2),
        })

    return {"underlying": underlying, "expiry": expiry, "date": date, "points": points}
