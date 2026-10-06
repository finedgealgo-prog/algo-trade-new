"""
daily_portfolio.py
──────────────────
Daily rolling per-instrument portfolio — ported from the old system's
shared/features-adjacent `algo.trade/api.py`'s `_resolve_daily_portfolio`
(get-or-create logic, ~line 720 there), reusing the SAME `algo_trade_
portfolio` Mongo collection (not a new one) so an old-system-activated
BTCUSD strategy and an algo-2_0-activated BTCUSD strategy on the SAME day
end up tagged with the SAME `trade_portfolio` id — user's explicit ask:
"antha logic already old algo.trade la irukum, athu than inga varanum...
current day la every different instrument-kum oru portfolio create
aganum, athu vachu antha instrument la active pantra all strategy-kum
same portfolio id than poganum, next day ku new portfolio".

Key: (trade_date, activation_mode, trade_index) — trade_index is the
underlying ticker (uppercased, e.g. "BTCUSD"/"NIFTY"). One document per
(day, mode, instrument); every strategy activated for that instrument,
that mode, that day reuses the SAME doc's own `trade_portfolio` id — a
new calendar day (or a different instrument) gets a fresh one. algo-2_0
only ever activates in "fast-forward" mode today, so in practice this
rolls over once every IST midnight per instrument.

`trade_group_portfolio` is a SECOND id shared across every INSTRUMENT
activated the same (day, mode) — the old system's own "portfolio of
portfolios" concept: the first instrument activated that day/mode mints
it, every subsequent instrument's own new doc reuses that same value
(a sibling-doc lookup, never regenerated) — same field FastForward2.tsx's
getRecordTradeGroupPortfolioId already reads.

This is what `/analyse-crypto/portfolio/:id` groups by (FastForward2.tsx's
getRecordTradePortfolioId reads `rec.portfolio.trade_portfolio`) — before
this module existed, algo-2_0 just aliased trade_portfolio to whatever
portfolio_id the CALLER happened to pass (empty for a direct-strategy
activation), so two direct BTCUSD activations on the same day never
shared one, and Analyse-by-portfolio only ever showed a single strategy.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bson import ObjectId

from shared.db.mongo import Mongo

_IST = timezone(timedelta(hours=5, minutes=30))

COLLECTION = "algo_trade_portfolio"


def _today_ist_date() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d")


def resolve_daily_portfolio(mongo: Mongo, activation_mode: str, trade_index: str) -> tuple[str, str]:
    """Get-or-create today's (trade_date, activation_mode, trade_index)
    portfolio doc. Returns (trade_portfolio_id, trade_group_portfolio_id).

    Synchronous (plain PyMongo, same as the old system's own version) —
    callers run this via asyncio.to_thread, matching every other blocking
    Mongo call already in services/strategy_activation.py.

    The insert_one/race-fallback shape mirrors the old system's own
    benign-race handling exactly (two near-simultaneous activations for a
    brand-new instrument/day racing to create the first doc) — a unique
    index on (trade_date, activation_mode, trade_index) would make the
    losing insert raise and fall through to the re-query below; without
    one, the loser's insert just succeeds as a harmless duplicate doc and
    every activation still resolves the SAME id going forward since the
    lookup always takes find_one's first match. Not worth adding a new
    index for a single-process dev/fast-forward engine with no concurrent-
    writer risk in practice."""
    trade_date = _today_ist_date()
    normalized_index = str(trade_index or "NIFTY").strip().upper() or "NIFTY"
    normalized_mode = str(activation_mode or "fast-forward").strip() or "fast-forward"

    collection = mongo.raw[COLLECTION]
    query = {"trade_date": trade_date, "activation_mode": normalized_mode, "trade_index": normalized_index}
    projection = {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1}
    existing = collection.find_one(query, projection)
    if existing:
        return str(existing["_id"]), str(existing.get("trade_group_portfolio") or "")

    new_oid = ObjectId()
    now_iso = datetime.now(timezone.utc).isoformat()
    sibling = collection.find_one(
        {"trade_date": trade_date, "activation_mode": normalized_mode, "trade_group_portfolio": {"$exists": True, "$ne": ""}},
        {"trade_group_portfolio": 1},
    )
    trade_group_portfolio = str((sibling or {}).get("trade_group_portfolio") or "").strip() or str(ObjectId())
    new_doc = {
        "_id": new_oid,
        "trade_portfolio": str(new_oid),
        "trade_group_portfolio": trade_group_portfolio,
        "trade_index": normalized_index,
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    try:
        collection.insert_one(new_doc)
        return str(new_oid), trade_group_portfolio
    except Exception:
        fallback = collection.find_one(query, projection)
        if fallback:
            return str(fallback["_id"]), str(fallback.get("trade_group_portfolio") or "")
        return str(new_oid), trade_group_portfolio
