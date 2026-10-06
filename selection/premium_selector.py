"""
premium_selector.py
──────────────────────
Closest/Geq/Lte premium-based strike selection — ported from the live path
(shared/features/live_option_chain.py:1234-1287 `select_strike_live`'s
Premium branch). Operates on chain rows already assembled by
strike_resolver.py (from ltp_cache + instrument_master — never Mongo).

Row shape (matches strike_resolver's ChainRow): {strike, token, ltp, ...}.
`_match_price` in the old code prefers mark_raw (mid of bid/ask) over ltp;
algo-2_0's chain rows use `ltp` directly (bid/ask depth isn't populated for
every token yet — see instrument_master/ltp_cache docstrings) — this is a
deliberate simplification flagged here, not an oversight.
"""

from __future__ import annotations


def select_closest_premium(rows: list[dict], target: float) -> dict | None:
    """Nearest-below vs nearest-above by price; ties go to the BELOW
    candidate (conservative — matches the old code's tie-break)."""
    priced = [r for r in rows if float(r.get("ltp") or 0) > 0]
    if not priced:
        return None
    below = sorted((r for r in priced if r["ltp"] <= target), key=lambda r: -r["ltp"])
    above = sorted((r for r in priced if r["ltp"] > target), key=lambda r: r["ltp"])
    if not below:
        return above[0] if above else None
    if not above:
        return below[0]
    below_diff = abs(below[0]["ltp"] - target)
    above_diff = abs(above[0]["ltp"] - target)
    return below[0] if below_diff <= above_diff else above[0]


def select_premium_geq(rows: list[dict], target: float) -> dict | None:
    """Cheapest candidate with price >= target."""
    candidates = [r for r in rows if float(r.get("ltp") or 0) >= target]
    if not candidates:
        return None
    return min(candidates, key=lambda r: r["ltp"])


def select_premium_leq(rows: list[dict], target: float) -> dict | None:
    """Richest candidate with price <= target."""
    candidates = [r for r in rows if 0 < float(r.get("ltp") or 0) <= target]
    if not candidates:
        return None
    return max(candidates, key=lambda r: r["ltp"])
