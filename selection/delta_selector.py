"""
delta_selector.py
────────────────────
Delta-based strike selection — Black-Scholes-Merton pricing/Greeks + IV
bisection, ported EXACTLY (verified byte-for-byte against source) from
shared/features/kite_delta_chain.py's _bs_price/_calc_iv/_calc_greeks/
_time_to_expiry, plus shared/features/delta_selector.py's
select_closest_delta/select_delta_range. Real-money strike selection depends
on these formulas — they were re-read from source and checked line-by-line
before porting, not reconstructed from memory/description.

Constants (same source): risk-free rate 6.8% (India 91-day T-bill),
per-index continuous dividend yields, IV solved via 120-iteration bisection
in [1e-5, 20.0] with 0.001 convergence tolerance.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

_IST = timezone(timedelta(hours=5, minutes=30))
_RISK_FREE_RATE = 0.068
_MIN_T = 1 / (365 * 24 * 60)

_DIVIDEND_YIELDS: dict[str, float] = {
    "NIFTY": 0.012, "BANKNIFTY": 0.005, "FINNIFTY": 0.015, "SENSEX": 0.012, "MIDCPNIFTY": 0.008,
}
_DEFAULT_DIVIDEND_YIELD = 0.01

_SQRT_2 = math.sqrt(2.0)
_SQRT_2PI = math.sqrt(2.0 * math.pi)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / _SQRT_2))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def dividend_yield(underlying: str) -> float:
    return _DIVIDEND_YIELDS.get(str(underlying or "").upper(), _DEFAULT_DIVIDEND_YIELD)


def time_to_expiry(expiry_str: str) -> float:
    """Fraction of a year from now (IST) to market close (15:30) on expiry_str."""
    try:
        exp_day = datetime.fromisoformat(expiry_str[:10])
        exp_close = exp_day.replace(hour=15, minute=30, tzinfo=_IST)
        now_ist = datetime.now(_IST)
        secs = (exp_close - now_ist).total_seconds()
        return max(_MIN_T, secs / (365.0 * 86400))
    except Exception:
        return _MIN_T


def bs_price(
    S: float, K: float, T: float, r: float, sigma: float, opt: str, q: float = 0.0,
    *, sqrt_T: float | None = None, exp_qT: float | None = None, exp_rT: float | None = None,
) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if opt == "CE" else (K - S))
    if sqrt_T is None:
        sqrt_T = math.sqrt(T)
    if exp_qT is None:
        exp_qT = math.exp(-q * T)
    if exp_rT is None:
        exp_rT = math.exp(-r * T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    if opt == "CE":
        return S * exp_qT * _norm_cdf(d1) - K * exp_rT * _norm_cdf(d2)
    return K * exp_rT * _norm_cdf(-d2) - S * exp_qT * _norm_cdf(-d1)


def calc_iv(ltp: float, S: float, K: float, T: float, r: float, opt: str, q: float = 0.0) -> float:
    """Implied volatility via bisection. Returns annual vol (0.15 = 15%)."""
    if ltp <= 0 or S <= 0 or K <= 0 or T <= 0:
        return 0.0
    intrinsic = max(0.0, (S - K) if opt == "CE" else (K - S))
    if ltp < intrinsic:
        return 0.0
    sqrt_T = math.sqrt(T)
    exp_qT = math.exp(-q * T)
    exp_rT = math.exp(-r * T)
    lo, hi = 1e-5, 20.0
    for _ in range(120):
        mid = (lo + hi) * 0.5
        price = bs_price(S, K, T, r, mid, opt, q, sqrt_T=sqrt_T, exp_qT=exp_qT, exp_rT=exp_rT)
        if abs(price - ltp) < 0.001:
            return mid
        if price < ltp:
            lo = mid
        else:
            hi = mid
    return (lo + hi) * 0.5


def calc_greeks(S: float, K: float, T: float, r: float, sigma: float, opt: str, q: float = 0.0) -> dict[str, float]:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    nd1 = _norm_pdf(d1)
    exp_rT = math.exp(-r * T)
    exp_qT = math.exp(-q * T)

    gamma = exp_qT * nd1 / (S * sigma * sqrt_T)
    vega = S * exp_qT * nd1 * sqrt_T / 100.0

    if opt == "CE":
        delta = exp_qT * _norm_cdf(d1)
        theta = (-(S * nd1 * sigma * exp_qT) / (2.0 * sqrt_T) + q * S * exp_qT * _norm_cdf(d1) - r * K * exp_rT * _norm_cdf(d2)) / 365.0
    else:
        delta = exp_qT * (_norm_cdf(d1) - 1.0)
        theta = (-(S * nd1 * sigma * exp_qT) / (2.0 * sqrt_T) - q * S * exp_qT * _norm_cdf(-d1) + r * K * exp_rT * _norm_cdf(-d2)) / 365.0

    return {"delta": round(delta, 4), "gamma": round(gamma, 6), "theta": round(theta, 4), "vega": round(vega, 4)}


def enrich_row_with_greeks(row: dict[str, Any], spot: float, expiry: str, underlying: str, option_type: str) -> dict[str, Any]:
    """Adds iv/delta/gamma/theta/vega to a chain row (strike, ltp already
    present) in place, computed off the row's own ltp. Returns the row."""
    ltp = float(row.get("ltp") or 0)
    strike = float(row.get("strike") or 0)
    if ltp <= 0 or strike <= 0 or spot <= 0:
        row.update(iv=0.0, delta=0.0, gamma=0.0, theta=0.0, vega=0.0)
        return row
    T = time_to_expiry(expiry)
    q = dividend_yield(underlying)
    iv = calc_iv(ltp, spot, strike, T, _RISK_FREE_RATE, option_type, q)
    greeks = calc_greeks(spot, strike, T, _RISK_FREE_RATE, iv, option_type, q)
    row.update(iv=round(iv * 100, 2), **greeks)
    return row


# ── Selection (ported from shared/features/delta_selector.py) ──────────────

def _valid_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        d = float(r.get("delta") or 0)
        p = float(r.get("ltp") or r.get("close") or 0)
        if d != 0.0 and p > 0:
            out.append(r)
    return out


def select_closest_delta(rows: list[dict], target_pct: float, option_type: str) -> dict | None:
    """EntryByDelta — nearest delta to target_pct/100. PE deltas negative
    internally; target_pct is always a positive 0-100 input."""
    target = target_pct / 100.0
    if option_type.upper() == "PE":
        target = -abs(target)
    valid = _valid_rows(rows)
    if not valid:
        return None
    return min(valid, key=lambda r: abs(float(r.get("delta") or 0) - target))


def select_delta_range(rows: list[dict], lower_pct: float, upper_pct: float, option_type: str, is_sell: bool, spot_price: float = 0.0) -> dict | None:
    """EntryByDeltaRange — Sell targets the upper bound, Buy the lower bound;
    tiebreak nearest-to-ATM. Excludes candidates >5% away from spot (deep-ITM
    anomaly guard) unless that empties the set."""
    lower, upper = lower_pct / 100.0, upper_pct / 100.0
    is_pe = option_type.upper() == "PE"
    valid = _valid_rows(rows)

    if is_pe:
        candidates = [r for r in valid if -upper <= float(r.get("delta") or 0) <= -lower]
        target_delta = -upper if is_sell else -lower
    else:
        candidates = [r for r in valid if lower <= float(r.get("delta") or 0) <= upper]
        target_delta = upper if is_sell else lower

    if not candidates:
        return None

    if spot_price > 0:
        max_dist = spot_price * 0.05
        near = [r for r in candidates if abs(float(r.get("strike") or 0) - spot_price) <= max_dist]
        if near:
            candidates = near

    candidates.sort(key=lambda r: (
        abs(float(r.get("delta") or 0) - target_delta),
        abs(float(r.get("strike") or 0) - spot_price) if spot_price > 0 else 0,
    ))
    return candidates[0]
