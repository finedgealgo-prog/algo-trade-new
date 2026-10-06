"""
atm_selector.py
──────────────────
ATM / OTM / ITM strike resolution — ported from the LIVE path
(shared/features/live_option_chain.py:190-192 `_atm_from_spot`, 1313-1320's
regex offset logic), NOT shared/features/strike_selector.py's backtest
offset table — that one has a real bug (`_select_atm_offset` coerces
`StrikeType.OTM1` to float before dispatch, so it silently resolves to ATM
instead of OTM1; confirmed by re-reading `resolve_strike`'s `_safe_float`
coercion at its call site). The live regex path doesn't have this bug and is
what real trades actually use, so it's the one ported here.
"""

from __future__ import annotations

import re

_STRIKE_STEP_MAP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "SENSEX": 100, "MIDCPNIFTY": 25}
_DEFAULT_STEP = 100


def strike_step(underlying: str) -> int:
    return _STRIKE_STEP_MAP.get(str(underlying or "").upper(), _DEFAULT_STEP)


def atm_from_spot(spot: float, step: int) -> float:
    if step <= 0:
        return round(spot)
    return round(spot / step) * step


def resolve_atm_otm_itm_strike(spot: float, underlying: str, option_type: str, strike_param: str) -> float:
    """strike_param like "ATM", "OTM3", "ITM2" -> final strike.

    PE direction is reversed (master-doc-neutral fact, ported exactly from
    live_option_chain.py:1313-1320): for a CE, OTM means strikes ABOVE spot;
    for a PE, OTM means strikes BELOW spot — same +offset*step magnitude,
    sign flipped for PE.
    """
    step = strike_step(underlying)
    atm = atm_from_spot(spot, step)
    sp_str = str(strike_param or "").strip().upper()

    m_otm = re.search(r"OTM(\d+)", sp_str)
    m_itm = re.search(r"ITM(\d+)", sp_str)
    if not m_otm and not m_itm:
        return atm

    raw_offset = int((m_otm or m_itm).group(1))
    if m_itm:
        raw_offset = -raw_offset  # ITM moves toward spot, opposite direction of OTM

    is_pe = "PE" in str(option_type or "").upper()
    offset_n = -raw_offset if is_pe else raw_offset
    return atm + offset_n * step


def resolve_atm_otm_itm_strike_from_rows(rows: list[dict], spot: float, option_type: str, strike_param: str) -> float | None:
    """Same ATM/OTM/ITM semantics as resolve_atm_otm_itm_strike above, but
    for an underlying with no entry in _STRIKE_STEP_MAP (any crypto
    underlying — BTCUSD/ETHUSD strike spacing has nothing to do with NSE's
    index tables) — derives the ATM strike and OTM/ITM offsets from the
    REAL listed strikes in `rows` instead of a fixed step size.

    Confirmed live: a real BTCUSD EntryByStrikeType leg failed "strike not
    resolved" on every activation attempt — resolve_atm_otm_itm_strike fell
    back to _DEFAULT_STEP=100 (BTCUSD isn't in _STRIKE_STEP_MAP), computed
    a target strike from spot/100 rounding, and strike_resolver.py's caller
    requires an EXACT match against Delta's actually-listed strikes — which
    that computed value has no guarantee of ever landing on (Delta's real
    BTC strike grid is a different spacing entirely). Walking the sorted
    REAL strikes by INDEX for the OTM/ITM offset instead guarantees the
    result is always one Delta actually lists, never an arithmetic value
    that has to accidentally match."""
    strikes = sorted({float(r["strike"]) for r in rows if r.get("strike") not in (None, "")})
    if not strikes:
        return None
    atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
    sp_str = str(strike_param or "").strip().upper()

    m_otm = re.search(r"OTM(\d+)", sp_str)
    m_itm = re.search(r"ITM(\d+)", sp_str)
    if not m_otm and not m_itm:
        return strikes[atm_idx]

    raw_offset = int((m_otm or m_itm).group(1))
    if m_itm:
        raw_offset = -raw_offset

    is_pe = "PE" in str(option_type or "").upper()
    offset_n = -raw_offset if is_pe else raw_offset
    idx = max(0, min(len(strikes) - 1, atm_idx + offset_n))
    return strikes[idx]
