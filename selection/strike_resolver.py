"""
strike_resolver.py
─────────────────────
Orchestrator: given underlying/expiry/option_type/entry_kind, assemble chain
rows from instrument_master (static strike/token) + ltp_cache (live price)
— never Mongo, matching master-doc §7 ("logical option chain = option_master
+ ltp_cache + greeks_cache", generated fresh from cache, not rebuilt from a
DB query per tick) — then dispatch to the right selector.

`entry_kind` strings match the old system's exactly (EntryType.* — see
delta_selector.py/live_option_chain.py) so existing strategy configs need no
translation: "ATM"/"OTM3"/"ITM2" style strike params, "EntryByPremium(Geq|Leq)",
"EntryByDelta", "EntryByDeltaRange".
"""

from __future__ import annotations

from contextvars import ContextVar

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from selection import atm_selector, delta_selector, premium_selector
from shared.brokers.delta import client as delta_client
from shared.logging.logger import get_logger
from shared.market.instrument_master import InstrumentMaster
from shared.market.ltp_cache import LtpCache

log = get_logger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

# Delta's mark_price channel publishes every ~2s per contract; anything much
# older means that contract's feed is not live right now.
_CRYPTO_MAX_QUOTE_AGE_SECONDS = 30.0

# Tighter limit for a trigger-driven entry (straddle spike alert): the entry
# must be priced at the trigger moment. Set by strategy_activation around an
# immediate (skip_entry_time) activation; a ContextVar so it reaches the
# background entry task and its to_thread calls without new parameters.
_TRIGGER_MAX_QUOTE_AGE_SECONDS = 5.0
crypto_quote_max_age: ContextVar[float] = ContextVar("crypto_quote_max_age", default=_CRYPTO_MAX_QUOTE_AGE_SECONDS)


def is_crypto_underlying(underlying: str) -> bool:
    return underlying.upper() in delta_client.UNDERLYING_TO_ASSET


def _print_chain_snapshot(underlying: str, expiry: str, option_type: str, rows: list[dict], source: str) -> None:
    """Prints the exact chain data build_chain_rows is about to hand the
    selector — every strike + its LTP (Delta's raw per-1-underlying-unit
    quote, same convention as entry_trade.price everywhere else — see
    shared/brokers/delta/client.py's _leg_row) — right before a strike is
    picked from it, i.e. right before an entry. `source` says
    where the data came from (cache = delta_instrument_cache RAM read,
    rest_* = a live REST fallback) so a cold/stale-cache case is visible
    here too, not just inferred later. User's own ask: see the option
    chain data backing an entry BEFORE it happens, not just the one
    strike that got picked."""
    if not log.isEnabledFor(10):
        return
    ts = datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    strikes = sorted(rows, key=lambda r: r["strike"])
    strike_str = ", ".join(f"{r['strike']:g}={r['ltp']:.4f}" for r in strikes)
    log.debug(f"[{ts}] [CHAIN] {underlying} {expiry} {option_type} source={source} count={len(rows)} strikes=[{strike_str}]")


@dataclass
class StrikeSelection:
    token: str
    strike: float
    symbol: str
    ltp: float
    option_type: str
    expiry: str
    delta: float = 0.0
    iv: float = 0.0


def build_chain_rows(instrument_master: InstrumentMaster, ltp_cache: LtpCache, underlying: str, expiry: str, option_type: str) -> list[dict]:
    """Every instrument_master row for this (underlying, expiry, option_type),
    enriched with live ltp from ltp_cache. Rows with no live price yet
    (never ticked/not subscribed) are skipped — a selector can't pick a
    strike it has no usable price for.

    Crypto underlyings (BTCUSD/ETHUSD) read from shared/market/
    delta_instrument_cache.py instead — the SAME pattern as the NSE branch
    below (static metadata cache + ltp_cache's live tick-fed price, zero
    per-call REST/Mongo I/O), not a separate architecture (see that
    module's docstring for why this exists — the user's own ask: match
    NSE's cached-instrument approach for crypto instead of a synchronous
    Delta REST call on every single strike resolution). Falls back to the
    original direct REST fetch only when the cache genuinely has nothing
    for this chain yet (cold start before the first periodic refresh, or a
    brand-new expiry that cycle hasn't seen) — never a hard failure just
    because the cache hasn't caught up."""
    if is_crypto_underlying(underlying):
        from shared.market.delta_instrument_cache import get_delta_instrument_cache
        metas = get_delta_instrument_cache().by_underlying_expiry_type(underlying, expiry, option_type)
        if not metas:
            rest_rows = delta_client.fetch_option_chain_rest(underlying, expiry, option_type)
            _print_chain_snapshot(underlying, expiry, option_type, rest_rows, source="rest_cold_cache")
            return rest_rows
        rows = []
        for meta in metas:
            ltp = ltp_cache.get_ltp(meta.token)
            if not ltp or ltp_cache.is_stale(meta.token, crypto_quote_max_age.get()):
                # A quote the feed stopped updating must not become an
                # entry price — if nothing fresh is left, the REST fallback
                # below fetches live marks instead.
                continue
            # delta/gamma/theta/vega/iv carried through — EntryByDelta/
            # EntryByDeltaRange selection (delta_selector.select_closest_
            # delta/select_delta_range) filters rows through _valid_rows(),
            # which drops anything with delta==0; omitting these here (as
            # an earlier version of this cache did) silently emptied that
            # filter and made every EntryByDelta selection fail once the
            # cache was warm — see DeltaInstrumentMeta's own docstring.
            rows.append({
                "strike": meta.strike, "token": meta.token, "symbol": meta.symbol, "ltp": ltp,
                "delta": meta.delta, "gamma": meta.gamma, "theta": meta.theta, "vega": meta.vega, "iv": meta.iv,
            })
        if not rows:
            # Metadata is cached but nothing has ticked yet (subscribe just
            # landed, mark_price hasn't published its first tick) — same
            # REST fallback as the cold-cache case, not a failure.
            rest_rows = delta_client.fetch_option_chain_rest(underlying, expiry, option_type)
            _print_chain_snapshot(underlying, expiry, option_type, rest_rows, source="rest_no_ticks_yet")
            return rest_rows
        _print_chain_snapshot(underlying, expiry, option_type, rows, source="cache")
        return rows
    rows: list[dict] = []
    for meta in instrument_master.by_underlying_expiry_type(underlying, expiry, option_type):
        ltp = ltp_cache.get_ltp(meta.token)
        if not ltp:
            continue
        rows.append({"strike": meta.strike, "token": meta.token, "symbol": meta.symbol, "ltp": ltp})
    return rows


def resolve_strike(
    instrument_master: InstrumentMaster, ltp_cache: LtpCache,
    underlying: str, expiry: str, option_type: str, entry_kind: str, strike_param: Any,
    spot_price: float, is_sell: bool = False,
) -> StrikeSelection | None:
    """`strike_param` is the leg config's raw `StrikeParameter` value — its
    shape depends on entry_kind, exactly as leg_builder.py produces it:
    a "StrikeType.*" string for EntryByStrikeType, a plain number for
    EntryByPremium(Geq|Leq)/EntryByDelta, or a {"LowerRange","UpperRange"}
    dict for EntryByPremiumRange/EntryByDeltaRange.

    Real saved strategies use several EntryType variants, and StrikeParameter's
    shape genuinely varies per variant (PremiumCloseToStraddle's dict crashed a
    plain `float(strike_param)` here before that branch was added) — an
    activation attempt failing to resolve a strike is a normal, expected
    outcome (ActivationError, caller-visible), but a strike_param shape this
    module doesn't yet handle correctly must never surface as an unhandled 500.
    Wrapped defensively; any unexpected exception here is logged and treated
    as "couldn't resolve", same as a genuinely unmatched strike."""
    try:
        return _resolve_strike(instrument_master, ltp_cache, underlying, expiry, option_type, entry_kind, strike_param, spot_price, is_sell)
    except Exception:
        log.exception("[StrikeResolver] entry_kind=%s strike_param=%r failed to resolve — treating as unresolved", entry_kind, strike_param)
        return None


def _resolve_strike(
    instrument_master: InstrumentMaster, ltp_cache: LtpCache,
    underlying: str, expiry: str, option_type: str, entry_kind: str, strike_param: Any,
    spot_price: float, is_sell: bool = False,
) -> StrikeSelection | None:
    kind = str(entry_kind or "")
    rows = build_chain_rows(instrument_master, ltp_cache, underlying, expiry, option_type)

    if option_type == "FUT":
        # leg_builder.py's InstrumentKind.FUTURES — no strike concept at
        # all, so entry_kind/strike_param (StrikeType/premium-range/delta/
        # etc, everything every branch below implements) are meaningless
        # here. Exactly one contract exists per (underlying, expiry) —
        # dhan_token_sync.py tags it option_type="FUT", strike=0.0 in the
        # SAME active_option_tokens rows instrument_master.py already
        # loads unfiltered, so build_chain_rows' normal chain-key lookup
        # above already found it — nothing new to fetch.
        priced = [r for r in rows if float(r.get("ltp") or 0) > 0]
        if not priced:
            return None
        chosen = priced[0]
        return StrikeSelection(chosen["token"], float(chosen.get("strike") or 0), chosen["symbol"], chosen["ltp"], "FUT", expiry)

    if "Delta" in kind:
        # Crypto rows already carry REAL, Delta-Exchange-computed greeks
        # (see client._leg_row) — recomputing via our own Black-Scholes
        # here would need a live spot price (often 0 for a token that
        # isn't separately tracked in ltp_cache.spot_map) and would
        # overwrite a correct value with an incorrect one, not improve it.
        if not is_crypto_underlying(underlying):
            for row in rows:
                delta_selector.enrich_row_with_greeks(row, spot_price, expiry, underlying, option_type)
        if "Range" in kind and isinstance(strike_param, dict):
            lower = float(strike_param.get("LowerRange") or 0)
            upper = float(strike_param.get("UpperRange") or 0)
            chosen = delta_selector.select_delta_range(rows, lower, upper, option_type, is_sell, spot_price)
        else:
            chosen = delta_selector.select_closest_delta(rows, float(strike_param or 0), option_type)
        if chosen is None:
            return None
        return StrikeSelection(chosen["token"], chosen["strike"], chosen["symbol"], chosen["ltp"], option_type, expiry, chosen.get("delta", 0.0), chosen.get("iv", 0.0))

    if "PremiumCloseToStraddle" in kind:
        # Ported exactly from the old system's live_option_chain.py
        # ("PremiumCloseToStraddle" section) — the crash this replaced
        # (`float(strike_param or 0)` on a dict) was hit by a REAL saved
        # strategy, not a hypothetical: StrikeParameter here is always
        # {"Multiplier": <float>, ...}, never a scalar.
        if spot_price <= 0:
            return None
        multiplier = float((strike_param or {}).get("Multiplier") or 0.5) if isinstance(strike_param, dict) else 0.5
        if is_crypto_underlying(underlying):
            # Delta's strike grid isn't a fixed step — take ATM from the
            # listed strikes, same as the plain ATM/OTM/ITM crypto path.
            atm_strike = atm_selector.resolve_atm_otm_itm_strike_from_rows(rows, spot_price, option_type, "ATM")
        else:
            atm_strike = atm_selector.resolve_atm_otm_itm_strike(spot_price, underlying, option_type, "ATM")
        ce_rows = rows if option_type == "CE" else build_chain_rows(instrument_master, ltp_cache, underlying, expiry, "CE")
        pe_rows = rows if option_type == "PE" else build_chain_rows(instrument_master, ltp_cache, underlying, expiry, "PE")
        atm_ce = next((r for r in ce_rows if float(r["strike"]) == atm_strike), None)
        atm_pe = next((r for r in pe_rows if float(r["strike"]) == atm_strike), None)
        ce_ltp = float(atm_ce["ltp"]) if atm_ce else 0.0
        pe_ltp = float(atm_pe["ltp"]) if atm_pe else 0.0
        straddle = ce_ltp + pe_ltp
        if straddle <= 0:
            return None
        target = straddle * multiplier
        # Only match against rows with a real live price — an unfiltered
        # min() by |ltp-target| would happily "match" an unpriced (ltp=0)
        # row as if it were closest, same guard the old system added after
        # a live SENSEX pick landed 7800 points away on a zero-priced row.
        priced_rows = [r for r in rows if float(r.get("ltp") or 0) > 0]
        if not priced_rows:
            return None
        chosen = min(priced_rows, key=lambda r: abs(float(r["ltp"]) - target))
        return StrikeSelection(chosen["token"], chosen["strike"], chosen["symbol"], chosen["ltp"], option_type, expiry)

    if "Premium" in kind:
        if "Range" in kind and isinstance(strike_param, dict):
            lower = float(strike_param.get("LowerRange") or 0)
            upper = float(strike_param.get("UpperRange") or 0)
            candidates = [r for r in rows if lower <= float(r.get("ltp") or 0) <= upper]
            chosen = min(candidates, key=lambda r: abs(r["ltp"] - (lower + upper) / 2)) if candidates else None
        elif "Geq" in kind:
            chosen = premium_selector.select_premium_geq(rows, float(strike_param or 0))
        elif "Leq" in kind:
            chosen = premium_selector.select_premium_leq(rows, float(strike_param or 0))
        else:
            chosen = premium_selector.select_closest_premium(rows, float(strike_param or 0))
        if chosen is None:
            return None
        return StrikeSelection(chosen["token"], chosen["strike"], chosen["symbol"], chosen["ltp"], option_type, expiry)

    # Default: ATM / OTM<N> / ITM<N> by strike-type string. Crypto uses the
    # REAL listed strikes in `rows` to compute this (see atm_selector.
    # resolve_atm_otm_itm_strike_from_rows's own docstring — BTCUSD/ETHUSD
    # aren't in the NSE-index step table the non-crypto branch below uses,
    # and an arithmetic target computed from a wrong default step has no
    # guarantee of matching one of Delta's actual listed strikes).
    if spot_price <= 0:
        # ATM/OTM/ITM is relative to spot — with no spot the nearest strike
        # to 0 (deepest ITM call) would be picked silently.
        log.warning("[StrikeResolver] %s %s %s: no underlying spot, strike not resolved", underlying, expiry, option_type)
        return None
    if is_crypto_underlying(underlying):
        target_strike = atm_selector.resolve_atm_otm_itm_strike_from_rows(rows, spot_price, option_type, str(strike_param or "ATM"))
    else:
        target_strike = atm_selector.resolve_atm_otm_itm_strike(spot_price, underlying, option_type, str(strike_param or "ATM"))
    if target_strike is None:
        return None
    for row in rows:
        if float(row["strike"]) == target_strike:
            return StrikeSelection(row["token"], row["strike"], row["symbol"], row["ltp"], option_type, expiry)
    return None
