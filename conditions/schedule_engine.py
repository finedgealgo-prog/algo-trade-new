"""
schedule_engine.py
─────────────────────
EntryIndicators scheduled-entry support — TimeIndicator only (e.g. "enter at
09:20 IST"). Full technical-indicator trees (RSI/moving-average/etc gated
entry) are explicitly NOT ported in this pass — a real gap, flagged so it
isn't mistaken for an oversight, not faked with a stub that always passes.

EntryIndicators tree shape — confirmed against real `saved_strategies` docs
(every one of them, not a hypothetical): the root is almost always an
OperandNode wrapping a LIST of child nodes in `Value`, e.g.

  {"Type": "IndicatorTreeNodeType.OperandNode", "OperandType": "OperandType.And",
   "Value": [
       {"Type": "IndicatorTreeNodeType.DataNode",
        "Value": {"IndicatorName": "IndicatorType.TimeIndicator",
                   "Parameters": {"Hour": 9, "Minute": 20}}},
       ...
   ]}

A bare DataNode (`{"Value": {"IndicatorName": ..., "Parameters": ...}}`) is
also accepted directly, for callers that already unwrapped one level. Only
the first TimeIndicator found anywhere in the tree is used — nested
multi-condition trees combining several indicator types are a later
extension (this module only supports TimeIndicator-gated entries at all,
per the module-level docstring above).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

_IST = timezone(timedelta(hours=5, minutes=30))


def extract_time_indicator(entry_indicators: dict) -> tuple[int, int] | None:
    node = entry_indicators or {}
    value = node.get("Value")

    if isinstance(value, list):
        for child in value:
            if not isinstance(child, dict):
                continue
            found = extract_time_indicator(child)
            if found is not None:
                return found
        return None

    if not isinstance(value, dict):
        return None
    if "TimeIndicator" not in str(value.get("IndicatorName") or ""):
        return None
    params = value.get("Parameters") or {}
    hour, minute = params.get("Hour"), params.get("Minute")
    if hour is None or minute is None:
        return None
    return int(hour), int(minute)


# Crypto (Delta) trading day = one option-expiry cycle: from 17:30 IST (the
# moment the previous daily expiry settles and the next one is the live
# contract) to 17:30 IST the next calendar day.
CRYPTO_SESSION_START_HOUR, CRYPTO_SESSION_START_MINUTE = 17, 30


def crypto_session_start(now: datetime | None = None) -> datetime:
    """Start (17:30 IST) of the crypto session `now` falls in."""
    now_ist = (now or datetime.now(_IST)).astimezone(_IST)
    start = now_ist.replace(hour=CRYPTO_SESSION_START_HOUR, minute=CRYPTO_SESSION_START_MINUTE, second=0, microsecond=0)
    if now_ist < start:
        start -= timedelta(days=1)
    return start


def trading_day(is_crypto: bool, now: datetime | None = None) -> str:
    """"YYYY-MM-DD" of the trading day `now` belongs to: the IST calendar
    date for NSE; for crypto the date its 17:30 IST session started on
    (so 30-Sep 18:00 and 01-Oct 04:11 are both trading day 30-Sep)."""
    if is_crypto:
        return crypto_session_start(now).date().isoformat()
    return (now or datetime.now(_IST)).astimezone(_IST).date().isoformat()


def resolve_target_time_ist(entry_indicators: dict, now: datetime | None = None, is_crypto: bool = False) -> datetime | None:
    """The entry instant for the strategy's TimeIndicator.

    NSE: that clock time today (IST).
    Crypto: that clock time's occurrence INSIDE the current 17:30→17:30
    session — e.g. activated 30-Sep 18:30: 20:00 -> 30-Sep 20:00, 04:11 ->
    01-Oct 04:11, 15:57 -> 01-Oct 15:57 (the morning belongs to the session
    that started the previous evening, trading that session's next-day
    expiry), 18:00 -> 30-Sep 18:00 (already passed -> enter now)."""
    parsed = extract_time_indicator(entry_indicators)
    if parsed is None:
        return None
    hour, minute = parsed
    now_ist = (now or datetime.now(_IST)).astimezone(_IST)
    if not is_crypto:
        return now_ist.replace(hour=hour, minute=minute, second=0, microsecond=0)
    start = crypto_session_start(now_ist)
    target = start.replace(hour=hour, minute=minute)
    if target < start:
        target += timedelta(days=1)
    return target


def is_time_reached(entry_indicators: dict, now: datetime | None = None, is_crypto: bool = False) -> bool:
    """True if there's no TimeIndicator configured at all (nothing to gate
    on) OR the configured time has already passed (today for NSE, in the
    current 17:30→17:30 session for crypto)."""
    target = resolve_target_time_ist(entry_indicators, now, is_crypto)
    if target is None:
        return True
    return (now or datetime.now(_IST)) >= target
