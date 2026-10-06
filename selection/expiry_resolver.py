"""
expiry_resolver.py
─────────────────────
Weekly/NextWeekly/Monthly/NextMonthly expiry resolution — ported from
shared/features/strike_selector.py's resolve_expiry (lines ~85-130):
sorted ascending expiries; Weekly=expiries[0], NextWeekly=expiries[1];
Monthly groups by YYYY-MM taking the LAST date in each month, monthly[0];
NextMonthly=monthly[1].
"""

from __future__ import annotations

from collections import defaultdict


def resolve_expiry(expiries: list[str], expiry_kind: str) -> str | None:
    if not expiries:
        return None
    kind = str(expiry_kind or "")

    if "Monthly" in kind:
        by_month: dict[str, list[str]] = defaultdict(list)
        for e in expiries:
            by_month[e[:7]].append(e)  # "YYYY-MM"
        monthly = [max(dates) for _, dates in sorted(by_month.items())]
        if "Next" in kind:
            return monthly[1] if len(monthly) > 1 else None
        return monthly[0] if monthly else None

    if "NextWeekly" in kind:
        return expiries[1] if len(expiries) > 1 else None

    return expiries[0]
