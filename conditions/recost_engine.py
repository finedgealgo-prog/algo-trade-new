"""
recost_engine.py
───────────────────
Re-cost (AtCost re-entry) — master-doc §40/§41/§43. Exact port of
shared/features/execution_socket.py's `_handle_reentry` AtCost branch: same
token/strike as the parent leg (never re-selected), waits for price to
return to the parent's original entry cost, direction derived from the
parent's position (SELL waits for a fall back to cost, BUY waits for a rise
back to cost — see RecostRuntime's docstring for why this must never default).

master-doc §42 "keep three different values, don't overwrite into one
field" — reference_price (the threshold) stays on RecostRuntime; the
eventual trigger tick LTP and actual broker fill price are NOT this
object's concern (Phase 6's order engine records those separately).
"""

from __future__ import annotations

from runtime.leg_runtime import LegRuntime
from runtime.recost_runtime import RecostRuntime


def arm_recost_watcher(recost_id: str, parent_leg: LegRuntime) -> RecostRuntime | None:
    """Freeze token + cost price off the parent leg and arm a watcher —
    master-doc §40. Returns None if the re-cost budget (§43) is exhausted or
    the parent has no usable entry price."""
    if parent_leg.recost_used >= parent_leg.recost_max:
        return None
    if parent_leg.entry_price <= 0:
        return None
    direction = "DOWN" if parent_leg.is_sell else "UP"
    return RecostRuntime(
        recost_id=recost_id,
        strategy_id=parent_leg.strategy_id,
        parent_leg_id=parent_leg.leg_id,
        token=parent_leg.token,
        reference_price=parent_leg.entry_price,
        direction=direction,
        used=parent_leg.recost_used,
        max=parent_leg.recost_max,
        status="ARMED",
    )


def check_recost_trigger(recost: RecostRuntime, current_price: float) -> bool:
    if recost.status != "ARMED" or current_price <= 0:
        return False
    if recost.direction == "DOWN":
        return current_price <= recost.reference_price
    return current_price >= recost.reference_price
