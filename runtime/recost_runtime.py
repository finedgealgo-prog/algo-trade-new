"""
recost_runtime.py
────────────────────
In-memory runtime object for one armed Re-cost (AtCost re-entry) watcher —
master-doc §40/§41. Unlike Immediate/LikeOriginal re-entry, AtCost reuses the
PARENT leg's exact token/strike (never a fresh selection) and waits for price
to return to the ORIGINAL entry cost — ported from
shared/features/execution_socket.py's `_handle_reentry`'s AtCost branch
(lines 5169-5251), which copies the parent's strike/expiry/token/symbol
verbatim and arms a wait for price to return to `entry_trade.price`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RecostRuntime:
    recost_id: str
    strategy_id: str
    parent_leg_id: str

    token: str  # frozen — SAME token as the parent leg, never re-resolved
    reference_price: float  # the parent leg's original entry (cost) price

    # "DOWN" for a SELL parent (its SL was hit by price rising, so cost is
    # below current price — must fall back down); "UP" for a BUY parent
    # (opposite). Ported from execution_socket.py:5221-5224's
    # `_at_is_sell` direction derivation — the exact bug it fixes (a SELL
    # AtCost re-entry firing immediately instead of waiting) is why this
    # direction must be derived from parent position, never defaulted.
    direction: str

    used: int
    max: int

    # ARMED -> TRIGGERED -> ORDER_PENDING -> ENTERED (master-doc §37, same
    # state shape as LazyRuntime)
    status: str = "ARMED"
