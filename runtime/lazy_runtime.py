"""
lazy_runtime.py
──────────────────
In-memory runtime object for one armed Lazy Leg watcher — master-doc §32/§37.

Once armed, `token`/`strike`/`reference_ltp`/`trigger_price` are FROZEN
(master-doc §32: "Select strike ONCE, freeze token, freeze reference LTP,
calculate trigger, watch same token — do NOT continuously re-select the
strike"). All the fields needed to reconstruct that frozen state after a
restart (master-doc §61) live right here — nothing else to fetch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LazyRuntime:
    lazy_id: str
    strategy_id: str
    parent_leg_id: str  # the leg (or lazy leg) whose exit armed this watcher

    token: str
    strike: float
    option_type: str  # "CE" | "PE"

    momentum_type: str  # raw config Type string, e.g. "...PercentageUp" / "...PointsDown"
    reference_ltp: float
    trigger_price: float

    # Everything token_router's LAZY_TRIGGERED completion (main.py's
    # _on_lazy_triggered -> services/leg_followup.complete_lazy_entry) needs
    # to actually place the entry order and build the new LegRuntime once
    # this watcher fires — the parent leg that armed this watcher may
    # already be long gone from a fresh strike's perspective (NextLeg/
    # LikeOriginal both select a NEW contract, not the parent's own), so
    # none of this can be re-derived from parent_leg_id alone at trigger
    # time. Frozen here at arm time instead, same "select once, don't
    # re-derive later" principle the rest of this object already follows.
    qty: int = 0
    is_sell: bool = False
    leg_cfg: dict[str, Any] = field(default_factory=dict)
    broker_scope_id: str = ""
    user_id: str = ""
    expiry: str = ""  # frozen alongside token/strike — same LegRuntime.expiry this becomes on entry

    # WAITING_PRICE -> ARMED -> TRIGGERED -> ORDER_PENDING -> ENTERED (master-doc §37)
    # LikeOriginal re-entry lineage budget, carried onto the leg this watcher
    # enters (reentry_max < 0 = not a LikeOriginal re-entry: the entered
    # leg's budget comes fresh from its own leg_cfg instead).
    reentry_used: int = 0
    reentry_max: int = -1
    status: str = "ARMED"

    # UTC ISO timestamp, set once at arm time — surfaced to the frontend as
    # a "pending_feature_leg" (persistence/serializers.py's
    # _lazy_watcher_to_pending_leg) so a leg still WAITING on its momentum
    # trigger shows up in /fast-forward_2 at all, same as a time-gated
    # (EntryIndicators pending) leg already does — user's explicit report:
    # a strategy with 2 plain legs + 2 LegMomentum legs only ever showed
    # the 2 plain ones; the 2 armed-but-not-yet-entered ones were entirely
    # invisible until they actually triggered.
    armed_at: str = ""
