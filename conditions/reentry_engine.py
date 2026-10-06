"""
reentry_engine.py
────────────────────
Immediate (RE-ASAP) and LikeOriginal (RE-MOMENTUM) re-entry — master-doc
§38/§39/§44/§45.

Counter design note: the old system (execution_socket.py's
`_reentry_budget_remaining`) uses ONE shared `reentry_count_remaining` field
per leg lineage for Immediate/AtCost/LikeOriginal alike — a single counter
regardless of which reentry TYPE fired. Master-doc §44 explicitly asks for
SEPARATE counters (reentry_max/used, recost_max/used, lazy_max/used)
instead — algo-2_0 follows the doc here rather than the old field, since
it's a real design improvement the doc calls for by name, not a business-
rule change: Immediate/LikeOriginal budget lives on LegRuntime.reentry_*,
AtCost's separately on .recost_*, Lazy's on .lazy_*. Ported/preserved
exactly from the old code: the budget is consumed at TRIGGER time (when
the re-entry is queued), not only after a confirmed fill — matching
`_handle_reentry`'s `new_leg['reentry_count_remaining'] = count - 1` done
immediately on queueing, before any broker confirmation.

What this module does NOT do yet: resolve a fresh strike for the new leg.
Immediate and LikeOriginal both re-select (master-doc §39 — unlike Lazy's
frozen strike, "a fresh Re-entry may select current strike again"), which
needs Phase 5's delta/premium/ATM selector. Both functions below return a
ReentrySpec describing WHAT should happen (kind, budget consumed, and for
LikeOriginal the momentum config to arm) — Phase 5/6 wiring turns that into
an actual new LegRuntime once strike resolution exists. This is a real,
explicitly-flagged gap, not a stand-in implementation of the selection step.
"""

from __future__ import annotations

from dataclasses import dataclass

from runtime.leg_runtime import LegRuntime


@dataclass
class ReentrySpec:
    kind: str  # "IMMEDIATE" | "LIKE_ORIGINAL"
    parent_leg_id: str
    strategy_id: str
    reentry_used: int  # budget already consumed — carry onto the new leg
    reentry_max: int
    momentum_type: str = ""  # LikeOriginal only — the leg's own LegMomentum config
    momentum_value: float = 0.0


def reentry_budget_remaining(used: int, max_count: int) -> int:
    return max(0, max_count - used)


def is_reentry_eligible(leg: LegRuntime) -> bool:
    return reentry_budget_remaining(leg.reentry_used, leg.reentry_max) > 0


def trigger_immediate_reentry(leg: LegRuntime) -> ReentrySpec | None:
    """master-doc §38 RE-ASAP: enter ASAP at a fresh selection, no waiting
    condition. Returns None (and leaves leg.reentry_used untouched) if the
    budget is exhausted."""
    if not is_reentry_eligible(leg):
        return None
    leg.reentry_used += 1  # consumed at trigger time, matching the old system
    return ReentrySpec(
        kind="IMMEDIATE", parent_leg_id=leg.leg_id, strategy_id=leg.strategy_id,
        reentry_used=leg.reentry_used, reentry_max=leg.reentry_max,
    )


def trigger_like_original_reentry(leg: LegRuntime, momentum_type: str, momentum_value: float) -> ReentrySpec | None:
    """master-doc §38 RE-MOMENTUM: fresh selection, but wait for the leg's
    OWN configured momentum condition before entering (not immediate).
    Once Phase 5 resolves the new strike + a reference LTP, the momentum
    wait itself needs no new engine code — conditions.lazy_engine.
    arm_lazy_watcher/check_lazy_trigger already implement this exact
    formula (see its docstring); LikeOriginal just arms one with the
    original leg's own momentum config instead of a lazy-leg's."""
    if not is_reentry_eligible(leg):
        return None
    leg.reentry_used += 1
    return ReentrySpec(
        kind="LIKE_ORIGINAL", parent_leg_id=leg.leg_id, strategy_id=leg.strategy_id,
        reentry_used=leg.reentry_used, reentry_max=leg.reentry_max,
        momentum_type=momentum_type, momentum_value=momentum_value,
    )
