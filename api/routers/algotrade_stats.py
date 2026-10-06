"""
routers/algotrade_stats.py
─────────────────────────────
Minimal algo-2_0 equivalent of the old system's wallet/credit-limit/
subscription-plan stack (SubscriptionContext.tsx, AlgoTradeStatsBar.tsx) —
NOT a port of that system. The old stack is a full commercial billing
product (admin-editable plan catalog, Razorpay wallet purchases, per-tier
expiring credit batches) — unrelated to the trading engine and wildly
disproportionate to replicate here.

algo-2_0 is virtual-only (VirtualBrokerAdapter, no real broker calls, no
real money) — there is no real resource being metered, so "access" is
simply unlimited. This route exists only so AlgoTradeStatsBar2.tsx (and
anything else checking "can this user activate a strategy") gets a real
answer from algo-2_0 instead of calling the old backend's billing API
(user's explicit instruction: nothing on this page should depend on the
old algo.trade backend).

If a real credits/plan system is ever wanted for algo-2_0 specifically,
that's its own separate, scoped feature — not bundled into this rewrite.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/algo/algotrade", tags=["algotrade-stats"])

_UNLIMITED = 999_999


@router.get("/stats")
def algotrade_stats() -> dict:
    from main import token_router

    live_count = sum(1 for s in token_router.strategies.values() if s.status != "EXITED")
    tiers = ["live_trade", "fast_forward", "forward_test"]
    tier_balances: dict[str, int] = {}
    for tier in tiers:
        tier_balances[tier] = _UNLIMITED
        tier_balances[f"balance_{tier}"] = max(0, _UNLIMITED - live_count)
    return {"wallet_balance": _UNLIMITED, "tier_balances": tier_balances}


@router.get("/wallet/balance")
def wallet_balance() -> dict:
    """Path-compatible with the old system's GET /algo/algotrade/wallet/balance
    ({"balance": <int>}) — same "unlimited" reasoning as /stats above, just
    under the old route shape for callers (e.g. a page still on old
    components) that hit this path specifically instead of /stats."""
    return {"balance": _UNLIMITED}


@router.get("/subscription/my-tier-balances")
def my_tier_balances() -> dict:
    """Path-compatible with the old system's GET
    /algo/algotrade/subscription/my-tier-balances — same fields
    (tier + balance_<tier> per tier), same "unlimited" values as /stats
    above, just flattened (no wallet_balance, no tier_balances wrapper) to
    match the old route's exact response shape."""
    from main import token_router

    live_count = sum(1 for s in token_router.strategies.values() if s.status != "EXITED")
    result: dict[str, int] = {}
    for tier in ("live_trade", "fast_forward", "forward_test"):
        result[tier] = _UNLIMITED
        result[f"balance_{tier}"] = max(0, _UNLIMITED - live_count)
    return result


@router.get("/subscription/deploy-status/{activation_mode}")
def deploy_status(activation_mode: str) -> dict:
    """Path-compatible with the old system's GET
    /algo/algotrade/subscription/deploy-status/{activation_mode} —
    PortfolioActivation.tsx calls this right before /portfolio/prepare-
    activation to gate on deploy limits. algo-2_0 has no such tier gating
    (virtual-only, same reasoning as /stats above) — `tier: null` here
    makes that page's `if (deployStatus.tier && ...)` check short-circuit
    before it even looks at tier_balance/currently_running."""
    return {"tier": None, "tier_balance": 0, "currently_running": 0}
