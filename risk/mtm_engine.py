"""
mtm_engine.py
───────────────
Incremental P&L — master-doc §13. Formula ported from trading_core.py's
compute_strategy_mtm (SELL: (entry-current)*qty, BUY: (current-entry)*qty,
qty already lots*lot_size). The "incremental" part is the point: only the
CHANGED leg's new pnl is computed here, and the delta is folded straight into
StrategyRuntime.mtm — never a full re-sum across every leg of the strategy.

Crypto (Delta) legs: prices are Delta's raw per-1-coin quote, so the leg's
pnl_multiplier (contract_value × USD→INR, set at registration) converts the
raw difference into ₹ — the same unit every OverallSL/OverallTgt/broker
SL/Target amount is configured in.
"""

from __future__ import annotations

from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime


def compute_leg_pnl(entry_price: float, current_price: float, qty: int, is_sell: bool, multiplier: float = 1.0) -> float:
    return ((entry_price - current_price) if is_sell else (current_price - entry_price)) * qty * multiplier


def apply_leg_pnl_delta(leg: LegRuntime) -> float:
    """Recompute this leg's pnl from its current_price, update leg.current_pnl
    in place, and return the delta. Unified-risk-engine master-doc §6: the
    SAME delta this returns must be reused for every parent scope (strategy,
    broker) — never recomputed independently per scope."""
    new_pnl = compute_leg_pnl(leg.entry_price, leg.current_price, leg.qty, leg.is_sell, leg.pnl_multiplier)
    delta = new_pnl - leg.current_pnl
    leg.current_pnl = new_pnl
    return delta


def apply_leg_pnl(leg: LegRuntime, strategy: StrategyRuntime) -> float:
    """Single-scope convenience wrapper (Phase 3, predates the unified
    strategy+broker risk engine) — computes the delta and folds it into
    strategy.mtm/peak_mtm only. token_router's tick dispatch now calls
    apply_leg_pnl_delta directly and propagates the same delta to every
    affected scope (strategy AND broker) itself; kept here for existing
    single-scope callers/tests."""
    delta = apply_leg_pnl_delta(leg)
    if delta:
        strategy.mtm += delta
        if strategy.mtm > strategy.peak_mtm:
            strategy.peak_mtm = strategy.mtm
    return strategy.mtm
