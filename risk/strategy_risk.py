"""
strategy_risk.py
───────────────────
StrategyRiskController — master-doc §50. Checks TWO independent things and
returns whichever fires first:

  1. Cycle-adjusted overall SL/Target (existing overall_risk.py logic,
     UNCHANGED — trading_core.py's resolve_overall_cycle_value/
     check_overall_sl_hit/check_overall_target_hit, already tested in
     Phase 3). Preserved exactly, not touched.
  2. Lock/LockAndTrail/TrailStopLoss floor breach — NEW, via the shared
     risk/evaluator.py, built from strategy_cfg's LockAndTrail sub-config
     the same way position_manager.py's parse_lock_and_trail already reads it.

Both checks matter: (1) is the old reentry-cycle-aware SL/Target the system
already has; (2) is the master-doc's newly-unified trailing modes, which the
old system never wired into an actual exit trigger for the overall/strategy
scope (only the broker-level check_broker_sl_target used LockAndTrail
end-to-end).
"""

from __future__ import annotations

from risk import overall_risk
from risk.evaluator import EvalResult, PeakWindow, evaluate_risk
from risk.models import RiskConfig, RiskDecision, TrailingMode
from runtime.strategy_runtime import StrategyRuntime


def build_strategy_risk_config(strategy_cfg: dict) -> RiskConfig:
    osl_type, osl_val = overall_risk.parse_overall_sl(strategy_cfg)
    otgt_type, otgt_val = overall_risk.parse_overall_tgt(strategy_cfg)

    lock_cfg = strategy_cfg.get("LockAndTrail") or {}
    lock_type = str(lock_cfg.get("Type") or "")
    lock_val = lock_cfg.get("Value") or {}

    trailing_mode = TrailingMode.NONE
    if "LockAndTrail" in lock_type:
        trailing_mode = TrailingMode.LOCK_AND_TRAIL
    elif "Lock" in lock_type:
        trailing_mode = TrailingMode.LOCK

    return RiskConfig(
        stop_loss_enabled=(osl_type == "MTM"), stop_loss_amount=osl_val,
        target_enabled=(otgt_type == "MTM"), target_amount=otgt_val,
        trailing_mode=trailing_mode,
        activation_profit=float(lock_val.get("ProfitReaches") or 0),
        lock_profit=float(lock_val.get("LockProfit") or 0),
        profit_step=float(lock_val.get("IncreaseInProfitBy") or 0),
        trail_by=float(lock_val.get("TrailProfitBy") or 0),
    )


# Peak confirmation per strategy (see evaluator.PeakWindow) — in memory only.
_PEAK_WINDOWS: dict[str, PeakWindow] = {}


def forget(strategy_id: str) -> None:
    """Drop a finished strategy's peak window (called when it leaves the router)."""
    _PEAK_WINDOWS.pop(strategy_id, None)


def evaluate_strategy_risk(strategy: StrategyRuntime) -> EvalResult:
    # Peak only from MTM that has held ~3s — a half-repriced multi-leg tick
    # must not lift the peak (OverallTrailSL / Lock read it).
    candidate = min(_PEAK_WINDOWS.setdefault(strategy.strategy_id, PeakWindow()).candidate(strategy.mtm), strategy.mtm)
    # peak first, so OverallTrailSL tightens from this tick's MTM too.
    strategy.peak_mtm = max(strategy.peak_mtm, candidate)
    overall_risk.apply_overall_trailing(strategy)
    if overall_risk.check_overall_sl_hit(strategy):
        return EvalResult(RiskDecision.STOP_LOSS_HIT, strategy.current_overall_sl_threshold, max(strategy.peak_mtm, strategy.mtm))
    if overall_risk.check_overall_target_hit(strategy):
        return EvalResult(RiskDecision.TARGET_HIT, new_peak_pnl=max(strategy.peak_mtm, strategy.mtm))

    config = build_strategy_risk_config(strategy.strategy_cfg)
    result = evaluate_risk(strategy.mtm, config, strategy.peak_mtm, strategy.current_floor, strategy.trailing_activated, peak_candidate=candidate)
    strategy.peak_mtm = result.new_peak_pnl
    strategy.current_floor = result.new_floor
    strategy.trailing_activated = result.trailing_activated
    return result
