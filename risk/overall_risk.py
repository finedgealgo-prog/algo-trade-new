"""
overall_risk.py
──────────────────
Overall Strategy SL/Target + Overall Trail SL — master-doc §14, exact port of
shared/features/position_manager.py's parse_overall_sl/parse_overall_tgt/
parse_overall_trail_sl/update_overall_trail_sl combined with trading_core.py's
check_overall_sl_hit/check_overall_target_hit/compute_next_overall_trail_sl
wrappers (cycle-adjusted threshold + dynamic trail threshold).

Only OverallSL.Type == "MTM" / OverallTgt.Type == "MTM" is wired here —
PremiumPercentage parsing is ported (parse_overall_sl/parse_overall_tgt
recognize it) but its hit-check isn't, matching trading_core.py's own
check_overall_sl/check_overall_tgt (which only implement the MTM branch too).
"""

from __future__ import annotations

from runtime.strategy_runtime import StrategyRuntime


def parse_overall_sl(strategy_cfg: dict) -> tuple[str, float]:
    cfg = strategy_cfg.get("OverallSL") or {}
    stype = str(cfg.get("Type") or "")
    val = float(cfg.get("Value") or 0)
    if "None" in stype or val <= 0:
        return "None", 0.0
    if "MTM" in stype:
        return "MTM", val
    if "PremiumPercentage" in stype or "Premium" in stype:
        return "PremiumPercentage", val
    return "None", 0.0


def parse_overall_tgt(strategy_cfg: dict) -> tuple[str, float]:
    cfg = strategy_cfg.get("OverallTgt") or {}
    stype = str(cfg.get("Type") or "")
    val = float(cfg.get("Value") or 0)
    if "None" in stype or val <= 0:
        return "None", 0.0
    if "MTM" in stype:
        return "MTM", val
    if "PremiumPercentage" in stype or "Premium" in stype:
        return "PremiumPercentage", val
    return "None", 0.0


def parse_overall_trail_sl(strategy_cfg: dict) -> tuple[str, float, float]:
    cfg = strategy_cfg.get("OverallTrailSL") or {}
    trail_type = str(cfg.get("Type") or "")
    if "None" in trail_type:
        return "None", 0.0, 0.0
    val = cfg.get("Value") or {}
    for_every = float(val.get("TrailForEvery") or 0)
    trail_by = float(val.get("TrailBy") or 0)
    if for_every <= 0 or trail_by <= 0:
        return "None", 0.0, 0.0
    return trail_type, for_every, trail_by


def update_overall_trail_sl(for_every: float, trail_by: float, initial_sl_value: float, peak_mtm: float) -> float:
    if for_every <= 0 or peak_mtm <= 0:
        return initial_sl_value
    steps = int(peak_mtm / for_every)
    return max(0.0, round(initial_sl_value - steps * trail_by, 2))


def resolve_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    """base_value × (completed_reentries + 1) — prevents cascading losses
    across reentry cycles (cycle 0 → base, cycle 1 → 2×base, ...)."""
    normalized = float(base_value or 0)
    if normalized <= 0:
        return 0.0
    cycle_index = max(0, int(completed_reentries or 0)) + 1
    return round(normalized * cycle_index, 2)


def _trail_cfg_source(strategy: StrategyRuntime) -> dict:
    """OverallTrailSL lives on the full config — strategy_cfg is trimmed to
    the risk keys and older checkpoints' trimmed copy didn't include it."""
    if strategy.strategy_cfg.get("OverallTrailSL"):
        return strategy.strategy_cfg
    return strategy.full_strategy_cfg or {}


def effective_overall_sl(strategy: StrategyRuntime) -> float | None:
    """Current overall SL amount (₹ loss that triggers exit), or None when no
    OverallSL is configured: the cycle-adjusted base (resolve_overall_cycle_
    value), tightened by OverallTrailSL — every TrailForEvery of peak MTM
    moves it TrailBy closer, down to 0 (break-even). Computed from peak_mtm
    each time, so it only ever tightens within a cycle."""
    osl_type, osl_val = parse_overall_sl(strategy.strategy_cfg)
    if osl_type == "None" or not osl_val:
        return None
    effective = resolve_overall_cycle_value(osl_val, strategy.sl_reentry_done)
    trail_type, for_every, trail_by = parse_overall_trail_sl(_trail_cfg_source(strategy))
    if trail_type != "None" and strategy.peak_mtm > 0:
        effective = min(effective, update_overall_trail_sl(for_every, trail_by, effective, strategy.peak_mtm))
    return effective


def check_overall_sl_hit(strategy: StrategyRuntime) -> bool:
    effective_sl = effective_overall_sl(strategy)
    if effective_sl is None:
        return False
    return strategy.mtm <= -effective_sl


def check_overall_target_hit(strategy: StrategyRuntime) -> bool:
    otgt_type, otgt_val = parse_overall_tgt(strategy.strategy_cfg)
    if otgt_type == "None" or not otgt_val:
        return False
    effective_tgt = resolve_overall_cycle_value(otgt_val, strategy.tgt_reentry_done)
    return effective_tgt > 0 and strategy.mtm >= effective_tgt


def apply_overall_trailing(strategy: StrategyRuntime) -> bool:
    """Refresh current_overall_sl_threshold (display/checkpoint copy of
    effective_overall_sl). Returns True if it changed."""
    effective = effective_overall_sl(strategy)
    new_threshold = effective if effective is not None else 0.0
    if new_threshold != strategy.current_overall_sl_threshold:
        strategy.current_overall_sl_threshold = new_threshold
        return True
    return False
