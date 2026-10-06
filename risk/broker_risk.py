"""
broker_risk.py
─────────────────
BrokerRiskController — master-doc §9/§50: same RiskConfig/evaluator as
strategy, different scope. `broker.mtm` is DAY NET PNL (realized + open),
matching the old shared/features/trading_core.py's compute_broker_group_mtm
semantics (§37) — token_router is responsible for keeping broker.mtm as
realized_pnl + sum of its legs' unrealized pnl; this module only evaluates
risk against whatever value is already there.

Config source: same shape as strategy's LockAndTrail (StopLoss/Target/
LockAndTrail), read off BrokerRuntime.config (populated from Mongo
`algo_borker_stoploss_settings` — same collection the old
get_broker_sl_settings reads — at broker registration/recovery time, not
re-read on the hot path).
"""

from __future__ import annotations

from risk.evaluator import EvalResult, PeakWindow, evaluate_risk
from risk.models import RiskConfig, TrailingMode
from runtime.broker_runtime import BrokerRuntime


def _positive(value) -> float:
    try:
        return max(0.0, float(value or 0))
    except (TypeError, ValueError):
        return 0.0


def build_broker_risk_config(settings_doc: dict) -> RiskConfig:
    """Accepts both config shapes:

    - the one FastForward2.tsx actually saves (old algo_borker_stoploss_settings
      shape, evaluated by trading_core.check_broker_sl_target):
        LockAndTrail  = {InstrumentMove: activate-at profit, StopLossMove: locked floor}
        OverallTrailSL = {InstrumentMove: every X, StopLossMove: by Y}
          - with LockAndTrail  -> floor trails up Y per X above activation
          - without LockAndTrail -> StopLoss shrinks Y per X of peak profit
    - the strategy-style {Type, Value: {ProfitReaches, LockProfit,
      IncreaseInProfitBy, TrailProfitBy}} shape.
    """
    sl = _positive(settings_doc.get("StopLoss"))
    tgt = _positive(settings_doc.get("Target"))

    lock_cfg = settings_doc.get("LockAndTrail") or {}
    trail_cfg = settings_doc.get("OverallTrailSL") or {}
    trail_every = _positive(trail_cfg.get("InstrumentMove"))
    trail_by = _positive(trail_cfg.get("StopLossMove"))
    has_trail = trail_every > 0 and trail_by > 0

    config = RiskConfig(
        stop_loss_enabled=sl > 0, stop_loss_amount=sl,
        target_enabled=tgt > 0, target_amount=tgt,
    )

    if "InstrumentMove" in lock_cfg or "StopLossMove" in lock_cfg:
        activation = _positive(lock_cfg.get("InstrumentMove"))
        lock_profit = _positive(lock_cfg.get("StopLossMove"))
        if activation > 0 and lock_profit > 0:
            config.trailing_mode = TrailingMode.LOCK_AND_TRAIL if has_trail else TrailingMode.LOCK
            config.activation_profit = activation
            config.lock_profit = lock_profit
            if has_trail:
                config.profit_step = trail_every
                config.trail_by = trail_by
            # With Lock / Lock & Trail on, the broker Target is display-only
            # (the strip shows it until the lock activates) — profit is
            # protected by the locked floor, not cut off at the Target.
            config.target_enabled = False
        return config

    lock_type = str(lock_cfg.get("Type") or "")
    lock_val = lock_cfg.get("Value") or {}
    if "LockAndTrail" in lock_type:
        config.trailing_mode = TrailingMode.LOCK_AND_TRAIL
    elif "Lock" in lock_type and "None" not in lock_type:
        config.trailing_mode = TrailingMode.LOCK
    if config.trailing_mode != TrailingMode.NONE:
        config.activation_profit = _positive(lock_val.get("ProfitReaches"))
        config.lock_profit = _positive(lock_val.get("LockProfit"))
        config.profit_step = _positive(lock_val.get("IncreaseInProfitBy"))
        config.trail_by = _positive(lock_val.get("TrailProfitBy"))
    elif has_trail and sl > 0:
        config.stop_loss_trail_every = trail_every
        config.stop_loss_trail_by = trail_by
    return config


def settings_active(settings_doc: dict | None) -> bool:
    """Old system's `status == 1` flag (0 once a broker SL/Target fired and
    disable_broker_sl_settings ran); a doc without the field counts as active."""
    if not settings_doc:
        return False
    return settings_doc.get("status", 1) == 1


def risk_state(broker: BrokerRuntime) -> dict:
    """The broker's live lock/trail state in the old algo_borker_stoploss_
    settings field names FastForward2's broker strip reads:
      lock_activated     — profit has reached ProfitReaches (InstrumentMove)
      current_lock_floor — locked profit; only ever moves up
      lock_peak_mtm      — highest risk MTM seen (running strategies' MTM)
      effective_sl       — StopLoss after any standalone trail tightening"""
    from risk.evaluator import effective_stop_loss

    cfg = broker.config
    return {
        "lock_activated": bool(broker.trailing_activated),
        "current_lock_floor": round(float(broker.current_floor), 2) if broker.trailing_activated else 0.0,
        "lock_peak_mtm": round(float(broker.peak_pnl), 2),
        "effective_sl": effective_stop_loss(cfg, broker.peak_pnl) if cfg.stop_loss_enabled else None,
    }


def running_mtm(router, broker_scope_id: str) -> float:
    """The MTM broker-level SL/Target/Lock/Trail act on: the combined MTM of
    the strategies RUNNING under this broker right now. Strategies already
    squared off earlier today don't count (their losses made a fresh SL fire
    instantly), and profit already on the running positions counts the
    moment settings are saved (a Lock set at +200 locks immediately)."""
    return sum(
        s.mtm for s in router.strategies.values()
        if s.broker_scope_id == broker_scope_id and s.status != "EXITED"
    )


# Per-broker peak confirmation windows (in-memory only; not checkpointed —
# a restart simply starts a fresh 3s window).
_PEAK_WINDOWS: dict[str, PeakWindow] = {}


def reset_peak_window(broker_scope_id: str, seed: float | None = None) -> None:
    """New risk cycle (settings saved / exit): forget old readings; `seed`
    marks the current MTM as already-held so a save locks it instantly."""
    window = _PEAK_WINDOWS.setdefault(broker_scope_id, PeakWindow())
    window.reset()
    if seed is not None:
        window.candidate(seed)


def evaluate_broker_risk(broker: BrokerRuntime, risk_mtm: float | None = None) -> EvalResult:
    """risk_mtm: running strategies' MTM (running_mtm); defaults to
    broker.mtm for callers without a router. peak_pnl is in the same terms."""
    mtm = broker.mtm if risk_mtm is None else risk_mtm
    window = _PEAK_WINDOWS.setdefault(broker.broker_scope_id, PeakWindow())
    result = evaluate_risk(mtm, broker.config, broker.peak_pnl, broker.current_floor, broker.trailing_activated,
                           peak_candidate=window.candidate(mtm))
    broker.peak_pnl = result.new_peak_pnl
    broker.current_floor = result.new_floor
    broker.trailing_activated = result.trailing_activated
    return result
