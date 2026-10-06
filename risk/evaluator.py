"""
evaluator.py
──────────────
Master-doc §11/§50: ONE generic RiskEvaluator shared by Strategy and Broker
scope — "avoid duplicated formulas." Ported exactly from
shared/features/position_manager.py's check_lock_and_trail (Lock/LockAndTrail:
activation-gated floor, steps = floor((peak-activation)/profit_step), floor =
lock_profit + steps*trail_by) plus the plain SL/Target comparisons already
used identically by both strategy overall-risk (trading_core.py's
check_overall_sl_hit/check_overall_target_hit) and broker-level risk
(check_broker_sl_target) in the old code.

TRAIL_STOP_LOSS (master-doc §17) is a distinct mode from LOCK_AND_TRAIL: no
activation threshold — floor = peak_pnl - trailing_distance as soon as any
profit exists. Same peak-based shape as the old system's OverallTrailSL step
formula, generalized to a flat distance per the doc's explicit distinction.

Does NOT decide what to exit (master-doc §11) — only returns a decision +
threshold; scope controllers (strategy_risk.py/broker_risk.py) act on it.
"""

from __future__ import annotations

from dataclasses import dataclass

from risk.models import RiskConfig, RiskDecision, TrailingMode


@dataclass
class EvalResult:
    decision: RiskDecision
    threshold: float = 0.0
    new_peak_pnl: float = 0.0
    new_floor: float = 0.0
    trailing_activated: bool = False


def effective_stop_loss(config: RiskConfig, peak_pnl: float) -> float:
    """SL amount after the standalone broker trail (stop_loss_trail_every /
    stop_loss_trail_by) has tightened it for `peak_pnl`; the plain SL
    amount when no such trail is configured."""
    amount = config.stop_loss_amount
    if config.stop_loss_trail_every > 0 and config.stop_loss_trail_by > 0 and peak_pnl >= config.stop_loss_trail_every:
        steps = int(peak_pnl / config.stop_loss_trail_every)
        amount = round(amount - steps * config.stop_loss_trail_by, 2)
    return amount


import time

# Monotonic clock for PeakWindow — a module attribute so tests can drive time.
clock = time.monotonic


class PeakWindow:
    """Confirms a new profit peak once it has held for `seconds` (1s, so the
    lock/trail floor follows profit within about a second).

    Each contract's price arrives as its own tick (Delta: ~1-2s apart per
    contract, out of phase), and risk is evaluated after every tick. While
    only some legs of a multi-leg position have repriced, the MTM is a
    half-updated figure — e.g. a short straddle in a fast move showed +131
    for one tick (put repriced, call not yet) and -84 a second later. Feeding
    that spike into the monotonic peak activated the Lock and lifted the
    floor on a profit that never existed, then the very next tick "hit" it.

    candidate(mtm) returns the LOWEST MTM seen over the last `seconds`
    (current included, plus one anchor reading from just before if it is
    under 3s old) — the profit level that has actually held — and the peak
    is raised from that. Floor/SL breaches still use the live MTM. (A 3s
    window lagged the lock 10-30s with choppy ticks.)"""

    def __init__(self, seconds: float = 1.0) -> None:
        self.seconds = seconds
        self._points: list[tuple[float, float]] = []

    def candidate(self, mtm: float, now: float | None = None) -> float:
        now = clock() if now is None else now
        self._points.append((now, mtm))
        cutoff = now - self.seconds
        # Keep one point at/before the cutoff as an anchor, so a spike right
        # after a quiet gap (no ticks) still has to hold against the last
        # known level instead of being compared only with itself.
        while len(self._points) > 2 and self._points[1][0] <= cutoff:
            self._points.pop(0)
        # ...but only a recent anchor (legs normally tick every 1-2s); after
        # a long silence the old level says nothing about the current fill.
        if len(self._points) > 1 and self._points[0][0] < now - 3.0:
            self._points.pop(0)
        return min(v for _, v in self._points)

    def reset(self) -> None:
        self._points.clear()


def evaluate_risk(
    current_pnl: float, config: RiskConfig,
    peak_pnl: float = 0.0, current_floor: float = 0.0, trailing_activated: bool = False,
    peak_candidate: float | None = None,
) -> EvalResult:
    """peak_candidate: the MTM allowed to raise the peak (see PeakWindow);
    defaults to current_pnl. Breach checks always use current_pnl."""
    new_peak = max(peak_pnl, current_pnl if peak_candidate is None else min(peak_candidate, current_pnl))  # §16: monotonic upward only

    # SL/Target checked first — matches the old code's check ordering (SL,
    # then LockAndTrail) in check_broker_sl_target.
    stop_loss_amount = effective_stop_loss(config, new_peak)
    if config.stop_loss_enabled and config.stop_loss_amount > 0 and current_pnl <= -stop_loss_amount:
        return EvalResult(RiskDecision.STOP_LOSS_HIT, -stop_loss_amount, new_peak, current_floor, trailing_activated)

    if config.target_enabled and config.target_amount > 0 and current_pnl >= config.target_amount:
        return EvalResult(RiskDecision.TARGET_HIT, config.target_amount, new_peak, current_floor, trailing_activated)

    if config.trailing_mode == TrailingMode.LOCK:
        if new_peak < config.activation_profit:
            return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak, new_floor=current_floor)
        floor = config.lock_profit
        if current_pnl <= floor:
            return EvalResult(RiskDecision.LOCK_HIT, floor, new_peak, floor, True)
        return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak, new_floor=floor, trailing_activated=True)

    if config.trailing_mode == TrailingMode.LOCK_AND_TRAIL:
        if new_peak < config.activation_profit:
            return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak)
        if config.profit_step > 0:
            extra = max(0.0, new_peak - config.activation_profit)
            steps = int(extra / config.profit_step)
            floor = config.lock_profit + steps * config.trail_by
        else:
            floor = config.lock_profit
        floor = max(floor, current_floor)  # §16: never move backward
        if current_pnl <= floor:
            return EvalResult(RiskDecision.LOCK_HIT, floor, new_peak, floor, True)
        return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak, new_floor=floor, trailing_activated=True)

    if config.trailing_mode == TrailingMode.TRAIL_STOP_LOSS:
        if new_peak <= 0 or config.trailing_distance <= 0:
            return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak)
        floor = max(new_peak - config.trailing_distance, current_floor)
        if current_pnl <= floor:
            return EvalResult(RiskDecision.TRAILING_HIT, floor, new_peak, floor, True)
        return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak, new_floor=floor, trailing_activated=True)

    return EvalResult(RiskDecision.NONE, new_peak_pnl=new_peak, new_floor=current_floor, trailing_activated=trailing_activated)
