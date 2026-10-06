"""
lazy_engine.py
────────────────
Lazy Leg (simple momentum) — exact port of shared/features/execution_socket.py's
_resolve_simple_momentum_target / _is_simple_momentum_triggered (lines
3029-3050 there), which are the REAL live-tick formulas — not
shared/features/lazy_leg.py, which is a backtest-only historical-replay
module (idx.get_spot(day, time) over a full recorded day) with no live
tick-watcher equivalent at all.

Momentum direction/kind is read the same way the old code does: substring
checks on the raw Type string ('Percentage' vs points, 'Down' vs up) — kept
as substring checks rather than a parsed enum so a config value from Mongo
needs zero translation to work here.

master-doc §32: once armed, the strike/token/reference price are frozen —
arm_lazy_watcher() is the ONLY place that computes trigger_price; nothing
in check_lazy_trigger() ever re-derives it.
"""

from __future__ import annotations

from datetime import datetime, timezone

from runtime.lazy_runtime import LazyRuntime


def resolve_momentum_target(base_price: float, momentum_type: str, momentum_value: float) -> float:
    base = float(base_price or 0)
    value = float(momentum_value or 0)
    if base <= 0 or value <= 0:
        return 0.0
    if "Percentage" in str(momentum_type or ""):
        delta = base * (value / 100.0)
    else:
        delta = value
    if "Down" in str(momentum_type or ""):
        return round(base - delta, 2)
    return round(base + delta, 2)


def is_momentum_triggered(current_price: float, target_price: float, momentum_type: str) -> bool:
    current = float(current_price or 0)
    target = float(target_price or 0)
    if current <= 0 or target <= 0:
        return False
    if "Down" in str(momentum_type or ""):
        return current <= target
    return current >= target


def arm_lazy_watcher(
    lazy_id: str, strategy_id: str, parent_leg_id: str,
    token: str, strike: float, option_type: str,
    momentum_type: str, momentum_value: float, reference_ltp: float,
    qty: int = 0, is_sell: bool = False, leg_cfg: dict | None = None,
    broker_scope_id: str = "", user_id: str = "", expiry: str = "",
) -> LazyRuntime | None:
    """Freeze strike/token/reference price and compute the trigger price —
    master-doc §32. Returns None if the trigger price can't be resolved
    (bad config or zero reference price), matching the old code's SKIP
    behavior rather than arming a watcher that can never fire.

    qty/is_sell/leg_cfg/broker_scope_id/user_id are frozen here too (not
    just strike/token) — see LazyRuntime's own docstring for why the
    eventual trigger-time entry can't re-derive these from parent_leg_id."""
    trigger_price = resolve_momentum_target(reference_ltp, momentum_type, momentum_value)
    if trigger_price <= 0:
        return None
    return LazyRuntime(
        lazy_id=lazy_id, strategy_id=strategy_id, parent_leg_id=parent_leg_id,
        token=token, strike=strike, option_type=option_type,
        momentum_type=momentum_type, reference_ltp=reference_ltp, trigger_price=trigger_price,
        qty=qty, is_sell=is_sell, leg_cfg=leg_cfg or {}, broker_scope_id=broker_scope_id, user_id=user_id, expiry=expiry,
        status="ARMED", armed_at=datetime.now(timezone.utc).isoformat(),
    )


def check_lazy_trigger(lazy: LazyRuntime, current_price: float) -> bool:
    """Returns True (and does NOT mutate lazy.status — caller decides,
    same duplicate-trigger-safe pattern as sl_tp_engine) if the frozen
    trigger_price has been crossed."""
    if lazy.status != "ARMED":
        return False
    return is_momentum_triggered(current_price, lazy.trigger_price, lazy.momentum_type)
