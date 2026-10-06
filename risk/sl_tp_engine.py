"""
sl_tp_engine.py
──────────────────
Leg-level SL / Target check — exact port of shared/features/position_manager.py's
calc_sl_price/calc_tp_price/is_sl_hit/is_tp_hit + trading_core.py's check_leg_sl/
check_leg_target wrappers (same formulas, same direction rules), operating on
LegRuntime instead of a Mongo leg document.

SELL: SL is ABOVE entry (loss if price rises), TP is BELOW entry.
BUY:  SL is BELOW entry (loss if price falls), TP is ABOVE entry.
Underlying(spot)-based SL/TP compares against spot instead of premium, and
flips direction for PE legs (a short PE loses when spot FALLS, opposite of a
short CE) — see underlying_direction_is_sell.
"""

from __future__ import annotations

from runtime.leg_runtime import LegRuntime


def is_underlying_config(config: dict) -> bool:
    return "Underlying" in str((config or {}).get("Type") or "")


def underlying_direction_is_sell(is_sell: bool, option_type: str | None) -> bool:
    is_put = "PE" in str(option_type or "").upper()
    return (not is_sell) if is_put else is_sell


def calc_sl_price(
    entry_price: float, is_sell: bool, sl_config: dict,
    entry_spot: float | None = None, option_type: str | None = None,
) -> float | None:
    if not sl_config:
        return None
    sl_type = str(sl_config.get("Type") or "")
    sl_value = float(sl_config.get("Value") or 0)
    if "None" in sl_type or sl_value <= 0:
        return None
    if is_underlying_config(sl_config):
        base = float(entry_spot or 0)
        if base <= 0:
            return None
        is_sell = underlying_direction_is_sell(is_sell, option_type)
    else:
        base = entry_price
    if "Percentage" in sl_type:
        mult = (1 + sl_value / 100) if is_sell else (1 - sl_value / 100)
        return round(base * mult, 2)
    if "Points" in sl_type:
        return round(base + sl_value if is_sell else base - sl_value, 2)
    return None


def calc_tp_price(
    entry_price: float, is_sell: bool, tp_config: dict,
    entry_spot: float | None = None, option_type: str | None = None,
) -> float | None:
    if not tp_config:
        return None
    tp_type = str(tp_config.get("Type") or "")
    tp_value = float(tp_config.get("Value") or 0)
    if "None" in tp_type or tp_value <= 0:
        return None
    if is_underlying_config(tp_config):
        base = float(entry_spot or 0)
        if base <= 0:
            return None
        is_sell = underlying_direction_is_sell(is_sell, option_type)
    else:
        base = entry_price
    if "Percentage" in tp_type:
        mult = (1 - tp_value / 100) if is_sell else (1 + tp_value / 100)
        return round(base * mult, 2)
    if "Points" in tp_type:
        return round(base - tp_value if is_sell else base + tp_value, 2)
    return None


def is_sl_hit(current_price: float, sl_price: float | None, is_sell: bool) -> bool:
    if sl_price is None:
        return False
    return current_price >= sl_price if is_sell else current_price <= sl_price


def is_tp_hit(current_price: float, tp_price: float | None, is_sell: bool) -> bool:
    if tp_price is None:
        return False
    return current_price <= tp_price if is_sell else current_price >= tp_price


def initialize_sl_tp(leg: LegRuntime) -> None:
    """Compute SL/TP ONCE at entry and store them on the leg — master-doc
    principle (§9/§10): SL/TP is set at entry, not recomputed every tick.
    Must be called right after a LegRuntime is constructed (every entry
    site — strategy_activation.py, leg_followup.py's fresh re-entries,
    scheduled_entry.py, the admin debug endpoint).

    Without this, check_leg_sl/check_leg_target's `leg.current_sl_price or
    calc_sl_price(...)` fallback still works for SL/TP DETECTION (it lazily
    computes the price each call when the field is empty) — but the field
    itself stays 0.0 forever unless a hit occurs, which breaks two other
    things silently: (1) any UI reading leg.current_sl_price for display
    shows nothing until a hit, and (2) trailing_engine.apply_trailing's own
    `if not leg.current_sl_price: return False` guard means trailing NEVER
    activates for a leg whose SL was never explicitly initialized."""
    sl_config = leg.leg_cfg.get("LegStopLoss") or {}
    tp_config = leg.leg_cfg.get("LegTarget") or {}
    leg.current_sl_price = calc_sl_price(leg.entry_price, leg.is_sell, sl_config, leg.entry_spot, leg.option_type) or 0.0
    leg.current_tp_price = calc_tp_price(leg.entry_price, leg.is_sell, tp_config, leg.entry_spot, leg.option_type) or 0.0
    # Trailing's ratchet anchor (trailing_engine.py's best_price) starts at
    # entry_price — no favorable movement has happened yet. A fresh
    # re-entry/re-cost leg gets a brand-new LegRuntime (this function is
    # called on it too), so this naturally resets the extreme per
    # generation instead of inheriting a prior leg's trail progress.
    leg.best_price = leg.entry_price


def check_leg_sl(leg: LegRuntime) -> tuple[bool, float]:
    """Returns (sl_hit, sl_price). Stored current_sl_price (already set —
    initial calc or a trail-SL move) takes priority over freshly computing
    one, so a trail-SL move already applied never gets overwritten here."""
    sl_config = leg.leg_cfg.get("LegStopLoss") or {}
    sl_price = leg.current_sl_price or calc_sl_price(leg.entry_price, leg.is_sell, sl_config, leg.entry_spot, leg.option_type)
    if not sl_price:
        return False, 0.0
    is_underlying = is_underlying_config(sl_config)
    compare_price = leg.current_spot if is_underlying else leg.current_price
    if not compare_price:
        return False, sl_price
    direction = underlying_direction_is_sell(leg.is_sell, leg.option_type) if is_underlying else leg.is_sell
    return is_sl_hit(compare_price, sl_price, direction), sl_price


def check_leg_target(leg: LegRuntime) -> tuple[bool, float]:
    """Returns (tp_hit, tp_price)."""
    tp_config = leg.leg_cfg.get("LegTarget") or {}
    tp_price = leg.current_tp_price or calc_tp_price(leg.entry_price, leg.is_sell, tp_config, leg.entry_spot, leg.option_type)
    if not tp_price:
        return False, 0.0
    is_underlying = is_underlying_config(tp_config)
    compare_price = leg.current_spot if is_underlying else leg.current_price
    if not compare_price:
        return False, tp_price
    direction = underlying_direction_is_sell(leg.is_sell, leg.option_type) if is_underlying else leg.is_sell
    return is_tp_hit(compare_price, tp_price, direction), tp_price
