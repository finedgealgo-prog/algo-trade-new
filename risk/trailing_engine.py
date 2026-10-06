"""
trailing_engine.py
─────────────────────
Trailing Stop Loss — exact port of shared/features/position_manager.py's
get_trail_config/update_trail_sl. Trailing state lives entirely on
LegRuntime.current_sl_price (master-doc §12: don't write every TSL movement
to Mongo — periodic checkpointing, Phase 8, handles persistence instead).

apply_trailing() below must call update_trail_sl() with an explicit
`initial_sl` — the old system's real working caller does exactly this
(position_manager.py:790-797: `initial_sl = calc_sl_price(...)`, freshly
recomputed from entry_price/sl_config every call, never stored/cached,
since neither input ever changes after entry — passed alongside the live,
already-trailed `current_sl`). update_trail_sl's own `base_sl` MUST anchor
to this fixed initial value, not the live current_sl — using current_sl as
base (trading_core.py's OWN old check_trail_sl wrapper has this exact bug,
confirmed by reading it — an inconsistency within the old system itself,
not something to replicate) re-bases every call onto an already-shrunk
value, so a run of favorable ticks still sitting in the SAME step bucket
(e.g. favorable=11 and favorable=19 are both steps=1 for a 10-point step)
keeps subtracting StopLossMove again on every single one of those ticks
instead of once per bucket — a runaway compounding trail, not a clean
step-wise one.
"""

from __future__ import annotations

from risk.sl_tp_engine import calc_sl_price, is_underlying_config, underlying_direction_is_sell
from runtime.leg_runtime import LegRuntime


def get_trail_config(leg_cfg: dict) -> dict:
    trail = leg_cfg.get("LegTrailSL") or {}
    if not trail or str(trail.get("Type") or "") == "None":
        trail = (leg_cfg.get("LegStopLoss") or {}).get("Trail") or {}
    return trail


def update_trail_sl(
    entry_price: float, current_price: float, current_sl: float, is_sell: bool,
    trail_config: dict, initial_sl: float | None = None,
) -> float:
    if not trail_config:
        return current_sl
    trail_type = str(trail_config.get("Type") or "")
    if "None" in trail_type:
        return current_sl

    val = trail_config.get("Value") or {}
    x = float(val.get("InstrumentMove") or 0)  # instrument must move X
    y = float(val.get("StopLossMove") or 0)  # then SL moves Y
    if x <= 0 or y <= 0:
        return current_sl

    base_sl = float(initial_sl if initial_sl is not None else current_sl)
    if base_sl <= 0:
        return current_sl

    if "Points" in trail_type:
        if is_sell:
            favorable = entry_price - current_price  # fell = good for sell
            if favorable > 0:
                steps = int(favorable / x)
                new_sl = base_sl - steps * y
                return min(current_sl, round(new_sl, 2)) if new_sl < current_sl else current_sl
        else:
            favorable = current_price - entry_price  # rose = good for buy
            if favorable > 0:
                steps = int(favorable / x)
                new_sl = base_sl + steps * y
                return max(current_sl, round(new_sl, 2)) if new_sl > current_sl else current_sl

    if "Percentage" in trail_type:
        sl_step = entry_price * (y / 100)
        if is_sell:
            favorable_pct = (entry_price - current_price) / entry_price * 100
            if favorable_pct > 0:
                steps = int(favorable_pct / x)
                new_sl = base_sl - steps * sl_step
                return min(current_sl, round(new_sl, 2)) if new_sl < current_sl else current_sl
        else:
            favorable_pct = (current_price - entry_price) / entry_price * 100
            if favorable_pct > 0:
                steps = int(favorable_pct / x)
                new_sl = base_sl + steps * sl_step
                return max(current_sl, round(new_sl, 2)) if new_sl > current_sl else current_sl

    return current_sl


def _update_best_price(leg: LegRuntime) -> None:
    """Tracks the single best (most-favorable) price ever seen for this leg
    — SELL wants the LOWEST price reached, BUY the HIGHEST. This is the
    ratchet's real anchor: a later bounce-back tick must never move this
    backward, only a NEW extreme advances it. Already part of the
    checkpoint schema (persistence/serializers.py's _runtime_best_price)
    and already restored on recovery — it was simply never written to
    before now, so it sat at 0.0 forever."""
    if leg.best_price <= 0:
        leg.best_price = leg.current_price
        return
    if leg.is_sell:
        if leg.current_price < leg.best_price:
            leg.best_price = leg.current_price
    else:
        if leg.current_price > leg.best_price:
            leg.best_price = leg.current_price


def apply_trailing(leg: LegRuntime) -> bool:
    """Recompute and apply the trail-SL move for this leg in place.
    Returns True if current_sl_price changed.

    `initial_sl` is recomputed fresh here (not read off any stored field)
    from entry_price/leg_cfg, which never change post-entry, so this is a
    cheap, deterministic, idempotent recompute — same value every call,
    survives a checkpoint/restart cycle with zero extra state to persist.

    The trail step itself is computed off `leg.best_price` (the all-time
    favorable extreme), NOT the raw current tick — matching the correct
    ratchet formula exactly (favorable_move = entry_price - best_price for
    a SELL). Using the current tick alone still ratchets correctly *within
    one continuous RAM session* (the min()/max() guard below rejects any
    candidate worse than what's already stored) — but best_price is what
    makes that correctness explicit and, critically, what makes it survive
    a restart: it's checkpointed the same as current_sl_price, so recovery
    restores the real historical extreme instead of only the latest tick."""
    trail_cfg = get_trail_config(leg.leg_cfg)
    if not trail_cfg or not leg.current_sl_price:
        return False
    sl_config = leg.leg_cfg.get("LegStopLoss") or {}
    if is_underlying_config(sl_config):
        return _apply_underlying_trailing(leg, trail_cfg, sl_config)
    _update_best_price(leg)
    initial_sl = calc_sl_price(leg.entry_price, leg.is_sell, sl_config, leg.entry_spot, leg.option_type) or leg.current_sl_price
    new_sl = update_trail_sl(leg.entry_price, leg.best_price, leg.current_sl_price, leg.is_sell, trail_cfg, initial_sl=initial_sl)
    if new_sl != leg.current_sl_price:
        leg.current_sl_price = new_sl
        return True
    return False


def _apply_underlying_trailing(leg: LegRuntime, trail_cfg: dict, sl_config: dict) -> bool:
    """Underlying (spot) based SL: current_sl_price is a SPOT level, so it
    must trail on the underlying's own favourable move from entry_spot, in
    the spot direction that is adverse for this leg (a short PE / long CE
    loses when spot falls — see sl_tp_engine.underlying_direction_is_sell).
    Trailing it on premium moves shifted a spot level by premium points and,
    for a short PE, moved it the wrong way (looser)."""
    if leg.entry_spot <= 0 or leg.current_spot <= 0:
        return False
    spot_is_sell = underlying_direction_is_sell(leg.is_sell, leg.option_type)
    if leg.best_spot <= 0:
        leg.best_spot = leg.entry_spot
    if spot_is_sell:
        leg.best_spot = min(leg.best_spot, leg.current_spot)
    else:
        leg.best_spot = max(leg.best_spot, leg.current_spot)
    initial_sl = calc_sl_price(leg.entry_price, leg.is_sell, sl_config, leg.entry_spot, leg.option_type) or leg.current_sl_price
    new_sl = update_trail_sl(leg.entry_spot, leg.best_spot, leg.current_sl_price, spot_is_sell, trail_cfg, initial_sl=initial_sl)
    if new_sl != leg.current_sl_price:
        leg.current_sl_price = new_sl
        return True
    return False
