"""
gap_recovery.py
──────────────────
Startup-only, one-shot TSL gap recovery — closes the ONE gap
algo_trade_TSL_HA_restart_recovery_master_prompt.md's "Why Latest LTP Is
Not Enough" section describes: a market move that happened ENTIRELY while
this process was down is invisible to the live feed's first post-restart
tick, so trailing_engine.apply_trailing() alone (correct as it now is)
would silently miss it — best_price/current_sl would resume from whatever
was last checkpointed, not from the true extreme the market actually
reached during downtime.

Scope is deliberately narrow — TSL only, per explicit instruction. NOT
built here (out of scope, not oversights): HA/fencing, broker-position
reconciliation, atomic idempotent exit-across-crashes. A detected breach
re-uses the SAME exit path a live SL_HIT already goes through
(token_router._emit -> order_engine.on_trigger) rather than inventing a
second one.

Covers BOTH brokers this engine trades — NSE/Dhan (shared/brokers/dhan/
historical.py) and crypto/Delta (shared/brokers/delta/historical.py) —
dispatched per-token by trying each module's own resolve_instrument_type
in turn (whichever recognizes the token owns the fetch); the user's own
ask was explicit parity here, not a crypto-specific gap ("Indian market la
panra madhiri crypto kum varanum, edhavadhu gap irukka koodathu"). Delta's
side works directly against an option contract's OWN symbol on GET /v2/
history/candles (confirmed live against Delta's real API — see that
module's own docstring), the same per-token, one-call-per-symbol shape
Dhan's side already has, so the replay loop below needs no per-broker
branching beyond which historical module answered "yes, mine."

Runs ONCE, after persistence.recovery.recover() has already rebuilt
token_router's in-memory state, and BEFORE tick_client.start() (main.py) —
matching the master prompt's own startup ordering: restore -> detect gap
-> replay -> THEN resume live monitoring. Never touches the tick path
itself (broadcast_tick_update, on_tick) — zero ongoing CPU cost.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from persistence.checkpoint import checkpoint_strategy
from persistence.serializers import ENGINE_TAG
from risk.sl_tp_engine import calc_sl_price, is_sl_hit, is_underlying_config
from risk.trailing_engine import apply_trailing, get_trail_config
from shared.brokers.delta import historical as delta_historical
from shared.brokers.dhan import historical as dhan_historical
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger
from token_router import TokenRouter, TriggerEvent

log = get_logger(__name__)

# Shorter than this = a normal reconnect blip (the common case), not real
# downtime — skip the whole historical-fetch/replay pass entirely rather
# than paying a Dhan API round-trip for a gap too small to matter.
_MIN_GAP_SECONDS = 30


def _stale_strategy_gaps(mongo: Mongo, now: datetime) -> dict[str, float]:
    """strategy_id -> gap_seconds, for every currently-open algo-2_0-owned
    strategy whose last checkpoint is older than _MIN_GAP_SECONDS. Own,
    separate read of algo_trades (same collection + engine filter
    persistence/recovery.py already uses) — recovery.py itself is untouched."""
    out: dict[str, float] = {}
    for doc in mongo.raw["algo_trades"].find(
        {"engine": ENGINE_TAG, "active_on_server": True}, {"_id": 1, "creation_ts": 1},
    ):
        creation_ts = str(doc.get("creation_ts") or "").strip()
        try:
            last_checkpoint = datetime.fromisoformat(creation_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        gap = (now - last_checkpoint).total_seconds()
        if gap > _MIN_GAP_SECONDS:
            out[str(doc["_id"])] = gap
    return out


def _replay_candle(leg, candle: dict) -> bool:
    """Advances one leg's trail/SL state through one historical candle,
    conservatively checking the ADVERSE extreme (the side that could
    breach SL) before the FAVORABLE one (the side trailing_engine tracks) —
    this can't recover the exact intra-candle tick order (master prompt's
    documented OHLC ambiguity), but never understates a possible breach.
    Returns True if this candle breaches the leg's (pre-candle) SL."""
    adverse_price = candle["high"] if leg.is_sell else candle["low"]
    if is_sl_hit(adverse_price, leg.current_sl_price, leg.is_sell):
        return True
    leg.current_price = candle["low"] if leg.is_sell else candle["high"]
    apply_trailing(leg)
    # The favourable extreme may have tightened the SL inside this same
    # candle; the bar's order of high/low is unknown, so a path that trails
    # first and then reverses to the adverse extreme is also possible —
    # treat a breach of the tightened SL as a breach too (never understate).
    if is_sl_hit(adverse_price, leg.current_sl_price, leg.is_sell):
        return True
    leg.current_price = candle["close"] or leg.current_price
    return False


def recover_tsl_gaps(router: TokenRouter, mongo: Mongo) -> dict[str, int]:
    """Entry point — called once from main.py's on_startup, after
    persistence.recovery.recover(). Returns a small summary dict for the
    startup log line (mirrors persistence.recovery.recover's own style)."""
    summary = {"strategies_checked": 0, "tokens_fetched": 0, "legs_replayed": 0, "breaches_found": 0}
    now = datetime.now(timezone.utc)
    stale = _stale_strategy_gaps(mongo, now)
    if not stale:
        return summary
    summary["strategies_checked"] = len(stale)

    # Group affected legs by TOKEN (never per-leg) — Dhan's own API has no
    # bulk/multi-token endpoint, so this is the one thing that actually
    # bounds the number of real API calls (master prompt's "50,000 legs,
    # 2,000 unique tokens" example). Only legs with a real TrailSL config
    # are worth fetching history for at all.
    legs_by_token: dict[str, list] = {}
    for strategy_id, gap_seconds in stale.items():
        strategy = router.strategies.get(strategy_id)
        if strategy is None:
            continue
        for leg_id in strategy.leg_ids:
            leg = router.legs.get(leg_id)
            if leg is None or leg.status != "ACTIVE" or not get_trail_config(leg.leg_cfg):
                continue
            # Premium candles can't be compared with an Underlying (spot-level)
            # SL — that mixed units and flagged false breaches on restart.
            # Those legs resume on the live spot feed instead.
            if is_underlying_config(leg.leg_cfg.get("LegStopLoss") or {}):
                continue
            legs_by_token.setdefault(leg.token, []).append((leg, gap_seconds))

    for token, leg_gap_pairs in legs_by_token.items():
        # Try each broker's own resolver in turn — whichever recognizes the
        # token owns the fetch. An NSE security_id is purely numeric and
        # never matches Delta's symbol shapes, and vice versa, so at most
        # one of these ever returns non-None for a given token.
        dhan_resolved = dhan_historical.resolve_instrument_type(mongo, token)
        if dhan_resolved is not None:
            dhan_instrument_type, exchange_segment = dhan_resolved
            widest_gap = max(gap for _, gap in leg_gap_pairs)
            from_dt = now - timedelta(seconds=widest_gap)
            candles = dhan_historical.fetch_intraday_candles(mongo, token, exchange_segment, dhan_instrument_type, from_dt, now)
        else:
            delta_resolved = delta_historical.resolve_instrument_type(token)
            if delta_resolved is None:
                continue  # not a recognized NSE or Delta instrument at all
            delta_instrument_type, delta_segment = delta_resolved
            widest_gap = max(gap for _, gap in leg_gap_pairs)
            from_dt = now - timedelta(seconds=widest_gap)
            candles = delta_historical.fetch_intraday_candles(token, delta_segment, delta_instrument_type, from_dt, now)
        summary["tokens_fetched"] += 1
        if not candles:
            continue

        touched_strategy_ids: set[str] = set()
        for leg, gap in leg_gap_pairs:
            summary["legs_replayed"] += 1
            # Only this leg's own downtime window, not the widest gap among
            # every leg sharing the token (older history predates this
            # leg's own newer checkpoint).
            leg_from = now - timedelta(seconds=gap)
            leg_candles = [c for c in candles if c["ts"] >= leg_from]
            initial_sl = calc_sl_price(leg.entry_price, leg.is_sell, leg.leg_cfg.get("LegStopLoss") or {}, leg.entry_spot, leg.option_type)
            if initial_sl:
                leg.current_sl_price = leg.current_sl_price or initial_sl
            breached = False
            for candle in leg_candles:
                if _replay_candle(leg, candle):
                    breached = True
                    break
            if breached:
                summary["breaches_found"] += 1
                leg.status = "SL_HIT"
                log.warning("[GapRecovery] leg=%s token=%s SL breach detected during downtime replay", leg.leg_id, token)
                router._emit(TriggerEvent("SL_HIT", leg.strategy_id, leg.leg_id, leg.current_sl_price, "GAP_RECOVERY_SL"))
            touched_strategy_ids.add(leg.strategy_id)

        for strategy_id in touched_strategy_ids:
            checkpoint_strategy(router, strategy_id)

    log.info(
        "[GapRecovery] checked=%d tokens_fetched=%d legs_replayed=%d breaches=%d",
        summary["strategies_checked"], summary["tokens_fetched"], summary["legs_replayed"], summary["breaches_found"],
    )
    return summary
