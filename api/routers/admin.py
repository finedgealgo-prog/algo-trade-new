"""
routers/admin.py
────────────────────
Minimal admin surface — user list + a live runtime overview (strategies/
brokers currently registered in-memory in token_router). The old system's
full admin surface (subscriptions, pricing plans, telegram broadcast) is not
ported — out of scope for the trading-engine rewrite.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter
from pydantic import BaseModel

from risk import sl_tp_engine
from runtime.broker_runtime import BrokerRuntime
from runtime.leg_runtime import LegRuntime
from runtime.pending_entry_runtime import PendingEntryRuntime
from runtime.strategy_runtime import StrategyRuntime
from shared.auth.service import USERS_COLLECTION, public_user
from shared.db.mongo import get_mongo

router = APIRouter(prefix="/algo/admin", tags=["admin"])

_IST = timezone(timedelta(hours=5, minutes=30))


class DebugLegRequest(BaseModel):
    token: str  # e.g. "BTCUSD" — any token the tick feed streams, NSE or crypto
    is_sell: bool
    qty: int = 1
    stop_loss_points: float = 0.0
    target_points: float = 0.0
    broker_scope_id: str = "debug:local"
    # Optional "HH:MM" (24h, IST) — entry is taken at this time instead of
    # immediately (conditions/schedule_engine.py's TimeIndicator gate, same
    # mechanism real saved-strategy EntryIndicators use). If the time has
    # already passed today, entry fires on the very next tick — matches
    # schedule_engine.is_time_reached's "already past = reached" behavior.
    entry_time: str | None = None


@router.get("/users")
def list_users(q: str = "") -> dict:
    mongo = get_mongo()
    query: dict = {}
    if q:
        query = {"$or": [{"name": {"$regex": q, "$options": "i"}}, {"mobile": {"$regex": q}}]}
    docs = mongo.raw[USERS_COLLECTION].find(query, limit=100)
    return {"users": [public_user(d) for d in docs]}


@router.post("/debug/cleanup-zombies")
def cleanup_zombies() -> dict:
    """Removes any strategy currently registered with ZERO legs (entry
    attempt(s) ultimately failed, e.g. no live price — created before
    services/strategy_activation.py's cleanup_if_no_legs fix landed), OR
    every one of whose legs has already reached a terminal status
    (SL_HIT/TP_HIT/EXITED — e.g. manually squared off via `/ws/execute-
    orders`'s square_off) but the strategy itself never got marked EXITED,
    because nothing in this engine currently flips strategy.status on its
    last leg exiting (a real gap, not covered by risk/mtm_engine.py, which
    only handles OverallSL/OverallTgt-triggered exits) — that ACTIVE-with-
    zero-open-legs strategy would otherwise sit in token_router forever and
    get checkpointed straight back into `algo_trades` on every periodic
    checkpoint tick, and re-recovered on every restart, looking like it
    "keeps coming back" even after being squared off and deleted from Mongo.
    No pending-entry check needed for the has-real-legs case (unlike the
    zero-legs case) — a strategy with real legs, terminal or not, was never
    still waiting on a pending entry.

    `strategy.leg_ids` can reference a leg_id that ISN'T in
    `token_router.legs` at all — recovery.py's docs_to_legs deliberately
    skips re-registering a terminal-status leg on restart (nothing left to
    resume), but doc_to_strategy_kwargs still rebuilds leg_ids from every
    leg_id embedded in the doc regardless of status (confirmed live: a
    strategy recovered post-restart with leg_ids={"...leg_0"} but zero
    entries in token_router.legs). So "no open leg" must be judged by
    whether any of strategy.leg_ids resolves to a STILL-REGISTERED,
    non-terminal leg — not by requiring every id to resolve to something,
    which an all-missing leg_ids set would otherwise vacuously fail."""
    from persistence.serializers import _TERMINAL_LEG_STATUSES

    from main import token_router
    from persistence.checkpoint import get_checkpoint_writer

    removed = []
    for strategy_id, strategy in list(token_router.strategies.items()):
        if strategy.leg_ids:
            has_open_leg = any(
                lid in token_router.legs and token_router.legs[lid].status not in _TERMINAL_LEG_STATUSES
                for lid in strategy.leg_ids
            )
            if has_open_leg:
                continue
        else:
            still_pending = any(p.strategy_id == strategy_id for p in token_router.pending_entries.values())
            if still_pending:
                continue
        strategy.status = "EXITED"
        for leg_id in list(strategy.leg_ids):
            token_router.unregister_leg(leg_id)
        token_router.strategies.pop(strategy_id, None)
        # Matching cleanup TokenRouter.finalize_if_no_open_legs also does —
        # see that method's own comment: register_strategy() adds to
        # broker.strategy_ids on the way in, nothing removed it on the way
        # out until this fix.
        if strategy.broker_scope_id:
            broker = token_router.brokers.get(strategy.broker_scope_id)
            if broker:
                broker.strategy_ids.discard(strategy_id)
        get_checkpoint_writer().queue_strategy_delete(strategy_id)
        removed.append(strategy_id)
    return {"success": True, "removed": removed, "count": len(removed)}


@router.get("/runtime-overview")
def runtime_overview() -> dict:
    from main import token_router

    return {
        "strategies": {
            sid: {"status": s.status, "mtm": s.mtm, "broker_scope_id": s.broker_scope_id, "leg_count": len(s.leg_ids)}
            for sid, s in token_router.strategies.items()
        },
        "brokers": {
            bid: {"status": b.status, "mtm": b.mtm, "strategy_count": len(b.strategy_ids), "leg_count": len(b.leg_ids)}
            for bid, b in token_router.brokers.items()
        },
        "active_legs": len(token_router.legs),
        "active_lazy_watchers": len(token_router.lazy_watchers),
        "active_recost_watchers": len(token_router.recost_watchers),
        "pending_entries": len(token_router.pending_entries),
    }


@router.get("/debug/spot")
def debug_spot() -> dict:
    """Dumps ltp_cache.spot_map as it exists RIGHT NOW — the whole map, not
    one underlying at a time, since when this is empty entirely (as it
    always was before algo.websocket's dhan_feed.py started subscribing the
    NIFTY/BANKNIFTY/FINNIFTY/SENSEX/MIDCPNIFTY index tokens) every NSE
    underlying fails strike resolution the same way, and seeing all of them
    at once is what actually answers "is this wired up at all"."""
    from shared.market.ltp_cache import get_ltp_cache

    cache = get_ltp_cache()
    return {"spot_map": dict(cache.spot_map), "tick_count": cache.tick_count, "ltp_map_size": len(cache.ltp_map)}


@router.get("/debug/chain-ltp")
def debug_chain_ltp(underlying: str, expiry: str, option_type: str) -> dict:
    """Dumps ltp_cache's live LTP for every token instrument_master knows
    for this (underlying, expiry, option_type) RIGHT NOW — same rows
    strike_resolver.build_chain_rows's NSE branch would read, without the
    ATM/strike-selection logic on top, so a "strike not resolved" can be
    told apart from "subscribed but Dhan never sent an LTP for this
    specific illiquid/far-dated strike" vs "never subscribed at all"."""
    from shared.market.instrument_master import get_instrument_master
    from shared.market.ltp_cache import get_ltp_cache

    ltp_cache = get_ltp_cache()
    metas = get_instrument_master().by_underlying_expiry_type(underlying, expiry, option_type)
    rows = [
        {
            "strike": m.strike, "token": m.token, "ltp": ltp_cache.get_ltp(m.token),
            "bid": ltp_cache.bid_map.get(m.token), "ask": ltp_cache.ask_map.get(m.token),
            "oi": ltp_cache.oi_map.get(m.token),
        }
        for m in metas
    ]
    rows.sort(key=lambda r: r["strike"])
    return {"underlying": underlying, "expiry": expiry, "option_type": option_type, "total": len(rows), "with_ltp": sum(1 for r in rows if r["ltp"]), "rows": rows}


@router.get("/debug/resolve-strike")
def debug_resolve_strike(underlying: str, option_type: str, expiry_kind: str = "ExpiryType.Weekly", entry_type: str = "EntryType.EntryByStrikeType", strike_param: str = "StrikeType.ATM", is_sell: bool = True) -> dict:
    """Runs the EXACT same expiry_resolver.resolve_expiry + strike_
    resolver.resolve_strike calls services/strategy_activation.py's own
    entry loop makes, against this process's live instrument_master/
    ltp_cache singletons — so a real activation's "strike not resolved"
    can be reproduced and inspected directly (available expiries, resolved
    spot, the actual selection or lack of one) instead of guessed at from
    outside the process. Same diagnostic role /debug/chain-ltp already
    plays for "is Dhan sending ticks for this contract at all", one level
    up the same call chain."""
    from selection import expiry_resolver, strike_resolver
    from shared.market.instrument_master import get_instrument_master
    from shared.market.ltp_cache import get_ltp_cache

    instrument_master = get_instrument_master()
    ltp_cache = get_ltp_cache()
    underlying = underlying.upper()
    expiries = instrument_master.expiries_for(underlying, option_type)
    expiry = expiry_resolver.resolve_expiry(expiries, expiry_kind)
    spot = ltp_cache.get_spot(underlying) or 0.0
    result = {
        "underlying": underlying, "option_type": option_type, "expiry_kind": expiry_kind,
        "available_expiries": expiries, "resolved_expiry": expiry, "spot": spot,
    }
    if not expiry:
        result["error"] = "no expiry available"
        return result
    try:
        selection = strike_resolver.resolve_strike(
            instrument_master, ltp_cache, underlying, expiry, option_type,
            entry_type, strike_param, spot, is_sell,
        )
    except Exception as exc:
        result["exception"] = repr(exc)
        return result
    result["selection"] = None if selection is None else {
        "token": selection.token, "strike": selection.strike, "symbol": selection.symbol,
        "ltp": selection.ltp, "option_type": selection.option_type, "expiry": selection.expiry,
    }
    return result


@router.get("/debug/delta-chain")
def debug_delta_chain(underlying: str, expiry: str, option_type: str) -> dict:
    """Dumps exactly what strike_resolver.build_chain_rows's crypto branch
    would read RIGHT NOW for this (underlying, expiry, option_type) —
    delta_instrument_cache's cached metadata joined with ltp_cache's live
    (tick-fed) price for each token, same join build_chain_rows itself
    does. Exists so the cached data an entry is actually about to use can
    be inspected/compared against Delta's own UI directly, on demand, not
    only inferred from an already-placed entry's price after the fact."""
    from shared.market.delta_instrument_cache import get_delta_instrument_cache
    from shared.market.ltp_cache import get_ltp_cache

    cache = get_delta_instrument_cache()
    ltp_cache = get_ltp_cache()
    metas = cache.by_underlying_expiry_type(underlying, expiry, option_type)
    rows = [
        {
            "strike": m.strike, "symbol": m.symbol, "token": m.token,
            # Raw per-1-underlying-unit quote — same convention as
            # entry_trade.price everywhere else (see shared/brokers/delta/
            # client.py's _leg_row), directly comparable to Delta's own UI.
            "ltp": ltp_cache.get_ltp(m.token),
        }
        for m in metas
    ]
    rows.sort(key=lambda r: r["strike"])
    return {"underlying": underlying.upper(), "expiry": expiry, "option_type": option_type.upper(), "count": len(rows), "rows": rows, "cache_status": cache.get_status()}


@router.post("/debug/register-leg")
async def debug_register_leg(payload: DebugLegRequest) -> dict:
    """Dev/test only — registers a leg directly against ANY token the tick
    feed streams (NSE via strike_resolver, or a non-option token like a
    crypto perpetual "BTCUSD"/"ETHUSD" that has no option chain to resolve
    a strike from). Bypasses strategy_activation.py's Mongo-backed strategy/
    strike-resolution path entirely — for exercising the SL/TP/risk engine
    against a real live tick stream without needing a full saved strategy.
    This is the activation endpoint `/fast-forward_2` calls (async now so it
    can push an immediate `/ws/execute-orders` snapshot on registration,
    rather than waiting for the next tick)."""
    from main import order_engine, token_router
    from persistence.checkpoint import checkpoint_strategy

    strategy_id = f"debug_{uuid.uuid4().hex[:10]}"
    if payload.broker_scope_id not in token_router.brokers:
        broker = BrokerRuntime(broker_scope_id=payload.broker_scope_id, user_id="debug")
        token_router.register_broker(broker)

    strategy = StrategyRuntime(strategy_id=strategy_id, user_id="debug", name=f"{payload.token} {'SELL' if payload.is_sell else 'BUY'}", broker_scope_id=payload.broker_scope_id)
    token_router.register_strategy(strategy)
    checkpoint_strategy(token_router, strategy_id)

    entry_time = (payload.entry_time or "").strip()
    if entry_time:
        try:
            hour, minute = (int(p) for p in entry_time.split(":", 1))
        except (ValueError, TypeError):
            return {"success": False, "error": 'entry_time must be "HH:MM" (24h, IST)'}
        target_time_ist = datetime.now(_IST).replace(hour=hour, minute=minute, second=0, microsecond=0)

        pending = PendingEntryRuntime(
            pending_id=f"debugpending_{uuid.uuid4().hex[:10]}", strategy_id=strategy_id,
            broker_scope_id=payload.broker_scope_id, target_time_ist=target_time_ist,
            leg_cfg={
                "_debug_direct": True, "token": payload.token, "is_sell": payload.is_sell,
                "qty": payload.qty, "stop_loss_points": payload.stop_loss_points,
                "target_points": payload.target_points,
            },
        )
        token_router.register_pending_entry(pending)
        return {
            "success": True, "scheduled": True, "pending_id": pending.pending_id,
            "strategy_id": strategy_id, "token": payload.token,
            "entry_time_ist": target_time_ist.isoformat(),
        }

    leg = _enter_debug_leg(token_router, order_engine, strategy_id, payload.broker_scope_id, payload.token, payload.is_sell, payload.qty, payload.stop_loss_points, payload.target_points)
    if leg is None:
        return {"success": False, "error": f"No live LTP yet for token={payload.token} — is it subscribed/streaming?"}

    from api.live_broadcast import broadcast_strategy_snapshots
    await broadcast_strategy_snapshots(token_router, [strategy_id])

    return {
        "success": True, "leg_id": leg.leg_id, "strategy_id": strategy_id,
        "token": payload.token, "entry_price": leg.entry_price,
        "sl_price": round(leg.current_sl_price, 2) if leg.current_sl_price else None,
        "tp_price": round(leg.current_tp_price, 2) if leg.current_tp_price else None,
    }


def _enter_debug_leg(token_router, order_engine, strategy_id: str, broker_scope_id: str, token: str, is_sell: bool, qty: int, stop_loss_points: float, target_points: float) -> LegRuntime | None:
    """Shared by the immediate-entry path above and
    services/debug_scheduled_entry.py's entry-time-triggered path — same
    virtual-order + LegRuntime construction either way, only the timing
    differs."""
    from persistence.checkpoint import checkpoint_strategy
    from orders.order_engine import order_venue
    from shared.market.ltp_cache import get_ltp_cache

    current_ltp = get_ltp_cache().get_ltp(token)
    if not current_ltp:
        return None

    order_engine.broker.place_order(
        tradingsymbol=token, **order_venue(token),
        transaction_type="SELL" if is_sell else "BUY", quantity=qty,
        order_type="MARKET", fill_price=current_ltp,
    )

    leg = LegRuntime(
        leg_id=f"{strategy_id}_leg1", strategy_id=strategy_id, user_id="debug", token=token,
        is_sell=is_sell, option_type="", qty=qty, entry_price=current_ltp, current_price=current_ltp,
        leg_cfg={
            "LegStopLoss": {"Type": "LegTgtSLType.Points", "Value": stop_loss_points} if stop_loss_points > 0 else {"Type": "None", "Value": 0},
            "LegTarget": {"Type": "LegTgtSLType.Points", "Value": target_points} if target_points > 0 else {"Type": "None", "Value": 0},
        },
        broker_scope_id=broker_scope_id,
    )
    sl_tp_engine.initialize_sl_tp(leg)
    token_router.register_leg(leg)
    checkpoint_strategy(token_router, strategy_id)
    return leg


@router.get("/debug/leg/{leg_id}")
def debug_get_leg(leg_id: str) -> dict:
    """Full live detail for one leg (status/current_price/pnl/sl/tp) — the
    admin runtime-overview endpoint only aggregates at strategy/broker level,
    this is for a UI that wants to show one specific virtual trade's live
    state (e.g. the algo-admin "Virtual Trade" test page)."""
    from main import token_router

    leg = token_router.legs.get(leg_id)
    if leg is None:
        return {"found": False}
    strategy = token_router.strategies.get(leg.strategy_id)
    return {
        "found": True,
        "leg_id": leg.leg_id, "strategy_id": leg.strategy_id, "token": leg.token,
        "is_sell": leg.is_sell, "qty": leg.qty, "status": leg.status,
        "entry_price": leg.entry_price, "current_price": leg.current_price, "current_pnl": leg.current_pnl,
        "current_sl_price": leg.current_sl_price or None, "current_tp_price": leg.current_tp_price or None,
        "strategy_status": strategy.status if strategy else None,
        "strategy_mtm": strategy.mtm if strategy else None,
    }
