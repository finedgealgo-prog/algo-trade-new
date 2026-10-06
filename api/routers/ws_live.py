"""
routers/ws_live.py
──────────────────────
The two client-facing (browser) push sockets `/fast-forward_2` connects to,
inside algo-2_0 itself — no old-system backend involved (user confirmed:
"socket ellame namma algo-2_0 intha folder kulla irukura socket la than
connect agnum, all backend server ellame algo-2_0 kulla than irukanum rest
api also" — everything, WS AND REST, lives inside algo-2_0).

Mounted at `/algo/ws/...` (not bare `/ws/...`) specifically so that
FastForward2.tsx — an EXACT clone of the old FastForward.tsx, per the user's
explicit instruction to change only backend/socket logic and leave the page
itself untouched — can build these URLs with its own unmodified
`buildWsUrl()` helper, which derives the socket path from the same API_BASE
used for every REST call (old convention: API_BASE already ends in `/algo`).

  /algo/ws/update           — server -> client only: LTP ticks + per-leg current_price.
  /algo/ws/execute-orders   — server -> client: strategy/leg record snapshots.
                              client -> server: {"type":"square_off","leg_id":...}
                              (old system's sendDeploymentAction equivalent).

Both require the SAME first-message auth handshake as the old sockets
(`{"type":"auth","token":<jwt>}`), so `/fast-forward_2` can reuse its
existing connect logic unchanged — see ws_client_hub.py.
"""

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from api.live_broadcast import broadcast_strategy_snapshots
from api.ws_client_hub import execute_orders_hub, update_hub
from persistence.serializers import _today_ist_date, load_legacy_broker_settings, load_legacy_execute_order_records
from shared.db.mongo import get_mongo
from shared.logging.logger import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/algo", tags=["ws-live"])


@router.websocket("/ws/update")
async def ws_update(websocket: WebSocket) -> None:
    user_id = await update_hub.authenticate(websocket)
    if user_id is None:
        return
    client_id = uuid.uuid4().hex
    update_hub.register(client_id, websocket, user_id)
    try:
        while True:
            # This socket is server -> client only, but we still read to
            # detect disconnects promptly (matches old system's ping/idle
            # handling on `/ws/update`).
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("[ws/update] client=%s error", client_id)
    finally:
        update_hub.unregister(client_id)


@router.websocket("/ws/execute-orders")
async def ws_execute_orders(websocket: WebSocket, activation_mode: str = Query(default="")) -> None:
    user_id = await execute_orders_hub.authenticate(websocket)
    if user_id is None:
        return
    client_id = uuid.uuid4().hex
    execute_orders_hub.register(client_id, websocket, user_id)

    # Same opening ack the old system's own /ws/execute-orders sends first
    # (execution_socket.py:14518-14522) — FastForward2.tsx doesn't require
    # it, but nothing reads this connection as "ready" before it either.
    await websocket.send_text(json.dumps({
        "type": "connection_established",
        "message": "Execute orders websocket connected",
        "data": {"channel": "execute-orders", "user_id": user_id, "activation_mode": activation_mode},
    }))

    from main import order_engine, token_router

    # Real currently-active trades (any engine — old system's or algo-2_0's
    # own) straight from Mongo, SAME query + leg source the old system's own
    # "Initial strategies loaded" emit uses (execution_socket.py:14536-14554)
    # — see load_legacy_execute_order_records's own docstring for exactly
    # which tables/fields. Sent directly to THIS connecting client only
    # (never token_router — a pure display read, no risk-engine takeover).
    normalized_mode = str(activation_mode or "").strip() or "fast-forward"
    mongo = get_mongo()
    # ws_client_hub.authenticate() returns the literal string "anonymous"
    # when AUTH_ENFORCEMENT_ENABLED is off and the token doesn't decode
    # (e.g. a real browser JWT signed by the OLD backend's own secret,
    # which will never match algo-2_0's dev JWT_SECRET_KEY) — same "no real
    # per-user boundary to enforce" case strategy.py's /list route already
    # handles by not filtering at all, rather than filtering on a sentinel
    # that matches zero real documents and silently returns nothing.
    real_user_id = "" if user_id == "anonymous" else user_id
    legacy_records = load_legacy_execute_order_records(mongo, normalized_mode, real_user_id)
    trade_date = _today_ist_date()
    subscribed_group_id = ""
    if legacy_records:
        subscribed_group_id = str(((legacy_records[0].get("portfolio") or {}).get("group_id")) or "")
        await websocket.send_text(json.dumps({
            "type": "execute_order",
            "message": "Initial strategies loaded",
            "data": {"group_id": subscribed_group_id, "records": legacy_records},
        }))

    # Same guard the old system uses (execution_socket.py:14566-14576) — only
    # bother if at least one leg from the initial snapshot is actually open.
    has_open_leg = any(
        not leg.get("exit_trade") and leg.get("entry_trade")
        for rec in legacy_records for leg in (rec.get("legs") or [])
    )
    if user_id and has_open_leg:
        broker_rows = load_legacy_broker_settings(mongo, user_id, normalized_mode)
        if broker_rows:
            await websocket.send_text(json.dumps({
                "type": "broker-settings",
                "message": "Broker settings updated",
                "data": {"brokers": broker_rows},
            }))

    # Snapshot everything currently live in algo-2_0's OWN in-memory engine
    # immediately on connect too, so a freshly-opened page doesn't have to
    # wait for the next tick to see strategies THIS engine already activated.
    if token_router.strategies:
        await broadcast_strategy_snapshots(token_router, list(token_router.strategies.keys()))

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("type") == "square_off":
                leg_id = str(msg.get("leg_id") or "")
                leg = token_router.legs.get(leg_id)
                if leg is None:
                    continue
                strategy_id = leg.strategy_id
                owner = token_router.strategies.get(strategy_id)
                owner_id = (owner.user_id if owner is not None else leg.user_id) or ""
                # Only the position's owner may exit it. "debug" / ownerless
                # legs come from the admin test endpoint; "anonymous" is the
                # auth-enforcement-off dev mode (no user boundary at all).
                if user_id != "anonymous" and owner_id not in ("", "debug") and owner_id != user_id:
                    log.warning("[ws/execute-orders] square_off denied leg=%s owner=%s requester=%s", leg_id, owner_id, user_id)
                    await websocket.send_text(json.dumps({"type": "error", "message": "Not allowed to square off this position"}))
                    continue
                order_engine.manual_square_off(leg_id)
                # Broadcast BEFORE finalizing, not after — finalize_if_no_
                # open_legs (below) pops the strategy (and its legs) out of
                # token_router entirely once nothing's left open, so a
                # broadcast attempted afterward finds nothing to send and
                # the frontend never learns this exact strategy just
                # exited (confirmed live: squaring off a single-leg
                # strategy's only leg produced zero socket messages at
                # all — the deleted-from-Mongo card just silently sat
                # there until the next full page reload). This send
                # reflects the leg's just-set EXITED status while the
                # strategy record is still there to build.
                await broadcast_strategy_snapshots(token_router, [strategy_id])
                # A manual square-off never arms a re-entry (no leg_followup
                # call on this path — see main.py's _on_leg_exit_arm_followup,
                # gated to SL_HIT/TP_HIT only), so if this was the strategy's
                # last open leg it's now genuinely, permanently done — finalize
                # it immediately instead of leaving it ACTIVE-with-zero-legs
                # to get checkpointed straight back into algo_trades on the
                # next periodic tick and reappear on every page reload (see
                # TokenRouter.finalize_if_no_open_legs's own docstring for the
                # full story — confirmed live, this is exactly what a user
                # squaring off a position kept seeing).
                from services import strategy_finalize
                strategy_finalize.finalize_and_publish(token_router, strategy_id)
                continue
            # The client's subscribe message (buildWsUrl's onopen handler)
            # carries no "type" at all — {"activation_mode":...,"status":...,
            # "autoload":...,"reason":"subscribe"} — same shape the old
            # system acks with subscription_ack (execution_socket.py's
            # per-message handling block, ~14602-14624).
            if "activation_mode" in msg or "status" in msg:
                requested_date = str(msg.get("trade_date") or "").strip()
                if requested_date:
                    trade_date = requested_date
                requested_group = str(msg.get("group_id") or "").strip()
                if requested_group:
                    subscribed_group_id = requested_group
                await websocket.send_text(json.dumps({
                    "type": "subscription_ack",
                    "message": "Execute orders websocket subscription confirmed",
                    "data": {
                        "trade_date": trade_date,
                        "activation_mode": str(msg.get("activation_mode") or normalized_mode),
                        "group_id": subscribed_group_id,
                    },
                }))
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("[ws/execute-orders] client=%s error", client_id)
    finally:
        execute_orders_hub.unregister(client_id)
