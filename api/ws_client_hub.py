"""
ws_client_hub.py
───────────────────
Client-facing (browser) WebSocket push — replaces REST polling for live data
(user's explicit ask: "socket la than data varanum, no rest api"). Mirrors
the OLD system's two-socket pattern (verified against
algo-admin/src/pages/AlgoTrade/FastForward.tsx, which is what the new
`/fast-forward_2` page connects to instead of REST):

  /ws/update           — LTP ticks. Same message shapes FastForward.tsx
                         already parses:
                           {"type":"ltp_update","data":{"ltp":[{"token","ltp"}]}}
                           {"type":"update","data":{"open_positions":[{"trade_id","leg_id","current_price"}]}}
  /ws/execute-orders   — strategy/leg record snapshots, pushed on every
                         state change (entry, SL/TP hit, overall/broker
                         exit) — NOT client-polled. Also receives square-off
                         actions from the client (old system's
                         sendDeploymentAction), same as FastForward.tsx sends.

SL/TP/target values themselves are ALWAYS computed and evaluated backend-side
(risk/evaluator.py, sl_tp_engine.py, token_router.py) — this hub only pushes
the resulting state out; it never re-implements risk logic client-side.

Auth handshake matches the old convention: first client message
`{"type":"auth","token":"<jwt>"}`, this hub decodes it with the SAME
shared/auth/service.py used by the REST routes — a WS connection with no
valid token is closed (code 4401, matching FastForward.tsx's
`event.code===4401` -> forceLogout() handling) rather than silently treated
as anonymous, even though AUTH_ENFORCEMENT_ENABLED can be off for REST.
"""

from __future__ import annotations

import json
from typing import Any

import jwt
from fastapi import WebSocket, WebSocketDisconnect

from shared.auth.service import decode_access_token
from shared.config.settings import get_settings
from shared.logging.logger import get_logger
from shared.market.socket_sender import SocketSender

log = get_logger(__name__)


class ClientHub:
    """One instance per socket kind (`update` / `execute-orders`) — kept
    separate (not merged into one) to match the old system's two-socket
    split, which `/fast-forward_2` already expects."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._clients: dict[str, WebSocket] = {}
        # Which authenticated user each client_id belongs to (register()'s
        # own caller already has this from authenticate() — stored here so
        # send_to_user below can target just that one user's tab(s) instead
        # of every connected browser regardless of whose data it is. Only
        # meaningful for execute_orders_hub in practice — a strategy record
        # (or a broker-settings row) belongs to exactly one user, and
        # broadcasting it to everyone leaked one user's live positions/SL-
        # TP-Trail config into every OTHER open tab on this same page
        # (user's explicit report). update_hub keeps using plain broadcast()
        # — LTP itself isn't per-user data.
        self._user_of: dict[str, str] = {}
        self._senders: dict[str, SocketSender] = {}

    async def authenticate(self, websocket: WebSocket) -> str | None:
        """Old-system handshake: accept the connection, wait for the first
        `{"type":"auth","token":...}` frame, decode it. Returns the user_id
        on success; closes the socket (code 4401) and returns None on
        failure — same close code FastForward.tsx already checks for.

        Honors the SAME Settings.auth_enforcement_enabled kill switch the
        REST dependency (shared/auth/dependency.py) uses — with it off
        (dev/test default) a connection is accepted even with no/invalid
        token, so `/fast-forward_2` doesn't need its own separate login
        flow against algo-2_0 while auth enforcement is disabled."""
        await websocket.accept()
        settings = get_settings()
        try:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
        except Exception:
            if not settings.auth_enforcement_enabled:
                return "anonymous"
            await websocket.close(code=4401)
            return None
        if msg.get("type") != "auth":
            if not settings.auth_enforcement_enabled:
                return "anonymous"
            await websocket.close(code=4401)
            return None
        token = msg.get("token")
        if not token:
            if not settings.auth_enforcement_enabled:
                return "anonymous"
            await websocket.close(code=4401)
            return None
        try:
            claims = decode_access_token(token)
            return str(claims.get("sub") or "")
        except Exception:
            pass
        if not settings.auth_enforcement_enabled:
            # A real browser session's token is routinely signed by whatever
            # backend actually issued it (the old system's own JWT_SECRET,
            # not algo-2_0's dev one) — signature verification above will
            # always fail for that, every time, not just on a genuinely bad
            # token. With enforcement off there's no real security boundary
            # to uphold anyway (same reasoning as the "anonymous" fallbacks
            # above), so still recover the real `sub` claim unverified rather
            # than treating every real session as "anonymous" — that
            # "anonymous" identity is what silently emptied every user_id-
            # scoped query (execute-orders' initial snapshot, broker-
            # settings) for a real, valid session.
            try:
                unverified = jwt.decode(token, options={"verify_signature": False})
                sub = str(unverified.get("sub") or "").strip()
                if sub:
                    return sub
            except Exception:
                pass
            return "anonymous"
        await websocket.close(code=4401)
        return None

    def register(self, client_id: str, websocket: WebSocket, user_id: str = "") -> None:
        self.unregister(client_id)
        self._clients[client_id] = websocket
        self._user_of[client_id] = user_id
        self._senders[client_id] = SocketSender(
            websocket, lambda sender: self._remove_sender(client_id, sender),
        )

    def _remove_sender(self, client_id: str, sender: SocketSender) -> None:
        if self._senders.get(client_id) is sender:
            self._senders.pop(client_id, None)
            self._clients.pop(client_id, None)
            self._user_of.pop(client_id, None)

    def unregister(self, client_id: str) -> None:
        sender = self._senders.pop(client_id, None)
        if sender is not None:
            sender.stop()
        self._clients.pop(client_id, None)
        self._user_of.pop(client_id, None)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        if self._senders:
            msg = json.dumps(payload)
            for sender in tuple(self._senders.values()):
                sender.send(msg)

    async def send_to_user(self, user_id: str, payload: dict[str, Any]) -> None:
        if not user_id:
            return
        targets = [self._senders[cid] for cid, uid in self._user_of.items() if uid == user_id]
        if targets:
            msg = json.dumps(payload)
            for sender in targets:
                sender.send(msg)

    async def close(self) -> None:
        import asyncio
        senders = list(self._senders.values())
        for sender in senders:
            sender.stop()
        await asyncio.gather(*(sender.task for sender in senders), return_exceptions=True)

    def get_status(self) -> dict:
        return {"connected_clients": len(self._clients)}


update_hub = ClientHub("update")
execute_orders_hub = ClientHub("execute-orders")
