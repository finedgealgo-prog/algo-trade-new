"""
routers/broker_config.py
───────────────────────────
Broker connection registration + broker-level risk settings.

Fresh routes (old system's OAuth-consent flow at /broker/dhan/generate-consent
etc isn't ported — this is the direct-credential path already used by
shared/brokers/dhan/credentials.py/totp_login.py). Writes to the SAME Mongo
`kite_market_config` collection those already use (data reuse, not a new
schema), and additionally registers a live BrokerRuntime in token_router so
strategies can be activated under this broker_scope_id immediately.

`/broker-stoploss-settings/save` configures the broker-level RiskConfig
(SL/Target/LockAndTrail) that risk/broker_risk.py evaluates — same config
shape strategy risk uses (master-doc "unified risk engine" §9/§50: one
RiskConfig model, no duplicated shape).
"""

from __future__ import annotations

import asyncio

from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from features.broker_accounts import DEFAULT_APP_USER_ID

from api.ws_client_hub import execute_orders_hub
from persistence.checkpoint import get_checkpoint_writer
from persistence.serializers import broker_to_doc, load_legacy_broker_settings
from services import broker_scope
from runtime.broker_runtime import BrokerRuntime
from shared.auth.dependency import get_current_user
from shared.brokers.dhan.credentials import save_dhan_access_token
from shared.db.mongo import get_mongo

router = APIRouter(prefix="/algo/broker-configuration", tags=["broker-configuration"])

# Separate router (no "/broker-configuration" prefix) for the one route below
# that mirrors the OLD system's path exactly: GET /algo/broker-configurations
# (plural) — see that route's own docstring for why this exists alongside
# /broker-configuration/list above.
plural_router = APIRouter(prefix="/algo", tags=["broker-configuration"])


class DhanCredentialsRequest(BaseModel):
    broker_connection_id: str  # user-chosen label, e.g. "dhan01" — see master-doc §2
    client_id: str
    access_token: str
    expiry_time: Optional[str] = None


class BrokerStopLossSettingsRequest(BaseModel):
    broker_scope_id: str
    stop_loss: float = 0.0
    target: float = 0.0
    lock_and_trail: Optional[dict[str, Any]] = None  # {"Type": "...", "Value": {...}} same shape as strategy LockAndTrail


def _broker_scope_id(user_id: str, connection_id: str) -> str:
    return f"{user_id}:{connection_id}"


@router.post("/dhan/save")
def save_dhan_credentials(payload: DhanCredentialsRequest, user: dict = Depends(get_current_user)) -> dict:
    from main import token_router

    mongo = get_mongo()
    save_dhan_access_token(mongo, payload.client_id, payload.access_token, payload.expiry_time or "")

    user_id = str(user.get("_id") or "")
    broker_scope_id = _broker_scope_id(user_id, payload.broker_connection_id)
    if broker_scope_id not in token_router.brokers:
        broker = BrokerRuntime(broker_scope_id=broker_scope_id, user_id=user_id)
        token_router.register_broker(broker)
        get_checkpoint_writer().queue_broker(broker_to_doc(broker))

    return {"success": True, "broker_scope_id": broker_scope_id}


@router.get("/list")
def list_broker_scopes(user: dict = Depends(get_current_user)) -> dict:
    from main import token_router

    user_id = str(user.get("_id") or "")
    scopes = [
        {"broker_scope_id": bid, "status": b.status, "mtm": b.mtm}
        for bid, b in token_router.brokers.items() if b.user_id == user_id
    ]
    return {"broker_scopes": scopes}


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = "") -> str:
    """Ported as-is from the old system's api.py — `name`/`display_name` is the
    user's own chosen connection label; the rest are internal type keys, only
    used as a fallback for docs with no real name set. Order matters (checked
    top to bottom) — `broker_name` must come AFTER the user-chosen fields since
    every broker_configuration doc has broker_name set, so checking it first
    would silently hide the name the user actually typed."""
    for key in ("display_name", "name", "title", "broker_name", "broker", "broker_type", "provider", "vendor"):
        value = str(document.get(key) or "").strip()
        if value:
            return value
    return str(fallback_broker_id or "").strip()


@plural_router.get("/broker-configurations")
def list_broker_configurations(broker_type: str = "", user_id: str = "") -> dict:
    """Route-compatible with the old algo.trade's GET /algo/broker-configurations
    (api.py) — same path, same query params, same `broker_configuration` Mongo
    collection (shared data, see module docstring) — so StrategySetupModal2/
    PortfolioActivation2's broker picker can show real named broker connections
    instead of only live-registered runtime broker_scope_ids from /list above.

    NOT ported: the old route's live Kite/FlatTrade session ping
    (_validate_broker_configuration_session) — that's old-system broker_gateway
    machinery algo-2_0 doesn't have. `is_logged_in` here is a simple
    access_token-presence check instead of a real live ping; good enough for a
    broker-picker dropdown, not a substitute for an actual session check."""
    mongo = get_mongo()
    query: dict = {}
    normalized_broker_type = str(broker_type or "").strip()
    if normalized_broker_type:
        query["broker_type"] = normalized_broker_type
    if str(user_id or "").strip():
        query["app_user_id"] = str(user_id).strip()

    cursor = mongo.raw["broker_configuration"].find(
        query,
        {
            "_id": 1, "name": 1, "broker_name": 1, "display_name": 1, "title": 1,
            "broker": 1, "broker_type": 1, "broker_icon": 1, "provider": 1, "vendor": 1,
            "login_time": 1, "user_id": 1, "access_token": 1,
        },
    )
    records = []
    for item in cursor:
        broker_id = str(item.get("_id") or "").strip()
        if not broker_id:
            continue
        records.append({
            "_id": broker_id,
            "name": _extract_broker_configuration_label(item, broker_id),
            "broker_name": str(item.get("broker_name") or "").strip(),
            "broker_type": str(item.get("broker_type") or "").strip(),
            "broker_icon": str(item.get("broker_icon") or "").strip(),
            "user_id": str(item.get("user_id") or "").strip(),
            "login_time": str(item.get("login_time") or "").strip(),
            "is_logged_in": bool(str(item.get("access_token") or "").strip()),
        })

    return {"success": True, "count": len(records), "records": records}


@plural_router.get("/get_broker_stoploss_settings/{user_id}/{broker}/{activation_mode}")
def get_broker_stoploss_settings(user_id: str, broker: str, activation_mode: str) -> dict:
    """Path- and shape-compatible with the old algo.trade's GET
    /algo/get_broker_stoploss_settings/{user_id}/{broker}/{activation_mode}
    (api.py:6789-6844) — same `algo_borker_stoploss_settings` collection
    (name/typo kept as-is, it's the real old collection name) keyed on
    (user_id, broker, activation_mode), same `broker_configuration` lookup
    for broker_name/broker_details. Still unauthenticated with user_id in
    the path, same as the old route, for the same reason: legacy dashboard
    callers hit this exact shape with no Authorization header."""
    mongo = get_mongo()
    normalized_user_id = str(user_id or "").strip()
    normalized_broker = str(broker or "").strip()
    normalized_activation_mode = str(activation_mode or "").strip() or "algo-backtest"

    document = mongo.raw["algo_borker_stoploss_settings"].find_one(
        {
            "user_id": normalized_user_id,
            "broker": normalized_broker,
            "activation_mode": normalized_activation_mode,
        },
        {"_id": 0},
    )
    broker_name = ""
    broker_details: dict = {}
    try:
        broker_doc = mongo.raw["broker_configuration"].find_one(
            {"_id": ObjectId(normalized_broker)},
            {"_id": 0, "broker_name": 1, "display_name": 1, "name": 1, "title": 1, "broker": 1, "broker_icon": 1},
        )
        if broker_doc:
            broker_details = {k: str(v or "") for k, v in broker_doc.items()}
            for key in ("broker_name", "display_name", "name", "title", "broker"):
                val = str(broker_doc.get(key) or "").strip()
                if val:
                    broker_name = val
                    break
    except Exception:
        pass

    return {
        "success": True,
        "found": document is not None,
        "settings": document,
        "broker_name": broker_name,
        "broker_details": broker_details,
    }


@plural_router.post("/broker-stoploss-settings/save")
async def save_legacy_broker_stoploss_settings(payload: dict) -> dict:
    """Path/shape-compatible with the old algo.trade's POST
    /algo/broker-stoploss-settings/save (api.py:6603-6700ish) —
    FastForward2.tsx's stop-loss/target/lock-and-trail settings panel still
    posts this exact shape (user_id/broker/activation_mode + StopLoss/
    Target/OverallTrailSL/LockAndTrail). Same `algo_borker_stoploss_settings`
    collection as GET /algo/get_broker_stoploss_settings above, upserted on
    (user_id, broker, activation_mode).

    NOT ported: the old route's on-change lock/trail *live-state* reset
    (current_lock_floor/lock_activated/sl_peak_mtm etc. wiped when
    SL/Target/LockAndTrail actually change) — that state is evaluated by
    the old system's trading_core.py tick loop, which algo-2_0's own
    risk/broker_risk.py + sl_tp_engine.py don't share; nothing on algo-2_0
    reads those specific fields today, so there's nothing to invalidate.

    THIS save was, until now, write-only: it persisted to `algo_borker_
    stoploss_settings` and returned, but nothing ever read that collection
    back into a live BrokerRuntime.config — risk/broker_risk.py's
    evaluate_broker_risk() only ever evaluates whatever RiskConfig is
    already on the in-memory broker object, and the ONLY code path that
    ever set broker.config (build_broker_risk_config, in POST /algo/
    broker-configuration/stoploss-settings/save below) is a DIFFERENT
    endpoint FastForward2.tsx's real settings panel never calls — confirmed
    via that panel's own real request shape matching THIS route, not that
    one. So broker-level StopLoss/Target/LockAndTrail set through the
    actual UI was silently inert: it saved, displayed back correctly on
    reload (GET reads the same collection), but evaluate_broker_risk()
    never saw it, so it never actually triggered. Now also pushes the SAME
    RiskConfig onto the live broker (if currently registered) and
    checkpoints it — same effect POST .../stoploss-settings/save already
    had, just reachable from the endpoint that's actually used.

    broker_scope_id lookup tries both conventions this codebase uses for
    it (a raw broker_configuration _id used directly, or a composite
    "{user_id}:{connection_id}" tag) since this payload's `broker` field
    doesn't say which one produced it — updates whichever is actually
    registered, no-ops harmlessly if neither is (the broker simply hasn't
    been activated under yet; the Mongo save above still succeeds either
    way, matching the old behavior for that case)."""
    mongo = get_mongo()
    user_id = str(payload.get("user_id") or "").strip()
    broker = str(payload.get("broker") or "").strip()
    activation_mode = str(payload.get("activation_mode") or "algo-backtest").strip() or "algo-backtest"

    document = {
        "user_id": user_id,
        "broker": broker,
        "activation_mode": activation_mode,
        "broker_type": "Broker.Backtest",
        "status": 1,
        "StopLoss": payload.get("StopLoss"),
        "Target": payload.get("Target"),
        "OverallTrailSL": payload.get("OverallTrailSL"),
        "LockAndTrail": payload.get("LockAndTrail"),
        # New settings start a fresh lock/trail cycle; the live engine
        # re-derives and re-saves the state (services/broker_scope).
        **broker_scope.CLEARED_LOCK_STATE,
    }
    if user_id and broker and activation_mode:
        mongo.raw["algo_borker_stoploss_settings"].update_one(
            {"user_id": user_id, "broker": broker, "activation_mode": activation_mode},
            {"$set": document, "$setOnInsert": {"creation_ts": datetime.now(timezone.utc).isoformat()}},
            upsert=True,
        )

    from main import token_router

    # Applies to the LIVE engine immediately — strategies already running
    # under this broker pick up the new SL/Target/Lock/Trail on their next
    # tick (see services/broker_scope.apply_live_settings).
    for candidate_scope_id in {broker, f"{user_id}:{broker}"}:
        closed_mtm = 0.0
        if candidate_scope_id not in token_router.brokers:
            closed_mtm = await asyncio.to_thread(broker_scope.closed_day_mtm, mongo, candidate_scope_id)
        broker_scope.apply_live_settings(token_router, candidate_scope_id, user_id, document, closed_mtm, activation_mode)

    # ws_live.py only ever sends "broker-settings" once, on the socket's
    # own initial connect (load_legacy_broker_settings there) — a save made
    # while the tab is already open had no live path back to it at all, so
    # the sidebar strip only ever picked up a fresh save on a hard page
    # reload (user's own report). Rebuild this exact same user's rows the
    # same way that initial-connect snapshot does and push it out now, on
    # every save, so an already-open tab updates immediately too.
    if user_id:
        broker_rows = load_legacy_broker_settings(mongo, user_id, activation_mode)
        if broker_rows:
            await execute_orders_hub.broadcast({
                "type": "broker-settings",
                "message": "Broker settings updated",
                "data": {"brokers": broker_rows},
            })

    return {"success": True}


@router.post("/stoploss-settings/save")
def save_broker_stoploss_settings(payload: BrokerStopLossSettingsRequest) -> dict:
    from main import token_router

    broker = token_router.brokers.get(payload.broker_scope_id)
    if broker is None:
        raise HTTPException(status_code=404, detail="Broker scope not registered — save credentials first")

    settings_doc: dict[str, Any] = {"StopLoss": payload.stop_loss, "Target": payload.target}
    if payload.lock_and_trail:
        settings_doc["LockAndTrail"] = payload.lock_and_trail
    broker_scope.apply_live_settings(token_router, payload.broker_scope_id, broker.user_id, settings_doc)

    return {"success": True, "broker_scope_id": payload.broker_scope_id}


_BROKER_CONFIGURATION_SAVE_FIELDS = {
    "name", "broker_name", "broker_type", "broker_icon",
    "api_key", "api_secret", "redirect_url", "app_user_id",
}


@plural_router.post("/broker-configuration/save")
def save_broker_configuration(payload: dict) -> dict:
    """Path/payload-compatible with the old algo.trade's POST
    /algo/broker-configuration/save (api.py:5826-5919) — same
    `broker_configuration` collection, same accepted fields, same
    create-vs-update-by-_id behavior, same broker_icon/broker_global_type
    derivation from broker_name, same FlatTrade redirect_url/postback_url
    auto-fill (needed since the FlatTrade OAuth login/callback routes this
    codebase doesn't have yet still read those two fields off this same
    collection once ported in a later phase)."""
    mongo = get_mongo()
    doc_id = str(payload.get("_id") or "").strip()
    fields: dict[str, Any] = {
        k: str(v or "").strip() for k, v in payload.items() if k in _BROKER_CONFIGURATION_SAVE_FIELDS
    }
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()

    if not fields.get("app_user_id"):
        if doc_id:
            fields.pop("app_user_id", None)
        else:
            fields["app_user_id"] = DEFAULT_APP_USER_ID

    if not fields.get("broker_icon"):
        bname = fields.get("broker_name", "").lower()
        if "flattrade" in bname:
            fields["broker_icon"] = "flattrade.svg"
        elif "zerodha" in bname or "kite" in bname:
            fields["broker_icon"] = "kite-logo.svg"
        elif "delta" in bname:
            fields["broker_icon"] = "delta_exchange.svg"

    raw_global_type = str(payload.get("broker_global_type") or "").strip()
    if raw_global_type:
        fields["broker_global_type"] = int(raw_global_type)
    elif not doc_id:
        fields["broker_global_type"] = 2 if "delta" in fields.get("broker_name", "").lower() else 1

    col = mongo.raw["broker_configuration"]
    base_url = str(payload.get("base_url") or "https://finedgealgo.com").rstrip("/")

    if doc_id:
        existing = col.find_one({"_id": ObjectId(doc_id)}, {"broker_name": 1, "redirect_url": 1, "postback_url": 1}) or {}
        bname = fields.get("broker_name") or str(existing.get("broker_name") or "flattrade")
        if "flattrade" in bname.lower():
            if not fields.get("redirect_url") and not existing.get("redirect_url"):
                fields["redirect_url"] = f"{base_url}/broker/flattrade/callback/{doc_id}"
            if not existing.get("postback_url"):
                fields["postback_url"] = f"{base_url}/broker/flattrade/postback/{doc_id}"
        result = col.update_one({"_id": ObjectId(doc_id)}, {"$set": fields})
        return {
            "success": True, "action": "updated", "_id": doc_id,
            "redirect_url": fields.get("redirect_url") or str(existing.get("redirect_url") or ""),
            "postback_url": fields.get("postback_url") or str(existing.get("postback_url") or ""),
            "matched": result.matched_count,
        }

    fields["created_at"] = fields["updated_at"]
    result = col.insert_one(fields)
    new_id = str(result.inserted_id)
    bname = fields.get("broker_name", "flattrade").lower()
    redirect_url = postback_url = ""
    if "flattrade" in bname:
        redirect_url = f"{base_url}/broker/flattrade/callback/{new_id}"
        postback_url = f"{base_url}/broker/flattrade/postback/{new_id}"
        col.update_one({"_id": result.inserted_id}, {"$set": {"redirect_url": redirect_url, "postback_url": postback_url}})
    return {"success": True, "action": "created", "_id": new_id, "redirect_url": redirect_url, "postback_url": postback_url}


@plural_router.delete("/broker-configuration/{doc_id}")
def delete_broker_configuration(doc_id: str) -> dict:
    """Path-compatible with the old algo.trade's DELETE
    /algo/broker-configuration/{doc_id} (api.py:5922-5934)."""
    mongo = get_mongo()
    try:
        result = mongo.raw["broker_configuration"].delete_one({"_id": ObjectId(doc_id)})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"success": True, "deleted": result.deleted_count}
