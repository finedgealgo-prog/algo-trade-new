"""
routers/strategy.py
──────────────────────
Route-compatible with the old algo.trade's /algo/strategy/* (check-name,
save, list, GET/PUT by id — api.py:2630-5493) — same paths, `saved_strategies`
collection, and doc shape (full_config.strategy holds the leg_builder.py-
shaped strategy JSON verbatim, no transformation, confirmed against the old
code). `/activate` is a fresh route (the old activation pipeline — portfolio
shadow docs, group execution — isn't ported; this activates directly via
services/strategy_activation.py).
"""

from __future__ import annotations

import asyncio

from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.strategy_activation import ActivationError, activate_strategy
from shared.auth.dependency import get_current_user
from shared.db.mongo import get_mongo
from shared.market.instrument_master import get_instrument_master
from shared.market.ltp_cache import get_ltp_cache

router = APIRouter(prefix="/algo/strategy", tags=["strategy"])

_COLLECTION = "saved_strategies"


class SaveStrategyRequest(BaseModel):
    name: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    strategy: dict[str, Any]
    report_data: Optional[dict[str, Any]] = None


class ActivateRequest(BaseModel):
    # Optional: FastForward2.tsx's Activate button sends only
    # {activation_mode, current_datetime} — the broker was already chosen in
    # "Edit Setup" (/setup/update) and saved on the strategy doc's `broker`
    # field, which is what the old system activates under too. An explicit
    # value here (portfolio activation) still wins.
    broker_scope_id: Optional[str] = None
    # Set only when this activate call is one member of a portfolio
    # activation (PortfolioActivation2.tsx's handleActivate) — drives
    # StrategyRuntime.group_id/group_name so LiveTrade.tsx's groupRecords()
    # nests every strategy from the SAME portfolio activation under one
    # card, matching the old system's real grouping convention. Absent for
    # a standalone single-strategy activate — that strategy stays its own
    # card (is_direct_strategy=True, the existing default).
    #
    # group_id/group_name come from ONE prior call to GET /algo/portfolio/
    # {id}/resolve-activation-group (see that route's own docstring for the
    # full "why" — old-system-parity name dedup + a fresh per-batch id),
    # shared across every strategy in this same activation batch by the
    # frontend, NOT resolved independently per strategy here (that would
    # race between the parallel /activate calls a portfolio activation
    # fires). portfolio_id alone (no group_id) is accepted as a fallback so
    # a caller that hasn't been updated to the two-step resolve flow still
    # gets SOME grouping instead of none, just without dedup numbering.
    portfolio_id: Optional[str] = None
    group_id: Optional[str] = None
    group_name: Optional[str] = None
    # "fast-forward" (FastForward2, paper) / "live" (AlgoTrade2, real orders).
    activation_mode: Optional[str] = None


class ExecutionSettings(BaseModel):
    execution_config_base: dict[str, Any] = {}
    is_weekdays: bool = True
    weekdays: dict[str, bool] = {}
    dte: list[int] = []


class StrategySetupUpdateRequest(BaseModel):
    strategy_id: str
    broker: str
    execution_settings: ExecutionSettings


def _serialize(doc: dict) -> dict:
    doc = dict(doc)
    doc["_id"] = str(doc["_id"])
    return doc


@router.get("/check-name")
def check_name(name: str) -> dict:
    mongo = get_mongo()
    exists = mongo.raw[_COLLECTION].find_one({"name": name}, {"_id": 1}) is not None
    return {"exists": exists}


@router.post("/save")
def save_strategy(payload: SaveStrategyRequest, user: dict = Depends(get_current_user)) -> dict:
    mongo = get_mongo()
    col = mongo.raw[_COLLECTION]
    if col.find_one({"name": payload.name}, {"_id": 1}):
        raise HTTPException(status_code=409, detail="Strategy name already exists")

    doc = {
        "name": payload.name,
        "underlying": payload.strategy.get("Ticker"),
        "user_id": str(user.get("_id") or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "full_config": {
            "name": payload.name, "start_date": payload.start_date, "end_date": payload.end_date,
            "strategy": payload.strategy, "report_data": payload.report_data,
        },
        "setup_complete": False,
    }
    result = col.insert_one(doc)
    return {"success": True, "id": str(result.inserted_id), "name": payload.name}


@router.get("/list")
def list_strategies(user: dict = Depends(get_current_user)) -> dict:
    mongo = get_mongo()
    # With auth enforcement disabled (dev/test default), get_current_user
    # returns a dummy identity with no real _id — there's no per-user
    # boundary to enforce in that mode anyway, so show every saved strategy
    # rather than silently filtering to an id that matches nothing.
    query = {"user_id": str(user["_id"])} if user.get("_id") else {}
    docs = mongo.raw[_COLLECTION].find(
        query, {"name": 1, "created_at": 1, "underlying": 1, "setup_complete": 1},
    )
    return {"strategies": [_serialize(d) for d in docs]}


@router.get("/list-with-status")
def list_strategies_with_status(activation_mode: str = "fast-forward") -> dict:
    """Ported logic-for-logic from the old algo.trade's GET
    /algo/strategy/list-with-status (api.py:2694-2735) — same
    `saved_strategies` projection and the same `algo_trades` deployed-count
    aggregation (source_strategy_id in this page's strategy_ids AND
    portfolio.is_direct_strategy=True AND activation_mode=<param> AND
    active_on_server=True, grouped by source_strategy_id). The old route's
    `status: Live_Running` shortcut this previously used was wrong: it
    ignored the activation_mode query param entirely (a fast-forward count
    would include live/forward-test deploys too) and counted portfolio
    strategies, not just individually-activated ones — is_direct_strategy is
    what actually scopes it to "activated straight from the Strategies tab"."""
    mongo = get_mongo()
    normalized_mode = str(activation_mode or "").strip() or "fast-forward"
    docs = list(
        mongo.raw[_COLLECTION].find(
            {},
            {
                "_id": 1, "name": 1, "underlying": 1, "created_at": 1,
                "setup_complete": 1, "execution_config_base": 1,
                "is_weekdays": 1, "weekdays": 1, "dte": 1, "broker": 1,
            },
        ).sort("_id", -1)  # newest first (ObjectId = creation time)
    )
    strategy_ids = [str(d["_id"]) for d in docs]
    deployed_counts: dict[str, int] = {}
    if strategy_ids:
        pipeline = [
            {"$match": {
                "source_strategy_id": {"$in": strategy_ids},
                "portfolio.is_direct_strategy": True,
                "activation_mode": normalized_mode,
                "active_on_server": True,
            }},
            {"$group": {"_id": "$source_strategy_id", "count": {"$sum": 1}}},
        ]
        for row in mongo.raw["algo_trades"].aggregate(pipeline):
            deployed_counts[str(row["_id"])] = int(row.get("count") or 0)

    for d in docs:
        sid = str(d["_id"])
        d["_id"] = sid
        d["setup_complete"] = bool(d.get("setup_complete"))
        d["deployed_count"] = deployed_counts.get(sid, 0)
    return {"strategies": docs}


@router.get("/{strategy_id}")
def get_strategy(strategy_id: str) -> dict:
    mongo = get_mongo()
    try:
        doc = mongo.raw[_COLLECTION].find_one({"_id": ObjectId(strategy_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy id")
    if not doc:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return _serialize(doc)


@router.put("/{strategy_id}")
def update_strategy(strategy_id: str, payload: SaveStrategyRequest) -> dict:
    mongo = get_mongo()
    try:
        oid = ObjectId(strategy_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy id")
    result = mongo.raw[_COLLECTION].update_one(
        {"_id": oid},
        {"$set": {
            "name": payload.name, "underlying": payload.strategy.get("Ticker"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "full_config": {
                "name": payload.name, "start_date": payload.start_date, "end_date": payload.end_date,
                "strategy": payload.strategy, "report_data": payload.report_data,
            },
        }},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"success": True, "id": strategy_id}


@router.post("/setup/update")
def update_strategy_setup(payload: StrategySetupUpdateRequest) -> dict:
    """StrategySetupModal2.tsx's "Edit Setup" save — assigns a broker scope
    and execution config (qty multiplier, monitoring mode, weekdays) to a
    saved strategy, and marks it setup_complete so the Activation table's
    "Ready to deploy" pill reflects it (algo-2_0's own /activate doesn't
    gate on this flag — see services/strategy_activation.py — it's display
    only here, matching what the old system's badge meant)."""
    mongo = get_mongo()
    try:
        oid = ObjectId(payload.strategy_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy id")
    result = mongo.raw[_COLLECTION].update_one(
        {"_id": oid},
        {"$set": {
            "broker": payload.broker,
            "execution_config_base": payload.execution_settings.execution_config_base,
            "is_weekdays": payload.execution_settings.is_weekdays,
            "weekdays": payload.execution_settings.weekdays,
            "dte": payload.execution_settings.dte,
            "setup_complete": True,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"success": True, "id": payload.strategy_id}


@router.post("/{strategy_id}/activate")
async def activate(strategy_id: str, payload: ActivateRequest, user: dict = Depends(get_current_user)) -> dict:
    # Deferred import — main.py imports this router at module load time,
    # before its own token_router/order_engine module attributes exist;
    # deferring to call-time (inside the request handler) breaks the cycle.
    from main import order_engine, token_router

    mongo = get_mongo()
    try:
        doc = await asyncio.to_thread(mongo.raw[_COLLECTION].find_one, {"_id": ObjectId(strategy_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy id")
    if not doc:
        raise HTTPException(status_code=404, detail="Strategy not found")

    broker_scope_id = (payload.broker_scope_id or "").strip() or str(doc.get("broker") or "").strip()
    if not broker_scope_id:
        raise HTTPException(status_code=422, detail="No broker set for this strategy — choose one in Edit Setup first")

    portfolio_id = (payload.portfolio_id or "").strip()
    group_id = (payload.group_id or "").strip() or portfolio_id
    try:
        result = await activate_strategy(
            token_router, order_engine, get_instrument_master(), get_ltp_cache(), mongo,
            doc, str(user.get("_id") or ""), broker_scope_id,
            is_direct_strategy=not portfolio_id,
            group_id=group_id, group_name=(payload.group_name or "").strip(), portfolio_id=portfolio_id,
            activation_mode=(payload.activation_mode or "").strip() or "fast-forward",
        )
    except ActivationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # activate_strategy() itself never pushes to the socket — an already-
    # open /algo/ws/execute-orders connection otherwise only learns about
    # this brand-new strategy on the NEXT real tick for one of its leg
    # tokens (broadcast_tick_update's touched_strategy_ids), which for a
    # thin/illiquid symbol (a Delta option contract especially — see
    # delta_feed.py's own docstring on how sparse real trade prints can be)
    # can be minutes away or never. admin.py's /debug/register-leg already
    # pushes an immediate snapshot on entry for exactly this reason; the
    # real activation path needs the same push, not just the entered leg's
    # LTP eventually catching up.
    from api.live_broadcast import broadcast_strategy_snapshots
    await broadcast_strategy_snapshots(token_router, [result["strategy_id"]])

    return {"success": True, **result}
