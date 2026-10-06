"""
routers/runtime_risk.py
───────────────────────
FastForward2.tsx's per-strategy / per-portfolio "Risk Settings" (set after
activation, same modal + payload shape as broker settings). See
risk/runtime_risk.py for how they're evaluated and persisted.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from risk import runtime_risk
from risk.broker_risk import build_broker_risk_config
from risk.models import TrailingMode
from shared.auth.dependency import get_current_user
from shared.db.mongo import get_mongo

router = APIRouter(prefix="/algo/runtime-risk", tags=["runtime-risk"])


class SaveRuntimeRiskRequest(BaseModel):
    scope: Literal["strategy", "group"]
    target_id: str
    name: str = ""
    StopLoss: Optional[float] = None
    Target: Optional[float] = None
    LockAndTrail: Optional[dict[str, Any]] = None
    OverallTrailSL: Optional[dict[str, Any]] = None


def _scope_strategies(token_router, scope: str, target_id: str) -> list:
    if scope == "strategy":
        s = token_router.strategies.get(target_id)
        return [s] if s is not None else []
    return [s for s in token_router.strategies.values() if s.group_id == target_id]


def _is_empty(cfg) -> bool:
    return not cfg.stop_loss_enabled and not cfg.target_enabled and cfg.trailing_mode == TrailingMode.NONE


@router.get("/list")
def list_runtime_risks(user: dict = Depends(get_current_user)) -> dict:
    """Today's settings for the user (armed and already-hit) — FastForward2
    shows them as chips on each strategy / portfolio row."""
    query: dict[str, Any] = {"trade_date": runtime_risk.today_ist(), "status": {"$in": ["ACTIVE", "HIT"]}}
    if user.get("_id"):
        query["user_id"] = str(user["_id"])
    from main import token_router

    out = []
    for doc in get_mongo().raw[runtime_risk.COLLECTION].find(query):
        live = token_router.runtime_risks.get(doc["_id"])
        out.append(runtime_risk.to_doc(live) if live is not None else doc)
    return {"risks": out}


@router.get("/{scope}/{target_id}")
def get_runtime_risk(scope: str, target_id: str) -> dict:
    from main import token_router

    live = token_router.runtime_risks.get(runtime_risk.risk_key(scope, target_id))
    if live is not None:
        return {"found": True, "risk": runtime_risk.to_doc(live)}
    doc = get_mongo().raw[runtime_risk.COLLECTION].find_one({"_id": runtime_risk.risk_key(scope, target_id), "trade_date": runtime_risk.today_ist()})
    return {"found": bool(doc), "risk": doc}


@router.post("/save")
async def save_runtime_risk(payload: SaveRuntimeRiskRequest, user: dict = Depends(get_current_user)) -> dict:
    from main import publish_runtime_risk, token_router

    target_id = payload.target_id.strip()
    members = _scope_strategies(token_router, payload.scope, target_id)
    if not members:
        raise HTTPException(status_code=404, detail=f"No running {payload.scope} with id {target_id}.")

    settings = {
        "StopLoss": payload.StopLoss, "Target": payload.Target,
        "LockAndTrail": payload.LockAndTrail, "OverallTrailSL": payload.OverallTrailSL,
    }
    config = build_broker_risk_config(settings)
    key = runtime_risk.risk_key(payload.scope, target_id)
    existing = token_router.runtime_risks.get(key)

    if _is_empty(config):
        # Everything switched off → disarm.
        if existing is not None:
            token_router.runtime_risks.pop(key, None)
        await asyncio.to_thread(get_mongo().raw[runtime_risk.COLLECTION].update_one, {"_id": key}, {"$set": {"status": "OFF"}})
        return {"success": True, "status": "OFF"}

    rr = runtime_risk.RuntimeRisk(
        scope=payload.scope, target_id=target_id,
        user_id=str(user.get("_id") or members[0].user_id or ""),
        name=payload.name.strip() or (members[0].group_name if payload.scope == "group" else members[0].name),
        settings=settings, config=config,
    )
    if existing is not None and existing.status == "ACTIVE":
        # Re-save keeps today's progress — a locked floor never drops back.
        rr.peak, rr.floor, rr.trailing_activated = existing.peak, existing.floor, existing.trailing_activated
        rr.member_mtm = dict(existing.member_mtm)
    for s in members:
        rr.member_mtm[s.strategy_id] = s.mtm
    rr.last_mtm = sum(rr.member_mtm.values()) if payload.scope == "group" else members[0].mtm
    token_router.runtime_risks[key] = rr
    await publish_runtime_risk(rr)
    return {"success": True, "risk": runtime_risk.to_doc(rr)}


@router.post("/{scope}/{target_id}/disable")
async def disable_runtime_risk(scope: str, target_id: str, user: dict = Depends(get_current_user)) -> dict:
    from main import token_router

    key = runtime_risk.risk_key(scope, target_id)
    token_router.runtime_risks.pop(key, None)
    await asyncio.to_thread(get_mongo().raw[runtime_risk.COLLECTION].update_one, {"_id": key}, {"$set": {"status": "OFF"}})
    return {"success": True, "status": "OFF"}
