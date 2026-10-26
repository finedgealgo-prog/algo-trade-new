"""
routers/auto_activation.py
──────────────────────────
Portfolio "Auto Activate" settings (FastForward2 / AlgoTrade2) — see
services/auto_activation.py for when and how the daily 05:31 IST run works.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services import auto_activation, broker_scope
from shared.auth.dependency import get_current_user
from shared.db.mongo import get_mongo

router = APIRouter(prefix="/algo/auto-activation", tags=["auto-activation"])


class SaveAutoActivationRequest(BaseModel):
    # Exactly one: a portfolio, or a single strategy.
    portfolio_id: Optional[str] = None
    strategy_id: Optional[str] = None
    activation_mode: str = "fast-forward"
    broker_scope_id: str
    slippage_pct: float = 0.0
    portfolio_qty: int = 1
    # Final per-strategy qty (row qty × portfolio qty) from the popup.
    strategy_qty: dict[str, int] = {}
    # Strategy target: its qty multiplier.
    qty_multiplier: int = 1
    # Portfolio target: strategies the user unticked — never auto-activated.
    excluded_strategy_ids: list[str] = []


class DisableAutoActivationRequest(BaseModel):
    portfolio_id: Optional[str] = None
    strategy_id: Optional[str] = None
    activation_mode: str = "fast-forward"


def _target(portfolio_id: Optional[str], strategy_id: Optional[str]) -> tuple[str, str]:
    pid, sid = (portfolio_id or "").strip(), (strategy_id or "").strip()
    if bool(pid) == bool(sid):
        raise HTTPException(status_code=400, detail="Give either portfolio_id or strategy_id")
    return ("portfolio", pid) if pid else ("strategy", sid)


def _public(doc: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in doc.items() if k not in ("strategy_qty",)} | {"strategy_qty": doc.get("strategy_qty") or {}}


@router.get("/list")
def list_auto_activations(activation_mode: str = "", user: dict = Depends(get_current_user)) -> dict:
    query: dict[str, Any] = {}
    if user.get("_id"):
        query["user_id"] = str(user["_id"])
    if activation_mode:
        query["activation_mode"] = activation_mode
    return {"items": [_public(d) for d in get_mongo().raw[auto_activation.COLLECTION].find(query)]}


@router.get("/runs")
def list_auto_activation_runs(target_id: str = "", limit: int = 30, user: dict = Depends(get_current_user)) -> dict:
    """This user's auto activation history (newest first)."""
    query: dict[str, Any] = {}
    if user.get("_id"):
        query["user_id"] = str(user["_id"])
    if target_id:
        query["target_id"] = target_id
    docs = get_mongo().raw[auto_activation.RUNS_COLLECTION].find(query, {"_id": 0}).sort("ran_at", -1).limit(max(1, min(limit, 200)))
    return {"runs": list(docs)}


@router.post("/save")
async def save_auto_activation(payload: SaveAutoActivationRequest, user: dict = Depends(get_current_user)) -> dict:
    mode = payload.activation_mode.strip() or "fast-forward"
    if mode not in broker_scope.ACTIVATION_MODES:
        raise HTTPException(status_code=400, detail=f"Unsupported activation_mode '{mode}'")
    if not payload.broker_scope_id.strip():
        raise HTTPException(status_code=400, detail="Select a broker")
    mongo = get_mongo()
    target_type, target_id = _target(payload.portfolio_id, payload.strategy_id)
    collection = "saved_portfolios" if target_type == "portfolio" else "saved_strategies"
    try:
        target = await asyncio.to_thread(mongo.raw[collection].find_one, {"_id": ObjectId(target_id)}, {"name": 1})
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid {target_type} id")
    if not target:
        raise HTTPException(status_code=404, detail=f"{target_type.title()} not found")
    if mode == broker_scope.LIVE_MODE:
        from main import order_engine

        live = getattr(order_engine, "live", None)
        if live is None or not live.enabled:
            raise HTTPException(status_code=422, detail="Real orders are switched off on the server (LIVE_ORDER_ENABLED)")
        ready, reason = await asyncio.to_thread(live.check_ready, payload.broker_scope_id)
        if not ready:
            raise HTTPException(status_code=422, detail=reason)

    today = auto_activation.now_ist()
    cfg_id = auto_activation.config_id(target_id, mode, target_type)
    existing = await asyncio.to_thread(mongo.raw[auto_activation.COLLECTION].find_one, {"_id": cfg_id}) or {}
    doc: dict[str, Any] = {
        "target_type": target_type, "target_id": target_id, "target_name": target.get("name") or "",
        "activation_mode": mode, "user_id": str(user.get("_id") or ""),
        "broker_scope_id": payload.broker_scope_id.strip(),
        "slippage_pct": max(0.0, float(payload.slippage_pct or 0)),
        "portfolio_qty": max(1, int(payload.portfolio_qty or 1)),
        "strategy_qty": {str(k): max(1, int(v or 1)) for k, v in payload.strategy_qty.items()},
        "qty_multiplier": max(1, int(payload.qty_multiplier or 1)),
        "excluded_strategy_ids": [str(i) for i in payload.excluded_strategy_ids if str(i).strip()],
        "enabled": True, "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if target_type == "portfolio":
        doc.update({"portfolio_id": target_id, "portfolio_name": doc["target_name"]})
    else:
        doc.update({"strategy_id": target_id})
    if not existing.get("enabled"):
        # Turned on after today's 17:31 -> first automatic run is tomorrow
        # (activate today with the normal Activate button if needed).
        doc["last_run_date"] = today.strftime("%Y-%m-%d") if today.time() >= auto_activation.RUN_AT_IST else existing.get("last_run_date", "")
        doc["enabled_at"] = doc["updated_at"]
    await asyncio.to_thread(mongo.raw[auto_activation.COLLECTION].update_one, {"_id": cfg_id}, {"$set": doc}, upsert=True)
    saved = await asyncio.to_thread(mongo.raw[auto_activation.COLLECTION].find_one, {"_id": cfg_id})
    return {"success": True, "item": _public(saved)}


@router.post("/disable")
def disable_auto_activation(payload: DisableAutoActivationRequest, user: dict = Depends(get_current_user)) -> dict:
    target_type, target_id = _target(payload.portfolio_id, payload.strategy_id)
    query: dict[str, Any] = {"_id": auto_activation.config_id(target_id, payload.activation_mode or "fast-forward", target_type)}
    if user.get("_id"):
        query["user_id"] = str(user["_id"])
    res = get_mongo().raw[auto_activation.COLLECTION].update_one(
        query, {"$set": {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()}},
    )
    return {"success": True, "disabled": res.modified_count}
