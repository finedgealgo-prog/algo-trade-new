"""
routers/straddle_alert.py
─────────────────────────
REST for FastForward2.tsx's crypto "Straddle Alert" — create / list / stop.
The 30-second monitor itself is services/straddle_alert.py's loop (started
from main.py's on_startup).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Literal, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from features.delta_exchange_client import list_expiries
from services import straddle_alert as svc
from shared.auth.dependency import get_current_user
from shared.db.mongo import get_mongo

router = APIRouter(prefix="/algo/straddle-alerts", tags=["straddle-alerts"])

SUPPORTED_UNDERLYINGS = ("BTC", "ETH")


class TriggerStrategy(BaseModel):
    id: str
    name: str = ""
    # Final multiplier for this strategy (row qty × portfolio qty, as the
    # PortfolioActivation screen computes it).
    qty_multiplier: int = Field(default=1, ge=1)


class OnTrigger(BaseModel):
    """What to activate the moment the alert fires (entry time ignored)."""
    kind: Literal["strategy", "portfolio"]
    id: str
    name: str = ""
    broker: str = ""  # broker_configuration _id; blank → each strategy's own saved broker
    broker_name: str = ""  # display only (FastForward2 deployed list)
    qty_multiplier: Optional[int] = Field(default=None, ge=1)
    # Portfolio only: the exact rows ticked on the PortfolioActivation screen.
    # Omitted → today's-weekday / checked members are picked at trigger time.
    strategies: Optional[list[TriggerStrategy]] = None
    slippage_pct: float = Field(default=0.0, ge=0)


class CreateStraddleAlertRequest(BaseModel):
    underlying: str = "BTC"
    expiry: str
    rise_pct: float = Field(default=30.0, gt=0, le=1000)
    notify_mobile: str = ""
    on_trigger: Optional[OnTrigger] = None


def _serialize(doc: dict) -> dict:
    doc = dict(doc)
    doc["_id"] = str(doc["_id"])
    return doc


def _coll():
    return get_mongo().raw[svc.COLLECTION]


@router.get("/expiries")
async def straddle_alert_expiries(underlying: str = "BTC") -> dict:
    underlying = underlying.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'.")
    return {"underlying": underlying, "expiries": await asyncio.to_thread(list_expiries, underlying)}


@router.get("")
def list_straddle_alerts(user: dict = Depends(get_current_user)) -> dict:
    # Today's alerts only (since 00:00 UTC = 05:30 IST).
    query: dict = {"created_at": {"$gte": svc.day_start_iso()}}
    if user.get("_id"):
        query["user_id"] = str(user.get("_id"))
    docs = _coll().find(query).sort("created_at", -1).limit(100)
    return {"alerts": [_serialize(d) for d in docs]}


@router.post("")
async def create_straddle_alert(payload: CreateStraddleAlertRequest, user: dict = Depends(get_current_user)) -> dict:
    underlying = payload.underlying.strip().upper()
    if underlying not in SUPPORTED_UNDERLYINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported underlying '{underlying}'.")
    expiry = payload.expiry.strip()
    if svc._expiry_cutoff(expiry) is None:
        raise HTTPException(status_code=400, detail="expiry must be DD-MM-YYYY.")

    on_trigger = None
    if payload.on_trigger:
        on_trigger = payload.on_trigger.model_dump()
        coll_name = "saved_strategies" if on_trigger["kind"] == "strategy" else "saved_portfolios"
        try:
            target = get_mongo().raw[coll_name].find_one({"_id": ObjectId(on_trigger["id"])}, {"name": 1, "broker": 1})
        except Exception:
            target = None
        if not target:
            raise HTTPException(status_code=404, detail=f"{on_trigger['kind'].capitalize()} not found.")
        on_trigger["name"] = on_trigger["name"] or str(target.get("name") or "")
        if on_trigger["kind"] == "strategy" and not on_trigger["broker"] and not target.get("broker"):
            raise HTTPException(status_code=422, detail="Pick a broker — this strategy has none saved in Edit Setup.")
        if on_trigger["kind"] == "portfolio" and not on_trigger["broker"]:
            raise HTTPException(status_code=422, detail="Pick a broker for the portfolio activation.")
        if on_trigger["kind"] == "portfolio" and on_trigger["strategies"] is not None and not on_trigger["strategies"]:
            raise HTTPException(status_code=422, detail="Select at least one strategy to activate.")

    # First reading right away so "lowest since start" begins at start.
    try:
        reading = await asyncio.to_thread(svc.compute_atm_straddle, underlying, expiry)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Delta chain fetch failed: {exc}")
    if reading is None:
        raise HTTPException(status_code=502, detail="No ATM CE/PE quote on Delta for that expiry right now.")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {
        "user_id": str(user.get("_id") or ""),
        "underlying": underlying,
        "expiry": expiry,
        "rise_pct": payload.rise_pct,
        "notify_mobile": payload.notify_mobile.strip() or str(user.get("mobile") or ""),
        "on_trigger": on_trigger,
        "status": svc.STATUS_MONITORING,
        "created_at": now_iso,
        "last_checked_at": now_iso,
        "start_reading": reading,
        "current": reading,
        "lowest_straddle": reading["straddle"],
        "lowest_at": now_iso,
        "lowest_reading": reading,
    }
    result = _coll().insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"alert": _serialize(doc)}


@router.post("/{alert_id}/stop")
def stop_straddle_alert(alert_id: str, user: dict = Depends(get_current_user)) -> dict:
    try:
        oid = ObjectId(alert_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid alert id.")
    query: dict = {"_id": oid, "status": svc.STATUS_MONITORING}
    if user.get("_id"):
        query["user_id"] = str(user["_id"])
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    res = _coll().update_one(query, {"$set": {"status": svc.STATUS_STOPPED, "stopped_at": now_iso}})
    if not res.matched_count:
        raise HTTPException(status_code=404, detail="No active alert with that id.")
    return {"status": "stopped", "_id": alert_id}


# Monitors page row (algo-admin src/pages/Admin/Monitors.tsx). Separate
# prefix so "/monitor/stop" can't collide with "/{alert_id}/stop" above.
monitor_router = APIRouter(prefix="/algo/straddle-alert-monitor", tags=["straddle-alerts"])


@monitor_router.get("/status")
def straddle_monitor_status() -> dict:
    return svc.loop_status()


@monitor_router.api_route("/start", methods=["GET", "POST"])
async def straddle_monitor_start(user: dict = Depends(get_current_user)) -> dict:
    return svc.start_loop()


@monitor_router.api_route("/stop", methods=["GET", "POST"])
async def straddle_monitor_stop(user: dict = Depends(get_current_user)) -> dict:
    return await svc.stop_loop()
