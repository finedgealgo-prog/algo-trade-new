"""
routers/portfolio.py
────────────────────────
Portfolio = a named group of saved strategies, activated together. Fresh,
simplified routes (the old system's portfolio pipeline — shadow docs,
group execution, `saved_portfolios`/`executed_strategies` — is not ported
wholesale; this is a thin group wrapper over the already-working single-
strategy `POST /algo/strategy/{id}/activate` path, calling
services/strategy_activation.activate_strategy once per member strategy).
"""

from __future__ import annotations

import asyncio
import time

import re
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

router = APIRouter(prefix="/algo/portfolio", tags=["portfolio"])

_COLLECTION = "saved_portfolios"
_STRATEGY_COLLECTION = "saved_strategies"


class SavePortfolioRequest(BaseModel):
    name: str
    strategy_ids: list[str] = []
    # Edit/update path (Backtest/Portfolio.tsx) — absent on create.
    portfolio_id: Optional[str] = None
    strategies: list[dict[str, Any]] = []
    qty_multiplier: Optional[int] = None
    is_weekdays: Optional[bool] = None


class ActivatePortfolioRequest(BaseModel):
    broker_scope_id: str
    # "fast-forward" (FastForward2, paper) / "live" (AlgoTrade2, real orders).
    activation_mode: Optional[str] = None


class ExecutionSettingsBody(BaseModel):
    execution_config_base: dict[str, Any] = {}
    execution_config_extra: dict[str, Any] = {}


class ExecutionSettingsUpdateRequest(BaseModel):
    portfolio_id: Optional[str] = None
    source_strategy_id: str
    activation_mode: Optional[str] = None
    execution_settings: ExecutionSettingsBody


def _serialize(doc: dict) -> dict:
    doc = dict(doc)
    doc["_id"] = str(doc["_id"])
    # Some pre-existing docs (saved by the old system) store strategy_ids as
    # raw BSON ObjectId, not strings — FastAPI's JSON encoder can't
    # serialize ObjectId, so this 500'd on /list until normalized here.
    if doc.get("strategy_ids"):
        doc["strategy_ids"] = [str(sid) for sid in doc["strategy_ids"]]
    return doc


@router.post("/save")
def save_portfolio(payload: SavePortfolioRequest, user: dict = Depends(get_current_user)) -> dict:
    """Create, or update in place when portfolio_id is given — ported from the
    old system's /portfolio/save (algo.trade/api.py), incl. per-strategy meta
    (checked/dte/weekdays/qty/slippage) and portfolio-level qty/is_weekdays."""
    mongo = get_mongo()
    col = mongo.raw[_COLLECTION]
    user_id = str(user.get("_id") or "")
    name = payload.name.strip()

    strategy_ids = payload.strategy_ids
    if payload.strategies:
        strategy_ids = [str(m["id"]) for m in payload.strategies if m.get("id")]
    if not name:
        raise HTTPException(status_code=400, detail="Portfolio name is required")
    if not strategy_ids:
        raise HTTPException(status_code=400, detail="At least one strategy must be selected")

    existing = None
    if payload.portfolio_id:
        try:
            existing = col.find_one({"_id": ObjectId(payload.portfolio_id)})
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid portfolio_id")
        if existing and user_id and existing.get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="You do not have access to this portfolio")

    name_query: dict[str, Any] = {"name": name, "user_id": user_id}
    if existing:
        name_query["_id"] = {"$ne": existing["_id"]}
    if col.find_one(name_query, {"_id": 1}):
        raise HTTPException(status_code=409, detail=f"Portfolio '{name}' already exists")

    if not all(ObjectId.is_valid(sid) for sid in strategy_ids):
        bad = next(sid for sid in strategy_ids if not ObjectId.is_valid(sid))
        raise HTTPException(status_code=400, detail=f"Invalid strategy_id: {bad}")
    object_ids = [ObjectId(sid) for sid in strategy_ids]
    found = {d["_id"] for d in mongo.raw[_STRATEGY_COLLECTION].find({"_id": {"$in": object_ids}}, {"_id": 1})}
    missing = [str(oid) for oid in object_ids if oid not in found]
    if missing:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {missing[0]}")

    meta_map = {str(m["id"]): m for m in payload.strategies if m.get("id")}
    strategies_meta = []
    for sid in strategy_ids:
        meta = meta_map.get(sid, {})
        entry = {
            "id": sid,
            "checked": bool(meta.get("checked", True)),
            "dte": meta.get("dte") or [0],
            "dte_gate_enabled": bool(meta.get("dte_gate_enabled", False)),
            "qty_multiplier": meta.get("qty_multiplier", 1),
            "slippage": meta.get("slippage", 0),
            "weekdays": meta.get("weekdays") or ["M", "T", "W", "Th", "F"],
        }
        if meta.get("budget_days"):
            entry["budget_days"] = meta["budget_days"]
        strategies_meta.append(entry)

    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "name": name, "user_id": user_id,
        # ObjectId, same as the old system's writer — legacy_api readers
        # query saved_strategies with these values as-is.
        "strategy_ids": object_ids,
        "strategies": strategies_meta,
        "qty_multiplier": int(payload.qty_multiplier or 1),
        "is_weekdays": True if payload.is_weekdays is None else bool(payload.is_weekdays),
        "updated_at": now,
    }
    if existing:
        col.update_one({"_id": existing["_id"]}, {"$set": doc})
        result_id = existing["_id"]
    else:
        doc["created_at"] = now
        result_id = col.insert_one(doc).inserted_id

    return {
        "success": True, "id": str(result_id), "portfolio_id": str(result_id), "name": name,
        "strategy_ids": strategy_ids, "strategies": strategies_meta,
        "qty_multiplier": doc["qty_multiplier"], "is_weekdays": doc["is_weekdays"],
    }


@router.get("/list")
def list_portfolios(user: dict = Depends(get_current_user)) -> dict:
    mongo = get_mongo()
    # Same dev/test fallback as strategy.py's list route — with auth
    # enforcement disabled there's no real per-user identity to filter by.
    query = {"user_id": str(user["_id"])} if user.get("_id") else {}
    # Newest first — ObjectId carries the creation time (created_at strings
    # come in several formats across old/new writers, _id is uniform).
    docs = [_serialize(d) for d in mongo.raw[_COLLECTION].find(query).sort("_id", -1)]
    # Portfolios page renders strategy_names per card — resolve them here
    # (the old system's /portfolio/list did the same; docs don't store names).
    all_ids = {sid for d in docs for sid in d.get("strategy_ids") or []}
    oids = [ObjectId(sid) for sid in all_ids if ObjectId.is_valid(sid)]
    name_map = {
        str(s["_id"]): s.get("name") or ""
        for s in mongo.raw[_STRATEGY_COLLECTION].find({"_id": {"$in": oids}}, {"name": 1})
    } if oids else {}
    for d in docs:
        d.setdefault("strategy_ids", [])
        d["strategy_names"] = [name_map[sid] for sid in d["strategy_ids"] if name_map.get(sid)]
    return {"portfolios": docs}


@router.get("/{portfolio_id}")
def get_portfolio(portfolio_id: str) -> dict:
    mongo = get_mongo()
    try:
        doc = mongo.raw[_COLLECTION].find_one({"_id": ObjectId(portfolio_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio id")
    if not doc:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    doc = _serialize(doc)
    doc.setdefault("strategy_ids", [])

    # The doc's own `strategies` is per-strategy meta only ({id, checked,
    # dte, ...}) — join name/underlying/product from saved_strategies, in
    # strategy_ids order, same shape the old system's /portfolio/{id} returned
    # (Backtest/Portfolio.tsx, PortfolioActivation.tsx read s._id / s.name).
    meta_map = {
        str(m.get("id") or m.get("_id") or ""): m
        for m in doc.get("strategies") or [] if isinstance(m, dict)
    }
    oids = [ObjectId(sid) for sid in doc["strategy_ids"] if ObjectId.is_valid(sid)]
    strategy_map = {
        str(s["_id"]): s
        for s in mongo.raw[_STRATEGY_COLLECTION].find(
            {"_id": {"$in": oids}}, {"name": 1, "underlying": 1, "full_config": 1},
        )
    } if oids else {}

    ordered = []
    for sid in doc["strategy_ids"]:
        item = strategy_map.get(sid)
        if not item:
            continue
        strategy_config = (item.get("full_config") or {}).get("strategy") or {}
        meta = meta_map.get(sid, {})
        ordered.append({
            "_id": sid,
            "id": sid,
            "name": item.get("name") or "",
            "underlying": item.get("underlying") or strategy_config.get("Ticker") or "",
            "product": strategy_config.get("Product") or "INTRADAY",
            "checked": bool(meta.get("checked", True)),
            "dte": meta.get("dte") or [0],
            "dte_gate_enabled": bool(meta.get("dte_gate_enabled", False)),
            "qty_multiplier": meta.get("qty_multiplier", 1),
            "slippage": meta.get("slippage", 0),
            "weekdays": meta.get("weekdays") or ["M", "T", "W", "Th", "F"],
            **({"budget_days": meta["budget_days"]} if meta.get("budget_days") else {}),
        })
    doc["strategies"] = ordered
    doc["qty_multiplier"] = doc.get("qty_multiplier", 1)
    doc["is_weekdays"] = bool(doc.get("is_weekdays", True))
    return doc


_GROUP_NAME_SUFFIX_RE = re.compile(r" \((\d+)\)$")


def _resolve_activation_group(mongo, token_router, portfolio_id: str, portfolio_doc=None) -> tuple[str, str]:
    """Resolves the (group_id, group_name) ONE portfolio-activation batch
    should use for every strategy in it — ported exactly from the old
    system's real logic (algo.trade/api.py:3773 `activation_batch_group_id
    = str(ObjectId())`, :3820-3843's name dedup) since the user's own ask
    was byte-for-byte parity here, not a fresh design:

      group_name: the portfolio's own name, stripped of any trailing
      " (N)" — unless a group with that exact name is ALREADY active right
      now (a previous, still-running activation of this same portfolio),
      in which case it's suffixed " (N+1)" past whatever's already in use.

      group_id: a FRESH id, unique to this ONE activation batch — NOT the
      portfolio's own static _id, so a second activation of the same
      portfolio while the first is still running creates a genuinely
      separate, newly-numbered group instead of merging into the existing
      one (the same live semantics the old system's fresh-ObjectId-per-
      request gives, checked against the currently-registered strategies
      in token_router — the live equivalent of the old system's Mongo
      query, since token_router.strategies already IS this engine's live,
      authoritative "what's active right now").

    Called ONCE per activation batch — activate_prepared_trades (the REAL,
    actually-used path: FastForward2.tsx embeds the OLD PortfolioActivation.
    tsx, which posts its whole trades[] array to THIS one endpoint in ONE
    call — confirmed via App.tsx's route table + FastForward2.tsx's own
    <PortfolioActivation .../> embed, NOT the unmounted PortfolioActivation2.
    tsx this logic was originally, wrongly, wired to only from the frontend
    side) calls this before its loop, reusing the SAME resolved pair for
    every trade in the batch — resolving it separately per trade would
    re-roll a FRESH group_id each time and could disagree on the numbered
    name if two trades raced, splitting one activation across two groups."""
    doc = portfolio_doc
    if doc is None:
        try:
            doc = mongo.raw[_COLLECTION].find_one({"_id": ObjectId(portfolio_id)})
        except Exception:
            doc = None
    if not doc:
        return "", ""

    base_name = _GROUP_NAME_SUFFIX_RE.sub("", str(doc.get("name") or "").strip()).strip() or "Portfolio Activation"
    name_pattern = re.compile(rf"^{re.escape(base_name)}(?: \((\d+)\))?$")
    existing_names = [
        s.group_name for s in token_router.strategies.values()
        if s.status == "ACTIVE" and s.portfolio_id == portfolio_id and s.group_name and name_pattern.match(s.group_name)
    ]

    if base_name not in existing_names:
        resolved_name = base_name
    else:
        highest = 0
        for name in existing_names:
            if name == base_name:
                continue
            m = _GROUP_NAME_SUFFIX_RE.search(name)
            if m:
                highest = max(highest, int(m.group(1)))
        resolved_name = f"{base_name} ({highest + 1})"

    # ObjectId-style (24 hex chars), not uuid4 — matches the old system's
    # real activation_batch_group_id = str(ObjectId()) exactly (a live
    # captured payload's group_id/strategy_group_id are both this shape).
    return str(ObjectId()), resolved_name


@router.get("/{portfolio_id}/resolve-activation-group")
def resolve_activation_group(portfolio_id: str) -> dict:
    """Thin HTTP wrapper around _resolve_activation_group — kept as its own
    endpoint for any caller that needs to resolve a group ahead of a
    per-strategy activation loop it drives itself (e.g. a future portfolio-
    activation UI built directly against /strategy/{id}/activate instead of
    this router's own bulk /activate)."""
    mongo = get_mongo()
    try:
        ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio id")
    if not mongo.raw[_COLLECTION].find_one({"_id": ObjectId(portfolio_id)}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="Portfolio not found")

    from main import token_router
    group_id, group_name = _resolve_activation_group(mongo, token_router, portfolio_id)
    return {"group_id": group_id, "group_name": group_name}


@router.post("/execution-settings/update")
def update_execution_settings(payload: ExecutionSettingsUpdateRequest) -> dict:
    """EditExecutionModal2.tsx's per-leg execution config save (product
    type, entry/exit order type, MPP). algo-2_0 doesn't model per-portfolio-
    member overrides separately — this writes straight onto the strategy
    doc (source_strategy_id), same place StrategySetupModal2 writes
    execution_config_base. NOTE: order-type/MIS-NRML config has no effect
    on algo-2_0's actual entry/exit yet — VirtualBrokerAdapter always fills
    instantly at market price regardless (real-order concerns were
    explicitly deferred, see orders/order_engine.py's docstring) — this
    just makes the save itself work end-to-end against algo-2_0 without
    touching the old backend, per the user's instruction."""
    mongo = get_mongo()
    try:
        oid = ObjectId(payload.source_strategy_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy id")
    result = mongo.raw[_STRATEGY_COLLECTION].update_one(
        {"_id": oid},
        {"$set": {
            "execution_config_base": payload.execution_settings.execution_config_base,
            "execution_config_extra": payload.execution_settings.execution_config_extra,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"success": True, "id": payload.source_strategy_id}


# PortfolioActivation.tsx sends the activation screen's Qty Multiplier
# (row × portfolio qty) only to /portfolio/prepare-activation, not in the
# /portfolio/activate trades[] that follow it — the old backend persisted it
# on executed_strategies. Kept here in memory for the few seconds between the
# two calls, keyed by (user, source strategy).
_ACTIVATION_MULTIPLIER_TTL_SECONDS = 600
_activation_multipliers: dict[tuple[str, str], tuple[float, int]] = {}


def _positive_int(value) -> int | None:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _remember_activation_multiplier(user_id: str, strategy_id: str, value) -> None:
    number = _positive_int(value)
    if strategy_id and number:
        _activation_multipliers[(user_id, strategy_id)] = (time.monotonic(), number)


def _take_activation_multiplier(user_id: str, strategy_id: str) -> int | None:
    now = time.monotonic()
    for key in [k for k, (ts, _v) in _activation_multipliers.items() if now - ts > _ACTIVATION_MULTIPLIER_TTL_SECONDS]:
        _activation_multipliers.pop(key, None)
    entry = _activation_multipliers.pop((user_id, strategy_id), None)
    return entry[1] if entry else None


@router.post("/prepare-activation")
def prepare_activation(payload: dict, user: dict = Depends(get_current_user)) -> dict:
    """Path/shape-compatible with the old algo.trade's POST
    /algo/portfolio/prepare-activation — PortfolioActivation.tsx (the old,
    still-embedded-in-FastForward2 modal) calls this before /activate and
    needs an `executed_strategies` array back to build its own trades[]
    payload. algo-2_0 has no separate "assigned execution identity" step —
    activate_strategy() below generates its own exec_<uuid> id at real
    entry time — so every field here is just echoed straight from the
    request, not a newly persisted identity; nothing on algo-2_0's side
    reads a pre-assigned id before the real /portfolio/activate call."""
    portfolio_id = str(payload.get("portfolio_id") or "").strip()
    strategies = payload.get("strategies") or []
    user_id = str(user.get("_id") or "")
    for s in strategies:
        if isinstance(s, dict):
            _remember_activation_multiplier(user_id, str(s.get("strategy_id") or ""), s.get("qty_multiplier"))
    executed_strategies = [
        {
            "source_strategy_id": str(s.get("strategy_id") or ""),
            "assigned_strategy_id": str(s.get("strategy_id") or ""),
            "number_of_executions": 1,
            "ticker": str(s.get("ticker") or "").upper(),
            "trade_portfolio_id": portfolio_id,
            "trade_index": str(s.get("ticker") or "").upper(),
        }
        for s in strategies if isinstance(s, dict)
    ]
    return {"portfolio_id": portfolio_id, "trade_portfolio_id": portfolio_id, "executed_strategies": executed_strategies}


@router.post("/activate")
async def activate_prepared_trades(payload: dict, user: dict = Depends(get_current_user)) -> dict:
    """Path/shape-compatible with the old algo.trade's POST
    /algo/portfolio/activate — accepts the same client-built `trades[]`
    array PortfolioActivation.tsx already sends (see that page's own
    handleActivate), but instead of the old system's insert-then-defer-to-
    a-background-monitor model, enters each trade for real, synchronously,
    via the SAME activate_strategy() the single-strategy
    POST /algo/strategy/{id}/activate route uses (services/
    strategy_activation.py) — algo-2_0 has no separate portfolio-activation
    background monitor to defer entry to.

    `trade["broker"]` here is a broker_configuration _id (from the old
    GET /algo/broker-configurations picker this page's broker dropdown
    still uses), not a real registered broker_scope_id —
    activate_strategy()'s `router.brokers.get(broker_scope_id)` returns
    None for an unrecognized id, which its own check treats as "no broker
    gate", so this works fine as a plain scope tag without that
    broker_configuration doc needing to be registered as a real
    BrokerRuntime first."""
    from main import order_engine, token_router

    mongo = get_mongo()
    trades = payload.get("trades") or []
    if not isinstance(trades, list) or not trades:
        raise HTTPException(status_code=400, detail="At least one trade record is required")

    # THE real portfolio-activation entry point (see _resolve_activation_
    # group's own docstring for how this was confirmed) — group_id/
    # group_name resolved ONCE here, before the loop, so every trade in
    # this one batch shares it instead of each independently re-rolling a
    # fresh id (which is exactly how every strategy here ended up with an
    # EMPTY portfolio.group_id/group_name before this fix: is_direct_
    # strategy=False was hardcoded, but nothing ever passed group_id/
    # group_name/portfolio_id through to activate_strategy() at all).
    portfolio_id = str(payload.get("portfolio_id") or "").strip()
    group_doc = {}
    if portfolio_id:
        try:
            group_doc = await asyncio.to_thread(mongo.raw[_COLLECTION].find_one, {"_id": ObjectId(portfolio_id)}) or {}
        except Exception:
            pass
    group_id, group_name = _resolve_activation_group(mongo, token_router, portfolio_id, group_doc)

    user_id = str(user.get("_id") or "")
    # Activation screen's Slippage % — applies to every strategy activated in
    # this call, on entry and exit fill prices only (orders/slippage.py).
    try:
        activation_slippage = max(0.0, float(payload.get("slippage_pct") or 0))
    except (TypeError, ValueError):
        activation_slippage = 0.0
    results: list[dict[str, Any]] = []
    for item in trades:
        if not isinstance(item, dict):
            continue
        strategy_id = str(item.get("strategy_id") or "").strip()
        broker_scope_id = str(item.get("broker") or "").strip()
        activation_multiplier = _positive_int(item.get("qty_multiplier") or item.get("multiplier")) \
            or _take_activation_multiplier(user_id, strategy_id)
        try:
            strategy_doc = await asyncio.to_thread(mongo.raw[_STRATEGY_COLLECTION].find_one, {"_id": ObjectId(strategy_id)})
        except Exception:
            results.append({"strategy_id": strategy_id, "success": False, "error": "invalid strategy id"})
            continue
        if not strategy_doc:
            results.append({"strategy_id": strategy_id, "success": False, "error": "strategy not found"})
            continue
        try:
            result = await activate_strategy(
                token_router, order_engine, get_instrument_master(), get_ltp_cache(), mongo,
                strategy_doc, user_id, broker_scope_id, is_direct_strategy=not portfolio_id,
                group_id=group_id, group_name=group_name, portfolio_id=portfolio_id,
                qty_multiplier=activation_multiplier,
                slippage_pct=activation_slippage,
                # PortfolioActivation.tsx stamps each trade with its page's mode.
                activation_mode=str(item.get("activation_mode") or payload.get("activation_mode") or "").strip() or "fast-forward",
            )
            results.append({"strategy_id": strategy_id, "success": True, **result})
        except ActivationError as exc:
            results.append({"strategy_id": strategy_id, "success": False, "error": str(exc)})

    if not any(r.get("success") for r in results):
        first_error = next((r.get("error") for r in results if not r.get("success")), "Activation failed")
        raise HTTPException(status_code=400, detail=first_error)

    # See strategy.py's /{strategy_id}/activate for why this push is needed —
    # activate_strategy() itself never emits, so an already-open execute-
    # orders socket would otherwise only learn about these strategies off
    # the next real tick on one of their leg tokens.
    from api.live_broadcast import broadcast_strategy_snapshots
    await broadcast_strategy_snapshots(token_router, [r["strategy_id"] for r in results if r.get("success")])

    return {"success": True, "results": results}


@router.post("/{portfolio_id}/activate")
async def activate_portfolio(portfolio_id: str, payload: ActivatePortfolioRequest, user: dict = Depends(get_current_user)) -> dict:
    from main import order_engine, token_router

    mongo = get_mongo()
    try:
        portfolio_doc = await asyncio.to_thread(mongo.raw[_COLLECTION].find_one, {"_id": ObjectId(portfolio_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio id")
    if not portfolio_doc:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    group_id, group_name = _resolve_activation_group(mongo, token_router, portfolio_id, portfolio_doc)

    user_id = str(user.get("_id") or "")
    results: list[dict[str, Any]] = []
    for strategy_id in portfolio_doc.get("strategy_ids") or []:
        # Same ObjectId-not-JSON-serializable issue /list's _serialize() was
        # patched for — this route reads portfolio_doc raw (not through
        # _serialize()), so strategy_ids here can still be actual ObjectId
        # values on older docs, not strings.
        strategy_id = str(strategy_id)
        try:
            strategy_doc = await asyncio.to_thread(mongo.raw[_STRATEGY_COLLECTION].find_one, {"_id": ObjectId(strategy_id)})
        except Exception:
            results.append({"strategy_id": strategy_id, "success": False, "error": "invalid strategy id"})
            continue
        if not strategy_doc:
            results.append({"strategy_id": strategy_id, "success": False, "error": "strategy not found"})
            continue
        try:
            result = await activate_strategy(
                token_router, order_engine, get_instrument_master(), get_ltp_cache(), mongo,
                strategy_doc, user_id, payload.broker_scope_id, is_direct_strategy=False,
                group_id=group_id, group_name=group_name, portfolio_id=portfolio_id,
                activation_mode=(payload.activation_mode or "").strip() or "fast-forward",
            )
            results.append({"strategy_id": strategy_id, "success": True, **result})
        except ActivationError as exc:
            results.append({"strategy_id": strategy_id, "success": False, "error": str(exc)})

    # See strategy.py's /{strategy_id}/activate for why this push is needed.
    from api.live_broadcast import broadcast_strategy_snapshots
    await broadcast_strategy_snapshots(token_router, [r["strategy_id"] for r in results if r.get("success")])

    return {"portfolio_id": portfolio_id, "results": results}
