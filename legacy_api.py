"""
legacy_api.py
─────────────
Routes copied over from the old algo-backend/algo.trade/api.py (port 8000)
that algo-2_0 did not have yet — backtest, payment/plans, span/margin,
option-chain cache/backfill, trade-static, monitor/mock, chart, fast-backtest,
etc. Handlers algo-2_0 already re-implemented (auth, strategy, portfolio,
broker setup/config, dhan/kite/flattrade login, live, mtm) were removed from
this copy.

`app` below is never served on its own — main.py calls
mount_missing_routes(app) to copy every route from here that its own app
doesn't already have, so algo-2_0's own version always wins.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import multiprocessing
import orjson
import os
import re
import secrets
import threading

import pathlib as _pathlib
from dotenv import load_dotenv
# Old features/* read their config (Kite/Flattrade/Razorpay keys etc.) from
# shared/.env — algo-2_0/.env is loaded first by main.py, so it wins on overlap.
load_dotenv(_pathlib.Path(__file__).resolve().parent.parent.parent / "shared" / ".env")

log = logging.getLogger(__name__)
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from bson import ObjectId
from pymongo.errors import DuplicateKeyError
from fastapi import FastAPI, HTTPException, APIRouter, Query, Request, Response, UploadFile, File, Depends
from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse, JSONResponse, ORJSONResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from starlette.routing import WebSocketRoute

from features.nse_market_hours import nse_trade_date
from features.backtest_engine import run_backtest
from features.iron_condor_v2_backtest import run_backtest as run_iron_condor_v2_backtest
from features.portfolio_worker import strategy_worker
from features.mongo_data     import MongoData
from features.expiry_config  import seed_expiry_config
from features.time_utils     import to_utc_iso
from features import auth as app_auth
from features.broker_gateway import (
    broker_get_login_url            as get_login_url,
    broker_generate_session         as generate_session,
    get_broker_rest_client_with_token as get_kite_instance,
    save_broker_session             as save_kite_session,
    get_stored_broker_access_token  as get_stored_access_token,
    broker_ticker_manager           as ticker_manager,
)
from features.mock_ticker import mock_ticker_manager
from features.market_hours_scheduler import run_market_hours_scheduler
from features.broker_gateway import (
    load_broker_instruments         as _load_kite_instruments,
    BROKER_INDEX_TOKENS             as KITE_INDEX_TOKENS,
    get_broker_expiries             as get_kite_expiries,
    list_broker_option_contracts    as list_kite_option_contracts,
    get_broker_credentials          as get_common_credentials,
    get_broker_ltp_map              as get_ltp_map,
    broker_is_configured            as is_configured,
    load_broker_credentials_from_db as load_credentials_from_db,
)
from features.spot_atm_utils import get_cached_spot_doc
from features.execution_socket import (
    broadcast_backtest_simulation_step,
    emit_broker_settings_for_user,
    emit_execute_order_for_user,
    queue_execute_order_group_start,
    run_backtest_simulation_step,
    socket_router,
    _fetch_dhan_broker_option_positions,
    _build_message,
    _extract_broker_configuration_label,
)
from features.live_fast_monitor import live_fast_monitor_supervisor, live_fast_monitor_supervisor_crypto
from features.live_monitor_socket import live_monitor_loop
from features import live_entry_monitor
from features import live_exit_monitor
from features import leg_range_monitor
from features.broker_accounts import (
    validate_broker_configuration_session as _validate_broker_configuration_session,
    DEFAULT_APP_USER_ID,
    get_broker_accounts_for_user,
)
from features.mock_kite_socket import mock_kite_socket_router
from features.live_quote_socket import live_quote_socket_router
from features.payment import payment_router, _algotrade_tier_balance, _algotrade_currently_running

# ─── Config ───────────────────────────────────────────────────────────────────

REQUEST_JSON_PATH = Path(__file__).parent / "current_backtest_request.json"
SAMPLE_RESULT_PATH = Path(__file__).parent / "sample_backtest_result" / "new_portfolio_result.json"
JOB_STATE_DIR = Path("/tmp/option_algo_backtest_jobs")
CACHE_DIR = Path("/tmp/option_algo_backtest_cache")

JOB_TTL_SECONDS = 3600       # auto-delete completed jobs older than 1 hour
MAX_JOBS        = 10         # max jobs kept in memory at once

# ─── Job store (in-memory) ────────────────────────────────────────────────────
# job_id → { status, completed, total, percent, current_day, result, error, created_at }

_jobs: dict = {}
_jobs_lock = multiprocessing.Lock()
_LIST_CACHE_TTL_SECONDS = 30.0
_list_cache: dict[str, dict] = {}
_list_cache_lock = threading.Lock()

_ACTIVE_OPTION_CHAIN_CACHE: dict[str, dict[str, Any]] = {}
_ACTIVE_OPTION_CHAIN_CACHE_LOCK = threading.Lock()
_shared_mongo = MongoData()
IST = timezone(timedelta(hours=5, minutes=30))
ALGO_TRADE_PORTFOLIO_COLLECTION = "algo_trade_portfolio"


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized_value = str(value or "").strip()
    if normalized_value:
        return normalized_value
    return DEFAULT_APP_USER_ID


def _normalize_runtime_activation_mode(value: str | None = None) -> str:
    return str(value or "").strip().lower() or "algo-backtest"


def _default_runtime_trade_date(value: str | None = None, date_hint: str | None = None) -> str:
    normalized_date = str(date_hint or "").strip()
    if normalized_date:
        return normalized_date
    normalized_mode = _normalize_runtime_activation_mode(value)
    if normalized_mode in {"live", "fast-forward", "forward-test"}:
        return datetime.now(IST).strftime("%Y-%m-%d")
    return ""


def _list_cache_get(key: str):
    now = time.time()
    with _list_cache_lock:
        item = _list_cache.get(key)
        if not item:
            return None
        if now - item.get("ts", 0) > _LIST_CACHE_TTL_SECONDS:
            _list_cache.pop(key, None)
            return None
        return deepcopy(item["value"])


def _list_cache_set(key: str, value) -> None:
    with _list_cache_lock:
        _list_cache[key] = {"ts": time.time(), "value": deepcopy(value)}


def _invalidate_list_cache(*keys: str) -> None:
    with _list_cache_lock:
        if not keys:
            _list_cache.clear()
            return
        for key in keys:
            _list_cache.pop(key, None)


def _load_active_option_chain_cache() -> dict[str, dict[str, Any]]:
    db = MongoData()
    try:
        contracts = list(
            db._db["active_option_tokens"].find(
                {},
                {
                    "_id": 0,
                    "instrument": 1,
                    "option_type": 1,
                    "expiry": 1,
                    "strike": 1,
                    "exchange": 1,
                    "symbol": 1,
                    "token": 1,
                    "tokens": 1,
                    "created_at": 1,
                    "updated_at": 1,
                },
            ).sort([("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)])
        )
    finally:
        db.close()

    cache: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        instrument = str(contract.get("instrument") or "").strip().upper()
        expiry = str(contract.get("expiry") or "").strip()[:10]
        option_type = str(contract.get("option_type") or "").strip().upper()
        token = str(contract.get("token") or contract.get("tokens") or "").strip()
        if not instrument or not expiry:
            continue

        instrument_bucket = cache.setdefault(
            instrument,
            {
                "instrument": instrument,
                "expiries": [],
                "expiry_count": 0,
                "total_contracts": 0,
                "source": "active_option_tokens",
                "option_chain": [],
                "grouped_option_chain": {},
            },
        )
        if expiry not in instrument_bucket["expiries"]:
            instrument_bucket["expiries"].append(expiry)

        grouped_bucket = instrument_bucket["grouped_option_chain"].setdefault(
            expiry,
            {"CE": [], "PE": []},
        )

        strike_raw = contract.get("strike")
        try:
            strike_value = float(strike_raw)
        except (TypeError, ValueError):
            strike_value = 0.0
        strike = int(strike_value) if strike_value.is_integer() else strike_value

        row = {
            "instrument": instrument,
            "expiry": expiry,
            "strike": strike,
            "option_type": option_type,
            "token": token,
            "tokens": token,
            "symbol": str(contract.get("symbol") or "").strip(),
            "exchange": str(contract.get("exchange") or "").strip(),
            "ltp": 0.0,
            "created_at": str(contract.get("created_at") or "").strip(),
            "updated_at": str(contract.get("updated_at") or "").strip(),
        }
        instrument_bucket["option_chain"].append(row)
        if option_type in {"CE", "PE"}:
            grouped_bucket[option_type].append(row)

    for instrument_bucket in cache.values():
        instrument_bucket["expiries"].sort()
        instrument_bucket["expiry_count"] = len(instrument_bucket["expiries"])
        instrument_bucket["total_contracts"] = len(instrument_bucket["option_chain"])
        for expiry_bucket in instrument_bucket["grouped_option_chain"].values():
            expiry_bucket["CE"].sort(key=lambda item: float(item.get("strike") or 0.0))
            expiry_bucket["PE"].sort(key=lambda item: float(item.get("strike") or 0.0))

    return cache


def _refresh_active_option_chain_cache() -> dict[str, dict[str, Any]]:
    cache = _load_active_option_chain_cache()
    with _ACTIVE_OPTION_CHAIN_CACHE_LOCK:
        _ACTIVE_OPTION_CHAIN_CACHE.clear()
        _ACTIVE_OPTION_CHAIN_CACHE.update(cache)
    return cache


def _get_active_option_chain_cache(instrument: str) -> dict[str, Any] | None:
    normalized_instrument = str(instrument or "").strip().upper()
    with _ACTIVE_OPTION_CHAIN_CACHE_LOCK:
        cached = _ACTIVE_OPTION_CHAIN_CACHE.get(normalized_instrument)
        if cached is not None:
            return cached

    cache = _refresh_active_option_chain_cache()
    return cache.get(normalized_instrument)


def _request_fingerprint(request: dict) -> str:
    payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _estimate_total_steps(request: dict) -> int:
    start_date = request.get("start_date")
    end_date = request.get("end_date")
    if not start_date or not end_date:
        return 0
    try:
        db = MongoData()
        holidays = db.get_holidays()
        cur = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        total_days = 0
        while cur <= end_dt:
            if cur.weekday() < 5 and cur.strftime("%Y-%m-%d") not in holidays:
                total_days += 1
            cur += timedelta(days=1)
        db.close()
        return total_days + 1 if total_days > 0 else 0
    except Exception:
        return 0


def _job_state_path(job_id: str) -> Path:
    JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return JOB_STATE_DIR / f"{job_id}.json"


def _cache_path(fingerprint: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{fingerprint}.json"


def _write_job_state(job_id: str, payload: dict) -> None:
    # orjson: ~7x faster than stdlib json.dump on large backtest result payloads
    # (measured: a 22MB 4-strategy portfolio result — 278ms stdlib vs 39ms orjson).
    # numpy types can't reach here (backtest_engine.py's DataIndex layer converts
    # everything to native float/int before building trade dicts), so no
    # option/default handling is needed.
    path = _job_state_path(job_id)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "wb") as f:
        f.write(orjson.dumps(payload))
    os.replace(tmp_path, path)


def _read_job_state(job_id: str) -> dict | None:
    path = _job_state_path(job_id)
    if not path.exists():
        return None
    try:
        with open(path, "rb") as f:
            return orjson.loads(f.read())
    except Exception:
        return None


def _read_cached_result(fingerprint: str) -> dict | None:
    path = _cache_path(fingerprint)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_cached_result(fingerprint: str, result: dict) -> None:
    path = _cache_path(fingerprint)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(result, f)
    os.replace(tmp_path, path)


def _cleanup_old_jobs():
    """Remove finished jobs older than JOB_TTL_SECONDS and enforce MAX_JOBS limit."""
    # Sync in-memory "running" jobs from file — child process only writes files,
    # so _jobs in the parent can be stale (still "running" after the child finishes).
    for jid, job in list(_jobs.items()):
        if job["status"] == "running":
            file_state = _read_job_state(jid)
            if file_state and file_state.get("status") != "running":
                _jobs[jid].update(file_state)

    now = time.time()
    expired = [jid for jid, j in _jobs.items()
               if j["status"] != "running"
               and now - j.get("created_at", now) > JOB_TTL_SECONDS]
    for jid in expired:
        state_path = _job_state_path(jid)
        if state_path.exists():
            state_path.unlink()
        del _jobs[jid]

    # if still over limit, remove oldest completed jobs first
    if len(_jobs) >= MAX_JOBS:
        done = sorted(
            [(jid, j) for jid, j in _jobs.items() if j["status"] != "running"],
            key=lambda x: x[1].get("created_at", 0),
        )
        for jid, _ in done[:len(_jobs) - MAX_JOBS + 1]:
            del _jobs[jid]




def _run_job(job_id: str, request: dict):
    try:
        os.nice(15)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}

    def on_progress(completed: int, total: int, day: str):
        state.update({
            "job_id": job_id,
            "status": "running",
            "completed": completed,
            "total": total,
            "percent": round(completed / total * 100, 1) if total else 0,
            "current_day": day,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)

    try:
        result = run_backtest(request, on_progress=on_progress)
        fingerprint = state.get("fingerprint")
        if fingerprint:
            _write_cached_result(fingerprint, result)
        total = state.get("total", 0)
        state.update({
            "job_id": job_id,
            "status": "done",
            "completed": total,
            "percent": 100.0 if total else 0.0,
            "current_day": "Completed",
            "result": result,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)
    except Exception as e:
        state.update({
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


def strategy_worker(args: dict):
    strategy_id_str = str((args or {}).get("strategy_id_str") or "")
    backtest_req = dict((args or {}).get("backtest_req") or {})
    job_id = str((args or {}).get("job_id") or "")

    # Write per-strategy progress to a temp file — avoids Manager IPC complexity
    prog_path = JOB_STATE_DIR / f"{job_id}_{strategy_id_str}.prog" if job_id else None

    def on_progress(completed: int, total: int, day: str):
        if not prog_path:
            return
        try:
            tmp = prog_path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump({"completed": completed, "total": total, "day": day}, f)
            os.replace(tmp, prog_path)
        except Exception:
            pass

    try:
        result = run_backtest(backtest_req, on_progress=on_progress)
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "completed",
            "error": None,
            "results": result,
        }
    except Exception as exc:
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "error",
            "error": str(exc),
            "results": None,
        }
    finally:
        # Reverted: explicitly closing the cached MongoClient here broke
        # multi-strategy portfolios, because ProcessPoolExecutor REUSES a
        # worker process across multiple strategy_worker calls (it doesn't
        # spawn a fresh process per task) — MongoData's client is a
        # process-level singleton (_client_cache), so closing it after
        # strategy #1 left strategies #2+ hitting "Cannot use MongoClient
        # after close" in the same reused process. The actual stuck-job root
        # cause was the unbounded per-day pkl5-write threads in
        # backtest_engine.py (_save_pkl5_background) piling up under I/O
        # contention, now fixed with a bounded worker pool there instead.
        # Clean up progress file on completion/error
        if prog_path and prog_path.exists():
            try:
                prog_path.unlink()
            except Exception:
                pass


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_datetime_string(value: Any) -> datetime | None:
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return None
    normalized_value = normalized_value.replace("T", " ")
    # entry_time/exit_time/check_after_ts are now sent as true UTC ('...Z') —
    # strip the marker to parse; _shift_datetime_string_by_minutes below
    # re-attaches it on output so the result stays explicitly UTC.
    if normalized_value.endswith("Z"):
        normalized_value = normalized_value[:-1]
    for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(normalized_value, pattern)
        except ValueError:
            continue
    return None


def _shift_datetime_string_by_minutes(value: Any, minutes: int) -> Any:
    if not minutes:
        return value
    parsed_value = _parse_datetime_string(value)
    if parsed_value is None:
        return value
    shifted_value = parsed_value - timedelta(minutes=minutes)
    # Always output true-UTC 'T...Z' regardless of the input's own format —
    # every "now" source on this platform is true UTC already (see the
    # UTC-standardization plan), so an old un-migrated/space-separated
    # input's *value* is still correct UTC; only its label was ever wrong.
    # Normalize the output instead of preserving that old ambiguous shape.
    return shifted_value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{shifted_value.microsecond // 1000:03d}Z"


def _load_strategy_time_difference_minutes(db: MongoData, activation_mode: str) -> int:
    normalized_mode = str(activation_mode or "").strip()
    if not normalized_mode:
        return 0

    query_candidates = [
        {"activation_mode": normalized_mode, "status": 1},
        {"activation_mode": normalized_mode, "is_active": True},
        {"activation_mode": normalized_mode, "active": True},
        {"activation_mode": normalized_mode},
    ]

    for query in query_candidates:
        try:
            config_doc = db._db["strategy_entry_time_difference"].find_one(
                query,
                {"difference_time_interval": 1},
                sort=[("_id", -1)],
            )
        except Exception:
            config_doc = None
        if config_doc:
            return max(0, _safe_int(config_doc.get("difference_time_interval"), 0))
    return 0


def _load_activation_portfolio_doc(db: MongoData, portfolio_id: str):
    normalized_portfolio_id = str(portfolio_id or "").strip()
    if not normalized_portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio_id is required")
    try:
        portfolio_oid = ObjectId(normalized_portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    # is_direct_strategy must be projected here — /portfolio/activate reads it
    # off this doc (via _load_source_portfolio_root) to tag algo_trades.portfolio
    # so the deployed list can tell a single-strategy "shadow" portfolio apart
    # from a real one. Dropping it from the projection silently defaults every
    # activation to "not direct", even through /strategy/{id}/activate.
    source_doc = db._db["saved_portfolios"].find_one({"_id": portfolio_oid}, {"_id": 1, "name": 1, "is_direct_strategy": 1})
    if source_doc:
        return "source", portfolio_oid, source_doc

    daily_doc = db._db[ALGO_TRADE_PORTFOLIO_COLLECTION].find_one(
        {"_id": portfolio_oid},
        {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1},
    )
    if daily_doc:
        return "daily", portfolio_oid, daily_doc

    raise HTTPException(status_code=404, detail="Portfolio not found")


def _get_source_portfolio_id_from_doc(portfolio_kind: str, portfolio_oid, portfolio_doc: dict) -> str:
    if portfolio_kind == "daily":
        resolved = str((portfolio_doc or {}).get("source_portfolio_id") or "").strip()
        if resolved:
            return resolved
    return str(portfolio_oid)


def _load_source_portfolio_root(db: MongoData, portfolio_kind: str, portfolio_oid, portfolio_doc: dict):
    if portfolio_kind == "source":
        return portfolio_oid, portfolio_doc or {}

    source_portfolio_id = str((portfolio_doc or {}).get("source_portfolio_id") or "").strip()
    if source_portfolio_id:
        try:
            source_oid = ObjectId(source_portfolio_id)
            source_doc = db._db["saved_portfolios"].find_one({"_id": source_oid}, {"_id": 1, "name": 1, "is_direct_strategy": 1}) or {}
            return source_oid, source_doc
        except Exception:
            pass
    return portfolio_oid, {"_id": portfolio_oid, "name": str((portfolio_doc or {}).get("source_portfolio_name") or (portfolio_doc or {}).get("name") or "").strip()}


def _normalize_trade_index(value: Any) -> str:
    return str(value or "").strip().upper()


def _extract_trade_index(*candidates: Any) -> str:
    for candidate in candidates:
        if isinstance(candidate, dict):
            nested_value = _extract_trade_index(
                candidate.get("trade_index"),
                candidate.get("ticker"),
                candidate.get("underlying"),
                ((candidate.get("config") or {}) if isinstance(candidate.get("config"), dict) else {}).get("Ticker"),
                ((candidate.get("strategy_detail") or {}) if isinstance(candidate.get("strategy_detail"), dict) else {}).get("underlying"),
                ((candidate.get("strategy") or {}) if isinstance(candidate.get("strategy"), dict) else {}).get("Ticker"),
            )
            if nested_value:
                return nested_value
            continue
        normalized = _normalize_trade_index(candidate)
        if normalized:
            return normalized
    return "NIFTY"


def _resolve_daily_portfolio(
    db: MongoData,
    source_portfolio_oid,
    source_portfolio_doc: dict,
    activation_mode: str = "",
    trade_date_hint: str = "",
    trade_index: str = "",
):
    """Find or create a daily runtime portfolio in algo_trade_portfolio.

    Runtime portfolio identity is scoped by:
      trade_date + activation_mode + trade_index

    Returns (portfolio_id_str, portfolio_doc_dict).
    """
    normalized_mode = _normalize_runtime_activation_mode(activation_mode)
    trade_date = _default_runtime_trade_date(normalized_mode, str(trade_date_hint or "").strip()[:10])
    if not trade_date:
        trade_date = datetime.now(IST).strftime("%Y-%m-%d")
    normalized_trade_index = _extract_trade_index(trade_index)

    collection = db._db[ALGO_TRADE_PORTFOLIO_COLLECTION]
    query = {
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "trade_index": normalized_trade_index,
    }
    existing = collection.find_one(
        query,
        {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1, "created_at": 1, "updated_at": 1},
    )
    if existing:
        return str(existing["_id"]), existing

    new_oid = ObjectId()
    now_iso = to_utc_iso(datetime.utcnow())
    sibling_doc = collection.find_one(
        {
            "trade_date": trade_date,
            "activation_mode": normalized_mode,
            "trade_group_portfolio": {"$exists": True, "$ne": ""},
        },
        {"trade_group_portfolio": 1},
    )
    trade_group_portfolio = str((sibling_doc or {}).get("trade_group_portfolio") or "").strip() or str(ObjectId())
    new_doc = {
        "_id": new_oid,
        "trade_portfolio": str(new_oid),
        "trade_group_portfolio": trade_group_portfolio,
        "trade_index": normalized_trade_index,
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    try:
        result = collection.insert_one(new_doc)
        return str(result.inserted_id), {
            "_id": result.inserted_id,
            "trade_portfolio": str(new_doc["trade_portfolio"]),
            "trade_group_portfolio": trade_group_portfolio,
            "trade_index": normalized_trade_index,
            "trade_date": trade_date,
            "activation_mode": normalized_mode,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    except Exception:
        fallback = collection.find_one(
            query,
            {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1, "created_at": 1, "updated_at": 1},
        )
        if fallback:
            return str(fallback["_id"]), fallback
        return str(source_portfolio_oid), source_portfolio_doc


def _apply_strategy_time_difference_to_trade(trade_doc: dict, difference_minutes: int) -> dict:
    if difference_minutes <= 0 or not isinstance(trade_doc, dict):
        return trade_doc

    adjusted_doc = dict(trade_doc)
    for field_name in ("entry_time", "exit_time", "check_after_ts"):
        if field_name in adjusted_doc:
            adjusted_doc[field_name] = _shift_datetime_string_by_minutes(
                adjusted_doc.get(field_name),
                difference_minutes,
            )
    return adjusted_doc


def _calc_leg_pnl(leg: dict) -> dict:
    entry_trade = leg.get("entry_trade") if isinstance(leg.get("entry_trade"), dict) else {}
    exit_trade = leg.get("exit_trade") if isinstance(leg.get("exit_trade"), dict) else {}
    entry_price = _safe_float(entry_trade.get("price"))
    quantity = _safe_int(leg.get("quantity") or entry_trade.get("quantity"))
    lot_size = _safe_int(leg.get("lot_size"), 1)
    effective_quantity = max(0, quantity) * max(1, lot_size)
    is_sell = "sell" in str(leg.get("position") or "").lower()

    if exit_trade:
        mark_price = _safe_float(exit_trade.get("price"))
        pnl_price_source = "exit_trade"
    else:
        mark_price = _safe_float(leg.get("last_saw_price"))
        pnl_price_source = "last_saw_price"

    if entry_price <= 0 or effective_quantity <= 0:
        pnl_value = 0.0
    else:
        pnl_value = ((entry_price - mark_price) if is_sell else (mark_price - entry_price)) * effective_quantity

    leg_payload = dict(leg)
    leg_payload["entry_price"] = entry_price
    leg_payload["mark_price"] = round(mark_price, 2)
    leg_payload["effective_quantity"] = effective_quantity
    leg_payload["pnl_price_source"] = pnl_price_source
    leg_payload["pnl"] = round(pnl_value, 2)
    return leg_payload


def _populate_history_legs(db_instance, records: list) -> list:
    """
    Batch-fetch all algo_trade_positions_history docs for the given trade records
    by querying trade_id. Groups docs per trade and attaches them as legs[].
    Status counts are derived from history docs:
      status=1 → open_legs_count
      status=2 → closed_legs_count
      status=0 → pending_legs_count
    """
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    # Single batch query: all history docs for all trades at once
    history_by_trade: dict[str, list] = {tid: [] for tid in trade_ids}
    try:
        history_col = db_instance["algo_trade_positions_history"]
        for doc in history_col.find({"trade_id": {"$in": trade_ids}}):
            doc["_id"] = str(doc.get("_id") or "")
            tid = str(doc.get("trade_id") or "")
            if tid in history_by_trade:
                history_by_trade[tid].append(doc)
    except Exception:
        pass

    populated = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        history_legs = history_by_trade.get(trade_id) or []
        new_rec = dict(rec)
        new_rec["legs"] = history_legs
        new_rec["open_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 1)
        new_rec["closed_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 2)
        new_rec["pending_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 0)
        populated.append(new_rec)
    return populated


def _format_feature_status_timestamp(value) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    return raw_value.replace("T", " ")


def _format_feature_status_price(value) -> str:
    numeric = _safe_float(value)
    if numeric <= 0:
        return "-"
    return f"₹{numeric:.2f}"


def _describe_feature_status_row(row: dict) -> str:
    if not isinstance(row, dict):
        return ""

    description = str(row.get("trigger_description") or "").strip()
    if description:
        return description

    feature_key = str(row.get("feature") or "").strip()
    if feature_key in {"overall_sl", "overall_target"}:
        label = "Overall SL" if feature_key == "overall_sl" else "Overall Target"
        cycle_number = int(row.get("cycle_number") or 1)
        trigger_value = _format_feature_status_price(row.get("trigger_value"))
        next_value = _format_feature_status_price(row.get("next_trigger_value"))
        reentry_type = str(row.get("reentry_type") or "None")
        reentry_count = int(row.get("reentry_count") or 0)
        reentry_done = int(row.get("reentry_done") or 0)
        return (
            f"{label} active for cycle {cycle_number}. "
            f"Current threshold {trigger_value}. "
            f"Re-entry {reentry_type} used {reentry_done}/{reentry_count}. "
            f"Next cycle threshold {next_value}."
        )
    if feature_key == "pending_entry":
        option = str(row.get("option") or "").strip().upper() or "-"
        position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
        strike = str(row.get("strike") or "").strip() or "-"
        queued_at = _format_feature_status_timestamp(row.get("queued_at"))
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        status = str(row.get("status") or "").strip().lower()

        if status == "triggered":
            return (
                f"Pending entry triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
            )

        return (
            f"Pending entry active for {strike} {option} {position} leg since {queued_at or '-'}. "
            f"Waiting for next entry cycle."
        )

    if feature_key != "momentum_pending":
        return ""

    status = str(row.get("status") or "").strip().lower()
    option = str(row.get("option") or "").strip().upper() or "-"
    position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
    strike = str(row.get("strike") or "").strip() or "-"
    momentum_type = str(row.get("momentum_type") or "").split(".")[-1].strip() or "Momentum"
    momentum_value = _safe_float(row.get("momentum_value"))
    base_price = _format_feature_status_price(row.get("momentum_base_price"))
    target_price = _format_feature_status_price(row.get("momentum_target_price"))
    queued_at = _format_feature_status_timestamp(row.get("queued_at"))
    armed_at = _format_feature_status_timestamp(row.get("armed_at"))

    if status == "triggered":
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        return (
            f"Momentum triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
        )

    if _safe_float(row.get("momentum_base_price")) > 0 and _safe_float(row.get("momentum_target_price")) > 0:
        return (
            f"Momentum waiting for {strike} {option} {position} leg. "
            f"{momentum_type} {momentum_value:g} armed at {armed_at or queued_at or '-'} "
            f"with base {base_price} and target {target_price}."
        )

    return (
        f"Momentum queue active for {strike} {option} {position} leg since {queued_at or '-'}. "
        f"Waiting to arm {momentum_type} {momentum_value:g}."
    )


def _build_pending_feature_leg(row: dict) -> dict:
    row_copy = dict(row)
    description = _describe_feature_status_row(row_copy)
    if description:
        row_copy["trigger_description"] = description

    feature_map = {}
    feature_key = str(row_copy.get("feature") or "").strip()
    if feature_key:
        feature_map[feature_key] = row_copy

    return {
        "id": str(row_copy.get("leg_id") or ""),
        "leg_id": str(row_copy.get("leg_id") or ""),
        "status": 0,
        "position": row_copy.get("position"),
        "option": row_copy.get("option"),
        "strike": row_copy.get("strike"),
        "expiry_date": row_copy.get("expiry_date"),
        "token": row_copy.get("token"),
        "symbol": row_copy.get("symbol"),
        "quantity": 0,
        "lot_config_value": int(row_copy.get("lot_config_value") or 1),
        "entry_trade": None,
        "exit_trade": None,
        "last_saw_price": row_copy.get("momentum_base_price"),
        "is_lazy": True,
        "is_pending_feature_leg": True,
        "queued_at": row_copy.get("queued_at"),
        "armed_at": row_copy.get("armed_at"),
        "triggered_at": row_copy.get("triggered_at"),
        "leg_type": row_copy.get("leg_type"),
        "momentum_base_price": row_copy.get("momentum_base_price"),
        "momentum_target_price": row_copy.get("momentum_target_price"),
        "feature_status_rows": [row_copy],
        "feature_status_map": feature_map,
        "active_trigger_descriptions": [description] if description else [],
    }


def _attach_leg_feature_statuses(db_instance, records: list) -> list:
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    feature_rows_by_key: dict[tuple[str, str], list] = {}
    try:
        feature_col = db_instance["algo_leg_feature_status"]
        for doc in feature_col.find(
            {
                "trade_id": {"$in": trade_ids},
                "enabled": True,
            }
        ):
            trade_id = str(doc.get("trade_id") or "")
            leg_id = str(doc.get("leg_id") or "")
            if not trade_id or not leg_id:
                continue
            doc["_id"] = str(doc.get("_id") or "")
            feature_rows_by_key.setdefault((trade_id, leg_id), []).append(doc)
    except Exception:
        return records

    enriched_records = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        legs = rec.get("legs") if isinstance(rec.get("legs"), list) else []
        existing_leg_ids = set()
        enriched_legs = []
        for leg in legs:
            if not isinstance(leg, dict):
                enriched_legs.append(leg)
                continue
            # algo_leg_feature_status docs are keyed by the leg CONFIG id (history_leg['id'] in
            # execution_socket.py's _store_position_history, e.g. "0"/"1") — never by this position
            # doc's own Mongo _id, which is a different identifier entirely. _id was checked first
            # here and is always present/truthy, so the real key (leg_id) never got used and this
            # lookup always missed — feature_status_map came back permanently empty for every leg.
            leg_id = str(leg.get("leg_id") or leg.get("id") or leg.get("_id") or "")
            if leg_id:
                existing_leg_ids.add(leg_id)
            feature_rows = feature_rows_by_key.get((trade_id, leg_id), [])
            leg_copy = dict(leg)
            leg_copy["feature_status_rows"] = feature_rows
            feature_map = {}
            active_descriptions = []
            for row in feature_rows:
                feature_key = str(row.get("feature") or "").strip()
                if not feature_key:
                    continue
                row_copy = dict(row)
                description = _describe_feature_status_row(row_copy)
                if description:
                    row_copy["trigger_description"] = description
                feature_map[feature_key] = row_copy
                if description:
                    active_descriptions.append(description)
            leg_copy["feature_status_map"] = feature_map
            leg_copy["feature_status_rows"] = list(feature_map.values()) if feature_map else feature_rows
            leg_copy["active_trigger_descriptions"] = active_descriptions
            enriched_legs.append(leg_copy)

        pending_feature_legs = []
        strategy_feature_rows = []
        for (feature_trade_id, feature_leg_id), feature_rows in feature_rows_by_key.items():
            if feature_trade_id != trade_id or not feature_leg_id or feature_leg_id in existing_leg_ids:
                continue
            if feature_leg_id == "__overall__":
                for row in feature_rows:
                    row_copy = dict(row)
                    description = _describe_feature_status_row(row_copy)
                    if description:
                        row_copy["trigger_description"] = description
                    strategy_feature_rows.append(row_copy)
                continue
            for row in feature_rows:
                if str(row.get("feature") or "").strip() not in {"momentum_pending", "pending_entry"}:
                    continue
                if str(row.get("status") or "").strip().lower() != "active":
                    continue
                pending_feature_legs.append(_build_pending_feature_leg(row))

        new_rec = dict(rec)
        new_rec["legs"] = enriched_legs
        new_rec["pending_feature_legs"] = pending_feature_legs
        new_rec["strategy_feature_status_rows"] = strategy_feature_rows
        enriched_records.append(new_rec)
    return enriched_records


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = "") -> str:
    """`name`/`display_name` are the user's own chosen label for this connection
    (BrokerSetup.tsx's "Connection Name" field — "My Main Account", "Test Delta
    Account", ...); `broker_name`/`broker`/`broker_type`/etc are internal type keys
    ("flattrade", "delta_exchange", "live") that only ever existed as a fallback for
    the handful of docs with no real name set. broker_name has to be checked AFTER
    the user-chosen fields, not before — every broker_configuration doc the generic
    Add Broker form creates has broker_name set, so checking it first meant the
    Connection Name the user actually typed was silently thrown away and every
    broker of the same type showed the identical generic label instead."""
    if not isinstance(document, dict):
        return fallback_broker_id
    for key in (
        "display_name",
        "name",
        "title",
        "broker_name",
        "broker",
        "broker_type",
        "provider",
        "vendor",
    ):
        value = str(document.get(key) or "").strip()
        if value:
            return value
    return str(fallback_broker_id or "").strip()


def _attach_broker_configuration_details(db_instance, records: list) -> list:
    if not records:
        return records

    broker_ids = []
    broker_object_ids = []
    for record in records:
        broker_id = str((record or {}).get("broker") or "").strip()
        if not broker_id:
            continue
        broker_ids.append(broker_id)
        try:
            broker_object_ids.append(ObjectId(broker_id))
        except Exception:
            continue

    if not broker_ids:
        return records

    broker_docs_by_id = {}
    try:
        cursor = db_instance["broker_configuration"].find(
            {"_id": {"$in": broker_object_ids}},
            {
                "_id": 1,
                "broker_name": 1,
                "display_name": 1,
                "name": 1,
                "title": 1,
                "broker": 1,
                "broker_icon": 1,
                "broker_type": 1,
                "provider": 1,
                "vendor": 1,
            },
        )
        for item in cursor:
            if not item:
                continue
            item_id = str(item.get("_id") or "").strip()
            if item_id:
                broker_docs_by_id[item_id] = item
    except Exception:
        return records

    if not broker_docs_by_id:
        return records

    enriched_records = []
    for record in records:
        new_record = dict(record)
        broker_id = str(new_record.get("broker") or "").strip()
        broker_doc = broker_docs_by_id.get(broker_id)
        if broker_doc:
            broker_details = dict(broker_doc)
            broker_details["_id"] = str(broker_doc.get("_id") or broker_id)
            new_record["broker_details"] = broker_details
            new_record["broker_label"] = _extract_broker_configuration_label(broker_doc, broker_id)
        enriched_records.append(new_record)
    return enriched_records


def _enrich_execution_record_with_pnl(record: dict) -> dict:
    legs = record.get("legs") if isinstance(record.get("legs"), list) else []
    enriched_legs = [_calc_leg_pnl(leg) for leg in legs if isinstance(leg, dict)]
    enriched_record = dict(record)
    enriched_record["legs"] = enriched_legs
    return enriched_record


def _run_portfolio_job(job_id: str, request: dict):
    """
    Subprocess worker for portfolio backtest.
    Runs all strategies in parallel using ProcessPoolExecutor.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed, wait
    import multiprocessing

    try:
        os.nice(10)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}
    portfolio_id = request.get("portfolio")
    start_date   = request.get("start_date")
    end_date     = request.get("end_date")

    try:
        db = MongoData()
        portfolio = db._db["saved_portfolios"].find_one({"_id": ObjectId(portfolio_id)})
        if not portfolio:
            state.update({"job_id": job_id, "status": "error",
                          "error": f"Portfolio {portfolio_id} not found",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_ids = portfolio.get("strategy_ids", [])
        if not strategy_ids:
            state.update({"job_id": job_id, "status": "error",
                          "error": "Portfolio has no strategies",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_docs = list(db._db["saved_strategies"].find(
            {"_id": {"$in": strategy_ids}},
            {"_id": 1, "name": 1, "full_config": 1},
        ))
        db.close()

        strategy_map     = {str(d["_id"]): d for d in strategy_docs}
        total_strategies = len(strategy_ids)
        name_map         = {}

        # Build per-strategy worker args
        worker_args = []
        error_results = []
        for strategy_id_obj in strategy_ids:
            strategy_id_str = str(strategy_id_obj)
            strategy_doc    = strategy_map.get(strategy_id_str)
            strategy_name   = (strategy_doc or {}).get("name") or strategy_id_str
            name_map[strategy_id_str] = strategy_name

            if not strategy_doc:
                error_results.append({
                    "_id":     strategy_id_str,
                    "item_id": strategy_id_str,
                    "status":  "error",
                    "error":   "Strategy not found",
                    "results": None,
                })
                continue

            full_config  = strategy_doc.get("full_config") or {}
            backtest_req = dict(full_config)
            backtest_req["start_date"] = start_date
            backtest_req["end_date"]   = end_date
            if "weekly_old_regime" in request:
                backtest_req["weekly_old_regime"] = request["weekly_old_regime"]

            worker_args.append({
                "strategy_id_str": strategy_id_str,
                "backtest_req":    backtest_req,
                "job_id":          job_id,
            })

        # Initial progress state
        state.update({
            "job_id":         job_id,
            "status":         "running",
            "strategy_count": total_strategies,
            "completed":      0,
            "total":          total_strategies,
            "percent":        0.0,
            "current_day":    f"Running {total_strategies} strategies in parallel…",
            "error":          None,
            "updated_at":     time.time(),
        })
        _write_job_state(job_id, state)

        results_by_id = {}
        for r in error_results:
            results_by_id[r["item_id"]] = r

        # Run in parallel, capped well below cpu_count — each worker independently
        # holds its own _MONTH_DF_CACHE (see backtest_engine.py) plus a full
        # in-memory backtest result, so peak memory scales with concurrent
        # workers, not just strategy count. A 5-strategy portfolio at
        # max_workers=5 was observed pushing the host into swap thrashing/OOM
        # territory, silently killing the coordinator process (SIGKILL bypasses
        # Python's except block, so the job just sits at its last-written
        # progress forever with no error). MAX_PORTFOLIO_WORKERS is env-tunable
        # for boxes with more headroom.
        # 3 still wasn't safe on this host's current baseline memory load
        # (coordinator kept dying mid-run) — 1 (fully sequential) trades
        # parallelism for reliability. Raise via env once the host has more
        # free RAM headroom to work with.
        MAX_PORTFOLIO_WORKERS = int(os.environ.get("MAX_PORTFOLIO_WORKERS", "1"))
        max_workers  = max(1, min(len(worker_args), os.cpu_count() or 4, MAX_PORTFOLIO_WORKERS))
        done_count   = len(error_results)

        def _read_prog_files() -> dict:
            """Read all per-strategy progress files for this job."""
            result = {}
            try:
                for p in JOB_STATE_DIR.glob(f"{job_id}_*.prog"):
                    try:
                        with open(p) as f:
                            data = json.load(f)
                        sid = p.stem[len(job_id) + 1:]
                        result[sid] = data
                    except Exception:
                        pass
            except Exception:
                pass
            return result

        if worker_args:
            with ProcessPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(strategy_worker, args): args["strategy_id_str"]
                    for args in worker_args
                }
                while futures:
                    done, not_done = wait(futures, timeout=1.0)

                    # Read per-strategy progress from temp files
                    prog_files = _read_prog_files()
                    total_pct  = done_count * 100.0
                    active_day = f"Completed {done_count}/{total_strategies}"
                    for sid, info in prog_files.items():
                        if info.get("total"):
                            worker_pct = (info["completed"] / info["total"]) * 100.0
                            worker_pct = max(worker_pct, 2.0)
                            total_pct += worker_pct
                            if info.get("day"):
                                active_day = info["day"]

                    overall_pct = round(total_pct / total_strategies, 1) if total_strategies else 0.0
                    state.update({
                        "job_id":      job_id,
                        "status":      "running",
                        "completed":   done_count,
                        "percent":     overall_pct,
                        "current_day": active_day,
                        "error":       None,
                        "updated_at":  time.time(),
                    })
                    _write_job_state(job_id, state)

                    for future in done:
                        result_item = future.result()
                        sid         = result_item["item_id"]
                        results_by_id[sid] = result_item
                        done_count += 1
                        del futures[future]

                        # Write immediately after each strategy completes
                        pct = round(done_count / total_strategies * 100, 1) if total_strategies else 0.0
                        state.update({
                            "completed":   done_count,
                            "percent":     pct,
                            "current_day": f"Completed {done_count}/{total_strategies} strategies",
                            "updated_at":  time.time(),
                        })
                        _write_job_state(job_id, state)

        # Preserve original strategy order
        results = [results_by_id[str(sid)] for sid in strategy_ids if str(sid) in results_by_id]

        final_result = {
            "status":   "completed",
            "progress": 100,
            "results":  results,
        }

        state.update({
            "job_id":      job_id,
            "status":      "done",
            "completed":   total_strategies,
            "total":       total_strategies,
            "percent":     100.0,
            "current_day": "Completed",
            "result":      final_result,
            "error":       None,
            "updated_at":  time.time(),
        })
        _write_job_state(job_id, state)

    except Exception as e:
        import traceback
        state.update({
            "job_id":     job_id,
            "status":     "error",
            "error":      traceback.format_exc(),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


# ─── App ──────────────────────────────────────────────────────────────────────

app    = FastAPI(title="algo-trade legacy routes")
router = APIRouter(prefix="/algo")
trade_router = APIRouter(prefix="/trade")   # live order placement/management lives at /trade/...

# fno-stocks and historical-data are common/shared concerns — code lives in
# shared/features/ but is served ONLY from algo.websocket (8003), not mounted
# here too. algo.trade's own api only contains algo.trade-specific routes.


class TelegramUsernameIn(BaseModel):
    telegram_username: str


class AdminTelegramMessageIn(BaseModel):
    user_id: str
    message: str


class AdminCreateUserIn(BaseModel):
    mobile: str
    name: str
    email: str
    password: str
    referral_code: Optional[str] = None


class AdminNotificationUserIn(BaseModel):
    user_id: str
    telegram_username: str


class PTPortfolioIn(BaseModel):
    name: str


class ZerodhaConfigRequest(BaseModel):
    api_key: str
    api_secret: str


class PTPositionIn(BaseModel):
    type: str
    option_type: str
    strike: float = 0.0  # 0.0 for a futures leg (option_type "FUT") — no strike on a future
    expiry: str
    token: Optional[str] = None
    entry_price: float
    entry_time: Optional[str] = None
    lots: Optional[int] = 1
    lot_size: Optional[int] = 75
    quantity: Optional[float] = None
    exited: Optional[bool] = False
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None


class PTStrategyIn(BaseModel):
    portfolio_name: str
    strategy_name: str
    instrument: Optional[str] = "nifty"
    spot_price: Optional[float] = None
    config: Optional[dict[str, Any]] = None
    positions: Optional[list[PTPositionIn]] = []
    # "backtest" for strategies saved from the historical-data builder
    # (PaperTradeBacktest.tsx), "live" for everything saved from the
    # live-broker/positions views — pure bookkeeping, doesn't gate the risk
    # monitor (that's alert_status, set separately by the "Add Alert" toggle).
    mode: Optional[str] = "live"


class PTWebhookIn(BaseModel):
    strategy_id: str
    adjustment_id: str


class PTTriggerIn(BaseModel):
    broker_id: str
    leg_id: str
    underlying: Optional[str] = None
    expiry: Optional[str] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    side: Optional[str] = None
    sl_mode: str
    sl_value: float
    tp_mode: str
    tp_value: float
    entry_price: float
    quantity: int
    exited: Optional[bool] = False


class PortfolioLegSnapshot(BaseModel):
    leg_id: str
    quantity: int


class PTPortfolioTriggerIn(BaseModel):
    broker_id: str
    underlying: str
    sl_upper: Optional[float] = None
    sl_lower: Optional[float] = None
    legs_snapshot: list[PortfolioLegSnapshot] = []


class PTAlertConfigLegSnapshot(BaseModel):
    leg_id: str
    quantity: int
    entry_price: float
    side: str


class PTAlertConfigToggle(BaseModel):
    enabled: bool = False
    unit: str = "points"
    value: float = 0.0


class PTAlertConfigTrailingStop(BaseModel):
    enabled: bool = False
    unit: str = "points"
    x: float = 0.0
    y: float = 0.0


class PTAlertConfigHedgeStrikeType(BaseModel):
    enabled: bool = False
    mode: str = "delta"
    value: float = 0.0
    strike: str = "ATM"


class PTAlertConfigHedgeTimeControl(BaseModel):
    enabled: bool = False
    entry_time: str = "09:15"
    exit_time: str = "15:30"


class PTAlertConfigIn(BaseModel):
    broker_id: str
    underlying: str
    # "alert_only" -> a leg/basket SL-TP hit is logged + Telegrammed to the
    # user (see simulator_risk_monitor.py's notify_user calls) but no real
    # order is placed. "auto" -> today's existing behavior (fires for real,
    # gated only by the global AUTO_FIRE_ENABLED kill-switch).
    trading_mode: str = "auto"
    stoploss: PTAlertConfigToggle
    target: PTAlertConfigToggle
    trailing_stop: PTAlertConfigTrailingStop
    hedge_strike_type: PTAlertConfigHedgeStrikeType
    hedge_time_control: PTAlertConfigHedgeTimeControl
    legs_snapshot: list[PTAlertConfigLegSnapshot] = []


class AdjustmentPositionIn(BaseModel):
    side: str
    lots: int
    qty: int
    strike: float
    option_type: str
    expiry: str
    entry_price: float
    tag: str  # "EXIT" | "NEW"


class PTAdjustmentIn(BaseModel):
    # Live-broker view keys by (broker_id, underlying); a saved/virtual
    # strategy (no broker_id/leg_id) keys by strategy_id instead — exactly
    # one of the two pairs is ever sent by the frontend depending on which
    # view (PaperTradeNew.tsx's isSavedStrategyView) is open.
    broker_id: Optional[str] = None
    underlying: Optional[str] = None
    strategy_id: Optional[str] = None
    trigger_condition: Optional[str] = None
    trigger_price: Optional[float] = None
    positions: list[AdjustmentPositionIn] = []
    # True while this is the live, armed config the risk monitor will act on; the
    # monitor flips it to False (never deletes) once fired, so simulator_adjustments
    # keeps a history of past adjustments instead of losing them.
    status: bool = True


class PTAdjustmentPatchIn(BaseModel):
    positions: list[AdjustmentPositionIn] = []
    trigger_price: Optional[float] = None
    trigger_condition: Optional[str] = None


class SimulatorBrokerPositionsRequest(BaseModel):
    broker_id: Optional[str] = None


class ManualOrderLeg(BaseModel):
    underlying: str
    expiry: str            # "YYYY-MM-DD"
    strike: float = 0.0    # 0.0 for a futures leg (option_type "FUT")
    option_type: str       # "CE" / "PE" / "FUT"
    side: str               # "BUY" / "SELL"
    quantity: int
    order_type: str         # "MARKET" / "LIMIT" / "SL"
    product: str             # "NRML" / "MIS"
    price: float = 0.0
    trigger_price: float = 0.0


class ManualOrderRequest(BaseModel):
    broker_id: str
    orders: list[ManualOrderLeg]


def _normalize_pt_option_type(option_type: str) -> str:
    normalized = str(option_type or "").strip().upper()
    if normalized in {"CALL", "CE"}:
        return "CE"
    if normalized in {"PUT", "PE"}:
        return "PE"
    return normalized


def _resolve_pt_position_token(position: dict, instrument: str = "") -> str:
    direct_token = str(position.get("token") or position.get("tokens") or "").strip()
    if direct_token:
        return direct_token

    normalized_instrument = str(instrument or position.get("instrument") or "").strip().upper()
    normalized_expiry = str(position.get("expiry") or "").strip()[:10]
    normalized_option_type = _normalize_pt_option_type(str(position.get("option_type") or ""))
    is_future = normalized_option_type == "FUT"
    try:
        strike_value = float(position.get("strike") or 0)
    except (TypeError, ValueError):
        strike_value = 0.0

    if not normalized_instrument or not normalized_expiry or not normalized_option_type:
        return ""
    if not is_future and strike_value <= 0:
        return ""

    # _enrich_pt_strategy_positions' own "cross_tokens" fallback (below) already
    # does this exact broker-aware active_option_tokens lookup correctly — but
    # only for positions that already have *some* stored token needing
    # cross-broker resolution. A position with no stored token at all (this
    # function's whole reason to exist) used to only ever try
    # _load_kite_instruments(), a Kite-only instrument master — with Dhan as
    # the active broker that's always empty/wrong, so current_ltp/MTM never
    # got computed and the frontend never even subscribed the leg's token for
    # live updates. Try the active broker's own token collection first.
    try:
        from features.broker_gateway import _active_broker
        if _active_broker() == "dhan":
            # A futures contract has no strike (always stored as 0.0 — see
            # _sync_dhan_index_future_tokens), so the query must omit it entirely
            # rather than matching strike: 0.0 literally against whatever this
            # position happens to carry.
            query = {
                "instrument": normalized_instrument,
                "expiry": {"$regex": f"^{normalized_expiry}"},
                "option_type": normalized_option_type,
                "broker": "dhan",
            }
            if not is_future:
                query["strike"] = strike_value
            doc = _shared_mongo._db["active_option_tokens"].find_one(
                query,
                {"token": 1, "tokens": 1, "_id": 0},
            )
            if doc:
                return str(doc.get("token") or doc.get("tokens") or "").strip()
            return ""
    except Exception:
        pass

    try:
        instrument_doc = (_load_kite_instruments() or {}).get(
            (normalized_instrument, normalized_expiry, strike_value, normalized_option_type)
        ) or {}
        return str(instrument_doc.get("token") or instrument_doc.get("tokens") or "").strip()
    except Exception:
        return ""


def _enrich_pt_strategy_positions(strategy_doc: dict) -> dict:
    enriched = dict(strategy_doc or {})
    instrument = str(enriched.get("instrument") or "").strip().upper()

    # Step 1: resolve tokens
    positions = []
    for raw_position in (enriched.get("positions") or []):
        if not isinstance(raw_position, dict):
            positions.append(raw_position)
            continue
        position = dict(raw_position)
        resolved_token = _resolve_pt_position_token(position, instrument)
        if resolved_token:
            position["token"] = resolved_token
        positions.append(position)

    # Step 2: fetch current LTP for all position tokens
    try:
        from features.broker_gateway import get_broker_ltp_map, get_broker_rest_quotes, _active_broker  # type: ignore
        ws_ltp = get_broker_ltp_map() or {}
        active_broker = _active_broker()

        # Build broker-native token map: stored token → active broker's token
        # Needed when positions have Kite tokens but Dhan is active (or vice-versa)
        broker_token_for: dict[str, str] = {}  # stored_token → broker_token
        ws_seg_for: dict[str, str] = {}         # broker_token → ws_segment

        stored_tokens = [str(p.get("token") or "") for p in positions if isinstance(p, dict) and p.get("token")]
        if stored_tokens:
            db_docs = list(_shared_mongo._db["active_option_tokens"].find(
                {"token": {"$in": stored_tokens}, "broker": active_broker},
                {"_id": 0, "token": 1, "ws_segment": 1},
            ))
            found_broker_tokens = {str(d["token"]) for d in db_docs}
            for d in db_docs:
                t = str(d["token"])
                broker_token_for[t] = t   # already a broker token
                ws_seg_for[t] = str(d.get("ws_segment") or "NSE_FNO")

            # Positions with non-broker tokens → resolve by strike/expiry/option_type
            cross_tokens = [t for t in stored_tokens if t not in found_broker_tokens]
            if cross_tokens:
                # Batch-fetch the position details we need for cross-resolution
                pos_by_token = {str(p.get("token") or ""): p for p in positions if isinstance(p, dict) and p.get("token")}
                for stored_tok in cross_tokens:
                    pos = pos_by_token.get(stored_tok) or {}
                    instr = str(pos.get("instrument") or instrument or "").upper()
                    expiry = str(pos.get("expiry") or "")[:10]
                    strike = pos.get("strike")
                    ot = _normalize_pt_option_type(str(pos.get("option_type") or ""))
                    is_future = ot == "FUT"
                    # A futures position's strike is always 0.0 (falsy) — only CE/PE
                    # positions need a real strike to resolve, see PTPositionIn.
                    if not (instr and expiry and ot) or (not is_future and not strike):
                        continue
                    try:
                        cross_query = {"instrument": instr, "expiry": {"$regex": f"^{expiry}"},
                                        "option_type": ot, "broker": active_broker}
                        if not is_future:
                            cross_query["strike"] = float(strike)
                        dhan_doc = _shared_mongo._db["active_option_tokens"].find_one(
                            cross_query,
                            {"token": 1, "ws_segment": 1, "_id": 0},
                        )
                        if dhan_doc:
                            bt = str(dhan_doc["token"])
                            broker_token_for[stored_tok] = bt
                            ws_seg_for[bt] = str(dhan_doc.get("ws_segment") or "NSE_FNO")
                    except Exception:
                        pass

        # Collect all broker tokens for REST fallback
        all_broker_tokens = list({bt for bt in broker_token_for.values() if bt})
        missing_ltp = [t for t in all_broker_tokens if not ws_ltp.get(t)]
        rest_quotes: dict = {}
        if missing_ltp:
            try:
                rest_quotes = get_broker_rest_quotes(missing_ltp, _shared_mongo._db, ws_seg_for)
            except Exception:
                pass

        for position in positions:
            if not isinstance(position, dict):
                continue
            stored_tok = str(position.get("token") or "")
            if not stored_tok:
                continue
            bt = broker_token_for.get(stored_tok, stored_tok)
            ltp = float(ws_ltp.get(bt) or 0)
            if ltp == 0:
                ltp = float((rest_quotes.get(bt) or {}).get("ltp") or 0)
            if ltp > 0:
                position["current_ltp"] = round(ltp, 2)
    except Exception:
        pass

    enriched["positions"] = positions
    return enriched


_DEFAULT_PAPER_TRADE_SPOT_BROKER_ID = "69e18416c3d234dc8c90e6ca"


def _serialize_instrument_spot_token(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id") or "").strip(),
        "broker_id": str(doc.get("broker_id") or "").strip(),
        "instrument": str(doc.get("instrument") or "").strip().upper(),
        "code": str(doc.get("code") or "").strip().upper(),
        "token": str(doc.get("token") or "").strip(),
    }


def _get_instrument_spot_token_docs(broker_id: str = "") -> list[dict]:
    resolved_broker_id = str(broker_id or _DEFAULT_PAPER_TRADE_SPOT_BROKER_ID).strip()
    query = {"broker_id": resolved_broker_id} if resolved_broker_id else {}
    docs = list(
        _shared_mongo._db["instrument_spot_token"].find(
            query,
            {"broker_id": 1, "instrument": 1, "code": 1, "token": 1},
        ).sort("instrument", 1)
    )
    return [_serialize_instrument_spot_token(doc) for doc in docs]


def _get_simulator_default_quote_tokens(broker_id: str = "") -> list[str]:
    return [
        str(item.get("token") or "").strip()
        for item in _get_instrument_spot_token_docs(broker_id)
        if str(item.get("token") or "").strip()
    ]



def legacy_startup() -> None:
    """Called from main.py's startup. Only the warm-ups the routes below
    need — the old process's other startup hooks (broker ticker, alert
    checker, telegram linking poll, live/crypto monitor auto-start, redis
    prewarm) are NOT run here: old algo.trade (8000) still owns those, and
    running them twice would double-fire alerts/orders."""
    try:
        MongoData().ensure_core_indexes()
    except Exception:
        log.exception("Failed to ensure MongoDB indexes at startup")
    try:
        _refresh_active_option_chain_cache()
    except Exception:
        log.exception("Failed to preload active option chain cache at startup")
    try:
        from features.span_file import save_defaults_to_db, fetch_span_file
        save_defaults_to_db()
        fetch_span_file()
    except Exception:
        log.exception("SPAN params startup failed — hardcoded defaults will be used")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/health")
def health():
    return {"status": "ok"}


# ─── App user auth (mobile + password, JWT) ──────────────────────────────────

def _set_shard_routing_cookie(response: Response, user_id: str) -> None:
    """Routing-only cookie for the reverse proxy in front of multiple
    algo.trade shard processes (see algo.trade/deploy/) — NOT part of auth
    (Bearer JWT is unchanged). Carries no sensitive data, just which shard
    (shared/features/shard_config.py's shard_for_user — single source of
    truth, same function owns_user() already uses for shard-safety) owns
    this user, so nginx can route every subsequent request/WebSocket for
    this session to the correct backend without decoding the JWT itself.
    A no-op in effect until SHARD_COUNT > 1 is actually configured — every
    user resolves to shard 0, matching today's single-process deployment."""
    from features.shard_config import shard_for_user

    response.set_cookie(
        key="algo_shard",
        value=str(shard_for_user(user_id)),
        max_age=60 * 60 * 24 * 30,
        samesite="lax",
        secure=True,
        httponly=False,
    )


@router.get("/auth/telegram-bot-info")
def auth_telegram_bot_info():
    """Profile page's "Link Telegram" popup needs the bot's @username to tell
    the user who to message after submitting (Telegram won't let the bot
    message them first — see telegram_notifier.telegram_linking_poll_loop)."""
    from features.telegram_notifier import get_bot_username
    username = get_bot_username()
    return {"username": username, "telegram_url": f"https://t.me/{username}" if username else ""}


@router.put("/auth/telegram-username")
def auth_set_telegram_username(payload: TelegramUsernameIn, current_user: dict = Depends(app_auth.require_current_user)):
    """Profile page's "Link Telegram" submit — stores the username pending a
    match (see set_pending_telegram_username); the user still has to send the
    bot one message in Telegram before telegram_linked actually flips true.
    Also upserts into finedge_telegram_notification_users so the user becomes
    searchable in the admin /telegram-send picker immediately."""
    from features.telegram_notifier import set_pending_telegram_username
    username = (payload.telegram_username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="Enter a Telegram username")
    set_pending_telegram_username(current_user["_id"], username)
    MongoData()._db["finedge_telegram_notification_users"].update_one(
        {"user_id": str(current_user["_id"])},
        {
            "$set": {"username": username, "status": 1},
            "$setOnInsert": {
                "user_id": str(current_user["_id"]),
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        upsert=True,
    )
    refreshed = MongoData()._db[app_auth.USERS_COLLECTION].find_one({"_id": current_user["_id"]})
    return app_auth.public_user(refreshed or current_user)


@router.post("/admin/telegram/send-message")
def admin_send_telegram_message(payload: AdminTelegramMessageIn):
    """Admin-composed Telegram message to one registered user — picked by site
    username/mobile (see admin_list_users), not their Telegram handle. Routes
    through notify_user_for so it lands on the user's own linked chat; if they
    haven't linked Telegram yet it falls back to the shared user chat instead
    of silently going nowhere (see telegram_notifier.notify_user_for).
    Intentionally open, no auth — see admin_list_users."""
    from bson import ObjectId
    from features.telegram_notifier import notify_user_for
    message = (payload.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message can't be empty")
    try:
        oid = ObjectId(payload.user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")
    target = MongoData()._db[app_auth.USERS_COLLECTION].find_one({"_id": oid})
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    sent, used_own_chat = notify_user_for(target, "ADMIN_MESSAGE", message)
    return {
        "sent": sent,
        "sent_to": "user" if used_own_chat else "shared_fallback_chat",
        "telegram_linked": bool(target.get("telegram_linked")),
    }


@router.get("/admin/user/{user_id}")
def admin_get_user(user_id: str):
    """Fetch a single user from user_details by MongoDB _id. No auth — keep off public deployments."""
    from bson import ObjectId
    db = MongoData()._db
    try:
        doc = db[app_auth.USERS_COLLECTION].find_one({"_id": ObjectId(user_id)}, {"password": 0})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    return app_auth.public_user(doc)


@router.get("/admin/all-users")
def admin_list_all_users(q: Optional[str] = None, page: int = 1, limit: int = 50):
    """List all users from user_details — admin panel user management.
    Intentionally open (no auth) — keep off public-facing deployments."""
    db = MongoData()._db
    col = db[app_auth.USERS_COLLECTION]
    query: dict = {}
    if q and q.strip():
        needle = q.strip()
        query = {"$or": [
            {"name": {"$regex": needle, "$options": "i"}},
            {"mobile": {"$regex": needle, "$options": "i"}},
            {"email": {"$regex": needle, "$options": "i"}},
        ]}
    total = col.count_documents(query)
    skip = (page - 1) * limit
    docs = list(col.find(query, {"password": 0}).sort("created_at", -1).skip(skip).limit(limit))
    users = [app_auth.public_user(d) for d in docs]
    return {"total": total, "page": page, "limit": limit, "users": users}


@router.post("/admin/create-user")
def admin_create_user(payload: AdminCreateUserIn):
    """Admin-only: create a new user directly (bypasses normal registration flow).
    Intentionally open (no auth) — keep off public-facing deployments."""
    db = MongoData()
    user = app_auth.register_user(db, payload.model_dump())
    return {"success": True, "user": app_auth.public_user(user)}


@router.post("/admin/telegram/notification-users")
def admin_add_notification_user(payload: AdminNotificationUserIn):
    """Add (or re-register) a user in finedge_telegram_notification_users and
    keep user_details.telegram_username in sync so the bot linking poll loop
    can match them when they next message @finedgealgo_bot.
    No auth — same policy as the other admin/telegram endpoints."""
    from bson import ObjectId
    from features.telegram_notifier import set_pending_telegram_username
    username = (payload.telegram_username or "").strip().lstrip("@")
    if not username:
        raise HTTPException(status_code=400, detail="telegram_username is required")
    try:
        oid = ObjectId(payload.user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user_id")
    db = MongoData()._db
    user = db[app_auth.USERS_COLLECTION].find_one({"_id": oid}, {"name": 1, "telegram_username": 1})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    # sync username into user_details so the poll loop can match this user
    set_pending_telegram_username(oid, username)
    # upsert into notification list
    db["finedge_telegram_notification_users"].update_one(
        {"user_id": str(oid)},
        {
            "$set": {"username": username, "status": 1},
            "$setOnInsert": {
                "user_id": str(oid),
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        upsert=True,
    )
    return {"ok": True, "user_id": str(oid), "telegram_username": username}


@router.post("/admin/telegram/sync-notification-users")
def admin_sync_notification_users():
    """One-shot sync: for every user in finedge_telegram_notification_users,
    update user_details.telegram_username if it doesn't already match —
    ensures users added manually (not via Profile page) can still be linked
    by the bot poll loop. Safe to call multiple times."""
    from features.telegram_notifier import set_pending_telegram_username
    db = MongoData()._db
    notif_docs = list(db["finedge_telegram_notification_users"].find({"status": 1}))
    updated = []
    skipped = []
    for notif in notif_docs:
        username = (notif.get("username") or "").strip().lstrip("@")
        user_id = notif.get("user_id") or ""
        if not username or not user_id:
            skipped.append({"user_id": user_id, "reason": "missing username or user_id"})
            continue
        try:
            from bson import ObjectId
            oid = ObjectId(user_id)
        except Exception:
            skipped.append({"user_id": user_id, "reason": "invalid ObjectId"})
            continue
        user = db[app_auth.USERS_COLLECTION].find_one({"_id": oid}, {"telegram_username": 1, "telegram_linked": 1})
        if not user:
            skipped.append({"user_id": user_id, "reason": "user not found in user_details"})
            continue
        existing = (user.get("telegram_username") or "").strip()
        if existing.lower() == username.lower() and user.get("telegram_linked"):
            skipped.append({"user_id": user_id, "username": username, "reason": "already in sync and linked"})
            continue
        set_pending_telegram_username(oid, username)
        updated.append({"user_id": user_id, "telegram_username": username})
    return {"updated": updated, "skipped": skipped}


@router.post("/admin/seed-expiry-config")
def seed_expiry():
    """Re-seed expiry_day_config collection from in-memory EXPIRY_RULES."""
    try:
        db    = MongoData()
        count = seed_expiry_config(db)
        return {"seeded": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/sample-result")
async def backtest_sample_result():
    """Return sample backtest result JSON for frontend table rendering."""
    if not SAMPLE_RESULT_PATH.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {SAMPLE_RESULT_PATH}")

    try:
        with open(SAMPLE_RESULT_PATH) as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Blocking endpoints (existing behaviour) ───────────────────────────────────

@router.post("/backtest")
async def backtest_from_body(request: dict):
    """Run backtest synchronously — waits until complete then returns result."""
    try:
        return run_backtest(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class IronCondorV2Request(BaseModel):
    underlying: str = "NIFTY"
    start_date: str
    end_date: str
    capital: float = 1_000_000.0
    risk_pct: float = 2.0
    # Advanced — entry-filter thresholds. Leave as defaults unless you know
    # why you're changing them; loosening iv_rank_min/adx_max lets more weeks
    # through, at the cost of trading into worse regimes.
    iv_rank_min: Optional[float] = None
    adx_max: Optional[float] = None
    adx_half_size: Optional[float] = None
    min_iv_samples: Optional[int] = None


@router.post("/iron-condor-v2/backtest")
async def iron_condor_v2_backtest(request: IronCondorV2Request):
    """V2 Condor: delta-based Iron Condor with IV-Rank/ADX entry filter,
    skew-adjusted put strike, capital-based sizing, hybrid roll adjustment
    (challenged side out + safe side closer by half), and a two-step profit
    ladder. Runs synchronously — waits until complete then returns
    trades + metrics."""
    try:
        overrides = {
            k: v for k, v in {
                "iv_rank_min": request.iv_rank_min,
                "adx_max": request.adx_max,
                "adx_half_size": request.adx_half_size,
                "min_iv_samples": request.min_iv_samples,
            }.items() if v is not None
        }
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: run_iron_condor_v2_backtest(
                request.underlying, request.start_date, request.end_date,
                capital=request.capital, risk_pct=request.risk_pct,
                **overrides,
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backtest/file")
async def backtest_from_file():
    """Run backtest from current_backtest_request.json synchronously."""
    if not REQUEST_JSON_PATH.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {REQUEST_JSON_PATH}")
    with open(REQUEST_JSON_PATH) as f:
        request = json.load(f)
    try:
        return run_backtest(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Background job endpoints (with progress) ──────────────────────────────────

@router.post("/backtest/start")
async def backtest_start(request: dict):
    """
    Start backtest in background. Returns job_id immediately.

    Postman flow:
      1. POST /backtest/start  → { "job_id": "abc123" }
      2. GET  /backtest/status/abc123  →  poll until status=done
      3. GET  /backtest/result/abc123  →  get final result
    """
    fingerprint = _request_fingerprint(request)
    estimated_total = _estimate_total_steps(request)
    cached_result = _read_cached_result(fingerprint)
    if cached_result is not None:
        job_id = str(uuid.uuid4())[:8]
        with _jobs_lock:
            _cleanup_old_jobs()
            _jobs[job_id] = {
                "status": "done",
                "completed": estimated_total,
                "total": estimated_total,
                "percent": 100.0 if estimated_total else 0.0,
                "current_day": "Cached Result",
                "error": None,
                "created_at": time.time(),
                "fingerprint": fingerprint,
            }
        _write_job_state(job_id, {
            "job_id": job_id,
            "status": "done",
            "completed": estimated_total,
            "total": estimated_total,
            "percent": 100.0 if estimated_total else 0.0,
            "current_day": "Cached Result",
            "result": cached_result,
            "error": None,
            "updated_at": time.time(),
        })
        return {"job_id": job_id, "status": "done", "cached": True}
    with _jobs_lock:
        _cleanup_old_jobs()
        for existing_job_id, job in _jobs.items():
            if job["status"] == "running":
                if job.get("fingerprint") == fingerprint:
                    return {
                        "job_id": existing_job_id,
                        "status": "running",
                        "message": "Identical backtest is already running",
                    }
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Another backtest is already running. Wait for it to finish or poll its status.",
                        "job_id": existing_job_id,
                    },
                )
        job_id = str(uuid.uuid4())[:8]
        _jobs[job_id] = {
            "status":      "running",
            "completed":   0,
            "total":       estimated_total,
            "percent":     0.0,
            "current_day": "Queued",
            "error":       None,
            "created_at":  time.time(),
            "fingerprint": fingerprint,
        }
    _write_job_state(job_id, {
        "job_id": job_id,
        "status": "running",
        "completed": 0,
        "total": estimated_total,
        "percent": 0.0,
        "current_day": "Queued",
        "fingerprint": fingerprint,
        "error": None,
        "updated_at": time.time(),
    })
    proc = multiprocessing.Process(target=_run_job, args=(job_id, request), daemon=True)
    proc.start()
    with _jobs_lock:
        _jobs[job_id]["pid"] = proc.pid
    return {"job_id": job_id, "status": "running"}


@router.post("/backtest/start/file")
async def backtest_start_file():
    """Start backtest from current_backtest_request.json in background."""
    if not REQUEST_JSON_PATH.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {REQUEST_JSON_PATH}")
    with open(REQUEST_JSON_PATH) as f:
        request = json.load(f)
    fingerprint = _request_fingerprint(request)
    estimated_total = _estimate_total_steps(request)
    cached_result = _read_cached_result(fingerprint)
    if cached_result is not None:
        job_id = str(uuid.uuid4())[:8]
        with _jobs_lock:
            _cleanup_old_jobs()
            _jobs[job_id] = {
                "status": "done",
                "completed": estimated_total,
                "total": estimated_total,
                "percent": 100.0 if estimated_total else 0.0,
                "current_day": "Cached Result",
                "error": None,
                "created_at": time.time(),
                "fingerprint": fingerprint,
            }
        _write_job_state(job_id, {
            "job_id": job_id,
            "status": "done",
            "completed": estimated_total,
            "total": estimated_total,
            "percent": 100.0 if estimated_total else 0.0,
            "current_day": "Cached Result",
            "result": cached_result,
            "error": None,
            "updated_at": time.time(),
        })
        return {"job_id": job_id, "status": "done", "cached": True}
    with _jobs_lock:
        _cleanup_old_jobs()
        for existing_job_id, job in _jobs.items():
            if job["status"] == "running":
                if job.get("fingerprint") == fingerprint:
                    return {
                        "job_id": existing_job_id,
                        "status": "running",
                        "message": "Identical backtest is already running",
                    }
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Another backtest is already running. Wait for it to finish or poll its status.",
                        "job_id": existing_job_id,
                    },
                )
        job_id = str(uuid.uuid4())[:8]
        _jobs[job_id] = {
            "status":      "running",
            "completed":   0,
            "total":       estimated_total,
            "percent":     0.0,
            "current_day": "Queued",
            "error":       None,
            "created_at":  time.time(),
            "fingerprint": fingerprint,
        }
    _write_job_state(job_id, {
        "job_id": job_id,
        "status": "running",
        "completed": 0,
        "total": estimated_total,
        "percent": 0.0,
        "current_day": "Queued",
        "fingerprint": fingerprint,
        "error": None,
        "updated_at": time.time(),
    })
    proc = multiprocessing.Process(target=_run_job, args=(job_id, request), daemon=True)
    proc.start()
    with _jobs_lock:
        _jobs[job_id]["pid"] = proc.pid
    return {"job_id": job_id, "status": "running"}


@router.get("/backtest/status/{job_id}")
async def backtest_status(job_id: str):
    """
    Poll progress of a running backtest.

    Response:
      { "status": "running", "completed": 45, "total": 250,
        "percent": 18.0, "current_day": "2024-03-15" }

      status values: "running" | "done" | "error"
    """
    job = _read_job_state(job_id) or _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return {
        "job_id":      job_id,
        "status":      job["status"],
        "completed":   job["completed"],
        "total":       job["total"],
        "percent":     job["percent"],
        "current_day": job["current_day"],
        "error":       job["error"],
        "result_ready": job["status"] == "done" and "result" in job,
    }


def _ensure_leg_config_ids(strategy: dict) -> dict:
    """Backfill a stable `id` onto every ListOfLegConfigs/IdleLegConfigs entry
    that's missing one.

    Every downstream consumer (SL/Target/TrailSL feature-status tracking,
    reentry, position history, the analyse pages) keys a leg by this id via
    trading_core.resolve_trade_leg_configs — a leg config without one can
    never be matched back to its live position, so its SL/Target/TrailSL
    silently show as permanently "Disabled" no matter what's configured.
    The crypto strategy builder (CryptoAlgoStrategy.tsx) is supposed to
    always send one, but this has recurred repeatedly from other/older save
    paths (a strategy saved before that fix existed, edited and re-saved
    without ever being reloaded, cloned, or built via some other client) —
    this is the backend-side guarantee that closes all of those regardless
    of which frontend path produced the payload. Index-based fallback
    ("0", "1", ...) matches the convention execution_socket.py's own crypto
    leg-id assignment already uses, so it stays stable across saves as long
    as leg order isn't reshuffled.
    """
    if not isinstance(strategy, dict):
        return strategy
    legs = strategy.get("ListOfLegConfigs")
    if isinstance(legs, list):
        for index, leg in enumerate(legs):
            if isinstance(leg, dict) and not str(leg.get("id") or "").strip():
                leg["id"] = str(index)
    idle_legs = strategy.get("IdleLegConfigs")
    if isinstance(idle_legs, dict):
        for idle_key, idle_leg in idle_legs.items():
            if isinstance(idle_leg, dict) and not str(idle_leg.get("id") or "").strip():
                idle_leg["id"] = str(idle_key)
    return strategy


@router.delete("/portfolio/{portfolio_id}")
async def portfolio_delete(portfolio_id: str, current_user: dict = Depends(app_auth.require_current_user)):
    """Delete a portfolio owned by the authenticated user."""
    user_id = str(current_user["_id"])
    try:
        oid = ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    db = MongoData()
    portfolio_col = db._db["saved_portfolios"]
    doc = portfolio_col.find_one({"_id": oid}, {"user_id": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    if doc.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="You do not have access to this portfolio")

    portfolio_col.delete_one({"_id": oid})
    return {"success": True}


def _extract_indicator_minutes(node):
    if isinstance(node, list):
        for item in node:
            minutes = _extract_indicator_minutes(item)
            if minutes is not None:
                return minutes
        return None
    if not isinstance(node, dict):
        return None
    value = node.get("Value")
    if isinstance(value, dict) and value.get("IndicatorName") == "IndicatorType.TimeIndicator":
        params = value.get("Parameters") or {}
        try:
            hour = int(params.get("Hour", 0))
            minute = int(params.get("Minute", 0))
            return hour * 60 + minute
        except Exception:
            return None
    if isinstance(value, list):
        nested = _extract_indicator_minutes(value)
        if nested is not None:
            return nested
    children = node.get("children") or node.get("Children")
    if isinstance(children, list):
        return _extract_indicator_minutes(children)
    return None


def _normalize_leg_instrument(option_value, instrument_kind):
    option = str(option_value or "").strip()
    if option.startswith("LegType."):
        return option
    if option in {"CE", "PE", "FUT"}:
        return f"LegType.{option}"
    instrument = str(instrument_kind or "").strip()
    if instrument.startswith("LegType."):
        return instrument
    if instrument in {"CE", "PE", "FUT"}:
        return f"LegType.{instrument}"
    return "LegType.CE"


def _normalize_weekdays_map(values):
    normalized = {
        "monday": False,
        "tuesday": False,
        "wednesday": False,
        "thursday": False,
        "friday": False,
        "saturday": False,
        "sunday": False,
    }
    mapping = {
        "m": "monday",
        "monday": "monday",
        "t": "tuesday",
        "tuesday": "tuesday",
        "w": "wednesday",
        "wednesday": "wednesday",
        "th": "thursday",
        "thu": "thursday",
        "thursday": "thursday",
        "f": "friday",
        "friday": "friday",
        "sat": "saturday",
        "saturday": "saturday",
        "sun": "sunday",
        "sunday": "sunday",
    }
    for value in values if isinstance(values, list) else []:
        key = mapping.get(str(value or "").strip().lower())
        if key:
            normalized[key] = True
    return normalized


def _default_leg_execution_config():
    return {
        "ProductType": "ProductType.NRML",
        "ExitOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {
                    "ModificationFrequency": 5,
                    "ContinuousMonitoring": "True",
                    "MarketOrderAfter": 1,
                },
            },
        },
        "EntryOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {"MarketOrderAfter": 40},
            },
        },
        "ReferenceForTgtSL": "PriceReferenceType.Trigger",
        "EntryDelay": 0,
    }


def _build_execution_cache(strategy_detail: dict, strategy_state: dict):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    ticker = detail.get("underlying") or strategy.get("Ticker") or strategy_state.get("ticker") or "NIFTY"

    lot_config = []
    expiries = []
    instruments = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        lot = leg.get("LotConfig") if isinstance(leg.get("LotConfig"), dict) else {}
        contract = leg.get("ContractType") if isinstance(leg.get("ContractType"), dict) else {}
        lot_config.append({
            "type": lot.get("Type") or "LotType.Quantity",
            "value": int(lot.get("Value", 1) or 1),
        })
        expiries.append(contract.get("Expiry") or "ExpiryType.Weekly")
        instruments.append(_normalize_leg_instrument(contract.get("Option"), contract.get("InstrumentKind")))

    return {
        "execution_version": "v2",
        "entry_time": _extract_indicator_minutes(strategy.get("EntryIndicators")),
        "exit_time": _extract_indicator_minutes(strategy.get("ExitIndicators")),
        "num_original_legs": len(legs),
        "lot_config": lot_config,
        "expiries": expiries,
        "instruments": instruments,
        "ticker": ticker,
        "strategy_type": strategy.get("StrategyType") or "StrategyType.IntradaySameDay",
        "reentry_restriction": strategy.get("ReentryTimeRestriction"),
    }


def _build_strategy_execution_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []

    execution_config_base = detail.get("execution_config_base") if isinstance(detail.get("execution_config_base"), dict) else {}
    if not execution_config_base:
        execution_config_base = {
            "Multiplier": int(strategy_state.get("qty_multiplier") or 1),
            "LikeBacktester": activation_mode != "live",
            "MarginAutoSquareOff": True,
            "TimeDelta": 0,
        }
    else:
        execution_config_base = dict(execution_config_base)
        execution_config_base.setdefault("Multiplier", int(strategy_state.get("qty_multiplier") or 1))
        execution_config_base.setdefault("LikeBacktester", activation_mode != "live")
        execution_config_base.setdefault("MarginAutoSquareOff", True)
        execution_config_base.setdefault("TimeDelta", 0)

    execution_config_extra = detail.get("execution_config_extra") if isinstance(detail.get("execution_config_extra"), dict) else {}
    if not execution_config_extra or not isinstance(execution_config_extra.get("ListOfLegExecutionConfig"), list):
        execution_config_extra = {
            "ListOfLegExecutionConfig": [_default_leg_execution_config() for _ in legs]
        }
    else:
        execution_config_extra = dict(execution_config_extra)

    return {
        "execution_config_base": execution_config_base,
        "execution_config_extra": execution_config_extra,
        "is_weekdays": bool(strategy_state.get("is_weekdays", True)),
        "dte": strategy_state.get("dte") if isinstance(strategy_state.get("dte"), list) else [],
        "weekdays": _normalize_weekdays_map(strategy_state.get("weekdays") if isinstance(strategy_state.get("weekdays"), list) else []),
        "view_config": detail.get("view_config") if isinstance(detail.get("view_config"), dict) else {"advanced_exec_config_modal": True},
    }


def _normalize_execution_settings_payload(source_detail: dict, payload: dict, activation_mode: str):
    detail = _clone_json_value(source_detail) if isinstance(source_detail, dict) else {}
    incoming = payload if isinstance(payload, dict) else {}

    if isinstance(incoming.get("execution_config_base"), dict):
        detail["execution_config_base"] = _clone_json_value(incoming.get("execution_config_base"))
    if isinstance(incoming.get("execution_config_extra"), dict):
        detail["execution_config_extra"] = _clone_json_value(incoming.get("execution_config_extra"))
    if isinstance(incoming.get("view_config"), dict):
        detail["view_config"] = _clone_json_value(incoming.get("view_config"))

    normalized = _build_strategy_execution_config(
        detail,
        {
            "qty_multiplier": ((incoming.get("execution_config_base") or {}).get("Multiplier") if isinstance(incoming.get("execution_config_base"), dict) else 1) or 1,
            "is_weekdays": incoming.get("is_weekdays", True),
            "dte": incoming.get("dte") if isinstance(incoming.get("dte"), list) else [],
            "weekdays": list((incoming.get("weekdays") or {}).keys()) if isinstance(incoming.get("weekdays"), dict) else [],
        },
        activation_mode,
    )

    if isinstance(incoming.get("weekdays"), dict):
        normalized["weekdays"] = {
            "friday": bool(incoming["weekdays"].get("friday")),
            "monday": bool(incoming["weekdays"].get("monday")),
            "saturday": bool(incoming["weekdays"].get("saturday")),
            "sunday": bool(incoming["weekdays"].get("sunday")),
            "thursday": bool(incoming["weekdays"].get("thursday")),
            "tuesday": bool(incoming["weekdays"].get("tuesday")),
            "wednesday": bool(incoming["weekdays"].get("wednesday")),
        }
    normalized["is_weekdays"] = bool(incoming.get("is_weekdays", normalized.get("is_weekdays", True)))
    normalized["dte"] = incoming.get("dte") if isinstance(incoming.get("dte"), list) else normalized.get("dte", [])
    normalized["view_config"] = incoming.get("view_config") if isinstance(incoming.get("view_config"), dict) else normalized.get("view_config", {"advanced_exec_config_modal": True})
    return normalized


def _clone_json_value(value):
    return deepcopy(value)


def _normalize_optional_config(config):
    if not isinstance(config, dict):
        return None
    normalized = _clone_json_value(config)
    config_type = str(normalized.get("Type") or "").strip()
    if not config_type or config_type == "None":
        return None
    return normalized


def _normalize_reentry_value(config):
    if not isinstance(config, dict):
        return None
    reentry_type = str(config.get("Type") or "").strip()
    if not reentry_type or reentry_type == "None":
        return None

    raw_value = config.get("Value")
    normalized_value = raw_value
    if isinstance(raw_value, dict):
        if "NextLegRef" in raw_value:
            normalized_value = raw_value.get("NextLegRef")
        elif "ReentryCount" in raw_value:
            normalized_value = raw_value.get("ReentryCount")
        elif len(raw_value) == 1:
            normalized_value = next(iter(raw_value.values()))

    return {
        "Type": reentry_type,
        "Value": normalized_value,
    }


def _normalize_option_kind(instrument_kind: str):
    value = str(instrument_kind or "").upper()
    if "PE" in value:
        return "PE"
    return "CE"


def _normalize_contract_strike(value):
    if isinstance(value, (int, float)):
        return value
    raw_value = str(value or "").strip()
    if not raw_value:
        return 0
    if raw_value == "StrikeType.ATM":
        return 0
    numeric_match = re.fullmatch(r"-?\d+(?:\.\d+)?", raw_value)
    if numeric_match:
        parsed = float(raw_value)
        return int(parsed) if parsed.is_integer() else parsed
    return raw_value


def _scale_strategy_lot_values(strategy: dict, multiplier: int) -> dict:
    """
    Multiply every leg's LotConfig.Value by the activation-time multiplier
    the user picked (PortfolioActivation.tsx's per-row/portfolio qty
    stepper). Every quantity read at execution time — see execution_socket.py's
    several `lot_config_value = ... (leg_cfg.get('LotConfig') or {}).get('Value') ...`
    call sites — comes straight from this stored value with no separate
    multiplier applied anywhere downstream, so a strategy saved with
    LotConfig.Value=1 traded at exactly 1 lot regardless of what multiplier
    was chosen when activating it unless this is scaled in up front, once,
    right here. Covers both ListOfLegConfigs (the active legs) and
    IdleLegConfigs (lazy/reentry leg templates keyed by leg id) — a
    re-entered leg must trade at the same multiplier as the original.
    """
    if not isinstance(strategy, dict) or multiplier == 1:
        return strategy

    def _scale_one(leg: dict) -> None:
        lot_config = leg.get("LotConfig")
        if not isinstance(lot_config, dict):
            return
        try:
            lot_config["Value"] = int(lot_config.get("Value") or 1) * multiplier
        except (TypeError, ValueError):
            pass

    legs = strategy.get("ListOfLegConfigs")
    if isinstance(legs, list):
        for leg in legs:
            if isinstance(leg, dict):
                _scale_one(leg)

    idle_legs = strategy.get("IdleLegConfigs")
    if isinstance(idle_legs, dict):
        for leg in idle_legs.values():
            if isinstance(leg, dict):
                _scale_one(leg)

    return strategy


def _build_algo_leg_config_entry(leg_config: dict):
    leg = leg_config if isinstance(leg_config, dict) else {}
    stop_loss = _normalize_optional_config(leg.get("LegStopLoss"))
    target = _normalize_optional_config(leg.get("LegTarget"))
    trail = _normalize_optional_config(leg.get("LegTrailSL"))
    momentum = _normalize_optional_config(leg.get("LegMomentum"))
    stop_reentry = _normalize_reentry_value(leg.get("LegReentrySL"))
    target_reentry = _normalize_reentry_value(leg.get("LegReentryTP"))

    if stop_loss and stop_reentry:
        stop_loss["Reentry"] = stop_reentry
    if stop_loss and trail:
        stop_loss["Trail"] = trail
    if target and target_reentry:
        target["Reentry"] = target_reentry

    return {
        "PositionType": leg.get("PositionType") or "PositionType.Sell",
        "ContractType": {
            "Option": _normalize_option_kind(leg.get("InstrumentKind")),
            "Expiry": leg.get("ExpiryKind") or "ExpiryType.Weekly",
            "InstrumentKind": "OPT",
            "StrikeParameter": _normalize_contract_strike(leg.get("StrikeParameter")),
            "EntryKind": leg.get("EntryType") or "EntryType.EntryByStrikeType",
        },
        "LotConfig": _clone_json_value(leg.get("LotConfig")) if isinstance(leg.get("LotConfig"), dict) else {
            "Type": "LotType.Quantity",
            "Value": 1,
        },
        "LegMomentum": momentum,
        "LegTarget": target,
        "LegStopLoss": stop_loss,
    }


def _build_algo_execution_leg_entry(leg_execution_config: dict):
    config = leg_execution_config if isinstance(leg_execution_config, dict) else {}
    entry_order = config.get("EntryOrder") if isinstance(config.get("EntryOrder"), dict) else {}
    exit_order = config.get("ExitOrder") if isinstance(config.get("ExitOrder"), dict) else {}

    entry_order_config = _clone_json_value(entry_order.get("Config")) if isinstance(entry_order.get("Config"), dict) else _clone_json_value(entry_order)
    exit_order_config = _clone_json_value(exit_order.get("Config")) if isinstance(exit_order.get("Config"), dict) else _clone_json_value(exit_order)
    if not entry_order_config:
        entry_order_config = {"Type": "OrderType.Market"}
    if not exit_order_config:
        exit_order_config = {"Type": "OrderType.Market"}

    return {
        "Product": config.get("Product") or config.get("ProductType") or "ProductType.NRML",
        "Reference": config.get("Reference") or config.get("ReferenceForTgtSL") or "PriceReferenceType.Trigger",
        "EntryOrder": {
            "Config": entry_order_config,
            "Delay": int(config.get("EntryDelay") or entry_order.get("Delay") or 0),
        },
        "ExitOrder": {
            "Config": exit_order_config,
            "Delay": int(config.get("ExitDelay") or exit_order.get("Delay") or 0),
        },
    }


def _build_algo_trade_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    if not strategy:
        return None

    parent_legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    idle_legs = strategy.get("IdleLegConfigs") if isinstance(strategy.get("IdleLegConfigs"), dict) else {}
    execution_config = _build_strategy_execution_config(detail, strategy_state, activation_mode)
    execution_base = execution_config.get("execution_config_base") if isinstance(execution_config.get("execution_config_base"), dict) else {}
    execution_extra = execution_config.get("execution_config_extra") if isinstance(execution_config.get("execution_config_extra"), dict) else {}
    execution_leg_configs = execution_extra.get("ListOfLegExecutionConfig") if isinstance(execution_extra.get("ListOfLegExecutionConfig"), list) else []

    keyed_leg_configs = {}
    keyed_execution_legs = {}
    for index, leg in enumerate(parent_legs, start=1):
        leg_key = f"og_leg_{index}"
        keyed_leg_configs[leg_key] = _build_algo_leg_config_entry(leg)
        keyed_execution_legs[leg_key] = _build_algo_execution_leg_entry(
            execution_leg_configs[index - 1] if index - 1 < len(execution_leg_configs) else {}
        )

    normalized_idle_legs = {}
    for idle_key, idle_leg in idle_legs.items():
        normalized_idle_legs[str(idle_key)] = _build_algo_leg_config_entry(idle_leg)

    return {
        "ExecutionConfig": {
            "LikeBacktester": bool(execution_base.get("LikeBacktester", activation_mode != "live")),
            "MarginAutoSquareOff": bool(execution_base.get("MarginAutoSquareOff", True)),
            "LotMultiplier": int(execution_base.get("Multiplier") or strategy_state.get("qty_multiplier") or 1),
            "LegsConfig": keyed_execution_legs,
        },
        "Ticker": strategy.get("Ticker") or detail.get("underlying") or strategy_state.get("ticker") or "NIFTY",
        "TakeUnderlyingFromCash": str(strategy.get("TakeUnderlyingFromCashOrNot") or "True").lower() == "true",
        "TrailSLtoBreakeven": _normalize_optional_config(strategy.get("TrailSLtoBreakeven")),
        "SquareOffAllLegs": str(strategy.get("SquareOffAllLegs") or "False").lower() == "true",
        "LegConfigs": keyed_leg_configs,
        "IdleLegConfigs": normalized_idle_legs,
        "OverallSL": _normalize_optional_config(strategy.get("OverallSL")),
        "OverallTgt": _normalize_optional_config(strategy.get("OverallTgt")),
        "LockAndTrail": _normalize_optional_config(strategy.get("LockAndTrail")),
        "OverallTrailSL": _normalize_optional_config(strategy.get("OverallTrailSL")),
        "OverallReentrySL": _normalize_optional_config(strategy.get("OverallReentrySL")),
        "OverallReentryTgt": _normalize_optional_config(strategy.get("OverallReentryTgt")),
        "OverallMomentum": _normalize_optional_config(strategy.get("OverallMomentum")),
    }


# Maps the activation_mode strings PortfolioActivation.tsx sends to the
# AlgoTrade wallet tier name _algotrade_tier_balance expects. "algo-backtest"
# has no entry — backtests don't spend AlgoTrade credits or count against any
# deploy limit, only live/fast-forward/forward-test do.
ACTIVATION_MODE_TO_ALGOTRADE_TIER: dict[str, str] = {
    "live": "live_trade",
    "fast-forward": "fast_forward",
    "forward-test": "forward_test",
}


def _algotrade_deploy_status(db: MongoData, user_id: str, activation_mode: str) -> dict:
    """This user's credits vs. currently-running strategies for one AlgoTrade
    tier — the single check both the pre-activation precheck (so the page can
    show the buy-limit popup before even attempting activation) and
    portfolio_activate's own gate (defense-in-depth against a stale/skipped
    precheck) run, so the two can never drift on what counts as over-limit."""
    tier = ACTIVATION_MODE_TO_ALGOTRADE_TIER.get(activation_mode)
    if not tier:
        return {"tier": None, "tier_balance": 0, "currently_running": 0}
    tier_balance = _algotrade_tier_balance(db._db, user_id, tier)
    currently_running = _algotrade_currently_running(db._db, user_id, activation_mode)
    return {"tier": tier, "tier_balance": tier_balance, "currently_running": currently_running}


def _compute_today_dte_for_strategy(db: MongoData, underlying: str, expiry_kind: str, trade_date: str) -> Optional[int]:
    """
    Days-to-expiry for `underlying`'s relevant expiry (per `expiry_kind`,
    e.g. "ExpiryType.Weekly") as of `trade_date`. Backs the opt-in DTE
    portfolio-activation gate below — lets a portfolio contain two saved
    strategy variants (e.g. one using ExpiryType.Weekly, one using
    ExpiryType.NextWeekly for expiry-day rollover) and activate only the
    one matching today's DTE.

    Returns None if it can't be computed (no live expiries, holiday, etc.)
    — callers should treat None as "don't gate" rather than "always skip".

    Reuses the same functions the rest of the live/backtest system already
    uses for expiry resolution and DTE arithmetic — no new math.
    """
    from features.spot_atm_utils import get_kite_expiries  # type: ignore
    from features.backtest_engine import _resolve_expiry, _get_trading_days  # type: ignore
    from features.range_breakout import compute_dte  # type: ignore
    from features.trading_calendar import is_trading_day  # type: ignore

    holidays = db.get_holidays()
    if not is_trading_day(trade_date, holidays):
        return None

    expiries = get_kite_expiries(underlying, trade_date)
    if not expiries:
        return None

    expiry = _resolve_expiry(trade_date, expiry_kind or "ExpiryType.Weekly", expiries)
    if not expiry:
        return None

    start_date = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=40)).strftime("%Y-%m-%d")
    trading_days = _get_trading_days(start_date, expiry, holidays)
    return compute_dte(trade_date, expiry, trading_days)


@router.get("/portfolio/start/{group_id}")
async def portfolio_start_group(group_id: str, activation_mode: str = "algo-backtest"):
    normalized_group_id = str(group_id or "").strip()
    normalized_mode = str(activation_mode or "").strip() or "algo-backtest"

    if not normalized_group_id:
        raise HTTPException(status_code=400, detail="group_id is required")

    db = MongoData()
    algo_trades_col = db._db["algo_trades"]
    records = list(
        algo_trades_col.find(
            {
                "portfolio.group_id": normalized_group_id,
                "activation_mode": normalized_mode,
            }
        ).sort("creation_ts", 1)
    )
    if not records:
        raise HTTPException(status_code=404, detail="No activated strategies found for this group_id")

    normalized_records = []
    for item in records:
        record = dict(item)
        record["_id"] = str(record.get("_id") or "")
        normalized_records.append(record)

    queue_execute_order_group_start(normalized_group_id, normalized_records)

    return {
        "success": True,
        "group_id": normalized_group_id,
        "activation_mode": normalized_mode,
        "count": len(normalized_records),
        "records": normalized_records,
    }


def _calculate_margin_sync(body: dict) -> dict:
    """Run all blocking DB + CPU work in a thread — keeps the async event loop free."""
    from features.span_margin import calculate_margin, SpanPosition

    legs_raw = body.get("legs", [])
    positions = []
    resolved_legs: list[dict[str, Any]] = []
    broker_margin: dict[str, Any] | None = None
    db = MongoData()
    try:
        try:
            load_credentials_from_db(db)
        except Exception:
            log.exception("Failed to load Kite credentials for margin calculation")

        for leg in legs_raw:
            underlying = str(leg.get("underlying", "NIFTY")).upper().strip()
            instrument_type = str(leg.get("instrument_type", "CE")).upper().strip()
            expiry = str(leg.get("expiry", "")).strip()
            strike = float(leg.get("strike", 0) or 0)
            transaction_type = str(leg.get("transaction_type", "SELL")).upper().strip()
            quantity = int(leg.get("quantity", 1))
            lot_size = int(leg.get("lot_size", 1))
            ltp = float(leg.get("ltp", 0) or 0)
            spot = float(leg.get("spot", 0) or 0)

            if spot <= 0:
                spot_doc = get_cached_spot_doc(db._db, underlying)
                spot = float(
                    (spot_doc or {}).get("spot_price")
                    or (spot_doc or {}).get("ltp")
                    or (spot_doc or {}).get("close")
                    or 0.0
                )

            if instrument_type in {"CE", "PE"} and ltp <= 0:
                ltp = _resolve_single_option_ltp(
                    db._db, underlying, expiry, strike, instrument_type,
                )
            elif instrument_type == "FUT" and ltp <= 0:
                ltp = spot

            positions.append(SpanPosition(
                underlying=underlying, instrument_type=instrument_type,
                expiry=expiry, strike=strike, transaction_type=transaction_type,
                quantity=quantity, lot_size=lot_size, ltp=ltp, spot=spot,
            ))
            resolved_legs.append({
                "underlying": underlying, "instrument_type": instrument_type,
                "expiry": expiry, "strike": strike, "transaction_type": transaction_type,
                "quantity": quantity, "lot_size": lot_size, "ltp": ltp, "spot": spot,
            })

        use_broker_api = body.get("use_broker_api", True)
        if resolved_legs and use_broker_api:
            broker_margin = _calculate_kite_basket_margin(db._db, resolved_legs)
    finally:
        db.close()

    if not positions:
        return {"span_margin": 0, "exposure_margin": 0, "total_margin": 0, "premium_received": 0, "net_margin": 0, "legs": []}

    product = str(body.get("product", "NRML")).upper()
    broker  = str(body.get("broker",  "kite")).lower()
    result  = calculate_margin(positions, product=product, broker=broker)
    broker_final = (broker_margin or {}).get("final") or {}
    if isinstance(broker_final, dict) and broker_final:
        premium_received_display = 0.0
        for leg in resolved_legs:
            it = str(leg.get("instrument_type") or "").upper()
            if it not in {"CE", "PE"}:
                continue
            leg_premium_value = float(leg.get("ltp") or 0.0) * int(leg.get("quantity") or 0) * int(leg.get("lot_size") or 0)
            if str(leg.get("transaction_type") or "").upper() == "SELL":
                premium_received_display += leg_premium_value
            else:
                premium_received_display -= leg_premium_value
        return {
            "span_margin": float(broker_final.get("span") or 0.0),
            "exposure_margin": float(broker_final.get("exposure") or 0.0),
            "total_margin": float(broker_final.get("total") or 0.0),
            "premium_received": round(premium_received_display, 2),
            "net_margin": float(broker_final.get("total") or 0.0),
            "source": "kite_basket_order_margins",
            "broker_margin": broker_margin,
            "legs": [
                {"underlying": l.underlying, "instrument_type": l.instrument_type,
                 "expiry": l.expiry, "strike": l.strike, "transaction_type": l.transaction_type,
                 "quantity": l.quantity, "lot_size": l.lot_size, "ltp": l.ltp,
                 "span_contribution": l.span_contribution, "exposure_margin": l.exposure_margin,
                 "total_margin": l.total_margin, "implied_vol": l.implied_vol}
                for l in result.legs
            ],
        }
    return {
        "span_margin": result.span_margin, "exposure_margin": result.exposure_margin,
        "total_margin": result.total_margin, "premium_received": result.premium_received,
        "net_margin": result.net_margin, "source": "local_span_engine",
        "legs": [
            {"underlying": l.underlying, "instrument_type": l.instrument_type,
             "expiry": l.expiry, "strike": l.strike, "transaction_type": l.transaction_type,
             "quantity": l.quantity, "lot_size": l.lot_size, "ltp": l.ltp,
             "span_contribution": l.span_contribution, "exposure_margin": l.exposure_margin,
             "total_margin": l.total_margin, "implied_vol": l.implied_vol}
            for l in result.legs
        ],
    }


@router.post("/margin/calculate")
async def calculate_margin_api(request: Request):
    import asyncio
    body = await request.json()
    if not body.get("legs"):
        return {"span_margin": 0, "exposure_margin": 0, "total_margin": 0, "premium_received": 0, "net_margin": 0, "legs": []}
    return await asyncio.to_thread(_calculate_margin_sync, body)


@router.get("/span/refresh")
async def refresh_span_file():
    """
    Manually trigger NSE + BSE SPAN file download and cache update.
    Call once every ~5 trading days to keep margin params fresh.
    Covers: all NSE index + stock options, all BSE index + stock options.
    """
    import asyncio
    from features.span_file import (
        fetch_span_file, get_cache_date, get_params, is_loaded,
        DEFAULTS, _nse_cache, _bse_cache,
    )

    ok = await asyncio.to_thread(fetch_span_file)

    # Index params summary
    index_summary = {
        u: {
            "psr_pct":     get_params(u).get("psr_pct"),
            "somc":        get_params(u).get("somc"),
            "inter_month": get_params(u).get("inter_month"),
            "from_file":   get_params(u).get("from_file", False),
        }
        for u in DEFAULTS
    }

    return {
        "success":        ok,
        "source":         "span_file" if (ok and is_loaded()) else "defaults",
        "file_date":      get_cache_date() or None,
        "nse_underlyings_loaded": len(_nse_cache),
        "bse_underlyings_loaded": len(_bse_cache),
        "index_params":   index_summary,
    }


@router.post("/span/upload")
async def upload_span_file(file: UploadFile = File(...)):
    """
    Upload NSE or BSE SPAN zip file directly.
    Filename must start with NSEFO_SPAN_ or BSEFO_SPAN_.

    Steps:
      1. Download NSEFO_SPAN_DDMMMYYYY.zip from NSE website (Derivatives → SPAN)
      2. Download BSEFO_SPAN_DDMMMYYYY.zip from BSE website (Derivatives → SPAN)
      3. POST each file to this endpoint
      4. System auto-parses and caches — margin calc uses live params immediately
    """
    import asyncio
    from features.span_file import fetch_span_file, get_cache_date, is_loaded, _SPAN_DIR, DEFAULTS, _nse_cache, _bse_cache, get_params

    fname = file.filename or ""
    fname_upper = fname.upper()

    if not (fname_upper.startswith("NSEFO_SPAN_") or fname_upper.startswith("BSEFO_SPAN_")):
        raise HTTPException(
            status_code=400,
            detail="Filename must start with NSEFO_SPAN_ or BSEFO_SPAN_ (e.g. NSEFO_SPAN_22MAY2026.zip)",
        )

    if not (fname_upper.endswith(".ZIP") or fname_upper.endswith(".SPN")):
        raise HTTPException(status_code=400, detail="Only .zip or .spn files accepted")

    # Save to span directory
    import os
    span_dir = os.path.abspath(_SPAN_DIR)
    os.makedirs(span_dir, exist_ok=True)
    save_path = os.path.join(span_dir, fname)

    content = await file.read()
    with open(save_path, "wb") as f:
        f.write(content)

    # Reload cache from disk
    ok = await asyncio.to_thread(fetch_span_file)

    index_summary = {
        u: {
            "psr_pct":     get_params(u).get("psr_pct"),
            "somc":        get_params(u).get("somc"),
            "inter_month": get_params(u).get("inter_month"),
            "from_file":   get_params(u).get("from_file", False),
        }
        for u in DEFAULTS
    }

    return {
        "success":               ok,
        "saved_as":              save_path,
        "source":                "span_file" if (ok and is_loaded()) else "defaults",
        "file_date":             get_cache_date() or None,
        "nse_underlyings_loaded": len(_nse_cache),
        "bse_underlyings_loaded": len(_bse_cache),
        "index_params":          index_summary,
    }


@router.get("/span/params")
async def get_span_params():
    """
    View current SPAN params for all underlyings in DB.
    Use this to verify what values are loaded.
    """
    from features.span_file import DEFAULTS, _db_cache, get_params
    db = MongoData()
    docs = list(db._db["span_params"].find({}, {"_id": 0}).sort("underlying", 1))
    db.close()
    return {
        "count": len(docs),
        "params": docs,
        "in_memory_count": len(_db_cache),
    }


@router.put("/span/params")
async def update_span_params(request: Request):
    """
    Update SPAN params in DB. Call this quarterly when NSE/BSE revises parameters.

    Body: list of param objects OR single object.
    Example (single index):
      { "underlying": "NIFTY", "psr_pct": 0.093, "somc": 21000, "inter_month_pct": 0.0175, "vsr": 0.04 }

    Example (bulk):
      [
        { "underlying": "NIFTY",     "psr_pct": 0.093, ... },
        { "underlying": "BANKNIFTY", "psr_pct": 0.093, ... }
      ]
    """
    import asyncio
    from datetime import datetime
    from features.span_file import load_from_db

    body = await request.json()
    items = body if isinstance(body, list) else [body]

    db = MongoData()
    updated = []
    for item in items:
        underlying = str(item.get("underlying") or "").strip().upper()
        if not underlying:
            continue
        update_doc = {k: v for k, v in item.items() if k != "underlying"}
        update_doc["updated_at"] = datetime.utcnow().strftime("%Y-%m-%d")
        update_doc["source"] = "manual"
        db._db["span_params"].update_one(
            {"underlying": underlying},
            {"$set": {"underlying": underlying, **update_doc}},
            upsert=True,
        )
        updated.append(underlying)
    db.close()

    # Reload memory cache
    count = await asyncio.to_thread(load_from_db)
    return {"updated": updated, "db_count": count}


@router.get("/trades/list")
async def list_algo_trades(date: str = "", activation_mode: str = "algo-backtest", trade_status: Optional[int] = None):
    """
    List algo trade execution records by activation mode and creation date.

    Query params:
      - date: YYYY-MM-DD
      - activation_mode: algo-backtest|forward-test|live
      - trade_status: numeric trade status filter
    """
    db = MongoData()
    algo_trades_col = db._db["algo_trades"]
    normalized_mode = _normalize_runtime_activation_mode(activation_mode)
    normalized_date = _default_runtime_trade_date(normalized_mode, date)

    query = {"activation_mode": normalized_mode}
    if normalized_date:
        query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
    if trade_status is not None:
        query["trade_status"] = trade_status

    raw_records = []
    cursor = algo_trades_col.find(query).sort("creation_ts", -1)
    for item in cursor:
        raw_records.append({
            "_id": str(item.get("_id") or ""),
            "strategy_id": str(item.get("strategy_id") or ""),
            "name": item.get("name") or "",
            "status": item.get("status") or "",
            "trade_status": item.get("trade_status"),
            "active_on_server": bool(item.get("active_on_server")),
            "activation_mode": item.get("activation_mode") or normalized_mode,
            "broker": item.get("broker") or "",
            "user_id": item.get("user_id") or "",
            "ticker": item.get("ticker") or "",
            "creation_ts": item.get("creation_ts") or "",
            "entry_time": item.get("entry_time") or "",
            "exit_time": item.get("exit_time") or "",
            "portfolio": item.get("portfolio") if isinstance(item.get("portfolio"), dict) else {},
        })

    # Populate string leg IDs with full algo_trade_positions_history docs (single batch query)
    populated_records = _populate_history_legs(db._db, raw_records)
    populated_records = _attach_leg_feature_statuses(db._db, populated_records)
    populated_records = _attach_broker_configuration_details(db._db, populated_records)
    records = [_enrich_execution_record_with_pnl(rec) for rec in populated_records]

    return {
        "success": True,
        "date": normalized_date,
        "activation_mode": normalized_mode,
        "count": len(records),
        "records": records,
    }


@router.get("/executions")
async def list_algo_executions(environment: str = "algo-backtest", is_signal: bool = False, date: str = "", trade_status: Optional[int] = None):
    """
    List execution records using an environment-based query shape.

    Query params:
      - environment: algo-backtest|forward-test|live
      - is_signal: reserved for parity with the upstream API
      - date: YYYY-MM-DD (required when environment=algo-backtest)
      - trade_status: numeric trade status filter
    """
    normalized_environment = _normalize_runtime_activation_mode(environment)
    normalized_date = _default_runtime_trade_date(normalized_environment, date)

    if normalized_environment == "algo-backtest" and not normalized_date:
        raise HTTPException(status_code=400, detail="date is required when environment=algo-backtest")

    return await list_algo_trades(
        date=normalized_date,
        activation_mode=normalized_environment,
        trade_status=trade_status,
    )


def _build_algo2_trade_history_payload(raw_trade: dict, normalized_strategy_id: str, normalized_status: str) -> dict:
    """`_build_trade_history_payload`'s branch for a strategy activated
    through algo-2_0 (`raw_trade["engine"] == "algo-2_0"`). Shares this SAME
    `algo_trades` collection, but not this old system's other two
    conventions the rest of `_build_trade_history_payload` assumes: a
    top-level `strategy` field with the full ListOfLegConfigs (algo-2_0
    keeps that under its own `_runtime_full_strategy_cfg`, never
    `strategy`), and per-leg history rows in `algo_trade_positions_history`
    (algo-2_0 embeds legs directly IN the algo_trades doc's own `legs[]`,
    it never writes that collection at all). Building the payload the old
    way for one of these silently returned an empty config + zero legs —
    confirmed live: AnalyseCryptoPaperTrade.tsx's fetchExternalEntity (the
    real caller behind FastForward2.tsx's "Analyse" button AND behind a
    portfolio/group Analyse click — this function is the ONE place all
    three of get_strategy_trade_history/get_group_trade_history/
    get_portfolio_trade_history funnel through per-trade) rendered that as
    a blank "Build your Strategy" page instead of the real position, for
    EVERY algo-2_0-owned strategy regardless of which of the three routes
    found it.

    algo-2_0 already builds this exact per-strategy shape correctly for
    its own docs (api/routers/strategy_history.py in algo-2_0/algo.trade —
    same "trade.strategy" + "legs.open/closed" shape, including the
    strike/expiry backfill for a leg entered before those fields existed,
    and the ListOfLegConfigs-from-per-leg-_runtime_leg_cfg reconstruction
    for a doc checkpointed before _runtime_full_strategy_cfg existed) — so
    that response is proxied here and reshaped into this function's own
    return shape, rather than re-implementing that same backfill/rebuild
    logic a second time against a different codebase's instrument-metadata
    modules. A plain synchronous `requests.get` (not httpx/async) since
    this whole function, and everything upstream of it in this file, is
    synchronous — matches the existing style here rather than threading
    async through three route handlers for one branch."""
    # In-process call, NOT an HTTP request to ALGO2_API_BASE_URL: this file is
    # mounted inside the same algo-2_0 app (main.py), and its async route
    # handlers (get_group/portfolio_trade_history) call this synchronously
    # on the event loop — a blocking requests.get back to our own port could
    # never be served while the loop was blocked, so it always hit the 10s
    # read timeout and the Analyse page came back empty.
    from api.routers.strategy_history import strategy_trade_history

    algo2_trade: dict[str, Any] = {}
    open_legs: list[dict] = []
    closed_legs: list[dict] = []
    try:
        algo2_payload = strategy_trade_history(normalized_strategy_id, normalized_status)
        algo2_trade = algo2_payload.get("trade") or {}
        legs_bucket = algo2_payload.get("legs") or {}
        open_legs = legs_bucket.get("open") or []
        closed_legs = legs_bucket.get("closed") or []
    except Exception:
        log.exception("[TradeHistory] algo-2_0 proxy failed for strategy=%s", normalized_strategy_id)

    all_legs = open_legs + closed_legs
    trade_mtm = round(sum(float((leg or {}).get("_runtime_current_pnl") or 0) for leg in all_legs if isinstance(leg, dict)), 2)

    trade_record = {
        "_id": normalized_strategy_id,
        "strategy_id": str(raw_trade.get("strategy_id") or ""),
        "source_strategy_id": str(raw_trade.get("source_strategy_id") or ""),
        "name": algo2_trade.get("name") or raw_trade.get("name") or "",
        "status": raw_trade.get("status") or "",
        "trade_status": raw_trade.get("trade_status"),
        "active_on_server": bool(raw_trade.get("active_on_server")),
        "activation_mode": raw_trade.get("activation_mode") or normalized_status,
        "trade_date": raw_trade.get("trade_date") or "",
        "broker": raw_trade.get("broker") or "",
        "user_id": raw_trade.get("user_id") or "",
        "ticker": algo2_trade.get("ticker") or raw_trade.get("ticker") or "",
        "creation_ts": raw_trade.get("creation_ts") or "",
        "last_activation_ts": raw_trade.get("last_activation_ts") or "",
        "entry_time": raw_trade.get("entry_time") or "",
        "exit_time": raw_trade.get("exit_time") or "",
        "portfolio": raw_trade.get("portfolio") if isinstance(raw_trade.get("portfolio"), dict) else {},
        "strategy": algo2_trade.get("strategy") or {},
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
        "legs": all_legs,
        "pending_feature_legs": [],
    }

    return {
        "success": True,
        "view_type": "strategy",
        "strategy_id": normalized_strategy_id,
        "group_id": str(((trade_record.get("portfolio") or {}).get("group_id")) or "").strip(),
        "activation_mode": str(trade_record.get("activation_mode") or normalized_status),
        "trade": trade_record,
        "summary": {
            "mtm": trade_mtm,
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": 0,
            "broker_orders_count": 0,
            "notifications_count": 0,
        },
        "legs": {
            "all": all_legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": [],
            "pending_feature_legs": [],
        },
        "broker_orders": [],
        "open_orders": [],
        "notifications": [],
        "notification_status": {},
        "trade_notifications": [],
        "execution_config_base": trade_record["execution_config_base"],
        "execution_config_extra": trade_record["execution_config_extra"],
    }


def _build_trade_history_payload(db_obj, raw_trade: dict, normalized_status: str):
    normalized_strategy_id = str(raw_trade.get("_id") or "").strip()
    if raw_trade.get("engine") == "algo-2_0":
        return _build_algo2_trade_history_payload(raw_trade, normalized_strategy_id, normalized_status)
    trade_record = {
        "_id": normalized_strategy_id,
        "strategy_id": str(raw_trade.get("strategy_id") or ""),
        "source_strategy_id": str(raw_trade.get("source_strategy_id") or ""),
        "name": raw_trade.get("name") or "",
        "status": raw_trade.get("status") or "",
        "trade_status": raw_trade.get("trade_status"),
        "active_on_server": bool(raw_trade.get("active_on_server")),
        "activation_mode": raw_trade.get("activation_mode") or normalized_status,
        "trade_date": raw_trade.get("trade_date") or "",
        "broker": raw_trade.get("broker") or "",
        "user_id": raw_trade.get("user_id") or "",
        "ticker": raw_trade.get("ticker") or "",
        "creation_ts": raw_trade.get("creation_ts") or "",
        "last_activation_ts": raw_trade.get("last_activation_ts") or "",
        "entry_time": raw_trade.get("entry_time") or "",
        "exit_time": raw_trade.get("exit_time") or "",
        "portfolio": raw_trade.get("portfolio") if isinstance(raw_trade.get("portfolio"), dict) else {},
        "strategy": raw_trade.get("strategy") if isinstance(raw_trade.get("strategy"), dict) else {},
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
    }

    populated_records = _populate_history_legs(db_obj, [trade_record])
    populated_records = _attach_leg_feature_statuses(db_obj, populated_records)
    populated_records = _attach_broker_configuration_details(db_obj, populated_records)
    detailed_trade = _enrich_execution_record_with_pnl((populated_records or [trade_record])[0])

    legs = detailed_trade.get("legs") if isinstance(detailed_trade.get("legs"), list) else []
    pending_feature_legs = detailed_trade.get("pending_feature_legs") if isinstance(detailed_trade.get("pending_feature_legs"), list) else []

    trade_mtm = round(sum(float((leg or {}).get("pnl") or 0) for leg in legs if isinstance(leg, dict)), 2)
    open_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 1]
    closed_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 2]
    pending_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 0]

    orders = []
    for doc in (
        db_obj["broker_orders"]
        .find({"trade_id": normalized_strategy_id})
        .sort("placed_at", -1)
        .limit(1000)
    ):
        doc["_id"] = str(doc.get("_id") or "")
        orders.append(doc)

    notifications = []
    feature_filters = [{"trade_id": normalized_strategy_id}]
    related_strategy_ids = {
        str(detailed_trade.get("strategy_id") or "").strip(),
        str(detailed_trade.get("source_strategy_id") or "").strip(),
    }
    related_strategy_ids.discard("")
    for related_id in related_strategy_ids:
        feature_filters.append({"strategy_id": related_id})

    for doc in (
        db_obj["algo_leg_feature_status"]
        .find({"$or": feature_filters})
        .sort("created_at", -1)
        .limit(1000)
    ):
        normalized_doc = dict(doc)
        normalized_doc["_id"] = str(doc.get("_id") or "")
        normalized_doc["type"] = str(doc.get("feature") or "").strip() or "feature_status"
        normalized_doc["event_type"] = normalized_doc["type"]
        normalized_doc["timestamp"] = (
            doc.get("triggered_at")
            or doc.get("updated_at")
            or doc.get("created_at")
            or ""
        )
        notifications.append(normalized_doc)

    notification_status = {}
    for item in notifications:
        event_type = str(item.get("event_type") or item.get("type") or "unknown").strip() or "unknown"
        notification_status[event_type] = notification_status.get(event_type, 0) + 1

    trade_notifications = []
    for doc in (
        db_obj["algo_trade_notification"]
        .find({"$or": feature_filters})
        .sort("timestamp", -1)
        .limit(1000)
    ):
        normalized_doc = dict(doc)
        normalized_doc["_id"] = str(doc.get("_id") or "")
        trade_notifications.append(normalized_doc)

    return {
        "success": True,
        "view_type": "strategy",
        "strategy_id": normalized_strategy_id,
        "group_id": str(((detailed_trade.get("portfolio") or {}).get("group_id")) or "").strip(),
        "activation_mode": str(detailed_trade.get("activation_mode") or normalized_status),
        "trade": detailed_trade,
        "summary": {
            "mtm": trade_mtm,
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": len(pending_legs),
            "broker_orders_count": len(orders),
            "notifications_count": len(notifications),
        },
        "legs": {
            "all": legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": pending_legs,
            "pending_feature_legs": pending_feature_legs,
        },
        "broker_orders": orders,
        "open_orders": [
            order for order in orders
            if str(order.get("status") or "").strip().upper() in {"OPEN", "PENDING", "TRIGGER PENDING"}
        ],
        "notifications": notifications,
        "notification_status": notification_status,
        "trade_notifications": trade_notifications,
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
    }


def _aggregate_group_trade_history_payload(group_id: str, normalized_status: str, payloads: list[dict]):
    valid_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    if not valid_payloads:
        raise HTTPException(status_code=404, detail="Strategy trade history not found for this group_id")

    primary_payload = valid_payloads[0]
    primary_trade = primary_payload.get("trade") if isinstance(primary_payload.get("trade"), dict) else {}
    group_name = str(((primary_trade.get("portfolio") or {}).get("group_name")) or "").strip() or f"Group {group_id}"
    strategy_names = []
    tickers = set()
    broker_labels = set()
    user_id = ""
    all_legs = []
    open_legs = []
    closed_legs = []
    pending_legs = []
    pending_feature_legs = []
    broker_orders = []
    notifications = []
    trade_notifications = []
    strategy_execution_configs = []
    notification_status = {}
    total_mtm = 0.0

    for payload in valid_payloads:
        trade = payload.get("trade") if isinstance(payload.get("trade"), dict) else {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        legs = payload.get("legs") if isinstance(payload.get("legs"), dict) else {}
        trade_name = str(trade.get("name") or "").strip()
        if trade_name:
            strategy_names.append(trade_name)
        ticker = str(trade.get("ticker") or trade.get("underlying") or "").strip()
        if ticker:
            tickers.add(ticker)
        broker_label = str(
            ((trade.get("broker_details") or {}).get("broker_name"))
            or ((trade.get("broker_details") or {}).get("display_name"))
            or trade.get("broker_label")
            or trade.get("broker")
            or ""
        ).strip()
        if broker_label:
            broker_labels.add(broker_label)
        if not user_id:
            user_id = str(trade.get("user_id") or "").strip()

        total_mtm += float(summary.get("mtm") or 0)
        all_legs.extend(legs.get("all") if isinstance(legs.get("all"), list) else [])
        open_legs.extend(legs.get("open") if isinstance(legs.get("open"), list) else [])
        closed_legs.extend(legs.get("closed") if isinstance(legs.get("closed"), list) else [])
        pending_legs.extend(legs.get("pending") if isinstance(legs.get("pending"), list) else [])
        pending_feature_legs.extend(legs.get("pending_feature_legs") if isinstance(legs.get("pending_feature_legs"), list) else [])
        broker_orders.extend(payload.get("broker_orders") if isinstance(payload.get("broker_orders"), list) else [])
        notifications.extend(payload.get("notifications") if isinstance(payload.get("notifications"), list) else [])
        trade_notifications.extend(payload.get("trade_notifications") if isinstance(payload.get("trade_notifications"), list) else [])

        for key, value in (payload.get("notification_status") or {}).items():
            normalized_key = str(key or "").strip() or "unknown"
            notification_status[normalized_key] = notification_status.get(normalized_key, 0) + int(value or 0)

        strategy_execution_configs.append({
            "strategy_id": str(trade.get("_id") or payload.get("strategy_id") or "").strip(),
            "name": str(trade.get("name") or "").strip(),
            "execution_config_base": payload.get("execution_config_base") if isinstance(payload.get("execution_config_base"), dict) else {},
            "execution_config_extra": payload.get("execution_config_extra") if isinstance(payload.get("execution_config_extra"), dict) else {},
        })

    broker_orders.sort(key=lambda item: str((item or {}).get("placed_at") or ""), reverse=True)
    notifications.sort(
        key=lambda item: str(
            (item or {}).get("timestamp")
            or (item or {}).get("triggered_at")
            or (item or {}).get("updated_at")
            or (item or {}).get("created_at")
            or ""
        ),
        reverse=True,
    )
    trade_notifications.sort(key=lambda item: str((item or {}).get("timestamp") or ""), reverse=True)

    strategy_count = len(valid_payloads)
    tickers_label = ", ".join(sorted(tickers)) if tickers else "Multiple"
    broker_label_text = ", ".join(sorted(broker_labels)) if broker_labels else (primary_trade.get("broker") or "-")
    trade = deepcopy(primary_trade)
    trade["_id"] = group_id
    trade["name"] = f"{group_name} ({strategy_count})"
    trade["ticker"] = tickers_label
    trade["user_id"] = user_id or str(trade.get("user_id") or "")
    trade["activation_mode"] = normalized_status
    trade["broker_label"] = broker_label_text
    portfolio_meta = trade.get("portfolio") if isinstance(trade.get("portfolio"), dict) else {}
    portfolio_meta["group_id"] = group_id
    portfolio_meta["group_name"] = group_name
    portfolio_meta["strategy_count"] = strategy_count
    trade["portfolio"] = portfolio_meta
    trade["strategy_names"] = strategy_names
    trade["status"] = trade.get("status") or "Group"

    return {
        "success": True,
        "view_type": "group",
        "group_id": group_id,
        "strategy_id": "",
        "activation_mode": normalized_status,
        "trade": trade,
        "summary": {
            "mtm": round(total_mtm, 2),
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": len(pending_legs),
            "broker_orders_count": len(broker_orders),
            "notifications_count": len(notifications),
            "strategy_count": strategy_count,
        },
        "legs": {
            "all": all_legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": pending_legs,
            "pending_feature_legs": pending_feature_legs,
        },
        "broker_orders": broker_orders[:1000],
        "open_orders": [
            order for order in broker_orders
            if str(order.get("status") or "").strip().upper() in {"OPEN", "PENDING", "TRIGGER PENDING"}
        ][:1000],
        "notifications": notifications[:1000],
        "notification_status": notification_status,
        "trade_notifications": trade_notifications[:1000],
        "execution_config_base": (strategy_execution_configs[0].get("execution_config_base") if strategy_execution_configs else {}),
        "execution_config_extra": (strategy_execution_configs[0].get("execution_config_extra") if strategy_execution_configs else {}),
        "strategy_execution_configs": strategy_execution_configs,
    }


def _aggregate_portfolio_trade_history_payload(portfolio_id: str, normalized_status: str, payloads: list[dict]):
    valid_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    if not valid_payloads:
        raise HTTPException(status_code=404, detail="Strategy trade history not found for this portfolio")

    # Group individual strategy payloads by group_id
    groups_map: dict[str, list[dict]] = {}
    for payload in valid_payloads:
        gid = str(payload.get("group_id") or ((payload.get("trade") or {}).get("portfolio") or {}).get("group_id") or "").strip()
        if not gid:
            gid = "__no_group__"
        groups_map.setdefault(gid, []).append(payload)

    # Build per-group aggregations
    groups = []
    for gid, group_payloads in groups_map.items():
        group_agg = _aggregate_group_trade_history_payload(gid, normalized_status, group_payloads)
        group_agg["strategies"] = group_payloads
        groups.append(group_agg)

    # Sort groups by group_id for stable ordering
    groups.sort(key=lambda g: str(g.get("group_id") or ""))

    # Portfolio-level aggregation (sum of all strategies)
    portfolio_agg = _aggregate_group_trade_history_payload(portfolio_id, normalized_status, valid_payloads)
    trade = portfolio_agg.get("trade") if isinstance(portfolio_agg.get("trade"), dict) else {}
    portfolio_meta = trade.get("portfolio") if isinstance(trade.get("portfolio"), dict) else {}
    portfolio_name = str(portfolio_meta.get("group_name") or trade.get("name") or "").strip() or f"Portfolio {portfolio_id}"
    strategy_count = len(valid_payloads)
    group_count = len(groups)

    trade["_id"] = portfolio_id
    trade["name"] = f"{portfolio_name} ({strategy_count})"
    portfolio_meta["portfolio"] = portfolio_id
    trade["portfolio"] = portfolio_meta

    portfolio_agg["view_type"] = "portfolio"
    portfolio_agg["portfolio_id"] = portfolio_id
    portfolio_agg["group_id"] = str(portfolio_meta.get("group_id") or "").strip()
    portfolio_agg["strategy_id"] = ""
    portfolio_agg["trade"] = trade
    portfolio_agg["summary"]["group_count"] = group_count
    portfolio_agg["groups"] = groups
    portfolio_agg["strategies"] = valid_payloads
    return portfolio_agg


@router.get("/strategy-trade-history/group/{group_id}")
async def get_group_trade_history(group_id: str, status: str = "algo-backtest"):
    db = MongoData()
    try:
        normalized_group_id = str(group_id or "").strip()
        normalized_status = _normalize_runtime_activation_mode(status)
        normalized_date = _default_runtime_trade_date(normalized_status)
        if not normalized_group_id:
            raise HTTPException(status_code=400, detail="group_id is required")

        group_query: dict[str, Any] = {
            "strategy_group_id": normalized_group_id,
            "activation_mode": normalized_status,
        }
        if normalized_date:
            group_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
        raw_trades = list(db._db["algo_trades"].find(group_query))
        if not raw_trades:
            fallback_query: dict[str, Any] = {"strategy_group_id": normalized_group_id}
            if normalized_date:
                fallback_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(fallback_query))
        if not raw_trades:
            legacy_query: dict[str, Any] = {
                "portfolio.group_id": normalized_group_id,
                "activation_mode": normalized_status,
            }
            if normalized_date:
                legacy_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(legacy_query))
        if not raw_trades:
            legacy_fallback_query: dict[str, Any] = {"portfolio.group_id": normalized_group_id}
            if normalized_date:
                legacy_fallback_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(legacy_fallback_query))
        if not raw_trades:
            raise HTTPException(status_code=404, detail="Strategy trade history not found for this group_id")

        # Use actual activation_mode from trade (not query param) so closed/live trades are resolved correctly
        payloads = [
            _build_trade_history_payload(db._db, raw_trade, str(raw_trade.get("activation_mode") or normalized_status))
            for raw_trade in raw_trades
        ]
        result = _aggregate_group_trade_history_payload(normalized_group_id, normalized_status, payloads)
        result["strategies"] = payloads
        return result
    finally:
        try:
            db.close()
        except Exception:
            pass


@router.get("/strategy-trade-history/portfolio/{portfolio_id}")
async def get_portfolio_trade_history(portfolio_id: str, status: str = "algo-backtest"):
    db = MongoData()
    try:
        normalized_portfolio_id = str(portfolio_id or "").strip()
        normalized_status = _normalize_runtime_activation_mode(status)
        normalized_date = _default_runtime_trade_date(normalized_status)
        if not normalized_portfolio_id:
            raise HTTPException(status_code=400, detail="portfolio_id is required")

        portfolio_query: dict[str, Any] = {
            "trade_portfolio": normalized_portfolio_id,
            "activation_mode": normalized_status,
        }
        if normalized_date:
            portfolio_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
        raw_trades = list(db._db["algo_trades"].find(portfolio_query))
        if not raw_trades:
            fallback_query: dict[str, Any] = {"trade_portfolio": normalized_portfolio_id}
            if normalized_date:
                fallback_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(fallback_query))
        if not raw_trades:
            legacy_query: dict[str, Any] = {
                "portfolio.portfolio": normalized_portfolio_id,
                "activation_mode": normalized_status,
            }
            if normalized_date:
                legacy_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(legacy_query))
        if not raw_trades:
            legacy_fallback_query: dict[str, Any] = {"portfolio.portfolio": normalized_portfolio_id}
            if normalized_date:
                legacy_fallback_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(legacy_fallback_query))
        if not raw_trades:
            tgp_query: dict[str, Any] = {
                "portfolio.trade_group_portfolio": normalized_portfolio_id,
                "activation_mode": normalized_status,
            }
            if normalized_date:
                tgp_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(tgp_query))
        if not raw_trades:
            tgp_fallback_query: dict[str, Any] = {"portfolio.trade_group_portfolio": normalized_portfolio_id}
            if normalized_date:
                tgp_fallback_query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
            raw_trades = list(db._db["algo_trades"].find(tgp_fallback_query))
        if not raw_trades:
            raise HTTPException(status_code=404, detail="Strategy trade history not found for this portfolio")

        payloads = [
            _build_trade_history_payload(db._db, raw_trade, normalized_status)
            for raw_trade in raw_trades
        ]
        return _aggregate_portfolio_trade_history_payload(normalized_portfolio_id, normalized_status, payloads)
    finally:
        try:
            db.close()
        except Exception:
            pass


@router.post("/portfolio/backtest/start")
async def portfolio_backtest_start(request: dict):
    """
    Start a portfolio backtest in background.
    Runs backtest for every strategy in the portfolio sequentially.

    Request body:
      { "portfolio": "<portfolio_id>", "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD", "weekly_old_regime": true, "source": "WEB" }

    Returns: { "job_id": "...", "status": "running", "strategy_count": N }

    Then poll:  GET /algo/backtest/status/{job_id}
    Then fetch: GET /algo/backtest/result/{job_id}
      → { "status": "completed", "progress": 100,
          "results": [ { "_id": "...", "item_id": "<strategy_id>",
                         "status": "completed", "results": {...} }, ... ] }
    """
    portfolio_id = (request.get("portfolio") or "").strip()
    start_date   = request.get("start_date")
    end_date     = request.get("end_date")

    if not portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio field is required")
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required")

    try:
        oid = ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio id")

    db        = MongoData()
    portfolio = db._db["saved_portfolios"].find_one({"_id": oid}, {"strategy_ids": 1})
    db.close()

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    strategy_ids     = portfolio.get("strategy_ids", [])
    total_strategies = len(strategy_ids)

    if not strategy_ids:
        raise HTTPException(status_code=400, detail="Portfolio has no strategies")

    fingerprint      = _request_fingerprint(request)
    # Estimate trading days × number of strategies for progress denominator
    estimated_days   = _estimate_total_steps({"start_date": start_date, "end_date": end_date})
    estimated_total  = total_strategies * max(estimated_days, 1)

    with _jobs_lock:
        _cleanup_old_jobs()
        for existing_job_id, job in _jobs.items():
            if job["status"] == "running":
                if job.get("fingerprint") == fingerprint:
                    return {
                        "job_id":         existing_job_id,
                        "status":         "running",
                        "message":        "Identical portfolio backtest is already running",
                        "strategy_count": total_strategies,
                    }
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Another backtest is already running. Wait for it to finish.",
                        "job_id":  existing_job_id,
                    },
                )

        job_id = str(uuid.uuid4())[:8]
        _jobs[job_id] = {
            "status":         "running",
            "completed":      0,
            "total":          estimated_total,
            "percent":        0.0,
            "current_day":    "Queued",
            "error":          None,
            "created_at":     time.time(),
            "fingerprint":    fingerprint,
            "strategy_count": total_strategies,
            "strategy_index": 0,
        }

    _write_job_state(job_id, {
        "job_id":         job_id,
        "status":         "running",
        "completed":      0,
        "total":          estimated_total,
        "percent":        0.0,
        "current_day":    "Queued",
        "fingerprint":    fingerprint,
        "error":          None,
        "strategy_count": total_strategies,
        "strategy_index": 0,
        "updated_at":     time.time(),
    })

    proc = multiprocessing.Process(
        target=_run_portfolio_job, args=(job_id, request), daemon=False
    )
    proc.start()
    with _jobs_lock:
        _jobs[job_id]["pid"] = proc.pid

    return {"job_id": job_id, "status": "running", "strategy_count": total_strategies}


@router.get("/backtest/result/{job_id}", response_class=ORJSONResponse)
async def backtest_result(job_id: str):
    """
    Get final result once status=done.
    Returns 400 if still running, 500 if errored.

    ORJSONResponse instead of FastAPI's default (stdlib json under the hood):
    a multi-strategy portfolio result can be 20MB+ (trades with per-minute
    P&L timelines x hundreds of trades x several strategies) — stdlib json
    was measured at ~280ms just to re-encode that for the HTTP response;
    orjson does the same encode in ~40ms.
    """
    job = _read_job_state(job_id) or _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if job["status"] == "running":
        raise HTTPException(status_code=400, detail="Backtest still running. Check /backtest/status/{job_id}")
    if job["status"] == "error":
        raise HTTPException(status_code=500, detail=job["error"])
    return job["result"]


# ─── Notification history ──────────────────────────────────────────────────────

@router.get("/notifications")
async def get_notifications(
    trade_id: str = "",
    strategy_id: str = "",
    trade_date: str = "",
    event_type: str = "",
    limit: int = 200,
):
    """
    Fetch algo_trade_notification records.

    Query params (all optional):
      - trade_id      : filter by trade _id
      - strategy_id   : filter by strategy_id
      - trade_date    : YYYY-MM-DD
      - event_type    : entry_taken | sl_hit | target_hit | trail_sl_changed | ...
      - limit         : max records (default 200)
    """
    db = MongoData()
    col = db._db["algo_trade_notification"]

    query: dict = {}
    if trade_id:
        query["trade_id"] = trade_id.strip()
    if strategy_id:
        query["strategy_id"] = strategy_id.strip()
    if trade_date:
        query["trade_date"] = trade_date.strip()
    if event_type:
        query["event_type"] = event_type.strip()

    safe_limit = max(1, min(int(limit), 1000))
    docs = list(
        col.find(query, {"_id": 0})
           .sort("timestamp", 1)
           .limit(safe_limit)
    )
    return {
        "success": True,
        "count": len(docs),
        "notifications": docs,
    }


def _sync_broker_configuration_mirror(
    db, *, match_name: str, broker_icon: str, broker_key: str,
    api_key: str, api_secret: str, broker_user_id: str, app_user_id: str, base_url: str,
) -> None:
    """Keep the broker_configuration doc that live order placement reads from in sync
    with a broker's own credentials store (kite_market_config for Dhan today — any
    other broker's save path can call this the same way later). Matches by stable
    name/broker_type identity, never by api_key/secret (that's exactly what's
    changing), so a key rotation never orphans the link the way matching on the old
    key/secret would. Never sets access_token/login_time — only a real login (cron or
    OAuth consent) should ever do that.
    """
    if not api_key and not api_secret:
        return
    col = db._db["broker_configuration"]
    match = {"name": match_name, "broker_type": "live"}
    set_fields: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if api_key:
        set_fields["api_key"] = api_key
    if api_secret:
        set_fields["api_secret"] = api_secret
    if broker_user_id:
        set_fields["broker_user_id"] = broker_user_id
    set_on_insert = {
        "broker_icon": broker_icon,
        "name": match_name,
        "broker_type": "live",
        "redirect_url": f"{base_url}/broker/{broker_key}/callback",
        "user_id": app_user_id,
        "created_at": set_fields["updated_at"],
    }
    col.update_one(match, {"$set": set_fields, "$setOnInsert": set_on_insert}, upsert=True)


# ---------------------------------------------------------------------------
# broker_setup — the static catalog of every broker we could integrate
# (icon, exchanges, whether we actually support it yet). This is distinct
# from `broker_configuration`, which holds each app user's own connected
# accounts (API keys, tokens, login state). The frontend's "choose a broker"
# grid reads this catalog; picking an active broker there is what creates a
# broker_configuration doc for that user.
# ---------------------------------------------------------------------------

BROKER_SETUP_SEED: list[dict] = [
    # -- brokers we actually integrate today --
    {"broker_key": "dhan",       "display_name": "Dhan Client",   "broker_icon": "dhan.svg",       "badges": ["NSE", "BSE", "MCX"], "pricing_note": "", "is_active": True, "sort_order": 10},
    {"broker_key": "zerodha",    "display_name": "Zerodha Kite",  "broker_icon": "kite-logo.svg",  "badges": ["NSE", "BSE"],        "pricing_note": "", "is_active": True, "sort_order": 20},
    {"broker_key": "flattrade",  "display_name": "FlatTrade",     "broker_icon": "flattrade.svg",  "badges": ["NSE", "BSE"],        "pricing_note": "", "is_active": True, "sort_order": 30},
    # -- catalog of brokers not yet wired up (shown as "Coming Soon") --
    {"broker_key": "upstox",             "display_name": "Upstox Client",          "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 100},
    {"broker_key": "5paisa_xstream",     "display_name": "5Paisa XStream (Client)","badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 110},
    {"broker_key": "5paisa_xts",         "display_name": "5Paisa XTS",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 120},
    {"broker_key": "ac_agrawal",         "display_name": "A.C. Agrawal Shares",    "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 130},
    {"broker_key": "aliceblue",          "display_name": "Alice Blue Client",      "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 140},
    {"broker_key": "angelone",           "display_name": "Angel One Limited",      "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 150},
    {"broker_key": "arham",              "display_name": "Arham Wealth",           "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 160},
    {"broker_key": "bigul",              "display_name": "Bigul",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 170},
    {"broker_key": "coinswitch",         "display_name": "CoinSwitch",             "badges": ["COINSWITCH"], "pricing_note": "", "is_active": False, "sort_order": 180},
    {"broker_key": "db_international",   "display_name": "DB International",      "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 190},
    {"broker_key": "delta_exchange",     "display_name": "Delta Exchange",         "badges": ["DELTA"], "pricing_note": "", "is_active": True, "sort_order": 200},
    {"broker_key": "delta_exchange_api", "display_name": "Delta Exchange (API)",   "badges": ["DELTA"], "pricing_note": "", "is_active": False, "sort_order": 210},
    {"broker_key": "findoc",             "display_name": "Findoc",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 220},
    {"broker_key": "findoc_xts",         "display_name": "Findoc XTS",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 230},
    {"broker_key": "firstock",           "display_name": "Firstock",               "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 240},
    {"broker_key": "fyers",              "display_name": "Fyers",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 250},
    {"broker_key": "groww",              "display_name": "Groww",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 260},
    {"broker_key": "hdfc_sky",           "display_name": "HDFC Sky",               "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 270},
    {"broker_key": "ibulls",             "display_name": "IBulls",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 280},
    {"broker_key": "iifl",               "display_name": "IIFL",                   "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 290},
    {"broker_key": "iiflcapital",        "display_name": "IIFL Capital",           "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 300},
    {"broker_key": "indmoney",           "display_name": "IndMoney",               "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 310},
    {"broker_key": "jainam_xts",         "display_name": "Jainam XTS",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 320},
    {"broker_key": "kotak",              "display_name": "Kotak Securities",       "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 330},
    {"broker_key": "motilal",            "display_name": "Motilal Oswal",          "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 340},
    {"broker_key": "mstock",             "display_name": "mStock",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 350},
    {"broker_key": "nubra",              "display_name": "Nubra",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 360},
    {"broker_key": "paytm",              "display_name": "Paytm Money",            "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 370},
    {"broker_key": "pocketful",          "display_name": "Pocketful",              "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 380},
    {"broker_key": "rmoney",             "display_name": "RMoney",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 390},
    {"broker_key": "samco",              "display_name": "Samco",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 400},
    {"broker_key": "shoonya",            "display_name": "Shoonya",                "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 410},
    {"broker_key": "tradejini",          "display_name": "TradeJini",              "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 420},
    {"broker_key": "tradesmart",         "display_name": "TradeSmart",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 430},
    {"broker_key": "wisdom_xts",         "display_name": "Wisdom Capital (XTS)",   "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 440},
    {"broker_key": "zebu",               "display_name": "Zebu",                   "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 450},
    {"broker_key": "compositedge",       "display_name": "CompositEdge",           "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 460},
]


def _f(key: str, label: str, placeholder: str = "") -> dict:
    """One setup-form field. masked is derived by name convention (secret/totp/pin
    fields get password-masked) — matches AlgoTest's own field data, which never
    stores an explicit type/secure flag either, just a canonical key name."""
    return {
        "key": key,
        "label": label,
        "placeholder": placeholder or label,
        "masked": ("secret" in key) or ("totp" in key) or key == "pin",
    }


def _i(text: str, href: str | None = None, copy: str | None = None, label: str | None = None) -> dict:
    """One instruction step. `instructions` entries are either a plain str
    (old convention — auto-links only if the text contains the literal
    phrase "this link", pointing at the broker's single `website_url`) or
    one of these richer dicts (own href + own copy-paste snippet per step,
    for brokers whose real setup flow needs per-step links/snippets — mail
    templates, promo codes, docs links). Both shapes are handled by the
    frontend's buildInstructionItems (BrokerSetupDetail.tsx and
    BrokerSetupInstructions.tsx each have their own copy).

    `label` is the exact substring of `text` that should become the clickable
    link (e.g. text="Go to login.upstox.com and login", label="login.upstox.com")
    — mirrors the old string convention's "this link" phrase-matching, just
    with an explicit label instead of a hardcoded phrase. Without it, `href`
    still renders as a standalone "open link" tail."""
    step: dict = {"text": text}
    if href:
        step["href"] = href
    if copy:
        step["copy"] = copy
    if label:
        step["label"] = label
    return step


_XTS_FIELDS = [
    _f("api_key", "Interactive API Key"),
    _f("api_secret", "Interactive API Secret"),
    _f("market_data_api_key", "Market Data API Key"),
    _f("market_data_api_secret", "Market Data API Secret"),
]

# Per-broker field schema, sourced from AlgoTest's
# Common/constants/BrokerInstructions/*/index.ts InputFieldList data.
#
# `login_url_template` is the LITERAL broker OAuth/login URL AlgoTest's code
# builds, copied verbatim from each broker's *_LOGIN_METHOD function (with
# {api_key}/{redirect_uri}/{broker_id} standing in for the runtime values it
# interpolates) — real, working broker endpoints, not guesses. Where a
# template embeds a partner/vendor-specific value (5paisa's VendorKey,
# Findoc's ODIN sPartnerId), that value is AlgoTest's own partner
# registration — we'd need our own with that broker to actually use it;
# flagged inline. Left None where the broker's on_login was server-side,
# unwired, or not present in this backup — better than inventing one.
#
# `instructions` — the *.mdx source files aren't in this decompiled backup,
# but their COMPILED output (the actual step-by-step copy AlgoTest shows) is
# baked into algotest/test.js as JSX call trees, one per broker, in the
# broker-instructions region (~test.js:65700-68700). Where that was
# recoverable and confidently matched to a broker (via its own domain/
# contact-email appearing in the text — see extraction notes, 2026-08-10),
# the real multi-step content is here (via `_i()`, richer than a plain str).
# Brokers not yet matched still carry the old placeholder/generic text.
#
# Brokers with no field-list match in the source get a generic
# client_id/api_key/api_secret fallback, flagged "fallback": True.
#
# Dhan is the one deliberate exception throughout: our own integration uses
# unattended TOTP+PIN login (dhan_totp_login.py), not AlgoTest's OAuth-
# consent flow, so its fields reflect what OUR system needs.
_XTS_NOTE = ["XTS-family broker — the four API keys are used directly against the broker's XTS API; there's no login redirect."]

BROKER_FIELD_SCHEMA: dict[str, dict] = {
    # totp_token/pin deliberately excluded from `fields` — that pair drives the
    # one shared platform-wide daily auto-login (dhan_totp_login.py), admin-side
    # only, never a per-user field. DhanCredentialsForm.tsx has its own field
    # list (not this generic schema) and enforces the same exclusion.
    "dhan":               {"auth_type": "totp_pin", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": None,
                            "instructions": [
                                _i(text="Please go to web.dhan.co and login to your account", href="https://web.dhan.co/", label="web.dhan.co"),
                                _i(text='Click on "My Profile on Dhan" (Top right corner, Under your photo)'),
                                _i(text="Copy your Client ID from under Profile Details, and paste it below"),
                            ]},
    "zerodha":            {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://kite.zerodha.com/connect/login?v=3&api_key={api_key}", "instructions": [
                            _i(text="Go to Kite Connect's Developer Portal and create a new app. Choose Type as Connect or Personal, enter any App Name you like, and enter your Broker Client ID."),
                            _i(text="Copy the API key and API secret and paste them in below and click Add."),
                        ]},
    "flattrade":          {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://auth.flattrade.in/?app_key={api_key}", "instructions": [
                            _i(text='Please to go to https://wall.flattrade.in and login to your account', href='https://wall.flattrade.in/', label='https://wall.flattrade.in'),
                            _i(text='Navigate to “Pi” on the menu section of the Page.'),
                            _i(text='Leave the Postback as blank'),
                            _i(text='Copy the App Key and Secret Key and paste them below'),
                        ]},
    "upstox":             {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}&state={api_key}", "instructions": [
                            _i(text='Please to go to login.upstox.com and login to your account.', href='https://login.upstox.com/', label='login.upstox.com'),
                            _i(text='Go to "My Account" section, choose the App tab, then click on New App'),
                            _i(text='Enter any App Name.'),
                            _i(text='Click on App Details. Copy your API Key and Secret, and paste them below.'),
                        ]},
    "5paisa_xstream":     {"auth_type": "oauth", "fields": [_f("client_id", "User ID"), _f("api_key", "Client Code"), _f("api_secret", "API Key"), _f("host_lookup_url", "Encryption Key")],
                            "login_url_template": "https://dev-openapi.5paisa.com/WebVendorLogin/VLogin/Index?VendorKey={vendor_key}&ResponseURL={redirect_uri}",
                            "website_url": "https://www.5paisa.com",
                            "instructions": ["Please go to this link and login to your account.", "Click on profile from top right, copy the client ID.", "Paste the client ID above."],
                            "note": "Requires our own 5paisa API vendor-key partnership to actually go live — the VendorKey AlgoTest uses is theirs, not ours."},
    "5paisa_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='If you do not have a 5Paisa account, you can open one here for free . Use this promo code to get ₹2000 worth vouchers from top brands', href='https://5paisa.page.link/pFrBGaxBvHJASyGW8', copy='RAGH4073'),
                            _i(text='If you already have API key & Secret pair, for both the Interactive as well as Market Data, then you can skip these steps, and directly copy/paste those in the respective fields below.'),
                            _i(text='API Activation: Send an email to shrikant.shirke@5paisa.com nisha.thakur@5paisa.com , for activating XTS API and terminal for your account with the following information:', href='mailto:shrikant.shirke@5paisa.com', label='shrikant.shirke@5paisa.com'),
                            _i(text='Keep the mail subject as', copy='XTS API Creation for {UICONFIG.brandName}'),
                            _i(text='Keep the Mail body as', copy='Please get my XTS API created subjected to below details:'),
                            _i(text='Client Name'),
                            _i(text='Client Code'),
                            _i(text='Once you receive the email with API Key details and your login ID and password. Copy the API key and secret pair, and paste them in the respective fields below.'),
                            _i(text='Make sure you do not use the same API key and secret pair to fire trades from anywhere else, otherwise your executions would run in unexpected errors.'),
                            _i(text='Once your XTS account has been made and XTS API been created, you will no longer be able to input any orders from the 5Paisa trading terminal/App. You can still view your live MTM & position detail from the 5Paisa trading terminal/App.'),
                            _i(text="Don't worry, you can still take manual trades from your 5Paisa XTS account, please follow this link to login to your XTS account(For manual XTS web trading)", href='https://xtsmum.5paisa.com/'),
                            _i(text='For any issue in any of the previous step, please contact Mr. Deepak Tripathi:', href='tel:+91-9699054837', label='Mr. Deepak Tripathi'),
                        ]},
    "ac_agrawal":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='Please to go to https://www.acagarwal.com and select Trading API from Product and tools tab.', href='https://www.acagarwal.com', label='https://www.acagarwal.com'),
                            _i(text='Click on Request API link and enter Client id, confirm it by entering OTP'),
                            _i(text='Upon submission Agree to the terms and condition column and click submit'),
                            _i(text='You will receive a mail within 24 hours of submission and then can proceed for API key generation'),
                            _i(text='Click on API Link, first link under API Dashboard, as shared in mail and select Trading API from Product and tools tab.', href='https://www.acagarwal.com/', label='as shared in mail'),
                            _i(text="Create profile by entering details. Field marked with '*' are mandatory"),
                            _i(text='Verification mail will be sent on your email id. Click the link in the email to verify your email.'),
                            _i(text="Post successful registration & Email address verification. Click 'Continue to Login'"),
                            _i(text='Login Id & password will be shared over mail'),
                            _i(text='Note : Below steps to be initiated only after Activation of API registration request.'),
                            _i(text='Proceed for API Subscription by clicking API Catalogue'),
                            _i(text="Click on 'Create New Application'. It will ask to Validate your XTS login by putting your XTS Client Id and XTS login password shared with you on mail in (5)"),
                            _i(text='Post validation, subscribe to Interactive and Market Data API'),
                            _i(text="Under the subscription page, please mention below URL in the 'Redirect URL' Box. URL for Marketdata API & Interactive API are mentioned below. Note the URLs are different from the link mentioned in the shared PDF in step 4"),
                            _i(text='MarketData API :', copy='https://developers.symphonyfintech.in/doc/marketdata/'),
                            _i(text='Interactive API :', copy='https://developers.symphonyfintech.in/doc/interactive/'),
                            _i(text='Post subscription Login credentials along with secret key will be shared. User can login to Live Environment using the same.'),
                            _i(text='Copy and paste the API key and secret below, once the App becomes active.'),
                            _i(text='For any issue in any of the previous step, please contact Mr. Yogesh Sharma:', href='tel:+91-6378882400', label='Mr. Yogesh Sharma'),
                        ]},
    "aliceblue":          {"auth_type": "oauth", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://ant.aliceblueonline.com/?appcode={api_key}&state={broker_id}", "instructions": [
                            _i(text='If you do not have a Aliceblue account, you can open one here with following benefits', href='https://aliceblueonline.com/open-account-fill-kyc-request-call-back?C=SHYD1331'),
                            _i(text='BROKERAGE: ₹15 / order'),
                            _i(text='Please to go to https://ant.aliceblueonline.com/ and login to your account', href='https://ant.aliceblueonline.com/', label='https://ant.aliceblueonline.com/'),
                            _i(text='Enter the client id visible on the top right of the screen, below your name.'),
                        ]},
    "angelone":           {"auth_type": "oauth", "fields": [_f("api_key", "API Key")],
                            "login_url_template": "https://smartapi.angelbroking.com/publisher-login?api_key={api_key}&redirect_url={redirect_uri}", "instructions": [
                            _i(text='Please go to this link and login to your account.', href='https://bit.ly/4ktlAu4'),
                            _i(text='Create a new app, and fill out the following details'),
                            _i(text='App Name -'),
                            _i(text='Enter the API key of the generated app below.'),
                        ]},
    "arham":              {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key")], "login_url_template": None, "instructions": [
                            _i(text='Enter your client id'),
                            _i(text='Enter your API Key'),
                        ], "fallback": True},
    "bigul":              {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='Go to https://trading.bigul.co/dashboard#!/login', href='https://trading.bigul.co/dashboard#!/login', label='https://trading.bigul.co/dashboard#!/login'),
                            _i(text="Click 'Create Account' and enter the necessary details to sign up on the API dashboard portal. Keep the user ID the same as your client ID and the password the same as your trading account password for convenience."),
                            _i(text='Post Login create the API account and fill all the information as per the account opening done with Bigul. USER ID is your client code or your trading code need to enter as per the and fill the information as per the bigul trading account. Once you log in for the first time, you will have to validate your trading ID. When it prompts put in your client ID and trading account password to validate your trading account. Once the trading account is validated, you will be able to create a new API app'),
                            _i(text='You will have to create 2 API apps: Interactive API app (Order Management API) & Market data API app'),
                            _i(text='Refer to the link for further information. How to create API apps for Bigul?', href='https://bigul.co/en/wp-content/uploads/2024/02/How-to-activate-API-2.pdf', label='How to create API apps for Bigul'),
                        ]},
    "coinswitch":         {"auth_type": "other", "fields": [_f("api_key", "API Key"), _f("api_secret", "Secret Key")], "login_url_template": None, "instructions": [
                            _i(text='Go to coinswitch.co and log in to your CoinSwitch account.', href='https://coinswitch.co/', label='coinswitch.co'),
                            _i(text='Navigate to API Trading.'),
                            _i(text='Click Generate API Key & Secret.'),
                            _i(text='Copy the generated API Key and API Secret.'),
                            _i(text='Paste the API Key and API Secret in the fields provided here.'),
                        ]},
    "db_international":  {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": _XTS_NOTE},
    "delta_exchange":     {"auth_type": "other", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None,
                            "instructions": [
                            _i(text='Use this link to create an account and avail 10% discount on transaction charges https://www.delta.exchange/?code='),
                            _i(text='Copy and paste your client ID'),
                        ]},
    "delta_exchange_api": {"auth_type": "other", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None,
                            "instructions": ["Login is handled server-side in AlgoTest's source — the exact redirect URL wasn't recoverable from this backup."]},
    "findoc":             {"auth_type": "consent", "fields": [_f("api_key", "Client ID")],
                            "login_url_template": "https://connector-app.odinconnector.co.in/landing/redirect?sAppToken={app_token}&sTwoWayToken={two_way_token}&sPartnerId={partner_id}&oEchoBackObject={state}",
                            "instructions": ["Requires our own ODIN connector partner registration (sAppToken/sTwoWayToken/sPartnerId) — AlgoTest's are theirs, not usable by us."]},
    "findoc_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='Go to https://xts.myfindoc.com/dashboard#!/login', href='https://xts.myfindoc.com/dashboard#!/login', label='https://xts.myfindoc.com/dashboard#!/login'),
                            _i(text="Click 'Create Account' and enter the necessary details to sign up on the API dashboard portal. Keep the user ID the same as your client ID and the password the same as your trading account password for convenience."),
                            _i(text='Post Login create the API account and fill all the information as per the account opening done with Findoc. USER ID is your client code or your trading code need to enter as per the and fill the information as per the Findoc trading account. Once you log in for the first time, you will have to validate your trading ID. When it prompts put in your client ID and trading account password to validate your trading account. Once the trading account is validated, you will be able to create a new API app'),
                            _i(text='You will have to create 2 API apps: Interactive API app (Order Management API) & Market data API app'),
                        ]},
    "firstock":           {"auth_type": "other", "fields": [_f("client_id", "Vendor Code"), _f("api_key", "API Key")], "login_url_template": None, "instructions": [
                            _i(text='Set up your API key and Vendor code by logging into key generation and generate a vendor code and API key.', href='https://connect.thefirstock.com/login', label='key generation'),
                            _i(text='Paste the generated vendor code and API key below'),
                        ]},
    "fyers":              {"auth_type": "oauth", "fields": [_f("api_key", "App ID"), _f("api_secret", "Secret ID")],
                            "login_url_template": "https://api-t1.fyers.in/api/v3/generate-authcode?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}&state={broker_id}", "instructions": [
                            _i(text='BROKERAGE: ₹20/order'),
                            _i(text='Go to https://fyers.in/web/api-dashboard/user-apps and create a new app. Fill out the following details', href='https://fyers.in/web/api-dashboard/user-apps', label='https://fyers.in/web/api-dashboard/user-apps'),
                            _i(text='App Icon'),
                            _i(text='App Name'),
                            _i(text='Give all available permissions for the app'),
                            _i(text='Post App Creation, Copy the App ID and the Secret ID of the generated App and paste them here'),
                        ]},
    "groww":              {"auth_type": "other", "fields": [_f("api_key", "Client ID"), _f("totp_token", "TOTP Token")], "login_url_template": None,
                            "instructions": [
                            _i(text='Go to https://groww.in/trade-api/api-keys and generate your API key', href='https://groww.in/trade-api/api-keys', label='https://groww.in/trade-api/api-keys'),
                            _i(text='Click on Generate API key > Generate TOTP token to set up TOTP-based authentication'),
                            _i(text='Copy your API Key TOTP Token and paste it below'),
                        ]},
    "hdfc_sky":           {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": []},
    "ibulls":             {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "iifl":               {"auth_type": "consent", "fields": [_f("client_id", "Client ID"), _f("api_secret", "API Secret"), _f("host_lookup_url", "App Key")], "login_url_template": None,
                            "instructions": [
                            _i(text='If you already have API key & Secret pair, for both the Interactive as well as Market Data, then you can skip steps 2 to 7.'),
                            _i(text='F&O Activation: Make sure F&O has been activated for your account. You can check the status here: IIFL F&O Status and activate it from the same link. You can also get in touch with your IIFL RM to activate your F&O', href='https://ttweb.indiainfoline.com/Trade/Dashboard.aspx'),
                            _i(text='Login and Go to Dashboard -> My Account -> My Details -> Trading Preference', href='https://ttweb.indiainfoline.com/Trade/Dashboard.aspx', label='Dashboard'),
                            _i(text='API Activation: Send an email to ttblazesupport@iifl.com for activating XTS API and Blaze terminal for your account with the following information:', href='mailto:ttblazesupport@iifl.com', label='ttblazesupport@iifl.com'),
                            _i(text='Client ID'),
                            _i(text='Registered Name'),
                            _i(text='Mobile Number'),
                            _i(text='PAN Number'),
                            _i(text='Date of Birth'),
                            _i(text='Location(City)'),
                            _i(text='Segments: CM, F&O'),
                            _i(text='Email'),
                            _i(text='Once you receive a welcome mail with Login ID and password from Blaze IIFL, create a new account on Blaze API using this link: Blaze terminal', href='https://ttblaze.iifl.com/dashboard#!/login'),
                            _i(text='After creating the account validate your Trading ID with the Login ID and Password received on the mail in the previous step.'),
                            _i(text='Under My App -> Create new app. You need to create an Interactive Data API as well as Market Data API.'),
                            _i(text='Once created you will get a mail with your API key and secret for both the APIs. The current status of the API will be De-activated.'),
                            _i(text='Once the status of both Interactive and Market Data app changes to Active you can copy the respective API key and secret pair, and paste them below.'),
                            _i(text='Make sure you do not use the same API key and secret pair to fire trades from anywhere else, otherwise your executions would run in unexpected errors.'),
                            _i(text='For any issue in any of the previous step, please contact Mr. Santosh Gupta:', href='tel:+91-8591403350', label='Mr. Santosh Gupta'),
                        ]},
    "iiflcapital":        {"auth_type": "other", "fields": [_f("client_id", "Client ID")], "login_url_template": None, "instructions": [], "fallback": True},
    "indmoney":           {"auth_type": "oauth", "fields": [_f("api_key", "Client ID")], "login_url_template": None,
                            "instructions": ["Uses PKCE OAuth (code_challenge generated server-side) — the exact URL wasn't fully recoverable from this backup."]},
    "jainam_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": _XTS_NOTE},
    "kotak":              {"auth_type": "other", "fields": [_f("api_key", "Access Token")], "login_url_template": None,
                            "instructions": ["No login redirect — paste an access token generated from Kotak Neo's own dashboard."]},
    "motilal":            {"auth_type": "oauth", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://invest.motilaloswal.com/OpenAPI/Login.aspx?apikey={api_key}", "instructions": [
                            _i(text='You will need to provide consent to use open API. Go to onlinetrade.motilaloswal.com/emailers/TGS/2022/Mailer/Jun22/14Jun2022/API-consent-Mailer.html and follow the given instructions.', href='https://onlinetrade.motilaloswal.com/emailers/TGS/2022/Mailer/Jun22/14Jun2022/API-consent-Mailer.html'),
                            _i(text='You also need setup TOTP for your account. Learn more here'),
                            _i(text='Copy your API key and Client ID and paste them here'),
                        ]},
    "mstock":             {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key")], "login_url_template": None, "instructions": [
                            _i(text='Login to trade.mstock.com', href='https://trade.mstock.com', label='trade.mstock.com'),
                            _i(text='Expand sidebar from top left near the broker logo, and click on "Trading APIs"'),
                            _i(text='Click on "Generate New API Key"'),
                            _i(text="Enter Application Name as your app's name."),
        _i(text='Choose API Type as "B"'),
                            _i(text='Keep the validity as 1 year and click on Submit.'),
                            _i(text='Enter your API Key and Client ID in the fields below'),
                        ]},
    "nubra":              {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "paytm":              {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://login.paytmmoney.com/merchant-login?apiKey={api_key}&state={api_key}", "instructions": [
                            _i(text='Please to go to developer.paytmmoney.com and click on SIGN IN', href='https://developer.paytmmoney.com', label='developer.paytmmoney.com'),
                            _i(text='Login with Paytm Money Registered Mobile Number/ Email.'),
                            _i(text='Click on Create New App. A single user can generate API keys and Secrets for up to five apps.'),
                            _i(text='Click proceed to create the app.'),
                            _i(text='Click proceed to get the API Key and secret. You can access the API Key and API secret for the app in the dashboard section.'),
                            _i(text='If you have any further questions, refer to the official documentation', href='https://www.paytmmoney.com/blog/how-to-create-api-key-and-secret/', label='official documentation'),
                        ]},
    "pocketful":          {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "rmoney":             {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='You are required to send a request for XTS API activation to RMoney.'),
                            _i(text='Please connect with your broker to get the interactive API and market data API credentials.'),
                        ]},
    "samco":              {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "shoonya":            {"auth_type": "oauth", "fields": [_f("api_key", "Client Code"), _f("api_secret", "Secret Code")],
                            "login_url_template": "https://trade.shoonya.com/OAuthlogin/authorize/oauth?client_id={api_key}", "instructions": [
                            _i(text='Please enter your Shoonya Client ID for broker setup.'),
                        ]},
    "tradejini":          {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://api.tradejini.com/v2/api-gw/oauth/authorize?client_id={api_key}&redirect_uri={redirect_uri}&response_type=code&scope=general&state={redirect_uri}", "instructions": [
                            _i(text="Login to Tradejini's API portal . If you don't have a account on Tradejini API Portal please create a new account by clicking on signup button.", href='https://api.tradejini.com/developer-portal/main', label="Tradejini's API portal"),
                            _i(text='Click on Create an App.'),
                            _i(text='Leave all other fields blank & Click on Save button.'),
                            _i(text='Copy the API Key and API Secret from there and paste it in below API Key and API Secret fields.'),
                        ]},
    "tradesmart":         {"auth_type": "other", "fields": [_f("api_key", "Client Code")], "login_url_template": None,
                            "instructions": [
                            _i(text='IMPORTANT - Please make sure Authenticator TOTP is setup. Go to TradeSmart Setting and make sure you have setup TOTP by generating QR Code and setting it up in Google Authenticator.', href='https://web.tradesmartonline.in/settings', label='TradeSmart Setting'),
                        ]},
    "wisdom_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='If you do not have a Wisdom Capital account, you can open one here', href='https://wisdomcapital.in/referral_chintan/'),
                            _i(text='BENEFITS: ₹999/month for unlimited trading.'),
                            _i(text='Please to go to trade.wisdomcapital.in/dashboard and login to your account', href='https://trade.wisdomcapital.in/dashboard#!/login', label='trade.wisdomcapital.in/dashboard'),
                            _i(text='For any issue in any of the previous step, please contact Trading API Helpline: 01206633215', href='tel:01206633215', label='01206633215'),
                            _i(text='Obtain API key & Secret pair, for both the Interactive as well as Market Data, and copy/paste those in the respective fields below.'),
                        ]},
    "zebu":               {"auth_type": "other", "fields": [_f("client_id", "Vendor Code"), _f("api_key", "API Key")], "login_url_template": None, "instructions": []},
    "compositedge":       {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": _XTS_NOTE, "fallback": True},
}

# Brand logos — downloaded once from AlgoTest's public asset host
# (algotest.in/brokers/*, the same trademark icons every broker-integration
# platform shows) into algo-admin/public/images/broker/, filename == broker_
# key. Only brokers that actually resolved to a real image are listed here;
# the rest keep the colored-initial-avatar fallback BrokerIcon already has.
BROKER_ICON_OVERRIDES: dict[str, str] = {
    "upstox": "upstox.svg", "5paisa_xstream": "5paisa_xstream.svg", "5paisa_xts": "5paisa_xts.svg",
    "ac_agrawal": "ac_agrawal.svg", "aliceblue": "aliceblue.svg", "angelone": "angelone.svg",
    "arham": "arham.svg", "bigul": "bigul.svg", "coinswitch": "coinswitch.svg",
    "db_international": "db_international.png", "delta_exchange": "delta_exchange.svg",
    "delta_exchange_api": "delta_exchange_api.svg", "findoc": "findoc.svg", "findoc_xts": "findoc_xts.svg",
    "firstock": "firstock.svg", "fyers": "fyers.svg", "groww": "groww.svg", "hdfc_sky": "hdfc_sky.svg",
    "iifl": "iifl.svg", "iiflcapital": "iiflcapital.svg", "indmoney": "indmoney.svg",
    "jainam_xts": "jainam_xts.svg", "kotak": "kotak.svg", "motilal": "motilal.svg", "mstock": "mstock.svg",
    "paytm": "paytm.svg", "rmoney": "rmoney.svg", "shoonya": "shoonya.svg", "tradejini": "tradejini.svg",
    "tradesmart": "tradesmart.svg", "wisdom_xts": "wisdom_xts.svg", "zebu": "zebu.svg",
}

for _entry in BROKER_SETUP_SEED:
    _schema = BROKER_FIELD_SCHEMA.get(_entry["broker_key"])
    if _schema:
        _entry.update(_schema)
    _icon = BROKER_ICON_OVERRIDES.get(_entry["broker_key"])
    if _icon:
        _entry["broker_icon"] = _icon


# ---------------------------------------------------------------------------
# broker_credentials — one unified collection for whatever a broker's
# broker_setup.fields schema asks for, across every broker in the catalog
# (not just the 3 wired up today). This is deliberately separate from
# broker_configuration/kite_market_config, which existing runtime code
# (dhan_ticker.py, live_order_manager.py, kite_broker.py, flattrade_broker.py,
# dhan_totp_login.py's cron) reads directly and must keep working unchanged.
# For Dhan specifically — the one non-OAuth broker actually live — saving
# here also mirrors into kite_market_config so the existing TOTP-refresh
# cron and feed picks it up; every other broker just lands here until its
# own integration is built.
# ---------------------------------------------------------------------------


def _mirror_dhan_credentials(db, fields: dict, app_user_id: str) -> None:
    set_fields: dict = {}
    if fields.get("client_id"):
        set_fields["user_id"] = fields["client_id"]
    for key in ("api_key", "api_secret", "totp_token", "pin"):
        if fields.get(key):
            set_fields["totp" if key == "totp_token" else key] = fields[key]
    if not set_fields:
        return
    set_fields["broker"] = "dhan"
    set_fields["app_user_id"] = app_user_id
    db._db["kite_market_config"].update_one(
        {"broker": "dhan"}, {"$set": set_fields}, upsert=True,
    )


@router.get("/broker-orders")
async def list_broker_orders(
    trade_id:     str = "",
    broker_doc_id:str = "",
    status:       str = "",
    order_side:   str = "",
    limit:        int = 200,
):
    """
    Fetch broker orders from the broker_orders collection.

    Query params (all optional):
      trade_id      – filter by algo_trade _id
      broker_doc_id – filter by broker configuration _id
      status        – OPEN | COMPLETE | REJECTED | CANCELLED | FAILED
      order_side    – entry | exit
      limit         – max records (default 200)
    """
    query: dict = {}
    if trade_id.strip():
        query["trade_id"] = trade_id.strip()
    if broker_doc_id.strip():
        query["broker_doc_id"] = broker_doc_id.strip()
    if status.strip():
        query["status"] = status.strip().upper()
    if order_side.strip():
        query["order_side"] = order_side.strip().lower()

    db = MongoData()
    try:
        cursor = (
            db._db["broker_orders"]
            .find(query)
            .sort("placed_at", -1)
            .limit(max(1, min(int(limit), 1000)))
        )
        records = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            records.append(doc)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load broker orders: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    return {"success": True, "count": len(records), "records": records}


@router.get("/algo-backtest-simulator")
async def algo_backtest_simulator(
    listen_timestamp: str = Query(..., description="Backtest listen timestamp in YYYY-MM-DDTHH:MM:SS"),
    autoload: bool = Query(True, description="Frontend autoload status for reference"),
    activation_mode: str = Query("algo-backtest", description="Activation mode: algo-backtest, fast-forward, or live"),
):
    # NOTE: still unauthenticated — assets/js/algo-backtest-dashboard.js (the
    # legacy dashboards) calls this with no Authorization header. Don't gate
    # behind app_auth here until that caller is migrated too.
    normalized_timestamp = str(listen_timestamp or "").strip()
    if not normalized_timestamp:
        raise HTTPException(status_code=400, detail="listen_timestamp is required")
    if len(normalized_timestamp) < 19:
        raise HTTPException(status_code=400, detail="listen_timestamp must be in YYYY-MM-DDTHH:MM:SS format")
    normalized_mode = str(activation_mode or "algo-backtest").strip() or "algo-backtest"
    if normalized_mode not in {"algo-backtest", "fast-forward", "forward-test", "live"}:
        raise HTTPException(status_code=400, detail=f"activation_mode must be algo-backtest, fast-forward, forward-test, or live")

    db = MongoData()
    try:
        if normalized_mode == "live":
            from features.execution_socket import (
                _append_momentum_pending_to_contracts,
                _build_active_contracts_from_records,
                _extract_running_positions,
                _load_running_trade_records,
            )
            from features.kite_event import broker_live_tick
            from features.live_tick_dispatcher import _run_entries_for_mode

            # trade_date is the NSE trading calendar day (IST midnight boundary) —
            # keep deriving it from IST wall clock. entry_time/exit_time are now
            # stored as true UTC (frontend converts IST market-hour clock time
            # before sending — see PortfolioActivation.tsx's istWallClockToUtcIso),
            # so live_now itself (used for the entry/exit gate inside
            # _run_entries_for_mode/broker_live_tick, and echoed back to the
            # frontend's polling loop as listen_timestamp) must be true UTC.
            trade_date = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")
            live_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            listen_hhmm = live_now[11:16]
            _start_monitor_services(trade_date=trade_date)

            _run_entries_for_mode(
                db,
                trade_date,
                "live",
                listen_hhmm,
                live_now,
            )
            live_records = _load_running_trade_records(db, trade_date, activation_mode="live")
            broker_result = broker_live_tick(
                db,
                trade_date,
                live_now,
                dict(ticker_manager.ltp_map),
                activation_mode="live",
                running_trades=live_records,
            )
            live_records = _load_running_trade_records(db, trade_date, activation_mode="live")
            active_contracts = _build_active_contracts_from_records(
                live_records,
                db=db,
                trade_date=trade_date,
                market_cache=None,
                activation_mode="live",
            )
            _append_momentum_pending_to_contracts(active_contracts, db, live_records)
            live_ltp = _build_live_ltp_payload(active_contracts, live_now)
            position_snapshot = _extract_running_positions(
                db,
                trade_date,
                listen_hhmm,
                include_position_snapshots=True,
                running_trades=None,
                market_cache=None,
                activation_mode="live",
            )
            result = {
                "listen_timestamp": live_now,
                "listen_time": live_now[11:19],
                "trade_date": trade_date,
                "records": live_records,
                "entry_snapshots": [],
                "entries_executed": [],
                "actions_taken": list(broker_result.get("actions_taken") or []),
                "ltp": live_ltp,
                "open_positions": list(position_snapshot.get("open_positions") or []),
                "active_leg_tokens": list(position_snapshot.get("active_leg_tokens") or []),
                "count": len(live_records),
                "open_positions_count": len(position_snapshot.get("open_positions") or []),
            }
        else:
            result = run_backtest_simulation_step(
                db,
                normalized_timestamp,
                activation_mode=normalized_mode,
            )
        delivered = await broadcast_backtest_simulation_step(db, result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"algo-backtest-simulator failed: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    # Return a slim response — LTP and order details are pushed via sockets.
    # The frontend receives full data through:
    #   update channel  → ltp_update  (LTP for active + momentum-pending legs)
    #   update channel  → update      (open positions snapshot)
    #   execute-orders  → execute_order (records + entries_executed order fills)
    #   executions      → countdown_update (listening state + entry snapshots)
    return {
        "success": True,
        "autoload": bool(autoload),
        "listen_timestamp": result.get("listen_timestamp") or normalized_timestamp,
        "listen_time": result.get("listen_time") or "",
        "trade_date": result.get("trade_date") or "",
        "actions_taken": result.get("actions_taken") or [],
        "entries_count": len(result.get("entries_executed") or []),
        "open_positions_count": result.get("open_positions_count") or 0,
        "active_tokens_count": len(result.get("active_leg_tokens") or []),
        "socket_broadcast": delivered,
    }


def _str_id(doc: dict | None) -> dict | None:
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc





















# ── Simulator risk monitor (SL/Target/hedge auto-exit on simulator_triggers
# / simulator_portfolio_triggers) — separate engine from the monitor above,
# see features/simulator_risk_monitor.py. Same start/stop/status page pattern
# as /simulator/monitor/* but its own toggle, since starting that one must
# never implicitly arm this one (real broker exit orders) or vice versa.











































_manual_order_kite_cache: dict[tuple, dict] = {}
_manual_order_kite_cache_date: str = ""


def _fetch_manual_order_kite_cache(raw_db, kite_doc: dict | None) -> dict[tuple, dict]:
    """
    Same shape/keying as spot_atm_utils._load_kite_instruments(), fetched directly with a
    specific Kite account's own credentials instead of going through that shared helper —
    which silently skips fetching (returns its empty cache) whenever Dhan is the active
    market-data feed broker, a global/unrelated setting that has nothing to do with whether
    a real Kite account is configured for placing this order.
    """
    global _manual_order_kite_cache, _manual_order_kite_cache_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _manual_order_kite_cache_date == today and _manual_order_kite_cache:
        return _manual_order_kite_cache

    doc = kite_doc
    if doc is None:
        for candidate in raw_db["broker_configuration"].find({"broker_type": "live"}):
            name = str(candidate.get("broker_name") or candidate.get("name") or "").lower()
            if ("kite" in name or "zerodha" in name) and candidate.get("api_key") and candidate.get("access_token"):
                doc = candidate
                break
    if not doc:
        return {}

    try:
        from kiteconnect import KiteConnect  # type: ignore

        kite = KiteConnect(api_key=str(doc.get("api_key") or "").strip())
        kite.set_access_token(str(doc.get("access_token") or "").strip())
        new_cache: dict[tuple, dict] = {}
        for segment in ("NFO", "BFO"):
            for inst in kite.instruments(segment):
                name = str(inst.get("name") or "").strip().upper()
                inst_type = str(inst.get("instrument_type") or "").strip().upper()
                exp = inst.get("expiry")
                stk = inst.get("strike")
                sym = str(inst.get("tradingsymbol") or "").strip()
                if not (name and inst_type in ("CE", "PE") and exp and stk is not None and sym):
                    continue
                try:
                    exp_str = exp.strftime("%Y-%m-%d")
                except AttributeError:
                    exp_str = str(exp)[:10]
                new_cache[(name, exp_str, float(stk), inst_type)] = {
                    "symbol": sym,
                    "exchange": str(inst.get("exchange") or segment),
                }
        _manual_order_kite_cache = new_cache
        _manual_order_kite_cache_date = today
        return new_cache
    except Exception as exc:
        log.debug("manual order kite instrument fetch error: %s", exc)
        return {}


def _resolve_manual_order_symbol(leg: "ManualOrderLeg", raw_db, kite_doc: dict | None = None) -> tuple[str, str] | None:
    """
    Kite-native (underlying, expiry, strike, option_type) → (tradingsymbol, exchange).
    Same instrument metadata _to_flattrade_symbol() already uses for the FlatTrade
    conversion — account-agnostic, so it's safe to resolve this way regardless of
    which broker_id is actually placing the order.
    """
    from features.spot_atm_utils import _load_kite_instruments

    cache = _load_kite_instruments()
    if not cache:
        cache = _fetch_manual_order_kite_cache(raw_db, kite_doc)

    key = (
        leg.underlying.strip().upper(),
        leg.expiry.strip()[:10],
        float(leg.strike),
        leg.option_type.strip().upper(),
    )
    inst = cache.get(key)
    if not inst:
        return None
    return str(inst["symbol"]), str(inst["exchange"])


def _resolve_dhan_security(leg: "ManualOrderLeg", raw_db) -> dict | None:
    """
    (underlying, expiry, strike, option_type) → Dhan's own securityId/symbol/exchangeSegment,
    from the same active_option_tokens collection execution_socket.py already keys positions off
    of. Dhan identifies instruments by numeric securityId, not a tradingsymbol string, so this
    doesn't reuse _resolve_manual_order_symbol (that one resolves the Kite-style symbol).
    """
    doc = raw_db["active_option_tokens"].find_one({
        "broker": "dhan",
        "instrument": leg.underlying.strip().upper(),
        "expiry": leg.expiry.strip()[:10],
        "strike": float(leg.strike),
        "option_type": leg.option_type.strip().upper(),
    })
    if not doc:
        return None
    security_id = str(doc.get("token") or "").strip()
    if not security_id:
        return None
    return {
        "security_id": security_id,
        "symbol": str(doc.get("symbol") or "").strip(),
        "exchange_segment": str(doc.get("ws_segment") or "").strip().upper() or "NSE_FNO",
    }


async def _fetch_dhan_quote_for_leg(leg: "ManualOrderLeg", raw_db) -> dict | None:
    """
    Resolves this leg's Dhan security_id and returns its live quote {"symbol","ltp","bid","ask"}.
    Returns None if Dhan has no contract match for this leg at all.

    Shared by _resolve_mpp_price and _resolve_ltp_price — every order's price, regardless of
    which broker (FlatTrade/Kite/Dhan) actually executes it, is read from this one feed. Dhan
    already streams/queries the full F&O chain, whereas Kite's own feed isn't even running
    unless Kite is the active market-data broker (kite_market_config) — and the broker that
    places the order has nothing to do with which one is the best price source.
    """
    resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
    if not resolved:
        return None
    quote = (await asyncio.to_thread(
        _fetch_dhan_market_data, resolved["exchange_segment"], [int(resolved["security_id"])], _shared_mongo,
    )).get(resolved["security_id"], {})
    return {
        "symbol": resolved["symbol"],
        "ltp": float(quote.get("ltp") or 0),
        "bid": float(quote.get("bid") or 0),
        "ask": float(quote.get("ask") or 0),
        # Full depth (best→worst) when available — see _fetch_dhan_market_data. WS-sourced
        # quotes only ever have 1 level (Dhan's other 4 WS depth levels aren't parsed); REST
        # quotes carry whatever Dhan's /marketfeed/quote returns (typically 5).
        "bid_depth": quote.get("bid_depth") or ([{"price": float(quote.get("bid") or 0), "quantity": 0}] if quote.get("bid") else []),
        "ask_depth": quote.get("ask_depth") or ([{"price": float(quote.get("ask") or 0), "quantity": 0}] if quote.get("ask") else []),
    }


def _notify_mpp_ltp_price_unresolved(kind: str, message: str) -> None:
    """
    Shared by _resolve_mpp_price/_resolve_ltp_price — every failure to resolve a real,
    fresh price pages admin via Telegram instead of failing silently, since the only other
    signal is a 0.0 return the caller must already be checking for.
    """
    # print(f"[{kind} PRICE] {message}", flush=True)
    try:
        from features.telegram_notifier import notify_admin
        notify_admin(f"{kind.lower()}_price_unresolved", message)
    except Exception as exc:
        log.warning("[%s PRICE] notify_admin failed: %s", kind, exc)


async def _resolve_mpp_price(leg: "ManualOrderLeg", raw_db) -> float:
    """
    MPP's walked-depth + protection% formula, priced off Dhan's feed regardless of the
    execution broker (see _fetch_dhan_quote_for_leg). The order itself still goes out
    through whichever broker/symbol the caller resolved separately.

    Prices off the side this leg's qty actually has to sweep to fill — a BUY matches
    against resting ASK orders, a SELL matches against resting BID orders, not the leg's
    own resting side — and walks that side's depth levels (best→worst) until leg.quantity
    is covered, instead of assuming the top level alone holds the whole order. Same
    reasoning as live_order_manager.py's place_live_entry_order/place_live_exit_order and
    algo.simulator/api.py's copy of this same function.

    Returns 0.0 — NEVER leg.price or ltp as a stand-in for a missing bid/ask — when Dhan has
    no contract match or no live depth on the side this leg needs. Every caller already
    treats a <= 0 return as "unresolved" and aborts the order instead of placing it;
    substituting ltp here would silently hand back a fabricated "protected" price with no
    real depth behind it — exactly the risk that made this whole function worth having.
    """
    from features.live_order_manager import _mpp_protection_pct, _clamp_limit_price, _walk_depth_for_qty

    quote = await _fetch_dhan_quote_for_leg(leg, raw_db)
    if not quote:
        _notify_mpp_ltp_price_unresolved(
            "MPP", f"No Dhan contract match for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0

    ltp = quote["ltp"]
    is_buy = leg.side == "BUY"
    consume_side = quote["ask_depth"] if is_buy else quote["bid_depth"]
    walked_price, fully_covered = _walk_depth_for_qty(consume_side, leg.quantity)
    # Never substitute ltp for missing depth — that isn't a live book price and would place
    # a real order at a fabricated "protected" price with no depth behind it.
    if walked_price <= 0:
        _notify_mpp_ltp_price_unresolved(
            "MPP",
            f"No live depth for {quote.get('symbol')} (side={'ask' if is_buy else 'bid'}, qty={leg.quantity}) — order NOT placed.",
        )
        return 0.0
    if not fully_covered:
        pass
        # print(
            # f"[MPP PRICE][dhan-feed] symbol={quote['symbol']} qty={leg.quantity} not fully covered by visible "
            # f"{'ask' if is_buy else 'bid'}-side depth — pricing off deepest visible level {walked_price}, "
            # f"remainder may not fill immediately.",
            # flush=True,
        # )

    # NSE's MPP protection band is sized differently for options vs futures (tighter for
    # futures — see _mpp_protection_pct's docstring) — a futures leg must not get priced
    # with the wider option band.
    pct = _mpp_protection_pct(ltp, is_option=leg.option_type.strip().upper() != "FUT")
    raw_price = walked_price * (1 + pct / 100) if is_buy else walked_price * (1 - pct / 100)
    price = _clamp_limit_price(raw_price, is_buy)
    # print(
        # f"[MPP PRICE][dhan-feed] symbol={quote['symbol']} ltp={ltp} qty={leg.quantity} "
        # f"walked_price={walked_price} fully_covered={fully_covered} pct={pct}% price={price} is_buy={is_buy}",
        # flush=True,
    # )
    return price


async def _resolve_ltp_price(leg: "ManualOrderLeg", raw_db) -> float:
    """
    "Execute At LTP" price source — same Dhan-feed-regardless-of-execution-broker principle as
    _resolve_mpp_price, just without the protection-band markup: submits a plain LIMIT order at
    Dhan's current ltp instead of trusting the order pad row's possibly-seconds-stale client-side
    ltp.

    Returns 0.0 — never leg.price — if Dhan has no match/quote yet; see _resolve_mpp_price's
    docstring for why no fallback price is used here.
    """
    quote = await _fetch_dhan_quote_for_leg(leg, raw_db)
    if not quote or quote["ltp"] <= 0:
        _notify_mpp_ltp_price_unresolved(
            "LTP", f"No Dhan quote for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0
    # print(f"[LTP PRICE][dhan-feed] symbol={quote['symbol']} ltp={quote['ltp']}", flush=True)
    return quote["ltp"]


async def _simulator_place_manual_order_core(body: ManualOrderRequest) -> dict:
    """
    Places real orders with the broker — this is live money, not a simulation.
    FlatTrade/Kite use their own place_order() already proven elsewhere in this
    codebase. Dhan goes straight to https://api.dhan.co/v2/orders (same direct-
    REST pattern already used for Dhan positions/quotes) — UNVERIFIED against a
    live order, unlike the other two: dhanhq SDK isn't installed, and this is
    adapted from an untested reference in the sibling option-algo repo. Test
    with one small/throwaway order before relying on it for size.
    """
    broker_id = str(body.broker_id or "").strip()
    # print(f"[PLACE_ORDER] request broker_id={broker_id} legs={len(body.orders)} orders={[o.model_dump() for o in body.orders]}", flush=True)
    try:
        raw_db = _shared_mongo._db

        # Looked up by _id (the specific account the user picked in the Order Pad) in
        # broker_configuration — same collection every other broker (FlatTrade/Kite) uses
        # for order placement. kite_market_config is feed-only; a Dhan account used for order
        # placement must be its own broker_configuration doc so routing is scoped to that one
        # account, not a global "whichever is enabled" doc every user's orders would share.
        dhan_cfg = {}
        if broker_id:
            try:
                from bson import ObjectId
                from features.dhan_broker import _is_dhan_doc
                _candidate = raw_db["broker_configuration"].find_one({"_id": ObjectId(broker_id)})
                if _candidate and _is_dhan_doc(_candidate):
                    dhan_cfg = _candidate
            except Exception:
                dhan_cfg = {}
        if dhan_cfg:
            dhan_client_id = str(dhan_cfg.get("broker_user_id") or dhan_cfg.get("user_id") or "").strip()
            dhan_access_token = str(dhan_cfg.get("access_token") or "").strip()
            if not dhan_access_token or not dhan_client_id:
                # print("[PLACE_ORDER][dhan] credentials not configured", flush=True)
                return {"status": "error", "message": "Dhan credentials not configured.", "results": []}

            try:
                from features.dhan_order_update import dhan_order_update_pool
                dhan_order_update_pool.ensure_started(dhan_client_id, dhan_access_token)
            except Exception:
                log.warning("[PLACE_ORDER][dhan] order-update WS warm-start failed", exc_info=True)

            from features.dhan_broker import get_dhan_instance
            from features.order_execution import place_broker_order

            dhan_order_type_map = {"LIMIT": "LIMIT", "MARKET": "MARKET", "SL": "SL"}
            dhan_adapter = get_dhan_instance(_shared_mongo, dhan_client_id, dhan_access_token)

            async def _place_one_dhan_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
                if not resolved:
                    # print(f"[PLACE_ORDER][dhan] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}

                price = leg.price
                requested_type = leg.order_type
                if requested_type == "MPP":
                    price = await _resolve_mpp_price(leg, raw_db)
                    if price <= 0:
                        # print(f"[PLACE_ORDER][dhan] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"
                elif requested_type == "LTP":
                    price = await _resolve_ltp_price(leg, raw_db)
                    if price <= 0:
                        # print(f"[PLACE_ORDER][dhan] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"

                dhan_order_type = dhan_order_type_map.get(requested_type, "LIMIT")
                result = await asyncio.to_thread(
                    place_broker_order,
                    dhan_adapter,
                    tradingsymbol=resolved["symbol"],
                    exchange="NFO",
                    transaction_type="BUY" if leg.side == "BUY" else "SELL",
                    quantity=leg.quantity,
                    order_type=dhan_order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price or 0.0,
                    context={"purpose": "manual_order_pad", "broker": "dhan", "symbol": resolved["symbol"]},
                    broker_kwargs={"security_id": resolved["security_id"], "exchange_segment": resolved["exchange_segment"]},
                )
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {
                    "leg": leg.model_dump(), "status": "success", "order_id": result["order_id"],
                    "broker_status": result.get("broker_status", "UNKNOWN"),
                    "average_price": result.get("average_price"),
                    "filled_quantity": result.get("filled_quantity"),
                }

            # BUY legs place first (as one batch), then SELL legs (as a second batch) —
            # see place_legs_hedge_ordered's docstring: gives the broker a real BUY
            # position ahead of the SELL leg instead of both sides landing at once.
            from features.order_execution import place_legs_hedge_ordered
            dhan_results: list[dict] = await place_legs_hedge_ordered(body.orders, _place_one_dhan_leg)

            any_ok = any(r["status"] == "success" for r in dhan_results)
            all_ok = bool(dhan_results) and all(r["status"] == "success" for r in dhan_results)
            overall_status = "success" if all_ok else ("partial" if any_ok else "error")
            # print(f"[PLACE_ORDER] done status={overall_status} results={dhan_results}", flush=True)
            return {"status": overall_status, "results": dhan_results}

        try:
            doc = raw_db["broker_configuration"].find_one({"_id": ObjectId(broker_id)})
        except Exception:
            doc = None
        if not doc:
            # print(f"[PLACE_ORDER] broker account not found for broker_id={broker_id}", flush=True)
            return {"status": "error", "message": "Broker account not found.", "results": []}

        broker_name = str(doc.get("broker_name") or doc.get("name") or "").strip().lower()
        is_flattrade = "flattrade" in broker_name
        is_kite = "zerodha" in broker_name or "kite" in broker_name
        # print(f"[PLACE_ORDER] resolved broker_name={broker_name} is_flattrade={is_flattrade} is_kite={is_kite}", flush=True)
        if not is_flattrade and not is_kite:
            # print(f"[PLACE_ORDER] rejected — order placement not supported for broker_name={broker_name}", flush=True)
            return {"status": "error", "message": "Order placement isn't available for this broker yet.", "results": []}

        results: list[dict] = []

        if is_flattrade:
            from features.flattrade_broker import get_flattrade_instance

            adapter = get_flattrade_instance(str(doc.get("user_id") or ""), str(doc.get("access_token") or ""))
            if adapter is None:
                # print("[PLACE_ORDER][flattrade] session not available", flush=True)
                return {"status": "error", "message": "FlatTrade session not available.", "results": []}

            async def _place_one_flattrade_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db)
                if not resolved:
                    # print(f"[PLACE_ORDER][flattrade] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    # FlatTrade has no native MPP order type — "MPP" would silently fall back to
                    # a plain LIMIT at price=0 (rejected by the exchange) if sent through as-is.
                    # Price source is always Dhan's feed (see _resolve_mpp_price), independent of
                    # FlatTrade being the execution broker here.
                    price = await _resolve_mpp_price(leg, raw_db)
                    if price <= 0:
                        # print(f"[PLACE_ORDER][flattrade] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    # Same Dhan-feed-regardless-of-execution-broker principle — submit at Dhan's
                    # current ltp instead of trusting a possibly-stale client-side price.
                    price = await _resolve_ltp_price(leg, raw_db)
                    if price <= 0:
                        # print(f"[PLACE_ORDER][flattrade] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                # print(
                    # f"[PLACE_ORDER][flattrade] placing tradingsymbol={symbol} exchange={exchange} "
                    # f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    # f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    # flush=True,
                # )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    adapter,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price,
                    context={"purpose": "manual_order_pad", "broker": "flattrade", "symbol": symbol},
                )
                # print(f"[PLACE_ORDER][flattrade] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {
                    "leg": leg.model_dump(), "status": "success", "order_id": result["order_id"],
                    "broker_status": result.get("broker_status", "UNKNOWN"),
                    "average_price": result.get("average_price"),
                    "filled_quantity": result.get("filled_quantity"),
                }

            # BUY-then-SELL batching — same reasoning as the Dhan branch above.
            from features.order_execution import place_legs_hedge_ordered
            results = await place_legs_hedge_ordered(body.orders, _place_one_flattrade_leg)
        else:
            from kiteconnect import KiteConnect  # type: ignore

            api_key = str(doc.get("api_key") or "").strip()
            access_token = str(doc.get("access_token") or "").strip()
            if not api_key or not access_token:
                # print("[PLACE_ORDER][kite] session not available", flush=True)
                return {"status": "error", "message": "Kite session not available.", "results": []}
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)

            async def _place_one_kite_leg(leg: "ManualOrderLeg") -> dict:
                # Resolve with this exact account's own token — instrument metadata fetched via
                # Dhan's feed wouldn't reflect this Kite account's session, and the shared cache
                # is empty whenever Dhan (not Kite) is the active market-data broker anyway.
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db, doc)
                if not resolved:
                    # print(f"[PLACE_ORDER][kite] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    # Kite has no native MPP order type either — price source is always Dhan's
                    # feed (see _resolve_mpp_price), independent of Kite being the execution
                    # broker here.
                    price = await _resolve_mpp_price(leg, raw_db)
                    if price <= 0:
                        # print(f"[PLACE_ORDER][kite] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    # Same Dhan-feed-regardless-of-execution-broker principle — submit at Dhan's
                    # current ltp instead of trusting a possibly-stale client-side price.
                    price = await _resolve_ltp_price(leg, raw_db)
                    if price <= 0:
                        # print(f"[PLACE_ORDER][kite] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                # print(
                    # f"[PLACE_ORDER][kite] placing tradingsymbol={symbol} exchange={exchange} "
                    # f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    # f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    # flush=True,
                # )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    kite,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price or 0.0,
                    trigger_price=leg.trigger_price or 0.0,
                    variety=kite.VARIETY_REGULAR,
                    context={"purpose": "manual_order_pad", "broker": "kite", "symbol": symbol},
                )
                # print(f"[PLACE_ORDER][kite] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {
                    "leg": leg.model_dump(), "status": "success", "order_id": result["order_id"],
                    "broker_status": result.get("broker_status", "UNKNOWN"),
                    "average_price": result.get("average_price"),
                    "filled_quantity": result.get("filled_quantity"),
                }

            # BUY-then-SELL batching — same reasoning as the Dhan branch above.
            from features.order_execution import place_legs_hedge_ordered
            results = await place_legs_hedge_ordered(body.orders, _place_one_kite_leg)

        any_ok = any(r["status"] == "success" for r in results)
        all_ok = bool(results) and all(r["status"] == "success" for r in results)
        overall_status = "success" if all_ok else ("partial" if any_ok else "error")
        # print(f"[PLACE_ORDER] done status={overall_status} results={results}", flush=True)
        return {
            "status": overall_status,
            "results": results,
        }
    except Exception as exc:
        # print(f"[PLACE_ORDER] unhandled error={exc}", flush=True)
        return {"status": "error", "message": str(exc), "results": []}


@trade_router.post("/positions/place-order")
async def simulator_place_manual_order(body: ManualOrderRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Thin wrapper around _simulator_place_manual_order_core so every order
    placement — manual order-pad AND the risk monitor's auto-fire/hedge/
    adjustment paths, all of which call this same function directly (see
    features/simulator_risk_monitor.py) — gets one consistent Telegram
    notification on the way out, success or failure, instead of each caller
    having to remember to send its own.
    """
    result = await _simulator_place_manual_order_core(body)
    try:
        from features.telegram_notifier import notify_user

        status = str(result.get("status") or "")
        leg_summary = ", ".join(
            f"{o.side} {o.underlying} {o.strike}{o.option_type} x{o.quantity}" for o in body.orders
        )
        if status == "success":
            notify_user("PT_ORDER_PLACED", f"Order placed — {leg_summary}", {"broker": body.broker_id})
        elif status in ("error", "partial"):
            notify_user(
                "PT_ORDER_FAILED" if status == "error" else "PT_ORDER_PARTIAL",
                f"Order {status} — {leg_summary} — {result.get('message', '')}",
                {"broker": body.broker_id},
            )
    except Exception as exc:
        pass
        # print(f"[PLACE_ORDER] telegram notify error={exc}", flush=True)
    return result
















































@router.get("/angel-stock-list/backfill-kite-token")
async def backfill_angel_stock_list_kite_token(limit: int = Query(default=0, ge=0)) -> dict:
    try:
        from features.live_event import resolve_kite_token_for_symbol

        collection = _shared_mongo._db["angel_stock_list"]
        cursor = collection.find(
            {},
            {
                "_id": 1,
                "symbol": 1,
                "tradingsymbol": 1,
                "kite_token": 1,
                "token": 1,
                "tokens": 1,
                "instrument_token": 1,
                "exchange_token": 1,
            },
        )
        if limit > 0:
            cursor = cursor.limit(limit)

        total_rows = 0
        updated_count = 0
        skipped_existing_count = 0
        missing_token_count = 0
        failed_symbols: list[str] = []

        for row in cursor:
            total_rows += 1
            symbol = str(row.get("symbol") or "").strip()
            tradingsymbol = str(row.get("tradingsymbol") or symbol).strip()
            existing_token = str(row.get("kite_token") or "").strip()

            if existing_token:
                skipped_existing_count += 1
                continue

            resolved_token = str(
                row.get("token")
                or row.get("tokens")
                or row.get("instrument_token")
                or row.get("exchange_token")
                or ""
            ).strip()

            if not resolved_token and tradingsymbol:
                try:
                    resolved_token = str(resolve_kite_token_for_symbol(tradingsymbol) or "").strip()
                except Exception:
                    resolved_token = ""

            if resolved_token:
                result = collection.update_one(
                    {"_id": row.get("_id")},
                    {"$set": {"kite_token": resolved_token}},
                )
                if result.modified_count > 0:
                    updated_count += 1
                else:
                    skipped_existing_count += 1
            else:
                missing_token_count += 1
                if symbol:
                    failed_symbols.append(symbol)

        return {
            "status": "success",
            "collection": "angel_stock_list",
            "total_rows": total_rows,
            "updated_count": updated_count,
            "skipped_existing_count": skipped_existing_count,
            "missing_token_count": missing_token_count,
            "failed_symbols_sample": failed_symbols[:25],
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/investment-portfolio/backfill-symbol-token")
async def backfill_investment_portfolio_symbol_token(symbol: str = Query(default="")) -> dict:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return {"status": "error", "message": "symbol query param is required"}

    try:
        db = _shared_mongo._db
        source_doc = db["angel_stock_list"].find_one(
            {"symbol": normalized_symbol},
            {
                "_id": 0,
                "symbol": 1,
                "kite_token": 1,
                "token": 1,
                "tokens": 1,
                "instrument_token": 1,
                "exchange_token": 1,
            },
        ) or {}

        resolved_token = (
            source_doc.get("kite_token")
            or source_doc.get("token")
            or source_doc.get("tokens")
            or source_doc.get("instrument_token")
            or source_doc.get("exchange_token")
        )

        if resolved_token in (None, ""):
            fallback_doc = db["stocks_list"].find_one(
                {"symbol": normalized_symbol},
                {
                    "_id": 0,
                    "token": 1,
                    "tokens": 1,
                    "instrument_token": 1,
                    "exchange_token": 1,
                    "code": 1,
                },
            ) or {}
            resolved_token = (
                fallback_doc.get("token")
                or fallback_doc.get("tokens")
                or fallback_doc.get("instrument_token")
                or fallback_doc.get("exchange_token")
                or fallback_doc.get("code")
            )

        if resolved_token in (None, ""):
            return {
                "status": "error",
                "message": f"Token not found for symbol {normalized_symbol}",
            }

        result = db["investment_portfolio"].update_many(
            {"symbol": normalized_symbol},
            {"$set": {"symbol_token": resolved_token}},
        )

        return {
            "status": "success",
            "collection": "investment_portfolio",
            "symbol": normalized_symbol,
            "symbol_token": resolved_token,
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/investment-portfolio/backfill-symbol-token-all")
async def backfill_investment_portfolio_symbol_token_all(limit: int = Query(default=0, ge=0)) -> dict:
    try:
        db = _shared_mongo._db
        source_cache: dict[str, Any] = {}

        def resolve_symbol_token(normalized_symbol: str) -> Any:
            if normalized_symbol in source_cache:
                return source_cache[normalized_symbol]

            source_doc = db["angel_stock_list"].find_one(
                {"symbol": normalized_symbol},
                {
                    "_id": 0,
                    "kite_token": 1,
                    "token": 1,
                    "tokens": 1,
                    "instrument_token": 1,
                    "exchange_token": 1,
                },
            ) or {}

            resolved = (
                source_doc.get("kite_token")
                or source_doc.get("token")
                or source_doc.get("tokens")
                or source_doc.get("instrument_token")
                or source_doc.get("exchange_token")
            )

            if resolved in (None, ""):
                fallback_doc = db["stocks_list"].find_one(
                    {"symbol": normalized_symbol},
                    {
                        "_id": 0,
                        "token": 1,
                        "tokens": 1,
                        "instrument_token": 1,
                        "exchange_token": 1,
                        "code": 1,
                    },
                ) or {}
                resolved = (
                    fallback_doc.get("token")
                    or fallback_doc.get("tokens")
                    or fallback_doc.get("instrument_token")
                    or fallback_doc.get("exchange_token")
                    or fallback_doc.get("code")
                )

            source_cache[normalized_symbol] = resolved
            return resolved

        cursor = db["investment_portfolio"].find(
            {},
            {"_id": 1, "symbol": 1, "symbol_token": 1},
        )
        if limit > 0:
            cursor = cursor.limit(limit)

        total_rows = 0
        updated_count = 0
        skipped_existing_count = 0
        missing_token_count = 0
        failed_symbols: list[str] = []

        for row in cursor:
            total_rows += 1
            normalized_symbol = str(row.get("symbol") or "").strip().upper()
            existing_symbol_token = row.get("symbol_token")

            if not normalized_symbol:
                missing_token_count += 1
                continue

            if existing_symbol_token not in (None, ""):
                skipped_existing_count += 1
                continue

            resolved_token = resolve_symbol_token(normalized_symbol)
            if resolved_token in (None, ""):
                missing_token_count += 1
                failed_symbols.append(normalized_symbol)
                continue

            result = db["investment_portfolio"].update_one(
                {"_id": row.get("_id")},
                {"$set": {"symbol_token": resolved_token}},
            )
            if result.modified_count > 0:
                updated_count += 1
            else:
                skipped_existing_count += 1

        return {
            "status": "success",
            "collection": "investment_portfolio",
            "total_rows": total_rows,
            "updated_count": updated_count,
            "skipped_existing_count": skipped_existing_count,
            "missing_token_count": missing_token_count,
            "failed_symbols_sample": failed_symbols[:25],
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}







@router.get("/spot/historical-data")
async def spot_historical_data_range(
    underlying: str = Query(default=""),
    start_dt: str = Query(default=""),
    end_dt: str = Query(default=""),
    current_user: dict = Depends(app_auth.require_current_user),
):
    """
    Explicit-range twin of algo.websocket's /algo/spot/historical-data (port
    8003, candle-based) — see mtm_historical_data_range above for why this
    lives on algo.trade (port 8000) instead.

    Query params:
        underlying – e.g. NIFTY, BANKNIFTY
        start_dt   – ISO timestamp  e.g. 2026-07-24T09:15
        end_dt     – ISO timestamp  e.g. 2026-07-24T15:30
    """
    from features.spot_historical_data import get_spot_historical_data_range

    if not underlying.strip():
        raise HTTPException(status_code=400, detail="underlying param is required")
    if not start_dt.strip() or not end_dt.strip():
        raise HTTPException(status_code=400, detail="start_dt and end_dt params are required")

    db = MongoData()
    try:
        data = get_spot_historical_data_range(db, underlying, start_dt, end_dt)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"spot historical data error: {exc}") from exc
    finally:
        db.close()

    return data


app.include_router(router)
app.include_router(trade_router)
app.include_router(socket_router)
app.include_router(mock_kite_socket_router)
app.include_router(live_quote_socket_router)
app.include_router(payment_router, prefix="/algo")

# Same two mounts old algo_main.py added on top of api.py.
from chart_api import router as chart_router  # noqa: E402
from fast_backtest_api import router as fast_backtest_router  # noqa: E402

app.include_router(chart_router)
app.include_router(fast_backtest_router)


# ─── Kite Broker Endpoints ────────────────────────────────────────────────────

# Temporary in-memory store: session_id → broker_doc_id
# Cleared after use (one-time use per login)
_kite_pending: dict = {}


# ── kite_market_config CRUD (market-data feed credentials) ───────────────────
# Market-data counterpart to the /broker-configuration/* trio above (which
# covers broker_configuration, i.e. trade-execution credentials). Before this,
# kite_market_config only had the narrow /broker/dhan/config save path
# (client_id + access_token) — totp/pin (needed by dhan_totp_login.py's daily
# auto-refresh cron) and the Kite side of this collection had no save path at
# all and had to be inserted into Mongo by hand.


# ── Dhan OAuth endpoints ──────────────────────────────────────────────────────
_dhan_pending: dict[str, str] = {}


def _dhan_popup_result_html(success: bool, message: str) -> str:
    import json as _json
    payload_js = _json.dumps({"type": "DHAN_LOGIN", "success": success, "message": message})
    icon  = "✓" if success else "✗"
    color = "#22c55e" if success else "#ef4444"
    title = "Login Successful" if success else "Login Failed"
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Dhan Login</title>
<style>
  body{{font-family:-apple-system,sans-serif;display:flex;align-items:center;
       justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#f1f5f9;}}
  .card{{text-align:center;padding:2rem;background:#1e293b;border-radius:12px;border:1px solid #334155;}}
  .icon{{font-size:3rem;color:{color};}}
  p{{color:#94a3b8;}}
</style></head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h2>{title}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) window.opener.postMessage(payload, "*");
    setTimeout(() => window.close(), 1500);
  </script>
</body></html>"""


_FNO_MASTER_CACHE: dict = {}        # {"rows": {symbol: [contracts]}, "fetched_at": float}
_FNO_CACHE_TTL = 3600               # refresh once per hour

_DHAN_SCRIP_MASTER_CACHE: dict = {}  # {"rows": [csv_row_dict, ...], "date": "YYYY-MM-DD"}


def _get_dhan_scrip_master_rows() -> list[dict]:
    """
    Raw Dhan scrip master CSV rows (~30MB file), downloaded once per calendar day
    and shared by every Dhan contract sync — stocks, indices, anything else —
    so the file is fetched at most once a day no matter how many instruments sync.
    """
    import io as _io, csv as _csv, requests as _req
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _DHAN_SCRIP_MASTER_CACHE.get("rows") and _DHAN_SCRIP_MASTER_CACHE.get("date") == today_str:
        return _DHAN_SCRIP_MASTER_CACHE["rows"]

    resp = _req.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=30)
    resp.raise_for_status()
    rows = list(_csv.DictReader(_io.StringIO(resp.text)))
    _DHAN_SCRIP_MASTER_CACHE["rows"] = rows
    _DHAN_SCRIP_MASTER_CACHE["date"] = today_str
    return rows


def _get_dhan_fno_master() -> dict[str, list[dict]]:
    """
    Returns {symbol: [{sec_id, strike, opt_type, expiry, exchange}]} from
    Dhan security master CSV.  Cached for 1 hour.
    Also populates _FNO_MASTER_CACHE["equity_ids"] = {symbol: sec_id} for spot lookup.
    """
    import time as _t
    if _FNO_MASTER_CACHE.get("rows") and (_t.time() - _FNO_MASTER_CACHE.get("fetched_at", 0)) < _FNO_CACHE_TTL:
        return _FNO_MASTER_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    equity_ids: dict[str, str] = {}
    for row in reader:
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        exch = row.get("SEM_EXM_EXCH_ID", "").strip()
        sec_id = row.get("SEM_SMST_SECURITY_ID", "").strip()
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()

        # Capture NSE equity security IDs for spot price lookup
        # Dhan CSV may use EQUITY, ES, EQ or similar for cash equity
        _deriv_types = {"OPTSTK", "OPTIDX", "FUTSTK", "FUTIDX", "FUTCUR", "OPTCUR", "FUTCOM", "OPTFUT"}
        if exch == "NSE" and inst not in _deriv_types and ts and sec_id:
            sym = ts.split("-")[0].strip()
            if sym and sym not in equity_ids:
                equity_ids[sym] = sec_id

        if inst != "OPTSTK":
            continue
        symbol = ts.split("-")[0].strip() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        entry = {
            "sec_id":   sec_id,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": exch,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        }
        master.setdefault(symbol, []).append(entry)

    _FNO_MASTER_CACHE["rows"] = master
    _FNO_MASTER_CACHE["equity_ids"] = equity_ids
    _FNO_MASTER_CACHE["fetched_at"] = _t.time()
    return master


_LAST_GOOD_EQUITY_COLLISION_QUOTE: dict[str, float] = {}  # frontend (kite) token -> last real ltp


def _get_dhan_equity_sec_id(symbol: str) -> str:
    """Return the NSE equity security ID for a stock symbol from Dhan CSV cache."""
    _get_dhan_fno_master()  # ensure cache is populated
    return str(_FNO_MASTER_CACHE.get("equity_ids", {}).get(symbol.strip().upper()) or "")


def _resolve_dhan_equity_ids_by_kite_tokens(kite_tokens: list[str], db) -> dict[str, str]:
    """
    kite_token -> dhan_security_id for scanner equity holdings, via scanner_stocks_list
    (same dhan_security_id field scanner/service.py's historical-data sync already
    resolves per-row via _resolve_stock_dhan_security_id — this just batches that lookup
    by token for the live quote endpoint). Lets a caller tell a scanner stock's Kite-space
    token apart from a simulator FNO/option token before deciding which Dhan segment to
    query — scanner holdings were always falling into the FNO-only lookup otherwise.
    """
    if not kite_tokens:
        return {}
    docs = db["scanner_stocks_list"].find(
        {"kite_token": {"$in": kite_tokens}},
        {"_id": 0, "kite_token": 1, "dhan_security_id": 1},
    )
    return {
        str(doc["kite_token"]): str(doc["dhan_security_id"])
        for doc in docs
        if doc.get("kite_token") and doc.get("dhan_security_id")
    }


_DHAN_INDEX_OPTION_CACHE: dict = {}  # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}


def _get_dhan_index_option_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for index (OPTIDX) contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV. The CSV is ~30MB, so it's downloaded once per calendar day and
    reused for every call that day, same caching shape as _get_dhan_fno_master() above.
    """
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _DHAN_INDEX_OPTION_CACHE.get("rows") and _DHAN_INDEX_OPTION_CACHE.get("date") == today_str:
        return _DHAN_INDEX_OPTION_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "OPTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_INDEX_OPTION_CACHE["rows"] = master
    _DHAN_INDEX_OPTION_CACHE["date"] = today_str
    return master


_DHAN_INDEX_FUTURE_CACHE: dict = {}  # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}

# token -> last real nonzero LTP ever seen for it via /simulator/paper-trade/futures-chain.
# Never evicted, same "a slightly stale real quote beats showing 0" reasoning as
# execution_socket.py's _LAST_GOOD_UNDERLYING_QUOTE — futures/ATM-option tokens are
# priced via dhan_quote_post_blocking (see simulator_pt_futures_chain), not the
# shared get_broker_rest_quotes/_LAST_GOOD_QUOTE path, so this is this endpoint's own.
_LAST_GOOD_FUTURES_TOKEN_QUOTE: dict[str, float] = {}


def _get_dhan_index_future_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, expiry, exchange, lot_size}]} for index
    (FUTIDX) futures contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV, same caching shape as _get_dhan_index_option_master() above.

    These were never synced into active_option_tokens: _get_dhan_fno_master() and
    _get_dhan_index_option_master() both explicitly skip every FUT* instrument type
    (they only ever kept OPTSTK/OPTIDX), so there's no Mongo collection to query —
    this reads the CSV directly instead, same as the option masters do.
    """
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _DHAN_INDEX_FUTURE_CACHE.get("rows") and _DHAN_INDEX_FUTURE_CACHE.get("date") == today_str:
        return _DHAN_INDEX_FUTURE_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "FUTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    for contracts in master.values():
        contracts.sort(key=lambda c: c["expiry"])

    _DHAN_INDEX_FUTURE_CACHE["rows"] = master
    _DHAN_INDEX_FUTURE_CACHE["date"] = today_str
    return master


_ACTIVE_OPTION_TOKENS_INDEX_ENSURED = False


def _ensure_active_option_tokens_index(col) -> None:
    """
    Create the compound index every Dhan contract upsert matches on, once per process.
    Without it, each upsert inside a bulk_write does a full collection scan to check for
    an existing match — that alone turned a multi-thousand-contract sync from under a
    second into ~10s per instrument (measured: NIFTY's 4080 contracts 9.8s -> 0.28s).
    """
    global _ACTIVE_OPTION_TOKENS_INDEX_ENSURED
    if _ACTIVE_OPTION_TOKENS_INDEX_ENSURED:
        return
    try:
        col.create_index(
            [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
            name="idx_active_option_contract_v2",
        )
    except Exception:
        pass
    _ACTIVE_OPTION_TOKENS_INDEX_ENSURED = True


def _sync_dhan_index_option_tokens(instrument: str) -> dict:
    """
    Refresh active_option_tokens for one index instrument from Dhan's scrip master
    (see _get_dhan_index_option_master). Replaces the Kite-instrument-cache path for
    indices when Dhan is the active broker — that path is skipped entirely for Dhan
    and was only ever serving a stale, narrow strike range from whatever was already
    in the DB.
    """
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_option_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index option contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "index",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        removed = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count
            removed = col.delete_many({
                "broker": "dhan", "instrument": normalized, "instrument_type": "index",
                "updated_at": {"$ne": now_ts},
            }).deleted_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "removed": removed,
            "message": "active_option_tokens sync completed from Dhan scrip master",
        }
    finally:
        db.close()


def _sync_dhan_index_future_tokens(instrument: str) -> dict:
    """
    Refresh active_option_tokens for one index's FUTIDX contracts (see
    _get_dhan_index_future_master). Same `option_type: "FUT", strike: 0.0` shape
    _sync_dhan_commodity_tokens already uses for MCX futures (FUTCOM) — that's
    proof this collection's compound index and every downstream reader already
    tolerate a strike-less contract; this just does the same thing for index
    futures, which were never synced anywhere before (every other index-token
    sync explicitly skips FUT* instrument types).
    """
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_future_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index future contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": 0.0,
                "option_type": "FUT",
            }
            update_payload = {
                **key,
                "instrument_type": "future",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-FUT",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        removed = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count
            removed = col.delete_many({
                "broker": "dhan", "instrument": normalized, "instrument_type": "future",
                "updated_at": {"$ne": now_ts},
            }).deleted_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "removed": removed,
            "message": "active_option_tokens FUT sync completed from Dhan scrip master",
        }
    finally:
        db.close()


_DHAN_COMMODITY_MASTER_CACHE: dict = {}  # {"rows": {underlying: [contract, ...]}, "date": "YYYY-MM-DD"}


def _get_dhan_commodity_master() -> dict[str, list[dict]]:
    """
    Returns {underlying: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for every MCX commodity — gold, silver, crude oil, copper, and everything else Dhan
    lists on MCX — covering both futures (FUTCOM, opt_type "FUT", strike 0) and options
    on futures (OPTFUT, opt_type CE/PE). Underlyings aren't a fixed list like the indices;
    they're discovered straight from whatever Dhan's scrip master actually carries.
    """
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _DHAN_COMMODITY_MASTER_CACHE.get("rows") and _DHAN_COMMODITY_MASTER_CACHE.get("date") == today_str:
        return _DHAN_COMMODITY_MASTER_CACHE["rows"]

    rows = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID", "").strip() != "MCX":
            continue
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        if inst not in ("FUTCOM", "OPTFUT"):
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        if inst == "FUTCOM":
            opt_type, strike = "FUT", 0.0
        else:
            opt_type = row.get("SEM_OPTION_TYPE", "").strip().upper()
            strike = float(row.get("SEM_STRIKE_PRICE") or 0)
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   strike,
            "opt_type": opt_type,
            "expiry":   expiry,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_COMMODITY_MASTER_CACHE["rows"] = master
    _DHAN_COMMODITY_MASTER_CACHE["date"] = today_str
    return master


def _sync_dhan_commodity_tokens(instrument: str) -> dict:
    """Refresh active_option_tokens for one MCX commodity (futures + options) from Dhan's scrip master."""
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_commodity_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan commodity contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE", "FUT"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "commodity",
                "exchange": "MCX",
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "MCX_COMM",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        removed = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count
            removed = col.delete_many({
                "broker": "dhan", "instrument": normalized, "instrument_type": "commodity",
                "updated_at": {"$ne": now_ts},
            }).deleted_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "removed": removed,
            "message": "active_option_tokens sync completed from Dhan scrip master (commodity)",
        }
    finally:
        db.close()


@app.get("/algo/debug/dhan-ltp")
async def debug_dhan_ltp(
    segment: str = Query(default="NSE_FNO"),
    sec_ids: str = Query(default=""),
):
    """
    Debug endpoint: calls Dhan /marketfeed/ltp directly and returns raw response.
    Usage: /algo/debug/dhan-ltp?segment=NSE_FNO&sec_ids=1123435,456789
    Also: /algo/debug/dhan-ltp?segment=IDX_I&sec_ids=13   (NIFTY spot)
    Also: /algo/debug/dhan-ltp?segment=NSE_EQ&sec_ids=4102  (equity spot)
    """
    db = MongoData()
    try:
        raw_db = db._db
        cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
        access_token = str(cfg.get("access_token") or "").strip()
        client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        if not access_token or not client_id:
            return {"error": "No Dhan credentials found in kite_market_config", "cfg_keys": list(cfg.keys())}

        ids_list = [int(x.strip()) for x in sec_ids.split(",") if x.strip().lstrip("-").isdigit()]
        if not ids_list:
            return {"error": "No valid sec_ids provided", "example": "?segment=NSE_FNO&sec_ids=1123435"}

        import requests as _req
        r = _req.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            headers={"access-token": access_token, "client-id": client_id, "Content-Type": "application/json"},
            json={segment: ids_list},
            timeout=5,
        )
        return {
            "status_code": r.status_code,
            "request_body": {segment: ids_list},
            "response": r.json() if r.status_code == 200 else r.text,
            "credentials_found": True,
            "client_id": client_id[:4] + "****",
        }
    finally:
        db.close()


@app.get("/algo/debug/dhan-equity-sec-id/{symbol}")
async def debug_dhan_equity_sec_id(symbol: str):
    """Check what equity sec_id is found for a stock symbol in Dhan CSV cache."""
    _get_dhan_fno_master()  # ensure populated
    equity_ids = _FNO_MASTER_CACHE.get("equity_ids", {})
    sym = symbol.strip().upper()
    return {
        "symbol": sym,
        "equity_sec_id": equity_ids.get(sym, "NOT FOUND"),
        "total_equity_ids_cached": len(equity_ids),
        "sample_keys": list(equity_ids.keys())[:10],
    }


@app.get("/algo/debug/dhan-quote")
async def debug_dhan_quote(
    segment: str = Query(default="NSE_FNO"),
    sec_ids: str = Query(default=""),
):
    """
    Debug endpoint: calls Dhan /marketfeed/quote and shows raw response (for OI field name check).
    Usage: /algo/debug/dhan-quote?segment=NSE_FNO&sec_ids=65174
    """
    db = MongoData()
    try:
        raw_db = db._db
        cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
        access_token = str(cfg.get("access_token") or "").strip()
        client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        if not access_token or not client_id:
            return {"error": "No Dhan credentials found", "cfg_keys": list(cfg.keys())}

        ids_list = [int(x.strip()) for x in sec_ids.split(",") if x.strip().lstrip("-").isdigit()]
        if not ids_list:
            return {"error": "No valid sec_ids", "example": "?segment=NSE_FNO&sec_ids=65174"}

        import requests as _req
        r = _req.post(
            "https://api.dhan.co/v2/marketfeed/quote",
            headers={"access-token": access_token, "client-id": client_id, "Content-Type": "application/json"},
            json={segment: ids_list},
            timeout=5,
        )
        rj = r.json() if r.status_code == 200 else {}
        # Show parsed OI/LTP for quick verification
        parsed: dict = {}
        if rj:
            seg_data = (rj.get("data") or {}).get(segment) or {}
            for sid, info in (seg_data.items() if isinstance(seg_data, dict) else []):
                if isinstance(info, dict):
                    parsed[str(sid)] = {"ltp": info.get("last_price"), "oi": info.get("oi")}
        return {
            "status_code": r.status_code,
            "dhan_status": rj.get("status"),
            "request_body": {segment: ids_list},
            "parsed_ltp_oi": parsed,
            "raw_response": rj if r.status_code == 200 else r.text,
        }
    finally:
        db.close()


@app.get("/algo/debug/chain-sources/{instrument}")
async def debug_chain_sources(instrument: str, expiry: str = Query(default="")):
    """
    Debug: shows what each LTP source returns for an instrument.
    Usage: /algo/debug/chain-sources/NIFTY?expiry=2026-06-23
           /algo/debug/chain-sources/360ONE?expiry=2026-06-30
    """
    from features.broker_gateway import get_broker_ltp_map, BROKER_INDEX_TOKENS, _active_broker as _ab
    normalized = instrument.strip().upper()
    active_broker = _ab()

    db = MongoData()
    out: dict = {"instrument": normalized, "active_broker": active_broker}
    try:
        # 1. WS ltp_map
        ltp_map = get_broker_ltp_map() or {}
        out["ws_ltp_map_size"] = len(ltp_map)

        # 2. Active option tokens from DB
        tok_col = db._db["active_option_tokens"]
        req_expiry = str(expiry or "").strip()[:10]
        if not req_expiry:
            from datetime import date as _date
            req_expiry = _date.today().isoformat()
        sample_contracts = list(tok_col.find(
            {"instrument": normalized, "expiry": {"$regex": f"^{req_expiry}"}, "broker": active_broker},
            {"_id": 0, "token": 1, "strike": 1, "option_type": 1},
        ).limit(5))
        all_tokens = [str(c.get("token") or "") for c in list(tok_col.find(
            {"instrument": normalized, "expiry": {"$regex": f"^{req_expiry}"}, "broker": active_broker},
            {"_id": 0, "token": 1},
        ))]
        all_tokens = [t for t in all_tokens if t]
        out["total_tokens_in_db"] = len(all_tokens)
        out["sample_contracts"] = sample_contracts
        out["tokens_in_ws_ltp_map"] = sum(1 for t in all_tokens if float(ltp_map.get(t) or 0) > 0)

        # 3. Dhan /ltp for first 5 tokens
        if active_broker == "dhan" and all_tokens:
            sample_ids = [int(t) for t in all_tokens[:5] if t.lstrip("-").isdigit()]
            fetched = _fetch_dhan_market_data("NSE_FNO", sample_ids, db)
            out["dhan_ltp_sample"] = fetched
            out["dhan_ltp_all_count"] = 0
            all_int_ids = [int(t) for t in all_tokens if t.lstrip("-").isdigit()]
            if all_int_ids:
                all_fetched = _fetch_dhan_market_data("NSE_FNO", all_int_ids, db)
                out["dhan_ltp_all_count"] = sum(1 for v in all_fetched.values() if v.get("ltp") or 0 > 0)
                out["dhan_ltp_all_sample"] = dict(list(all_fetched.items())[:5])

        # 3b. WS OI map
        try:
            from features.dhan_ticker import dhan_ticker_manager as _dtm_dbg
            _dbg_oi_map = _dtm_dbg.oi_map or {}
            out["ws_oi_map_size"] = len(_dbg_oi_map)
            out["tokens_in_ws_oi_map"] = sum(1 for t in all_tokens if int(_dbg_oi_map.get(t) or 0) > 0)
        except Exception:
            out["ws_oi_map_size"] = "n/a"

        # 4. NSE chain — with raw expiry dates for diagnosis
        import requests as _req_dbg
        from datetime import datetime as _dt_dbg
        nse_data = {}
        nse_raw_expiries: list = []
        nse_status_code = 0
        try:
            _is_idx = normalized in {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
            _nse_url = (f"https://www.nseindia.com/api/option-chain-indices?symbol={normalized}"
                        if _is_idx else
                        f"https://www.nseindia.com/api/option-chain-equities?symbol={normalized}")
            _sess_dbg = _req_dbg.Session()
            _sess_dbg.get("https://www.nseindia.com", timeout=5, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
            _r_dbg = _sess_dbg.get(_nse_url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com",
                "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9"})
            nse_status_code = _r_dbg.status_code
            if _r_dbg.status_code == 200:
                _rj = _r_dbg.json()
                _recs = _rj.get("records") or {}
                _rows = _recs.get("data") or []
                seen_exp: set = set()
                for _rw in _rows:
                    _exp = str(_rw.get("expiryDate") or "").strip()
                    if _exp and _exp not in seen_exp:
                        nse_raw_expiries.append(_exp)
                        seen_exp.add(_exp)
                    if len(nse_raw_expiries) >= 6:
                        break
        except Exception as _e_dbg:
            out["nse_raw_error"] = str(_e_dbg)
        out["nse_status_code"] = nse_status_code
        out["nse_raw_expiry_dates"] = nse_raw_expiries
        try:
            _dt_parsed = _dt_dbg.strptime(req_expiry, "%Y-%m-%d")
            _day = _dt_parsed.strftime("%d").lstrip("0")
            out["expiry_formats_we_match"] = [
                f"{_day}-{_dt_parsed.strftime('%b')}-{_dt_parsed.strftime('%Y')}",
                f"{_day} {_dt_parsed.strftime('%b')} {_dt_parsed.strftime('%Y')}",
            ]
        except Exception:
            out["expiry_formats_we_match"] = []
        try:
            nse_data = _fetch_nse_chain_data(normalized, req_expiry)
        except Exception as e:
            nse_data = {"error": str(e)}
        out["nse_chain_spot"] = nse_data.get("spot")
        out["nse_chain_strikes_count"] = len(nse_data.get("chain") or {})
        out["nse_chain_sample"] = dict(list((nse_data.get("chain") or {}).items())[:4])

    finally:
        db.close()
    return out



def _kite_popup_html(
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    payload = {
        "type":          "KITE_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    import json as _json
    payload_js = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kite Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">You can close this window after checking the URL.</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
  </script>
</body>
</html>"""


# ─── FlatTrade postback (order status push) ──────────────────────────────────

async def _parse_flattrade_postback_payload(request: Request) -> dict:
    data: dict = {}
    try:
        query_params = dict(request.query_params or {})
    except Exception:
        query_params = {}

    try:
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8", errors="replace")
    except Exception:
        body_str = ""

    try:
        if body_str.startswith("jData="):
            import urllib.parse
            parsed = urllib.parse.parse_qs(body_str)
            jdata_str = (parsed.get("jData") or ["{}"])[0]
            data = json.loads(jdata_str)
        elif body_str.strip():
            data = json.loads(body_str)
    except Exception as exc:
        log.warning("[FLATTRADE POSTBACK] body parse error: %s", exc)
        data = {}

    if not data and query_params:
        if "jData" in query_params:
            try:
                data = json.loads(str(query_params.get("jData") or "{}"))
            except Exception as exc:
                log.warning("[FLATTRADE POSTBACK] query jData parse error: %s", exc)
                data = {}
        else:
            data = query_params
    return data if isinstance(data, dict) else {}


def _process_flattrade_postback_payload(
    *,
    data: dict,
    broker_doc_id: str = "",
    source_tag: str = "FLATTRADE POSTBACK",
) -> None:
    from features.live_order_manager import process_broker_order_update

    order_id = str(data.get("norenordno") or data.get("order_id") or "").strip()
    status_raw = str(data.get("status") or "").upper().strip()
    fill_price = float(data.get("avgprc") or data.get("flprc") or data.get("prc") or 0)
    fill_qty = int(data.get("fillshares") or data.get("filledshares") or data.get("qty") or 0)
    rej_reason = str(data.get("rejreason") or data.get("emsg") or "").lower()
    uid = str(data.get("uid") or data.get("actid") or "").strip()

    log.info(
        "[%s] broker=%s uid=%s order_id=%s status=%s fill=%.2f qty=%d payload=%s",
        source_tag,
        broker_doc_id or "-",
        uid or "-",
        order_id,
        status_raw,
        fill_price,
        fill_qty,
        data,
    )

    if not order_id:
        return

    _status_map = {
        "COMPLETE": "COMPLETE",
        "COMPLETED": "COMPLETE",
        "REJECTED": "REJECTED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "OPEN": "OPEN",
        "TRIGGER_PENDING": "TRIGGER_PENDING",
    }
    status = _status_map.get(status_raw, status_raw)
    if status not in ("COMPLETE", "REJECTED", "CANCELLED"):
        return

    local_db = MongoData()
    try:
        if broker_doc_id:
            broker_order = local_db._db["broker_orders"].find_one(
                {"order_id": order_id},
                {"trade_id": 1},
            )
            if broker_order:
                trade_id = str(broker_order.get("trade_id") or "").strip()
                trade = local_db._db["algo_trades"].find_one(
                    {"_id": trade_id},
                    {"broker": 1},
                )
                trade_broker = str((trade or {}).get("broker") or "").strip()
                if trade_broker and trade_broker != broker_doc_id:
                    log.warning(
                        "[%s] broker=%s order_id=%s belongs_to=%s - skipping",
                        source_tag, broker_doc_id, order_id, trade_broker,
                    )
                    return

        updated = process_broker_order_update(
            local_db,
            order_id=order_id,
            status=status,
            fill_price=fill_price,
            fill_qty=fill_qty,
            rejection_reason=rej_reason,
            source="postback",
        )
        if not updated and status == "COMPLETE":
            exit_doc = local_db._db["broker_orders"].find_one(
                {"order_id": order_id},
                {"order_side": 1, "status": 1, "trade_id": 1, "leg_id": 1, "exit_reason": 1},
            ) or {}
            if str(exit_doc.get("order_side") or "").strip() == "exit":
                from features.live_order_manager import _sync_live_exit_fill
                trade_id = str(exit_doc.get("trade_id") or "").strip()
                leg_id = str(exit_doc.get("leg_id") or "").strip()
                exit_reason = str(exit_doc.get("exit_reason") or "stoploss").strip() or "stoploss"
                if trade_id and leg_id and fill_price > 0:
                    local_db._db["broker_orders"].update_one(
                        {"order_id": order_id},
                        {"$set": {
                            "status": "COMPLETE",
                            "fill_price": float(fill_price or 0),
                            "fill_qty": int(fill_qty or 0),
                            "filled_at": to_utc_iso(datetime.now(timezone.utc)),
                            "updated_at": to_utc_iso(datetime.now(timezone.utc)),
                        }},
                    )
                    _sync_live_exit_fill(local_db, trade_id, leg_id, exit_reason, fill_price)
                    updated = True
                    log.info(
                        "[%s] forced exit sync broker=%s order_id=%s trade=%s leg=%s reason=%s fill=%.2f",
                        source_tag, broker_doc_id or "-", order_id, trade_id, leg_id, exit_reason, fill_price,
                    )
        log.info("[%s] broker=%s order_id=%s updated=%s", source_tag, broker_doc_id or "-", order_id, updated)
    except Exception as exc:
        log.error("[%s] processing error broker=%s order_id=%s: %s", source_tag, broker_doc_id or "-", order_id, exc)
    finally:
        try:
            local_db.close()
        except Exception:
            pass


@router.get("/broker/flattrade/postback")
async def flattrade_postback_get(request: Request):
    data = await _parse_flattrade_postback_payload(request)
    if data:
        _process_flattrade_postback_payload(data=data, source_tag="FLATTRADE POSTBACK GET")
    return {"stat": "Ok"}


@router.post("/broker/flattrade/postback")
async def flattrade_postback(request: Request):
    """
    FlatTrade Order Notification postback endpoint.

    Configure in FlatTrade Developer Portal:
      Order Notification URL → https://your-server.com/broker/flattrade/postback

    FlatTrade POSTs order updates here when order status changes (fill / reject / cancel).
    Payload format (NorenApi / FlatTrade):
      Content-Type: application/x-www-form-urlencoded
      Body: jData={"t":"om","norenordno":"...","status":"COMPLETE","avgprc":"100.25",
                   "fillshares":"75","rejreason":"","uid":"...","exch":"NFO","tsym":"..."}

    Status mapping:
      COMPLETE        → order filled
      REJECTED        → broker rejected
      CANCELLED       → user / system cancelled
      OPEN            → order acknowledged (ignore — no DB change)
      TRIGGER_PENDING → SL trigger waiting (ignore)
    """
    data = await _parse_flattrade_postback_payload(request)
    if data:
        _process_flattrade_postback_payload(data=data, source_tag="FLATTRADE POSTBACK POST")
    return {"stat": "Ok"}


@router.get("/broker/flattrade/postback/{broker_doc_id}")
async def flattrade_postback_dynamic_get(request: Request, broker_doc_id: str):
    """GET handler so FlatTrade URL validation passes, and also process GET payload fallback."""
    data = await _parse_flattrade_postback_payload(request)
    if data:
        _process_flattrade_postback_payload(
            data=data,
            broker_doc_id=broker_doc_id,
            source_tag="FLATTRADE POSTBACK DYNAMIC GET",
        )
    return {"stat": "Ok", "broker": broker_doc_id}


@router.post("/broker/flattrade/postback/{broker_doc_id}")
async def flattrade_postback_dynamic(request: Request, broker_doc_id: str):
    """
    Dynamic postback per broker account.
    URL: https://finedgealgo.com/algo/broker/flattrade/postback/{broker_doc_id}
    FlatTrade sends order updates here (fill / reject / cancel).
    """
    data = await _parse_flattrade_postback_payload(request)
    if data:
        _process_flattrade_postback_payload(
            data=data,
            broker_doc_id=broker_doc_id,
            source_tag="FLATTRADE POSTBACK DYNAMIC POST",
        )
    return {"stat": "Ok"}


# ─── FlatTrade broker login ───────────────────────────────────────────────────

_flattrade_pending: dict = {}


@app.get("/broker/flattrade/postback")
async def flattrade_postback_validation_get(request: Request):
    """GET handler so FlatTrade URL validation passes."""
    return await flattrade_postback_get(request)


@app.post("/broker/flattrade/postback")
async def flattrade_postback_app_post(request: Request):
    """Fallback POST handler for postback URLs saved without /algo prefix."""
    return await flattrade_postback(request)


@app.get("/broker/flattrade/postback/{broker_doc_id}")
async def flattrade_postback_dynamic_app_get(request: Request, broker_doc_id: str):
    """GET handler so FlatTrade URL validation passes for dynamic postback URLs."""
    return await flattrade_postback_dynamic_get(request, broker_doc_id)


@app.post("/broker/flattrade/postback/{broker_doc_id}")
async def flattrade_postback_dynamic_app_post(request: Request, broker_doc_id: str):
    """Fallback POST handler for dynamic postback URLs saved without /algo prefix."""
    return await flattrade_postback_dynamic(request, broker_doc_id)


def _broker_popup_html(
    broker: str,
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    import json as _json
    payload = {
        "type":          f"{broker.upper()}_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    payload_js   = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{broker} Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
    setTimeout(() => window.close(), 1500);
  </script>
</body>
</html>"""


# ─── Live Market Data (KiteTicker) ───────────────────────────────────────────

_LIVE_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live Trade Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 420px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #94a3b8;
      margin-bottom: 2rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 2rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #22c55e; box-shadow: 0 0 8px #22c55e; animation: pulse 1.5s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #22c55e; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1.1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.5rem 0.75rem;
      display: flex; justify-content: space-between; align-items: center;
    }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #22c55e; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg {
      margin-top: 1rem; font-size: 0.8rem; color: #f87171;
      background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none;
    }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Live Trade Control</div>
  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>
  <button class="btn" id="actionBtn" disabled onclick="handleAction()">...</button>
  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks Received</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
  </div>
  <div class="spot-section">
    <div class="spot-title">Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>
  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>
<script>
  const API = '';

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/live/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn = document.getElementById('actionBtn');
    btn.disabled = false;
    if (status === 'running') {
      btn.textContent = 'Stop Live Trading';
      btn.className   = 'btn btn-stop';
    } else if (status === 'connecting') {
      btn.textContent = 'Connecting...';
      btn.className   = 'btn btn-start';
      btn.disabled    = true;
    } else {
      btn.textContent = 'Start Live Trading';
      btn.className   = 'btn btn-start';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el = document.getElementById('spot-' + sym);
      const v  = spotMap[sym];
      if (!el) return;
      if (v) {
        el.textContent = '\\u20B9' + Number(v).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';
    try {
      const url = status === 'running' ? '/live/stop' : '/live/start';
      await fetch(API + url + '?ui=1');
    } catch(e) { console.error(e); }
    setTimeout(fetchStatus, 800);
    setTimeout(fetchStatus, 2000);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 3000);
</script>
</body>
</html>"""


def _start_ticker_bg():
    """Run in background thread — loads tokens from DB and starts KiteTicker."""
    _db = MongoData()
    try:
        # print(
            # f'[MONITOR TICKER START] '
            # f'current_status={ticker_manager.status} '
            # f'tick_count={int(ticker_manager.tick_count or 0)}'
        # )
        if ticker_manager.status == "running":
            ticker_manager.restart(_db._db)
        else:
            ticker_manager.start(_db._db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("ticker start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _build_monitor_control_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live + Fast-Forward Monitor</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(34, 197, 94, 0.14), transparent 34%),
        linear-gradient(160deg, #07111f 0%, #0f172a 55%, #111827 100%);
      color: #e5eefb;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(560px, calc(100vw - 32px));
      background: rgba(10, 19, 34, 0.94);
      border: 1px solid rgba(148, 163, 184, 0.18);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 22px 70px rgba(0, 0, 0, 0.35);
    }}
    .eyebrow {{
      color: #7dd3fc;
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .title {{
      font-size: 30px;
      font-weight: 700;
      margin-bottom: 10px;
    }}
    .subtitle {{
      color: #94a3b8;
      font-size: 14px;
      line-height: 1.6;
      margin-bottom: 20px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      align-items: center;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(125, 211, 252, 0.12);
      border-radius: 18px;
      padding: 18px 20px;
      margin-bottom: 18px;
    }}
    .status-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 18px;
      font-weight: 600;
    }}
    .dot {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: #64748b;
      box-shadow: 0 0 0 transparent;
    }}
    .dot.running {{ background: #22c55e; box-shadow: 0 0 12px rgba(34, 197, 94, 0.8); }}
    .dot.connecting {{ background: #f59e0b; box-shadow: 0 0 12px rgba(245, 158, 11, 0.8); }}
    .dot.stopped {{ background: #ef4444; box-shadow: 0 0 12px rgba(239, 68, 68, 0.45); }}
    .clock-box {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .clock-label {{
      color: #64748b;
      font-size: 11px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    .clock-value {{
      margin-top: 6px;
      font-size: 18px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .stat {{
      background: rgba(15, 23, 42, 0.85);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 16px;
      padding: 14px 16px;
    }}
    .stat-label {{
      font-size: 11px;
      color: #64748b;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }}
    .stat-value {{
      font-size: 19px;
      font-weight: 700;
      line-height: 1.35;
      word-break: break-word;
    }}
    .actions {{
      display: flex;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .btn {{
      flex: 1;
      border: none;
      border-radius: 14px;
      padding: 14px 16px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn-primary {{ background: linear-gradient(135deg, #22c55e, #16a34a); color: #04110a; }}
    .btn-danger {{ background: linear-gradient(135deg, #f97316, #ef4444); color: #fff7ed; }}
    .btn-secondary {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148, 163, 184, 0.18); }}
    .btn:disabled {{ opacity: 0.55; cursor: not-allowed; }}
    .panel {{
      background: rgba(15, 23, 42, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 18px;
      padding: 16px;
    }}
    .panel-title {{
      color: #cbd5e1;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .strategies {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      max-height: 220px;
      overflow: auto;
    }}
    .strategy-item {{
      border-radius: 12px;
      padding: 12px 14px;
      background: rgba(8, 15, 28, 0.9);
      border: 1px solid rgba(148, 163, 184, 0.1);
    }}
    .strategy-name {{
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .strategy-meta {{
      color: #94a3b8;
      font-size: 12px;
      line-height: 1.5;
    }}
    .empty {{
      color: #94a3b8;
      font-size: 13px;
      line-height: 1.6;
      padding: 10px 4px 2px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="eyebrow">Auto Monitor</div>
    <div class="title">Live + Fast-Forward Monitor</div>
    <div class="subtitle">
      Single control page for both <b>live</b> and <b>fast-forward</b>. The backend supervisor starts automatically,
      refreshes active strategies every second, and keeps the live execution path highest priority.
    </div>

    <div class="hero">
      <div class="status-row">
        <span class="dot stopped" id="statusDot"></span>
        <span id="statusText">Loading...</span>
      </div>
      <div class="clock-box">
        <div class="clock-label">Server Time</div>
        <div class="clock-value" id="serverTime">--</div>
      </div>
    </div>

    <div class="grid">
      <div class="stat">
        <div class="stat-label">Trade Date</div>
        <div class="stat-value" id="tradeDateValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Live Count</div>
        <div class="stat-value" id="liveCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Started At</div>
        <div class="stat-value" id="startedAtValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Fast-Forward Count</div>
        <div class="stat-value" id="ffCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Last Tick</div>
        <div class="stat-value" id="lastTickValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Ticker Ticks</div>
        <div class="stat-value" id="tickCountValue">0</div>
      </div>
    </div>

    <div class="actions">
      <button class="btn btn-primary" id="toggleBtn" onclick="toggleMonitor()" disabled>Loading...</button>
      <button class="btn btn-secondary" onclick="refreshStatus()">Refresh</button>
    </div>

    <div class="panel">
      <div class="panel-title">Live Strategies</div>
      <div class="strategies" id="strategiesBox">
        <div class="empty">Checking active live strategies...</div>
      </div>
    </div>

    <div class="panel" style="margin-top: 14px;">
      <div class="panel-title">Fast-Forward Strategies</div>
      <div class="strategies" id="ffStrategiesBox">
        <div class="empty">Checking active fast-forward strategies...</div>
      </div>
    </div>
  </div>

  <script>
    function formatDateTime(value) {{
      if (!value) return '--';
      return String(value).replace('T', ' ').slice(0, 19);
    }}

    function escapeHtml(value) {{
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    async function startMonitorSilently() {{
      try {{
        await fetch('/monitor/start');
      }} catch (err) {{
        console.error(err);
      }}
    }}

    async function refreshStatus() {{
      try {{
        const res = await fetch('/monitor/status');
        const data = await res.json();
        renderStatus(data);
      }} catch (err) {{
        console.error(err);
      }}
    }}

    function renderStatus(data) {{
      const running = !!data.running;
      const status = data.monitor_status || (running ? 'running' : 'stopped');
      const button = document.getElementById('toggleBtn');
      const statusDot = document.getElementById('statusDot');
      const statusText = document.getElementById('statusText');
      const serverTime = document.getElementById('serverTime');
      const tradeDate = document.getElementById('tradeDateValue');
      const startedAt = document.getElementById('startedAtValue');
      const lastTick = document.getElementById('lastTickValue');
      const liveCountValue = document.getElementById('liveCountValue');
      const ffCountValue = document.getElementById('ffCountValue');
      const tickCountValue = document.getElementById('tickCountValue');
      const strategiesBox = document.getElementById('strategiesBox');
      const ffStrategiesBox = document.getElementById('ffStrategiesBox');

      statusDot.className = 'dot ' + (running ? 'running' : 'stopped');
      statusText.textContent = running ? 'Listening' : 'Stopped';
      serverTime.textContent = formatDateTime(data.server_time);
      tradeDate.textContent = data.trade_date || '--';
      startedAt.textContent = formatDateTime(data.started_at);
      lastTick.textContent = formatDateTime(data.last_tick_at);
      liveCountValue.textContent = String(((data.counts || {}).live) || 0);
      ffCountValue.textContent = String(((data.counts || {})['fast-forward']) || 0);
      tickCountValue.textContent = String(data.tick_count || 0);

      button.disabled = false;
      button.textContent = running ? 'Stop Listening' : 'Start Listening';
      button.className = 'btn ' + (running ? 'btn-danger' : 'btn-primary');
      button.dataset.running = running ? '1' : '0';

      const recordsByMode = data.records_by_mode || {{}};
      const liveRecords = Array.isArray(recordsByMode.live) ? recordsByMode.live : [];
      const ffRecords = Array.isArray(recordsByMode['fast-forward']) ? recordsByMode['fast-forward'] : [];

      function renderRecords(records, emptyText) {{
        if (!records.length) {{
          return '<div class="empty">' + emptyText + '</div>';
        }}
        return records.map(function(record) {{
          return (
            '<div class="strategy-item">' +
              '<div class="strategy-name">' + escapeHtml(record.name || '-') + '</div>' +
              '<div class="strategy-meta">' +
                'Group: ' + escapeHtml(record.group_name || '-') + '<br>' +
                'Ticker: ' + escapeHtml(record.ticker || '-') + '<br>' +
                'Mode: ' + escapeHtml(record.activation_mode || '-') + '<br>' +
                'Entry: ' + escapeHtml(record.entry_time || '-') + ' | Exit: ' + escapeHtml(record.exit_time || '-') + '<br>' +
                'Open Legs: ' + escapeHtml(record.open_legs || 0) + '/' + escapeHtml(record.total_legs || 0) +
              '</div>' +
            '</div>'
          );
        }}).join('');
      }}

      strategiesBox.innerHTML = renderRecords(
        liveRecords,
        'No active live strategies right now. Supervisor still keeps checking every second.'
      );
      ffStrategiesBox.innerHTML = renderRecords(
        ffRecords,
        'No active fast-forward strategies right now. Supervisor still keeps checking every second.'
      );
    }}

    async function toggleMonitor() {{
      const button = document.getElementById('toggleBtn');
      const running = button.dataset.running === '1';
      button.disabled = true;
      button.textContent = 'Please wait...';
      try {{
        const path = running ? '/monitor/stop' : '/monitor/start';
        await fetch(path);
      }} catch (err) {{
        console.error(err);
      }}
      setTimeout(refreshStatus, 400);
      setTimeout(refreshStatus, 1200);
    }}

    startMonitorSilently().then(function() {{
      refreshStatus();
      setInterval(refreshStatus, 1000);
    }});
  </script>
</body>
</html>"""


def _resolve_monitor_supervisor(scope: str):
    return live_fast_monitor_supervisor_crypto if scope == 'crypto' else live_fast_monitor_supervisor


def _start_monitor_services(trade_date: str = '', scope: str = 'nse') -> dict:
    import threading
    import asyncio

    normalized_trade_date = str(trade_date or '').strip() or nse_trade_date()
    supervisor = _resolve_monitor_supervisor(scope)
    # print(
        # f'[MONITOR START REQUEST] scope={scope} '
        # f'trade_date={normalized_trade_date} '
        # f'ticker_status={ticker_manager.status} '
        # f'tick_count={int(ticker_manager.tick_count or 0)}'
    # )
    if scope == 'crypto':
        from features.delta_exchange_ws import delta_ticker_manager
        delta_ticker_manager.start()
        # Proactively subscribes every running crypto strategy's option-leg
        # contracts BEFORE its entry attempt needs them — closes the cold-
        # start gap where the first entry attempt itself triggered
        # ensure_subscribed(), paying 3 chained Delta REST round trips
        # (~7-10s measured) before that leg's first price was available.
        # See crypto_leg_prewarm.py's module docstring for the full trace.
        from features.crypto_leg_prewarm import ensure_started as ensure_crypto_prewarm_started
        ensure_crypto_prewarm_started()
    else:
        if ticker_manager.status not in ('running', 'connecting'):
            threading.Thread(target=_start_ticker_bg, daemon=True).start()
        # NSE/Dhan counterpart of the crypto prewarm above — proactively
        # subscribes every running strategy's option-chain tokens BEFORE its
        # entry attempt needs them. See option_leg_prewarm.py's module
        # docstring for the full trace.
        from features.option_leg_prewarm import ensure_started as ensure_option_prewarm_started
        ensure_option_prewarm_started()
    supervisor.start(trade_date=normalized_trade_date)
    try:
        loop = asyncio.get_running_loop()
        live_entry_monitor.start(loop, scope=scope)
        live_exit_monitor.start(loop, scope=scope)
        leg_range_monitor.start(loop, scope=scope)
    except RuntimeError:
        pass
    return {
        'ok': True,
        'message': 'Global monitor started',
        'scope': scope,
        'trade_date': supervisor.trade_date,
    }


def _scheduled_start_monitor_services() -> dict:
    """Same as /monitor/start, minus the HTML response — used by the
    NSE market-hours scheduler (see _live_monitor_market_hours_schedule) so a
    not-yet-configured broker session doesn't raise, it just skips for today;
    the manual /monitor/start page remains the override. NSE-only — crypto
    auto-starts unconditionally at process boot instead, see
    _crypto_monitor_auto_start."""
    is_ready, reason = _get_kite_market_session_status()
    if not is_ready:
        log.info("[market_hours_scheduler] skip auto-start — session not ready: %s", reason)
        return {'ok': False, 'message': reason}
    return _start_monitor_services(scope='nse')


def _build_monitor_status_payload(scope: str = 'nse') -> dict:
    supervisor = _resolve_monitor_supervisor(scope)
    supervisor_status = supervisor.get_status()
    payload = {
        'server_time': to_utc_iso(datetime.now(timezone.utc)),
        'scope': scope,
        'running': bool(supervisor_status.get('running')),
        'monitor_status': 'running' if bool(supervisor_status.get('running')) else 'stopped',
        'trade_date': str(supervisor_status.get('trade_date') or nse_trade_date()),
        'started_at': str(supervisor_status.get('started_at') or ''),
        'last_tick_at': str(supervisor_status.get('last_tick_at') or ''),
        'last_refresh_at': str(supervisor_status.get('last_refresh_at') or ''),
        'counts': supervisor_status.get('counts') or {},
        'records_by_mode': supervisor_status.get('records_by_mode') or {},
    }
    if scope == 'crypto':
        from features.delta_exchange_ws import delta_ticker_manager
        crypto_ticker_status = delta_ticker_manager.get_status()
        payload.update({
            'ticker_status': str(crypto_ticker_status.get('status') or ('running' if crypto_ticker_status.get('connected') else 'stopped')),
            'tick_count': crypto_ticker_status.get('tick_count'),
            'ltp_count': crypto_ticker_status.get('ltp_count'),
            'spot_map': crypto_ticker_status.get('spot_map') or {},
            'ticker_error': str(crypto_ticker_status.get('error') or ''),
        })
    else:
        ticker_status = ticker_manager.get_status()
        payload.update({
            'ticker_status': str(ticker_status.get('status') or ''),
            'tick_count': ticker_status.get('tick_count'),
            'ltp_count': ticker_status.get('ltp_count'),
            'spot_map': ticker_status.get('spot_map') or {},
            'ticker_error': str(ticker_status.get('error') or ''),
        })
    return payload


def _build_live_ltp_payload(active_contracts: list[dict], now_ts: str) -> list[dict]:
    payload: list[dict] = []
    for contract in (active_contracts or []):
        token = str(contract.get("token") or "").strip()
        option_type = str(contract.get("option") or "").strip()
        if option_type == "SPOT":
            underlying = str(contract.get("underlying") or "").strip().upper()
            spot_price = float(ticker_manager.get_spot(underlying) or 0.0)
            if spot_price <= 0:
                continue
            payload.append({
                "token": token,
                "timestamp": now_ts,
                "ltp": spot_price,
                "bb_qty": 0,
                "bb_price": 0.0,
                "ba_qty": 0,
                "ba_price": 0.0,
                "vol_in_day": 0,
                "underlying": underlying,
                "option_type": "SPOT",
            })
            continue

        live_ltp = float(ticker_manager.get_ltp(token) or 0.0)
        if live_ltp <= 0:
            continue
        payload.append({
            "token": token,
            "timestamp": now_ts,
            "ltp": live_ltp,
            "bb_qty": 0,
            "bb_price": 0.0,
            "ba_qty": 0,
            "ba_price": 0.0,
            "vol_in_day": 0,
            "expiry": str(contract.get("expiry_date") or ""),
            "strike": contract.get("strike"),
            "option_type": option_type,
        })
    return payload


def _save_market_kite_session(session: dict) -> None:
    api_key = session.get("api_key") or str(getattr(get_kite_instance(), "api_key", "") or "").strip()
    access_token = session.get("access_token")
    login_time = to_utc_iso(datetime.now(timezone.utc))
    update_fields = {
        "broker": "kite",
        "api_key": api_key,
        "access_token": access_token,
        "login_time": login_time,
        "user_id": session.get("user_id"),
        "user_name": session.get("user_name"),
        "app_user_id": _resolve_app_user_id(),
    }
    local_db = MongoData()
    try:
        # Match by broker, not by whichever doc currently has enabled:True —
        # that used to match Dhan's doc whenever Dhan was the active broker,
        # overwriting its credentials with this Kite session. Each broker's
        # own login should never be able to touch another broker's doc.
        existing = local_db._db["kite_market_config"].find_one({"broker": "kite"}, {"api_secret": 1}) or {}
        api_secret = str(existing.get("api_secret") or "").strip()
        local_db._db["kite_market_config"].update_one(
            {"broker": "kite"},
            {"$set": update_fields},
            upsert=True,
        )
        from features.kite_broker import sync_kite_access_token_by_credentials
        sync_kite_access_token_by_credentials(
            local_db._db, api_key, api_secret, access_token, login_time,
            skip_collection="kite_market_config",
        )
    finally:
        local_db.close()


def _clear_market_kite_session() -> None:
    local_db = MongoData()
    try:
        local_db._db["kite_market_config"].update_one(
            {"enabled": True},
            {"$set": {"access_token": "", "login_time": to_utc_iso(datetime.now(timezone.utc))}},
            upsert=True,
        )
    finally:
        local_db.close()


def _get_kite_market_session_status() -> tuple[bool, str]:
    local_db = MongoData()
    try:
        cfg = local_db._db["kite_market_config"].find_one(
            {"enabled": True},
            {"access_token": 1, "api_key": 1, "user_id": 1, "broker": 1, "login_time": 1},
        ) or {}
        broker = str(cfg.get("broker") or "kite").strip().lower()
        access_token = str(cfg.get("access_token") or "").strip()
        login_time = str(cfg.get("login_time") or "").strip()

        if broker == "dhan":
            user_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
            if not user_id:
                return False, "Dhan config missing user_id in kite_market_config"
            if not access_token:
                return False, "Dhan access_token not found in kite_market_config"
            # Load into dhan_broker_ws cache so the WS can start
            try:
                from features.dhan_broker_ws import set_common_credentials  # type: ignore
                set_common_credentials(user_id, access_token)
            except Exception:
                pass
            # Validate via Dhan profile API
            try:
                import requests as _req  # type: ignore
                resp = _req.get(
                    "https://api.dhan.co/v2/profile",
                    headers={"access-token": access_token, "Content-Type": "application/json"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    return True, "Dhan access token valid"
                return False, f"Dhan token invalid (HTTP {resp.status_code})"
            except Exception as exc:
                return False, f"Dhan token validation error: {exc}"

        # ── Kite path ──
        api_key = str(cfg.get("api_key") or "").strip()
        if not api_key:
            return False, (
                "Kite market config missing api_key"
                + (f" (login_time: {login_time})" if login_time else "")
            )
        if not access_token:
            return False, "Access token not found"
    finally:
        local_db.close()

    try:
        kite = get_kite_instance(access_token)
        kite.profile()
        return True, "Access token valid"
    except Exception as exc:
        try:
            _clear_market_kite_session()
        except Exception:
            pass
        return False, f"Access token invalid or expired: {exc}"


def _has_ready_kite_market_session() -> bool:
    is_ready, _ = _get_kite_market_session_status()
    return is_ready


def _build_monitor_dhan_token_page(trade_date: str = '', reason: str = '', retry_url: str = '') -> str:
    reason_text = str(reason or "Dhan access token not configured").strip()
    if not retry_url:
        retry_url = '/monitor/start' + (f'?trade_date={trade_date}' if trade_date else '')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dhan Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; min-height: 100vh; display: flex; align-items: center;
      justify-content: center;
      background: radial-gradient(circle at top, rgba(249,115,22,0.12), transparent 34%),
                  linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0; font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(480px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(249,115,22,0.22);
      border-radius: 28px; padding: 36px 28px;
      box-shadow: 0 28px 80px rgba(0,0,0,0.38); text-align: center;
    }}
    .badge {{
      display: inline-block; padding: 7px 16px; border-radius: 999px;
      background: rgba(249,115,22,0.12); border: 1px solid rgba(249,115,22,0.28);
      color: #fb923c; letter-spacing: 0.12em; font-size: 11px; text-transform: uppercase;
    }}
    h1 {{ margin: 18px 0 10px; font-size: 26px; line-height: 1.15; color: #f8fafc; }}
    p {{ margin: 0 auto 0; max-width: 400px; color: #94a3b8; line-height: 1.7; font-size: 15px; }}
    .reason {{ margin-top: 10px; color: #f87171; font-size: 13px; }}
    .actions {{ margin-top: 26px; display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }}
    .btn {{
      display: inline-flex; align-items: center; justify-content: center;
      min-width: 180px; padding: 14px 22px; border-radius: 14px;
      text-decoration: none; border: none; cursor: pointer;
      font-size: 15px; font-weight: 700; transition: opacity .15s;
    }}
    .btn:active {{ opacity: .8; }}
    .btn-dhan {{ background: linear-gradient(135deg, #f97316, #ea580c); color: #fff;
                 box-shadow: 0 8px 24px rgba(249,115,22,0.3); }}
    .btn-retry {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148,163,184,0.18); }}
    .hint {{ margin-top: 18px; color: #64748b; font-size: 13px; min-height: 1.4rem; }}
    .divider {{ margin: 24px 0 16px; border: none; border-top: 1px solid rgba(148,163,184,0.1); }}
    .manual-label {{
      font-size: 12px; color: #475569; cursor: pointer; text-decoration: underline;
      display: block; margin-bottom: 12px;
    }}
    .form-row {{ display: flex; flex-direction: column; gap: 10px; text-align: left; }}
    label {{ font-size: 13px; color: #94a3b8; margin-bottom: 2px; }}
    input {{
      width: 100%; padding: 11px 14px; border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.22); background: rgba(30,41,59,0.8);
      color: #f1f5f9; font-size: 14px;
    }}
    input:focus {{ outline: none; border-color: #f97316; }}
    .btn-save {{
      width: 100%; margin-top: 12px; padding: 12px; border-radius: 10px;
      background: linear-gradient(135deg,#f97316,#ea580c); border: none;
      color: #fff; font-size: 15px; font-weight: 700; cursor: pointer;
    }}
    .err {{ color: #f87171; font-size: 13px; margin-top: 8px; display: none; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Dhan Login Required</div>
    <h1>Connect Dhan Account</h1>
    <p>Login with Dhan to start the live monitor. Your credentials are already saved — just click the button below.</p>
    <p class="reason">Reason: {reason_text}</p>

    <div class="actions">
      <button class="btn btn-dhan" onclick="openDhanLogin()">Login with Dhan →</button>
      <a class="btn btn-retry" href="{retry_url}">Retry</a>
    </div>
    <div class="hint" id="hintText">A Dhan login window will open. Complete login there and return here.</div>

    <hr class="divider">
    <span class="manual-label" onclick="document.getElementById('manualForm').style.display='block';this.style.display='none'">
      Or enter access token manually
    </span>
    <div id="manualForm" style="display:none">
      <div class="form-row">
        <div>
          <label>Dhan Client ID</label>
          <input id="clientId" type="text" placeholder="e.g. HA9835" autocomplete="off" />
        </div>
        <div>
          <label>Access Token</label>
          <input id="accessToken" type="password" placeholder="Paste Dhan access token" autocomplete="off" />
        </div>
      </div>
      <div class="err" id="errMsg"></div>
      <button class="btn-save" onclick="saveToken()">Save &amp; Start Monitor</button>
    </div>
  </div>
  <script>
    let _popup = null;

    function openDhanLogin() {{
      const hint = document.getElementById('hintText');
      hint.textContent = 'Opening Dhan login window...';
      _popup = window.open('/broker/dhan/login', 'DhanLogin',
        'width=420,height=560,resizable=yes,scrollbars=yes');
      if (!_popup) {{
        hint.textContent = 'Popup blocked — allow popups and try again, or use the link below.';
        return;
      }}
      hint.textContent = 'Complete login in the Dhan window. This page will refresh automatically.';
    }}

    window.addEventListener('message', function(e) {{
      if (!e.data || e.data.type !== 'DHAN_LOGIN') return;
      const hint = document.getElementById('hintText');
      if (e.data.success) {{
        hint.textContent = 'Login successful! Redirecting to monitor...';
        hint.style.color = '#22c55e';
        setTimeout(() => {{ window.location.href = {json.dumps(retry_url)}; }}, 1000);
      }} else {{
        hint.textContent = 'Login failed: ' + (e.data.message || 'Unknown error');
        hint.style.color = '#f87171';
      }}
    }});

    async function saveToken() {{
      const clientId    = document.getElementById('clientId').value.trim();
      const accessToken = document.getElementById('accessToken').value.trim();
      const err  = document.getElementById('errMsg');
      err.style.display = 'none';
      if (!clientId || !accessToken) {{
        err.textContent = 'Both Client ID and Access Token are required.';
        err.style.display = 'block';
        return;
      }}
      const hint = document.getElementById('hintText');
      hint.textContent = 'Saving credentials...';
      try {{
        const res = await fetch('/broker/dhan/config', {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{ client_id: clientId, access_token: accessToken }}),
        }});
        const data = await res.json();
        if (!res.ok || data.status === 'error') {{
          err.textContent = data.message || 'Failed to save credentials.';
          err.style.display = 'block';
          hint.textContent = 'Save failed.';
          return;
        }}
        hint.textContent = 'Saved! Starting monitor...';
        window.location.href = {json.dumps(retry_url)};
      }} catch (e) {{
        err.textContent = 'Network error: ' + e.message;
        err.style.display = 'block';
      }}
    }}
  </script>
</body>
</html>"""


def _build_monitor_kite_login_page(trade_date: str = '', reason: str = '') -> str:
    normalized_trade_date = str(trade_date or '').strip()
    retry_url = "/monitor/start"
    if normalized_trade_date:
        retry_url += f"?trade_date={normalized_trade_date}"
    reason_text = str(reason or "No broker session found").strip()

    # Detect active broker and show appropriate page
    try:
        _local_db = MongoData()
        _cfg = _local_db._db["kite_market_config"].find_one({"enabled": True}, {"broker": 1}) or {}
        _local_db.close()
        if str(_cfg.get("broker") or "kite").strip().lower() == "dhan":
            return _build_monitor_dhan_token_page(
                trade_date=trade_date, reason=reason_text, retry_url=retry_url,
            )
    except Exception:
        pass
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Kite Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(59, 130, 246, 0.16), transparent 34%),
        linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(520px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .badge {{
      display: inline-block;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.14);
      color: #7dd3fc;
      letter-spacing: 0.14em;
      font-size: 12px;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 18px 0 12px;
      font-size: 32px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      color: #94a3b8;
      line-height: 1.7;
      font-size: 15px;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 220px;
      padding: 16px 22px;
      border-radius: 18px;
      text-decoration: none;
      border: none;
      cursor: pointer;
      font-size: 17px;
      font-weight: 700;
    }}
    .btn.primary {{
      background: linear-gradient(135deg, #38bdf8, #2563eb);
      color: #eff6ff;
      box-shadow: 0 16px 32px rgba(37, 99, 235, 0.24);
    }}
    .btn.secondary {{
      background: #1e293b;
      color: #cbd5e1;
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
    .hint {{
      margin-top: 18px;
      color: #7dd3fc;
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Kite Required</div>
    <h1>Connect Kite API First</h1>
    <p>
      Monitor start needs a valid Kite access token. Login popup will open, save the access token,
      and then this page will automatically start the server listener.
    </p>
    <p style="margin-top:14px;color:#7dd3fc;font-size:13px;">Reason: {reason_text}</p>
    <div class="actions">
      <button class="btn primary" onclick="openKiteLogin()">Connect Kite API</button>
      <a class="btn secondary" href="/monitor/stop">Open Stop Page</a>
    </div>
    <div class="hint" id="hintText">Waiting for Kite login...</div>
  </div>

  <script>
    let kitePopup = null;

    function openKiteLogin() {{
      kitePopup = window.open('/broker/kite/login', 'kiteLogin', 'width=540,height=720');
      if (!kitePopup) {{
        document.getElementById('hintText').textContent = 'Popup blocked. Please allow popups and click again.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login popup opened. Complete login to continue.';
    }}

    window.addEventListener('message', function(event) {{
      const data = event.data || {{}};
      if (data.type !== 'KITE_LOGIN') return;
      if (!data.success) {{
        document.getElementById('hintText').textContent = data.message || 'Kite login failed.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login successful. Starting monitor...';
      window.location.href = {json.dumps(retry_url)};
    }});

    setTimeout(openKiteLogin, 250);
  </script>
</body>
</html>"""


def _build_monitor_action_page(*, running: bool, trade_date: str = '') -> str:
    title = 'Monitor Running' if running else 'Monitor Stopped'
    status_text = 'Listening is active' if running else 'Listening is stopped'
    button_label = 'Stop Listening' if running else 'Start Listening'
    button_href = '/monitor/stop' if running else '/monitor/start'
    button_class = 'danger' if running else 'success'
    trade_date_text = str(trade_date or '').strip() or nse_trade_date()
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(56, 189, 248, 0.18), transparent 32%),
        linear-gradient(155deg, #06101d 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .shell {{
      width: min(540px, calc(100vw - 32px));
      padding: 18px;
    }}
    .card {{
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.14);
      font-size: 13px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: #cbd5e1;
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: {('#22c55e' if running else '#ef4444')};
      box-shadow: 0 0 14px {('rgba(34, 197, 94, 0.85)' if running else 'rgba(239, 68, 68, 0.7)')};
    }}
    h1 {{
      margin: 20px 0 12px;
      font-size: 34px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      font-size: 15px;
      line-height: 1.7;
      color: #94a3b8;
    }}
    .meta {{
      margin-top: 18px;
      font-size: 13px;
      color: #7dd3fc;
      letter-spacing: 0.06em;
      font-variant-numeric: tabular-nums;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 240px;
      padding: 16px 24px;
      border-radius: 18px;
      text-decoration: none;
      font-size: 18px;
      font-weight: 700;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn.success {{
      background: linear-gradient(135deg, #22c55e, #16a34a);
      color: #04110a;
      box-shadow: 0 16px 32px rgba(22, 163, 74, 0.28);
    }}
    .btn.danger {{
      background: linear-gradient(135deg, #fb7185, #ef4444);
      color: #fff7ed;
      box-shadow: 0 16px 32px rgba(239, 68, 68, 0.24);
    }}
    .link-row {{
      margin-top: 18px;
      font-size: 14px;
    }}
    .link-row a {{
      color: #7dd3fc;
      text-decoration: none;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="card">
      <div class="pill"><span class="dot"></span>{status_text}</div>
      <h1>{title}</h1>
      <p>
        Single monitor service for live and fast-forward is currently
        {'running and checking active strategies every second.' if running else 'stopped. Click below to start listening again.'}
      </p>
      <div class="meta">Trade Date: {trade_date_text}</div>
      <div class="actions">
        <a class="btn {button_class}" href="{button_href}">{button_label}</a>
      </div>
      <div class="link-row"><a href="/monitor">Open Full Monitor</a></div>
    </div>
  </div>
</body>
</html>"""


@app.get("/live/loop-status")
async def live_loop_status():
    """
    live_monitor_loop's OWN running state (started/stopped, current
    activation_mode) — distinct from /live/status above, which returns the
    NSE ticker_manager's status instead (a different thing: that's the
    Dhan/Kite broker connection, this is the LTP/MTM broadcast loop itself,
    the one /live/start's activation_mode param actually controls). Added
    for the admin Monitors page's crypto fast-forward loop card, which needs
    this loop's real running flag, not the NSE ticker's.
    """
    return live_monitor_loop.get_status()


@app.get("/monitor", response_class=HTMLResponse)
async def monitor_page(trade_date: str = Query(default='')):
    _start_monitor_services(trade_date=trade_date)
    return HTMLResponse(content=_build_monitor_control_html())


@app.get("/monitor/start")
async def monitor_start(trade_date: str = Query(default='')):
    is_ready, reason = _get_kite_market_session_status()
    if not is_ready:
        return HTMLResponse(content=_build_monitor_kite_login_page(trade_date=trade_date, reason=reason))
    payload = _start_monitor_services(trade_date=trade_date)
    return HTMLResponse(
        content=_build_monitor_action_page(
            running=True,
            trade_date=str(payload.get('trade_date') or trade_date or ''),
        )
    )


def _stop_monitor_services(scope: str = 'nse') -> dict:
    """Stop one scope's monitor pieces only — NSE and crypto each own their
    own supervisor/entry/exit/leg-range instances (see live_fast_monitor.py's
    module docstring), so stopping NSE can never touch a running crypto trade
    and vice versa. The old cross-scope guard (_has_active_crypto_trades,
    which used to skip stopping the single shared supervisor whenever a
    crypto trade was active — the reason Stop looked broken) is gone; it's
    structurally impossible to need it now.

    live_monitor_loop (/live/*) is deliberately NOT touched here — it already
    has its own independent lifecycle/admin card (see /live/start, /live/stop,
    /live/loop-status); this function stopping it too was pre-existing scope
    creep, not something either scope's monitor is supposed to control.
    """
    supervisor = _resolve_monitor_supervisor(scope)
    trade_date = supervisor.trade_date
    if scope == 'crypto':
        from features.delta_exchange_ws import delta_ticker_manager
        delta_ticker_manager.stop()
    else:
        ticker_manager.stop()
    supervisor.stop()
    live_entry_monitor.stop(scope=scope)
    live_exit_monitor.stop(scope=scope)
    leg_range_monitor.stop(scope=scope)
    return {'ok': True, 'message': 'Monitor stopped', 'scope': scope, 'trade_date': trade_date}


@app.get("/monitor/stop")
async def monitor_stop():
    payload = _stop_monitor_services(scope='nse')
    return HTMLResponse(
        content=_build_monitor_action_page(
            running=False,
            trade_date=payload['trade_date'],
        )
    )


@app.get("/monitor/status")
async def monitor_status():
    return _build_monitor_status_payload(scope='nse')


@app.get("/monitor/crypto/start")
async def monitor_crypto_start(trade_date: str = Query(default='')):
    """Crypto counterpart of /monitor/start. No Kite-session gate — Delta
    uses permanent API keys, not a login session — and no market-hours
    restriction, since crypto trades 24/7 (see the crypto auto-start hook,
    which already starts this scope unconditionally at process boot; this
    route exists as the manual override/admin-card action, same relationship
    /monitor/start has to NSE's own auto-start)."""
    payload = _start_monitor_services(trade_date=trade_date, scope='crypto')
    return payload


@app.get("/monitor/crypto/stop")
async def monitor_crypto_stop():
    return _stop_monitor_services(scope='crypto')


@app.get("/monitor/crypto/status")
async def monitor_crypto_status():
    return _build_monitor_status_payload(scope='crypto')


@app.get("/delta/status")
async def delta_status():
    """Diagnostic: is this process's own Delta Exchange WS connection (see
    algo_main.py's _auto_start_delta_ticker) actually connected and holding
    live ticks, or is it stuck at boot / never reached Delta at all. Direct
    read of delta_ticker_manager's own state, no DB, no network call here."""
    from features.delta_exchange_ws import delta_ticker_manager
    status = delta_ticker_manager.get_status()
    sample_btc = delta_ticker_manager.get_ticker("BTCUSD")
    return {
        "ticker_status": status,
        "btcusd_perpetual_sample": sample_btc,
    }


@app.get("/delta/sync-to-analysis/{trade_id}")
async def delta_sync_to_analysis(trade_id: str):
    """
    Mirrors an algo.trade crypto trade into crypto_simulator_strategy so it
    shows up on the existing crypto Analysis/payoff page (algo-admin's
    CryptoTradeNew.tsx via /simulator/crypto/trade/{strategy_id}) — see
    shared/features/crypto_algo_trade_sync.py's module docstring for why a
    mirror instead of teaching that page to read algo_trades directly.
    Idempotent — safe to call again for the same trade_id (updates the
    existing mirror instead of duplicating it).
    """
    from features.crypto_algo_trade_sync import sync_algo_trade_to_crypto_simulator
    from features.mongo_data import MongoData
    sync_db = MongoData()
    try:
        strategy_id = sync_algo_trade_to_crypto_simulator(sync_db, trade_id)
    finally:
        sync_db.close()
    if not strategy_id:
        return {"ok": False, "message": "Trade not found, not a crypto trade, or has no entered legs yet."}
    return {
        "ok": True,
        "strategy_id": strategy_id,
        "url": f"/simulator/crypto/trade/{strategy_id}",
    }


# ─── Mock Ticker ──────────────────────────────────────────────────────────────

_MOCK_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mock Ticker Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 460px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #a78bfa;
      margin-bottom: 0.5rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .subtitle {
      font-size: 0.75rem; color: #475569;
      margin-bottom: 2rem;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 1.5rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #a78bfa; box-shadow: 0 0 8px #a78bfa; animation: pulse 1.5s infinite; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .mock-time-badge {
      font-size: 0.78rem; color: #a78bfa; margin-bottom: 1.25rem;
      font-variant-numeric: tabular-nums; min-height: 1.2em;
    }
    /* Time picker row — only shown when stopped */
    .time-row {
      display: flex; gap: 0.5rem; margin-bottom: 1.25rem;
    }
    .time-input {
      flex: 1; padding: 0.65rem 0.75rem;
      background: #0f172a; border: 1px solid #334155;
      border-radius: 8px; color: #e2e8f0; font-size: 0.875rem; outline: none;
      color-scheme: dark;
    }
    .time-input:focus { border-color: #7c3aed; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #7c3aed; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 0.5rem 0.75rem; display: flex; justify-content: space-between; align-items: center; }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #a78bfa; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg { margin-top: 1rem; font-size: 0.8rem; color: #f87171; background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none; }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Mock Ticker</div>
  <div class="subtitle">Simulates Kite WebSocket using historical DB data</div>

  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>

  <div class="mock-time-badge" id="mockTimeBadge"></div>

  <!-- Time picker — hidden when running -->
  <div class="time-row" id="timeRow">
    <input class="time-input" type="datetime-local" id="mockTimeInput" step="60" />
  </div>

  <button class="btn btn-start" id="actionBtn" disabled onclick="handleAction()">...</button>

  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Subscribed</div>
      <div class="stat-value" id="subCount">—</div>
    </div>
  </div>

  <div class="spot-section">
    <div class="spot-title">Mock Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>

  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>

<script>
  const API = '';   // same origin

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/mock/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn      = document.getElementById('actionBtn');
    const timeRow  = document.getElementById('timeRow');
    const badgeEl  = document.getElementById('mockTimeBadge');

    btn.disabled = false;

    if (status === 'running' || status === 'connecting') {
      btn.textContent = 'Stop Mock Server';
      btn.className   = 'btn btn-stop';
      if (status === 'connecting') btn.disabled = true;
      timeRow.style.display = 'none';
      badgeEl.textContent   = data.mock_time
        ? '\\u25B6 Simulating: ' + data.mock_time.replace('T', ' ')
        : '';
    } else {
      btn.textContent       = 'Start Listening';
      btn.className         = 'btn btn-start';
      timeRow.style.display = 'flex';
      const inputEl = document.getElementById('mockTimeInput');
      if (inputEl && data.mock_time) {
        inputEl.value = data.mock_time.slice(0, 16);
      }
      badgeEl.textContent   = data.mock_time
        ? 'Last stopped at: ' + data.mock_time.replace('T', ' ')
        : 'Set simulation start time above';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';
    document.getElementById('subCount').textContent =
      data.subscribed_tokens !== undefined ? data.subscribed_tokens.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el  = document.getElementById('spot-' + sym);
      const val = spotMap[sym];
      if (!el) return;
      if (val) {
        el.textContent = '\\u20B9' + Number(val).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';

    try {
      if (status === 'running') {
        await fetch(API + '/mock/stop');
      } else {
        const raw = document.getElementById('mockTimeInput').value;
        if (!raw) {
          await fetch(API + '/mock/start');
        } else {
          const timeStr = raw.length === 16 ? raw + ':00' : raw;
          await fetch(API + '/mock/start?time=' + encodeURIComponent(timeStr));
        }
      }
    } catch(e) { console.error(e); }

    setTimeout(fetchStatus, 600);
    setTimeout(fetchStatus, 1800);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 2000);
</script>
</body>
</html>"""


def _start_mock_bg(time_str: str) -> None:
    """Run in a daemon thread — sets mock time then starts MockTicker."""
    result = mock_ticker_manager.set_mock_time(time_str)
    if not result.get("ok"):
        import logging
        logging.getLogger(__name__).error("mock set_mock_time failed: %s", result)
        return
    _db = MongoData()
    try:
        mock_ticker_manager.start(_db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("mock start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _upsert_contracts_into_col(
    active_tokens_col,
    contracts: list[dict],
    stock_name: str,
    now_ts: str,
    broker: str = "",
) -> tuple[int, int]:
    if not contracts:
        return 0, 0

    from pymongo import UpdateOne

    _idx_set = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    instrument_type = "index" if stock_name.upper() in _idx_set else "stock"
    ops = []
    for contract in contracts:
        expiry_val = str(contract.get("expiry") or "").strip()[:10]
        opt_type_val = str(contract.get("option_type") or contract.get("opt_type") or "").strip().upper()
        strike_val = contract.get("strike")
        query: dict = {
            "instrument": stock_name,
            "expiry": expiry_val,
            "strike": strike_val,
            "option_type": opt_type_val,
        }
        if broker:
            query["broker"] = broker
        update_payload: dict = {
            "instrument": stock_name,
            "instrument_type": instrument_type,
            "expiry": expiry_val,
            "strike": strike_val,
            "option_type": opt_type_val,
            "token": str(contract.get("token") or "").strip(),
            "tokens": str(contract.get("tokens") or contract.get("token") or "").strip(),
            "symbol": str(contract.get("symbol") or "").strip(),
            "exchange": str(contract.get("exchange") or "").strip(),
            "updated_at": to_utc_iso(now_ts),
        }
        if broker:
            update_payload["broker"] = broker
        ops.append(UpdateOne(
            query,
            {"$set": update_payload, "$setOnInsert": {"created_at": to_utc_iso(now_ts)}},
            upsert=True,
        ))

    # Batched in one round-trip per call (ordered=False so one bad op can't stall the rest)
    # instead of one update_one() per contract — this is what made syncing thousands of
    # contracts take tens of seconds.
    result = active_tokens_col.bulk_write(ops, ordered=False)
    created = result.upserted_count
    updated = result.matched_count
    return created, updated


def _sync_active_option_tokens(instrument: str) -> dict:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    # NSE's own trading-calendar day (IST), not UTC — an option's
    # expired-vs-active classification is an NSE-calendar concept.
    today_str = nse_trade_date()
    db = MongoData()
    try:
        credentials_loaded = load_credentials_from_db(db)
        active_tokens_col = db._db["active_option_tokens"]
        try:
            active_tokens_col.create_index(
                [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
                name="idx_active_option_contract_v2",
            )
        except Exception:
            pass

        from features.broker_gateway import _active_broker as _sync_get_broker  # type: ignore
        active_broker = _sync_get_broker()

        _INDEX_SET = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}

        # Special case: iterate ALL non-index FNO stock underlyings
        if normalized_instrument == "FNO-STOCKS":
            now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
            created_count = 0
            updated_count = 0
            contracts_processed = 0
            all_expiries: set[str] = set()

            removed_count = 0
            if active_broker == "dhan":
                # Clear existing FNO stock contracts (expired + stale) before re-inserting
                deleted = active_tokens_col.delete_many({
                    "broker": "dhan",
                    "instrument_type": "stock",
                })
                removed_count = deleted.deleted_count
                if deleted.deleted_count == 0:
                    # First run — no instrument_type field yet, clear by excluding known indices
                    removed_count = active_tokens_col.delete_many({
                        "broker": "dhan",
                        "instrument": {"$nin": list(_INDEX_SET)},
                    }).deleted_count

                # Dhan: use CSV master directly (avoids circular DB read)
                from pymongo import UpdateOne

                master = _get_dhan_fno_master()
                ops = []
                for symbol, all_contracts in master.items():
                    for c in all_contracts:
                        exp = str(c.get("expiry") or "").strip()[:10]
                        if not exp or exp < today_str:
                            continue
                        all_expiries.add(exp)
                        contracts_processed += 1
                        opt_type = str(c.get("opt_type") or "").strip().upper()
                        query = {
                            "broker": "dhan",
                            "instrument": symbol,
                            "expiry": exp,
                            "strike": c.get("strike"),
                            "option_type": opt_type,
                        }
                        payload = {
                            "broker": "dhan",
                            "instrument": symbol,
                            "instrument_type": "stock",
                            "expiry": exp,
                            "strike": c.get("strike"),
                            "option_type": opt_type,
                            "token": str(c.get("sec_id") or "").strip(),
                            "tokens": str(c.get("sec_id") or "").strip(),
                            "symbol": f"{symbol}{int(c['strike']) if float(c['strike']).is_integer() else c['strike']}{opt_type}",
                            "exchange": str(c.get("exchange") or "NSE").strip(),
                            "lot_size": c.get("lot_size"),
                            "updated_at": now_ts,
                        }
                        ops.append(UpdateOne(
                            query, {"$set": payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
                        ))

                # Single batched round trip instead of one update_one() per contract —
                # FNO-STOCKS routinely has 90k+ contracts across 200+ symbols, and this
                # loop used to issue that many individual synchronous Mongo calls, which
                # is what made GET /algo/get_active_tokens/ALL (and /algo/sync-tokens/
                # start/ALL, which shares this function) take long enough to look hung.
                # bulk_write chunks internally to Mongo's own per-batch op limit, same
                # pattern _sync_dhan_index_option_tokens already uses.
                if ops:
                    result = active_tokens_col.bulk_write(ops, ordered=False)
                    created_count = result.upserted_count
                    updated_count = result.matched_count
            else:
                # Kite: load from Kite REST instruments API
                from features.spot_atm_utils import (  # type: ignore
                    _load_kite_instruments as _kite_inst_load,
                    list_kite_option_contracts as _kite_list_contracts,
                )
                known_indices = set(KITE_INDEX_TOKENS.keys())
                cache = _kite_inst_load(force=True)
                if not cache:
                    return {
                        "instrument": "FNO-STOCKS",
                        "expiries": [],
                        "contracts_processed": 0,
                        "created": 0,
                        "updated": 0,
                        "message": "No active option contracts found",
                        "credentials_loaded": credentials_loaded,
                        "hint": "Check kite_market_config access_token/login if this instrument should have live contracts",
                    }

                underlyings: dict[str, set[str]] = {}
                for (name, exp, _strike, _type) in cache:
                    if name not in known_indices and exp >= today_str:
                        underlyings.setdefault(name, set()).add(exp)

                for stock_name, expiry_set in underlyings.items():
                    for expiry in sorted(expiry_set):
                        contracts = _kite_list_contracts(stock_name, expiry)
                        all_expiries.update(expiry_set)
                        contracts_processed += len(contracts)
                        c, u = _upsert_contracts_into_col(
                            active_tokens_col, contracts, stock_name, now_ts, broker="kite"
                        )
                        created_count += c
                        updated_count += u

            return {
                "instrument": "FNO-STOCKS",
                "underlyings_count": contracts_processed,
                "expiries": sorted(all_expiries),
                "contracts_processed": contracts_processed,
                "created": created_count,
                "updated": updated_count,
                "removed": removed_count,
                "credentials_loaded": credentials_loaded,
                "message": "active_option_tokens sync completed" if contracts_processed else "No active option contracts found",
            }

        expiries = get_kite_expiries(normalized_instrument, today_str, force_refresh=True)
        if not expiries:
            return {
                "instrument": normalized_instrument,
                "expiries": [],
                "contracts_processed": 0,
                "created": 0,
                "updated": 0,
                "message": "No active option contracts found",
                "credentials_loaded": credentials_loaded,
                "hint": (
                    "Check kite_market_config access_token/login if this instrument should have live contracts"
                ),
            }

        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
        created_count = 0
        updated_count = 0
        contracts_processed = 0

        for expiry_index, expiry in enumerate(expiries):
            contracts = list_kite_option_contracts(
                normalized_instrument,
                expiry,
                force_refresh=(expiry_index == 0),
            )
            contracts_processed += len(contracts)
            c, u = _upsert_contracts_into_col(
                active_tokens_col, contracts, normalized_instrument, now_ts, broker=active_broker
            )
            created_count += c
            updated_count += u

        return {
            "instrument": normalized_instrument,
            "expiries": expiries,
            "contracts_processed": contracts_processed,
            "created": created_count,
            "updated": updated_count,
            "credentials_loaded": credentials_loaded,
            "message": "active_option_tokens sync completed",
        }
    finally:
        db.close()


def _get_live_index_spot_price(normalized_instrument: str) -> float:
    index_token = KITE_INDEX_TOKENS.get(normalized_instrument)
    if not index_token:
        return 0.0
    try:
        from features.broker_gateway import get_broker_ltp_map  # type: ignore

        ltp_value = (get_broker_ltp_map() or {}).get(str(index_token), 0.0)
        return float(ltp_value or 0.0)
    except Exception:
        return 0.0


def _resolve_single_option_ltp(
    db,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> float:
    normalized_underlying = str(underlying or "").strip().upper()
    normalized_expiry = str(expiry or "").strip()[:10]
    normalized_option_type = str(option_type or "").strip().upper()

    contract = {}
    try:
        contract = db["active_option_tokens"].find_one(
            {
                "instrument": normalized_underlying,
                "expiry": normalized_expiry,
                "strike": strike,
                "option_type": normalized_option_type,
            },
            {
                "_id": 0,
                "token": 1,
                "tokens": 1,
                "symbol": 1,
            },
        ) or {}
    except Exception:
        contract = {}

    token = str(contract.get("token") or contract.get("tokens") or "").strip()
    symbol = str(contract.get("symbol") or "").strip()
    if not token:
        try:
            inst = (_load_kite_instruments() or {}).get(
                (normalized_underlying, normalized_expiry, float(strike), normalized_option_type)
            ) or {}
            token = str(inst.get("token") or "").strip()
            symbol = str(inst.get("symbol") or "").strip()
        except Exception:
            token = ""

    if not token:
        log.warning(
            "margin quote token not found underlying=%s expiry=%s strike=%s option_type=%s",
            normalized_underlying,
            normalized_expiry,
            strike,
            normalized_option_type,
        )
        return 0.0

    try:
        live_ltp = float((get_ltp_map() or {}).get(token, 0.0) or 0.0)
        if live_ltp > 0:
            return live_ltp
    except Exception:
        pass

    try:
        if not is_configured():
            return 0.0
        api_key, access_token = get_common_credentials()
        if not api_key or not access_token:
            return 0.0
        kite = get_kite_instance(access_token)
        quotes = kite.quote([int(token)]) or {}
        for _quote_key, quote_doc in quotes.items():
            quote_ltp = float(
                quote_doc.get("last_price")
                or (quote_doc.get("ohlc") or {}).get("close")
                or 0.0
            )
            if quote_ltp > 0:
                # print(
                    # f"[MARGIN SINGLE QUOTE] underlying={normalized_underlying} "
                    # f"expiry={normalized_expiry} strike={strike} type={normalized_option_type} "
                    # f"token={token} symbol={symbol or '-'} ltp={quote_ltp}",
                    # flush=True,
                # )
                return quote_ltp
    except Exception as exc:
        log.warning(
            "margin single quote error underlying=%s expiry=%s strike=%s option_type=%s token=%s: %s",
            normalized_underlying,
            normalized_expiry,
            strike,
            normalized_option_type,
            token,
            exc,
        )

    return 0.0


def _resolve_margin_order_contract(
    db,
    underlying: str,
    instrument_type: str,
    expiry: str,
    strike: float,
) -> dict[str, Any]:
    normalized_underlying = str(underlying or "").strip().upper()
    normalized_instrument_type = str(instrument_type or "").strip().upper()
    normalized_expiry = str(expiry or "").strip()[:10]

    if normalized_instrument_type in {"CE", "PE"}:
        contract = db["active_option_tokens"].find_one(
            {
                "instrument": normalized_underlying,
                "expiry": normalized_expiry,
                "strike": strike,
                "option_type": normalized_instrument_type,
            },
            {
                "_id": 0,
                "symbol": 1,
                "exchange": 1,
            },
        ) or {}
        symbol = str(contract.get("symbol") or "").strip()
        exchange = str(contract.get("exchange") or "").strip() or ("BFO" if normalized_underlying in {"SENSEX", "BANKEX"} else "NFO")
        if symbol:
            return {"tradingsymbol": symbol, "exchange": exchange}
    return {}


def _calculate_kite_basket_margin(db, legs: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not legs or not is_configured():
        return None

    api_key, access_token = get_common_credentials()
    if not api_key or not access_token:
        return None

    orders: list[dict[str, Any]] = []
    for leg in legs:
        contract = _resolve_margin_order_contract(
            db,
            leg.get("underlying"),
            leg.get("instrument_type"),
            leg.get("expiry"),
            float(leg.get("strike") or 0.0),
        )
        tradingsymbol = str(contract.get("tradingsymbol") or "").strip()
        exchange = str(contract.get("exchange") or "").strip()
        quantity = int(leg.get("quantity") or 0) * int(leg.get("lot_size") or 0)
        if not tradingsymbol or not exchange or quantity <= 0:
            return None
        orders.append(
            {
                "exchange": exchange,
                "tradingsymbol": tradingsymbol,
                "transaction_type": str(leg.get("transaction_type") or "SELL").upper(),
                "variety": "regular",
                "product": "NRML",
                "order_type": "MARKET",
                "quantity": quantity,
                "price": 0,
                "trigger_price": 0,
            }
        )

    try:
        kite = get_kite_instance(access_token)
        return kite.basket_order_margins(orders, consider_positions=False) or {}
    except Exception as exc:
        log.warning("kite basket margin error: %s", exc)
        return None


def _build_full_option_chain_response(instrument: str) -> dict[str, Any]:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    allowed_instruments = {"NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY"}
    if normalized_instrument not in allowed_instruments:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported instrument '{normalized_instrument}'. "
                "Use one of: NIFTY, BANKNIFTY, SENSEX, FINNIFTY, MIDCPNIFTY"
            ),
        )

    cached_base = _get_active_option_chain_cache(normalized_instrument)
    if not cached_base:
        raise HTTPException(
            status_code=404,
            detail=f"No option chain rows found in active_option_tokens for instrument {normalized_instrument}",
        )

    response = deepcopy(cached_base)
    return {
        **response,
        "spot_price": _get_live_index_spot_price(normalized_instrument),
    }


@app.get("/algo/get-option-chain/{instrument}")
@app.get("/algo/get-opiton-chain/{instrument}")
async def get_option_chain_algo(instrument: str):
    return _build_full_option_chain_response(instrument)


@app.get("/get-option-chain/{instrument}")
@app.get("/get-opiton-chain/{instrument}")
async def get_option_chain(instrument: str):
    return _build_full_option_chain_response(instrument)


@app.get("/algo/option-chain-snapshot/{instrument}")
async def get_option_chain_snapshot(
    instrument: str,
    ts: str = Query(default=""),
    _activation_mode: str = Query(default="", alias="activation_mode"),
):
    """Historical option chain at listen_timestamp.
    Returns the exact same shape as /algo/get-option-chain/{instrument}.
    Falls back to Kite historical_data when DB data is more than 7 days stale."""
    normalized = str(instrument or "").strip().upper()
    norm_ts = str(ts or "").strip().replace(" ", "T").rstrip("Z")
    if not normalized:
        raise HTTPException(status_code=400, detail="Instrument is required")
    if not norm_ts:
        return _build_full_option_chain_response(normalized)

    req_date = norm_ts[:10]
    day_start = f"{req_date}T00:00:00" if len(req_date) == 10 else ""
    day_end = f"{req_date}T23:59:59" if len(req_date) == 10 else ""

    db = MongoData()
    try:
        chain_col = db._db["option_chain_historical_data"]

        # Step 1: find the nearest minute-tick timestamp at or before norm_ts
        # within the requested trading day only, so replay never leaks into a
        # previous day's snapshot when current-day rows are still loading.
        pivot_query: dict[str, Any] = {
            "underlying": normalized,
            "timestamp": {"$lte": norm_ts},
        }
        if day_start and day_end:
            pivot_query["timestamp"]["$gte"] = day_start

        pivot = chain_col.find_one(
            pivot_query,
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", -1)],
        )
        pivot_ts = pivot["timestamp"] if pivot else None

        # If same-day DB data is missing, try Kite fallback.
        _stale = not pivot_ts
        if pivot_ts and day_start and not (day_start <= pivot_ts <= day_end):
            _stale = True

        if _stale:
            from option_chain_kite_snapshot import get_option_chain_kite_snapshot
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None, lambda: get_option_chain_kite_snapshot(db, normalized, norm_ts)
            )

        # Step 2: all rows at that exact pivot timestamp
        raw_rows = list(chain_col.find(
            {"underlying": normalized, "timestamp": pivot_ts},
            {"_id": 0, "expiry": 1, "strike": 1, "type": 1, "token": 1, "close": 1,
             "iv": 1, "delta": 1, "oi": 1, "timestamp": 1},
        ))
        if not raw_rows:
            from option_chain_kite_snapshot import get_option_chain_kite_snapshot
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None, lambda: get_option_chain_kite_snapshot(db, normalized, norm_ts)
            )

        # Spot price: use $lte to always find nearest value
        spot_query: dict[str, Any] = {
            "underlying": normalized,
            "timestamp": {"$lte": pivot_ts},
        }
        if day_start and day_end:
            spot_query["timestamp"]["$gte"] = day_start

        spot_doc = db._db["option_chain_index_spot"].find_one(
            spot_query,
            {"_id": 0, "close": 1, "spot_price": 1},
            sort=[("timestamp", -1)],
        )
        _sd = spot_doc or {}
        spot = float(_sd.get("close") or _sd.get("spot_price") or 0)

        # Step 3: build response in the SAME shape as _build_full_option_chain_response
        expiry_set: set[str] = set()
        option_chain: list[dict] = []
        grouped_option_chain: dict[str, dict] = {}

        for row in raw_rows:
            expiry = str(row.get("expiry") or "")[:10]   # normalise to YYYY-MM-DD
            strike = row.get("strike")
            opt_type = str(row.get("type") or "").upper()
            if not expiry or strike is None or opt_type not in ("CE", "PE"):
                continue

            expiry_set.add(expiry)
            chain_row: dict[str, Any] = {
                "underlying": normalized,
                "expiry": expiry,
                "strike": float(strike),
                "type": opt_type,
                "token": str(row.get("token") or "").strip(),
                "close": float(row.get("close") or 0),
                "iv": float(row.get("iv") or 0) or None,
                "delta": float(row.get("delta") or 0) or None,
                "oi": int(row.get("oi") or 0),
                "spot_price": spot,
                "timestamp": pivot_ts,
            }
            option_chain.append(chain_row)

            exp_bucket = grouped_option_chain.setdefault(expiry, {})
            strike_key = str(int(strike)) if float(strike) == int(float(strike)) else str(strike)
            exp_bucket.setdefault(strike_key, {"CE": None, "PE": None})[opt_type] = chain_row

        expiries_sorted = sorted(expiry_set)
        return {
            "instrument": normalized,
            "expiries": expiries_sorted,
            "expiry_count": len(expiries_sorted),
            "total_contracts": len(option_chain),
            "source": "option_chain_snapshot",
            "option_chain": option_chain,
            "grouped_option_chain": grouped_option_chain,
            "spot_price": spot,
            "timestamp": pivot_ts,
        }
    finally:
        db.close()


@app.delete("/algo/option-chain-kite-cache")
async def clear_option_chain_kite_cache(
    underlying: str = Query(default=""),
    date: str = Query(default=""),
):
    """Clear the in-process Kite option-chain day-cache so the next snapshot
    request fetches fresh data from Kite.
    ?underlying=NIFTY&date=2026-05-26  → clear one entry
    ?underlying=NIFTY                  → clear all dates for that underlying
    (no params)                        → clear everything"""
    from option_chain_kite_snapshot import clear_day_cache
    clear_day_cache(underlying.upper() or None, date or None)
    return {"cleared": True, "underlying": underlying or "all", "date": date or "all"}


@app.post("/algo/option-chain/backfill-today/{instrument}")
async def backfill_option_chain_today(
    instrument: str,
    atm_range: int = Query(default=1000),
):
    """Legacy POST backfill (ATM-range only, no Greeks).  Use GET version instead."""
    ul = str(instrument or "").strip().upper()
    if not ul:
        raise HTTPException(status_code=400, detail="instrument required")

    from option_chain_kite_snapshot import backfill_today_to_db
    db = MongoData()
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, lambda: backfill_today_to_db(db, ul, atm_range=atm_range)
        )
    finally:
        db.close()
    return result


@app.get("/algo/option-chain/backfill-today/{instrument}")
async def backfill_option_chain_today_get(
    instrument: str,
    max_days_ahead: int = Query(default=0, description="Expiries up to N days ahead. 0 = all expiries."),
    workers: int = Query(default=8, description="Parallel Kite fetch workers (safe max 8)."),
    date: str = Query(default="", description="Date to backfill (YYYY-MM-DD). Defaults to today."),
    expiry: str = Query(default="", description="Exact expiry to backfill (YYYY-MM-DD)."),
    catchup: bool = Query(default=False, description="Sync only missing current-day minutes from last stored timestamp."),
):
    """
    Backfill today's full option chain (spot + India VIX + all expiries + Greeks) to DB.

    Runs in the background — returns immediately.  Track progress with:
      GET /algo/option-chain/backfill-status

    What gets written:
      • option_chain_index_spot     — spot price per minute
      • india_vix_historical        — India VIX per minute
      • option_chain_historical_data — close + IV + delta/gamma/theta/vega per strike per minute

    After this runs, bar-replay snapshot API reads from DB (24ms) instead of
    hitting Kite on-demand (~20s cold start).
    """
    ul = str(instrument or "").strip().upper()
    expiry_filter = str(expiry or "").strip()[:10]
    if not ul:
        raise HTTPException(status_code=400, detail="instrument required")
    if expiry_filter and len(expiry_filter) != 10:
        raise HTTPException(status_code=400, detail="expiry must be YYYY-MM-DD")

    from option_chain_backfill import start_backfill
    return start_backfill(
        ul,
        date_str=date.strip() or None,
        max_days_ahead=max_days_ahead,
        workers=workers,
        expiry_filter=expiry_filter or None,
        sync_from_last=bool(catchup),
    )


@app.get("/algo/option-chain/backfill-status")
async def get_backfill_status():
    """Check progress of a running or completed backfill."""
    from option_chain_backfill import get_backfill_status
    return get_backfill_status()


@app.get("/algo/option-chain/backfill-stop")
async def stop_option_chain_backfill():
    """Request the running option-chain backfill thread to stop."""
    from option_chain_backfill import stop_backfill
    return stop_backfill()


_INDEX_KITE_SYMBOLS: dict[str, str] = {
    "NIFTY":      "NSE:NIFTY 50",
    "BANKNIFTY":  "NSE:NIFTY BANK",
    "FINNIFTY":   "NSE:NIFTY FIN SERVICE",
    "SENSEX":     "BSE:SENSEX",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
}


def _get_kite_rest_client():
    """Return a configured broker REST client using DB credentials, or None."""
    try:
        from features.broker_gateway import get_broker_rest_client  # type: ignore
        return get_broker_rest_client()
    except Exception:
        return None


# NSE option chain in-process cache — keyed by "SYMBOL:YYYY-MM-DD"
_nse_chain_cache: dict[str, tuple[float, dict]] = {}
_nse_chain_cache_lock = threading.Lock()
_NSE_CHAIN_CACHE_TTL = 60.0  # seconds

# India VIX NSE-API fallback cache — see get_live_greeks_chain's VIX section.
_india_vix_cache: dict[str, tuple[float, float]] = {}
_INDIA_VIX_CACHE_TTL = 60.0  # seconds


def _resolve_chain_reference_spot(
    rows_by_side: dict[str, dict[float, dict]],
    spot_price: float,
    T: float,
    r: float,
    q: float,
) -> float:
    """
    Convert the ATM synthetic future into a spot-equivalent reference price.

    When spot_price is 0 (equity spot fetch failed), estimates spot from
    put-call parity by finding the strike where |CE_ltp - PE_ltp| is minimum.
    """
    ce_by_strike = rows_by_side.get("CE") or {}
    pe_by_strike = rows_by_side.get("PE") or {}
    common_strikes = [
        strike
        for strike in set(ce_by_strike) & set(pe_by_strike)
        if float((ce_by_strike.get(strike) or {}).get("ltp") or 0) > 0
        and float((pe_by_strike.get(strike) or {}).get("ltp") or 0) > 0
    ]
    if not common_strikes:
        return spot_price

    if spot_price > 0:
        atm_strike = min(common_strikes, key=lambda strike: abs(strike - spot_price))
    else:
        # Estimate ATM via put-call parity: strike where |CE - PE| is minimized
        atm_strike = min(
            common_strikes,
            key=lambda strike: abs(
                float((ce_by_strike.get(strike) or {}).get("ltp") or 0)
                - float((pe_by_strike.get(strike) or {}).get("ltp") or 0)
            ),
        )

    ce_ltp = float((ce_by_strike.get(atm_strike) or {}).get("ltp") or 0)
    pe_ltp = float((pe_by_strike.get(atm_strike) or {}).get("ltp") or 0)
    synthetic_future = atm_strike + ce_ltp - pe_ltp
    if synthetic_future <= 0:
        return spot_price

    # Convert forward/synthetic reference back to a BSM-compatible spot input.
    return synthetic_future * math.exp(-(r - q) * max(T, 0.0))


def _fetch_nse_chain_data(symbol: str, expiry_iso: str) -> dict:
    """
    Fetch LTP + OI + spot from NSE option chain for a symbol + expiry.
    Returns {"spot": float, "chain": {"24500_CE": {"ltp": 22.3, "oi": 131000}, ...}}
    Results are cached for 60 seconds to avoid repeated slow HTTP calls.
    """
    import requests as _req
    from datetime import datetime as _dt

    cache_key = f"{symbol.upper()}:{expiry_iso[:10]}"
    _now = time.monotonic()
    with _nse_chain_cache_lock:
        _hit = _nse_chain_cache.get(cache_key)
        if _hit and (_now - _hit[0]) < _NSE_CHAIN_CACHE_TTL:
            return _hit[1]

    try:
        expiry_dt = _dt.strptime(expiry_iso[:10], "%Y-%m-%d")
        _day = expiry_dt.strftime("%d").lstrip("0")
        _mon = expiry_dt.strftime("%b")
        _yr  = expiry_dt.strftime("%Y")
        expiry_nse_dash  = f"{_day}-{_mon}-{_yr}"   # "23-Jun-2026"
        expiry_nse_space = f"{_day} {_mon} {_yr}"   # "23 Jun 2026"
    except Exception:
        expiry_nse_dash = expiry_nse_space = ""

    _INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    is_index = symbol.upper() in _INDICES
    url = (
        f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol.upper()}"
        if is_index
        else f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol.upper()}"
    )

    empty: dict = {"spot": 0.0, "chain": {}}
    try:
        sess = _req.Session()
        sess.get("https://www.nseindia.com", timeout=5,
                 headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
        r = sess.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.nseindia.com",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if r.status_code != 200:
            log.warning("[NSE CHAIN] %s HTTP %s", symbol, r.status_code)
            return empty
        records = r.json().get("records") or {}
        data_rows = records.get("data") or []
        spot = float(records.get("underlyingValue") or 0)
        chain: dict[str, dict] = {}
        for row in data_rows:
            _row_expiry = str(row.get("expiryDate") or "").strip()
            if expiry_nse_dash and _row_expiry not in (expiry_nse_dash, expiry_nse_space):
                continue
            strike = row.get("strikePrice")
            if strike is None:
                continue
            strike_int = int(float(strike))
            if not spot:
                spot = float(row.get("CE", {}).get("underlyingValue") or row.get("PE", {}).get("underlyingValue") or 0)
            for opt_type in ("CE", "PE"):
                opt_data = row.get(opt_type) or {}
                chain[f"{strike_int}_{opt_type}"] = {
                    "ltp": float(opt_data.get("lastPrice") or 0),
                    "oi":  int(opt_data.get("openInterest") or 0),
                }
        result = {"spot": spot, "chain": chain}
        if chain:
            with _nse_chain_cache_lock:
                _nse_chain_cache[cache_key] = (time.monotonic(), result)
        return result
    except Exception as _e:
        log.warning("[NSE CHAIN] %s error: %s", symbol, _e)
        return empty


def _fetch_nse_oi_map(symbol: str, expiry_iso: str) -> dict[str, int]:
    """Backward-compat wrapper — returns only OI map."""
    return {k: v["oi"] for k, v in _fetch_nse_chain_data(symbol, expiry_iso).get("chain", {}).items()}


# f"{segment}:{sec_id}" → last-seen-good market-data dict. Never evicted —
# see the resilience note in _fetch_dhan_market_data()'s docstring below.
_DHAN_MARKET_DATA_LAST_GOOD: dict[str, dict] = {}


def _fetch_dhan_market_data(segment: str, sec_ids: list[int], db) -> dict[str, dict]:
    """
    Fetch LTP + OI + best bid/ask from Dhan /marketfeed/quote for a list of security IDs.
    Returns {str(sec_id): {"ltp": float, "oi": int, "bid": float, "ask": float, "prev_close": float}}.
    Dhan /quote supports up to 1000 per segment — send as few requests as possible.

    WS-first + last-good fallback, same resilience as
    features.broker_gateway.get_broker_rest_quotes: Dhan's REST quote
    endpoint rate-limits to ~1 req/sec per account, and this function used
    to retry a 429 with a blocking time.sleep(1s/2s/3s) per batch — on
    /live-greeks-chain, which calls this 2+ times sequentially (equity
    spot, then the whole NSE_FNO/BSE_FNO chain), that alone could add
    several seconds to one page load. A WS ltp_map hit resolves a sec_id
    with zero REST round trip; a 429/failed REST attempt now falls straight
    back to the last real value seen for that sec_id instead of blocking
    to retry.
    """
    if not sec_ids:
        return {}
    raw_db = db._db if hasattr(db, "_db") else db
    cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    access_token = str(cfg.get("access_token") or "").strip()
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    if not access_token or not client_id:
        return {}

    result: dict[str, dict] = {}

    # WS ltp_map/oi_map are keyed by bare numeric security id regardless of
    # segment (index/equity/FNO ticks all land there — see dhan_ticker.py's
    # binary parser), so a hit here is an in-memory read, no REST call at all.
    try:
        from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore
        for sid in sec_ids:
            sid_str = str(sid)
            ws_ltp = float(_dtm.ltp_map.get(sid_str) or 0)
            if ws_ltp > 0:
                cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}") or {}
                ws_bid = float(_dtm.bid_map.get(sid_str) or 0)
                ws_ask = float(_dtm.ask_map.get(sid_str) or 0)
                ws_bid_qty = int(_dtm.bid_qty_map.get(sid_str) or 0)
                ws_ask_qty = int(_dtm.ask_qty_map.get(sid_str) or 0)
                result[sid_str] = {
                    "ltp": ws_ltp,
                    "oi": int(_dtm.oi_map.get(sid_str) or cached.get("oi", 0)),
                    "bid": ws_bid or cached.get("bid", 0.0),
                    "ask": ws_ask or cached.get("ask", 0.0),
                    # Level-0 qty only — see algo.simulator/api.py's _fetch_dhan_market_data
                    # for why the walk still falls back to REST depth when this isn't enough.
                    "bid_depth": [{"price": ws_bid, "quantity": ws_bid_qty}] if ws_bid > 0 else cached.get("bid_depth", []),
                    "ask_depth": [{"price": ws_ask, "quantity": ws_ask_qty}] if ws_ask > 0 else cached.get("ask_depth", []),
                    "prev_close": cached.get("prev_close", 0.0),
                }
    except Exception:
        pass

    missing = [sid for sid in sec_ids if str(sid) not in result]
    if missing:
        from features.broker_gateway import dhan_quote_post_blocking

        _BATCH = 500  # Dhan /quote supports up to 1000 per segment
        batches = [missing[i: i + _BATCH] for i in range(0, len(missing), _BATCH)]

        for batch in batches:
            for _attempt in range(3):
                try:
                    # Blocking, not skip-on-busy: this is usually called right
                    # after a spot-price quote on the same rate gate (e.g.
                    # get_live_greeks_chain fetches index spot, then the whole
                    # chain, microseconds apart) — skip-on-busy made the second
                    # call lose that race almost every time, rendering the
                    # whole chain as ltp=0. See dhan_quote_post_blocking's docstring.
                    r = dhan_quote_post_blocking({segment: batch}, access_token, client_id, timeout=15.0)
                    if r is None:
                        continue
                    if r.status_code == 200:
                        raw = r.json()
                        data = (raw.get("data") or raw).get(segment) or {}
                        for sid, info in data.items():
                            if not isinstance(info, dict):
                                continue
                            depth = info.get("depth") or {}
                            buy_levels = depth.get("buy") or []
                            sell_levels = depth.get("sell") or []
                            entry = {
                                "ltp": float(info.get("last_price") or 0),
                                "oi":  int(info.get("oi") or 0),
                                # Best bid/ask (level 0) — 0 if that side of the book is empty.
                                "bid": float((buy_levels[0] or {}).get("price") or 0) if buy_levels else 0.0,
                                "ask": float((sell_levels[0] or {}).get("price") or 0) if sell_levels else 0.0,
                                # Full depth (all levels Dhan returns, best→worst) — lets
                                # _resolve_mpp_price walk past level 0 for an order qty bigger
                                # than the top level alone holds instead of pricing as if it
                                # were the whole book.
                                "bid_depth": [
                                    {"price": float(lvl.get("price") or 0), "quantity": int(lvl.get("quantity") or 0)}
                                    for lvl in buy_levels if float(lvl.get("price") or 0) > 0
                                ],
                                "ask_depth": [
                                    {"price": float(lvl.get("price") or 0), "quantity": int(lvl.get("quantity") or 0)}
                                    for lvl in sell_levels if float(lvl.get("price") or 0) > 0
                                ],
                                # Previous trading day's close — Dhan's own quote response
                                # already carries this in ohlc.close, additive field so
                                # nothing keying off just ['ltp']/['oi'] etc. is affected.
                                "prev_close": float((info.get("ohlc") or {}).get("close") or 0),
                            }
                            result[str(sid)] = entry
                            if entry["ltp"] > 0:
                                _DHAN_MARKET_DATA_LAST_GOOD[f"{segment}:{sid}"] = entry
                        break
                    else:
                        # Most commonly a 429 — retry a couple times (spaced
                        # by the shared gate) before giving up to the
                        # last-good backfill below.
                        log.warning("[DHAN QUOTE] segment=%s status=%d attempt=%d body=%s",
                                    segment, r.status_code, _attempt, r.text[:200])
                except Exception as _e:
                    log.warning("[DHAN QUOTE] error=%s attempt=%d", _e, _attempt)

    for sid in sec_ids:
        sid_str = str(sid)
        if sid_str not in result or not result[sid_str].get("ltp"):
            cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}")
            if cached:
                result[sid_str] = cached

    return result


def _fetch_dhan_ltp(segment: str, sec_ids: list[int], db) -> dict[str, float]:
    """Convenience wrapper — returns {str(sec_id): ltp}."""
    return {k: v["ltp"] for k, v in _fetch_dhan_market_data(segment, sec_ids, db).items()}


# /live-greeks-chain/{instrument} moved to shared/features/live_greeks_chain_
# socket.py (WS + one-shot REST), mounted only on algo.websocket (8003) — a
# common/shared market-data endpoint must not live in a domain-specific api.
#
# /rest-option-chain/{instrument} moved to algo.scanner/api.py (2026-08-06,
# user request) — algo.trade runs the live monitors, so any new/experimental
# REST-endpoint work stays out of this process to avoid any risk to that
# critical path, even though this endpoint itself never touched monitor code.


@app.get("/refresh-option-chain-cache")
async def refresh_option_chain_cache():
    cache = _refresh_active_option_chain_cache()
    return {
        "status": "ok",
        "instruments": sorted(cache.keys()),
        "instrument_count": len(cache),
    }


@app.get("/mock/start")
async def mock_start(time: str = Query(default="")):
    """
    Start mock ticker.
    Pass ?time=2025-11-03T09:15:00 to set the simulation start time.
    If time is omitted, the last saved mock time is resumed.
    Returns HTML control page.
    """
    resume_time = (time or mock_ticker_manager.mock_current_time or "").strip()
    if mock_ticker_manager.status not in ("running", "connecting"):
        if resume_time:
            import threading
            threading.Thread(target=_start_mock_bg, args=(resume_time,), daemon=True).start()
    trade_date = resume_time[:10] if "T" in resume_time else ""
    live_monitor_loop.start(
        trade_date=trade_date,
        activation_mode="fast-forward",
    )
    return HTMLResponse(content=_MOCK_CONTROL_HTML)


def _sync_all_dhan_index_and_commodity_tokens() -> dict:
    """
    Indices (options + futures) + MCX commodities + equity F&O stocks (all
    non-index underlyings, "FNO-STOCKS") — literally everything
    active_option_tokens holds, all in one call. Shared by
    GET /algo/get_active_tokens/ALL and the background /algo/sync-tokens/start/ALL
    path (_run_bg_sync) so both entry points that claim to "sync everything"
    actually do the same thing — previously only this synchronous route had
    the ALL branch (and it excluded stocks); the background one fell through
    to the equity/Kite path and silently synced nothing for "ALL".

    FNO-STOCKS syncs first and works under either broker (Dhan CSV or Kite
    REST, branches internally) — it's always run. Indices/commodities only
    have a Dhan path today, so they're skipped (not errored) when Dhan isn't
    the active broker, rather than failing the whole "ALL" call.
    """
    from features.broker_gateway import _active_broker as _get_active_broker_for_sync

    stock_result = _sync_active_option_tokens("FNO-STOCKS")

    if _get_active_broker_for_sync() != "dhan":
        return {
            "status": "partial",
            "message": "Active broker is not dhan — synced equity F&O stocks only; indices/commodities need Dhan.",
            "stocks": stock_result,
        }

    _INDEX_SET = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    index_results = {idx: _sync_dhan_index_option_tokens(idx) for idx in sorted(_INDEX_SET)}
    index_future_results = {idx: _sync_dhan_index_future_tokens(idx) for idx in sorted(_INDEX_SET)}
    commodity_master = _get_dhan_commodity_master()
    commodity_results = {sym: _sync_dhan_commodity_tokens(sym) for sym in sorted(commodity_master.keys())}
    all_results = (
        list(index_results.values()) + list(index_future_results.values())
        + list(commodity_results.values()) + [stock_result]
    )
    return {
        "status": "success",
        "broker": "dhan",
        "indices": index_results,
        "index_futures": index_future_results,
        "commodities": commodity_results,
        "stocks": stock_result,
        "totals": {
            "contracts_processed": sum(r.get("contracts_processed", 0) for r in all_results),
            "created": sum(r.get("created", 0) for r in all_results),
            "updated": sum(r.get("updated", 0) for r in all_results),
            "removed": sum(r.get("removed", 0) for r in all_results),
        },
    }


@app.get("/algo/get_active_tokens/{instrument}")
async def get_active_tokens(instrument: str):
    normalized = str(instrument or "").strip().upper()
    _INDEX_SET = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}

    if normalized == "ALL":
        return _sync_all_dhan_index_and_commodity_tokens()

    if normalized in _get_dhan_commodity_master():
        from features.broker_gateway import _active_broker as _get_active_broker_for_sync
        if _get_active_broker_for_sync() == "dhan":
            return _sync_dhan_commodity_tokens(normalized)

    if normalized in _INDEX_SET:
        from features.broker_gateway import _active_broker as _get_active_broker_for_sync
        if _get_active_broker_for_sync() == "dhan":
            option_result = _sync_dhan_index_option_tokens(normalized)
            future_result = _sync_dhan_index_future_tokens(normalized)
            return {
                "status": "success",
                "instrument": normalized,
                "options": option_result,
                "futures": future_result,
                "contracts_processed": option_result.get("contracts_processed", 0) + future_result.get("contracts_processed", 0),
                "created": option_result.get("created", 0) + future_result.get("created", 0),
                "updated": option_result.get("updated", 0) + future_result.get("updated", 0),
            }
    return _sync_active_option_tokens(instrument)


# ── Background sync state ─────────────────────────────────────────────────────
_bg_sync_state: dict = {
    "running": False,
    "instrument": "",
    "started_at": "",
    "finished_at": "",
    "result": None,
    "error": "",
}
_bg_sync_thread: threading.Thread | None = None


def _run_bg_sync(instrument: str) -> None:
    global _bg_sync_state
    _bg_sync_state["running"] = True
    _bg_sync_state["instrument"] = instrument
    _bg_sync_state["started_at"] = to_utc_iso(datetime.now(timezone.utc))
    _bg_sync_state["finished_at"] = ""
    _bg_sync_state["result"] = None
    _bg_sync_state["error"] = ""
    try:
        if str(instrument or "").strip().upper() == "ALL":
            result = _sync_all_dhan_index_and_commodity_tokens()
        else:
            result = _sync_active_option_tokens(instrument)
        _bg_sync_state["result"] = result
    except Exception as exc:
        _bg_sync_state["error"] = str(exc)
    finally:
        _bg_sync_state["running"] = False
        _bg_sync_state["finished_at"] = to_utc_iso(datetime.now(timezone.utc))


@app.get("/algo/sync-tokens/start/{instrument}")
async def bg_sync_start(instrument: str):
    """Start a background sync of active_option_tokens. Returns immediately."""
    global _bg_sync_thread
    if _bg_sync_state["running"]:
        return {
            "status": "already_running",
            "instrument": _bg_sync_state["instrument"],
            "started_at": _bg_sync_state["started_at"],
            "message": "Sync already in progress. Check /algo/sync-tokens/status",
        }
    _bg_sync_thread = threading.Thread(
        target=_run_bg_sync, args=(instrument,), daemon=True
    )
    _bg_sync_thread.start()
    return {
        "status": "started",
        "instrument": instrument.upper(),
        "message": "Sync running in background. Check /algo/sync-tokens/status",
        "status_url": "/algo/sync-tokens/status",
        "stop_url": "/algo/sync-tokens/stop",
    }


@app.get("/algo/sync-tokens/status")
async def bg_sync_status():
    """Check the status of the background active_option_tokens sync."""
    state = dict(_bg_sync_state)
    if state["running"]:
        status = "running"
    elif state["error"]:
        status = "error"
    elif state["result"] is not None:
        status = "completed"
    else:
        status = "idle"
    return {"status": status, **state}


@app.get("/algo/sync-tokens/stop")
async def bg_sync_stop():
    """Signal the background sync to stop (marks as not running; thread finishes current batch)."""
    global _bg_sync_state
    if not _bg_sync_state["running"]:
        return {"status": "not_running", "message": "No sync is currently running."}
    _bg_sync_state["running"] = False
    _bg_sync_state["error"] = "Stopped by user"
    _bg_sync_state["finished_at"] = to_utc_iso(datetime.now(timezone.utc))
    return {"status": "stop_requested", "message": "Stop signal sent. Thread will finish current batch."}


@app.get("/mock/stop")
async def mock_stop():
    """Stop mock ticker. Returns HTML control page."""
    mock_ticker_manager.stop()
    live_monitor_loop.stop()
    return HTMLResponse(content=_MOCK_CONTROL_HTML)


@app.get("/mock/status")
async def mock_status():
    """JSON status — polled by the control page every 2 s."""
    return mock_ticker_manager.get_status()


@app.get("/mock/ltp/{token}")
async def mock_ltp(token: str):
    ltp = mock_ticker_manager.get_ltp(token)
    if ltp is None:
        raise HTTPException(status_code=404, detail=f"No mock LTP for token {token}")
    return {"token": token, "ltp": ltp}


_TRADE_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "algoreq" / "trade-data"


def _read_trade_static_json(filename: str):
    path = _TRADE_DATA_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    import json as _json_mod
    return _json_mod.loads(path.read_text(encoding="utf-8"))


@app.get("/algo/trade-static/algoentry")
async def get_algoentry_data():
    """Return the current algoentry_data.json (legs + trade metadata)."""
    return _read_trade_static_json("algoentry_data.json")


@app.get("/algo/trade-static/scanner-symbol")
async def get_scanner_symbol_data():
    """Return the current scannersymbolinvest.json (per-token OHLCV including NSE_0 spot)."""
    return _read_trade_static_json("scannersymbolinvest.json")


@app.get("/algo/trade-static/algotest-all-position-data")
async def get_algotest_all_position_data():
    """Return the static AlgoTest multi-strategy positions payload."""
    return _read_trade_static_json("algotest-all-position-data.json")


@app.get("/algo/trade-static/algotest-all-position-data-his")
async def get_algotest_all_position_data_his():
    """Return the static AlgoTest historical token series payload."""
    return _read_trade_static_json("algotest-all-position-data-his.json")


@app.get("/algo/system/status")
@app.get("/system/status")
async def system_status():
    """Check live order status and system configuration."""
    from features.live_order_manager import _is_live_order_punch_enabled
    live_order_enabled = _is_live_order_punch_enabled()
    return {
        "live_order_status": live_order_enabled,
        "live_order_status_label": "REAL ORDERS" if live_order_enabled else "SIMULATED ORDERS",
        "env_LIVE_ORDER_STATUS": os.getenv("LIVE_ORDER_STATUS", "not set"),
    }


# ─── Data Migration ───────────────────────────────────────────────────────────

@app.get("/algo/admin/migrate-month")
async def migrate_month(month: str = Query(..., description="YYYY-MM e.g. 2025-09")):
    """
    Migrate one month of option chain data from legacy `option_chain` collection
    to the two new collections:
      • option_chain_historical_data  — candle rows (no spot_price)
      • option_chain_index_spot       — one spot-price row per minute per underlying

    Safe to re-run: candles are upserted on (underlying, timestamp, expiry, strike, type).
    Spot rows are upserted on (underlying, timestamp).

    GET /algo/admin/migrate-month?month=2025-09
    """
    import re as _re
    if not _re.fullmatch(r"\d{4}-\d{2}", month):
        raise HTTPException(status_code=400, detail="month must be YYYY-MM format, e.g. 2025-09")

    from pymongo import UpdateOne, ASCENDING as _ASC
    from features.mongo_data import MongoData

    db   = MongoData()
    src  = db._db["option_chain"]
    dst  = db._db["option_chain_historical_data"]
    spot = db._db["option_chain_index_spot"]

    # 'Z' suffix — matches the source collection's stamped format (see
    # live_option_chain_collector.py / crypto_live_option_chain_collector.py)
    # so this string-range query stays a true prefix match either side.
    ts_start = f"{month}-01T00:00:00Z"
    # Last day: works for any month length
    import calendar as _cal
    year_int, mon_int = int(month[:4]), int(month[5:7])
    last_day = _cal.monthrange(year_int, mon_int)[1]
    ts_end   = f"{month}-{last_day:02d}T23:59:59Z"

    # ── Ensure indexes on dst and spot ───────────────────────────────────────
    try:
        dst.create_index(
            [("underlying", _ASC), ("timestamp", _ASC), ("expiry", _ASC),
             ("strike", _ASC), ("type", _ASC)],
            name="chain_upsert_key", background=True,
        )
        spot.create_index(
            [("underlying", _ASC), ("timestamp", _ASC)],
            name="spot_upsert_key", background=True,
        )
    except Exception:
        pass

    # ── Stream source in batches ──────────────────────────────────────────────
    BATCH = 5000
    candle_ops: list = []
    spot_seen:  set  = set()
    spot_ops:   list = []

    candles_upserted = 0
    spot_upserted    = 0

    cursor = src.find(
        {"timestamp": {"$gte": ts_start, "$lte": ts_end}},
        {"_id": 0, "timestamp": 1, "underlying": 1, "expiry": 1,
         "strike": 1, "type": 1, "close": 1, "oi": 1,
         "iv": 1, "delta": 1, "gamma": 1, "theta": 1, "vega": 1,
         "rho": 1, "spot_price": 1},
    ).batch_size(BATCH)

    def _flush_candles():
        nonlocal candles_upserted
        if not candle_ops:
            return
        res = dst.bulk_write(candle_ops, ordered=False)
        candles_upserted += res.upserted_count + res.modified_count
        candle_ops.clear()

    def _flush_spot():
        nonlocal spot_upserted
        if not spot_ops:
            return
        res = spot.bulk_write(spot_ops, ordered=False)
        spot_upserted += res.upserted_count + res.modified_count
        spot_ops.clear()

    for doc in cursor:
        ts         = doc.get("timestamp") or ""
        underlying = doc.get("underlying") or ""
        expiry     = doc.get("expiry") or ""
        strike     = doc.get("strike")
        otype      = doc.get("type") or ""
        close      = doc.get("close")
        sp         = doc.get("spot_price")

        # ── Candle upsert ─────────────────────────────────────────────────
        candle_doc = {
            "timestamp":  ts,
            "underlying": underlying,
            "expiry":     expiry,
            "strike":     strike,
            "type":       otype,
            "close":      close,
            "oi":         doc.get("oi"),
            "iv":         doc.get("iv"),
            "delta":      doc.get("delta"),
            "gamma":      doc.get("gamma"),
            "theta":      doc.get("theta"),
            "vega":       doc.get("vega"),
            "rho":        doc.get("rho"),
        }
        candle_ops.append(UpdateOne(
            {"underlying": underlying, "timestamp": ts,
             "expiry": expiry, "strike": strike, "type": otype},
            {"$setOnInsert": candle_doc},
            upsert=True,
        ))

        # ── Spot upsert (one per minute per underlying) ───────────────────
        minute_key = (underlying, ts[:16])   # "2025-09-01T09:16"
        if sp is not None and minute_key not in spot_seen:
            spot_seen.add(minute_key)
            spot_ops.append(UpdateOne(
                {"underlying": underlying, "timestamp": ts},
                {"$setOnInsert": {
                    "underlying": underlying,
                    "timestamp":  ts,
                    "spot_price": float(sp),
                    "token":      "NSE_01",
                }},
                upsert=True,
            ))

        if len(candle_ops) >= BATCH:
            _flush_candles()
        if len(spot_ops) >= BATCH:
            _flush_spot()

    _flush_candles()
    _flush_spot()

    return {
        "status":            "done",
        "month":             month,
        "candles_upserted":  candles_upserted,
        "spot_upserted":     spot_upserted,
        "ts_range":          f"{ts_start} → {ts_end}",
    }


# Old algo_main.py dropped these too — served by algo.websocket instead.
_DROP_ROUTE_PREFIXES = ("/ws/live-quotes", "/live-quotes")


def _route_keys(route) -> set[tuple[str, str]]:
    path = re.sub(r"\{[^}]*\}", "{}", str(getattr(route, "path", "")))
    methods = getattr(route, "methods", None) or {"WS"}
    return {(m, path) for m in methods}


def mount_missing_routes(target_app: FastAPI) -> int:
    """Append every legacy route target_app doesn't already serve (same
    method + path, path-param names ignored). Returns how many were added."""
    existing: set[tuple[str, str]] = set()
    for r in target_app.router.routes:
        existing |= _route_keys(r)
    added = 0
    for r in app.router.routes:
        if not isinstance(r, (APIRoute, WebSocketRoute)):
            continue
        if str(r.path).startswith(_DROP_ROUTE_PREFIXES):
            continue
        keys = _route_keys(r)
        if keys & existing:
            continue
        target_app.router.routes.append(r)
        existing |= keys
        added += 1
    return added
