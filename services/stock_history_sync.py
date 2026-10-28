"""
stock_history_sync.py
─────────────────────
Daily stock + index EOD candles into scanner_stock_historical_data /
scanner_index_historical_data — copied from algo.scanner's
/scanner/sync_historical_data (scanner/service.py's
sync_scanner_historical_data and its helpers), same collections, same doc
shape, same Kite/Dhan fetch path (common/historical_data.py), so the scanner's
scoring/backtests read these rows exactly as before.

Runs by itself every trading day at RUN_AT_IST for that day only (main.py's
_daily_stock_history_loop), and on demand via api/routers/stock_history.py.
16:00, not the scanner's 15:50 snapshot time: that snapshot upserts today's
rows by (h_symbol_token, ch_timestamp); running after it means this sync
finds them and skips, never writing a second row for the same day. Rows
written here also carry h_symbol_token, so a later snapshot updates them in
place instead of duplicating.
"""

from __future__ import annotations

import threading
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any

import pandas as pd

from common.historical_data import (
    DHAN_HISTORICAL_CHUNK_DAYS,
    DHAN_INTRADAY_CHUNK_DAYS,
    HISTORICAL_CHUNK_DAYS,
    fetch_dhan_daily_candles as _fetch_dhan_daily_candles,
    fetch_dhan_daily_index_candles_cached as _fetch_dhan_daily_index_candles_cached,
    fetch_kite_daily_candles as _fetch_kite_daily_candles,
)
from features.mongo_data import MongoData
from features.spot_atm_utils import KITE_INDEX_TOKENS
from shared.logging.logger import get_logger

log = get_logger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
RUN_AT_IST = dtime(16, 0)

STOCKS_COLLECTION = "scanner_stocks_list"
HISTORY_COLLECTION = "scanner_stock_historical_data"
INDEX_HISTORY_COLLECTION = "scanner_index_historical_data"
INDEX_STOCKS_COLLECTION = "scanner_index_stocks"
MARKET_HOLIDAYS_COLLECTION = "market_holidays"
UNIVERSE_STOCK_LIST_COLLECTION = "scanner_universe_stock_list"
HISTORICAL_LOOKBACK_BUFFER_DAYS = 10

DEFAULT_SCANNER_INDEXES: list[dict[str, Any]] = [
    {"lookup_symbol": "GOLDBEES", "raw_symbol": "GOLDBEES-EQ", "normalized_symbol": "gold_bees", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY", "raw_symbol": "NIFTY50", "normalized_symbol": "nifty_50", "fetch_mode": "token"},
    {"lookup_symbol": "NIFTY500", "raw_symbol": "NIFTY 500", "normalized_symbol": "nifty_500", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYMIDCAP100", "raw_symbol": "NIFTY MIDCAP 100", "normalized_symbol": "nifty_midcap_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYMIDCAP50", "raw_symbol": "NIFTY MIDCAP 50", "normalized_symbol": "nifty_midcap_50", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY100", "raw_symbol": "NIFTY 100", "normalized_symbol": "nifty_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYNXT50", "raw_symbol": "NIFTY NEXT 50", "normalized_symbol": "nifty_next_50", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYSMLCAP100", "raw_symbol": "NIFTY SMLCAP 100", "normalized_symbol": "nifty_smallcap_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY200", "raw_symbol": "NIFTY 200", "normalized_symbol": "nifty_200", "fetch_mode": "nse"},
    {"lookup_symbol": "BANKNIFTY", "raw_symbol": "BANKNIFTY", "normalized_symbol": "nifty_bank", "fetch_mode": "token"},
    {"lookup_symbol": "FINNIFTY", "raw_symbol": "FINNIFTY", "normalized_symbol": "nifty_fin_service", "fetch_mode": "token"},
    {"lookup_symbol": "MIDCPNIFTY", "raw_symbol": "MIDCPNIFTY", "normalized_symbol": "nifty_mid_select", "fetch_mode": "token"},
    {"lookup_symbol": "SENSEX", "raw_symbol": "SENSEX", "normalized_symbol": "sensex", "fetch_mode": "token"},
    {"lookup_symbol": "INDIA_VIX", "raw_symbol": "INDIA VIX", "normalized_symbol": "india_vix", "fetch_mode": "token"},
]

_sync_stop_requested = False
_sync_running = False
_sync_thread: threading.Thread | None = None
_sync_last_result: dict[str, Any] | None = None
_sync_lock = threading.Lock()


# ── helpers (same as algo.scanner/scanner/service.py) ─────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.strip().replace(",", "").replace("%", "")
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    if numeric != numeric or numeric in (float("inf"), float("-inf")):
        return default
    return numeric


def _to_ymd(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    try:
        return pd.to_datetime(value, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return str(value or "")[:10]


def _parse_ymd_date(value: Any, *, field_name: str) -> datetime:
    raw = str(value or "").strip()
    try:
        return datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}. Expected YYYY-MM-DD.") from exc


def _get_active_broker(db) -> str:
    try:
        cfg = db["kite_market_config"].find_one({"enabled": True}, {"broker": 1}) or {}
        return str(cfg.get("broker") or "kite").strip().lower()
    except Exception:
        return "kite"


def _load_dhan_access_token(db) -> str:
    cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    return str(cfg.get("access_token") or "").strip()


def _build_kite_client_from_config(db):
    from kiteconnect import KiteConnect  # type: ignore

    doc = db["kite_market_config"].find_one({"broker": "kite"}, {"api_key": 1, "access_token": 1}) or {}
    api_key = str(doc.get("api_key") or "").strip()
    access_token = str(doc.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("Kite access token not configured.")
    if not api_key:
        raise ValueError("Kite api key not configured.")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _load_market_holiday_dates(db) -> set[str]:
    return {
        str(row.get("date") or "").strip()
        for row in db[MARKET_HOLIDAYS_COLLECTION].find({}, {"_id": 0, "date": 1})
        if str(row.get("date") or "").strip()
    }


def _resolve_stock_dhan_security_id(stock: dict[str, Any]) -> tuple[str, str]:
    symbol = str(stock.get("symbol") or stock.get("tradingsymbol") or "").strip().upper()
    raw = str(stock.get("dhan_security_id") or "").strip()
    try:
        nse_id = str(int(float(raw))) if raw else ""
    except (ValueError, TypeError):
        nse_id = raw
    return symbol, nse_id


def _resolve_stock_kite_token(stock: dict[str, Any]) -> tuple[str, str]:
    symbol = str(stock.get("symbol") or stock.get("tradingsymbol") or "").strip().upper()
    raw_token = str(
        stock.get("kite_token") or stock.get("token") or stock.get("tokens")
        or stock.get("instrument_token") or stock.get("exchange_token") or stock.get("code") or ""
    ).strip()
    try:
        token = str(int(float(raw_token))) if raw_token else ""
    except (ValueError, TypeError):
        token = raw_token
    return symbol, token


def _resolve_kite_access_token_error(exc: Exception) -> str | None:
    message = str(exc or "").strip().lower()
    if not message:
        return None
    if "expired" in message:
        return "Expired Kite access token."
    if any(t in message for t in (
        "tokenexception", "invalid api_key or access_token", "invalid access token",
        "incorrect `api_key` or `access_token`", "access_token", "token is invalid",
    )):
        return "Invalid Kite access token."
    return None


def _load_kite_nse_instrument_map(kite) -> dict[str, dict[str, Any]]:
    instrument_map: dict[str, dict[str, Any]] = {}
    try:
        for inst in kite.instruments("NSE") or []:
            tradingsymbol = str(inst.get("tradingsymbol") or "").strip().upper()
            if tradingsymbol:
                instrument_map[tradingsymbol] = inst
    except Exception:
        return {}
    return instrument_map


def _resolve_nse_index_instrument(nse_instrument_map, index_key: str, raw_symbol: str, normalized_symbol: str) -> dict[str, Any]:
    candidates = [
        str(raw_symbol or "").strip().upper(),
        str(index_key or "").strip().upper(),
        str(normalized_symbol or "").strip().upper().replace("_", " "),
        str(index_key or "").strip().upper().replace("MIDCAP", " MIDCAP ").replace("SMLCAP", " SMLCAP "),
    ]
    default_index = next(
        (i for i in DEFAULT_SCANNER_INDEXES if str(i.get("lookup_symbol") or "").strip().upper() == str(index_key or "").strip().upper()),
        None,
    )
    if default_index:
        candidates.append(str(default_index.get("raw_symbol") or "").strip().upper())
    seen: set[str] = set()
    for candidate in candidates:
        cleaned = " ".join(str(candidate or "").split())
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        inst = nse_instrument_map.get(cleaned)
        if inst:
            return inst
    return {}


def _load_scanner_index_master(db, nse_instrument_map) -> dict[str, dict[str, Any]]:
    index_rows: dict[str, dict[str, Any]] = {}
    rows = db[INDEX_STOCKS_COLLECTION].find({}, {
        "_id": 0, "filter_symbol": 1, "name": 1, "symbol": 1, "tradingsymbol": 1,
        "normalized_symbol": 1, "fetch_mode": 1, "dhan_security_id": 1, "dhan_segment": 1,
    })
    for row in rows:
        index_key = str(row.get("symbol") or "").strip().upper()
        normalized_symbol = str(row.get("normalized_symbol") or row.get("filter_symbol") or "").strip().lower()
        if not index_key or not normalized_symbol:
            continue
        raw_symbol = (
            str(row.get("tradingsymbol") or "").strip() or str(row.get("symbol") or "").strip()
            or str(row.get("filter_symbol") or "").strip() or str(row.get("name") or "").strip() or index_key
        )
        fetch_mode = str(row.get("fetch_mode") or "").strip().lower() or ("token" if index_key in KITE_INDEX_TOKENS else "nse")
        if fetch_mode == "nse":
            inst = _resolve_nse_index_instrument(nse_instrument_map, index_key, raw_symbol, normalized_symbol)
            if inst:
                raw_symbol = str(inst.get("tradingsymbol") or raw_symbol).strip()
        index_rows[index_key] = {
            "kite_key": index_key, "normalized_symbol": normalized_symbol, "raw_symbol": raw_symbol,
            "fetch_mode": fetch_mode,
            "dhan_security_id": str(row.get("dhan_security_id") or "").strip(),
            "dhan_segment": str(row.get("dhan_segment") or "").strip().upper(),
        }
    for default_index in DEFAULT_SCANNER_INDEXES:
        lookup_symbol = str(default_index["lookup_symbol"]).strip().upper()
        if lookup_symbol not in index_rows:
            index_rows[lookup_symbol] = {
                "kite_key": lookup_symbol, "normalized_symbol": str(default_index["normalized_symbol"]),
                "raw_symbol": str(default_index["raw_symbol"]), "fetch_mode": str(default_index["fetch_mode"]),
                "dhan_security_id": "", "dhan_segment": "",
            }
    return index_rows


def transform_stock_candles(symbol: str, token: str, candles: list[dict[str, Any]], valid_dates: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    previous_close: float | None = None
    for candle in candles:
        candle_date = _to_ymd(candle.get("date"))
        close_price = _safe_float(candle.get("close"), default=0.0)
        if candle_date in valid_dates:
            out.append({
                "h_symbol": symbol,
                "token": token,
                "h_symbol_token": token,  # scanner's 15:50 snapshot upsert key — see module docstring
                "ch_timestamp": candle_date,
                "ch_series": "EQ",
                "ch_opening_price": _safe_float(candle.get("open")),
                "ch_high_price": _safe_float(candle.get("high")),
                "ch_low_price": _safe_float(candle.get("low")),
                "ch_closing_price": close_price,
                "ch_volume": int(_safe_float(candle.get("volume"))),
                "ch_previous_cls_price": previous_close,
            })
        if close_price > 0:
            previous_close = close_price
    return out


def transform_index_candles(normalized_symbol: str, raw_symbol: str, candles: list[dict[str, Any]], valid_dates: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    previous_close: float | None = None
    for candle in candles:
        candle_date = _to_ymd(candle.get("date"))
        close_price = _safe_float(candle.get("close"), default=0.0)
        if candle_date in valid_dates:
            change_pct = round(((close_price - previous_close) / previous_close) * 100.0, 2) if previous_close and previous_close > 0 else 0.0
            out.append({
                "i_symbol": normalized_symbol,
                "index_symbol": raw_symbol,
                "ih_timestamp": candle_date,
                "i_open_price": _safe_float(candle.get("open")),
                "i_high_price": _safe_float(candle.get("high")),
                "i_low_price": _safe_float(candle.get("low")),
                "i_close_price": close_price,
                "i_volume": int(_safe_float(candle.get("volume"))),
                "i_previous_close": previous_close if previous_close is not None else close_price,
                "i_ch": change_pct,
            })
        if close_price > 0:
            previous_close = close_price
    return out


def trading_dates(from_dt: datetime, to_dt: datetime, holidays: set[str]) -> set[str]:
    out: set[str] = set()
    current = from_dt
    while current <= to_dt:
        date_str = current.strftime("%Y-%m-%d")
        if current.weekday() < 5 and date_str not in holidays:
            out.add(date_str)
        current += timedelta(days=1)
    return out


# ── sync (same flow as scanner's sync_scanner_historical_data) ────────────────

def sync_historical_data(start_date: str, end_date: str, use_universe_list: bool = False) -> dict[str, Any]:
    global _sync_running, _sync_stop_requested

    from_dt = _parse_ymd_date(start_date, field_name="start_date")
    to_dt = _parse_ymd_date(end_date, field_name="end_date")
    if from_dt > to_dt:
        raise ValueError("start_date cannot be after end_date.")
    if _sync_running:
        raise ValueError("Historical sync is already running.")
    _sync_running = True
    _sync_stop_requested = False

    mongo = MongoData()
    try:
        db = mongo._db
        broker = _get_active_broker(db)
        kite = None
        dhan_access_token = ""
        if broker == "dhan":
            dhan_access_token = _load_dhan_access_token(db)
            if not dhan_access_token:
                raise ValueError("Dhan access token not configured.")
        else:
            kite = _build_kite_client_from_config(db)
        auth_validated = False

        target_dates = trading_dates(from_dt, to_dt, _load_market_holiday_dates(db))
        delete_from, delete_to = from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d")
        log.info("[StockHistorySync] %s -> %s broker=%s trading_days=%d", delete_from, delete_to, broker, len(target_dates))
        if not target_dates:
            return {
                "status": "success", "message": "Selected range contains only market holidays.",
                "start_date": delete_from, "end_date": delete_to, "stocks_processed": 0, "indexes_processed": 0,
            }

        stock_fields = {
            "_id": 0, "symbol": 1, "tradingsymbol": 1, "name": 1, "series": 1,
            "kite_token": 1, "token": 1, "tokens": 1, "instrument_token": 1, "exchange_token": 1, "code": 1,
            "dhan_security_id": 1,
        }
        if use_universe_list:
            universe_symbols: set[str] = set()
            for rec in db[UNIVERSE_STOCK_LIST_COLLECTION].find({"universe_period_end_date": ""}, {"_id": 0, "stocks": 1}):
                universe_symbols.update(str(s or "").strip().upper() for s in rec.get("stocks") or [] if str(s or "").strip())
            stock_rows = list(db[STOCKS_COLLECTION].find({"symbol": {"$in": list(universe_symbols)}}, stock_fields))
        else:
            stock_rows = list(db[STOCKS_COLLECTION].find({}, stock_fields))
        nse_instrument_map = {} if broker == "dhan" else _load_kite_nse_instrument_map(kite)
        index_rows = _load_scanner_index_master(db, nse_instrument_map)

        stock_collection = db[HISTORY_COLLECTION]
        index_collection = db[INDEX_HISTORY_COLLECTION]
        existing_stock_symbols = set(stock_collection.distinct("h_symbol", {"ch_timestamp": {"$gte": delete_from, "$lte": delete_to}}))
        existing_index_symbols = set(index_collection.distinct("i_symbol", {"ih_timestamp": {"$gte": delete_from, "$lte": delete_to}}))

        stocks_processed = indexes_processed = 0
        stock_records_inserted = index_records_inserted = 0
        stocks_skipped_existing = indexes_skipped_existing = 0
        skipped: list[dict[str, Any]] = []
        stop_reason = ""
        fetches_tried = 0
        fetch_start_base = from_dt - timedelta(days=HISTORICAL_LOOKBACK_BUFFER_DAYS)

        for _, index_row in sorted(index_rows.items()):
            if _sync_stop_requested:
                stop_reason = "Stopped by user request before stock processing."
                break
            kite_key = str(index_row["kite_key"]).strip().upper()
            normalized_symbol = str(index_row["normalized_symbol"]).strip()
            if normalized_symbol in existing_index_symbols:
                indexes_skipped_existing += 1
                continue

            all_candles: list[dict[str, Any]] = []
            current_from = fetch_start_base
            if broker == "dhan":
                dhan_security_id = str(index_row.get("dhan_security_id") or "").strip()
                dhan_segment = str(index_row.get("dhan_segment") or "").strip().upper()
                is_dhan_index = dhan_segment == "IDX_I"
                if not dhan_security_id or not dhan_segment:
                    skipped.append({"symbol": kite_key, "reason": "missing_dhan_index_security_id"})
                    continue
                # Dhan's EOD endpoint rejects IDX_I (DH-905) — daily index bars
                # come from intraday instead (90-day chunks), same as the scanner.
                chunk_days = DHAN_INTRADAY_CHUNK_DAYS if is_dhan_index else DHAN_HISTORICAL_CHUNK_DAYS
                while current_from <= to_dt:
                    current_to = min(current_from + timedelta(days=chunk_days), to_dt)
                    fetches_tried += 1
                    try:
                        if is_dhan_index:
                            all_candles.extend(_fetch_dhan_daily_index_candles_cached(
                                dhan_security_id, dhan_access_token, normalized_symbol, current_from, current_to))
                        else:
                            all_candles.extend(_fetch_dhan_daily_candles(
                                dhan_access_token, dhan_security_id, dhan_segment, "EQUITY", current_from, current_to))
                        auth_validated = True
                    except Exception as exc:
                        skipped.append({"symbol": kite_key, "reason": f"dhan_error:{exc}"})
                        all_candles = []
                        break
                    current_from = current_to + timedelta(days=1)
            else:
                fetch_mode = str(index_row.get("fetch_mode") or "").strip().lower() or ("token" if kite_key in KITE_INDEX_TOKENS else "nse")
                if fetch_mode == "nse":
                    inst = _resolve_nse_index_instrument(nse_instrument_map, kite_key, str(index_row.get("raw_symbol") or ""), normalized_symbol)
                    index_token = int(str((inst or {}).get("instrument_token") or "0").strip() or 0)
                else:
                    index_token = int(KITE_INDEX_TOKENS.get(kite_key) or 0)
                if not index_token:
                    skipped.append({"symbol": kite_key, "reason": "missing_index_token"})
                    continue
                while current_from <= to_dt:
                    current_to = min(current_from + timedelta(days=HISTORICAL_CHUNK_DAYS), to_dt)
                    fetches_tried += 1
                    try:
                        all_candles.extend(_fetch_kite_daily_candles(kite, index_token, current_from, current_to))
                        auth_validated = True
                    except Exception as exc:
                        token_error = _resolve_kite_access_token_error(exc)
                        if token_error:
                            raise ValueError(token_error) from exc
                        skipped.append({"symbol": kite_key, "reason": f"kite_error:{exc}"})
                        all_candles = []
                        break
                    current_from = current_to + timedelta(days=1)

            docs = transform_index_candles(normalized_symbol, str(index_row["raw_symbol"]), all_candles, target_dates)
            if docs:
                index_collection.insert_many(docs, ordered=False)
                indexes_processed += 1
                index_records_inserted += len(docs)

        if not stop_reason:
            for row in stock_rows:
                if _sync_stop_requested:
                    stop_reason = "Stopped by user request during stock processing."
                    break
                if broker == "dhan":
                    symbol, token = _resolve_stock_dhan_security_id(row)
                else:
                    symbol, token = _resolve_stock_kite_token(row)
                if not symbol or not token.isdigit():
                    skipped.append({"symbol": symbol or "-", "reason": "missing_dhan_security_id" if broker == "dhan" else "missing_kite_token"})
                    continue
                if symbol in existing_stock_symbols:
                    stocks_skipped_existing += 1
                    continue

                all_candles = []
                current_from = fetch_start_base
                while current_from <= to_dt:
                    chunk_days = DHAN_HISTORICAL_CHUNK_DAYS if broker == "dhan" else HISTORICAL_CHUNK_DAYS
                    current_to = min(current_from + timedelta(days=chunk_days), to_dt)
                    fetches_tried += 1
                    try:
                        if broker == "dhan":
                            all_candles.extend(_fetch_dhan_daily_candles(dhan_access_token, token, "NSE_EQ", "EQUITY", current_from, current_to))
                        else:
                            all_candles.extend(_fetch_kite_daily_candles(kite, int(token), current_from, current_to))
                        auth_validated = True
                    except Exception as exc:
                        token_error = None if broker == "dhan" else _resolve_kite_access_token_error(exc)
                        if token_error:
                            raise ValueError(token_error) from exc
                        skipped.append({"symbol": symbol, "reason": f"{broker}_error:{exc}"})
                        all_candles = []
                        break
                    current_from = current_to + timedelta(days=1)

                docs = transform_stock_candles(symbol, token, all_candles, target_dates)
                if docs:
                    stock_collection.insert_many(docs, ordered=False)
                    stocks_processed += 1
                    stock_records_inserted += len(docs)

        # Only a fetch that was tried and never once succeeded means a bad
        # token — a day the 15:50 snapshot already filled makes no calls at all.
        if fetches_tried and not auth_validated:
            raise ValueError(f"Invalid {'Dhan' if broker == 'dhan' else 'Kite'} access token.")

        result = {
            "status": "success",
            "message": "Historical sync stopped by user." if stop_reason else "Historical sync completed.",
            "stopped": bool(stop_reason),
            "start_date": delete_from, "end_date": delete_to,
            "trading_days": sorted(target_dates),
            "stocks_processed": stocks_processed, "stock_records_inserted": stock_records_inserted,
            "stocks_skipped_existing": stocks_skipped_existing,
            "indexes_processed": indexes_processed, "index_records_inserted": index_records_inserted,
            "indexes_skipped_existing": indexes_skipped_existing,
            "skipped": skipped,
        }
        log.info("[StockHistorySync] done %s -> %s stocks=%d (+%d rows, %d existing) indexes=%d (+%d rows, %d existing) skipped=%d",
                 delete_from, delete_to, stocks_processed, stock_records_inserted, stocks_skipped_existing,
                 indexes_processed, index_records_inserted, indexes_skipped_existing, len(skipped))
        return result
    finally:
        _sync_running = False
        _sync_stop_requested = False
        mongo.close()


# ── background start / stop / status (same API as the scanner's) ──────────────

def _worker(start_date: str, end_date: str, use_universe_list: bool) -> None:
    global _sync_last_result, _sync_thread
    try:
        _sync_last_result = sync_historical_data(start_date, end_date, use_universe_list=use_universe_list)
    except Exception as exc:
        log.exception("[StockHistorySync] failed %s -> %s", start_date, end_date)
        _sync_last_result = {"status": "error", "message": str(exc), "start_date": start_date, "end_date": end_date}
    finally:
        _sync_thread = None


def start_sync(start_date: str, end_date: str, use_universe_list: bool = False) -> dict[str, Any]:
    global _sync_thread, _sync_last_result
    _parse_ymd_date(start_date, field_name="start_date")
    _parse_ymd_date(end_date, field_name="end_date")
    source = "universe_list" if use_universe_list else "stocks_list"
    with _sync_lock:
        if _sync_running or (_sync_thread and _sync_thread.is_alive()):
            raise ValueError("Historical sync is already running.")
        _sync_last_result = {
            "status": "started", "message": f"Historical sync started in background (source={source}).",
            "start_date": start_date, "end_date": end_date, "source": source,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        _sync_thread = threading.Thread(target=_worker, args=(start_date, end_date, use_universe_list),
                                        daemon=True, name="stock-history-sync")
        _sync_thread.start()
    return {"status": "success", "message": f"Historical sync started in background (source={source}).",
            "start_date": start_date, "end_date": end_date, "source": source, "running": True}


def request_stop() -> dict[str, Any]:
    global _sync_stop_requested
    _sync_stop_requested = True
    return {"status": "success", "message": "Stop requested. Historical sync will stop after the current symbol completes."}


def get_status() -> dict[str, Any]:
    return {"status": "success", "running": bool(_sync_running), "stop_requested": bool(_sync_stop_requested),
            "last_result": _sync_last_result}


def is_trading_day(day: datetime, db) -> bool:
    return day.weekday() < 5 and not db[MARKET_HOLIDAYS_COLLECTION].find_one({"date": day.strftime("%Y-%m-%d")})


def run_today(now: datetime | None = None) -> dict[str, Any]:
    """The daily job: today's (IST) candles only, skipped on weekends/holidays."""
    now = now or datetime.now(IST)
    day = now.strftime("%Y-%m-%d")
    mongo = MongoData()
    try:
        trading = is_trading_day(now, mongo._db)
    finally:
        mongo.close()
    if not trading:
        return {"status": "skipped", "message": f"{day} is not a trading day."}
    return sync_historical_data(day, day)
