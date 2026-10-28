"""
routers/stock_history.py
────────────────────────
Same paths and responses as algo.scanner's historical-sync endpoints, now
served by algo-2_0 (services/stock_history_sync.py). The daily 16:00 IST run
is main.py's _daily_stock_history_loop; these are the manual controls.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from services import stock_history_sync

router = APIRouter(prefix="/scanner", tags=["stock-history"])


@router.get("/sync_historical_data")
def sync_historical_data(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
) -> dict[str, Any]:
    try:
        return stock_history_sync.start_sync(start_date, end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/sync_universe_historical_data")
def sync_universe_historical_data(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
) -> dict[str, Any]:
    try:
        return stock_history_sync.start_sync(start_date, end_date, use_universe_list=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/stop_historical_data_sync")
def stop_historical_data_sync() -> dict[str, Any]:
    return stock_history_sync.request_stop()


@router.get("/historical_data_sync_status")
def historical_data_sync_status() -> dict[str, Any]:
    return stock_history_sync.get_status()
