"""
routers/live.py
─────────────────
Route-compatible with the old algo.trade's /live/{start,stop,status,ltp/{token}}
(api.py:11401-11590) — same paths, same response shape (get_status()'s
{status, tick_count, ltp_count, spot_map, started_at, error, mode, hub} and
{token, ltp} for the ltp lookup), so frontend polling code needs no changes.

/live/loop-status (the separate live-monitor MTM broadcast loop's own status)
is NOT ported yet — that's the frontend live-P&L broadcast loop, which lands
with the persistence/runtime phases, not the tick-pipeline phase this belongs to.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from shared.market.tick_client import TickClient

router = APIRouter(prefix="/live", tags=["live"])

_tick_client: TickClient | None = None


def bind_tick_client(client: TickClient) -> None:
    global _tick_client
    _tick_client = client


@router.get("/start")
async def live_start() -> dict:
    if _tick_client is None:
        raise HTTPException(status_code=503, detail="Tick client not initialized")
    _tick_client.start()
    return {"ok": True, "message": "Tick client started"}


@router.get("/stop")
async def live_stop() -> dict:
    if _tick_client is None:
        raise HTTPException(status_code=503, detail="Tick client not initialized")
    _tick_client.stop()
    return {"ok": True, "message": "Tick client stopped"}


@router.get("/status")
async def live_status() -> dict:
    if _tick_client is None:
        raise HTTPException(status_code=503, detail="Tick client not initialized")
    return _tick_client.get_status()


@router.get("/ltp/{token}")
async def live_ltp(token: str) -> dict:
    if _tick_client is None:
        raise HTTPException(status_code=503, detail="Tick client not initialized")
    ltp = _tick_client.get_ltp(token)
    if ltp is None:
        raise HTTPException(status_code=404, detail=f"No LTP for token {token}")
    return {"token": token, "ltp": ltp}
