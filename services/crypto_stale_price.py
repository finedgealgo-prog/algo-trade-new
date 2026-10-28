"""
crypto_stale_price.py
─────────────────────
Never run SL/TP/MTM on an old crypto price. An open Delta option leg whose
token has not ticked for STALE_AFTER_SECONDS gets Delta's current mark over
REST, fed through the normal tick path (token_router + page), the same as a
WS tick. main.py's _crypto_stale_price_loop calls refresh_once every
POLL_SECONDS.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Awaitable, Callable

import requests

from shared.brokers.delta import client as delta_client
from shared.logging.logger import get_logger
from shared.market.ltp_cache import LtpCache, TickUpdate

log = get_logger(__name__)

POLL_SECONDS = 5
STALE_AFTER_SECONDS = 10
_TICKER_URL = "https://api.india.delta.exchange/v2/tickers/{symbol}"


def stale_option_tokens(router, cache: LtpCache, max_age: float = STALE_AFTER_SECONDS) -> list[str]:
    return sorted({
        leg.token for leg in list(router.legs.values())
        if leg.status == "ACTIVE" and delta_client.asset_for_token(leg.token)
        and leg.token not in delta_client.UNDERLYING_TO_ASSET
        and cache.is_stale(leg.token, max_age)
    })


def fetch_delta_marks(symbols: list[str]) -> dict[str, float]:
    marks: dict[str, float] = {}
    for symbol in symbols:
        try:
            data = requests.get(_TICKER_URL.format(symbol=symbol), timeout=5).json()
            result = (data.get("result") or {}) if data.get("success") else {}
            price = float(result.get("mark_price") or 0)
            if price > 0:
                marks[symbol] = price
        except Exception:
            log.warning("[CryptoStalePrice] REST mark fetch failed for %s", symbol)
    return marks


async def refresh_once(
    router,
    cache: LtpCache,
    dispatch: Callable[[TickUpdate], Awaitable[None]],
    fetch: Callable[[list[str]], Awaitable[dict[str, float]]],
) -> dict[str, float]:
    stale = stale_option_tokens(router, cache)
    if not stale:
        return {}
    marks = await fetch(stale)
    if marks:
        log.info("[CryptoStalePrice] no tick for %ds — REST mark for %s", STALE_AFTER_SECONDS, sorted(marks))
        await dispatch(TickUpdate(changed_ltp=marks, now_ts=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))
    missing = [s for s in stale if s not in marks]
    if missing:
        log.error("[CryptoStalePrice] NO live price (WS or REST) for %s", missing)
    return marks
