"""
live_broadcast.py
────────────────────
Pushes TokenRouter's in-memory state out over the two client sockets
(api/routers/ws_live.py) — using the EXACT SAME serializer
(persistence/serializers.py's strategy_to_doc) that the checkpoint writer
uses for `algo_trades`. Deliberately the same function, not two parallel
shapes of the same data — see serializers.py's own module docstring for why
(user's instruction: same table/shape everywhere, nothing invented twice).

MTM/PnL are still NEVER sent as a pre-computed number — only entry_trade.price
+ live LTP (via `/algo/ws/update`'s ltp_update) go out; the unmodified
FastForward2.tsx computes PnL itself via strategyLegPnl.ts's
computeLegLivePnl, exactly like it always has against the old backend.
"""

from __future__ import annotations

import asyncio
import re
from typing import Iterable

import httpx

from api.ws_client_hub import execute_orders_hub, update_hub
from persistence.serializers import load_legacy_open_leg_tokens, strategy_to_doc
from shared.config.settings import get_settings
from shared.db.mongo import get_mongo
from shared.logging.logger import get_logger
from shared.market.ltp_cache import TickUpdate
from token_router import TokenRouter

log = get_logger(__name__)

# Currently-open legacy (old-system) leg tokens — read by broadcast_tick_update
# below (the tick hot path) as a PLAIN set lookup only, never a Mongo call.
# Same "RAM for the hot path, Mongo only off it" rule persistence/checkpoint.py
# already follows (master-doc §46/§47/§59) — refresh_legacy_open_tokens() is
# the ONLY thing allowed to write this, and it's driven by main.py's own
# periodic background task (mirroring _periodic_checkpoint_loop exactly),
# never called inline from a tick. A brand-new legacy position's ticks start
# flowing on the next refresh tick, not instantly — positions open/close far
# less often than ticks arrive, so that lag is the deliberate trade for never
# blocking tick processing on a Mongo round-trip.
_legacy_open_tokens_cache: set[str] = set()


_DELTA_UNDERLYINGS = {"BTCUSD", "ETHUSD"}
_DELTA_OPTION_RE = re.compile(r"^[CP]-(BTC|ETH)-\d+-\d+$")


def _is_delta_token(token: str) -> bool:
    """Positive match on the two real Delta token shapes only — the
    BTCUSD/ETHUSD perpetuals, and option contracts like
    "C-BTC-83000-180926". A plain `not token.isdigit()` (the earlier
    version of this check) is NOT the right test: an NSE *underlying*
    ticker like "NIFTY" (this module also collects tokens' parent
    algo_trades.ticker, not just leg tokens — see
    load_legacy_open_leg_tokens) is a name, not a numeric security_id
    either, so that check wrongly classified it as crypto and requested a
    Delta subscribe for a symbol Delta has never heard of (confirmed live:
    a real "NIFTY-p-60" position's underlying leaked into
    DeltaFeed.get_status()'s symbol list this way)."""
    return token in _DELTA_UNDERLYINGS or bool(_DELTA_OPTION_RE.match(token))


async def refresh_legacy_open_tokens() -> None:
    """Refreshes the RAM cache broadcast_tick_update reads (see module
    docstring), and — for any crypto tokens seen here for the FIRST time —
    asks algo.websocket to subscribe them on DeltaFeed's live v2/ticker
    channel (same architecture as an NSE leg's DhanFeed subscription,
    see delta_feed.py's own module docstring), since a token only ever
    ticks if that channel actually requested it. NSE tokens are left alone
    here — subscribing those is a separate, already-existing concern this
    function doesn't own."""
    global _legacy_open_tokens_cache
    previous = _legacy_open_tokens_cache
    try:
        _legacy_open_tokens_cache = await asyncio.to_thread(load_legacy_open_leg_tokens, get_mongo())
    except Exception:
        log.exception("[LiveBroadcast] refresh_legacy_open_tokens failed")
        return

    newly_open_crypto = [t for t in _legacy_open_tokens_cache - previous if _is_delta_token(t)]
    if not newly_open_crypto:
        return
    ws_base = get_settings().websocket_base_url.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{ws_base}/ticker/subscribe", json={"tokens": newly_open_crypto, "exchange": "DELTA"})
        log.info("[LiveBroadcast] requested Delta subscribe for %d newly-open token(s): %s", len(newly_open_crypto), newly_open_crypto)
    except Exception:
        log.warning("[LiveBroadcast] Delta subscribe POST failed for %s", newly_open_crypto)


async def broadcast_strategy_snapshots(router: TokenRouter, strategy_ids: Iterable[str]) -> None:
    # Grouped and sent per-owner (send_to_user), NOT a single broadcast() to
    # every connected tab — a strategy record is one user's live position
    # (entry/exit price, qty, SL/TP levels), and broadcasting it to every
    # other browser with this same page open leaked that into tabs that had
    # no business seeing it (user's explicit report — action taken on one
    # user's strategy was visibly reaching every other user's socket too).
    records_by_user: dict[str, list[dict]] = {}
    for sid in strategy_ids:
        strategy = router.strategies.get(sid)
        if strategy is None:
            continue
        legs = [router.legs[lid] for lid in strategy.leg_ids if lid in router.legs]
        lazy_watchers = [w for w in router.lazy_watchers.values() if w.strategy_id == sid]
        records_by_user.setdefault(strategy.user_id or "", []).append(strategy_to_doc(strategy, legs, lazy_watchers))
    for owner_user_id, records in records_by_user.items():
        # No real owner on this record (e.g. a stale pre-fix doc recovered
        # from before every activation carried its real user_id) — nothing
        # safe to target, so it's dropped rather than falling back to
        # broadcasting it to everyone (send_to_user's own no-op-on-empty
        # already does this; this check just avoids the wasted json.dumps).
        if not owner_user_id:
            continue
        await execute_orders_hub.send_to_user(owner_user_id, {"data": {"records": records}})


async def broadcast_tick_update(router: TokenRouter, update: TickUpdate) -> None:
    """Registered as a second (async) TickClient listener, run AFTER
    token_router.on_tick (registration order = execution order — see
    tick_client.py's `_on_raw`), so leg.current_price/status/sl/tp already
    reflect this tick by the time we read them here.

    ltp_update is filtered to tokens an OPEN position actually holds
    (algo-2_0's own token_router.token_to_legs — already in RAM, no
    lookup cost beyond the dict itself — union'd with _legacy_open_tokens_
    cache, refreshed off-tick, see module docstring) — the tick feed itself
    isn't scoped per user/strategy, so without this every subscribed
    token's tick would go out on every page load, not just the ones the
    user has open. Zero I/O in this function, same as the rest of the tick
    path."""
    ltp_items = [
        {"token": token, "ltp": price}
        for token, price in update.changed_ltp.items()
        if token in router.token_to_legs or token in _legacy_open_tokens_cache
    ]
    if ltp_items:
        await update_hub.broadcast({"type": "ltp_update", "data": {"ltp": ltp_items}})

    positions_by_user: dict[str, list[dict]] = {}
    for token in update.changed_ltp:
        leg_ids = router.token_to_legs.get(token)
        if not leg_ids:
            continue
        for leg_id in leg_ids:
            leg = router.legs.get(leg_id)
            if leg is None:
                continue
            strategy = router.strategies.get(leg.strategy_id)
            owner = strategy.user_id if strategy is not None else leg.user_id
            positions_by_user.setdefault(owner, []).append({"trade_id": leg.strategy_id, "leg_id": leg.leg_id, "current_price": leg.current_price})
    for user_id, open_positions in positions_by_user.items():
        await update_hub.send_to_user(user_id, {"type": "update", "data": {"open_positions": open_positions}})

    # Deliberately NOT calling broadcast_strategy_snapshots here on every
    # tick (it used to, unconditionally, for every strategy any changed
    # token belonged to) — the execute-orders socket is meant for discrete
    # STATE changes (new strategy, leg entry/exit/square-off — every other
    # call site above passes the one specific strategy_id that action just
    # touched), not a price feed; the `update` message just above already
    # carries current_price at full tick rate on its own channel, and the
    # frontend's own PnL math (this module's docstring above, and
    # FastForward2.tsx's refreshMtm) is computed client-side from that same
    # `update`/`ltp_update` stream, never from a re-sent record. Re-sending
    # every open strategy's full record on every tick was pure waste over
    # the wire and, per the user's own report, made it look like the whole
    # dashboard was "constantly emitting" instead of only on a real action —
    # exactly the old system's own `/fast-forward` page behavior this page
    # is supposed to match.
