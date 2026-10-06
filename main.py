"""
main.py
───────
algo-2_0's trade service entrypoint. Directory is named `algo.trade/` (dot
included) and run standalone from inside itself, matching the old repo's
per-service convention (algo.trade/, algo.websocket/, algo.simulator/ each
run their own uvicorn process from their own directory) — see the local
`shared -> ../shared` symlink, which is exactly the old convention's
`features -> ../shared/features` pattern, applied to algo-2_0's own shared
module instead of the old one.

Run (from algo-2_0/algo.trade/):
    uvicorn main:app --reload --port 8010
"""

from __future__ import annotations

import asyncio
import pathlib
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).resolve().parent.parent / ".env")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.config.settings import get_settings
from shared.logging.logger import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)

from shared.db.mongo import get_mongo
from shared.market.delta_instrument_cache import get_delta_instrument_cache
from shared.market.instrument_master import get_instrument_master
from shared.market.ltp_cache import get_ltp_cache
from shared.market.subscription_bridge import SubscriptionBridge
from shared.market.tick_client import TickClient
from api.routers import admin as admin_router
from api.routers import auth as auth_router
from api.routers import broker_config as broker_config_router
from api.routers import live as live_router
from api.routers import portfolio as portfolio_router
from api.routers import strategy as strategy_router
from api.routers import strategy_history as strategy_history_router
from api.routers import algotrade_stats as algotrade_stats_router
from api.routers import mtm as mtm_router
from api.routers import ws_live as ws_live_router
from api.routers import broker_setup as broker_setup_router
from api.routers import dhan_oauth as dhan_oauth_router
from api.routers import kite_oauth as kite_oauth_router
from api.routers import flattrade_oauth as flattrade_oauth_router
from api.routers import crypto_straddle as crypto_straddle_router
from api.routers import straddle_alert as straddle_alert_router
from api.routers import runtime_risk as runtime_risk_router
from api.live_broadcast import broadcast_tick_update, refresh_legacy_open_tokens
from orders.order_engine import OrderEngine
from persistence import recovery
from persistence.checkpoint import checkpoint_strategy, get_checkpoint_writer
from persistence.serializers import broker_to_doc, strategy_to_doc
from risk import overall_risk
from services import straddle_alert
from services import broker_scope, debug_scheduled_entry, gap_recovery, leg_followup, range_breakout_service, scheduled_entry, strategy_activation, strategy_finalize
from shared.brokers.virtual.adapter import VirtualBrokerAdapter
from token_router import TokenRouter

app = FastAPI(title="algo-trade-2_0")

# Dev-only, permissive CORS — algo-admin (a browser frontend, different
# origin/port) needs to call this API directly for the "Virtual Trade" test
# page. Tighten this (explicit origin allowlist) before any real deployment;
# wide open is fine for a local-only dev service with no real money at risk
# (VirtualBrokerAdapter, see orders/order_engine.py).
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

app.include_router(auth_router.router)
app.include_router(live_router.router)
app.include_router(strategy_router.router)
app.include_router(broker_config_router.router)
app.include_router(broker_config_router.plural_router)
app.include_router(portfolio_router.router)
app.include_router(admin_router.router)
app.include_router(strategy_history_router.router)
app.include_router(algotrade_stats_router.router)
app.include_router(mtm_router.router)
app.include_router(ws_live_router.router)
app.include_router(broker_setup_router.router)
app.include_router(dhan_oauth_router.router)
app.include_router(kite_oauth_router.router)
app.include_router(flattrade_oauth_router.router)
app.include_router(crypto_straddle_router.router)
app.include_router(straddle_alert_router.router)
app.include_router(straddle_alert_router.monitor_router)
app.include_router(runtime_risk_router.router)

settings = get_settings()
tick_client = TickClient(settings.websocket_base_url, get_ltp_cache())
subscription_bridge = SubscriptionBridge(settings.websocket_base_url)
live_router.bind_tick_client(tick_client)

token_router = TokenRouter()
tick_client.add_listener(token_router.on_tick)


async def _ws_broadcast_listener(update) -> None:
    """Registered AFTER token_router.on_tick (registration order = execution
    order, see tick_client.py's `_on_raw`) — pushes the post-tick state
    (LTP + touched-strategy record snapshots) out over `/ws/update` and
    `/ws/execute-orders` to any connected `/fast-forward_2` browser clients.
    This is the ONLY live-data path for that page — no REST polling
    (user's explicit ask)."""
    await broadcast_tick_update(token_router, update)
    if token_router.broker_risk_state_changed:
        changed = list(token_router.broker_risk_state_changed)
        token_router.broker_risk_state_changed.clear()
        for broker_id in changed:
            broker = token_router.brokers.get(broker_id)
            if broker is not None:
                broker_scope.schedule_publish_lock_state(broker)
    if token_router.runtime_risk_changed:
        changed = list(token_router.runtime_risk_changed)
        token_router.runtime_risk_changed.clear()
        for key in changed:
            rr = token_router.runtime_risks.get(key)
            if rr is not None:
                _schedule_runtime_risk_publish(rr)


_runtime_risk_tasks: set = set()


async def publish_runtime_risk(rr) -> None:
    """Persist one strategy/group risk setting's state and push it to the
    owner's open FastForward2 page ("runtime-risk" socket message)."""
    from api.ws_client_hub import execute_orders_hub
    from risk import runtime_risk

    try:
        await asyncio.to_thread(runtime_risk.save, get_mongo(), rr)
        if rr.user_id:
            await execute_orders_hub.send_to_user(rr.user_id, {
                "type": "runtime-risk", "message": "Runtime risk updated",
                "data": {"risks": [runtime_risk.to_doc(rr)]},
            })
    except Exception:
        log.exception("[RuntimeRisk] publish failed key=%s", rr.key)


def _schedule_runtime_risk_publish(rr) -> None:
    task = asyncio.get_running_loop().create_task(publish_runtime_risk(rr))
    _runtime_risk_tasks.add(task)  # strong ref until done
    task.add_done_callback(_runtime_risk_tasks.discard)


tick_client.add_listener(_ws_broadcast_listener)

# Virtual-only for this phase (see orders/order_engine.py docstring) — swap
# VirtualBrokerAdapter() for a real DhanAdapter instance to go live later.
order_engine = OrderEngine(token_router, VirtualBrokerAdapter())


def _on_leg_exit_arm_followup(event) -> None:
    """Registered AFTER order_engine (see below) — trigger listeners fire in
    registration order, so by the time this runs, order_engine's own
    listener has already executed the exit and flipped leg.status to
    EXITED. services/leg_followup.py then decides whether a Lazy/Re-entry/
    Re-cost followup should arm, per the exited leg's own config.

    The followup itself is deferred to right after the current tick's
    dispatch (call_soon): a leg SL/TP fires inside TokenRouter._process_leg,
    BEFORE that same tick's broker/strategy risk evaluation — running the
    re-entry synchronously here let a fresh leg enter under a broker or
    strategy that the very same tick was about to lock/exit. Deferred, it
    sees the settled broker/strategy status and entry_allowed() blocks it."""
    if event.event_type not in ("SL_HIT", "TP_HIT"):
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        loop.call_soon(_run_leg_exit_followup, event)
    else:
        _run_leg_exit_followup(event)


def _run_leg_exit_followup(event) -> None:
    try:
        _leg_exit_followup(event)
    except Exception:
        log.exception("[LegExitFollowup] failed for leg=%s", event.leg_id)


def _leg_exit_followup(event) -> None:
    leg = token_router.legs.get(event.leg_id)
    if leg is None or leg.status != "EXITED":
        return
    strategy_id = leg.strategy_id
    leg_followup.handle_leg_exit(token_router, order_engine, get_instrument_master(), get_ltp_cache(), leg, event.event_type)
    # An SL/TP exit's ONLY route to an already-open execute-orders socket
    # used to be the tick-driven broadcast in broadcast_tick_update (any
    # later tick touching one of this strategy's OTHER still-open legs'
    # tokens would happen to re-send it) — that broadcast has been removed
    # entirely (see its own docstring: it's meant for discrete actions, not
    # every price tick), so this exit needs its own explicit push now, the
    # same way ws_live.py's square_off handler and the BROKER_ branch below
    # already do. Built/scheduled BEFORE finalize_if_no_open_legs, not
    # after, for the same reason those two do it that way — finalize pops
    # the strategy (and, if this was its last leg, unregisters it) out of
    # token_router, so a broadcast attempted afterward would find nothing
    # left to build a record from.
    strategy = token_router.strategies.get(strategy_id)
    if strategy is not None and strategy.user_id:
        legs = [token_router.legs[lid] for lid in strategy.leg_ids if lid in token_router.legs]
        lazy_watchers = [w for w in token_router.lazy_watchers.values() if w.strategy_id == strategy.strategy_id]
        record = strategy_to_doc(strategy, legs, lazy_watchers)
        import asyncio

        from api.ws_client_hub import execute_orders_hub
        try:
            # send_to_user, not broadcast — this strategy's own owner only
            # (see broadcast_strategy_snapshots's docstring in
            # api/live_broadcast.py for the full "why").
            asyncio.create_task(execute_orders_hub.send_to_user(strategy.user_id, {"data": {"records": [record]}}))
        except Exception:
            log.exception("[LegExitFollowup] execute_orders_hub.send_to_user scheduling failed for strategy=%s", strategy_id)
    # handle_leg_exit may have just registered a fresh re-entry leg back onto
    # this same strategy (Immediate reentry) — finalize_if_no_open_legs is a
    # no-op in that case (has_open_leg true again). Only when NO followup was
    # configured/armed does this leg's exit genuinely leave the strategy with
    # zero open legs, and it needs the same finalize this file's ws_live.py
    # square_off handler does — otherwise an SL/TP exit with no re-entry
    # configured leaves the strategy ACTIVE-with-zero-legs forever, same
    # "keeps reappearing" symptom as the manual-square-off gap.
    strategy_finalize.finalize_and_publish(token_router, strategy_id)


token_router.add_trigger_listener(_on_leg_exit_arm_followup)


def _on_debug_scheduled_entry_ready(event) -> None:
    """Registered BEFORE the real-strategy scheduled-entry listener below —
    a debug-direct pending entry (see api/routers/admin.py's `entry_time`
    field) is claimed/unregistered here first, so the real-strategy handler's
    own `pending is None` no-op check skips it cleanly instead of attempting
    (and failing) NSE strike resolution against a raw crypto token."""
    if event.event_type != "SCHEDULED_ENTRY_READY":
        return
    debug_scheduled_entry.handle_debug_scheduled_entry_ready(token_router, order_engine, get_ltp_cache(), event)


token_router.add_trigger_listener(_on_debug_scheduled_entry_ready)


def _on_scheduled_entry_ready(event) -> None:
    if event.event_type != "SCHEDULED_ENTRY_READY":
        return
    scheduled_entry.handle_scheduled_entry_ready(token_router, order_engine, get_instrument_master(), get_ltp_cache(), event)


token_router.add_trigger_listener(_on_scheduled_entry_ready)


def _on_lazy_or_recost_triggered(event) -> None:
    """Completes the entry orders/order_engine.py's own docstring flagged
    as "NOT wired yet: LAZY_TRIGGERED / RECOST_TRIGGERED -> entry order" —
    token_router already detects both triggers correctly (arm + the
    trigger-price check in _process_lazy_watcher/_process_recost_watcher
    both already worked before this listener existed), nothing downstream
    ever turned that trigger into a real order or LegRuntime. See
    services/leg_followup.py's complete_lazy_entry/complete_recost_entry
    docstrings for what each actually does; this listener just dispatches
    by event_type + the lazy_id/recost_id packed into event.reason (see
    token_router.py's _process_lazy_watcher/_process_recost_watcher, the
    only two places that ever emit these two event types), then pushes the
    resulting new leg out over the socket — same explicit "the HTTP/tick
    path already moved on, so an already-open connection only learns about
    this off the next tick unless we push now" reasoning as every other
    explicit push in this file."""
    if event.event_type == "LAZY_TRIGGERED":
        lazy_id = event.reason.split(":", 1)[-1] if event.reason.startswith("LAZY:") else ""
        if not lazy_id:
            return
        leg_followup.complete_lazy_entry(token_router, order_engine, lazy_id, event.trigger_ltp)
    elif event.event_type == "RECOST_TRIGGERED":
        recost_id = event.reason.split(":", 1)[-1] if event.reason.startswith("RECOST:") else ""
        if not recost_id:
            return
        leg_followup.complete_recost_entry(token_router, order_engine, recost_id, event.trigger_ltp)
    else:
        return

    strategy = token_router.strategies.get(event.strategy_id)
    if strategy is not None and strategy.user_id:
        legs = [token_router.legs[lid] for lid in strategy.leg_ids if lid in token_router.legs]
        lazy_watchers = [w for w in token_router.lazy_watchers.values() if w.strategy_id == strategy.strategy_id]
        record = strategy_to_doc(strategy, legs, lazy_watchers)
        import asyncio

        from api.ws_client_hub import execute_orders_hub
        try:
            asyncio.create_task(execute_orders_hub.send_to_user(strategy.user_id, {"data": {"records": [record]}}))
        except Exception:
            log.exception("[LazyRecostFollowup] execute_orders_hub.send_to_user scheduling failed for strategy=%s", event.strategy_id)


token_router.add_trigger_listener(_on_lazy_or_recost_triggered)


def _on_overall_reentry(event) -> None:
    """OverallReentrySL/OverallReentryTgt — user's explicit ask to port
    these. Registered BEFORE _on_trigger_checkpoint below (order matters:
    order_engine's own listener, registered earliest of all, already
    exited every leg for this STRATEGY_ event by the time this runs, same
    as _on_trigger_checkpoint's own finalize check relies on) so this can
    set strategy.activation_in_progress = True and stop that finalize from
    popping the strategy before services.strategy_activation.
    handle_overall_reentry's background task ever gets to run — see that
    function's own docstring for why.

    Re-checks overall_risk.check_overall_sl_hit/check_overall_target_hit
    directly (same pure functions strategy_risk.evaluate_strategy_risk
    itself already called to produce this exact event, nothing mutated
    strategy.mtm in between — this is still the same synchronous, same-
    tick dispatch) rather than trusting the STRATEGY_STOP_LOSS_HIT/
    STRATEGY_TARGET_HIT event_type alone: that decision can ALSO come from
    the Lock/LockAndTrail/TrailStopLoss evaluator further down strategy_
    risk.py's own evaluate_strategy_risk, which OverallReentrySL/Tgt were
    never meant to apply to (the old system never wired a strategy-level
    exit for LockAndTrail at all — see strategy_risk.py's own docstring)."""
    if event.event_type not in ("STRATEGY_STOP_LOSS_HIT", "STRATEGY_TARGET_HIT"):
        return
    strategy = token_router.strategies.get(event.strategy_id)
    if strategy is None:
        return

    if event.event_type == "STRATEGY_STOP_LOSS_HIT":
        if not overall_risk.check_overall_sl_hit(strategy):
            return
        kind, key, done = "SL", "OverallReentrySL", strategy.sl_reentry_done
    else:
        if not overall_risk.check_overall_target_hit(strategy):
            return
        kind, key, done = "TGT", "OverallReentryTgt", strategy.tgt_reentry_done

    reentry_type, reentry_count = strategy_activation.parse_overall_reentry(strategy.full_strategy_cfg, key)
    if reentry_type == "None" or reentry_count <= 0 or done >= reentry_count:
        return

    # Synchronous — must land BEFORE _on_trigger_checkpoint's finalize
    # check runs (next listener, same event dispatch).
    strategy.activation_in_progress = True
    asyncio.create_task(
        strategy_activation.handle_overall_reentry(token_router, order_engine, get_instrument_master(), get_ltp_cache(), event.strategy_id, kind),
        name=f"overall_reentry_{event.strategy_id}",
    )


token_router.add_trigger_listener(_on_overall_reentry)


def _on_runtime_risk_hit(event) -> None:
    """User-set strategy / portfolio-group SL/Target/Lock/Trail fired
    (token_router._evaluate_runtime_risks) — square off every strategy in
    that scope, the same way a broker-level hit squares off its strategies."""
    if not event.event_type.startswith("RUNTIME_RISK_"):
        return
    scope, _, target_id = event.reason.partition(":")
    if scope == "strategy":
        strategy_ids = [target_id]
    else:
        strategy_ids = [sid for sid, s in token_router.strategies.items() if s.group_id == target_id]
    for strategy_id in strategy_ids:
        try:
            strategy_finalize.square_off_strategy(token_router, order_engine, strategy_id, reason=event.event_type)
        except Exception:
            log.exception("[RuntimeRisk] square-off failed strategy=%s scope=%s", strategy_id, event.reason)
    log.info("[RuntimeRisk] %s hit %s at mtm=%s — squared off %d strategies", event.reason, event.event_type, event.trigger_ltp, len(strategy_ids))


token_router.add_trigger_listener(_on_runtime_risk_hit)

checkpoint_writer = get_checkpoint_writer()


def _on_trigger_checkpoint(event) -> None:
    """Event-driven checkpoint (master-doc §46/§47) — every status
    transition (exit, overall SL/TP hit) is checkpointed immediately;
    routine mtm/trailing drift between events is covered by the periodic
    snapshot loop below instead of a write on every single tick. Legs are
    embedded in their parent strategy's algo_trades doc (persistence/
    serializers.py), so a leg-level event re-checkpoints the whole strategy,
    not the leg alone."""
    if event.strategy_id:
        checkpoint_strategy(token_router, event.strategy_id)
    elif event.leg_id:
        leg = token_router.legs.get(event.leg_id)
        if leg is not None:
            checkpoint_strategy(token_router, leg.strategy_id)
    if event.event_type.startswith("STRATEGY_"):
        # Overall strategy-level SL/Target/Lock/Trail hit (token_router.
        # _evaluate_strategies -> order_engine's own STRATEGY_ branch, which
        # runs BEFORE this listener and has already exited every leg) — same
        # gap the BROKER_ branch below already had and was fixed for:
        # nothing ever called finalize_if_no_open_legs for this path, so a
        # strategy whose OVERALL SL/Target fired sat at EXIT_PENDING
        # forever, with every leg genuinely EXITED underneath it — confirmed
        # live (this exact scenario, StopLoss=MTM ₹5, both legs exited
        # correctly, strategy status stuck at EXIT_PENDING, no socket push,
        # never removed from token_router or algo_trades). Broadcast BEFORE
        # finalize, not after, same reasoning as every other finalize call
        # site in this file — finalize pops the strategy out of token_router
        # entirely, so a broadcast attempted afterward finds nothing to
        # build a record from.
        strategy = token_router.strategies.get(event.strategy_id)
        if strategy is not None and strategy.user_id:
            legs = [token_router.legs[lid] for lid in strategy.leg_ids if lid in token_router.legs]
            lazy_watchers = [w for w in token_router.lazy_watchers.values() if w.strategy_id == event.strategy_id]
            record = strategy_to_doc(strategy, legs, lazy_watchers)
            import asyncio

            from api.ws_client_hub import execute_orders_hub
            try:
                asyncio.create_task(execute_orders_hub.send_to_user(strategy.user_id, {"data": {"records": [record]}}))
            except Exception:
                log.exception("[TriggerCheckpoint] execute_orders_hub.send_to_user scheduling failed for strategy=%s", event.strategy_id)
        strategy_finalize.finalize_and_publish(token_router, event.strategy_id)
    if event.event_type.startswith("BROKER_"):
        broker = token_router.brokers.get(event.reason)
        if broker is not None:
            # Broker SL/Target/Lock/Trail fired: order_engine (earlier
            # listener) has already exited every leg under it. Same as the old
            # system's check_broker_sl_target -> disable_broker_sl_settings:
            # the broker's risk settings are switched OFF (status 0 in
            # algo_borker_stoploss_settings, empty RiskConfig here) and the
            # broker goes straight back to ACTIVE with a fresh MTM — new
            # strategies can be activated under it right away (user's ask:
            # no day-long lock); re-saving broker settings arms them again.
            # Every strategy running under this broker is squared off — not
            # just the legs order_engine already closed: its pending
            # time-gated entries and lazy/recost/range watchers are cancelled
            # too, any leg still open is exited, and each strategy is saved
            # and pushed to the page as "Squared Off" (finalize_and_publish),
            # instead of disappearing from it.
            strategy_ids = set(broker.strategy_ids) | {
                sid for sid, s in token_router.strategies.items() if s.broker_scope_id == broker.broker_scope_id
            }
            for strategy_id in strategy_ids:
                try:
                    strategy_finalize.square_off_strategy(token_router, order_engine, strategy_id, reason=event.event_type)
                except Exception:
                    log.exception("[BrokerExit] square-off failed strategy=%s broker=%s", strategy_id, broker.broker_scope_id)
            # Then settings off + broker ACTIVE again (old system's
            # disable_broker_sl_settings) — new strategies may be activated
            # under it right away; re-saving broker settings re-arms them.
            broker_scope.reset_after_risk_exit(broker)


token_router.add_trigger_listener(_on_trigger_checkpoint)

_PERIODIC_CHECKPOINT_SECONDS = 30
_background_tasks: list[asyncio.Task] = []


async def _periodic_checkpoint_loop() -> None:
    """Safety-net snapshot (master-doc §46 "periodic checkpoint") — catches
    ongoing mtm/trailing drift the event-driven checkpoints above don't
    cover on their own, without writing Mongo on every tick."""
    while True:
        await asyncio.sleep(_PERIODIC_CHECKPOINT_SECONDS)
        for index, strategy_id in enumerate(tuple(token_router.strategies)):
            strategy = token_router.strategies.get(strategy_id)
            if strategy is not None and strategy.status == "ACTIVE":
                checkpoint_strategy(token_router, strategy_id)
            if index % 100 == 99:
                await asyncio.sleep(0)
        for index, broker_id in enumerate(tuple(token_router.brokers)):
            broker = token_router.brokers.get(broker_id)
            if broker is not None and broker.status == "ACTIVE":
                checkpoint_writer.queue_broker(broker_to_doc(broker))
            if index % 100 == 99:
                await asyncio.sleep(0)


_LEGACY_OPEN_TOKENS_REFRESH_SECONDS = 5


async def _legacy_open_tokens_loop() -> None:
    """Keeps api/live_broadcast.py's _legacy_open_tokens_cache warm — the
    ltp_update tick path (broadcast_tick_update) only ever reads that
    in-RAM set, never queries Mongo itself (master-doc §46/§47/§59: no
    Mongo on the hot path). A brand-new legacy (old-system) position's
    ticks start flowing on the next refresh here, not instantly — that lag
    is the deliberate trade for keeping every tick free of I/O, same trade
    _periodic_checkpoint_loop above already makes for persistence."""
    while True:
        await refresh_legacy_open_tokens()
        await asyncio.sleep(_LEGACY_OPEN_TOKENS_REFRESH_SECONDS)


_MONGO_RECONCILE_POLL_SECONDS = 10


async def _mongo_reconcile_loop() -> None:
    """Detects a strategy deleted directly from `algo_trades` in Mongo —
    dev/admin tooling, or a manual DB cleanup, bypassing this engine's own
    checkpoint_writer.queue_strategy_delete path entirely — and mirrors
    that removal into token_router's in-memory state (token_router.
    force_remove_strategy, see its own docstring). User's explicit report,
    confirmed live: deleting a strategy's algo_trades doc by hand did NOT
    clear this engine's own in-memory cache of it — it kept showing on
    /fast-forward_2, and the NEXT periodic checkpoint (_periodic_
    checkpoint_loop above, every 30s) even wrote it straight back into
    `algo_trades` as if nothing had happened, undoing the delete.

    Plain poll, not a Mongo change stream — this engine doesn't require/
    assume a replica set (unlike shared/features/db_change_watcher.py's
    old-system change-stream+poll-fallback hybrid, which explicitly
    supports both). One batched `_id in [...]` existence check per cycle
    covering every strategy currently in RAM, not one query per strategy."""
    mongo = get_mongo()
    while True:
        await asyncio.sleep(_MONGO_RECONCILE_POLL_SECONDS)
        strategy_ids = list(token_router.strategies.keys())
        if not strategy_ids:
            continue
        try:
            existing_ids = {
                str(d["_id"]) for d in mongo.raw["algo_trades"].find(
                    {"_id": {"$in": strategy_ids}}, {"_id": 1},
                )
            }
        except Exception:
            log.exception("[MongoReconcile] existence check failed")
            continue
        missing_ids = [sid for sid in strategy_ids if sid not in existing_ids]
        for strategy_id in missing_ids:
            if token_router.force_remove_strategy(strategy_id):
                log.info("[MongoReconcile] strategy=%s no longer in algo_trades — removed from cache", strategy_id)


_RANGE_BREAKOUT_POLL_SECONDS = 5


async def _range_breakout_loop() -> None:
    """LegRangeBreakout (ORB/BTST) — advances every armed range watcher's
    state machine (range-collection, day rollover, breakout check) on a
    plain poll, same "wall-clock driven, not tick-driven" reasoning
    services/range_breakout_service.py's own module docstring explains.
    5s (vs the old system's 1s Mongo-polled equivalent, leg_range_monitor.
    py) since this is a pure in-memory scan, not a DB round trip — cheap
    enough to run more often, but no real feature needs sub-5s breakout
    latency here either."""
    while True:
        await asyncio.sleep(_RANGE_BREAKOUT_POLL_SECONDS)
        if token_router.range_watchers:
            await range_breakout_service.advance_all(token_router, order_engine, get_instrument_master(), get_ltp_cache())


_DELTA_INSTRUMENT_CACHE_REFRESH_SECONDS = 30


async def _delta_instrument_cache_loop() -> None:
    """Crypto's equivalent of instrument_master's one-time startup load —
    see shared/market/delta_instrument_cache.py's own module docstring for
    the full "why": keeps the whole BTC/ETH option universe's metadata
    cached AND subscribed on DeltaFeed's mark_price channel, so strike_
    resolver.build_chain_rows's crypto branch reads cache + ltp_cache like
    the NSE branch does, instead of a synchronous Delta REST call on every
    single strike resolution. Runs off the main event loop's own thread
    pool (get_delta_instrument_cache().refresh() is synchronous httpx, same
    as every other crypto REST call site) — asyncio.sleep between cycles is
    the only actual await here, so this never blocks tick processing."""
    cache = get_delta_instrument_cache()
    while True:
        await asyncio.to_thread(cache.refresh)
        await asyncio.to_thread(cache.subscribe_all)
        await asyncio.sleep(_DELTA_INSTRUMENT_CACHE_REFRESH_SECONDS)


_IST = timezone(timedelta(hours=5, minutes=30))
_DAILY_BROKER_RESET_POLL_SECONDS = 60


async def _daily_broker_reset_loop() -> None:
    """Day rollover for broker MTM (a broker that hit its SL/Target was
    already put back to ACTIVE by broker_scope.reset_after_risk_exit; this
    loop only zeroes every broker's day P&L). Original notes:
    Completes the ACTIVE -> EXIT_PENDING -> LOCKED_FOR_DAY cycle
    (runtime/broker_runtime.py's own long-documented, never-built master-doc
    §33 state machine): once a broker's SL/Target/TrailSL fires, the BROKER_
    branch in _on_trigger_checkpoint above now parks it at LOCKED_FOR_DAY
    instead of the old permanent EXIT_PENDING dead end. This loop is the
    other half — the only way back to ACTIVE — firing once per IST calendar
    date rollover, for every broker regardless of whether it actually
    locked (broker.mtm is documented as DAY NET PNL in risk/broker_risk.py,
    so even a broker nothing ever hit must still start the new day at zero,
    not carry yesterday's running total forward). A 60s poll (not a
    sleep-until-midnight) matches this file's other loops in trading off a
    little latency after actual midnight for simplicity/robustness across
    restarts, and costs nothing extra since it only touches Mongo on the
    date it actually changes."""
    last_reset_date = datetime.now(_IST).date()
    while True:
        await asyncio.sleep(_DAILY_BROKER_RESET_POLL_SECONDS)
        today = datetime.now(_IST).date()
        if today == last_reset_date:
            continue
        last_reset_date = today
        for broker in token_router.brokers.values():
            broker.mtm = 0.0
            broker.cycle_base_mtm = 0.0
            broker.realized_pnl = 0.0
            broker.peak_pnl = 0.0
            broker.current_floor = 0.0
            broker.trailing_activated = False
            broker.status = "ACTIVE"
            checkpoint_writer.queue_broker(broker_to_doc(broker))
            broker_scope.schedule_publish_lock_state(broker)
        log.info("daily_broker_reset: reset %d broker(s) for %s", len(token_router.brokers), today)


async def _daily_eod_square_off_loop() -> None:
    """IST calendar-date rollover -> auto square-off + drop anything left
    over from a prior day (user's explicit ask: at IST date rollover,
    whatever isn't from "today" must square off and disappear from
    /fast-forward_2, not sit there looking live indefinitely — this service
    has no old-system-style EOD job of its own, see persistence/serializers.
    py's load_legacy_execute_order_records docstring for the previous state
    of affairs, where a stale active_on_server=True position from days ago
    was treated as "still genuinely open" on purpose). Same poll-not-sleep-
    until-midnight approach as _daily_broker_reset_loop above, for the same
    restart-robustness reason.

    Compares each strategy's `activated_date` (StrategyRuntime, set once at
    activation time — see that field's own docstring for why the doc-level
    `creation_ts` can't be used for this) against today's IST date; ""
    (a checkpoint recovered from before this field existed) counts as stale
    too, same as a genuinely old date, so nothing already-active gets
    grandfathered in past the very first midnight after this loop starts.

    Squares off every still-open leg via order_engine.manual_square_off —
    the exact same single-leg exit path `/ws/execute-orders`'s own
    square_off action already uses — rather than a separate bulk-exit
    route, so this is subject to the identical idempotency/broker-fill
    behavior a user-initiated square-off gets. Broadcasts the just-emptied
    record BEFORE finalizing, then finalizes + queues the Mongo delete —
    same ordering (and reason) every other finalize call site in this file
    uses: finalize pops the strategy out of token_router, so a broadcast
    attempted afterward would find nothing left to build a record from."""
    # Each market rolls over on its own trading day: NSE at IST midnight,
    # crypto at 17:30 IST (end of the daily-expiry session — see
    # conditions/schedule_engine.trading_day). A crypto trade entered at
    # 20:00 (or scheduled for 04:11 next morning) belongs to that session and
    # must not be swept at midnight.
    from conditions import schedule_engine
    from selection import strike_resolver

    def _is_crypto(strategy) -> bool:
        return strike_resolver.is_crypto_underlying(str(strategy.strategy_cfg.get("Ticker") or ""))

    last_day = {False: schedule_engine.trading_day(False), True: schedule_engine.trading_day(True)}
    while True:
        await asyncio.sleep(_DAILY_BROKER_RESET_POLL_SECONDS)
        for is_crypto in (False, True):
            current = schedule_engine.trading_day(is_crypto)
            if current == last_day[is_crypto]:
                continue
            last_day[is_crypto] = current
            stale_ids = [
                sid for sid, strategy in token_router.strategies.items()
                if _is_crypto(strategy) == is_crypto and strategy.activated_date != current
            ]
            for strategy_id in stale_ids:
                try:
                    strategy_finalize.square_off_strategy(token_router, order_engine, strategy_id, reason="EOD_SQUARE_OFF")
                except Exception:
                    log.exception("[EODSquareOff] square-off failed strategy=%s", strategy_id)
            log.info("daily_eod_square_off: %s trading day %s — swept %d stale strategy(ies)",
                     "crypto" if is_crypto else "NSE", current, len(stale_ids))


_CRYPTO_EXPIRY_POLL_SECONDS = 30


async def _crypto_expiry_square_off_loop() -> None:
    """A Delta option leg still open at its contract's expiry (17:30 IST)
    would otherwise sit ACTIVE with a frozen last mark — the contract stops
    publishing, so SL/TP can never fire again and its P&L never settles —
    until the midnight sweep. Squares such legs off at their last price
    through the same single-leg exit path a user square-off uses, then
    broadcasts/finalizes exactly like _daily_eod_square_off_loop."""
    from api.ws_client_hub import execute_orders_hub
    from shared.brokers.delta import client as delta_client

    while True:
        await asyncio.sleep(_CRYPTO_EXPIRY_POLL_SECONDS)
        now = datetime.now(_IST)
        expired_by_strategy: dict[str, list[str]] = {}
        for leg in list(token_router.legs.values()):
            if leg.status != "ACTIVE" or not delta_client.asset_for_token(leg.token) or leg.token in delta_client.UNDERLYING_TO_ASSET:
                continue
            expiry_at = delta_client.delta_expiry_at(leg.expiry)
            if expiry_at is not None and now >= expiry_at:
                expired_by_strategy.setdefault(leg.strategy_id, []).append(leg.leg_id)
        for strategy_id, leg_ids in expired_by_strategy.items():
            for leg_id in leg_ids:
                order_engine.manual_square_off(leg_id, reason="EXPIRY")
                log.info("[CryptoExpiry] leg=%s squared off at contract expiry", leg_id)
            strategy = token_router.strategies.get(strategy_id)
            if strategy is None:
                continue
            checkpoint_strategy(token_router, strategy_id)
            if strategy.user_id:
                legs = [token_router.legs[lid] for lid in strategy.leg_ids if lid in token_router.legs]
                lazy_watchers = [w for w in token_router.lazy_watchers.values() if w.strategy_id == strategy_id]
                try:
                    await execute_orders_hub.send_to_user(strategy.user_id, {"data": {"records": [strategy_to_doc(strategy, legs, lazy_watchers)]}})
                except Exception:
                    log.exception("[CryptoExpiry] execute_orders_hub.send_to_user failed for strategy=%s", strategy_id)
            strategy_finalize.finalize_and_publish(token_router, strategy_id)


async def _chain_prewarm_once() -> None:
    """One-shot at startup, not a loop — instrument_master itself is only
    ever loaded once (this same on_startup, above), so re-running this on a
    timer would just re-subscribe the exact same tokens repeatedly. Matches
    shared/features/dhan_ticker.py's own _prewarm_index_chains, which also
    runs exactly once, right at ticker startup, not periodically. See
    shared/market/dhan_chain_prewarm.py's own module docstring for the full
    "why" — this is the NSE-side counterpart to crypto's own
    _delta_instrument_cache_loop above, closing the same "first
    activation on a cold token pays a real subscribe-and-wait" gap crypto
    already had solved. Backgrounded (asyncio.to_thread) since the
    underlying call is a synchronous httpx POST to algo.websocket, same as
    every other prewarm/subscribe call site in this codebase."""
    from shared.market.dhan_chain_prewarm import prewarm_index_chains
    count = await asyncio.to_thread(prewarm_index_chains, get_mongo(), get_instrument_master())
    log.info("[Startup] chain_prewarm subscribed %d tokens", count)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "algo-trade-2_0"}


# Old algo.trade (8000) routes algo-2_0 doesn't have its own version of —
# mounted LAST so every route above always wins. See legacy_api.py.
import legacy_api  # noqa: E402

log.info("legacy routes mounted: %d", legacy_api.mount_missing_routes(app))


@app.on_event("startup")
async def on_startup() -> None:
    log.info("algo-trade-2_0 starting up")
    loaded = get_instrument_master().load(get_mongo())
    log.info("instrument_master prewarmed: %d tokens", loaded)

    # Recovery BEFORE the tick stream starts (master-doc §47/§60) — restored
    # legs/strategies/brokers must already be registered (token_to_legs etc
    # rebuilt) before the first live tick can reach them.
    restored = recovery.recover(token_router, get_mongo())
    log.info("recovery: %s", restored)

    checkpoint_writer.start()

    # Gap detection + historical replay (master-prompt "Why Latest LTP Is
    # Not Enough") — STILL before tick_client.start() below, same ordering
    # reasoning as recovery.recover() above: any market move that happened
    # entirely during this process's downtime must be reconstructed before
    # the first live tick arrives, or a leg's best_price/current_sl would
    # silently resume from a stale, less-protective value. Synchronous by
    # design here (blocks startup, not the tick path) — the master
    # prompt's own rule is "do not report the risk engine healthy before
    # critical recovery finishes", which FastAPI's own startup-gate already
    # gives us for free as long as this runs inline, not as a background task.
    # Broker MTM = day total (today's squared-off strategies + running ones),
    # the same figure FastForward2's Total MTM shows.
    broker_scope.rebase_day_mtm(token_router, get_mongo())

    from risk import runtime_risk
    for rr in runtime_risk.load_active(get_mongo()):
        token_router.runtime_risks[rr.key] = rr
    log.info("runtime_risk: restored %d strategy/group risk settings", len(token_router.runtime_risks))

    gap_summary = gap_recovery.recover_tsl_gaps(token_router, get_mongo())
    log.info("gap_recovery: %s", gap_summary)

    _background_tasks.append(asyncio.create_task(_periodic_checkpoint_loop(), name="periodic_checkpoint"))
    _background_tasks.append(asyncio.create_task(_legacy_open_tokens_loop(), name="legacy_open_tokens_refresh"))
    _background_tasks.append(asyncio.create_task(_delta_instrument_cache_loop(), name="delta_instrument_cache_refresh"))
    _background_tasks.append(asyncio.create_task(_daily_broker_reset_loop(), name="daily_broker_reset"))
    _background_tasks.append(asyncio.create_task(_daily_eod_square_off_loop(), name="daily_eod_square_off"))
    _background_tasks.append(asyncio.create_task(_range_breakout_loop(), name="range_breakout"))
    _background_tasks.append(asyncio.create_task(_crypto_expiry_square_off_loop(), name="crypto_expiry_square_off"))
    _background_tasks.append(asyncio.create_task(_mongo_reconcile_loop(), name="mongo_reconcile"))
    _background_tasks.append(asyncio.create_task(_chain_prewarm_once(), name="chain_prewarm"))
    _background_tasks.append(asyncio.create_task(asyncio.to_thread(legacy_api.legacy_startup), name="legacy_startup"))
    straddle_alert.start_loop()
    tick_client.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    tick_client.stop()
    if token_router._release_task is not None:
        token_router._release_task.cancel()
        await asyncio.gather(token_router._release_task, return_exceptions=True)
    await straddle_alert.stop_loop()
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)
    _background_tasks.clear()
    for strategy_id in tuple(token_router.strategies):
        checkpoint_strategy(token_router, strategy_id)
    for broker in token_router.brokers.values():
        checkpoint_writer.queue_broker(broker_to_doc(broker))
    await checkpoint_writer.stop()
    from api.ws_client_hub import update_hub, execute_orders_hub
    await asyncio.gather(update_hub.close(), execute_orders_hub.close())
    log.info("algo-trade-2_0 shutting down")
