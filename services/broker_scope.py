"""
broker_scope.py
─────────────────
Makes sure a real activation's broker scope has a live BrokerRuntime with
its broker-level SL/Target/Lock/Trail settings loaded.

Strategy and portfolio activation pass a broker_configuration `_id` as
broker_scope_id (the broker picker's value). Before this, a BrokerRuntime
only ever existed for scopes created through /broker-configuration/dhan/save
or the admin debug endpoint, so for every real activation
`router.brokers.get(broker_scope_id)` returned None and broker-level risk was
silently never evaluated, even though FastForward2.tsx had saved settings for
that exact broker into `algo_borker_stoploss_settings`.
"""

from __future__ import annotations

from typing import Any

from persistence.checkpoint import get_checkpoint_writer
from persistence.serializers import _today_ist_date, broker_to_doc, load_legacy_broker_settings
from risk.broker_risk import build_broker_risk_config, evaluate_broker_risk, reset_peak_window, risk_state, running_mtm, settings_active
from risk.models import RiskConfig
from runtime.broker_runtime import BrokerRuntime
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger
from token_router import TokenRouter

log = get_logger(__name__)

ACTIVATION_MODE = "fast-forward"
LIVE_MODE = "live"
FORWARD_TEST_MODE = "forward-test"
# Paper modes (fast-forward, forward-test) get virtual fills only; "live"
# (AlgoTrade2) also places real Delta orders (orders/live_executor.py).
ACTIVATION_MODES = (ACTIVATION_MODE, FORWARD_TEST_MODE, LIVE_MODE)


def load_broker_settings(mongo: Mongo, user_id: str, broker_scope_id: str, activation_mode: str = ACTIVATION_MODE) -> dict[str, Any] | None:
    """Blocking Mongo read — call via asyncio.to_thread from async code."""
    collection = mongo.raw["algo_borker_stoploss_settings"]
    query: dict[str, Any] = {"broker": broker_scope_id, "activation_mode": activation_mode}
    if user_id:
        doc = collection.find_one({**query, "user_id": user_id})
        if doc is not None:
            return doc
    return collection.find_one(query)


def _current_trading_days() -> list[str]:
    from conditions.schedule_engine import trading_day

    return sorted({_today_ist_date(), trading_day(True)})


def closed_day_mtm(mongo: Mongo, broker_scope_id: str) -> float:
    """Blocking — P&L of this broker's strategies already squared off today
    (algo-2_0's final algo_trades snapshots). Added to the running
    strategies' MTM it gives the broker's day total."""
    total = 0.0
    for doc in mongo.raw["algo_trades"].find(
        {"engine": "algo-2_0", "broker": broker_scope_id, "_runtime_status": "EXITED",
         "_runtime_activated_date": {"$in": _current_trading_days()}},
        {"_runtime_mtm": 1},
    ):
        total += float(doc.get("_runtime_mtm") or 0.0)
    return total


def rebase_day_mtm(router: TokenRouter, mongo: Mongo) -> None:
    """Startup (after recovery, before any tick):
    - risk config rebuilt from the SAVED broker settings (DB is the source
      of truth — a checkpointed config can predate a rule change or a save
      made while the service was down);
    - MTM set to the day total = today's squared-off strategies (DB) +
      strategies running now (RAM)."""
    for broker in router.brokers.values():
        try:
            settings_doc = load_broker_settings(mongo, broker.user_id, broker.broker_scope_id, broker.activation_mode)
        except Exception:
            log.exception("[BrokerScope] settings reload failed broker=%s", broker.broker_scope_id)
            settings_doc = None
        broker.config = build_broker_risk_config(settings_doc or {}) if settings_active(settings_doc) else RiskConfig()
        running = running_mtm(router, broker.broker_scope_id)
        broker.mtm = closed_day_mtm(mongo, broker.broker_scope_id) + running
        broker.cycle_base_mtm = 0.0
        # Risk peak is in running-MTM terms; an active lock keeps its peak.
        broker.peak_pnl = max(broker.peak_pnl, running) if broker.trailing_activated else running
        if settings_active(settings_doc):
            # The checkpoint can predate a lock that activated just before a
            # crash; the settings doc is written on every lock change.
            _seed_lock_state(broker, settings_doc or {})


def ensure_broker_runtime(router: TokenRouter, broker_scope_id: str, user_id: str, settings_doc: dict[str, Any] | None, closed_mtm: float = 0.0, activation_mode: str = ACTIVATION_MODE) -> BrokerRuntime | None:
    """Registers a BrokerRuntime for `broker_scope_id` if none is live yet,
    configured from `settings_doc` (see load_broker_settings). An already
    registered broker is returned untouched — its day MTM, lock state and
    status must not be reset by a new activation."""
    if not broker_scope_id:
        return None
    broker = router.brokers.get(broker_scope_id)
    if broker is not None:
        return broker
    broker = BrokerRuntime(broker_scope_id=broker_scope_id, user_id=user_id, mtm=closed_mtm, peak_pnl=closed_mtm, activation_mode=activation_mode)
    if settings_active(settings_doc):
        broker.config = build_broker_risk_config(settings_doc or {})
    router.register_broker(broker)
    _attach_running_positions(router, broker)
    # Risk acts on the running strategies' MTM (risk.broker_risk.running_mtm);
    # the peak starts there — never at 0 against a loss, which read as a huge
    # profit and fired the Lock instantly.
    broker.cycle_base_mtm = 0.0
    broker.peak_pnl = running_mtm(router, broker_scope_id)
    reset_peak_window(broker_scope_id, broker.peak_pnl)
    if settings_active(settings_doc):
        _seed_lock_state(broker, settings_doc or {})
    get_checkpoint_writer().queue_broker(broker_to_doc(broker))
    log.info(
        "[BrokerScope] registered broker=%s sl=%s target=%s trailing=%s",
        broker_scope_id, broker.config.stop_loss_amount, broker.config.target_amount, broker.config.trailing_mode.value,
    )
    schedule_publish_lock_state(broker)
    return broker


# Lock/trail state fields written onto the saved broker settings doc — the
# same names the old system used and FastForward2's broker strip reads.
CLEARED_LOCK_STATE = {"lock_activated": False, "current_lock_floor": 0.0, "lock_peak_mtm": 0.0, "effective_sl": None, "lock_state_date": ""}


def _seed_lock_state(broker: BrokerRuntime, settings_doc: dict[str, Any]) -> None:
    """A runtime for a broker whose lock already activated earlier today
    (fresh registration, or a recovered checkpoint older than the lock)
    resumes from the DB state, so the locked floor never drops back —
    whichever of runtime / DB floor is higher wins."""
    if settings_doc.get("lock_state_date") != _today_ist_date() or not settings_doc.get("lock_activated"):
        return
    broker.trailing_activated = True
    broker.current_floor = max(float(broker.current_floor or 0.0), float(settings_doc.get("current_lock_floor") or 0.0))
    broker.peak_pnl = max(broker.peak_pnl, float(settings_doc.get("lock_peak_mtm") or 0.0))


def save_lock_state(mongo: Mongo, broker: BrokerRuntime) -> None:
    """Blocking — writes the broker's live lock/trail state to its saved
    settings doc (algo_borker_stoploss_settings)."""
    query: dict[str, Any] = {"broker": broker.broker_scope_id, "activation_mode": broker.activation_mode}
    if broker.user_id:
        query["user_id"] = broker.user_id
    state = {**risk_state(broker), "lock_state_date": _today_ist_date()}
    mongo.raw["algo_borker_stoploss_settings"].update_many(query, {"$set": state})


async def publish_lock_state(broker: BrokerRuntime, mongo: Mongo | None = None) -> None:
    """Persist the lock/trail state, then push the user's broker-settings
    rows — read back from the DB — to their open FastForward2 page."""
    import asyncio

    from api.ws_client_hub import execute_orders_hub
    from shared.db.mongo import get_mongo

    target = mongo or get_mongo()
    try:
        await asyncio.to_thread(save_lock_state, target, broker)
        if not broker.user_id:
            return
        rows = await asyncio.to_thread(load_legacy_broker_settings, target, broker.user_id, broker.activation_mode)
        if rows:
            await execute_orders_hub.send_to_user(broker.user_id, {
                "type": "broker-settings", "message": "Broker settings updated", "data": {"brokers": rows},
            })
    except Exception:
        log.exception("[BrokerScope] lock state publish failed broker=%s", broker.broker_scope_id)


_publish_tasks: set = set()


def schedule_publish_lock_state(broker: BrokerRuntime) -> None:
    import asyncio

    try:
        task = asyncio.get_running_loop().create_task(publish_lock_state(broker))
    except RuntimeError:
        return  # no event loop (tests / sync callers)
    _publish_tasks.add(task)  # strong ref until done (asyncio keeps only a weak one)
    task.add_done_callback(_publish_tasks.discard)


def disable_saved_settings(mongo: Mongo, broker_scope_id: str, user_id: str, activation_mode: str = ACTIVATION_MODE) -> None:
    """Blocking — after a broker SL/Target/Lock/Trail hit: the saved doc goes
    inactive (status 0) AND its values are emptied, so FastForward2's Broker
    Settings card/modal show blank ("-") instead of the spent values, and
    nothing is re-applied on the next activation/restart until the user
    saves new settings."""
    query: dict[str, Any] = {"broker": broker_scope_id, "activation_mode": activation_mode}
    if user_id:
        query["user_id"] = user_id
    mongo.raw["algo_borker_stoploss_settings"].update_many(query, {"$set": {
        "status": 0, "StopLoss": None, "Target": None, "LockAndTrail": None, "OverallTrailSL": None,
        **CLEARED_LOCK_STATE,
    }})


def reset_after_risk_exit(broker: BrokerRuntime, mongo: Mongo | None = None) -> None:
    """After a broker-level SL/Target/Lock/Trail exit (all its legs already
    closed): settings off, lock floor cleared, status ACTIVE — so
    new strategies can be activated under this broker immediately. Must be
    called on the event loop; the Mongo update runs in a worker thread."""
    import asyncio

    # MTM (and its day peak) stay as they are: broker MTM is the DAY total of
    # every strategy run under it today — the same figure FastForward2's
    # Total MTM shows — not a counter restarted after each broker exit.
    broker.config = RiskConfig()
    broker.peak_pnl = 0.0  # everything under it is squared off: running MTM is 0
    broker.cycle_base_mtm = 0.0
    reset_peak_window(broker.broker_scope_id)
    broker.current_floor = 0.0
    broker.trailing_activated = False
    broker.status = "ACTIVE"
    get_checkpoint_writer().queue_broker(broker_to_doc(broker))
    log.info("[BrokerScope] broker=%s risk exit done — settings disabled, broker ACTIVE again", broker.broker_scope_id)
    try:
        from shared.db.mongo import get_mongo

        target = mongo or get_mongo()

        async def _disable_and_publish() -> None:
            await asyncio.to_thread(disable_saved_settings, target, broker.broker_scope_id, broker.user_id, broker.activation_mode)
            await publish_lock_state(broker, target)  # page gets the emptied settings right away

        task = asyncio.get_running_loop().create_task(_disable_and_publish())
        _publish_tasks.add(task)
        task.add_done_callback(_publish_tasks.discard)
    except RuntimeError:
        pass  # no event loop (sync caller / tests) — runtime state is already reset


def _attach_running_positions(router: TokenRouter, broker: BrokerRuntime) -> None:
    """Adopts strategies/legs already running under this scope (activated
    before the scope had a BrokerRuntime). Their legs already carry this
    broker_scope_id, so from the next tick on every P&L delta also flows
    into broker.mtm; the broker's day MTM starts from their current MTM."""
    for strategy in router.strategies.values():
        if strategy.broker_scope_id != broker.broker_scope_id or strategy.status == "EXITED":
            continue
        broker.strategy_ids.add(strategy.strategy_id)
        broker.mtm += strategy.mtm
        for leg_id in strategy.leg_ids:
            leg = router.legs.get(leg_id)
            if leg is not None and leg.broker_scope_id == broker.broker_scope_id:
                broker.leg_ids.add(leg_id)


def apply_live_settings(router: TokenRouter, broker_scope_id: str, user_id: str, settings_doc: dict[str, Any], closed_mtm: float = 0.0, activation_mode: str = ACTIVATION_MODE) -> BrokerRuntime | None:
    """Applies a broker SL/Target/Lock/Trail save to the LIVE engine, so it
    takes effect on already-running strategies from the very next tick:

    - broker already live -> new RiskConfig; the lock floor/activation state
      is recomputed under the new parameters (the day's peak MTM is kept, so
      a trail set mid-day still counts profit already made);
    - no BrokerRuntime yet but strategies are running under this scope ->
      create it and adopt those positions;
    - nothing running -> None (the next activation loads the saved doc).
    A broker mid-exit (EXIT_PENDING) keeps that status."""
    broker = router.brokers.get(broker_scope_id)
    if broker is None:
        if not any(s.broker_scope_id == broker_scope_id and s.status != "EXITED" for s in router.strategies.values()):
            return None
        return ensure_broker_runtime(router, broker_scope_id, user_id, settings_doc, closed_mtm, activation_mode)
    if broker.activation_mode != activation_mode:
        # Settings saved for this broker under the other page's mode — not
        # the ones this running broker evaluates.
        return broker
    new_config = build_broker_risk_config(settings_doc) if settings_active(settings_doc) else RiskConfig()
    if new_config != broker.config:
        broker.config = new_config
        # New settings = new lock/trail cycle (old system: settings-signature
        # change resets lock_peak_mtm): the peak restarts from the MTM right
        # now, so a peak from an earlier cycle can't pre-activate the lock
        # and instantly square off a freshly added strategy.
        broker.current_floor = 0.0
        broker.trailing_activated = False
        # Evaluated on the running strategies' MTM right now — profit already
        # made counts immediately (Lock 60->40 every 5->+2 saved at +200 locks
        # 96 at once, not after the next +60).
        current = running_mtm(router, broker_scope_id)
        broker.peak_pnl = current
        broker.cycle_base_mtm = 0.0
        reset_peak_window(broker_scope_id, current)
        if broker.status == "ACTIVE":
            # Recompute lock/floor under the new parameters now (display
            # updates on save); any exit it implies fires on the next tick.
            evaluate_broker_risk(broker, current)
        log.info(
            "[BrokerScope] live settings updated broker=%s sl=%s target=%s trailing=%s sl_trail=%s/%s",
            broker_scope_id, new_config.stop_loss_amount, new_config.target_amount, new_config.trailing_mode.value,
            new_config.stop_loss_trail_every, new_config.stop_loss_trail_by,
        )
    get_checkpoint_writer().queue_broker(broker_to_doc(broker))
    # Always: the save just wrote a cleared lock state to the settings doc,
    # so write the engine's real current state back (and push it), even when
    # the re-saved values are identical and nothing was recomputed.
    schedule_publish_lock_state(broker)
    return broker
