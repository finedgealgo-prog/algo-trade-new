"""
recovery.py
──────────────
On startup, reconstruct token_router's in-memory state from the last
checkpoint — master-doc §47/§60: "Load active data -> rebuild broker/
strategy runtime -> rebuild token indexes -> resume." Order matters:
brokers first, then strategies (register_strategy links it into its
broker's strategy_ids if the broker is already registered), then legs
(register_leg links into both its strategy's and broker's leg_ids, and
rebuilds token_to_legs — master-doc §23's "no searching" navigation).

Strategies (with legs embedded) come from `algo_trades` — the SAME
collection the real system's own `/ws/execute-orders` reads (user's
explicit instruction: use the same table, not a separate one) — filtered
to `engine: ENGINE_TAG` only. That filter is load-bearing, not decorative:
the old system also writes into `algo_trades`, and (confirmed) both happen
to use the same "exec_" id-prefix convention, so `_id` alone can't tell a
real old-system trade apart from one of this engine's own. Recovering a
trade this engine never actually placed would mean silently re-managing
(and potentially re-exiting) a real position — the `engine` tag exists
specifically to make that impossible.

Only non-terminal (not EXITED) checkpoints are restored — an exited
strategy/leg has nothing left to resume. Brokers still get their own small
collection (algo2_broker_checkpoints) — no old-system analog for live
broker-scope risk runtime state exists to reuse.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from risk import mtm_engine
from persistence.checkpoint import BROKER_COLLECTION, STRATEGY_COLLECTION
from persistence.serializers import ENGINE_TAG, _today_ist_date, doc_to_broker_kwargs, doc_to_strategy_kwargs, docs_to_legs
from runtime.broker_runtime import BrokerRuntime
from runtime.lazy_runtime import LazyRuntime
from runtime.pending_entry_runtime import PendingEntryRuntime
from runtime.range_watcher_runtime import RangeWatcherRuntime
from runtime.recost_runtime import RecostRuntime
from runtime.strategy_runtime import StrategyRuntime
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger
from token_router import TokenRouter

log = get_logger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

_TERMINAL_LEG_STATUSES = {"EXITED", "SL_HIT", "TP_HIT"}


def _doc_trading_day(doc: dict, ist_today: str) -> str:
    """Trading day a checkpointed strategy must match to still be live:
    crypto -> current 17:30 IST session, NSE -> today's IST date."""
    from conditions.schedule_engine import trading_day
    from shared.brokers.delta import client as delta_client

    ticker = delta_client.normalize_underlying(doc.get("ticker") or (doc.get("_runtime_strategy_cfg") or {}).get("Ticker"))
    if ticker in delta_client.UNDERLYING_TO_ASSET:
        return trading_day(True)
    return ist_today


def _restore_pending_work(router: TokenRouter, doc: dict, recost_docs: list) -> int:
    """Re-registers the strategy's pending time-gated entries and armed
    lazy/recost/range watchers (persistence/serializers.pending_work_to_doc).
    Anything caught mid-trigger at shutdown is re-armed, not dropped — its
    entry had not completed (completion unregisters it before the
    checkpoint that follows)."""
    count = 0
    for item in doc.get("_runtime_pending_entries") or []:
        try:
            item = dict(item)
            target = item.get("target_time_ist")
            item["target_time_ist"] = datetime.fromisoformat(target) if target else datetime.now(_IST)
            item["status"] = "WAITING_TIME"
            router.register_pending_entry(PendingEntryRuntime(**item))
            count += 1
        except Exception:
            log.exception("[Recovery] pending entry restore failed strategy=%s", doc.get("_id"))
    for item in doc.get("_runtime_lazy_watchers") or []:
        try:
            router.register_lazy_watcher(LazyRuntime(**{**item, "status": "ARMED"}))
            count += 1
        except Exception:
            log.exception("[Recovery] lazy watcher restore failed strategy=%s", doc.get("_id"))
    for item in recost_docs:
        try:
            router.register_recost_watcher(RecostRuntime(**{**item, "status": "ARMED"}))
            count += 1
        except Exception:
            log.exception("[Recovery] recost watcher restore failed strategy=%s", doc.get("_id"))
    for item in doc.get("_runtime_range_watchers") or []:
        try:
            router.register_range_watcher(RangeWatcherRuntime(**item))
            count += 1
        except Exception:
            log.exception("[Recovery] range watcher restore failed strategy=%s", doc.get("_id"))
    return count


def recover(router: TokenRouter, mongo: Mongo) -> dict[str, int]:
    restored = {"brokers": 0, "strategies": 0, "legs": 0, "watchers": 0, "stale_closed": 0}

    for doc in mongo.raw[BROKER_COLLECTION].find({"status": {"$ne": "EXITED"}}):
        broker = BrokerRuntime(**doc_to_broker_kwargs(doc))
        if broker.status != "ACTIVE":
            # Checkpointed mid-exit or under the retired LOCKED_FOR_DAY rule:
            # its legs are closed, so resume it the way a finished risk exit
            # does (settings off, fresh MTM, ACTIVE).
            from services.broker_scope import reset_after_risk_exit

            reset_after_risk_exit(broker, mongo)
        router.register_broker(broker)
        restored["brokers"] += 1

    # Intraday-only (user's explicit ask): a strategy is recovered ONLY if
    # it was activated on today's IST calendar date — not merely "not yet
    # EXITED". Restarting the process days after a position was opened must
    # NOT bring it back to life as if it were still a genuinely live
    # today's trade (confirmed live: a 5-day-old checkpoint kept reappearing
    # on /fast-forward_2 after every restart, since only `_runtime_status`
    # was checked before). `_daily_eod_square_off_loop` (main.py) enforces
    # this same same-day rule for a process that stays up across a
    # midnight rollover without restarting; this is the restart-time half.
    today = _today_ist_date()
    for doc in mongo.raw[STRATEGY_COLLECTION].find({"engine": ENGINE_TAG, "_runtime_status": {"$ne": "EXITED"}}):
        if (doc.get("_runtime_activated_date") or "") != _doc_trading_day(doc, today):
            # Never registered into token_router, so there's no live leg for
            # order_engine to actually square off against here — just mark
            # the doc closed directly so algo_trades stops reporting it
            # active_on_server: True forever (same end-state the live
            # midnight sweep leaves a stale strategy in).
            mongo.raw[STRATEGY_COLLECTION].update_one(
                {"_id": doc["_id"]},
                {"$set": {"active_on_server": False, "status": "EXITED", "_runtime_status": "EXITED"}},
            )
            restored["stale_closed"] += 1
            continue
        strategy = StrategyRuntime(**doc_to_strategy_kwargs(doc))
        router.register_strategy(strategy)
        restored["strategies"] += 1
        recost_docs = doc.get("_runtime_recost_watchers") or []
        # An AtCost watcher re-enters from its EXITED parent leg's own
        # token/qty/config, so that parent must be back in the router too.
        recost_parent_ids = {str(w.get("parent_leg_id") or "") for w in recost_docs}
        for leg in docs_to_legs(doc, mongo):
            if leg.status in _TERMINAL_LEG_STATUSES and leg.leg_id not in recost_parent_ids:
                continue
            router.register_leg(leg)
            # Re-derive this leg's pnl with its (just-set) pnl_multiplier and
            # fold any difference into the parent scopes — a no-op for a
            # checkpoint already written in ₹, a correction for one written
            # before crypto P&L was contract/INR-scaled.
            delta = mtm_engine.apply_leg_pnl_delta(leg)
            if delta:
                strategy.mtm += delta
                broker = router.brokers.get(leg.broker_scope_id) if leg.broker_scope_id else None
                if broker is not None:
                    broker.mtm += delta
            restored["legs"] += 1
        restored["watchers"] += _restore_pending_work(router, doc, recost_docs)

    log.info(
        "[Recovery] restored brokers=%d strategies=%d legs=%d watchers=%d stale_closed=%d",
        restored["brokers"], restored["strategies"], restored["legs"], restored["watchers"], restored["stale_closed"],
    )
    return restored
