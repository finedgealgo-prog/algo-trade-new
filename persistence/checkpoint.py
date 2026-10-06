"""
checkpoint.py
────────────────
Async, non-blocking persistence — master-doc §46/§47/§59: "do not write
MongoDB on every P&L change... use RAM for the hot path... persistence:
asynchronous, periodic checkpoint, important state events."

Writes into `algo_trades` — the SAME collection the real system's own
`/ws/execute-orders` socket reads from (user's explicit instruction: use the
same table the actual page uses, not a separate one). One doc per strategy,
legs embedded as an array field (persistence/serializers.py builds the full
doc — same function the live socket push uses too, so checkpoint and socket
are never two parallel shapes of the same data).

Broker runtime (live risk state: mtm, trailing floor, RiskConfig) has no
old-system analog to reuse — the old system's broker-level settings
(`algo_borker_stoploss_settings`) are config, not live runtime state — so
that one still gets its own small collection.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from functools import lru_cache
from typing import Any, TYPE_CHECKING

from shared.db.mongo import Mongo, get_mongo
from shared.logging.logger import get_logger

if TYPE_CHECKING:
    from token_router import TokenRouter

log = get_logger(__name__)

STRATEGY_COLLECTION = "algo_trades"
BROKER_COLLECTION = "algo2_broker_checkpoints"

_QUEUE_MAXSIZE = 10_000


class CheckpointWriter:
    def __init__(self, mongo: Mongo) -> None:
        self.mongo = mongo
        # Latest snapshot per entity: repeated MTM checkpoints do not grow
        # the backlog. A later delete supersedes an unwritten upsert.
        self._pending: OrderedDict[tuple[str, str], tuple[str, Any]] = OrderedDict()
        self._ready = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._task: asyncio.Task | None = None
        self.last_error = ""

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._worker(), name="checkpoint_writer")

    async def stop(self, timeout: float = 10.0) -> None:
        if self._task is not None:
            # Do not cancel an in-flight thread write: drain in FIFO order.
            try:
                await asyncio.wait_for(self._idle.wait(), timeout)
            except asyncio.TimeoutError:
                log.error("[Checkpoint] shutdown drain timed out; recovery snapshot may be incomplete")
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def queue_strategy(self, doc: dict[str, Any]) -> None:
        self._enqueue("upsert", STRATEGY_COLLECTION, doc)

    def queue_broker(self, doc: dict[str, Any]) -> None:
        self._enqueue("upsert", BROKER_COLLECTION, doc)

    def queue_strategy_delete(self, strategy_id: str) -> None:
        self._enqueue("delete", STRATEGY_COLLECTION, strategy_id)

    def _enqueue(self, op: str, collection: str, payload: Any) -> None:
        key = (collection, payload if op == "delete" else payload["_id"])
        self._pending[key] = (op, payload)
        self._idle.clear()
        self._ready.set()
        if len(self._pending) == _QUEUE_MAXSIZE:
            log.error("[Checkpoint] %d distinct entities pending; retaining state for retry", len(self._pending))

    def _write(self, key, op, payload) -> None:
        collection, identifier = key
        if op == "delete":
            self.mongo.raw[collection].delete_one({"_id": identifier})
        else:
            self.mongo.raw[collection].update_one({"_id": identifier}, {"$set": payload}, upsert=True)

    async def _worker(self) -> None:
        while True:
            await self._ready.wait()
            while self._pending:
                key, (op, payload) = self._pending.popitem(last=False)
                try:
                    await asyncio.to_thread(self._write, key, op, payload)
                    self.last_error = ""
                except Exception as exc:
                    self.last_error = str(exc)
                    # A newer queued snapshot/delete takes precedence on retry.
                    self._pending.setdefault(key, (op, payload))
                    log.exception("[Checkpoint] write failed; retrying")
                    await asyncio.sleep(1)
            self._ready.clear()
            self._idle.set()


@lru_cache(maxsize=1)
def get_checkpoint_writer() -> CheckpointWriter:
    return CheckpointWriter(get_mongo())


def checkpoint_strategy(router: "TokenRouter", strategy_id: str) -> None:
    """The one place every caller (activation, leg exit, followups,
    scheduled entry, the trigger listener in main.py) should reach for —
    legs are embedded IN the strategy's algo_trades doc (see
    persistence/serializers.py), so any leg-level change means re-queuing
    the WHOLE parent strategy snapshot, not just that one leg. Looks up the
    strategy's current legs from the router itself so callers never have to
    assemble that list by hand."""
    from persistence.serializers import pending_work_to_doc, strategy_to_doc

    strategy = router.strategies.get(strategy_id)
    if strategy is None:
        return
    legs = [router.legs[lid] for lid in strategy.leg_ids if lid in router.legs]
    lazy_watchers = [w for w in router.lazy_watchers.values() if w.strategy_id == strategy_id]
    doc = strategy_to_doc(strategy, legs, lazy_watchers)
    doc.update(pending_work_to_doc(
        [p for p in router.pending_entries.values() if p.strategy_id == strategy_id],
        lazy_watchers,
        [w for w in router.recost_watchers.values() if w.strategy_id == strategy_id],
        [w for w in router.range_watchers.values() if w.strategy_id == strategy_id],
    ))
    get_checkpoint_writer().queue_strategy(doc)
