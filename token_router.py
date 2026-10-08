"""
token_router.py
──────────────────
The master-docs' central rule: "market data event → who actually needs this
event → process only them." Registers as a TickClient listener; every tick
dispatches ONLY to the legs mapped to the token(s) that actually changed, via
token_to_active_legs — never a scan of every active leg.

Unified Strategy+Broker risk engine (second master-doc) additions on top of
Phase 3's leg-level SL/TP:
  - broker_to_active_legs index (§21/§23), parallel to strategy_to_active_legs
  - the SAME leg P&L delta is propagated to both strategy.mtm and broker.mtm
    (§6 — never recomputed independently per scope)
  - BATCHED aggregation (§24-27): every token in a tick is processed first
    (updating legs + collecting which strategies/brokers were touched),
    THEN each touched strategy/broker is risk-evaluated exactly ONCE for
    the whole tick — not once per leg/token
  - BROKER > STRATEGY priority (§28/§63): brokers are evaluated first, so a
    broker-level exit claims its legs (ACTIVE -> EXIT_PENDING) before
    strategy-level evaluation gets a chance — a leg already claimed by its
    broker is skipped when its strategy tries to claim it too (§30
    idempotency), satisfied here purely by evaluation order + a status check,
    no extra locking needed under this process's single-threaded dispatch.

Duplicate-trigger protection (§30/§46/§68): a leg/strategy/broker is only
evaluated/claimed while status == "ACTIVE"; the moment it fires, status
flips immediately, before any other processing in this tick can re-observe
it as ACTIVE. Safe under this process's single-threaded asyncio dispatch.

Trigger events are collected in-memory for now (Phase 6 — order engine —
turns these into real broker orders); this phase's job is correct,
low-latency detection, not execution yet.
"""

from __future__ import annotations

import asyncio
import heapq
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from conditions import lazy_engine, recost_engine
from risk import broker_risk, mtm_engine, strategy_risk, sl_tp_engine, trailing_engine
from orders.slippage import slipped_price
from risk.models import RiskDecision
from runtime.broker_runtime import BrokerRuntime
from runtime.lazy_runtime import LazyRuntime
from runtime.leg_runtime import LegRuntime
from runtime.pending_entry_runtime import PendingEntryRuntime
from runtime.range_watcher_runtime import RangeWatcherRuntime
from runtime.recost_runtime import RecostRuntime
from runtime.strategy_runtime import StrategyRuntime
from shared.brokers.delta import client as delta_client
from shared.logging.logger import get_logger
from shared.market.ltp_cache import TickUpdate

_IST = timezone(timedelta(hours=5, minutes=30))

# Mass-same-second entry release (entry-#CLAUDE CODE IMPLEMENTATION
# PROMPT.md §26/§48) — measured at 100k simultaneous entries, a fully
# synchronous release took ~10s of blocking event-loop time, during which
# SL/TP on unrelated live positions would be completely stuck. Chunking the
# release with a yield every N entries keeps each chunk's own blocking
# window tiny, so a new incoming tick's risk dispatch is never stuck behind
# a mass release for more than one chunk's worth of work.
_RELEASE_CHUNK_SIZE = 200

# Same set as persistence/serializers.py's own _TERMINAL_LEG_STATUSES —
# duplicated (not imported) deliberately: this module is the runtime core
# every other layer (persistence included) depends ON, so it must never
# import FROM the persistence layer, even for one trivial constant.
_TERMINAL_LEG_STATUSES = {"SL_HIT", "TP_HIT", "EXITED"}

log = get_logger(__name__)


@dataclass
class TriggerEvent:
    event_type: str
    strategy_id: str
    leg_id: str = ""
    trigger_ltp: float = 0.0
    reason: str = ""


class TokenRouter:
    def __init__(self) -> None:
        self.token_to_legs: dict[str, set[str]] = {}
        self.underlying_to_legs: dict[str, set[str]] = {}
        self.spot_prices: dict[str, float] = {}
        self.legs: dict[str, LegRuntime] = {}
        self.strategies: dict[str, StrategyRuntime] = {}
        self.brokers: dict[str, BrokerRuntime] = {}

        # Selection-signature cache for ONE release batch (mass-same-second
        # entry concept — see entry-#CLAUDE CODE IMPLEMENTATION PROMPT.md
        # §11-17): when N strategies scheduled for the same instant need the
        # SAME strike selection (same underlying/expiry/option_type/method/
        # param), resolve it once and let the rest reuse the result instead
        # of each independently recomputing it. Reset at the START of every
        # _check_pending_entries() call (= one release batch = one market
        # snapshot) — NEVER carried across ticks, since reusing a stale
        # selection across different snapshots would be a correctness bug,
        # not just a missed optimization (doc §17: "correctness has
        # priority" over sharing). services/scheduled_entry.py reads/writes
        # this directly (router is already passed to it).
        self.selection_cache: dict[tuple, object] = {}

        # Guards against spawning a second overlapping release task while
        # one is still mid-flight (e.g. a later tick's due-check fires again
        # before a large prior release has finished its chunks).
        self._release_task: "asyncio.Task | None" = None

        self.token_to_lazy_watchers: dict[str, set[str]] = {}
        self.lazy_watchers: dict[str, LazyRuntime] = {}

        self.token_to_recost_watchers: dict[str, set[str]] = {}
        self.recost_watchers: dict[str, RecostRuntime] = {}

        # EntryIndicators time-gated entries — master-doc §17 "smart
        # time-indexed scheduler": a min-heap keyed by due-time, so checking
        # "is anything due" is an O(1) peek at the earliest entry, never a
        # scan of every pending entry (see _check_pending_entries below).
        # `pending_entries` stays the lookup-by-id map; `_pending_entry_heap`
        # is purely an ordering index over the same objects — a heap entry
        # can go stale (its pending_id already unregistered/re-triggered by
        # the time it's popped), which _check_pending_entries discards
        # rather than re-validating eagerly (cheaper than keeping the heap
        # perfectly in sync on every unregister).
        self.pending_entries: dict[str, PendingEntryRuntime] = {}
        self._pending_entry_heap: list[tuple[datetime, str]] = []

        # LegRangeBreakout (ORB/BTST) watchers — unlike lazy/recost, these
        # advance on WALL-CLOCK time (range-collection window, day
        # rollover for BTST), not per-token ticks, so there's no
        # token_to_range_watchers index: services/range_breakout_service.py's
        # periodic loop just iterates every entry here directly. See
        # runtime/range_watcher_runtime.py's own docstring.
        self.range_watchers: dict[str, RangeWatcherRuntime] = {}

        # Brokers whose lock/trail state (lock activated, locked floor,
        # trailed SL) changed during the last tick — drained by main.py,
        # which persists it and pushes a "broker-settings" socket update.
        self.broker_risk_state_changed: set[str] = set()

        # Post-activation strategy / portfolio-group risk set from
        # FastForward2 (risk/runtime_risk.py), keyed "strategy:<id>" /
        # "group:<group_id>". *_changed is drained by main.py, which
        # persists the state and pushes it to the page.
        self.runtime_risks: dict = {}
        self.runtime_risk_changed: set[str] = set()
        # Called (loop thread) with a leg whose SL the trailing engine just
        # moved — main.py points it at the live executor, which moves that
        # leg's resting Delta stop too.
        self.on_sl_moved = None

        self.trigger_log: list[TriggerEvent] = []
        self._trigger_listeners: list = []

    # ── Registration (Phase 8's recovery path rebuilds these from Mongo) ──

    def register_strategy(self, strategy: StrategyRuntime) -> None:
        self.strategies[strategy.strategy_id] = strategy
        if strategy.broker_scope_id:
            broker = self.brokers.get(strategy.broker_scope_id)
            if broker:
                broker.strategy_ids.add(strategy.strategy_id)

    def register_broker(self, broker: BrokerRuntime) -> None:
        self.brokers[broker.broker_scope_id] = broker

    def register_leg(self, leg: LegRuntime) -> None:
        self.legs[leg.leg_id] = leg
        if not leg.entry_ts:  # fresh entry (a recovered leg carries its own)
            leg.entry_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.token_to_legs.setdefault(leg.token, set()).add(leg.leg_id)
        strategy = self.strategies.get(leg.strategy_id)
        underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper() if strategy else ""
        leg.pnl_multiplier = delta_client.pnl_multiplier(underlying, leg.token)
        if strategy is not None and strategy.slippage_pct and not leg.entry_slippage_applied:
            # Fresh entry under a slippage-% activation: fill price moves
            # against the trade (BUY higher / SELL lower) and SL/TP re-anchor
            # on that fill. current_price stays the real market price.
            leg.slippage_pct = strategy.slippage_pct
            leg.entry_price = slipped_price(leg.entry_price, leg.slippage_pct, buying=not leg.is_sell)
            sl_tp_engine.initialize_sl_tp(leg)
        leg.entry_slippage_applied = True
        if strategy:
            strategy.leg_ids.add(leg.leg_id)
            if underlying:
                leg.current_spot = self.spot_prices.get(underlying, leg.current_spot or leg.entry_spot)
                if any(sl_tp_engine.is_underlying_config(leg.leg_cfg.get(key) or {})
                       for key in ("LegStopLoss", "LegTarget")):
                    self.underlying_to_legs.setdefault(underlying, set()).add(leg.leg_id)
        if leg.broker_scope_id:
            broker = self.brokers.get(leg.broker_scope_id)
            if broker:
                broker.leg_ids.add(leg.leg_id)

    def unregister_leg(self, leg_id: str) -> None:
        leg = self.legs.pop(leg_id, None)
        if leg is None:
            return
        token_legs = self.token_to_legs.get(leg.token)
        if token_legs:
            token_legs.discard(leg_id)
            if not token_legs:
                del self.token_to_legs[leg.token]
        strategy = self.strategies.get(leg.strategy_id)
        if strategy:
            strategy.leg_ids.discard(leg_id)
            underlying = str(strategy.strategy_cfg.get("Ticker") or "").upper()
            indexed = self.underlying_to_legs.get(underlying)
            if indexed is not None:
                indexed.discard(leg_id)
                if not indexed:
                    del self.underlying_to_legs[underlying]
        if leg.broker_scope_id:
            broker = self.brokers.get(leg.broker_scope_id)
            if broker:
                broker.leg_ids.discard(leg_id)

    def register_lazy_watcher(self, lazy: LazyRuntime) -> None:
        self.lazy_watchers[lazy.lazy_id] = lazy
        self.token_to_lazy_watchers.setdefault(lazy.token, set()).add(lazy.lazy_id)

    def finalize_if_no_open_legs(self, strategy_id: str) -> bool:
        """Marks a strategy EXITED and unregisters it (+ its legs) once none
        of its legs are still open — call this after ANY leg exit (manual
        square-off, or a risk-engine SL/TP exit that had no re-entry
        configured/armed) so a fully-closed strategy actually leaves
        `self.strategies` instead of sitting there ACTIVE forever.

        Nothing in this engine flipped strategy.status on its last leg
        exiting before this existed — confirmed live: a strategy manually
        squared off via /ws/execute-orders's square_off stayed ACTIVE with
        zero open legs, got checkpointed straight back into `algo_trades`
        on the next periodic tick, and reappeared on every page reload —
        indistinguishable from a phantom "new" record to anyone watching,
        even though nothing new was ever created (api/routers/admin.py's
        cleanup-zombies endpoint could clear it manually, but nothing
        called that automatically, and it's not reachable from the regular
        UI at all).

        Returns True if the strategy was finalized (caller should then
        queue its Mongo delete — this method deliberately does ONLY the
        in-memory side, matching every other TokenRouter method; it must
        never import the persistence layer, which itself depends on
        runtime types this module owns)."""
        strategy = self.strategies.get(strategy_id)
        if strategy is None or strategy.activation_in_progress:
            return False
        if any(p.strategy_id == strategy_id for p in self.pending_entries.values()):
            return False
        has_open_leg = any(
            lid in self.legs and self.legs[lid].status not in _TERMINAL_LEG_STATUSES
            for lid in strategy.leg_ids
        )
        if has_open_leg:
            return False
        # A leg exit that armed a lazy (NextLeg) or recost (AtCost) watcher
        # instead of an immediate re-entry leaves the strategy with ZERO
        # open legs right now but genuine pending work — confirmed live:
        # a single-leg AtCost strategy's only leg hit SL, leg_followup armed
        # a recost watcher, and this method (called right after, same as
        # every other call site) finalized and deleted the strategy anyway
        # since it only ever looked at strategy.leg_ids — orphaning that
        # watcher, which would have tried to open a fresh leg under a
        # strategy_id no longer in self.strategies (and already deleted
        # from Mongo) whenever it eventually fired. Both watcher types
        # carry their own strategy_id (see runtime/recost_runtime.py,
        # runtime/lazy_runtime.py), so this is a direct filter, not a scan
        # keyed through leg_ids like the leg check above.
        has_pending_watcher = any(w.strategy_id == strategy_id for w in self.recost_watchers.values()) or \
            any(w.strategy_id == strategy_id for w in self.lazy_watchers.values()) or \
            any(w.strategy_id == strategy_id for w in self.range_watchers.values())
        if has_pending_watcher:
            return False
        strategy.status = "EXITED"
        for leg_id in list(strategy.leg_ids):
            self.unregister_leg(leg_id)
        self.strategies.pop(strategy_id, None)
        strategy_risk.forget(strategy_id)
        # register_strategy() adds strategy_id to broker.strategy_ids on the
        # way in (line ~138) but nothing ever removed it on the way out —
        # confirmed live: a broker's reported strategy_count stayed
        # permanently inflated by every strategy that had EVER run under it,
        # long after they'd all fully exited, and (more importantly) that
        # stale id sitting in strategy_ids was enough to make a caller
        # believe this broker still had something live to evaluate/exit
        # even when it didn't. unregister_leg above already correctly
        # discards from broker.leg_ids per-leg; this is the matching
        # cleanup one level up, once every leg is gone.
        if strategy.broker_scope_id:
            broker = self.brokers.get(strategy.broker_scope_id)
            if broker:
                broker.strategy_ids.discard(strategy_id)
        return True

    def force_remove_strategy(self, strategy_id: str) -> bool:
        """Unconditionally drops a strategy (+ every leg/lazy/recost/range
        watcher tied to it) out of RAM — unlike finalize_if_no_open_legs,
        this does NOT check has_open_leg/has_pending_watcher/
        activation_in_progress first, because the caller (main.py's
        _mongo_reconcile_loop) already knows the underlying `algo_trades`
        doc is simply gone (deleted directly in Mongo, bypassing this
        engine's own checkpoint_writer.queue_strategy_delete path entirely
        — dev/admin tooling, manual cleanup) — user's explicit report: a
        strategy manually deleted from the DB kept showing on /fast-
        forward_2 from this engine's own in-memory cache, and the next
        periodic checkpoint (_periodic_checkpoint_loop, main.py) even
        wrote it straight back into `algo_trades` as if nothing happened.
        There is no Mongo doc left to reconcile against here, so there is
        nothing further for this method to persist — purely the in-memory
        side, same division of responsibility finalize_if_no_open_legs
        documents for itself. Returns False if the strategy was already
        gone (nothing to do)."""
        strategy = self.strategies.pop(strategy_id, None)
        if strategy is None:
            return False
        strategy_risk.forget(strategy_id)
        for leg_id in list(strategy.leg_ids):
            self.unregister_leg(leg_id)
        for lazy_id in [lid for lid, w in self.lazy_watchers.items() if w.strategy_id == strategy_id]:
            self.unregister_lazy_watcher(lazy_id)
        for recost_id in [rid for rid, w in self.recost_watchers.items() if w.strategy_id == strategy_id]:
            self.unregister_recost_watcher(recost_id)
        for range_id in [rid for rid, w in self.range_watchers.items() if w.strategy_id == strategy_id]:
            self.unregister_range_watcher(range_id)
        for pending_id in [pid for pid, p in self.pending_entries.items() if p.strategy_id == strategy_id]:
            self.unregister_pending_entry(pending_id)
        if strategy.broker_scope_id:
            broker = self.brokers.get(strategy.broker_scope_id)
            if broker:
                broker.strategy_ids.discard(strategy_id)
        return True

    def cancel_pending_work(self, strategy_id: str) -> None:
        """Drops every not-yet-entered piece of work a strategy still has
        (time-gated pending entries, lazy/recost/range watchers) — used when
        the whole strategy is squared off (broker-level risk exit)."""
        for lazy_id in [lid for lid, w in self.lazy_watchers.items() if w.strategy_id == strategy_id]:
            self.unregister_lazy_watcher(lazy_id)
        for recost_id in [rid for rid, w in self.recost_watchers.items() if w.strategy_id == strategy_id]:
            self.unregister_recost_watcher(recost_id)
        for range_id in [rid for rid, w in self.range_watchers.items() if w.strategy_id == strategy_id]:
            self.unregister_range_watcher(range_id)
        for pending_id in [pid for pid, p in self.pending_entries.items() if p.strategy_id == strategy_id]:
            self.unregister_pending_entry(pending_id)

    def unregister_lazy_watcher(self, lazy_id: str) -> None:
        lazy = self.lazy_watchers.pop(lazy_id, None)
        if lazy is None:
            return
        token_watchers = self.token_to_lazy_watchers.get(lazy.token)
        if token_watchers:
            token_watchers.discard(lazy_id)
            if not token_watchers:
                del self.token_to_lazy_watchers[lazy.token]

    def register_recost_watcher(self, recost: RecostRuntime) -> None:
        self.recost_watchers[recost.recost_id] = recost
        self.token_to_recost_watchers.setdefault(recost.token, set()).add(recost.recost_id)

    def unregister_recost_watcher(self, recost_id: str) -> None:
        recost = self.recost_watchers.pop(recost_id, None)
        if recost is None:
            return
        token_watchers = self.token_to_recost_watchers.get(recost.token)
        if token_watchers:
            token_watchers.discard(recost_id)
            if not token_watchers:
                del self.token_to_recost_watchers[recost.token]

    def register_range_watcher(self, watcher: RangeWatcherRuntime) -> None:
        self.range_watchers[watcher.range_id] = watcher

    def unregister_range_watcher(self, range_id: str) -> None:
        self.range_watchers.pop(range_id, None)

    def register_pending_entry(self, pending: PendingEntryRuntime) -> None:
        self.pending_entries[pending.pending_id] = pending
        heapq.heappush(self._pending_entry_heap, (pending.target_time_ist, pending.pending_id))

    def unregister_pending_entry(self, pending_id: str) -> None:
        self.pending_entries.pop(pending_id, None)

    def add_trigger_listener(self, listener) -> None:
        self._trigger_listeners.append(listener)

    def _emit(self, event: TriggerEvent) -> None:
        self.trigger_log.append(event)
        log.info("[TokenRouter] trigger %s strategy=%s leg=%s ltp=%s reason=%s", event.event_type, event.strategy_id, event.leg_id, event.trigger_ltp, event.reason)
        for listener in self._trigger_listeners:
            try:
                listener(event)
            except Exception:
                log.exception("[TokenRouter] trigger listener error")

    # ── Tick dispatch (the hot path) ───────────────────────────────────────

    def on_tick(self, update: TickUpdate) -> None:
        touched_strategy_ids: set[str] = set()
        touched_broker_ids: set[str] = set()

        # Set the whole spot snapshot before evaluating premiums. Each leg
        # is evaluated once when a frame contains both spot and premium.
        spot = update.spot
        for underlying in delta_client.UNDERLYING_TO_ASSET:
            # A crypto perpetual's own LTP is that strategy's underlying spot
            # (TickClient already mirrors it; kept here for direct callers).
            if underlying in update.changed_ltp and underlying not in spot:
                spot = {**spot, underlying: update.changed_ltp[underlying]}
        self.spot_prices.update(spot)
        spot_only_legs: list[str] = []
        for underlying, price in spot.items():
            for leg_id in tuple(self.underlying_to_legs.get(underlying, ())):
                leg = self.legs.get(leg_id)
                if leg is not None:
                    leg.current_spot = price
                    if leg.token not in update.changed_ltp:
                        spot_only_legs.append(leg_id)

        for token, price in update.changed_ltp.items():
            leg_ids = self.token_to_legs.get(token)
            if leg_ids:
                for leg_id in list(leg_ids):
                    self._process_leg(leg_id, price, touched_strategy_ids, touched_broker_ids)

            lazy_ids = self.token_to_lazy_watchers.get(token)
            if lazy_ids:
                for lazy_id in list(lazy_ids):
                    self._process_lazy_watcher(lazy_id, price)

            recost_ids = self.token_to_recost_watchers.get(token)
            if recost_ids:
                for recost_id in list(recost_ids):
                    self._process_recost_watcher(recost_id, price)

        for leg_id in spot_only_legs:
            leg = self.legs.get(leg_id)
            if leg is not None:
                self._process_leg(leg_id, leg.current_price, touched_strategy_ids, touched_broker_ids)

        # Batched, once-per-tick aggregation (§24-27). Broker before strategy
        # (§28/§63 priority) — a broker exit claims its legs first, so a
        # strategy under an already-exited broker finds those legs already
        # EXIT_PENDING and simply can't re-claim them (§30 idempotency).
        if touched_broker_ids:
            self._evaluate_brokers(touched_broker_ids)
        if touched_strategy_ids:
            self._evaluate_strategies(touched_strategy_ids)
            if self.runtime_risks:
                self._evaluate_runtime_risks(touched_strategy_ids)

        if self.pending_entries and self._pending_entry_heap and self._pending_entry_heap[0][0] <= datetime.now(_IST):
            # Mass-same-second entry release, offloaded from this
            # synchronous tick-dispatch path into its own task (measured:
            # 100k due at once = ~10s of blocking work — see
            # _release_due_entries_async's docstring) so a large release can
            # NEVER stall SL/TP/risk dispatch for OTHER, unrelated ticks —
            # master-doc §26 "Risk Engine must remain responsive". Guarded
            # so a still-in-progress release isn't duplicated by a later
            # tick's due-check firing again mid-release.
            if self._release_task is None or self._release_task.done():
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # No running event loop (on_tick called synchronously
                    # outside asyncio — unit tests, or any future sync
                    # caller) — fall back to the immediate full-drain path
                    # rather than losing the release entirely. Checked
                    # BEFORE constructing the coroutine below, not via a
                    # try/except around create_task itself — that would
                    # still construct (and then leak/never-await) the
                    # coroutine object before the RuntimeError surfaced.
                    self._check_pending_entries()
                    loop = None
                if loop is not None:
                    self._release_task = loop.create_task(self._release_due_entries_async(), name="entry_release")

    def _check_pending_entries(self) -> None:
        """Synchronous, full-drain variant — processes every currently-due
        pending entry in one go, no yielding. Used directly by callers that
        want an immediate, complete result in the same call (tests; any
        small, known-bounded batch) — the real on_tick path uses
        _release_due_entries_async instead, precisely because a large batch
        must NOT run to completion in one uninterrupted synchronous call
        (see that method's docstring)."""
        self.selection_cache = {}
        now = datetime.now(_IST)
        while self._pop_and_trigger_one(now):
            pass

    async def _release_due_entries_async(self) -> None:
        """Mass-same-second entry release (entry-#CLAUDE CODE
        IMPLEMENTATION PROMPT.md §8/§9/§26/§48) — same heap-drain as
        _check_pending_entries, but yields to the event loop every
        _RELEASE_CHUNK_SIZE entries via `await asyncio.sleep(0)`. Measured:
        100,000 entries released fully synchronously blocked the event loop
        for ~10 seconds — during that window SL/TP dispatch for OTHER,
        already-active positions on unrelated tokens would be completely
        stuck behind the release. Chunking bounds each uninterrupted burst
        to _RELEASE_CHUNK_SIZE entries' worth of work, so a new incoming
        tick's risk dispatch (on_tick, called synchronously per tick) is
        never stuck behind more than one chunk."""
        self.selection_cache = {}
        processed_since_yield = 0
        while True:
            now = datetime.now(_IST)
            if not self._pop_and_trigger_one(now):
                break
            processed_since_yield += 1
            if processed_since_yield >= _RELEASE_CHUNK_SIZE:
                processed_since_yield = 0
                await asyncio.sleep(0)
                # Ticks can change selection inputs during the yield.
                self.selection_cache.clear()

    def _pop_and_trigger_one(self, now: datetime) -> bool:
        """Pops and processes exactly one due entry (if any). Returns False
        (no-op) once the heap is empty or its earliest entry isn't due yet —
        the shared O(1)-peek primitive both drain variants above loop on."""
        heap = self._pending_entry_heap
        if not heap or heap[0][0] > now:
            return False
        _, pending_id = heapq.heappop(heap)
        pending = self.pending_entries.get(pending_id)
        if pending is None or pending.status != "WAITING_TIME":
            return True  # stale heap entry, discarded — caller should keep looping
        pending.status = "TRIGGERED"
        self._emit(TriggerEvent("SCHEDULED_ENTRY_READY", pending.strategy_id, reason=pending.pending_id))
        return True

    def _process_lazy_watcher(self, lazy_id: str, price: float) -> None:
        lazy = self.lazy_watchers.get(lazy_id)
        if lazy is None or lazy.status != "ARMED":
            return
        if lazy_engine.check_lazy_trigger(lazy, price):
            lazy.status = "TRIGGERED"
            self._emit(TriggerEvent("LAZY_TRIGGERED", lazy.strategy_id, lazy.parent_leg_id, price, f"LAZY:{lazy.lazy_id}"))

    def _process_recost_watcher(self, recost_id: str, price: float) -> None:
        recost = self.recost_watchers.get(recost_id)
        if recost is None or recost.status != "ARMED":
            return
        if recost_engine.check_recost_trigger(recost, price):
            recost.status = "TRIGGERED"
            self._emit(TriggerEvent("RECOST_TRIGGERED", recost.strategy_id, recost.parent_leg_id, price, f"RECOST:{recost.recost_id}"))

    def _process_leg(self, leg_id: str, price: float, touched_strategy_ids: set[str], touched_broker_ids: set[str]) -> None:
        leg = self.legs.get(leg_id)
        if leg is None or leg.status != "ACTIVE":
            return
        strategy = self.strategies.get(leg.strategy_id)
        if strategy is None or strategy.status != "ACTIVE":
            return
        broker = self.brokers.get(leg.broker_scope_id) if leg.broker_scope_id else None
        if broker is not None and broker.status != "ACTIVE":
            return

        leg.current_price = price

        delta = mtm_engine.apply_leg_pnl_delta(leg)
        if delta:
            strategy.mtm += delta
            if broker is not None:
                broker.mtm += delta
        touched_strategy_ids.add(strategy.strategy_id)
        if broker is not None:
            touched_broker_ids.add(broker.broker_scope_id)

        if leg.awaiting_live_fill:
            # Real entry still filling on Delta — SL/Target/Trail start from
            # the real fill price, not this provisional one.
            return

        # No event/checkpoint on a trail move itself — master-prompt's
        # "Checkpointing" section is explicit: "Do NOT write every TSL
        # movement to MongoDB", and TSL is deliberately absent from its
        # "checkpoint important transitions more strongly" list (entry,
        # re-entry generation, EXIT_PENDING/SENT, CLOSED, risk changes —
        # not TSL). A checkpoint-stale best_price/current_sl is NOT a
        # correctness gap here: services/gap_recovery.py reconstructs the
        # true value from real Dhan historical candles on restart,
        # regardless of how long ago the last checkpoint was — that's
        # what actually closes this gap, not writing Mongo more often.
        if trailing_engine.apply_trailing(leg) and self.on_sl_moved is not None:
            try:
                self.on_sl_moved(leg)
            except Exception:
                log.exception("[TokenRouter] on_sl_moved failed leg=%s", leg.leg_id)

        sl_hit, sl_price = sl_tp_engine.check_leg_sl(leg)
        if sl_hit:
            leg.current_sl_price = sl_price
            leg.status = "SL_HIT"
            self._emit(TriggerEvent("SL_HIT", strategy.strategy_id, leg.leg_id, price, "SL"))
        else:
            tp_hit, tp_price = sl_tp_engine.check_leg_target(leg)
            if tp_hit:
                leg.current_tp_price = tp_price
                leg.status = "TP_HIT"
                self._emit(TriggerEvent("TP_HIT", strategy.strategy_id, leg.leg_id, price, "TP"))

    # ── Batched, once-per-tick scope evaluation (§24-27) ────────────────────

    def _evaluate_brokers(self, broker_ids: set[str]) -> None:
        for broker_id in broker_ids:
            broker = self.brokers.get(broker_id)
            if broker is None or broker.status != "ACTIVE":
                continue
            before = broker_risk.risk_state(broker)
            risk_mtm = broker_risk.running_mtm(self, broker_id)
            result = broker_risk.evaluate_broker_risk(broker, risk_mtm)
            after = broker_risk.risk_state(broker)
            # Persist + push only on a real lock/trail/SL change — not on every
            # peak tick (that re-pushed broker-settings constantly, resetting
            # the page's settings form). The peak still rides along whenever
            # the lock is on, so a restart can resume the trail.
            keys = ("lock_activated", "current_lock_floor", "effective_sl")
            if any(after[k] != before[k] for k in keys) or (after["lock_activated"] and after["lock_peak_mtm"] != before["lock_peak_mtm"]):
                self.broker_risk_state_changed.add(broker_id)
            if (after["lock_activated"], after["current_lock_floor"]) != (before["lock_activated"], before["current_lock_floor"]) or result.decision != RiskDecision.NONE:
                # Audit trail for every lock/trail transition and every hit.
                cfg = broker.config
                log.info(
                    "[BrokerRisk] broker=%s running_mtm=%.2f peak=%.2f lock=%s floor=%.2f decision=%s | cfg sl=%s tgt=%s mode=%s reach=%s lock=%s every=%s trail=%s",
                    broker_id, risk_mtm, after["lock_peak_mtm"], after["lock_activated"], after["current_lock_floor"],
                    result.decision.value, cfg.stop_loss_amount, cfg.target_amount, cfg.trailing_mode.value,
                    cfg.activation_profit, cfg.lock_profit, cfg.profit_step, cfg.trail_by,
                )
            if result.decision != RiskDecision.NONE:
                broker.status = "EXIT_PENDING"
                self._claim_legs(broker.leg_ids)
                self._emit(TriggerEvent(f"BROKER_{result.decision.value}", "", trigger_ltp=broker.mtm, reason=broker.broker_scope_id))

    def _evaluate_strategies(self, strategy_ids: set[str]) -> None:
        for strategy_id in strategy_ids:
            strategy = self.strategies.get(strategy_id)
            if strategy is None or strategy.status != "ACTIVE":
                continue
            broker = self.brokers.get(strategy.broker_scope_id) if strategy.broker_scope_id else None
            if broker is not None and broker.status != "ACTIVE":
                continue  # §35: broker exit blocks new strategy-level exits/entries under it too
            result = strategy_risk.evaluate_strategy_risk(strategy)
            if result.decision != RiskDecision.NONE:
                strategy.status = "EXIT_PENDING"
                self._claim_legs(strategy.leg_ids)
                self._emit(TriggerEvent(f"STRATEGY_{result.decision.value}", strategy.strategy_id, trigger_ltp=strategy.mtm, reason=strategy.strategy_id))

    def _evaluate_runtime_risks(self, strategy_ids: set[str]) -> None:
        """User-set strategy / portfolio-group risk (risk/runtime_risk.py).
        A hit emits RUNTIME_RISK_<decision> (reason = scope key); main.py
        squares off every strategy in that scope."""
        touched_groups = set()
        for strategy_id in strategy_ids:
            strategy = self.strategies.get(strategy_id)
            if strategy is None:
                continue
            rr = self.runtime_risks.get(f"strategy:{strategy_id}")
            if rr is not None and rr.status == "ACTIVE" and strategy.status == "ACTIVE":
                self._apply_runtime_risk(rr, strategy.mtm, strategy_id)
            if strategy.group_id:
                grr = self.runtime_risks.get(f"group:{strategy.group_id}")
                if grr is not None and grr.status == "ACTIVE":
                    grr.member_mtm[strategy_id] = strategy.mtm
                    touched_groups.add(grr.key)
        for key in touched_groups:
            grr = self.runtime_risks[key]
            self._apply_runtime_risk(grr, sum(grr.member_mtm.values()), "")

    def _apply_runtime_risk(self, rr, mtm: float, strategy_id: str) -> None:
        before = (rr.peak, rr.floor, rr.trailing_activated)
        result = rr.evaluate(mtm)
        if (rr.peak, rr.floor, rr.trailing_activated) != before:
            self.runtime_risk_changed.add(rr.key)
        if result.decision != RiskDecision.NONE:
            rr.status = "HIT"
            rr.hit_reason = result.decision.value
            self.runtime_risk_changed.add(rr.key)
            self._emit(TriggerEvent(f"RUNTIME_RISK_{result.decision.value}", strategy_id, trigger_ltp=mtm, reason=rr.key))

    def _claim_legs(self, leg_ids: set[str]) -> list[str]:
        """Atomic ACTIVE -> EXIT_PENDING claim (§30) — only legs still ACTIVE
        get claimed; a leg already claimed by a higher-priority exit (broker,
        evaluated first) or already SL/TP-hit is left untouched."""
        claimed = []
        for leg_id in list(leg_ids):
            leg = self.legs.get(leg_id)
            if leg is not None and leg.status == "ACTIVE":
                leg.status = "EXIT_PENDING"
                claimed.append(leg_id)
        return claimed
