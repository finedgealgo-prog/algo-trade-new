"""
live_executor.py
───────────────────
REAL order execution for crypto (Delta Exchange) legs, sitting behind
OrderEngine. The risk engine's state machine stays synchronous and in-memory
(a leg is ACTIVE the moment it is registered, EXITED the moment its exit is
decided) — this module mirrors each of those decisions onto the real account
and then reconciles the engine with what actually filled:

  entry  -> marketable LIMIT (LTP ± LIVE_LIMIT_BUFFER_PCT) -> wait
            LIVE_FILL_TIMEOUT_SECONDS -> cancel + MARKET for the remainder.
            Filled: the leg's entry_price/qty become the real average fill and
            SL/TP are re-anchored on it. Nothing filled: the leg is aborted
            (no position exists, so no exit order is ever sent for it).
  exit   -> same limit-then-market sequence, always reduce_only (a stale or
            duplicate exit can only ever close, never open, a position), and
            retried; the leg's exit P&L becomes the real average fill.
            It never goes out before that leg's own entry order has finished,
            and only for the quantity that entry actually filled.

Broker I/O runs on a thread pool — never on the event loop, where it would
stall every other leg's SL/TP. Results come back to the loop thread via
call_soon_threadsafe, so all runtime mutation stays single-threaded.

Only crypto legs, and only with LIVE_ORDER_ENABLED=true. NSE legs stay
virtual paper fills.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from risk import mtm_engine, sl_tp_engine
from runtime.leg_runtime import LegRuntime
from shared.brokers.delta import client as delta_client
from shared.brokers.delta.order_adapter import DeltaOrderAdapter, DeltaOrderError, is_delta_broker_doc
from shared.config.settings import get_settings
from shared.logging.logger import get_logger

log = get_logger(__name__)

LIVE_ORDERS_COLLECTION = "algo2_live_orders"
_POLL_INTERVAL_SECONDS = 0.5
_EXIT_ATTEMPTS = 3


@dataclass
class ExecutionResult:
    requested: int
    filled: int = 0
    avg_price: float = 0.0
    order_ids: list[str] = field(default_factory=list)
    error: str = ""

    @property
    def status(self) -> str:
        if self.filled >= self.requested > 0:
            return "FILLED"
        return "PARTIAL" if self.filled > 0 else "FAILED"


class LiveOrderExecutor:
    def __init__(self, router, mongo=None, *, adapter_resolver: Callable[[str], DeltaOrderAdapter | None] | None = None,
                 settings=None, max_workers: int = 8, sleep: Callable[[float], None] = time.sleep) -> None:
        self.router = router
        self.mongo = mongo
        self.settings = settings or get_settings()
        self._resolve = adapter_resolver or self._adapter_from_broker_configuration
        self._adapters: dict[str, DeltaOrderAdapter | None] = {}
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="live_order")
        self._entry_futures: dict[str, Future] = {}
        self._entry_filled: dict[str, int] = {}
        self._sleep = sleep
        # Set by main.py: checkpoint + socket push + finalize for a strategy
        # whose legs changed after a real fill / rejection.
        self.on_strategy_changed: Callable[[str], None] = lambda strategy_id: None

    # ── gating ───────────────────────────────────────────────────────────────

    @property
    def enabled(self) -> bool:
        return bool(self.settings.live_order_enabled)

    def applies_to(self, token: str) -> bool:
        return self.enabled and bool(delta_client.asset_for_token(token))

    def _adapter_from_broker_configuration(self, broker_scope_id: str) -> DeltaOrderAdapter | None:
        if self.mongo is None:
            return None
        from bson import ObjectId

        try:
            doc = self.mongo.raw["broker_configuration"].find_one({"_id": ObjectId(broker_scope_id)})
        except Exception:
            return None
        if not doc or not is_delta_broker_doc(doc):
            return None
        api_key = str(doc.get("api_key") or "").strip()
        api_secret = str(doc.get("api_secret") or "").strip()
        if not api_key or not api_secret:
            return None
        return DeltaOrderAdapter(api_key, api_secret)

    def adapter_for(self, broker_scope_id: str) -> DeltaOrderAdapter | None:
        """Blocking on a cache miss (Mongo read) — worker threads, or
        asyncio.to_thread from async code."""
        if broker_scope_id not in self._adapters or self._adapters[broker_scope_id] is None:
            self._adapters[broker_scope_id] = self._resolve(broker_scope_id) if broker_scope_id else None
        return self._adapters[broker_scope_id]

    def check_ready(self, broker_scope_id: str) -> tuple[bool, str]:
        """Pre-activation gate (blocking): a live crypto activation must have
        a Delta account whose credentials actually authenticate."""
        adapter = self.adapter_for(broker_scope_id)
        if adapter is None:
            return False, "LIVE orders are on but this broker is not a Delta Exchange connection with api_key/api_secret"
        try:
            adapter.verify_credentials()
        except Exception as exc:
            self._adapters.pop(broker_scope_id, None)
            return False, f"Delta credentials check failed: {exc}"
        return True, ""

    # ── submission (event-loop thread) ───────────────────────────────────────

    def submit_entry(self, leg: LegRuntime) -> None:
        if not self.applies_to(leg.token):
            return
        loop = _running_loop()
        side = "sell" if leg.is_sell else "buy"
        future = self._pool.submit(self._run_entry, leg, side, leg.qty, leg.entry_price, loop)
        self._entry_futures[leg.leg_id] = future

    def submit_exit(self, leg: LegRuntime, reason: str) -> None:
        if not self.applies_to(leg.token):
            return
        loop = _running_loop()
        side = "buy" if leg.is_sell else "sell"
        ref_price = leg.current_price
        entry_future = self._entry_futures.get(leg.leg_id)

        def start(_done: Future | None = None) -> None:
            self._pool.submit(self._run_exit, leg, side, ref_price, reason, loop)

        if entry_future is not None and not entry_future.done():
            entry_future.add_done_callback(start)  # never close before the open has finished
        else:
            start()

    # ── worker threads ───────────────────────────────────────────────────────

    def _run_entry(self, leg: LegRuntime, side: str, size: int, ref_price: float, loop) -> None:
        adapter = self.adapter_for(leg.broker_scope_id)
        if adapter is None:
            result = ExecutionResult(requested=size, error="no Delta credentials for broker " + (leg.broker_scope_id or "-"))
        else:
            result = self._execute(adapter, leg.token, side, size, ref_price, reduce_only=False)
        self._entry_filled[leg.leg_id] = result.filled
        self._audit(leg, "ENTRY", side, result)
        self._call_on_loop(loop, self._on_entry_done, leg, result)

    def _run_exit(self, leg: LegRuntime, side: str, ref_price: float, reason: str, loop) -> None:
        size = self._entry_filled.get(leg.leg_id, leg.qty)
        if size <= 0:
            log.info("[LiveOrder] exit skipped leg=%s — entry never filled", leg.leg_id)
            return
        adapter = self.adapter_for(leg.broker_scope_id)
        result = ExecutionResult(requested=size, error="" if adapter is not None else "no Delta credentials")
        if adapter is not None:
            for attempt in range(1, _EXIT_ATTEMPTS + 1):
                remaining = size - result.filled
                step = self._execute(adapter, leg.token, side, remaining, ref_price, reduce_only=True)
                result = _merge(result, step)
                if result.filled >= size:
                    break
                log.error("[LiveOrder] exit attempt %d/%d incomplete leg=%s filled=%d/%d err=%s", attempt, _EXIT_ATTEMPTS, leg.leg_id, result.filled, size, step.error)
                self._sleep(1.0)
        if result.filled < size:
            log.critical(
                "[LiveOrder] EXIT NOT COMPLETED leg=%s token=%s filled=%d/%d reason=%s — position may still be OPEN on Delta, check the account",
                leg.leg_id, leg.token, result.filled, size, result.error,
            )
        self._audit(leg, f"EXIT:{reason}", side, result)
        self._call_on_loop(loop, self._on_exit_done, leg, result)

    def _execute(self, adapter: DeltaOrderAdapter, symbol: str, side: str, size: int, ref_price: float, *, reduce_only: bool) -> ExecutionResult:
        """Marketable limit, then market for whatever is left."""
        result = ExecutionResult(requested=size)
        if size <= 0:
            return result
        if ref_price > 0:
            buffer = self.settings.live_limit_buffer_pct / 100.0
            limit_price = ref_price * (1 + buffer) if side == "buy" else ref_price * (1 - buffer)
            result = _merge(result, self._place_and_wait(adapter, symbol, side, size, "limit_order", limit_price, reduce_only))
        remaining = size - result.filled
        if remaining > 0:
            result = _merge(result, self._place_and_wait(adapter, symbol, side, remaining, "market_order", 0.0, reduce_only))
        return result

    def _place_and_wait(self, adapter: DeltaOrderAdapter, symbol: str, side: str, size: int, order_type: str, limit_price: float, reduce_only: bool) -> ExecutionResult:
        result = ExecutionResult(requested=size)
        try:
            order = adapter.place_order(
                symbol=symbol, side=side, size=size, order_type=order_type, limit_price=limit_price,
                reduce_only=reduce_only, client_order_id=f"a2{uuid.uuid4().hex[:24]}",
            )
        except Exception as exc:
            result.error = str(exc)
            log.warning("[LiveOrder] %s %s %s x%d rejected: %s", order_type, side, symbol, size, exc)
            return result
        summary = DeltaOrderAdapter.order_summary(order)
        result.order_ids.append(summary["order_id"])
        deadline = time.monotonic() + self.settings.live_fill_timeout_seconds
        while summary["status"] == "OPEN" and time.monotonic() < deadline:
            self._sleep(_POLL_INTERVAL_SECONDS)
            try:
                summary = DeltaOrderAdapter.order_summary(adapter.get_order(summary["order_id"]))
            except Exception as exc:
                log.warning("[LiveOrder] order poll failed id=%s: %s", summary["order_id"], exc)
        if summary["status"] == "OPEN":
            try:
                summary = DeltaOrderAdapter.order_summary(adapter.cancel_order(summary["order_id"], summary["product_id"]))
            except Exception as exc:
                log.warning("[LiveOrder] cancel failed id=%s: %s", summary["order_id"], exc)
                try:
                    summary = DeltaOrderAdapter.order_summary(adapter.get_order(summary["order_id"]))
                except Exception:
                    pass
        result.filled = min(size, summary["filled"])
        result.avg_price = summary["avg_price"]
        if result.filled < size:
            result.error = summary["reason"] or f"{order_type} unfilled {size - result.filled}/{size}"
        return result

    def _audit(self, leg: LegRuntime, action: str, side: str, result: ExecutionResult) -> None:
        log.info("[LiveOrder] %s leg=%s %s %s status=%s filled=%d/%d avg=%s ids=%s err=%s",
                 action, leg.leg_id, side, leg.token, result.status, result.filled, result.requested,
                 result.avg_price, result.order_ids, result.error)
        if self.mongo is None:
            return
        try:
            self.mongo.raw[LIVE_ORDERS_COLLECTION].insert_one({
                "ts": datetime.now(timezone.utc).isoformat(), "action": action, "side": side,
                "leg_id": leg.leg_id, "strategy_id": leg.strategy_id, "user_id": leg.user_id,
                "broker_scope_id": leg.broker_scope_id, "symbol": leg.token,
                "requested": result.requested, "filled": result.filled, "avg_price": result.avg_price,
                "status": result.status, "order_ids": result.order_ids, "error": result.error,
            })
        except Exception:
            log.exception("[LiveOrder] audit write failed leg=%s", leg.leg_id)

    @staticmethod
    def _call_on_loop(loop, fn, *args) -> None:
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(fn, *args)
        else:
            fn(*args)

    # ── reconciliation (event-loop thread) ───────────────────────────────────

    def _propagate(self, leg: LegRuntime, delta: float) -> None:
        if not delta:
            return
        strategy = self.router.strategies.get(leg.strategy_id)
        if strategy is not None:
            strategy.mtm += delta
        broker = self.router.brokers.get(leg.broker_scope_id) if leg.broker_scope_id else None
        if broker is not None:
            broker.mtm += delta

    def _on_entry_done(self, leg: LegRuntime, result: ExecutionResult) -> None:
        if result.filled <= 0:
            log.error("[LiveOrder] ENTRY REJECTED leg=%s token=%s — leg aborted (no position): %s", leg.leg_id, leg.token, result.error)
            if leg.status not in ("EXITED", "SL_HIT", "TP_HIT"):
                leg.current_price = leg.entry_price
                self._propagate(leg, mtm_engine.apply_leg_pnl_delta(leg))
                leg.status = "EXITED"
            self.on_strategy_changed(leg.strategy_id)
            return
        leg.qty = result.filled
        if result.avg_price > 0:
            leg.entry_price = result.avg_price
        if leg.status == "ACTIVE":
            sl_tp_engine.initialize_sl_tp(leg)  # re-anchor SL/TP on the real fill
        self._propagate(leg, mtm_engine.apply_leg_pnl_delta(leg))
        self.on_strategy_changed(leg.strategy_id)

    def _on_exit_done(self, leg: LegRuntime, result: ExecutionResult) -> None:
        if result.filled > 0 and result.avg_price > 0:
            leg.current_price = result.avg_price
            self._propagate(leg, mtm_engine.apply_leg_pnl_delta(leg))
        self.on_strategy_changed(leg.strategy_id)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True, cancel_futures=False)


def _merge(total: ExecutionResult, step: ExecutionResult) -> ExecutionResult:
    filled = total.filled + step.filled
    avg = ((total.avg_price * total.filled) + (step.avg_price * step.filled)) / filled if filled else 0.0
    return ExecutionResult(
        requested=total.requested, filled=filled, avg_price=avg,
        order_ids=total.order_ids + step.order_ids, error=step.error or total.error,
    )


def _running_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None
