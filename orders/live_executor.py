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

Only crypto legs of strategies activated in "live" mode (AlgoTrade2.tsx),
and only with LIVE_ORDER_ENABLED=true (master switch). FastForward2's
"fast-forward" strategies and NSE legs stay virtual paper fills.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from types import SimpleNamespace
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
# Exchange-resident protective stop per live leg (survives a restart).
LIVE_STOPS_COLLECTION = "algo2_live_stops"
_STOP_POLL_SECONDS = 3.0
_STOP_EDIT_MIN_INTERVAL = 1.0
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
                 settings=None, max_workers: int = 8, sleep: Callable[[float], None] = time.sleep,
                 notifier: Callable[[str, str, str], None] | None = None) -> None:
        self.router = router
        self.mongo = mongo
        self.settings = settings or get_settings()
        self._resolve = adapter_resolver or self._adapter_from_broker_configuration
        self._adapters: dict[str, DeltaOrderAdapter | None] = {}
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="live_order")
        self._entry_futures: dict[str, Future] = {}
        # Order type each live leg trades with, fixed at its entry so its exit
        # uses the same even after the strategy is finalized.
        self._leg_order_type: dict[str, str] = {}
        self._entry_filled: dict[str, int] = {}
        self._sleep = sleep
        # (user_id, event_type, message) -> Telegram; blocking, worker threads only.
        self._notifier = notifier or _telegram_notify
        # Set by main.py: checkpoint + socket push + finalize for a strategy
        # whose legs changed after a real fill / rejection.
        self.on_strategy_changed: Callable[[str], None] = lambda strategy_id: None
        # Set by main.py: a Delta-side stop filled (exchange SL hit — e.g.
        # while this server was down) -> exit that leg in the engine. The
        # exit then sends no order (see _broker_exited).
        self.on_broker_stop_filled: Callable[[str, float], None] = lambda leg_id, price: None

        # Protective stops: leg_id -> {order_id, product_id, stop_price, size,
        # symbol, side, broker_scope_id, user_id, strategy_id, order_type}.
        self._stops: dict[str, dict[str, Any]] = {}
        self._stop_futures: dict[str, Future] = {}
        self._stop_lock = threading.Lock()
        self._stop_edit_pending: set[str] = set()
        self._stop_last_edit: dict[str, float] = {}
        # leg_id -> ExecutionResult of a Delta-side stop that already closed it.
        self._broker_exited: dict[str, ExecutionResult] = {}
        self._loop = None
        # leg_id -> consecutive polls Delta reported no position for it.
        self._flat_polls: dict[str, int] = {}
        self._poller_stop = threading.Event()
        self._poller: threading.Thread | None = None

    # ── gating ───────────────────────────────────────────────────────────────

    @property
    def enabled(self) -> bool:
        return bool(self.settings.live_order_enabled)

    def applies_to(self, leg: LegRuntime) -> bool:
        """Real orders only for a crypto leg of a strategy activated in
        "live" mode (AlgoTrade2). FastForward2's "fast-forward" strategies
        stay paper fills even with LIVE_ORDER_ENABLED on."""
        if not self.enabled or not delta_client.asset_for_token(leg.token):
            return False
        strategy = self.router.strategies.get(leg.strategy_id)
        return strategy is not None and strategy.activation_mode == "live"

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

    def _broker_name(self, broker_scope_id: str) -> str:
        if self.mongo is None:
            return broker_scope_id or "-"
        try:
            from bson import ObjectId

            doc = self.mongo.raw["broker_configuration"].find_one({"_id": ObjectId(broker_scope_id)}, {"name": 1, "broker_name": 1, "display_name": 1})
        except Exception:
            doc = None
        return str((doc or {}).get("display_name") or (doc or {}).get("name") or (doc or {}).get("broker_name") or broker_scope_id or "-")

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
            return False, (f"Real orders need a Delta Exchange broker — '{self._broker_name(broker_scope_id)}' is not a Delta "
                           f"Exchange connection with api_key/api_secret. Select your Delta broker (e.g. Broker.DeltaExchange) and activate again.")
        try:
            adapter.verify_credentials()
        except Exception as exc:
            self._adapters.pop(broker_scope_id, None)
            return False, f"Delta credentials check failed: {exc}"
        return True, ""

    # ── submission (event-loop thread) ───────────────────────────────────────

    def submit_entry(self, leg: LegRuntime) -> None:
        if not self.applies_to(leg):
            return
        loop = _running_loop()
        self._loop = loop or self._loop
        side = "sell" if leg.is_sell else "buy"
        self._leg_order_type[leg.leg_id] = self._order_type_for(leg)
        leg.awaiting_live_fill = True
        future = self._pool.submit(self._run_entry, leg, side, leg.qty, leg.entry_price, loop)
        self._entry_futures[leg.leg_id] = future

    def submit_exit(self, leg: LegRuntime, reason: str) -> None:
        # A leg whose entry went to the real account always gets its real
        # exit, even if its strategy is no longer registered by now.
        if not self.applies_to(leg) and leg.leg_id not in self._entry_futures:
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

    # ── protective stop-loss on Delta ────────────────────────────────────────

    def _schedule_stop(self, leg: LegRuntime) -> None:
        """Loop thread, right after a real entry fill: put the leg's SL on
        Delta as a resting reduce_only stop, so it holds even if this server
        goes down. Spot/underlying-based SLs can't be expressed as an option
        stop — those stay engine-only (with an alert)."""
        if not leg.current_sl_price or leg.current_sl_price <= 0:
            return
        if sl_tp_engine.is_underlying_config(leg.leg_cfg.get("LegStopLoss") or {}):
            self._pool.submit(self._alert, leg, "LIVE_STOP_ENGINE_ONLY",
                              f"{leg.token}: SL is underlying/spot based — NOT placed on Delta, engine-only (needs this server running).")
            return
        self._stop_futures[leg.leg_id] = self._pool.submit(self._place_stop, leg, float(leg.current_sl_price), int(leg.qty))

    def _replace_stop(self, leg_id: str) -> None:
        """Loop thread — re-place a leg's stop that was cancelled outside the
        engine, while the leg is still open."""
        leg = self.router.legs.get(leg_id)
        if leg is not None and leg.status == "ACTIVE" and leg_id not in self._stops:
            self._schedule_stop(leg)

    def _stop_limit_price(self, leg: LegRuntime, side: str, stop_price: float) -> float:
        """Delta rejects stop-MARKET orders on options ("unsupported" —
        confirmed live), so the stop is always a stop-limit: limit set
        LIVE_MAX_SLIPPAGE_PCT past the stop so it fills like a market order
        in all but a gap through that band."""
        slip = self.settings.live_max_slippage_pct / 100.0
        return stop_price * (1 + slip) if side == "buy" else stop_price * (1 - slip)

    @staticmethod
    def _stop_client_id(leg_id: str) -> str:
        """Same id every time for a leg's stop — a second placement attempt
        (restart / reload racing the first) finds and adopts the existing
        stop instead of stacking a duplicate on Delta."""
        import hashlib

        return "a2s" + hashlib.sha1(leg_id.encode()).hexdigest()[:28]

    def _place_stop(self, leg: LegRuntime, stop_price: float, size: int) -> None:
        adapter = self.adapter_for(leg.broker_scope_id)
        side = "buy" if leg.is_sell else "sell"
        if adapter is None or size <= 0:
            return
        client_id = self._stop_client_id(leg.leg_id)
        try:
            existing = next((o for o in adapter.open_orders() if o.get("client_order_id") == client_id), None)
        except Exception as exc:
            log.warning("[LiveOrder] open-orders check failed leg=%s: %s", leg.leg_id, exc)
            existing = None
        try:
            if existing is not None:
                order = existing
                stop_price = float(existing.get("stop_price") or stop_price)
                log.info("[LiveOrder] adopted existing Delta stop leg=%s id=%s", leg.leg_id, existing.get("id"))
            else:
                order = adapter.place_stop_order(
                    symbol=leg.token, side=side, size=size, stop_price=stop_price,
                    limit_price=self._stop_limit_price(leg, side, stop_price), client_order_id=client_id,
                )
        except Exception as exc:
            log.error("[LiveOrder] STOP NOT PLACED leg=%s %s: %s", leg.leg_id, leg.token, exc)
            self._alert(leg, "LIVE_STOP_FAILED",
                        f"⚠️ SL order NOT placed on Delta — {leg.token} {side.upper()} x{size} stop {stop_price:g}.\n"
                        f"Engine SL still active, but only while this server runs.\n{exc}")
            return
        summary = DeltaOrderAdapter.order_summary(order)
        rec = {
            "order_id": summary["order_id"], "product_id": summary["product_id"], "stop_price": stop_price,
            "size": size, "symbol": leg.token, "side": side, "broker_scope_id": leg.broker_scope_id,
            "user_id": leg.user_id, "strategy_id": leg.strategy_id, "order_type": self._order_type_for(leg),
        }
        with self._stop_lock:
            self._stops[leg.leg_id] = rec
        self._persist_stop(leg.leg_id, rec, "ACTIVE")
        log.info("[LiveOrder] STOP placed leg=%s %s %s x%d stop=%s id=%s", leg.leg_id, side, leg.token, size, stop_price, rec["order_id"])

    def sync_stop(self, leg: LegRuntime) -> None:
        """Loop thread — the engine moved this leg's SL (trailing): move the
        Delta stop too. Throttled; the worker always sends the latest SL."""
        rec = self._stops.get(leg.leg_id)
        if rec is None or leg.status != "ACTIVE" or not leg.current_sl_price:
            return
        if abs(float(leg.current_sl_price) - float(rec["stop_price"])) < 1e-9 or leg.leg_id in self._stop_edit_pending:
            return
        if time.monotonic() - self._stop_last_edit.get(leg.leg_id, 0.0) < _STOP_EDIT_MIN_INTERVAL:
            return  # next tick retries
        self._stop_edit_pending.add(leg.leg_id)
        self._pool.submit(self._edit_stop, leg)

    def _edit_stop(self, leg: LegRuntime) -> None:
        try:
            rec = self._stops.get(leg.leg_id)
            adapter = self.adapter_for(leg.broker_scope_id)
            target = float(leg.current_sl_price or 0)
            if rec is None or adapter is None or target <= 0 or leg.status != "ACTIVE":
                return
            adapter.edit_stop_order(rec["order_id"], rec["product_id"], rec["symbol"], rec["side"], target,
                                    self._stop_limit_price(leg, rec["side"], target))
            rec["stop_price"] = target
            self._persist_stop(leg.leg_id, rec, "ACTIVE")
            log.info("[LiveOrder] STOP moved leg=%s -> %s", leg.leg_id, target)
        except Exception as exc:
            log.error("[LiveOrder] stop edit failed leg=%s: %s", leg.leg_id, exc)
            self._alert(leg, "LIVE_STOP_EDIT_FAILED", f"Trailing SL NOT updated on Delta for {leg.token} (wanted {leg.current_sl_price:g}). Engine still trails it.\n{exc}")
        finally:
            self._stop_last_edit[leg.leg_id] = time.monotonic()
            self._stop_edit_pending.discard(leg.leg_id)

    def _cancel_stop(self, adapter: DeltaOrderAdapter, leg: LegRuntime) -> ExecutionResult:
        """Worker thread, before an engine exit: take the leg's resting stop
        off Delta. Returns what that stop itself filled (0 if it never
        triggered) — that part of the position is already closed."""
        future = self._stop_futures.pop(leg.leg_id, None)
        if future is not None:
            try:
                future.result(timeout=30)  # never race a stop still being placed
            except Exception:
                pass
        with self._stop_lock:
            rec = self._stops.pop(leg.leg_id, None)
        out = ExecutionResult(requested=0)
        if rec is None:
            return out
        summary = None
        try:
            summary = DeltaOrderAdapter.order_summary(adapter.cancel_order(rec["order_id"], rec["product_id"]))
        except Exception as exc:
            log.warning("[LiveOrder] stop cancel failed leg=%s id=%s: %s — checking its state", leg.leg_id, rec["order_id"], exc)
            for _ in range(5):
                try:
                    summary = DeltaOrderAdapter.order_summary(adapter.get_order(rec["order_id"]))
                except Exception:
                    summary = None
                if summary is not None and summary["status"] != "OPEN":
                    break
                self._sleep(1.0)
        filled = int(summary["filled"]) if summary else 0
        self._persist_stop(leg.leg_id, rec, "FILLED" if filled else "CANCELLED")
        if filled:
            out = ExecutionResult(requested=0, filled=filled, avg_price=summary["avg_price"], order_ids=[rec["order_id"]])
            log.info("[LiveOrder] stop had already filled leg=%s %d @ %s", leg.leg_id, filled, summary["avg_price"])
        return out

    def _persist_stop(self, leg_id: str, rec: dict[str, Any], status: str) -> None:
        if self.mongo is None:
            return
        try:
            self.mongo.raw[LIVE_STOPS_COLLECTION].update_one(
                {"_id": leg_id},
                {"$set": {**rec, "status": status, "updated_at": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            )
        except Exception:
            log.exception("[LiveOrder] stop persist failed leg=%s", leg_id)

    def load_stops(self) -> int:
        """Startup (blocking): resume watching the stops still resting on
        Delta for legs the engine recovered."""
        if self.mongo is None:
            return 0
        try:
            docs = list(self.mongo.raw[LIVE_STOPS_COLLECTION].find({"status": "ACTIVE"}))
        except Exception:
            log.exception("[LiveOrder] stop reload failed")
            return 0
        with self._stop_lock:
            for doc in docs:
                leg_id = doc.pop("_id")
                doc.pop("status", None)
                doc.pop("updated_at", None)
                self._stops[leg_id] = doc
                if doc.get("order_type"):
                    self._leg_order_type[leg_id] = doc["order_type"]
        log.info("[LiveOrder] resumed %d Delta stop(s)", len(docs))
        return len(docs)

    def ensure_recovered_stops(self) -> int:
        """Startup (blocking), after load_stops: a recovered live leg that
        really holds a position on Delta but has no resting stop (its
        placement failed, or it entered before stops existed) gets one now."""
        placed = 0
        for leg in list(self.router.legs.values()):
            if leg.status != "ACTIVE" or leg.leg_id in self._stops or not self.applies_to(leg):
                continue
            size = self._entry_filled.get(leg.leg_id) or self._recorded_entry_fill(leg.leg_id)
            if size <= 0 or not leg.current_sl_price:
                continue
            self._entry_filled[leg.leg_id] = size
            leg.qty = size
            self._schedule_stop(leg)
            placed += 1
        log.info("[LiveOrder] protective stops scheduled for %d recovered leg(s)", placed)
        return placed

    def start_stop_poller(self, loop) -> None:
        self._loop = loop
        if self._poller is None:
            self._poller = threading.Thread(target=self._poll_stops, name="live_stop_poller", daemon=True)
            self._poller.start()

    def _poll_stops(self) -> None:
        while not self._poller_stop.wait(_STOP_POLL_SECONDS):
            self.check_stops_once()

    def check_stops_once(self) -> None:
        """Did Delta fire (or someone cancel) a resting stop? A filled stop
        exits the leg in the engine with that fill — no second order. Then:
        was the position closed by hand on Delta (app / website)?"""
        with self._stop_lock:
            items = list(self._stops.items())
        self._check_stop_orders(items)
        self._check_closed_outside()

    def _check_closed_outside(self) -> None:
        """A live leg whose contract Delta reports FLAT on two polls in a
        row was closed outside the engine: take its stop off Delta (else it
        would hit a position opened later in that contract), exit the leg in
        the engine with no order, alert. Two polls, so a momentary positions-
        API lag right after an entry can't trigger it."""
        with self._stop_lock:
            items = list(self._stops.items())
        by_broker: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for leg_id, rec in items:
            by_broker.setdefault(rec.get("broker_scope_id", ""), []).append((leg_id, rec))
        for broker_scope_id, recs in by_broker.items():
            adapter = self.adapter_for(broker_scope_id)
            if adapter is None:
                continue
            try:
                open_symbols = {
                    (p.get("product") or {}).get("symbol") or p.get("product_symbol")
                    for p in adapter.positions() if float(p.get("size") or 0) != 0
                }
            except Exception as exc:
                log.warning("[LiveOrder] positions poll failed: %s", exc)
                continue
            for leg_id, rec in recs:
                if rec["symbol"] in open_symbols:
                    self._flat_polls.pop(leg_id, None)
                    continue
                self._flat_polls[leg_id] = self._flat_polls.get(leg_id, 0) + 1
                if self._flat_polls[leg_id] < 2:
                    continue
                self._flat_polls.pop(leg_id, None)
                with self._stop_lock:
                    if self._stops.pop(leg_id, None) is None:
                        continue  # an engine exit took it meanwhile
                try:
                    adapter.cancel_order(rec["order_id"], rec["product_id"])
                except Exception as exc:
                    log.warning("[LiveOrder] stop cancel (closed outside) failed leg=%s: %s", leg_id, exc)
                self._persist_stop(leg_id, rec, "CANCELLED")
                self._broker_exited[leg_id] = ExecutionResult(requested=int(rec["size"]), filled=int(rec["size"]), error="closed outside the engine")
                self._alert(SimpleNamespace(leg_id=leg_id, user_id=rec.get("user_id", "")), "LIVE_CLOSED_OUTSIDE",
                            f"{rec['symbol']}: position closed on Delta outside the engine — its SL order cancelled and the leg marked exited.")
                self._call_on_loop(self._loop, self.on_broker_stop_filled, leg_id, 0.0)

    def _check_stop_orders(self, items) -> None:
        for leg_id, rec in items:
            adapter = self.adapter_for(rec.get("broker_scope_id", ""))
            if adapter is None:
                continue
            try:
                summary = DeltaOrderAdapter.order_summary(adapter.get_order(rec["order_id"]))
            except Exception as exc:
                log.warning("[LiveOrder] stop poll failed leg=%s: %s", leg_id, exc)
                continue
            if summary["status"] == "OPEN":
                continue
            with self._stop_lock:
                if self._stops.pop(leg_id, None) is None:
                    continue  # an engine exit took it meanwhile
            alert_leg = SimpleNamespace(leg_id=leg_id, user_id=rec.get("user_id", ""))
            if summary["filled"] > 0:
                result = ExecutionResult(requested=int(rec["size"]), filled=int(summary["filled"]), avg_price=summary["avg_price"], order_ids=[rec["order_id"]])
                self._broker_exited[leg_id] = result
                self._persist_stop(leg_id, rec, "FILLED")
                self._audit(SimpleNamespace(leg_id=leg_id, strategy_id=rec.get("strategy_id", ""), user_id=rec.get("user_id", ""),
                                            broker_scope_id=rec.get("broker_scope_id", ""), token=rec["symbol"]),
                            "EXIT:BROKER_SL", rec["side"], result)
                self._alert(alert_leg, "LIVE_BROKER_SL_HIT",
                            f"Delta SL hit — {rec['symbol']} {rec['side'].upper()} {result.filled}/{rec['size']} @ {result.avg_price:g} (stop {rec['stop_price']:g}).")
                self._call_on_loop(self._loop, self.on_broker_stop_filled, leg_id, result.avg_price)
            else:
                self._persist_stop(leg_id, rec, "CANCELLED")
                self._alert(alert_leg, "LIVE_STOP_CANCELLED",
                            f"⚠️ SL order for {rec['symbol']} was cancelled on Delta (not by the engine) — placing it again.")
                # The position is still open: never leave it without its stop.
                self._call_on_loop(self._loop, self._replace_stop, leg_id)

    # ── worker threads ───────────────────────────────────────────────────────

    def _run_entry(self, leg: LegRuntime, side: str, size: int, ref_price: float, loop) -> None:
        adapter = self.adapter_for(leg.broker_scope_id)
        if adapter is None:
            result = ExecutionResult(requested=size, error="no Delta credentials for broker " + (leg.broker_scope_id or "-"))
        else:
            result = self._execute(adapter, leg.token, side, size, ref_price, reduce_only=False, order_type=self._order_type_for(leg))
        self._entry_filled[leg.leg_id] = result.filled
        self._audit(leg, "ENTRY", side, result)
        if result.filled <= 0:
            self._alert(leg, "LIVE_ENTRY_REJECTED", f"Entry NOT placed — {leg.token} {side.upper()} x{size}: leg dropped.\n{result.error}")
        elif result.filled < size:
            self._alert(leg, "LIVE_ENTRY_PARTIAL", f"Entry partly filled — {leg.token} {side.upper()} {result.filled}/{size} @ {result.avg_price:g}; running with {result.filled}.\n{result.error}")
        self._call_on_loop(loop, self._on_entry_done, leg, result)

    @staticmethod
    def _position_is_flat(adapter: DeltaOrderAdapter, symbol: str) -> bool:
        """True only when Delta positively reports no open position in this
        contract; any doubt (API error) -> False, so the exit is still sent."""
        try:
            positions = adapter.positions()
        except Exception as exc:
            log.warning("[LiveOrder] positions check failed for %s: %s", symbol, exc)
            return False
        for p in positions:
            sym = (p.get("product") or {}).get("symbol") or p.get("product_symbol")
            if sym == symbol and float(p.get("size") or 0) != 0:
                return False
        return True

    def _recorded_entry_fill(self, leg_id: str) -> int:
        """Real quantity this leg's entry filled, from the audit trail —
        for a leg entered before this process started (restart recovery).
        0 when no real entry is on record: the exit must then NOT be sent,
        or a reduce_only order would close some other position the account
        holds in the same contract."""
        if self.mongo is None:
            return 0
        try:
            doc = self.mongo.raw[LIVE_ORDERS_COLLECTION].find_one({"leg_id": leg_id, "action": "ENTRY"}, sort=[("ts", -1)])
        except Exception:
            log.exception("[LiveOrder] entry-fill lookup failed leg=%s", leg_id)
            return 0
        return int((doc or {}).get("filled") or 0)

    def _run_exit(self, leg: LegRuntime, side: str, ref_price: float, reason: str, loop) -> None:
        size = self._entry_filled[leg.leg_id] if leg.leg_id in self._entry_filled else self._recorded_entry_fill(leg.leg_id)
        if size <= 0:
            log.info("[LiveOrder] exit skipped leg=%s — entry never filled", leg.leg_id)
            return
        adapter = self.adapter_for(leg.broker_scope_id)
        result = ExecutionResult(requested=size, error="" if adapter is not None else "no Delta credentials")
        broker_exit = self._broker_exited.pop(leg.leg_id, None)
        if broker_exit is not None:
            # Delta's own stop already closed this position — nothing to send.
            result = broker_exit
        elif adapter is not None:
            # The resting stop must go first: cancelled, or — if it already
            # triggered — its fill IS (part of) this exit.
            result = _merge(result, self._cancel_stop(adapter, leg))
        if adapter is not None and result.filled < size and self._position_is_flat(adapter, leg.token):
            # Closed outside the engine (Delta app / website) — sending a
            # reduce_only order now would only be rejected.
            log.warning("[LiveOrder] exit leg=%s: no %s position on Delta — already closed outside the engine", leg.leg_id, leg.token)
            self._alert(leg, "LIVE_ALREADY_FLAT", f"{leg.token}: no position on Delta at exit ({reason}) — it was closed outside the engine. Leg marked exited; no order sent.")
            result = _merge(result, ExecutionResult(requested=0, filled=size - result.filled, avg_price=ref_price, error="already flat on Delta"))
        if adapter is not None and result.filled < size:
            for attempt in range(1, _EXIT_ATTEMPTS + 1):
                remaining = size - result.filled
                step = self._execute(adapter, leg.token, side, remaining, ref_price, reduce_only=True, order_type=self._order_type_for(leg))
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
            self._alert(leg, "LIVE_EXIT_FAILED",
                        f"⚠️ EXIT NOT COMPLETED — {leg.token} {side.upper()} {result.filled}/{size} filled ({reason}).\n"
                        f"Position may still be OPEN on Delta — check the account now.\n{result.error}")
        self._audit(leg, f"EXIT:{reason}", side, result)
        self._call_on_loop(loop, self._on_exit_done, leg, result)

    def _order_type_for(self, leg: LegRuntime) -> str:
        """The leg's strategy's Order Type (Edit Setup / Edit Config), else
        the server default LIVE_ORDER_TYPE."""
        if leg.leg_id in self._leg_order_type:
            return self._leg_order_type[leg.leg_id]
        strategy = self.router.strategies.get(leg.strategy_id)
        chosen = (getattr(strategy, "live_order_type", "") or "").lower() if strategy is not None else ""
        return chosen if chosen in ("market", "limit") else (getattr(self.settings, "live_order_type", "market") or "market")

    def _execute(self, adapter: DeltaOrderAdapter, symbol: str, side: str, size: int, ref_price: float, *, reduce_only: bool, order_type: str = "") -> ExecutionResult:
        """LIVE_ORDER_TYPE=market (default): one market order.

        LIVE_ORDER_TYPE=limit: marketable limit (LTP ± buffer), then a second limit at the max
        slippage price (LTP ± LIVE_MAX_SLIPPAGE_PCT). An entry stops there —
        whatever is still unfilled is dropped rather than sent as a market
        order (a market sell into a thin Delta option book filled at 0.2
        against an LTP of 0.52). An exit must close the position, so only it
        falls back to market for the remainder."""
        result = ExecutionResult(requested=size)
        if size <= 0:
            return result
        if (order_type or getattr(self.settings, "live_order_type", "market")) != "limit":
            return self._place_and_wait(adapter, symbol, side, size, "market_order", 0.0, reduce_only)
        if ref_price > 0:
            for pct in (self.settings.live_limit_buffer_pct, self.settings.live_max_slippage_pct):
                remaining = size - result.filled
                if remaining <= 0:
                    break
                factor = pct / 100.0
                limit_price = ref_price * (1 + factor) if side == "buy" else ref_price * (1 - factor)
                result = _merge(result, self._place_and_wait(adapter, symbol, side, remaining, "limit_order", limit_price, reduce_only))
        elif not reduce_only:
            result.error = "no reference price for the slippage-capped entry limit"
            return result
        remaining = size - result.filled
        if remaining > 0:
            if not reduce_only:
                result.error = (result.error or "") + f" | entry not filled within {self.settings.live_max_slippage_pct:g}% of LTP {ref_price:g} — remaining {remaining} dropped"
                return result
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

    def _alert(self, leg: LegRuntime, event_type: str, message: str) -> None:
        """Best-effort Telegram to the leg's user — never raises."""
        try:
            self._notifier(leg.user_id, event_type, f"[Algo Trade 2.0] {message}")
        except Exception:
            log.exception("[LiveOrder] alert failed leg=%s event=%s", leg.leg_id, event_type)

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
        leg.awaiting_live_fill = False
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
            # Entry price is now the real Delta fill; SL / Target / Trail are
            # computed from it, and only then is the stop placed on Delta.
            sl_tp_engine.initialize_sl_tp(leg)
        self._propagate(leg, mtm_engine.apply_leg_pnl_delta(leg))
        if leg.status == "ACTIVE":
            self._schedule_stop(leg)
        self.on_strategy_changed(leg.strategy_id)

    def _on_exit_done(self, leg: LegRuntime, result: ExecutionResult) -> None:
        if result.filled > 0 and result.avg_price > 0:
            leg.current_price = result.avg_price
            self._propagate(leg, mtm_engine.apply_leg_pnl_delta(leg))
        self.on_strategy_changed(leg.strategy_id)

    def shutdown(self) -> None:
        self._poller_stop.set()
        self._pool.shutdown(wait=True, cancel_futures=False)


def _telegram_notify(user_id: str, event_type: str, message: str) -> None:
    from features.telegram_notifier import notify_user_for  # type: ignore

    sent, _ = notify_user_for(user_id or None, event_type, message, category="algo")
    if not sent:
        log.warning("[LiveOrder] telegram not sent event=%s user=%s (notifications off / no chat)", event_type, user_id)


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
