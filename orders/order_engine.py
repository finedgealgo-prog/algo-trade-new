"""
order_engine.py
──────────────────
Turns TriggerEvents into orders — master-doc §53 "ExitCoordinator": risk
evaluation (token_router) decides WHAT should exit, this module actually
executes it, kept as a separate concern ("do not directly loop broker API
inside RiskEvaluator").

LIVE ORDERS: with LIVE_ORDER_ENABLED=true, crypto (Delta) legs are also
mirrored onto the real account by orders/live_executor.py (entries via
submit_live_entry, exits from _exit_leg below). Everything else — and every
leg when the flag is off — keeps the virtual behavior described here:

VIRTUAL ORDERS (default):
every order goes through VirtualBrokerAdapter — instant simulated fill at
the leg's current price, no real broker call. Matches the old system's
LIVE_ORDER_STATUS=false behavior exactly (shared/features/live_order_manager.py's
_build_simulated_live_order_id / _is_live_order_punch_enabled — "when off,
orders are simulated with instant COMPLETE status, no broker call at all").
Real-order concerns (MPP protection pricing, tick rounding, SL-MKT→SL-LMT
conversion, retry-then-market-order-then-square-off escalation — all found
in live_order_manager.py) are deliberately NOT built yet; this module
depends only on the BrokerAdapter interface, so swapping VirtualBrokerAdapter
for DhanAdapter later needs no rewrite here.

NOT wired yet: LAZY_TRIGGERED / RECOST_TRIGGERED -> entry order. That needs
Phase 5's strike_resolver hooked to each armed watcher's frozen
strike/expiry plus execution.entry_guard's ACTIVE check — flagged, not faked.
"""

from __future__ import annotations

from datetime import datetime, timezone

from orders.idempotency import IdempotencyRegistry, make_idempotency_key
from orders.slippage import slipped_price
from risk import mtm_engine
from runtime.order_runtime import OrderRuntime
from shared.brokers.base import BrokerAdapter
from shared.brokers.delta import client as delta_client
from shared.logging.logger import get_logger
from token_router import TokenRouter, TriggerEvent

log = get_logger(__name__)

_LEG_EXIT_EVENTS = {"SL_HIT", "TP_HIT"}


def order_venue(tradingsymbol: str) -> dict[str, str]:
    """exchange/product for an order on `tradingsymbol` — a Delta contract
    (BTCUSD/ETHUSD perpetual or a C-/P- option symbol) is not an NFO/MIS
    instrument, so it must not be labelled as one on the order record."""
    if delta_client.is_crypto_underlying_token(tradingsymbol):
        return {"exchange": "DELTA", "product": "NRML"}
    return {"exchange": "NFO", "product": "MIS"}


class OrderEngine:
    def __init__(self, router: TokenRouter, broker: BrokerAdapter, live=None) -> None:
        self.router = router
        self.broker = broker
        # orders/live_executor.LiveOrderExecutor — mirrors crypto entries/exits
        # onto the real Delta account when LIVE_ORDER_ENABLED; None in tests.
        self.live = live
        self.idempotency = IdempotencyRegistry()
        self.orders: dict[str, OrderRuntime] = {}
        router.add_trigger_listener(self.on_trigger)

    def on_trigger(self, event: TriggerEvent) -> None:
        if event.event_type in _LEG_EXIT_EVENTS:
            self._exit_leg(event.leg_id, reason=event.event_type)
        elif event.event_type.startswith("STRATEGY_"):
            strategy = self.router.strategies.get(event.strategy_id)
            if strategy:
                self._exit_legs(strategy.leg_ids, reason=event.event_type)
        elif event.event_type.startswith("BROKER_"):
            broker_runtime = self.router.brokers.get(event.reason)  # token_router puts broker_scope_id in `reason`
            if broker_runtime:
                self._exit_legs(broker_runtime.leg_ids, reason=event.event_type)

    def _exit_legs(self, leg_ids, reason: str) -> None:
        for leg_id in list(leg_ids):
            leg = self.router.legs.get(leg_id)
            if leg is not None and leg.status == "EXIT_PENDING":
                self._exit_leg(leg_id, reason)

    def submit_live_entry(self, leg) -> None:
        """Call right after router.register_leg() at every NEW-entry site
        (never on recovery) — sends the real entry order for a crypto leg
        when live orders are on; a no-op otherwise."""
        if self.live is not None:
            self.live.submit_entry(leg)

    def manual_square_off(self, leg_id: str, reason: str = "MANUAL_SQUARE_OFF") -> OrderRuntime | None:
        """User-initiated exit (the `/ws/execute-orders` "square_off" action
        `/fast-forward_2` sends — old system's sendDeploymentAction
        equivalent). Only acts on a still-ACTIVE leg; an already
        EXIT_PENDING/EXITED leg is left to whatever risk-engine trigger is
        already handling it, same idempotency guard as SL/TP/broker exits."""
        leg = self.router.legs.get(leg_id)
        if leg is None or leg.status != "ACTIVE":
            return None
        leg.status = "EXIT_PENDING"
        return self._exit_leg(leg_id, reason=reason)

    def _exit_leg(self, leg_id: str, reason: str) -> OrderRuntime | None:
        leg = self.router.legs.get(leg_id)
        if leg is None or leg.status == "EXITED":
            return None

        key = make_idempotency_key(leg.strategy_id, leg.leg_id, 1, f"EXIT:{reason}")
        if not self.idempotency.claim(key):
            log.warning("[OrderEngine] duplicate exit suppressed leg=%s reason=%s", leg_id, reason)
            return None

        exit_side = "BUY" if leg.is_sell else "SELL"  # closing transaction is opposite of entry
        if leg.slippage_pct:
            # Activation slippage on the exit fill too (closing a SELL buys
            # higher, closing a BUY sells lower); P&L follows the fill.
            leg.current_price = slipped_price(leg.current_price, leg.slippage_pct, buying=leg.is_sell)
            delta = mtm_engine.apply_leg_pnl_delta(leg)
            strategy = self.router.strategies.get(leg.strategy_id)
            if strategy is not None:
                strategy.mtm += delta
            broker_runtime = self.router.brokers.get(leg.broker_scope_id) if leg.broker_scope_id else None
            if broker_runtime is not None:
                broker_runtime.mtm += delta
        order_id = self.broker.place_order(
            tradingsymbol=leg.token, **order_venue(leg.token), transaction_type=exit_side,
            quantity=leg.qty, order_type="MARKET",
            fill_price=leg.current_price,
        )
        self.idempotency.record_order_id(key, order_id)

        order = OrderRuntime(
            order_id=order_id, idempotency_key=key, leg_id=leg.leg_id, strategy_id=leg.strategy_id,
            broker_scope_id=leg.broker_scope_id, order_side="EXIT", transaction_type=exit_side,
            tradingsymbol=leg.token, exchange=order_venue(leg.token)["exchange"], quantity=leg.qty, order_type="MARKET",
            status="FILLED", fill_price=leg.current_price, fill_qty=leg.qty,
        )
        self.orders[order_id] = order

        leg.status = "EXITED"
        leg.exit_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if self.live is not None:
            self.live.submit_exit(leg, reason)
        log.info("[OrderEngine] leg exited leg=%s reason=%s fill_price=%s order_id=%s", leg_id, reason, leg.current_price, order_id)
        return order
