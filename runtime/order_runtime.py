"""
order_runtime.py
───────────────────
In-memory runtime object for one broker order — master-doc §55-58, ported
state shape from shared/features/live_order_manager.py's order-state model
(ORDER_SENT/ORDER_ACKNOWLEDGED/PARTIALLY_FILLED/FILLED/REJECTED/CANCELLED,
generalized from its Mongo `broker_orders` row shape into a runtime object).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OrderRuntime:
    order_id: str  # broker's own order id ("" until ACK)
    idempotency_key: str

    leg_id: str
    strategy_id: str
    broker_scope_id: str

    order_side: str  # "ENTRY" | "EXIT"
    transaction_type: str  # "BUY" | "SELL"
    tradingsymbol: str
    exchange: str
    quantity: int
    order_type: str  # "LIMIT" | "MARKET" | "SL" | "SL-M"
    price: float = 0.0
    trigger_price: float = 0.0

    # SENT -> ACKNOWLEDGED -> (PARTIALLY_FILLED ->)? FILLED | REJECTED | CANCELLED
    status: str = "SENT"

    fill_price: float = 0.0
    fill_qty: int = 0
    rejection_reason: str = ""

    attempt_number: int = 1
