"""
broker_runtime.py
────────────────────
In-memory runtime object for one broker-connection risk scope — master-doc
§10/§23.

`broker_scope_id` = `user_id:broker_connection_id` (master-doc §2 — NOT
`user_id + broker_name`, which is what the old system's
check_broker_sl_target actually groups by today: `f'{user_id}|{broker}|
{activation_mode}'`, per `shared/features/trading_core.py:1706`. That's a
real limitation the doc explicitly calls out and asks to fix — a user with
two Dhan accounts couldn't have independent broker-level risk under the old
grouping. algo-2_0 fixes it here by keying on connection id, per doc.

`mtm` here is DAY NET PNL (realized + unrealized) per master-doc §37 — same
semantic the old `compute_broker_group_mtm` already uses (open_mtm +
closed_mtm), preserved exactly, not reinvented.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from risk.models import RiskConfig


@dataclass
class BrokerRuntime:
    broker_scope_id: str  # "user_id:broker_connection_id"
    user_id: str

    config: RiskConfig = field(default_factory=RiskConfig)

    strategy_ids: set[str] = field(default_factory=set)
    leg_ids: set[str] = field(default_factory=set)

    realized_pnl: float = 0.0  # closed-trade PnL for the day
    mtm: float = 0.0  # day net = realized_pnl + open (unrealized) pnl

    peak_pnl: float = 0.0
    current_floor: float = 0.0
    trailing_activated: bool = False

    # Unused (always 0) — kept so existing checkpoints load. Broker risk now
    # acts on the running strategies' MTM (risk.broker_risk.running_mtm);
    # peak_pnl/current_floor are in those terms. `mtm` stays the day total.
    cycle_base_mtm: float = 0.0

    # ACTIVE -> EXIT_PENDING (SL/Target hit, legs exiting) -> ACTIVE again
    # with its risk settings disabled (services/broker_scope.reset_after_risk_exit)
    status: str = "ACTIVE"

    extra: dict[str, Any] = field(default_factory=dict)
