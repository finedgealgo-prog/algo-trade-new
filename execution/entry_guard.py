"""
entry_guard.py
─────────────────
Master-doc §34/§35/§36: before ANY new entry (regular, scheduled, lazy,
re-entry, re-cost, adjustment, momentum), BOTH the owning strategy AND its
broker scope must be ACTIVE — both checks must pass. A broker-level exit
blocks every strategy under it (§35); a strategy-level exit blocks only that
strategy, siblings stay eligible (§36).

TokenRouter's Phase 6 order/lazy/re-entry wiring must call `entry_allowed`
before arming a new lazy watcher, re-cost watcher, or firing a re-entry —
this module only provides the check; nothing yet calls it automatically
(Phase 6 — order engine — is where entries actually get triggered).
"""

from __future__ import annotations

from runtime.broker_runtime import BrokerRuntime
from runtime.strategy_runtime import StrategyRuntime


def entry_allowed(strategy: StrategyRuntime, broker: BrokerRuntime | None) -> bool:
    if strategy.status != "ACTIVE":
        return False
    if broker is not None and broker.status != "ACTIVE":
        return False
    return True
