"""
pending_entry_runtime.py
────────────────────────────
A leg not entered yet because its EntryIndicators time gate hasn't been
reached (conditions/schedule_engine.py). Checked once per tick by
token_router (cheap — bounded by the count of pending entries waiting for
their time, not by total leg/strategy count), not on a separate poller.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class PendingEntryRuntime:
    pending_id: str
    strategy_id: str
    broker_scope_id: str
    leg_cfg: dict[str, Any]
    target_time_ist: datetime

    # WAITING_TIME -> TRIGGERED -> ENTERED
    status: str = "WAITING_TIME"
    extra: dict[str, Any] = field(default_factory=dict)
