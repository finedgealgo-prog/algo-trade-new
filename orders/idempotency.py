"""
idempotency.py
─────────────────
Master-doc §56/§30: every broker order must have an idempotency identity —
`strategy_id + leg_id + attempt_number + event_type`. The old system has no
cryptographic idempotency key either (see shared/features/live_order_manager.py's
DB-state + in-memory-set dedup — _register_active_entry_order, the
current_status==status early-return in process_broker_order_update); algo-2_0's
equivalent is simpler because the whole risk pipeline already runs in-memory,
single-threaded per process (token_router's duplicate-trigger protection,
§46/§68 — a leg/strategy/broker only fires once because status flips
immediately). This registry is the belt-and-suspenders check specifically at
the ORDER layer: even if something upstream ever re-emits the same
TriggerEvent, the same idempotency_key resolves to the same OrderRuntime
instead of a second broker call.
"""

from __future__ import annotations


def make_idempotency_key(strategy_id: str, leg_id: str, attempt_number: int, event_type: str) -> str:
    return f"{strategy_id}:{leg_id}:{attempt_number}:{event_type}"


class IdempotencyRegistry:
    def __init__(self) -> None:
        self._seen: dict[str, str] = {}  # idempotency_key -> order_id (or "" if not yet ACKed)

    def claim(self, key: str) -> bool:
        """Returns True if this key was NOT already claimed (caller should
        proceed to place the order); False if it was (caller must not place
        a second order — the existing OrderRuntime for this key is authoritative)."""
        if key in self._seen:
            return False
        self._seen[key] = ""
        return True

    def record_order_id(self, key: str, order_id: str) -> None:
        self._seen[key] = order_id

    def get_order_id(self, key: str) -> str | None:
        return self._seen.get(key)
