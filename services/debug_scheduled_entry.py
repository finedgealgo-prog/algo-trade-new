"""
debug_scheduled_entry.py
────────────────────────────
Entry-time-gated counterpart to `api/routers/admin.py`'s debug/register-leg
path (raw-token virtual legs, e.g. crypto BTCUSD/ETHUSD — no strike
resolution). `services/scheduled_entry.py` already handles SCHEDULED_ENTRY_READY
for real saved-strategy legs via strike_resolver; this handles the same event
for a PendingEntryRuntime whose `leg_cfg["_debug_direct"]` marker says it was
armed by the debug endpoint instead — entering directly against the raw
token at whatever the live LTP is when the entry time arrives, same shape
as debug_register_leg's own immediate-entry path.
"""

from __future__ import annotations

from orders.order_engine import OrderEngine
from services.strategy_activation import cleanup_if_no_legs
from shared.logging.logger import get_logger
from shared.market.ltp_cache import LtpCache
from token_router import TokenRouter, TriggerEvent

log = get_logger(__name__)


def handle_debug_scheduled_entry_ready(router: TokenRouter, order_engine: OrderEngine, ltp_cache: LtpCache, event: TriggerEvent) -> None:
    pending = router.pending_entries.get(event.reason)
    if pending is None or pending.status != "TRIGGERED" or not pending.leg_cfg.get("_debug_direct"):
        return  # not ours — services/scheduled_entry.py's own listener handles real strategies

    from api.routers.admin import _enter_debug_leg  # shared virtual-entry construction

    token = str(pending.leg_cfg.get("token") or "")
    leg = _enter_debug_leg(
        router, order_engine, pending.strategy_id, pending.broker_scope_id, token,
        bool(pending.leg_cfg.get("is_sell")), int(pending.leg_cfg.get("qty") or 1),
        float(pending.leg_cfg.get("stop_loss_points") or 0), float(pending.leg_cfg.get("target_points") or 0),
    )
    router.unregister_pending_entry(pending.pending_id)
    if leg is None:
        log.warning("[DebugScheduledEntry] pending=%s no live LTP for token=%s — dropped", pending.pending_id, token)
        cleanup_if_no_legs(router, pending.strategy_id)
        return
    log.info("[DebugScheduledEntry] entered leg=%s token=%s price=%s", leg.leg_id, token, leg.entry_price)
