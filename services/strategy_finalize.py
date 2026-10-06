"""
strategy_finalize.py
───────────────────────
One place that closes out a strategy whose legs are all done:

  - TokenRouter.finalize_if_no_open_legs (marks EXITED, drops it from RAM),
  - persists the FINAL snapshot instead of deleting the algo_trades doc when
    the strategy actually traded — status "StrategyStatus.SquaredOff",
    active_on_server False — so the execute-orders initial load keeps showing
    it as squared off for the rest of the day (a strategy that never entered
    a single leg is still deleted, as before),
  - pushes that same final record to the owner's /ws/execute-orders socket,
    so an open page flips it to Squared Off without a reload.

Also square_off_strategy(): the whole-strategy exit used when a broker-level
SL/Target/Lock/Trail fires — cancels its not-yet-entered work (pending
time-gated entries, lazy/recost/range watchers), exits any leg still open,
and finalizes it.
"""

from __future__ import annotations

import asyncio

from persistence.checkpoint import get_checkpoint_writer
from persistence.serializers import pending_work_to_doc, strategy_to_doc
from shared.logging.logger import get_logger

log = get_logger(__name__)

_TERMINAL = {"SL_HIT", "TP_HIT", "EXITED"}


def _publish(user_id: str, record: dict) -> None:
    if not user_id:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    from api.ws_client_hub import execute_orders_hub

    loop.create_task(execute_orders_hub.send_to_user(user_id, {"data": {"records": [record]}}))


def finalize_and_publish(router, strategy_id: str, keep_if_empty: bool = False) -> bool:
    """Returns True when the strategy was finalized (EXITED and removed).
    `keep_if_empty`: save it as squared off even if it never entered a leg
    (a broker-level exit squaring off a strategy still waiting to enter)."""
    strategy = router.strategies.get(strategy_id)
    if strategy is None:
        return False
    legs = [router.legs[lid] for lid in strategy.leg_ids if lid in router.legs]
    if not router.finalize_if_no_open_legs(strategy_id):
        return False
    writer = get_checkpoint_writer()
    if not legs and not keep_if_empty:
        writer.queue_strategy_delete(strategy_id)  # never traded — nothing to show
        return True
    record = strategy_to_doc(strategy, legs, [])
    doc = dict(record)
    doc.update(pending_work_to_doc([], [], [], []))
    writer.queue_strategy(doc)
    _publish(strategy.user_id, record)
    log.info("[Finalize] strategy=%s squared off (%d legs)", strategy_id, len(legs))
    return True


def square_off_strategy(router, order_engine, strategy_id: str, reason: str) -> None:
    """Whole-strategy exit (broker-level risk hit): no further entries of
    any kind, every open leg closed, strategy finalized as squared off."""
    strategy = router.strategies.get(strategy_id)
    if strategy is None:
        return
    router.cancel_pending_work(strategy_id)
    for leg_id in list(strategy.leg_ids):
        leg = router.legs.get(leg_id)
        if leg is not None and leg.status not in _TERMINAL:
            if leg.status == "ACTIVE":
                order_engine.manual_square_off(leg_id, reason=reason)
            else:  # EXIT_PENDING claimed by another scope — finish it
                order_engine._exit_leg(leg_id, reason)
    # Stops an activation / re-entry still running in the background from
    # entering anything more (they re-check strategy.status before each leg).
    strategy.status = "EXITED"
    if not finalize_and_publish(router, strategy_id, keep_if_empty=True):
        # Still held by an in-flight background activation — publish the
        # squared-off state now; that task finalizes it when it unwinds.
        legs = [router.legs[lid] for lid in strategy.leg_ids if lid in router.legs]
        _publish(strategy.user_id, strategy_to_doc(strategy, legs, []))
