"""
serializers.py
─────────────────
Runtime-object <-> Mongo-document conversion — for BOTH the checkpoint/
recovery path (persistence/checkpoint.py, persistence/recovery.py) AND the
live socket push (api/live_broadcast.py). Same function, same shape, same
collection, by explicit design (user's instruction: "actual page enna table
use pannirukomo same table than use paniko, vera table use pannatha" — use
the SAME `algo_trades` collection the real system's own `/ws/execute-orders`
already reads, don't invent a parallel one).

One doc per strategy in `algo_trades`, with legs embedded as an array field
(matching the old system's own real shape — confirmed against
execution_socket.py's `{'$push': {'legs': new_leg}}` pattern: legs live
inside the strategy's own trade document, not a separate collection).

`engine: "algo-2_0"` marks every row this service writes — algo_trades is
also written by the OLD system, and both happen to use the same "exec_"
id prefix convention (confirmed against the old system's own trade-id
generation), so `_id` alone can't safely tell them apart. Recovery filters
on this field explicitly — reading a real old-system trade in as if
algo-2_0 owned it would mean recovering (and potentially re-managing) a
position this engine never actually placed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from runtime.broker_runtime import BrokerRuntime
from runtime.lazy_runtime import LazyRuntime
from runtime.leg_runtime import LegRuntime
from runtime.strategy_runtime import StrategyRuntime

ENGINE_TAG = "algo-2_0"

_TERMINAL_LEG_STATUSES = {"SL_HIT", "TP_HIT", "EXITED"}


def _leg_to_wire(leg: LegRuntime, underlying: str = "") -> dict[str, Any]:
    is_open = leg.status not in _TERMINAL_LEG_STATUSES and leg.status != "EXIT_PENDING"
    doc: dict[str, Any] = {
        "leg_id": leg.leg_id,
        "id": leg.leg_id,
        "token": leg.token,
        # The strategy's own underlying (e.g. "BTCUSD"), NOT this leg's own
        # contract token — FastForward2.tsx's computeLegLivePnl/
        # computeStrategyPnl (utils/strategyLegPnl.ts) read `leg.ticker ??
        # leg.underlying` to decide isDeltaUnderlying (crypto vs NSE) for
        # THIS leg. A per-leg token like "C-BTC-77200-130926" never matches
        # DELTA_CONTRACT_VALUE's "BTC"/"ETH" keys, so isDeltaUnderlying
        # silently came back false and every crypto leg's P&L fell through
        # to the plain NSE formula — no ×0.001 contract_value, no fixed
        # ×85 USD→INR conversion — user's explicit report: a leg showing
        # the CORRECT ₹28 P&L on the (already-fixed) Analyse page showed
        # ₹333 — almost exactly 12x too high — on /fast-forward_2 for the
        # SAME position at the same instant.
        "underlying": underlying or leg.token,
        "ticker": underlying or leg.token,
        "option": leg.option_type,
        "option_type": leg.option_type,
        "strike": leg.strike,
        "expiry_date": leg.expiry,
        "expiry": leg.expiry,
        "position": "Sell" if leg.is_sell else "Buy",
        "quantity": leg.qty,
        "lot_size": 1,
        "entry_trade": {"price": leg.entry_price, "strike": leg.strike, "expiry": leg.expiry, "option_type": leg.option_type, "traded_timestamp": leg.entry_ts},
        "status": 1 if is_open else 2,
        "feature_status_map": {
            "sl": {"current_sl_price": leg.current_sl_price} if leg.current_sl_price else None,
            "target": {"trigger_price": leg.current_tp_price} if leg.current_tp_price else None,
        },
        "active_trigger_descriptions": [],
        # Recovery-only fields (not part of the socket wire shape, harmless
        # extra keys for the frontend which only reads what it knows about).
        "_runtime_status": leg.status,
        "_runtime_qty": leg.qty,
        "_runtime_reentry_max": leg.reentry_max,
        "_runtime_reentry_used": leg.reentry_used,
        "_runtime_recost_max": leg.recost_max,
        "_runtime_recost_used": leg.recost_used,
        "_runtime_lazy_max": leg.lazy_max,
        "_runtime_lazy_used": leg.lazy_used,
        "_runtime_broker_scope_id": leg.broker_scope_id,
        "_runtime_current_price": leg.current_price,
        "_runtime_current_spot": leg.current_spot,
        "_runtime_current_pnl": leg.current_pnl,
        "_runtime_best_price": leg.best_price,
        "_runtime_leg_cfg": leg.leg_cfg,
        "_runtime_user_id": leg.user_id,
        "_runtime_entry_spot": leg.entry_spot,
        "_runtime_best_spot": leg.best_spot,
        "_runtime_slippage_pct": leg.slippage_pct,
        "_runtime_entry_slippage_applied": leg.entry_slippage_applied,
        "_runtime_entry_ts": leg.entry_ts,
        "_runtime_exit_ts": leg.exit_ts,
    }
    if not is_open:
        doc["exit_trade"] = {"price": leg.current_price, "exit_reason": "sl" if leg.status == "SL_HIT" else "target" if leg.status == "TP_HIT" else "", "traded_timestamp": leg.exit_ts}
    return doc


def _wire_to_leg_kwargs(strategy_id: str, doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "leg_id": doc["leg_id"],
        "strategy_id": strategy_id,
        "user_id": doc.get("_runtime_user_id") or "",
        "token": doc["token"],
        "is_sell": doc.get("position") == "Sell",
        "option_type": doc.get("option_type") or "",
        "qty": int(doc.get("_runtime_qty") or doc.get("quantity") or 1),
        "entry_price": float((doc.get("entry_trade") or {}).get("price") or 0),
        "entry_spot": float(doc.get("_runtime_entry_spot") or 0),
        "strike": float(doc.get("strike") or 0),
        "expiry": str(doc.get("expiry_date") or doc.get("expiry") or ""),
        "leg_cfg": doc.get("_runtime_leg_cfg") or {},
        "current_price": float(doc.get("_runtime_current_price") or (doc.get("entry_trade") or {}).get("price") or 0),
        "current_spot": float(doc.get("_runtime_current_spot") or 0),
        "current_sl_price": float(((doc.get("feature_status_map") or {}).get("sl") or {}).get("current_sl_price") or 0),
        "current_tp_price": float(((doc.get("feature_status_map") or {}).get("target") or {}).get("trigger_price") or 0),
        "current_pnl": float(doc.get("_runtime_current_pnl") or 0),
        "best_price": float(doc.get("_runtime_best_price") or 0),
        "best_spot": float(doc.get("_runtime_best_spot") or 0),
        "slippage_pct": float(doc.get("_runtime_slippage_pct") or 0),
        # A recovered leg's entry_price is already final (slipped or not).
        "entry_ts": str(doc.get("_runtime_entry_ts") or (doc.get("entry_trade") or {}).get("traded_timestamp") or ""),
        "exit_ts": str(doc.get("_runtime_exit_ts") or (doc.get("exit_trade") or {}).get("traded_timestamp") or ""),
        "entry_slippage_applied": True,
        "status": doc.get("_runtime_status") or ("ACTIVE" if doc.get("status") == 1 else "EXITED"),
        "reentry_max": int(doc.get("_runtime_reentry_max") or 0),
        "reentry_used": int(doc.get("_runtime_reentry_used") or 0),
        "recost_max": int(doc.get("_runtime_recost_max") or 0),
        "recost_used": int(doc.get("_runtime_recost_used") or 0),
        "lazy_max": int(doc.get("_runtime_lazy_max") or 0),
        "lazy_used": int(doc.get("_runtime_lazy_used") or 0),
        "broker_scope_id": doc.get("_runtime_broker_scope_id") or "",
    }


def _momentum_direction_text(momentum_type: str) -> str:
    kind = "underlying spot" if "Underlying" in momentum_type else "option premium"
    unit = "%" if "Percentage" in momentum_type else " pts"
    direction = "drop" if "Down" in momentum_type else "rise"
    return f"{kind} {direction}", unit


def _lazy_watcher_to_pending_leg(watcher: LazyRuntime) -> dict[str, Any]:
    """A leg armed on LegMomentum (initial entry) or a re-entry's own
    momentum wait (NextLeg/LikeOriginal), not entered yet — surfaced to the
    frontend as a `pending_feature_legs` entry (FastForward2.tsx's own
    `is_pending_feature_leg` branch already renders this shape: Queued At/
    Armed At/LTP/Base Price/Target Price + active_trigger_descriptions —
    same shape the old system's momentum-queue leg already used, nothing
    new on the frontend side needed). Without this, a momentum-gated leg
    was entirely invisible until the moment it actually triggered — user's
    explicit report: a 4-leg strategy (2 plain, 2 LegMomentum) only ever
    showed the 2 plain legs."""
    direction_text, unit = _momentum_direction_text(watcher.momentum_type)
    description = (
        f"Waiting for {direction_text} of {watcher.trigger_price:g}{unit} "
        f"reference — armed from {watcher.reference_ltp:g}, target {watcher.trigger_price:g}"
        if "Percentage" not in watcher.momentum_type
        else f"Waiting for {direction_text} to {watcher.trigger_price:g} (armed from {watcher.reference_ltp:g})"
    )
    return {
        "leg_id": watcher.lazy_id,
        "id": watcher.lazy_id,
        "token": watcher.token,
        "strike": watcher.strike,
        "option": watcher.option_type,
        "option_type": watcher.option_type,
        "position": "Sell" if watcher.is_sell else "Buy",
        "quantity": watcher.qty,
        "lot_size": 1,
        "is_pending_feature_leg": True,
        "is_lazy": True,
        "queued_at": watcher.armed_at,
        "armed_at": watcher.armed_at,
        "momentum_base_price": watcher.reference_ltp,
        "momentum_target_price": watcher.trigger_price,
        "active_trigger_descriptions": [description],
        "status": 0,
    }


def _strategy_status_string(strategy: StrategyRuntime) -> str:
    if strategy.status in ("EXITED", "EXIT_PENDING"):
        return "StrategyStatus.SquaredOff"
    return "StrategyStatus.Live_Running"


def _strategy_ticker(strategy: StrategyRuntime, legs: list[LegRuntime]) -> str:
    ticker = str(strategy.strategy_cfg.get("Ticker") or "").upper()
    if ticker:
        return ticker
    for leg in legs:
        if leg.token:
            return leg.token
    return "NIFTY"


def strategy_to_doc(strategy: StrategyRuntime, legs: list[LegRuntime], lazy_watchers: list[LazyRuntime] | None = None) -> dict[str, Any]:
    """The ONE shape written to `algo_trades` (checkpoint) and pushed over
    `/algo/ws/execute-orders` (socket) — see module docstring for why these
    are deliberately the same function/shape now, not two parallel ones.

    `lazy_watchers` (optional, default none) — this strategy's OWN armed-
    but-not-yet-entered LegMomentum/NextLeg/LikeOriginal watchers, surfaced
    as `pending_feature_legs` (see _lazy_watcher_to_pending_leg). Every
    call site that has a `router` handy should pass
    `[w for w in router.lazy_watchers.values() if w.strategy_id == strategy.strategy_id]`
    — omitted (not just empty-by-coincidence) callers simply get no
    pending legs in this one doc, same as before this parameter existed."""
    open_legs = [leg for leg in legs if leg.status not in _TERMINAL_LEG_STATUSES and leg.status != "EXIT_PENDING"]
    underlying = _strategy_ticker(strategy, legs)
    return {
        "_id": strategy.strategy_id,
        "engine": ENGINE_TAG,
        "trade_id": strategy.strategy_id,
        "name": strategy.name or strategy.strategy_id,
        "ticker": underlying,
        "status": _strategy_status_string(strategy),
        "active_on_server": strategy.status != "EXITED",
        "activation_mode": strategy.activation_mode or "fast-forward",
        "broker": strategy.broker_scope_id,
        "broker_label": strategy.broker_scope_id.split(":", 1)[-1] if strategy.broker_scope_id else "",
        # Looked up once at activation (services/strategy_activation.py),
        # cached on the runtime — FastForward2.tsx's getBrokerName() reads
        # this FIRST (alias/display_name/broker_name/name/title), before
        # falling back to broker_label/broker above; without it, a real
        # broker_scope_id (always ObjectId-shaped) fails every fallback
        # there too and renders as "Unknown" even though the real broker
        # was known the whole time.
        "broker_details": strategy.broker_details,
        "user_id": strategy.user_id,
        "open_legs_count": len(open_legs),
        "entry_time": strategy.entry_time,
        "legs": [_leg_to_wire(leg, underlying) for leg in legs],
        "pending_feature_legs": [_lazy_watcher_to_pending_leg(w) for w in (lazy_watchers or [])],
        "strategy_feature_status_rows": [],
        "portfolio": {
            "is_direct_strategy": strategy.is_direct_strategy,
            "group_id": strategy.group_id,
            "group_name": strategy.group_name,
            "portfolio": strategy.portfolio_id,
            "trade_portfolio": strategy.trade_portfolio,
            "trade_group_portfolio": strategy.trade_group_portfolio,
            "strategy_group_id": strategy.strategy_group_id,
        },
        # Old system duplicates these at the doc's top level too (alongside
        # the nested portfolio.* copies above) — see api.py's real doc-
        # building (doc["trade_portfolio"]/["trade_group_portfolio"]/
        # ["strategy_group_id"]), matched here for exact shape parity.
        "trade_portfolio": strategy.trade_portfolio,
        "trade_group_portfolio": strategy.trade_group_portfolio,
        "strategy_group_id": strategy.strategy_group_id,
        "trade_portfolio_id": strategy.trade_portfolio,
        "creation_ts": datetime.now(timezone.utc).isoformat(),
        # Recovery-only fields, not read by the frontend.
        "_runtime_status": strategy.status,
        "_runtime_group_id": strategy.group_id,
        "_runtime_group_name": strategy.group_name,
        "_runtime_trade_portfolio": strategy.trade_portfolio,
        "_runtime_trade_group_portfolio": strategy.trade_group_portfolio,
        "_runtime_strategy_group_id": strategy.strategy_group_id,
        "_runtime_portfolio_id": strategy.portfolio_id,
        "_runtime_strategy_cfg": strategy.strategy_cfg,
        # The FULL, untrimmed config (ListOfLegConfigs/EntryIndicators/
        # ExitIndicators/etc) — strategy.strategy_cfg above is deliberately
        # trimmed to just the risk-relevant keys (see its own comment at
        # construction in strategy_activation.py), so it was never enough
        # to reconstruct "what was this strategy actually deployed with".
        # Before this field, api/routers/strategy_history.py's `doc.get
        # ("strategy")` read a key that was NEVER WRITTEN AT ALL — every
        # "view deployed config" / analyse-view read silently got back an
        # empty config (user's explicit report: clicking Analyse on a real
        # strategy showed a blank "Build your Strategy" page instead of
        # its actual legs).
        "_runtime_full_strategy_cfg": strategy.full_strategy_cfg,
        "_runtime_broker_scope_id": strategy.broker_scope_id,
        "_runtime_live_order_type": strategy.live_order_type,
        "_runtime_mtm": strategy.mtm,
        "_runtime_peak_mtm": strategy.peak_mtm,
        "_runtime_current_overall_sl_threshold": strategy.current_overall_sl_threshold,
        "_runtime_current_floor": strategy.current_floor,
        "_runtime_trailing_activated": strategy.trailing_activated,
        "_runtime_sl_reentry_done": strategy.sl_reentry_done,
        "_runtime_tgt_reentry_done": strategy.tgt_reentry_done,
        "_runtime_is_direct_strategy": strategy.is_direct_strategy,
        "_runtime_activated_date": strategy.activated_date,
        "_runtime_qty_multiplier": strategy.qty_multiplier,
        "_runtime_slippage_pct": strategy.slippage_pct,
    }


def pending_work_to_doc(pending_entries: list, lazy_watchers: list, recost_watchers: list, range_watchers: list) -> dict[str, Any]:
    """Recovery-only snapshot of a strategy's not-yet-entered work — time-
    gated pending entries and armed Lazy/LikeOriginal/momentum, AtCost and
    LegRangeBreakout watchers. Without this a restart silently dropped
    every one of them (the strategy came back with nothing left to enter)."""
    from dataclasses import asdict

    pending_docs = []
    for pending in pending_entries:
        item = asdict(pending)
        item["target_time_ist"] = pending.target_time_ist.isoformat() if pending.target_time_ist else None
        pending_docs.append(item)
    return {
        "_runtime_pending_entries": pending_docs,
        "_runtime_lazy_watchers": [asdict(w) for w in lazy_watchers],
        "_runtime_recost_watchers": [asdict(w) for w in recost_watchers],
        "_runtime_range_watchers": [asdict(w) for w in range_watchers],
    }


def doc_to_strategy_kwargs(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": doc["_id"],
        "user_id": doc.get("user_id") or "",
        "name": doc.get("name") or "",
        "is_direct_strategy": bool(doc.get("_runtime_is_direct_strategy", True)),
        "activation_mode": doc.get("activation_mode") or "fast-forward",
        "live_order_type": doc.get("_runtime_live_order_type") or "",
        "group_id": doc.get("_runtime_group_id") or "",
        "group_name": doc.get("_runtime_group_name") or "",
        "portfolio_id": doc.get("_runtime_portfolio_id") or "",
        "trade_portfolio": doc.get("_runtime_trade_portfolio") or "",
        "trade_group_portfolio": doc.get("_runtime_trade_group_portfolio") or "",
        "strategy_group_id": doc.get("_runtime_strategy_group_id") or "",
        "entry_time": doc.get("entry_time") or "",
        "broker_details": doc.get("broker_details") or {},
        "strategy_cfg": doc.get("_runtime_strategy_cfg") or {},
        "full_strategy_cfg": doc.get("_runtime_full_strategy_cfg") or {},
        "leg_ids": {leg["leg_id"] for leg in (doc.get("legs") or [])},
        "broker_scope_id": doc.get("_runtime_broker_scope_id") or "",
        "mtm": float(doc.get("_runtime_mtm") or 0),
        "peak_mtm": float(doc.get("_runtime_peak_mtm") or 0),
        "current_overall_sl_threshold": float(doc.get("_runtime_current_overall_sl_threshold") or 0),
        "current_floor": float(doc.get("_runtime_current_floor") or 0),
        "trailing_activated": bool(doc.get("_runtime_trailing_activated")),
        "sl_reentry_done": int(doc.get("_runtime_sl_reentry_done") or 0),
        "tgt_reentry_done": int(doc.get("_runtime_tgt_reentry_done") or 0),
        "status": doc.get("_runtime_status") or "ACTIVE",
        # "" for a checkpoint written before this field existed — treated as
        # "unknown/stale" by _daily_eod_square_off_loop, same as a genuinely
        # old date (never as "today"), so pre-existing leftover positions
        # get swept on the very next midnight rollover instead of being
        # grandfathered in forever.
        "activated_date": doc.get("_runtime_activated_date") or "",
        "qty_multiplier": int(doc.get("_runtime_qty_multiplier") or 1),
        "slippage_pct": float(doc.get("_runtime_slippage_pct") or 0),
    }


def _history_doc_to_wire(ref_doc: dict[str, Any]) -> dict[str, Any]:
    """Real old-system schema (`algo_trade_positions_history`, confirmed
    against a live doc — execution_socket.py's push_new_leg_in_db writes
    here) translated into the SAME wire shape _wire_to_leg_kwargs already
    reads for algo-2_0's own docs (entry_trade.price, feature_status_map.sl/
    target, plain quantity/status). Only field-name translation — SL/target
    live at the TOP level here (current_sl_price/display_target_value), not
    nested under feature_status_map like algo-2_0's own writes, so this
    repackages rather than teaching _wire_to_leg_kwargs two schemas.

    `leg_id` is deliberately the history doc's own `_id` (globally unique),
    NOT its `leg_id` field ("0", "w51rftcu", ...) — that field is only
    unique within one trade's own leg list, and token_router.legs is a
    single global dict keyed by leg_id across every strategy."""
    position = str(ref_doc.get("position") or "")
    return {
        "leg_id": str(ref_doc.get("_id") or ""),
        "token": ref_doc.get("token") or "",
        "option_type": ref_doc.get("option") or "",
        "position": "Sell" if "sell" in position.lower() else "Buy",
        "quantity": ref_doc.get("quantity"),
        "entry_trade": {"price": (ref_doc.get("entry_trade") or {}).get("price")},
        "feature_status_map": {
            "sl": {"current_sl_price": ref_doc.get("current_sl_price")},
            "target": {"trigger_price": ref_doc.get("display_target_value")},
        },
        "status": ref_doc.get("status"),
    }


def docs_to_legs(doc: dict[str, Any], mongo: Any = None) -> list[LegRuntime]:
    """`doc["legs"]` holds either algo-2_0's own embedded leg dicts (written
    by strategy_to_doc below) OR, for a real old-system trade, plain
    ObjectId strings referencing `algo_trade_positions_history` — that
    collection is where token/entry-price/SL/status per leg actually live;
    `algo_trades.legs[]` there is just a list of references, never embedded
    data (confirmed against a live doc: engine-tag-less docs' `legs` entries
    are bare id strings, not dicts). `mongo` resolves those references;
    pass None to skip resolution entirely (e.g. a caller that only ever
    deals in algo-2_0's own docs, where every entry is already a dict)."""
    strategy_id = doc["_id"]
    leg_docs: list[dict[str, Any]] = []
    for entry in (doc.get("legs") or []):
        if isinstance(entry, dict):
            leg_docs.append(entry)
            continue
        if mongo is None:
            continue
        try:
            ref_doc = mongo.raw["algo_trade_positions_history"].find_one({"_id": ObjectId(str(entry))})
        except Exception:
            ref_doc = None
        if ref_doc:
            leg_docs.append(_history_doc_to_wire(ref_doc))
    return [LegRuntime(**_wire_to_leg_kwargs(strategy_id, leg_doc)) for leg_doc in leg_docs]


def broker_to_doc(broker: BrokerRuntime) -> dict[str, Any]:
    from dataclasses import asdict
    doc = asdict(broker)
    doc["_id"] = doc.pop("broker_scope_id")
    doc["strategy_ids"] = sorted(broker.strategy_ids)
    doc["leg_ids"] = sorted(broker.leg_ids)
    doc["config"]["trailing_mode"] = broker.config.trailing_mode.value
    return doc


def _today_ist_date() -> str:
    """Same calendar-date-in-IST logic as the old system's execution_socket.py
    _today_ist() — `creation_ts` prefix-matches against this exact string."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    total_minutes = now.hour * 60 + now.minute + 330
    extra_days = total_minutes // 1440
    return (now.date() + timedelta(days=extra_days)).strftime("%Y-%m-%d")


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = "") -> str:
    """Same priority order as api/routers/broker_config.py's own copy of
    this (ported from the old system's api.py) — kept as a second copy here
    rather than importing that router module into persistence/, which
    would be a layering inversion (persistence/ has no business importing
    from api/)."""
    for key in ("display_name", "name", "title", "broker_name", "broker", "broker_type", "provider", "vendor"):
        value = str(document.get(key) or "").strip()
        if value:
            return value
    return str(fallback_broker_id or "").strip()


def load_legacy_broker_settings(mongo: Any, user_id: str, activation_mode: str = "") -> list[dict[str, Any]]:
    """Real-table equivalent of the old system's emit_broker_settings_for_user
    (execution_socket.py:6795-6896) — same `algo_borker_stoploss_settings`
    query (user_id + optional activation_mode) and same `broker_configuration`
    label join, same output shape (broker_id/broker_label/mode/settings_active/
    stop_loss/target/lock_and_trail/trail_sl/state). LockAndTrail/OverallTrailSL
    are only surfaced when BOTH their InstrumentMove and StopLossMove are
    truthy — same guard the old system uses, since a half-configured block
    (e.g. Value defaults to {} with both at 0) isn't a real active setting."""
    uid = str(user_id or "").strip()
    if not uid:
        return []
    query: dict[str, Any] = {"user_id": uid}
    if activation_mode:
        query["activation_mode"] = str(activation_mode).strip()
    docs = list(mongo.raw["algo_borker_stoploss_settings"].find(query))
    if not docs:
        return []

    broker_ids: list[ObjectId] = []
    for doc in docs:
        try:
            broker_ids.append(ObjectId(str(doc.get("broker") or "").strip()))
        except Exception:
            pass
    label_map: dict[str, str] = {}
    if broker_ids:
        for bconf in mongo.raw["broker_configuration"].find(
            {"_id": {"$in": broker_ids}},
            {"broker_name": 1, "display_name": 1, "name": 1, "title": 1, "broker": 1},
        ):
            bid = str(bconf.get("_id") or "")
            label_map[bid] = _extract_broker_configuration_label(bconf, bid)

    out = []
    for doc in docs:
        broker_id = str(doc.get("broker") or "").strip()
        lat_cfg = doc.get("LockAndTrail") or {}
        trail_cfg = doc.get("OverallTrailSL") or {}
        lat_out = {"instrument_move": lat_cfg["InstrumentMove"], "stop_loss_move": lat_cfg["StopLossMove"]} \
            if lat_cfg.get("InstrumentMove") and lat_cfg.get("StopLossMove") else None
        trail_out = {"instrument_move": trail_cfg["InstrumentMove"], "stop_loss_move": trail_cfg["StopLossMove"]} \
            if trail_cfg.get("InstrumentMove") and trail_cfg.get("StopLossMove") else None
        out.append({
            "broker_id": broker_id,
            "broker_label": label_map.get(broker_id) or broker_id,
            "user_id": uid,
            "mode": str(doc.get("activation_mode") or "").strip(),
            "settings_active": bool(doc.get("status") == 1),
            "stop_loss": doc.get("StopLoss"),
            "target": doc.get("Target"),
            "lock_and_trail": lat_out,
            "trail_sl": trail_out,
            "state": {
                "lock_activated": bool(doc.get("lock_activated") or False),
                "current_lock_floor": float(doc.get("current_lock_floor") or 0),
                "lock_peak_mtm": float(doc.get("lock_peak_mtm") or 0),
                "effective_sl": doc.get("effective_sl"),
                "sl_peak_mtm": float(doc.get("sl_peak_mtm") or 0),
            },
        })
    return out


def load_legacy_open_leg_tokens(mongo: Any) -> set[str]:
    """Tokens for every currently-open leg belonging to a trade that's
    genuinely still open right now — `active_on_server: True`, the actual
    "is this really live" flag (flipped False by a real square-off), NOT a
    same-calendar-day restriction. An earlier version of this filter also
    required `creation_ts` to start with today's IST date (matching the old
    system's own execute-orders query) — that's correct THERE because the
    old system's own EOD job squares every fast-forward/live/forward-test
    trade off at day-end, so a doc from a prior day is never genuinely
    still open by the time "today" rolls around. This service has no such
    EOD job yet, so a trade left active_on_server=True from days ago (e.g.
    this dev box being off over a weekend) IS still genuinely open — the
    day-restriction was hiding a real open position's LTP for no reason
    other than its creation_ts being old (user's explicit report: "current
    open position" must show its LTP regardless of which day it opened).

    `algo_trade_positions_history` alone has no liveness scoping of its
    own — `status=1, exit_trade=None` on that collection matches
    practically every leg ever recorded there (confirmed against real
    data: 900+ rows, almost the whole collection, going back to old
    backtest/test runs that never got a proper exit written) — active_on_server
    on the PARENT trade is what actually narrows this down to real open
    positions. Used to filter the `/algo/ws/update` ltp_update stream down
    to only tokens an open position actually holds. A plain read, same as
    load_legacy_execute_order_records — never touches token_router.

    Also includes each active trade's own `ticker` (the underlying, e.g.
    "BTCUSD") alongside its option-contract tokens — same "option_ltp +
    spot_ltp" dual-stream the old system's own live LTP payload sends
    (execution_socket.py:13888). Load-bearing for crypto specifically:
    algo.websocket's DeltaFeed only ever streams the BTCUSD/ETHUSD spot/
    perpetual mark price (see feed/delta_feed.py's DEFAULT_SYMBOLS) — it
    has no live re-pricing feed for individual option-contract tokens at
    all, so an option leg's own token never ticks; without the underlying
    included here too, a crypto position would get zero ltp_update ticks,
    ever, for a reason that has nothing to do with this filter.

    Two different sources for the option-contract tokens themselves,
    because `algo_trades.legs[]` means two different things depending on
    which engine wrote the doc: the OLD system's own `legs` field there is
    "just a list of references, never embedded" (see recovery.py's own
    note) — its real per-leg rows live in the separate
    `algo_trade_positions_history` collection, so that's queried for those
    docs. algo-2_0's OWN docs (`engine: ENGINE_TAG`) embed the real leg
    rows directly IN `algo_trades.legs[]` (see strategy_to_doc/_leg_to_wire
    above — algo-2_0 never writes to `algo_trade_positions_history` at
    all), so reading that collection for an algo-2_0 strategy's option
    tokens always came back empty — confirmed live: a real activated BTC
    strategy's underlying (BTCUSD) ticked fine (picked up below), but its
    own option-leg tokens sat un-subscribed on DeltaFeed forever, LTP
    frozen at entry price, because this function only ever looked for them
    in a collection this engine never populates."""
    active_trades = list(
        mongo.raw["algo_trades"].find(
            {"active_on_server": True},
            {"_id": 1, "ticker": 1, "engine": 1, "legs": 1},
        )
    )
    if not active_trades:
        return set()
    tokens = {str(doc.get("ticker") or "") for doc in active_trades if doc.get("ticker")}
    legacy_trade_ids = []
    for doc in active_trades:
        if doc.get("engine") == ENGINE_TAG:
            for leg in doc.get("legs") or []:
                if leg.get("status") == 1 and leg.get("token"):
                    tokens.add(str(leg["token"]))
        else:
            legacy_trade_ids.append(str(doc["_id"]))
    if legacy_trade_ids:
        for doc in mongo.raw["algo_trade_positions_history"].find(
            {"trade_id": {"$in": legacy_trade_ids}, "status": 1, "exit_trade": None}, {"token": 1},
        ):
            if doc.get("token"):
                tokens.add(str(doc["token"]))
    return tokens


def load_legacy_execute_order_records(mongo: Any, activation_mode: str, user_id: str = "") -> list[dict[str, Any]]:
    """Real-table equivalent of the old system's execute-orders socket
    "Initial strategies loaded" query (execution_socket.py:14536-14554,
    :7183 _populate_legs_from_history) — `trade_status in [1, 2]` there
    means RUNNING **or** cancelled/squared-off, not just running, so this
    does NOT filter on `active_on_server` at the Mongo level either: an
    exited-today strategy must still come back (frontend's own
    normalizeStatus() already renders active_on_server===false as "SQD
    OFF", same as every other exited record) — user's explicit report,
    confirmed against real data: `algo_trades` had 7 today's docs, 5
    already exited, and the initial load was wrongly hiding all 5 instead
    of showing them as squared-off.

    What DOES gate every doc here is the same intraday-only restriction
    the old system applies via its own `creation_ts` IST-date prefix match
    (only today's — IST 00:00 to 23:59 — data should ever come back here;
    a strategy from a PRIOR day, active or not, must stay hidden — see
    persistence/recovery.py's own docstring for the "why" this was added).
    Engine-aware because the two engines' `creation_ts` conventions differ:
    an old-system doc's `creation_ts` is a real, once-set IST-prefixed
    string (their own convention, safe to prefix-match directly, exactly
    like execution_socket.py does); an algo-2_0 (`engine=ENGINE_TAG`) doc's
    `creation_ts` is instead re-stamped to UTC-now on EVERY checkpoint
    (persistence/checkpoint.py), so it can never be used to tell an old
    activation from a new one — those use the dedicated
    `_runtime_activated_date` field instead (set once at activation, see
    runtime/strategy_runtime.py's `activated_date` field docstring, never
    touched again even once a doc is later marked exited), same field
    persistence/recovery.py checks on restart and main.py's
    `_daily_eod_square_off_loop` checks live, mid-session.

    SAME leg source otherwise: legs are NOT read from `algo_trades.legs[]`
    (that field is just reference ids on real old-system docs, see
    docs_to_legs above) — they're queried straight from
    `algo_trade_positions_history` by `trade_id`, exactly like
    _populate_legs_from_history does. Also attaches, per leg,
    `algo_leg_feature_status` rows (feature_status_rows/feature_status_map/
    active_trigger_descriptions — same "enabled only" filter the old
    system's frontend-facing shape uses) and, per record, broker_details/
    broker_label from `broker_configuration` and open/closed/pending leg
    counts — matching the reference payload field-for-field.

    This is a plain read for the initial WS snapshot — it does NOT touch
    token_router, so it never puts a trade under algo-2_0's own risk/SL-TP
    engine; that stays whichever engine (old system or algo-2_0) actually
    placed it. Purely a display-data fix, not a management/ownership one."""
    query: dict[str, Any] = {
        "activation_mode": activation_mode,
        # Squared-off strategies the user archived from the page.
        "archived": {"$ne": True},
    }
    if user_id:
        query["user_id"] = user_id

    today = _today_ist_date()
    # algo-2_0 docs carry their trading day: IST date (NSE) or the 17:30 IST
    # crypto session date — either counts as "today's" data here.
    from conditions.schedule_engine import trading_day
    current_days = {today, trading_day(True)}

    def _is_today(d: dict[str, Any]) -> bool:
        if d.get("engine") == ENGINE_TAG:
            activated_date = d.get("_runtime_activated_date") or ""
            if activated_date:
                return activated_date in current_days
            # No _runtime_activated_date at all — a doc checkpointed
            # before that field existed (transitional only: every strategy
            # activated from now on always carries it). Its `creation_ts`
            # is otherwise unusable for a STILL-ACTIVE algo-2_0 doc (re-
            # stamped to UTC-now on every periodic checkpoint — see this
            # function's own docstring), but once a strategy is EXITED
            # nothing checkpoints it again, so creation_ts is frozen at
            # whatever it last was — a decent one-time fallback signal
            # for "did this happen today" on an already-closed doc (user's
            # explicit report: these must show as squared-off, not vanish).
            return str(d.get("creation_ts") or "").startswith(today)
        return str(d.get("creation_ts") or "").startswith(today)

    docs = list(mongo.raw["algo_trades"].find(query).sort("creation_ts", 1))
    docs = [d for d in docs if _is_today(d)]
    if not docs:
        return []

    trade_ids = [str(d.get("_id") or "") for d in docs]
    history_by_trade: dict[str, list[dict[str, Any]]] = {tid: [] for tid in trade_ids}
    for leg_doc in mongo.raw["algo_trade_positions_history"].find({"trade_id": {"$in": trade_ids}}):
        leg_doc = dict(leg_doc)
        leg_doc["_id"] = str(leg_doc.get("_id") or "")
        tid = str(leg_doc.get("trade_id") or "")
        if tid in history_by_trade:
            history_by_trade[tid].append(leg_doc)

    # (trade_id, leg_id) -> enabled algo_leg_feature_status rows, for the
    # feature_status_rows/feature_status_map/active_trigger_descriptions
    # fields each leg gets below.
    feature_rows_by_leg: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for frow in mongo.raw["algo_leg_feature_status"].find({"trade_id": {"$in": trade_ids}, "enabled": True}):
        frow = dict(frow)
        frow["_id"] = str(frow.get("_id") or "")
        key = (str(frow.get("trade_id") or ""), str(frow.get("leg_id") or ""))
        feature_rows_by_leg.setdefault(key, []).append(frow)

    broker_ids: list[ObjectId] = []
    for d in docs:
        try:
            broker_ids.append(ObjectId(str(d.get("broker") or "").strip()))
        except Exception:
            pass
    broker_details_map: dict[str, dict[str, Any]] = {}
    if broker_ids:
        for bconf in mongo.raw["broker_configuration"].find(
            {"_id": {"$in": broker_ids}},
            {"broker_icon": 1, "broker_name": 1, "display_name": 1, "name": 1, "title": 1, "broker": 1, "broker_type": 1},
        ):
            bid = str(bconf.get("_id") or "")
            details = {k: v for k, v in bconf.items() if k != "_id"}
            details["_id"] = bid
            broker_details_map[bid] = details

    records = []
    for doc in docs:
        record = dict(doc)
        trade_id = str(record.get("_id") or "")
        record["_id"] = trade_id

        open_count = closed_count = pending_count = 0
        legs_out = []
        # algo-2_0's own docs embed real leg rows directly in algo_trades.
        # legs[] (_leg_to_wire) and never write algo_trade_positions_history
        # (see load_legacy_open_leg_tokens's docstring above for the same
        # distinction) — joining against that collection for these docs
        # always came back empty, silently zeroing open_legs_count/legs for
        # every algo-2_0 strategy despite token_router genuinely holding
        # open legs for it. That in turn starved the has_open_leg gate below
        # in ws_live.py (it reads THIS record's legs), so a real algo-2_0
        # broker with live positions never got its "broker-settings" socket
        # push — confirmed live: Forward1's SL/Target/LockAndTrail were
        # saved and active, but never rendered on the sidebar because this
        # exact record always reported zero legs to that gate.
        leg_source = doc.get("legs") or [] if doc.get("engine") == ENGINE_TAG else history_by_trade.get(trade_id, [])
        for leg in leg_source:
            leg = dict(leg)
            leg_status = leg.get("status")
            if leg_status == 1:
                open_count += 1
            elif leg_status == 2:
                closed_count += 1
            else:
                pending_count += 1

            frows = feature_rows_by_leg.get((trade_id, str(leg.get("leg_id") or "")), [])
            leg["feature_status_rows"] = frows
            leg["feature_status_map"] = {str(r.get("feature") or ""): r for r in frows if r.get("feature")}
            leg["active_trigger_descriptions"] = [r["trigger_description"] for r in frows if r.get("trigger_description")]
            legs_out.append(leg)

        record["legs"] = legs_out
        record["open_legs_count"] = open_count
        record["closed_legs_count"] = closed_count
        record["pending_legs_count"] = pending_count

        broker_id = str(record.get("broker") or "").strip()
        record["broker_details"] = broker_details_map.get(broker_id, {})
        record["broker_label"] = _extract_broker_configuration_label(record["broker_details"], broker_id)
        record["position_refresh_trigger"] = "initial_load"
        records.append(record)
    return records


def doc_to_broker_kwargs(doc: dict[str, Any]) -> dict[str, Any]:
    from risk.models import RiskConfig, TrailingMode

    doc = dict(doc)
    doc["broker_scope_id"] = doc.pop("_id")
    doc["cycle_base_mtm"] = 0.0  # unused; peak is re-derived by rebase_day_mtm
    doc["strategy_ids"] = set(doc.get("strategy_ids") or [])
    doc["leg_ids"] = set(doc.get("leg_ids") or [])
    config_doc = dict(doc.get("config") or {})
    config_doc["trailing_mode"] = TrailingMode(config_doc.get("trailing_mode") or "NONE")
    doc["config"] = RiskConfig(**config_doc)
    return doc
