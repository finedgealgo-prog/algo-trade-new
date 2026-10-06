"""
leg_runtime.py
────────────────
In-memory runtime object for one active leg — master-doc §9. No DB read on
the tick path: everything sl_tp_engine/trailing_engine/mtm_engine need is
kept right here, updated in place as ticks/config changes arrive.

`leg_cfg` carries the raw LegStopLoss/LegTarget/LegTrailSL sub-configs exactly
as stored in Mongo today (same {Type, Value} shape position_manager.py's
calc_sl_price/calc_tp_price/get_trail_config expect) — kept as a dict rather
than modeled field-by-field so the risk engine ports those functions verbatim
without a config-shape translation layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LegRuntime:
    leg_id: str
    strategy_id: str
    user_id: str
    token: str

    is_sell: bool
    option_type: str  # "CE" | "PE" | ""
    qty: int  # already lots * lot_size

    entry_price: float
    entry_spot: float = 0.0

    # Frozen at entry (StrikeSelection.strike/expiry — selection/
    # strike_resolver.py) — never re-derived afterward. Needed for
    # anything that displays this leg outside the live tick path itself
    # (api/routers/strategy_history.py's "view deployed config"/analyse-
    # view read, which never had a real strike at all before this field
    # existed — _leg_to_wire used to alias it to `token`, which broke the
    # frontend's `if (!strike || !expiry) return null` leg filter for any
    # non-numeric token).
    strike: float = 0.0
    expiry: str = ""

    leg_cfg: dict[str, Any] = field(default_factory=dict)  # LegStopLoss/LegTarget/LegTrailSL

    current_price: float = 0.0
    current_spot: float = 0.0

    current_sl_price: float = 0.0
    current_tp_price: float = 0.0

    current_pnl: float = 0.0

    # ₹ per 1 raw-quote point per unit of qty — 1.0 for NSE; for a Delta
    # leg, contract_value × USD→INR (entry/current prices stay Delta's raw
    # per-1-coin quote, same as the UI shows). Set by TokenRouter.register_leg
    # so every entry site and recovery gets it without passing it around.
    pnl_multiplier: float = 1.0

    # Activation slippage % (from the strategy) and whether it has already
    # been applied to entry_price — set once in TokenRouter.register_leg for
    # a fresh entry; a recovered leg already carries its slipped price.
    slippage_pct: float = 0.0
    entry_slippage_applied: bool = False

    # True UTC "YYYY-MM-DD HH:MM:SS" — when the leg entered / exited. Sent as
    # entry_trade/exit_trade.traded_timestamp (FastForward2's MTM graph
    # starts/stops each leg's P&L line at these minutes).
    entry_ts: str = ""
    exit_ts: str = ""

    # Trailing state — master-doc §12: kept in memory, not written to Mongo
    # on every movement (persistence is a periodic checkpoint, Phase 8).
    best_price: float = 0.0
    # Underlying-based trailing SL anchor — most favourable spot seen (see
    # trailing_engine.apply_trailing).
    best_spot: float = 0.0

    # CREATED -> ACTIVE -> SL_HIT|TP_HIT -> ORDER_PENDING -> EXITED (master-doc §47)
    status: str = "ACTIVE"

    # Separate counters per condition kind (master-doc §44 — "do not use one
    # retry counter"). Carried forward across a lineage: a re-entry/re-cost
    # leg's *_used starts at its parent's *_used + 1, so the budget is
    # enforced across generations, not reset each time (ports the old
    # system's _reentry_budget_remaining's "read the closing leg's own
    # remaining budget" behavior, just tracked as used/max instead of a
    # single remaining-count field).
    reentry_max: int = 0
    reentry_used: int = 0
    recost_max: int = 0
    recost_used: int = 0
    lazy_max: int = 0
    lazy_used: int = 0

    # "user_id:broker_connection_id" (master-doc, unified risk engine §2/§23)
    # — default "" for legs not yet wired to a broker scope (Phase 3's tests
    # predate this field and don't set it).
    broker_scope_id: str = ""
