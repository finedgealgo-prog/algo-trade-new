"""
strategy_runtime.py
──────────────────────
In-memory runtime object for one strategy's overall risk state — master-doc
§14. `strategy_cfg` carries the raw OverallSL/OverallTgt/OverallTrailSL
sub-configs exactly as stored today (same shape position_manager.py's
parse_overall_sl/parse_overall_tgt/parse_overall_trail_sl expect).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StrategyRuntime:
    strategy_id: str
    user_id: str

    # Display name (saved_strategies.name for a real activation, a readable
    # label for a debug/quick-test leg) — kept separate from strategy_id
    # (an opaque exec_/debug_ runtime id) so the UI can show something a
    # human recognizes instead of the raw id.
    name: str = ""

    # FastForward2.tsx (unmodified from the old FastForward.tsx) decides
    # "Strategies" section (flat row) vs "Portfolios" section (expandable
    # group box, "RUNNING N/M" header) purely from
    # `rec.portfolio?.is_direct_strategy` — True for a standalone
    # /algo/strategy/{id}/activate call, False for a member of a
    # /algo/portfolio/{id}/activate call. Defaults True (the common case:
    # api/routers/admin.py's debug endpoint and a direct strategy activate);
    # portfolio.py explicitly sets it False for each member it activates.
    is_direct_strategy: bool = True

    # UTC "YYYY-MM-DD HH:MM:SS" (no timezone marker — matches FastForward2.
    # tsx's/LiveTrade.tsx's own `entry_time` convention exactly, see that
    # component's toUtcMs comment) — set only for a time-gated activation
    # still WAITING on its EntryIndicators time (services/strategy_
    # activation.py's PENDING branch), so CountdownOrMtm can render "Entry
    # in HH:MM:SS" instead of MTM until that moment passes. Left as-is (not
    # cleared) once the strategy actually enters — CountdownOrMtm itself
    # already falls through to MTM the instant `remaining <= 0`, so a
    # stale past entry_time is harmless, not read as "still waiting."
    entry_time: str = ""

    # Portfolio-activation grouping — old system's real convention
    # (algo.trade/api.py's portfolio-activation doc-building:
    # doc["portfolio"]["group_id"/"group_name"/"portfolio"]), read by
    # FastForward2.tsx's groupRecords() (algo-admin/src/pages/AlgoTrade/
    # LiveTrade.tsx) as `pm.group_id ?? pm.portfolio ?? rec._id` to decide
    # which strategies nest under the SAME portfolio card. Empty for a
    # direct (non-portfolio) strategy activation — groupRecords() then
    # falls back to rec._id, i.e. its own standalone card, same as
    # is_direct_strategy=True already implies. All THREE strategies
    # activated together from one /algo/portfolio/{id}/activate (or
    # /algo/portfolio/activate) call share the SAME group_id/group_name/
    # portfolio_id, set once by that endpoint before calling
    # activate_strategy() per member — never generated per-strategy here.
    group_id: str = ""
    group_name: str = ""
    portfolio_id: str = ""
    # Real reference shape (a live captured old-system portfolio.activate
    # payload) also carries these three alongside group_id/group_name/
    # portfolio — added for exact field-parity:
    #   trade_portfolio        — same value as portfolio_id today (algo-2_0
    #                             has no separate daily-rolling portfolio
    #                             doc the old system's trade_portfolio_id
    #                             sometimes is — /portfolio/prepare-
    #                             activation already made this same
    #                             portfolio_id==trade_portfolio_id
    #                             simplification, this mirrors it).
    #   trade_group_portfolio  — same value as group_id (this whole
    #                             activation batch's shared id).
    #   strategy_group_id      — a FRESH id, UNIQUE PER STRATEGY (unlike
    #                             group_id, shared across every strategy in
    #                             the batch) — generated once in
    #                             services/strategy_activation.py's
    #                             activate_strategy for every activation,
    #                             direct or portfolio alike.
    trade_portfolio: str = ""
    trade_group_portfolio: str = ""
    strategy_group_id: str = ""

    # broker_configuration doc (alias/display_name/broker_name/name/title/
    # broker_icon), looked up ONCE at activation time (services/strategy_
    # activation.py) and cached here — never re-queried on the hot path.
    # FastForward2.tsx's getBrokerName() reads rec.broker_details.{alias,
    # display_name,...} FIRST, before falling back to rec.broker_label/
    # rec.broker (and only if those AREN'T ObjectId-shaped strings, which a
    # real broker_scope_id always is) — algo-2_0's checkpoint never
    # populated broker_details at all, so every real (non-test) activation
    # fell through every fallback and showed "Unknown" in the UI (e.g. the
    # Portfolio Monitor widget) even though the strategy's real, correct
    # broker was known the whole time, just never serialized.
    broker_details: dict[str, Any] = field(default_factory=dict)

    strategy_cfg: dict[str, Any] = field(default_factory=dict)  # OverallSL/OverallTgt/OverallTrailSL

    # "Quantity Multiplier" from the strategy's Edit Setup
    # (saved_strategies.execution_config_base.Multiplier): every leg enters
    # LotConfig lots × this many lots.
    qty_multiplier: int = 1

    # Slippage % chosen on the activation screen for THIS activation — every
    # leg of this strategy gets it on its entry and exit fill price.
    slippage_pct: float = 0.0
    leg_ids: set[str] = field(default_factory=set)

    # Original saved_strategies._id + FULL (untrimmed) config — kept only
    # for api/routers/strategy_history.py's "view deployed strategy config"
    # read (StrategyConfigModal2.tsx), separate from strategy_cfg above
    # (which is deliberately trimmed to just the risk-relevant keys —
    # see the comment where strategy_activation.py builds it).
    source_strategy_id: str = ""
    full_strategy_cfg: dict[str, Any] = field(default_factory=dict)

    # "user_id:broker_connection_id" this strategy runs under — master-doc
    # (unified risk engine) §23. "" for strategies not yet wired to a scope.
    broker_scope_id: str = ""

    mtm: float = 0.0
    peak_mtm: float = 0.0
    current_overall_sl_threshold: float = 0.0

    # LOCK/LOCK_AND_TRAIL/TRAIL_STOP_LOSS floor state (risk/evaluator.py) —
    # distinct from current_overall_sl_threshold above, which is the older
    # cycle-adjusted-SL-only trailing (§14 OverallTrailSL in overall_risk.py).
    current_floor: float = 0.0
    trailing_activated: bool = False

    # Reentry-cycle counters — master-doc §38/§44. Owned here (not a separate
    # counters object yet) since overall_risk's cycle-adjusted SL/target needs
    # them; Phase 4 will read/increment these as re-entries happen.
    sl_reentry_done: int = 0
    tgt_reentry_done: int = 0

    # ACTIVE -> EXIT_PENDING -> EXITED (master-doc §14/§47)
    status: str = "ACTIVE"
    activation_in_progress: bool = False

    # IST calendar date ("YYYY-MM-DD") this strategy was first activated —
    # set ONCE at construction (services/strategy_activation.py) and never
    # recomputed on later checkpoints, unlike the doc-level `creation_ts`
    # (persistence/serializers.py's strategy_to_doc stamps that fresh on
    # EVERY checkpoint, so it can't be used to tell an old position from a
    # new one). main.py's `_daily_eod_square_off_loop` compares this against
    # today's IST date at midnight rollover to auto-square-off + drop
    # anything left over from a prior day (user's explicit report: without
    # this, a position that was already effectively stale from days ago —
    # or recovered from a checkpoint written before this field existed, "" —
    # keeps showing on /fast-forward_2 as if still live).
    activated_date: str = ""
