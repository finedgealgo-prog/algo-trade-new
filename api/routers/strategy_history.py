"""
routers/strategy_history.py
────────────────────────────
`GET /algo/strategy-trade-history/{strategy_id}` — TWO real callers read
this same route:
  - StrategyConfigModal2.tsx's "view deployed strategy config" overlay
    (feeds parseStrategyConfig() -> ResultSummaryTable/leg cards).
  - AnalyseCryptoPaperTrade.tsx's fetchExternalEntity — the `/analyse-
    crypto/strategy/:id` "Analyse" button flow off FastForward2.tsx —
    which additionally needs `legs.open`/`legs.closed` (real entry/exit
    prices, strike, expiry, feature_status_map) to actually draw the
    payoff/P&L view; without them it silently rendered an empty "Build
    your Strategy" page instead of the real position (user's explicit
    report, confirmed: this route used to return `{"trade": {"strategy":
    doc.get("strategy") ...}}` — `doc["strategy"]` was never a real key
    on an algo_trades doc THIS engine writes at all, always empty, and
    `legs` was missing from the response entirely).

Backed by algo-2_0's own `algo_trades` collection — services/
strategy_activation.py writes one row there per activation. The FULL,
untrimmed config lives under `_runtime_full_strategy_cfg` (StrategyRuntime.
full_strategy_cfg — see that field's own comment: StrategyRuntime.
strategy_cfg is deliberately trimmed to just the risk-relevant keys, never
enough to reconstruct what was actually deployed). `legs` is already the
exact wire shape persistence/serializers.py's _leg_to_wire produces
(entry_trade/exit_trade/strike/expiry_date/feature_status_map/token/...) —
split into open (status==1) / closed (anything else) buckets here, same
open/closed shape the frontend's fetchExternalEntity expects.

Debug-registered legs (api/routers/admin.py's /debug/register-leg —
crypto quick-test, no saved strategy behind them) never write to
algo_trades, so they 404 here — correct, there's no "config" to show for
those.

Two backfills below cover a strategy that was already ACTIVE (checkpointed
repeatedly, in RAM this whole time) at the moment this route's own fix
was deployed — its StrategyRuntime.full_strategy_cfg/LegRuntime.strike/
expiry stayed at their PRE-fix values (empty/0) in RAM, so every
checkpoint since keeps re-persisting those same empty values; nothing
about redeploying this route retroactively fixes a doc already stuck that
way, only a fresh activation would. Rather than leave every already-open
position broken until it happens to exit and re-enter, both fields are
reconstructed here from data that's independently still available:
  - `strategy`: _runtime_strategy_cfg (Ticker/OverallSL/OverallTgt/
    LockAndTrail/IdleLegConfigs) + each leg's own `_runtime_leg_cfg`
    (always correct — checkpoint_strategy's legs= re-reads token_router
    fresh every call) rebuilt into ListOfLegConfigs.
  - leg strike/expiry: looked up by token from the SAME static instrument
    caches strike_resolver.py itself reads from (instrument_master for
    NSE, delta_instrument_cache for crypto) — a pure display fallback,
    never written back to the doc.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from shared.db.mongo import get_mongo
from shared.market.delta_instrument_cache import get_delta_instrument_cache
from shared.market.instrument_master import get_instrument_master

router = APIRouter(prefix="/algo", tags=["strategy-history"])


def _backfill_leg_strike_expiry(leg: dict) -> dict:
    if leg.get("strike") and leg.get("expiry_date"):
        return leg
    token = str(leg.get("token") or "")
    if not token:
        return leg
    meta = get_instrument_master().get(token) or get_delta_instrument_cache().get(token)
    if meta is None:
        return leg
    leg = dict(leg)
    if not leg.get("strike"):
        leg["strike"] = meta.strike
    if not leg.get("expiry_date"):
        leg["expiry_date"] = meta.expiry
        leg["expiry"] = meta.expiry
    return leg


@router.get("/strategy-trade-history/{strategy_id}")
def strategy_trade_history(strategy_id: str, status: str = "") -> dict:
    mongo = get_mongo()
    doc = mongo.raw["algo_trades"].find_one({"_id": strategy_id})
    if not doc:
        raise HTTPException(status_code=404, detail="No deployment record found for this strategy")
    legs = [_backfill_leg_strike_expiry(leg) for leg in (doc.get("legs") or [])]
    open_legs = [leg for leg in legs if leg.get("status") == 1]
    closed_legs = [leg for leg in legs if leg.get("status") != 1]

    strategy_cfg = doc.get("_runtime_full_strategy_cfg") or {}
    if not strategy_cfg.get("ListOfLegConfigs"):
        rebuilt_legs = [leg.get("_runtime_leg_cfg") for leg in legs if leg.get("_runtime_leg_cfg")]
        if rebuilt_legs:
            strategy_cfg = {**(doc.get("_runtime_strategy_cfg") or {}), "ListOfLegConfigs": rebuilt_legs}

    return {
        "trade": {
            "strategy": strategy_cfg,
            "name": doc.get("name") or "",
            "ticker": doc.get("ticker") or "",
        },
        "legs": {"open": open_legs, "closed": closed_legs},
    }
