"""
runtime_risk.py
───────────────
Post-activation SL / Target / Lock / Lock & Trail / Trail SL for ONE running
strategy or ONE portfolio activation group — set from FastForward2.tsx the
same way broker settings are (same modal, same old-system settings shape:
StopLoss, Target, LockAndTrail {InstrumentMove, StopLossMove},
OverallTrailSL {InstrumentMove, StopLossMove}).

Independent of, and evaluated after, the broker and the strategy's own
saved-config risk (token_router._evaluate_runtime_risks). When one fires,
every strategy in its scope is squared off (main.py's trigger listener)
and the setting goes to status HIT — it never re-arms by itself.

Scope MTM:
  strategy → that strategy's MTM.
  group    → sum of the last MTM seen for each member strategy, so a member
             that already exited still counts with its final P&L.

Persisted in `algo2_runtime_risk_settings` (settings + lock/trail state),
one doc per scope target; today's ACTIVE docs are reloaded on restart.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from risk.broker_risk import build_broker_risk_config
from risk.evaluator import EvalResult, PeakWindow, effective_stop_loss, evaluate_risk
from risk.models import RiskConfig

log = logging.getLogger(__name__)

COLLECTION = "algo2_runtime_risk_settings"
SCOPES = ("strategy", "group")
_IST = timezone(timedelta(hours=5, minutes=30))


def today_ist() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d")


def risk_key(scope: str, target_id: str) -> str:
    return f"{scope}:{target_id}"


# Peak confirmation per scope key (see evaluator.PeakWindow) — in memory only.
_PEAK_WINDOWS: dict[str, PeakWindow] = {}


@dataclass
class RuntimeRisk:
    scope: str
    target_id: str
    user_id: str
    name: str
    settings: dict[str, Any]
    config: RiskConfig
    peak: float = 0.0
    floor: float = 0.0
    trailing_activated: bool = False
    status: str = "ACTIVE"  # ACTIVE | HIT
    hit_reason: str = ""
    last_mtm: float = 0.0
    member_mtm: dict[str, float] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return risk_key(self.scope, self.target_id)

    def evaluate(self, mtm: float) -> EvalResult:
        window = _PEAK_WINDOWS.setdefault(self.key, PeakWindow())
        result = evaluate_risk(mtm, self.config, self.peak, self.floor, self.trailing_activated, peak_candidate=window.candidate(mtm))
        self.peak = result.new_peak_pnl
        self.floor = result.new_floor
        self.trailing_activated = result.trailing_activated
        self.last_mtm = mtm
        return result

    def state(self) -> dict[str, Any]:
        """Same field names as the broker strip's `state` block."""
        cfg = self.config
        return {
            "lock_activated": bool(self.trailing_activated),
            "current_lock_floor": round(float(self.floor), 2) if self.trailing_activated else 0.0,
            "lock_peak_mtm": round(float(self.peak), 2),
            "effective_sl": effective_stop_loss(cfg, self.peak) if cfg.stop_loss_enabled else None,
            "mtm": round(float(self.last_mtm), 2),
        }


def from_doc(doc: dict[str, Any]) -> RuntimeRisk:
    settings = doc.get("settings") or {}
    state = doc.get("state") or {}
    rr = RuntimeRisk(
        scope=doc["scope"], target_id=doc["target_id"], user_id=str(doc.get("user_id") or ""),
        name=str(doc.get("name") or ""), settings=settings, config=build_broker_risk_config(settings),
        status=str(doc.get("status") or "ACTIVE"), hit_reason=str(doc.get("hit_reason") or ""),
    )
    # Lock state only carries over within the same IST day.
    if doc.get("trade_date") == today_ist():
        rr.peak = float(state.get("lock_peak_mtm") or 0.0)
        rr.trailing_activated = bool(state.get("lock_activated"))
        rr.floor = float(state.get("current_lock_floor") or 0.0) if rr.trailing_activated else 0.0
        rr.last_mtm = float(state.get("mtm") or 0.0)
        rr.member_mtm = {str(k): float(v) for k, v in (doc.get("member_mtm") or {}).items()}
    return rr


def to_doc(rr: RuntimeRisk) -> dict[str, Any]:
    return {
        "_id": rr.key, "scope": rr.scope, "target_id": rr.target_id, "user_id": rr.user_id,
        "name": rr.name, "settings": rr.settings, "status": rr.status, "hit_reason": rr.hit_reason,
        "state": rr.state(), "member_mtm": rr.member_mtm, "trade_date": today_ist(),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def save(mongo, rr: RuntimeRisk) -> None:
    """Blocking."""
    mongo.raw[COLLECTION].replace_one({"_id": rr.key}, to_doc(rr), upsert=True)


def load_active(mongo) -> list[RuntimeRisk]:
    """Blocking — today's still-armed settings, for startup."""
    docs = mongo.raw[COLLECTION].find({"status": "ACTIVE", "trade_date": today_ist()})
    out = []
    for doc in docs:
        try:
            out.append(from_doc(doc))
        except Exception:
            log.exception("[RuntimeRisk] bad settings doc %s", doc.get("_id"))
    return out
