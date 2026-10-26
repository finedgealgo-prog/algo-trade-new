"""
auto_activation.py
──────────────────
Portfolio "Auto Activate" (FastForward2 / AlgoTrade2): the user picks broker,
qty and slippage once in the activation popup; every day at 17:31 IST
(5:31 PM — just after the 17:30 crypto day rollover) this activates that
portfolio again — only the strategies scheduled for that day,
by the same rule PortfolioActivation.tsx uses to pre-tick rows:

  Weekdays mode (portfolio.is_weekdays, default) -> a strategy runs when its
      weekdays (default M-F) include today.
  DTE mode -> the strategy's saved `checked` flag.

The portfolio doc is read fresh every day, so edits to it (strategies added,
weekdays changed) apply from the next run.

Exactly once per IST day per portfolio+mode: the run is claimed with an
atomic last_run_date update before anything is activated, so a restart (or
two loop ticks) can never activate it twice; a restart after 05:31 still
runs that day's activation (catch-up).

A single strategy can be auto-activated too (target_type "strategy"): it
runs on the days its Edit Setup execution days allow (weekdays map; every
day in DTE mode), as a direct strategy activation.

Persisted in `algo2_auto_activations`, one doc per (target, mode).
"""

from __future__ import annotations

from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any

from shared.logging.logger import get_logger

log = get_logger(__name__)

COLLECTION = "algo2_auto_activations"
# One row per (config, day) run — per-user history of what auto activation did.
RUNS_COLLECTION = "algo2_auto_activation_runs"
RUN_AT_IST = dtime(17, 31)  # 5:31 PM IST, right after the 17:30 crypto day rollover
_IST = timezone(timedelta(hours=5, minutes=30))
_DAY_CODES = ("M", "T", "W", "Th", "F", "Sa", "Su")
_DEFAULT_WEEKDAYS = ["M", "T", "W", "Th", "F"]


_FULL_DAY = {"M": "monday", "T": "tuesday", "W": "wednesday", "Th": "thursday", "F": "friday", "Sa": "saturday", "Su": "sunday"}


def config_id(target_id: str, activation_mode: str, target_type: str = "portfolio") -> str:
    if target_type == "strategy":
        return f"strategy:{target_id}:{activation_mode}"
    return f"{target_id}:{activation_mode}"


def strategy_runs_today(strategy_doc: dict[str, Any], day: datetime) -> bool:
    """Edit Setup's execution days: weekdays map ({"monday": true, ...},
    default M-F when none set); DTE mode -> every day."""
    if strategy_doc.get("is_weekdays") is False:
        return True
    weekdays = strategy_doc.get("weekdays") or {}
    code = today_code(day)
    if not any(weekdays.values()):
        return code in _DEFAULT_WEEKDAYS
    return bool(weekdays.get(_FULL_DAY[code]))


def now_ist() -> datetime:
    return datetime.now(_IST)


def today_code(day: datetime) -> str:
    return _DAY_CODES[day.weekday()]


def todays_strategy_ids(portfolio_doc: dict[str, Any], day: datetime, excluded=None) -> list[str]:
    """Strategy ids of this portfolio that run on `day` (see module doc)."""
    weekday_mode = portfolio_doc.get("is_weekdays", True) is not False
    code = today_code(day)
    meta_by_id = {
        str(m.get("id") or m.get("_id") or ""): m
        for m in portfolio_doc.get("strategies") or [] if isinstance(m, dict)
    }
    out = []
    excluded = set(excluded or [])
    for sid in [str(s) for s in portfolio_doc.get("strategy_ids") or []]:
        if sid in excluded:
            continue
        meta = meta_by_id.get(sid, {})
        if weekday_mode:
            if code in (meta.get("weekdays") or _DEFAULT_WEEKDAYS):
                out.append(sid)
        elif meta.get("checked", True) is not False:
            out.append(sid)
    return out


def record_run(mongo, cfg: dict[str, Any], day: datetime, summary: dict[str, Any], target_name: str) -> None:
    """Blocking — append this run to the per-user history table."""
    try:
        mongo.raw[RUNS_COLLECTION].insert_one({
            "config_id": cfg.get("_id"), "user_id": cfg.get("user_id", ""),
            "target_type": cfg.get("target_type", "portfolio"),
            "target_id": cfg.get("target_id") or cfg.get("portfolio_id") or cfg.get("strategy_id"),
            "target_name": target_name, "activation_mode": cfg.get("activation_mode"),
            "run_date": day.strftime("%Y-%m-%d"), "ran_at": datetime.now(timezone.utc).isoformat(),
            "ok": summary.get("ok", 0), "failed": summary.get("failed", 0), "skipped": summary.get("skipped", 0),
            "error": summary.get("error", ""), "results": summary.get("results", []),
        })
    except Exception:
        log.exception("[AutoActivation] run history write failed config=%s", cfg.get("_id"))


def already_running(token_router, source_strategy_id: str, activation_mode: str) -> bool:
    """A strategy the user already activated by hand today (still running in
    this mode) is not activated a second time by the auto run."""
    if token_router is None:
        return False
    return any(
        s.source_strategy_id == source_strategy_id and (s.activation_mode or "fast-forward") == activation_mode
        and s.status != "EXITED"
        for s in list(token_router.strategies.values())
    )


def due(cfg: dict[str, Any], day: datetime) -> bool:
    return bool(cfg.get("enabled")) and day.time() >= RUN_AT_IST and cfg.get("last_run_date") != day.strftime("%Y-%m-%d")


def claim(mongo, cfg_id: str, day: datetime) -> bool:
    """Atomic once-per-day claim. Blocking."""
    date = day.strftime("%Y-%m-%d")
    res = mongo.raw[COLLECTION].update_one(
        {"_id": cfg_id, "enabled": True, "last_run_date": {"$ne": date}},
        {"$set": {"last_run_date": date, "last_run_started_at": datetime.now(timezone.utc).isoformat()}},
    )
    return res.modified_count == 1


async def run_one(cfg: dict[str, Any], day: datetime, *, token_router, order_engine, mongo, activate, resolve_group,
                  notify=None) -> dict[str, Any]:
    """Activate one portfolio's strategies for `day`. `activate` is
    services.strategy_activation.activate_strategy (with router/engine/etc.
    bound by the caller); `resolve_group` portfolio._resolve_activation_group."""
    import asyncio

    from bson import ObjectId

    from services.strategy_activation import ActivationError

    if cfg.get("target_type") == "strategy":
        return await _run_strategy(cfg, day, mongo=mongo, activate=activate, notify=notify, token_router=token_router)

    portfolio_id = cfg["portfolio_id"]
    portfolio_doc = await asyncio.to_thread(mongo.raw["saved_portfolios"].find_one, {"_id": ObjectId(portfolio_id)}) or {}
    if not portfolio_doc:
        summary = {"ok": 0, "failed": 0, "skipped": 0, "error": "portfolio not found"}
    else:
        ids = todays_strategy_ids(portfolio_doc, day, cfg.get("excluded_strategy_ids"))
        group_id, group_name = resolve_group(mongo, token_router, portfolio_id, portfolio_doc)
        strategy_qty = cfg.get("strategy_qty") or {}
        portfolio_qty = max(1, int(cfg.get("portfolio_qty") or 1))
        results = []
        for sid in ids:
            try:
                strategy_doc = await asyncio.to_thread(mongo.raw["saved_strategies"].find_one, {"_id": ObjectId(sid)})
            except Exception:
                strategy_doc = None
            if not strategy_doc:
                results.append({"strategy_id": sid, "success": False, "error": "strategy not found"})
                continue
            if already_running(token_router, sid, cfg["activation_mode"]):
                results.append({"strategy_id": sid, "name": strategy_doc.get("name"), "success": False,
                                "already_running": True, "error": "already running (activated manually) — not activated again"})
                continue
            qty = int(strategy_qty.get(sid) or portfolio_qty)
            try:
                res = await activate(
                    strategy_doc, cfg["user_id"], cfg["broker_scope_id"], is_direct_strategy=False,
                    group_id=group_id, group_name=group_name, portfolio_id=portfolio_id,
                    qty_multiplier=max(1, qty), slippage_pct=float(cfg.get("slippage_pct") or 0),
                    activation_mode=cfg["activation_mode"],
                )
                results.append({"strategy_id": sid, "name": strategy_doc.get("name"), "success": True, **res})
            except ActivationError as exc:
                results.append({"strategy_id": sid, "name": strategy_doc.get("name"), "success": False, "error": str(exc)})
            except Exception as exc:  # never let one strategy stop the rest
                log.exception("[AutoActivation] strategy=%s failed", sid)
                results.append({"strategy_id": sid, "name": strategy_doc.get("name"), "success": False, "error": str(exc)})
        summary = {
            "ok": sum(1 for r in results if r["success"]),
            "failed": sum(1 for r in results if not r["success"] and not r.get("already_running")),
            "skipped": len([str(s) for s in portfolio_doc.get("strategy_ids") or []]) - len(ids)
                       + sum(1 for r in results if r.get("already_running")),
            "results": results, "group_id": group_id,
        }
    await asyncio.to_thread(
        mongo.raw[COLLECTION].update_one, {"_id": config_id(portfolio_id, cfg["activation_mode"])},
        {"$set": {"last_result": summary, "last_run_finished_at": datetime.now(timezone.utc).isoformat()}},
    )
    await asyncio.to_thread(record_run, mongo, cfg, day, summary, portfolio_doc.get("name") or portfolio_id)
    log.info("[AutoActivation] portfolio=%s mode=%s day=%s -> ok=%s failed=%s skipped=%s",
             portfolio_id, cfg["activation_mode"], day.date(), summary.get("ok"), summary.get("failed"), summary.get("skipped"))
    if notify is not None:
        name = portfolio_doc.get("name") or portfolio_id
        lines = [f"Auto Activate — {name} ({cfg['activation_mode']}) {day.strftime('%d-%m-%Y')}: "
                 f"{summary.get('ok', 0)} activated, {summary.get('failed', 0)} failed, {summary.get('skipped', 0)} not scheduled today."]
        lines += [f"✗ {r.get('name') or r['strategy_id']}: {r.get('error')}" for r in summary.get("results", []) if not r["success"] and not r.get("already_running")]
        lines += [f"• {r.get('name') or r['strategy_id']}: already running (manual) — skipped" for r in summary.get("results", []) if r.get("already_running")]
        if summary.get("error"):
            lines.append(f"✗ {summary['error']}")
        try:
            await asyncio.to_thread(notify, cfg["user_id"], "AUTO_ACTIVATION", "\n".join(lines))
        except Exception:
            log.exception("[AutoActivation] notify failed")
    return summary


async def _run_strategy(cfg: dict[str, Any], day: datetime, *, mongo, activate, notify=None, token_router=None) -> dict[str, Any]:
    import asyncio

    from bson import ObjectId

    from services.strategy_activation import ActivationError

    sid = cfg["strategy_id"]
    try:
        doc = await asyncio.to_thread(mongo.raw["saved_strategies"].find_one, {"_id": ObjectId(sid)})
    except Exception:
        doc = None
    name = (doc or {}).get("name") or sid
    if not doc:
        summary = {"ok": 0, "failed": 0, "skipped": 0, "error": "strategy not found", "results": []}
    elif not strategy_runs_today(doc, day):
        summary = {"ok": 0, "failed": 0, "skipped": 1, "results": []}
    elif already_running(token_router, sid, cfg["activation_mode"]):
        summary = {"ok": 0, "failed": 0, "skipped": 1, "already_running": True, "results": [
            {"strategy_id": sid, "name": name, "success": False, "already_running": True,
             "error": "already running (activated manually) — not activated again"}]}
    else:
        try:
            res = await activate(
                doc, cfg["user_id"], cfg["broker_scope_id"], is_direct_strategy=True,
                qty_multiplier=max(1, int(cfg.get("qty_multiplier") or 1)),
                slippage_pct=float(cfg.get("slippage_pct") or 0), activation_mode=cfg["activation_mode"],
            )
            results = [{"strategy_id": sid, "name": name, "success": True, **res}]
        except ActivationError as exc:
            results = [{"strategy_id": sid, "name": name, "success": False, "error": str(exc)}]
        except Exception as exc:
            log.exception("[AutoActivation] strategy=%s failed", sid)
            results = [{"strategy_id": sid, "name": name, "success": False, "error": str(exc)}]
        summary = {"ok": int(results[0]["success"]), "failed": int(not results[0]["success"]), "skipped": 0, "results": results}
    await asyncio.to_thread(
        mongo.raw[COLLECTION].update_one, {"_id": config_id(sid, cfg["activation_mode"], "strategy")},
        {"$set": {"last_result": summary, "last_run_finished_at": datetime.now(timezone.utc).isoformat()}},
    )
    await asyncio.to_thread(record_run, mongo, cfg, day, summary, name)
    log.info("[AutoActivation] strategy=%s mode=%s day=%s -> %s", sid, cfg["activation_mode"], day.date(), summary)
    if notify is not None and not summary.get("skipped"):
        line = (f"Auto Activate — {name} ({cfg['activation_mode']}) {day.strftime('%d-%m-%Y')}: "
                + ("activated." if summary.get("ok") else f"FAILED — {(summary.get('results') or [{}])[0].get('error') or summary.get('error')}"))
        try:
            await asyncio.to_thread(notify, cfg["user_id"], "AUTO_ACTIVATION", line)
        except Exception:
            log.exception("[AutoActivation] notify failed")
    return summary
