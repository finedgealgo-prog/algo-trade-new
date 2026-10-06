"""
services/straddle_alert.py
──────────────────────────
Crypto ATM straddle spike alert (FastForward2.tsx's "Straddle Alert").

Each alert doc in `crypto_straddle_alerts` tracks one underlying+expiry.
Every CHECK_INTERVAL_SECONDS the loop pulls a fresh Delta REST chain, picks
that moment's ATM (strike nearest spot with both CE and PE quoted), and
records CE+PE. It keeps the lowest straddle seen since the alert started;
once the current straddle is >= lowest * (1 + rise_pct/100) the alert
becomes `triggered` (one-shot). If the alert has an `on_trigger` target
(a saved strategy or portfolio), that target is activated right then with
its entry time ignored (activate_strategy(skip_entry_time=True)) — the
spike itself is the entry signal. Then one Telegram message goes out with
the trigger + activation outcome.

Prices are Delta's raw per-1-BTC/ETH marks (`mark_raw`), the same unit the
straddle chart and the option_chain collector use.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from features.delta_exchange_client import fetch_option_chain_rest
from shared.db.mongo import get_mongo

log = logging.getLogger(__name__)

COLLECTION = "crypto_straddle_alerts"
CHECK_INTERVAL_SECONDS = 30
TELEGRAM_CATEGORY = "algo"

STATUS_MONITORING = "monitoring"
STATUS_TRIGGERED = "triggered"
STATUS_STOPPED = "stopped"
STATUS_EXPIRED = "expired"

# Delta options settle at 12:00 UTC (17:30 IST) on the expiry date.
_EXPIRY_HOUR_UTC = 12


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def day_start_iso() -> str:
    """Start of the current alert day — 00:00 UTC = 05:30 IST. Alerts are
    day-scoped: the list shows only today's, and anything still monitoring
    from before this boundary is stopped by the loop."""
    return _utc_now().strftime("%Y-%m-%dT00:00:00Z")


def _expiry_cutoff(expiry: str) -> datetime | None:
    try:
        d = datetime.strptime(expiry, "%d-%m-%Y")
    except ValueError:
        return None
    return d.replace(hour=_EXPIRY_HOUR_UTC, tzinfo=timezone.utc)


def compute_atm_straddle(underlying: str, expiry: str) -> dict | None:
    """Blocking (one Delta REST call). Returns the current ATM straddle
    reading, or None if Delta has no usable quote right now."""
    chain = fetch_option_chain_rest(underlying, expiry)
    spot = float(chain.get("spot_price") or 0.0)
    if spot <= 0:
        return None

    ce_by_strike = {float(r["strike"]): float(r.get("mark_raw") or 0) for r in chain.get("chain", {}).get("CE", [])}
    pe_by_strike = {float(r["strike"]): float(r.get("mark_raw") or 0) for r in chain.get("chain", {}).get("PE", [])}
    strikes = [k for k in ce_by_strike if ce_by_strike[k] > 0 and pe_by_strike.get(k, 0) > 0]
    if not strikes:
        return None

    strike = min(strikes, key=lambda k: abs(k - spot))
    ce, pe = ce_by_strike[strike], pe_by_strike[strike]
    return {
        "spot": round(spot, 2),
        "strike": strike,
        "ce": round(ce, 2),
        "pe": round(pe, 2),
        "straddle": round(ce + pe, 2),
    }


def _send_alert(alert: dict, reading: dict, rise_pct_actual: float, activation_summary: str = "") -> None:
    from features.telegram_notifier import notify_user_for  # type: ignore

    db = get_mongo().raw
    mobile = str(alert.get("notify_mobile") or "")
    user_doc = db["user_details"].find_one({"mobile": mobile}) if mobile else None
    message = (
        f"{alert['underlying']} {alert['expiry']} ATM straddle up {rise_pct_actual:.1f}% "
        f"from lowest {alert['lowest_straddle']:.2f} → now {reading['straddle']:.2f}\n"
        f"Strike {reading['strike']:.0f}  CE {reading['ce']:.2f}  PE {reading['pe']:.2f}  Spot {reading['spot']:.2f}\n"
        f"Threshold: +{alert['rise_pct']}% (lowest at {alert.get('lowest_at', '')})"
    )
    if activation_summary:
        message += f"\n{activation_summary}"
    sent, _ = notify_user_for(
        user_doc or mobile, "STRADDLE_SPIKE", message,
        context={"trade_id": str(alert["_id"])}, category=TELEGRAM_CATEGORY,
    )
    if not sent:
        log.warning("[straddle_alert] telegram not sent for alert %s (notifications disabled or no chat configured)", alert["_id"])


def check_alert(alert: dict, reading: dict | None) -> dict:
    """Applies one reading to one alert; returns the Mongo $set to persist."""
    now = _utc_now()
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    update: dict = {"last_checked_at": now_iso}

    cutoff = _expiry_cutoff(alert["expiry"])
    if cutoff and now >= cutoff:
        update["status"] = STATUS_EXPIRED
        return update
    if str(alert.get("created_at") or "") < day_start_iso():
        update["status"] = STATUS_EXPIRED
        update["expired_reason"] = "day_end"
        return update
    if reading is None:
        return update

    update["current"] = reading
    lowest = alert.get("lowest_straddle")
    if lowest is None or reading["straddle"] < lowest:
        update["lowest_straddle"] = reading["straddle"]
        update["lowest_at"] = now_iso
        update["lowest_reading"] = reading
        return update

    target = lowest * (1 + float(alert["rise_pct"]) / 100.0)
    if reading["straddle"] >= target:
        rise = (reading["straddle"] / lowest - 1) * 100 if lowest > 0 else 0.0
        update["status"] = STATUS_TRIGGERED
        update["triggered_at"] = now_iso
        update["triggered_reading"] = reading
        update["triggered_rise_pct"] = round(rise, 2)
        log.info("[straddle_alert] %s triggered: %s → %s (+%.1f%%)", alert["_id"], lowest, reading["straddle"], rise)
    return update


def run_checks_once() -> list[tuple[dict, dict]]:
    """Blocking — one pass over every monitoring alert. One REST call per
    distinct underlying+expiry, shared by every alert on it. Returns the
    (alert, update) pairs that triggered on this pass — their activation +
    Telegram is done by the async loop, which has the event loop that
    activate_strategy needs."""
    coll = get_mongo().raw[COLLECTION]
    alerts = list(coll.find({"status": STATUS_MONITORING}))
    readings: dict[tuple[str, str], dict | None] = {}
    triggered: list[tuple[dict, dict]] = []
    for alert in alerts:
        key = (alert["underlying"], alert["expiry"])
        if key not in readings:
            try:
                readings[key] = compute_atm_straddle(*key)
            except Exception:
                log.exception("[straddle_alert] chain fetch failed for %s %s", *key)
                readings[key] = None
        update = check_alert(alert, readings[key])
        res = coll.update_one({"_id": alert["_id"], "status": STATUS_MONITORING}, {"$set": update})
        if res.modified_count and update.get("status") == STATUS_TRIGGERED:
            triggered.append((alert, update))
    return triggered


_IST = timezone(timedelta(hours=5, minutes=30))
_WEEKDAY_CODES = ("M", "T", "W", "Th", "F", "Sa", "Su")  # datetime.weekday() order


def _portfolio_members(portfolio: dict) -> list[str]:
    """Same member selection PortfolioActivation.tsx makes by default:
    Weekdays mode → strategies scheduled for today (IST); DTE mode → the
    saved checked rows."""
    meta = {str(m.get("id")): m for m in portfolio.get("strategies") or [] if isinstance(m, dict)}
    weekday_mode = portfolio.get("is_weekdays") is not False
    today = _WEEKDAY_CODES[datetime.now(_IST).weekday()]
    members = []
    for sid in portfolio.get("strategy_ids") or []:
        m = meta.get(str(sid), {})
        if weekday_mode:
            if today in (m.get("weekdays") or ["M", "T", "W", "Th", "F"]):
                members.append(str(sid))
        elif m.get("checked", True) is not False:
            members.append(str(sid))
    return members


async def _activate_target(alert: dict) -> tuple[list[dict], str]:
    """Activates the alert's on_trigger target now, entry time ignored.
    Returns (per-strategy results, one-line summary for Telegram)."""
    from bson import ObjectId

    from main import order_engine, token_router
    from api.live_broadcast import broadcast_strategy_snapshots
    from shared.market.instrument_master import get_instrument_master
    from shared.market.ltp_cache import get_ltp_cache
    from services.strategy_activation import ActivationError, activate_strategy

    target = alert.get("on_trigger") or {}
    kind, target_id, broker = target.get("kind"), str(target.get("id") or ""), str(target.get("broker") or "")
    if kind not in ("strategy", "portfolio") or not target_id:
        return [], ""
    mongo = get_mongo()
    user_id = str(alert.get("user_id") or "")
    alert_qty = max(1, int(target.get("qty_multiplier") or 1))

    group_id = group_name = portfolio_id = ""
    if kind == "portfolio":
        from api.routers.portfolio import _resolve_activation_group
        portfolio = await asyncio.to_thread(mongo.raw["saved_portfolios"].find_one, {"_id": ObjectId(target_id)})
        if not portfolio:
            return [], f"Portfolio {target_id} not found — nothing activated"
        picked = target.get("strategies")
        if picked:
            # Exact rows + qty chosen on the PortfolioActivation screen.
            plan = [(str(p["id"]), max(1, int(p.get("qty_multiplier") or 1))) for p in picked]
        else:
            portfolio_qty = alert_qty * max(1, int(portfolio.get("qty_multiplier") or 1))
            plan = [(sid, portfolio_qty) for sid in _portfolio_members(portfolio)]
        if not plan:
            return [], f"Portfolio '{portfolio.get('name')}': no strategy scheduled for today — nothing activated"
        portfolio_id = target_id
        group_id, group_name = _resolve_activation_group(mongo, token_router, portfolio_id, portfolio)
    else:
        # Only override the strategy's own saved multiplier when one was set on the alert.
        plan = [(target_id, alert_qty if target.get("qty_multiplier") else None)]
    slippage = max(0.0, float(target.get("slippage_pct") or 0))

    results: list[dict] = []
    for sid, qty in plan:
        doc = await asyncio.to_thread(mongo.raw["saved_strategies"].find_one, {"_id": ObjectId(sid)})
        if not doc:
            results.append({"strategy_id": sid, "success": False, "error": "strategy not found"})
            continue
        broker_scope_id = broker or str(doc.get("broker") or "")
        try:
            res = await activate_strategy(
                token_router, order_engine, get_instrument_master(), get_ltp_cache(), mongo,
                doc, user_id, broker_scope_id, is_direct_strategy=not portfolio_id,
                group_id=group_id, group_name=group_name, portfolio_id=portfolio_id,
                qty_multiplier=qty, slippage_pct=slippage, skip_entry_time=True,
            )
            results.append({"strategy_id": sid, "name": doc.get("name"), "success": True, **res})
        except ActivationError as exc:
            results.append({"strategy_id": sid, "name": doc.get("name"), "success": False, "error": str(exc)})
        except Exception as exc:
            log.exception("[straddle_alert] activation failed for strategy %s", sid)
            results.append({"strategy_id": sid, "name": doc.get("name"), "success": False, "error": str(exc)})

    ok_ids = [r["strategy_id"] for r in results if r.get("success")]
    if ok_ids:
        await broadcast_strategy_snapshots(token_router, ok_ids)
    ok = sum(1 for r in results if r.get("success"))
    summary = f"Activated {kind} '{target.get('name') or target_id}': {ok}/{len(results)} strategies entered"
    failed = [f"{r.get('name') or r['strategy_id']}: {r.get('error')}" for r in results if not r.get("success")]
    if failed:
        summary += "\nFailed: " + "; ".join(failed)
    return results, summary


async def _handle_triggered(alert: dict, update: dict) -> None:
    alert = {**alert, **update}
    results, summary = [], ""
    if alert.get("on_trigger"):
        try:
            results, summary = await _activate_target(alert)
        except Exception as exc:
            log.exception("[straddle_alert] on_trigger activation crashed for alert %s", alert["_id"])
            summary = f"Activation failed: {exc}"
        await asyncio.to_thread(
            get_mongo().raw[COLLECTION].update_one,
            {"_id": alert["_id"]},
            {"$set": {"activation_results": results, "activation_summary": summary}},
        )
    await asyncio.to_thread(
        _send_alert, alert, update["triggered_reading"], update["triggered_rise_pct"], summary,
    )


# ── Loop control (Monitors page: start / stop / status) ─────────────────
_loop_task: asyncio.Task | None = None
_loop_state: dict = {"started_at": None, "last_pass_at": None, "last_error": None}


async def straddle_alert_loop() -> None:
    while True:
        try:
            triggered = await asyncio.to_thread(run_checks_once)
            for alert, update in triggered:
                await _handle_triggered(alert, update)
            _loop_state["last_error"] = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("[straddle_alert] check pass failed")
            _loop_state["last_error"] = str(exc)
        _loop_state["last_pass_at"] = _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


def is_running() -> bool:
    return _loop_task is not None and not _loop_task.done()


def start_loop() -> dict:
    """Idempotent. Must be called from the event loop (startup / a route)."""
    global _loop_task
    if is_running():
        return {"status": "already_running"}
    _loop_task = asyncio.get_running_loop().create_task(straddle_alert_loop(), name="crypto_straddle_alert")
    _loop_state["started_at"] = _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("[straddle_alert] monitor started")
    return {"status": "started"}


async def stop_loop() -> dict:
    global _loop_task
    if not is_running():
        return {"status": "already_stopped"}
    _loop_task.cancel()
    await asyncio.gather(_loop_task, return_exceptions=True)
    _loop_task = None
    log.info("[straddle_alert] monitor stopped")
    return {"status": "stopped"}


def loop_status() -> dict:
    active = get_mongo().raw[COLLECTION].count_documents({"status": STATUS_MONITORING})
    return {
        "running": is_running(),
        "check_interval_seconds": CHECK_INTERVAL_SECONDS,
        "active_alerts": active,
        **_loop_state,
    }
