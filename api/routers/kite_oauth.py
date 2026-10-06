"""
routers/kite_oauth.py
────────────────────────
Route/path-compatible port of the old algo.trade's Kite/Zerodha OAuth login
flow (api.py:8131-9690). No "/algo" prefix — same bare paths as the old
system since Zerodha's own Kite Connect app settings register
/broker/kite/redirect (or /live/kite-callback) as a literal redirect URL.

All session/token logic is unchanged, real code from
shared/features/kite_broker.py (get_login_url, generate_session,
save_kite_session, get_stored_access_token,
sync_kite_access_token_by_credentials — reached via the algo.trade/features
symlink) — this file only adds the FastAPI routes and the same
`kite_market_config` Mongo reads/writes the old ones did.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from features.broker_accounts import DEFAULT_APP_USER_ID
from features.kite_broker import (
    generate_session,
    get_kite_instance,
    get_login_url,
    get_stored_access_token,
    save_kite_session,
    sync_kite_access_token_by_credentials,
)
from features.time_utils import to_utc_iso

from shared.db.mongo import get_mongo

router = APIRouter(tags=["kite-oauth"])

_kite_pending: dict[str, str] = {}


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized = str(value or "").strip()
    return normalized or DEFAULT_APP_USER_ID


def _kite_popup_html(
    success: bool, message: str, access_token: str = "", user_id: str = "",
    user_name: str = "", broker_doc_id: str = "",
) -> str:
    import json as _json
    payload_js = _json.dumps({
        "type": "KITE_LOGIN", "success": success, "message": message,
        "access_token": access_token, "user_id": user_id, "user_name": user_name,
        "broker_doc_id": broker_doc_id,
    })
    status_color = "#22c55e" if success else "#ef4444"
    status_icon = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kite Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">You can close this window after checking the URL.</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
  </script>
</body>
</html>"""


def _save_market_kite_session(session: dict) -> None:
    api_key = session.get("api_key") or str(getattr(get_kite_instance(), "api_key", "") or "").strip()
    access_token = session.get("access_token")
    login_time = to_utc_iso(datetime.now(timezone.utc))
    update_fields = {
        "broker": "kite", "api_key": api_key, "access_token": access_token,
        "login_time": login_time, "user_id": session.get("user_id"),
        "user_name": session.get("user_name"), "app_user_id": _resolve_app_user_id(),
    }
    mongo = get_mongo()
    # Match by broker, not by whichever doc currently has enabled:True — a
    # Kite login should never be able to overwrite Dhan's doc.
    existing = mongo.raw["kite_market_config"].find_one({"broker": "kite"}, {"api_secret": 1}) or {}
    api_secret = str(existing.get("api_secret") or "").strip()
    mongo.raw["kite_market_config"].update_one({"broker": "kite"}, {"$set": update_fields}, upsert=True)
    sync_kite_access_token_by_credentials(
        mongo.raw, api_key, api_secret, access_token, login_time,
        skip_collection="kite_market_config",
    )


@router.get("/broker/kite/login-url")
async def kite_login_url():
    return {"login_url": get_login_url()}


@router.get("/live/kite-callback", response_class=HTMLResponse)
async def kite_live_callback(request: Request):
    """Kite console redirect URL. Handles
    ?status=success&request_token=xxx&action=login&type=login"""
    status = request.query_params.get("status", "").strip()
    request_token = request.query_params.get("request_token", "").strip()
    state = request.query_params.get("state", "").strip()
    broker_doc_id = _kite_pending.pop(state, "") if state else ""

    if status != "success" or not request_token:
        return HTMLResponse(content=_kite_popup_html(False, f"Login failed or no token received (status={status})"))

    try:
        session = generate_session(request_token)
    except Exception as e:
        return HTMLResponse(content=_kite_popup_html(False, f"Session error: {e}"))

    if broker_doc_id:
        try:
            save_kite_session(get_mongo().raw, broker_doc_id, session)
        except Exception:
            pass
    else:
        try:
            _save_market_kite_session(session)
        except Exception:
            pass

    return HTMLResponse(content=_kite_popup_html(
        True, "Login successful", access_token=session.get("access_token", ""),
        user_id=session.get("user_id", ""), user_name=session.get("user_name", ""),
        broker_doc_id=broker_doc_id,
    ))


@router.get("/broker/kite/login")
async def kite_login(broker_doc_id: str = ""):
    """Hit this URL directly → auto redirect to Zerodha login page. After
    login, Zerodha redirects back → access_token auto generated & saved."""
    session_id = secrets.token_hex(16)
    _kite_pending[session_id] = broker_doc_id

    login_url = get_login_url()
    if not login_url:
        return JSONResponse({"status": "error", "message": "Kite api_key is not configured"}, status_code=400)
    return RedirectResponse(url=f"{login_url}&state={session_id}")


@router.get("/broker/kite/redirect", response_class=HTMLResponse)
async def kite_redirect(request: Request):
    """Zerodha redirects here after login with ?request_token=xxx.
    Auto-generates access_token, saves to MongoDB, closes popup, and sends
    result back to parent window via postMessage."""
    request_token = request.query_params.get("request_token", "").strip()
    error_msg = request.query_params.get("error", "").strip()
    state = request.query_params.get("state", "").strip()
    broker_doc_id = _kite_pending.pop(state, "") or request.query_params.get("broker_doc_id", "").strip()

    if error_msg or not request_token:
        return HTMLResponse(content=_kite_popup_html(False, error_msg or "No request_token received"))

    try:
        session = generate_session(request_token)
    except Exception as e:
        return HTMLResponse(content=_kite_popup_html(False, f"Session error: {e}"))

    if broker_doc_id:
        try:
            save_kite_session(get_mongo().raw, broker_doc_id, session)
        except Exception:
            pass
    else:
        try:
            _save_market_kite_session(session)
        except Exception:
            pass

    return HTMLResponse(content=_kite_popup_html(
        True, "Login successful", access_token=session.get("access_token", ""),
        user_id=session.get("user_id", ""), user_name=session.get("user_name", ""),
        broker_doc_id=broker_doc_id,
    ))


@router.get("/broker/kite/access-token/{broker_doc_id}")
async def kite_get_access_token(broker_doc_id: str):
    token = get_stored_access_token(get_mongo().raw, broker_doc_id)
    if not token:
        raise HTTPException(status_code=404, detail="No access token found")
    return {"access_token": token}
