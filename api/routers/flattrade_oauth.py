"""
routers/flattrade_oauth.py
─────────────────────────────
Route/path-compatible port of the old algo.trade's FlatTrade OAuth login
flow (api.py:9936-10158) — login/redirect/callback only. No "/algo" prefix
— same bare paths as the old system since FlatTrade's own app settings
register /broker/flattrade/redirect (or the dynamic per-broker callback) as
a literal redirect URL.

All session/token logic is unchanged, real code from
shared/features/flattrade_broker.py (get_login_url, generate_session,
save_flattrade_session — reached via the algo.trade/features symlink).

NOT ported: the old system's /broker/flattrade/postback* order-status
handlers (api.py:9693-9933) — those write fills into `broker_orders`/
`algo_trades` and call features.live_order_manager.process_broker_order_update,
which updates the OLD system's live order/trade state. algo-2_0 tracks its
own live order/trade state differently (orders/, runtime/, persistence/ —
in-memory token_router + its own checkpoint collections), and has no
FlatTrade broker adapter wired into that yet (shared/brokers/flattrade/ is
still an empty stub, unused by any call site — confirmed during the Phase 0
audit). Porting the postback handlers now would parse and log real FlatTrade
webhooks correctly but silently no-op past that — dead code pretending to be
wired up. Revisit once algo-2_0 has its own FlatTrade order integration to
actually feed.
"""

from __future__ import annotations

import secrets
from urllib.parse import quote, unquote

from bson import ObjectId
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from features.flattrade_broker import (
    _session_token,
    _session_user_id,
    generate_session,
    get_login_url,
    save_flattrade_session,
)

from shared.db.mongo import get_mongo

router = APIRouter(tags=["flattrade-oauth"])

_flattrade_pending: dict[str, str] = {}


def _broker_popup_html(
    broker: str, success: bool, message: str, access_token: str = "",
    user_id: str = "", user_name: str = "", broker_doc_id: str = "",
) -> str:
    import json as _json
    payload_js = _json.dumps({
        "type": f"{broker.upper()}_LOGIN", "success": success, "message": message,
        "access_token": access_token, "user_id": user_id, "user_name": user_name,
        "broker_doc_id": broker_doc_id,
    })
    status_color = "#22c55e" if success else "#ef4444"
    status_icon = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{broker} Login</title>
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
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
    setTimeout(() => window.close(), 1500);
  </script>
</body>
</html>"""


def _read_broker_credentials(broker_doc_id: str) -> tuple[str, str]:
    if not broker_doc_id:
        return "", ""
    try:
        doc = get_mongo().raw["broker_configuration"].find_one(
            {"_id": ObjectId(broker_doc_id)}, {"api_key": 1, "api_secret": 1}
        )
        return str((doc or {}).get("api_key") or "").strip(), str((doc or {}).get("api_secret") or "").strip()
    except Exception:
        return "", ""


@router.get("/broker/flattrade/login")
async def flattrade_login(broker_doc_id: str = ""):
    """Redirect to FlatTrade login page."""
    ft_api_key, _ = _read_broker_credentials(broker_doc_id)
    session_id = secrets.token_hex(16)
    state = f"{session_id}:{quote(broker_doc_id, safe='')}" if broker_doc_id else session_id
    _flattrade_pending[state] = broker_doc_id
    login_url = get_login_url(state=state, api_key=ft_api_key)
    response = RedirectResponse(url=login_url)
    if broker_doc_id:
        response.set_cookie(key="flattrade_broker_doc_id", value=broker_doc_id, max_age=600, httponly=True, samesite="lax")
    return response


@router.get("/broker/flattrade/redirect", response_class=HTMLResponse)
async def flattrade_redirect(request: Request):
    """FlatTrade redirects here after login with ?code=<request_code>&state=<session_id>.
    Exchanges the code for a jKey session token and saves it to broker_configuration."""
    request_code = request.query_params.get("code", "").strip()
    error_msg = request.query_params.get("error", "").strip()
    state = request.query_params.get("state", "").strip()
    ft_client_id = request.query_params.get("client", "").strip()
    broker_doc_id = _flattrade_pending.pop(state, "") or request.query_params.get("broker_doc_id", "").strip()
    if not broker_doc_id and ":" in state:
        broker_doc_id = unquote(state.rsplit(":", 1)[-1]).strip()
    if not broker_doc_id:
        broker_doc_id = request.cookies.get("flattrade_broker_doc_id", "").strip()
    if not broker_doc_id and ft_client_id:
        try:
            doc = get_mongo().raw["broker_configuration"].find_one({"user_id": ft_client_id}, {"_id": 1})
            if doc:
                broker_doc_id = str(doc["_id"])
        except Exception:
            pass

    if error_msg or not request_code:
        response = HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade", success=False, message=error_msg or "No request code received",
        ))
        response.delete_cookie("flattrade_broker_doc_id")
        return response

    ft_api_key, ft_api_secret = _read_broker_credentials(broker_doc_id)
    try:
        session = generate_session(request_code, api_key=ft_api_key, api_secret=ft_api_secret)
    except Exception as exc:
        response = HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade", success=False, message=f"Session error: {exc}",
        ))
        response.delete_cookie("flattrade_broker_doc_id")
        return response

    if broker_doc_id:
        try:
            save_flattrade_session(get_mongo().raw, broker_doc_id, session)
        except Exception as exc:
            response = HTMLResponse(content=_broker_popup_html(
                broker="FlatTrade", success=False, message=f"Token generated but DB save failed: {exc}",
            ))
            response.delete_cookie("flattrade_broker_doc_id")
            return response

    response = HTMLResponse(content=_broker_popup_html(
        broker="FlatTrade", success=True, message="Login successful",
        access_token=_session_token(session), user_id=_session_user_id(session),
        user_name=_session_user_id(session), broker_doc_id=broker_doc_id,
    ))
    response.delete_cookie("flattrade_broker_doc_id")
    return response


@router.get("/broker/flattrade/callback/{broker_doc_id}", response_class=HTMLResponse)
async def flattrade_callback(request: Request, broker_doc_id: str):
    """Dynamic per-broker redirect URL: /broker/flattrade/callback/{broker_doc_id}
    — broker_doc_id is embedded in the path, no state/cookie needed."""
    request_code = request.query_params.get("code", "").strip()
    error_msg = request.query_params.get("error", "").strip()

    if error_msg or not request_code:
        return HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade", success=False, message=error_msg or "No request code received",
        ))

    ft_api_key, ft_api_secret = _read_broker_credentials(broker_doc_id)
    try:
        session = generate_session(request_code, api_key=ft_api_key, api_secret=ft_api_secret)
    except Exception as exc:
        return HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade", success=False, message=f"Session error: {exc}",
        ))

    try:
        save_flattrade_session(get_mongo().raw, broker_doc_id, session)
    except Exception as exc:
        return HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade", success=False, message=f"Token generated but DB save failed: {exc}",
        ))

    return HTMLResponse(content=_broker_popup_html(
        broker="FlatTrade", success=True, message="Login successful",
        access_token=_session_token(session), user_id=_session_user_id(session),
        user_name=_session_user_id(session), broker_doc_id=broker_doc_id,
    ))
