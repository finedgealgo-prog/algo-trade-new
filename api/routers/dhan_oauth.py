"""
routers/dhan_oauth.py
────────────────────────
Route/path-compatible port of the old algo.trade's Dhan OAuth login flow and
generic kite_market_config (market-data feed credentials) CRUD
(api.py:8137-8753). No "/algo" prefix — same bare paths as the old system
(/broker/dhan/..., /broker/market-config...) since Dhan's own app settings
register /broker/dhan/callback as a literal redirect URL, and the daily TOTP
cron hits /broker/dhan/totp-login by exact path.

All the actual OAuth/TOTP/token logic is unchanged, real code from
shared/features/dhan_broker_ws.py and shared/features/dhan_totp_login.py
(reached via the algo.trade/features symlink) — this file only adds the
FastAPI routes and the same `kite_market_config` Mongo reads/writes the old
ones did, via shared/db/mongo.py (now backed by the same MongoData the old
system uses).
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

import requests
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from features.broker_accounts import DEFAULT_APP_USER_ID
from features.broker_gateway import reset_broker_cache
from features.dhan_broker_ws import decode_access_token_expiry, set_common_credentials
from features.dhan_totp_login import refresh_dhan_token_via_totp
from features.time_utils import to_utc_iso

from shared.db.mongo import get_mongo

router = APIRouter(tags=["dhan-oauth"])

_dhan_pending: dict[str, str] = {}


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized = str(value or "").strip()
    return normalized or DEFAULT_APP_USER_ID


def _dhan_popup_result_html(success: bool, message: str) -> str:
    import json as _json
    payload_js = _json.dumps({"type": "DHAN_LOGIN", "success": success, "message": message})
    icon = "✓" if success else "✗"
    color = "#22c55e" if success else "#ef4444"
    title = "Login Successful" if success else "Login Failed"
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Dhan Login</title>
<style>
  body{{font-family:-apple-system,sans-serif;display:flex;align-items:center;
       justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#f1f5f9;}}
  .card{{text-align:center;padding:2rem;background:#1e293b;border-radius:12px;border:1px solid #334155;}}
  .icon{{font-size:3rem;color:{color};}}
  p{{color:#94a3b8;}}
</style></head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h2>{title}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) window.opener.postMessage(payload, "*");
    setTimeout(() => window.close(), 1500);
  </script>
</body></html>"""


@router.post("/broker/dhan/config")
async def dhan_save_config(request: Request):
    """Save Dhan client_id + access_token to kite_market_config (broker=dhan, enabled=true)."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"status": "error", "message": "Invalid JSON body"}, status_code=400)
    client_id = str(body.get("client_id") or "").strip()
    access_token = str(body.get("access_token") or "").strip()
    if not client_id or not access_token:
        return JSONResponse({"status": "error", "message": "client_id and access_token are required"}, status_code=400)

    mongo = get_mongo()
    try:
        mongo.raw["kite_market_config"].update_one(
            {"broker": "dhan"},
            {"$set": {
                "broker": "dhan", "enabled": True, "user_id": client_id,
                "access_token": access_token, "app_user_id": _resolve_app_user_id(),
            }},
            upsert=True,
        )
        mongo.raw["kite_market_config"].update_many(
            {"broker": {"$ne": "dhan"}, "enabled": True}, {"$set": {"enabled": False}},
        )
        try:
            set_common_credentials(client_id, access_token)
            reset_broker_cache()
        except Exception:
            pass
        return {"status": "ok", "message": "Dhan credentials saved"}
    except Exception as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.get("/broker/dhan/login", response_class=HTMLResponse)
async def dhan_login():
    """Dhan login popup. Shows a one-click Login button if credentials are
    already saved, otherwise the full credentials form."""
    cfg = get_mongo().raw["kite_market_config"].find_one({"broker": "dhan"}) or {}

    has_creds = bool(cfg.get("api_key") and cfg.get("api_secret") and (cfg.get("user_id") or cfg.get("dhan_client_id")))
    user_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    api_key = str(cfg.get("api_key") or "").strip()
    api_secret = str(cfg.get("api_secret") or "").strip()

    if has_creds:
        return HTMLResponse(content=f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"><title>Dhan HQ Login</title>
  <style>
    body{{font-family:-apple-system,sans-serif;display:flex;align-items:center;
         justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#f1f5f9;}}
    .card{{padding:2.5rem;background:#1e293b;border-radius:14px;border:1px solid #334155;
           width:340px;text-align:center;}}
    h2{{margin:0 0 0.5rem;color:#f97316;}}
    .info{{color:#64748b;font-size:0.82rem;margin-bottom:1.8rem;}}
    .row{{display:flex;justify-content:space-between;font-size:0.82rem;
          color:#94a3b8;margin-bottom:0.4rem;}}
    .val{{color:#f1f5f9;font-weight:500;}}
    .btn{{width:100%;padding:0.8rem;background:linear-gradient(135deg,#f97316,#ea580c);
          border:none;border-radius:10px;color:#fff;font-size:1.05rem;font-weight:700;
          cursor:pointer;margin-top:1.5rem;}}
    .btn:hover{{background:linear-gradient(135deg,#fb923c,#f97316);}}
    .link{{display:block;margin-top:1rem;font-size:0.78rem;color:#475569;cursor:pointer;}}
    #msg{{margin-top:1rem;font-size:0.85rem;color:#f97316;min-height:1.2rem;}}
  </style>
</head>
<body>
  <div class="card">
    <h2>Dhan HQ Login</h2>
    <p class="info">Credentials already saved. Click to connect.</p>
    <div class="row"><span>Client ID</span><span class="val">{user_id}</span></div>
    <div class="row"><span>API Key</span><span class="val">{api_key}</span></div>
    <div class="row"><span>API Secret</span><span class="val">{'•' * min(len(api_secret), 8)}...</span></div>
    <button class="btn" onclick="doLogin()">Login with Dhan →</button>
    <span class="link" onclick="document.getElementById('fullform').style.display='block';this.style.display='none'">
      Change credentials
    </span>
    <div id="msg"></div>

    <div id="fullform" style="display:none;margin-top:1.5rem;text-align:left">
      <label style="font-size:0.82rem;color:#94a3b8">Client ID (user_id)</label>
      <input id="cid" style="width:100%;padding:0.5rem;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;margin-bottom:0.7rem;box-sizing:border-box" value="{user_id}">
      <label style="font-size:0.82rem;color:#94a3b8">API Key</label>
      <input id="akey" style="width:100%;padding:0.5rem;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;margin-bottom:0.7rem;box-sizing:border-box" value="{api_key}">
      <label style="font-size:0.82rem;color:#94a3b8">API Secret</label>
      <input id="asecret" type="password" style="width:100%;padding:0.5rem;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;margin-bottom:0.7rem;box-sizing:border-box" placeholder="Enter API Secret">
      <button class="btn" onclick="doLoginForm()">Save & Login →</button>
    </div>
  </div>
  <script>
    async function doLogin() {{
      document.getElementById("msg").textContent = "Generating consent...";
      const res = await fetch("/broker/dhan/generate-consent", {{
        method:"POST", headers:{{"Content-Type":"application/json"}},
        body: JSON.stringify({{use_saved: true}}),
      }});
      const d = await res.json();
      if (d.ok && d.login_url) {{ window.location.href = d.login_url; }}
      else {{
        document.getElementById("msg").textContent = d.message || "Error";
        document.getElementById("msg").style.color = "#ef4444";
      }}
    }}
    async function doLoginForm() {{
      document.getElementById("msg").textContent = "Saving & generating consent...";
      const res = await fetch("/broker/dhan/generate-consent", {{
        method:"POST", headers:{{"Content-Type":"application/json"}},
        body: JSON.stringify({{
          user_id:    document.getElementById("cid").value.trim(),
          api_key:    document.getElementById("akey").value.trim(),
          api_secret: document.getElementById("asecret").value.trim(),
        }}),
      }});
      const d = await res.json();
      if (d.ok && d.login_url) {{ window.location.href = d.login_url; }}
      else {{
        document.getElementById("msg").textContent = d.message || "Error";
        document.getElementById("msg").style.color = "#ef4444";
      }}
    }}
    if (window.opener) setTimeout(doLogin, 300);
  </script>
</body>
</html>""")

    return HTMLResponse(content=f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"><title>Dhan HQ Setup</title>
  <style>
    body{{font-family:-apple-system,sans-serif;display:flex;align-items:center;
         justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#f1f5f9;}}
    .card{{padding:2rem 2.5rem;background:#1e293b;border-radius:14px;
           border:1px solid #334155;width:360px;}}
    h2{{margin:0 0 0.2rem;color:#f97316;}}
    .sub{{color:#64748b;font-size:0.8rem;margin-bottom:1.2rem;}}
    label{{display:block;font-size:0.82rem;color:#94a3b8;margin-bottom:0.25rem;margin-top:0.7rem;}}
    input{{width:100%;padding:0.5rem 0.7rem;background:#0f172a;border:1px solid #334155;
           border-radius:6px;color:#f1f5f9;font-size:0.9rem;box-sizing:border-box;}}
    .btn{{width:100%;padding:0.7rem;background:linear-gradient(135deg,#f97316,#ea580c);
          border:none;border-radius:8px;color:#fff;font-size:1rem;font-weight:700;
          cursor:pointer;margin-top:1.2rem;}}
    #msg{{margin-top:0.8rem;font-size:0.85rem;color:#f97316;min-height:1.2rem;}}
  </style>
</head>
<body>
  <div class="card">
    <h2>Dhan HQ Setup</h2>
    <p class="sub">web.dhan.co → My Profile → Access DhanHQ APIs → API Key tab</p>
    <label>Client ID (user_id)</label>
    <input id="cid" placeholder="e.g. 1103877976">
    <label>API Key</label>
    <input id="akey" placeholder="e.g. a8065854">
    <label>API Secret</label>
    <input id="asecret" type="password" placeholder="Paste API Secret">
    <button class="btn" onclick="doSetup()">Save & Login →</button>
    <div id="msg"></div>
  </div>
  <script>
    async function doSetup() {{
      document.getElementById("msg").textContent = "Saving credentials...";
      const res = await fetch("/broker/dhan/generate-consent", {{
        method:"POST", headers:{{"Content-Type":"application/json"}},
        body: JSON.stringify({{
          user_id:    document.getElementById("cid").value.trim(),
          api_key:    document.getElementById("akey").value.trim(),
          api_secret: document.getElementById("asecret").value.trim(),
        }}),
      }});
      const d = await res.json();
      if (d.ok && d.login_url) {{ window.location.href = d.login_url; }}
      else {{
        document.getElementById("msg").textContent = d.message || "Error";
        document.getElementById("msg").style.color = "#ef4444";
      }}
    }}
  </script>
</body>
</html>""")


@router.post("/broker/dhan/generate-consent")
async def dhan_generate_consent(request: Request):
    """Step 1 of Dhan OAuth. use_saved=true reads credentials from DB;
    otherwise saves new credentials first, then generates consent."""
    try:
        body = await request.json()
    except Exception:
        return {"ok": False, "message": "Invalid JSON"}

    use_saved = body.get("use_saved", False)
    mongo = get_mongo()
    cfg = mongo.raw["kite_market_config"].find_one({"broker": "dhan"}) or {}

    if use_saved:
        user_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
        api_key = str(cfg.get("api_key") or "").strip()
        api_secret = str(cfg.get("api_secret") or "").strip()
        if not user_id or not api_key or not api_secret:
            return {"ok": False, "message": "Saved credentials incomplete — please enter them again"}
    else:
        user_id = str(body.get("user_id") or body.get("dhan_client_id") or "").strip()
        api_key = str(body.get("api_key") or "").strip()
        api_secret = str(body.get("api_secret") or "").strip()
        if not user_id or not api_key or not api_secret:
            return {"ok": False, "message": "user_id, api_key and api_secret are required"}
        mongo.raw["kite_market_config"].update_one(
            {"broker": "dhan"},
            {"$set": {
                "broker": "dhan", "user_id": user_id, "api_key": api_key, "api_secret": api_secret,
                "access_token": "", "enabled": True, "app_user_id": _resolve_app_user_id(),
            }},
            upsert=True,
        )

    session_id = secrets.token_hex(16)
    _dhan_pending[session_id] = user_id

    try:
        resp = requests.post(
            f"https://auth.dhan.co/app/generate-consent?client_id={user_id}",
            headers={"app_id": api_key, "app_secret": api_secret},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        consent_app_id = data.get("consentAppId") or ""
        if not consent_app_id:
            return {"ok": False, "message": f"No consentAppId in response: {data}"}
    except Exception as exc:
        return {"ok": False, "message": f"Dhan generate-consent error: {exc}"}

    login_url = f"https://auth.dhan.co/login/consentApp-login?consentAppId={consent_app_id}&state={session_id}"
    return {"ok": True, "login_url": login_url, "consent_app_id": consent_app_id}


@router.get("/broker/dhan/callback", response_class=HTMLResponse)
async def dhan_callback(request: Request):
    """Step 3 of Dhan OAuth — Dhan redirects here with ?tokenId=xxx after
    user login. Registered as Redirect URL in Dhan app settings."""
    token_id = request.query_params.get("tokenId", "").strip()
    state = request.query_params.get("state", "").strip()

    if not token_id:
        return HTMLResponse(content=_dhan_popup_result_html(
            False, f"tokenId missing in callback — params: {dict(request.query_params)}"
        ))

    mongo = get_mongo()
    cfg = mongo.raw["kite_market_config"].find_one({"broker": "dhan"}) or {}
    api_key = str(cfg.get("api_key") or "").strip()
    api_secret = str(cfg.get("api_secret") or "").strip()
    dhan_client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or _dhan_pending.pop(state, "") or "").strip()

    if not api_key or not api_secret:
        return HTMLResponse(content=_dhan_popup_result_html(
            False, "API Key/Secret not found — complete Step 1 first at /broker/dhan/login"
        ))

    try:
        resp = requests.get(
            f"https://auth.dhan.co/app/consumeApp-consent?tokenId={token_id}",
            headers={"app_id": api_key, "app_secret": api_secret},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        access_token = str(data.get("accessToken") or "").strip()
        if not access_token:
            return HTMLResponse(content=_dhan_popup_result_html(False, f"No accessToken in response: {data}"))
        expiry_time = str(data.get("expiryTime") or "").strip() or decode_access_token_expiry(access_token)
    except Exception as exc:
        return HTMLResponse(content=_dhan_popup_result_html(False, f"consumeApp-consent error: {exc}"))

    mongo.raw["kite_market_config"].update_one(
        {"broker": "dhan"},
        {"$set": {
            "access_token": access_token, "login_time": to_utc_iso(datetime.now(timezone.utc)),
            "expiry_time": expiry_time, "app_user_id": _resolve_app_user_id(),
        }},
    )
    try:
        set_common_credentials(dhan_client_id or str(cfg.get("user_id") or ""), access_token)
        reset_broker_cache()
    except Exception:
        pass
    return HTMLResponse(content=_dhan_popup_result_html(
        True, f"Login successful! Token valid until {expiry_time or 'check Dhan portal'}."
    ))


@router.get("/broker/dhan/renew-token")
async def dhan_renew_token():
    """Renew Dhan access token for another 24 hours via POST /v2/RenewToken."""
    mongo = get_mongo()
    cfg = mongo.raw["kite_market_config"].find_one({"broker": "dhan"}) or {}
    access_token = str(cfg.get("access_token") or "").strip()
    dhan_client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()

    if not access_token or not dhan_client_id:
        return {"ok": False, "message": "No valid access_token or dhan_client_id found"}

    try:
        resp = requests.get(
            "https://api.dhan.co/v2/RenewToken",
            headers={"access-token": access_token, "dhanClientId": dhan_client_id},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        new_token = str(data.get("accessToken") or "").strip()
        if not new_token:
            return {"ok": False, "message": f"No new token in response: {data}"}
        expiry_time = str(data.get("expiryTime") or "").strip() or decode_access_token_expiry(new_token)
    except Exception as exc:
        return {"ok": False, "message": f"RenewToken error: {exc}"}

    mongo.raw["kite_market_config"].update_one(
        {"broker": "dhan"},
        {"$set": {
            "access_token": new_token, "login_time": to_utc_iso(datetime.now(timezone.utc)),
            "expiry_time": expiry_time, "app_user_id": _resolve_app_user_id(),
        }},
    )
    try:
        set_common_credentials(dhan_client_id, new_token)
    except Exception:
        pass
    return {"ok": True, "message": "Token renewed", "expiry_time": expiry_time}


@router.get("/broker/dhan/totp-login")
async def dhan_totp_login():
    """Daily auto-login: refreshes the Dhan access_token via PIN+TOTP — no
    browser/consent step. Hit once a day by cron before market open."""
    return refresh_dhan_token_via_totp()


@router.get("/broker/dhan/status")
async def dhan_feed_status():
    """Current Dhan market config status."""
    try:
        cfg = get_mongo().raw["kite_market_config"].find_one(
            {"broker": "dhan"},
            {"user_id": 1, "api_key": 1, "login_time": 1, "enabled": 1, "access_token": 1, "expiry_time": 1},
        ) or {}
        return {
            "ok": True, "enabled": bool(cfg.get("enabled")), "user_id": str(cfg.get("user_id") or ""),
            "api_key": str(cfg.get("api_key") or ""), "has_token": bool(cfg.get("access_token")),
            "login_time": str(cfg.get("login_time") or ""), "expiry_time": str(cfg.get("expiry_time") or ""),
        }
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


# ── kite_market_config CRUD (market-data feed credentials, any broker) ─────

def _sync_broker_configuration_mirror(
    *, match_name: str, broker_icon: str, broker_key: str,
    api_key: str, api_secret: str, broker_user_id: str, app_user_id: str, base_url: str,
) -> None:
    """Keep the broker_configuration doc live order placement reads from in
    sync with a broker's own credentials store. Matches by stable
    name/broker_type identity, never by api_key/secret. Never sets
    access_token/login_time — only a real login should do that."""
    if not api_key and not api_secret:
        return
    col = get_mongo().raw["broker_configuration"]
    match = {"name": match_name, "broker_type": "live"}
    set_fields: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if api_key:
        set_fields["api_key"] = api_key
    if api_secret:
        set_fields["api_secret"] = api_secret
    if broker_user_id:
        set_fields["broker_user_id"] = broker_user_id
    set_on_insert = {
        "broker_icon": broker_icon, "name": match_name, "broker_type": "live",
        "redirect_url": f"{base_url}/broker/{broker_key}/callback",
        "user_id": app_user_id, "created_at": set_fields["updated_at"],
    }
    col.update_one(match, {"$set": set_fields, "$setOnInsert": set_on_insert}, upsert=True)


@router.get("/broker/market-configs")
async def list_market_configs(broker: str = "", user_id: str = ""):
    """List kite_market_config docs. totp/pin reported as booleans only,
    never echoed back in plaintext."""
    mongo = get_mongo()
    query: dict = {}
    normalized_broker = str(broker or "").strip().lower()
    if normalized_broker:
        query["broker"] = normalized_broker
    if str(user_id or "").strip():
        query["app_user_id"] = _resolve_app_user_id(user_id)

    try:
        records = []
        for doc in mongo.raw["kite_market_config"].find(query):
            broker_name = str(doc.get("broker") or "").strip()
            access_token = str(doc.get("access_token") or "").strip()
            is_logged_in = False
            if access_token:
                if broker_name == "dhan":
                    from features.dhan_broker_ws import validate_access_token as _validate_dhan
                    is_logged_in = _validate_dhan(access_token)
                elif broker_name == "kite":
                    try:
                        from features.kite_broker_ws import validate_access_token as _validate_kite
                        is_logged_in = _validate_kite(access_token)
                    except Exception:
                        is_logged_in = True
                else:
                    is_logged_in = True
            records.append({
                "_id": str(doc.get("_id") or ""), "broker": broker_name,
                "user_id": str(doc.get("user_id") or "").strip(),
                "api_key": str(doc.get("api_key") or "").strip(),
                "api_secret": str(doc.get("api_secret") or "").strip(),
                "has_totp": bool(str(doc.get("totp") or "").strip()),
                "has_pin": bool(str(doc.get("pin") or "").strip()),
                "enabled": bool(doc.get("enabled") or False),
                "login_time": str(doc.get("login_time") or "").strip(),
                "expiry_time": str(doc.get("expiry_time") or "").strip(),
                "app_user_id": str(doc.get("app_user_id") or "").strip(),
                "is_logged_in": is_logged_in,
            })
        return {"success": True, "count": len(records), "records": records}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load market configurations: {exc}") from exc


@router.post("/broker/market-config/save")
async def save_market_config(payload: dict):
    """Create or update a kite_market_config doc. If _id is given → update
    that doc; otherwise upsert by broker name."""
    doc_id = str(payload.get("_id") or "").strip()
    broker = str(payload.get("broker") or "").strip().lower()
    mongo = get_mongo()
    col = mongo.raw["kite_market_config"]

    query: dict = {"_id": ObjectId(doc_id)} if doc_id else {"broker": broker}
    existing = col.find_one(query) if (doc_id or broker) else None
    if not existing and not broker:
        raise HTTPException(status_code=400, detail="broker is required when creating a new market config")

    allowed = {"broker", "user_id", "api_key", "api_secret", "access_token", "totp", "pin", "app_user_id"}
    fields: dict[str, Any] = {k: str(v or "").strip() for k, v in payload.items() if k in allowed}
    if "enabled" in payload:
        fields["enabled"] = bool(payload.get("enabled"))
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    if not fields.get("app_user_id"):
        if existing:
            fields.pop("app_user_id", None)
        else:
            fields["app_user_id"] = _resolve_app_user_id()

    col.update_one(query, {"$set": fields}, upsert=True)
    saved_doc = col.find_one(query) or {}
    saved_id = str(saved_doc.get("_id") or "")
    saved_broker = fields.get("broker") or (existing or {}).get("broker") or broker

    if fields.get("enabled"):
        col.update_many({"broker": {"$ne": saved_broker}, "enabled": True}, {"$set": {"enabled": False}})

    try:
        reset_broker_cache()
    except Exception:
        pass

    if saved_broker == "dhan":
        _sync_broker_configuration_mirror(
            match_name="Broker.Dhan", broker_icon="dhan.svg", broker_key="dhan",
            api_key=str(saved_doc.get("api_key") or ""), api_secret=str(saved_doc.get("api_secret") or ""),
            broker_user_id=str(saved_doc.get("user_id") or ""), app_user_id=str(saved_doc.get("app_user_id") or ""),
            base_url=str(payload.get("base_url") or "https://finedgealgo.com").rstrip("/"),
        )

    return {"success": True, "action": "updated" if existing else "created", "_id": saved_id}


@router.delete("/broker/market-config/{doc_id}")
async def delete_market_config(doc_id: str):
    result = get_mongo().raw["kite_market_config"].delete_one({"_id": ObjectId(doc_id)})
    return {"success": True, "deleted": result.deleted_count}
