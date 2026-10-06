"""
routers/auth.py
────────────────
Route-compatible with the old algo.trade's /algo/auth/register|login|me
(api.py:2052-2080) — same paths, same request/response JSON shape — so
algo-admin/algo-admin-v2/algo-admin-old-menu need zero changes for this
endpoint group at cutover. Internals (shared/auth/service.py) are fully
rewritten; only the external contract is preserved.

NOT ported here: the `algo_shard` routing cookie the old handlers set
(_set_shard_routing_cookie) — that's tied to the old system's nginx
shard-routing setup, which algo-2_0 doesn't have yet. Revisit if/when
algo-2_0 needs multi-instance routing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional

from shared.auth.dependency import get_current_user
from shared.auth.service import (
    authenticate_user,
    create_access_token,
    public_user,
    register_user,
)
from shared.db.mongo import get_mongo

# Same collection old algo.trade's shared/features/payment.py (payment_router,
# mounted at /algo) reads for GET /auth/subscriptions.
SUBSCRIPTIONS_COLLECTION = "user_subscriptions"

router = APIRouter(prefix="/algo/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    mobile: str
    name: str
    email: str
    password: str
    referral_code: Optional[str] = None


class LoginRequest(BaseModel):
    mobile: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict[str, Any]


@router.post("/register", response_model=TokenResponse)
def register(payload: RegisterRequest) -> TokenResponse:
    mongo = get_mongo()
    doc = register_user(mongo, payload.model_dump())
    token = create_access_token(str(doc["_id"]), doc["mobile"])
    return TokenResponse(access_token=token, user=public_user(doc))


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest) -> TokenResponse:
    mongo = get_mongo()
    doc = authenticate_user(mongo, payload.mobile, payload.password)
    token = create_access_token(str(doc["_id"]), doc["mobile"])
    return TokenResponse(access_token=token, user=public_user(doc))


@router.get("/me")
def me(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
    return public_user(user)


@router.get("/subscriptions")
def subscriptions(user: dict[str, Any] = Depends(get_current_user)) -> list[dict[str, Any]]:
    """Ported from old algo.trade's shared/features/payment.py get_subscriptions
    (mounted there as GET /algo/auth/subscriptions) — same collection, same
    lazy expiry-on-read behavior, same response shape."""
    db = get_mongo().raw
    user_id = str(user["_id"])
    now = datetime.now(timezone.utc).isoformat()

    docs = list(db[SUBSCRIPTIONS_COLLECTION].find({"user_id": user_id}))
    result = []
    for d in docs:
        status = d.get("status", "active")
        expires_at = d.get("expires_at")
        if status == "active" and expires_at and expires_at < now:
            status = "expired"
            db[SUBSCRIPTIONS_COLLECTION].update_one({"_id": d["_id"]}, {"$set": {"status": "expired"}})
        result.append({
            "id": str(d["_id"]),
            "feature": d.get("feature"),
            "plan_id": d.get("plan_id"),
            "plan_name": d.get("plan_name"),
            "status": status,
            "expires_at": expires_at,
            "created_at": d.get("created_at"),
        })
    return result
