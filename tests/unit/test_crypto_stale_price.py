"""Crypto LTP: no SL/TP/MTM on an old price, and the page gets the live price
on load instead of showing the entry price."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from api import live_broadcast
from persistence.serializers import _leg_to_wire
from runtime.leg_runtime import LegRuntime
from services import crypto_stale_price
from shared.market.ltp_cache import LtpCache, TickUpdate

TOK = "C-BTC-82800-271026"


def _leg(token=TOK, status="ACTIVE", current=0.54):
    leg = LegRuntime(leg_id=f"L-{token}", strategy_id="s1", user_id="u", token=token, is_sell=True,
                     option_type="CE", qty=1, entry_price=10.67, current_price=current)
    leg.status = status
    return leg


def _router(*legs):
    return SimpleNamespace(legs={l.leg_id: l for l in legs},
                           token_to_legs={l.token: {l.leg_id} for l in legs if l.status == "ACTIVE"})


def _tick(cache, token, price, age_seconds):
    ts = (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cache.apply(TickUpdate(changed_ltp={token: price}, now_ts=ts))


def test_only_open_option_legs_without_a_recent_tick_are_stale():
    cache = LtpCache()
    fresh, old, never = _leg("C-BTC-1-271026"), _leg("P-BTC-2-271026"), _leg("C-ETH-3-271026")
    _tick(cache, fresh.token, 1.0, 2)
    _tick(cache, old.token, 1.0, 30)
    router = _router(fresh, old, never, _leg("P-BTC-4-271026", status="EXITED"), _leg("BTCUSD"), _leg("12345"))
    assert crypto_stale_price.stale_option_tokens(router, cache) == ["C-ETH-3-271026", "P-BTC-2-271026"]


def test_stale_leg_gets_the_rest_mark_through_the_tick_path():
    cache = LtpCache()
    _tick(cache, TOK, 0.60, 30)
    dispatched = []

    async def dispatch(update):
        cache.apply(update)
        dispatched.append(update)

    async def fetch(symbols):
        assert symbols == [TOK]
        return {TOK: 0.547}

    marks = asyncio.run(crypto_stale_price.refresh_once(_router(_leg()), cache, dispatch, fetch))
    assert marks == {TOK: 0.547}
    assert dispatched[0].changed_ltp == {TOK: 0.547}
    assert cache.get_ltp(TOK) == 0.547 and not cache.is_stale(TOK, 10)


def test_no_rest_call_while_ticks_are_flowing():
    cache = LtpCache()
    _tick(cache, TOK, 0.55, 1)

    async def fetch(symbols):
        raise AssertionError("must not fetch")

    async def dispatch(update):
        raise AssertionError("must not dispatch")

    assert asyncio.run(crypto_stale_price.refresh_once(_router(_leg()), cache, dispatch, fetch)) == {}


def test_rest_failure_never_injects_an_old_price():
    cache = LtpCache()
    _tick(cache, TOK, 0.60, 30)
    dispatched = []

    async def dispatch(update):
        dispatched.append(update)

    async def fetch(symbols):
        return {}

    asyncio.run(crypto_stale_price.refresh_once(_router(_leg()), cache, dispatch, fetch))
    assert dispatched == []


def test_open_leg_record_carries_live_price():
    doc = _leg_to_wire(_leg(current=0.553), "BTCUSD")
    assert doc["ltp"] == doc["last_saw_price"] == 0.553
    closed = _leg_to_wire(_leg(status="EXITED", current=0.4), "BTCUSD")
    assert "ltp" not in closed and closed["exit_trade"]["price"] == 0.4


def test_update_socket_snapshot_uses_live_cache_prices_only(monkeypatch):
    cache = LtpCache()
    _tick(cache, TOK, 0.547, 1)
    monkeypatch.setattr(live_broadcast, "get_ltp_cache", lambda: cache)
    monkeypatch.setattr(live_broadcast, "_legacy_open_tokens_cache", set())
    items = live_broadcast.open_token_ltp_items(_router(_leg(), _leg("P-BTC-76800-271026", current=3.79)))
    assert items == [{"token": TOK, "ltp": 0.547}]  # no tick yet -> no engine-side fallback
