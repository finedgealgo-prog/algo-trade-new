"""
test_selection.py
────────────────────
Phase 5 (Selection engine) unit tests — ATM/OTM/ITM regex offset, closest/
geq/leq premium, and delta selection against known Black-Scholes fixtures.

Run (from algo-2_0/algo.trade/): python3 -m pytest tests/unit/test_selection.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from selection import atm_selector, delta_selector, premium_selector


# ── ATM / OTM / ITM ──────────────────────────────────────────────────────

def test_atm_from_spot_rounds_to_step():
    assert atm_selector.atm_from_spot(24980, 50) == 25000
    assert atm_selector.atm_from_spot(24970, 50) == 24950


def test_ce_otm_moves_above_spot():
    # CE OTM3, NIFTY step=50, spot=25000 -> ATM=25000, +3*50=25150
    strike = atm_selector.resolve_atm_otm_itm_strike(25000, "NIFTY", "CE", "OTM3")
    assert strike == 25150


def test_ce_itm_moves_below_spot():
    strike = atm_selector.resolve_atm_otm_itm_strike(25000, "NIFTY", "CE", "ITM2")
    assert strike == 24900


def test_pe_otm_direction_is_reversed():
    # PE OTM3 -> BELOW spot (opposite of CE)
    strike = atm_selector.resolve_atm_otm_itm_strike(25000, "NIFTY", "PE", "OTM3")
    assert strike == 24850


def test_pe_itm_moves_above_spot():
    strike = atm_selector.resolve_atm_otm_itm_strike(25000, "NIFTY", "PE", "ITM2")
    assert strike == 25100


def test_plain_atm_no_offset():
    assert atm_selector.resolve_atm_otm_itm_strike(25000, "NIFTY", "CE", "ATM") == 25000


# ── Premium selection ────────────────────────────────────────────────────

def test_closest_premium_ties_go_below():
    rows = [{"strike": 25000, "ltp": 90, "token": "1", "symbol": "A"}, {"strike": 25100, "ltp": 110, "token": "2", "symbol": "B"}]
    chosen = premium_selector.select_closest_premium(rows, 100)  # |90-100|=10, |110-100|=10 -> tie -> below
    assert chosen["token"] == "1"


def test_premium_geq_picks_cheapest_at_or_above():
    rows = [{"strike": 25000, "ltp": 80, "token": "1", "symbol": "A"}, {"strike": 25100, "ltp": 105, "token": "2", "symbol": "B"}, {"strike": 25200, "ltp": 130, "token": "3", "symbol": "C"}]
    chosen = premium_selector.select_premium_geq(rows, 100)
    assert chosen["token"] == "2"


def test_premium_leq_picks_richest_at_or_below():
    rows = [{"strike": 25000, "ltp": 80, "token": "1", "symbol": "A"}, {"strike": 25100, "ltp": 105, "token": "2", "symbol": "B"}]
    chosen = premium_selector.select_premium_leq(rows, 100)
    assert chosen["token"] == "1"


# ── Delta selection (Black-Scholes) ─────────────────────────────────────

def test_bs_price_and_iv_roundtrip():
    # A known price should recover ~the same IV that produced it.
    S, K, T, r, q = 25000, 25000, 30 / 365, 0.068, 0.012
    true_sigma = 0.15
    price = delta_selector.bs_price(S, K, T, r, true_sigma, "CE", q)
    recovered_iv = delta_selector.calc_iv(price, S, K, T, r, "CE", q)
    assert abs(recovered_iv - true_sigma) < 0.001


def test_atm_call_delta_near_half():
    # ATM CE delta should be roughly 0.5-0.6 range (with positive carry) for a short-dated option
    greeks = delta_selector.calc_greeks(25000, 25000, 15 / 365, 0.068, 0.15, "CE", 0.012)
    assert 0.4 < greeks["delta"] < 0.65


def test_put_delta_is_negative():
    greeks = delta_selector.calc_greeks(25000, 25000, 15 / 365, 0.068, 0.15, "PE", 0.012)
    assert greeks["delta"] < 0


def test_select_closest_delta_ce():
    rows = [
        {"strike": 25000, "delta": 0.54, "ltp": 100, "token": "1", "symbol": "A"},
        {"strike": 25100, "delta": 0.43, "ltp": 80, "token": "2", "symbol": "B"},
        {"strike": 25200, "delta": 0.34, "ltp": 60, "token": "3", "symbol": "C"},
        {"strike": 25300, "delta": 0.28, "ltp": 45, "token": "4", "symbol": "D"},
    ]
    # target 0.30 -> closest is 0.28 (25300), per master-doc §30 worked example
    chosen = delta_selector.select_closest_delta(rows, 30, "CE")
    assert chosen["token"] == "4"


def test_select_closest_delta_pe_uses_negative_target():
    rows = [
        {"strike": 24800, "delta": -0.28, "ltp": 45, "token": "1", "symbol": "A"},
        {"strike": 24900, "delta": -0.45, "ltp": 70, "token": "2", "symbol": "B"},
    ]
    chosen = delta_selector.select_closest_delta(rows, 30, "PE")
    assert chosen["token"] == "1"


def test_select_delta_range_sell_targets_upper_bound():
    rows = [
        {"strike": 25000, "delta": 0.75, "ltp": 100, "token": "1", "symbol": "A"},
        {"strike": 25100, "delta": 0.55, "ltp": 70, "token": "2", "symbol": "B"},
        {"strike": 25200, "delta": 0.35, "ltp": 40, "token": "3", "symbol": "C"},
    ]
    chosen = delta_selector.select_delta_range(rows, lower_pct=40, upper_pct=70, option_type="CE", is_sell=True)
    assert chosen["token"] == "2"  # closest to 0.70 within [0.40,0.70] is 0.55... actually check nearest to upper


def test_select_delta_range_returns_none_when_empty():
    rows = [{"strike": 25000, "delta": 0.90, "ltp": 100, "token": "1", "symbol": "A"}]
    chosen = delta_selector.select_delta_range(rows, lower_pct=10, upper_pct=20, option_type="CE", is_sell=True)
    assert chosen is None
