"""EntryType.EntryByPremium uses the leg's own StrikeParameter (e.g. 10 from an
imported .algtst), as a number or a numeric string — never a default."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from selection import strike_resolver

ROWS = [{"token": f"C-ETH-{k}-271026", "strike": k, "symbol": f"C-ETH-{k}-271026", "ltp": p}
        for k, p in ((2600, 20.0), (2650, 15.0), (2700, 12.0), (2750, 9.0), (2800, 5.0))]


@pytest.fixture
def chain(monkeypatch):
    monkeypatch.setattr(strike_resolver, "build_chain_rows", lambda *a, **k: [dict(r) for r in ROWS])


@pytest.mark.parametrize("param", [10, "10", 10.0])
def test_closest_premium_uses_strike_parameter(chain, param):
    sel = strike_resolver.resolve_strike(None, None, "ETHUSD", "27-10-2026", "CE", "EntryType.EntryByPremium", param, 2700.0, is_sell=True)
    assert sel is not None and sel.ltp == 9.0  # closest to 10 — not the 15 row


def test_imported_algtst_leg_resolves_by_its_own_premium(chain):
    path = Path("/home/ashok-innoppl/Downloads/eth-cl-pre-mon-23-38-pm-1-recost.algtst")
    if not path.exists():
        pytest.skip("sample file not on this machine")
    leg = json.loads(path.read_text())["strategy"]["ListOfLegConfigs"][0]
    assert leg["EntryType"] == "EntryType.EntryByPremium" and leg["StrikeParameter"] == 10
    sel = strike_resolver.resolve_strike(None, None, "ETHUSD", "27-10-2026", "CE", leg["EntryType"], leg["StrikeParameter"], 2700.0, is_sell=True)
    assert sel.ltp == 9.0
