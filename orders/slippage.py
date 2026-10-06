"""
slippage.py
─────────────
Activation-level slippage % (PortfolioActivation.tsx's "Slippage" box):
applied to FILL prices only — the entry price a leg is opened at and the
price it's closed at — never to strike selection (option chain / delta /
premium matching keep using the real market price).

Always against the trader: buying fills higher, selling fills lower.
"""

from __future__ import annotations


def slipped_price(price: float, slippage_pct: float, buying: bool) -> float:
    if not slippage_pct or price <= 0:
        return price
    factor = 1 + slippage_pct / 100.0 if buying else 1 - slippage_pct / 100.0
    return round(max(0.0, price * factor), 8)
