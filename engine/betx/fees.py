"""Kalshi fee model.

Kalshi's published trading-fee formula (Fee Schedule, 2025-2026):

    fee = ceil_to_cent( rate * C * P * (1 - P) )

where C = number of contracts, P = execution price in dollars, and rate is
0.07 for general markets and 0.035 for select index markets (S&P/Nasdaq
range series). Maker (resting) orders are fee-free on general markets; a
small set of series charge a flat per-contract maker fee. Settlement is
fee-free.

The experiment's gate must be fee-aware, so `expected_fee_per_contract` is
the quantity strategies subtract from edge.
"""
from __future__ import annotations

import math

GENERAL_RATE = 0.07
REDUCED_RATE = 0.035

# Series prefixes billed at the reduced rate (index range markets).
REDUCED_RATE_SERIES = (
    "KXINX",     # S&P 500 range
    "KXINXU",
    "KXNASDAQ100",
    "KXNASDAQ100U",
)

# Series with flat maker fees, dollars per contract.
MAKER_FEE_SERIES: dict[str, float] = {}
DEFAULT_MAKER_FEE = 0.0


def _series_of(ticker: str) -> str:
    # KXINX-25JUL25-B6000 -> KXINX ; INXD-23AUG18-T4600 -> INXD
    return ticker.split("-", 1)[0].upper() if ticker else ""


def taker_rate(ticker: str) -> float:
    series = _series_of(ticker)
    return REDUCED_RATE if series in REDUCED_RATE_SERIES else GENERAL_RATE


def ceil_to_cent(x: float) -> float:
    return math.ceil(round(x * 100, 6)) / 100.0


def taker_fee(ticker: str, contracts: int, price_dollars: float) -> float:
    """Total taker fee in dollars for a fill of `contracts` at `price_dollars`."""
    if contracts <= 0:
        return 0.0
    raw = taker_rate(ticker) * contracts * price_dollars * (1.0 - price_dollars)
    return ceil_to_cent(raw)


def maker_fee(ticker: str, contracts: int) -> float:
    per = MAKER_FEE_SERIES.get(_series_of(ticker), DEFAULT_MAKER_FEE)
    return ceil_to_cent(per * contracts)


def expected_fee_per_contract(ticker: str, price_dollars: float, taker: bool = True) -> float:
    """Fee per contract in dollars (probability units) used in the edge gate.

    Not rounded: the gate should use the smooth expectation, rounding only
    matters for realized accounting on small fills.
    """
    if taker:
        return taker_rate(ticker) * price_dollars * (1.0 - price_dollars)
    return MAKER_FEE_SERIES.get(_series_of(ticker), DEFAULT_MAKER_FEE)
