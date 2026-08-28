"""Derived order-book microstructure, shared by both venue collectors.

Everything here is computable from the stored ladders, so none of it is
strictly necessary — it is stored because recomputing it across hundreds of
millions of ladder rows later is far more expensive than 40 bytes now.

A ladder is [[price, size], ...] sorted best-price-first: highest bid, lowest
ask. Sizes are fractional (Polymarket settles in USDC; Kalshi's `_fp` shapes
are fractional too), so everything returns floats.
"""
from __future__ import annotations

_EPS = 1e-12

Ladder = list[list[float]]


def depth(levels: Ladder | None, top: int | None = None) -> float:
    """Total resting size, optionally over the best `top` levels only."""
    if not levels:
        return 0.0
    use = levels[:top] if top else levels
    return float(sum(s for _, s in use))


def imbalance(bid_depth: float, ask_depth: float) -> float | None:
    """Order-book imbalance in [-1, +1]: +1 all bid, -1 all ask, 0 balanced.

    (bid - ask) / (bid + ask). None when both sides are empty, which is a
    real state (a market with no book) and must not be conflated with 0.0,
    which means a genuinely balanced book.
    """
    total = bid_depth + ask_depth
    if total <= _EPS:
        return None
    return round((bid_depth - ask_depth) / total, 6)


def obi(bids: Ladder | None, asks: Ladder | None, top: int | None = None) -> float | None:
    return imbalance(depth(bids, top), depth(asks, top))


def microprice(bids: Ladder | None, asks: Ladder | None) -> float | None:
    """Size-weighted top-of-book price: (P_bid·Q_ask + P_ask·Q_bid)/(Q_bid+Q_ask).

    Leans toward the side with less size behind it — the direction the next
    trade is likelier to push. Needs both sides; None otherwise.
    """
    if not bids or not asks:
        return None
    (pb, qb), (pa, qa) = bids[0], asks[0]
    total = qb + qa
    if total <= _EPS:
        return None
    return round((pb * qa + pa * qb) / total, 6)


def metrics(bids: Ladder | None, asks: Ladder | None) -> dict[str, float | None]:
    """The full derived set stored on every book snapshot row, both venues."""
    bd, ad = depth(bids), depth(asks)
    return {
        "bid_depth": round(bd, 6),
        "ask_depth": round(ad, 6),
        "obi": imbalance(bd, ad),
        "obi_1": obi(bids, asks, top=1),
        "obi_5": obi(bids, asks, top=5),
        "microprice": microprice(bids, asks),
    }
