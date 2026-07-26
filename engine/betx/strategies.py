"""Betting strategy lanes — a faithful port of our live-betting framework
— simplified to this experiment.

Two lanes, both betting the same model's predictions:

* ``momentum`` — force-to-target: the newest forecast fully determines the
  signed position; the delta trades in either direction as ONE limit buy
  (Kalshi nets YES against NO automatically). A forecast inside the fee band
  exits to flat.
* ``fundamental`` — identical decision math but always decided as if flat
  (`position=0`): opens the full ``floor(edge×100)`` fresh every forecast,
  never reduces or exits; positions ride to settlement.

Gates (the ONLY filters, per experiment design):
    (1) fee-aware within-spread gate — the edge is measured against the ask
        of the side being bought; ``n = floor(edge*100 + 1e-9)`` contracts
        must satisfy ``edge*n > kalshi_fee(n, ask)`` (position-level value
        comparison, venue-exact ceil'd fee);
    (2) no new bets within 24h of close (enforced by the engine before the
        book fetch, fail-closed on missing close time);
    (3) mechanical guards: thin_book (skip entirely, never shrink),
        insufficient_cash, at_target, too_small, no_book.

All categories are bet — there is no category filter anywhere.
Prices are dollars in [0, 1]. 1 contract pays $1.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from .fees import taker_fee as kalshi_fee

_EPS = 1e-9

# Ladder = list of (price_dollars, contracts) ask levels, any order.
Ladder = list[tuple[float, int]]


@dataclass
class Skip:
    reason: str
    edge: float = 0.0
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Order:
    side: str                  # "yes" | "no"
    contracts: int
    limit_price: float         # dollars
    cost_usd: float
    fee_usd: float             # ceil'd reserve; actual venue fee replaces it on fill
    edge: float                # raw edge of the entry side (0.0 for pure exits)
    target_position: int       # signed YES-equivalent target
    position_before: int
    detail: dict[str, Any] = field(default_factory=dict)


def best_ask(ladder: Ladder | None) -> float | None:
    if not ladder:
        return None
    return min(p for p, _ in ladder)


def _depth_at_or_below(ladder: Ladder, limit: float) -> int:
    """Contracts fillable by a limit buy at `limit` (all levels priced <= limit)."""
    return sum(c for p, c in ladder if p <= limit + _EPS)


def decide(
    p_e: float,
    yes_asks: Ladder | None,
    no_asks: Ladder | None,
    free_cash: float,
    position: int = 0,
) -> Order | Skip:
    """The momentum (force-to-target) decision. Port of the framework's strategy.decide."""
    best_yes = best_ask(yes_asks)
    best_no = best_ask(no_asks)
    yes_edge = (p_e - best_yes) if best_yes is not None else None
    no_edge = ((1.0 - p_e) - best_no) if best_no is not None else None

    # pick the side with the LARGER positive edge; YES wins ties
    edge, gate_ask, sign = 0.0, None, 0
    if yes_edge is not None and yes_edge > 0 and (no_edge is None or yes_edge >= no_edge):
        edge, gate_ask, sign = yes_edge, best_yes, 1
    elif no_edge is not None and no_edge > 0:
        edge, gate_ask, sign = no_edge, best_no, -1

    n = math.floor(edge * 100.0 + _EPS) if sign != 0 else 0
    clears_fee = (
        sign != 0 and gate_ask is not None and n > 0
        and edge * n > kalshi_fee("", n, gate_ask)
    )
    target = sign * n if clears_fee else 0
    if target == 0:
        edge = 0.0  # a within-fee raw edge must not be logged as if it justified a position
    delta = target - position

    if delta == 0:
        if position != 0:
            return Skip("at_target")
        if sign != 0:
            return Skip("too_small" if n == 0 else "within_spread")
        if best_yes is None or best_no is None:
            return Skip("no_book")
        return Skip("within_spread")

    side, ask, ladder = ("yes", best_yes, yes_asks) if delta > 0 else ("no", best_no, no_asks)
    if ask is None or ladder is None:
        return Skip("no_book")
    contracts = abs(delta)
    if _depth_at_or_below(ladder, ask) < contracts:
        return Skip("thin_book", edge=edge, detail={"wanted": contracts, "depth": _depth_at_or_below(ladder, ask)})
    cost = contracts * ask
    fee = kalshi_fee("", contracts, ask)
    if cost + fee > free_cash:
        return Skip("insufficient_cash", edge=edge, detail={"cost": cost, "fee": fee, "free_cash": free_cash})
    return Order(
        side=side, contracts=contracts, limit_price=ask,
        cost_usd=round(cost, 4), fee_usd=fee, edge=edge,
        target_position=target, position_before=position,
        detail={"best_yes": best_yes, "best_no": best_no, "p_e": p_e},
    )


def decide_fundamental(
    p_e: float,
    yes_asks: Ladder | None,
    no_asks: Ladder | None,
    free_cash: float,
    position: int = 0,
) -> Order | Skip:
    """The FUNDAMENTAL strategy: open a FRESH position every forecast, never
    netting against existing holdings — exactly `decide` as if flat. The real
    position is still recorded on the audit row (`position_before`)."""
    result = decide(p_e, yes_asks, no_asks, free_cash, position=0)
    if isinstance(result, Order):
        result.position_before = position
    return result


DECIDERS = {
    "momentum": decide,
    "fundamental": decide_fundamental,
}


def ladders_from_orderbook(orderbook: dict) -> tuple[Ladder, Ladder]:
    """Kalshi's orderbook lists BIDS per side. The executable ask ladder for
    YES is the mirror of NO bids: yes_ask = 1 - no_bid, with that level's
    full size — and vice versa.

    Handles both API shapes: `orderbook_fp` ({"yes_dollars": [["0.40","90.00"],
    ...]} — dollar strings, fractional sizes) and the legacy cents shape
    ({"yes": [[40, 90], ...]}).
    """
    def bids(side: str) -> list[tuple[float, int]]:
        fp = orderbook.get(f"{side}_dollars")
        if fp is not None:
            return [(float(p), int(float(c))) for p, c in fp]
        return [(p / 100.0, int(c)) for p, c in (orderbook.get(side) or [])]

    yes_asks: Ladder = [
        (round(1.0 - p, 4), c) for p, c in bids("no") if 0.0 < p < 1.0 and c > 0
    ]
    no_asks: Ladder = [
        (round(1.0 - p, 4), c) for p, c in bids("yes") if 0.0 < p < 1.0 and c > 0
    ]
    return yes_asks, no_asks
