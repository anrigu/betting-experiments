"""Strategy math tests — pins the framework-exact behavior, including the venue
fee test vectors and the load-bearing floor epsilon."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from betx.fees import taker_fee
from betx.strategies import Order, Skip, best_ask, decide, decide_fundamental, ladders_from_orderbook


def test_kalshi_fee_vectors():
    # upstream test vectors: ceil(0.07*C*P*(1-P)*100)/100, whole-order rounding
    assert taker_fee("T", 15, 0.40) == 0.26
    assert taker_fee("T", 1, 0.50) == 0.02
    assert taker_fee("T", 100, 0.50) == 1.75
    assert taker_fee("T", 0, 0.50) == 0.0


def test_floor_epsilon_is_load_bearing():
    # 0.60 - 0.40 -> 19.999...96 without the epsilon; must yield 20 contracts
    d = decide(0.60, [(0.40, 500)], [(0.62, 500)], free_cash=1000.0, position=0)
    assert isinstance(d, Order)
    assert d.side == "yes"
    assert d.contracts == 20


def test_within_spread_skips():
    d = decide(0.50, [(0.55, 100)], [(0.55, 100)], free_cash=100.0, position=0)
    assert isinstance(d, Skip) and d.reason == "within_spread"
    assert d.edge == 0.0


def test_edge_between_smooth_and_exact_fee_still_skips():
    # edge = 0.018 -> n = 1; smooth fee at 0.50 = 0.0175 < 0.018,
    # but the venue charges ceil -> 0.02 >= 0.018 -> must skip
    d = decide(0.518, [(0.50, 100)], [(0.52, 100)], free_cash=100.0, position=0)
    assert isinstance(d, Skip)
    assert d.reason == "within_spread"


def test_fee_gate_clears_when_edge_beats_ceil():
    # edge = 0.03 -> n = 3, fee = ceil(0.07*3*0.5*0.5*100)/100 = 0.06 < 0.09
    d = decide(0.53, [(0.50, 100)], [(0.52, 100)], free_cash=100.0, position=0)
    assert isinstance(d, Order)
    assert d.contracts == 3 and d.side == "yes"
    assert d.fee_usd == 0.06


def test_no_side_pick_and_tie_goes_yes():
    d = decide(0.20, [(0.40, 100)], [(0.55, 100)], free_cash=100.0, position=0)
    assert isinstance(d, Order) and d.side == "no"
    assert d.contracts == 25  # (1-0.20) - 0.55 = 0.25


def test_momentum_exits_to_flat_within_band():
    # held 10 YES, forecast now inside the band -> buy 10 NO to flatten
    d = decide(0.50, [(0.52, 100)], [(0.52, 100)], free_cash=100.0, position=10)
    assert isinstance(d, Order)
    assert d.side == "no" and d.contracts == 10
    assert d.target_position == 0 and d.edge == 0.0


def test_momentum_at_target():
    d = decide(0.60, [(0.40, 500)], [(0.62, 500)], free_cash=1000.0, position=20)
    assert isinstance(d, Skip) and d.reason == "at_target"


def test_momentum_nets_delta():
    # target 20, holding 5 -> buy 15 more YES
    d = decide(0.60, [(0.40, 500)], [(0.62, 500)], free_cash=1000.0, position=5)
    assert isinstance(d, Order) and d.side == "yes" and d.contracts == 15


def test_fundamental_ignores_position():
    d = decide_fundamental(0.60, [(0.40, 500)], [(0.62, 500)], free_cash=1000.0, position=20)
    assert isinstance(d, Order)
    assert d.contracts == 20 and d.side == "yes"
    assert d.position_before == 20
    # and a within-band forecast is a no-op, never an exit
    d2 = decide_fundamental(0.50, [(0.52, 100)], [(0.52, 100)], free_cash=100.0, position=10)
    assert isinstance(d2, Skip) and d2.reason == "within_spread"


def test_thin_book_skips_entirely():
    d = decide(0.60, [(0.40, 5)], [(0.62, 100)], free_cash=1000.0, position=0)
    assert isinstance(d, Skip) and d.reason == "thin_book"


def test_limit_at_best_ask_never_walks_the_book():
    # best ask is 0.38 (edge 0.22 -> 22 contracts) but only 12 sit at/below
    # the limit; deeper 0.40 liquidity must NOT be walked -> thin_book skip
    d = decide(0.60, [(0.40, 10), (0.38, 12)], [(0.62, 100)], free_cash=1000.0, position=0)
    assert isinstance(d, Skip) and d.reason == "thin_book"
    # with enough size at the best level it fills at that price
    d2 = decide(0.60, [(0.38, 25)], [(0.62, 100)], free_cash=1000.0, position=0)
    assert isinstance(d2, Order)
    assert d2.limit_price == 0.38 and d2.contracts == 22


def test_insufficient_cash():
    d = decide(0.60, [(0.40, 500)], [(0.62, 500)], free_cash=5.0, position=0)
    assert isinstance(d, Skip) and d.reason == "insufficient_cash"


def test_no_book():
    d = decide(0.60, None, None, free_cash=100.0, position=0)
    assert isinstance(d, Skip) and d.reason == "no_book"


def test_ladders_from_orderbook_legacy_cents():
    # Kalshi orderbook lists BIDS in cents; asks are the mirror of the other side
    ob = {"yes": [[40, 100], [39, 50]], "no": [[55, 200], [50, 80]]}
    yes_asks, no_asks = ladders_from_orderbook(ob)
    assert (0.45, 200) in yes_asks and (0.50, 80) in yes_asks   # 1 - no bids
    assert (0.60, 100) in no_asks and (0.61, 50) in no_asks     # 1 - yes bids


def test_ladders_from_orderbook_fp_dollars():
    # current API: orderbook_fp with dollar-string bid levels
    ob = {"yes_dollars": [["0.0010", "80000.00"], ["0.0020", "439000.00"]],
          "no_dollars": [["0.5500", "88.00"], ["0.4200", "258.00"]]}
    yes_asks, no_asks = ladders_from_orderbook(ob)
    assert (0.45, 88) in yes_asks and (0.58, 258) in yes_asks
    assert (0.999, 80000) in no_asks and (0.998, 439000) in no_asks
    assert best_ask(yes_asks) == 0.45
    assert best_ask(no_asks) == 0.998


# ── vectors ported from sooth tests/test_strategy.py (the reference spec) ──

def test_edge_within_spread_plus_fee_skips():
    # edge 0.01 clears the spread but 1 contract's $0.01 < ceil-fee $0.02 at 0.50
    d = decide(0.51, [(0.50, 500)], [(0.55, 500)], free_cash=500.0)
    assert isinstance(d, Skip) and d.reason == "within_spread"


def test_no_side_edge_within_fee_skips_and_clearing_trades():
    # symmetric NO path: p=0.49 -> no_edge 0.01 at NO ask 0.50 -> skip
    d = decide(0.49, [(0.55, 500)], [(0.50, 500)], free_cash=500.0)
    assert isinstance(d, Skip) and d.reason == "within_spread"
    # p=0.47 -> no_edge 0.03 -> 3 NO, $0.09 > fee $0.06 -> trades
    d2 = decide(0.47, [(0.55, 500)], [(0.50, 500)], free_cash=500.0)
    assert isinstance(d2, Order) and d2.side == "no" and d2.contracts == 3


def test_held_position_edge_in_fee_band_exits_to_flat():
    # holding 15 YES; edge decays to 0.01 (inside fee band) -> exit all 15
    d = decide(0.51, [(0.50, 500)], [(0.52, 500)], free_cash=500.0, position=15)
    assert isinstance(d, Order)
    assert d.side == "no" and d.contracts == 15 and d.target_position == 0


def test_crossed_book_takes_larger_edge():
    # pathological both-positive book: YES edge 0.15 beats NO edge 0.05
    d = decide(0.55, [(0.40, 500)], [(0.40, 500)], free_cash=500.0)
    assert isinstance(d, Order) and d.side == "yes"


def test_rebalance_buys_only_the_difference_to_target():
    d = decide(0.60, [(0.40, 500)], [(0.62, 500)], free_cash=500.0, position=15)
    assert isinstance(d, Order)
    assert d.side == "yes" and d.contracts == 5
    assert d.target_position == 20 and d.position_before == 15


def test_rebalance_reduces_to_smaller_target():
    # holding 15 YES; new edge 0.10 -> target 10 -> buy 5 NO (venue nets down)
    d = decide(0.55, [(0.45, 500)], [(0.62, 500)], free_cash=500.0, position=15)
    assert isinstance(d, Order)
    assert d.side == "no" and d.contracts == 5 and d.target_position == 10


def test_flip_flattens_and_establishes_in_one_order():
    # nominal edge tie (0.10 vs 0.10) resolved by float arithmetic exactly as
    # in the reference implementation: NO wins, flip 15 YES -> 10 NO in one order
    d = decide(0.70, [(0.60, 500)], [(0.20, 500)], free_cash=500.0, position=15)
    assert isinstance(d, Order)
    assert d.side == "no" and d.contracts == 25 and d.target_position == -10


def test_flip_with_subcent_target_flattens_only():
    # NO edge positive but < 1 cent -> target 0 -> exit the 15 YES only
    d = decide(0.70, [(0.80, 500)], [(0.295, 500)], free_cash=500.0, position=15)
    assert isinstance(d, Order)
    assert d.side == "no" and d.contracts == 15 and d.target_position == 0


def test_rebalance_depth_gate_applies_to_full_delta():
    # flip needs 25 NO but only 20 rest -> skip entirely, position untouched
    d = decide(0.70, [(0.60, 500)], [(0.20, 20)], free_cash=500.0, position=15)
    assert isinstance(d, Skip) and d.reason == "thin_book"


def test_within_spread_with_position_exits():
    # no edge either side -> target 0 -> close the whole position
    d = decide(0.57, [(0.60, 500)], [(0.45, 500)], free_cash=500.0, position=15)
    assert isinstance(d, Order)
    assert d.side == "no" and d.contracts == 15 and d.target_position == 0


def test_empty_book_one_side():
    # YES side has the edge but no asks rest there -> no_book
    d = decide(0.55, [], [(0.62, 500)], free_cash=500.0)
    assert isinstance(d, Skip) and d.reason == "no_book"


def test_depth_sums_same_price_levels():
    # two levels at the same best price: 8 + 8 >= 15 -> fills
    d = decide(0.55, [(0.40, 8), (0.40, 8)], [(0.62, 500)], free_cash=500.0)
    assert isinstance(d, Order) and d.contracts == 15


def test_insufficient_cash_boundary():
    # needs $6.00 + $0.26 fee > $5 -> skip
    d = decide(0.55, [(0.40, 500)], [(0.62, 500)], free_cash=5.0)
    assert isinstance(d, Skip) and d.reason == "insufficient_cash"


def test_lane_config_parsing(monkeypatch):
    monkeypatch.setenv(
        "BETX_LANES",
        "gemini:agent-gemini-3.1-pro:fundamental:150, fable-5:agent-claude-fable-5:fundamental:125",
    )
    from betx.config import Config
    cfg = Config(lanes_spec="gemini:agent-gemini-3.1-pro:fundamental:150,"
                            "fable-5:agent-claude-fable-5:fundamental:125")
    lanes = cfg.lanes
    assert [(l.name, l.predictor, l.strategy, l.bankroll) for l in lanes] == [
        ("gemini", "agent-gemini-3.1-pro", "fundamental", 150.0),
        ("fable-5", "agent-claude-fable-5", "fundamental", 125.0),
    ]
    assert cfg.bet_predictors == ["agent-gemini-3.1-pro", "agent-claude-fable-5"]
    assert [l.name for l in cfg.lanes_for("agent-claude-fable-5")] == ["fable-5"]


def test_money_helpers():
    from betx.kalshi import count_of, market_quotes, money_usd
    o = {"taker_fees_dollars": "0.26", "taker_fees": 999999}      # dollars wins, never summed
    assert money_usd(o, "taker_fees") == 0.26
    assert money_usd({"taker_fees": 26}, "taker_fees") == 0.26    # legacy cents
    assert money_usd({}, "taker_fees") is None
    assert count_of({"fill_count_fp": "15.00"}, "fill_count") == 15
    assert count_of({"fill_count": 7}, "fill_count") == 7
    q = market_quotes({"yes_ask_dollars": "0.4500", "no_bid_dollars": "0.5500", "last_price_dollars": "0.0000"})
    assert q["yes_ask"] == 45 and q["no_bid"] == 55 and q["last_price"] == 0
    assert q["yes_bid"] is None
