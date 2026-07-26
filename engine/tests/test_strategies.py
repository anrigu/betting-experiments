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
