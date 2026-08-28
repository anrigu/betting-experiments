"""Kalshi collector tests — ladder derivation and the derived microstructure
both venues share. No network: fixtures are verbatim live shapes.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from betx.book_metrics import depth, imbalance, metrics, microprice, obi
from betx.kalshi_collect import book_fingerprint, ladders

# live shape: BIDS per side, dollar strings, fractional sizes
ORDERBOOK_FP = {
    "yes_dollars": [["0.2000", "141.00"], ["0.2100", "39.00"], ["0.6100", "3000.00"]],
    "no_dollars": [["0.0100", "16.00"], ["0.3500", "93.51"]],
}


def test_yes_asks_are_the_mirror_of_no_bids():
    """Kalshi lists bids per side; the executable YES ask ladder is
    1 - no_bid at that level's full size."""
    l = ladders(ORDERBOOK_FP)
    assert l["yes_asks"] == [[0.65, 93.51], [0.99, 16.0]]     # lowest ask first
    assert l["no_asks"] == [[0.39, 3000.0], [0.79, 39.0], [0.8, 141.0]]


def test_bids_are_best_first_and_keep_fractional_size():
    l = ladders(ORDERBOOK_FP)
    assert l["yes_bids"] == [[0.61, 3000.0], [0.21, 39.0], [0.2, 141.0]]
    assert l["no_bids"] == [[0.35, 93.51], [0.01, 16.0]]      # 93.51, not 93


def test_legacy_cents_shape_still_parses():
    l = ladders({"yes": [[40, 90]], "no": [[55, 10]]})
    assert l["yes_bids"] == [[0.4, 90.0]]
    assert l["yes_asks"] == [[0.45, 10.0]]


def test_zero_and_boundary_levels_are_dropped():
    l = ladders({"yes_dollars": [["0.5000", "0"], ["1.0000", "5"]], "no_dollars": []})
    assert l["yes_bids"] == [[1.0, 5.0]]      # zero size dropped
    assert l["no_asks"] == []                 # p=1.0 has no complement


def test_fingerprint_changes_only_with_the_book():
    a = ladders(ORDERBOOK_FP)
    b = ladders(ORDERBOOK_FP)
    assert book_fingerprint(a) == book_fingerprint(b)
    moved = ladders({**ORDERBOOK_FP, "yes_dollars": [["0.2000", "142.00"]]})
    assert book_fingerprint(a) != book_fingerprint(moved)


# --- shared microstructure ------------------------------------------------
def test_imbalance_bounds_and_sign():
    assert imbalance(100.0, 0.0) == 1.0        # all bid
    assert imbalance(0.0, 100.0) == -1.0       # all ask
    assert imbalance(50.0, 50.0) == 0.0        # balanced


def test_empty_book_is_none_not_zero():
    """A book with nothing on it is not a balanced book."""
    assert imbalance(0.0, 0.0) is None
    assert obi([], []) is None
    assert microprice([], [[0.5, 1.0]]) is None


def test_obi_can_be_taken_over_top_levels_only():
    bids = [[0.5, 10.0], [0.4, 990.0]]
    asks = [[0.6, 10.0], [0.7, 10.0]]
    assert obi(bids, asks, top=1) == 0.0            # top of book balanced
    assert obi(bids, asks) == 0.960784              # (1000-20)/1020, bid-heavy
    assert depth(bids, top=1) == 10.0 and depth(bids) == 1000.0


def test_microprice_leans_toward_the_thin_side():
    """Heavy bid, thin ask -> price sits near the ask."""
    mp = microprice([[0.40, 900.0]], [[0.60, 100.0]])
    assert mp is not None and mp > 0.55


def test_metrics_bundle_has_every_stored_column():
    m = metrics([[0.4, 10.0]], [[0.6, 30.0]])
    assert set(m) == {"bid_depth", "ask_depth", "obi", "obi_1", "obi_5", "microprice"}
    assert m["bid_depth"] == 10.0 and m["ask_depth"] == 30.0
    assert m["obi"] == -0.5


# --- collection scope -----------------------------------------------------
def test_zero_disables_a_scope_window():
    """0 must mean 'no limit'. Passed straight to SQL it becomes
    `close_time < now()`, which excludes every open market."""
    from betx.config import Config

    assert Config._window(0.0) is None
    assert Config._window(-1.0) is None
    assert Config._window(30.0) == 30.0


def test_close_time_is_not_a_collection_filter_by_default():
    """The engine's only close gate is 24h, so it trades markets resolving
    months out. Scoping collection by close time would drop the books for
    markets it actually bets on — measured at 390 of 2,201 live candidates.
    """
    from betx.config import Config

    cfg = Config(kx_forecast_within_days=30.0, kx_closing_within_days=0.0)
    assert cfg.kx_closing_window is None      # no close-time filter
    assert cfg.kx_forecast_window == 30.0     # forecast recency is the scope
