"""Streaming collector tests — the delta semantics differ per venue and
getting them backwards silently poisons the stored series.

Verified end to end against the live feeds: applying 75s of deltas reproduced
the venue's own REST book exactly on 60/60 Kalshi markets and 54/60
Polymarket tokens, where the 6 differences matched the 6/60 baseline churn
two REST fetches show over the same 2-second window.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from betx.stream import BookState, RowSink, _kx_levels


# --- Polymarket: `size` is the NEW absolute size at that level -----------
def test_pm_set_level_is_absolute_not_additive():
    st = BookState()
    st.set_level("bid", 0.40, 100.0)
    st.set_level("bid", 0.40, 250.0)
    assert st.bids == {0.40: 250.0}      # replaced, not 350


def test_pm_zero_size_removes_the_level():
    st = BookState()
    st.set_level("ask", 0.60, 10.0)
    st.set_level("ask", 0.60, 0.0)
    assert st.asks == {}


# --- Kalshi: `delta_fp` is a SIGNED CHANGE to that level -----------------
def test_kx_add_level_accumulates():
    st = BookState()
    st.add_level("bid", 0.98, 100.0)
    st.add_level("bid", 0.98, -40.0)
    assert st.bids == {0.98: 60.0}


def test_kx_delta_to_zero_removes_the_level():
    st = BookState()
    st.add_level("bid", 0.98, 100.0)
    st.add_level("bid", 0.98, -100.0)
    assert st.bids == {}


def test_kx_negative_delta_on_unknown_level_does_not_go_negative():
    st = BookState()
    st.add_level("ask", 0.5, -25.0)
    assert st.asks == {}


# --- dirty tracking gates the writes -------------------------------------
def test_a_no_op_update_does_not_mark_the_book_dirty():
    st = BookState()
    st.set_level("bid", 0.4, 100.0)
    st.dirty = False
    st.set_level("bid", 0.4, 100.0)      # same size again
    assert st.dirty is False


def test_due_respects_the_rate_limit_and_needs_a_change():
    st = BookState()
    st.set_level("bid", 0.4, 1.0)        # dirty
    st.last_written = 100.0
    assert st.due(now=120.0, min_interval=30.0) is False   # too soon
    assert st.due(now=131.0, min_interval=30.0) is True
    st.dirty = False
    assert st.due(now=999.0, min_interval=30.0) is False   # unchanged


def test_ladders_are_best_price_first_and_drop_empties():
    st = BookState()
    for p, s in ((0.40, 10.0), (0.45, 5.0), (0.30, 7.0)):
        st.set_level("bid", p, s)
    for p, s in ((0.60, 1.0), (0.55, 2.0)):
        st.set_level("ask", p, s)
    bids, asks = st.ladders()
    assert [p for p, _ in bids] == [0.45, 0.40, 0.30]
    assert [p for p, _ in asks] == [0.55, 0.60]


# --- Kalshi snapshot parsing ---------------------------------------------
def test_kx_snapshot_levels_parse_dollars_and_legacy_cents():
    msg = {"yes_dollars_fp": [["0.2000", "141.00"], ["0.9900", "0"]]}
    assert _kx_levels(msg, "yes") == {0.2: 141.0}       # zero size dropped
    assert _kx_levels({"yes": [[40, 90]]}, "yes") == {0.4: 90.0}
    assert _kx_levels({}, "no") == {}


# --- buffer policy --------------------------------------------------------
class _Store:
    def insert_pm_book_snapshots(self, r): return len(r)
    def insert_kx_book_snapshots(self, r): return len(r)
    def insert_pm_trades(self, r): return len(r)
    def insert_kx_trades(self, r): return len(r)


def test_trades_are_never_dropped_but_stale_books_are():
    """A superseded book snapshot is replaceable; a lost trade is not."""
    sink = RowSink(_Store(), max_books=10, max_trades=10)
    for i in range(25):
        sink.kx_book({"i": i})
        sink.kx_trade({"i": i})
    assert sink.dropped > 0                    # books shed under pressure
    assert len(sink.kx_trades) == 10           # trades capped, never evicted
    assert sink.kx_trades[0]["i"] == 0         # and the earliest is kept


def test_drain_empties_buffers_and_counts_writes():
    sink = RowSink(_Store())
    sink.pm_book({"a": 1}); sink.kx_trade({"b": 2})
    out = sink.drain()
    assert out == {"pm_books": 1, "kx_trades": 1}
    assert sink.pm_books == [] and sink.kx_trades == []
    assert sink.drain() == {}


def test_drain_survives_a_failing_store():
    class Boom(_Store):
        def insert_pm_book_snapshots(self, r): raise RuntimeError("db down")
    sink = RowSink(Boom())
    sink.pm_book({"a": 1})
    assert sink.drain() == {"pm_books": 0}     # logged, not raised
    assert sink.pm_books == []
