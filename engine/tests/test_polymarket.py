"""Polymarket collection tests — the mapping and parsing that the collector's
correctness rests on, pinned against the real shapes the venue returns.

No network: every fixture below is a verbatim trim of a live response.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from betx.arena import _markets_list
from betx.pm_collect import trade_key
from betx.polymarket import (
    PMEvent,
    ladder,
    norm_title,
    slug_from_arena_ticker,
    _jload,
    _ts,
)


# --- fixtures: trimmed live payloads -------------------------------------
NEG_RISK_EVENT = {
    "id": "850694",
    "slug": "best-ai-model-on-august-31",
    "title": "Best AI model on August 31?",
    "negRisk": True,
    "closed": False,
    "markets": [
        {
            "id": "3592036",
            "question": "Will claude-opus-5-max be the best AI model on August 31, 2026?",
            "groupItemTitle": "claude-opus-5-max",
            "conditionId": "0x5fd7cf50",
            "clobTokenIds": '["3117645470", "3118045021"]',
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.925", "0.075"]',
            "orderPriceMinTickSize": 0.01,
            "orderMinSize": 5,
            "negRisk": True,
            "feesEnabled": True,
            "feeType": "tech_fees",
            "feeSchedule": {"exponent": 1, "rate": 0.04, "takerOnly": True, "rebateRate": 0.25},
            "endDate": "2026-09-10",
        },
        {
            "id": "3592037",
            "question": "Will claude-opus-5-high be the best AI model on August 31, 2026?",
            "groupItemTitle": "claude-opus-5-high",
            "conditionId": "0x4eb1ac3d",
            "clobTokenIds": '["9076325125", "2525770860"]',
            "outcomes": '["Yes", "No"]',
            "orderPriceMinTickSize": 0.001,
            "negRisk": True,
        },
    ],
}

BINARY_EVENT = {
    "id": "1",
    "slug": "floyd-mayweather-vs-manny-pacquiao-2",
    "title": "Floyd Mayweather vs. Manny Pacquiao 2",
    "markets": [
        {
            "id": "9",
            "question": "Floyd Mayweather vs. Manny Pacquiao 2",
            "groupItemTitle": "",
            "conditionId": "0xabc",
            "clobTokenIds": '["6340140774", "1111111111"]',
            "outcomes": '["Mayweather", "Pacquiao"]',
        }
    ],
}


# --- gamma's JSON-in-a-string fields -------------------------------------
def test_gamma_encodes_lists_as_json_strings():
    assert _jload('["Yes", "No"]') == ["Yes", "No"]
    assert _jload(["Yes", "No"]) == ["Yes", "No"]
    assert _jload(None) == [] and _jload("not json") == [] and _jload("{}") == []


def test_arena_markets_column_is_a_varchar_holding_json():
    # read without parsing, this VARCHAR iterates character by character
    assert _markets_list('["A", "B"]') == ["A", "B"]
    assert _markets_list("[]") == []
    assert _markets_list(None) == []


def test_arena_ticker_to_slug():
    assert slug_from_arena_ticker("PM-best-ai-model-on-august-31") == "best-ai-model-on-august-31"
    assert slug_from_arena_ticker("KXNFLGAME-26SEP13ATLPIT") is None
    assert slug_from_arena_ticker("PM-") is None


# --- the two event shapes ------------------------------------------------
def test_negrisk_event_matches_on_group_item_title():
    ev = PMEvent.from_gamma(NEG_RISK_EVENT)
    hit = ev.match_outcome("claude-opus-5-high")
    assert hit is not None
    market, token = hit
    assert market.condition_id == "0x4eb1ac3d"
    # the arena outcome is that sub-market's YES token, not the event's first
    assert token.token_id == "9076325125"
    assert token.outcome == "Yes" and token.outcome_index == 0


def test_negrisk_event_never_falls_through_to_yes_no():
    """"Yes" must NOT resolve to the first sub-market of a neg-risk event."""
    ev = PMEvent.from_gamma(NEG_RISK_EVENT)
    assert ev.match_outcome("Yes") is None
    assert ev.match_outcome("nonexistent-model") is None


def test_binary_event_matches_on_outcome_names():
    ev = PMEvent.from_gamma(BINARY_EVENT)
    hit = ev.match_outcome("Pacquiao")
    assert hit is not None
    market, token = hit
    assert market.condition_id == "0xabc"
    assert token.token_id == "1111111111" and token.outcome_index == 1


def test_both_tokens_of_every_market_are_collected():
    """The NO book is a separate book, not the complement of the YES book."""
    ev = PMEvent.from_gamma(NEG_RISK_EVENT)
    assert [t.token_id for t in ev.tokens] == [
        "3117645470", "3118045021", "9076325125", "2525770860",
    ]


def test_tick_size_varies_within_one_event():
    ev = PMEvent.from_gamma(NEG_RISK_EVENT)
    assert [m.tick_size for m in ev.markets] == [0.01, 0.001]
    assert ev.markets[0].min_order_size == 5  # not 1 contract like kalshi


def test_title_matching_folds_dashes_case_and_spacing():
    assert norm_title("340–354") == norm_title("340-354")   # en dash vs hyphen
    assert norm_title("  No  Change ") == norm_title("no change")
    assert norm_title(None) == "" and norm_title("") == ""


# --- book normalisation --------------------------------------------------
def test_ladder_orders_best_price_first_per_side():
    raw = [{"price": "0.93", "size": "19"},
           {"price": "0.96", "size": "1498.97"},
           {"price": "0.94", "size": "453.32"}]
    assert ladder(raw, descending=False)[0] == [0.93, 19.0]   # asks: lowest first
    assert ladder(raw, descending=True)[0] == [0.96, 1498.97]  # bids: highest first


def test_ladder_tolerates_junk_levels():
    assert ladder([{"price": "x", "size": "1"}, {"price": "0.5"}], descending=True) == []
    assert ladder(None, descending=True) == []


def test_sizes_are_fractional_not_whole_contracts():
    assert ladder([{"price": "0.08", "size": "15.34"}], descending=True) == [[0.08, 15.34]]


# --- trade tape dedupe ---------------------------------------------------
def _trade(**kw):
    base = {"transactionHash": "0xdead", "asset": "111", "proxyWallet": "0xw",
            "side": "BUY", "size": 40, "price": 0.08, "timestamp": 1787927092,
            "outcomeIndex": 1}
    base.update(kw)
    return base


def test_trade_key_separates_fills_sharing_a_transaction_hash():
    """One tx routinely covers several fills — a taker sweeping maker levels —
    so the hash alone cannot be the dedupe key."""
    assert trade_key(_trade()) != trade_key(_trade(price=0.09))
    assert trade_key(_trade()) != trade_key(_trade(size=41))
    assert trade_key(_trade()) != trade_key(_trade(proxyWallet="0xother"))


def test_trade_key_is_stable_for_the_same_fill():
    assert trade_key(_trade()) == trade_key(_trade())


# --- timestamps ----------------------------------------------------------
def test_ts_accepts_iso_and_epoch():
    assert _ts("2026-08-31T23:59:00Z").year == 2026
    assert _ts(1787927092).tzinfo is not None
    assert _ts("1787927092").tzinfo is not None
    assert _ts(None) is None and _ts("") is None and _ts("garbage") is None


# --- fees are real and category-specific --------------------------------
def test_fee_schedule_is_captured():
    """Polymarket charges taker fees whose rate depends on the category
    (tech/culture/sports), shaped like Kalshi's rate*C*P*(1-P). Any future
    edge gate needs this, so it has to be collected."""
    m = PMEvent.from_gamma(NEG_RISK_EVENT).markets[0]
    assert m.fees_enabled is True
    assert m.fee_type == "tech_fees"
    assert m.fee_schedule["rate"] == 0.04
    assert m.fee_schedule["takerOnly"] is True


def test_date_only_end_date_becomes_utc_aware():
    """A naive timestamp would be read in the DB session timezone."""
    m = PMEvent.from_gamma(NEG_RISK_EVENT).markets[0]
    assert m.end_date is not None and m.end_date.tzinfo is not None


# --- book fetch universe -------------------------------------------------
def test_only_live_markets_are_asked_for_a_book():
    """Resolved / deactivated markets have no book; asking anyway would make
    a short batch response indistinguishable from a real failure."""
    from betx.pm_collect import _bookable

    live = PMEvent.from_gamma(NEG_RISK_EVENT).markets[0]
    live.closed, live.active, live.accepting_orders = False, True, True
    assert _bookable(live) is True

    for attr, bad in (("closed", True), ("active", False),
                      ("accepting_orders", False), ("enable_order_book", False)):
        m = PMEvent.from_gamma(NEG_RISK_EVENT).markets[0]
        m.closed, m.active, m.accepting_orders, m.enable_order_book = False, True, True, True
        setattr(m, attr, bad)
        assert _bookable(m) is False, attr


# --- schema string safety ------------------------------------------------
def test_ddl_strings_are_formattable():
    """Both DDL blocks go through .format(s=schema); a stray brace in a SQL
    comment (a JSON shape, say) makes migrate() raise KeyError at startup."""
    from betx.db import COLLECT_DDL, DDL

    assert "pm_book_snapshots" in COLLECT_DDL.format(s="probe_schema")
    assert "probe_schema.pm_trades" in COLLECT_DDL.format(s="probe_schema")
    assert "probe_schema.predictions" in DDL.format(s="probe_schema")


# --- storage: raw payload is dropped, so columns must cover it -----------
CLOB_BOOK = {
    "market": "0x5fd7cf50",
    "asset_id": "3117645470",
    "hash": "b4107f36b1353447f508350cfd8380508a532f85",
    "timestamp": "1787931754867",
    "bids": [{"price": "0.92", "size": "247.46"}, {"price": "0.91", "size": "1219.99"}],
    "asks": [{"price": "0.93", "size": "19"}, {"price": "0.94", "size": "453.32"}],
    "last_trade_price": "0.925",
    "tick_size": "0.01",
    "min_order_size": "5",
    "neg_risk": True,
}

# Every key the CLOB /book response carries, and the pm_book_snapshots column
# that captures it. `raw` is not stored: at 551 bytes/row it was pure
# duplication of this mapping. If the venue adds a field, this test fails and
# the choice gets revisited rather than silently losing data.
BOOK_FIELD_COVERAGE = {
    "market": "condition_id",
    "asset_id": "token_id",
    "hash": "book_hash",
    "timestamp": "book_ts",
    "bids": "bids",
    "asks": "asks",
    "last_trade_price": "last_trade_price",
    "tick_size": "tick_size",
    "min_order_size": "min_order_size",
    "neg_risk": "neg_risk",
}


def test_every_book_field_has_a_column():
    assert set(CLOB_BOOK) == set(BOOK_FIELD_COVERAGE), (
        "CLOB /book shape changed; pm_book_snapshots drops `raw`, so a new "
        "field needs a column or it is lost"
    )


def test_book_snapshot_schema_has_no_raw_column():
    from betx.db import COLLECT_DDL

    book_ddl = COLLECT_DDL.split("pm_book_snapshots")[1].split(");")[0]
    assert "raw JSONB" not in book_ddl
    for col in BOOK_FIELD_COVERAGE.values():
        assert col in book_ddl, col


def test_book_timestamp_is_milliseconds():
    """The CLOB returns epoch MILLIseconds; reading it as seconds puts every
    book snapshot ~55,000 years in the future."""
    from betx.pm_collect import _book_ts

    ts = _book_ts(CLOB_BOOK)
    assert ts is not None and ts.year == 2026


# --- bulk insert SQL ------------------------------------------------------
class _FakeCur:
    def __init__(self): self.sql = []
    def execute(self, sql, params=None): self.sql.append(sql); return self
    def fetchall(self): return [(1,)]
    def commit(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False


def test_bulk_insert_never_assumes_an_id_column():
    """pm_events, pm_markets and kx_markets are keyed on a natural primary key
    and have no `id`. `RETURNING id` there fails with UndefinedColumn at
    runtime — a stub store cannot catch it, so pin the generated SQL."""
    from contextlib import contextmanager

    from betx.db import Store

    store = Store("postgres://x/y", "s")
    cur = _FakeCur()

    @contextmanager
    def fake_conn():
        yield cur

    store.conn = fake_conn
    store._insert_many("pm_events", [{"slug": "a"}], conflict="ON CONFLICT (slug) DO NOTHING")
    assert cur.sql and "RETURNING 1" in cur.sql[0]
    assert "RETURNING id" not in cur.sql[0]


def test_natural_key_tables_have_no_id_column():
    from betx.db import COLLECT_DDL

    ddl = COLLECT_DDL.format(s="s")
    for table, pk in (("pm_events", "slug"), ("pm_markets", "condition_id"),
                      ("kx_markets", "ticker")):
        body = ddl.split(f"{table} (")[1].split(");")[0]
        assert f"{pk} TEXT PRIMARY KEY" in body, table
        assert "id BIGSERIAL" not in body, table
