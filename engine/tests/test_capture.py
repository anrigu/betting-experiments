"""Trade-time data capture + public API routing.

Pins two behaviors: (1) every order submit — dry-run, live, or failed —
writes a 'post_trade' book snapshot carrying the full book, the complete
market payload, and the public trade tape, linked to the order row;
(2) get_public falls back to the signed host on outage-shaped failures
(transport, 429, 5xx) and 404 (public-host replica lag), but propagates
deterministic client errors directly.
"""
import datetime as dt
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from betx import strategies
from betx.config import Config
from betx.engine import Candidate, Engine
from betx.kalshi import KalshiClient, KalshiError

FULL_BOOK = {
    "yes_dollars": [["0.40", "90.00"], ["0.39", "50.00"]],
    "no_dollars": [["0.55", "30.00"], ["0.50", "10.00"]],
}
MARKET = {
    "ticker": "T-1", "status": "active", "close_time": "2030-01-01T00:00:00Z",
    "yes_bid_dollars": "0.4000", "yes_ask_dollars": "0.4500",
    "no_bid_dollars": "0.5500", "no_ask_dollars": "0.6000",
    "last_price_dollars": "0.4200", "volume_24h": 1234, "open_interest": 500,
    "liquidity_dollars": "1000.00", "rules_primary": "full payload survives",
}
TAPE = [{"trade_id": "t1", "yes_price_dollars": "0.4100"}]


class FakeStore:
    def __init__(self):
        self.orders = []
        self.snapshots = []

    def insert_order(self, **row):
        self.orders.append(row)
        return 100 + len(self.orders)

    def insert_book_snapshot(self, **row):
        self.snapshots.append(row)
        return 900 + len(self.snapshots)


class FakeKalshi:
    def __init__(self, order_response=None, order_error=None):
        self._order_response = order_response
        self._order_error = order_error

    def market(self, ticker):
        return dict(MARKET, ticker=ticker)

    def orderbook(self, ticker, depth=None):
        return FULL_BOOK

    def recent_trades(self, ticker, limit=100):
        return TAPE

    def create_order(self, **kw):
        if self._order_error:
            raise self._order_error
        return self._order_response


def _engine(dry_run, kalshi):
    cfg = Config()
    cfg.instance_name = "test"
    cfg.dry_run = dry_run
    store = FakeStore()
    return Engine(cfg, store, kalshi, arena=None), store


def _cand():
    return Candidate(
        prediction_row_id=1, predictor_name="agent-x", ticker="T-1",
        event_ticker="E-1", title="t", category="c",
        close_time=dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc),
        p_model=0.6, predicted_at=dt.datetime.now(dt.timezone.utc),
    )


def _order():
    return strategies.Order(
        side="yes", contracts=3, limit_price=0.45, cost_usd=1.35,
        fee_usd=0.03, edge=0.15, position_before=0, target_position=3,
    )


def _assert_post_trade_snapshot(snap, order_row_id):
    assert snap["kind"] == "post_trade"
    assert snap["order_id"] == order_row_id
    assert snap["orderbook"] == FULL_BOOK
    assert snap["trades"] == TAPE
    assert snap["raw_market"]["rules_primary"] == "full payload survives"
    assert snap["yes_bid"] == 40 and snap["no_bid"] == 55


def test_dry_run_submit_writes_post_trade_snapshot():
    eng, store = _engine(dry_run=True, kalshi=FakeKalshi())
    assert eng._submit(1, 5, "lane-a", _cand(), _order()) == 1
    assert store.orders[0]["status"] == "dry_run"
    _assert_post_trade_snapshot(store.snapshots[0], order_row_id=101)


def test_live_submit_writes_post_trade_snapshot():
    resp = {"order": {"status": "executed", "order_id": "abc", "fill_count": 3}}
    eng, store = _engine(dry_run=False, kalshi=FakeKalshi(order_response=resp))
    assert eng._submit(1, 5, "lane-a", _cand(), _order()) == 1
    assert store.orders[0]["status"] == "executed"
    _assert_post_trade_snapshot(store.snapshots[0], order_row_id=101)


def test_failed_submit_still_captures_market_state():
    eng, store = _engine(dry_run=False, kalshi=FakeKalshi(order_error=KalshiError(400, "rejected")))
    assert eng._submit(1, 5, "lane-a", _cand(), _order()) == 0
    assert store.orders[0]["status"] == "failed"
    _assert_post_trade_snapshot(store.snapshots[0], order_row_id=101)


def test_snapshot_failure_never_fails_the_trade_path():
    class BrokenKalshi(FakeKalshi):
        def market(self, ticker):
            raise KalshiError(500, "down")

    eng, store = _engine(dry_run=True, kalshi=BrokenKalshi())
    assert eng._submit(1, 5, "lane-a", _cand(), _order()) == 1
    assert store.snapshots == []


# --------------------------------------------------------------- get_public

def _routing_client(has_key=True, public="https://pub.example"):
    c = KalshiClient("https://signed.example", public_base_url=public)
    if has_key:
        c._key = object()  # signed path is stubbed below; only the None-check matters
    return c


def _patch(c, public_exc):
    calls = []

    def fake_request(method, path, params=None, json_body=None, retries=3, auth=True):
        calls.append(auth)
        if not auth:
            raise public_exc
        return {"host": "signed"}

    c._request = fake_request
    return calls


@pytest.mark.parametrize("exc", [
    KalshiError(503, "down"), KalshiError(429, "slow"), KalshiError(404, "lag"),
    httpx.ConnectError("boom"),
])
def test_get_public_falls_back_to_signed(exc):
    c = _routing_client()
    calls = _patch(c, exc)
    assert c.get_public("/markets") == {"host": "signed"}
    assert calls == [False, True]


@pytest.mark.parametrize("status", [400, 401, 403])
def test_get_public_propagates_deterministic_4xx(status):
    c = _routing_client()
    calls = _patch(c, KalshiError(status, "bad request"))
    with pytest.raises(KalshiError):
        c.get_public("/markets")
    assert calls == [False]


def test_get_public_without_key_raises_public_error():
    c = _routing_client(has_key=False)
    calls = _patch(c, KalshiError(503, "down"))
    with pytest.raises(KalshiError):
        c.get_public("/markets")
    assert calls == [False]


def test_get_public_without_public_base_uses_signed():
    c = _routing_client(public="")
    calls = _patch(c, AssertionError("public path must not run"))
    assert c.get_public("/markets") == {"host": "signed"}
    assert calls == [True]
