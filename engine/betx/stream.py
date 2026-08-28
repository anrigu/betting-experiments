"""Event-driven collection over both venues' WebSocket feeds.

Complements the polling collector rather than replacing it: polling still
owns metadata, price history, predictions and the periodic full-truth
baseline, while this captures what happens *between* polls.

Why not a row per message. Measured on the live feeds, 400 Kalshi tickers
produced 2,928 orderbook deltas in 40 seconds — ~1,400/sec at full scope.
So the streams maintain each book in memory and a snapshot is written only
when the book actually changed AND at most once per
`BETX_STREAM_MIN_INTERVAL_SEC` per market. Trades are different: they are
bounded by real trading activity and are the highest-value rows here, so
every one is written.

Message shapes, captured from the live feeds:

  Polymarket (`wss://ws-subscriptions-clob.polymarket.com/ws/market`, no auth)
    book          full snapshot, same shape as REST /book
    price_change  {price_changes: [{asset_id, price, size, side}]} where
                  `size` is the NEW absolute size at that level (0 removes),
                  side BUY = bid, SELL = ask

  Kalshi (`wss://api.elections.kalshi.com/trade-api/ws/v2`, RSA-PSS signed —
  the same headers the REST client builds)
    orderbook_snapshot  {market_ticker, yes_dollars_fp, no_dollars_fp}
    orderbook_delta     {market_ticker, price_dollars, delta_fp, side} where
                        `delta_fp` is a SIGNED CHANGE to that level, not a
                        new size
    trade               {trade_id, yes_price_dollars, count_fp, ...}
  `seq` runs per subscription id, not per market; a gap means messages were
  missed and the affected books are resynced over REST.
"""
from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import logging
from typing import Any

import websockets
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

from .book_metrics import metrics as book_metrics

log = logging.getLogger(__name__)

PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KX_WS_PATH = "/trade-api/ws/v2"


def _f(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class BookState:
    """One book as {price: size} per side, with ladders on demand."""

    __slots__ = ("bids", "asks", "dirty", "last_written")

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.dirty = False
        self.last_written = 0.0

    def set_level(self, side: str, price: float, size: float) -> None:
        """Absolute set (Polymarket semantics)."""
        book = self.bids if side == "bid" else self.asks
        if size <= 0:
            if book.pop(price, None) is not None:
                self.dirty = True
        elif book.get(price) != size:
            book[price] = size
            self.dirty = True

    def add_level(self, side: str, price: float, delta: float) -> None:
        """Signed delta (Kalshi semantics)."""
        book = self.bids if side == "bid" else self.asks
        new = book.get(price, 0.0) + delta
        if new <= 1e-9:
            if book.pop(price, None) is not None:
                self.dirty = True
        else:
            book[price] = new
            self.dirty = True

    def replace(self, bids: dict[float, float], asks: dict[float, float]) -> None:
        self.bids, self.asks = bids, asks
        self.dirty = True

    def ladders(self) -> tuple[list[list[float]], list[list[float]]]:
        b = sorted(([p, s] for p, s in self.bids.items() if s > 0), key=lambda x: -x[0])
        a = sorted(([p, s] for p, s in self.asks.items() if s > 0), key=lambda x: x[0])
        return b, a

    def due(self, now: float, min_interval: float) -> bool:
        return self.dirty and (now - self.last_written) >= min_interval


class _Stream:
    """Shared reconnect loop. Subclasses implement connect/subscribe/handle."""

    name = "stream"

    def __init__(self, cfg, sink: "RowSink"):
        self.cfg = cfg
        self.sink = sink
        self.books: dict[str, BookState] = {}
        self.connected = False
        self.stats = {"messages": 0, "reconnects": 0, "resyncs": 0, "gaps": 0}

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                await self._session(stop)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connected = False
                self.stats["reconnects"] += 1
                log.warning("%s stream dropped (%s); reconnecting in %.0fs",
                            self.name, str(e)[:160], backoff)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60.0)

    async def _session(self, stop: asyncio.Event) -> None:
        raise NotImplementedError


class PolymarketStream(_Stream):
    name = "polymarket"

    def __init__(self, cfg, sink: "RowSink", tokens: list[str], meta: dict[str, dict]):
        super().__init__(cfg, sink)
        self.tokens = tokens
        self.meta = meta                      # token_id -> row context

    async def _session(self, stop: asyncio.Event) -> None:
        async with websockets.connect(PM_WS, open_timeout=20, ping_interval=20) as ws:
            # The feed accepts one subscribe frame; chunk to keep it modest.
            for i in range(0, len(self.tokens), self.cfg.stream_pm_chunk):
                await ws.send(json.dumps({
                    "assets_ids": self.tokens[i:i + self.cfg.stream_pm_chunk],
                    "type": "market",
                }))
            self.connected = True
            log.info("polymarket stream: subscribed %d tokens", len(self.tokens))
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg.stream_idle_timeout_sec)
                self._handle(raw)

    def _handle(self, raw: str) -> None:
        msgs = json.loads(raw)
        if isinstance(msgs, dict):
            msgs = [msgs]
        for m in msgs:
            self.stats["messages"] += 1
            et = m.get("event_type")
            if et == "book":
                tid = str(m.get("asset_id") or "")
                if not tid:
                    continue
                st = self.books.setdefault(tid, BookState())
                st.replace(
                    {float(l["price"]): float(l["size"]) for l in m.get("bids") or []},
                    {float(l["price"]): float(l["size"]) for l in m.get("asks") or []},
                )
            elif et == "price_change":
                for ch in m.get("price_changes") or []:
                    tid = str(ch.get("asset_id") or "")
                    p, s = _f(ch.get("price")), _f(ch.get("size"))
                    if not tid or p is None or s is None:
                        continue
                    side = "bid" if str(ch.get("side", "")).upper() == "BUY" else "ask"
                    # `size` is the new absolute size at this level, not a delta
                    self.books.setdefault(tid, BookState()).set_level(side, p, s)
            elif et == "last_trade_price":
                tid = str(m.get("asset_id") or "")
                price, size = _f(m.get("price")), _f(m.get("size"))
                if not tid or price is None:
                    continue
                ctx = self.meta.get(tid, {})
                self.sink.pm_trade({
                    "instance": self.cfg.instance_name,
                    "trade_key": f"stream:{tid}:{m.get('timestamp')}:{price}:{size}",
                    "condition_id": ctx.get("condition_id") or str(m.get("market") or ""),
                    "token_id": tid,
                    "event_slug": ctx.get("event_slug"),
                    "arena_event_ticker": ctx.get("arena_event_ticker"),
                    "side": m.get("side"),
                    "outcome": ctx.get("outcome"),
                    "outcome_index": ctx.get("outcome_index"),
                    "price": price,
                    "size": size,
                    "notional_usd": round(price * size, 6) if size is not None else None,
                    "traded_at": _ms(m.get("timestamp")),
                    "proxy_wallet": None,
                    "transaction_hash": None,
                    "source": "stream",
                    "raw": m,
                })

    def flush_books(self, now: float, cycle_id: int | None) -> None:
        mi = self.cfg.stream_min_interval_sec
        for tid, st in self.books.items():
            if not st.due(now, mi):
                continue
            bids, asks = st.ladders()
            if not bids and not asks:
                st.dirty = False
                continue
            ctx = self.meta.get(tid, {})
            self.sink.pm_book({
                "instance": self.cfg.instance_name,
                "cycle_id": cycle_id,
                "event_slug": ctx.get("event_slug"),
                "arena_event_ticker": ctx.get("arena_event_ticker"),
                "condition_id": ctx.get("condition_id"),
                "token_id": tid,
                "outcome": ctx.get("outcome"),
                "outcome_index": ctx.get("outcome_index"),
                "arena_market_title": ctx.get("arena_market_title"),
                "book_hash": None,
                "book_ts": dt.datetime.now(dt.timezone.utc),
                "best_bid": bids[0][0] if bids else None,
                "best_ask": asks[0][0] if asks else None,
                "midpoint": round((bids[0][0] + asks[0][0]) / 2, 6) if bids and asks else None,
                "spread": round(asks[0][0] - bids[0][0], 6) if bids and asks else None,
                "last_trade_price": None,
                **book_metrics(bids, asks),
                "bid_levels": len(bids),
                "ask_levels": len(asks),
                "tick_size": ctx.get("tick_size"),
                "min_order_size": ctx.get("min_order_size"),
                "neg_risk": ctx.get("neg_risk"),
                "bids": bids,
                "asks": asks,
                "source": "stream",
            })
            st.dirty = False
            st.last_written = now


class KalshiStream(_Stream):
    name = "kalshi"

    def __init__(self, cfg, sink: "RowSink", tickers: list[str], meta: dict[str, dict],
                 key, api_key_id: str, base_url: str):
        super().__init__(cfg, sink)
        self.tickers = tickers
        self.meta = meta
        self._key = key
        self._api_key_id = api_key_id
        self._url = base_url.replace("https://", "wss://").rstrip("/") + KX_WS_PATH
        self._seq: dict[int, int] = {}

    def _headers(self) -> dict[str, str]:
        ts = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))
        sig = self._key.sign(
            (ts + "GET" + KX_WS_PATH).encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def _session(self, stop: asyncio.Event) -> None:
        async with websockets.connect(self._url, additional_headers=self._headers(),
                                      open_timeout=20, ping_interval=20) as ws:
            await ws.send(json.dumps({
                "id": 1, "cmd": "subscribe",
                "params": {"channels": ["orderbook_delta", "trade"],
                           "market_tickers": self.tickers},
            }))
            self.connected = True
            self._seq.clear()
            log.info("kalshi stream: subscribed %d tickers", len(self.tickers))
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg.stream_idle_timeout_sec)
                self._handle(raw)

    def _handle(self, raw: str) -> None:
        m = json.loads(raw)
        self.stats["messages"] += 1
        typ, msg = m.get("type"), m.get("msg") or {}
        sid, seq = m.get("sid"), m.get("seq")
        if typ == "error":
            log.warning("kalshi stream error: %s", msg)
            return
        if isinstance(sid, int) and isinstance(seq, int):
            prev = self._seq.get(sid)
            # seq runs per subscription, not per market; a jump means we
            # missed messages and the in-memory books may be wrong.
            if prev is not None and seq != prev + 1:
                self.stats["gaps"] += 1
                log.warning("kalshi seq gap on sid %s: %s -> %s; books need resync",
                            sid, prev, seq)
                self._mark_stale()
            self._seq[sid] = seq

        ticker = msg.get("market_ticker")
        if typ == "orderbook_snapshot" and ticker:
            st = self.books.setdefault(ticker, BookState())
            st.replace(_kx_levels(msg, "yes"), _kx_levels(msg, "no"))
        elif typ == "orderbook_delta" and ticker:
            price = _f(msg.get("price_dollars"))
            if price is None and msg.get("price") is not None:
                price = float(msg["price"]) / 100.0
            delta = _f(msg.get("delta_fp"))
            if delta is None:
                delta = _f(msg.get("delta"))
            if price is None or delta is None:
                return
            # `delta_fp` is a signed change to that level, not a new size
            side = "bid" if msg.get("side") == "yes" else "ask"
            self.books.setdefault(ticker, BookState()).add_level(side, price, delta)
        elif typ == "trade" and ticker:
            ctx = self.meta.get(ticker, {})
            yes_p = _f(msg.get("yes_price_dollars"))
            cnt = _f(msg.get("count_fp")) or _f(msg.get("count"))
            self.sink.kx_trade({
                "instance": self.cfg.instance_name,
                "trade_id": msg.get("trade_id"),
                "ticker": ticker,
                "event_ticker": ctx.get("event_ticker"),
                "taker_side": msg.get("taker_side"),
                "taker_book_side": msg.get("taker_book_side"),
                "is_block_trade": msg.get("is_block_trade"),
                "count": cnt,
                "yes_price": yes_p,
                "no_price": _f(msg.get("no_price_dollars")),
                "notional_usd": round(cnt * yes_p, 6) if cnt is not None and yes_p is not None else None,
                "traded_at": _ms(msg.get("ts_ms")) or _sec(msg.get("ts")),
                "source": "stream",
                "raw": msg,
            })

    def _mark_stale(self) -> None:
        """Drop in-memory books after a gap. The next snapshot (on reconnect)
        or the polling collector re-establishes truth; writing deltas onto a
        book we know is wrong would silently poison the series."""
        self.stats["resyncs"] += 1
        self.books.clear()

    def flush_books(self, now: float, cycle_id: int | None) -> None:
        mi = self.cfg.stream_min_interval_sec
        for ticker, st in self.books.items():
            if not st.due(now, mi):
                continue
            yb, ya = st.ladders()
            if not yb and not ya:
                st.dirty = False
                continue
            ctx = self.meta.get(ticker, {})
            # `asks` here is the NO bid ladder; the executable YES ask is its
            # complement, matching the polling collector's convention.
            yes_asks = sorted(([round(1.0 - p, 4), s] for p, s in ya if 0.0 < p < 1.0),
                              key=lambda x: x[0])
            no_asks = sorted(([round(1.0 - p, 4), s] for p, s in yb if 0.0 < p < 1.0),
                             key=lambda x: x[0])
            best_bid = yb[0][0] if yb else None
            best_ask = yes_asks[0][0] if yes_asks else None
            self.sink.kx_book({
                "instance": self.cfg.instance_name,
                "cycle_id": cycle_id,
                "ticker": ticker,
                "event_ticker": ctx.get("event_ticker"),
                "arena_event_ticker": ctx.get("arena_event_ticker"),
                "arena_market_title": ctx.get("arena_market_title"),
                "status": None,
                "close_time": ctx.get("close_time"),
                "book_hash": None,
                "yes_bid": best_bid,
                "yes_ask": best_ask,
                "no_bid": ya[0][0] if ya else None,
                "no_ask": no_asks[0][0] if no_asks else None,
                "last_price": None,
                "midpoint": round((best_bid + best_ask) / 2, 6) if best_bid is not None and best_ask is not None else None,
                "spread": round(best_ask - best_bid, 6) if best_bid is not None and best_ask is not None else None,
                **book_metrics(yb, yes_asks),
                "bid_levels": len(yb),
                "ask_levels": len(yes_asks),
                "open_interest": None, "volume": None, "volume_24h": None, "liquidity": None,
                "yes_bids": yb, "no_bids": ya,
                "yes_asks": yes_asks, "no_asks": no_asks,
                "source": "stream",
            })
            st.dirty = False
            st.last_written = now


def _kx_levels(msg: dict, side: str) -> dict[float, float]:
    raw = msg.get(f"{side}_dollars_fp") or msg.get(f"{side}_dollars") or msg.get(side) or []
    out: dict[float, float] = {}
    for lv in raw:
        try:
            p, s = float(lv[0]), float(lv[1])
        except (TypeError, ValueError, IndexError):
            continue
        if p > 1.0:          # legacy cents shape
            p = p / 100.0
        if s > 0:
            out[round(p, 4)] = s
    return out


def _ms(v: Any) -> dt.datetime | None:
    f = _f(v)
    if f is None:
        return None
    return dt.datetime.fromtimestamp(f / 1000.0 if f > 1e11 else f, tz=dt.timezone.utc)


def _sec(v: Any) -> dt.datetime | None:
    f = _f(v)
    return dt.datetime.fromtimestamp(f, tz=dt.timezone.utc) if f else None


class RowSink:
    """Buffers rows off the event loop and flushes them in batches.

    Writing per message would put a ~300ms Supabase round trip in the path of
    a 1,400/sec feed. Buffers are bounded: on overflow the OLDEST book rows
    are dropped (a newer snapshot supersedes them anyway) while trades are
    never dropped, because a lost trade is unrecoverable.
    """

    def __init__(self, store, max_books: int = 200_000, max_trades: int = 200_000):
        self.store = store
        self.pm_books: list[dict] = []
        self.kx_books: list[dict] = []
        self.pm_trades: list[dict] = []
        self.kx_trades: list[dict] = []
        self.max_books = max_books
        self.max_trades = max_trades
        self.dropped = 0
        self.written = {"pm_books": 0, "kx_books": 0, "pm_trades": 0, "kx_trades": 0}

    def pm_book(self, row: dict) -> None:
        self._push(self.pm_books, row, self.max_books, drop_oldest=True)

    def kx_book(self, row: dict) -> None:
        self._push(self.kx_books, row, self.max_books, drop_oldest=True)

    def pm_trade(self, row: dict) -> None:
        self._push(self.pm_trades, row, self.max_trades, drop_oldest=False)

    def kx_trade(self, row: dict) -> None:
        self._push(self.kx_trades, row, self.max_trades, drop_oldest=False)

    def _push(self, buf: list, row: dict, cap: int, drop_oldest: bool) -> None:
        if len(buf) >= cap:
            if not drop_oldest:
                return
            del buf[: cap // 10]
            self.dropped += cap // 10
        buf.append(row)

    def drain(self) -> dict[str, int]:
        """Called in a worker thread — every store call here is blocking."""
        out = {}
        for key, buf, fn in (
            ("pm_books", self.pm_books, self.store.insert_pm_book_snapshots),
            ("kx_books", self.kx_books, self.store.insert_kx_book_snapshots),
            ("pm_trades", self.pm_trades, self.store.insert_pm_trades),
            ("kx_trades", self.kx_trades, self.store.insert_kx_trades),
        ):
            if not buf:
                continue
            batch, buf[:] = list(buf), []
            try:
                n = fn(batch)
            except Exception:
                log.exception("stream flush failed for %s (%d rows dropped)", key, len(batch))
                n = 0
            out[key] = n
            self.written[key] += n
        return out
