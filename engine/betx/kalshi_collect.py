"""Kalshi read-only collector — the `kx_*` mirror of pm_collect.py.

Same guarantees as the Polymarket side: writes only `kx_*` tables, which the
engine never reads, and touches no signed endpoint beyond the market-data
fallback the client already had.

Three things make this venue different from Polymarket, and all three shape
the design:

  * **Scale.** Arena tracks ~15k open Kalshi markets against Polymarket's
    ~400 tokens. Snapshotting all of them every cycle is ~173 GB/month of
    rows, so the universe is scoped to markets carrying a recent FORECAST —
    the thing that makes a book series worth having. Deliberately not scoped
    by close time: the engine's only close gate is 24h, so it trades markets
    resolving months out, and a close-time filter would drop books for
    markets it actually bets on.
  * **One book, two sides.** A Kalshi market is a single book with a bid
    ladder per side; the executable YES ask ladder is the mirror of the NO
    bids. So there is one row per ticker, not one per token, and the NO
    contract's imbalance is exactly `-obi`.
  * **No book hash.** Polymarket hands out a hash that makes unchanged books
    free to detect. Kalshi does not, so we hash the ladders ourselves —
    most of those 15k markets never move, and skipping unchanged books is
    the difference between affordable and not.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

from .arena import ArenaDB, ArenaKalshiMarket
from .book_metrics import metrics as book_metrics
from .config import Config
from .db import Store
from .kalshi import KalshiClient, KalshiError, dollars, money_usd

log = logging.getLogger(__name__)


def _levels(raw: list | None, complement: bool) -> list[list[float]]:
    """One Kalshi ladder as [[price, size], ...], best price first.

    Parsed as floats rather than reusing `strategies.ladders_from_orderbook`,
    which casts sizes to int for the trading path — collection wants the
    fractional size the venue actually reports.
    """
    out: list[list[float]] = []
    for lv in raw or []:
        try:
            p, s = float(lv[0]), float(lv[1])
        except (TypeError, ValueError, IndexError):
            continue
        if s <= 0:
            continue
        if complement:
            if not (0.0 < p < 1.0):
                continue
            p = round(1.0 - p, 4)
        out.append([p, s])
    out.sort(key=lambda x: x[0], reverse=not complement)
    return out


def ladders(orderbook: dict) -> dict[str, list[list[float]]]:
    """All four ladders from one Kalshi book.

    The venue lists BIDS per side. `yes_dollars` are YES bids (dollar
    strings, fractional sizes); the legacy shape is `yes` in cents. The
    executable YES asks are the mirror of the NO bids: yes_ask = 1 - no_bid
    at that level's full size.
    """
    def side(name: str) -> list:
        fp = orderbook.get(f"{name}_dollars")
        if fp is not None:
            return fp
        return [[p / 100.0, c] for p, c in (orderbook.get(name) or [])]

    yes_raw, no_raw = side("yes"), side("no")
    return {
        "yes_bids": _levels(yes_raw, complement=False),
        "no_bids": _levels(no_raw, complement=False),
        "yes_asks": _levels(no_raw, complement=True),
        "no_asks": _levels(yes_raw, complement=True),
    }


def book_fingerprint(l: dict[str, list[list[float]]]) -> str:
    """Content hash standing in for the book hash Kalshi does not provide."""
    return hashlib.sha1(
        json.dumps([l["yes_bids"], l["no_bids"]], separators=(",", ":")).encode()
    ).hexdigest()


class KalshiCollector:
    def __init__(self, cfg: Config, store: Store, arena: ArenaDB, client: KalshiClient):
        self.cfg = cfg
        self.store = store
        self.arena = arena
        self.client = client
        self._prev_hash: dict[str, str] = {}

    # ------------------------------------------------------------- cycle
    def collect(self, cycle_id: int, cycle_n: int) -> tuple[dict, dict]:
        cfg = self.cfg
        stats: dict[str, Any] = {
            "markets_seen": 0, "books_captured": 0, "trades_ingested": 0, "unmapped": 0,
        }
        detail: dict[str, Any] = {}
        t0 = time.monotonic()

        universe, unmapped = self.arena.kalshi_markets(
            open_only=not cfg.kx_include_closed,
            forecast_within_days=cfg.kx_forecast_window,
            closing_within_days=cfg.kx_closing_window,
            limit=cfg.kx_max_markets,
        )
        if unmapped:
            stats["unmapped"] = len(unmapped)
            self.store.record_kx_unmapped([
                {"instance": cfg.instance_name, "arena_event_ticker": et,
                 "arena_market_title": mt, "reason": "arena market row has no other_info.ticker"}
                for et, mt in unmapped
            ])
            log.warning("%d arena kalshi outcome(s) have no ticker (recorded in kx_unmapped)",
                        len(unmapped))
        if not universe:
            detail["elapsed_sec"] = round(time.monotonic() - t0, 2)
            return stats, detail

        by_ticker: dict[str, ArenaKalshiMarket] = {m.ticker: m for m in universe}
        tickers = list(by_ticker)
        stats["markets_seen"] = len(tickers)

        markets = self.client.markets_by_tickers(tickers)
        detail["markets_returned"] = len(markets)
        prev_volume = self.store.kx_market_volumes()
        self._persist_markets(markets, by_ticker)

        if cfg.kx_collect_books:
            stats["books_captured"] = self._collect_books(
                cycle_id, tickers, markets, by_ticker, detail)
        if cfg.kx_collect_trades:
            stats["trades_ingested"] = self._collect_trades(
                markets, by_ticker, prev_volume, detail)

        detail["elapsed_sec"] = round(time.monotonic() - t0, 2)
        return stats, detail

    # ---------------------------------------------------------- metadata
    def _persist_markets(self, markets: dict[str, dict],
                         by_ticker: dict[str, ArenaKalshiMarket]) -> None:
        rows = []
        for ticker, m in markets.items():
            a = by_ticker.get(ticker)
            rows.append({
                "ticker": ticker,
                "event_ticker": m.get("event_ticker"),
                "arena_event_ticker": a.event_ticker if a else None,
                "arena_market_title": a.market_title if a else None,
                "series_ticker": (ticker.split("-", 1)[0] or None),
                "title": m.get("title"),
                "subtitle": m.get("subtitle"),
                "yes_sub_title": m.get("yes_sub_title"),
                "market_type": m.get("market_type"),
                "strike_type": m.get("strike_type"),
                "status": m.get("status"),
                "open_time": m.get("open_time"),
                "close_time": m.get("close_time"),
                "expiration_time": m.get("expiration_time"),
                "expected_expiration_time": m.get("expected_expiration_time"),
                "can_close_early": m.get("can_close_early"),
                "result": m.get("result") or None,
                "rules_primary": (m.get("rules_primary") or "")[:8000] or None,
                "notional_value": money_usd(m, "notional_value"),
                "volume": _fp(m, "volume"),
                "volume_24h": _fp(m, "volume_24h"),
                "open_interest": _fp(m, "open_interest"),
                "liquidity": money_usd(m, "liquidity"),
                "raw": m,
            })
        self.store.upsert_kx_markets(rows)

    # ------------------------------------------------------------- books
    def _collect_books(self, cycle_id: int, tickers: list[str], markets: dict[str, dict],
                       by_ticker: dict[str, ArenaKalshiMarket], detail: dict) -> int:
        books = self.client.orderbooks(tickers)
        detail["books_returned"] = len(books)
        if self.cfg.kx_skip_unchanged_books and not self._prev_hash:
            self._prev_hash = self.store.kx_last_book_hashes(self.cfg.instance_name)

        rows, unchanged, empty = [], 0, 0
        for ticker, ob in books.items():
            l = ladders(ob)
            if not l["yes_bids"] and not l["no_bids"]:
                empty += 1          # a real state: market open, nothing resting
                continue
            fp = book_fingerprint(l)
            if self.cfg.kx_skip_unchanged_books and self._prev_hash.get(ticker) == fp:
                unchanged += 1
                continue
            self._prev_hash[ticker] = fp
            m = markets.get(ticker) or {}
            a = by_ticker.get(ticker)
            yb, ya = l["yes_bids"], l["yes_asks"]
            best_bid = yb[0][0] if yb else None
            best_ask = ya[0][0] if ya else None
            rows.append({
                "instance": self.cfg.instance_name,
                "cycle_id": cycle_id,
                "ticker": ticker,
                "event_ticker": m.get("event_ticker"),
                "arena_event_ticker": a.event_ticker if a else None,
                "arena_market_title": a.market_title if a else None,
                "status": m.get("status"),
                "close_time": m.get("close_time"),
                "book_hash": fp,
                "yes_bid": best_bid,
                "yes_ask": best_ask,
                "no_bid": l["no_bids"][0][0] if l["no_bids"] else None,
                "no_ask": l["no_asks"][0][0] if l["no_asks"] else None,
                "last_price": money_usd(m, "last_price"),
                "midpoint": round((best_bid + best_ask) / 2, 6) if best_bid is not None and best_ask is not None else None,
                "spread": round(best_ask - best_bid, 6) if best_bid is not None and best_ask is not None else None,
                **book_metrics(yb, ya),
                "bid_levels": len(yb),
                "ask_levels": len(ya),
                "open_interest": _fp(m, "open_interest"),
                "volume": _fp(m, "volume"),
                "volume_24h": _fp(m, "volume_24h"),
                "liquidity": money_usd(m, "liquidity"),
                "yes_bids": l["yes_bids"],
                "no_bids": l["no_bids"],
                "yes_asks": l["yes_asks"],
                "no_asks": l["no_asks"],
            })
        if unchanged:
            detail["books_unchanged_skipped"] = unchanged
        if empty:
            detail["books_empty"] = empty
        return self.store.insert_kx_book_snapshots(rows)

    # ------------------------------------------------------------ trades
    def _collect_trades(self, markets: dict[str, dict],
                        by_ticker: dict[str, ArenaKalshiMarket],
                        prev_volume: dict[str, float], detail: dict) -> int:
        """Pull the tape only where volume actually moved.

        `/markets/trades` takes one ticker at a time, so sweeping thousands
        of markets per cycle would dominate everything. The batched market
        payload already tells us total volume, so a market whose volume is
        unchanged since last cycle cannot have traded and is skipped.
        """
        marks = self.store.kx_trade_watermarks()
        candidates = []
        for ticker, m in markets.items():
            vol = _fp(m, "volume")
            if vol is None:
                continue
            prev = prev_volume.get(ticker)
            if prev is not None and vol <= prev:
                continue
            candidates.append(ticker)
        detail["trade_tape_candidates"] = len(candidates)
        if len(candidates) > self.cfg.kx_trades_max_markets:
            # cold start: everything looks new. Oldest watermark first so the
            # backlog drains deterministically across cycles.
            candidates.sort(key=lambda t: marks.get(t, 0))
            candidates = candidates[:self.cfg.kx_trades_max_markets]
            detail["trade_tape_capped"] = True

        rows, failures = [], 0
        for ticker in candidates:
            since = marks.get(ticker)
            try:
                tape = self._tape_since(ticker, since)
            except KalshiError as e:
                failures += 1
                log.warning("kalshi tape failed for %s (%s)", ticker, e)
                continue
            m = markets.get(ticker) or {}
            for t in tape:
                yes_p = dollars(t.get("yes_price_dollars"))
                if yes_p is None and t.get("yes_price") is not None:
                    yes_p = float(t["yes_price"]) / 100.0
                no_p = dollars(t.get("no_price_dollars"))
                if no_p is None and t.get("no_price") is not None:
                    no_p = float(t["no_price"]) / 100.0
                cnt = _num(t.get("count_fp")) or _num(t.get("count"))
                rows.append({
                    "instance": self.cfg.instance_name,
                    "trade_id": t.get("trade_id"),
                    "ticker": ticker,
                    "event_ticker": m.get("event_ticker"),
                    "taker_side": t.get("taker_side"),
                    "taker_book_side": t.get("taker_book_side"),
                    "is_block_trade": t.get("is_block_trade"),
                    "count": cnt,
                    "yes_price": yes_p,
                    "no_price": no_p,
                    "notional_usd": round(cnt * yes_p, 6) if cnt is not None and yes_p is not None else None,
                    "traded_at": t.get("created_time"),
                    "raw": t,
                })
        if failures:
            detail["trade_tape_failures"] = failures
        rows = [r for r in rows if r["trade_id"]]
        seen: set[str] = set()
        uniq = [r for r in rows if not (r["trade_id"] in seen or seen.add(r["trade_id"]))]
        return self.store.insert_kx_trades(uniq)

    def _tape_since(self, ticker: str, since: int | None) -> list[dict]:
        out: list[dict] = []
        cursor = None
        for _ in range(self.cfg.kx_trades_max_pages):
            page = self.client.market_trades(
                ticker, limit=self.cfg.kx_trades_page_size, cursor=cursor,
                min_ts=since + 1 if since else None)
            trades = page.get("trades") or []
            out.extend(trades)
            cursor = page.get("cursor")
            if not cursor or not trades:
                break
        return out


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fp(m: dict, key: str) -> float | None:
    """Fractional count from `<key>_fp` (string) or `<key>` (legacy int)."""
    v = _num(m.get(f"{key}_fp"))
    return v if v is not None else _num(m.get(key))
