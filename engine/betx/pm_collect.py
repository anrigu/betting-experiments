"""Polymarket read-only collector.

Captures, per cycle, everything Polymarket exposes for the events arena
tracks: Gamma event/market metadata, a point-in-time market snapshot,
full-depth order books for BOTH tokens of every market, the public trade
tape, 1-minute price history, and arena's own forecasts on those events.

Deliberate boundaries:
  * nothing here can place an order — the client has no signing path;
  * nothing here writes to a table the engine reads, so a collector bug
    cannot change a Kalshi decision;
  * arena outcomes that fail to map to a token are recorded in `pm_unmapped`
    rather than dropped. The Kalshi path lost unmapped outcomes silently and
    that was indistinguishable from "no predictions yet".

Cadence: books and the tape run every cycle; price history is cumulative and
runs every Nth cycle (it re-derives the same series, so paying for it each
cycle buys nothing the book snapshots do not already give at higher
resolution).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import time
from typing import Any

from .arena import ArenaDB, ArenaPMEvent
from .book_metrics import metrics as book_metrics
from .config import Config
from .db import Store
from .polymarket import PMEvent, PMMarket, PMToken, PolymarketClient, PolymarketError, ladder

log = logging.getLogger(__name__)


def trade_key(t: dict) -> str:
    """Stable identity for a tape row. The tape has no trade id, and one
    transaction hash routinely covers several fills (a taker sweeping
    multiple maker levels), so the hash has to include the economics."""
    ident = "|".join(
        str(t.get(k) if t.get(k) is not None else "")
        for k in ("transactionHash", "asset", "proxyWallet", "side",
                  "size", "price", "timestamp", "outcomeIndex")
    )
    return hashlib.sha256(ident.encode()).hexdigest()


class PolymarketCollector:
    def __init__(self, cfg: Config, store: Store, arena: ArenaDB,
                 client: PolymarketClient | None = None):
        self.cfg = cfg
        self.store = store
        self.arena = arena
        self.client = client or PolymarketClient(
            timeout=cfg.pm_timeout_sec,
            min_interval_sec=cfg.pm_min_interval_sec,
            batch_size=cfg.pm_batch_size,
        )
        self._cycle_n = 0

    # ------------------------------------------------------------- cycle
    def collect(self, cycle_id: int, cycle_n: int) -> tuple[dict, dict]:
        """Run one Polymarket pass. Returns (stats, detail).

        The cycle row is owned by the shared runner in collect.py, which also
        isolates venues from each other — this may raise.
        """
        cfg = self.cfg
        self._cycle_n = cycle_n
        stats: dict[str, Any] = {
            "events_seen": 0, "markets_seen": 0, "books_captured": 0,
            "trades_ingested": 0, "history_points": 0,
            "predictions_ingested": 0, "unmapped": 0,
        }
        detail: dict[str, Any] = {}
        t0 = time.monotonic()

        arena_events = self._arena_universe()
        events, missing = self._resolve(arena_events)
        if missing:
            detail["unresolved_slugs"] = missing[:50]
            log.warning("%d arena PM slug(s) did not resolve on gamma: %s",
                        len(missing), ", ".join(missing[:5]))
        stats["events_seen"] = len(events)
        stats["markets_seen"] = sum(len(e.markets) for e in events)

        mapping = self._map_outcomes(events, arena_events, stats)
        self._persist_metadata(cycle_id, events, arena_events, mapping)

        if cfg.pm_collect_books:
            stats["books_captured"] = self._collect_books(cycle_id, events, mapping, detail)
        if cfg.pm_collect_trades and self._due(cfg.pm_trades_every_n_cycles):
            stats["trades_ingested"] = self._collect_trades(cycle_id, events, detail)
        if cfg.pm_collect_history and self._due(cfg.pm_history_every_n_cycles):
            stats["history_points"] = self._collect_history(events, detail)
        if cfg.pm_collect_predictions:
            stats["predictions_ingested"] = self._collect_predictions(cycle_id, mapping, arena_events)

        detail["elapsed_sec"] = round(time.monotonic() - t0, 2)
        return stats, detail

    def _due(self, every: int) -> bool:
        return every <= 1 or (self._cycle_n - 1) % every == 0

    # ----------------------------------------------------------- universe
    def _arena_universe(self) -> dict[str, ArenaPMEvent]:
        cfg = self.cfg
        out: dict[str, ArenaPMEvent] = {}
        if cfg.pm_universe in ("arena", "both"):
            for e in self.arena.pm_events(open_only=not cfg.pm_include_closed):
                out[e.slug] = e
        for slug in cfg.pm_extra_slugs:
            out.setdefault(slug, ArenaPMEvent(
                event_ticker="", slug=slug, title="", category=None,
                close_time=None, rules=None,
            ))
        return out

    def _resolve(self, arena_events: dict[str, ArenaPMEvent]) -> tuple[list[PMEvent], list[str]]:
        slugs = sorted(arena_events)
        events = self.client.events_by_slug(slugs) if slugs else []
        if self.cfg.pm_universe in ("all", "both"):
            have = {e.slug for e in events}
            for e in self.client.open_events(max_events=self.cfg.pm_max_events):
                if e.slug not in have:
                    events.append(e)
                    have.add(e.slug)
        missing = [s for s in slugs if s not in {e.slug for e in events}]
        if self.cfg.pm_order_book_only:
            events = [e for e in events if any(m.token_ids for m in e.markets)]
        return events, missing

    # ------------------------------------------------------------ mapping
    def _map_outcomes(
        self,
        events: list[PMEvent],
        arena_events: dict[str, ArenaPMEvent],
        stats: dict,
    ) -> dict[str, Any]:
        """arena outcome name -> the exact CLOB token that pays if it happens.

        Returns lookup tables keyed by token and (when 1:1) by condition id,
        so every downstream row can carry the arena title it corresponds to."""
        by_token: dict[str, str] = {}
        by_condition: dict[str, str] = {}
        by_event_title: dict[tuple[str, str], tuple[str, str]] = {}
        arena_ticker_by_slug: dict[str, str] = {
            s: a.event_ticker for s, a in arena_events.items() if a.event_ticker
        }
        unmapped: list[dict] = []
        for ev in events:
            a = arena_events.get(ev.slug)
            if not a or not a.market_titles:
                continue
            candidates = sorted(
                {m.group_item_title for m in ev.markets if m.group_item_title}
                or {o for m in ev.markets for o in m.outcomes}
            )
            for title in a.market_titles:
                hit = ev.match_outcome(title)
                if hit is None:
                    unmapped.append({
                        "instance": self.cfg.instance_name,
                        "arena_event_ticker": a.event_ticker or f"PM-{ev.slug}",
                        "event_slug": ev.slug,
                        "arena_market_title": title,
                        "reason": "no gamma market or outcome matched",
                        "candidates": candidates[:40],
                    })
                    continue
                market, token = hit
                by_token[token.token_id] = title
                by_event_title[(ev.slug, title)] = (market.condition_id, token.token_id)
                # A single binary market carries BOTH arena outcomes, so the
                # market <-> title link is only 1:1 on multi-outcome events.
                if market.group_item_title:
                    by_condition[market.condition_id] = title
        if unmapped:
            stats["unmapped"] = len(unmapped)
            self.store.record_pm_unmapped(unmapped)
            log.warning("%d arena outcome(s) could not be mapped to a polymarket token "
                        "(recorded in pm_unmapped): %s", len(unmapped),
                        ", ".join(f'{u["event_slug"]}/{u["arena_market_title"]}' for u in unmapped[:5]))
        return {
            "by_token": by_token,
            "by_condition": by_condition,
            "by_event_title": by_event_title,
            "arena_ticker_by_slug": arena_ticker_by_slug,
        }

    # ----------------------------------------------------------- metadata
    def _persist_metadata(
        self,
        cycle_id: int,
        events: list[PMEvent],
        arena_events: dict[str, ArenaPMEvent],
        mapping: dict,
    ) -> None:
        by_condition = mapping["by_condition"]
        ev_rows, mk_rows, snap_rows = [], [], []
        for ev in events:
            a = arena_events.get(ev.slug)
            ticker = a.event_ticker if a and a.event_ticker else None
            ev_rows.append({
                "slug": ev.slug,
                "pm_event_id": ev.event_id,
                "arena_event_ticker": ticker,
                "title": ev.title,
                "arena_category": a.category if a else None,
                "description": ev.description[:8000] if ev.description else None,
                "rules": (a.rules[:8000] if a and a.rules else None),
                "neg_risk": ev.neg_risk,
                "closed": ev.closed,
                "active": ev.active,
                "start_date": ev.start_date,
                "end_date": ev.end_date,
                "arena_close_time": a.close_time if a else None,
                "liquidity": ev.liquidity,
                "volume": ev.volume,
                "volume_24h": ev.volume_24h,
                "open_interest": ev.open_interest,
                "raw": ev.raw,
            })
            for m in ev.markets:
                if not m.condition_id:
                    continue
                toks = m.token_ids
                mk_rows.append({
                    "condition_id": m.condition_id,
                    "pm_market_id": m.market_id,
                    "event_slug": ev.slug,
                    "arena_event_ticker": ticker,
                    "arena_market_title": by_condition.get(m.condition_id),
                    "question": m.question,
                    "market_slug": m.slug,
                    "group_item_title": m.group_item_title or None,
                    "outcomes": m.outcomes,
                    "token_ids": toks,
                    "yes_token_id": toks[0] if len(toks) > 0 else None,
                    "no_token_id": toks[1] if len(toks) > 1 else None,
                    "tick_size": m.tick_size,
                    "min_order_size": m.min_order_size,
                    "neg_risk": m.neg_risk if m.neg_risk is not None else ev.neg_risk,
                    "maker_base_fee": m.maker_base_fee,
                    "taker_base_fee": m.taker_base_fee,
                    "fees_enabled": m.fees_enabled,
                    "fee_type": m.fee_type,
                    "fee_schedule": m.fee_schedule or None,
                    "enable_order_book": m.enable_order_book,
                    "accepting_orders": m.accepting_orders,
                    "closed": m.closed,
                    "active": m.active,
                    "end_date": m.end_date,
                    "raw": m.raw,
                })
                snap_rows.append({
                    "instance": self.cfg.instance_name,
                    "cycle_id": cycle_id,
                    "event_slug": ev.slug,
                    "arena_event_ticker": ticker,
                    "condition_id": m.condition_id,
                    "arena_market_title": by_condition.get(m.condition_id),
                    "best_bid": m.best_bid,
                    "best_ask": m.best_ask,
                    "spread": m.spread,
                    "last_trade_price": m.last_trade_price,
                    "outcome_prices": m.outcome_prices,
                    "volume": m.volume,
                    "volume_24h": m.volume_24h,
                    "liquidity": m.liquidity,
                    "one_day_price_change": m.one_day_price_change,
                    "fees_enabled": m.fees_enabled,
                    "fee_type": m.fee_type,
                    "fee_schedule": m.fee_schedule or None,
                    "accepting_orders": m.accepting_orders,
                    "closed": m.closed,
                    "active": m.active,
                })
        self.store.upsert_pm_events(ev_rows)
        self.store.upsert_pm_markets(mk_rows)
        self.store.insert_pm_market_snapshots(snap_rows)

    # -------------------------------------------------------------- books
    def _collect_books(self, cycle_id: int, events: list[PMEvent],
                       mapping: dict, detail: dict) -> int:
        by_token = mapping["by_token"]
        index: dict[str, tuple[PMEvent, PMMarket, PMToken]] = {}
        skipped = 0
        for ev in events:
            for m in ev.markets:
                # A resolved or deactivated market has no book to fetch.
                # Skipping them keeps `books_returned < tokens_requested` a
                # real signal instead of routine noise; their state is still
                # captured in pm_market_snapshots.
                if not _bookable(m):
                    skipped += len(m.tokens)
                    continue
                for t in m.tokens:
                    index[t.token_id] = (ev, m, t)
        token_ids = list(index)
        if skipped:
            detail["tokens_skipped_unbookable"] = skipped
        if not token_ids:
            return 0

        books = self.client.books(token_ids)
        mids = self.client.midpoints(token_ids) if self.cfg.pm_collect_quotes else {}
        spreads = self.client.spreads(token_ids) if self.cfg.pm_collect_quotes else {}
        detail["tokens_requested"] = len(token_ids)
        detail["books_returned"] = len(books)

        prior = self.store.pm_last_book_hashes(self.cfg.instance_name) if self.cfg.pm_skip_unchanged_books else {}
        rows = []
        for tid, book in books.items():
            entry = index.get(tid)
            if entry is None:
                continue
            ev, m, tok = entry
            bhash = book.get("hash")
            if prior and bhash and prior.get(tid) == bhash:
                continue
            bids = ladder(book.get("bids"), descending=True)
            asks = ladder(book.get("asks"), descending=False)
            rows.append({
                **book_metrics(bids, asks),
                "instance": self.cfg.instance_name,
                "cycle_id": cycle_id,
                "event_slug": ev.slug,
                "arena_event_ticker": mapping["arena_ticker_by_slug"].get(ev.slug),
                "condition_id": m.condition_id,
                "token_id": tid,
                "outcome": tok.outcome,
                "outcome_index": tok.outcome_index,
                "arena_market_title": by_token.get(tid),
                "book_hash": bhash,
                "book_ts": _book_ts(book),
                "best_bid": bids[0][0] if bids else None,
                "best_ask": asks[0][0] if asks else None,
                "midpoint": mids.get(tid),
                "spread": spreads.get(tid),
                "last_trade_price": _num(book.get("last_trade_price")),
                "bid_levels": len(bids),
                "ask_levels": len(asks),
                "tick_size": _num(book.get("tick_size")) or m.tick_size,
                "min_order_size": _num(book.get("min_order_size")) or m.min_order_size,
                "neg_risk": book.get("neg_risk") if isinstance(book.get("neg_risk"), bool) else m.neg_risk,
                "bids": bids,
                "asks": asks,
            })
        return self.store.insert_pm_book_snapshots(rows)

    # ------------------------------------------------------------- trades
    def _collect_trades(self, cycle_id: int, events: list[PMEvent], detail: dict) -> int:
        marks = self.store.pm_trade_watermarks(self.cfg.instance_name)
        token_meta: dict[str, tuple[str, str]] = {}
        targets: list[tuple[PMEvent, PMMarket]] = []
        for ev in events:
            for m in ev.markets:
                if not m.condition_id:
                    continue
                targets.append((ev, m))
                for t in m.tokens:
                    token_meta[t.token_id] = (t.outcome, ev.slug)

        rows: list[dict] = []
        failures = 0
        for ev, m in targets:
            since = marks.get(m.condition_id)
            try:
                tape = self.client.trades_since(
                    m.condition_id, since,
                    limit=self.cfg.pm_trades_page_size,
                    max_pages=self.cfg.pm_trades_max_pages if since is None
                    else self.cfg.pm_trades_max_pages_warm,
                )
            except PolymarketError as e:
                failures += 1
                log.warning("trade tape failed for %s (%s)", m.condition_id, e)
                continue
            for t in tape:
                asset = str(t.get("asset") or "")
                price, size = _num(t.get("price")), _num(t.get("size"))
                rows.append({
                    "instance": self.cfg.instance_name,
                    "trade_key": trade_key(t),
                    "condition_id": m.condition_id,
                    "token_id": asset or None,
                    "event_slug": ev.slug,
                    "arena_event_ticker": f"PM-{ev.slug}",
                    "side": t.get("side"),
                    "outcome": t.get("outcome") or token_meta.get(asset, (None, None))[0],
                    "outcome_index": _int(t.get("outcomeIndex")),
                    "price": price,
                    "size": size,
                    "notional_usd": round(price * size, 6) if price is not None and size is not None else None,
                    "traded_at": _ts_epoch(t.get("timestamp")),
                    "proxy_wallet": t.get("proxyWallet"),
                    "transaction_hash": t.get("transactionHash"),
                    "raw": t,
                })
        if failures:
            detail["trade_tape_failures"] = failures
        # One tape row can surface under two markets in a neg-risk event; the
        # unique (instance, trade_key) index settles it, but de-duping in
        # memory first keeps the insert from fighting itself.
        seen: set[str] = set()
        uniq = [r for r in rows if not (r["trade_key"] in seen or seen.add(r["trade_key"]))]
        return self.store.insert_pm_trades(uniq)

    # ------------------------------------------------------------ history
    def _collect_history(self, events: list[PMEvent], detail: dict) -> int:
        marks = self.store.pm_history_watermarks()
        rows: list[dict] = []
        failures = 0
        for ev in events:
            for m in ev.markets:
                for t in m.tokens:
                    since = marks.get(t.token_id)
                    try:
                        if since is None:
                            pts = self.client.price_history(
                                t.token_id, interval=self.cfg.pm_history_cold_interval,
                                fidelity=self.cfg.pm_history_fidelity)
                        else:
                            pts = self.client.price_history(
                                t.token_id, start_ts=since + 1,
                                fidelity=self.cfg.pm_history_fidelity)
                    except PolymarketError as e:
                        failures += 1
                        log.warning("price history failed for token %s (%s)", t.token_id, e)
                        continue
                    for pt in pts:
                        ts, price = _ts_epoch(pt.get("t")), _num(pt.get("p"))
                        if ts is None or price is None:
                            continue
                        rows.append({
                            "token_id": t.token_id,
                            "condition_id": m.condition_id,
                            "event_slug": ev.slug,
                            "ts": ts,
                            "price": price,
                        })
        if failures:
            detail["history_failures"] = failures
        seen: set[tuple] = set()
        uniq = [r for r in rows if not ((r["token_id"], r["ts"]) in seen or seen.add((r["token_id"], r["ts"])))]
        return self.store.insert_pm_price_history(uniq)

    # -------------------------------------------------------- predictions
    def _collect_predictions(self, cycle_id: int, mapping: dict,
                             arena_events: dict[str, ArenaPMEvent]) -> int:
        cfg = self.cfg
        watermark = self.store.pm_prediction_watermark(cfg.instance_name)
        if watermark is None:
            watermark = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=cfg.pm_backfill_hours)
        else:
            watermark -= dt.timedelta(minutes=10)  # arena commits slightly out of order
        preds = self.arena.fetch_pm_predictions(cfg.pm_predictors or None, since=watermark)
        if not preds:
            return 0

        # Each row carries the token it is a forecast OF, not just a free-text
        # outcome name. A prediction on an event that left the universe this
        # cycle (closed, or arena stopped tracking it) lands with a NULL
        # condition_id and keeps arena_market_title for a later backfill.
        token_by_event_title: dict[tuple[str, str], tuple[str, str]] = mapping["by_event_title"]

        rows = []
        for p in preds:
            key = (p.slug, p.market_title)
            cond_tok = token_by_event_title.get(key)
            rows.append({
                "instance": cfg.instance_name,
                "cycle_id": cycle_id,
                "arena_prediction_id": p.arena_prediction_id,
                "predictor_name": p.predictor_name,
                "harness": "agentic" if p.predictor_name.startswith("agent-") else "fixed-context",
                "arena_event_ticker": p.event_ticker,
                "event_slug": p.slug,
                "event_title": p.event_title,
                "category": p.category,
                "close_time": p.close_time,
                "arena_market_title": p.market_title,
                "condition_id": cond_tok[0] if cond_tok else None,
                "token_id": cond_tok[1] if cond_tok else None,
                "p_model": p.p_model,
                "reasoning": p.rationale or None,
                "predicted_at": p.predicted_at,
                "external_id": f"{p.arena_prediction_id}:{p.market_title}",
                "raw": {"market_title": p.market_title},
            })
        return self.store.insert_pm_predictions(rows)

    def close(self) -> None:
        self.client.close()


def _bookable(m: PMMarket) -> bool:
    """Whether the CLOB will serve a book for this market's tokens. Verified
    against the live set: every market that is open, active and accepting
    orders returned a book; every one that was not returned nothing."""
    return not (
        m.closed is True
        or m.active is False
        or m.accepting_orders is False
        or m.enable_order_book is False
    )


# ------------------------------------------------------------------ coerce
def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    f = _num(v)
    return int(f) if f is not None else None


def _ts_epoch(v: Any) -> dt.datetime | None:
    f = _num(v)
    if f is None:
        return None
    try:
        return dt.datetime.fromtimestamp(f, tz=dt.timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _book_ts(book: dict) -> dt.datetime | None:
    """CLOB book timestamps are epoch MILLIseconds."""
    f = _num(book.get("timestamp"))
    if f is None:
        return None
    return _ts_epoch(f / 1000.0 if f > 1e11 else f)
