"""Streaming collector entrypoint — event-driven capture on both venues.

    python engine/stream.py [--venue pm|kalshi] [--duration SEC]

Runs alongside `engine/collect.py`, not instead of it. Polling owns metadata,
price history, predictions and the periodic full-truth baseline; this fills
in what happens between polls and captures every trade as it prints.

Rows land in the same tables tagged `source='stream'`, so a query can use
either or both.

Kalshi's socket needs the same RSA-PSS credentials as the REST client, but
only for market data — this process never touches a portfolio or order
endpoint. Polymarket's needs nothing.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from cryptography.hazmat.primitives import serialization

from betx import config
from betx.arena import ArenaDB
from betx.db import Store
from betx.polymarket import PolymarketClient
from betx.stream import KalshiStream, PolymarketStream, RowSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("betx.stream")


def pm_universe(cfg) -> tuple[list[str], dict[str, dict]]:
    """Every token of every open arena PM event, with the context each row needs."""
    arena = ArenaDB(cfg.arena_database_url)
    client = PolymarketClient(timeout=cfg.pm_timeout_sec, min_interval_sec=cfg.pm_min_interval_sec)
    try:
        arena_events = {e.slug: e for e in arena.pm_events(open_only=True)}
        events = client.events_by_slug(sorted(arena_events))
        tokens: list[str] = []
        meta: dict[str, dict] = {}
        for ev in events:
            a = arena_events.get(ev.slug)
            titles = {}
            if a:
                for t in a.market_titles:
                    hit = ev.match_outcome(t)
                    if hit:
                        titles[hit[1].token_id] = t
            for m in ev.markets:
                if m.closed is True or m.active is False or m.accepting_orders is False:
                    continue
                for tok in m.tokens:
                    tokens.append(tok.token_id)
                    meta[tok.token_id] = {
                        "condition_id": m.condition_id,
                        "event_slug": ev.slug,
                        "arena_event_ticker": a.event_ticker if a else None,
                        "arena_market_title": titles.get(tok.token_id),
                        "outcome": tok.outcome,
                        "outcome_index": tok.outcome_index,
                        "tick_size": m.tick_size,
                        "min_order_size": m.min_order_size,
                        "neg_risk": m.neg_risk,
                    }
        return tokens, meta
    finally:
        client.close()


def kx_universe(cfg) -> tuple[list[str], dict[str, dict]]:
    arena = ArenaDB(cfg.arena_database_url)
    mapped, _ = arena.kalshi_markets(
        open_only=not cfg.kx_include_closed,
        forecast_within_days=cfg.kx_forecast_window,
        closing_within_days=cfg.kx_closing_window,
        limit=cfg.kx_max_markets,
    )
    # Closest to resolution first: if the subscription has to be capped, the
    # markets about to settle are the ones whose books matter most.
    mapped.sort(key=lambda m: (m.close_time is None, m.close_time))
    mapped = mapped[: cfg.stream_kx_max_tickers]
    meta = {
        m.ticker: {
            "event_ticker": m.event_ticker,
            "arena_event_ticker": m.event_ticker,
            "arena_market_title": m.market_title,
            "close_time": m.close_time,
        }
        for m in mapped
    }
    return [m.ticker for m in mapped], meta


async def run(cfg, store: Store, duration: float | None) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    sink = RowSink(store)
    started = time.monotonic()

    while not stop.is_set():
        streams = []
        if cfg.stream_pm_enabled:
            tokens, meta = await asyncio.to_thread(pm_universe, cfg)
            if tokens:
                streams.append(PolymarketStream(cfg, sink, tokens, meta))
                log.info("polymarket universe: %d tokens", len(tokens))
        if cfg.stream_kx_enabled:
            tickers, meta = await asyncio.to_thread(kx_universe, cfg)
            if tickers:
                key = serialization.load_pem_private_key(cfg.kalshi_private_key_pem, password=None)
                streams.append(KalshiStream(cfg, sink, tickers, meta, key,
                                            cfg.kalshi_api_key_id, cfg.kalshi_base_url))
                log.info("kalshi universe: %d tickers", len(tickers))
        if not streams:
            log.error("no streams to run; exiting")
            return

        cycle_stop = asyncio.Event()
        tasks = [asyncio.create_task(s.run(cycle_stop)) for s in streams]
        tasks.append(asyncio.create_task(_pump(cfg, sink, streams, cycle_stop, stop, started, duration)))
        try:
            await asyncio.gather(*tasks)
        finally:
            cycle_stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            n = await asyncio.to_thread(sink.drain)
            log.info("final flush: %s", n)
        if duration and time.monotonic() - started >= duration:
            return


async def _pump(cfg, sink: RowSink, streams, cycle_stop: asyncio.Event,
                stop: asyncio.Event, started: float, duration: float | None) -> None:
    """Flush loop: snapshot due books, drain buffers, and time out the
    universe refresh."""
    last_report = time.monotonic()
    while not cycle_stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.stream_flush_sec)
        except asyncio.TimeoutError:
            pass
        now = time.monotonic()
        for s in streams:
            try:
                s.flush_books(now, None)
            except Exception:
                log.exception("%s flush_books failed", s.name)
        written = await asyncio.to_thread(sink.drain)
        if written and now - last_report >= 30:
            log.info("written %s | buffered pm_b=%d kx_b=%d pm_t=%d kx_t=%d | %s",
                     sink.written, len(sink.pm_books), len(sink.kx_books),
                     len(sink.pm_trades), len(sink.kx_trades),
                     {s.name: {**s.stats, "books": len(s.books), "up": s.connected} for s in streams})
            last_report = now
        if stop.is_set() or (duration and now - started >= duration):
            cycle_stop.set()
            return
        if now - started >= cfg.stream_refresh_sec:
            log.info("universe refresh due; cycling connections")
            cycle_stop.set()
            return


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--venue", choices=("pm", "kalshi"), default=None)
    ap.add_argument("--duration", type=float, default=None,
                    help="run for N seconds then exit (verification)")
    args = ap.parse_args()

    cfg = config.load()
    if args.venue == "pm":
        cfg.stream_kx_enabled = False
    elif args.venue == "kalshi":
        cfg.stream_pm_enabled = False
    cfg.require("database_url", "arena_database_url")
    if cfg.stream_kx_enabled:
        cfg.require("kalshi_api_key_id", "kalshi_private_key_b64")

    store = Store(cfg.database_url, cfg.db_schema)
    store.migrate()
    log.info("stream starting: instance=%s schema=%s pm=%s kalshi=%s min_interval=%ss",
             cfg.instance_name, cfg.db_schema, cfg.stream_pm_enabled,
             cfg.stream_kx_enabled, cfg.stream_min_interval_sec)
    asyncio.run(run(cfg, store, args.duration))


if __name__ == "__main__":
    main()
