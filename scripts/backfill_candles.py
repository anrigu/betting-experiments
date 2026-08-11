"""Backfill 1-minute top-of-book candles for every market we hold predictions on.

Kalshi's candlestick endpoint returns, per minute: OHLC of yes_bid / yes_ask
(top of book), OHLC+mean of trade price, volume, and open interest — for the
market's whole life, including long-settled markets. Full L2 depth is NOT
recoverable historically; this fills the L1 gap around prediction timestamps.

Window per ticker: (first prediction seen - buffer) .. min(close_time, now),
chunked to respect the API's 5000-candles-per-request cap. Progress is kept in
{schema}.candles_backfill_state so the script is idempotent and resumable —
rerunning extends still-open markets and skips completed ones.

Usage:
  uv run --with httpx --with "psycopg[binary]" --with python-dotenv \
      python scripts/backfill_candles.py [--limit N] [--workers 6]
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import httpx
import psycopg
from dotenv import load_dotenv

log = logging.getLogger("backfill_candles")

API = "https://api.elections.kalshi.com/trade-api/v2"
CHUNK_MINUTES = 4900          # API caps a request at 5000 candles
RESUME_OVERLAP_S = 3600       # re-fetch the last hour when extending a ticker

DDL = """
CREATE TABLE IF NOT EXISTS {s}.candles_1m (
    ticker        TEXT NOT NULL,
    period_ts     TIMESTAMPTZ NOT NULL,   -- end of the 1-minute period
    price_open    NUMERIC, price_high NUMERIC, price_low NUMERIC,
    price_close   NUMERIC, price_mean NUMERIC,
    yes_bid_open  NUMERIC, yes_bid_high NUMERIC, yes_bid_low NUMERIC, yes_bid_close NUMERIC,
    yes_ask_open  NUMERIC, yes_ask_high NUMERIC, yes_ask_low NUMERIC, yes_ask_close NUMERIC,
    volume        NUMERIC,
    open_interest NUMERIC,
    PRIMARY KEY (ticker, period_ts)
);
CREATE TABLE IF NOT EXISTS {s}.candles_backfill_state (
    ticker       TEXT PRIMARY KEY,
    series       TEXT,
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    candles      INTEGER DEFAULT 0,
    status       TEXT,                    -- done | error
    error        TEXT,
    updated_at   TIMESTAMPTZ DEFAULT now()
);
"""

INSERT = """
INSERT INTO {s}.candles_1m (
    ticker, period_ts,
    price_open, price_high, price_low, price_close, price_mean,
    yes_bid_open, yes_bid_high, yes_bid_low, yes_bid_close,
    yes_ask_open, yes_ask_high, yes_ask_low, yes_ask_close,
    volume, open_interest
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, period_ts) DO NOTHING
"""

UNIVERSE = """
SELECT p.ticker,
       max(p.event_ticker)           AS event_ticker,
       min(p.created_at)             AS first_seen,
       max(p.close_time)             AS close_time
FROM {s}.predictions p
GROUP BY p.ticker
ORDER BY p.ticker
"""


class RateLimiter:
    """Global cross-thread limiter: at most one request per `interval` seconds."""

    def __init__(self, interval: float):
        self.interval = interval
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next - now
            self._next = max(now, self._next) + self.interval
        if delay > 0:
            time.sleep(delay)


def dollars(v) -> Decimal | None:
    return Decimal(v) if v not in (None, "") else None


def candle_row(ticker: str, c: dict) -> tuple:
    price = c.get("price") or {}
    bid = c.get("yes_bid") or {}
    ask = c.get("yes_ask") or {}
    return (
        ticker,
        dt.datetime.fromtimestamp(c["end_period_ts"], dt.timezone.utc),
        dollars(price.get("open_dollars")), dollars(price.get("high_dollars")),
        dollars(price.get("low_dollars")), dollars(price.get("close_dollars")),
        dollars(price.get("mean_dollars")),
        dollars(bid.get("open_dollars")), dollars(bid.get("high_dollars")),
        dollars(bid.get("low_dollars")), dollars(bid.get("close_dollars")),
        dollars(ask.get("open_dollars")), dollars(ask.get("high_dollars")),
        dollars(ask.get("low_dollars")), dollars(ask.get("close_dollars")),
        dollars(c.get("volume_fp")), dollars(c.get("open_interest_fp")),
    )


def fetch_candles(
    http: httpx.Client, limiter: RateLimiter, series: str, ticker: str, start_ts: int, end_ts: int
) -> list[dict]:
    out: list[dict] = []
    chunk = start_ts
    while chunk < end_ts:
        chunk_end = min(chunk + CHUNK_MINUTES * 60, end_ts)
        for attempt in range(4):
            limiter.wait()
            r = http.get(
                f"{API}/series/{series}/markets/{ticker}/candlesticks",
                params={"start_ts": chunk, "end_ts": chunk_end, "period_interval": 1},
            )
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2.0 * (attempt + 1))
                continue
            r.raise_for_status()
            out.extend(r.json().get("candlesticks", []))
            break
        else:
            raise RuntimeError(f"gave up after retries on {ticker} [{chunk}..{chunk_end}]")
        chunk = chunk_end
    return out


def process_ticker(
    schema: str,
    database_url: str,
    limiter: RateLimiter,
    local: threading.local,
    task: dict,
    buffer_minutes: int,
) -> tuple[str, int]:
    if not hasattr(local, "conn"):
        local.conn = psycopg.connect(database_url, connect_timeout=30)
        local.http = httpx.Client(timeout=30.0)
    conn: psycopg.Connection = local.conn

    ticker = task["ticker"]
    series = task["event_ticker"].split("-")[0]
    now = dt.datetime.now(dt.timezone.utc)
    start = task["first_seen"] - dt.timedelta(minutes=buffer_minutes)
    end = min(task["close_time"], now)
    prior = task.get("prior_end")
    if prior is not None:  # resuming: extend from where we left off, minus overlap
        start = max(start, prior - dt.timedelta(seconds=RESUME_OVERLAP_S))
    if end <= start:
        return ticker, 0

    try:
        candles = fetch_candles(local.http, limiter, series, ticker, int(start.timestamp()), int(end.timestamp()))
        rows = [candle_row(ticker, c) for c in candles]
        with conn.cursor() as cur:
            if rows:
                cur.executemany(INSERT.format(s=schema), rows)
            cur.execute(
                f"""INSERT INTO {schema}.candles_backfill_state
                        (ticker, series, window_start, window_end, candles, status, error, updated_at)
                    VALUES (%s, %s, %s, %s, %s, 'done', NULL, now())
                    ON CONFLICT (ticker) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        candles = candles_backfill_state.candles + EXCLUDED.candles,
                        status = 'done', error = NULL, updated_at = now()""",
                (ticker, series, start, end, len(rows)),
            )
        conn.commit()
        return ticker, len(rows)
    except Exception as e:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {schema}.candles_backfill_state
                        (ticker, series, window_start, window_end, status, error, updated_at)
                    VALUES (%s, %s, %s, %s, 'error', %s, now())
                    ON CONFLICT (ticker) DO UPDATE SET
                        status = 'error', error = EXCLUDED.error, updated_at = now()""",
                (ticker, series, start, end, str(e)[:500]),
            )
        conn.commit()
        log.warning("FAILED %s: %s", ticker, e)
        return ticker, -1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="only process first N tickers (testing)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--rps", type=float, default=8.0, help="global request rate cap")
    ap.add_argument("--buffer-minutes", type=int, default=1440, help="candle history before first prediction")
    args = ap.parse_args()

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    database_url = os.environ["DATABASE_URL"]
    schema = os.environ.get("BETX_DB_SCHEMA", "betting_experiments")

    with psycopg.connect(database_url, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL.format(s=schema))
            cur.execute(UNIVERSE.format(s=schema))
            cols = [d.name for d in cur.description]
            universe = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute(f"SELECT ticker, window_end, status FROM {schema}.candles_backfill_state")
            state = {t: (we, st) for t, we, st in cur.fetchall()}
        conn.commit()

    now = dt.datetime.now(dt.timezone.utc)
    tasks = []
    for t in universe:
        prior_end, status = state.get(t["ticker"], (None, None))
        if status == "done" and prior_end is not None:
            # fully covered if we already fetched through (near) the market end
            if prior_end >= min(t["close_time"], now) - dt.timedelta(seconds=90):
                continue
            t["prior_end"] = prior_end
        tasks.append(t)
    if args.limit:
        tasks = tasks[: args.limit]
    log.info("universe=%d already-complete=%d to-fetch=%d", len(universe), len(universe) - len(tasks), len(tasks))
    if not tasks:
        return 0

    limiter = RateLimiter(1.0 / args.rps)
    local = threading.local()
    done = failed = total_candles = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(process_ticker, schema, database_url, limiter, local, t, args.buffer_minutes)
            for t in tasks
        ]
        for f in as_completed(futures):
            ticker, n = f.result()
            if n < 0:
                failed += 1
            else:
                done += 1
                total_candles += n
            if (done + failed) % 200 == 0:
                log.info("progress %d/%d tickers, %d candles, %d failed", done + failed, len(tasks), total_candles, failed)

    log.info("finished: %d ok, %d failed, %d candles inserted", done, failed, total_candles)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
