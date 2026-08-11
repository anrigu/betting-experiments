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
import base64
import datetime as dt
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import psycopg
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "engine"))
from betx.kalshi import KalshiClient, KalshiError  # noqa: E402

log = logging.getLogger("backfill_candles")

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


def make_client() -> KalshiClient:
    """Signed client when credentials are configured (account-scoped rate
    limits — needed from datacenter IPs, which Kalshi throttles hard);
    unauthenticated public client otherwise."""
    base = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com")
    key_id = os.environ.get("KALSHI_API_KEY_ID", "")
    key_b64 = os.environ.get("KALSHI_PRIVATE_KEY_B64", "")
    if key_id and key_b64:
        log.info("using signed Kalshi requests (key %s...)", key_id[:8])
        return KalshiClient(base_url=base, api_key_id=key_id, private_key_pem=base64.b64decode(key_b64))
    log.info("no Kalshi credentials; using unauthenticated requests")
    return KalshiClient(base_url=base, public_base_url=base)


def fetch_chunk(
    kalshi: KalshiClient, limiter: RateLimiter, series: str, ticker: str, start_ts: int, end_ts: int
) -> list[dict]:
    signed = kalshi._key is not None
    path = f"/series/{series}/markets/{ticker}/candlesticks"
    params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": 1}
    last: Exception | None = None
    for attempt in range(3):  # outer retries on top of the client's own
        limiter.wait()
        try:
            resp = kalshi.get(path, **params) if signed else kalshi.get_public(path, **params)
            return resp.get("candlesticks", [])
        except KalshiError as e:
            if e.status not in (429,) and e.status < 500:
                raise  # 404 etc: deterministic, don't retry
            last = e
            time.sleep(10.0 * (attempt + 1))
    raise RuntimeError(f"gave up on {ticker} [{start_ts}..{end_ts}]: {last}")


def fetch_candles(
    kalshi: KalshiClient, limiter: RateLimiter, series: str, ticker: str, start_ts: int, end_ts: int
) -> list[dict]:
    out: list[dict] = []
    chunk = start_ts
    while chunk < end_ts:
        chunk_end = min(chunk + CHUNK_MINUTES * 60, end_ts)
        out.extend(fetch_chunk(kalshi, limiter, series, ticker, chunk, chunk_end))
        chunk = chunk_end
    return out


def fetch_back_to_activity(
    kalshi: KalshiClient,
    limiter: RateLimiter,
    series: str,
    ticker: str,
    before_ts: int,
    floor_ts: int,
) -> list[dict]:
    """Walk backwards from `before_ts` one chunk at a time until a chunk with
    candles is found (the market's standing quote for carry-forward pricing),
    the market open (`floor_ts`) is reached, or the cap runs out."""
    end = before_ts
    for _ in range(30):  # cap: ~100 days of walk-back
        if end <= floor_ts:
            return []
        start = max(end - CHUNK_MINUTES * 60, floor_ts)
        candles = fetch_chunk(kalshi, limiter, series, ticker, start, end)
        if candles:
            return candles
        end = start
    return []


def process_ticker(
    schema: str,
    database_url: str,
    kalshi: KalshiClient,
    limiter: RateLimiter,
    local: threading.local,
    task: dict,
    buffer_minutes: int,
) -> tuple[str, int]:
    if not hasattr(local, "conn"):
        local.conn = psycopg.connect(database_url, connect_timeout=30)
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
        candles = fetch_candles(kalshi, limiter, series, ticker, int(start.timestamp()), int(end.timestamp()))
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
        log.warning("FAILED %s: %s", ticker, e)
        # Best-effort error record. If the connection itself died (pooler
        # drop), rebuild it for the next ticker instead of crashing the pool.
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {schema}.candles_backfill_state
                            (ticker, series, window_start, window_end, status, error, updated_at)
                        VALUES (%s, %s, %s, %s, 'error', %s, now())
                        ON CONFLICT (ticker) DO UPDATE SET
                            -- a failed extension must not demote a completed ticker:
                            -- its candles up to window_end are already stored
                            status = CASE WHEN candles_backfill_state.status = 'done'
                                          THEN 'done' ELSE 'error' END,
                            error = EXCLUDED.error, updated_at = now()""",
                    (ticker, series, start, end, str(e)[:500]),
                )
            conn.commit()
        except Exception:
            log.exception("state write failed for %s; recycling connection", ticker)
            try:
                conn.close()
            except Exception:
                pass
            del local.conn
        return ticker, -1


GAP_UNIVERSE = """
WITH per AS (
    SELECT p.ticker, max(p.event_ticker) AS event_ticker,
           min(p.created_at) AS first_pred, min(c.period_ts) AS first_candle
    FROM {s}.predictions p LEFT JOIN {s}.candles_1m c USING (ticker)
    GROUP BY 1)
SELECT ticker, event_ticker, first_pred, first_candle
FROM per
WHERE (first_candle IS NULL OR first_candle > first_pred)
  -- delisted markets (404s) can never be fetched; don't retry them here
  AND ticker NOT IN (SELECT ticker FROM {s}.candles_backfill_state WHERE status = 'error')
ORDER BY ticker
"""


def process_gap_ticker(
    schema: str,
    database_url: str,
    kalshi: KalshiClient,
    limiter: RateLimiter,
    local: threading.local,
    task: dict,
) -> tuple[str, int]:
    """Fetch the standing quote history for a ticker whose predictions predate
    its earliest stored candle: walk back from that boundary to the market's
    last prior activity."""
    if not hasattr(local, "conn"):
        local.conn = psycopg.connect(database_url, connect_timeout=30)
    conn: psycopg.Connection = local.conn
    ticker = task["ticker"]
    series = task["event_ticker"].split("-")[0]
    boundary = min(
        [t for t in (task["first_candle"], task["first_pred"] + dt.timedelta(minutes=1)) if t is not None]
    )
    try:
        limiter.wait()
        market = kalshi.market(ticker)
        open_time = market.get("open_time")
        floor_ts = (
            int(dt.datetime.fromisoformat(open_time.replace("Z", "+00:00")).timestamp())
            if open_time else int(boundary.timestamp()) - 100 * 86400
        )
        candles = fetch_back_to_activity(
            kalshi, limiter, series, ticker, int(boundary.timestamp()), floor_ts
        )
        rows = [candle_row(ticker, c) for c in candles]
        with conn.cursor() as cur:
            if rows:
                cur.executemany(INSERT.format(s=schema), rows)
            cur.execute(
                f"""UPDATE {schema}.candles_backfill_state
                    SET candles = candles + %s, updated_at = now() WHERE ticker = %s""",
                (len(rows), ticker),
            )
        conn.commit()
        return ticker, len(rows)
    except Exception as e:
        log.warning("GAP-FILL FAILED %s: %s", ticker, e)
        try:
            conn.rollback()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            del local.conn
        return ticker, -1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None, help="only process first N tickers (testing)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--rps", type=float, default=8.0, help="global request rate cap")
    ap.add_argument("--buffer-minutes", type=int, default=1440, help="candle history before first prediction")
    ap.add_argument("--gap-fill", action="store_true",
                    help="fetch pre-window standing quotes for tickers whose predictions predate their first candle")
    args = ap.parse_args()

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    database_url = os.environ["DATABASE_URL"]
    schema = os.environ.get("BETX_DB_SCHEMA", "betting_experiments")

    with psycopg.connect(database_url, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL.format(s=schema))
            cur.execute((GAP_UNIVERSE if args.gap_fill else UNIVERSE).format(s=schema))
            cols = [d.name for d in cur.description]
            universe = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute(f"SELECT ticker, window_end, status FROM {schema}.candles_backfill_state")
            state = {t: (we, st) for t, we, st in cur.fetchall()}
        conn.commit()

    if args.gap_fill:
        tasks = universe[: args.limit] if args.limit else universe
        log.info("gap-fill: %d tickers with predictions before their first candle", len(tasks))
        kalshi = make_client()
        limiter = RateLimiter(1.0 / args.rps)
        local = threading.local()
        done = failed = total_candles = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [
                pool.submit(process_gap_ticker, schema, database_url, kalshi, limiter, local, t)
                for t in tasks
            ]
            for f in as_completed(futures):
                try:
                    ticker, n = f.result()
                except Exception:
                    log.exception("gap-fill worker crashed; ticker skipped this run")
                    failed += 1
                    continue
                if n < 0:
                    failed += 1
                else:
                    done += 1
                    total_candles += n
                if (done + failed) % 200 == 0:
                    log.info("gap-fill progress %d/%d, %d candles, %d failed", done + failed, len(tasks), total_candles, failed)
        log.info("gap-fill finished: %d ok, %d failed, %d candles inserted", done, failed, total_candles)
        return 0

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
    complete = len(universe) - len(tasks)
    if args.limit:
        tasks = tasks[: args.limit]
    log.info("universe=%d already-complete=%d to-fetch=%d", len(universe), complete, len(tasks))
    if not tasks:
        return 0

    kalshi = make_client()
    limiter = RateLimiter(1.0 / args.rps)
    local = threading.local()
    done = failed = total_candles = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(process_ticker, schema, database_url, kalshi, limiter, local, t, args.buffer_minutes)
            for t in tasks
        ]
        for f in as_completed(futures):
            try:
                ticker, n = f.result()
            except Exception:
                log.exception("worker crashed; ticker skipped this run")
                failed += 1
                continue
            if n < 0:
                failed += 1
            else:
                done += 1
                total_candles += n
            if (done + failed) % 200 == 0:
                log.info("progress %d/%d tickers, %d candles, %d failed", done + failed, len(tasks), total_candles, failed)

    log.info("finished: %d ok, %d failed, %d candles inserted", done, failed, total_candles)
    # Per-ticker failures are recorded in candles_backfill_state and retried on
    # the next run; a completed sweep is success from the job runner's view.
    return 0


if __name__ == "__main__":
    sys.exit(main())
