"""Postgres (Supabase) persistence layer.

Everything lives in its own schema (default `betting_experiments`) so it
cannot collide with the legacy kalshi-trading tables in `public`.
Plain psycopg3 — no ORM — with a small connection-per-call pattern that is
friendly to Supabase's session pooler.
"""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)

DDL = """
CREATE SCHEMA IF NOT EXISTS {s};

CREATE TABLE IF NOT EXISTS {s}.engine_cycles (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    instance TEXT NOT NULL,
    mode TEXT NOT NULL,                 -- live | dry_run
    markets_considered INT DEFAULT 0,
    predictions_made INT DEFAULT 0,
    bets_placed INT DEFAULT 0,
    error TEXT,
    detail JSONB
);

CREATE TABLE IF NOT EXISTS {s}.predictions (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT REFERENCES {s}.engine_cycles(id),
    source TEXT NOT NULL,               -- arena | predictor
    model_spec TEXT NOT NULL,
    harness TEXT,
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    title TEXT,
    category TEXT,
    close_time TIMESTAMPTZ,
    p_model DOUBLE PRECISION NOT NULL,  -- model P(yes)
    confidence DOUBLE PRECISION,
    yes_bid INT, yes_ask INT, no_bid INT, no_ask INT,   -- cents at prediction time
    reasoning TEXT,
    latency_ms INT,
    external_id TEXT,                   -- arena prediction id for dedupe
    raw JSONB,
    UNIQUE (instance, source, external_id)
);

CREATE TABLE IF NOT EXISTS {s}.llm_calls (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    prediction_id BIGINT REFERENCES {s}.predictions(id),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    purpose TEXT NOT NULL,              -- forecast | research | other
    input_tokens INT DEFAULT 0,
    output_tokens INT DEFAULT 0,
    cached_tokens INT DEFAULT 0,
    cost_usd DOUBLE PRECISION,
    latency_ms INT,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.decisions (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT REFERENCES {s}.engine_cycles(id),
    prediction_id BIGINT REFERENCES {s}.predictions(id),
    strategy TEXT NOT NULL,             -- spread_gate | fundamental
    ticker TEXT NOT NULL,
    side TEXT,                          -- yes | no
    action TEXT NOT NULL,               -- bet | skip
    skip_reason TEXT,
    p_model DOUBLE PRECISION,
    p_market DOUBLE PRECISION,
    edge DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    fee_per_contract DOUBLE PRECISION,
    ev_per_contract DOUBLE PRECISION,   -- edge - costs, dollars
    price_cents INT,
    contracts INT,
    stake_usd DOUBLE PRECISION,
    detail JSONB
);

CREATE TABLE IF NOT EXISTS {s}.orders (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    decision_id BIGINT REFERENCES {s}.decisions(id),
    strategy TEXT NOT NULL,
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    title TEXT,
    category TEXT,
    side TEXT NOT NULL,
    action TEXT NOT NULL DEFAULT 'buy',
    count INT NOT NULL,
    limit_price_cents INT,
    kalshi_order_id TEXT UNIQUE,
    client_order_id TEXT,
    status TEXT NOT NULL DEFAULT 'submitted',  -- submitted|resting|executed|canceled|failed|dry_run
    filled_count INT DEFAULT 0,
    avg_fill_price_cents DOUBLE PRECISION,
    fees_usd DOUBLE PRECISION DEFAULT 0,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    close_time TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    settlement_result TEXT,              -- yes | no | void
    settlement_revenue_usd DOUBLE PRECISION,
    realized_pnl_usd DOUBLE PRECISION,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.fills (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    kalshi_order_id TEXT,
    trade_id TEXT UNIQUE,
    ticker TEXT NOT NULL,
    side TEXT,
    action TEXT,
    count INT,
    price_cents INT,
    is_taker BOOLEAN,
    fee_usd DOUBLE PRECISION,
    filled_ts TIMESTAMPTZ,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.equity_snapshots (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    balance_usd DOUBLE PRECISION NOT NULL,
    portfolio_value_usd DOUBLE PRECISION NOT NULL,
    equity_usd DOUBLE PRECISION NOT NULL,
    open_positions INT,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.position_snapshots (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    ticker TEXT NOT NULL,
    position INT,                        -- +yes / -no contracts
    market_exposure_usd DOUBLE PRECISION,
    realized_pnl_usd DOUBLE PRECISION,
    fees_paid_usd DOUBLE PRECISION,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT,
    title TEXT,
    category TEXT,
    close_time TIMESTAMPTZ,
    status TEXT,
    yes_bid INT, yes_ask INT, no_bid INT, no_ask INT,
    last_price INT,
    volume_24h DOUBLE PRECISION,
    liquidity DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.settlements (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    ticker TEXT NOT NULL,
    market_result TEXT,
    yes_count INT, no_count INT,
    revenue_usd DOUBLE PRECISION,
    settled_ts TIMESTAMPTZ,
    raw JSONB,
    UNIQUE (instance, ticker, settled_ts)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_decisions_dedupe
    ON {s}.decisions (instance, strategy, prediction_id, ticker);
CREATE TABLE IF NOT EXISTS {s}.book_snapshots (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT,
    ticker TEXT NOT NULL,
    status TEXT,
    close_time TIMESTAMPTZ,
    yes_bid INT, yes_ask INT, no_bid INT, no_ask INT, last_price INT,
    volume_24h DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    liquidity DOUBLE PRECISION,
    orderbook JSONB,                 -- raw venue book (bid ladders per side)
    yes_asks JSONB, no_asks JSONB,   -- derived executable ask ladders decisions priced from
    raw_market JSONB                 -- full market payload at decision time
);

ALTER TABLE {s}.decisions ADD COLUMN IF NOT EXISTS book_snapshot_id BIGINT;

-- trade-time capture: 'decision' snapshots are the book decisions priced
-- from; 'post_trade' snapshots record book + market + public tape right
-- after each order submit, linked to the order row.
ALTER TABLE {s}.book_snapshots ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'decision';
ALTER TABLE {s}.book_snapshots ADD COLUMN IF NOT EXISTS order_id BIGINT;
ALTER TABLE {s}.book_snapshots ADD COLUMN IF NOT EXISTS trades JSONB;

CREATE INDEX IF NOT EXISTS idx_book_snapshots_order ON {s}.book_snapshots (order_id) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_book_snapshots_ticker ON {s}.book_snapshots (ticker, created_at);
CREATE INDEX IF NOT EXISTS idx_predictions_ticker ON {s}.predictions (ticker, created_at);
CREATE INDEX IF NOT EXISTS idx_decisions_strategy ON {s}.decisions (strategy, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_strategy ON {s}.orders (strategy, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status ON {s}.orders (status);
CREATE INDEX IF NOT EXISTS idx_fills_ticker ON {s}.fills (ticker, filled_ts);
CREATE INDEX IF NOT EXISTS idx_equity_time ON {s}.equity_snapshots (instance, created_at);
"""


# Read-only collection: Polymarket in `pm_*`, Kalshi in `kx_*`, one shared
# `collect_cycles`. Nothing here is on the trading path — the engine reads
# none of it, so a collector change can never affect an order.
#
# Both venues store prices as DOUBLE PRECISION decimals in [0, 1] and sizes
# as fractional (Polymarket settles USDC; Kalshi's `_fp` shapes are
# fractional too). That is deliberately NOT the cents-int convention the
# trading tables use, so the two are never accidentally joined.
COLLECT_DDL = """
CREATE TABLE IF NOT EXISTS {s}.collect_cycles (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    instance TEXT NOT NULL,
    venues TEXT,                        -- 'polymarket', 'kalshi', or both
    events_seen INT DEFAULT 0,
    markets_seen INT DEFAULT 0,
    books_captured INT DEFAULT 0,
    trades_ingested INT DEFAULT 0,
    history_points INT DEFAULT 0,
    predictions_ingested INT DEFAULT 0,
    unmapped INT DEFAULT 0,
    error TEXT,
    detail JSONB                        -- per-venue breakdown and timings
);

CREATE TABLE IF NOT EXISTS {s}.pm_events (
    slug TEXT PRIMARY KEY,
    pm_event_id TEXT,
    arena_event_ticker TEXT,           -- 'PM-<slug>' when arena tracks it
    title TEXT,
    arena_category TEXT,
    description TEXT,
    rules TEXT,
    neg_risk BOOLEAN,
    closed BOOLEAN,
    active BOOLEAN,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    arena_close_time TIMESTAMPTZ,
    liquidity DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.pm_markets (
    condition_id TEXT PRIMARY KEY,
    pm_market_id TEXT,
    event_slug TEXT,
    arena_event_ticker TEXT,
    arena_market_title TEXT,           -- the arena outcome this market IS
    question TEXT,
    market_slug TEXT,
    group_item_title TEXT,
    outcomes JSONB,                    -- ["Yes", "No"] / ["Mayweather", ...]
    token_ids JSONB,                   -- CLOB token per outcome, same order
    yes_token_id TEXT,                 -- token_ids[0]: pays $1 if this outcome
    no_token_id TEXT,
    tick_size DOUBLE PRECISION,        -- varies per market, even within an event
    min_order_size DOUBLE PRECISION,
    neg_risk BOOLEAN,
    maker_base_fee DOUBLE PRECISION,
    taker_base_fee DOUBLE PRECISION,
    fees_enabled BOOLEAN,
    fee_type TEXT,                     -- tech_fees | culture_fees | sports_fees_v2 | ...
    fee_schedule JSONB,                -- exponent, rate, takerOnly, rebateRate
    enable_order_book BOOLEAN,
    accepting_orders BOOLEAN,
    closed BOOLEAN,
    active BOOLEAN,
    end_date TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw JSONB
);

-- Point-in-time Gamma payload per market: prices, volume, liquidity, flags.
CREATE TABLE IF NOT EXISTS {s}.pm_market_snapshots (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT,
    event_slug TEXT,
    arena_event_ticker TEXT,
    condition_id TEXT NOT NULL,
    arena_market_title TEXT,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    last_trade_price DOUBLE PRECISION,
    outcome_prices JSONB,
    volume DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    liquidity DOUBLE PRECISION,
    one_day_price_change DOUBLE PRECISION,
    fees_enabled BOOLEAN,
    fee_type TEXT,
    fee_schedule JSONB,                -- captured per snapshot: rates change
    accepting_orders BOOLEAN,
    closed BOOLEAN,
    active BOOLEAN
    -- No raw payload here: at 3.9 KB/row it cost more than every other
    -- table's growth combined. pm_markets.raw keeps the full Gamma payload
    -- for current state; what is dropped is only a per-cycle HISTORY of
    -- near-static metadata (tags, clob rewards, uma bond).
);

-- Full-depth book per TOKEN. A market has two tokens and two independent
-- books; the NO book is not the complement of the YES book.
CREATE TABLE IF NOT EXISTS {s}.pm_book_snapshots (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT,
    event_slug TEXT,
    arena_event_ticker TEXT,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT,
    outcome_index INT,
    arena_market_title TEXT,
    book_hash TEXT,                    -- venue hash: equal hash == identical book
    book_ts TIMESTAMPTZ,               -- venue-side timestamp
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    midpoint DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    last_trade_price DOUBLE PRECISION,
    bid_levels INT, ask_levels INT,
    bid_depth DOUBLE PRECISION,        -- total resting size, all levels
    ask_depth DOUBLE PRECISION,
    -- Derived microstructure (see book_metrics.py). Recomputable from the
    -- ladders, stored because doing so later across hundreds of millions of
    -- rows costs far more than 32 bytes now. Each Polymarket token has its
    -- OWN book, so obi(YES) is not -obi(NO) the way it is on Kalshi.
    obi DOUBLE PRECISION,              -- (bid-ask)/(bid+ask), all levels
    obi_1 DOUBLE PRECISION,            -- top of book only
    obi_5 DOUBLE PRECISION,            -- best 5 levels
    microprice DOUBLE PRECISION,       -- size-weighted top-of-book price
    tick_size DOUBLE PRECISION,
    min_order_size DOUBLE PRECISION,
    neg_risk BOOLEAN,
    source TEXT NOT NULL DEFAULT 'poll',   -- poll | stream
    bids JSONB,                        -- [[price, size], ...] best first
    asks JSONB
    -- No raw payload: the CLOB /book response is bids, asks, hash,
    -- last_trade_price, tick_size, min_order_size, neg_risk and timestamp,
    -- every one of which is already a typed column above. Storing it was
    -- pure duplication at 551 bytes/row.
);

-- Public trade tape. The tape carries no trade id and one transaction hash
-- can cover several fills, so dedupe is on a content hash.
CREATE TABLE IF NOT EXISTS {s}.pm_trades (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    trade_key TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT,
    event_slug TEXT,
    arena_event_ticker TEXT,
    side TEXT,                         -- BUY | SELL (taker side)
    outcome TEXT,
    outcome_index INT,
    price DOUBLE PRECISION,
    size DOUBLE PRECISION,
    notional_usd DOUBLE PRECISION,
    traded_at TIMESTAMPTZ,
    proxy_wallet TEXT,
    transaction_hash TEXT,
    source TEXT NOT NULL DEFAULT 'poll',
    raw JSONB
    -- Uniqueness is on trade_key alone, created below: a trade is venue
    -- truth, so `instance` is provenance and must not partition it.
);

-- 1-minute mid-price series per token (CLOB /prices-history).
CREATE TABLE IF NOT EXISTS {s}.pm_price_history (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    token_id TEXT NOT NULL,
    condition_id TEXT,
    event_slug TEXT,
    ts TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    UNIQUE (token_id, ts)
);

-- Arena forecasts on Polymarket events, kept OUT of {s}.predictions on
-- purpose: a row there from a bet predictor becomes a Kalshi trading
-- candidate, and these tickers are not Kalshi tickers.
CREATE TABLE IF NOT EXISTS {s}.pm_predictions (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT,
    arena_prediction_id TEXT NOT NULL,
    predictor_name TEXT NOT NULL,
    harness TEXT,
    arena_event_ticker TEXT NOT NULL,
    event_slug TEXT,
    event_title TEXT,
    category TEXT,
    close_time TIMESTAMPTZ,
    arena_market_title TEXT NOT NULL,
    condition_id TEXT,                 -- resolved mapping, NULL if unmapped
    token_id TEXT,
    p_model DOUBLE PRECISION NOT NULL,
    reasoning TEXT,
    predicted_at TIMESTAMPTZ,
    external_id TEXT NOT NULL,
    raw JSONB,
    UNIQUE (instance, external_id)
);

-- Every arena outcome we could NOT map to a token, with the candidates we
-- saw. The Kalshi path drops unmapped outcomes silently; this one refuses to.
CREATE TABLE IF NOT EXISTS {s}.pm_unmapped (
    id BIGSERIAL PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    seen_count INT NOT NULL DEFAULT 1,
    instance TEXT NOT NULL,
    arena_event_ticker TEXT NOT NULL,
    event_slug TEXT,
    arena_market_title TEXT NOT NULL,
    reason TEXT,
    candidates JSONB,
    UNIQUE (instance, arena_event_ticker, arena_market_title)
);

-- ------------------------------------------------------------------ kalshi
-- Unlike Polymarket, a Kalshi market is ONE book with a bid ladder per side;
-- the executable YES ask ladder is the mirror of the NO bids. So there is one
-- row per ticker, not one per token.
CREATE TABLE IF NOT EXISTS {s}.kx_markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT,
    arena_event_ticker TEXT,
    arena_market_title TEXT,
    series_ticker TEXT,
    title TEXT,
    subtitle TEXT,
    yes_sub_title TEXT,
    market_type TEXT,
    strike_type TEXT,
    status TEXT,
    open_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    expiration_time TIMESTAMPTZ,
    expected_expiration_time TIMESTAMPTZ,
    can_close_early BOOLEAN,
    result TEXT,
    rules_primary TEXT,
    notional_value DOUBLE PRECISION,
    volume DOUBLE PRECISION,           -- diffed each cycle to find markets
    volume_24h DOUBLE PRECISION,       -- that actually traded, so the tape is
    open_interest DOUBLE PRECISION,    -- pulled for those only
    liquidity DOUBLE PRECISION,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw JSONB
);

CREATE TABLE IF NOT EXISTS {s}.kx_book_snapshots (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    cycle_id BIGINT,
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    arena_event_ticker TEXT,
    arena_market_title TEXT,
    status TEXT,
    close_time TIMESTAMPTZ,
    book_hash TEXT,                    -- self-computed: Kalshi issues none
    yes_bid DOUBLE PRECISION, yes_ask DOUBLE PRECISION,
    no_bid DOUBLE PRECISION, no_ask DOUBLE PRECISION,
    last_price DOUBLE PRECISION,
    midpoint DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    -- YES-contract perspective. On Kalshi both sides are one book:
    -- yes_ask_depth IS no_bid_depth by construction, so the NO contract's
    -- imbalance is exactly -obi and is not stored twice.
    obi DOUBLE PRECISION,
    obi_1 DOUBLE PRECISION,
    obi_5 DOUBLE PRECISION,
    microprice DOUBLE PRECISION,
    bid_depth DOUBLE PRECISION, ask_depth DOUBLE PRECISION,
    bid_levels INT, ask_levels INT,
    open_interest DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    liquidity DOUBLE PRECISION,
    source TEXT NOT NULL DEFAULT 'poll',   -- poll | stream
    yes_bids JSONB, no_bids JSONB,     -- raw venue bid ladders, best first
    yes_asks JSONB, no_asks JSONB      -- derived executable ask ladders
    -- No raw payload: every field of /orderbook is in the four ladders, and
    -- the market payload lives in kx_markets.raw as current state.
);

CREATE TABLE IF NOT EXISTS {s}.kx_trades (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance TEXT NOT NULL,
    trade_id TEXT NOT NULL,            -- kalshi issues a real id, unlike PM
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    taker_side TEXT,                   -- yes | no
    taker_book_side TEXT,              -- bid | ask
    is_block_trade BOOLEAN,
    count DOUBLE PRECISION,
    yes_price DOUBLE PRECISION,
    no_price DOUBLE PRECISION,
    notional_usd DOUBLE PRECISION,
    traded_at TIMESTAMPTZ,
    source TEXT NOT NULL DEFAULT 'poll',
    raw JSONB
    -- Uniqueness is on trade_id alone, created below (see pm_trades).
);

-- Arena outcomes on Kalshi events that carry no ticker mapping. The trading
-- path skips these; recording them here makes the gap countable.
CREATE TABLE IF NOT EXISTS {s}.kx_unmapped (
    id BIGSERIAL PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    seen_count INT NOT NULL DEFAULT 1,
    instance TEXT NOT NULL,
    arena_event_ticker TEXT NOT NULL,
    arena_market_title TEXT NOT NULL,
    reason TEXT,
    UNIQUE (instance, arena_event_ticker, arena_market_title)
);

CREATE INDEX IF NOT EXISTS idx_kx_book_ticker ON {s}.kx_book_snapshots (ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_kx_book_cycle ON {s}.kx_book_snapshots (cycle_id);
CREATE INDEX IF NOT EXISTS idx_kx_trades_ticker ON {s}.kx_trades (ticker, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_kx_markets_event ON {s}.kx_markets (event_ticker);

-- A trade is a fact about the venue, so storing it once per collector run
-- was pure duplication: two runs over the same market produced two rows for
-- the same fill (50% of pm_trades at the time this landed). `instance` stays
-- as provenance but leaves the unique key, and the tape watermarks go global
-- to match -- otherwise a collector refetches trades that already exist
-- under another instance label, every cycle, forever.
--
-- Guarded on the index existing so the dedupe runs once, not on every start.
-- Book snapshots are deliberately untouched: multiple rows per (market, time)
-- are the entire point of a time series.
DO $do$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'ux_pm_trades_key' AND relnamespace = '{s}'::regnamespace
  ) THEN
    DELETE FROM {s}.pm_trades t USING {s}.pm_trades keep
      WHERE t.trade_key = keep.trade_key AND t.id > keep.id;
    ALTER TABLE {s}.pm_trades DROP CONSTRAINT IF EXISTS pm_trades_instance_trade_key_key;
    ALTER TABLE {s}.pm_trades DROP CONSTRAINT IF EXISTS pm_trades_trade_key_key;
    CREATE UNIQUE INDEX ux_pm_trades_key ON {s}.pm_trades (trade_key);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'ux_kx_trades_id' AND relnamespace = '{s}'::regnamespace
  ) THEN
    DELETE FROM {s}.kx_trades t USING {s}.kx_trades keep
      WHERE t.trade_id = keep.trade_id AND t.id > keep.id;
    ALTER TABLE {s}.kx_trades DROP CONSTRAINT IF EXISTS kx_trades_instance_trade_id_key;
    ALTER TABLE {s}.kx_trades DROP CONSTRAINT IF EXISTS kx_trades_trade_id_key;
    CREATE UNIQUE INDEX ux_kx_trades_id ON {s}.kx_trades (trade_id);
  END IF;
END
$do$;

CREATE INDEX IF NOT EXISTS idx_pm_book_token ON {s}.pm_book_snapshots (token_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_book_cond ON {s}.pm_book_snapshots (condition_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_book_cycle ON {s}.pm_book_snapshots (cycle_id);
CREATE INDEX IF NOT EXISTS idx_pm_mktsnap_cond ON {s}.pm_market_snapshots (condition_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_trades_cond ON {s}.pm_trades (condition_id, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_trades_token ON {s}.pm_trades (token_id, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_hist_token ON {s}.pm_price_history (token_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_pm_markets_event ON {s}.pm_markets (event_slug);
CREATE INDEX IF NOT EXISTS idx_pm_pred_event ON {s}.pm_predictions (arena_event_ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_pred_predictor ON {s}.pm_predictions (predictor_name, created_at DESC);
"""



class Store:
    def __init__(self, database_url: str, schema: str = "betting_experiments"):
        if not database_url:
            raise RuntimeError("DATABASE_URL is not set")
        self.url = database_url
        self.schema = schema

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.url, connect_timeout=20, row_factory=dict_row) as c:
            yield c

    def migrate(self, collectors: bool = True) -> None:
        with self.conn() as c:
            c.execute(DDL.format(s=self.schema))
            if collectors:
                c.execute(COLLECT_DDL.format(s=self.schema))
            c.commit()
        log.info("migrated schema %s", self.schema)

    # --- generic helpers ---
    def _insert(self, table: str, row: dict[str, Any]) -> int | None:
        cols = list(row.keys())
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (
            f"INSERT INTO {self.schema}.{table} ({', '.join(cols)}) "
            f"VALUES ({placeholders}) RETURNING id"
        )
        with self.conn() as c:
            cur = c.execute(sql, vals)
            rid = cur.fetchone()
            c.commit()
            return rid["id"] if rid else None

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self.conn() as c:
            return c.execute(sql.format(s=self.schema), params).fetchall()

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self.conn() as c:
            c.execute(sql.format(s=self.schema), params)
            c.commit()

    # --- typed inserts ---
    def start_cycle(self, instance: str, mode: str) -> int:
        return self._insert("engine_cycles", {"instance": instance, "mode": mode})  # type: ignore[return-value]

    def finish_cycle(self, cycle_id: int, **fields: Any) -> None:
        sets = ", ".join(f"{k} = %s" for k in fields)
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in fields.values()]
        self.execute(
            f"UPDATE {{s}}.engine_cycles SET finished_at = now(), {sets} WHERE id = %s",
            (*vals, cycle_id),
        )

    def insert_prediction(self, **row: Any) -> int | None:
        try:
            return self._insert("predictions", row)
        except psycopg.errors.UniqueViolation:
            return None  # already ingested (arena dedupe)

    def insert_predictions_bulk(self, rows: list[dict[str, Any]], chunk: int = 500) -> dict[str, int]:
        """Insert many prediction rows (chunked to stay under the wire param
        limit); returns {external_id: id} for the rows that were actually new."""
        if not rows:
            return {}
        cols = list(rows[0].keys())
        out: dict[str, int] = {}
        with self.conn() as c:
            for i in range(0, len(rows), chunk):
                batch = rows[i:i + chunk]
                values_sql = ", ".join(
                    "(" + ", ".join(["%s"] * len(cols)) + ")" for _ in batch
                )
                params: list[Any] = []
                for r in batch:
                    params.extend(json.dumps(v) if isinstance(v, (dict, list)) else v for v in (r[c2] for c2 in cols))
                sql = (
                    f"INSERT INTO {self.schema}.predictions ({', '.join(cols)}) VALUES {values_sql} "
                    f"ON CONFLICT (instance, source, external_id) DO NOTHING RETURNING external_id, id"
                )
                out.update({r["external_id"]: r["id"] for r in c.execute(sql, params).fetchall()})
            c.commit()
        return out

    def insert_book_snapshot(self, **row: Any) -> int | None:
        return self._insert("book_snapshots", row)

    def unprocessed_predictions(self, instance: str, predictors: list[str], lookback_hours: float) -> list[dict]:
        """Recent bet-predictor predictions with their already-decided lanes —
        the engine re-derives candidates from this instead of trusting the
        in-memory insert map, so a crash between prediction-ingest and
        decision never silently drops a forecast."""
        preds = self.query(
            "SELECT p.id, p.model_spec, p.ticker, p.event_ticker, p.title, p.category, "
            "p.close_time, p.p_model, p.reasoning, p.raw->>'predicted_at' AS predicted_at, "
            "p.raw->>'market_title' AS market_title, p.external_id "
            "FROM {s}.predictions p WHERE p.instance = %s AND p.source = 'arena' "
            "AND p.model_spec = ANY(%s) AND p.created_at > now() - (interval '1 hour' * %s)",
            (instance, predictors, lookback_hours),
        )
        if not preds:
            return []
        ids = [p["id"] for p in preds]
        decided = self.query(
            "SELECT prediction_id, strategy FROM {s}.decisions WHERE instance = %s AND prediction_id = ANY(%s)",
            (instance, ids),
        )
        by_pred: dict[int, set[str]] = {}
        for d in decided:
            by_pred.setdefault(d["prediction_id"], set()).add(d["strategy"])
        for p in preds:
            p["decided_lanes"] = by_pred.get(p["id"], set())
        return preds

    def insert_llm_call(self, **row: Any) -> int | None:
        return self._insert("llm_calls", row)

    def insert_decision(self, **row: Any) -> int | None:
        return self._insert("decisions", row)

    def insert_order(self, **row: Any) -> int | None:
        return self._insert("orders", row)

    def upsert_fill(self, **row: Any) -> None:
        try:
            self._insert("fills", row)
        except psycopg.errors.UniqueViolation:
            pass

    def snapshot_equity(self, **row: Any) -> None:
        self._insert("equity_snapshots", row)

    def snapshot_position(self, **row: Any) -> None:
        self._insert("position_snapshots", row)

    def upsert_market(self, m: dict[str, Any]) -> None:
        cols = list(m.keys())
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in m.values()]
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "ticker")
        self.execute(
            f"INSERT INTO {{s}}.markets ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT (ticker) DO UPDATE SET {updates}, updated_at = now()",
            tuple(vals),
        )

    def upsert_settlement(self, **row: Any) -> None:
        try:
            self._insert("settlements", row)
        except psycopg.errors.UniqueViolation:
            pass

    # --- ledger fold (lane ledger fold, conservative by construction) ---
    def lane_orders(self, instance: str, strategy: str) -> list[dict]:
        return self.query(
            "SELECT id, ticker, side, status, count, filled_count, limit_price_cents, "
            "avg_fill_price_cents, fees_usd, dry_run, settled_at, settlement_revenue_usd, raw "
            "FROM {s}.orders WHERE instance = %s AND strategy = %s AND status <> 'failed'",
            (instance, strategy),
        )

    def lane_ledger(self, instance: str, strategy: str, starting: float, include_dry_run: bool = False) -> dict:
        """free_cash = starting - reserved - spent + payouts + netting_credit.

        reserved: unfilled portion of open (non-terminal) orders at limit + fee reserve
        spent:    every filled contract at actual fill price + actual fees
        payouts:  settlement revenue on settled orders
        netting:  min(open_yes, open_no) x $1 per ticker over unsettled fills
                  (offsetting pairs return $1 regardless of outcome; the venue
                  frees that collateral immediately)
        """
        from .fees import taker_fee
        orders = self.lane_orders(instance, strategy)
        reserved = spent = payouts = 0.0
        open_by_ticker: dict[str, dict[str, int]] = {}
        for o in orders:
            if o["dry_run"] and not include_dry_run:
                continue
            filled = o["filled_count"] or 0
            limit_px = (o["limit_price_cents"] or 0) / 100.0
            fill_px = (o["avg_fill_price_cents"] or o["limit_price_cents"] or 0) / 100.0
            fees = o["fees_usd"] or 0.0
            if o["status"] in ("submitted", "resting", "pending"):
                unfilled = max((o["count"] or 0) - filled, 0)
                reserved += unfilled * limit_px + (taker_fee(o["ticker"], unfilled, limit_px) if unfilled else 0.0)
            if filled > 0:
                spent += filled * fill_px + fees
                if o["settled_at"] is None:
                    t = open_by_ticker.setdefault(o["ticker"], {"yes": 0, "no": 0})
                    t[o["side"]] += filled
            if o["settled_at"] is not None:
                payouts += o["settlement_revenue_usd"] or 0.0
        netting = sum(min(t["yes"], t["no"]) * 1.0 for t in open_by_ticker.values())
        free_cash = starting - reserved - spent + payouts + netting
        return {
            "free_cash": round(free_cash, 4),
            "reserved": round(reserved, 4),
            "spent": round(spent, 4),
            "payouts": round(payouts, 4),
            "netting": round(netting, 4),
        }

    def lane_positions_all(self, instance: str, strategy: str, include_dry_run: bool = False) -> dict[str, int]:
        """Signed YES-equivalent contracts per ticker (unsettled fills only;
        Kalshi nets YES vs NO at the account level)."""
        rows = self.query(
            "SELECT ticker, side, COALESCE(SUM(filled_count), 0) AS n FROM {s}.orders "
            "WHERE instance = %s AND strategy = %s AND settled_at IS NULL "
            "AND status <> 'failed' AND (dry_run = %s OR dry_run = FALSE) GROUP BY ticker, side",
            (instance, strategy, include_dry_run),
        )
        pos: dict[str, int] = {}
        for r in rows:
            pos[r["ticker"]] = pos.get(r["ticker"], 0) + (int(r["n"]) if r["side"] == "yes" else -int(r["n"]))
        return pos

    def prediction_watermark(self, instance: str, source: str) -> Any:
        rows = self.query(
            "SELECT max((raw->>'predicted_at')::timestamptz) AS m FROM {s}.predictions "
            "WHERE instance = %s AND source = %s",
            (instance, source),
        )
        return rows[0]["m"] if rows else None

    # ------------------------------------------------------------------
    # Polymarket collection (pm_* tables). Read-only ingest — none of these
    # are consulted by the engine, so they cannot influence a Kalshi order.
    # ------------------------------------------------------------------
    def _insert_many(self, table: str, rows: list[dict[str, Any]], *,
                     conflict: str = "", chunk: int = 500) -> int:
        """Bulk insert with an optional full ON CONFLICT clause. Rows need not
        share keys — the column set comes from the first row and missing keys
        insert as NULL. Returns the number of rows actually written."""
        if not rows:
            return 0
        cols = list(rows[0].keys())
        written = 0
        with self.conn() as c:
            for i in range(0, len(rows), chunk):
                batch = rows[i:i + chunk]
                values_sql = ", ".join("(" + ", ".join(["%s"] * len(cols)) + ")" for _ in batch)
                params: list[Any] = []
                for r in batch:
                    params.extend(
                        json.dumps(v) if isinstance(v, (dict, list)) else v
                        for v in (r.get(k) for k in cols)
                    )
                # RETURNING 1, not RETURNING id: the upsert tables are keyed
                # on a natural primary key (slug / condition_id / ticker) and
                # have no id column at all. Counting returned rows still gives
                # rows-actually-written, since ON CONFLICT DO NOTHING returns
                # only the inserts.
                sql = (
                    f"INSERT INTO {self.schema}.{table} ({', '.join(cols)}) "
                    f"VALUES {values_sql} {conflict} RETURNING 1"
                )
                written += len(c.execute(sql, params).fetchall())
            c.commit()
        return written

    def _upsert_many(self, table: str, rows: list[dict[str, Any]], key: str,
                     chunk: int = 500) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != key and c != "first_seen_at")
        conflict = f"ON CONFLICT ({key}) DO UPDATE SET {sets}, updated_at = now()"
        return self._insert_many(table, rows, conflict=conflict, chunk=chunk)

    def fail_stale_collect_cycles(self, instance: str) -> int:
        """Close out cycles orphaned by a restart.

        A row with finished_at NULL is indistinguishable from one genuinely
        in flight, and every deploy that lands mid-cycle leaves one behind.
        Called at startup, when by definition nothing of ours is running."""
        with self.conn() as c:
            cur = c.execute(
                f"UPDATE {self.schema}.collect_cycles SET finished_at = now(), "
                "error = COALESCE(error, 'abandoned: collector restarted') "
                "WHERE instance = %s AND finished_at IS NULL RETURNING id",
                (instance,),
            )
            n = len(cur.fetchall())
            c.commit()
        return n

    def start_collect_cycle(self, instance: str, venues: str) -> int:
        return self._insert(  # type: ignore[return-value]
            "collect_cycles", {"instance": instance, "venues": venues}
        )

    def finish_collect_cycle(self, cycle_id: int, **fields: Any) -> None:
        sets = ", ".join(f"{k} = %s" for k in fields)
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in fields.values()]
        self.execute(
            f"UPDATE {{s}}.collect_cycles SET finished_at = now(), {sets} WHERE id = %s",
            (*vals, cycle_id),
        )

    def upsert_pm_events(self, rows: list[dict[str, Any]]) -> int:
        return self._upsert_many("pm_events", rows, "slug")

    def upsert_pm_markets(self, rows: list[dict[str, Any]]) -> int:
        return self._upsert_many("pm_markets", rows, "condition_id")

    def insert_pm_market_snapshots(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many("pm_market_snapshots", rows)

    def insert_pm_book_snapshots(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many("pm_book_snapshots", rows)

    def insert_pm_trades(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many(
            "pm_trades", rows, conflict="ON CONFLICT (trade_key) DO NOTHING"
        )

    def insert_pm_price_history(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many(
            "pm_price_history", rows, conflict="ON CONFLICT (token_id, ts) DO NOTHING"
        )

    def insert_pm_predictions(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many(
            "pm_predictions", rows, conflict="ON CONFLICT (instance, external_id) DO NOTHING"
        )

    def record_pm_unmapped(self, rows: list[dict[str, Any]]) -> int:
        """Bump-on-conflict so a persistently unmappable outcome shows up as a
        rising seen_count rather than a wall of duplicate rows."""
        return self._insert_many(
            "pm_unmapped", rows,
            conflict=(
                "ON CONFLICT (instance, arena_event_ticker, arena_market_title) DO UPDATE SET "
                "last_seen_at = now(), seen_count = {s}.pm_unmapped.seen_count + 1, "
                "reason = EXCLUDED.reason, candidates = EXCLUDED.candidates, "
                "event_slug = EXCLUDED.event_slug"
            ).format(s=self.schema),
        )

    def pm_trade_watermarks(self) -> dict[str, int]:
        """{condition_id: latest trade epoch seconds} in one round trip —
        per-market watermark queries would dominate the cycle.

        Not scoped to an instance: trades are unique venue-wide, so a
        per-instance watermark would refetch trades that already exist under
        another label and silently discard them on conflict."""
        rows = self.query(
            "SELECT condition_id, EXTRACT(EPOCH FROM max(traded_at))::bigint AS ts "
            "FROM {s}.pm_trades GROUP BY condition_id"
        )
        return {r["condition_id"]: int(r["ts"]) for r in rows if r["ts"] is not None}

    def pm_history_watermarks(self) -> dict[str, int]:
        rows = self.query(
            "SELECT token_id, EXTRACT(EPOCH FROM max(ts))::bigint AS ts "
            "FROM {s}.pm_price_history GROUP BY token_id"
        )
        return {r["token_id"]: int(r["ts"]) for r in rows if r["ts"] is not None}

    def pm_prediction_watermark(self, instance: str) -> Any:
        rows = self.query(
            "SELECT max(predicted_at) AS m FROM {s}.pm_predictions WHERE instance = %s",
            (instance,),
        )
        return rows[0]["m"] if rows else None

    def pm_last_book_hashes(self, instance: str) -> dict[str, str]:
        """Latest book hash per token, for optional unchanged-book skipping."""
        rows = self.query(
            "SELECT DISTINCT ON (token_id) token_id, book_hash FROM {s}.pm_book_snapshots "
            "WHERE instance = %s ORDER BY token_id, created_at DESC",
            (instance,),
        )
        return {r["token_id"]: r["book_hash"] for r in rows if r["book_hash"]}

    # --- kalshi collection (kx_* tables; same isolation as pm_*) ---
    def upsert_kx_markets(self, rows: list[dict[str, Any]]) -> int:
        return self._upsert_many("kx_markets", rows, "ticker")

    def insert_kx_book_snapshots(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many("kx_book_snapshots", rows)

    def insert_kx_trades(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many(
            "kx_trades", rows, conflict="ON CONFLICT (trade_id) DO NOTHING"
        )

    def kx_trade_watermarks(self) -> dict[str, int]:
        """Global, for the same reason as pm_trade_watermarks."""
        rows = self.query(
            "SELECT ticker, EXTRACT(EPOCH FROM max(traded_at))::bigint AS ts "
            "FROM {s}.kx_trades GROUP BY ticker"
        )
        return {r["ticker"]: int(r["ts"]) for r in rows if r["ts"] is not None}

    def record_kx_unmapped(self, rows: list[dict[str, Any]]) -> int:
        return self._insert_many(
            "kx_unmapped", rows,
            conflict=(
                "ON CONFLICT (instance, arena_event_ticker, arena_market_title) DO UPDATE SET "
                "last_seen_at = now(), seen_count = {s}.kx_unmapped.seen_count + 1, "
                "reason = EXCLUDED.reason"
            ).format(s=self.schema),
        )

    def kx_market_volumes(self) -> dict[str, float]:
        """Last-seen total volume per ticker. Read BEFORE the upsert each
        cycle: a ticker whose volume has not moved cannot have traded, which
        is what keeps the per-ticker tape sweep affordable."""
        rows = self.query(
            "SELECT ticker, volume FROM {s}.kx_markets WHERE volume IS NOT NULL"
        )
        return {r["ticker"]: float(r["volume"]) for r in rows}

    def kx_last_book_hashes(self, instance: str) -> dict[str, str]:
        rows = self.query(
            "SELECT DISTINCT ON (ticker) ticker, book_hash FROM {s}.kx_book_snapshots "
            "WHERE instance = %s ORDER BY ticker, created_at DESC",
            (instance,),
        )
        return {r["ticker"]: r["book_hash"] for r in rows if r["book_hash"]}
