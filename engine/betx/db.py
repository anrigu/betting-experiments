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

CREATE INDEX IF NOT EXISTS idx_book_snapshots_ticker ON {s}.book_snapshots (ticker, created_at);
CREATE INDEX IF NOT EXISTS idx_predictions_ticker ON {s}.predictions (ticker, created_at);
CREATE INDEX IF NOT EXISTS idx_decisions_strategy ON {s}.decisions (strategy, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_strategy ON {s}.orders (strategy, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status ON {s}.orders (status);
CREATE INDEX IF NOT EXISTS idx_fills_ticker ON {s}.fills (ticker, filled_ts);
CREATE INDEX IF NOT EXISTS idx_equity_time ON {s}.equity_snapshots (instance, created_at);
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

    def migrate(self) -> None:
        with self.conn() as c:
            c.execute(DDL.format(s=self.schema))
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
