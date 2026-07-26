"""ProphetArena prediction source.

Reads predictions directly from the ProphetArena production Postgres
(`ARENA_DATABASE_URL`) — the same data the arena leaderboard is built from —
and maps each outcome onto its Kalshi ticker via the arena `market` table
(`other_info->>'ticker'`, prices stored in cents).

We bet on `agent-gemini-3.1-pro` (the agentic web-search harness: paired
head-to-head Brier on common resolved events beats the fixed-context
variant) and also ingest reference predictors for dashboard comparison.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)


@dataclass
class ArenaMarketPrediction:
    """One (prediction, outcome) pair mapped to a Kalshi market."""
    arena_prediction_id: str
    predictor_name: str
    event_ticker: str
    event_title: str
    category: str | None
    close_time: datetime | None
    market_title: str
    ticker: str                 # Kalshi ticker
    p_model: float
    predicted_at: datetime
    # arena's last market snapshot (cents) — informational; execution uses live quotes
    snap_yes_ask: float | None = None
    snap_no_ask: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


class ArenaDB:
    def __init__(self, url: str):
        if not url:
            raise RuntimeError("ARENA_DATABASE_URL is not set")
        self.url = url

    def _conn(self):
        return psycopg.connect(self.url, connect_timeout=20, row_factory=dict_row)

    def fetch_new_predictions(
        self,
        predictor_names: list[str],
        since: datetime | None,
        limit: int = 500,
    ) -> list[ArenaMarketPrediction]:
        """Fetch predictions created after `since`, expanded per-outcome and
        mapped to Kalshi tickers. Events with no ticker mapping are skipped."""
        sql = """
        SELECT p.id::text AS pid, p.predictor_name, p.event_ticker, p.prediction,
               p.created_at, e.title AS event_title, e.category, e.close_time,
               e.market_outcome
        FROM prediction p
        JOIN event e ON e.event_ticker = p.event_ticker
        WHERE p.predictor_name = ANY(%s)
          AND (%s::timestamptz IS NULL OR p.created_at > %s)
        ORDER BY p.created_at ASC
        LIMIT %s
        """
        out: list[ArenaMarketPrediction] = []
        with self._conn() as c:
            rows = c.execute(sql, (predictor_names, since, since, limit)).fetchall()
            if not rows:
                return out
            event_tickers = sorted({r["event_ticker"] for r in rows})
            tick_map = self._ticker_map(c, event_tickers)
        for r in rows:
            if r["market_outcome"] is not None:
                continue  # already resolved
            pred = r["prediction"] or {}
            probs = pred.get("probabilities") or []
            emap = tick_map.get(r["event_ticker"], {})
            for item in probs:
                title = item.get("market")
                p = item.get("probability")
                if title is None or p is None:
                    continue
                m = emap.get(title)
                if not m or not m.get("ticker"):
                    continue
                out.append(
                    ArenaMarketPrediction(
                        arena_prediction_id=r["pid"],
                        predictor_name=r["predictor_name"],
                        event_ticker=r["event_ticker"],
                        event_title=r["event_title"] or "",
                        category=r["category"],
                        close_time=r["close_time"],
                        market_title=title,
                        ticker=m["ticker"],
                        p_model=float(p),
                        predicted_at=r["created_at"],
                        snap_yes_ask=m.get("yes_ask"),
                        snap_no_ask=m.get("no_ask"),
                        raw={"rationale": (pred.get("rationale") or "")[:2000]},
                    )
                )
        return out

    @staticmethod
    def _ticker_map(conn, event_tickers: list[str]) -> dict[str, dict[str, dict]]:
        """{event_ticker: {market_title: {ticker, yes_ask, no_ask}}} using the
        latest arena market snapshot per (event, title)."""
        sql = """
        SELECT DISTINCT ON (event_ticker, market_title)
               event_ticker, market_title,
               other_info->>'ticker' AS ticker, yes_ask, no_ask
        FROM market
        WHERE event_ticker = ANY(%s)
        ORDER BY event_ticker, market_title, created_at DESC
        """
        out: dict[str, dict[str, dict]] = {}
        for r in conn.execute(sql, (event_tickers,)).fetchall():
            out.setdefault(r["event_ticker"], {})[r["market_title"]] = {
                "ticker": r["ticker"],
                "yes_ask": r["yes_ask"],
                "no_ask": r["no_ask"],
            }
        return out

    def latest_prediction_time(self, predictor_names: list[str]) -> datetime | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT max(created_at) AS m FROM prediction WHERE predictor_name = ANY(%s)",
                (predictor_names,),
            ).fetchone()
            return row["m"] if row else None

    def brier_snapshot(self, predictor_names: list[str], since_days: int = 45) -> list[dict]:
        """Rolling multiclass Brier per predictor on resolved events, for the
        dashboard's harness-comparison panel."""
        sql = """
        WITH latest AS (
          SELECT DISTINCT ON (p.predictor_name, p.event_ticker)
                 p.predictor_name, p.event_ticker, p.prediction
          FROM prediction p
          WHERE p.predictor_name = ANY(%s)
            AND p.created_at > now() - make_interval(days => %s)
          ORDER BY p.predictor_name, p.event_ticker, p.created_at DESC
        )
        SELECT l.predictor_name, l.prediction, e.market_outcome
        FROM latest l JOIN event e ON e.event_ticker = l.event_ticker
        WHERE e.market_outcome IS NOT NULL
        """
        acc: dict[str, list[float]] = {}
        with self._conn() as c:
            for r in c.execute(sql, (predictor_names, since_days)).fetchall():
                probs = (r["prediction"] or {}).get("probabilities") or []
                outcome = r["market_outcome"] or {}
                pm = {d["market"]: d.get("probability") for d in probs if d.get("probability") is not None}
                common = [m for m in pm if m in outcome]
                if not common:
                    continue
                se = sum((pm[m] - float(outcome[m])) ** 2 for m in common) / len(common)
                acc.setdefault(r["predictor_name"], []).append(se)
        return [
            {"predictor": k, "events": len(v), "brier": sum(v) / len(v)}
            for k, v in sorted(acc.items())
        ]
