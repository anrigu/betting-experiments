"""ProphetArena prediction source.

Reads predictions directly from the ProphetArena production Postgres
(`ARENA_DATABASE_URL`) — the same data the arena leaderboard is built from —
and maps each outcome onto its Kalshi ticker via the arena `market` table
(`other_info->>'ticker'`, prices stored in cents).

We bet on `agent-gemini-3.1-pro` (the agentic web-search harness: paired
head-to-head Brier on common resolved events beats the fixed-context
variant) and also ingest reference predictors for dashboard comparison.

Arena also carries Polymarket events, tickered `PM-<polymarket slug>`. Those
have no Kalshi ticker and are never trading candidates; `pm_events` and
`fetch_pm_predictions` feed the read-only Polymarket collector instead.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)


PM_PREFIX = "PM-"


def _markets_list(v: Any) -> list[str]:
    """`event.markets` is a VARCHAR holding a JSON array, not a json column —
    reading it without a parse silently iterates the string character by
    character."""
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            out = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            return []
        return [str(x) for x in out] if isinstance(out, list) else []
    return []


@dataclass
class ArenaPMEvent:
    """A Polymarket event arena tracks; `slug` addresses it on Gamma."""
    event_ticker: str
    slug: str
    title: str
    category: str | None
    close_time: datetime | None
    rules: str | None
    market_titles: list[str] = field(default_factory=list)


@dataclass
class ArenaPMPrediction:
    """One (prediction, outcome) pair on a Polymarket event. Deliberately not
    an ArenaMarketPrediction: there is no Kalshi ticker to map to."""
    arena_prediction_id: str
    predictor_name: str
    event_ticker: str
    slug: str
    event_title: str
    category: str | None
    close_time: datetime | None
    market_title: str
    p_model: float
    predicted_at: datetime
    rationale: str = ""


@dataclass
class ArenaKalshiMarket:
    """One arena outcome that maps to a Kalshi ticker, for the collector."""
    event_ticker: str
    market_title: str
    ticker: str
    event_title: str
    category: str | None
    close_time: datetime | None


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
        # Outcomes with no venue ticker are skipped. Count them by cause and
        # log: Polymarket events legitimately have none (the pm_* collector
        # owns those), but an unmapped KALSHI outcome is a real loss and used
        # to vanish with no trace at all.
        skipped_pm = 0
        skipped_unmapped: list[str] = []
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
                    if r["event_ticker"].startswith(PM_PREFIX):
                        skipped_pm += 1
                    elif len(skipped_unmapped) < 20:
                        skipped_unmapped.append(f'{r["event_ticker"]}/{title}')
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
        if skipped_pm:
            log.info("skipped %d polymarket outcome(s) with no kalshi ticker "
                     "(expected; collected by the pm_* pipeline)", skipped_pm)
        if skipped_unmapped:
            log.warning("skipped %d kalshi outcome(s) with no ticker mapping: %s",
                        len(skipped_unmapped), ", ".join(skipped_unmapped[:10]))
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
            AND p.created_at > now() - (interval '1 day' * %s)
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

    # ---------------------------------------------------------- polymarket
    def pm_events(self, open_only: bool = True, limit: int = 2000) -> list[ArenaPMEvent]:
        """Polymarket events arena tracks. The `PM-` strip yields the exact
        Gamma slug (verified against every PM event arena currently holds)."""
        sql = """
        SELECT event_ticker, title, category, close_time, rules, markets
        FROM event
        WHERE event_ticker LIKE 'PM-%%'
          AND (%s = FALSE OR market_outcome IS NULL)
        ORDER BY updated_at DESC
        LIMIT %s
        """
        out: list[ArenaPMEvent] = []
        with self._conn() as c:
            for r in c.execute(sql, (open_only, limit)).fetchall():
                ticker = r["event_ticker"]
                out.append(
                    ArenaPMEvent(
                        event_ticker=ticker,
                        slug=ticker[len(PM_PREFIX):],
                        title=r["title"] or "",
                        category=r["category"],
                        close_time=r["close_time"],
                        rules=r["rules"],
                        market_titles=_markets_list(r["markets"]),
                    )
                )
        return out

    def fetch_pm_predictions(
        self,
        predictor_names: list[str] | None,
        since: datetime | None,
        limit: int = 2000,
    ) -> list[ArenaPMPrediction]:
        """Every arena forecast on a Polymarket event, expanded per outcome.

        `predictor_names=None` means all predictors — for collection we want
        the whole field, not just the lanes that trade."""
        sql = """
        SELECT p.id::text AS pid, p.predictor_name, p.event_ticker, p.prediction,
               p.created_at, e.title AS event_title, e.category, e.close_time
        FROM prediction p
        JOIN event e ON e.event_ticker = p.event_ticker
        WHERE p.event_ticker LIKE 'PM-%%'
          AND (%s::text[] IS NULL OR p.predictor_name = ANY(%s))
          AND (%s::timestamptz IS NULL OR p.created_at > %s)
        ORDER BY p.created_at ASC
        LIMIT %s
        """
        out: list[ArenaPMPrediction] = []
        with self._conn() as c:
            rows = c.execute(
                sql, (predictor_names, predictor_names, since, since, limit)
            ).fetchall()
        for r in rows:
            pred = r["prediction"] or {}
            rationale = (pred.get("rationale") or "")[:4000]
            for item in pred.get("probabilities") or []:
                title = item.get("market")
                p = item.get("probability")
                if title is None or p is None:
                    continue
                out.append(
                    ArenaPMPrediction(
                        arena_prediction_id=r["pid"],
                        predictor_name=r["predictor_name"],
                        event_ticker=r["event_ticker"],
                        slug=r["event_ticker"][len(PM_PREFIX):],
                        event_title=r["event_title"] or "",
                        category=r["category"],
                        close_time=r["close_time"],
                        market_title=str(title),
                        p_model=float(p),
                        predicted_at=r["created_at"],
                        rationale=rationale,
                    )
                )
        return out

    # -------------------------------------------------------------- kalshi
    def kalshi_markets(
        self,
        open_only: bool = True,
        forecast_within_days: float | None = None,
        closing_within_days: float | None = None,
        limit: int = 40000,
    ) -> tuple[list["ArenaKalshiMarket"], list[tuple[str, str]]]:
        """Arena's Kalshi universe for the collector, as (mapped, unmapped).

        Arena tracks ~15k open Kalshi markets, far more than is worth
        snapshotting every cycle, so the scope filters are part of the query
        rather than something the caller trims afterwards:
          * `forecast_within_days` — only events some predictor has forecast
            recently, i.e. where a book series pairs with a prediction;
          * `closing_within_days` — only markets resolving soon, where the
            book is actually informative.
        """
        sql = """
        SELECT DISTINCT ON (m.event_ticker, m.market_title)
               m.event_ticker, m.market_title, m.other_info->>'ticker' AS ticker,
               e.title AS event_title, e.category, e.close_time
        FROM market m
        JOIN event e ON e.event_ticker = m.event_ticker
        WHERE m.event_ticker NOT LIKE 'PM-%%'
          AND (%s = FALSE OR e.market_outcome IS NULL)
          AND (%s::float IS NULL OR e.close_time < now() + (interval '1 day' * %s))
          AND (%s::float IS NULL OR EXISTS (
                SELECT 1 FROM prediction p
                WHERE p.event_ticker = m.event_ticker
                  AND p.created_at > now() - (interval '1 day' * %s)))
        ORDER BY m.event_ticker, m.market_title, m.created_at DESC
        LIMIT %s
        """
        params = (open_only, closing_within_days, closing_within_days,
                  forecast_within_days, forecast_within_days, limit)
        mapped: list[ArenaKalshiMarket] = []
        unmapped: list[tuple[str, str]] = []
        with self._conn() as c:
            rows = c.execute(sql, params).fetchall()
        for r in rows:
            if not r["ticker"]:
                unmapped.append((r["event_ticker"], r["market_title"]))
                continue
            mapped.append(
                ArenaKalshiMarket(
                    event_ticker=r["event_ticker"],
                    market_title=r["market_title"],
                    ticker=r["ticker"],
                    event_title=r["event_title"] or "",
                    category=r["category"],
                    close_time=r["close_time"],
                )
            )
        return mapped, unmapped
