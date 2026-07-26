"""Betting-experiments dashboard — FastAPI service.

Single JSON endpoint (/api/overview) + a self-contained static page. All
venue truth (equity, fees, fills) comes from the engine's reconciled tables
in Supabase; the arena DB is tapped read-only for the harness Brier
comparison. 30s in-process cache, error paths return a full-shape payload.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "engine"))

from betx.arena import ArenaDB          # noqa: E402
from betx.config import load as load_config  # noqa: E402
from betx.db import Store               # noqa: E402

log = logging.getLogger("betx.dashboard")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="betting-experiments dashboard")

cfg = load_config()
store = Store(cfg.database_url, cfg.db_schema)

_CACHE_TTL = 30.0
_cache: dict[str, Any] = {"ts": 0.0, "payload": None}
_cache_lock = threading.Lock()

STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/overview")
def overview() -> JSONResponse:
    with _cache_lock:
        if _cache["payload"] is not None and time.time() - _cache["ts"] < _CACHE_TTL:
            return JSONResponse(_cache["payload"])
    try:
        payload = build_payload()
    except Exception as e:  # full-shape null payload so the frontend never breaks
        log.exception("overview failed")
        payload = _empty_payload(warning=f"{type(e).__name__}: {str(e)[:200]}")
    with _cache_lock:
        _cache.update(ts=time.time(), payload=payload)
    return JSONResponse(payload)


# --------------------------------------------------------------------- build

def build_payload() -> dict:
    now = dt.datetime.now(dt.timezone.utc)
    inst = cfg.instance_name

    equity = store.query(
        "SELECT created_at, balance_usd, portfolio_value_usd, equity_usd, open_positions "
        "FROM {s}.equity_snapshots WHERE instance = %s ORDER BY created_at DESC LIMIT 1",
        (inst,),
    )
    latest_eq = equity[0] if equity else None

    series = store.query(
        """
        SELECT DISTINCT ON (bucket) bucket, equity_usd, balance_usd, portfolio_value_usd
        FROM (
            SELECT date_trunc('hour', created_at)
                   + floor(extract(minute FROM created_at) / 10) * interval '10 min' AS bucket,
                   equity_usd, balance_usd, portfolio_value_usd, created_at
            FROM {s}.equity_snapshots WHERE instance = %s
        ) t ORDER BY bucket, created_at DESC
        """,
        (inst,),
    )

    lanes = []
    for lane, starting in cfg.lane_bankrolls.items():
        ledger = store.lane_ledger(inst, lane, starting, include_dry_run=cfg.dry_run)
        stats = store.query(
            """
            SELECT
              count(*) FILTER (WHERE status NOT IN ('failed'))                          AS orders,
              count(*) FILTER (WHERE settled_at IS NOT NULL)                            AS settled,
              count(*) FILTER (WHERE settled_at IS NOT NULL AND realized_pnl_usd > 0)   AS wins,
              COALESCE(sum(realized_pnl_usd) FILTER (WHERE settled_at IS NOT NULL), 0)  AS realized,
              COALESCE(sum(fees_usd), 0)                                                AS fees,
              COALESCE(sum(filled_count * COALESCE(avg_fill_price_cents, limit_price_cents) / 100.0)
                       FILTER (WHERE settled_at IS NOT NULL), 0)                        AS settled_cost
            FROM {s}.orders WHERE instance = %s AND strategy = %s AND dry_run = %s
            """,
            (inst, lane, cfg.dry_run),
        )[0]
        skips = store.query(
            "SELECT count(*) AS n FROM {s}.decisions WHERE instance = %s AND strategy = %s AND action = 'skip'",
            (inst, lane),
        )[0]["n"]
        holdings = _lane_holdings(inst, lane)
        holdings_mtm = sum(h["value_usd"] for h in holdings)
        holdings_cost = sum(h["cost_usd"] for h in holdings)
        realized = float(stats["realized"] or 0)
        mtm_equity = ledger["free_cash"] + ledger["reserved"] + holdings_mtm
        lanes.append(
            {
                "lane": lane,
                "starting_bankroll": starting,
                "free_cash": ledger["free_cash"],
                "reserved": ledger["reserved"],
                "netting_credit": ledger["netting"],
                "holdings_mtm": round(holdings_mtm, 2),
                "holdings_cost": round(holdings_cost, 2),
                "mtm_equity": round(mtm_equity, 2),
                "unrealized": round(mtm_equity - starting - realized, 2),
                "realized": round(realized, 2),
                "roi_settled": round(realized / float(stats["settled_cost"]), 4) if stats["settled_cost"] else None,
                "orders": stats["orders"],
                "settled": stats["settled"],
                "wins": stats["wins"],
                "fees": round(float(stats["fees"] or 0), 2),
                "skips": skips,
                "open_markets": len(holdings),
                "holdings": holdings,
            }
        )

    orders = store.query(
        """
        SELECT o.created_at, o.strategy, o.ticker, o.title, o.category, o.side, o.count,
               o.limit_price_cents, o.status, o.filled_count, o.avg_fill_price_cents,
               o.fees_usd, o.settled_at, o.settlement_result, o.realized_pnl_usd, o.dry_run,
               d.edge, d.p_model
        FROM {s}.orders o LEFT JOIN {s}.decisions d ON d.id = o.decision_id
        WHERE o.instance = %s ORDER BY o.created_at DESC LIMIT 200
        """,
        (inst,),
    )

    skip_summary = store.query(
        "SELECT strategy, skip_reason, count(*) AS n FROM {s}.decisions "
        "WHERE instance = %s AND action = 'skip' GROUP BY 1, 2 ORDER BY n DESC LIMIT 40",
        (inst,),
    )

    llm = store.query(
        "SELECT COALESCE(sum(cost_usd), 0) AS total, count(*) AS calls, "
        "COALESCE(sum(input_tokens), 0) AS in_tok, COALESCE(sum(output_tokens), 0) AS out_tok "
        "FROM {s}.llm_calls WHERE instance = %s",
        (inst,),
    )[0]
    llm_by_day = store.query(
        "SELECT date_trunc('day', created_at)::date::text AS day, round(sum(cost_usd)::numeric, 4) AS cost, count(*) AS calls "
        "FROM {s}.llm_calls WHERE instance = %s GROUP BY 1 ORDER BY 1 DESC LIMIT 30",
        (inst,),
    )

    pred_stats = store.query(
        "SELECT model_spec, harness, count(*) AS n, max(created_at) AS latest "
        "FROM {s}.predictions WHERE instance = %s GROUP BY 1, 2 ORDER BY n DESC",
        (inst,),
    )

    cycles = store.query(
        "SELECT id, started_at, finished_at, mode, markets_considered, predictions_made, "
        "bets_placed, error FROM {s}.engine_cycles WHERE instance = %s ORDER BY id DESC LIMIT 10",
        (inst,),
    )
    last_cycle = cycles[0] if cycles else None
    engine_healthy = bool(
        last_cycle
        and last_cycle["finished_at"]
        and (now - last_cycle["finished_at"]).total_seconds() < max(cfg.poll_interval_sec * 2.5, 1800)
        and not last_cycle["error"]
    )

    brier = []
    try:
        arena = ArenaDB(cfg.arena_database_url)
        brier = arena.brier_snapshot([cfg.arena_predictor] + cfg.reference_predictors, since_days=45)
    except Exception as e:
        log.warning("brier snapshot failed: %s", e)

    total_realized = sum(l["realized"] for l in lanes)
    total_fees = sum(l["fees"] for l in lanes)

    return _json_safe(
        {
            "as_of": now.isoformat(),
            "instance": inst,
            "mode": "dry_run" if cfg.dry_run else "live",
            "predictor": cfg.arena_predictor,
            "reference_predictors": cfg.reference_predictors,
            "close_buffer_hours": cfg.close_buffer_hours,
            "account": {
                "balance_usd": latest_eq["balance_usd"] if latest_eq else None,
                "portfolio_value_usd": latest_eq["portfolio_value_usd"] if latest_eq else None,
                "equity_usd": latest_eq["equity_usd"] if latest_eq else None,
                "open_positions": latest_eq["open_positions"] if latest_eq else None,
                "snapshot_at": latest_eq["created_at"] if latest_eq else None,
            },
            "totals": {
                "realized_pnl": round(total_realized, 2),
                "fees": round(total_fees, 2),
                "llm_cost": round(float(llm["total"] or 0), 4),
                "llm_calls": llm["calls"],
                "llm_tokens_in": llm["in_tok"],
                "llm_tokens_out": llm["out_tok"],
            },
            "lanes": lanes,
            "equity_series": series,
            "orders": orders,
            "skip_summary": skip_summary,
            "llm_by_day": llm_by_day,
            "prediction_stats": pred_stats,
            "cycles": cycles,
            "engine_healthy": engine_healthy,
            "brier": brier,
            "warning": None,
        }
    )


def _lane_holdings(inst: str, lane: str) -> list[dict]:
    """Net unsettled position per ticker, marked to the venue bid."""
    rows = store.query(
        """
        SELECT o.ticker, max(o.title) AS title,
               COALESCE(sum(o.filled_count) FILTER (WHERE o.side = 'yes'), 0) AS yes_n,
               COALESCE(sum(o.filled_count) FILTER (WHERE o.side = 'no'), 0)  AS no_n,
               COALESCE(sum(o.filled_count * COALESCE(o.avg_fill_price_cents, o.limit_price_cents) / 100.0), 0) AS cost,
               m.yes_bid, m.no_bid, m.last_price, m.close_time
        FROM {s}.orders o LEFT JOIN {s}.markets m ON m.ticker = o.ticker
        WHERE o.instance = %s AND o.strategy = %s AND o.settled_at IS NULL
          AND o.status <> 'failed' AND o.dry_run = %s AND COALESCE(o.filled_count, 0) > 0
        GROUP BY o.ticker, m.yes_bid, m.no_bid, m.last_price, m.close_time
        """,
        (inst, lane, cfg.dry_run),
    )
    out = []
    for r in rows:
        pos = int(r["yes_n"]) - int(r["no_n"])
        netted = min(int(r["yes_n"]), int(r["no_n"]))
        if pos > 0:
            mark = (r["yes_bid"] or r["last_price"] or 0) / 100.0
            value = pos * mark
        elif pos < 0:
            mark = (r["no_bid"] or (100 - (r["last_price"] or 0)) or 0) / 100.0
            value = -pos * mark
        else:
            mark, value = None, 0.0
        value += netted * 1.0  # offsetting pairs pay $1 regardless of outcome
        out.append(
            {
                "ticker": r["ticker"],
                "title": r["title"],
                "position": pos,
                "netted_pairs": netted,
                "mark": mark,
                "value_usd": round(value, 2),
                "cost_usd": round(float(r["cost"]), 2),
                "close_time": r["close_time"],
            }
        )
    return sorted(out, key=lambda h: -abs(h["value_usd"]))


def _empty_payload(warning: str) -> dict:
    return {
        "as_of": dt.datetime.now(dt.timezone.utc).isoformat(),
        "instance": cfg.instance_name,
        "mode": "dry_run" if cfg.dry_run else "live",
        "predictor": cfg.arena_predictor,
        "reference_predictors": cfg.reference_predictors,
        "close_buffer_hours": cfg.close_buffer_hours,
        "account": {"balance_usd": None, "portfolio_value_usd": None, "equity_usd": None,
                    "open_positions": None, "snapshot_at": None},
        "totals": {"realized_pnl": 0, "fees": 0, "llm_cost": 0, "llm_calls": 0,
                   "llm_tokens_in": 0, "llm_tokens_out": 0},
        "lanes": [], "equity_series": [], "orders": [], "skip_summary": [],
        "llm_by_day": [], "prediction_stats": [], "cycles": [],
        "engine_healthy": False, "brier": [], "warning": warning,
    }


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, dt.datetime):
        return obj.isoformat()
    if isinstance(obj, dt.date):
        return obj.isoformat()
    if hasattr(obj, "__float__") and not isinstance(obj, (int, float, bool)):
        return float(obj)
    return obj
