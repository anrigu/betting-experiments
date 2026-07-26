"""Reconciliation with Kalshi: order statuses, fills, settlements, equity.

Runs at the end of every engine cycle. All account truth (fees, fills,
settlement revenue) comes from Kalshi; our tables only add strategy
attribution on top.
"""
from __future__ import annotations

import datetime as dt
import json
import logging

from .config import Config
from .db import Store
from .kalshi import KalshiClient, KalshiError, count_of, money_usd

log = logging.getLogger(__name__)

STALE_ORDER_HOURS = 2.0


def _parse_ts(v) -> dt.datetime | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return dt.datetime.fromtimestamp(v, tz=dt.timezone.utc)
    try:
        return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def sync_orders(store: Store, kalshi: KalshiClient, cfg: Config) -> None:
    """Refresh status/fills/fees for our non-terminal orders; cancel stale ones."""
    rows = store.query(
        "SELECT id, kalshi_order_id, created_at, side, count, filled_count FROM {s}.orders "
        "WHERE instance = %s AND dry_run = FALSE AND kalshi_order_id IS NOT NULL "
        "AND status IN ('submitted','resting','pending')",
        (cfg.instance_name,),
    )
    now = dt.datetime.now(dt.timezone.utc)
    for r in rows:
        try:
            o = kalshi.get(f"/portfolio/orders/{r['kalshi_order_id']}").get("order", {})
        except KalshiError as e:
            log.warning("order %s fetch failed: %s", r["kalshi_order_id"], e)
            continue
        status = o.get("status", "")
        filled = count_of(o, "fill_count")
        fees = _fee_dollars(o)
        avg_price = _avg_fill_cents(o, r["side"], filled)
        mapped = {
            "executed": "executed",
            "canceled": "canceled",
            "resting": "resting",
            "pending": "pending",
        }.get(status, status or "unknown")
        store.execute(
            "UPDATE {s}.orders SET status = %s, filled_count = %s, avg_fill_price_cents = %s, "
            "fees_usd = %s, raw = %s WHERE id = %s",
            (mapped, filled, avg_price, fees, json.dumps(o), r["id"]),
        )
        age_h = (now - r["created_at"]).total_seconds() / 3600.0
        if mapped == "resting" and age_h > STALE_ORDER_HOURS:
            try:
                kalshi.cancel_order(r["kalshi_order_id"])
                store.execute(
                    "UPDATE {s}.orders SET status = 'canceled' WHERE id = %s", (r["id"],)
                )
                log.info("canceled stale order %s (%.1fh old, %s/%s filled)",
                         r["kalshi_order_id"], age_h, filled, r["count"])
            except KalshiError as e:
                log.warning("cancel failed for %s: %s", r["kalshi_order_id"], e)


def _fee_dollars(order: dict) -> float:
    """Actual venue fees on an order. Dollars fields win over legacy cents;
    aliases are never summed with each other."""
    taker = money_usd(order, "taker_fees") or 0.0
    maker = money_usd(order, "maker_fees") or 0.0
    return round(taker + maker, 4)


def _avg_fill_cents(order: dict, side: str, filled: int) -> float | None:
    if not filled:
        return None
    taker = money_usd(order, "taker_fill_cost")
    maker = money_usd(order, "maker_fill_cost")
    if taker is not None or maker is not None:
        total = (taker or 0.0) + (maker or 0.0)  # separate fills, legitimately additive
        if total > 0:
            return round(total / filled * 100.0, 2)  # cents per contract
    px = money_usd(order, f"{side}_price")
    return round(px * 100.0, 2) if px is not None else None


def sync_fills(store: Store, kalshi: KalshiClient, cfg: Config) -> int:
    rows = store.query("SELECT max(filled_ts) AS m FROM {s}.fills WHERE instance = %s", (cfg.instance_name,))
    watermark = rows[0]["m"]
    min_ts = int(watermark.timestamp()) - 3600 if watermark else None
    n = 0
    for f in kalshi.all_fills(min_ts=min_ts):
        side = f.get("side")
        px = money_usd(f, "yes_price") if side == "yes" else money_usd(f, "no_price")
        store.upsert_fill(
            instance=cfg.instance_name,
            kalshi_order_id=f.get("order_id"),
            trade_id=f.get("trade_id"),
            ticker=f.get("ticker", ""),
            side=side,
            action=f.get("action"),
            count=count_of(f, "count"),
            price_cents=int(round(px * 100)) if px is not None else None,
            is_taker=f.get("is_taker"),
            fee_usd=money_usd(f, "fee_cost") or money_usd(f, "fee"),
            filled_ts=_parse_ts(f.get("created_time")),
            raw=f,
        )
        n += 1
    return n


def sync_settlements(store: Store, kalshi: KalshiClient, cfg: Config) -> int:
    n = 0
    for s in kalshi.all_settlements():
        ticker = s.get("ticker", "")
        settled_ts = _parse_ts(s.get("settled_time"))
        store.upsert_settlement(
            instance=cfg.instance_name,
            ticker=ticker,
            market_result=s.get("market_result"),
            yes_count=count_of(s, "yes_count"),
            no_count=count_of(s, "no_count"),
            revenue_usd=round(money_usd(s, "revenue") or 0.0, 4),
            settled_ts=settled_ts,
            raw=s,
        )
        n += 1
    _attribute_settlements(store, cfg)
    return n


def _attribute_settlements(store: Store, cfg: Config) -> None:
    """Mark our orders settled and compute per-order realized P&L:
    win  -> filled * $1 - cost - fees ; loss -> -cost - fees."""
    rows = store.query(
        """
        SELECT o.id, o.side, o.filled_count, o.avg_fill_price_cents, o.limit_price_cents,
               o.fees_usd, o.dry_run, s.market_result, s.settled_ts
        FROM {s}.orders o
        JOIN LATERAL (
            SELECT market_result, settled_ts FROM {s}.settlements st
            WHERE st.ticker = o.ticker AND st.instance = o.instance
              AND st.settled_ts > o.created_at
            ORDER BY st.settled_ts ASC LIMIT 1
        ) s ON TRUE
        WHERE o.instance = %s AND o.settled_at IS NULL
          AND o.status IN ('executed','resting','canceled','dry_run')
          AND COALESCE(o.filled_count, 0) > 0
        """,
        (cfg.instance_name,),
    )
    for r in rows:
        filled = r["filled_count"] or 0
        px = (r["avg_fill_price_cents"] or r["limit_price_cents"] or 0) / 100.0
        cost = filled * px
        fees = r["fees_usd"] or 0.0
        result = (r["market_result"] or "").lower()
        if result in ("yes", "no"):
            won = result == r["side"]
            revenue = filled * 1.0 if won else 0.0
        else:  # void / scalar — venue refunds cost AND fees; never book a phantom loss
            revenue = cost + fees
        pnl = round(revenue - cost - fees, 4)
        store.execute(
            "UPDATE {s}.orders SET settled_at = %s, settlement_result = %s, "
            "settlement_revenue_usd = %s, realized_pnl_usd = %s WHERE id = %s",
            (r["settled_ts"], result or "void", round(revenue, 4), pnl, r["id"]),
        )


def snapshot_account(store: Store, kalshi: KalshiClient, cfg: Config) -> None:
    bal = kalshi.balance()
    positions = kalshi.all_positions()
    open_pos = [p for p in positions if count_of(p, "position") != 0]
    balance_usd = money_usd(bal, "balance") or 0.0
    port = money_usd(bal, "portfolio_value") or 0.0
    if port == 0 and open_pos:
        port = sum(abs(money_usd(p, "market_exposure") or 0.0) for p in open_pos)
    store.snapshot_equity(
        instance=cfg.instance_name,
        balance_usd=round(balance_usd, 4),
        portfolio_value_usd=round(port, 2),
        equity_usd=round(balance_usd + port, 2),
        open_positions=len(open_pos),
        raw={"balance": bal, "n_positions": len(positions)},
    )
    for p in open_pos:
        store.snapshot_position(
            instance=cfg.instance_name,
            ticker=p.get("ticker", ""),
            position=count_of(p, "position"),
            market_exposure_usd=round(money_usd(p, "market_exposure") or 0.0, 4),
            realized_pnl_usd=round(money_usd(p, "realized_pnl") or 0.0, 4),
            fees_paid_usd=round(money_usd(p, "fees_paid") or 0.0, 4),
            raw=p,
        )


def sync_all(store: Store, kalshi: KalshiClient, cfg: Config) -> None:
    sync_orders(store, kalshi, cfg)
    try:
        sync_fills(store, kalshi, cfg)
    except KalshiError as e:
        log.warning("fills sync failed: %s", e)
    try:
        sync_settlements(store, kalshi, cfg)
    except KalshiError as e:
        log.warning("settlements sync failed: %s", e)
    snapshot_account(store, kalshi, cfg)


# ------------------------------------------------------------------- dry-run

def settle_dry_run(store: Store, kalshi: KalshiClient, cfg: Config) -> int:
    """Settle simulated orders from real market outcomes so a multi-day
    dry-run shows genuine P&L. Binary results only; anything else waits."""
    rows = store.query(
        "SELECT DISTINCT ticker FROM {s}.orders WHERE instance = %s AND dry_run = TRUE "
        "AND settled_at IS NULL AND COALESCE(filled_count, 0) > 0",
        (cfg.instance_name,),
    )
    n = 0
    for r in rows:
        try:
            m = kalshi.market(r["ticker"])
        except KalshiError:
            continue
        result = (m.get("result") or "").lower()
        if result not in ("yes", "no"):
            continue
        orders = store.query(
            "SELECT id, side, filled_count, avg_fill_price_cents, limit_price_cents, fees_usd "
            "FROM {s}.orders WHERE instance = %s AND ticker = %s AND dry_run = TRUE AND settled_at IS NULL",
            (cfg.instance_name, r["ticker"]),
        )
        for o in orders:
            filled = o["filled_count"] or 0
            px = (o["avg_fill_price_cents"] or o["limit_price_cents"] or 0) / 100.0
            cost = filled * px
            fees = o["fees_usd"] or 0.0
            revenue = filled * 1.0 if result == o["side"] else 0.0
            store.execute(
                "UPDATE {s}.orders SET settled_at = now(), settlement_result = %s, "
                "settlement_revenue_usd = %s, realized_pnl_usd = %s WHERE id = %s",
                (result, round(revenue, 4), round(revenue - cost - fees, 4), o["id"]),
            )
            n += 1
    return n


def snapshot_sim_equity(store: Store, cfg: Config) -> None:
    """Write a simulated equity snapshot (sum of lane ledgers + holdings
    marked to the venue bid) so the dashboard equity curve works in dry-run."""
    total_cash = 0.0
    total_value = 0.0
    open_positions = 0
    marks = {
        r["ticker"]: r
        for r in store.query("SELECT ticker, yes_bid, no_bid, last_price FROM {s}.markets")
    }
    for lane, starting in cfg.lane_bankrolls.items():
        ledger = store.lane_ledger(cfg.instance_name, lane, starting, include_dry_run=True)
        total_cash += ledger["free_cash"] + ledger["reserved"] - ledger["netting"]
        positions = store.lane_positions_all(cfg.instance_name, lane, include_dry_run=True)
        pairs = store.query(
            "SELECT ticker, COALESCE(SUM(filled_count) FILTER (WHERE side='yes'),0) AS y, "
            "COALESCE(SUM(filled_count) FILTER (WHERE side='no'),0) AS n FROM {s}.orders "
            "WHERE instance = %s AND strategy = %s AND settled_at IS NULL AND dry_run = TRUE GROUP BY ticker",
            (cfg.instance_name, lane),
        )
        netted = {p["ticker"]: min(int(p["y"]), int(p["n"])) for p in pairs}
        for ticker, pos in positions.items():
            if pos == 0 and not netted.get(ticker):
                continue
            open_positions += 1
            m = marks.get(ticker) or {}
            if pos > 0:
                mark = (m.get("yes_bid") or m.get("last_price") or 0) / 100.0
                total_value += pos * mark
            elif pos < 0:
                lp = m.get("last_price")
                mark = (m.get("no_bid") or (100 - lp if lp is not None else 0)) / 100.0
                total_value += -pos * mark
            total_value += netted.get(ticker, 0) * 1.0
    store.snapshot_equity(
        instance=cfg.instance_name,
        balance_usd=round(total_cash, 2),
        portfolio_value_usd=round(total_value, 2),
        equity_usd=round(total_cash + total_value, 2),
        open_positions=open_positions,
        raw={"simulated": True},
    )
