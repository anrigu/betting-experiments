"""Engine cycle: ingest ProphetArena predictions -> decide per lane -> execute.

Safety properties (learned from the framework's live run):
  * claim-before-submit — the decision row is inserted (unique on
    instance/strategy/prediction/ticker) BEFORE any order goes to Kalshi, so
    a crash or a concurrent worker costs at most a skipped bet, never a
    double bet; client_order_id is a uuid5 of the same identity for venue-
    level idempotency;
  * decide off the live book, always — ladders come from the venue's
    full-depth orderbook (public market-data API, signed fallback) at
    decision time, never from arena snapshots;
  * 24h close gate fails closed — no close_time means no bet;
  * orders are limit buys at the ask with a 5-minute venue-side expiration —
    the depth pre-check makes an immediate fill likely and a moved book
    kills the order instead of letting it rest stale.

Data capture: every decision gets a full-depth book snapshot it priced from
('decision'), and every order submit — live, dry-run, or failed — gets a
fresh 'post_trade' snapshot (full book + complete market payload + recent
public trade tape), taken AFTER submit so it never delays the order.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import random
import uuid
from typing import Any

import psycopg

from dataclasses import dataclass, field

from . import kalshi as kalshi_mod
from . import strategies, sync
from .arena import ArenaDB
from .config import Config, Lane
from .db import Store
from .fees import taker_fee
from .kalshi import KalshiClient, KalshiError

log = logging.getLogger(__name__)


@dataclass
class Candidate:
    """A (prediction row, Kalshi market) pair awaiting lane decisions —
    re-derived from the DB each cycle so nothing is lost across crashes."""
    prediction_row_id: int
    predictor_name: str
    ticker: str
    event_ticker: str | None
    title: str
    category: str | None
    close_time: dt.datetime | None
    p_model: float
    predicted_at: dt.datetime
    decided_lanes: set[str] = field(default_factory=set)


class LaneBook:
    """Per-cycle cached lane state: the ledger fold and per-ticker positions
    are read once, then updated in memory as orders are placed — Supabase
    round-trips are ~300ms, so folding per candidate is prohibitive."""

    def __init__(self, store: Store, cfg: Config, lane: Lane):
        self.lane = lane
        self.ledger = store.lane_ledger(cfg.instance_name, lane.name, lane.bankroll, include_dry_run=cfg.dry_run)
        self.positions = store.lane_positions_all(cfg.instance_name, lane.name, include_dry_run=cfg.dry_run)
        self._dry_run = cfg.dry_run

    @property
    def free_cash(self) -> float:
        return self.ledger["free_cash"]

    def position(self, ticker: str) -> int:
        return self.positions.get(ticker, 0)

    def apply_order(self, ticker: str, side: str, contracts: int, price: float, fee: float) -> None:
        self.ledger["free_cash"] = round(self.ledger["free_cash"] - (contracts * price + fee), 4)
        if self._dry_run:  # dry-run simulates a full immediate fill
            delta = contracts if side == "yes" else -contracts
            self.positions[ticker] = self.positions.get(ticker, 0) + delta


class Engine:
    def __init__(self, cfg: Config, store: Store, kalshi: KalshiClient, arena: ArenaDB):
        self.cfg = cfg
        self.store = store
        self.kalshi = kalshi
        self.arena = arena
        self._market_cache: dict[str, dict[str, Any]] = {}
        self._ob_cache: dict[str, dict] = {}
        self._book_snap_ids: dict[str, int | None] = {}
        self._books: dict[str, LaneBook] = {}

    # ------------------------------------------------------------------ cycle
    def run_cycle(self) -> dict:
        cfg = self.cfg
        self._market_cache = {}
        self._ob_cache = {}
        self._book_snap_ids = {}
        mode = "dry_run" if cfg.dry_run else "live"
        cycle_id = self.store.start_cycle(cfg.instance_name, mode)
        stats = {"markets_considered": 0, "predictions_made": 0, "bets_placed": 0}
        try:
            self._books = {lane.name: LaneBook(self.store, cfg, lane) for lane in cfg.lanes}
            candidates = self._ingest(cycle_id, stats)
            random.shuffle(candidates)
            for cand in candidates:
                stats["markets_considered"] += 1
                try:
                    placed = self._process_candidate(cand, cycle_id)
                    stats["bets_placed"] += placed
                except Exception:
                    log.exception("candidate %s failed", cand.ticker)
            if cfg.live_enabled and not cfg.dry_run:
                sync.sync_all(self.store, self.kalshi, cfg)
            settled = sync.settle_dry_run(self.store, self.kalshi, cfg)
            if settled:
                log.info("settled %d dry-run orders from real market outcomes", settled)
            if cfg.dry_run:
                sync.snapshot_sim_equity(self.store, cfg)
            self.store.finish_cycle(cycle_id, **stats)
        except Exception as e:
            log.exception("cycle failed")
            self.store.finish_cycle(cycle_id, error=str(e)[:500], **stats)
        return stats

    # ----------------------------------------------------------------- ingest
    def _ingest(self, cycle_id: int, stats: dict) -> list[Candidate]:
        cfg = self.cfg
        watermark = self.store.prediction_watermark(cfg.instance_name, "arena")
        if watermark is None:
            watermark = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=cfg.backfill_hours)
        else:
            # overlap buffer: arena rows can commit slightly out of created_at
            # order; the unique (source, external_id) constraint dedupes re-reads
            watermark -= dt.timedelta(minutes=10)
        names = list(dict.fromkeys(cfg.bet_predictors + cfg.reference_predictors))
        preds = self.arena.fetch_new_predictions(names, since=watermark)
        log.info("fetched %d (prediction x market) rows since %s", len(preds), watermark)

        rows = []
        for p in preds:
            harness = "agentic" if p.predictor_name.startswith("agent-") else "fixed-context"
            rows.append(
                dict(
                    instance=cfg.instance_name,
                    cycle_id=cycle_id,
                    source="arena",
                    model_spec=p.predictor_name,
                    harness=harness,
                    ticker=p.ticker,
                    event_ticker=p.event_ticker,
                    title=f"{p.event_title} — {p.market_title}",
                    category=p.category,
                    close_time=p.close_time,
                    p_model=p.p_model,
                    yes_ask=int(p.snap_yes_ask) if p.snap_yes_ask is not None else None,
                    no_ask=int(p.snap_no_ask) if p.snap_no_ask is not None else None,
                    reasoning=p.raw.get("rationale"),
                    external_id=f"{p.arena_prediction_id}:{p.ticker}",
                    raw={"predicted_at": p.predicted_at.isoformat(), "market_title": p.market_title},
                )
            )
        inserted = self.store.insert_predictions_bulk(rows)
        stats["predictions_made"] += len(inserted)

        # Candidates come from the DB, not the insert map: any recent bet-
        # predictor prediction still missing a decision for one of its lanes
        # (survives crashes between ingest and decide, and code redeploys).
        lookback = cfg.backfill_hours + 24
        unprocessed = self.store.unprocessed_predictions(cfg.instance_name, cfg.bet_predictors, lookback)
        epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        cands = []
        for p in unprocessed:
            cands.append(
                Candidate(
                    prediction_row_id=p["id"],
                    predictor_name=p["model_spec"],
                    ticker=p["ticker"],
                    event_ticker=p["event_ticker"],
                    title=p["title"] or p["ticker"],
                    category=p["category"],
                    close_time=p["close_time"],
                    p_model=float(p["p_model"]),
                    predicted_at=_parse_ts(p["predicted_at"]) or epoch,
                    decided_lanes=set(p["decided_lanes"]),
                )
            )

        # Only each predictor's NEWEST forecast per ticker trades. The newest
        # map is computed over ALL recent predictions — including fully-decided
        # ones — so a late-committing OLDER forecast can never trade after a
        # newer one has already been acted on (sooth's stale-fire_ts guard).
        newest: dict[tuple[str, str], dt.datetime] = {}
        for cand in cands:
            key = (cand.predictor_name, cand.ticker)
            if cand.predicted_at > newest.get(key, epoch):
                newest[key] = cand.predicted_at
        out: list[Candidate] = []
        for cand in cands:
            missing = [l for l in cfg.lanes_for(cand.predictor_name) if l.name not in cand.decided_lanes]
            if not missing:
                continue
            if cand.predicted_at < newest[(cand.predictor_name, cand.ticker)]:
                for lane in missing:
                    self._save_skip(cycle_id, cand, lane.name, "superseded")
            else:
                out.append(cand)
        return out

    # ------------------------------------------------------------- candidates
    def _process_candidate(self, cand: Candidate, cycle_id: int) -> int:
        cfg = self.cfg
        now = dt.datetime.now(dt.timezone.utc)
        lanes = [l for l in cfg.lanes_for(cand.predictor_name) if l.name not in cand.decided_lanes]
        if not lanes:
            return 0
        market = self._get_market(cand.ticker)
        if market is None:
            for lane in lanes:
                self._save_skip(cycle_id, cand, lane.name, "market_not_found")
            return 0

        close_time = _effective_close(market) or cand.close_time
        cand.close_time = close_time  # orders/snapshots record the effective time
        status = market.get("status", "")
        self._upsert_market(cand, market, close_time)

        # 24h RESOLUTION gate, fail-closed, before the book fetch. Uses the
        # earliest of close_time / expected_expiration_time / occurrence_datetime
        # — early-close and occurrence markets resolve well before their nominal
        # close, and "no trading within 24h of resolve" means resolve, not close.
        if close_time is None:
            reason = "no_close_time"
        elif close_time - now <= dt.timedelta(hours=cfg.close_buffer_hours):
            reason = "near_resolution"
        elif status not in ("open", "active"):
            reason = f"market_status:{status}"
        else:
            reason = None
        if reason:
            for lane in lanes:
                self._save_skip(cycle_id, cand, lane.name, reason)
            return 0

        try:
            ob = self._get_orderbook(cand.ticker)
        except KalshiError as e:
            log.warning("orderbook %s failed: %s", cand.ticker, e)
            for lane in lanes:
                self._save_skip(cycle_id, cand, lane.name, "book_fetch_failed")
            return 0
        yes_asks, no_asks = strategies.ladders_from_orderbook(ob)
        snap_id = self._snapshot_book(cycle_id, cand.ticker, market, ob, yes_asks, no_asks, close_time)

        placed = 0
        for lane in lanes:
            book = self._books[lane.name]
            position = book.position(cand.ticker)
            result = strategies.DECIDERS[lane.strategy](
                cand.p_model, yes_asks, no_asks, book.free_cash, position,
                sizing=lane.sizing,
            )
            placed += self._record_and_execute(
                cycle_id, lane.name, cand, result, book, position, yes_asks, no_asks, snap_id
            )
        return placed

    def _record_and_execute(
        self,
        cycle_id: int,
        lane: str,
        cand: Candidate,
        result: strategies.Order | strategies.Skip,
        book: LaneBook,
        position: int,
        yes_asks: strategies.Ladder,
        no_asks: strategies.Ladder,
        book_snapshot_id: int | None,
    ) -> int:
        cfg = self.cfg
        by = strategies.best_ask(yes_asks)
        bn = strategies.best_ask(no_asks)
        spread = (by + bn - 1.0) if (by is not None and bn is not None) else None
        common = dict(
            instance=cfg.instance_name,
            cycle_id=cycle_id,
            prediction_id=cand.prediction_row_id,
            strategy=lane,
            ticker=cand.ticker,
            p_model=cand.p_model,
            p_market=by,
            spread=spread,
            book_snapshot_id=book_snapshot_id,
        )
        if isinstance(result, strategies.Skip):
            try:
                self.store.insert_decision(
                    action="skip", skip_reason=result.reason, edge=result.edge,
                    detail={"free_cash": book.free_cash, "position": position, **result.detail},
                    **common,
                )
            except psycopg.errors.UniqueViolation:
                pass
            return 0

        o = result
        fee_pc = taker_fee(cand.ticker, 1, o.limit_price)
        try:
            decision_id = self.store.insert_decision(
                action="bet", side=o.side, edge=o.edge,
                fee_per_contract=fee_pc,
                ev_per_contract=(o.edge - (o.fee_usd / o.contracts if o.contracts else 0.0)),
                price_cents=int(round(o.limit_price * 100)),
                contracts=o.contracts,
                stake_usd=o.cost_usd,
                detail={
                    "free_cash": book.free_cash, "position_before": o.position_before,
                    "target_position": o.target_position, "fee_reserve": o.fee_usd, **o.detail,
                },
                **common,
            )
        except psycopg.errors.UniqueViolation:
            log.warning("decision already claimed for %s/%s/%s — skipping submit", lane, cand.prediction_row_id, cand.ticker)
            return 0

        if not cfg.live_enabled:
            return 0
        placed = self._submit(cycle_id, decision_id, lane, cand, o)
        if placed:
            book.apply_order(cand.ticker, o.side, o.contracts, o.limit_price, o.fee_usd)
        return placed

    def _submit(self, cycle_id: int, decision_id: int, lane: str, cand: Candidate, o: strategies.Order) -> int:
        cfg = self.cfg
        price_cents = max(1, min(99, int(round(o.limit_price * 100))))
        client_order_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"betx/{cfg.instance_name}/{lane}/{decision_id}"))
        base_row = dict(
            instance=cfg.instance_name,
            decision_id=decision_id,
            strategy=lane,
            ticker=cand.ticker,
            event_ticker=cand.event_ticker,
            title=cand.title,
            category=cand.category,
            side=o.side,
            action="buy",
            count=o.contracts,
            limit_price_cents=price_cents,
            client_order_id=client_order_id,
            close_time=cand.close_time,
        )
        if cfg.dry_run:
            order_row_id = self.store.insert_order(
                status="dry_run", dry_run=True,
                filled_count=o.contracts, avg_fill_price_cents=float(price_cents),
                fees_usd=o.fee_usd,
                raw={"simulated": True}, **base_row,
            )
            log.info("[dry-run] %s %s %dx %s @ %d¢", lane, o.side, o.contracts, cand.ticker, price_cents)
            self._snapshot_trade(cycle_id, order_row_id, cand.ticker)
            return 1
        try:
            expiration = int(dt.datetime.now(dt.timezone.utc).timestamp()) + cfg.order_expiration_sec
            resp = self.kalshi.create_order(
                ticker=cand.ticker, side=o.side, count=o.contracts,
                price_cents=price_cents, client_order_id=client_order_id,
                expiration_ts=expiration,
            )
            od = resp.get("order", {})
            status = {"executed": "executed", "resting": "resting", "pending": "pending", "canceled": "canceled"}.get(
                od.get("status", ""), od.get("status") or "submitted"
            )
            order_row_id = self.store.insert_order(
                status=status,
                kalshi_order_id=od.get("order_id"),
                filled_count=kalshi_mod.count_of(od, "fill_count") or (o.contracts if status == "executed" else 0),
                raw=od, **base_row,
            )
            log.info("placed %s %s %dx %s @ %d¢ -> %s", lane, o.side, o.contracts, cand.ticker, price_cents, status)
            self._snapshot_trade(cycle_id, order_row_id, cand.ticker)
            return 1
        except KalshiError as e:
            order_row_id = self.store.insert_order(status="failed", raw={"error": str(e)[:500]}, **base_row)
            log.error("order failed %s %s: %s", lane, cand.ticker, e)
            self._snapshot_trade(cycle_id, order_row_id, cand.ticker)
            return 0

    # ---------------------------------------------------------------- helpers
    def _save_skip(self, cycle_id: int, cand: Candidate, lane: str, reason: str) -> None:
        try:
            self.store.insert_decision(
                instance=self.cfg.instance_name, cycle_id=cycle_id, prediction_id=cand.prediction_row_id,
                strategy=lane, ticker=cand.ticker, action="skip", skip_reason=reason,
                p_model=cand.p_model,
            )
        except psycopg.errors.UniqueViolation:
            pass

    def _snapshot_book(
        self,
        cycle_id: int,
        ticker: str,
        market: dict,
        ob: dict,
        yes_asks: strategies.Ladder,
        no_asks: strategies.Ladder,
        close_time,
    ) -> int | None:
        """Persist the exact market state + full book every decision priced
        from — one snapshot per (cycle, ticker), shared across lanes."""
        if ticker in self._book_snap_ids:
            return self._book_snap_ids[ticker]
        q = kalshi_mod.market_quotes(market)
        try:
            snap_id = self.store.insert_book_snapshot(
                instance=self.cfg.instance_name,
                cycle_id=cycle_id,
                ticker=ticker,
                kind="decision",
                status=market.get("status"),
                close_time=close_time,
                yes_bid=q["yes_bid"], yes_ask=q["yes_ask"],
                no_bid=q["no_bid"], no_ask=q["no_ask"],
                last_price=q["last_price"],
                volume_24h=kalshi_mod.count_of(market, "volume_24h") or None,
                open_interest=kalshi_mod.count_of(market, "open_interest") or None,
                liquidity=q["liquidity"],
                orderbook=ob,
                yes_asks=[[p, c] for p, c in yes_asks],
                no_asks=[[p, c] for p, c in no_asks],
                raw_market=market,
            )
        except Exception:
            log.exception("book snapshot failed for %s", ticker)
            snap_id = None
        self._book_snap_ids[ticker] = snap_id
        return snap_id

    def _snapshot_trade(self, cycle_id: int, order_row_id: int | None, ticker: str) -> None:
        """Full market-state capture at trade time: fresh full-depth book,
        the complete market payload, and the recent public trade tape —
        fetched AFTER submit (never delays the order) and deliberately NOT
        through the per-cycle caches, so it reflects the venue at this
        moment, including our own fill's impact. Best-effort: a capture
        failure never fails the trade path."""
        try:
            market = self.kalshi.market(ticker)
            ob = self.kalshi.orderbook(ticker)
            try:
                tape = self.kalshi.recent_trades(ticker, limit=100)
            except Exception:
                log.exception("trade tape fetch failed for %s", ticker)
                tape = None
            yes_asks, no_asks = strategies.ladders_from_orderbook(ob)
            q = kalshi_mod.market_quotes(market)
            self.store.insert_book_snapshot(
                instance=self.cfg.instance_name,
                cycle_id=cycle_id,
                ticker=ticker,
                kind="post_trade",
                order_id=order_row_id,
                status=market.get("status"),
                close_time=_effective_close(market),
                yes_bid=q["yes_bid"], yes_ask=q["yes_ask"],
                no_bid=q["no_bid"], no_ask=q["no_ask"],
                last_price=q["last_price"],
                volume_24h=kalshi_mod.count_of(market, "volume_24h") or None,
                open_interest=kalshi_mod.count_of(market, "open_interest") or None,
                liquidity=q["liquidity"],
                orderbook=ob,
                yes_asks=[[p, c] for p, c in yes_asks],
                no_asks=[[p, c] for p, c in no_asks],
                trades=tape,
                raw_market=market,
            )
        except Exception:
            log.exception("post-trade snapshot failed for %s", ticker)

    def _get_market(self, ticker: str) -> dict | None:
        if ticker in self._market_cache:
            return self._market_cache[ticker]
        try:
            m = self.kalshi.market(ticker)
        except KalshiError as e:
            log.warning("market %s fetch failed: %s", ticker, e)
            m = None
        self._market_cache[ticker] = m or None
        return self._market_cache[ticker]

    def _get_orderbook(self, ticker: str) -> dict:
        """Per-cycle cache: two lanes' predictors can hit the same ticker in
        one cycle; within a cycle they should decide off the same book."""
        if ticker not in self._ob_cache:
            self._ob_cache[ticker] = self.kalshi.orderbook(ticker)
        return self._ob_cache[ticker]

    def _upsert_market(self, cand: Candidate, market: dict, close_time) -> None:
        q = kalshi_mod.market_quotes(market)
        self.store.upsert_market(
            {
                "ticker": cand.ticker,
                "event_ticker": cand.event_ticker,
                "title": market.get("title") or cand.title,
                "category": cand.category,
                "close_time": close_time,
                "status": market.get("status"),
                "yes_bid": q["yes_bid"],
                "yes_ask": q["yes_ask"],
                "no_bid": q["no_bid"],
                "no_ask": q["no_ask"],
                "last_price": q["last_price"],
                "volume_24h": kalshi_mod.count_of(market, "volume_24h") or None,
                "liquidity": q["liquidity"],
                "raw": market,
            }
        )


def _parse_ts(v) -> dt.datetime | None:
    if not v:
        return None
    try:
        return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def _effective_close(market: dict) -> dt.datetime | None:
    """Earliest plausible resolution time: min of close_time,
    expected_expiration_time, occurrence_datetime (whichever are present)."""
    times = [
        _parse_ts(market.get(k))
        for k in ("close_time", "expected_expiration_time", "occurrence_datetime")
    ]
    times = [t for t in times if t is not None]
    return min(times) if times else None
