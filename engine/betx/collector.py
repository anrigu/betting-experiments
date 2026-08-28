"""Collection runner: one cycle, both venues, one cadence.

Owns the `collect_cycles` row and isolates the venues from each other — a
Polymarket outage must not cost a Kalshi cycle, and vice versa, so each
venue's pass is wrapped independently and its error recorded rather than
raised.

The trading engine is deliberately NOT part of this. Collection cadence
(`BETX_COLLECT_INTERVAL_SEC`) and decision cadence
(`BETX_POLL_INTERVAL_SEC`) are separate knobs because changing how often you
snapshot a book should never change when the engine decides a trade.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from .arena import ArenaDB
from .config import Config
from .db import Store
from .kalshi import KalshiClient
from .kalshi_collect import KalshiCollector
from .pm_collect import PolymarketCollector

log = logging.getLogger(__name__)

STAT_KEYS = (
    "events_seen", "markets_seen", "books_captured", "trades_ingested",
    "history_points", "predictions_ingested", "unmapped",
)


class Collector:
    def __init__(
        self,
        cfg: Config,
        store: Store,
        arena: ArenaDB,
        kalshi: KalshiClient | None = None,
    ):
        self.cfg = cfg
        self.store = store
        self._cycle_n = 0
        self.pm = PolymarketCollector(cfg, store, arena) if cfg.pm_enabled else None
        self.kx = None
        if cfg.kx_enabled:
            client = kalshi or KalshiClient(
                cfg.kalshi_base_url,
                public_base_url=cfg.kalshi_public_base_url or cfg.kalshi_base_url,
            )
            self.kx = KalshiCollector(cfg, store, arena, client)
            self._owns_kalshi = kalshi is None
        else:
            self._owns_kalshi = False

    def run_cycle(self) -> dict:
        cfg = self.cfg
        self._cycle_n += 1
        venues = cfg.collect_venues
        cycle_id = self.store.start_collect_cycle(cfg.instance_name, "+".join(venues))
        totals: dict[str, int] = {k: 0 for k in STAT_KEYS}
        detail: dict[str, Any] = {"cycle_n": self._cycle_n}
        errors: list[str] = []
        t0 = time.monotonic()

        for name, venue in (("polymarket", self.pm), ("kalshi", self.kx)):
            if venue is None:
                continue
            try:
                stats, vdetail = venue.collect(cycle_id, self._cycle_n)
                for k, v in stats.items():
                    totals[k] = totals.get(k, 0) + v
                detail[name] = {**vdetail, **stats}
                log.info("%s: %s", name, stats)
            except Exception as e:
                log.exception("%s collection failed", name)
                errors.append(f"{name}: {str(e)[:200]}")
                detail[name] = {"error": str(e)[:300]}

        detail["elapsed_sec"] = round(time.monotonic() - t0, 2)
        self.store.finish_collect_cycle(
            cycle_id, error="; ".join(errors)[:500] or None, detail=detail, **totals
        )
        return totals

    def close(self) -> None:
        if self.pm is not None:
            self.pm.close()
        if self.kx is not None and self._owns_kalshi:
            self.kx.client.close()
