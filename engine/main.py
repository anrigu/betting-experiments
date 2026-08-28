"""Betting-experiments worker entrypoint.

    python engine/main.py [--once] [--dry-run]

Polls the ProphetArena DB for new predictions from each lane's agent on a
UTC-aligned boundary (default every 10 min), decides per lane (see
BETX_LANES: name:predictor:strategy:bankroll), executes on Kalshi, then
reconciles orders/fills/settlements and snapshots account equity. All lanes
share one Kalshi account; the books are kept separate virtually.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from betx import config
from betx.arena import ArenaDB
from betx.db import Store
from betx.engine import Engine
from betx.kalshi import KalshiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("betx.main")

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    log.info("signal %s received; finishing current cycle then exiting", signum)


def next_boundary(now_ts: float, interval: int) -> float:
    return ((int(now_ts) // interval) + 1) * interval


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = config.load()
    if args.dry_run:
        cfg.dry_run = True
    cfg.require("database_url", "arena_database_url", "kalshi_api_key_id", "kalshi_private_key_b64")

    store = Store(cfg.database_url, cfg.db_schema)
    store.migrate()
    kalshi = KalshiClient(
        cfg.kalshi_base_url, cfg.kalshi_api_key_id, cfg.kalshi_private_key_pem,
        public_base_url=cfg.kalshi_public_base_url,
    )
    arena = ArenaDB(cfg.arena_database_url)
    engine = Engine(cfg, store, kalshi, arena)

    log.info(
        "betx starting: instance=%s lanes=%s mode=%s interval=%ss",
        cfg.instance_name,
        [(l.name, l.predictor, l.strategy, l.bankroll) for l in cfg.lanes],
        "DRY_RUN" if cfg.dry_run else "LIVE", cfg.poll_interval_sec,
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if args.once:
        stats = engine.run_cycle()
        log.info("cycle done: %s", stats)
        return

    while not _shutdown:
        stats = engine.run_cycle()
        log.info("cycle done: %s", stats)
        target = next_boundary(time.time(), cfg.poll_interval_sec)
        while not _shutdown and time.time() < target:
            time.sleep(1)


if __name__ == "__main__":
    main()
