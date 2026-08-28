"""Market-data collector entrypoint — both venues, one worker.

    python engine/collect.py [--once] [--interval SEC] [--venue pm|kalshi]

Separate from the trading engine on purpose:

  * it needs no Kalshi trading credentials and holds no wallet — both venue
    clients here are read-only, and the Polymarket one has no order-signing
    path at all;
  * it writes only `pm_*`, `kx_*` and `collect_cycles`, which the engine
    never reads, so it can be restarted, backfilled or broken without
    touching live betting;
  * it runs on its own cadence (`BETX_COLLECT_INTERVAL_SEC`, default 300s),
    independent of the decision loop's `BETX_POLL_INTERVAL_SEC`.
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
from betx.collector import Collector
from betx.db import Store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("betx.collect")

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
    parser.add_argument("--interval", type=int, default=None,
                        help="override BETX_COLLECT_INTERVAL_SEC")
    parser.add_argument("--venue", choices=("pm", "kalshi"), default=None,
                        help="run only one venue this process")
    args = parser.parse_args()

    cfg = config.load()
    if args.venue == "pm":
        cfg.kx_enabled = False
    elif args.venue == "kalshi":
        cfg.pm_enabled = False
    cfg.require("database_url", "arena_database_url")
    cfg.validate_collect()
    interval = args.interval or cfg.collect_interval_sec

    store = Store(cfg.database_url, cfg.db_schema)
    store.migrate()
    arena = ArenaDB(cfg.arena_database_url)
    collector = Collector(cfg, store, arena)

    log.info(
        "collector starting: instance=%s schema=%s venues=%s interval=%ss",
        cfg.instance_name, cfg.db_schema, cfg.collect_venues, interval,
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        if args.once:
            log.info("cycle done: %s", collector.run_cycle())
            return
        while not _shutdown:
            log.info("cycle done: %s", collector.run_cycle())
            target = next_boundary(time.time(), interval)
            while not _shutdown and time.time() < target:
                time.sleep(1)
    finally:
        collector.close()


if __name__ == "__main__":
    main()
