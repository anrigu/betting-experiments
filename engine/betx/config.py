"""Central configuration, loaded from environment variables.

All knobs for the experiment live here so a run is fully described by env +
this file's defaults. Strategy math itself has NO knobs — it is the
framework's live-betting `decide` / `decide_fundamental`, ported verbatim in
strategies.py; the code is the config.
"""
from __future__ import annotations

import base64
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Lane:
    """One virtual book: an arena predictor betting one strategy with its own
    bankroll. All lanes share the physical Kalshi account."""
    name: str
    predictor: str
    strategy: str          # "fundamental" | "momentum"
    bankroll: float
    sizing: str = "brier"  # proper-scoring sizing rule: brier | log | spherical


def _b(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _f(name: str, default: float) -> float:
    v = os.environ.get(name)
    return float(v) if v not in (None, "") else default


def _i(name: str, default: int) -> int:
    v = os.environ.get(name)
    return int(v) if v not in (None, "") else default


@dataclass
class Config:
    # --- identity ---
    instance_name: str = os.environ.get("BETX_INSTANCE", "betx-live-1")
    experiment: str = os.environ.get("BETX_EXPERIMENT", "betting-experiments-v1")

    # --- database (our audit store) ---
    database_url: str = os.environ.get("DATABASE_URL", "")
    db_schema: str = os.environ.get("BETX_DB_SCHEMA", "betting_experiments")

    # --- prediction source: ProphetArena production DB ---
    arena_database_url: str = os.environ.get("ARENA_DATABASE_URL", "")
    # ingested for dashboard Brier comparison only, never bet
    reference_predictors: list[str] = field(
        default_factory=lambda: [
            s.strip()
            for s in os.environ.get(
                "BETX_ARENA_REFERENCE_PREDICTORS", "gemini-3.1-pro,claude-fable-5"
            ).split(",")
            if s.strip()
        ]
    )

    # --- kalshi ---
    kalshi_base_url: str = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com")
    # unauthenticated market-data host (markets/orderbooks/events/trades);
    # set empty to force everything through the signed host
    kalshi_public_base_url: str = os.environ.get("KALSHI_PUBLIC_BASE_URL", "https://external-api.kalshi.com")
    kalshi_api_key_id: str = os.environ.get("KALSHI_API_KEY_ID", "")
    kalshi_private_key_b64: str = os.environ.get("KALSHI_PRIVATE_KEY_B64", "")

    # --- collection (read-only; both venues, one worker, one cadence) ---
    # BETX_POLL_INTERVAL_SEC is the TRADING loop and stays separate on
    # purpose: collection cadence must be tunable without changing when the
    # engine decides trades.
    collect_interval_sec: int = _i("BETX_COLLECT_INTERVAL_SEC", 3600)

    # --- polymarket collection ---
    # Universe: "arena" = the PM events ProphetArena tracks (default),
    # "all" = every open order-book event on Gamma, "both" = the union.
    pm_enabled: bool = _b("BETX_PM_ENABLED", True)
    pm_universe: str = os.environ.get("BETX_PM_UNIVERSE", "arena")
    pm_include_closed: bool = _b("BETX_PM_INCLUDE_CLOSED", False)
    pm_extra_slugs: list[str] = field(
        default_factory=lambda: [
            s.strip() for s in os.environ.get("BETX_PM_EXTRA_SLUGS", "").split(",") if s.strip()
        ]
    )
    pm_max_events: int = _i("BETX_PM_MAX_EVENTS", 5000)
    pm_order_book_only: bool = _b("BETX_PM_ORDER_BOOK_ONLY", True)
    pm_timeout_sec: float = _f("BETX_PM_TIMEOUT_SEC", 30.0)
    # politeness throttle between HTTP calls; a full cycle is ~1 batched call
    # per 50 tokens plus one tape call per market
    pm_min_interval_sec: float = _f("BETX_PM_MIN_INTERVAL_SEC", 0.05)
    pm_batch_size: int = _i("BETX_PM_BATCH_SIZE", 50)

    # what to capture
    pm_collect_books: bool = _b("BETX_PM_COLLECT_BOOKS", True)
    pm_collect_quotes: bool = _b("BETX_PM_COLLECT_QUOTES", True)
    pm_collect_trades: bool = _b("BETX_PM_COLLECT_TRADES", True)
    pm_collect_history: bool = _b("BETX_PM_COLLECT_HISTORY", True)
    pm_collect_predictions: bool = _b("BETX_PM_COLLECT_PREDICTIONS", True)
    # off by default: an unchanged book is still an observation, and the
    # venue hash makes the duplicate cheap to filter at read time
    pm_skip_unchanged_books: bool = _b("BETX_PM_SKIP_UNCHANGED_BOOKS", False)

    # cadences, in collector cycles
    pm_trades_every_n_cycles: int = _i("BETX_PM_TRADES_EVERY_N", 1)
    pm_trades_page_size: int = _i("BETX_PM_TRADES_PAGE_SIZE", 500)
    pm_trades_max_pages: int = _i("BETX_PM_TRADES_MAX_PAGES", 20)        # cold start
    pm_trades_max_pages_warm: int = _i("BETX_PM_TRADES_MAX_PAGES_WARM", 5)
    # Price history is cumulative: running it less often batches more points
    # rather than storing fewer, so EVERY_N controls API calls and FIDELITY
    # controls rows. Hourly (60) instead of per-minute cuts this table 60x;
    # the book snapshots carry the high-resolution record.
    pm_history_every_n_cycles: int = _i("BETX_PM_HISTORY_EVERY_N", 1)
    pm_history_fidelity: int = _i("BETX_PM_HISTORY_FIDELITY", 60)
    pm_history_cold_interval: str = os.environ.get("BETX_PM_HISTORY_COLD_INTERVAL", "max")

    # empty = every predictor arena has, not just the betting lanes
    pm_predictors: list[str] = field(
        default_factory=lambda: [
            s.strip() for s in os.environ.get("BETX_PM_PREDICTORS", "").split(",") if s.strip()
        ]
    )
    pm_backfill_hours: float = _f("BETX_PM_BACKFILL_HOURS", 72.0)

    # --- kalshi collection (read-only; same isolation as the pm_* side) ---
    # Arena tracks ~15k open Kalshi markets; all of them every cycle is
    # ~173 GB/month of rows. The scope is therefore driven by FORECAST
    # RECENCY, which is what makes a market interesting, and NOT by close
    # time: the engine's only close gate is "no bets within 24h of close",
    # so it will happily trade a market resolving months out. Filtering on
    # close time silently drops markets the engine acts on, so
    # BETX_KX_CLOSING_WITHIN_DAYS defaults to 0 (off) and exists only as a
    # cost lever of last resort. Long-dated books are cheap anyway — they
    # barely move, so the unchanged-book skip absorbs them.
    # Either window: 0 or negative disables it.
    kx_enabled: bool = _b("BETX_KX_ENABLED", True)
    kx_include_closed: bool = _b("BETX_KX_INCLUDE_CLOSED", False)
    kx_forecast_within_days: float = _f("BETX_KX_FORECAST_WITHIN_DAYS", 30.0)
    kx_closing_within_days: float = _f("BETX_KX_CLOSING_WITHIN_DAYS", 0.0)
    kx_max_markets: int = _i("BETX_KX_MAX_MARKETS", 40000)
    kx_collect_books: bool = _b("BETX_KX_COLLECT_BOOKS", True)
    kx_collect_trades: bool = _b("BETX_KX_COLLECT_TRADES", True)
    # ON by default here, unlike Polymarket: most of these markets never move,
    # and Kalshi issues no book hash so we compute one. An unchanged book is
    # byte-identical to the previous row.
    kx_skip_unchanged_books: bool = _b("BETX_KX_SKIP_UNCHANGED_BOOKS", True)
    # /markets/trades is one ticker per call, so the sweep is capped; markets
    # whose volume did not move are skipped before the cap applies.
    kx_trades_max_markets: int = _i("BETX_KX_TRADES_MAX_MARKETS", 1500)
    kx_trades_page_size: int = _i("BETX_KX_TRADES_PAGE_SIZE", 100)
    kx_trades_max_pages: int = _i("BETX_KX_TRADES_MAX_PAGES", 5)

    # --- streaming collection (engine/stream.py, separate worker) ---
    # Event-driven capture between polls. Measured on the live feeds, 400
    # Kalshi tickers emit ~2,900 orderbook deltas in 40s, so books are kept
    # in memory and a snapshot is written only on change and at most once
    # per STREAM_MIN_INTERVAL_SEC per market. Trades are always written.
    # OFF by default. The feeds are ~1,400 deltas/sec at full scope and even
    # rate-limited to one snapshot per market per 30s they add ~89 GB/month
    # of book rows, roughly 5x the polling total. Turn on only when
    # sub-minute book dynamics are actually the question.
    stream_enabled: bool = _b("BETX_STREAM_ENABLED", False)
    stream_pm_enabled: bool = _b("BETX_STREAM_PM_ENABLED", True)
    stream_kx_enabled: bool = _b("BETX_STREAM_KX_ENABLED", True)
    stream_min_interval_sec: float = _f("BETX_STREAM_MIN_INTERVAL_SEC", 30.0)
    stream_flush_sec: float = _f("BETX_STREAM_FLUSH_SEC", 10.0)
    stream_pm_chunk: int = _i("BETX_STREAM_PM_CHUNK", 400)
    stream_kx_max_tickers: int = _i("BETX_STREAM_KX_MAX_TICKERS", 5000)
    stream_idle_timeout_sec: float = _f("BETX_STREAM_IDLE_TIMEOUT_SEC", 120.0)
    # rebuild the universe and reconnect on this cadence, so newly listed
    # markets get picked up without a restart
    stream_refresh_sec: float = _f("BETX_STREAM_REFRESH_SEC", 3600.0)

    # --- execution ---
    dry_run: bool = _b("BETX_DRY_RUN", False)
    live_enabled: bool = _b("BETX_LIVE_ENABLED", True)
    poll_interval_sec: int = _i("BETX_POLL_INTERVAL_SEC", 600)
    order_expiration_sec: int = _i("BETX_ORDER_EXPIRATION_SEC", 300)
    backfill_hours: float = _f("BETX_BACKFILL_HOURS", 12.0)

    # --- gates ---
    # no bets within this many hours of market close; fail-closed on missing close time
    close_buffer_hours: float = _f("BETX_CLOSE_BUFFER_HOURS", 24.0)

    # --- lanes ---
    # Virtual books on one Kalshi account: each lane is
    # name:arena_predictor:strategy:bankroll_usd[:sizing] (comma-separated;
    # sizing = brier|log|spherical, default brier). Attribution is per-order
    # via the strategy column, which stores the LANE NAME.
    lanes_spec: str = os.environ.get(
        "BETX_LANES",
        "gemini:agent-gemini-3.1-pro:fundamental:150:brier,"
        "fable-5:agent-claude-fable-5:fundamental:150:brier,"
        "gemini-log:agent-gemini-3.1-pro:fundamental:150:log",
    )

    # --- optional own-schedule predictor mode (unused in arena-loop mode) ---
    model_spec: str = os.environ.get("BETX_MODEL_SPEC", "gemini:gemini-3.1-pro-preview")
    predictor_url: str = os.environ.get("PREDICTOR_SERVICE_URL", "")
    predictor_api_key: str = os.environ.get("PREDICTOR_API_KEY", "")
    gemini_api_key: str = os.environ.get("GEMINI_API_KEY", "")

    @property
    def lanes(self) -> list["Lane"]:
        out = []
        for part in self.lanes_spec.split(","):
            part = part.strip()
            if not part:
                continue
            fields = part.split(":")
            if len(fields) == 4:
                name, predictor, strategy, bankroll = fields
                sizing = "brier"
            else:
                name, predictor, strategy, bankroll, sizing = fields
            if strategy not in ("fundamental", "momentum"):
                raise RuntimeError(f"unknown lane strategy {strategy!r} in BETX_LANES")
            if sizing not in ("brier", "log", "spherical"):
                raise RuntimeError(f"unknown lane sizing {sizing!r} in BETX_LANES")
            out.append(Lane(name.strip(), predictor.strip(), strategy.strip(), float(bankroll), sizing.strip()))
        if len({l.name for l in out}) != len(out):
            raise RuntimeError("duplicate lane names in BETX_LANES")
        return out

    def validate_collect(self) -> None:
        if self.pm_universe not in ("arena", "all", "both"):
            raise RuntimeError(
                f"unknown BETX_PM_UNIVERSE {self.pm_universe!r} (arena|all|both)"
            )
        if not (self.pm_enabled or self.kx_enabled):
            raise RuntimeError(
                "both BETX_PM_ENABLED and BETX_KX_ENABLED are off; nothing to collect"
            )

    @staticmethod
    def _window(days: float) -> float | None:
        """A scope window in days, or None when disabled. Passing 0.0 straight
        through would mean `close_time < now()`, i.e. exclude everything —
        the opposite of 'no limit'."""
        return days if days and days > 0 else None

    @property
    def kx_forecast_window(self) -> float | None:
        return self._window(self.kx_forecast_within_days)

    @property
    def kx_closing_window(self) -> float | None:
        return self._window(self.kx_closing_within_days)

    @property
    def collect_venues(self) -> list[str]:
        return (["polymarket"] if self.pm_enabled else []) + (["kalshi"] if self.kx_enabled else [])

    @property
    def bet_predictors(self) -> list[str]:
        return list(dict.fromkeys(l.predictor for l in self.lanes))

    def lanes_for(self, predictor: str) -> list["Lane"]:
        return [l for l in self.lanes if l.predictor == predictor]

    @property
    def kalshi_private_key_pem(self) -> bytes:
        return base64.b64decode(self.kalshi_private_key_b64)

    def require(self, *names: str) -> None:
        missing = [n for n in names if not getattr(self, n)]
        if missing:
            raise RuntimeError(f"Missing required config: {missing}")


def load() -> Config:
    return Config()
