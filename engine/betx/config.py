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
    # the lane model: agentic harness gemini-3.1-pro (won the paired Brier
    # head-to-head vs the fixed-context variant on 603 common resolved events)
    arena_predictor: str = os.environ.get("BETX_ARENA_PREDICTOR", "agent-gemini-3.1-pro")
    # ingested for dashboard Brier comparison only, never bet
    reference_predictors: list[str] = field(
        default_factory=lambda: [
            s.strip()
            for s in os.environ.get("BETX_ARENA_REFERENCE_PREDICTORS", "gemini-3.1-pro").split(",")
            if s.strip()
        ]
    )

    # --- kalshi ---
    kalshi_base_url: str = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com")
    kalshi_api_key_id: str = os.environ.get("KALSHI_API_KEY_ID", "")
    kalshi_private_key_b64: str = os.environ.get("KALSHI_PRIVATE_KEY_B64", "")

    # --- execution ---
    dry_run: bool = _b("BETX_DRY_RUN", False)
    live_enabled: bool = _b("BETX_LIVE_ENABLED", True)
    poll_interval_sec: int = _i("BETX_POLL_INTERVAL_SEC", 600)
    order_expiration_sec: int = _i("BETX_ORDER_EXPIRATION_SEC", 300)
    backfill_hours: float = _f("BETX_BACKFILL_HOURS", 12.0)

    # --- gates ---
    # no bets within this many hours of market close; fail-closed on missing close time
    close_buffer_hours: float = _f("BETX_CLOSE_BUFFER_HOURS", 24.0)

    # --- lanes: {name: starting_bankroll_usd}; both trade the same account,
    # attribution is per-order via the strategy column ---
    lane_bankrolls: dict[str, float] = field(
        default_factory=lambda: {
            "momentum": _f("BETX_BANKROLL_MOMENTUM", 150.0),
            "fundamental": _f("BETX_BANKROLL_FUNDAMENTAL", 150.0),
        }
    )

    # --- optional own-schedule predictor mode (unused in arena-loop mode) ---
    model_spec: str = os.environ.get("BETX_MODEL_SPEC", "gemini:gemini-3.1-pro-preview")
    predictor_url: str = os.environ.get("PREDICTOR_SERVICE_URL", "")
    predictor_api_key: str = os.environ.get("PREDICTOR_API_KEY", "")
    gemini_api_key: str = os.environ.get("GEMINI_API_KEY", "")

    @property
    def lanes(self) -> list[str]:
        return list(self.lane_bankrolls.keys())

    @property
    def kalshi_private_key_pem(self) -> bytes:
        return base64.b64decode(self.kalshi_private_key_b64)

    def require(self, *names: str) -> None:
        missing = [n for n in names if not getattr(self, n)]
        if missing:
            raise RuntimeError(f"Missing required config: {missing}")


def load() -> Config:
    return Config()
