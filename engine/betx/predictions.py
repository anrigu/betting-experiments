"""Prediction sources.

Two modes:
  * predictor — our own schedule: for each candidate Kalshi market, call the
    AI Prophet predictor service (same one the kalshi-trading fleet used)
    with model_spec (default gemini:gemini-3.1-pro-preview). Returns p_yes,
    confidence, reasoning, sources and (when available) token usage in
    `analysis`, which we convert to running LLM cost.
  * arena — loop into ProphetArena's stored predictions for the same model
    and map them onto Kalshi tickers.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from . import costs

log = logging.getLogger(__name__)


@dataclass
class Prediction:
    ticker: str
    p_model: float
    confidence: float | None
    reasoning: str
    source: str
    model_spec: str
    harness: str | None = None
    external_id: str | None = None
    latency_ms: int | None = None
    usage: dict[str, int] = field(default_factory=dict)   # input/output/cached tokens
    cost_usd: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


class PredictorSource:
    """Calls the deployed AI Prophet predictor service."""

    def __init__(self, url: str, api_key: str, model_spec: str, gemini_api_key: str = "", harness: str = "predictor-service"):
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.model_spec = model_spec
        self.gemini_api_key = gemini_api_key
        self.harness = harness
        self._http = httpx.Client(timeout=httpx.Timeout(300.0, connect=20.0))

    def predict(self, market_info: dict[str, Any], instance_name: str) -> Prediction | None:
        body = {
            "model_spec": self.model_spec,
            "market_info": market_info,
            "instance_name": instance_name,
        }
        if self.gemini_api_key:
            body["api_keys"] = {"gemini": self.gemini_api_key}
        t0 = time.monotonic()
        try:
            r = self._http.post(
                self.url + "/predict",
                json=body,
                headers={"x-api-key": self.api_key},
            )
        except httpx.HTTPError as e:
            log.warning("predictor request failed for %s: %s", market_info.get("ticker"), e)
            return None
        latency_ms = int((time.monotonic() - t0) * 1000)
        if r.status_code != 200:
            log.warning("predictor %s for %s: %s", r.status_code, market_info.get("ticker"), r.text[:200])
            return None
        data = r.json()
        provider, _, model = self.model_spec.partition(":")
        analysis = data.get("analysis") or {}
        usage = _extract_usage(analysis)
        cost = costs.cost_usd(provider, model, usage.get("input_tokens", 0), usage.get("output_tokens", 0), usage.get("cached_tokens", 0)) if usage else None
        return Prediction(
            ticker=market_info.get("ticker", ""),
            p_model=float(data["p_yes"]),
            confidence=data.get("confidence"),
            reasoning=data.get("reasoning", ""),
            source="predictor",
            model_spec=self.model_spec,
            harness=self.harness,
            latency_ms=latency_ms,
            usage=usage,
            cost_usd=cost,
            raw=data,
        )


def _extract_usage(analysis: dict[str, Any]) -> dict[str, int]:
    """Pull token usage out of the predictor's analysis blob (best effort)."""
    for key in ("usage", "token_usage", "usage_metadata"):
        u = analysis.get(key)
        if isinstance(u, dict):
            return {
                "input_tokens": int(u.get("input_tokens") or u.get("prompt_token_count") or u.get("prompt_tokens") or 0),
                "output_tokens": int(u.get("output_tokens") or u.get("candidates_token_count") or u.get("completion_tokens") or 0),
                "cached_tokens": int(u.get("cached_tokens") or u.get("cached_content_token_count") or 0),
            }
    return {}


class ArenaSource:
    """Reads predictions already made on ProphetArena for our model and maps
    them to Kalshi tickers. Only usable for events that ProphetArena mirrors
    from Kalshi (arena market ids embed the source ticker)."""

    def __init__(self, api_base: str, model_spec: str):
        self.api = api_base.rstrip("/")
        self.model_spec = model_spec
        self._http = httpx.Client(timeout=60.0)

    def recent_predictions(self, limit: int = 200) -> list[Prediction]:
        """Fetch recent arena predictions for the configured model.

        NOTE: endpoint availability is verified at runtime; if the arena API
        does not expose predictions publicly this source raises and the
        engine falls back to the predictor schedule.
        """
        r = self._http.get(
            f"{self.api}/api/predictions/recent",
            params={"model": self.model_spec, "limit": limit},
        )
        r.raise_for_status()
        out: list[Prediction] = []
        for item in r.json().get("predictions", []):
            ticker = item.get("source_ticker") or item.get("kalshi_ticker") or ""
            if not ticker:
                continue
            out.append(
                Prediction(
                    ticker=ticker,
                    p_model=float(item["p_yes"]),
                    confidence=item.get("confidence"),
                    reasoning=item.get("reasoning", ""),
                    source="arena",
                    model_spec=self.model_spec,
                    harness=item.get("harness"),
                    external_id=str(item.get("id")),
                    raw=item,
                )
            )
        return out
