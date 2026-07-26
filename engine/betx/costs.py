"""LLM pricing table and cost computation for running-cost tracking.

Prices are USD per 1M tokens. Update here when providers change pricing.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Price:
    input_per_m: float
    output_per_m: float
    cached_input_per_m: float = 0.0


# Keyed by (provider, model) with prefix matching on model.
PRICES: dict[tuple[str, str], Price] = {
    ("gemini", "gemini-3.1-pro-preview"): Price(2.00, 12.00, 0.20),
    ("gemini", "gemini-3-pro-preview"): Price(2.00, 12.00, 0.20),
    ("gemini", "gemini-2.5-pro"): Price(1.25, 10.00, 0.125),
    ("gemini", "gemini-2.5-flash"): Price(0.30, 2.50, 0.03),
    ("openai", "gpt-5"): Price(1.25, 10.00, 0.125),
    ("anthropic", "claude-sonnet-5"): Price(3.00, 15.00, 0.30),
    ("anthropic", "claude-opus-5"): Price(15.00, 75.00, 1.50),
}


def price_for(provider: str, model: str) -> Price | None:
    provider = (provider or "").lower()
    model = (model or "").lower()
    best: tuple[int, Price] | None = None
    for (p, m), price in PRICES.items():
        if p == provider and model.startswith(m):
            if best is None or len(m) > best[0]:
                best = (len(m), price)
    return best[1] if best else None


def cost_usd(provider: str, model: str, input_tokens: int, output_tokens: int, cached_tokens: int = 0) -> float | None:
    """Return cost in USD, or None if the model is unknown (store tokens anyway)."""
    price = price_for(provider, model)
    if price is None:
        return None
    fresh_in = max(input_tokens - cached_tokens, 0)
    return round(
        fresh_in / 1e6 * price.input_per_m
        + cached_tokens / 1e6 * price.cached_input_per_m
        + output_tokens / 1e6 * price.output_per_m,
        6,
    )
