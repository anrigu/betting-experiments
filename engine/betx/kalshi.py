"""Minimal Kalshi trade-api v2 client (RSA-PSS signed), sync httpx.

Verified against the live API with the production account credentials.
"""
from __future__ import annotations

import base64
import datetime as dt
import logging
import time
import uuid
from typing import Any

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

log = logging.getLogger(__name__)

API_PREFIX = "/trade-api/v2"


def dollars(v: Any) -> float | None:
    """Parse a Kalshi dollars value (string like '0.4000' or float)."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def money_usd(obj: dict, cents_key: str) -> float | None:
    """Money in dollars from a payload that may carry either
    `<key>_dollars` (string dollars, current API) or `<key>` (cents, legacy).
    Never sums aliases — dollars wins when present."""
    d = dollars(obj.get(f"{cents_key}_dollars"))
    if d is not None:
        return d
    c = obj.get(cents_key)
    if c is None:
        return None
    try:
        return float(c) / 100.0
    except (TypeError, ValueError):
        return None


def count_of(obj: dict, key: str) -> int:
    """Contract count from `<key>` (int, legacy) or `<key>_fp` (string float)."""
    v = obj.get(key)
    if v is None:
        v = obj.get(f"{key}_fp")
    if v is None:
        return 0
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def market_quotes(m: dict) -> dict:
    """Top-of-book + last in CENTS ints (or None) from either API shape."""
    def cents(key: str) -> int | None:
        usd = money_usd(m, key)
        return int(round(usd * 100)) if usd is not None else None

    return {
        "yes_bid": cents("yes_bid"),
        "yes_ask": cents("yes_ask"),
        "no_bid": cents("no_bid"),
        "no_ask": cents("no_ask"),
        "last_price": cents("last_price"),
        "liquidity": money_usd(m, "liquidity"),
    }


class KalshiError(RuntimeError):
    def __init__(self, status: int, body: Any):
        super().__init__(f"Kalshi API error {status}: {body}")
        self.status = status
        self.body = body


class KalshiClient:
    def __init__(self, base_url: str, api_key_id: str, private_key_pem: bytes, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.api_key_id = api_key_id
        self._key = serialization.load_pem_private_key(private_key_pem, password=None)
        self._http = httpx.Client(timeout=timeout)

    # --- auth ---
    def _headers(self, method: str, path: str) -> dict[str, str]:
        ts = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))
        msg = f"{ts}{method}{path}".encode()
        sig = self._key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None, retries: int = 3) -> dict:
        full_path = API_PREFIX + path
        last: Exception | None = None
        for attempt in range(retries):
            try:
                r = self._http.request(
                    method,
                    self.base_url + full_path,
                    params=params,
                    json=json_body,
                    headers=self._headers(method, full_path),
                )
            except httpx.HTTPError as e:
                last = e
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code == 429 or r.status_code >= 500:
                last = KalshiError(r.status_code, r.text[:300])
                time.sleep(2.0 * (attempt + 1))
                continue
            if r.status_code >= 400:
                raise KalshiError(r.status_code, r.text[:500])
            return r.json() if r.content else {}
        raise last if last else RuntimeError("kalshi request failed")

    def get(self, path: str, **params) -> dict:
        return self._request("GET", path, params={k: v for k, v in params.items() if v is not None})

    def post(self, path: str, body: dict) -> dict:
        return self._request("POST", path, json_body=body)

    def delete(self, path: str) -> dict:
        return self._request("DELETE", path)

    # --- portfolio ---
    def balance(self) -> dict:
        return self.get("/portfolio/balance")

    def positions(self, limit: int = 200, cursor: str | None = None, **kw) -> dict:
        return self.get("/portfolio/positions", limit=limit, cursor=cursor, **kw)

    def all_positions(self) -> list[dict]:
        out: list[dict] = []
        cursor = None
        while True:
            page = self.positions(cursor=cursor)
            out.extend(page.get("market_positions", []))
            cursor = page.get("cursor")
            if not cursor:
                return out

    def orders(self, status: str | None = None, ticker: str | None = None, limit: int = 100, cursor: str | None = None) -> dict:
        return self.get("/portfolio/orders", status=status, ticker=ticker, limit=limit, cursor=cursor)

    def fills(self, ticker: str | None = None, order_id: str | None = None, limit: int = 100, cursor: str | None = None) -> dict:
        return self.get("/portfolio/fills", ticker=ticker, order_id=order_id, limit=limit, cursor=cursor)

    def all_fills(self, min_ts: int | None = None) -> list[dict]:
        out: list[dict] = []
        cursor = None
        while True:
            page = self.get("/portfolio/fills", limit=200, cursor=cursor, min_ts=min_ts)
            out.extend(page.get("fills", []))
            cursor = page.get("cursor")
            if not cursor:
                return out

    def settlements(self, limit: int = 100, cursor: str | None = None) -> dict:
        return self.get("/portfolio/settlements", limit=limit, cursor=cursor)

    def all_settlements(self) -> list[dict]:
        out: list[dict] = []
        cursor = None
        while True:
            page = self.settlements(limit=200, cursor=cursor)
            out.extend(page.get("settlements", []))
            cursor = page.get("cursor")
            if not cursor:
                return out

    # --- markets ---
    def markets(self, status: str = "open", limit: int = 200, cursor: str | None = None, **kw) -> dict:
        return self.get("/markets", status=status, limit=limit, cursor=cursor, **kw)

    def market(self, ticker: str) -> dict:
        return self.get(f"/markets/{ticker}").get("market", {})

    def orderbook(self, ticker: str, depth: int = 16) -> dict:
        """Returns the raw book dict. Current API shape is `orderbook_fp`
        ({"yes_dollars": [["0.4000","90.00"], ...], "no_dollars": [...]} —
        BID levels in dollar strings); older shape was `orderbook`
        ({"yes": [[40, 90], ...]} in cents)."""
        resp = self.get(f"/markets/{ticker}/orderbook", depth=depth)
        return resp.get("orderbook_fp") or resp.get("orderbook") or {}

    def events(self, status: str = "open", limit: int = 200, cursor: str | None = None, **kw) -> dict:
        return self.get("/events", status=status, limit=limit, cursor=cursor, **kw)

    # --- trading ---
    def create_order(
        self,
        ticker: str,
        side: str,               # "yes" | "no"
        count: int,
        price_cents: int,        # limit price for the chosen side
        action: str = "buy",
        order_type: str = "limit",
        client_order_id: str | None = None,
        expiration_ts: int | None = None,
    ) -> dict:
        body: dict[str, Any] = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": order_type,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if order_type == "limit":
            if side == "yes":
                body["yes_price"] = price_cents
            else:
                body["no_price"] = price_cents
        if expiration_ts:
            body["expiration_ts"] = expiration_ts
        return self.post("/portfolio/orders", body)

    def cancel_order(self, order_id: str) -> dict:
        return self.delete(f"/portfolio/orders/{order_id}")

    def close(self) -> None:
        self._http.close()
