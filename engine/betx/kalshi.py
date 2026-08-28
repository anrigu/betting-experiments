"""Minimal Kalshi trade-api v2 client, sync httpx.

Two hosts: market data (markets, orderbooks, events, public trades) goes to
Kalshi's unauthenticated external API (`external-api.kalshi.com`) with a
fallback to the signed host on outage; portfolio and order endpoints are
always RSA-PSS signed against the trading host.

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
    def __init__(
        self,
        base_url: str,
        api_key_id: str = "",
        private_key_pem: bytes = b"",
        timeout: float = 30.0,
        public_base_url: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.public_base_url = public_base_url.rstrip("/")
        self.api_key_id = api_key_id
        # Credentials are optional so the client can run public-market-data-only
        # (scripts, backtests); any signed endpoint then raises.
        self._key = serialization.load_pem_private_key(private_key_pem, password=None) if private_key_pem else None
        self._http = httpx.Client(timeout=timeout)

    # --- auth ---
    def _headers(self, method: str, path: str) -> dict[str, str]:
        if self._key is None:
            raise RuntimeError("Kalshi API credentials not configured (signed endpoint requested)")
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

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
        retries: int = 3,
        auth: bool = True,
    ) -> dict:
        full_path = API_PREFIX + path
        base = self.base_url if auth else self.public_base_url
        last: Exception | None = None
        for attempt in range(retries):
            try:
                r = self._http.request(
                    method,
                    base + full_path,
                    params=params,
                    json=json_body,
                    headers=self._headers(method, full_path) if auth else {"Content-Type": "application/json"},
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

    def _public(self, path: str, params: Any) -> dict:
        """Market-data GET: unauthenticated external API first, signed host as
        fallback. Falls back on outage-shaped failures (transport, 429, 5xx)
        AND on 404 — the public host lags market creation by minutes, and a
        genuinely missing market 404s on the signed host too. Deterministic
        client errors (400/401/403) propagate directly.

        `params` is a dict, or a list of pairs for endpoints that take a
        repeated key (the batch orderbook endpoint wants `tickers` once per
        ticker, not a comma-joined string — comma-joining silently returns
        one empty book for the joined literal)."""
        if self.public_base_url:
            try:
                return self._request("GET", path, params=params, auth=False)
            except httpx.HTTPError as e:
                if self._key is None:
                    raise
                log.warning("public API failed for %s (%s); falling back to signed host", path, e)
            except KalshiError as e:
                if self._key is None or (e.status < 500 and e.status not in (404, 429)):
                    raise
                log.warning("public API failed for %s (%s); falling back to signed host", path, e)
        return self._request("GET", path, params=params)

    def get_public(self, path: str, **params) -> dict:
        return self._public(path, {k: v for k, v in params.items() if v is not None})

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

    # --- markets (public market-data API, signed fallback) ---
    def markets(self, status: str = "open", limit: int = 200, cursor: str | None = None, **kw) -> dict:
        return self.get_public("/markets", status=status, limit=limit, cursor=cursor, **kw)

    def market(self, ticker: str) -> dict:
        return self.get_public(f"/markets/{ticker}").get("market", {})

    def orderbook(self, ticker: str, depth: int | None = None) -> dict:
        """Returns the raw book dict — FULL depth unless `depth` is given.
        Current API shape is `orderbook_fp`
        ({"yes_dollars": [["0.4000","90.00"], ...], "no_dollars": [...]} —
        BID levels in dollar strings); older shape was `orderbook`
        ({"yes": [[40, 90], ...]} in cents)."""
        resp = self.get_public(f"/markets/{ticker}/orderbook", depth=depth)
        return resp.get("orderbook_fp") or resp.get("orderbook") or {}

    # Batch market-data reads. The venue caps `tickers` at 100 per request
    # (200 is a 400), so callers chunk.
    BATCH_MAX = 100

    def orderbooks(self, tickers: list[str]) -> dict[str, dict]:
        """Full-depth books for many tickers, keyed by ticker. Chunked at
        BATCH_MAX; a failed chunk is logged and skipped, never fatal."""
        out: dict[str, dict] = {}
        uniq = list(dict.fromkeys(t for t in tickers if t))
        for i in range(0, len(uniq), self.BATCH_MAX):
            chunk = uniq[i:i + self.BATCH_MAX]
            try:
                resp = self._public("/markets/orderbooks", [("tickers", t) for t in chunk])
            except KalshiError as e:
                log.warning("batch orderbooks failed for %d tickers (%s)", len(chunk), e)
                continue
            for ob in resp.get("orderbooks") or []:
                t = ob.get("ticker")
                if t:
                    out[t] = ob.get("orderbook_fp") or ob.get("orderbook") or {}
        return out

    def markets_by_tickers(self, tickers: list[str]) -> dict[str, dict]:
        """Market payloads for many tickers, keyed by ticker."""
        out: dict[str, dict] = {}
        uniq = list(dict.fromkeys(t for t in tickers if t))
        for i in range(0, len(uniq), self.BATCH_MAX):
            chunk = uniq[i:i + self.BATCH_MAX]
            try:
                resp = self._public("/markets", {"tickers": ",".join(chunk), "limit": self.BATCH_MAX})
            except KalshiError as e:
                log.warning("batch markets failed for %d tickers (%s)", len(chunk), e)
                continue
            for m in resp.get("markets") or []:
                if m.get("ticker"):
                    out[m["ticker"]] = m
        return out

    def market_trades(self, ticker: str, limit: int = 100, cursor: str | None = None,
                      min_ts: int | None = None, max_ts: int | None = None) -> dict:
        """Public trade tape for one market (most recent first)."""
        return self.get_public("/markets/trades", ticker=ticker, limit=limit,
                               cursor=cursor, min_ts=min_ts, max_ts=max_ts)

    def recent_trades(self, ticker: str, limit: int = 100) -> list[dict]:
        return self.market_trades(ticker, limit=limit).get("trades", [])

    def events(self, status: str = "open", limit: int = 200, cursor: str | None = None, **kw) -> dict:
        return self.get_public("/events", status=status, limit=limit, cursor=cursor, **kw)

    def event(self, event_ticker: str, with_nested_markets: bool = False) -> dict:
        return self.get_public(f"/events/{event_ticker}", with_nested_markets=with_nested_markets or None)

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
