"""Polymarket read-only market-data client.

Three public hosts, no authentication anywhere:

  * ``gamma-api.polymarket.com`` — event/market metadata: slugs, condition
    ids, CLOB token ids, tick size, minimum order size, the neg-risk flag,
    the per-market fee schedule.
  * ``clob.polymarket.com`` — full-depth order books (batchable), midpoints,
    spreads, and 1-minute price history.
  * ``data-api.polymarket.com`` — the public trade tape. The CLOB's own
    ``/trades`` is key-gated and 401s; this host is open.

This module never signs or submits an order: no wallet, no EIP-712, no
private key, no allowance. Collection only — every call is a GET or a
read-only POST batch lookup.

Shape notes, all verified against the live API:
  * Gamma encodes ``outcomes``, ``clobTokenIds`` and ``outcomePrices`` as
    JSON-encoded *strings*, not arrays.
  * A Polymarket market is a PAIR of ERC-1155 tokens. The NO book is its own
    book, not the complement of the YES book — unlike Kalshi, where one
    ticker carries both sides.
  * A multi-outcome event is N binary markets (neg-risk), each with its own
    condition id and its own ``groupItemTitle``. A plain binary event is ONE
    market with a blank ``groupItemTitle`` whose two outcomes are named by
    its ``outcomes`` array.
  * ``tick_size`` varies per market (0.01 and 0.001 both occur inside a
    single event) and ``min_order_size`` is 5, not 1.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Iterable

import httpx

log = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

# Arena tags Polymarket events as `PM-<polymarket event slug>`; the strip is
# exact for every PM event arena currently carries (41/41 resolve on Gamma).
ARENA_PM_PREFIX = "PM-"


def slug_from_arena_ticker(event_ticker: str) -> str | None:
    if not event_ticker or not event_ticker.startswith(ARENA_PM_PREFIX):
        return None
    return event_ticker[len(ARENA_PM_PREFIX):] or None


# ------------------------------------------------------------------ parsing
def _jload(v: Any) -> list:
    """Gamma encodes list fields as JSON strings ('["Yes", "No"]')."""
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            out = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            return []
        return out if isinstance(out, list) else []
    return []


def _f(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _b(v: Any) -> bool | None:
    return bool(v) if isinstance(v, bool) else None


def _ts(v: Any) -> dt.datetime | None:
    """ISO-8601 (Gamma) or epoch seconds (CLOB/data-api) -> aware UTC."""
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)
    if isinstance(v, (int, float)):
        try:
            return dt.datetime.fromtimestamp(float(v), tz=dt.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    s = str(v).strip()
    if s.isdigit():
        return _ts(int(s))
    try:
        out = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Gamma sometimes returns a date-only endDate; leaving it naive would let
    # the DB read it in the session timezone.
    return out if out.tzinfo else out.replace(tzinfo=dt.timezone.utc)


_DASHES = dict.fromkeys(map(ord, "‐‑‒–—―−"), "-")


def norm_title(s: str | None) -> str:
    """Fold an outcome label for matching arena titles to Polymarket ones:
    NFKC, unicode dashes -> '-', collapse whitespace, casefold. Arena and
    Gamma agree on the text but not always on the dash or the spacing."""
    if not s:
        return ""
    out = unicodedata.normalize("NFKC", str(s)).translate(_DASHES)
    return re.sub(r"\s+", " ", out).strip().casefold()


# ------------------------------------------------------------------- models
@dataclass(frozen=True)
class PMToken:
    """One ERC-1155 outcome token — the unit an order book belongs to."""
    condition_id: str
    token_id: str
    outcome: str
    outcome_index: int


@dataclass
class PMMarket:
    condition_id: str
    market_id: str
    event_slug: str
    question: str
    slug: str
    group_item_title: str
    outcomes: list[str]
    token_ids: list[str]
    tick_size: float | None
    min_order_size: float | None
    neg_risk: bool | None
    maker_base_fee: float | None
    taker_base_fee: float | None
    fees_enabled: bool | None
    # Polymarket DOES charge fees, and the rate is category-specific:
    # feeType 'tech_fees'/'culture_fees'/'sports_fees_v2', each with a
    # feeSchedule {exponent, rate, takerOnly, rebateRate}. Same shape family
    # as Kalshi's rate*C*P*(1-P), so a future gate is largely portable.
    fee_type: str | None
    fee_schedule: dict
    accepting_orders: bool | None
    closed: bool | None
    active: bool | None
    enable_order_book: bool | None
    end_date: dt.datetime | None
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    last_trade_price: float | None
    outcome_prices: list[str]
    volume: float | None
    volume_24h: float | None
    liquidity: float | None
    one_day_price_change: float | None
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def tokens(self) -> list[PMToken]:
        out = []
        for i, tid in enumerate(self.token_ids):
            if not tid:
                continue
            label = self.outcomes[i] if i < len(self.outcomes) else f"outcome_{i}"
            out.append(PMToken(self.condition_id, str(tid), str(label), i))
        return out

    @classmethod
    def from_gamma(cls, m: dict, event_slug: str) -> "PMMarket":
        return cls(
            condition_id=str(m.get("conditionId") or ""),
            market_id=str(m.get("id") or ""),
            event_slug=event_slug,
            question=m.get("question") or "",
            slug=m.get("slug") or "",
            group_item_title=(m.get("groupItemTitle") or "").strip(),
            outcomes=[str(o) for o in _jload(m.get("outcomes"))],
            token_ids=[str(t) for t in _jload(m.get("clobTokenIds"))],
            tick_size=_f(m.get("orderPriceMinTickSize")),
            min_order_size=_f(m.get("orderMinSize")),
            neg_risk=_b(m.get("negRisk")),
            maker_base_fee=_f(m.get("makerBaseFee")),
            taker_base_fee=_f(m.get("takerBaseFee")),
            fees_enabled=_b(m.get("feesEnabled")),
            fee_type=m.get("feeType") or None,
            fee_schedule=m.get("feeSchedule") if isinstance(m.get("feeSchedule"), dict) else {},
            accepting_orders=_b(m.get("acceptingOrders")),
            closed=_b(m.get("closed")),
            active=_b(m.get("active")),
            enable_order_book=_b(m.get("enableOrderBook")),
            end_date=_ts(m.get("endDateIso") or m.get("endDate")),
            best_bid=_f(m.get("bestBid")),
            best_ask=_f(m.get("bestAsk")),
            spread=_f(m.get("spread")),
            last_trade_price=_f(m.get("lastTradePrice")),
            outcome_prices=[str(p) for p in _jload(m.get("outcomePrices"))],
            volume=_f(m.get("volumeNum")) or _f(m.get("volume")),
            volume_24h=_f(m.get("volume24hr")),
            liquidity=_f(m.get("liquidityNum")) or _f(m.get("liquidity")),
            one_day_price_change=_f(m.get("oneDayPriceChange")),
            raw=m,
        )


@dataclass
class PMEvent:
    slug: str
    event_id: str
    title: str
    description: str
    neg_risk: bool | None
    closed: bool | None
    active: bool | None
    start_date: dt.datetime | None
    end_date: dt.datetime | None
    liquidity: float | None
    volume: float | None
    volume_24h: float | None
    open_interest: float | None
    markets: list[PMMarket]
    raw: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_gamma(cls, e: dict) -> "PMEvent":
        slug = e.get("slug") or ""
        return cls(
            slug=slug,
            event_id=str(e.get("id") or ""),
            title=e.get("title") or "",
            description=e.get("description") or "",
            neg_risk=_b(e.get("negRisk")),
            closed=_b(e.get("closed")),
            active=_b(e.get("active")),
            start_date=_ts(e.get("startDate")),
            end_date=_ts(e.get("endDate")),
            liquidity=_f(e.get("liquidity")),
            volume=_f(e.get("volume")),
            volume_24h=_f(e.get("volume24hr")),
            open_interest=_f(e.get("openInterest")),
            markets=[PMMarket.from_gamma(m, slug) for m in (e.get("markets") or [])],
            raw=e,
        )

    @property
    def tokens(self) -> list[PMToken]:
        """Every token in the event — both sides of every market. The NO book
        is real and separate, so collection must cover it."""
        return [t for m in self.markets for t in m.tokens]

    def match_outcome(self, arena_title: str) -> tuple[PMMarket, PMToken] | None:
        """Map one arena outcome name onto the token that pays $1 if it happens.

        Two shapes, both present in arena's current PM set:
          * multi-outcome (neg-risk) — each arena outcome IS its own binary
            market; match `groupItemTitle`, take that market's YES token
            (index 0);
          * single binary market — `groupItemTitle` is blank and the arena
            outcomes are the market's own `outcomes` entries ("Yes"/"No", or
            participant names like "Mayweather"/"Pacquiao"); take the token
            at that outcome's index.

        Matching is strict: if the event uses group titles at all we never
        fall through to outcome names, because "Yes" would then match the
        first sub-market of a neg-risk event rather than the intended one.
        """
        want = norm_title(arena_title)
        if not want:
            return None
        if any(m.group_item_title for m in self.markets):
            for m in self.markets:
                if norm_title(m.group_item_title) == want:
                    toks = m.tokens
                    return (m, toks[0]) if toks else None
            return None
        for m in self.markets:
            for t in m.tokens:
                if norm_title(t.outcome) == want:
                    return (m, t)
        return None


# ------------------------------------------------------------------- client
class PolymarketError(RuntimeError):
    def __init__(self, status: int, body: Any):
        super().__init__(f"Polymarket API error {status}: {body}")
        self.status = status
        self.body = body


def ladder(levels: Iterable[dict] | None, descending: bool) -> list[list[float]]:
    """Normalise a CLOB ladder to [[price, size], ...], best price first.

    The venue returns ascending price for both sides; 'best' means highest
    for bids and lowest for asks."""
    out: list[list[float]] = []
    for lv in levels or []:
        p, s = _f(lv.get("price")), _f(lv.get("size"))
        if p is None or s is None:
            continue
        out.append([p, s])
    out.sort(key=lambda x: x[0], reverse=descending)
    return out


class PolymarketClient:
    """Read-only. There is deliberately no order-signing path in this class."""

    def __init__(
        self,
        gamma_base: str = GAMMA_BASE,
        clob_base: str = CLOB_BASE,
        data_base: str = DATA_BASE,
        timeout: float = 30.0,
        retries: int = 3,
        min_interval_sec: float = 0.0,
        batch_size: int = 50,
    ):
        self.gamma_base = gamma_base.rstrip("/")
        self.clob_base = clob_base.rstrip("/")
        self.data_base = data_base.rstrip("/")
        self.retries = retries
        self.min_interval_sec = min_interval_sec
        self.batch_size = max(1, batch_size)
        self._last_call = 0.0
        self._http = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": "betx-collector/1.0", "Accept": "application/json"},
        )

    def close(self) -> None:
        self._http.close()

    # --- transport ---
    def _throttle(self) -> None:
        if self.min_interval_sec <= 0:
            return
        wait = self._last_call + self.min_interval_sec - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, *, params: Any = None, json_body: Any = None) -> Any:
        last: Exception | None = None
        for attempt in range(self.retries):
            self._throttle()
            try:
                r = self._http.request(method, url, params=params, json=json_body)
            except httpx.HTTPError as e:
                last = e
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code == 429 or r.status_code >= 500:
                last = PolymarketError(r.status_code, r.text[:300])
                time.sleep(2.0 * (attempt + 1))
                continue
            if r.status_code >= 400:
                raise PolymarketError(r.status_code, r.text[:500])
            if not r.content:
                return None
            try:
                return r.json()
            except ValueError as e:
                raise PolymarketError(r.status_code, f"non-JSON body: {r.text[:200]}") from e
        raise last if last else RuntimeError("polymarket request failed")

    def _chunks(self, items: list, size: int | None = None) -> Iterable[list]:
        n = size or self.batch_size
        for i in range(0, len(items), n):
            yield items[i:i + n]

    # --- gamma: metadata ---
    def events_by_slug(self, slugs: list[str]) -> list[PMEvent]:
        """Batch slug lookup. Slugs that do not resolve are simply absent from
        the result — the caller is responsible for noticing."""
        out: list[PMEvent] = []
        for chunk in self._chunks([s for s in slugs if s], 20):
            data = self._request("GET", f"{self.gamma_base}/events",
                                 params=[("slug", s) for s in chunk])
            for e in data or []:
                out.append(PMEvent.from_gamma(e))
        return out

    def open_events(self, limit: int = 500, max_events: int = 5000, **filters: Any) -> list[PMEvent]:
        """Every open, order-book-enabled event, paged. Only used when the
        collector is configured to widen past arena's universe."""
        out: list[PMEvent] = []
        offset = 0
        while len(out) < max_events:
            params: dict[str, Any] = {"closed": "false", "limit": limit, "offset": offset}
            params.update({k: v for k, v in filters.items() if v is not None})
            data = self._request("GET", f"{self.gamma_base}/events", params=params)
            if not data:
                break
            out.extend(PMEvent.from_gamma(e) for e in data)
            if len(data) < limit:
                break
            offset += limit
        return out[:max_events]

    # --- clob: books and quotes ---
    def book(self, token_id: str) -> dict:
        return self._request("GET", f"{self.clob_base}/book", params={"token_id": token_id}) or {}

    def books(self, token_ids: list[str]) -> dict[str, dict]:
        """Batch full-depth books, keyed by token id. Missing tokens are
        omitted rather than raising, so one dead market cannot lose a cycle."""
        out: dict[str, dict] = {}
        for chunk in self._chunks(list(dict.fromkeys(t for t in token_ids if t))):
            try:
                data = self._request("POST", f"{self.clob_base}/books",
                                     json_body=[{"token_id": t} for t in chunk])
            except PolymarketError as e:
                log.warning("batch /books failed for %d tokens (%s); falling back one by one", len(chunk), e)
                for t in chunk:
                    try:
                        b = self.book(t)
                        if b:
                            out[str(b.get("asset_id") or t)] = b
                    except PolymarketError:
                        log.warning("book fetch failed for token %s", t)
                continue
            for b in data or []:
                aid = str(b.get("asset_id") or "")
                if aid:
                    out[aid] = b
        return out

    def _token_map(self, endpoint: str, token_ids: list[str]) -> dict[str, float]:
        out: dict[str, float] = {}
        for chunk in self._chunks(list(dict.fromkeys(t for t in token_ids if t))):
            try:
                data = self._request("POST", f"{self.clob_base}/{endpoint}",
                                     json_body=[{"token_id": t} for t in chunk])
            except PolymarketError as e:
                log.warning("batch /%s failed (%s); skipping this chunk", endpoint, e)
                continue
            for k, v in (data or {}).items():
                f = _f(v)
                if f is not None:
                    out[str(k)] = f
        return out

    def midpoints(self, token_ids: list[str]) -> dict[str, float]:
        return self._token_map("midpoints", token_ids)

    def spreads(self, token_ids: list[str]) -> dict[str, float]:
        return self._token_map("spreads", token_ids)

    # --- data-api: public trade tape ---
    def trades_page(self, condition_id: str, limit: int = 500, offset: int = 0) -> list[dict]:
        data = self._request("GET", f"{self.data_base}/trades",
                             params={"market": condition_id, "limit": limit, "offset": offset})
        return data or []

    def trades_since(
        self,
        condition_id: str,
        since_ts: int | None,
        limit: int = 500,
        max_pages: int = 20,
    ) -> list[dict]:
        """Newest-first tape paged backwards until it predates `since_ts`.

        `max_pages` caps a cold start on a very heavily traded market; the
        watermark makes every later cycle cheap. Returns the raw trade dicts
        (deduped downstream on a content hash — the tape has no trade id and
        one transaction hash can cover several fills)."""
        out: list[dict] = []
        seen: set[tuple] = set()
        for page in range(max_pages):
            rows = self.trades_page(condition_id, limit=limit, offset=page * limit)
            if not rows:
                break
            for r in rows:
                key = (r.get("transactionHash"), r.get("asset"), r.get("proxyWallet"),
                       r.get("side"), r.get("size"), r.get("price"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(r)
            oldest = min((int(r.get("timestamp") or 0) for r in rows), default=0)
            if since_ts is not None and oldest <= since_ts:
                break
            if len(rows) < limit:
                break
        if since_ts is not None:
            out = [r for r in out if int(r.get("timestamp") or 0) > since_ts]
        return out

    # --- clob: price history ---
    def price_history(
        self,
        token_id: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = None,
        fidelity: int = 1,
    ) -> list[dict]:
        """1-minute (fidelity=1) mid-price series. Pass either `interval`
        ('1d', '1w', 'max', ...) or an explicit start/end window."""
        params: dict[str, Any] = {"market": token_id, "fidelity": fidelity}
        if start_ts is not None:
            params["startTs"] = int(start_ts)
            params["endTs"] = int(end_ts if end_ts is not None else time.time())
        else:
            params["interval"] = interval or "1d"
        data = self._request("GET", f"{self.clob_base}/prices-history", params=params)
        return (data or {}).get("history") or []
