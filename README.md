# betting-experiments

Formalized live-betting experiment: **ProphetArena LLM predictions → Kalshi execution**, with a
strategy engine ported from our internal live-betting framework and full audit/tracking in Supabase +
a live dashboard.

## The experiment

**Two agents, one Kalshi account, virtually separated books.** Each lane is an arena agent
betting the **fundamental** strategy with its own bankroll; attribution is per-order, so the
shared account decomposes exactly into per-lane ledgers.

- **Lanes** (`BETX_LANES`, `name:predictor:strategy:bankroll`):
  - `gemini` — `agent-gemini-3.1-pro` (gemini-3.1-pro-preview, agentic web-search harness)
  - `fable-5` — `agent-claude-fable-5` (Claude Fable 5, agentic harness)
  Fixed-context variants of both are ingested as references for the dashboard's Brier panel.
- **Prediction source**: looped directly from ProphetArena's production DB (no LLM spend). The
  engine polls for new predictions every 10 minutes and maps each event outcome to its Kalshi
  ticker.
- **Bet on all categories.** No category filter anywhere.
- **The only filters**:
  1. *Fee-aware within-spread gate* — edge is measured against the ask of the side being bought;
     `n = floor(edge × 100)` contracts must satisfy `edge × n > kalshi_fee(n, ask)` where
     `kalshi_fee = ⌈0.07 · C · P · (1−P)⌉` (venue-exact, whole-order ceil).
  2. *No betting within 24h of close* (fail-closed on missing close time).
  3. Mechanical guards: `thin_book` (skip entirely, never shrink or walk the book),
     `insufficient_cash`, `superseded`.
- **Strategy `fundamental`** — every new forecast opens a fresh `floor(edge × 100)`-contract
  position as if flat: gross exposure accumulates, never reduces, rides to settlement.
  (A `momentum` force-to-target strategy is also implemented and lane-selectable.)
- **Sizing**: `floor(edge × 100)` contracts (proper-Brier linear rule). No Kelly.
- **Execution**: limit buys at the ask with 5-minute venue-side expiration; depth pre-checked.
- **Account**: the funded Kalshi account (RSA-PSS signed trade-api v2; credentials via env).
- **Market data**: reads (markets, full-depth orderbooks, events, public trade tape) go through
  Kalshi's unauthenticated external API (`KALSHI_PUBLIC_BASE_URL`, default
  `https://external-api.kalshi.com`) with automatic fallback to the signed host; only order
  submission and portfolio endpoints are signed.
- **Data capture**: every decision stores the full-depth book it priced from (`book_snapshots`,
  kind `decision`); every order submit — live, dry-run, or failed — stores a fresh `post_trade`
  snapshot with the full book, the complete market payload, and the last 100 public trades,
  linked to the order row via `book_snapshots.order_id`.

## Layout

```
engine/           the worker
  betx/
    strategies.py   decide / decide_fundamental — framework-exact math (tested)
    fees.py         venue-exact Kalshi fee model
    engine.py       cycle: ingest -> gate -> decide -> execute (claim-before-submit)
    arena.py        ProphetArena DB tap + Kalshi ticker mapping + Brier snapshots
    kalshi.py       Kalshi client (public market-data host + signed trading host)
    polymarket.py   Polymarket read-only client + arena->token outcome mapping
    pm_collect.py   Polymarket collection cycle (books, tape, history, metadata)
    kalshi_collect.py  Kalshi collection cycle (books, tape, metadata)
    book_metrics.py    shared derived microstructure (OBI, microprice, depth)
    collector.py    runs both venues in one cycle, isolating their failures
    stream.py       WebSocket book/trade capture for both venues
    sync.py         order/fill/settlement reconciliation + equity snapshots
    db.py           Supabase store (schema betting_experiments) + lane ledger fold
    costs.py        LLM pricing table (for predictor mode)
    predictions.py  optional own-schedule predictor mode
  main.py           entrypoint: python engine/main.py [--once] [--dry-run]
  collect.py        entrypoint: python engine/collect.py [--once] [--venue ...]
  stream.py         entrypoint: python engine/stream.py [--venue ...]
  tests/            strategy math pinned to upstream test vectors
dashboard/        FastAPI + self-contained page (uvicorn dashboard.app:app)
render.yaml       Render blueprint (worker + dashboard)
```

## Safety switches

| env | meaning |
|---|---|
| `BETX_DRY_RUN=true` | decide + audit everything, simulate fills, never touch Kalshi orders |
| `BETX_LIVE_ENABLED=false` | master kill switch: decisions recorded, zero orders (not even dry-run rows) |
| `BETX_LANES` | `name:arena_predictor:strategy:bankroll,...` (default: gemini + fable-5, fundamental, $150 each) |

**Going live is one flip:** set `BETX_DRY_RUN=false` on the `betx-engine` Render service.

## Run locally

```bash
pip install -r engine/requirements.txt
set -a; source .env; set +a
python engine/main.py --once --dry-run          # one cycle, simulated
python engine/collect.py --once                  # one collection cycle, both venues
python engine/stream.py --duration 60            # 60s of streaming capture
uvicorn dashboard.app:app --port 8901            # dashboard on :8901
pytest engine/tests/                             # strategy math + pm mapping
```

`.env` is gitignored; see `.env.example` for the required variables.

## Market-data collection (read-only)

ProphetArena also carries Polymarket events, tickered `PM-<polymarket slug>`.
Nothing trades there. `engine/collect.py` is **one worker that only collects**,
covering **both venues** on one cadence (`BETX_COLLECT_INTERVAL_SEC`, default
300s). It holds no wallet and no trading credentials, and the Polymarket client
in this repo has no order-signing path at all.

Why a separate service: it writes only `pm_*`, `kx_*` and `collect_cycles`,
which the engine never reads, so the collector can be restarted, backfilled or
broken without any possibility of affecting a live Kalshi order. The two
venues are also isolated from each other — a Gamma outage cannot cost a Kalshi
cycle.

**`BETX_COLLECT_INTERVAL_SEC` is not `BETX_POLL_INTERVAL_SEC`.** The latter
paces the trading loop and is deliberately left separate: changing how often
you snapshot a book must never change when the engine decides a trade. Note
too that the engine writes `book_snapshots` only for markets it is actively
deciding on, plus post-trade — it has never been a sweep, which is exactly the
gap `kx_*` fills.

**Sources** (all unauthenticated):

| host | what |
|---|---|
| `gamma-api.polymarket.com` | event/market metadata: condition ids, CLOB token ids, tick size, min order size, neg-risk flag, fee schedule |
| `clob.polymarket.com` | full-depth order books (batched), midpoints, spreads, 1-minute price history |
| `data-api.polymarket.com` | public trade tape (the CLOB's own `/trades` is key-gated) |

**Outcome mapping.** Arena names an outcome; Polymarket addresses a token. The
`PM-` strip yields the Gamma slug exactly (41/41 of arena's current PM events
resolve). From there two shapes exist, and both are handled:

- *multi-outcome (neg-risk)* — each arena outcome is its own binary market;
  match `groupItemTitle`, take that market's YES token;
- *single binary market* — `groupItemTitle` is blank and the arena outcomes are
  the market's own `outcomes` (`Yes`/`No`, or names like `Mayweather`).

Matching is strict: an event that uses group titles never falls through to
outcome names, or `"Yes"` would silently bind to the first sub-market of a
neg-risk event. **Anything that fails to map lands in `pm_unmapped` with the
candidates that were considered** — the Kalshi path used to drop unmapped
outcomes with no trace, which is indistinguishable from "no predictions yet".
That path now logs too.

**Shape differences that matter** (why this is not just "Kalshi with decimals"):

- a market is a *pair* of ERC-1155 tokens, and the NO book is its own book, not
  the complement of the YES book;
- prices are decimals in [0,1], sizes are fractional USDC, `min_order_size` is
  5, and `tick_size` varies per market — 0.01 and 0.001 both occur inside a
  single event;
- fees are real and category-specific: `feeType` (`tech_fees`, `culture_fees`,
  `sports_fees_v2`, `politics_fees`, `finance_prices_fees`) with a
  `feeSchedule` of `{exponent, rate, takerOnly, rebateRate}` — same shape
  family as Kalshi's `rate · C · P · (1−P)`, taker-only, with a maker rebate.
  Both are captured per market and per snapshot, since the gate any future
  execution needs depends on them.

### Kalshi (`kx_*`)

Three things make this venue different, and all three shape the collector:

- **Scale, scoped by forecast — not by close time.** Arena tracks ~15k open
  Kalshi markets against Polymarket's ~400 tokens; all of them every cycle is
  ~173 GB/month. The scope is therefore markets carrying a forecast in the
  last 30 days (`BETX_KX_FORECAST_WITHIN_DAYS`) — **7,691 markets today**,
  covering 100% of what the lanes forecast.
  `BETX_KX_CLOSING_WITHIN_DAYS` also exists but defaults to **0 (off)**, and
  should stay off: the engine's only close gate is "no bets within 24h of
  close", so it trades markets resolving months out. A 30-day close filter
  dropped 390 of the 2,201 markets the lanes had live forecasts on. Either
  window: 0 or negative disables it.
- **One book, two sides.** A Kalshi market is a single book with a bid ladder
  per side; the executable YES ask ladder is the mirror of the NO bids. So
  there is one row per ticker, not one per token, and the NO contract's
  imbalance is exactly `-obi`. Ladders are parsed as floats here rather than
  reusing `strategies.ladders_from_orderbook`, which casts size to int for the
  trading path.
- **No book hash.** Polymarket publishes one; Kalshi does not, so the
  collector hashes the ladders itself. Measured over real 5-minute intervals,
  **70% of these books were unchanged** — 66% on near-close markets, 89% on
  long-dated ones. That skew is what makes the wide scope affordable: tripling
  the universe from 2,266 to 7,691 markets costs only 9 → 16 GB/month, because
  the markets it adds barely move. Without the skip it would be 100 GB/month.

Both batch endpoints are used: `/markets/orderbooks` and `/markets` take up to
**100 tickers per request** (200 is a 400), and the `tickers` key must be
*repeated*, not comma-joined — comma-joining silently returns one empty book
for the joined literal. The trade tape does not batch, so rather than sweeping
2,266 tickers a cycle the collector diffs each market's total volume against
last cycle and pulls the tape only where it moved: **2,262 candidates on a cold
start, 169 on the next cycle.**

### Streaming (`engine/stream.py`)

Both venues push order-book and trade updates over WebSocket, and both are
subscribed:

| venue | endpoint | auth |
|---|---|---|
| Polymarket | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | none |
| Kalshi | `wss://api.elections.kalshi.com/trade-api/ws/v2` | the same RSA-PSS headers the REST client builds |

**Not a row per message.** Measured on the live feeds, 400 Kalshi tickers emit
**2,928 orderbook deltas in 40 seconds** — ~1,400/sec at full scope. So the
streams hold each book in memory and write a snapshot only when it changed and
at most once per `BETX_STREAM_MIN_INTERVAL_SEC` (default 30s) per market.
Trades are bounded by real activity and are the highest-value rows here, so
every one is written. Rows are tagged `source='stream'` (polling writes
`'poll'`), so queries can use either or both.

**The delta semantics differ per venue and are easy to get backwards:**

- Polymarket `price_change` carries the **new absolute size** at a level
  (0 removes it);
- Kalshi `orderbook_delta` carries a **signed change** to that level.

Kalshi's `seq` runs per subscription, not per market. A gap means messages were
missed, so the in-memory books are dropped rather than kept — applying deltas
onto a book known to be wrong would poison the series silently. The polling
collector and the next snapshot re-establish truth.

Verified against the venues' own REST books after 75 seconds of applied
deltas: **60/60 Kalshi markets exact**, 54/60 Polymarket tokens exact — and the
6 differences are accounted for by churn, since two REST fetches 2 seconds
apart already disagree on 6/60.

**Tables** (all additive, none read by the engine):

- `pm_events`, `pm_markets` — current state, upserted
- `pm_market_snapshots` — point-in-time market state per market (typed
  columns only; the full Gamma payload lives in `pm_markets.raw` for current
  state and is not re-stored every cycle)
- `pm_book_snapshots` — **full depth per token**, both sides, plus derived
  top-of-book, depth totals, and the venue's own book hash (equal hash ==
  identical book, so duplicates are cheap to filter at read time). No `raw`
  column: every field of the CLOB `/book` response is already a typed column,
  and a test asserts that stays true if the venue adds one.
- `pm_trades` — public tape, deduped on a content hash (the tape has no trade
  id and one transaction hash routinely covers several fills)
- `pm_price_history` — 1-minute mid series per token
- `pm_predictions` — arena forecasts on PM events, resolved to a token. Kept
  **out** of `predictions` deliberately: a row there from a bet predictor
  becomes a Kalshi trading candidate, and these are not Kalshi tickers.
- `pm_unmapped`, `kx_unmapped` — outcomes that could not be mapped to a
  token/ticker, with the candidates considered
- `kx_markets` — current Kalshi market state, upserted (its `volume` is what
  the tape diff reads)
- `kx_book_snapshots` — full depth per ticker: raw `yes_bids`/`no_bids` and
  derived executable `yes_asks`/`no_asks`
- `kx_trades` — public tape, deduped on Kalshi's real `trade_id`
- `collect_cycles` — one row per cycle covering both venues, with a per-venue
  breakdown in `detail`

Every book snapshot on both venues also carries derived microstructure from
`book_metrics.py`: `bid_depth`, `ask_depth`, `obi` (order-book imbalance,
`(bid-ask)/(bid+ask)`), `obi_1` and `obi_5` at the top 1 and 5 levels, and
`microprice` (size-weighted top-of-book). All are recomputable from the stored
ladders — they are stored because doing it later across hundreds of millions of
rows costs far more than 40 bytes now. An empty book yields `NULL`, not `0.0`,
which would mean a genuinely balanced book.

**Cadence and volume.** Full order book depth is the priority and is captured
every cycle (default 300s), unchanged books included. Everything else is
trimmed to pay for it.

Measured on-disk cost (heap + TOAST + indexes, real rows loaded into Postgres),
against arena's current PM set of 41 events / 227 markets / 394 live tokens:

| table | bytes/row | rows/day | MB/day |
|---|---:|---:|---:|
| `kx_book_snapshots` | 1,553 | 335,232 | 520 |
| `pm_book_snapshots` | 1,732 | 113,472 | 197 |
| `pm_market_snapshots` | 544 | 65,376 | 36 |
| `kx_trades` | 697 | (traffic-dependent) | ~35 |
| `pm_trades` | 2,167 | ~8,600 | 19 |
| `pm_price_history` | 671 | 9,456 | 6 |
| **total** | | | **~812** (~24 GB/month) |

`kx_markets`, `pm_markets` and `pm_events` are upserted at a fixed row count
and do not grow. Order books are ~88% of the total, which is the intent. Three
things bought the headroom: dropping the redundant `raw` payloads (−22% on PM
book snapshots, −86% on PM market snapshots), hourly rather than per-minute
price history, and skipping unchanged Kalshi books — 66% of them, measured over
real 5-minute intervals (70% overall). Without that last one, Kalshi books
alone would be 100 GB/month.

A cold start is heavier than steady state: the first cycle backfills each
market's trade history and every token's price series. Measured end to end,
cycles ran 188s cold and 92s warm, both inside the 300s interval.

Note that **`BETX_PM_HISTORY_EVERY_N` does not reduce stored rows** — the
series is cumulative, so running it less often just batches more points per
call. `BETX_PM_HISTORY_FIDELITY` (60 = hourly) is the row-volume knob;
`EVERY_N` is the API-call knob.

To spend less: raise `BETX_COLLECT_INTERVAL_SEC`, tighten the Kalshi scope
windows, or set `BETX_PM_SKIP_UNCHANGED_BOOKS=true` (Polymarket publishes a
book hash, so unchanged books there are exact duplicates too — it is off by
default only because all ~400 PM tokens are actively traded). To spend more:
lower the interval, or widen `BETX_KX_FORECAST_WITHIN_DAYS` /
`BETX_KX_CLOSING_WITHIN_DAYS`.

## Data

Everything lands in Supabase schema `profit_trading_exp` (configurable via `BETX_DB_SCHEMA`):

- `predictions` — every arena forecast ingested (bet lanes + fixed-context references), with
  rationale and per-instance dedupe on the arena prediction id
- `book_snapshots` — the **full order book** (raw venue bid ladders + derived executable ask
  ladders) and complete market payload (status, close, OI, volume, liquidity, quotes) captured
  at decision time, one per (cycle, ticker)
- `decisions` — every bet AND skip is an audit row, linked to its prediction and book snapshot
- `orders`, `fills`, `settlements` — execution trail; venue truth (fills, fees, settlement
  revenue) always overwrites estimates
- `equity_snapshots`, `position_snapshots`, `markets`, `engine_cycles`, `llm_calls`

A crash between prediction-ingest and decide loses nothing: candidates are re-derived each
cycle from predictions that still lack a decision row for one of their lanes.
