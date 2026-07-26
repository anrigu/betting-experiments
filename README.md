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

## Layout

```
engine/           the worker
  betx/
    strategies.py   decide / decide_fundamental — framework-exact math (tested)
    fees.py         venue-exact Kalshi fee model
    engine.py       cycle: ingest -> gate -> decide -> execute (claim-before-submit)
    arena.py        ProphetArena DB tap + Kalshi ticker mapping + Brier snapshots
    kalshi.py       signed Kalshi client
    sync.py         order/fill/settlement reconciliation + equity snapshots
    db.py           Supabase store (schema betting_experiments) + lane ledger fold
    costs.py        LLM pricing table (for predictor mode)
    predictions.py  optional own-schedule predictor mode
  main.py           entrypoint: python engine/main.py [--once] [--dry-run]
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
uvicorn dashboard.app:app --port 8901            # dashboard on :8901
pytest engine/tests/                             # strategy math
```

`.env` is gitignored; see `.env.example` for the required variables.

## Data

Everything lands in Supabase schema `betting_experiments`: `predictions`, `decisions` (every
bet AND skip is an audit row), `orders`, `fills`, `settlements`, `equity_snapshots`,
`position_snapshots`, `markets`, `llm_calls`, `engine_cycles`. Venue truth (fills, fees,
settlement revenue) always overwrites estimates.
