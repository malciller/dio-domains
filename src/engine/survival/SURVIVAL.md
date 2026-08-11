# DIO Capital Survival Engine — Explained

This document explains the `src/engine/survival/` module: what it computes,
how it works, how to run it, and how to read its output. It assumes you know
what a grid strategy is, but not much else.

The one-sentence version: **"Given my grid parameters (qty, grid interval,
fees), how much quote capital do I need so that the grid almost certainly
never runs out of money — and, given my capital, what's the deepest drawdown
my grid survives?"**

---

## 1. The core idea

A grid strategy places buy orders at descending price levels. Every buy
consumes quote capital (`qty × price × (1 + fee)`). If the price keeps
falling, the grid keeps buying, and eventually the quote capital is gone:
the grid is **exhausted**. The drawdown at which that happens is the grid's
**D_surv** (drawdown survived).

The module answers two questions:

1. **History**: How deep have drawdowns actually gone for this asset (or its
   risk class), over various horizons (30/90/180/365 days)?
2. **Sizing**: How much capital makes the grid survive a drawdown deep
   enough that the probability of exceeding it is only ~1%?

Everything else (percentile tables, blends, replay, portfolio mode) is
machinery serving those two questions.

---

## 2. The vocabulary (learn these 5 terms)

### 2.1 MFD — Maximum Fractional Drawdown

For a window starting at session `s` and running `h` sessions:

```
MFD(s, h) = 1 - (min low over sessions (s, s+h]) / (close at s)
```

- It's a fraction, e.g. `0.25` = the price dropped 25% below the start close
  before recovering (or before the window ended).
- The window is **half-open** `(s, s+h]`: the start's own low doesn't count,
  only the lows of the next `h` sessions.
- A window is **rejected** (not sampled) if it would run past the end of the
  data. An incomplete window would have a deceptively small min low and
  silently bias the whole distribution. This is why a horizon needs
  `warmup + horizon + 2` bars to exist at all.

### 2.2 F_h(d) — coverage (a CDF) and S_h(d) — survival

Take every valid start session `s`, compute `MFD(s,h)`, and ask: what share
of those windows drew down at most `d`?

```
F_h(d) = P(MFD_h <= d)      coverage  (a cumulative distribution function)
S_h(d) = 1 - F_h(d)         survival
```

- `F_h(0.3) = 0.95` means: 95% of all 30-day windows had a max drawdown of
  30% or less — i.e. only 5% of windows drew down more than 30%.
- F is **monotone non-decreasing** in `d`. That monotonicity is what makes
  all the binary searches in the sizing layer sound.

### 2.3 Percentiles and the "effective sample size"

The percentile table reports, for each horizon, the drawdown level `d` such
that `F_h(d) = p/100` (P50, P75, P90, P95, P99).

Two sample counts matter:

- `n_starts` — every overlapping start window (each session is a start).
  Overlapping windows are **autocorrelated**: one contiguous crash gets
  counted once per day it overlaps. This overstates the information.
- `n_eff` (`asset_eff` in the report) — **non-overlapping** windows only
  (stride = horizon sessions). This is the honest sample size. For a
  365-session horizon on ~1600 sessions you get only **4** independent
  windows, so P99 is literally "the worst of 4 windows" — the honest answer,
  but not authoritative. That's what the warning
  "only 4 independent 365-session windows ... not authoritative below 5
  windows" means: treat the tails with suspicion.

### 2.4 Risk classes and the kappa blend

A single asset's history is short and unique. To stabilize the tails, the
module also builds **class curves** from a pool of similar assets (the
`classes` map in config.json, e.g. `large_cap_volatile` = SOL, DOGE, ADA,
HYPE, XMR). The asset's curve is blended toward its class curve:

```
F_blend(d) = (n_a · F_asset(d) + kappa · F_class(d)) / (n_a + kappa)
```

- `kappa` is a **pseudocount**: "pretend the class is worth `kappa`
  additional independent windows of evidence". Default 200; ~11% of the vote
  on a ~1600-session history. A per-class `kappa` in config.json wins;
  `--kappa` wins over both.
- The asset's own history naturally takes over as it grows (the `n_a` term
  dominates).

### 2.5 The volatility-normalized class contribution (z-blend)

This is the subtle part. The class curve is **not** used as raw drawdown
fractions. A low-vol asset would be unfairly punished by a high-vol
classmate's 60% crashes. Instead:

```
for each asset start s with trailing vol sigma_s:
    tau_s(d) = d / (sigma_s · sqrt(h))        # "how many stddevs is drawdown d?"
    class contribution = F_class^z(tau_s(d))   # pooled class z-CDF at that point

F_class_avg(d) = average of those over all valid asset starts
```

- `F_class^z` is the empirical CDF of the class's **z-scores**:
  `z = MFD / (sigma_s · sqrt(h))`, pooled across all member assets.
- So the class only tells you "in units of the asset's own volatility, how
  bad is a drawdown of size `d`?" — and the asset is judged against its own
  volatility regime, not its classmate's raw swings.
- Starts with `sigma = 0` (flat/gap-adjacent data) carry **no** volatility
  information and are excluded from *both* sides of the blend. Mapping them
  to `tau = +infinity` would inject fake 100% class certainty.

---

## 3. The pipeline (how a report is produced)

```
raw bars (network or file)
        │  Survival_calendar: sort by ISO date, dedup, detect gaps
        ▼
series  ── max_gap > tolerance? ──► fail ("refusing to forward-fill")
        │
        ├─► Survival_mfd      : MFD samples, F_h surfaces, percentile tables (per asset)
        ├─► Survival_classes  : pooled class surfaces/tables + z-index over class members
        ├─► Survival_replay   : kappa-blended F_blend (z-blend), historical path coverage
        │                      + inverse sizing (min capital / max qty / empirical)
        ├─► Grid_core replay  : D_surv of the actual grid over the actual path
        └─► bin/survival.ml   : text or JSON report
```

### Gap handling (hard rule)

The calendar model differs per venue:

- **Crypto** (kraken, hyperliquid): a session is a calendar day; any missing
  day is a gap.
- **Equity** (alpaca): a session is a US market day (weekdays minus
  holidays, from Alpaca's calendar; falls back to Mon–Fri).

If the largest gap exceeds `--gap-tolerance` (default 5), analysis **fails
loudly** instead of forward-filling. Never forward-fill: a fabricated bar
would silently poison the drawdown distribution. Gaps smaller than the
tolerance are tolerated (data is just missing), but zero-volatility windows
around them get excluded from the blend (see 2.5).

### No-lookahead invariant

Trailing volatility at start `s` uses only closes from `s-w .. s` (bars
strictly before the window). The MFD window itself starts at `s`'s close.
Nothing about the future leaks into the present.

---

## 4. The grid replay and D_surv

`Survival_replay.replay_series` runs the **real** `Grid_core` engine over the
asset's full OHLC history, with the pessimistic `Buy_first` ordering (buys
are evaluated before sells within a bar — the worst case for capital). It
records:

- `D_surv` — the drawdown at which the grid's quote capital could no longer
  fund the next (dynamically sized) buy. If the grid **never** runs dry,
  `D_surv = 100%` (it survived everything the history threw at it).
- `fills` — buy/sell counts.
- `min_quote_drawdown` — the worst realized dip of the quote balance.

### Static (closed-form) runway — how sizing is computed without replaying

For `N` consecutive ladder buys with grid interval `gi`, the capital
consumed is the geometric sum:

```
C_used(N) = (1+fee) · q · start_price · (1-gi) · (1-(1-gi)^N) / gi
D_surv    = 1 - (1-gi)^N
```

Invert it: for a target drawdown `d`, the number of fills needed is
`N* = ceil(log(1-d) / log(1-gi))`, and the capital that funds exactly `N*`
buys is the sizing. This is the **safe** recommendation: it assumes the
price goes straight down with zero intermediate sells, which is the worst a
real path can do.

`floor_aware_runway_cost` is the same walk but respects venue floors
(`min_notional`, `qty_min`): when the floor forces a bigger buy size deep in
the ladder, the cost is computed by actually walking the ladder with
`Grid_core`'s dynamic buy up-sizing. This is the conservative bound used by
the sizing layer.

### Historical path coverage (the headline)

With `D_surv` in hand, the module evaluates `F_blend(D_surv)` per horizon —
the share of history (blended toward the class) whose max drawdown the grid
would have survived. This is the "Historical path coverage" table:
`asset`, `class`, and `blended` columns. 100% at 365d means the grid
survived even the deepest drawdown in the entire history at that horizon.

---

## 5. Inverse sizing (the numbers you actually act on)

All three are per-horizon, targeting a **blended** survival of
`--target-survival` (default 99%).

### 5.1 Min capital (`find_min_capital`)

1. Find `d*` = the smallest drawdown with `F_blend(d*) >= 0.99` (bisection
   over the monotone CDF — exact to machine precision).
2. Convert `d*` to a fill count `N*`, then to capital via the floor-aware
   runway cost.

Monotone CDF ⟹ bisection is sound. Replay `D_surv` is **not** monotone in
capital (sells shift which rung exhausts the grid), which is why the static
path is used here and the empirical path (below) can't be a plain binary
search.

### 5.2 Max qty (`max_qty`)

Same `d*`, but the runway cost is linear in `qty` (when no floor binds), so
solve `capital = per_unit_cost · qty` for `qty`. Advisory under a binding
floor — the replay-based empirical sizing is authoritative then.

### 5.3 Empirical min capital (advisory)

`empirical_min_capital` actually **replays** the grid at progressively
smaller capitals (log-spaced scan + bisection refinement + a second pass
below the first hit to catch non-monotone islands) and finds the smallest
capital whose replayed coverage clears the target. Because the static sizing
assumes a straight-down path, the empirical number is usually **much lower**
(the buffer column shows `static / empirical`, e.g. 3.57× means the static
sizing pays 3.57× what history needed). The static number is still the
recommendation — history didn't have to be this kind.

> Caveat printed in the code: when a venue's `min_notional` binds, the
> replayed path can burn capital *faster* per rung than the closed form, and
> the empirical number can exceed the static one instead of landing below.

### 5.4 `reachable = false` (the `*` in the report)

A row like `180d  1000000000.00  0.0% *` means: **no parameter within the
search bounds clears the target**. Two causes:

1. The required capital exceeds `--max-capital` (default 1e9), or the
   required qty is below `--qty-increment`.
2. The target survival sits in a **coverage gap**: the blended history can't
   reach 99% *with certainty* — there's a step in the empirical CDF, and the
   target lands between two steps. Surviving the whole history with
   certainty isn't achievable at that target, so the tool refuses to
   fabricate a number.

Also see the skipped-asset case below (SPCX).

---

## 6. How to run it

```bash
# One asset, live data
dune exec dio-survival -- BTC/USDC --exchange hyperliquid

# Every asset in config.json's "trading" list, each on its own exchange
dune exec dio-survival

# One asset with explicit parameters
dune exec dio-survival -- SOL/USD --exchange kraken --capital 500 \
  --target-survival 0.99 --gi 2.0 --qty 0.25 --fee 0.0016

# Equity calendar (sessions, not days)
dune exec dio-survival -- QQQ --exchange alpaca --capital 10000

# Offline from a CSV file (still needs a SYMBOL)
dune exec dio-survival -- BTC/USD --from-csv data/btc.csv

# JSON report (for scripting)
dune exec dio-survival -- ETH/USD --exchange kraken --json
```

### Config requirements (config.json)

- Every trading entry needs `"asset_class"` (e.g. `"large_cap_stable"`),
  matching a key in the top-level `"classes"` map — or pass `--class`.
- `"classes"` defines the member pools:

  ```json
  "classes": {
    "large_cap_stable":   ["BTC/USD", "ETH/USD"],
    "large_cap_volatile": { "members": ["SOL/USD", "DOGE/USD", "ADA/USD", "HYPE/USD", "XMR/USD"], "kappa": 250 }
  }
  ```

  Members are fetched per venue (never hardcoded). If no members are known,
  the asset runs alone with a warning.

- Grid parameters (`qty`, `grid_interval`, `sell_mult`, `maker_fee`,
  `accumulation_buffer`) default from the trading entry; every one is
  overridable on the CLI.

### Key CLI flags

| Flag | Effect |
|------|--------|
| `--exchange` | kraken \| hyperliquid \| alpaca (sets the calendar kind) |
| `--capital` | override replay capital; default is the computed static min capital across horizons |
| `--qty`, `--gi`, `--fee`, `--sell-mult` | override grid parameters |
| `--target-survival` | target blended survival for sizing (default 0.99) |
| `--horizons` | comma sessions list (default 30,90,180,365 crypto / 21,63,126,252 equity) |
| `--vol-window` | rolling vol window / warmup (default 60) |
| `--kappa` | blend weight (default 200) |
| `--class`, `--members` | risk class / member pool override |
| `--max-capital` | upper bound for the capital search (default 1e9) |
| `--gap-tolerance` | max missing sessions before refusing (default 5) |
| `--price-increment`, `--qty-increment`, `--qty-min`, `--min-notional` | venue gates |
| `--from-csv` / `--from-json` | offline mode (CSV or JSON bars; JSON accepts `{"bars": [...]}` or a plain array) |
| `--portfolio` + `--topology` / `--total-capital` / `--allocation` / `--transfer` | portfolio mode (see §8) |
| `--start` / `--end` / `--data-feed` | alpaca date range and feed (iex\|sip) |

---

## 7. Reading the report (annotated)

Take the BTC/USDC report:

```
Bars: 1684  (2022-01-01 .. 2026-08-11)   max gap: 0 (tolerance 5)
```
Clean data: ~4.6 years of daily bars, no missing sessions.

```
30d     n=1594 asset_eff=54 class_eff=108   P50    8.9%/  10.3%/   8.8%   ...
```
For 30-day windows: the asset's own median max drawdown is 8.9%; the class
pool's is 10.3%; the blended estimate is 8.8%. P99 = 38.9%/44.6%/48.5% —
but note `asset_eff=54` (54 non-overlapping 30-day windows), so the tail is
reasonably supported at 30d. At 365d, `asset_eff=4` — that's why the tool
warns the tails aren't authoritative there.

```
Grid replay (qty 0.0005  gi 0.75%  capital 2071.33  fee 0.040%):
  D_surv =  100.0% (never exhausted)
  fills: 149 buy / 16 sell   min quote drawdown   91.8%
```
Capital 2071.33 is the tool's own recommended static min capital (see §5.1).
With it, the grid never ran dry on the entire history (D_surv = 100%), even
though at one point the quote balance was drawn down 91.8%.

```
Historical path coverage at D_surv:
  horizon      asset      class    blended
  30d         100.0%     100.0%     100.0%
  ...
  365d        100.0%     100.0%     100.0%
```
At every horizon, 100% of history had a max drawdown smaller than what the
grid survives. The grid would have lived through everything this asset has
ever done.

```
Inverse sizing (target blended survival 99.0%):
  horizon    min-capital      cov   d_surv      max-qty      cov
  30d            1541.90    99.1%    48.8%       0.0007    99.1%
  90d            1778.18    99.0%    56.3%       0.0006    99.0%
  180d     1000000000.00    0.0% *   100.0%   1000000.0000    0.0% *
  365d           2071.33   100.0%    64.9%       0.0005   100.0%
```
To have 99% blended survival on 30-day windows you need ≥ 1541.90 quote
capital, which survives a 48.8% drawdown; etc. The 180d row is `*` —
unreachable in this configuration (see §5.4): a coverage gap means 99%
blended survival *with certainty* isn't reachable on 180-day windows for
this asset/class at these parameters.

```
Empirical min capital (advisory ...):
  30d            1289.81    99.1%    48.8%       1.20
```
History actually only needed 1289.81 (1.20× less than the static 1541.90) —
the static sizing pays a 1.2× buffer for the straight-down assumption. With
more volatile assets (e.g. XMR buffer 15.24×) the gap is much larger.

### Warnings you may see

| Warning | Meaning |
|---------|---------|
| `only 4 independent 365-session windows ... not authoritative below 5 windows` | `n_eff < 5`: the tails (P95/P99) are the best/worst of a handful of windows. Read them as rough bounds, not precise estimates. |
| `zero trailing volatility ... excluded from both the asset CDF and the class contribution` | Some windows were flat/gap-adjacent; they carry no vol information and were dropped from the blend (both sides, so it stays consistent). |
| `'SPCX' (alpaca) skipped: ... empty distribution for horizon 21 warmup 39` | The asset's history is shorter than `warmup + horizon + 2` bars. Fix by passing `--vol-window`/`--horizons` that fit the data, or remove it from config.json trading. |
| `no reachable static min capital ... pass an explicit --capital` | No horizon can clear the target within `--max-capital`; the tool refuses to invent a number. |

---

## 8. Portfolio mode (`--portfolio`)

The single-asset model treats each asset as its own runway. Portfolio mode
models the reality that **capital is locked per venue account**: everything
on `hyperliquid/USDC` draws from one pool, everything on `kraken/USD` from
another.

- `--total-capital` splits the total equally across venues; explicit
  funding via repeated `--allocation hyperliquid/BTC/USDC=1000`.
- `--transfer SESSION:FROM->TO=AMOUNT` moves budget between venue pools at a
  session boundary (applied before that session's bars). Same-venue and
  cross-quote transfers are rejected.
- A topology JSON file (`--topology`) can define the same positions and
  transfers; `--positions-file` / `--save-positions` persist pool shares and
  base checkpoints.
- When no capital is given, online runs fetch each venue's actual available
  quote balance.
- The replay runs `Grid_core` in lockstep over an aligned ISO-date timeline
  (`Survival_topology.align_series` — never forward-fills), and when one
  subgrid's buy can't be funded, every subgrid on that venue is starved too:
  the venue's survival is the pool's survival.

Relevant modules: `survival_portfolio.ml` (the simulation), `survival_topology.ml`
(topology parsing/validation/timeline alignment), `survival_portfolio_state.ml`
(persistence), `survival_balances.ml` (one-shot account balance snapshots).

---

## 9. Module map

| Module | Role |
|--------|------|
| `survival_types.ml` | Shared types: `bar`, `series`, `horizon`, `survival_surface`, `percentile_table`, `historical_path_coverage`, `sizing_result`, calendar kinds |
| `survival_calendar.ml` | Sort/dedup bars, gap detection (calendar-day for crypto, session-predicate for equity), ISO date helpers |
| `survival_sessions.ml` | Expected-market-session model for equity (weekdays, holidays, Alpaca calendar) |
| `survival_mfd.ml` | The heart: MFD windows, empirical `F_h`/`S_h`, surfaces, percentile tables, and the closed-form static runway cost + floor-aware variant |
| `survival_math.ml` | Dependency-free numerics: percentiles, weighted percentiles, std |
| `survival_stats.ml` | Trailing vol (no lookahead), z-scores, per-start (MFD, sigma) regimes, the `blend` function |
| `survival_classes.ml` | Pooled class curves from member histories + the sorted weighted z-index (O(log n) z-CDF) |
| `survival_replay.ml` | The orchestrator for coverage/sizing: blend models, `F_blend`, historical path coverage, `d_for_coverage`, min-capital / max-qty / empirical sizing |
| `survival_archetypes.ml` | Risk-class data model and default class/kappa definitions |
| `grid_adapter.ml` | Bridge config.json trading entries → `Grid_core.config` (increments, fees, venue floors) |
| `survival_fees.ml` | Resolve real maker/taker fees per exchange (cached; explicit config/CLI wins) |
| `survival_venues.ml` | Populate venue instrument metadata (ticks/lots) from real exchanges |
| `survival_fetch_kraken.ml` / `_hyperliquid.ml` / `_alpaca.ml` | Daily OHLC fetchers per venue (alpaca also fetches the market calendar) |
| `survival_loader.ml` | Offline CSV/JSON fixture IO |
| `survival_tasks.ml` | CLI symbol/exchange → analysis task resolution |
| `survival_topology.ml` | Qualified instrument identity, portfolio topology parsing/validation, timeline alignment |
| `survival_portfolio.ml` | Multi-asset, per-venue-pool replay over the aligned timeline |
| `survival_portfolio_state.ml` | Save/load portfolio pool shares and base checkpoints |
| `survival_balances.ml` | One-shot account balance snapshots (kraken/hyperliquid/alpaca) |
| `bin/survival.ml` | CLI: flags, fetching, class resolution, report generation (text + JSON) |

Tests live in `test/engine/survival/` — most notably
`test_analytical_vs_core.ml` (the static runway vs. actual replay) and
`test_survival_mfd.ml` / `test_survival_stats.ml` / `test_survival_topology.ml`.

---

## 10. Design rules worth knowing

1. **Never fabricate data.** No forward-filling of gaps; no percentile from
   an empty distribution (both raise instead of returning 0.0); no coverage
   from zero valid windows.
2. **Autocorrelation honesty.** Tail percentiles come from non-overlapping
   windows; the blend weights the asset by its *independent* window count,
   so a thin sample shrinks toward the class instead of pretending
   overlapping starts are independent evidence.
3. **No-lookahead.** Volatility windows use only bars at or before the
   start session.
4. **Consistency.** The blend's asset side and class side are estimated over
   the exact same windows (sigma > 0 only), so the blend is a true weighted
   average, never a mix of different denominators.
5. **Static = safe, empirical = advisory.** The closed-form runway assumes
   a straight-down path; the path replay is what history actually did. The
   static number is the recommendation; the empirical number shows the
   buffer the static sizing pays for.
