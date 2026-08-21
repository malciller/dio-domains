# The Capital Oracle — Technical Summary

The oracle is DIO's capital-survival sizing engine. For each trading asset it answers:
*"given this asset's real price history and the quote capital available on its exchange account, what order size (`qty`) and grid interval (`gi`) keep the grid ladder alive through the worst drawdowns this asset has actually experienced?"*

It computes drawdown statistics empirically from the asset's own history, blends them with a risk-class prior via a kappa-weighted, volatility-normalized blend, inverts the blended CDF for the governing drawdown at the statistical target, then sizes the grid so its capital runway survives **100% replay survival** over the whole history — verified by replaying the strategy over the historical path.

Full design doc: `test/engine/oracle/oracle.md`.

## Where it runs

| Mode | Entry point | Behavior |
|---|---|---|
| CLI | `bin/oracle.ml` (`dio-oracle`) | One-shot: fetch history → analyze → print report. `--portfolio`, `--from-csv/--from-json` offline modes, `--json`. |
| Runtime | `src/engine/oracle/oracle_runtime.ml` | Continuous loop inside the engine: re-analyzes every ~5 min, publishes decisions trading domains adopt. Supervised like any connection (`Supervisor.start_oracle`, registry name `"oracle"`): heartbeats per published pass + 10s liveness ticker, auto-restart on loop death. |

## Module map (`src/engine/oracle/`)

**Data layer**
- `oracle_types.ml` — shared types: `bar`, `series`, `gap`, `horizon`, `survival_surface`, `percentile_table`, `blended_surface`, `sizing_result`, `range_stats`, `p2v_stats`, `sizing_reference`, `deployment_row`, `asset_deployment`; `calendar_kind` = `Crypto` (24/7 days) | `Equity` (market sessions).
- `oracle_calendar.ml` — pure civil-date arithmetic (no `mktime`/TZ), bar sort/dedup, gap detection, and the shared clean-series normalization (`normalize_bars`) applied on every fetch *and* cache read.
- `oracle_sessions.ml` — equity expected-session models: `business_weekday`, `explicit_model`, `alpaca_model`, `with_holidays`.
- `oracle_fetch.ml` — registry-driven series pipeline used by both runtime and CLI. Venue dispatch through `Exchange_intf.Oracle.Registry` (adapters: `kraken_oracle.ml`, `hyperliquid_oracle.ml`, `alpaca_oracle.ml`). Yahoo deep history (`src/external/yahoo/yahoo_deep_history.ml`) prepends full daily history beyond venue caps (~720d Kraken, 2020 Alpaca IEX); crypto symbols only via whitelist (dead-token collision safety); pre-listing windows skipped with the empty prefix cached.
- `oracle_cache.ml` — disk cache `data/oracle_history/v2/<exchange>/<symbol>.json`: raw bars stored once, delta-refreshed (one small request per asset), read-time normalization so rule fixes self-heal without refetch; failed deltas fall back to cached history.
- `oracle_fees.ml` — real maker/taker fees per venue (Kraken TradeVolume tier, Hyperliquid userFees, Alpaca 0); config `maker_fee` wins; 5s timeout guards; cached per `(exchange, symbol)`.
- `oracle_venues.ml` — instrument metadata (ticks, lot sizes) fetched once per exchange.
- `oracle_balances.ml` — account snapshots; prefers WS-fed live stores where the venue declares equivalent semantics (`Exchange_intf.Oracle.S.live_balances`: Kraken/Alpaca yes, Hyperliquid deliberately `None` — REST spot balance authoritative).
- `oracle_tasks.ml` / `oracle_loader.ml` / `grid_adapter.ml` — task resolution, offline CSV/JSON fixtures, config→`Grid_core.config` replay bridge.

**Statistical core**
- `oracle_math.ml` — dependency-free helpers: Type-7 percentiles (raise on empty input), `range_stats_of` (ATH/ATL context), `peak_to_valley_stats_of` (deepest actual peak-to-valley event + `recovered` flag), `floor_overshoot_p90_of`, `sizing_reference_of` (ATH-scaled survival reference).
- `oracle_mfd.ml` — Maximum Fractional Drawdown machinery: `MFD(s,h) = 1 − min low(s, s+h] / close(s)`; empirical CDF/survival surfaces; stride = horizon gives non-overlapping (independent) windows for tail honesty (`n_eff`); closed-form ladder cost + floor-aware runway walk replicating `Grid_core` rounding/dynamic buy up-sizing.
- `oracle_stats.ml` — no-lookahead trailing volatility (window `[s−W, s]` only); vol-normalized drawdown `z(s,h) = MFD/(σ_s·√h)`; kappa pseudocount blend `F_blend = (n_a·F_asset + κ·F_class)/(n_a + κ)`.
- `oracle_archetypes.ml` — risk classes (`crypto_core`, `crypto_alt`, `equity_etf`, `equity_momentum`), default κ = 365/252 (one year of class prior weight).
- `oracle_classes.ml` — pooled class curves from `config.json` `"classes"` membership; pooled z-distribution index (O(log n)) evaluated at each asset start's own vol regime — makes the blend vol-fair across classmates.
- `oracle_replay.ml` — the blend model and inversions: `F_blend(d) = (n_a·F_asset(d) + κ·F_class_avg(d))/(n_a + κ)` with `F_class_avg(d) = mean_s F_class^z(d/(σ_s·√h))`; monotone → exact bisection (`d_for_coverage`); `Sizing(M)` functor: `find_min_capital`, `max_qty`, `empirical_min_capital` (scan-and-refine; replay D_surv is not monotone in capital).
- `oracle_strategy.ml` — strategy-model interface `S` (cost/funding functions, path `replay`, qty floors). Only implementation: `Grid` over `Grid_core` (pure mirror of live `Suicide_grid`). Replay accepts a seed (held base, resting-sell base, accumulated profit buffer) so the verdict models the grid *as it actually runs*; ladder anchors at the path's start, never-exhausted replays report `d_surv = 1.0`.

**Decision layer**
- `oracle_deploy.ml` — the deployment engine (`Engine(M)` functor, pure, strategy-generic). Resolution order:
  1. **Governing drawdown `d_gov`** — deepest drawdown whose blended coverage reaches the target (default 99% statistical basis; config default here 0.95) across all horizons; none reachable → raw fallback for immature assets; no MFD windows → inactive.
  2. **Sizing drawdown `d_cover`** — ATH-scaled reference: `floor_ref = ATH·(1 − max_drawdown)`; above the floor fund the remaining drop (capped by worst-ever drop); at/below the floor or unrecovered deepest event → measured P90 floor overshoot (0.15 fallback). Never an ATH-to-ATL construction (a 1000x run-up must not read as a phantom 99.9% drawdown).
  3. **Grid interval** — most aggressive (tightest) value in config `[lo, hi]` reaching 100% replay survival at minimum order size; unreachable → stretch mode (`gi = hi`, minimum qty).
  4. **Qty** — grows behind 100% survival only, up to `qty_min × qty_cap_mult` (ceiling, not a rule); final row re-verified at resolved `(gi, qty)`.
- `oracle_portfolio.ml` / `oracle_topology.ml` / `oracle_portfolio_state.ml` — legacy `--portfolio` multi-venue replay: capital pooled per venue account, session-boundary transfers, atomic position persistence.
- `oracle_reclaim.ml` — priority reclamation (runtime): when a higher-priority asset can't fund its first buy after a fill, the fewest lower-priority resting buys (lowest priority first) are canceled to close the gap; reclaimed assets publish INACTIVE-with-reclaim and their domain cancels through the normal order pipeline.

## Live runtime behavior

- **Event-driven, network-free decision path**: fills/cancels call `request_pass` (Atomic increment + Lwt_condition broadcast) and enqueue pool deltas (`notify_fill` / `notify_order_cancel`) applied in-process — decisions re-size in microseconds, never waiting on venue REST. A background refresher fiber owns all network I/O (balances, history deltas, fees, calendar, F&G) on its own cadence (poll 30s, refresh 300s, exponential backoff 30s→300s on failure) and publishes an immutable materialized world state via Atomic swap.
- **Memoization**: per-asset analysis keyed on materialized inputs (physical `==` fast path); per-account sizing keyed on analysis ids + pool bucketed to 0.5% + strategy state + F&G. Unchanged accounts re-publish previous decisions at microsecond cost.
- **Pass concurrency**: Phase A analyzes assets in parallel (each 60s-bounded); Phase B sizing is sequential per account (priority pass-down is order-dependent).
- **Publication**: one lock-free snapshot (`decisions ()`, Atomic-swapped immutable list). Domains adopt `active` / `qty` / `grid_interval` case-insensitively by exchange/symbol; a changed sizing sets `force_buy_reanchor` so the buy leg re-anchors both directions. When the oracle holds no decision, the domain spawner falls back to F&G-only sizing (explicitly logged) — one sizing, one owner.
- **Two-phase joint allocation**: Phase A computes each asset's reservation (informational); Phase B allocates sequentially by config priority against the entire remaining pool — nothing reserved downstream, surplus idles. Pool is venue-locked (venue+quote+testnet), never double-counted.
- **Committed capital respected**: an asset with a resting buy is never "cannot fund first buy"; sells are never capital-gated (buy-leg-only halt) so filled inventory can always be sold off.
- **Failure semantics**: last-known-good everywhere — failed/timed-out analyses keep previous decisions; failed accounts are skipped; a fully failed pass publishes nothing; the oracle never crashes the engine. Cold start publishes nothing until the first background refresh lands (startup deadline `startup_wait_seconds`, default 60, as escape hatch).
- **Observability**: per-pass latency profiling (`profiler_snapshots` / `asset_profiler_snapshots`) feeding the dashboard LATENCY KPI card; one scannable INFO line per asset per pass; warnings deduped per asset.

## Configuration

Global `oracle` section in `config.json` (see `config.json`): `target_survival`, `fng_weight`/`range_weight` (inert — sizing is survival-driven; kept for compatibility), `min_active_dsurv`, `qty_cap_mult`, `weight_by_sessions`, `no_deep_history`, `refresh_seconds`, plus optional per-symbol overrides under `assets` (a `"venue/symbol"` key wins over bare symbol; present keys merge, absent inherit). Cadence knobs (`poll_seconds`, `max_capital`, `startup_wait_seconds`) stay global. Risk-class membership comes from the top-level `classes` map; each trading entry's `asset_class` must name a class when the oracle is in use.

## Key design invariants

1. **Never forward-fill** — gaps are metadata; `analyze` refuses when `max_gap > tolerance` (default 5).
2. **No lookahead** — trailing volatility at start `s` uses only bars ≤ s.
3. **Non-overlapping windows for tails** — one contiguous crash counted once; `n_eff` is the honest sample size.
4. **Empty distributions raise**, never return 0.
5. **Blend is a true weighted average on one window basis** — σ=0 starts excluded from both sides; κ is a real pseudocount vs effective sample size.
6. **Coverage monotone → bisection sound**; replay D_surv not monotone in capital → scan-and-refine.
7. **100% survivability first** — gi from survival search, qty grows only behind 100% survival, `qty_cap_mult` is a ceiling.
8. **Verification funded with the actual pool** and seeded with accumulated state — "can THIS grid, as it runs, survive this history with the capital it's entitled to?"
9. **Failure = last-known-good**; every fetch timeout-bounded; network never on the decision path.
