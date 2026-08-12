The DIO Capital Oracle — What It Is
The oracle is a capital-survival sizing engine for the grid-trading strategy. The question it answers for each asset:
"Given this asset's real price history and the quote capital available on its exchange account, what order size (qty) and grid interval (gi) should the grid run at, so that it almost certainly (99% by default) survives the worst drawdowns this asset has actually experienced?"
It runs in two places:
1. bin/oracle.ml — the dio-oracle CLI (one-shot: fetch history → analyze → print report).
2. oracle_runtime.ml — inside the live engine (continuous loop: re-analyzes every ~5 min, publishes decisions the trading domains adopt).
The core statistical idea is MFD — Maximum Fractional Drawdown: over a window of h sessions, how deep did the price fall from the session-start close?
MFD(s, h) = 1 − (min low over sessions (s, s+h]) / close at s
F_h(d)    = P(MFD_h ≤ d)     ← "coverage" (a CDF)
S_h(d)    = 1 − F_h(d)        ← "survival"
The oracle computes these empirically from the asset's own history, blends them with a risk-class prior (pooled histories of "similar" assets) via a kappa-weighted, volatility-normalized blend, then inverts the blended CDF: "what drawdown must we fund to reach 99% survival?" — and finally sizes the grid ladder so its capital runway covers exactly that drawdown, replaying the strategy over the historical path to verify.
1. Data Layer
oracle_types.ml — shared types: bar, series, gap, horizon, survival_surface, percentile_table, blended_surface, sizing_result, range_stats, deployment_row, asset_deployment, venue_deployment, plus the calendar_kind (Crypto = 24/7 days; Equity = market sessions).
oracle_calendar.ml — session-consistent time handling:
- Pure civil-date arithmetic (days-from-epoch, Hinnant's algorithms) — deliberately not mktime, which is timezone-dependent and corrupted equity gap detection.
- Sorts bars by ISO date, de-duplicates.
- Gap detection: for crypto, any calendar day without a bar is a gap; for equity, a weekday missing from the expected-session model (US weekdays minus holidays, from Alpaca's calendar) is a gap.
- The "never forward-fill" rule: gaps are surfaced as metadata; Oracle.analyze refuses to analyze if max_gap > tolerance (default 5) — missing data is never fabricated.
oracle_sessions.ml — expected-session models for equities: business_weekday (Mon–Fri), explicit_model (exact date set), alpaca_model (weekday ∧ in calendar), with_holidays. Pure math.
oracle_fetch_*.ml — four data sources:
- Kraken: public /0/public/OHLC, paginated on last (capped ~720 daily candles).
- Hyperliquid: /info candleSnapshot; resolves spot vs perpetual — bare coin = perpetual, BASE/QUOTE maps through spotMeta universe to the candle coin (@N alias or PURR/USDC). A spot-named symbol with no matching pair gets no bars (→ inactive), never silently substituted with perp data.
- Alpaca: /v2/stocks/{symbol}/bars (IEX/SIP) + /v2/calendar for session dates.
- Yahoo (oracle_fetch_yahoo.ml): deep-history extension. Venue feeds cap history (~720 days Kraken, 2020 Alpaca IEX), so for the same underlying asset it fetches full daily history from Yahoo and prepends it (venue bars win on overlap; nothing synthesized). Crypto symbols only via an explicit whitelist (dead-token collision safety, e.g. HYPE-USD serves a dead 2021 token's prices).
oracle_fees.ml — resolves real maker/taker fees per venue (Kraken TradeVolume tier, Hyperliquid userFees, Alpaca = 0). Explicit maker_fee in config wins; fetch failures fall back to venue defaults; cached per (exchange, symbol).
oracle_venues.ml — venue instrument metadata (price ticks, lot sizes) fetched once from each exchange, so the replay uses real increments, not the 0.01 fallback. Also forces venue module registration (OCaml dead-code elimination would otherwise skip the registry side effects).
oracle_balances.ml — one-shot account balance snapshots (read-only; deliberately not the engine's background balance stores). Kraken Balance, Hyperliquid spot spotClearinghouseState (perp balances deliberately excluded — margin is not grid capital), Alpaca account + positions.
oracle_tasks.ml — resolves CLI symbol/--exchange into analysis tasks (or all config.json trading entries). Maps exchange → calendar kind. oracle_loader.ml — offline CSV/JSON fixture loading.
grid_adapter.ml — bridges a config.json trading_config into a Grid_core.config for replay: increments from the venue registry, min_notional (Hyperliquid spot = 10 USDC floor, others 0), exchange model.
2. Statistical Core
oracle_math.ml — dependency-free helpers:
- percentile (Type-7 linear interpolation) and weighted_percentile (same estimator with weights; reduces exactly to the unweighted version with unit weights). Both raise on empty input — an empty distribution must never masquerade as "this asset never drew down".
  - range_stats_of — per-asset ATH / all-time low / current price / d_from_ath / d_to_low / range_span. Report context only: the ATH-to-ATL span is deliberately NOT the sizing drawdown (a 1000x run-up would make it read like a 99.9% drawdown).
  - peak_to_valley_stats_of — the sizing drawdown: each bar's drawdown from the running peak of closes down to that bar's low, maximized over the whole (deepened) history. The largest ACTUAL peak-to-valley fall the asset has really taken, with the peak/valley prices and dates. None only when the history is empty or strictly monotone (never drew down).
- ath_anchored_drawdown — see §5; the absolute cap on ladder scale.
oracle_mfd.ml — the MFD machinery:
- mfd — rejects incomplete windows (would silently bias the CDF toward smaller drawdowns), non-finite/non-positive closes, zero horizons.
- f_h / survival / n_starts / samples — empirical CDF machinery over starts in [warmup, n−horizon−1]. The stride parameter is crucial: stride = horizon gives non-overlapping windows (independent observations); stride 1 gives overlapping (autocorrelated). Percentile tables are estimated on non-overlapping windows so one contiguous crash isn't counted once per rolling start — n_eff reports the true independent count (a 365-day horizon on ~1600 sessions yields only ~4 independent windows; P99 is then "the worst of those", which is the honest answer).
- surface / percentile_table — per-horizon output rows.
- Static runway math: the closed-form ladder cost C_used(N) = (1+fee)·q·C_s·(1−gi)·(1−(1−gi)^N)/gi and floor_aware_runway_cost — a walk of the actual ladder that replicates Grid_core's rounding and dynamic buy up-sizing (q_k = max(qty, ceil_lot(min_notional/level_k))), because the closed form understates cost when the venue's notional floor binds.
oracle_stats.ml — the no-lookahead invariant: trailing volatility at start s uses only closes from [s−W, s]. z(s,h) = MFD(s,h) / (σ_s·√h) — the volatility-normalized drawdown. blend is the kappa pseudocount: F_blend = (n_a·F_asset + κ·F_class)/(n_a + κ).
oracle_archetypes.ml — the risk-class data model (crypto_core, crypto_alt, equity_etf, equity_momentum) with default kappa = 365 (crypto) / 252 (equity) — one per-session "prior weight" per full year of class history.
oracle_classes.ml — pooled class curves (Phase 2):
- Membership comes from config.json's "classes" map, never hardcoded.
- pooled — all members' per-start MFD samples pooled with either session-count weighting (longer histories dominate) or equal weight per member.
- class_surface / class_percentile_table — the class's own CDF/percentile tables over the pooled samples.
- z_index_of / z_cdf_of — the pooled class z-distribution (sorted weighted z-samples with prefix sums, O(log n) evaluation). This is what makes the blend vol-fair: the class contribution is evaluated at each asset start's own volatility regime, so a low-vol asset isn't punished for a high-vol classmate's raw swing size.
oracle.ml — the facade:
- analyze: sort/dedup bars → detect gaps → fail-fast if max_gap > tolerance → compute per-horizon surfaces, percentile tables, and per-class estimates + blended surfaces/tables.
- default_config — crypto horizons [30; 90; 180; 365] days, equity [21; 63; 126; 252] sessions; thresholds 5–50%; percentiles 50–99%; vol_window 60; gap_tolerance 5.
- blended_percentile_table — inverts the z-blend CDF per percentile via bisection (the blend is monotone in d, so bisection is exact on the empirical step function).
3. The Blend Model (oracle_replay.ml)
The heart of the model. For one (asset, horizon, stride-basis):
F_asset(d)     = share of the asset's own windows with MFD ≤ d     (raw CDF)
F_class_avg(d) = (1/n)·Σ_s  F_class^z( d / (σ_s·√h) )
                 ← pooled class z-CDF evaluated at each asset start's own vol
F_blend(d)     = (n_a·F_asset(d) + κ·F_class_avg(d)) / (n_a + κ)
Key details:
- stride defaults to the horizon (non-overlapping windows) for every coverage evaluation — sizing inversion, surfaces, and percentile tables all share one consistent basis.
- n_asset is the window count on that same basis — so κ is a true pseudocount against the asset's effective independent sample size. A thin non-overlapping sample shrinks toward the class instead of pretending overlapping starts are independent information.
- Sigma = 0 (flat/gap-adjacent) windows are excluded from both sides — mapping them to τ = +∞ would inject false 100% class certainty; keeping them only in F_asset would make an inconsistent weighted average.
- All three are monotone non-decreasing in d → bisection is sound (explicitly noted because replay D_surv is not monotone in capital).
d_for_coverage — bisection over d ∈ (0, 0.999999) to find the smallest drawdown whose coverage reaches target; 40 iterations ≈ machine precision. The cap at 0.999999 protects fills_for_drawdown's log from saturation. Callers re-check achieved coverage because the cap can sit in the gap between "deepest exhausting coverage" and "never-exhausted 1.0".
Sizing functor (strategy-generic inversions, instantiated over Oracle_strategy.Grid):
- find_min_capital — smallest capital whose static runway survives the actual max drawdown (peak-to-valley; the largest fall the asset really took). The static runway is the closed-form worst case (N consecutive buys, no sells) — structurally pessimistic, so this is the safe recommendation.
- max_qty — largest qty the grid's start_quote budget can fund through the actual max drawdown (exponential growth + bisection; exact under floor-aware cost).
- empirical_min_capital — advisory: the smallest capital whose actual path replay clears the target. Scans a log-spaced capital grid and bisection-refines the first crossing (plus a second pass to bound non-monotone islands, since replay D_surv is path-dependent). Measures the "capital buffer" the pessimistic static sizing pays for.
oracle_strategy.ml — the strategy-model interface (S signature): a strategy supplies cost_at / fills_for_drawdown / drawdown_of_fills (funding function), replay (path replay), profit_proxy, qty floors. The only implementation is Grid, backed by Grid_core (the pure mirror of the live Suicide_grid semantics). Notable: the replay anchors the ladder at the path's start (the earliest bar), not today's close — anchoring at the last close would grind the ladder down through the whole historical range on any uptrend, burning capital on a phantom drawdown. A never-exhausted replay reports d_surv = 1.0 (keeps coverage monotone in capital for the binary searches).
4. The Deployment Engine (oracle_deploy.ml)
This is the core decision. Given a venue-locked capital pool, the asset's blend models, and the config's grid_interval range [lo, hi], it decides: tuned gi, order qty, and whether the asset stays active.
Resolution order (all pure, strategy-generic via the Engine functor):
1. Governing drawdown d_gov — across all horizons, the deepest drawdown whose blended coverage reaches the target (99%). This is the binding horizon for the coverage verification. If no horizon can reach it (coverage gap), fall back to the deepest drawdown the asset's own history actually observed ("raw" fallback for immature assets, flagged not-authoritative). No MFD windows at all → inactive ("no usable history").
2. Sizing drawdown d_cover — the largest ACTUAL peak-to-valley drawdown of the asset's history (peak_to_valley_stats_of): the fall from the current price down that the grid must fund. No ATH anchoring: a 1000x run-up only registers the falls that actually happened — never the ATH-to-ATL span, which would read as a phantom 99.9% drawdown (the old ATH-anchored model sized the fall to ATH·(1−d_gov), which treated the run-up's gain as fundable downside). An asset sitting at the valley of its worst event still must fund that drawdown from the current price.
3. Parameter scan — candidates across [lo, hi] (10 steps). For each: invert qty from the pool (bounded by the qty cap = config qty × --qty-cap-mult), then verify by replaying the path funded with the asset's actual pool budget (the honest question: "can it survive this history with the capital it's entitled to?" — static ladder cost understates a long path's burn). survival_parameter = the tightest gi that clears the target on the replayed path. If nothing clears, fall back to the tightest gi whose runway for the actual max drawdown the pool can fund at the sizing floor ("as aggressive as the capital allows").
4. Resolve the parameter — the three-sided blend:
- F&G side (crypto only): lo + (hi−lo)·fng/100 — the contrarian convention: fear tightens the grid (densifies levels, accumulates at depressed prices).
- Range side: lo + (1−position)·(hi−lo) where position = (peak − price)/(peak − valley) of the largest actual peak-to-valley event — above the event peak the full max drawdown is still ahead → widen (preserve runway); at the event valley the downside is bounded by what actually happened → tighten (aggressive accumulation).
- Survival side: the remainder weight. Equities are pure oracle — no F&G, and the range side is not allowed to loosen the survival parameter.
- Result is clamped to [lo, hi] and never tighter than survival_parameter ("runway wins over sentiment"). Clamping emits a warning naming the blend value that was clamped away.
5. Final row + qty — re-verify at the resolved parameter (down-sizing qty if needed). Crypto blends qty with F&G too: fear up-sizes toward the survival-max, greed pulls back toward the floor, never above the survival-max. The blended row is re-verified so published D_surv/coverage reflect what actually runs.
Inactive reasons: no usable history, pool can't fund the first buy at the sizing floor (max(venue qty_min, config qty) — never below the configured qty, since a venue qty_min is a lot precision, not a minimum order size), or replayed D_surv below --min-active-dsurv.
Fallback mode (immature history): the observed max drawdown is the only signal; the criterion becomes the static funding check (pool can fund the actual peak-to-valley drawdown at the sizing floor) and qty still grows up to the cap while the check holds — volume-driven, not pinned at the floor.
sizing_floor note: max(min_qty, design_qty) — sizing an order to the raw venue qty_min produces a sub-minimum buy the venue rejects ("cost basis must be ≥ minimal amount").
The tuning surface (tuning_rows) records every candidate row for the report: parameter, qty, deployed, static/replay D_surv, per-horizon coverage, passed, static_funded, profit_proxy.
5. The Live Runtime (oracle_runtime.ml)
Runs the same pipeline continuously inside the engine (started via Oracle_runtime.start, typically by the supervisor). Defaults mirror the CLI.
Publication model: one lock-free snapshot — an immutable decision list swapped via a single Atomic.set. Trading domains do an Atomic.get each cycle and adopt qty / grid_interval / active. Case-insensitive lookup by exchange/symbol. Each decision carries parameter_components — the blend composition (F&G value and its mapped gi, the survival-constrained gi, the range side and the weights) — so consumers adopt the blended value and never recompute a competing F&G-only gi. The domain spawner's per-cycle F&G re-evaluation therefore: (a) while the oracle holds a decision, only refreshes accumulation_buffer (which the oracle does not size) and logs the oracle's published blend; (b) only when the oracle has NO decision (asset not modeled / analysis failed) applies the F&G-only mapping over the config range, logged explicitly as fallback. One blend, one owner — the two signals never fight.
Every pass logs one scannable line per asset at INFO - the heartbeat of the decision: "[2/4] hyperliquid/HYPE/USDC ACTIVE — buy 0.5 HYPE every 5.00% | capital $10.06 of $864.27 | worst drop 70.9% (2024-12-21→2025-04-07) | survives 9.8% | UNDER-FUNDED". The detail block (drawdown event prices, model horizon, gi/qty blend composition) is logged once and then only when it changes; problems (under-funded, clamped grid, coverage gaps) go to WARN, deduped per asset. History/reservation lines are debug. Steady-state passes are ~10 short lines total, not a wall of text.
Per-asset knobs: the global "oracle" section in config.json can carry an optional "assets" map overriding any sizing/blend/history knob per trading symbol (case-insensitive; a "venue/symbol" key like "hyperliquid/HYPE/USDC" wins over the bare symbol): target_survival, fng_weight, range_weight, min_active_dsurv, qty_cap_mult, no_deep_history, weight_by_sessions, horizons. Present keys merge onto the global values, absent keys inherit; capital pooling is untouched (still venue-level joint allocation below). Cadence and wait machinery (refresh/poll/max_capital/startup_wait_seconds) stay global. The startup log lists the effective override map and warns on keys matching no tracked symbol.
Two-phase joint allocation per venue account (this is what makes it joint, not per-asset):
- Phase A: analyze every asset first, computing each asset's reservation: the minimum funding to reach its actual max peak-to-valley drawdown at the tightest config gi (ladder cost at the sizing floor + first-buy cost).
- Phase B: allocate sequentially, highest priority first. Each asset's budget = remaining pool minus a reserve for the lower-priority assets' minimum drawdown funding. So a priority asset only grows after the rest of the account can still fund their drawdowns at minimum qty; an asset is disabled only when even its first buy can't be funded. With enough capital every active asset reaches its full runway; only under a genuine shortage does the lowest-priority asset go inactive. Unused capital passes down the priority order; fully deployed accounts show ~zero pool (a normal state — "cannot fund the first buy" while awaiting sell fills to restore capital).
Cadence: normal refresh every 300s; but while no asset is active (e.g. capital-empty account), it polls at 30s so a capital return on sell fills resumes trading quickly. Jittered per-instance so multiple accounts don't pile up on the same tick.
Event-driven passes: the engine calls request_pass (one Atomic increment, microsecond-scale) on every fill and canceled/rejected/expired order; the loop wakes within ~50ms (checked in 50ms slices), coalescing bursts via a 2s minimum gap.
Failure semantics — last-known-good: an asset whose analysis fails keeps its previous decision; an account whose balance can't be fetched is skipped entirely; an entirely failed pass publishes nothing and the runtime never crashes the engine. publish merges fresh decisions over the old snapshot, so a partial pass never halts trading.
Deepening & members: each asset's venue series is extended with Yahoo deep history (unless disabled); class members are fetched on the asset's own exchange, deepened, and cached per pass. Alpaca gets a real market-calendar model; failures fall back to business weekdays. F&G is resolved (live index, cached, None on failure — never a fabricated sentiment value).
Warnings are deduped per asset (log once on change, then debug) — the oracle re-derives the same "under-funded" warnings every pass.
6. Portfolio Mode (legacy --portfolio)
oracle_topology.ml — qualified instrument identity (venue/symbol/base/quote/testnet), topology JSON parsing/validation, transfers (SESSION:FROM->TO=AMOUNT), timeline alignment without forward-filling.
oracle_portfolio.ml — a multi-asset replay: capital is pooled per venue account (venue + quote + testnet), never per asset, never system-wide. All positions on a venue draw from one shared pool (a cash_hook into Grid_core), so a sibling's buy starves your grid too — the venue's survival is the pool's survival. Transfers move budget between venue pools at session boundaries; troughs track the actual joint pool minimum.
oracle_portfolio_state.ml — persistence (--positions-file / --save-positions): pool/base per qualified key, atomic write (tmp + rename).
7. The CLI (bin/oracle.ml)
Usage: dio-oracle [SYMBOL] [options]; with no symbol, analyzes every config.json trading asset on its own exchange, grouped by venue account, in config priority order, threading remaining pool down. Key modes:
- Per-asset analysis: fetches fees → series → deepens → loads class members → analysis → blend models → deploy_asset → supporting analysis (replay at the deployment's sizing, per-horizon path coverage, static min capital, max qty, empirical min capital with buffer ratio).
- --portfolio / --topology / --allocation / --transfer: the venue-pooled portfolio replay with capital assignment rules (explicit allocations win; --total-capital splits; else live balances; 1000 offline).
- --from-csv / --from-json: offline mode, single asset, no network.
- --json: machine-readable report.
- Notable knobs: --capital, --target-survival, --fng / --fng-weight / --range-weight, --min-active-dsurv, --qty-cap-mult, --kappa / --class / --members, --horizons / --thresholds / --percentiles, --gap-tolerance, --vol-window, --no-deep-history, --max-capital, per-venue gates (--qty-min, --min-notional, increments).
Key Design Invariants (worth internalizing)
 1. Never forward-fill — gaps are metadata; analysis refuses when max_gap > tolerance.
 2. No lookahead — trailing volatility at start s uses only bars ≤ s.
 3. Non-overlapping windows for the tail — a single contiguous crash is counted once; n_eff is the honest sample size.
 4. Empty distributions raise, never return 0 — 0.0 from zero observations would masquerade as "never drew down".
 5. The blend is a true weighted average on one window basis — sigma=0 starts excluded from both sides; n_asset = effective window count so κ is a real pseudocount.
 6. Coverage functions are monotone → bisection is sound; replay D_surv is not monotone in capital → scan-and-refine for empirical sizing.
 7. The sizing drawdown is the largest ACTUAL peak-to-valley drawdown — the worst fall the asset really took, never an ATH-anchored or ATH-to-ATL construction (a 1000x run-up must not read as a 99.9% drawdown).
 8. Runway wins over sentiment — the resolved gi is never tighter than the survival-constrained parameter.
 9. Verification is funded with the actual pool budget — the honest survival question; static cost would under-state a long path's burn.
10. Failure = last-known-good everywhere — the runtime degrades gracefully, never crashes the engine.
A few observations from the review (not bugs, but worth knowing):
- Oracle_deploy.shrink_qty has a leftover Printf.eprintf "SHRINK gi ..." debug line that will spam stderr on every verification during CLI/runtime sizing — looks like debugging output that wasn't gated behind the logging module.
- Kappa defaults differ slightly by context: Oracle_archetypes says 365/252, but the runtime and CLI fall back to 200 when config has no per-class kappa (config.json "classes" entries override).
- The runtime and CLI duplicate a fair amount of pipeline code (fetch/analyze/size logic appears in both bin/oracle.ml run_one and oracle_runtime.ml analyze_asset/size_asset); the runtime's version is the maintained one and the CLI's is largely parallel.