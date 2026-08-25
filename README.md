# DIO

DIO is an OCaml trading engine for market making, grid trading, and capital management. It runs grid or market-making strategies on Kraken, Hyperliquid, Lighter, Interactive Brokers, and Alpaca from a single `config.json`, sizes every position with a capital-survival oracle, and exposes a live TUI dashboard over a Unix domain socket.

The engine is organized as one OCaml domain per trading asset, a supervisor that owns connection lifecycle and health, a lock-free order executor, and a supervised event-driven oracle that re-sizes positions on every fill/cancel (microsecond decision path, no network wait).

> **Warning:** the auto-hedge strategy (Hyperliquid perp shorts) is experimental. Review `auto_hedger.ml` and test on testnet before risking real capital.

---

## Quick Start

Requirements:

- OCaml 5.2 (any distribution: opam, Nix, Homebrew)
- `opam` and `dune`
- Linux or macOS (WSL2 works)

Build:

```sh
opam install . --deps-only
dune build
```

Run:

```sh
./_build/default/bin/main.exe
```

On first start the engine loads `config.json` from the working directory, connects the exchanges listed in `trading`, and lets each asset settle into its own trading domain. Logs go to stderr asynchronously (`HH:MM:SS.mmm LVL SECTION message`).

### Binaries

| Binary | Source | What it does |
| --- | --- | --- |
| `dio` | `bin/main.ml` | The engine itself |
| `dio-dashboard` | `bin/dashboard.ml` | TUI dashboard, connects to the running engine |
| `dio-oracle` | `bin/oracle.ml` | Configuration-tuning CLI: prints the decision surface offline (see Capital Oracle) |

```sh
dune exec dio-dashboard
dune exec dio-oracle -- --symbol BTC/USDC --quote 10000
```

---

## Configuration

The engine reads `config.json`. Top-level keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `logging_level` | `INFO` | One of `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL` |
| `logging_sections` | unset | Comma-separated section filters; unset means all sections |
| `logging_width` | autodetect | Message column width; autodetected from terminal or `COLUMNS` |
| `cycle_mod` | `10000` | Legacy interval for periodic background work; unused by current strategies |
| `latency_window_seconds` | `5.0` | Rolling window for network latency profiling stats |
| `gc` | see table | OCaml GC tunables applied before the engine starts |
| `oracle` | see table | Capital oracle knobs (runtime and tuning CLI) |
| `trading` | required | One entry per instrument to trade |
| `fng_check_threshold` | `1.5` | Price move (percent) from baseline that re-triggers a Fear & Greed check |
| `theme` | unset | Default dashboard theme id; overridable with `dio-dashboard --theme` |

Unknown keys under `trading` cause the engine to exit at startup; the schema is strict on purpose.

### GC tunables

Applied at process start through `Gc.set`. Units for `minor_heap_size` and `major_heap_increment` are OCaml words (8 bytes on 64-bit).

| Key | Default | Repo config |
| --- | --- | --- |
| `minor_heap_size` | `33554432` | `33554432` |
| `space_overhead` | `120` | `120` |
| `max_overhead` | `1000000` | `1000000` |
| `window_size` | `10` | `10` |
| `allocation_policy` | `2` | `2` |
| `major_heap_increment` | `100` | `8388608` |

### Trading entries

Each element of `trading` configures one symbol on one exchange:

```json
{
  "symbol": "BTC/USDC",
  "exchange": "hyperliquid",
  "qty": 0.01,
  "grid_interval": [1.0, 5.0],
  "strategy": "jacobs_ladder",
  "maker_fee": null
}
```

| Key | Applies to | Meaning |
| --- | --- | --- |
| `symbol` | all | Exchange symbol, e.g. `BTC/USD` (Kraken), `BTC` (Hyperliquid spot), `AAPL` (IBKR, Alpaca) |
| `exchange` | all | `kraken`, `hyperliquid`, `lighter`, `ibkr`, or `alpaca` |
| `qty` | all | Base order size in base currency |
| `grid_interval` | jacobs_ladder | `[gi_min, gi_max]`: the hardened bounds (in %) the oracle's parameter search walks; the strategy never reads them |
| `min_usd_balance` | MM only | Lower bound on account quote balance; MM halts below this |
| `max_exposure` | MM only | Upper bound on quote exposure for one symbol; MM halts above this |
| `strategy` | all | `jacobs_ladder` (aliases `Ladder`) or market making (`MM`, alias `market_maker`) |
| `maker_fee`, `taker_fee` | all | Explicit fee overrides (fractions, e.g. `0.0016`); `null` means use venue default or live fee lookup |
| `testnet` | HL, Lighter, IBKR, Alpaca | Route to sandbox/paper endpoints. Rejected for Kraken |
| `hedge` | Hyperliquid only | Enable the experimental perp short auto-hedge. Rejected elsewhere |
| `accumulation_buffer` | all | `[min, max]` retained quote profit buffer required before base accumulation; resolved live from Fear & Greed (crypto venues) |
| `data_feed` | Alpaca | `iex` (free, delayed) or `sip` (paid, real-time) |
| `sell_mult` | jacobs_ladder | Fraction of each ladder rung's qty sold per rung fill (`1.0` sells the full rung; smaller values accrue base) |
| `base_accumulation` | all | Persist accumulated base and profit state for this entry (default `true`) |
| `sell_levels` | jacobs_ladder | Persist pending sell levels for this entry (default `false`) |

Venue-specific restrictions are enforced at startup:

- `testnet`, `hedge`, and `data_feed` are rejected for Kraken.
- `hedge` is Hyperliquid-only.

### Oracle

The oracle runs inside the engine as a supervised module. The section is
optional; every key falls back to the defaults below. Unknown keys are
rejected at startup.

| Key | Default | Meaning |
| --- | --- | --- |
| `qty_cap_mult` | `1.5` | Buy-size upper bound multiplier: the search sizes `buy_qty` within `[qty, qty * qty_cap_mult]` |
| `target_survival` | `0.95` | Fraction of the historical max drawdown the runway covers: `runway_pct = max_drawdown_pct * target_survival`. Drives sizing only - never activity |
| `min_active_dsurv` | `0.0` | Active gate: a strategy is active iff its replayed `d_surv >= min_active_dsurv`, subject to affordability |
| `refresh_seconds` | `300.0` | Background fallback poll for history and balances (decisions are event-driven: fills/cancels re-resolve immediately) |
| `assets` | unset | Per-symbol overrides keyed by symbol, each accepting `{ target_survival, min_active_dsurv, qty_cap_mult }` |

---

## Environment Variables

Credentials and one-off knobs live in the environment. The engine loads `.env` if present (dotenv-style, `KEY=value` per line).

| Variable | Used by | Notes |
| --- | --- | --- |
| `KRAKEN_API_KEY` | Kraken | |
| `KRAKEN_API_SECRET` | Kraken | |
| `HYPERLIQUID_WALLET_ADDRESS` | Hyperliquid | Wallet that signs orders |
| `HYPERLIQUID_PRIVATE_KEY` | Hyperliquid | Private key for the EIP-712 order signer |
| `IBKR_GATEWAY_HOST` | IBKR | Default `127.0.0.1` |
| `IBKR_GATEWAY_PORT` | IBKR | Default `4002` (paper), forced to `4001` in live mode |
| `IBKR_TRADING_MODE` | IBKR | `paper` or `live`; default `paper` |
| `IBKR_CLIENT_ID` | IBKR | Default `0` |
| `IBKR_ACCOUNT_ID` | IBKR | Optional; auto-detected when unset |
| `ALPACA_API_KEY` | Alpaca | |
| `ALPACA_API_SECRET` | Alpaca | |
| `LIGHTER_API_PRIVATE_KEY` | Lighter | |
| `LIGHTER_API_KEY_INDEX` | Lighter | |
| `LIGHTER_ACCOUNT_INDEX` | Lighter | |
| `LIGHTER_SIGNER_LIB_PATH` | Lighter | Path to the signer shared library (`.dylib` on macOS, `.so` on Linux) |
| `LIGHTER_PROXY_URL` | Lighter | Comma-separated list of relay proxy URLs |
| `CMC_API_KEY` | Fear-and-Greed | CoinMarketCap API key; missing key falls back to a neutral value |
| `DISCORD_WEBHOOK_URL` | Discord notifier | Fill notifications; unset disables Discord |
| `DIO_BACKTRACE` | Engine | When set, pretty-prints OCaml backtraces on crashes |

---

## Exchanges

### Kraken

REST + WebSocket. Order book and authenticated feeds arrive over websockets; balance comes from the authenticated feed. Fees are looked up live (volume tiers) with a `0.0016` / `0.0026` maker/taker fallback. Order sizes are floored to the venue tick and lot size. No `testnet` mode.

### Hyperliquid

Spot and perpetual trading with an EIP-712 signer. Bare symbols (`BTC`) resolve to spot; `BTC/USDC` maps through the spot universe. Testnet via `"testnet": true`. `min_notional` is enforced at `10.0` USDC for spot symbols containing `/`.

The engine signs with the wallet key from `HYPERLIQUID_PRIVATE_KEY`. The agent contract (`Agent(string source, bytes32 connectionId)`) is constructed internally; there is no separate agent-address environment variable.

### Lighter

Perpetual DEX. Requires the Lighter signer shared library (`lighter-signer-darwin-arm64.dylib` on macOS, `lighter-signer-linux-amd64.so` on Linux), a funded account, and usually a relay proxy. `LIGHTER_PROXY_URL` can be a comma-separated pool of Cloudflare relay endpoints; the client round-robins and retries across them.

Lighter orders are time-limited (~28-day GTT). A renewal daemon cancel-and-replaces orders to approximate good-til-cancelled behavior, so do not rely on order IDs surviving a restart boundary.

### Interactive Brokers

Connects to an IB Gateway (e.g. `gnzsnz/ib-gateway-docker`) over TCP. `testnet` forces paper mode on port `4002`; live mode forces port `4001`. Live trading is entirely limit-based; the engine floors order quantities to whole shares. Account ID auto-detects unless `IBKR_ACCOUNT_ID` is set.

### Alpaca

US equities, paper or live. `data_feed` selects `iex` (free, 15-minute delayed bars) or `sip` (paid, real-time). The engine respects extended trading hours (pre-market 4 AM to 9:30 AM, after-hours 4 PM to 8 PM, overnight 8 PM to 4 AM ET) and uses `day` TIF with the extended-hours flag when needed. Alpaca pairs are 1:1 (no accumulation up-sizing) and fees default to zero.

---

## Strategies

### Grid (jacobs_ladder)

The default strategy. A pure executor: it buys price drops and sells the
bought base to offset volatility drag, with every sizing and activity value
coming from the oracle's decision record (`active`, `grid_interval`,
`buy_qty`, `sell_qty`). There is no config fallback path - before an oracle
decision exists for an asset the strategy places nothing.

- Buy side: exactly one resting buy below the current price, trailing upward.
- Sell side: layered sells above the current price, one per filled buy, fill-
  anchored at `buy_fill_price * (1 + grid_interval)`; never cancelled once
  placed. Sells run even while inactive: they need inventory, not quote.
- Balance model: the execution layer exposes `available_trading_balance`
  (already net of reserved_base); sell size is the oracle's `sell_qty` -
  the venue base pool minus reserved_base minus base tied in resting sells.

Accrual lives in the persistence layer: profitable sell fills reserve base via
`Base_accumulation_store`, which survives engine restarts. Base accumulation is
pre-funded out of realized quote earnings: base is reserved only when
accumulated net profit covers the acquisition cost of the withheld base plus the
configured `accumulation_buffer`, and accumulated profit is debited by that
acquisition cost upon reservation, preventing quote balance bleed.

### Market Maker (MM)

`market_maker.ml`. Places one buy and one sell around the top of book:

- Buy price = best ask minus the fee backoff (`ask * (1 - (2*fee + 0.0001))`), clamped so it never crosses the best bid. When the maker fee is `0.0` the buy sits exactly at the best bid.
- Sell price = best ask (rounded to the venue tick).
- A profitability guard refuses a spread that cannot cover round-trip fees: the rounded spread (sell minus buy) must be at least `ask * (2 * fee + 0.0001)`.

Per-symbol `min_usd_balance` and `max_exposure` bound the account; crossing either halts that symbol.

### Auto-Hedge (Hyperliquid, experimental)

`auto_hedger.ml`. Maintains one perp short per grid cycle: when the grid buys spot, it opens a short if none is open; when the grid sells, it closes the hedge. Hedges use IOC limit orders at the perp top of book. Enable with `"hedge": true` on a Hyperliquid entry.

---

## Architecture

```
config.json
   |
   v
Supervisor ── health monitor / circuit breaker / connection registry
   |
   +-- Domain (one per trading asset)
   |      strategies -> order executor -> exchange action
   |      exchange feeds -> ring buffers -> strategy loop
   |
   +-- Oracle (supervised, event-driven: re-sizes on fills/cancels, no network wait)
   |
   +-- Order executor (MPSC lock-free queue, in-flight tracking)
   |
   +-- Fill event bus -> Discord notifier, dashboard
   |
   +-- Dashboard server (UDS, JSON over length-prefixed frames)
```

### Supervisor

`supervisor.ml` is a thin orchestrator. `start_monitoring` starts the health monitor loop, a monitor for non-active assets, and the order-processing loop, then initializes feeds synchronously and returns the fee-augmented trading configs to the domain spawner.

Connections in the registry: `hyperliquid_ws`, `lighter_ws`, `kraken_orderbook_ws`, `kraken_auth_ws`, `alpaca_data_ws`, `alpaca_trading_ws`, `ibkr_gateway`, `oracle`. The order executor is deliberately not in the registry; it never blocks on network I/O.

Health rules:

- Exponential restart backoff `0/2/4/...` capped at 30 seconds (300 seconds for `ibkr_gateway` and `lighter_ws`).
- A connection idle for 60 seconds is restarted; a websocket stuck connecting for 120 seconds is killed.
- Websocket ping/pong: 15-second interval, 5-second timeout, 3 missed pongs before restart. Passive data feeds must heartbeat at least every 60 seconds.
- Circuit breaker: 5 consecutive failures open the breaker for 5 minutes, then it re-tries in half-open mode.

### Domains and wakeups

Each active asset runs its own domain. Exchange feeds write into single-writer/multi-reader ring buffers; the strategy loop wakes through `Exchange_wakeup`, which keeps a monotonic per-symbol generation counter. Producers bump the counter after writing data; the domain captures it at cycle start and waits against that baseline at cycle end, so a signal landing mid-cycle makes the wait return immediately instead of parking through pending data (the classic check-then-sleep lost-wakeup race). The wait spins briefly on the lock-free counter before parking on the condition variable, absorbing near-simultaneous signals without a kernel round-trip. Quiet domains park normally; nothing busy-polls.

Ring buffer cursors are absolute write positions, not slot indices: they stay valid across laps (a stalled reader resumes at the oldest surviving entry instead of aliasing to "empty") and across clears (resubscribes), and slots carry sequence numbers so a writer racing an iteration cannot cause duplicated or torn reads.

Order lifecycle events from REST callbacks (acks, rejects, amend results) do not touch strategy state directly: they are pushed onto a per-symbol lock-free queue and drained by the symbol's own domain at the top of each cycle, so strategy state has exactly one writer thread.

### Feed parsing

High-rate frame parsing runs on a dedicated worker domain (`src/engine/concurrency/parse_worker.ml`) rather than the Lwt scheduler thread that multiplexes all venue sockets. Kraken's WebSocket client diverts executions and orderbook frames by raw-string prefix before any JSON parsing; the worker parses and dispatches them sequentially, preserving per-venue order. When the worker's queue fills, frames fall back to inline parsing - never dropped (Kraken book updates are deltas, so a dropped update would desync the local book). Hyperliquid's l2Book channel needs no offloading: its top-of-book is extracted by a zero-copy string scan into an Atomic snapshot, and the full-book JSON parse only happens on the dashboard cadence.

### Order executor

Strategies enqueue intents into a lock-free MPSC queue (capacity 64k). The executor shards in-flight orders by `duplicate_key` across 64 shards, tracks amendments, and suppresses redundant no-change amendments. After a restart the executor re-syncs against open orders fetched from the venue before domains resume.

### Error handling

All exchange I/O funnels through `error_handling.ml`: callers classify errors (`Connection`, `Timeout`, `RateLimit`, `ServerError`, `ParseError`, `OrderRejected`, `InvalidRequest`, `Unknown`) and retry with exponential backoff (default: 3 attempts, 1000 ms base, 30 s max, factor 2). Feed loops self-restart with backoff instead of crashing the domain.

### Logging

`logging.ml` provides five levels and per-section colors, with column width autodetected from the terminal or `COLUMNS`. Messages drain through an async worker; `CRITICAL` flushes synchronously. Format: `HH:MM:SS.mmm LVL SECTION message`.

### Persistence

State lives in two JSON files under `data/`, written atomically (temp file + rename):

- `accumulation_state.json` (`base_accumulation_store.ml`, orchestrated by `persistence_orchestrator.ml`): per `{strategy}:{symbol}:{venue}` key, fields `reserved_base`, `accumulated_profit`, `last_fill_oid`, `last_buy_fill_price`, `last_sell_fill_price`, `last_buy_fill_qty`, `last_sell_fill_qty`. Opt-in per trading entry via `base_accumulation`.
- `sell_levels_state.json` (`sell_levels_store.ml`): pending sell levels for entries with `sell_levels: true`.

On startup, a legacy flat `data/accumulated_state.json` is migrated into these files and renamed to `accumulated_state.json.migrated.<ts>`. Used by the Jacobs ladder on Hyperliquid, Lighter, IBKR, and Alpaca. In Docker, mount `/app/data`.

---

## Capital Oracle

The oracle is a capital-survival sizing engine. For each asset it answers:
"given this asset's all-time price history and the quote capital available on
its venue, what order size and grid interval keep the ladder alive down the
runway this asset has actually walked?"

### Pipeline (one code path)

- **History**: the asset's all-time merged daily series - the venue adapter's
  bars (`Exchange_intf.Oracle.Registry`), disk-cached and delta-fetched,
  extended BACKWARDS with Yahoo deep history for the same underlying. Venue
  bars win on overlap; nothing is synthesized, there is no gap tolerance and
  no minimum length.
- **References**: `max_drawdown_pct` is the single worst peak-to-trough
  percentage decline in the whole series (running peak of closes to the
  deepest subsequent low - recovery is irrelevant), plus ATH/ATL from
  intrabar extremes.
- **Runway math**: `realized_dd = (ath - current) / ath`;
  `runway_pct = max_drawdown_pct * target_survival`;
  `floor_price = ath * (1 - runway_pct)`. Three regimes, evaluated in order:
  *Normal* (`current > floor_price`: fund the remaining drop),
  *Floor extension* (at/below the floor: funding extends down to ATL),
  *Unprecedented lows* (at the deepest drawdown AND at/below ATL:
  maximum conservatism). Aggressiveness = `realized_dd / max_drawdown_pct`
  biases parameter selection toward tighter grids and larger sizes deeper in
  the runway; it never overrides survival requirements or bounds.
- **Survival replay**: a candidate `(grid_interval, buy_qty)` walks the
  funded depth geometrically, paying each buy plus venue fees;
  `d_surv` is the fraction of that depth survived before the quote runs out
  (>= 1.0 means fully funded).
- **Parameter search**: `buy_qty` within `[qty, qty * qty_cap_mult]`,
  `grid_interval` within `[gi_min, gi_max]` (the strategy entry's bounds).
  Branches, in order: *Unreachable* (no candidate meets the funded-depth
  requirement -> conservative corner `qty` / `gi_max`); *Reachable* (largest
  size at tightest spacing keeping `d_surv >= target_survival`,
  aggressiveness-biased); *Surplus* (the aggressive corner exceeds the target
  with quote left over -> `qty_max` / `gi_min`).
- **Decision record**: exactly `{active, grid_interval, buy_qty, sell_qty}`,
  raw floats, generated by exactly one code path. Values are emitted for
  inactive strategies too, for visibility and immediate reactivation. All
  normalization (tick/lot rounding, min notional) happens at the exchange
  layer. Activity: `active <=> d_surv >= min_active_dsurv` subject to
  affordability; sells always run (they need inventory, not quote).

### Pooling, priority & cascades

Pools are per venue: one quote pool shared by that venue's strategies, never
crossing venues (two strategies may share one symbol on one venue).
Allocation walks strategies in config presentation order (first = highest
priority): each strategy sizes against the entire remaining availability, its
next buy ties up `buy_qty * current`, unfundable strategies are skipped and
capacity passes down - a lower-priority strategy is never starved while quote
exists. When a higher-priority need cannot fit, the cancellation cascade
cancels lower-priority resting buys - many lesser orders may be cancelled to
satisfy one greater - until it fits; if no combination fits, resolution
proceeds to the next-highest priority. Every cancelled strategy re-evaluates
on that event and resumes iff quote covers its buy.

### Runtime behavior

Inside the engine the oracle is a supervised module (registered as `oracle`),
heartbeated on every published pass plus a liveness ticker, auto-restarted by
the health monitor. Decision computation is pure and in-memory (<10 ms p99):
histories live in a cache refreshed on the background cadence, balances come
from the websocket-fed live store or the last snapshot adjusted IN PROCESS by
fill/cancel deltas, live prices read the in-process top of book. Event bursts
coalesce into a single re-resolve; only domains whose decision changed are
woken per pass.

### CLI (`dio-oracle`)

Runs the exact decision pipeline offline against configured assets and prints
the decision surface - the four contract values plus diagnostics - without
touching live balances:

```sh
dune exec dio-oracle                          # every trading entry, table output
dune exec dio-oracle -- --symbol BTC/USDC     # one asset
dune exec dio-oracle -- --quote 50000 --base 0.5   # synthetic pool sizing
dune exec dio-oracle -- --cache-only          # never touch the network
dune exec dio-oracle -- --json                # machine-readable
```

The oracle config section (`config.json`) drives both the runtime and this
CLI; strategy entries carry the hardened bounds the search walks.
