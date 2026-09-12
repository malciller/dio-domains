

**CAUTION.** The auto-hedge strategy (Hyperliquid perpetual shorts) is experimental. Review `auto_hedger.ml` and validate the strategy on a testnet before committing capital.

---

## Contents

- [1.0 SCOPE](#10-scope)
- [2.0 REFERENCED DOCUMENTS](#20-referenced-documents)
- [3.0 ABBREVIATIONS AND ACRONYMS](#30-abbreviations-and-acronyms)
- [4.0 SYSTEM ARCHITECTURE](#40-system-architecture)
- [5.0 CONFIGURATION](#50-configuration)
- [6.0 ENVIRONMENT VARIABLES](#60-environment-variables)
- [7.0 EXCHANGE ADAPTERS](#70-exchange-adapters)
- [8.0 STRATEGIES](#80-strategies)
- [9.0 CAPITAL ORACLE](#90-capital-oracle)
- [10.0 BUILD AND EXECUTION](#100-build-and-execution)
- [11.0 CAUTIONS AND LIMITATIONS](#110-cautions-and-limitations)

---

## 1.0 SCOPE

### 1.1 Identification

This document, DIO-SPS-001, is the software product specification for the DIO trading engine (hereinafter "the engine"). It defines the system architecture, configuration interface, exchange adapters, trading strategies, capital oracle, and execution procedures of the engine.

### 1.2 System Overview

DIO is an OCaml trading engine that performs market making, grid trading, and capital management. The engine executes grid and market-making strategies on Kraken, Hyperliquid, Lighter, Interactive Brokers, and Alpaca. A single configuration file, `config.json`, specifies all traded instruments. Position sizing is governed by a capital-survival oracle. A live terminal user interface (TUI) dashboard is served over a Unix domain socket (UDS).

The engine is partitioned into the following principal components:

a. one OCaml domain per trading asset;
b. a supervisor that owns connection lifecycle and health;
c. a lock-free order executor; and
d. a supervised, event-driven oracle that re-sizes positions on every fill or cancel.

The sizing decision path executes in memory and does not wait on network I/O.

### 1.3 Document Overview

Section 2 lists referenced documents. Section 3 defines abbreviations and acronyms. Section 4 describes the system architecture. Section 5 specifies the configuration file. Section 6 specifies environment variables. Section 7 describes the exchange adapters. Section 8 describes the trading strategies. Section 9 describes the capital oracle. Section 10 specifies build and execution. Section 11 records cautions and limitations.

---

## 2.0 REFERENCED DOCUMENTS

### 2.1 Project Documents

| Document | Description |
| --- | --- |
| `config.json` | Runtime configuration; see Section 5.0 |
| `dio.opam` | OCaml package manifest and dependency declarations |
| `dune-project` | Dune build-system project definition |
| `LICENSE` | Project license |
| `THIRD_PARTY_LICENSES` | Third-party license notices |

Source modules cited throughout Sections 4.0 through 9.0 are authoritative over the descriptions in this document.

---

## 3.0 ABBREVIATIONS AND ACRONYMS

| Abbreviation | Definition |
| --- | --- |
| ATH | All-Time High |
| ATL | All-Time Low |
| CLI | Command-Line Interface |
| DEX | Decentralized Exchange |
| EIP | Ethereum Improvement Proposal |
| ET | Eastern Time |
| GC | Garbage Collector |
| GTC | Good-Til-Cancelled |
| GTT | Good-Til-Time |
| IBKR | Interactive Brokers |
| IEX | Investors Exchange |
| JSON | JavaScript Object Notation |
| MM | Market Maker |
| MPSC | Multi-Producer, Single-Consumer |
| N/A | Not Applicable |
| p99 | 99th Percentile |
| REST | Representational State Transfer |
| SIP | Securities Information Processor |
| SSE | Server-Sent Events |
| TBD | To Be Determined |
| TIF | Time In Force |
| TUI | Text User Interface |
| UDS | Unix Domain Socket |
| USD | United States Dollar |
| USDC | USD Coin |
| WS | WebSocket |

---

## 4.0 SYSTEM ARCHITECTURE

### 4.1 Architectural Overview

Figure 1 depicts the runtime architecture. The supervisor owns connection lifecycle and health. Each active trading asset is serviced by an independent domain. Strategies and exchange feeds meet at the domain boundary; order intents funnel through a lock-free executor, and the oracle re-sizes positions on fill and cancel events.

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

**Figure 1. Engine architecture.**

### 4.2 Supervisor and Health Monitoring

`supervisor.ml` is a thin orchestrator. `start_monitoring` starts the health monitor loop, a monitor for non-active assets, and the order-processing loop; it then initializes feeds synchronously and returns the fee-augmented trading configurations to the domain spawner.

The connection registry contains the following connections: `hyperliquid_ws`, `lighter_ws`, `kraken_orderbook_ws`, `kraken_auth_ws`, `alpaca_data_ws`, `alpaca_trading_ws`, `ibkr_gateway`, and `oracle`. The order executor is deliberately excluded from the registry; it never blocks on network I/O.

Health rules are as follows:

a. Restart uses exponential backoff `0/2/4/...` capped at 30 seconds (300 seconds for `ibkr_gateway` and `lighter_ws`; Alpaca feeds wait 2 seconds on their first attempt).
b. A connection idle for 60 seconds is restarted; a websocket stuck connecting for 120 seconds is terminated.
c. Websocket ping/pong uses a 15-second interval and a 5-second timeout (10 seconds for `kraken_auth_ws`), with 3 missed pongs before restart. `kraken_orderbook_ws` is not pinged and relies on the passive heartbeat. Passive data feeds shall heartbeat at least every 60 seconds (`ibkr_gateway` is exempt).
d. The circuit breaker opens for 5 minutes after 5 consecutive failures and then re-tries in half-open mode. It is currently wired only for `ibkr_gateway`.
e. The main-loop watchdog (`main_loop_watchdog.ml`) requires the main Lwt loop to beat every 5 seconds; a stall exceeding 60 seconds shall force-exit the process for supervised restart. Setting `DIO_WATCHDOG_OFF` disables the watchdog.

### 4.3 Trading Domains and Wakeups

Each active asset runs in its own domain. Exchange feeds write into single-writer/multi-reader ring buffers. The strategy loop wakes through `Exchange_wakeup`, which maintains a monotonic per-symbol generation counter. Producers increment the counter after writing data; the domain captures the counter at cycle start and waits against that baseline at cycle end, so a signal that lands mid-cycle makes the wait return immediately instead of parking through pending data (the classic check-then-sleep lost-wakeup race). The wait spins briefly on the lock-free counter before parking on the condition variable, absorbing near-simultaneous signals without a kernel round-trip. Quiet domains park normally; no component busy-polls.

Ring buffer cursors are absolute write positions, not slot indices. They remain valid across laps (a stalled reader resumes at the oldest surviving entry instead of aliasing to "empty") and across clears (resubscribes), and slots carry sequence numbers so that a writer racing an iteration cannot cause duplicated or torn reads.

Order lifecycle events from REST callbacks (acks, rejects, amend results) do not touch strategy state directly. They are pushed onto a per-symbol lock-free queue and drained by the symbol's own domain at the top of each cycle, so strategy state has exactly one writer thread.

### 4.4 Feed Parsing

High-rate frame parsing runs on a dedicated worker domain (`src/engine/concurrency/parse_worker.ml`) rather than on the Lwt scheduler thread that multiplexes all venue sockets. Kraken's WebSocket client diverts execution frames by raw-string prefix before any JSON parsing; orderbook frames are submitted unconditionally. The worker parses and dispatches them sequentially, preserving per-venue order. When the worker's queue fills, frames fall back to inline parsing and are never dropped (Kraken book updates are deltas, so a dropped update would desynchronize the local book). Hyperliquid's l2Book channel requires no offloading: its top-of-book is extracted by a zero-copy string scan into an atomic snapshot, and the full-book JSON parse occurs only on the dashboard cadence.

### 4.5 Order Executor

Strategies enqueue intents into a lock-free MPSC queue (capacity 64k). The executor shards in-flight orders by `duplicate_key` across 64 shards, tracks amendments, and suppresses redundant no-change amendments. After a restart, the supervisor re-synchronizes against open orders fetched from the venue before domains resume.

### 4.6 Error Handling

All exchange I/O funnels through `error_handling.ml`. Callers classify errors (`Connection`, `Timeout`, `RateLimit`, `ServerError`, `ParseError`, `OrderRejected`, `InvalidRequest`, `Unknown`) and retry with exponential backoff (default: 3 attempts, 1000 ms base, 30 s maximum, factor 2). Feed loops self-restart with backoff instead of crashing the domain.

### 4.7 Logging

`logging.ml` provides five levels and per-section colors, with column width autodetected from the terminal or `COLUMNS`. Messages drain through an asynchronous worker; `CRITICAL` flushes synchronously. The log format is `HH:MM:SS.mmm LVL SECTION message`.

### 4.8 Persistence

State resides in two JSON files under the state directory (`data/`, `$DIO_DATA_DIR`, or `/app/data` in Docker), written atomically (temporary file plus rename):

a. `accumulation_state.json` (`base_accumulation_store.ml`, orchestrated by `persistence_orchestrator.ml`): keyed by `{strategy}:{symbol}:{venue}`, with fields `reserved_base`, `accumulated_profit`, `last_fill_oid`, `last_buy_fill_price`, `last_sell_fill_price`, `last_buy_fill_qty`, and `last_sell_fill_qty`. Enabled per trading entry via `base_accumulation` (default `true`).
b. `sell_levels_state.json` (`sell_levels_store.ml`): pending sell levels for entries with `sell_levels: true` (default `false`).

On startup, a legacy flat `data/accumulated_state.json` is migrated into these files and renamed to `accumulated_state.json.migrated.<ts>`. Persistence is used by the Jacobs ladder on all venues, including Kraken. In Docker, `/app/data` shall be mounted.

---

## 5.0 CONFIGURATION

### 5.1 Configuration File

The engine reads `config.json` from the working directory. Table 1 defines the top-level keys.

**Table 1. Top-level configuration keys.**

| Key | Default | Description |
| --- | --- | --- |
| `logging_level` | `INFO` | One of `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL` |
| `logging_sections` | unset | Comma-separated section filters; unset enables all sections |
| `logging_width` | autodetect | Message column width; autodetected from the terminal or `COLUMNS` |
| `cycle_mod` | `10000` | Legacy interval for periodic background work; unused by current strategies |
| `latency_window_seconds` | `5.0` | Rolling window for network latency profiling statistics |
| `latency_spike_threshold_us` | `10.0` | Per-stage ceiling; a window that breaches it emits one INFO line naming the offending stages, their worst spike, and breach count |
| `latency_spike_report` | `internal` | Which latency families emit spike logs: `internal` (per-domain pipeline), `network` (`ws_ping`/`ws_feed`/`rest_request`/`signer`), `both`, or `none` |
| `latency_network_spike_threshold_us` | `20000.0` | Ceiling for the network spike family, in microseconds (20 ms); separate from the 10 us internal-operation target |
| `gc` | see Table 2 | OCaml GC tunables applied before the engine starts |
| `oracle` | see Table 4 | Capital oracle knobs (runtime and tuning CLI) |
| `trading` | required | One entry per instrument to trade; see Section 5.3 |
| `fng_check_threshold` | `1.5` | Price movement (percent) from baseline that re-triggers a Fear & Greed check |
| `theme` | unset | Default dashboard theme identifier; overridable with `dio-dashboard --theme` |

The `trading` schema is strict: an unknown key under `trading` shall cause the engine to exit at startup.

### 5.2 GC Tunables

GC tunables are applied at process start through `Gc.set`. Units for `minor_heap_size` and `major_heap_increment` are OCaml words (8 bytes on 64-bit platforms). Table 2 defines the keys and the values used by the repository configuration.

**Table 2. GC tunables.**

| Key | Default | Repository value |
| --- | --- | --- |
| `minor_heap_size` | `33554432` | `262144` |
| `space_overhead` | `120` | `80` |
| `max_overhead` | `1000000` | `1000000` |
| `window_size` | `10` | `5` |
| `allocation_policy` | `2` | `2` |
| `major_heap_increment` | `100` | `1048576` |

The repository values are tuned for tail latency rather than throughput. A small minor heap (`262144` words = 2 MiB, OCaml's own default) makes minor collections short and frequent instead of one long pause, and the smaller major-heap increment keeps heap-growth slices fine. Combined with the allocation reductions on the trading hot path, this targets a sub-10 us internal-pipeline p99.

The per-window internal spike line's worst-cycle continuation reports per-stage allocation and GC deltas, for example:

```
ob:true ex:0 lev:0 st:false al:293w[ob:12 ex:0 prep:281 strat:0] (GC: minor=1)
```

A regression can therefore be attributed to allocation (per stage) or to a minor/major collection without a profiler attached.

### 5.3 Trading Entries

Each element of the `trading` array configures one symbol on one exchange. The following is a representative entry:

```json
{
  "symbol": "BTC/USDC",
  "exchange": "hyperliquid",
  "qty": "0.01",
  "grid_interval": [1.0, 5.0],
  "strategy": "jacobs_ladder",
  "maker_fee": null
}
```

Table 3 defines the trading-entry keys.

**Table 3. Trading-entry keys.**

| Key | Applies to | Description |
| --- | --- | --- |
| `symbol` | all | Exchange symbol, e.g. `BTC/USD` (Kraken), `BTC` (Hyperliquid perpetual), `BTC/USDC` (Hyperliquid spot), `AAPL` (IBKR, Alpaca) |
| `exchange` | all | `kraken`, `hyperliquid`, `lighter`, `ibkr`, or `alpaca` |
| `qty` | all | Base order size in base currency, encoded as a string (e.g. `"0.01"`) |
| `grid_interval` | jacobs_ladder | `[gi_min, gi_max]`: the hardened bounds (in percent) walked by the oracle's parameter search; the strategy reads only the oracle's resolved interval, never these bounds |
| `min_usd_balance` | MM only | Lower bound on account quote balance; MM pauses buys below this value |
| `max_exposure` | MM only | Upper bound on quote exposure for one symbol; MM pauses buys above this value |
| `strategy` | all | `jacobs_ladder` (alias `Ladder`) or market making (`MM`, alias `market_maker`) |
| `maker_fee`, `taker_fee` | all | Explicit fee overrides (fractions, e.g. `0.0016`); `null` selects the venue default or a live fee lookup |
| `testnet` | HL, Lighter, IBKR, Alpaca | Route to sandbox/paper endpoints; rejected for Kraken |
| `hedge` | Hyperliquid only | Enable the experimental perpetual-short auto-hedge; rejected elsewhere |
| `accumulation_buffer` | all | `[min, max]` retained quote profit buffer required before base accumulation; resolved live from Fear & Greed on crypto venues |
| `data_feed` | Alpaca | `iex` (free) or `sip` (paid, full-market); accepted on any recognized venue, consumed only by Alpaca |
| `sell_mult` | jacobs_ladder | Fraction of each ladder rung's quantity sold per rung fill (`1.0` sells the full rung; smaller values accrue base) |
| `base_accumulation` | all | Persist accumulated base and profit state for this entry (default `true`) |
| `sell_levels` | jacobs_ladder | Persist pending sell levels for this entry (default `false`) |

Venue-specific restrictions are enforced at startup:

a. `hedge` is Hyperliquid-only.
b. `testnet` is accepted for Hyperliquid, Lighter, IBKR, and Alpaca; it is rejected for Kraken.
c. `testnet`, `hedge`, `data_feed`, and `accumulation_buffer` are rejected for unrecognized (custom) exchanges.

### 5.4 Oracle Section

The oracle runs inside the engine as a supervised module. The `oracle` section is optional; every key falls back to the defaults in Table 4. Unknown keys are rejected at startup.

**Table 4. Oracle section keys.**

| Key | Default | Description |
| --- | --- | --- |
| `qty_cap_mult` | `1.5` | Buy-size upper-bound multiplier: the search sizes `buy_qty` within `[qty, qty * qty_cap_mult]` |
| `target_survival` | `0.95` | Fraction of the historical maximum drawdown the runway covers: `runway_pct = max_drawdown_pct * target_survival`; drives sizing only, never activity |
| `min_active_dsurv` | `0.0` | Active gate: a strategy is active if and only if its replayed `d_surv >= min_active_dsurv` (or it has a resting buy to preserve), subject to affordability |
| `refresh_seconds` | `300.0` | Background fallback poll for history and balances; decisions remain event-driven, because fills and cancels re-resolve immediately |
| `assets` | unset | Per-symbol overrides keyed by symbol, each accepting `{ target_survival, min_active_dsurv, qty_cap_mult }` |

---

## 6.0 ENVIRONMENT VARIABLES

Credentials and one-off knobs are supplied through the environment. The engine loads `.env` if present (dotenv-style, `KEY=value` per line). Table 5 defines the recognized variables.

**Table 5. Environment variables.**

| Variable | Used by | Notes |
| --- | --- | --- |
| `KRAKEN_API_KEY` | Kraken | |
| `KRAKEN_API_SECRET` | Kraken | |
| `HYPERLIQUID_WALLET_ADDRESS` | Hyperliquid | Account address used for balance and fee queries |
| `HYPERLIQUID_PRIVATE_KEY` | Hyperliquid | Private key for the EIP-712 order signer |
| `IBKR_GATEWAY_HOST` | IBKR | Default `127.0.0.1` |
| `IBKR_GATEWAY_PORT` | IBKR | Default `4002`; live mode forces `4001` when unset |
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
| `CMC_API_KEY` | Fear and Greed | CoinMarketCap API key; a missing key falls back to a neutral value |
| `DISCORD_WEBHOOK_URL` | Discord notifier | Fill notifications; unset disables Discord |
| `DIO_BACKTRACE` | Engine | When set, pretty-prints OCaml backtraces on crashes |
| `DIO_DATA_DIR` | Persistence | State-directory override; defaults to `/app/data` when `/app` exists, otherwise `data` |
| `DIO_WATCHDOG_OFF` | Engine | When set, disables the main-loop watchdog |
| `DIO_CANARY` | Engine | `0`/`false`/`off`/`no` disables the stop-the-world canary domain (which busy-spins one core while enabled) |
| `DIO_CANARY_THRESHOLD_US` | Engine | Canary spike threshold in microseconds (default `10`) |
| `DIO_CANARY_WINDOW_S` | Engine | Canary report window in seconds (default `5`) |
| `COLUMNS` | Engine | Fallback log width when stdout is not a TTY (default width `200`) |
| `DIO_MOTION` | Dashboard | `off`/`0`/`false`/`no` disables animations |
| `DIO_FPS` | Dashboard | Caps the animated frame rate (default `30.0`) |
| `DIO_DAMAGE` | Dashboard | `off`/`0`/`false`/`no` forces full-frame redraws instead of incremental damage rendering |
| `HOME` | Dashboard | Used for the persisted `~/.dio_theme` theme path |

---

## 7.0 EXCHANGE ADAPTERS

### 7.1 Kraken

Kraken uses REST and WebSocket interfaces. Order book and authenticated feeds arrive over websockets; balance is obtained from the authenticated feed. Fees are looked up live from volume tiers (`TradeVolume`). The capital-oracle adapter falls back to `0.0016` / `0.0026` maker/taker, and a failed live fee fetch at startup is fatal. Prices are rounded to the nearest venue tick, and the strategy layer floors order quantities to the venue lot size. No `testnet` mode is available.

### 7.2 Hyperliquid

Hyperliquid supports spot and perpetual trading with an EIP-712 signer. Bare symbols (`BTC`) resolve to the perpetual; `BTC/USDC` maps through the spot universe. Testnet is selected with `"testnet": true`. `min_notional` is enforced at `10.0` USDC for spot symbols containing `/` (the oracle sizes against spot balances).

The engine signs with the wallet key from `HYPERLIQUID_PRIVATE_KEY`. The agent contract, `Agent(string source, bytes32 connectionId)`, is constructed internally; there is no separate agent-address environment variable.

### 7.3 Lighter

Lighter is a perpetual DEX. It requires the Lighter signer shared library (`lighter-signer-darwin-arm64.dylib` on macOS, `lighter-signer-linux-amd64.so` on Linux), a funded account, and usually a relay proxy. `LIGHTER_PROXY_URL` may be a comma-separated pool of Cloudflare relay endpoints; the client round-robins and retries across them.

Lighter orders are time-limited (approximately 28-day GTT). A renewal daemon cancel-and-replaces orders to approximate good-til-cancelled behavior; order identifiers shall not be relied upon to survive a restart boundary.

### 7.4 Interactive Brokers

Interactive Brokers connectivity is provided through an IB Gateway (for example, `gnzsnz/ib-gateway-docker`) over TCP. `testnet` forces paper mode on port `4002`; live mode forces port `4001` unless `IBKR_GATEWAY_PORT` is set. The grid strategy submits limit orders only, and the engine floors order quantities to whole shares. The account identifier is auto-detected unless `IBKR_ACCOUNT_ID` is set.

### 7.5 Alpaca

Alpaca provides US equities in paper or live mode. `data_feed` selects `iex` (free) or `sip` (paid, full-market). The engine respects extended trading hours (pre-market 4:00 AM to 9:30 AM, after-hours 4:00 PM to 8:00 PM, overnight 8:00 PM to 4:00 AM ET). It attaches the extended-hours flag to limit orders placed in extended sessions and forces `day` TIF only for fractional equity orders; otherwise the requested TIF (default `gtc`) passes through. `min_notional` is `1.0`. Alpaca pairs are 1:1 (no accumulation up-sizing), and fees default to zero.

---

## 8.0 STRATEGIES

### 8.1 Grid (`jacobs_ladder`)

The ladder strategy is selected with `"strategy": "jacobs_ladder"` or `"Ladder"`. It is a pure executor: it buys price drops and sells the purchased base to offset volatility drag. The oracle's decision record is `{active, grid_interval, buy_qty, sell_qty}`. The grid consumes `active`, `grid_interval`, and `buy_qty`, and sizes sells locally as described below. There is no configuration fallback path; before an oracle decision exists for an asset, the strategy places nothing.

a. **Buy side.** Exactly one resting buy is placed below the current price and trails upward. It is amended down when it would intrude into the sell zone.
b. **Sell side.** Layered sells are placed above the current price, one per filled buy (and on a buy placement). Each is fill-anchored at `buy_fill_price * (1 + grid_interval / 100)`. On non-Alpaca venues the anchor re-bases to the live bid once the bid has drifted more than one grid step from the last fill. Sells are not proactively repriced or cancelled once placed, although a failed amendment can cancel a stale order. Sells run even while the strategy is inactive because they require inventory, not quote.
c. **Balance model.** Sell size is the last buy fill quantity (falling back to the venue lot of the grid quantity), capped by sellable base. On non-accumulation venues that is `base - reserved_base - base locked in resting sells`; on accumulation venues it is `ledger balance - reserved_base - unnetted hold` (resting-sell base is deliberately not subtracted).

Accrual resides in the persistence layer. Profitable sell fills reserve base through `Base_accumulation_store`, which survives engine restarts. Base accumulation is pre-funded out of realized quote earnings: base is reserved only when accumulated net profit covers the acquisition cost of the withheld base plus the Fear & Greed-interpolated `accumulation_buffer`. Accumulated profit is debited by that acquisition cost upon reservation, preventing quote-balance bleed.

### 8.2 Market Maker (MM)

`market_maker.ml` places one buy and one sell around the top of book:

a. **Buy price.** Best ask minus the fee backoff, `ask * (1 - (2*fee + 0.0001))`, clamped so that it never crosses the best bid. When the maker fee is `0.0`, the buy rests exactly at the best bid.
b. **Sell price.** The best ask, rounded to the venue tick.
c. **Profitability guard.** A spread that cannot cover round-trip fees is refused: the rounded spread (sell minus buy) shall be at least `ask * (2 * fee + 0.0001)`.

The per-symbol `min_usd_balance` and `max_exposure` bounds constrain the account. Crossing either bound pauses new buys on that symbol and places an emergency sell of free inventory.

### 8.3 Auto-Hedge (Hyperliquid, experimental)

`auto_hedger.ml` maintains one perpetual short per grid cycle. When the grid buys spot, the strategy opens a short if none is open; when the grid sells, it closes the hedge. Hedges use GTC limit orders at the perpetual top of book, falling back to a market order when no perpetual top of book is available. The strategy is enabled with `"hedge": true` on a Hyperliquid entry. Refer to the caution in Section 11.0.

---

## 9.0 CAPITAL ORACLE

The oracle is a capital-survival sizing engine. For each asset it answers the question: given the asset's all-time price history and the quote capital available on its venue, which order size and grid interval keep the ladder alive across the runway that the asset has actually walked?

### 9.1 Decision Pipeline

The pipeline is implemented in one code path:

a. **History.** The asset's all-time merged daily series consists of the venue adapter's bars (`Exchange_intf.Oracle.Registry`), disk-cached and delta-fetched, extended backwards with Yahoo deep history for the same underlying. Venue bars win on overlap. Nothing is synthesized; there is no gap tolerance and no minimum length.
b. **References.** `max_drawdown_pct` is the single worst peak-to-trough percentage decline in the whole series (running peak of closes to the deepest subsequent low; recovery is irrelevant). ATH and ATL are taken from intrabar extremes.
c. **Runway mathematics.** `realized_dd = (ath - current) / ath`; `runway_pct = max_drawdown_pct * target_survival`; and `floor_price = ath * (1 - runway_pct)`. Three regimes are evaluated in order: *Normal* (`current > floor_price`, funding the remaining drop); *Unprecedented lows* (at the deepest drawdown and at or below ATL, maximum conservatism); and *Floor extension* (at or below the floor but not at the deepest drawdown, funding extending down to ATL). `realized_dd` is clamped to `[0,1]`, and `max_drawdown_pct` is capped just below `1.0`. Aggressiveness is `realized_dd / max_drawdown_pct` (clamped to `[0,1]`), which biases parameter selection toward tighter grids and larger sizes deeper in the runway; it never overrides survival requirements or bounds.
d. **Survival replay.** A candidate `(grid_interval, buy_qty)` walks the funded depth geometrically, paying each buy plus venue fees. `d_surv` is the fraction of that depth survived before the quote runs out (`>= 1.0` means fully funded). The exhaustion price, that is, the deepest rung fillable with available capital, is reported alongside it: the venue-simulated shared-capital bottom rung (`P_funded`) for active strategies, and the single-asset replay's exhaustion point for inactive ones (inactive decisions report `d_surv = 0.0`).
e. **Parameter search.** `buy_qty` is searched within `[qty, qty * qty_cap_mult]`, and `grid_interval` within `[gi_min, gi_max]` (the strategy entry's bounds). Branches are evaluated in order: the deepest-drawdown *Unprecedented lows* case short-circuits to the conservative corner (`qty` / `gi_max`); *Surplus* (the aggressive corner is fully funded with quote left over) resolves to `qty_max` / `gi_min`; *Reachable* (largest size at tightest spacing that is still fully funded, aggressiveness-biased) is selected when available; otherwise *Unreachable* resolves to the conservative corner. The funded-depth test is the fully-funded `d_surv >= 1.0`, not `target_survival`; `target_survival` sets only the funded floor via `runway_pct`.
f. **Decision record.** The record is exactly `{active, grid_interval, buy_qty, sell_qty}`, as raw floats, generated by exactly one code path. Values are emitted for inactive strategies as well, for visibility and immediate reactivation. All normalization (tick and lot rounding, minimum notional) occurs at the exchange layer. A strategy is active when `d_surv >= min_active_dsurv` (or a resting buy must be preserved) and the next buy is affordable. Sells always run because they require inventory, not quote.

### 9.2 Pooling, Priority, and Cascades

Pools are per venue: one quote pool is shared by that venue's strategies and never crosses venues (two strategies may share one symbol on one venue). Allocation walks strategies in configuration presentation order, where first is highest priority. Each strategy sizes against the entire remaining availability; its next buy ties up `buy_qty * current`; and a strategy whose next buy does not fit is skipped while remaining capacity passes down the list. When a higher-priority need cannot fit, the cancellation cascade cancels lower-priority resting buys, many lesser orders if necessary, to satisfy one greater, until the need fits. If no combination fits, resolution proceeds to the next-highest priority. Every cancelled strategy re-evaluates on that event and resumes if and only if quote covers its buy.

### 9.3 Runtime Behavior

Inside the engine the oracle is a supervised module registered as `oracle`, heartbeated on every published pass plus a liveness ticker, and auto-restarted by the health monitor. Decision computation is pure and in-memory (<10 ms p99): histories reside in a cache refreshed on the background cadence; balances come from the websocket-fed live store or the last snapshot adjusted in process by fill and cancel deltas; and live prices read the in-process top of book. Event bursts coalesce into a single re-resolve. Only domains whose decision changed are woken per pass.

### 9.4 Command-Line Interface

`dio-oracle` runs the exact decision pipeline offline against configured assets and prints the decision surface (the four contract values plus diagnostics) without touching live balances. It supports the Kraken, Hyperliquid, and Alpaca venue adapters; Lighter and IBKR have no oracle adapter.

```sh
dune exec dio-oracle                                # every trading entry, table output
dune exec dio-oracle -- --symbol BTC/USDC           # one asset
dune exec dio-oracle -- --quote 50000 --base 0.5     # synthetic pool sizing
dune exec dio-oracle -- --cache-only                # never touch the network
dune exec dio-oracle -- --json                      # machine-readable
```

The oracle configuration section of `config.json` drives both the runtime and this CLI. Strategy entries carry the hardened bounds that the search walks.

---

## 10.0 BUILD AND EXECUTION

### 10.1 Prerequisites

a. OCaml 5.2 (any distribution: opam, Nix, or Homebrew).
b. `opam` and `dune`.
c. Linux or macOS. WSL2 is supported.

### 10.2 Build

```sh
opam install . --deps-only
dune build
```

### 10.3 Execution

```sh
./_build/default/bin/main.exe
```

On first start the engine loads `config.json` from the working directory, connects the exchanges listed in `trading`, and allows each asset to settle into its own trading domain. Logs are written to stderr asynchronously in the format `HH:MM:SS.mmm LVL SECTION message`.

### 10.4 Executables

Table 6 defines the executables.

**Table 6. Executables.**

| Executable | Source | Function |
| --- | --- | --- |
| `dio` | `bin/main.ml` | The engine itself |
| `dio-dashboard` | `bin/dashboard.ml` | TUI dashboard; connects to the running engine |
| `dio-oracle` | `bin/oracle.ml` | Configuration-tuning CLI; prints the decision surface offline (see Section 9.4) |

```sh
dune exec dio-dashboard
dune exec dio-oracle -- --symbol BTC/USDC --quote 10000
```

---

## 11.0 CAUTIONS AND LIMITATIONS

a. **Experimental auto-hedge.** The auto-hedge strategy (`auto_hedger.ml`) opens Hyperliquid perpetual shorts against spot grid inventory. It is experimental. Review the module and validate it on a testnet before committing capital.
b. **Restart boundaries.** Lighter order identifiers do not survive a restart boundary; the renewal daemon cancel-and-replaces orders to approximate good-til-cancelled behavior.
c. **Startup strictness.** The configuration schema is strict. Unknown keys under `trading`, unknown keys in the oracle section, and venue-inapplicable keys shall cause the engine to exit at startup.
d. **Fee lookup.** A failed live Kraken fee fetch at startup is fatal.
e. **Canary cost.** The stop-the-world canary domain busy-spins one core while enabled. Disable it with `DIO_CANARY` when core utilization is constrained.
