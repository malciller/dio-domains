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

On first start the engine loads `config.json` from the working directory, connects the exchanges listed in `trading`, and lets each asset settle into its own trading domain. Logs go to stdout asynchronously (`HH:MM:SS.mmm LVL SECTION message`).

### Binaries

| Binary | Source | What it does |
| --- | --- | --- |
| `dio` | `bin/main.ml` | The engine itself |
| `dio-dashboard` | `bin/dashboard.ml` | TUI dashboard, connects to the running engine |
| `dio-oracle` | `bin/oracle.ml` | One-shot capital sizing CLI (see Capital Oracle) |

```sh
dune exec dio-dashboard
dune exec dio-oracle -- BTC/USD --exchange kraken
```

---

## Configuration

The engine reads `config.json`. Top-level keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `logging_level` | `INFO` | One of `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL` |
| `logging_sections` | unset | Comma-separated section filters; unset means all sections |
| `logging_width` | autodetect | Message column width; autodetected from terminal or `COLUMNS` |
| `fng_check_threshold` | `1.5` | Percent price move that triggers a Fear-and-Greed re-evaluation of an active grid |
| `cycle_mod` | `10000` | Legacy interval for periodic background work; unused by current strategies |
| `latency_window_seconds` | `5.0` | Rolling window for network latency profiling stats |
| `gc` | see table | OCaml GC tunables applied before the engine starts |
| `classes` | unset | Risk classes used by the oracle |
| `oracle` | see table | Capital oracle knobs (runtime, not the CLI) |
| `trading` | required | One entry per instrument to trade |

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
  "symbol": "BTC/USD",
  "exchange": "kraken",
  "qty": 0.001,
  "grid_interval": [0.004, 0.01],
  "sell_mult": 0.6,
  "strategy": "Grid",
  "maker_fee": null,
  "taker_fee": null,
  "asset_class": "Crypto"
}
```

| Key | Applies to | Meaning |
| --- | --- | --- |
| `symbol` | all | Exchange symbol, e.g. `BTC/USD` (Kraken), `BTC` (Hyperliquid spot), `AAPL` (IBKR, Alpaca) |
| `exchange` | all | `kraken`, `hyperliquid`, `lighter`, `ibkr`, or `alpaca` |
| `qty` | all | Base order size in base currency |
| `grid_interval` | Grid only | Price interval in quote currency (fraction of price); min and max bound the grid |
| `sell_mult` | Grid | Multiplier applied to profit to decide how much base to keep vs sell (accumulation) |
| `min_usd_balance` | MM only | Lower bound on account quote balance; MM halts below this |
| `max_exposure` | MM only | Upper bound on quote exposure for one symbol; MM halts above this |
| `strategy` | all | `Grid` or `MM` |
| `maker_fee`, `taker_fee` | all | Explicit fee overrides (fractions, e.g. `0.0016`); `null` means use venue default or live fee lookup |
| `testnet` | HL, Lighter, IBKR, Alpaca | Route to sandbox/paper endpoints. Rejected for Kraken |
| `hedge` | Hyperliquid only | Enable the experimental perp short auto-hedge. Rejected elsewhere |
| `accumulation_buffer` | HL, Lighter, IBKR, Alpaca | `[min, max]` bounds on base reserved for accumulation. Rejected for Kraken |
| `data_feed` | Alpaca | `iex` (free, delayed) or `sip` (paid, real-time) |
| `asset_class` | all | Must name a key in `classes` when the oracle is in use |

Venue-specific restrictions are enforced at startup:

- `testnet`, `hedge`, `accumulation_buffer`, and `data_feed` are rejected for Kraken.
- `hedge` is Hyperliquid-only.
- `grid_interval` is only meaningful for the Grid strategy.

### Classes

The oracle needs to know which instruments belong to the same risk class so it can blend survival statistics across them.

Legacy form (pooled history only):

```json
"classes": { "Crypto": ["BTC", "ETH"] }
```

Extended form (kappa = cross-asset prior weight):

```json
"classes": { "Crypto": { "members": ["BTC", "ETH"], "kappa": 1.0 } }
```

### Oracle (runtime)

The runtime oracle runs inside the engine as a supervised module. Keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `target_survival` | `0.99` | Statistical survival target (probability the grid survives the drawn-down regime) |
| `fng_weight` | `0.5` | Weight of the Fear-and-Greed component in the blended sizing signal |
| `range_weight` | `0.25` | Weight of the price-range component in the blended sizing signal |
| `min_active_dsurv` | `0.0` | Minimum drawdown-survival for an asset to stay active |
| `qty_cap_mult` | `0.0` | Cap order size as a multiple of what the oracle would size; `0.0` disables |
| `no_deep_history` | `false` | Skip the Yahoo deep-history extension |
| `weight_by_sessions` | `true` | Weight blended statistics by number of sessions per asset |
| `refresh_seconds` | `300.0` | Full history-refresh cadence and the pass loop's safety-net deadline (decisions are event-driven: fills/cancels re-size immediately) |
| `poll_seconds` | `30.0` | Background refresher's authoritative balance-reconciliation cadence (the decision path never waits on it) |
| `startup_wait_seconds` | `60.0` | Delay before the first analysis pass (lets feeds warm up) |
| `horizons` | venue default | `[30; 90; 180; 365]` for crypto, `[21; 63; 126; 252]` for equities |
| `max_capital` | unset | Cap on total capital the oracle will deploy |
| `assets` | unset | Per-asset overrides for the keys above |

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

### Grid

The default strategy (`suicide_grid`). The grid places a resting buy ladder below the market and a sell ladder above. Order size and grid interval come from the oracle decision when one is available, or from `config.json` as a fallback.

Accumulation: `sell_mult` below `1.0` reserves a fraction of realized base. Accumulation venues (Hyperliquid, Lighter, IBKR, Alpaca) keep the resting sell up until the reserved floor is cleared. Non-accumulation venues (Kraken, Alpaca when sell_mult is exactly `1.0`) up-size the sell to the full notional instead.

Persistence: reserved base, accumulated profit, last fill IDs and prices, and sell levels are written atomically to `data/accumulated_state.json` so the grid can survive an engine restart.

The grid halts buying when account capital falls below the config threshold (oracle-driven sizing) and resumes when the oracle or Fear-and-Greed signal recovers.

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

`state_persistence.ml` writes `data/accumulated_state.json` atomically (temp file + rename, guarded by a mutex). Fields: `reserved_base`, `accumulated_profit`, `last_fill_oid`, `last_buy_fill_price`, `last_sell_fill_price`, `last_buy_fill_qty`, `last_sell_fill_qty`, `sell_levels`. Used by the suicide grid on Hyperliquid, Lighter, IBKR, and Alpaca. In Docker, mount `/app/data`.

---

## Capital Oracle

The oracle is a capital-survival sizing engine. For each asset it answers: "given this asset's real price history and the quote capital available on its exchange account, what order size and grid interval keep the grid ladder alive through the worst drawdowns this asset has actually experienced?"

Mechanics (see `test/engine/oracle/oracle.md` for the full design):

- Maximum Fractional Drawdown (MFD) over a horizon `h`: how deep the price fell from the session-start close. `S_h(d) = P(MFD_h <= d)` is the survival function; `d` such that `S_h(d) = target_survival` is the governing drawdown.
- The empirical distributions come from the asset's own history, blended with a risk-class prior (pooled histories of "similar" assets) using a kappa-weighted, volatility-normalized blend. At runtime the blended survival signal is itself blended with the Fear-and-Greed reading (`fng_weight`) and the price-range position (`range_weight`).
- Sizing anchors to the all-time high. The ladder is sized so the grid survives the replayed history at the minimum order size, grows quantity behind 100% replay survival to deploy the pool, and falls back to stretch sizing when 100% survival is unreachable.
- History comes from the venue adapter (`Exchange_intf.Oracle.Registry`), is disk-cached, and is extended with Yahoo daily history for crypto and equities unless `no_deep_history` is set. Missing data is never fabricated: gaps beyond tolerance invalidate the analysis, and the asset goes inactive rather than getting a garbage decision.

### Runtime behavior

Inside the engine the oracle is a supervised module (`supervisor.ml` starts it like any other connection, registered as `oracle`). It heartbeats on every published pass plus a 10-second liveness ticker and auto-restarts if its loop dies. Each asset's analysis is bounded at 60 seconds and falls back to last-known-good. Decisions publish as: `active`, `qty`, `grid_interval` (`gi`), `d_surv`, `reason`, `reclaim_capital`, and the blend components. Capital is allocated in two phases across the venue account pool, with priority reclamation of capital from shrinking assets.

The decision path is event-driven and network-free: a fill or cancel on any trading domain wakes the oracle immediately (one Atomic increment plus an Lwt-condition broadcast) and enqueues its pool delta (`notify_fill` / `notify_order_cancel`), which the pass applies to the account pool in process — decisions re-size in microseconds and never wait on a venue balance round-trip; the background refresher reconciles the authoritative balance on its own cadence. Only the affected domains are woken per pass (changed-only, per-symbol). On publish, domains adopt the new qty/gi via the lock-free snapshot.

Dynamic re-evaluation: when the price moves more than `fng_check_threshold` percent from the grid baseline, the oracle re-checks the Fear-and-Greed signal and can shrink or grow the grid before the next scheduled pass. Equities are pure oracle; Fear-and-Greed applies only to crypto.

### CLI (`dio-oracle`)

`bin/oracle.ml` runs the same analysis pipeline once and prints a report:

```sh
dune exec dio-oracle -- BTC/USD --exchange kraken
dune exec dio-oracle -- AAPL --exchange alpaca --capital 5000 --target-survival 0.99
dune exec dio-oracle -- --portfolio --exchange kraken --capital 50000   # every class member
```

All flags: `--exchange`, `--capital`, `--total-capital`, `--portfolio`, `--topology`, `--allocation`, `--split`, `--transfer`, `--positions-file`, `--save-positions`, `--qty`, `--gi`, `--fee`, `--sell-mult`, `--accumulation-buffer`, `--min-notional`, `--data-feed`, `--start-price`, `--price-increment`, `--qty-increment`, `--qty-min`, `--start`, `--end`, `--horizons`, `--thresholds`, `--percentiles`, `--gap-tolerance`, `--vol-window`, `--class`, `--members`, `--kappa`, `--target-survival`, `--fng`, `--min-active-dsurv`, `--qty-cap-mult`, `--no-deep-history`, `--no-weight-by-sessions`, `--max-capital`, `--json`.

Notes:

- `--fng-weight` and `--range-weight` are accepted for config compatibility but do not affect CLI sizing; the CLI is survival-driven. The runtime oracle applies those weights.
- `--from-csv` / `--from-json` load a single asset series from a file instead of the venue adapters, for offline analysis (see `test/engine/oracle/`). They do not combine with `--portfolio` or any topology/allocation option.
- `--portfolio` with `--topology <file>` or `--allocation <split>` sizes every asset in a class; `--transfer "SESSION:FROM->TO=AMOUNT"` (repeatable) builds a manual portfolio transfer.
- Venue adapters are registered at binary startup (force-reference in `bin/oracle.ml`), so the CLI supports any venue that implements `Exchange_intf.Oracle.S`.

---

## Dashboard

`dio-dashboard` is a full-screen TUI that talks to the running engine over a Unix domain socket.

### Socket and protocol

The engine listens on `/var/run/dio/dashboard.sock`, falling back to `/tmp/dio/dashboard.sock`. The client tries the fixed paths first, then scans `/tmp` for `dio-*.sock` newest-first, unlinks dead sockets, and honors `--socket` to override. Use `dune exec dio-dashboard -- --socket /path/to/custom.sock` for non-default setups.

Wire protocol: 4-byte big-endian length-prefixed JSON frames. Client commands: `S` (snapshot), `W` (watch), `P` (ping), `Q` (close). In watch mode the server pushes a full state frame every 500 ms (about 2 updates per second). At most 5 clients may watch; clients that stop pinging are pruned after 8 seconds.

### Layout

- **KPI cards**: Portfolio (net worth + cash), System Engine (active/idle strategies, uptime, fill count, fill rate), Latency (oracle publish p50/p99; green under 5 s, yellow under 30 s, red at or above 30 s), Memory/GC (heap MB and live ratio).
- **Live ticker**: every strategy with a mid price, grouped by base asset, with spread in basis points color-coded (<5 green, <20 yellow, <50 orange, >=50 red).
- **Holdings table**: per symbol: strategy, state glyphs (`▶` running, `⏸` halted, `⏹` idle, `$` holding), prices, spreads, buy/sell levels with Δ and gauge bars, accumulated base/quote, and row flash when the market is near the buy or sell level.
- **Recent fills**: a scrolling feed of the last 10 fills (the engine broadcasts the last 50; the UI ring keeps 10).
- **Detail view** (Enter on a row): summary card with the oracle line (`ACTIVE/INACTIVE qty gi D_surv% reason`), an L2 orderbook sidebar (own orders in cyan for buys and magenta for sells, MID banner, recent Alpaca trades), and a 15-minute braille price chart with a LIVE NOW badge and pins at own order levels.
- **Footer**: uptime, Fear-and-Greed reading colored green/yellow/red (>=60 / 40-60 / <40), and per-stream connectivity dots (green when a venue reports a valid top of book).

Keys: `↑`/`↓` or `k`/`j` move, `Enter` opens the detail view, `←`/`→` or `h`/`l` switch latency pages (CORE: oracle, orderbook, strategy, execution, cycle; NETWORK: ws_ping, ws_feed, rest_request, signer, each with p50/p99/p999 and an EMA p99 sparkline), `+`/`-` zoom the chart (clamped), `Esc`/`b`/`Backspace` go back, `q`/`Q` quits.

---

## Discord Notifications

When `DISCORD_WEBHOOK_URL` is set, the engine posts fill notifications through a rate-limited client (token bucket, 5 tokens refilled every 400 ms, 2.5 messages/second ceiling) batching up to 10 fills per embed.

---

## Deployment

### Docker

A multi-stage `Dockerfile` builds on `ocaml/opam:ubuntu-22.04-ocaml-5.2` and runs on `ubuntu:22.04`. It compiles `libsecp256k1` v0.7.1 with schnorrsig + recovery for the Hyperliquid signer, links jemalloc, runs as the non-root `dio` user, exposes port 8080, and `CMD ["dio"]`.

```sh
docker build -t dio .
docker run --rm -v "$PWD/.env:/app/.env:ro" \
  -v "$PWD/config.json:/app/config.json:ro" \
  -v "$PWD/data:/app/data" \
  -v dio-sock:/var/run/dio \
  --env-file .env dio
```

The dashboard runs as a separate container sharing the `dio-sock` volume:

```sh
docker run --rm -it -v dio-sock:/var/run/dio dio dio-dashboard
```

### deploy.sh

`deploy.sh` pushes the repo to a remote node, builds the amd64 image with `docker buildx`, and runs it with host networking, a read-only root filesystem, `tmpfs` for `/tmp`, the `dio-sock` and data volumes, and `.env` / `config.json` bind-mounted read-only.

### Lighter relay proxy

`proxy/cloudflare` is a Cloudflare Worker with a Durable Object that relays Lighter signed requests. Deploy with `wrangler` per its `wrangler.toml`; `LIGHTER_PROXY_URL` accepts a comma-separated pool of worker URLs so the client can fail over.

---

## Development

```sh
dune build            # build everything
dune runtest          # unit tests (engine, strategies, oracle)
dune fmt              # format (ocamlformat)
dune build @doc       # API docs
```

CI (`.github/workflows/ci.yml`) builds libsecp256k1, then runs `dune build @all` and `dune runtest` on OCaml 5.2.0 / Ubuntu 22.04.

Where things live:

| Path | Contents |
| --- | --- |
| `bin/main.ml` | Engine entry point, graceful shutdown, force-references all exchange modules |
| `bin/dashboard.ml` | Dashboard TUI entry point (thin wrapper over `src/dashboard_ui/`) |
| `bin/oracle.ml` | Oracle CLI entry point, module functors, force-references oracle adapters |
| `src/engine/config.ml` | `config.json` schema and validation |
| `src/engine/domain_spawner.ml` | Per-asset domains, strategy lifecycle, Fear-and-Greed trigger |
| `src/engine/supervisor/` | Connection registry, health monitor, circuit breaker, feed orchestration |
| `src/engine/concurrency/` | Ring buffers, event buses, exchange wakeup generations, feed parse worker, fill event bus, Lwt helpers |
| `src/engine/oracle/` | Oracle runtime, math, venues, tasks, loader |
| `src/engine/strategies/` | `suicide_grid`, `market_maker`, `auto_hedger`, grid core |
| `src/engine/latency_profiling/` | Rolling p50/p99/p999 network latency stats |
| `src/engine/error_handling/` | Error classification and retry/backoff |
| `src/engine/logging/` | Async logger |
| `src/engine/persistence/` | Atomic state persistence |
| `src/engine/dashboard/` | UDS dashboard server and state broadcast |
| `src/dashboard_ui/` | Notty TUI: ticker, holdings, KPI cards, latency pages, fills feed |
| `src/external/exchange_intf.ml` | The exchange integration interface |
| `src/external/kraken/`, `hyperliquid.xyz/`, `lighter.xyz/`, `interactivebrokers/`, `alpaca/` | Venue implementations |
| `src/external/yahoo/`, `coinmarketcap/`, `discord/` | Shared clients (deep history, Fear-and-Greed, notifications) |
| `test/` | Unit tests, oracle fixtures and design doc |
| `proxy/cloudflare/` | Lighter relay worker |

See `src/external/README.md` for how to add a new exchange.
