[![OCaml](https://img.shields.io/badge/Language-OCaml-blue.svg)](https://ocaml.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance OCaml 5.2 trading engine for Kraken and Hyperliquid
featuring domain-based parallel strategy execution. Each trading asset
runs in its own isolated domain with lock-free communication,
tick-driven event architecture, and real-time latency profiling.
Built for high-frequency trading with WebSocket data feeds and
asynchronous order execution.

> [!NOTE]
> **Full technical write-up**: [Diogrid v2.0.0](https://diophantsolutions.com/diogrid-wp)

> [!WARNING]
> **Auto-hedge for Hyperliquid is EXPERIMENTAL.** Testing is ongoing; features may be incomplete or unstable.

---

## Requirements

- OCaml 5.2.0 (opam)
- Kraken API key/secret and/or Hyperliquid wallet/key
- macOS / Linux / WSL

---

## Quick Start

### Install
```bash
git clone https://github.com/malciller/dio-domains.git
cd dio-domains
opam install . --deps-only
dune build
```

### Configure
Create `.env`:
```bash
KRAKEN_API_KEY=your_kraken_api_key
KRAKEN_API_SECRET=your_kraken_api_secret
HYPERLIQUID_WALLET_ADDRESS=your_hyperliquid_wallet_address  # public wallet address
HYPERLIQUID_AGENT_ADDRESS=your_hyperliquid_agent_address    # For L1 auth
HYPERLIQUID_PRIVATE_KEY=your_hyperliquid_private_key        # For L1 signature generation
CMC_API_KEY=your_cmc_api_key                               # Fear & Greed index (optional fallback to default)
```

Edit `config.json` (example):
```json
{
  "logging_level": "info",            // Log verbosity: debug, info, warn, error, critical
  "logging_sections": "",             // Filter logs by section (optional, comma-separated)
  "cycle_mod": 10000,                 // Cycle interval for periodic checks and logs
  "fng_check_threshold": 1.5,         // Minimum price change (%) to trigger a Fear & Greed check
  "gc": {                             // OCaml 5 Tuning for high-throughput websockets
    "minor_heap_size": 8388608,       // Words (64MB) to absorb JSON parsing bursts
    "space_overhead": 80,             // Moderate major heap spacing
    "max_overhead": 150,              // Safe compaction threshold
    "window_size": 10,                // Smooths major GC pacing
    "allocation_policy": 2,           // Best-fit policy (OCaml 5 default)
    "major_heap_increment": 100       // Double heap size on growth to prevent OS thrashing
  },
  "trading": [
    {
      "symbol": "BTC/USD",            // Pair to trade
      "exchange": "kraken",           // Exchange name: "kraken", "hyperliquid"
      "qty": "0.0002",                // Base asset quantity per order
      "grid_interval": [0.25, 1.25],  // Min/Max grid spacing (%); resolved dynamically from Fear & Greed
      "sell_mult": "0.999",           // Sell amount multiplier (qty * sell_mult = sell order size)
      "strategy": "Grid"              // Strategy name: "Grid"
    },
    {
      "symbol": "USDG/USD",           // Pair to trade
      "exchange": "kraken",           // Exchange name: "kraken", "hyperliquid"
      "qty": "100.0",                 // Trade size per market making quote
      "min_usd_balance": "500.0",     // Minimum USD balance required to run this strategy
      "max_exposure": "500.0",        // Maximum asset balance allowed before pausing strategy
      "strategy": "MM"                // Strategy name: "MM"
    },
    {
      "symbol": "BTC/USDC",            // Pair to trade
      "exchange": "hyperliquid",      // Exchange name: "kraken", "hyperliquid"
      "qty": "0.001",                 // Base asset quantity per order
      "grid_interval": [0.1, 0.5],    // Min/Max grid spacing (%)
      "accumulation_buffer": [0.75, 3.0], // Min/Max net quote profit buffer (USDC) for sell_mult accumulation; resolved dynamically from Fear & Greed
      "sell_mult": "1.0",             // Sell amount multiplier
      "strategy": "Grid",             // Strategy name: "Grid"
      "testnet": true                 // Use testnet
    },
    {
      "symbol": "HYPE/USDC",          // Pair to trade (spot)
      "exchange": "hyperliquid",      // Exchange name (must be "hyperliquid")
      "qty": "0.35",                  // Base asset quantity per order
      "grid_interval": [0.25, 0.5],   // Min/Max grid spacing (%)
      "sell_mult": "0.999",           // Sell amount multiplier (triggers discrete accumulation on Hyperliquid)
      "accumulation_buffer": [0.5, 5.0], // Min/Max net quote profit buffer (USDC) for sell_mult accumulation; resolved dynamically from Fear & Greed
      "strategy": "Grid",             // Strategy name: "Grid"
      "testnet": false,               // Use testnet
      "hedge": true                   // Enable auto-hedge (Hyperliquid only)
    }
  ]
}
```

### Run
```bash
# Start the core trading engine
./_build/default/bin/main.exe

# In a separate terminal, run the state monitoring dashboard
./_build/default/bin/dashboard.exe
```

---

## Strategies

**Grid**: Maintains buy/sell ladders around price with configurable spacing
and size. Acts as a market maker with optional DCA accumulation via
`sell_mult`. On Kraken, sells use `qty * sell_mult` directly.

Hyperliquid enforces discrete order sizes via `szDecimals` (e.g. HYPE
uses 2 decimal places, so the lot increment is 0.01). When
`sell_mult < 1.0`, the desired sell quantity often falls between valid
lot boundaries and must be floored, creating rounding loss that exceeds
the intended skim. For example, `0.35 * 0.999 = 0.34965` floors to
`0.34`, losing 0.01 HYPE per cycle (~2.86%) instead of the intended
~0.1%.

To handle this, the Grid strategy uses profit-gated accumulation. Most
cycles sell the full `qty` (1:1), growing quote balance through grid
spread profits. Once realized net profit (accounting for quantity and
maker fees on both legs) exceeds the rounding cost plus
`accumulation_buffer`, a single `sell_mult` sell is triggered to
accumulate base asset. The effective threshold is
`rounding_cost + accumulation_buffer`, where
`rounding_cost = rounding_diff x sell_price` (the USDC value of the
base being accumulated).

`accumulation_buffer` is configured as a `[min, max]` range and resolved
via linear interpolation against the Fear & Greed index, just like
`grid_interval`. Low F&G (fear) resolves closer to `min`, triggering
accumulation sooner (accumulate more aggressively in fearful markets).
High F&G (greed) resolves closer to `max`, requiring more profit before
accumulating (more conservative in greedy markets). For example, with
`qty=0.35`, `sell_mult=0.999`, `accumulation_buffer=[0.5, 5.0]`,
and HYPE at $39.55: the rounding cost is `0.01 x 39.55 = 0.40 USDC`;
at F&G=25 the resolved buffer is ~1.63 USDC (threshold ~2.03), while at
F&G=75 it is ~3.88 USDC (threshold ~4.28). This is adaptive: fearful
markets accumulate faster, greedy markets wait longer. Kraken is
unaffected.

**MM (Adaptive Market Maker)**: Dynamically adapts quoting style based
on market fees. Uses greedy quoting for no-fee markets and conservative
profit-guaranteeing quotes where fees apply.

**Fear & Greed**: Both `grid_interval` and `accumulation_buffer` are
resolved at domain startup using linear interpolation between their
configured `[min, max]` ranges based on the CoinMarketCap Fear & Greed
index (0 to 100). The index is dynamically re-evaluated at runtime
whenever the underlying asset price moves by a significant threshold
(e.g., +/- 3.5%) from the baseline, ensuring both grid spacing and
accumulation behavior adapt to changing market conditions.
`accumulation_buffer` scaling is Hyperliquid-only. Provide `CMC_API_KEY`
for live values; the index defaults to 50.0 when the key is missing or
the API is unreachable.

**Auto-Hedge** (Hyperliquid only): Single-short-per-cycle delta hedge
on perps. Spot buy fills open a perp short; spot sell fills close it.
Enable with `"hedge": true`. Kraken is not supported.

---

## Dashboard

The engine includes a standalone TUI dashboard binary
(`bin/dashboard.exe`) that connects to the running engine over a
Unix domain socket at `/var/run/dio/dashboard.sock` using a
length-prefixed JSON protocol.

### Local
```bash
# Auto-discovers the engine socket
./_build/default/bin/dashboard.exe

# Connect to a specific engine socket
./_build/default/bin/dashboard.exe --socket /var/run/dio/dashboard.sock
```

### Docker

The engine exposes its dashboard socket via the `dio-sock` named
volume. Run the dashboard in a **separate container** using
`docker run --rm -it` — this ties the dashboard's lifetime to your
SSH session so disconnecting SSH kills the dashboard and frees the
server connection slot.

```bash
docker run --rm -it -v dio-sock:/var/run/dio dio dio-dashboard
```

> [!WARNING]
> Do not use `docker exec -it` for the dashboard. Docker exec keeps
> the container-side PTY alive after SSH disconnects, leaving zombie
> connections that are never cleaned up.

The dashboard operates out-of-process for crash isolation and
automatically reconnects when the engine restarts. It runs in
watch mode, where the engine pushes state snapshots every 500ms.

### Panels

| Panel | Description |
|-------|-------------|
| **Header** | Uptime, Fear & Greed index, per-exchange connectivity status |
| **Memory & GC** | Heap size, live/free KB, major/minor collections, compactions, fragments |
| **Holdings & Strategy** | Per-strategy and non-strategy balances, mid-price, accumulated holdings, pending buy/sell distances, unrealized sell value, aggregated portfolio summary |
| **Latency Profiling** | Per-domain percentile profiling (p50, p90, p99, p999) in microseconds |
| **Domains** | Running/stopped status, restart count, last restart age |

### Wire Protocol

The dashboard server runs as an Lwt fiber within the engine process,
listening on `/var/run/dio/dashboard.sock` with file permissions
restricted to the current user (0600). Communication uses a 4-byte
big-endian length-prefixed JSON frame format.

| Command | Direction | Description |
|---------|-----------|-------------|
| `Q` | Client to Server | Close the connection |

Maximum concurrent clients: 5.


---

## Architecture

```
+------------------+     +--------------------+     +--------------------+
|   Main Entry     |---->|  Domain Spawner    |---->|  Strategy Domain   |
|  (CLI, signals)  |     |  (per-asset mgmt)  |     |  (Grid / MM / ..) |
+------------------+     +--------------------+     +--------------------+
                                  |                          |
                                  v                          v
                         +--------------------+     +--------------------+
                         |   Supervisor       |     |  Order Executor    |
                         | (circuit breaker,  |     |  (async placement, |
                         |  heartbeat, health)|     |   dedup, amend)    |
                         +--------------------+     +--------------------+
                                  |                          |
                                  v                          v
               +---------------------------------------------+
               |           Engine Core                       |
               |  - Lock-free tick bus & event registry      |
               |  - Histogram latency profiling (us)         |
               |  - Structured logging (level + section)     |
               |  - Dashboard UDS server (JSON over socket)  |
               +---------------------------------------------+
                        |                        |
                        v                        v
          +-----------------------+   +-----------------------+
          |  Kraken Integration   |   | Hyperliquid Integration|
          | - WS: ticker, book,  |   | - WS: allMids, L2,    |
          |   balance, exec      |   |   webData2, spotState  |
          | - Trading client     |   | - REST: L1 sigs,       |
          |   (auth, heartbeat)  |   |   retry + backoff      |
          | - Ring buffer store  |   | - Global order index,  |
          +-----------------------+   |   double-check locking |
                                      +-----------------------+
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KRAKEN_API_KEY` | Conditional | Kraken API key. Required when trading on Kraken. |
| `KRAKEN_API_SECRET` | Conditional | Kraken API secret. Required when trading on Kraken. |
| `HYPERLIQUID_WALLET_ADDRESS` | Conditional | Public wallet address. Required for Hyperliquid. |
| `HYPERLIQUID_AGENT_ADDRESS` | Conditional | Agent address for L1 auth. Required for Hyperliquid. |
| `HYPERLIQUID_PRIVATE_KEY` | Conditional | Private key for L1 signature generation. Required for Hyperliquid. |
| `CMC_API_KEY` | Optional | CoinMarketCap API key for Fear & Greed index. Falls back to 50.0 when absent. |
| `DIO_BACKTRACE` | Optional | Set to any value to enable exception backtraces. Disabled by default to avoid allocation overhead. |

---

## Logging

The engine uses a structured, domain-safe logging system with five
severity levels: `DEBUG`, `INFO`, `WARN`, `ERROR`, and `CRITICAL`.

Key properties:

- **Per-section filtering**: Log output can be restricted to specific
  sections via the `logging_sections` config field (comma-separated).
- **Domain safety**: All log output is serialized through a shared mutex
  to prevent interleaved lines from concurrent OCaml 5 domains.
- **Zero-allocation fast path**: Disabled log levels skip string
  formatting entirely using `Printf.ifprintf`.
- **ANSI color output**: Enabled by default for terminal output; each
  severity level uses a distinct color.
- **Timestamped**: All entries include millisecond-precision wall-clock
  timestamps with per-second caching to reduce `localtime` calls.

---

## Development

```bash
# Build
dune build

# Tests
dune test

# Format / Docs
dune fmt
dune build @doc
```

### Hyperliquid State Persistence

The Grid strategy persists accumulation state to disk so
`reserved_base`, `accumulated_profit`, and `last_fill_oid` survive
restarts.

**State file**: `data/accumulated_state.json` (local dev) or
`/app/data/accumulated_state.json` (Docker).

```json
{
  "HYPE/USDC": {
    "reserved_base": 0.15,
    "accumulated_profit": 0.482716,
    "last_fill_oid": "355883830672"
  }
}
```

State is written atomically (temp file + rename) with a mutex for
multi-domain safety. On startup the strategy loads persisted values;
missing fields default to `0.0` / `None`.

#### Docker

Mount a host volume for the `data/` directory so state survives
container rebuilds:

```bash
docker run -v /path/on/host/data:/app/data -v dio-sock:/var/run/dio --env-file .env dio
```

The Docker image uses `jemalloc` (`LD_PRELOAD=libjemalloc.so.2`) to
prevent glibc arena fragmentation, with tuning for fast dirty/muzzy
page decay and limited arenas (`narenas:2`) suited to OCaml 5
multicore workloads.

#### OS-Specific Notes

**macOS / Linux**: No additional setup needed. The `data/` directory
is created automatically on first write.

**Windows (WSL2, recommended)**: Run inside WSL2 (Ubuntu 22.04+).
The Unix build toolchain and `Sys.rename` atomicity work as expected:

```bash
# Inside WSL2
opam install . --deps-only
dune build
./_build/default/bin/main.exe
```

> [!TIP]
> Store the project and `data/` directory **inside the WSL filesystem** (e.g. `~/dio-domains/`), not on a `/mnt/c/` Windows mount. NTFS mounts have poor `inotify` support and `Sys.rename` across filesystem boundaries can fail silently.

**Windows (native, advanced)**: Requires MSVC or MinGW OCaml. Key
differences:

1. **Atomic rename**: `Sys.rename` on Windows fails if the target file is open by another process (e.g. antivirus scanner). Exclude `data/` from real-time scanning.
2. **Path separators**: `Filename.concat` handles `\` vs `/` correctly.
3. **Docker path**: `/app` detection does not trigger on native Windows; state always writes to `./data/`.
4. **libsecp256k1**: Must be compiled manually. See the [secp256k1 build docs](https://github.com/bitcoin-core/secp256k1#building-on-windows).

> [!WARNING]
> Native Windows is not actively tested. WSL2 or Docker is strongly recommended.

---

## Graceful Shutdown

The engine handles `SIGINT` and `SIGTERM` for orderly shutdown. A
second `SIGINT` forces immediate exit. The shutdown sequence:

1. Close all open Hyperliquid hedge positions (if any).
2. Tear down exchange feeds and strategy domains.
3. Clean up the dashboard UDS socket.
4. Force-exit after a 3-second timeout if teardown stalls.

Fatal signals (`SIGSEGV`, `SIGABRT`, `SIGBUS`, `SIGFPE`) are caught to
emit diagnostic snapshots (GC stats, domain status) before termination.

---

## Contributing

```
1. Fork
2. Create branch  -->  git checkout -b feature/xyz
     - Trading strategies:       src/engine/strategies/
     - Domain management:        src/engine/domain_spawner.ml
     - Concurrency & Bus:        src/engine/concurrency/
     - Latency Profiling:        src/engine/latency_profiling/
     - Connection supervision:   src/engine/supervisor/
     - Dashboard (engine):       src/engine/dashboard/
     - Dashboard (TUI):          bin/dashboard.ml
     - WebSocket feeds (Kraken): src/external/kraken/
     - WebSocket feeds (HL):     src/external/hyperliquid/
     - Exchange interface:       src/external/exchange_intf.ml
3. Commit  -->  git commit -m "..."
4. Push    -->  git push origin feature/xyz
5. Open PR
```

Guidelines: add tests, update docs, keep CI green. See
`src/external/README.md` for the exchange integration guide when adding
new exchange backends.

---

## License

MIT. See `LICENSE`.

```
Legal: Provided "as is", without warranty. Trading involves risk.
```