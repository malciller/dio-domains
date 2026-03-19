# Dio

[![OCaml](https://img.shields.io/badge/Language-OCaml-blue.svg)](https://ocaml.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance OCaml 5.2 trading engine for Kraken and Hyperliquid featuring domain-based parallel strategy execution. Each trading asset runs in its own isolated domain with lock-free communication, tick-driven event architecture, and real-time latency profiling. Built for high-frequency trading with WebSocket data feeds and asynchronous order execution.

> [!WARNING]
> **Auto-hedge for Hyperliquid is EXPERIMENTAL.** Testing is ongoing; features may be incomplete or unstable.

## Requirements

- OCaml 5.2.0 (opam)
- Kraken API key/secret and/or Hyperliquid wallet/key
- macOS/Linux/WSL

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
  "logging_level": "info",            // Log verbosity: debug, info, warning, error
  "logging_sections": "",             // Filter logs by section (optional, comma-separated)
  "logging_cycle_debug_mod": 1000000,   // Cycle interval for debug logs (logging_level must be debug for visibility)
  "logging_cycle_info_mod": 1000000,    // Cycle interval for info stats (logging_level must be info or debug for visibility)
  "engine": {
    "balance_check_mod": 10000,         // Cycle interval for balance checks
    "strategy_fallback_mod": 10000      // Cycle interval for strategy fallback execution
  },
  "trading": [
    {
      "symbol": "BTC/USD",            // Pair to trade
      "exchange": "kraken",           // Exchange name: "kraken", "hyperliquid"
      "qty": "0.0002",                // Base asset quantity per order
      "grid_interval": [0.25, 1.25],  // Min/Max grid spacing (%); resolved once from Fear & Greed
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
      "sell_mult": "1.0",             // Sell amount multiplier
      "strategy": "Grid",             // Strategy name: "Grid"
      "testnet": true                 // Use testnet
    },
    {
      "symbol": "HYPE/USDC",          // Pair to trade
      "exchange": "hyperliquid",      // Exchange name: "kraken", "hyperliquid"
      "qty": "1.0",                 // Trade size per market making quote
      "strategy": "MM",               // Strategy name: "MM"
      "testnet": false                // Use testnet
    },
    {
      "symbol": "HYPE/USDC",          // Pair to trade (spot)
      "exchange": "hyperliquid",      // Exchange name (must be "hyperliquid")
      "qty": "0.35",                  // Base asset quantity per order
      "grid_interval": [0.25, 0.5],   // Min/Max grid spacing (%)
      "sell_mult": "0.999",           // Sell amount multiplier (triggers discrete accumulation on Hyperliquid)
      "strategy": "Grid",             // Strategy name: "Grid"
      "testnet": false,               // Use testnet
      "hedge": true,                  // Enable auto-hedge (Hyperliquid only)
      "accumulation_buffer": 0.01     // Min quote profit buffer before triggering sell_mult accumulation (default: 0.01)
    }
  ]
}
```

### Run
```bash
./_build/default/bin/main.exe            
```

## Strategies

**Grid**: Maintains buy/sell ladders around price with configurable spacing and size. Acts as a market maker with optional DCA accumulation via `sell_mult`. On Kraken, sells use `qty * sell_mult` directly. On Hyperliquid, discrete sizing rules apply (see below).

**MM (Adaptive Market Maker)**: Dynamically adapts quoting style based on market fees. Uses greedy quoting for no-fee markets and conservative profit-guaranteeing quotes where fees apply.

**Fear & Greed**: Grid spacing is resolved once at domain startup using linear interpolation between configured `grid_interval` [min, max] based on the CoinMarketCap Fear & Greed index. Provide `CMC_API_KEY` for live values.

**Auto-Hedge** (Hyperliquid only): Single-short-per-cycle delta hedge on perps. Spot buy fills open a perp short; spot sell fills close it. Enable with `"hedge": true`. Kraken is not supported.

### Hyperliquid Discrete Sizing

Hyperliquid enforces discrete order sizes via `szDecimals` (e.g. HYPE uses 2 decimal places, so the lot increment is 0.01). When `sell_mult < 1.0`, the desired sell quantity often falls between valid lot boundaries and must be floored, creating rounding loss that exceeds the intended skim. For example, `0.35 * 0.999 = 0.34965` floors to `0.34`, losing 0.01 HYPE per cycle (~2.86%) instead of the intended ~0.1%.

To handle this, the Grid strategy uses profit-gated accumulation. Most cycles sell the full `qty` (1:1), growing quote balance through grid spread profits. Once realized net profit (accounting for quantity and maker fees on both legs) exceeds the rounding cost plus `accumulation_buffer`, a single `sell_mult` sell is triggered to accumulate base asset. The `accumulation_buffer` parameter (default: 0.01 USDC) sets the minimum quote profit required on top of the rounding cost before accumulation triggers. This is adaptive: fast markets accumulate faster, slow markets wait longer. Kraken is unaffected.

## Key Features

- **Domain-Based Parallelism**: Each trading asset runs in its own OCaml domain for true parallel execution without GIL limitations
- **Tick-Driven Architecture**: Event-driven engine with a global tick bus and lock-free ring buffers for high-throughput data processing
- **High-Performance Latency Profiling**: Real-time histogram-based profiling (p50-p999) of hot loops and strategy execution
- **Circuit Breaker Protection**: Automatic connection management with health monitoring and graceful degradation
- **High-Frequency Trading**: Optimized for low-latency execution with microsecond-precision timing
- **Fault Tolerance**: Supervised domains with automatic restart and exponential backoff

## Architecture

- **Main Entry Point**: Command-line interface with dashboard and headless modes, signal handling for graceful shutdown.
- **Domain Spawner**: Manages OCaml domains for parallel strategy execution. Each trading asset runs in its own isolated domain with supervision and auto-restart capabilities.
- **Supervisor**: Orchestrates WebSocket connections with circuit breaker patterns, heartbeat monitoring, and connection health management.
- **Engine Core**:
  - **Concurrency**: Lock-free global tick bus and event registry for inter-domain communication.
  - **Latency Profiling**: Histogram-based statistics tracking for microsecond-precision performance monitoring.
  - **Strategies**: High-frequency trading algorithms (Grid, MM) running in parallel domains with F&G integration.
  - **Order Executor**: Asynchronous order placement and amendment with duplicate detection.
  - **Logging**: Structured logging with configurable levels and sections.
- **External Integration (Kraken)**:
  - **WebSocket Feeds**: Real-time ticker, orderbook, balance, and execution data with ring buffer storage.
  - **Trading Client**: Authenticated order operations with ping/pong heartbeat monitoring.
  - **Authentication**: Secure token generation and management.
- **External Integration (Hyperliquid)**:
  - **WebSocket Feeds**: Real-time ticker (allMids), L2 orderbook, balance (webData2/spotState), and execution data with ring buffer storage.
  - **REST Actions**: Authenticated order placement, amendment, and cancellation with L1 signature generation and retry with exponential backoff.
  - **Concurrency**: Global order index, double-checked locking on stores, self-restarting processor tasks, and stale-order cleanup.

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

```bash
# Build and run the project
dune build
./_build/default/bin/main.exe
```
## Logging

- Debug, Info, Warning, Error, timestamped by component.

## Contributing

1. Fork
2. Create branch (`git checkout -b feature/xyz`)
    - Trading strategies: `src/engine/strategies/`
    - Domain management: `src/engine/domain_spawner.ml`
    - Concurrency & Bus: `src/engine/concurrency/`
    - Latency Profiling: `src/engine/latency_profiling/`
    - Connection supervision: `src/engine/supervisor/`
    - WebSocket feeds (Kraken): `src/external/kraken/`
    - WebSocket feeds (Hyperliquid): `src/external/hyperliquid/`
    - Exchange interface: `src/external/exchange_intf.ml`
3. Commit (`git commit -m "..."`)
4. Push (`git push origin feature/xyz`)
5. Open PR

Guidelines: add tests, update docs, keep CI green.

## License

MIT. See `LICENSE`.

Legal: Provided "as is", without warranty. Trading involves risk.