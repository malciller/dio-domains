# Dio

[![OCaml](https://img.shields.io/badge/Language-OCaml-blue.svg)](https://ocaml.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance OCaml 5.2 trading engine for Kraken featuring domain-based parallel strategy execution. Each trading asset runs in its own isolated domain with lock-free communication, circuit breaker connection management, and real-time telemetry. Built for high-frequency trading with WebSocket data feeds, asynchronous order execution.

## Requirements

- OCaml 5.2.0 (opam)
- Kraken API key/secret
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
ALLOWED_CLIENT_TOKEN=your_client_token
ALLOWED_CLIENT_IP=machine_name_or_ip_to_whitelist
```

Edit `config.json` (example):
```json
{
  "logging_level": "info",            // Log verbosity: debug, info, warning, error
  "logging_sections": "",    // Filter logs by section (optional, comma-separated)
    "metrics_broadcast": {
      "active": true, // bool, whether to broadcast metrics via tcp or not
      "port": 8080, // int, port to broadcast tcp stream
      "enable_token_auth": true, // bool, enable token authorization, leave false for no toke validation
      "enable_ip_whitelist": true // bool, enable specific ip access, leave false for all connection sources
  },         
  "trading": [
    {
      "symbol": "BTC/USD",            // Pair to trade
      "qty": "0.0002",                // Base asset quantity per order
      "grid_interval": "0.33",        // Distance between buy/sell orders (as price units)
      "sell_mult": "0.999",           // Sell amount multiplier (qty * sell_mult = sell order size)
      "strategy": "Grid"              // Strategy name: "Grid"
    },
    {
      "symbol": "USDG/USD",           // Pair to trade
      "qty": "100.0",                 // Trade size per market making quote
      "min_usd_balance": "500.0",     // Minimum USD balance required to run this strategy. Optional, but required if no max_exposure.
      "max_exposure": "500.0",        // Maximum asset balance allowed before pausing strategy. Optional, but required if no min_usd_balance.
      "strategy": "MM"                // Strategy name: "MM"
    }
  ]
}
```

### Run
```bash
./_build/default/bin/main.exe            
```

## Strategies

- GRID: Maintains buy/sell ladders around price with configurable spacing and size.
- MM (Adaptive Market Maker): Dynamically adapts its quoting style based on market fees—uses a greedy quoting approach for no-fee markets and a conservative, profit-guaranteeing strategy where trading fees apply. Always ensures configured profit margins are met by factoring in fees as necessary.

## Key Features

- **Domain-Based Parallelism**: Each trading asset runs in its own OCaml domain for true parallel execution without GIL limitations
- **Lock-Free Communication**: Event-driven architecture with ring buffers for high-throughput data processing
- **Circuit Breaker Protection**: Automatic connection management with health monitoring and graceful degradation
- **Real-Time Telemetry**: Comprehensive metrics collection for performance monitoring and debugging
- **High-Frequency Trading**: Optimized for low-latency execution with microsecond-precision timing
- **Fault Tolerance**: Supervised domains with automatic restart and exponential backoff
- **Reactive Dashboard**: Real-time terminal UI with live metrics and connection status

## Architecture

- **Main Entry Point**: Command-line interface with dashboard and headless modes, signal handling for graceful shutdown.
- **Domain Spawner**: Manages OCaml domains for parallel strategy execution - each trading asset runs in its own isolated domain with supervision and auto-restart capabilities.
- **Supervisor**: Orchestrates WebSocket connections with circuit breaker patterns, heartbeat monitoring, and connection health management.
- **Engine Core**:
  - **Concurrency**: Lock-free event bus and registry for inter-domain communication.
  - **Config**: Configuration management and validation.
  - **Strategies**: High-frequency trading algorithms (Grid, Adaptive Market Maker) running in parallel domains.
  - **Order Executor**: Asynchronous order placement, amendment, and cancellation with duplicate detection.
  - **Telemetry**: Real-time performance monitoring and metrics collection.
  - **Logging**: Structured logging with configurable levels and sections.
- **Exchange Integration (Kraken)**:
  - **WebSocket Feeds**: Real-time ticker, orderbook, balance, and execution data with ring buffer storage.
  - **Trading Client**: Authenticated order operations with ping/pong heartbeat monitoring.
  - **Authentication**: Secure token generation and management.
- **UI**: Raw TCP stream of vairous metrics and values from the bot. 


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

## Memory Profiling and Debugging

Dio includes built-in memory profiling tools to help identify memory leaks and performance issues:

### Enhanced Memory Monitoring

Dio provides comprehensive memory monitoring using OCaml's built-in `Gc.stat()` API, designed to work out-of-the-box with OCaml 5.1.1 multicore:

```bash
# Build and run the project
dune build
./_build/default/bin/main.exe

# Memory monitoring features:
# - Automatic leak detection (alerts when heap grows >100MB over 5+ minutes)
# - Per-domain heap tracking in multicore environments
# - Allocation rate monitoring
# - Detailed GC statistics logging every 5 minutes
# - Automatic GC triggering every 15 minutes to prevent memory creep
```

**Memory Monitoring Output:**
```
Memory leak detected: 150.2 MB growth in 420 seconds, triggering GC
GC Stats: heap=234.5MB live=189.3MB free=45.2MB fragments=12 compactions=3
Domain 1: heap=156.7MB (last update 12.3s ago)
```

### Memory Monitoring

The system automatically monitors memory usage and stream depths:

- **Periodic GC**: Automatic garbage collection every 15 minutes
- **Memory leak detection**: Alerts when heap grows by >100MB over recent readings
- **Per-domain tracking**: Monitors heap usage across multiple domains in multicore environments
- **Allocation rate tracking**: Monitors allocation patterns to identify hotspots
- **Stream depth monitoring**: Warns when event bus streams exceed 100 items
- **Detailed reporting**: Memory statistics logged every 5 minutes

### Common Issues and Solutions

**Backpressure Warnings**: If you see "consecutive timeouts" messages, the system is dropping items to prevent memory accumulation.

**Memory Growth**: Check logs for GC activity and memory statistics. The built-in monitoring identifies allocation hotspots and provides detailed heap analysis.

**Stream Depth Warnings**: High stream depths indicate consumers are slower than producers. The system automatically drops old items.

### External Memory Profiling Tools

For additional analysis, consider these external tools (note: these don't work with OCaml 5.1.1 multicore out-of-the-box):

```bash
# Valgrind (memory leak detection via system call tracing)
valgrind --tool=memcheck ./_build/default/bin/main.exe

# Heaptrack (memory allocation profiling)
heaptrack ./_build/default/bin/main.exe
```

**Future Note**: OCaml 5.3 will include statistical memory profiling (statmemprof) with full multicore support, providing more detailed allocation analysis than the current `Gc.stat()` approach.

## Logging

- Debug, Info, Warning, Error — timestamped by component.

## Contributing

1. Fork
2. Create branch (`git checkout -b feature/xyz`)
    - Trading strategies: `src/engine/strategies/`
    - Domain management: `src/engine/domain_spawner.ml`
    - Connection supervision: `src/engine/supervisor/`
    - WebSocket feeds: `src/exchange/kraken/`
    - Dashboard UI: `src/ui/`
3. Commit (`git commit -m "..."`)
4. Push (`git push origin feature/xyz`)
5. Open PR

Guidelines: add tests, update docs, keep CI green.

## License

MIT — see `LICENSE`.

Legal: Provided “as is”, without warranty. Trading involves risk.