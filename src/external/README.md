# Exchange Integration Guide

This document describes the architecture and requirements for integrating a new exchange into the trading system. All exchange-specific types, the canonical module signature, and the runtime registry are defined in `src/external/exchange_intf.ml`.

## Architecture

The trading engine uses a modular architecture where exchanges are implemented as first-class modules satisfying the `Exchange_intf.S` module type. The `order_executor.ml` component dispatches strategy-generated order intents to the appropriate exchange module, which is resolved at runtime through `Exchange_intf.Registry`.

### Directory Structure

- **Interface**: `src/external/exchange_intf.ml`
- **Implementations**: `src/external/<exchange_name>/` (e.g., `src/external/kraken/`, `src/external/hyperliquid/`)

### Current Implementations

Both exchange modules follow the same concurrency and safety patterns:

| Pattern | Description |
|---------|-------------|
| **Ring Buffers** | Lock-free `Concurrency.Ring_buffer.RingBuffer` for all feed data (tickers, executions, orderbook) |
| **Global Order Index** | `order_to_symbol` Hashtbl for O(1) order lookups across symbols |
| **Double-Checked Locking** | Mutex-protected store initialization to prevent TOCTOU races |
| **Atomic Flags** | `Atomic.t` for all shared boolean/float state (readiness, timestamps, config) |
| **Retry with Backoff** | Exponential backoff on transient API errors (timeouts, 5xx, rate limits), configured via `Types.retry_config` |
| **Crash Recovery** | Self-restarting processor tasks with backoff on failure |
| **Stale Order Cleanup** | Periodic removal of orders older than 24h to prevent unbounded growth |
| **Event Bus** | `Concurrency.Event_bus` for publishing order/balance updates |
| **Inline Hot Paths** | `[@inline always]` on all frequently-called accessor functions |

## Shared Types (`Exchange_intf.Types`)

The `Types` submodule defines all records and variants shared across exchange implementations:

| Type | Purpose |
|------|---------|
| `order_type` | Limit, Market, StopLoss, TakeProfit, StopLossLimit, TakeProfitLimit, SettlPosition, Other |
| `order_side` | Buy, Sell |
| `time_in_force` | GTC, IOC, FOK |
| `order_status` | Pending, New, PartiallyFilled, Filled, Canceled, Expired, Rejected, Unknown |
| `add_order_result` | Acknowledgment after order placement (order_id, cl_ord_id, order_userref) |
| `amend_order_result` | Acknowledgment after amendment (original_order_id, new_order_id, amend_id, cl_ord_id) |
| `cancel_order_result` | Acknowledgment after cancellation (order_id, cl_ord_id) |
| `open_order` | Snapshot of a live order including fill progress and optional client identifiers |
| `ticker_event` | Top-of-book snapshot: best bid, best ask, timestamp |
| `orderbook_event` | Depth snapshot: arrays of (price, size) bid/ask levels, timestamp |
| `execution_event` | Order state change report: status, fills, remaining qty, avg price |
| `retry_config` | Exponential backoff parameters: max_attempts, base_delay_ms, max_delay_ms, backoff_factor |

## Requirements for New Exchanges

To add a new exchange, implement the `Exchange_intf.S` module type.

### 1. Module Signature

Your module must provide all values specified in module type `S`. The interface is grouped into four categories:

#### Order Lifecycle

| Function | Signature | Description |
|----------|-----------|-------------|
| `place_order` | `token -> order_type -> side -> qty -> symbol -> ... -> (add_order_result, string) result Lwt.t` | Submit a new order. Required params: token, order_type, side, qty, symbol. Optional: limit_price, time_in_force, post_only, reduce_only, order_userref, cl_ord_id, trigger_price, display_qty, retry_config. |
| `amend_order` | `token -> order_id -> ... -> (amend_order_result, string) result Lwt.t` | Modify an existing order. Required: token, order_id. Optional: cl_ord_id, qty, limit_price, post_only, trigger_price, display_qty, symbol, retry_config. |
| `cancel_orders` | `token -> ... -> (cancel_order_result list, string) result Lwt.t` | Cancel orders by order_id, cl_ord_id, or order_userref. At least one identifier list must be non-empty. |

#### Market Data Accessors

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_ticker` | `symbol -> (float * float) option` | Return current (best_bid, best_ask), or None if unavailable. |
| `subscribe_ticker` | `symbol -> unit Lwt.t` | Dynamically subscribe to the ticker feed for a symbol. |
| `get_top_of_book` | `symbol -> (float * float * float * float) option` | Return (bid_price, bid_size, ask_price, ask_size), or None. |
| `get_balance` | `asset -> float` | Return the current balance for an asset. Returns 0.0 if unknown. |
| `get_all_balances` | `unit -> (string * float) list` | Return all cached (asset_name, balance) pairs. |
| `get_open_order` | `symbol -> order_id -> open_order option` | Look up a specific open order by symbol and order_id. |
| `get_open_orders` | `symbol -> open_order list` | Return all open orders for a symbol. |

#### Ring Buffer Event Feed Consumption

Each feed type (ticker, orderbook, execution) exposes three functions: a position query, a list-returning reader, and a zero-allocation iterator.

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_ticker_position` | `symbol -> int` | Current write position of the ticker ring buffer. |
| `read_ticker_events` | `symbol -> start_pos -> ticker_event list` | Read ticker events from start_pos to the current position. Allocates a list. |
| `iter_ticker_events` | `symbol -> start_pos -> (ticker_event -> unit) -> int` | Iterate over ticker events without allocation. Returns new read position. |
| `get_orderbook_position` | `symbol -> int` | Current write position of the orderbook ring buffer. |
| `read_orderbook_events` | `symbol -> start_pos -> orderbook_event list` | Read orderbook events from start_pos. Allocates a list. |
| `iter_orderbook_events` | `symbol -> start_pos -> (orderbook_event -> unit) -> int` | Iterate over orderbook events without allocation. Returns new read position. |
| `get_execution_feed_position` | `symbol -> int` | Current write position of the execution ring buffer. |
| `read_execution_events` | `symbol -> start_pos -> execution_event list` | Read execution events from start_pos. Allocates a list. |
| `iter_execution_events` | `symbol -> start_pos -> (execution_event -> unit) -> int` | Iterate over execution events without allocation. Returns new read position. |
| `fold_open_orders` | `symbol -> init -> f -> 'a` | Fold over open orders without allocating an intermediate list. |

#### Instrument Metadata

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_price_increment` | `symbol -> float option` | Minimum price increment (tick size). None if metadata unavailable. |
| `get_qty_increment` | `symbol -> float option` | Minimum quantity increment (lot step). None if metadata unavailable. |
| `get_qty_min` | `symbol -> float option` | Minimum order quantity. None if metadata unavailable. |
| `round_price` | `symbol -> price -> float` | Round price to exchange-valid precision. Hyperliquid: 5 significant figures capped at (MAX_DECIMALS - szDecimals) decimals. Kraken: nearest price_increment tick. |
| `get_fees` | `symbol -> (float option * float option)` | Return cached (maker_fee, taker_fee). Each is None if not yet fetched. |

### 2. Concurrency Requirements

New integrations must follow the established patterns:

- Use `Mutex.t` (not `Lwt_mutex`) for protecting shared data structures accessed from multiple domains.
- Use `Atomic.t` for all shared flags and scalar state. Never use bare `ref`.
- Use double-checked locking for lazy store initialization (`ensure_store` / `get_symbol_store`).
- Hold locks for the entire read-modify-write cycle. Never release between read and write.
- Use `Lwt_list.fold_left_s` or sequential processing for accumulating results. Never share mutable `ref` across concurrent Lwt promises.
- Implement retry with exponential backoff on all REST API operations, parameterized by `Types.retry_config`.
- Processor tasks must self-restart on crash (re-subscribe and resume after delay).

### 3. Registration

Register your exchange module via `Exchange_intf.Registry` at startup (typically in your module's initialization or `bin/main.ml`). The registry uses `Hashtbl.replace`, so re-registration overwrites any prior entry with the same name.

```ocaml
let () = Exchange_intf.Registry.register (module MyNewExchange)
```

Lookup at runtime:

```ocaml
match Exchange_intf.Registry.get "my_exchange" with
| Some (module E : Exchange_intf.S) -> (* use E *)
| None -> failwith "exchange not registered"
```

## Strategy Interaction

Strategies (e.g., `market_maker`, `suicide_grid`) operate generically over `Exchange_intf.S` modules. Key dependencies:

1. **Precision Handling**: Strategies call `get_price_increment`, `get_qty_increment`, and `round_price` to format prices and quantities before submitting orders. Inaccurate metadata causes `INVALID_ARGS` rejections.
2. **Order Tracking**: Strategies track orders via `cl_ord_id` or `order_userref`. Implementations must store and return these identifiers in `add_order_result` and propagate them through execution events.
3. **Balance Checks**: Strategies pause when `get_balance` returns insufficient funds. This value must stay current via WebSocket or periodic REST polling.
4. **Zero-Allocation Iteration**: Hot-path strategies use `iter_ticker_events`, `iter_orderbook_events`, `iter_execution_events`, and `fold_open_orders` to avoid per-tick list allocations.
5. **Dynamic Fear & Greed Re-evaluation**: Engine supervisors monitor price variations across ticker events. When price movement exceeds the dynamically configured `fng_check_threshold` (set in `config.json`), strategies trigger a Fear & Greed index recalculation. Timely ticker feeds are essential for accurate threshold adherence.

## Order Handling Specification

### place_order Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `token` | `string` | Yes | Auth token or API key identifier (implementation-specific). |
| `order_type` | `Types.order_type` | Yes | Limit, Market, StopLoss, etc. |
| `side` | `Types.order_side` | Yes | Buy or Sell. |
| `qty` | `float` | Yes | Order quantity. |
| `symbol` | `string` | Yes | Trading pair (e.g., "BTC/USD"). |
| `limit_price` | `float` | No | Required for Limit orders. |
| `time_in_force` | `Types.time_in_force` | No | GTC (default), IOC, or FOK. |
| `post_only` | `bool` | No | If true, order must be maker-only. |
| `reduce_only` | `bool` | No | If true, order only reduces an existing position. |
| `order_userref` | `int` | No | User-defined integer reference for tracking. |
| `cl_ord_id` | `string` | No | Client-side UUID for tracking. |
| `trigger_price` | `float` | No | Trigger price for conditional order types (StopLoss, TakeProfit). |
| `display_qty` | `float` | No | Visible quantity for iceberg orders. |
| `retry_config` | `Types.retry_config` | No | Override default retry/backoff behavior. |

### amend_order Notes

- Requires `token` and `order_id`. All other fields are optional.
- `symbol` may be required by certain exchange APIs (e.g., Kraken uses it to look up precision metadata for price/qty formatting).
- The exchange implementation is responsible for final wire-format truncation using internal instrument metadata.

## Implementation Checklist

- [ ] Create `src/external/<name>/` directory
- [ ] Create `<name>_actions.ml` implementing REST/WS calls with retry logic
- [ ] Create `<name>_module.ml` implementing the `Exchange_intf.S` module type
- [ ] Create feed modules (`_ticker_feed.ml`, `_executions_feed.ml`, `_orderbook_feed.ml`) with ring buffers, double-checked locking, and crash recovery
- [ ] Implement all `iter_*` and `fold_open_orders` functions for zero-allocation hot paths
- [ ] Implement `round_price` with exchange-specific precision rules
- [ ] Ensure `amend_order` respects precision rules for the target API
- [ ] Use `Atomic.t` for shared flags, `Mutex.t` for shared data structures
- [ ] Add self-restarting processor tasks with backoff
- [ ] Register the module in the main entry point
