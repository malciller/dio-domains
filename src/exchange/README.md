# Exchange Integration Guide

This document outlines the requirements and architecture for integrating a new exchange into the trading system.

## Architecture Guidelines

The trading engine uses a modular architecture where exchanges are implemented as modules satisfying a common interface (`Exchange.S`). The `order_executor.ml` component acts as the bridge between strategies and specific exchange implementations.

### Directory Structure
- **Interface**: `src/exchange/exchange_intf.ml`
- ** implementations**: `src/exchange/<exchange_name>/` (e.g., `src/exchange/kraken/`)

## Requirements for New Exchanges

To add a new exchange, you must implement the `Exchange.S` module type defined in `src/exchange/exchange_intf.ml`.

### 1. Minimal Interface Implementation

Your module must implement the following core capabilities:

- **Order Management**:
    - `place_order`: Handle new order placement.
    - `amend_order`: Handle modification of existing orders.
    - `cancel_orders`: Handle batch or single cancellation.
- **Market Data**:
    - `get_ticker` & `get_top_of_book`: Provide latest price data.
    - `get_balance`: Provide current asset balances.
    - `get_open_orders`: Return list of currently active orders.
- **Feeds (WebSocket/Stream)**:
    - Implement readers for Ticker, Orderbook, and Execution events.
- **Metadata**:
    - `get_price_increment`: Tick size for symbols.
    - `get_qty_increment`: Step size for quantity.
    - `get_fees`: Maker/Taker fee structure.

### 2. Registration

Register your exchange in `Exchange.Registry` at startup (usually in your module's initialization or `bin/main.ml`):
```ocaml
let () = Exchange.Registry.register (module MyNewExchange)
```

## Strategy Interaction & Requirements

Strategies (like `market_maker` and `suicide_grid`) operate generically but rely on specific exchange capabilities:

1.  **Precision Handling**: Strategies use `get_price_increment` and `get_qty_increment` to round prices and quantities *before* sending orders. Your `get_` implementation must be accurate to avoid `INVALID_ARGS` errors from the exchange.
2.  **Order Tracking**: Strategies track orders via `client_order_id` (`cl_ord_id`) or `userref`. Ensure your `place_order` stores and returns these identifiers correctly.
3.  **Balance Checks**: Strategies pause if `get_balance` returns insufficient funds. Ensure this is always up-to-date (via WS or REST).

## New Order Handling Specification

When the `order_executor` processes a new order request, it passes the following data to your `place_order` function:

| Field | Type | Description |
|-------|------|-------------|
| `token` | `string` | Auth token or API key identifier (implementation specific). |
| `symbol` | `string` | The trading pair (e.g., "BTC/USD"). |
| `side` | `Types.order_side` | `Buy` or `Sell`. |
| `order_type` | `Types.order_type` | `Limit`, `Market`, etc. |
| `qty` | `float` | Order quantity. |
| `limit_price` | `float option` | Required for Limit orders. |
| `post_only` | `bool option` | If true, order must be maker. |
| `reduce_only` | `bool option` | If true, order only reduces position. |
| `cl_ord_id` | `string option` | Client-side UUID for tracking. |

### Handling Specifics for Amendments

For `amend_order`:
- **Symbol Requirement**: While generic interfaces might make `symbol` optional, it is **CRITICAL** to handle it if provided.
- **Precision Truncation**: The exchange implementation is responsible for final wire-format truncation. Use internal metadata to format `price` and `qty` to the exact decimal strings required by the API (e.g., `%.2f` vs `%.8f`).
    - *Example*: Kraken requires the `symbol` to look up precision info during an amendment to format the JSON string correctly.

## Implementation Checklist

- [ ] Create `src/exchange/<name>/<name>_actions.ml` implementing the specific REST/WS calls.
- [ ] Create `src/exchange/<name>/<name>.ml` implementing the `Exchange.S` interface.
- [ ] Ensure `amend_order` respects precision rules for the specific API.
- [ ] Register the module in the main entry point.
