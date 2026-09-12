# DIO Exchange Integration (`dio.exchange`)

This directory is a set of libraries. The `dio.exchange` library contains only the shared interface (`exchange_intf.ml`). Each venue is a separate library, and three shared data clients live alongside them. The engine consumes these libraries; `dio.exchange` itself depends only on `lwt` and `dio.error_handling` (for the shared `retry_config` type), not on the engine's trading, strategy, or oracle code.

```
src/external/
  dune                     (library dio_exchange, public name dio.exchange, module Exchange_intf)
  exchange_intf.ml         (types, module types S and Oracle.S, registries)
  kraken/                  (library dio.kraken,         Kraken spot)
  hyperliquid.xyz/         (library dio.hyperliquid,    Hyperliquid spot + perps)
  lighter.xyz/             (library dio.lighter,        Lighter perps)
  interactivebrokers/      (library dio.ibkr,           IBKR US equities)
  alpaca/                  (library dio.alpaca,         Alpaca US equities)
  yahoo/                   (library dio.yahoo,          deep-history client, Yahoo_deep_history)
  coinmarketcap/           (library dio.cmc,            Fear-and-Greed client, Fear_and_greed)
  discord/                 (library dio.discord,        Discord webhook notifier, Discord.Notifier)
```

---

## The Interface (`Exchange_intf.S`)

Every exchange implements one module type, `Exchange_intf.S`. The engine reaches exchanges only through it. `exchange_intf.ml` is the source of truth; the summary below reflects the current tree.

### Core data types

Defined in `Exchange_intf.Types`:

- `exchange_id`: `Hyperliquid`, `Kraken`, `Lighter`, `Ibkr`, `Alpaca`, `Custom of string`. `exchange_of_string` / `string_of_exchange` map the lowercase names and fall back to `Custom`.
- `order_side`: `Buy`, `Sell`.
- `order_type`: `Limit`, `Market`, `StopLoss`, `TakeProfit`, `StopLossLimit`, `TakeProfitLimit`, `SettlPosition`, `Other of string`.
- `time_in_force`: `GTC`, `IOC`, `FOK`.
- `order_status`: `Pending`, `New`, `PartiallyFilled`, `Filled`, `Canceled`, `Expired`, `Rejected`, `Unknown of string`.
- `add_order_result`: `{ order_id; cl_ord_id; order_userref }`.
- `amend_order_result`: `{ original_order_id; new_order_id; amend_id; cl_ord_id }`.
- `cancel_order_result`: `{ order_id; cl_ord_id }`.
- `open_order`: `{ order_id; symbol; side; qty; cum_qty; remaining_qty; limit_price; status; user_ref; cl_ord_id }`.
- `orderbook_event`: `{ bids : (float * float) array; asks : (float * float) array; timestamp : float }`, where each level is `(price, size)`.
- `execution_event`: `{ order_id; order_status; limit_price; side; remaining_qty; filled_qty; avg_price; timestamp; is_amended; cl_ord_id }`. `is_amended` marks an in-place amendment confirmation (e.g. Kraken `exec_type=amended`) so domain workers skip treating it as a new-order acknowledgment.
- `retry_config`: `{ max_attempts; base_delay_ms; max_delay_ms; backoff_factor }`, re-exported from `Error_handling.retry_config`.
- `bar`: `{ date; open_; high; low; close; volume }`, one per session/date (`date` is ISO `YYYY-MM-DD`).
- `calendar_kind`: `Crypto` (session = calendar day) or `Equity` (market sessions only).

### Order operations

The `S` signature exposes three order operations. Note that `place_order` uses labeled arguments; there is no `order_req` record in the interface.

```ocaml
val place_order
  :  token:string
  -> order_type:Types.order_type
  -> side:Types.order_side
  -> qty:float
  -> symbol:string
  -> ?limit_price:float
  -> ?time_in_force:Types.time_in_force
  -> ?post_only:bool
  -> ?reduce_only:bool
  -> ?order_userref:int
  -> ?cl_ord_id:string
  -> ?trigger_price:float
  -> ?display_qty:float
  -> ?retry_config:Types.retry_config
  -> unit
  -> (Types.add_order_result, string) result Lwt.t

val amend_order
  :  token:string
  -> order_id:string
  -> ?cl_ord_id:string
  -> ?qty:float
  -> ?limit_price:float
  -> ?post_only:bool
  -> ?trigger_price:float
  -> ?display_qty:float
  -> ?symbol:string
  -> ?retry_config:Types.retry_config
  -> unit
  -> (Types.amend_order_result, string) result Lwt.t

val cancel_orders
  :  token:string
  -> ?order_ids:string list
  -> ?cl_ord_ids:string list
  -> ?order_userrefs:int list
  -> ?symbol:string
  -> ?retry_config:Types.retry_config
  -> unit
  -> (Types.cancel_order_result list, string) result Lwt.t
```

### Balance and position queries

There is no single `get_balance`. Balance access is split by intent:

| Function | Purpose |
| --- | --- |
| `get_tradeable_balance` | Tradeable balance available for new orders. |
| `get_tradeable_balance_fast` | Non-blocking cached read of the same figure. |
| `get_available_balance_fast` | Venue-authoritative immediately-sellable quantity (total minus base held by resting orders). On Alpaca, whose stored balance is gross, this is the venue's `qty_available`. Returns `0.0` (or `nan` where "unknown" is distinguished); callers treat that as not-sellable. |
| `get_balance_age_fast` | Seconds since the cached balance snapshot, or `None` when the venue does not track freshness. |
| `get_total_balance` | Total balance including staked/earn/vault balances. |
| `get_staked_balance` | Staked or locked balance that is part of the total but not tradeable. |
| `get_all_balances` | All cached `(asset, balance)` pairs. |

### Order queries

| Function | Purpose |
| --- | --- |
| `get_open_order` | One open order by symbol and order id. |
| `get_open_orders` | All open orders for a symbol. |
| `get_all_orders_for_asset` | Open orders for every symbol store keyed under `asset ^ "/"`; used by the dashboard for non-strategy balance assets. |
| `fold_open_orders` | Allocation-free fold over a symbol's open orders. |
| `iter_open_orders_fast` | Primitive-value iterator that avoids materializing `open_order` records. |
| `get_open_orders_generation` | Counter that increments whenever the account's open-order snapshot changes; `-1` means the venue exposes no signal and callers must treat the snapshot as changed on every call. |

### Market-data access and event feeds

| Function | Purpose |
| --- | --- |
| `subscribe_orderbook` | Subscribe the venue feed to order-book updates for a symbol list. |
| `get_top_of_book` / `get_top_of_book_fast` | `(bid_price, bid_size, ask_price, ask_size) option` from the cached book. |
| `get_orderbook_position` / `get_orderbook_position_fast` | Current write position of the order-book ring buffer. |
| `read_orderbook_events` / `iter_orderbook_events` | Read or iterate order-book events from a start position. |
| `iter_top_of_book_events` | Iterate only top-of-book, callback `(bid_price, bid_size, ask_price, ask_size)`. |
| `get_execution_feed_position` / `_fast` | Current write position of the execution ring buffer. |
| `has_execution_data` / `has_execution_data_fast` | Whether the execution feed has its initial snapshot; domain workers gate on this. |
| `read_execution_events` / `iter_execution_events` | Read or iterate execution events from a start position. |

The `_fast` variants return closures that avoid a hash lookup plus lock acquisition on the hot path. The engine consumes order-book state through ring buffers sized by the position functions, so a feed can lag a strategy without stalling it.

### Instrument metadata and fees

| Function | Purpose |
| --- | --- |
| `get_price_increment` | Minimum price increment (tick size), or `None`. |
| `get_qty_increment` | Minimum quantity increment (lot step), or `None`. |
| `get_qty_min` | Minimum order quantity, or `None`. |
| `round_price` | Round a price to the venue's valid precision. |
| `get_fees` | Cached `(maker_fee, taker_fee)`; each component is `None` until fetched. |

`fetch_fees`, `default_fees`, and `min_notional` are part of the oracle contract (`Oracle.S`, below), not of `S`.

---

## Registration

The engine discovers implementations through two registries, both in `exchange_intf.ml` and both keyed by exchange name:

- `Exchange_intf.Registry` maps `string -> (module S)`.
- `Exchange_intf.Oracle.Registry` maps `string -> (module Oracle.S)`.

Both use `Hashtbl.replace` semantics, so a later registration overrides an earlier one under the same name.

Registration is a load-time side effect of each venue module. Each venue aliases the interface as `Exchange`:

```ocaml
(* kraken/kraken_module.ml *)
module Exchange = Dio_exchange.Exchange_intf
...
let () = Exchange.Registry.register (module Kraken_impl)           (* line 588 *)
let () = Exchange.Oracle.Registry.register (module Kraken_oracle)  (* line 592 *)
```

Oracle adapters do not register themselves; the venue's `<name>_module.ml` performs both registrations. Current registrations:

| Venue | Exchange module / name | Oracle module | Location |
| --- | --- | --- | --- |
| Kraken | `Kraken_impl` / `"kraken"` | `Kraken_oracle` | `kraken/kraken_module.ml:588`, `:592` |
| Hyperliquid | `Hyperliquid_impl` / `"hyperliquid"` | `Hyperliquid_oracle` | `hyperliquid.xyz/hyperliquid_module.ml:1027`, `:1031` |
| Lighter | `Lighter_impl` / `"lighter"` | N/A | `lighter.xyz/lighter_module.ml:693` |
| IBKR | `Ibkr_impl` / `"ibkr"` | N/A | `interactivebrokers/ibkr_module.ml:580` |
| Alpaca | `Alpaca_impl` / `"alpaca"` | `Alpaca_oracle` | `alpaca/alpaca_module.ml:434`, `:438` |

Because OCaml dead-code elimination drops unreferenced modules, the binaries force-reference the venue modules so their registrations run:

```ocaml
(* bin/main.ml:322-324 *)
let () = ignore Kraken.Kraken_module.Kraken_impl.name
let () = ignore Hyperliquid.Module.Hyperliquid_impl.name
let () = ignore Alpaca.Module.Alpaca_impl.name
```

`bin/oracle.ml:302-304` force-references the same three oracle-capable venues. Lighter and IBKR are not force-referenced there; they are linked through the supervisor's feed wiring (`src/engine/supervisor/supervisor_feeds.ml`). A new venue must be reachable somewhere in the binary's link closure.

---

## Current Implementations

### Kraken (`kraken/`, `dio.kraken`)

- Public and private REST (`/0/public`, `/0/private`) plus public (`wss://ws.kraken.com/v2`) and authenticated (`wss://ws-auth.kraken.com/v2`) WebSockets.
- Balance via the authenticated WebSocket store (`kraken_balances_feed.ml`); `get_tradeable_balance` is the store balance minus resting-order holds. Vault/earn allocations are polled over private REST (`Earn/Allocations`).
- `default_fees`: `0.0016` maker / `0.0026` taker; live fees via the private `TradeVolume` endpoint.
- `default_quote`: `USD`. `min_notional`: `0.0`.
- Oracle adapter: public OHLC (`/0/public/OHLC`) at the daily interval (`1440`) and a maximum of 60 pages. The request is seeded with a `since` timestamp (`from = None` means pair inception / `0`); pagination then advances on the response's `last` cursor. A page failure returns the partial history rather than erroring.
- No `testnet` mode.

### Hyperliquid (`hyperliquid.xyz/`, `dio.hyperliquid`)

- Spot and perpetuals, with EIP-712 structured-data signing (`hyperliquid_signer.ml`).
- Live tradeable balance comes from the WebSocket store (`webData2` / `spotState`). The oracle's `fetch_balances` deliberately uses REST `spotClearinghouseState` instead: the live `USDC` store aggregates perp clearinghouse USDC with the spot wallet, while the oracle pool counts spot capital only. For the same reason `live_balances` returns `None`.
- `default_fees`: for spot symbols (containing `/`) `0.0` maker / `0.001` taker; for perps `0.0002` / `0.0005`. Live fees via `/info` with `userFees`.
- `default_quote`: `USDC`. `min_notional`: `10.0` for spot symbols containing `/`, otherwise `0.0`.
- Oracle adapter: `/info` with `candleSnapshot` at interval `1d`, resolving spot versus perp through `spotMeta`. A `/` symbol with no matching Hyperliquid spot pair returns no bars (the asset is inactive) and is never silently substituted with perpetual data.
- The executions feed maintains an order-id-to-symbol index (hashtable plus bounded FIFO queue) so fills can be routed when the venue omits the symbol.

### Lighter (`lighter.xyz/`, `dio.lighter`)

- Perpetual DEX. Signing goes through ctypes FFI to the precompiled Lighter signer shared library (`lighter_signer.ml`); requests are signed locally.
- `LIGHTER_SIGNER_LIB_PATH` must point at the shared library (default `./lighter-signer-<os>-<arch>`); `LIGHTER_API_PRIVATE_KEY` is required. Account selection uses `LIGHTER_API_KEY_INDEX` / `LIGHTER_ACCOUNT_INDEX` (both default `0`).
- `LIGHTER_PROXY_URL` is a comma-separated pool of relay endpoints with round-robin and failover. REST and the private/auth WebSocket go through the proxy when configured; the public market-data WebSocket is always direct.
- Orders are Good-Till-Time with a maximum 28-day TTL, and TIF cannot be changed by modify, so the renewal daemon (`lighter_tif_renewal.ml`) checks hourly and cancel-and-replaces orders within 1 day of expiry to approximate GTC. Order IDs are not stable across a restart.
- Trading-only: there is no `lighter_oracle.ml` and no oracle registration, so the oracle-only fields (`min_notional`, `default_fees`, `default_quote`) do not apply.
- The executions feed maintains an order-id-to-symbol index (hashtable plus bounded FIFO queue).

### Interactive Brokers (`interactivebrokers/`, `dio.ibkr`)

- TCP transport to an IB Gateway. Host `IBKR_GATEWAY_HOST` (default `127.0.0.1`); port `IBKR_GATEWAY_PORT` (default `4002` paper / `4001` live when unset); `IBKR_TRADING_MODE` (`paper` or `live`); `IBKR_CLIENT_ID` (default `0`); `IBKR_ACCOUNT_ID` (auto-detected when unset).
- The adapter supports both market and limit orders; the constraint to limit orders is imposed by the strategies, which submit `limit` only.
- Whole shares only: `get_qty_increment` and `get_qty_min` are both `1.0`.
- Executions come from gateway `orderStatus`, `openOrder`, and `execDetails` messages, tracked in a per-symbol store.
- Trading-only: no oracle adapter.

### Alpaca (`alpaca/`, `dio.alpaca`)

- US equities, paper or live. `data_feed` selects `iex` (default, free) or `sip` (paid, full-market).
- Balances and positions are REST-polled every 2 seconds (`/v2/account`, `/v2/positions`) and published to an atomic store that `get_tradeable_balance_fast` reads. `get_available_balance_fast` returns the venue's available quantity, because the stored balance is gross. Order and fill events arrive over SSE (`/v2/events/trades`), not WebSocket.
- Sessions: regular `09:30`–`16:00` ET; pre-market `04:00`–`09:30`; after-hours `16:00`–`20:00`; overnight `20:00`–`04:00`; the full tradable window runs from Sunday `20:00` to Friday `20:00` ET. The extended-hours flag is attached to non-crypto limit orders placed in an extended session. `day` TIF is forced only for fractional equity orders; otherwise the requested TIF (default `gtc`) is used.
- `default_fees`: `0.0` / `0.0`. `default_quote`: `USD`. `min_notional`: `1.0`.
- Oracle adapter: `data.alpaca.markets` `/v2/stocks/{symbol}/bars` (timeframe `1Day`, default feed `iex`, paginated on `next_page_token`, maximum 30 pages) plus `/v2/calendar` on the paper trading host for session dates.

---

## The Oracle Contract (`Oracle.S`)

A venue that participates in oracle modeling implements a second module type in addition to `S`. Lighter and IBKR do not.

```ocaml
module type S = sig
  val name : string
  val calendar_kind : Types.calendar_kind
  val fetch_bars
    :  ?feed:string
    -> ?end_date:string
    -> from:string option
    -> symbol:string
    -> unit
    -> Types.bar list Lwt.t
  val fetch_calendar : start_date:string -> end_date:string -> string list Lwt.t
  val fetch_fees : testnet:bool -> symbol:string -> (float * float) Lwt.t
  val default_fees : symbol:string -> float * float
  val fetch_balances
    :  testnet:bool
    -> ((string * float * float) list, string) result Lwt.t
  val init_instruments : testnet:bool -> symbols:string list -> unit Lwt.t
  val live_balances : unit -> (string * float * float) list option
  val default_quote : string
  val min_notional : symbol:string -> float
end
```

Implementation notes:

- `fetch_bars` returns raw source rows in any order. The oracle sorts, de-duplicates, and normalizes centrally (`Oracle_calendar.normalize_bars`) on every fetch and cache read, so a source emitting placeholder rows (for example, fabricated pre-listing candles) yields an inactive asset rather than a bad decision. Implementations may pre-clean, since normalization is idempotent.
- `fetch_balances` returns already-normalized `(asset, available, total)` triples (for example `XXBT -> BTC`, `UBTC -> BTC`).
- `live_balances` is the venue's answer to whether its live balance store is equivalent to REST for oracle sizing. Kraken and Alpaca return their stores; Hyperliquid returns `None`.
- `calendar_kind` drives gap detection: crypto gaps are missing calendar days; equity gaps are weekdays missing from the venue calendar (holidays).
- `default_quote` is used when a symbol is written without an explicit quote (`BTC` -> `USDC` on Hyperliquid, `USD` on Kraken/Alpaca).

Three reference adapters exist: `kraken_oracle.ml`, `hyperliquid_oracle.ml`, and `alpaca_oracle.ml`. The engine's fetch pipeline (`src/engine/oracle/oracle_fetch.ml`) dispatches through `Exchange_intf.Oracle.Registry`; there is no hardcoded venue dispatch.

---

## Transport and Data Sources

| Data | Kraken | Hyperliquid | Alpaca |
| --- | --- | --- | --- |
| Order book | WS (public) | WS (public) | WS (public) |
| Execution feed | WS (auth) | WS (auth) | SSE (`/v2/events/trades`) |
| Balance / positions | WS (auth store) + REST earn poll | WS store (live) / REST spot (oracle) | REST poll (2 s) |
| Fees | REST `TradeVolume` + `default_fees` | REST `userFees` + `default_fees` | static `default_fees` |
| Daily bars (oracle) | REST OHLC | REST `candleSnapshot` | REST bars + calendar |
| Deep history | Yahoo | Yahoo | Yahoo |

Lighter is REST plus WebSocket, signed locally and optionally relayed through the proxy pool; IBKR is a TCP gateway connection with polled/streamed messages. Neither has oracle data.

---

## Concurrency Patterns

These patterns recur across the implementations and are worth copying for a new venue.

- **Per-symbol stores with double-checked locking.** `get_symbol_store` creates a store under `initialization_mutex` on first use, then returns it lock-free on later calls. Examples: `kraken_executions_feed.ml`, `hyperliquid_executions_feed.ml`, `ibkr_executions_feed.ml`.
- **A separate mutex for the cross-symbol order index.** The name varies by venue: `global_orders_mutex` on Kraken, `order_index_mutex` on Hyperliquid, `global_mutex` on IBKR, and `initialization_mutex` itself on Lighter.
- **`order_to_symbol` hashtable plus bounded FIFO queue.** An execution feed must route an order id back to its symbol, which is unknown at fill time on some venues. The index is uncapped during startup ingestion, then locked to an adaptive cap; once capped, the oldest id is evicted first, with a hash/queue divergence guard. Present in Kraken, Hyperliquid, and Lighter.
- **Atomic one-shot guards.** `Atomic.make false` plus `Atomic.exchange` runs feed initialization exactly once.
- **Fast-path closures.** Every `_fast` function is a plain read of an `Atomic.t` or a mutex-guarded store; nothing blocks.
- **Inline hot paths.** Order-book read paths are annotated `[@inline always]` and are kept allocation-free.
- **Supervisor-owned reconnection.** WebSocket feeds do not self-heal; the supervisor reconnects them with exponential backoff (`0/2/4/...` capped at 30 s, or 300 s for `ibkr_gateway` and `lighter_ws`) in `src/engine/supervisor/supervisor_health.ml`. Lighter additionally runs its own fixed 0.5 s reconnect loop. `Error_handling.retry_with_backoff` is used on REST and order-submission paths, not for feed reconnection.
- **Stale-order cleanup.** `hyperliquid_executions_feed.ml` prunes orders older than 24 hours, amendment-blacklist entries older than 30 seconds, and processed trade IDs older than 10 minutes (per-symbol cap 256), so the store cannot grow without bound.

---

## Adding a New Exchange

1. Create `src/external/<name>/` with a library. Follow the newer wrapper convention `<Name>.Module` (for example `Hyperliquid.Module.Hyperliquid_impl`); Kraken is the older `<Name>.<name>_module` form. Give the library a `dio.<name>` public name.
2. Implement `Exchange_intf.S` in `<name>_module.ml`, using `<name>_actions.ml` for REST, `<name>_executions_feed.ml` / `<name>_orderbook_feed.ml` for feeds, and `<name>_balances.ml` when the venue has a live balance store.
3. Register at module load: `let () = Exchange.Registry.register (module Foo_impl)`, where `Exchange` is the local alias for `Dio_exchange.Exchange_intf`.
4. If the venue can serve historical daily bars, add `Foo_oracle.ml` implementing `Oracle.S` and register it from the venue module with `Exchange.Oracle.Registry.register (module Foo_oracle)`. Reference the venue from `bin/main.ml` (and `bin/oracle.ml` for oracle-capable venues) so linking pulls in the registration.
5. Add the venue name to `exchange_of_string` / `string_of_exchange` in `exchange_intf.ml`, and add restricted-key validation in `src/engine/config.ml`. Add the library to the appropriate dune dependencies (`src/engine/dune` and/or `src/engine/supervisor/dune`).
6. Wire the venue into the supervisor feeds (`src/engine/supervisor/supervisor_feeds.ml`) and the domain spawner (`src/engine/domain_spawner.ml`).
7. Add adapter tests under `test/external/<venue>/` (see `test_kraken_oracle.ml`, `test_hyperliquid_oracle.ml`, `test_alpaca_oracle.ml` for the fixture pattern). The `test/engine/oracle/` suite covers oracle-core behavior, not per-venue adapters.
