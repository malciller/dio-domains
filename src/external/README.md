# DIO Exchange Integration (`dio.exchange`)

This library defines the interface every exchange must implement for the engine to trade on it. It contains the shared interface (`exchange_intf.ml`), the venue implementations, and the shared API clients (Yahoo, CoinMarketCap, Discord). It has no dependency on the engine: the engine consumes `dio.exchange`, never the reverse.

Library layout:

```
src/external/
  dune                     (library dio_exchange, public name dio.exchange)
  exchange_intf.ml         (types, module type S, registries)
  kraken/                  (Kraken spot)
  hyperliquid.xyz/         (Hyperliquid spot + perps)
  lighter.xyz/             (Lighter perps)
  interactivebrokers/      (IBKR US equities)
  alpaca/                  (Alpaca US equities)
  yahoo/                   (deep-history client, dio.yahoo)
  coinmarketcap/           (Fear-and-Greed client, dio.cmc)
  discord/                 (Discord webhook notifier)
```

---

## The Interface (`Exchange_intf.S`)

Every exchange implements one module type. The engine reaches exchanges only through it. The signature in `exchange_intf.ml` is the source of truth; the highlights below are accurate as of the current tree.

### Core data types

- `order_type`: `Limit`, `Market`, `StopLoss`, `TakeProfit`, `StopLossLimit`, `TakeProfitLimit`, `SettlPosition`, `Other`.
- `time_in_force`: `GTC`, `IOC`, `FOK`.
- `exchange_id`: `Hyperliquid`, `Kraken`, `Lighter`, `Ibkr`, `Alpaca`, `Custom`.
- `venue_config`: `{ testnet : bool; symbol : string; quote_currency : string }`.
- `order_req`: a place-order request. Required fields: `symbol`, `token`, `order_type`, `side`, `qty`. Optional: `limit_price`, `time_in_force`, `post_only`, `reduce_only`, `order_userref`, `cl_ord_id`, `trigger_price`, `display_qty`, `retry_config`.
- `amend_order`: `token:string -> order_id:string -> ?cl_ord_id -> ?qty -> ?limit_price -> ?post_only -> ?trigger_price -> ?display_qty -> ?symbol -> ?retry_config -> unit -> (amend_order_result, string) result Lwt.t`. All mutable order fields are optional; amend only what changes.
- `retry_config` (from `error_handling.ml`, re-exported): `{ max_attempts; base_delay_ms; max_delay_ms; backoff_factor }`.

### Query functions

The interface no longer has a single `get_balance`. Balance queries come in several forms:

| Function | Purpose |
| --- | --- |
| `get_tradeable_balance` | The balance that can be used for new orders (free balance). |
| `get_total_balance` | Total balance including open-order reserves. |
| `get_tradeable_balance_fast` | Non-blocking cached read (see fast paths below). |
| `get_balance_age_fast` | Seconds since the balance store was last refreshed, or `None` when the exchange does not track freshness. |
| `get_all_orders_for_asset` | Full order list for one symbol. |
| `fold_open_orders` | Fold over all open orders for one symbol. |
| `iter_open_orders_fast` | Non-blocking iteration over the locally cached open-order set. |
| `subscribe_orderbook` | Subscribe the venue websocket to order book updates for a set of symbols. |
| `get_orderbook_position_fast` | Index of the best bid/ask in the locally cached book. |
| `get_top_of_book_fast` | `(bid_price, bid_size, ask_price, ask_size) option` from the cached book. |
| `get_execution_feed_position_fast` | Execution-feed position used to detect missed events across restarts. |
| `has_execution_data_fast` | Whether the execution store has data for a symbol yet. |

### Feeds

- `subscribe_orderbook ~symbols` (above).
- Order book events arrive through `iter_top_of_book_events`, called with `(bid_price, bid_size, ask_price, ask_size)`. The loop runs as a blocking callback on the engine side.
- Execution events (`Order_filled` / `Order_canceled`) arrive through the execution feed.
- The engine consumes order book state through ring buffers sized by `get_orderbook_position_fast`, so a feed can be behind a strategy without stalling it.

### Fees

- `fetch_fees ~testnet ~symbol : (float * float) Lwt.t` returns live `(maker, taker)` fees, e.g. Kraken volume tiers, Hyperliquid user fees.
- `default_fees ~symbol : float * float` returns the venue's built-in fallback.
- `min_notional ~symbol : float` returns the minimum order notional; `0.0` means unconstrained.

### Oracle support

Exchanges that can supply historical data implement the `Oracle.S` module type (see below). Venues that cannot (Lighter, IBKR) are trading-only and are omitted from the oracle registry.

---

## Registration

The engine discovers implementations through two registries. Both live in `exchange_intf.ml` and both are keyed by exchange name:

- `Exchange_intf.Registry` maps `string -> (module S)`.
- `Exchange_intf.Oracle.Registry` maps `string -> (module Oracle.S)`.

Registration happens as a load-time side effect of each venue module. A module referenced anywhere in the binary registers itself:

```ocaml
(* kraken/kraken_module.ml, line ~547 *)
let () = Exchange_intf.Registry.register (module Kraken_impl)
(* and in the same file's oracle adapter: *)
let () = Exchange_intf.Oracle.Registry.register (module Kraken_oracle)
```

Because OCaml dead-code elimination would otherwise drop unreferenced modules, the binaries force-reference every venue explicitly:

```ocaml
(* bin/main.ml and bin/oracle.ml *)
let () = ignore Kraken.Kraken_module.Kraken_impl.name
```

Add the same `ignore` line in `bin/main.ml` (and `bin/oracle.ml` if the venue has an oracle adapter) when adding a new exchange.

---

## Current Implementations

### Kraken (`kraken/`)

- Live and public REST (`/0/public`, `/0/private`) + websockets.
- Balance via the authenticated websocket store (`get_tradeable_balance_fast` reads the store).
- `default_fees`: `0.0016` maker / `0.0026` taker; live fees via the TradeVolume endpoint.
- `default_quote`: `USD`. `min_notional`: `0.0`.
- Oracle adapter: public OHLC history (daily interval), paginated on the `since` cursor back to pair inception, bounded at 60 pages.

### Hyperliquid (`hyperliquid.xyz/`)

- Spot + perpetuals, EIP-712 signed requests.
- Balance via REST (`spotClearinghouseState`); the live store is deliberately not used for oracle balance because it merges perp and spot USDC.
- `default_fees`: spot symbols (containing `/`) `0.0` maker / `0.001` taker; perps `0.0002` / `0.0005`. Live fees via `userFees`.
- `default_quote`: `USDC`. `min_notional`: `10.0` for spot symbols containing `/`, else `0.0`.
- Oracle adapter: `candleSnapshot`, resolving spot vs perp through `spotMeta`; spot-named symbols with no matching pair get no bars (inactive) rather than silent perp substitution.
- `order_to_symbol`: the executions feed maintains an order-id-to-symbol hashtable (with a bounded queue) so fills can be routed even when the venue does not return the symbol.

### Lighter (`lighter.xyz/`)

- Perp DEX, signed by the native Lighter signer library.
- `LIGHTER_SIGNER_LIB_PATH` must point at the shared library. Requests are signed locally and sent either directly or through a relay proxy (`LIGHTER_PROXY_URL`, comma-separated pool).
- Orders are time-limited (about 28 days) and a renewal daemon cancel-and-replaces them to approximate GTC.
- `min_notional`: `0.0`. No oracle adapter (trading-only).
- Account selection via `LIGHTER_API_KEY_INDEX` / `LIGHTER_ACCOUNT_INDEX`.

### Interactive Brokers (`interactivebrokers/`)

- TCP connection to an IB Gateway (port 4002 paper, 4001 live).
- Live trading is limit-only; quantities are floored to whole shares.
- Executions arrive as gateway messages (`orderStatus`, `openOrder`, `execDetails`), tracked in a per-symbol store like the other venues.
- No oracle adapter (trading-only).

### Alpaca (`alpaca/`)

- US equities, paper or live, `iex` or `sip` data feed.
- Balance and positions via websocket stores (`get_tradeable_balance_fast` reads the store).
- Respects extended hours (4 AM to 8 PM ET) and overnight sessions; `day` TIF with extended-hours flag.
- `default_fees`: `0.0` / `0.0`. `default_quote`: `USD`. `min_notional`: `1.0`.
- Oracle adapter: `/v2/stocks/{symbol}/bars` (IEX/SIP) + `/v2/calendar` for session dates.

---

## The Oracle Contract (`Oracle.S`)

```ocaml
module type S = sig
  val name : string                       (* human-readable venue name, also the Registry key *)
  val calendar_kind : Types.calendar_kind (* Crypto (session = day) or Equity (market sessions) *)
  val fetch_bars
    :  ?feed:string                      (* Alpaca IEX/SIP; others ignore *)
    -> ?end_date:string                  (* Alpaca-only request-window end *)
    -> from:string option                (* first day to include; None = full history *)
    -> symbol:string
    -> unit
    -> Types.bar list Lwt.t
  val fetch_calendar : start_date:string -> end_date:string -> string list Lwt.t
  val fetch_fees : testnet:bool -> symbol:string -> (float * float) Lwt.t
  val default_fees : symbol:string -> float * float
  val fetch_balances : testnet:bool -> ((string * float * float) list, string) result Lwt.t
  val init_instruments : testnet:bool -> symbols:string list -> unit Lwt.t
  val live_balances : unit -> (string * float * float) list option
  val default_quote : string
  val min_notional : symbol:string -> float
end
```

Implementation notes:

- `fetch_bars` must return clean, ascending daily bars with real volume. The oracle normalizes everything (`Oracle_calendar.normalize_bars`) on every fetch and cache read, so a source emitting placeholder rows (e.g. fabricated pre-listing candles) produces an inactive asset, never a garbage decision.
- `live_balances` is the venue's answer to "is your websocket-fed balance store equivalent to REST for oracle sizing?" Kraken and Alpaca return their stores; Hyperliquid returns `None` (REST spot balance is authoritative).
- `calendar_kind` drives gap detection: crypto gaps are missing calendar days; equity gaps are weekdays missing from the venue calendar (holidays).

Three reference adapters exist: `kraken_oracle.ml`, `hyperliquid_oracle.ml`, `alpaca_oracle.ml`. The shared fetch pipeline (`oracle_fetch.ml` in the engine) dispatches purely through `Exchange_intf.Oracle.Registry`; there is no hardcoded venue dispatch. Deep-history extension runs through the Yahoo client with a per-symbol whitelist.

---

## WebSocket vs REST

| Data | Kraken | Hyperliquid | Alpaca |
| --- | --- | --- | --- |
| Order book | WS (public) | WS (public) | WS (public) |
| Execution feed | WS (auth) | WS (auth) | WS (auth) |
| Balance / positions | WS (auth store) | REST | WS (auth store) |
| Fees | REST + `default_fees` | REST + `default_fees` | static `default_fees` |
| Daily bars (oracle) | REST OHLC | REST candleSnapshot | REST bars + calendar |
| Deep history | Yahoo | Yahoo | Yahoo |

Lighter and IBKR are REST/WS hybrids without oracle data: Lighter streams its own WS events and the IBKR gateway is polled.

---

## Concurrency Patterns

These patterns appear across the implementations and are worth copying when writing a new one.

- **Per-symbol stores with double-checked locking.** `get_symbol_store` creates a store under `initialization_mutex` the first time a symbol is seen, then returns it lock-free. This is the pattern in `kraken_executions_feed.ml` and `hyperliquid_executions_feed.ml`. A separate `global_orders_mutex` protects the cross-symbol open-order index.
- **`order_to_symbol` hashtable + bounded queue.** Execution feeds must route an order-id back to its symbol; the symbol is unknown at fill time on some venues. A hashtable with a FIFO queue and an adaptive cap evicts oldest entries under pressure (`lighter_executions_feed.ml`).
- **Atomic flags for one-time startup.** `Atomic.make false` + `Atomic.exchange` guards one-shot feed initialization so only the first caller runs it.
- **Fast-path closures.** Every `_fast` function is a plain read of an `Atomic.t` or mutex-guarded store; nothing blocking.
- **Inline hot paths.** Order book read paths are annotated `[@inline always]`; keep the read side allocation-free.
- **Self-restarting stream processors.** Feed loops catch their own exceptions and re-subscribe with backoff (from `error_handling.ml`), so a dropped websocket heals without engine involvement.
- **Stale-order cleanup.** `hyperliquid_executions_feed.ml` prunes orders older than 24 hours, expired amendment blacklist entries after 30 seconds, and processed trade IDs after 10 minutes, so the store cannot grow without bound.

---

## Adding a New Exchange

1. Create `src/external/<name>/` with a library (`dune`) exposing a module, e.g. `Foo.Foo_module`.
2. Implement `Exchange_intf.S` in `<name>_module.ml`, using `<name>_actions.ml` for REST calls, `<name>_executions_feed.ml` / `<name>_orderbook_feed.ml` for websocket feeds, and `<name>_balances.ml` when the venue has a live balance store.
3. Register in `Exchange_intf.Registry` at module load: `let () = Exchange_intf.Registry.register (module Foo_impl)`.
4. If the venue can serve historical bars, add an oracle adapter (`Foo_oracle.ml`) implementing `Oracle.S`, register it in `Exchange_intf.Oracle.Registry`, and reference both modules in `bin/main.ml` (and `bin/oracle.ml`).
5. Add the library to the engine's `dune` dependencies and add the venue to `Config.exchange` validation.
6. Wire the venue into the supervisor connection registry and `domain_spawner.ml` so domains can be spawned for it.
7. Add tests under `test/`; the oracle fixture tests (`test/engine/oracle/`) are a good template for adapter correctness.
