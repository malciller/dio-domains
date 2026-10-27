(** Root namespace for the Lighter exchange integration: reexports the client submodules
    covering signing, WebSocket connectivity, order actions, market data feeds, balances,
    domain types, and startup/shutdown orchestration. *)

(** EdDSA signatures over BabyJubJub, computed through FFI calls into a precompiled shared
    library. *)
module Signer = Lighter_signer

(** Asynchronous WebSocket client holding separate public market data and authenticated
    private connections to Lighter. *)
module Ws = Lighter_ws

(** Order submission, cancellation, and modification as signed transactions, sent over the
    private WebSocket or REST fallback. *)
module Actions = Lighter_actions

(** Ordered, non-blocking submission queue: one writer signs and sends txs in FIFO order
    so nonces reach the exchange monotonically without serialising callers on a
    round-trip. *)
module Outbox = Lighter_outbox

(** Account balance tracking: aggregates collateral updates from WS channels. *)
module Balances = Lighter_balances

(** Level 2 feed: builds local depth from an initial snapshot plus incremental deltas. *)
module Orderbook_feed = Lighter_orderbook_feed

(** Execution reports and order status transitions (fills, partial fills, cancels)
    consumed from the private WS channel. *)
module Executions_feed = Lighter_executions_feed

(** Market metadata: contract specs, precision parameters, symbol/market index mappings. *)
module Instruments_feed = Lighter_instruments_feed

(** Perp position and margin snapshots from the account channels. *)
module Positions = Lighter_positions

(** Funding, mark, index, and premium snapshots from [market_stats]. *)
module Market_stats = Lighter_market_stats

(** Optional engine-facing market-context capability. *)
module Market_context = Lighter_market_context

(** Oracle data-venue adapter (candles, fees, balances, instruments); implements
    [Exchange_intf.Oracle.S]. *)
module Oracle = Lighter_oracle

(** Optional engine-facing quote reservation capability. *)
module Capital_reservation = Lighter_capital_reservation

(** Lifecycle controller: initialization, startup, and shutdown of the connectivity
    subcomponents. *)
module Module = Lighter_module

(** Core domain types for the Lighter API interactions. *)
module Types = Lighter_types

(** Background task renewing resting orders before their exchange-side expiry, emulating
    GTC semantics. *)
module Tif_renewal = Lighter_tif_renewal
