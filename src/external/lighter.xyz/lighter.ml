(** Root namespace for the Lighter exchange integration: reexports the client
    submodules covering signing, WebSocket connectivity, order actions, market
    data feeds, balances, domain types, and startup/shutdown orchestration. *)

(** EdDSA signatures over BabyJubJub, computed through FFI calls into a
    precompiled shared library. *)
module Signer = Lighter_signer

(** Asynchronous WebSocket client holding separate public market data and
    authenticated private connections to Lighter. *)
module Ws = Lighter_ws

(** Order submission, cancellation, and modification as signed transactions,
    sent over the private WebSocket or REST fallback. *)
module Actions = Lighter_actions

(** Account balance tracking: aggregates collateral updates from WS channels. *)
module Balances = Lighter_balances

(** Level 2 feed: builds local depth from an initial snapshot plus incremental
    deltas. *)
module Orderbook_feed = Lighter_orderbook_feed

(** Execution reports and order status transitions (fills, partial fills,
    cancels) consumed from the private WS channel. *)
module Executions_feed = Lighter_executions_feed

(** Market metadata: contract specs, precision parameters, symbol/market index
    mappings. *)
module Instruments_feed = Lighter_instruments_feed

(** Lifecycle controller: initialization, startup, and shutdown of the
    connectivity subcomponents. *)
module Module = Lighter_module

(** Core domain types for the Lighter API interactions. *)
module Types = Lighter_types

(** Background task renewing resting orders before their exchange-side expiry,
    emulating GTC semantics. *)
module Tif_renewal = Lighter_tif_renewal
