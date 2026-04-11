(** Top-level namespace for the Lighter exchange integration.
    Re-exports all submodules for authentication, WebSocket connectivity,
    order actions, market data feeds, shared types, and lifecycle management. *)

(** EdDSA/BabyJubJub FFI signer via precompiled shared library. *)
module Signer = Lighter_signer

(** WebSocket client for public and authenticated Lighter channels. *)
module Ws = Lighter_ws

(** Order placement, cancellation, and modification via WS/REST actions. *)
module Actions = Lighter_actions

(** Account balance tracking. *)
module Balances = Lighter_balances

(** Real-time BBO ticker feed. *)
module Ticker_feed = Lighter_ticker_feed

(** L2 orderbook snapshot and delta feed. *)
module Orderbook_feed = Lighter_orderbook_feed

(** Order lifecycle and fill event feed. *)
module Executions_feed = Lighter_executions_feed

(** Tradable instrument metadata feed. *)
module Instruments_feed = Lighter_instruments_feed

(** Exchange module lifecycle management (init, start, stop). *)
module Module = Lighter_module

(** Shared type definitions for Lighter domain objects. *)
module Types = Lighter_types

(** Order TIF renewal background process (simulated GTC). *)
module Tif_renewal = Lighter_tif_renewal
