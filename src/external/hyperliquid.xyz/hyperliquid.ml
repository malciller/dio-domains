(** Top-level namespace for the Hyperliquid exchange integration.
    Re-exports all submodules: authentication, WebSocket connectivity,
    order actions, market data feeds, fee queries, lifecycle management,
    and shared types. *)

(** EIP-712 typed-data signing for Hyperliquid L1 actions. *)
module Signer = Hyperliquid_signer

(** WebSocket client for authenticated and public Hyperliquid channels. *)
module Ws = Hyperliquid_ws

(** Order placement, cancellation, and modification via the exchange API. *)
module Actions = Hyperliquid_actions

(** Balance and margin state queries. *)
module Balances = Hyperliquid_balances

(** L2 orderbook snapshot and incremental update feed. *)
module Orderbook_feed = Hyperliquid_orderbook_feed

(** Fill and execution notification feed. *)
module Executions_feed = Hyperliquid_executions_feed

(** Tradable instrument metadata feed. *)
module Instruments_feed = Hyperliquid_instruments_feed

(** Fee schedule and rate queries. *)
module Get_fee = Hyperliquid_get_fee

(** Exchange module lifecycle management (init, start, stop). *)
module Module = Hyperliquid_module

(** Shared type definitions for Hyperliquid domain objects. *)
module Types = Hyperliquid_types

(** Oracle data-venue adapter (historical candles, fees, spot balances,
    instruments for the capital oracle; implements
    [Exchange_intf.Oracle.S]). *)
module Hyperliquid_oracle = Hyperliquid_oracle
