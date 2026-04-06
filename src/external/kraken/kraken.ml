(**
   Root namespace for the Kraken exchange integration.
   Re-exports all submodules that implement WebSocket feed subscriptions,
   REST API clients, authentication, and shared type definitions.
*)

(** Shared type definitions and JSON codec utilities used across all Kraken submodules. *)
module Kraken_common_types = Kraken_common_types

(** Defines the set of exchange actions (order placement, cancellation) dispatched to Kraken. *)
module Kraken_actions = Kraken_actions

(** WebSocket feed subscription for real-time account balance updates. *)
module Kraken_balances_feed = Kraken_balances_feed

(** WebSocket feed subscription for real-time order execution reports. *)
module Kraken_executions_feed = Kraken_executions_feed

(** REST client for generating short-lived WebSocket authentication tokens. *)
module Kraken_generate_auth_token = Kraken_generate_auth_token

(** REST client for retrieving the current maker/taker fee schedule. *)
module Kraken_get_fee = Kraken_get_fee

(** WebSocket feed subscription for tradable instrument metadata and status. *)
module Kraken_instruments_feed = Kraken_instruments_feed

(** WebSocket feed subscription for L2 order book snapshots and incremental updates. *)
module Kraken_orderbook_feed = Kraken_orderbook_feed

(** WebSocket feed subscription for real-time ticker (best bid/ask, last trade) data. *)
module Kraken_ticker_feed = Kraken_ticker_feed

(** WebSocket client for submitting and managing orders via the Kraken trading channel. *)
module Kraken_trading_client = Kraken_trading_client

(** Top-level lifecycle manager that initializes connections, authenticates, and orchestrates all Kraken feeds. *)
module Kraken_module = Kraken_module

