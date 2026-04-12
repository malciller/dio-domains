(** Root namespace module for the Lighter decentralized exchange integration.
    This module centralizes and reexports the constituent submodules that collectively
    provide the API client functionality. The integration encompasses cryptographic
    authentication, persistent WebSocket connectivity, order lifecycle management,
    real time market data ingress, shared domain types, and component lifecycle coordination. *)

(** Cryptographic signature module utilizing EdDSA over the BabyJubJub elliptic curve.
    Signatures are computed via Foreign Function Interface calls to a precompiled
    shared library to handle the computationally intensive cryptographic operations. *)
module Signer = Lighter_signer

(** Asynchronous WebSocket client facilitating bidirectional communication with the
    Lighter exchange infrastructure. It manages multiplexed connections for both
    public market data streams and authenticated, private execution channels. *)
module Ws = Lighter_ws

(** Order execution interface covering order submission, cancellation, and modification.
    These operations are transmitted as cryptographically signed payloads over either
    the prioritized WebSocket connection or the fallback REST API endpoints. *)
module Actions = Lighter_actions

(** Real time account balance tracking and reconciliation. The submodule aggregates
    collateral updates from WebSocket channels to maintain an accurate internal ledger. *)
module Balances = Lighter_balances



(** Level 2 market data feed handler. Constructs and maintains a local cache of the
    orderbook depth by processing initial snapshots followed by sequential delta updates. *)
module Orderbook_feed = Lighter_orderbook_feed

(** Asynchronous feed for execution reports and order status transitions. Processes
    incoming WebSocket messages to update the system regarding fills, partial fills,
    and order cancellations. *)
module Executions_feed = Lighter_executions_feed

(** Static and dynamic metadata feed for tradable assets on the exchange. Retrieves
    contract specifications, precision parameters, and symbol to index mappings. *)
module Instruments_feed = Lighter_instruments_feed

(** Primary lifecycle controller for the Lighter exchange integration. Orchestrates
    the initialization, startup sequences, and graceful shutdown procedures for all
    concurrent exchange connectivity subcomponents. *)
module Module = Lighter_module

(** Central repository for core domain types, record structures, and variant definitions
    specific to the Lighter API interactions. Ensures type safety across the integration. *)
module Types = Lighter_types

(** Background concurrency task responsible for order Time In Force management. It
    simulates Good Till Cancelled semantics by systematically renewing resting orders
    before their exchange native expiration intervals elapse. *)
module Tif_renewal = Lighter_tif_renewal
