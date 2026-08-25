(** Interactive Brokers TWS API integration: root namespace re-exporting
    the specialized modules.

    * Types: orders, contracts, market state.
    * Codec: wire format encode/decode.
    * Connection: TCP socket and connection lifecycle.
    * Dispatcher: message routing and request correlation.
    * Contracts: symbol-to-contract resolution.
    * Orderbook_feed: depth subscription and order book snapshots.
    * Executions_feed: order status and fill tracking.
    * Balances: account balances and positions.
    * Actions: order placement, modification, cancellation.
    * Module: [Exchange_intf.S] adapter and engine integration.
    * Market_hours: US equity session state calculations. *)

module Types = Ibkr_types
module Codec = Ibkr_codec
module Connection = Ibkr_connection
module Dispatcher = Ibkr_dispatcher
module Contracts = Ibkr_contracts
module Orderbook_feed = Ibkr_orderbook_feed
module Executions_feed = Ibkr_executions_feed
module Balances = Ibkr_balances
module Actions = Ibkr_actions
module Module = Ibkr_module
module Market_hours = Ibkr_market_hours
