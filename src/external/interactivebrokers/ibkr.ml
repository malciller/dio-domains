(** Root namespace for the Interactive Brokers Trader Workstation API integration.

    This module serves as the primary entry point and unifying interface for the external 
    Interactive Brokers implementation. It encapsulates the core capabilities required to 
    interface with the Trader Workstation and Gateway applications by re-exporting the 
    underlying specialized modules.

    The exported modules provide the following functional boundaries:
    * Types: Core data structures representing orders, contracts, and market states.
    * Codec: Wire protocol serialization and deserialization mechanisms.
    * Connection: Transmission Control Protocol socket management and connection lifecycle handling.
    * Dispatcher: Asynchronous message routing and request correlation components.
    * Contracts: Instrument metadata and structural definitions for financial products.
    * Orderbook_feed: Level 2 market data subscription and order book state management.
    * Executions_feed: Trade execution tracking and order status monitoring.
    * Balances: Account collateral state tracking and risk management data.
    * Actions: Core business logic for routing orders and modifying market positions.
    * Module: Dependency injection and trading engine integration components.
    * Market_hours: Session state calculations and market schedule management. *)

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
