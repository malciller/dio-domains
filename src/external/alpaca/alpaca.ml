(** Root namespace for the Alpaca Markets exchange integration. *)

module Types = Alpaca_types
module Rest = Alpaca_rest
module Orderbook = Alpaca_orderbook
module Executions = Alpaca_executions
module Balances = Alpaca_balances
module Module = Alpaca_module
module Market_hours = Alpaca_market_hours

(** Oracle data-venue adapter (historical bars, market calendar, balances,
    fees for the capital oracle; implements [Exchange_intf.Oracle.S]). *)
module Alpaca_oracle = Alpaca_oracle
