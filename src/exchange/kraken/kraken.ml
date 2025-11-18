(** Main Kraken exchange module - includes all submodules *)
(* TODO: Inconsistent naming convention - mix of snake_case and PascalCase module names *)
(* TODO: Modules should be grouped by functionality (e.g., feeds, actions, utilities) *)
(* TODO: Add documentation for each module's purpose and dependencies *)
(* TODO: Consider adding API version information and stability guarantees *)
(* TODO: Module lacks initialization/setup functions - consider if this is appropriate for a library module *)

module Kraken_common_types = Kraken_common_types
(* TODO: Rename to kraken_common_types for consistency *)
module Kraken_actions = Kraken_actions
module Kraken_balances_feed = Kraken_balances_feed
module Kraken_executions_feed = Kraken_executions_feed
(* TODO: Rename to kraken_generate_auth_token for consistency *)
module Kraken_generate_auth_token = Kraken_generate_auth_token
(* TODO: Rename to kraken_get_fee for consistency *)
module Kraken_get_fee = Kraken_get_fee
module Kraken_instruments_feed = Kraken_instruments_feed
module Kraken_orderbook_feed = Kraken_orderbook_feed
module Kraken_ticker_feed = Kraken_ticker_feed
(* TODO: Rename to kraken_trading_client for consistency *)
module Kraken_trading_client = Kraken_trading_client
