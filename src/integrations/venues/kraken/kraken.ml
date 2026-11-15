(** Root namespace for the Kraken venue integration.

    The root holds the connector surface, and the two account-tier workflows live in
    subdirectories:

    - {!Kraken_account_tier} — which workflow the account is entitled to (standard =
      WebSocket, enterprise = FIX). The only policy in the venue.
    - {!Kraken_order_transport} — the seam every order path goes through. Both workflows
      register an implementation of the same record, so the dimension adapter and the
      execution feed never branch on the tier themselves.
    - [retail/] — the v2 WebSocket workflow: JSON feeds on one authenticated socket, REST
      clients, snapshot-driven order state.
    - [enterprise/] — the Unified FIX workflow: a dedicated sequenced session, its own
      codec, execution reports, and order entry. Its modules keep the [Kraken_fix_] prefix
      because the protocol is what the code speaks; the directory is named for the tier
      that can use it.
    - {!Kraken_common_types} — the wire-level result and envelope types both return.

    The retail modules keep their historical names, so the supervisor, the dashboard and
    the tests are unaffected by the split. *)

(** Wire-level result and response envelopes shared by both workflows. *)
module Kraken_common_types = Kraken_common_types

(** Account tier: selects the order-entry workflow. *)
module Kraken_account_tier = Kraken_account_tier

(** Order-entry transport seam. *)
module Kraken_order_transport = Kraken_order_transport

(** Unified FIX 4.4 workflow: session, codec, execution reports, order entry. *)
module Kraken_fix = Kraken_fix

(** --- WebSocket (retail) workflow --- *)

(** Defines the set of exchange actions (order placement, cancellation) dispatched to the
    retail WebSocket workflow. *)
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

(** REST client for fetching all open orders (bypasses the WS snapshot cap). *)
module Kraken_open_orders = Kraken_open_orders

(** WebSocket client for submitting and managing orders via the Kraken trading channel. *)
module Kraken_trading_client = Kraken_trading_client

(** Top-level lifecycle manager that initializes connections, authenticates, and
    orchestrates all Kraken feeds. *)
module Kraken_module = Kraken_module

(** Oracle data-venue adapter (historical bars, fees, balances, instruments for the
    capital oracle; implements [Exchange_intf.Oracle.S]). *)
module Kraken_oracle = Kraken_oracle
