(** Foundational type definitions and protocol constants for the Interactive Brokers TWS API integration.

    This module establishes the core data structures including trading contracts, order specifications, execution reports, and market data tick types. These definitions provide a unified type system utilized across the codec, connection management, data feeds, and action execution subsystems to ensure consistent protocol mapping. *)

let section = "ibkr_types"

(* Subsystem configuration defaults *)

(** Default queue capacity for the market data ticker Lwt stream. *)
let default_ring_buffer_size_ticker = 100

(** Default queue capacity for the Level 2 market depth Lwt stream. *)
let default_ring_buffer_size_orderbook = 16

(** Default queue capacity for the order execution report Lwt stream. *)
let default_ring_buffer_size_executions = 32

(** Maximum elapsed duration in seconds before an untracked open order is evicted from local state management. *)
let default_stale_order_threshold_s = 86400.0    (* 24 hours *)

(** Default magnitude of granular price levels to maintain symmetrically for order book state construction. *)
let default_orderbook_depth = 5

(** Base temporal delay in milliseconds applied prior to initiating a subsequent TCP socket reconnection attempt. *)
let default_reconnect_base_delay_ms = 1000.0

(** Upper bound constraint in milliseconds for the exponential backoff calculation during sustained network severance. *)
let default_reconnect_max_delay_ms = 30000.0

(** Multiplicative coefficient applied to the reconnection interval following consecutive socket initialization failures. *)
let default_reconnect_backoff_factor = 2.0

(* Protocol version specifications *)

(** Floor protocol version required to ensure compatibility with modern TWS Gateway message framing architectures. *)
let api_version_min = 100

(** Ceiling protocol version specifying the maximum structural schema the internal codec is equipped to deserialize. *)
let api_version_max = 176

(** Numeric identifier allocating the TCP session endpoint. Value zero designates the primary administrative connection capable of intercepting global execution events. *)
let default_client_id = 0

(* Client egress protocol message identifiers *)

let msg_req_mkt_data        = 1
let msg_cancel_mkt_data     = 2
let msg_place_order         = 3
let msg_cancel_order        = 4
let msg_req_open_orders     = 5
let msg_req_account_updates = 6
let msg_req_executions      = 7
let msg_req_ids             = 8
let msg_req_contract_details = 9
let msg_req_mkt_depth       = 10
let msg_cancel_mkt_depth    = 11
let msg_req_positions       = 61
let msg_req_market_data_type = 59
let msg_start_api           = 71

(* Gateway ingress protocol message identifiers *)

let msg_in_tick_price          = 1
let msg_in_tick_size           = 2
let msg_in_order_status        = 3
let msg_in_error               = 4
let msg_in_open_order          = 5
let msg_in_account_value       = 6
let msg_in_portfolio_value     = 7
let msg_in_account_update_time = 8
let msg_in_next_valid_id       = 9
let msg_in_contract_data       = 10
let msg_in_execution_data      = 11
let msg_in_market_depth        = 12
let msg_in_market_depth_l2     = 13
let msg_in_managed_accounts    = 15
let msg_in_contract_data_end   = 52
let msg_in_open_order_end      = 53
let msg_in_account_download_end = 54
let msg_in_execution_data_end  = 55
let msg_in_position_data       = 61
let msg_in_position_end        = 62
let msg_in_tick_string         = 46
let msg_in_tick_generic        = 45
let msg_in_market_data_type    = 58

(* Market data tick type enumerations *)

(** Realtime tick identifier constants mapping protocol integer codes to explicit market data field updates. *)
let tick_bid       = 1
let tick_ask       = 2
let tick_last      = 4
let tick_bid_size  = 0
let tick_ask_size  = 3
let tick_last_size = 5
let tick_high      = 6
let tick_low       = 7
let tick_volume    = 8
let tick_close     = 9

(* Delayed market data derivations incorporating the standard numerical offset from live equivalents *)
let tick_delayed_bid       = 66
let tick_delayed_ask       = 67
let tick_delayed_last      = 68
let tick_delayed_bid_size  = 69
let tick_delayed_ask_size  = 70
let tick_delayed_volume    = 74

(* Financial instrument contract definitions *)

(** Structural representation of a tradable financial instrument requisite for routing orders and subscribing to data feeds. Exchange Traded Funds are classified under the STK security type. *)
type contract = {
  con_id: int;
  symbol: string;
  sec_type: string;
  exchange: string;
  currency: string;
  local_symbol: string;
  trading_class: string;
  min_tick: float;
  multiplier: string;
}

(** Uninitialized contract instance utilized as a neutral allocation template prior to explicit variable assignment. *)
let empty_contract = {
  con_id = 0;
  symbol = "";
  sec_type = "STK";
  exchange = "SMART";
  currency = "USD";
  local_symbol = "";
  trading_class = "";
  min_tick = 0.01;
  multiplier = "";
}

(** Constructs an elementary equities contract structure instantiated primarily for programmatic symbol resolution operations. *)
let make_stk_contract ~symbol = {
  empty_contract with
  symbol;
  sec_type = "STK";
  exchange = "SMART";
  currency = "USD";
}

(* Execution routing order definitions *)

(** Data structure encapsulation for actionable trading directives transmitted to the broker matching engine. *)
type order = {
  order_id: int;
  action: string;            (** Designates standard directional intent such as BUY or SELL operations. *)
  total_qty: float;
  order_type: string;        (** Execution schema categorization including MKT and LMT variations. *)
  lmt_price: float;
  tif: string;               (** Temporal validity constraint determining order lifespan mechanics. *)
}

(** Instantiates a parameterized market execution directive configured for immediate fulfillment at prevailing collateral rates. *)
let make_market_order ~order_id ~action ~qty = {
  order_id;
  action;
  total_qty = qty;
  order_type = "MKT";
  lmt_price = 0.0;
  tif = "DAY";
}

(** Instantiates a parameterized limit execution directive constrained to fulfill exclusively at or exceeding the specified price bound. *)
let make_limit_order ~order_id ~action ~qty ~price = {
  order_id;
  action;
  total_qty = qty;
  order_type = "LMT";
  lmt_price = price;
  tif = "GTC";
}

(* Post trade execution semantics *)

(** Structural mapping for granular fulfillment events denoting partial or complete clearing of associated order constraints. *)
type execution = {
  exec_id: string;
  exec_order_id: int;
  exec_symbol: string;
  exec_side: string;
  exec_qty: float;
  exec_price: float;
  exec_time: string;
  exec_account: string;
  exec_exchange: string;
}

(* Lifecycle transition state enumerations *)

(** Formalized representations of permissible transition states detailing the real-time operational disposition of a routed order. *)
type tws_order_status =
  | PreSubmitted
  | Submitted
  | Filled
  | Cancelled
  | Inactive
  | ApiPending
  | ApiCancelled
  | Unknown_status of string

(** Lexical parser translating raw string enumerations broadcasted by the protocol gateway into structured variant subtypes. *)
let parse_tws_order_status = function
  | "PreSubmitted" -> PreSubmitted
  | "Submitted" -> Submitted
  | "Filled" -> Filled
  | "Cancelled" -> Cancelled
  | "Inactive" -> Inactive
  | "ApiPending" -> ApiPending
  | "ApiCancelled" -> ApiCancelled
  | s ->
      Logging.warn_f ~section "Unknown TWS order status: %s" s;
      Unknown_status s

(** Normalization function aligning proprietary Interactive Brokers state transitions with the unified generalized exchange protocol interface. *)
let to_exchange_order_status = function
  | PreSubmitted -> Dio_exchange.Exchange_intf.Types.Pending
  | Submitted -> Dio_exchange.Exchange_intf.Types.New
  | Filled -> Dio_exchange.Exchange_intf.Types.Filled
  | Cancelled -> Dio_exchange.Exchange_intf.Types.Canceled
  | Inactive -> Dio_exchange.Exchange_intf.Types.Rejected
  | ApiPending -> Dio_exchange.Exchange_intf.Types.Pending
  | ApiCancelled -> Dio_exchange.Exchange_intf.Types.Canceled
  | Unknown_status s -> Dio_exchange.Exchange_intf.Types.Unknown s
