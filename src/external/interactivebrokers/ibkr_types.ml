(** Shared types for the IBKR TWS API integration.

    Defines contract, order, execution, and tick types used uniformly
    across all IBKR subsystems (codec, connection, feeds, actions). *)

let section = "ibkr_types"

(* Feed configuration defaults *)

(** Ring buffer capacity for the ticker feed channel. *)
let default_ring_buffer_size_ticker = 100

(** Ring buffer capacity for the orderbook feed channel. *)
let default_ring_buffer_size_orderbook = 16

(** Ring buffer capacity for the executions feed channel. *)
let default_ring_buffer_size_executions = 32

(** Maximum age (seconds) before a stale open order is cleaned up. *)
let default_stale_order_threshold_s = 86400.0    (* 24h *)

(** Default number of order book levels to maintain per side. *)
let default_orderbook_depth = 5

(** Default reconnection delay between retry attempts (milliseconds). *)
let default_reconnect_base_delay_ms = 1000.0

(** Maximum reconnection delay (milliseconds). *)
let default_reconnect_max_delay_ms = 30000.0

(** Backoff multiplier applied after each failed reconnection attempt. *)
let default_reconnect_backoff_factor = 2.0

(* ---- TWS API constants ---- *)

(** Minimum API version we request during the handshake. *)
let api_version_min = 100

(** Maximum API version we support. *)
let api_version_max = 176

(** Client ID used for our API connection. Zero is conventionally
    the "master" client. *)
let default_client_id = 0

(* ---- TWS outbound message IDs (Client -> Gateway) ---- *)

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

(* ---- TWS inbound message IDs (Gateway -> Client) ---- *)

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

(* ---- Tick type IDs ---- *)

(** Tick type constants used in tickPrice / tickSize callbacks. *)
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

(* Delayed tick types (offset +65 from live) *)
let tick_delayed_bid       = 66
let tick_delayed_ask       = 67
let tick_delayed_last      = 68
let tick_delayed_bid_size  = 69
let tick_delayed_ask_size  = 70
let tick_delayed_volume    = 74

(* ---- Contract type ---- *)

(** TWS contract specification. ETFs use [sec_type = "STK"]. *)
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

(** Empty contract used as a placeholder before resolution. *)
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

(** Build a basic contract for symbol lookup. *)
let make_stk_contract ~symbol = {
  empty_contract with
  symbol;
  sec_type = "STK";
  exchange = "SMART";
  currency = "USD";
}

(* ---- Order type ---- *)

(** TWS order specification. *)
type order = {
  order_id: int;
  action: string;            (** "BUY" or "SELL" *)
  total_qty: float;
  order_type: string;        (** "MKT", "LMT", etc. *)
  lmt_price: float;
  tif: string;               (** "GTC", "DAY", "IOC", etc. *)
}

(** Build a market order. *)
let make_market_order ~order_id ~action ~qty = {
  order_id;
  action;
  total_qty = qty;
  order_type = "MKT";
  lmt_price = 0.0;
  tif = "DAY";
}

(** Build a limit order. *)
let make_limit_order ~order_id ~action ~qty ~price = {
  order_id;
  action;
  total_qty = qty;
  order_type = "LMT";
  lmt_price = price;
  tif = "GTC";
}

(* ---- Execution type ---- *)

(** TWS execution report. *)
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

(* ---- Order status type ---- *)

(** TWS order status variants. *)
type tws_order_status =
  | PreSubmitted
  | Submitted
  | Filled
  | Cancelled
  | Inactive
  | ApiPending
  | ApiCancelled
  | Unknown_status of string

(** Parse a TWS status string to a variant. *)
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

(** Map TWS status to unified Exchange_intf.Types.order_status. *)
let to_exchange_order_status = function
  | PreSubmitted -> Dio_exchange.Exchange_intf.Types.Pending
  | Submitted -> Dio_exchange.Exchange_intf.Types.New
  | Filled -> Dio_exchange.Exchange_intf.Types.Filled
  | Cancelled -> Dio_exchange.Exchange_intf.Types.Canceled
  | Inactive -> Dio_exchange.Exchange_intf.Types.Rejected
  | ApiPending -> Dio_exchange.Exchange_intf.Types.Pending
  | ApiCancelled -> Dio_exchange.Exchange_intf.Types.Canceled
  | Unknown_status s -> Dio_exchange.Exchange_intf.Types.Unknown s
