(** Shared type definitions and configuration parameters for Alpaca Markets integration. *)

let section = "alpaca_types"

module Config = struct
  let api_key () =
    try Sys.getenv "ALPACA_API_KEY"
    with Not_found -> ""

  let api_secret () =
    try Sys.getenv "ALPACA_API_SECRET"
    with Not_found -> ""

  let is_paper = ref true
  let data_feed = ref "iex" (* "iex" or "sip" *)
  let extended_hours = ref true

  let set_testnet testnet =
    is_paper := testnet;
    Logging.debug_f ~section "Alpaca trading mode set to %s"
      (if testnet then "PAPER" else "LIVE")

  let set_data_feed feed =
    data_feed := (if String.lowercase_ascii feed = "sip" then "sip" else "iex");
    Logging.debug_f ~section "Alpaca data feed set to %s" !data_feed

  let set_extended_hours enabled =
    extended_hours := enabled;
    Logging.debug_f ~section "Alpaca extended hours trading set to %B" enabled

  let rest_base_url () =
    if !is_paper then "https://paper-api.alpaca.markets"
    else "https://api.alpaca.markets"

  let trading_ws_url () =
    if !is_paper then "wss://paper-api.alpaca.markets/stream"
    else "wss://api.alpaca.markets/stream"

  let data_ws_url () =
    Printf.sprintf "wss://stream.data.alpaca.markets/v2/%s" !data_feed

  let data_rest_url () =
    "https://data.alpaca.markets"
end

type order_status =
  | New
  | PartiallyFilled
  | Filled
  | DoneForDay
  | Canceled
  | Expired
  | Replaced
  | PendingCancel
  | PendingReplace
  | Accepted
  | PendingNew
  | AcceptedForBidding
  | Stopped
  | Rejected
  | Suspended
  | Calculated
  | Unknown of string

let status_of_string = function
  | "new" -> New
  | "partially_filled" -> PartiallyFilled
  | "filled" -> Filled
  | "done_for_day" -> DoneForDay
  | "canceled" -> Canceled
  | "expired" -> Expired
  | "replaced" -> Replaced
  | "pending_cancel" -> PendingCancel
  | "pending_replace" -> PendingReplace
  | "accepted" -> Accepted
  | "pending_new" -> PendingNew
  | "accepted_for_bidding" -> AcceptedForBidding
  | "stopped" -> Stopped
  | "rejected" -> Rejected
  | "suspended" -> Suspended
  | "calculated" -> Calculated
  | s -> Unknown s

let string_of_status = function
  | New -> "new"
  | PartiallyFilled -> "partially_filled"
  | Filled -> "filled"
  | DoneForDay -> "done_for_day"
  | Canceled -> "canceled"
  | Expired -> "expired"
  | Replaced -> "replaced"
  | PendingCancel -> "pending_cancel"
  | PendingReplace -> "pending_replace"
  | Accepted -> "accepted"
  | PendingNew -> "pending_new"
  | AcceptedForBidding -> "accepted_for_bidding"
  | Stopped -> "stopped"
  | Rejected -> "rejected"
  | Suspended -> "suspended"
  | Calculated -> "calculated"
  | Unknown s -> s

type order_side = Buy | Sell

let side_of_string = function
  | "buy" -> Buy
  | "sell" -> Sell
  | s -> invalid_arg ("Unknown side: " ^ s)

let string_of_side = function
  | Buy -> "buy"
  | Sell -> "sell"

type add_order_result = {
  order_id: string;
  cl_ord_id: string option;
  order_userref: int option;
}

type amend_order_result = {
  original_order_id: string;
  new_order_id: string;
  amend_id: string option;
  cl_ord_id: string option;
}

type cancel_order_result = {
  order_id: string;
  cl_ord_id: string option;
}

type order_record = {
  id: string;
  client_order_id: string option;
  symbol: string;
  side: order_side;
  qty: float;
  filled_qty: float;
  type_str: string;
  side_str: string;
  status: order_status;
  limit_price: float option;
  created_at: string;
}

type account_record = {
  id: string;
  status: string;
  currency: string;
  buying_power: float;
  cash: float;
  portfolio_value: float;
  equity: float;
}

type position_record = {
  asset_id: string;
  symbol: string;
  exchange: string;
  qty: float;
  market_value: float;
  avg_entry_price: float;
  current_price: float;
  side: string;
}
