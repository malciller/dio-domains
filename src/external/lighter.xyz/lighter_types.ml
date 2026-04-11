(** Lighter-specific type definitions and conversion helpers.
    Maps between Lighter L2's integer-based protocol and the
    exchange-agnostic [Exchange_intf.Types]. *)

module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

(** Market metadata fetched from [orderBookDetails]. *)
type market_info = {
  market_index: int;           (** Integer market identifier on Lighter. *)
  symbol: string;              (** Human-readable symbol (e.g. "ETH"). *)
  supported_size_decimals: int;(** Decimal precision for base quantities. *)
  supported_price_decimals: int;(** Decimal precision for prices. *)
  min_base_amount: float;      (** Minimum order size in base units. *)
  min_quote_amount: float;     (** Minimum order notional in quote units. *)
  taker_fee: float;            (** Taker fee bps. *)
  maker_fee: float;            (** Maker fee bps. *)
}

(** Lighter order status integer codes to [Types.order_status].
    Reference: Lighter API documentation. *)
let status_of_lighter_int = function
  | 0 -> Types.Pending         (* InProgress *)
  | 1 -> Types.Pending         (* Pending *)
  | 2 -> Types.New             (* Active *)
  | 3 -> Types.Filled          (* Filled *)
  | 4 -> Types.Canceled        (* CanceledByOwner *)
  | 5 -> Types.Canceled        (* CanceledByEngine *)
  | 6 -> Types.Canceled        (* CanceledByProtocol *)
  | 7 -> Types.Expired         (* Expired *)
  | _n -> Types.Unknown "lighter_unknown_status"

let status_of_lighter_string = function
  | "InProgress" -> Types.Pending
  | "Pending" -> Types.Pending
  | "Active" -> Types.New
  | "Filled" -> Types.Filled
  | "CanceledByOwner" | "CanceledByEngine" | "CanceledByProtocol" -> Types.Canceled
  | "Expired" -> Types.Expired
  | _s -> Types.Unknown "lighter_unknown_status"

(** Map [Types.order_type] to Lighter's integer codes.
    0=Limit, 1=Market, 2=StopLoss, 3=TakeProfit *)
let lighter_order_type_int = function
  | Types.Limit -> 0
  | Types.Market -> 1
  | Types.StopLoss -> 2
  | Types.TakeProfit -> 3
  | Types.StopLossLimit -> 2  (* Fallback to StopLoss *)
  | Types.TakeProfitLimit -> 3 (* Fallback to TakeProfit *)
  | _ -> 0 (* Default to Limit *)

(** Map [Types.time_in_force] to Lighter's integer codes.
    0=IOC, 1=GoodTillTime, 2=PostOnly *)
let lighter_tif_int ?(post_only=false) = function
  | Types.IOC -> 0
  | Types.FOK -> 0  (* IOC closest approximation *)
  | Types.GTC -> if post_only then 2 else 1

(** Map [Types.order_side] to Lighter's is_ask boolean.
    Sell (ask) = true, Buy (bid) = false. *)
let is_ask_of_side = function
  | Types.Sell -> true
  | Types.Buy -> false

let side_of_is_ask = function
  | true -> Types.Sell
  | false -> Types.Buy

(** Convert a float price/size to Lighter's integer representation
    using the specified decimal precision.
    E.g., price 2181.83 with 2 decimals → 218183 *)
let[@inline always] float_to_lighter_int ~decimals value =
  let multiplier = 10.0 ** float_of_int decimals in
  Int64.of_float (Float.round (value *. multiplier))

(** Convert Lighter's integer representation back to a float.
    E.g., 218183 with 2 decimals → 2181.83 *)
let[@inline always] lighter_int_to_float ~decimals value =
  let multiplier = 10.0 ** float_of_int decimals in
  Int64.to_float value /. multiplier

(** Price increment derived from [supported_price_decimals]. *)
let[@inline always] price_increment_of_decimals decimals =
  10.0 ** (-. float_of_int decimals)

(** Quantity increment derived from [supported_size_decimals]. *)
let[@inline always] qty_increment_of_decimals decimals =
  10.0 ** (-. float_of_int decimals)

(** Round price to the exchange's valid precision for a given market.
    Rounds to nearest tick using [supported_price_decimals]. *)
let round_price ~price_decimals price =
  if price <= 0.0 then price
  else
    let multiplier = 10.0 ** float_of_int price_decimals in
    Float.round (price *. multiplier) /. multiplier

(** Round quantity down to the exchange's valid precision for a given market.
    Uses floor to prevent over-allocation. *)
let round_qty ~size_decimals qty =
  let multiplier = 10.0 ** float_of_int size_decimals in
  floor (qty *. multiplier) /. multiplier

(** Lighter transaction type codes. *)
let tx_type_create_order = 14
let tx_type_cancel_order = 15
let tx_type_modify_order = 17
let tx_type_cancel_all_orders = 18

(** JSON parsing helpers for Lighter's numeric format. *)
let parse_json_float json =
  let open Yojson.Safe.Util in
  match json with
  | `String s -> (try float_of_string s with _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> to_float json

let parse_json_int json =
  let open Yojson.Safe.Util in
  match json with
  | `String s -> (try int_of_string s with _ -> 0)
  | `Int i -> i
  | `Float f -> int_of_float f
  | _ -> to_int json

let parse_json_int64 json =
  match json with
  | `String s -> (try Int64.of_string s with _ -> 0L)
  | `Int i -> Int64.of_int i
  | `Float f -> Int64.of_float f
  | _ -> 0L
