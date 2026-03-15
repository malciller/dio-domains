(** Hyperliquid action packing and translation *)

open Hyperliquid_types
module ExTypes = Dio_exchange.Exchange_intf.Types

(** Convert Exchange standard order type to Hyperliquid order type (with TIF) *)
let hl_order_type (ot : ExTypes.order_type) (tif_opt : ExTypes.time_in_force option) : order_type_wire =
  let tif = match tif_opt with
    | Some ExTypes.GTC -> Gtc
    | Some ExTypes.IOC -> Ioc
    | Some ExTypes.FOK -> Ioc (* HL doesn't support FOK natively in the same way, best effort IOC *)
    | None -> Gtc
  in
  match ot with
  | ExTypes.Limit -> Limit { tif }
  | ExTypes.Market -> Limit { tif = Ioc } (* Market orders are usually IOC limits in HL *)
  | ExTypes.StopLoss -> Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Sl }
  | ExTypes.TakeProfit -> Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Tp }
  | ExTypes.StopLossLimit -> Trigger { triggerPx = "0.0"; isMarket = false; tpsl = Sl }
  | ExTypes.TakeProfitLimit -> Trigger { triggerPx = "0.0"; isMarket = false; tpsl = Tp }
  | _ -> Limit { tif } (* Fallback *)

(** Format float as a string rounded to a precision suitable for Hyperliquid.
    HL expects maximum 8 decimals, but crucially it must be normalized (no trailing zeros).
    This matches Python's Decimal(rounded).normalize() representation. *)
let format_number f =
  let rounded = Printf.sprintf "%.8f" f in
  (* Convert back to float and then to string to let OCaml handle trailing zeros,
     but OCaml's string_of_float can use scientific notation for very small/large numbers.
     Hyperliquid expects fixed-point strings. *)
  let rec strip_zeros s =
    if String.length s > 1 && s.[String.length s - 1] = '0' && String.contains s '.' then
      strip_zeros (String.sub s 0 (String.length s - 1))
    else if String.length s > 1 && s.[String.length s - 1] = '.' then
      String.sub s 0 (String.length s - 1)
    else s
  in
  let s = strip_zeros rounded in
  if s = "-0" then "0" else s

(** Convert generic order parameters into Hyperliquid's order_wire format *)
let to_hl_order_wire ~qty ~symbol:_ ~asset_index ~ot ~side ~limit_price ~reduce_only ~cl_ord_id : order_wire =
  let b = match side with
    | ExTypes.Buy -> true
    | ExTypes.Sell -> false
  in
  let p = match limit_price with
    | Some price -> format_number price
    | None -> "0.0" (* Market orders or triggers where price is handled differently *)
  in
  let s = format_number qty in
  let r = match reduce_only with
    | Some v -> v
    | None -> false
  in
  let a = asset_index in
  {
    a; b; p; s; r; t = ot; c = cl_ord_id
  }
