(* Grid_adapter - bridges a Strategy_common.trading_config (config.json) into a
   Grid_core.config for path replay.

   Resolves exchange_model by exchange string, maker_fee, sell_mult,
   accumulation_buffer and the price/qty increments. Increments prefer the live
   exchange registry (Suicide_grid_config get_price_increment /
   get_qty_increment_val) and fall back to 0.01 when the registry is empty
   (offline CLI runs); the guide's Phase 2 flag: pass explicit increments from
   the CLI to override. start_price/start_quote/grid_interval are supplied by
   the caller (quote capital is the inverse-sizing variable). *)

open Dio_strategies

let default_price_increment = 0.01
let default_qty_increment = 0.01
let default_maker_fee = 0.001
let exchange_model_of_string = Grid_core.exchange_model_of_string

let price_increment_of (tc : Strategy_common.trading_config) =
  try Suicide_grid_config.get_price_increment tc.symbol tc.exchange with
  | _ -> default_price_increment
;;

let qty_increment_of (tc : Strategy_common.trading_config) =
  try Suicide_grid_config.get_qty_increment_val tc.symbol tc.exchange with
  | _ -> default_qty_increment
;;

let float_of_string_opt def s =
  try float_of_string s with
  | _ -> def
;;

let of_trading_config
      (tc : Strategy_common.trading_config)
      ~start_price
      ~start_quote
      ~grid_interval_pct
  : Grid_core.config
  =
  let open Grid_core in
  let qty = float_of_string_opt 1.0 tc.qty in
  let sell_mult = float_of_string_opt 1.0 tc.sell_mult in
  let maker_fee = Option.value tc.maker_fee ~default:default_maker_fee in
  let accumulation_buffer = fst tc.accumulation_buffer in
  let exchange_model = exchange_model_of_string tc.exchange in
  { qty
  ; sell_mult
  ; grid_interval_pct
  ; maker_fee
  ; accumulation_buffer
  ; price_increment = price_increment_of tc
  ; qty_increment = qty_increment_of tc
  ; exchange_model
  ; start_price
  ; start_quote
  }
;;
