(* Grid_adapter - bridges a Strategy_common.trading_config (config.json) into a
   Grid_core.config for path replay.

   Resolves exchange_model by exchange string, maker_fee, sell_mult,
   accumulation_buffer and the price/qty increments. Increments prefer the live
   exchange registry (Suicide_grid_config get_price_increment /
   get_qty_increment_val) and fall back to 0.01 when the registry is empty
   (offline CLI runs); the guide's Phase 2 flag: pass explicit increments from
   the CLI to override. start_price/start_quote/grid_interval are supplied by
   the caller (quote capital is the inverse-sizing variable).

   Order-placeability floors: qty_min comes from the live venue registry
   (get_qty_min_val; 0.0 = unknown). min_notional defaults to the venue's
   documented minimum order notional (Hyperliquid's 10 USDC spot floor;
   Kraken/others are notional-unconstrained here - their qty_min governs), and
   both can be overridden from the CLI. *)

open Dio_strategies

let default_price_increment = 0.01
let default_qty_increment = 0.01
let default_maker_fee = 0.001
let exchange_model_of_string = Grid_core.exchange_model_of_string

(** Venue default minimum order notional (quote). Hyperliquid spot enforces
    MinTradeSpotNtl = 10 USDC; the perp/equity venues below are not
    notional-constrained in this model. *)
let default_min_notional exchange_model =
  match exchange_model with
  | Grid_core_types.Hyperliquid -> 10.0
  | Grid_core_types.Kraken
  | Grid_core_types.Lighter
  | Grid_core_types.Ibkr
  | Grid_core_types.Alpaca -> 0.0
;;

let price_increment_of (tc : Strategy_common.trading_config) =
  try Suicide_grid_config.get_price_increment tc.symbol tc.exchange with
  | _ -> default_price_increment
;;

let qty_increment_of (tc : Strategy_common.trading_config) =
  try Suicide_grid_config.get_qty_increment_val tc.symbol tc.exchange with
  | _ -> default_qty_increment
;;

let qty_min_of (tc : Strategy_common.trading_config) =
  try Suicide_grid_config.get_qty_min_val tc.symbol tc.exchange with
  | _ -> 0.0
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
  ; qty_min = qty_min_of tc
  ; min_notional = default_min_notional exchange_model
  ; exchange_model
  ; start_price
  ; start_quote
  ; cash_hook = None
  }
;;
