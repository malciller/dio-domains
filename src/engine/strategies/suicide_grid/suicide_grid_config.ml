(* Suicide Grid - Exchange Configuration & Dynamic Helpers *)

open Suicide_grid_types

let kraken_config = {
  time_in_force = "GTC";
  track_pending_sells = true;
  use_accumulation_sells = false;
  sell_uses_mult = true;
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;
  asset_low_requires_balance_change = true;
  merge_preserved_sells = true;
  check_stale_balance = true;
  remaintain_expired_sells = false;
}

let hyperliquid_config = {
  time_in_force = "Alo";
  track_pending_sells = false;
  use_accumulation_sells = true;
  sell_uses_mult = false;
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;
  asset_low_requires_balance_change = false;
  merge_preserved_sells = true;
  check_stale_balance = false;
  remaintain_expired_sells = false;
}

let ibkr_config = {
  time_in_force = "GTC";
  track_pending_sells = true;
  use_accumulation_sells = true;
  sell_uses_mult = false;
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;
  asset_low_requires_balance_change = false;
  merge_preserved_sells = true;
  check_stale_balance = true;
  remaintain_expired_sells = false;
}

let lighter_config = {
  time_in_force = "GTC";
  track_pending_sells = true;
  use_accumulation_sells = true;
  sell_uses_mult = false;
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;
  asset_low_requires_balance_change = false;
  merge_preserved_sells = true;
  check_stale_balance = false;
  remaintain_expired_sells = false;
}

let alpaca_config = {
  time_in_force = "GTC";
  track_pending_sells = true;
  use_accumulation_sells = false;
  sell_uses_mult = true;
  sell_failure_sets_asset_low = true;
  use_reserved_base_guard = true;
  asset_low_requires_balance_change = false;
  merge_preserved_sells = true;
  check_stale_balance = true;
  remaintain_expired_sells = true;
}

let get_exchange_config exchange =
  match Exchange.Types.exchange_of_string exchange with
  | Hyperliquid -> hyperliquid_config
  | Lighter -> lighter_config
  | Ibkr -> ibkr_config
  | Alpaca -> alpaca_config
  | Kraken | Custom _ -> kraken_config

let[@inline always] persistence_accumulation_exchange id =
  match Exchange.Types.exchange_of_string id with
  | Hyperliquid | Lighter | Ibkr | Alpaca -> true
  | Kraken | Custom _ -> false

let[@inline always] hl_like_spot_fee_exchange id =
  match Exchange.Types.exchange_of_string id with
  | Hyperliquid | Lighter -> true
  | Kraken | Ibkr | Alpaca | Custom _ -> false

let ibkr_commission ~qty ~price =
  let per_share_rate = 0.0035 in
  let raw = qty *. per_share_rate in
  let min_fee = 0.35 in
  let max_fee = 0.01 *. qty *. price in
  Float.max min_fee (Float.min raw max_fee)

let _exchange_module_cache : (string, (module Exchange.S)) Hashtbl.t =
  Hashtbl.create 4

let get_exchange_module exchange =
  match Hashtbl.find_opt _exchange_module_cache exchange with
  | Some m -> Some m
  | None ->
      (match Exchange.Registry.get exchange with
       | Some m ->
           Hashtbl.replace _exchange_module_cache exchange m;
           Some m
       | None -> None)

let _round_price_fn_cache : (string, float -> float) Hashtbl.t =
  Hashtbl.create 8

let get_round_price_fn symbol exchange =
  let key = symbol ^ "|" ^ exchange in
  match Hashtbl.find_opt _round_price_fn_cache key with
  | Some f -> f
  | None ->
      let f = match get_exchange_module exchange with
        | Some (module Ex : Exchange.S) -> (fun p -> Ex.round_price ~symbol ~price:p)
        | None -> Float.round
      in
      Hashtbl.replace _round_price_fn_cache key f;
      f

let get_price_increment symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_price_increment ~symbol) ~default:0.01
  | None ->
      Logging.warn_f ~section "No price increment info for %s/%s, using default 0.01" exchange symbol;
      0.01

let get_qty_increment_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_qty_increment ~symbol) ~default:0.01
  | None -> 0.01

let round_qty qty symbol exchange =
  let increment = get_qty_increment_val symbol exchange in
  let inv = 1.0 /. increment in
  floor (qty *. inv) /. inv

let venue_lot_qty grid_qty exchange state =
  match exchange with
  | "ibkr" ->
      if grid_qty <= 0.0 then 0.0
      else
        let q = let inv = 1.0 /. state.cached_qty_increment in floor (grid_qty *. inv) /. inv in
        if q > 0.0 then q else state.cached_qty_min
  | "lighter" ->
      if grid_qty <= 0.0 then 0.0
      else
        let q = let inv = 1.0 /. state.cached_qty_increment in floor (grid_qty *. inv) /. inv in
        if q > 0.0 then q else state.cached_qty_min
  | _ -> grid_qty

let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
      Logging.warn_f ~section "Invalid %s value '%s' for %s/%s, using default %.4f"
        value_name config exchange symbol default;
      default

let get_min_move_threshold price grid_interval_pct state =
  let base_increment = state.cached_price_increment in
  let pct_based = price *. (grid_interval_pct *. 0.05 /. 100.0) in
  max (base_increment *. 10.0) pct_based

let calculate_grid_price current_price grid_interval_pct is_above state =
  let interval = current_price *. (grid_interval_pct /. 100.0) in
  let raw_price = if is_above then current_price +. interval else current_price -. interval in
  state.cached_round_price raw_price
