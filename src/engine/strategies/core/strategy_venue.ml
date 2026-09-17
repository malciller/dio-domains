(* Venue configuration, precision helpers and grid pricing (strategy-agnostic). *)

open Strategy_state

(* Kraken matches HL/Lighter/IBKR: 1:1 sells and persistence-layer reserved_base accrual
   on profitable sell fills (Base_accumulation_store). *)
let kraken_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = false
  ; asset_low_requires_balance_change = true
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

let hyperliquid_config =
  { time_in_force = "Alo"
  ; track_pending_sells = false
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = true
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = false
  ; remaintain_expired_sells = false
  }
;;

let ibkr_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = false
  ; hold_netted_from_venue_state = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

let lighter_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = false
  ; hold_netted_from_venue_state = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = false
  ; remaintain_expired_sells = false
  }
;;

let alpaca_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = true
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = true
  }
;;

let get_exchange_config exchange =
  match Exchange.Types.exchange_of_string exchange with
  | Hyperliquid -> hyperliquid_config
  | Lighter -> lighter_config
  | Ibkr -> ibkr_config
  | Alpaca -> alpaca_config
  | Kraken | Custom _ -> kraken_config
;;

let[@inline always] hl_like_spot_fee_exchange id =
  match Exchange.Types.exchange_of_string id with
  | Hyperliquid | Lighter -> true
  | Kraken | Ibkr | Alpaca | Custom _ -> false
;;

let ibkr_commission ~qty ~price =
  let per_share_rate = 0.0035 in
  let raw = qty *. per_share_rate in
  let min_fee = 0.35 in
  let max_fee = 0.01 *. qty *. price in
  Float.max min_fee (Float.min raw max_fee)
;;

let _exchange_module_cache : (string, (module Exchange.S)) Hashtbl.t = Hashtbl.create 4

let get_exchange_module exchange =
  match Hashtbl.find_opt _exchange_module_cache exchange with
  | Some m -> Some m
  | None ->
    (match Exchange.Registry.get exchange with
     | Some m ->
       Hashtbl.replace _exchange_module_cache exchange m;
       Some m
     | None -> None)
;;

let _round_price_fn_cache : (string, float -> float) Hashtbl.t = Hashtbl.create 8

let get_round_price_fn symbol exchange =
  let key = symbol ^ "|" ^ exchange in
  match Hashtbl.find_opt _round_price_fn_cache key with
  | Some f -> f
  | None ->
    let f =
      match get_exchange_module exchange with
      | Some (module Ex : Exchange.S) -> fun p -> Ex.round_price ~symbol ~price:p
      | None -> Float.round
    in
    Hashtbl.replace _round_price_fn_cache key f;
    f
;;

let get_price_increment symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) ->
    (match Ex.get_price_increment ~symbol with
     | Some inc -> inc
     | None ->
       Logging.warn_f
         ~section
         "No price increment info for %s/%s (venue metadata missing); using default 0.01"
         exchange
         symbol;
       0.01)
  | None ->
    Logging.warn_f
      ~section
      "No price increment info for %s/%s (exchange module not registered); using default \
       0.01"
      exchange
      symbol;
    0.01
;;

let get_qty_increment_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) ->
    (match Ex.get_qty_increment ~symbol with
     | Some inc -> inc
     | None ->
       Logging.warn_f
         ~section
         "No qty increment info for %s/%s (venue metadata missing); using default 0.01"
         exchange
         symbol;
       0.01)
  | None ->
    Logging.warn_f
      ~section
      "No qty increment info for %s/%s (exchange module not registered); using default \
       0.01"
      exchange
      symbol;
    0.01
;;

let round_qty qty symbol exchange =
  let increment = get_qty_increment_val symbol exchange in
  let inv = 1.0 /. increment in
  floor ((qty *. inv) +. 1e-9) /. inv
;;

(** Minimum accepted order QUANTITY for [symbol] in base-asset units, from the live venue
    module (e.g. 0.0005 BTC on Hyperliquid spot; 0.0 = unknown). Venue floor every order
    must clear; independent of the grid's [qty]. *)
let get_qty_min_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_qty_min ~symbol) ~default:0.0
  | None -> 0.0
;;

(** Default minimum order notional for [symbol] in quote terms (0.0 = unconstrained), from
    the venue's oracle adapter ([Exchange_intf.Oracle.S.min_notional]): Hyperliquid 10
    USDC spot floor, Alpaca $1 fractional minimum, others 0.0. Unregistered venues are
    unconstrained. Same resolution as the oracle's [Grid_adapter], so the live grid and
    replay agree on the floor. *)
let get_min_notional_val symbol exchange =
  match Exchange.Oracle.Registry.get exchange with
  | Some (module V) -> V.min_notional ~symbol
  | None -> 0.0
;;

let venue_lot_qty grid_qty exchange state =
  match exchange with
  | "ibkr" ->
    if grid_qty <= 0.0
    then 0.0
    else (
      let q =
        let inv = 1.0 /. state.cached_qty_increment in
        floor (grid_qty *. inv) /. inv
      in
      if q > 0.0 then q else state.cached_venue_min_qty)
  | "lighter" ->
    if grid_qty <= 0.0
    then 0.0
    else (
      let q =
        let inv = 1.0 /. state.cached_qty_increment in
        floor (grid_qty *. inv) /. inv
      in
      if q > 0.0 then q else state.cached_venue_min_qty)
  | _ -> grid_qty
;;

let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
    Logging.warn_f
      ~section
      "Invalid %s value '%s' for %s/%s, using default %.4f"
      value_name
      config
      exchange
      symbol
      default;
    default
;;

let get_min_move_threshold price_increment =
  (* Exchange minimum price move: one tick ([price_increment], e.g. $0.01 on
     Alpaca/Hyperliquid). An amendment one tick away is a valid resting price, so the
     trailing leg re-anchors on every valid price step and the buy tracks price action
     without a deadband. *)
  price_increment
;;

(** Pure grid price: [current] moved by [grid_interval_pct] percent (up when [is_above]),
    snapped by [round_price]. Shared by the grid and the config-driven interpreter. *)
let grid_price ~round_price ~current ~grid_interval_pct ~is_above =
  let interval = current *. (grid_interval_pct /. 100.0) in
  round_price (if is_above then current +. interval else current -. interval)
;;

let calculate_grid_price current_price grid_interval_pct is_above state =
  grid_price
    ~round_price:state.cached_round_price
    ~current:current_price
    ~grid_interval_pct
    ~is_above
;;
