(** Aggregate inventory for a carry hedge controller.

    A controller owns one perp position and may be fed by several inventory legs (for
    example Kraken BTC and Hyperliquid BTC). This module computes the hedgeable base for
    each leg with the same rule, regardless of whether the leg also runs a grid:

    [Q = free_spot + max 0 (staked - open_sell_base)]

    [free_spot] is the venue's tradeable balance (open-order holds already netted out).
    [staked] is the non-tradeable earn/staked balance. Kraken auto-earn can put idle spot
    into earn while a pending sell is backed by it; up to [open_sell_base] of the staked
    balance is treated as committed to a sale and must not be counted as hedgeable
    inventory.

    A controller with [full_balance_ref] uses [total] instead, so the base locked in open
    sell orders is part of the hedgeable inventory.

    Reading balances and open orders is an in-memory store access on the overlay cycle,
    not the grid hot path. The open-order walk uses the venue's allocation-free iterator
    (primitive values, no [Types.open_order] list), so a pair with a large resting ladder
    does not allocate a record per order on every cycle. *)

let section = "overlay_inventory"

module Exchange = Dio_exchange.Exchange_intf

type snapshot =
  { total : float
  ; has_spot_leg : bool
  ; spot_symbol : string
  }

let base_coin_of_symbol symbol = Config.base_coin_of_symbol symbol

let get_module exchange =
  match Exchange.Registry.get exchange with
  | Some m -> Some m
  | None ->
    Logging.debug_f ~section "No exchange module for inventory venue '%s'" exchange;
    None
;;

(** Quantity of open sell orders for [symbol]. Returns 0 on any accessor failure.
    Allocation-free: [iter_open_orders_fast] yields primitives, so the overlay cycle no
    longer materializes one [Types.open_order] record per resting order. *)
let open_sell_base (module Ex : Exchange.S) ~symbol =
  let total = ref 0.0 in
  (try
     Ex.iter_open_orders_fast ~symbol (fun _oid _price qty side_str _userref ->
       if qty > 0.0 && String.equal side_str "sell" then total := !total +. qty)
   with
   | _ -> ());
  !total
;;

(** Per-leg cache for {!open_sell_base}. The venue's open-orders generation moves only
    when the book changes, so a quiet pair reuses the last sum instead of walking its
    resting ladder on every overlay cycle (the hot-read contract reserves O(open-orders)
    work for reconcile paths). *)
type open_sell_cache =
  { mutable gen : int
  ; mutable value : float
  }

let create_open_sell_cache () = { gen = -2; value = 0.0 }

let open_sell_base_cached (module Ex : Exchange.S) ~symbol ~(cache : open_sell_cache) =
  let gen =
    try Ex.get_open_orders_generation ~symbol with
    | _ -> -1
  in
  if gen >= 0 && gen = cache.gen
  then cache.value
  else (
    let value = open_sell_base (module Ex) ~symbol in
    cache.gen <- gen;
    cache.value <- value;
    value)
;;

let sanitize v = if Float.is_finite v && v > 0.0 then v else 0.0

(** Hedgeable base for one leg. [free] is the tradeable (hold-netted) figure; [total]
    includes earn/staked balances and the base locked in resting sell orders. Pending sell
    orders can be backed by auto-earn; those staked units are committed to a sale and are
    excluded from the accumulated basis. With no sells there is nothing to exclude, so the
    subtraction is skipped entirely. *)
let q_of_balances ~full_balance_ref ~free ~total ~open_sell_base =
  if full_balance_ref
  then sanitize total
  else (
    let staked = Float.max 0.0 (total -. free) in
    let staked =
      if open_sell_base <= 0.0 then staked else Float.max 0.0 (staked -. open_sell_base)
    in
    free +. staked)
;;

let leg_q ?cache ?(full_balance_ref = false) (leg : Config.overlay_leg) =
  match get_module leg.ol_exchange with
  | None -> 0.0
  | Some (module Ex : Exchange.S) ->
    let base = base_coin_of_symbol leg.ol_symbol in
    let total =
      try sanitize (Ex.get_total_balance ~asset:base) with
      | _ -> 0.0
    in
    if full_balance_ref
    then total
    else (
      let free =
        try sanitize (Ex.get_tradeable_balance ~asset:base) with
        | _ -> 0.0
      in
      let open_sell =
        match cache with
        | Some c -> open_sell_base_cached (module Ex) ~symbol:leg.ol_symbol ~cache:c
        | None -> open_sell_base (module Ex) ~symbol:leg.ol_symbol
      in
      q_of_balances ~full_balance_ref ~free ~total ~open_sell_base:open_sell)
;;

(** Per-leg caches aligned with [controller.oc_legs]. *)
let create_caches (controller : Config.overlay_controller) =
  Array.init (List.length controller.oc_legs) (fun _ -> create_open_sell_cache ())
;;

(** Sum the legs of [controller]. [spot_symbol] is the controller's Hyperliquid spot pair
    when it has an HL leg, else empty. The recycle base-accumulation path is only enabled
    when an HL spot leg exists. *)
let aggregate (controller : Config.overlay_controller) =
  let full_balance_ref = controller.oc_params.full_balance_ref in
  let total =
    List.fold_left
      (fun acc leg -> acc +. leg_q ~full_balance_ref leg)
      0.0
      controller.oc_legs
  in
  let hl_leg =
    List.find_opt
      (fun (l : Config.overlay_leg) -> Config.is_hyperliquid l.ol_exchange)
      controller.oc_legs
  in
  { total
  ; has_spot_leg = Option.is_some hl_leg
  ; spot_symbol =
      (match hl_leg with
       | Some l -> l.ol_symbol
       | None -> controller.oc_hedge_symbol)
  }
;;

(** {!aggregate} with per-leg open-sell caches. Falls back to [aggregate] when [caches] is
    not aligned with the controller's legs (a config reload / test fixture). *)
let aggregate_cached (controller : Config.overlay_controller) ~caches =
  let full_balance_ref = controller.oc_params.full_balance_ref in
  let legs = controller.oc_legs in
  if Array.length caches <> List.length legs
  then aggregate controller
  else (
    let i = ref 0 in
    let total =
      List.fold_left
        (fun acc leg ->
          let cache = Array.unsafe_get caches !i in
          incr i;
          acc +. leg_q ~cache ~full_balance_ref leg)
        0.0
        legs
    in
    let hl_leg =
      List.find_opt
        (fun (l : Config.overlay_leg) -> Config.is_hyperliquid l.ol_exchange)
        legs
    in
    { total
    ; has_spot_leg = Option.is_some hl_leg
    ; spot_symbol =
        (match hl_leg with
         | Some l -> l.ol_symbol
         | None -> controller.oc_hedge_symbol)
    })
;;
