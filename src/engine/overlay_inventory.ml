(** Aggregate inventory for a carry hedge controller.

    A controller owns one perp position and may be fed by several inventory legs (for
    example Kraken BTC and Hyperliquid BTC). This module computes the hedgeable base for
    each leg with the same rule, regardless of whether the leg also runs a grid:

    [Q = free_spot + staked - staked_committed_to_sells]

    [free_spot] is the venue's tradeable balance (open-order holds already netted out).
    [staked] is the venue's non-tradeable earn/staked balance, read explicitly: delegated
    HYPE on Hyperliquid is held inventory the hedge can cover, so it must be counted.
    Kraken auto-earn can put idle spot into earn while a pending sell is backed by it;
    that much of the staked balance is committed to a sale and is not hedgeable inventory.
    Only the part of [open_sell_base] the tradeable/locked figure does not already cover
    may be charged against staked, so a large resting ladder cannot erase genuine staking.

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

(** Per-leg cache for the resting-sell view. The venue's open-orders generation moves only
    when the book changes, so a quiet pair refolds a handful of cached [(price, qty)]
    pairs instead of walking its resting ladder on every overlay cycle (the hot-read
    contract reserves O(open-orders) work for reconcile paths). On a perp symbol that walk
    is not free even when quiet: it re-reads the process-wide order index under two
    mutexes. *)
type open_sell_cache =
  { mutable gen : int
  ; mutable value : float (* sum of resting sell base quantity *)
  ; mutable sells : (float * float) array (* (price, qty) of every priced resting sell *)
  }

let create_open_sell_cache () = { gen = -2; value = 0.0; sells = [||] }

(** One venue walk fills both views, so a refresh costs no more than the old base-only
    scan. *)
let open_sells (module Ex : Exchange.S) ~symbol =
  let base = ref 0.0 in
  let sells = ref [] in
  (try
     Ex.iter_open_orders_fast ~symbol (fun _oid price qty side_str _userref ->
       if qty > 0.0 && String.equal side_str "sell"
       then (
         base := !base +. qty;
         if price > 0.0 then sells := (price, qty) :: !sells))
   with
   | _ -> ());
  !base, Array.of_list (List.rev !sells)
;;

(** Refresh [cache] when the pair's open-orders generation moved. A generation of [-1]
    (accessor failure) always rescans, matching the pre-cache behaviour. *)
let refresh_open_sells (module Ex : Exchange.S) ~symbol ~(cache : open_sell_cache) =
  let gen =
    try Ex.get_open_orders_generation ~symbol with
    | _ -> -1
  in
  if gen < 0 || gen <> cache.gen
  then (
    let base, sells = open_sells (module Ex) ~symbol in
    cache.gen <- gen;
    cache.value <- base;
    cache.sells <- sells);
  cache
;;

let open_sell_base_cached (module Ex : Exchange.S) ~symbol ~cache =
  (refresh_open_sells (module Ex) ~symbol ~cache).value
;;

(** Notional of resting sells priced under each cap. [sells] must be the cached array, so
    the fold is over the resting ladder only - no venue access. *)
let proceeds_below_2 (sells : (float * float) array) ~cap_a ~cap_b =
  let a = ref 0.0 in
  let b = ref 0.0 in
  Array.iter
    (fun (price, qty) ->
      let notional = qty *. price in
      if cap_a > 0.0 && price < cap_a then a := !a +. notional;
      if cap_b > 0.0 && price < cap_b then b := !b +. notional)
    sells;
  !a, !b
;;

(** [(proceeds below price_a, proceeds below price_b)]. The caps move every cycle, so only
    the order set is cached; pass [~cache] on the per-cycle overlay path. *)
let open_sell_proceeds_below2 ?cache ~exchange ~symbol ~price_a ~price_b () =
  let cap_a = if Float.is_finite price_a && price_a > 0.0 then price_a else 0.0 in
  let cap_b = if Float.is_finite price_b && price_b > 0.0 then price_b else 0.0 in
  if cap_a <= 0.0 && cap_b <= 0.0
  then 0.0, 0.0
  else (
    match get_module exchange with
    | None -> 0.0, 0.0
    | Some (module Ex : Exchange.S) ->
      let sells =
        match cache with
        | Some cache -> (refresh_open_sells (module Ex) ~symbol ~cache).sells
        | None -> snd (open_sells (module Ex) ~symbol)
      in
      proceeds_below_2 sells ~cap_a ~cap_b)
;;

(** USDC notional of resting sells on [symbol] priced below [max_price] - only those fill
    before a requirement at that price. Walks the order book unless [~cache] is given. *)
let open_sell_proceeds_below ?cache ~exchange ~symbol ~max_price () =
  if (not (Float.is_finite max_price)) || max_price <= 0.0
  then 0.0
  else (
    match get_module exchange with
    | None -> 0.0
    | Some (module Ex : Exchange.S) ->
      let sells =
        match cache with
        | Some cache -> (refresh_open_sells (module Ex) ~symbol ~cache).sells
        | None -> snd (open_sells (module Ex) ~symbol)
      in
      let total = ref 0.0 in
      Array.iter
        (fun (price, qty) -> if price < max_price then total := !total +. (qty *. price))
        sells;
      !total)
;;

let sanitize v = if Float.is_finite v && v > 0.0 then v else 0.0

(** Hedgeable base for one leg. [free] is the tradeable (hold-netted) figure; [total]
    includes earn/staked balances and the base locked in resting sell orders. Pending sell
    orders can be backed by staked units (Kraken auto-earn); those units are committed to
    a sale and are excluded from the accumulated basis. Only the part of [open_sell_base]
    the locked (non-staked) figure does not already cover is charged against [staked]: a
    venue's genuine staking (delegated HYPE) never backs a resting sell, and a large
    ladder must not subtract it away. *)
let q_of_balances ~full_balance_ref ~free ~total ~staked ~open_sell_base =
  if full_balance_ref
  then sanitize total
  else (
    let staked = sanitize staked in
    let sell_locked = Float.max 0.0 (sanitize total -. sanitize free -. staked) in
    let staked_committed =
      Float.min staked (Float.max 0.0 (open_sell_base -. sell_locked))
    in
    sanitize free +. staked -. staked_committed)
;;

(** [leg_q] from primitives, so a generic overlay can price a leg without materializing a
    [Config.overlay_leg]. *)
let leg_q_of ?cache ?(full_balance_ref = false) ~exchange ~symbol () =
  match get_module exchange with
  | None -> 0.0
  | Some (module Ex : Exchange.S) ->
    let base = base_coin_of_symbol symbol in
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
      let staked =
        try sanitize (Ex.get_staked_balance ~asset:base) with
        | _ -> 0.0
      in
      let open_sell =
        match cache with
        | Some c -> open_sell_base_cached (module Ex) ~symbol ~cache:c
        | None -> open_sell_base (module Ex) ~symbol
      in
      q_of_balances ~full_balance_ref ~free ~total ~staked ~open_sell_base:open_sell)
;;

let leg_q ?cache ?full_balance_ref (leg : Config.overlay_leg) =
  leg_q_of ?cache ?full_balance_ref ~exchange:leg.ol_exchange ~symbol:leg.ol_symbol ()
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
