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

    Reading balances and open orders is an in-memory store access on the overlay cycle,
    not the grid hot path. The open-order scan is skipped entirely when there are no
    sells. *)

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

(** Quantity of open sell orders for [symbol]. Returns 0 on any accessor failure. *)
let open_sell_base (module Ex : Exchange.S) ~symbol =
  try
    List.fold_left
      (fun acc (o : Exchange.Types.open_order) ->
        match o.side with
        | Exchange.Types.Sell ->
          if o.status = Exchange.Types.New || o.status = Exchange.Types.PartiallyFilled
          then acc +. Float.max 0.0 o.remaining_qty
          else acc
        | Exchange.Types.Buy -> acc)
      0.0
      (Ex.get_open_orders ~symbol)
  with
  | _ -> 0.0
;;

(** Hedgeable base for one leg. [free] is the tradeable (hold-netted) figure; [total]
    includes earn/staked balances. Pending sell orders can be backed by auto-earn; those
    staked units are committed to a sale and are excluded. With no sells there is nothing
    to exclude, so the subtraction is skipped entirely. *)
let q_of_balances ~free ~total ~open_sell_base =
  let staked = Float.max 0.0 (total -. free) in
  let staked =
    if open_sell_base <= 0.0 then staked else Float.max 0.0 (staked -. open_sell_base)
  in
  free +. staked
;;

let sanitize v = if Float.is_finite v && v > 0.0 then v else 0.0

let leg_q (leg : Config.overlay_leg) =
  match get_module leg.ol_exchange with
  | None -> 0.0
  | Some (module Ex : Exchange.S) ->
    let base = base_coin_of_symbol leg.ol_symbol in
    let free =
      try sanitize (Ex.get_tradeable_balance ~asset:base) with
      | _ -> 0.0
    in
    let total =
      try sanitize (Ex.get_total_balance ~asset:base) with
      | _ -> 0.0
    in
    let open_sell = open_sell_base (module Ex) ~symbol:leg.ol_symbol in
    q_of_balances ~free ~total ~open_sell_base:open_sell
;;

(** Sum the legs of [controller]. [spot_symbol] is the controller's Hyperliquid spot pair
    when it has an HL leg, else empty. The recycle base-accumulation path is only enabled
    when an HL spot leg exists. *)
let aggregate (controller : Config.overlay_controller) =
  let total = List.fold_left (fun acc leg -> acc +. leg_q leg) 0.0 controller.oc_legs in
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
