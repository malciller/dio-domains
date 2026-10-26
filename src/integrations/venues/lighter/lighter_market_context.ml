(** Lighter implementation of the optional [Exchange_intf.Market_context] capability.

    Position, funding, and margin snapshots are published by {!Lighter_positions} and
    {!Lighter_market_stats}; this module is the engine-facing, venue-keyed capability so
    strategy code never reaches into the global store directly. *)

module Exchange = Dio_exchange.Exchange_intf
module Feed = Lighter_executions_feed

let name = "lighter"

let side_of (s : Feed.side) =
  match s with
  | Feed.Buy -> Exchange.Types.Buy
  | Feed.Sell -> Exchange.Types.Sell
;;

let status_of (s : Feed.order_status) =
  match s with
  | Feed.PendingStatus -> Exchange.Types.Pending
  | Feed.NewStatus -> Exchange.Types.New
  | Feed.PartiallyFilledStatus -> Exchange.Types.PartiallyFilled
  | Feed.FilledStatus -> Exchange.Types.Filled
  | Feed.CanceledStatus -> Exchange.Types.Canceled
  | Feed.ExpiredStatus -> Exchange.Types.Expired
  | Feed.RejectedStatus -> Exchange.Types.Rejected
  | Feed.UnknownStatus s -> Exchange.Types.Unknown s
;;

let order_of_feed (o : Feed.open_order) : Exchange.Types.open_order =
  { order_id = o.order_id
  ; symbol = o.symbol
  ; market = Exchange.Types.Perp
  ; side = side_of o.side
  ; qty = o.order_qty
  ; cum_qty = o.cum_qty
  ; remaining_qty = o.remaining_qty
  ; limit_price = o.limit_price
  ; status = status_of o.order_status
  ; user_ref = o.order_userref
  ; cl_ord_id = o.cl_ord_id
  }
;;

let perp_symbols_for_coin coin =
  Feed.get_all_symbols ()
  |> List.filter (fun symbol ->
    (not (String.contains symbol '/')) && Lighter_types.base_coin symbol = coin)
;;

module Impl : Exchange.Market_context.S = struct
  let name = "lighter"
  let get_funding ~coin = Dio_exchange.Market_context.get_funding ~exchange:name ~coin

  let get_positions_updated_at () =
    Dio_exchange.Market_context.get_positions_updated_at ~exchange:name
  ;;

  let get_position ~coin = Dio_exchange.Market_context.get_position ~exchange:name ~coin
  let get_margin () = Dio_exchange.Market_context.get_margin ~exchange:name
  let get_margin_ratio () = Dio_exchange.Market_context.get_margin_ratio ~exchange:name

  let get_open_orders ~coin =
    perp_symbols_for_coin coin
    |> List.concat_map Feed.get_open_orders
    |> List.map order_of_feed
  ;;
end
