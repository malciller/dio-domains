(** Hyperliquid implementation of the optional [Exchange_intf.Market_context] capability.

    The feed writers publish into {!Dio_exchange.Market_context}; this module is the
    engine-facing, venue-keyed capability so strategy code never reaches into the global
    store directly. *)

module Exchange = Dio_exchange.Exchange_intf
module Feed = Hyperliquid_executions_feed

let side_of (s : Feed.side) =
  match s with
  | Feed.Buy -> Exchange.Types.Buy
  | Feed.Sell -> Exchange.Types.Sell
;;

let status_of (s : Feed.order_status) =
  match s with
  | Feed.PendingNewStatus | Feed.NewStatus -> Exchange.Types.New
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
  ; market = o.market
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

module Impl : Exchange.Market_context.S = struct
  let name = "hyperliquid"
  let get_funding ~coin = Dio_exchange.Market_context.get_funding ~exchange:name ~coin

  let get_positions_updated_at () =
    Dio_exchange.Market_context.get_positions_updated_at ~exchange:name
  ;;

  let get_position ~coin = Dio_exchange.Market_context.get_position ~exchange:name ~coin
  let get_margin () = Dio_exchange.Market_context.get_margin ~exchange:name
  let get_margin_ratio () = Dio_exchange.Market_context.get_margin_ratio ~exchange:name

  let get_risk ~coin =
    match Hyperliquid_instruments_feed.lookup_info coin with
    | None -> None
    | Some info ->
      (match info.Hyperliquid_instruments_feed.max_leverage with
       | Some l when l > 0 ->
         let max_leverage = float_of_int l in
         (* Hyperliquid maintenance is half the initial requirement. At the venue maximum
            leverage that is [0.5 / max_leverage]. *)
         Some
           { Exchange.Market_context.maintenance_frac = 0.5 /. max_leverage
           ; max_leverage = Some max_leverage
           }
       | _ -> None)
  ;;

  let get_open_orders ~coin = Feed.get_perp_open_orders ~coin |> List.map order_of_feed
end
