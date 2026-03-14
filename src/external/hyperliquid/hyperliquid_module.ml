(** Hyperliquid Exchange Implementation *)

module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Hyperliquid_impl = struct
  let name = "hyperliquid"
  let section = "hyperliquid_module"

  let string_of_order_type = function
    | Types.Limit -> "limit"
    | Types.Market -> "market"
    | Types.StopLoss -> "stop-loss"
    | Types.TakeProfit -> "take-profit"
    | Types.StopLossLimit -> "stop-loss-limit"
    | Types.TakeProfitLimit -> "take-profit-limit"
    | Types.SettlPosition -> "settl-position"
    | Types.Other s -> s

  let string_of_side = function
    | Types.Buy -> "buy"
    | Types.Sell -> "sell"

  let string_of_time_in_force = function
    | Types.GTC -> "GTC"
    | Types.IOC -> "IOC"
    | Types.FOK -> "FOK"

  let status_of_hl_status = function
    | Hyperliquid_executions_feed.PendingNewStatus -> Types.Pending
    | Hyperliquid_executions_feed.NewStatus -> Types.New
    | Hyperliquid_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Hyperliquid_executions_feed.FilledStatus -> Types.Filled
    | Hyperliquid_executions_feed.CanceledStatus -> Types.Canceled
    | Hyperliquid_executions_feed.ExpiredStatus -> Types.Expired
    | Hyperliquid_executions_feed.RejectedStatus -> Types.Rejected
    | Hyperliquid_executions_feed.UnknownStatus s -> Types.Unknown s
  
  let side_of_hl_side = function
    | Hyperliquid_executions_feed.Buy -> Types.Buy
    | Hyperliquid_executions_feed.Sell -> Types.Sell

  (** Place a new order *)
  let place_order
      ~token
      ~order_type
      ~side
      ~qty
      ~symbol
      ?limit_price
      ?time_in_force
      ?post_only
      ?reduce_only
      ?order_userref
      ?cl_ord_id
      ?trigger_price
      ?display_qty
      ?retry_config
      () =
    
    Hyperliquid_actions.place_order
      ~token
      ~order_type
      ~side
      ~order_qty:qty
      ~symbol
      ?limit_price
      ?time_in_force
      ?post_only
      ?reduce_only
      ?order_userref
      ?cl_ord_id
      ?trigger_price
      ?display_qty
      ?retry_config
      ()

  (** Amend an existing order *)
  let amend_order
      ~token
      ~order_id
      ?cl_ord_id
      ?qty
      ?limit_price
      ?post_only
      ?trigger_price
      ?display_qty
      ?symbol
      ?retry_config
      () =
    
    Hyperliquid_actions.amend_order
      ~token
      ~order_id
      ?cl_ord_id
      ?order_qty:qty
      ?limit_price
      ?post_only
      ?trigger_price
      ?display_qty
      ?symbol
      ?retry_config
      ()

  (** Cancel orders *)
  let cancel_orders
      ~token
      ?order_ids
      ?cl_ord_ids
      ?order_userrefs
      ?retry_config
      () =
    
    Hyperliquid_actions.cancel_orders
      ~token
      ?order_ids
      ?cl_ord_ids
      ?order_userrefs
      ?retry_config
      ()

  (** Market Data Access *)
  
  let get_ticker ~symbol =
    match Hyperliquid_ticker_feed.get_latest_ticker symbol with
    | Some (t : Hyperliquid_ticker_feed.ticker) -> Some (t.bid, t.ask)
    | None -> None

  let get_top_of_book ~symbol =
    match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) -> 
        let safe_float s = try float_of_string s with _ -> 0.0 in
        Some (safe_float bp, safe_float bs, safe_float ap, safe_float as_val)
    | None -> None

  let get_balance ~asset =
    Hyperliquid_balances_feed.get_balance asset

  let get_open_order ~symbol ~order_id =
    match Hyperliquid_executions_feed.get_open_order symbol order_id with
    | Some (o : Hyperliquid_executions_feed.open_order) -> Some {
        Types.order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_hl_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_hl_status o.order_status;
        user_ref = o.order_userref;
        cl_ord_id = o.cl_ord_id;
      }
    | None -> None

  let get_open_orders ~symbol =
    let orders = Hyperliquid_executions_feed.get_open_orders symbol in
    List.map (fun (o : Hyperliquid_executions_feed.open_order) ->
      { Types.
        order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_hl_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_hl_status o.order_status;
        user_ref = o.order_userref;
        cl_ord_id = o.cl_ord_id;
      }
    ) orders

  let get_execution_feed_position ~symbol =
    Hyperliquid_executions_feed.get_current_position symbol

  let read_execution_events ~symbol ~start_pos =
    let events = Hyperliquid_executions_feed.read_execution_events symbol start_pos in
    List.map (fun (e : Hyperliquid_executions_feed.execution_event) ->
      { Types.
        order_id = e.order_id;
        order_status = status_of_hl_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_hl_side e.side;
        remaining_qty = e.order_qty -. e.cum_qty;
        filled_qty = e.cum_qty;
        timestamp = e.timestamp;
      }
    ) events

  let get_ticker_position ~symbol =
    Hyperliquid_ticker_feed.get_current_position symbol

  let read_ticker_events ~symbol ~start_pos =
    let events = Hyperliquid_ticker_feed.read_ticker_events symbol start_pos in
    List.map (fun (t : Hyperliquid_ticker_feed.ticker) ->
      { Types.
        bid = t.bid;
        ask = t.ask;
        timestamp = t.timestamp;
      }
    ) events

  let get_orderbook_position ~symbol =
    Hyperliquid_orderbook_feed.get_current_position symbol

  let read_orderbook_events ~symbol ~start_pos =
    let events = Hyperliquid_orderbook_feed.read_orderbook_events symbol start_pos in
    List.map (fun (ob : Hyperliquid_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Hyperliquid_orderbook_feed.level) ->
          (try float_of_string l.price with _ -> 0.0),
          (try float_of_string l.size with _ -> 0.0)
        ) levels
      in
      { Types.
        bids = map_levels ob.bids;
        asks = map_levels ob.asks;
        timestamp = ob.timestamp;
      }
    ) events

  let get_price_increment ~symbol =
    Hyperliquid_instruments_feed.get_price_increment symbol

  let get_qty_increment ~symbol =
    Hyperliquid_instruments_feed.get_qty_increment symbol

  let get_qty_min ~symbol =
    Hyperliquid_instruments_feed.get_qty_min symbol

  let get_fees ~symbol:_ =
    (None, None) (* Implement dynamically via info endpoint later if needed *)

end

(* Register the module *)
let () =
  Exchange.Registry.register (module Hyperliquid_impl)
