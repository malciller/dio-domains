(** Kraken Exchange Implementation *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Kraken_impl = struct
  let name = "kraken"
  let section = "kraken_module"

  (* Internal cache for fees *)
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16

  (* Helpers for type conversion *)
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

  let status_of_kraken_status = function
    | Kraken_executions_feed.PendingNewStatus -> Types.Pending
    | Kraken_executions_feed.NewStatus -> Types.New
    | Kraken_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Kraken_executions_feed.FilledStatus -> Types.Filled
    | Kraken_executions_feed.CanceledStatus -> Types.Canceled
    | Kraken_executions_feed.ExpiredStatus -> Types.Expired
    | Kraken_executions_feed.RejectedStatus -> Types.Rejected
    | Kraken_executions_feed.UnknownStatus s -> Types.Unknown s
  
  let side_of_kraken_side = function
    | Kraken_executions_feed.Buy -> Types.Buy
    | Kraken_executions_feed.Sell -> Types.Sell

  let convert_retry_config (config: Types.retry_config option) =
    match config with
    | Some c -> Some {
        Kraken_actions.max_attempts = c.max_attempts;
        base_delay_ms = c.base_delay_ms;
        max_delay_ms = c.max_delay_ms;
        backoff_factor = c.backoff_factor;
      }
    | None -> None

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
    
    let kraken_order_type = string_of_order_type order_type in
    let kraken_side = string_of_side side in
    let time_in_force_str = Option.map string_of_time_in_force time_in_force in
    let actual_retry_config = convert_retry_config retry_config in

    Kraken_actions.place_order
      ~token
      ~order_type:kraken_order_type
      ~side:kraken_side
      ~order_qty:qty
      ~symbol
      ?limit_price
      ?time_in_force:time_in_force_str
      ?post_only
      ?reduce_only
      ?order_userref
      ?cl_ord_id
      ?trigger_price
      ?display_qty
      ?retry_config:actual_retry_config
      ()
    >|= function
    | Ok (res : Kraken_common_types.add_order_result) ->
        Ok {
          Types.order_id = res.order_id;
          cl_ord_id = res.cl_ord_id;
          order_userref = res.order_userref;
        }
    | Error e -> Error e

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
    
    let actual_retry_config = convert_retry_config retry_config in

    Kraken_actions.amend_order
      ~token
      ~order_id
      ?cl_ord_id
      ?order_qty:qty
      ?limit_price
      ?post_only
      ?trigger_price
      ?display_qty
      ?symbol
      ?retry_config:actual_retry_config
      ()
    >|= function
    | Ok (res : Kraken_common_types.amend_order_result) ->
        Ok {
          Types.amend_id = res.amend_id;
          order_id = res.order_id;
          cl_ord_id = res.cl_ord_id;
        }
    | Error e -> Error e

  (** Cancel orders *)
  let cancel_orders
      ~token
      ?order_ids
      ?cl_ord_ids
      ?order_userrefs
      ?retry_config
      () =
    
    let actual_retry_config = convert_retry_config retry_config in
    
    (* Kraken_actions.cancel_orders takes lists directly *)
    Kraken_actions.cancel_orders
      ~token
      ?order_ids
      ?cl_ord_ids
      ?order_userrefs
      ?retry_config:actual_retry_config
      ()
    >|= function
    | Ok (res_list : Kraken_common_types.cancel_order_result list) ->
        let mapped = List.map (fun (r : Kraken_common_types.cancel_order_result) ->
          { Types.order_id = r.order_id;
            cl_ord_id = r.cl_ord_id
          }
        ) res_list in
        Ok mapped
    | Error e -> Error e

  (** Market Data Access *)
  
  let get_ticker ~symbol =
    match Kraken_ticker_feed.get_latest_ticker symbol with
    | Some t -> Some (t.bid, t.ask)
    | None -> None

  let get_top_of_book ~symbol =
    match Kraken_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) -> 
        (* Kraken_orderbook_feed returns (bid_price, bid_size, ask_price, ask_size) strings/floats? 
           Checking `get_best_bid_ask`: returns `Some (bid.price, bid.size, ask.price, ask.size)` 
           where price/size are strings in the `level` type but `price_float` exists.
           Wait, `get_best_bid_ask` returns STRINGS!
           I need to parse them to floats or use `get_latest_orderbook` directly.
           Checking `kraken_orderbook_feed.ml`:
             `Some (bid.price, bid.size, ask.price, ask.size)` -> Strings.
           I should update `get_best_bid_ask` or parse here.
           I'll parse here to be safe.
        *)
        let safe_float s = try float_of_string s with _ -> 0.0 in
        Some (safe_float bp, safe_float bs, safe_float ap, safe_float as_val)
    | None -> None

  let get_balance ~asset =
    Kraken_balances_feed.get_balance asset

  let get_open_order ~symbol ~order_id =
    match Kraken_executions_feed.get_open_order symbol order_id with
    | Some o -> Some {
        Types.order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_kraken_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_kraken_status o.order_status;
        user_ref = o.order_userref;
        cl_ord_id = o.cl_ord_id;
      }
    | None -> None

  let get_open_orders ~symbol =
    let orders = Kraken_executions_feed.get_open_orders symbol in
    List.map (fun (o : Kraken_executions_feed.open_order) ->
      { Types.
        order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_kraken_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_kraken_status o.order_status;
        user_ref = o.order_userref;
        cl_ord_id = o.cl_ord_id;
      }
    ) orders

  let get_execution_feed_position ~symbol =
    Kraken_executions_feed.get_current_position symbol

  let read_execution_events ~symbol ~start_pos =
    let events = Kraken_executions_feed.read_execution_events symbol start_pos in
    List.map (fun (e : Kraken_executions_feed.execution_event) ->
      { Types.
        order_id = e.order_id;
        order_status = status_of_kraken_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_kraken_side e.side;
        remaining_qty = e.order_qty -. e.cum_qty;
        filled_qty = e.cum_qty; (* Assuming cum_qty is filled_qty *)
        timestamp = e.timestamp;
      }
    ) events

  let get_ticker_position ~symbol =
    Kraken_ticker_feed.get_current_position symbol

  let read_ticker_events ~symbol ~start_pos =
    let events = Kraken_ticker_feed.read_ticker_events symbol start_pos in
    List.map (fun (t : Kraken_ticker_feed.ticker) ->
      { Types.
        bid = t.bid;
        ask = t.ask;
        timestamp = t.timestamp;
      }
    ) events

  let get_orderbook_position ~symbol =
    Kraken_orderbook_feed.get_current_position symbol

  let read_orderbook_events ~symbol ~start_pos =
    let events = Kraken_orderbook_feed.read_orderbook_events symbol start_pos in
    List.map (fun (ob : Kraken_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Kraken_orderbook_feed.level) ->
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

  (** Metadata Access *)
  
  let get_price_increment ~symbol =
    Kraken_instruments_feed.get_price_increment symbol

  let get_qty_increment ~symbol =
    Kraken_instruments_feed.get_qty_increment symbol

  let get_qty_min ~symbol =
    Kraken_instruments_feed.get_qty_min symbol

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some f -> (Some (fst f), Some (snd f))
    | None -> (None, None)

  (* Initialization helper - to be called by main app to populate fees etc *)
  let initialize_fees symbols =
    Lwt_list.iter_p (fun symbol ->
      Kraken_get_fee.get_fee_info symbol >|= function
      | Some info ->
          let maker = Option.value info.maker_fee ~default:0.0 in
          let taker = Option.value info.taker_fee ~default:0.0 in
          Hashtbl.replace fee_cache symbol (maker, taker)
      | None -> ()
    ) symbols
end

(* Register the module *)
let () =
  Exchange.Registry.register (module Kraken_impl)
