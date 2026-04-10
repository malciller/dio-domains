(**
   Kraken exchange adapter.

   Implements [Exchange_intf.S] for the Kraken exchange. Converts between
   Kraken-specific types and the unified Dio [Types] domain, delegates order
   actions to [Kraken_actions], and reads market data from the per-symbol
   WebSocket feed caches ([Kraken_ticker_feed], [Kraken_orderbook_feed],
   [Kraken_executions_feed], [Kraken_balances_feed]).

   Registered into [Exchange.Registry] at module load time via side-effecting
   [let () = ...] at the bottom of this file.
*)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Kraken_impl = struct
  let name = "kraken"
  let section = "kraken_module"

  (** In-memory cache mapping symbol to (maker_fee, taker_fee).
      Populated once at startup by [initialize_fees]. *)
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16

  (** Map unified [Types.order_type] to the Kraken API string representation. *)
  let string_of_order_type = function
    | Types.Limit -> "limit"
    | Types.Market -> "market"
    | Types.StopLoss -> "stop-loss"
    | Types.TakeProfit -> "take-profit"
    | Types.StopLossLimit -> "stop-loss-limit"
    | Types.TakeProfitLimit -> "take-profit-limit"
    | Types.SettlPosition -> "settl-position"
    | Types.Other s -> s

  (** Map unified [Types.side] to Kraken API side string. *)
  let string_of_side = function
    | Types.Buy -> "buy"
    | Types.Sell -> "sell"

  (** Map unified [Types.time_in_force] to Kraken API TIF string. *)
  let string_of_time_in_force = function
    | Types.GTC -> "GTC"
    | Types.IOC -> "IOC"
    | Types.FOK -> "FOK"

  (** Convert Kraken execution feed order status to unified [Types] status. *)
  let status_of_kraken_status = function
    | Kraken_executions_feed.PendingNewStatus -> Types.Pending
    | Kraken_executions_feed.NewStatus -> Types.New
    | Kraken_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Kraken_executions_feed.FilledStatus -> Types.Filled
    | Kraken_executions_feed.CanceledStatus -> Types.Canceled
    | Kraken_executions_feed.ExpiredStatus -> Types.Expired
    | Kraken_executions_feed.RejectedStatus -> Types.Rejected
    | Kraken_executions_feed.UnknownStatus s -> Types.Unknown s
  
  (** Convert Kraken execution feed side to unified [Types.side]. *)
  let side_of_kraken_side = function
    | Kraken_executions_feed.Buy -> Types.Buy
    | Kraken_executions_feed.Sell -> Types.Sell

  (** Convert optional unified retry config to [Kraken_actions.retry_config]. *)
  let convert_retry_config (config: Types.retry_config option) =
    match config with
    | Some c -> Some {
        Kraken_actions.max_attempts = c.max_attempts;
        base_delay_ms = c.base_delay_ms;
        max_delay_ms = c.max_delay_ms;
        backoff_factor = c.backoff_factor;
      }
    | None -> None

  (** Submit a new order via [Kraken_actions.place_order].
      Converts unified types to Kraken API strings, delegates the REST/WS call,
      and maps the response back to [Types.place_order_result]. *)
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

  (** Amend an existing order via [Kraken_actions.amend_order].

      Requires the order to exist in the local execution feed cache before
      dispatching. If absent (e.g. due to WS reconnect lag) returns [Error]
      immediately so the strategy can recover via fresh placement.

      When [qty] is not provided, the cached [order_qty] is used as the
      effective quantity, matching the Hyperliquid adapter convention.

      On success, proactively updates the cached limit price in
      [Kraken_executions_feed] so that the order executor's duplicate-check
      sees the new price before the async WS amended event arrives. *)
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

    (* Validate existence in local WS cache before dispatching the amend.
       Returns Error if missing so the strategy can fall back to re-placement. *)
    let sym = Option.value symbol ~default:"" in
    match Kraken_executions_feed.find_order_everywhere order_id with
    | None ->
        Logging.warn_f ~section "amend_order: order %s not in local cache%s — returning error for clean recovery"
          order_id (if sym <> "" then Printf.sprintf " [%s]" sym else "");
        Lwt.return (Error (Printf.sprintf "Order not found for amendment: %s" order_id))
    | Some existing ->
        (* Default to the cached order_qty when caller omits qty. *)
        let effective_qty = match qty with Some q -> q | None -> existing.order_qty in
        let actual_retry_config = convert_retry_config retry_config in

        Kraken_actions.amend_order
          ~token
          ~order_id
          ?cl_ord_id
          ~order_qty:effective_qty
          ?limit_price
          ?post_only
          ?trigger_price
          ?display_qty
          ?symbol
          ?retry_config:actual_retry_config
          ()
        >|= function
        | Ok (res : Kraken_common_types.amend_order_result) ->
            (* Eagerly update cached limit_price so the executor sees the new
               price before the async WS amended event arrives. *)
            (match limit_price with
             | Some new_price when sym <> "" ->
                 Kraken_executions_feed.update_open_order_price
                   ~symbol:sym ~order_id ~new_price
             | _ -> ());
            Ok {
              Types.original_order_id = res.order_id;
              Types.new_order_id = res.order_id;
              Types.amend_id = Some res.amend_id;
              Types.cl_ord_id = res.cl_ord_id;
            }
        | Error e -> Error e

  (** Cancel one or more orders via [Kraken_actions.cancel_orders].
      Accepts any combination of order_ids, cl_ord_ids, or order_userrefs.
      Returns a list of [Types.cancel_order_result] on success. *)
  let cancel_orders
      ~token
      ?order_ids
      ?cl_ord_ids
      ?order_userrefs
      ?retry_config
      () =
    
    let actual_retry_config = convert_retry_config retry_config in
    
    (* Delegate directly; Kraken_actions accepts optional list parameters. *)
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

  (* -- Market data accessors ------------------------------------------- *)

  (** Return the latest (bid, ask) from the ticker feed cache, or [None]. *)

  let get_ticker ~symbol =
    match Kraken_ticker_feed.get_latest_ticker symbol with
    | Some t -> Some (t.bid, t.ask)
    | None -> None

  (** Request the ticker feed to begin tracking [symbol]. *)
  let subscribe_ticker ~symbol =
    Kraken_ticker_feed.subscribe_ticker symbol

  (** Return best bid/ask as [(bid_price, bid_size, ask_price, ask_size)] floats.
      Directly consumes float values provided by [Kraken_orderbook_feed.get_best_bid_ask]. *)
  let get_top_of_book ~symbol =
    match Kraken_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) -> Some (bp, bs, ap, as_val)
    | None -> None

  (** Return the current balance for [asset] from the balances feed cache. *)
  let get_balance ~asset =
    Kraken_balances_feed.get_balance asset

  (** Return all assets with a positive balance as [(asset, balance)] pairs. *)
  let get_all_balances () =
    let assets = Kraken_balances_feed.get_all_assets () in
    List.filter_map (fun asset ->
      let bal = Kraken_balances_feed.get_balance asset in
      if bal > 0.0 then Some (asset, bal) else None
    ) assets

  (** Look up a single open order by symbol and order_id in the executions
      feed cache. Returns a unified [Types.open_order option]. *)
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

  (** Return all open orders for [symbol] from the executions feed cache,
      mapped to unified [Types.open_order] records. *)
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

  (** Return the current ring-buffer position for execution events on [symbol]. *)
  let get_execution_feed_position ~symbol =
    Kraken_executions_feed.get_current_position symbol

  (** Return [true] once the initial execution snapshot has been ingested for [symbol]. *)
  let has_execution_data ~symbol =
    Kraken_executions_feed.has_execution_data symbol

  (** Read execution events from [start_pos] onward for [symbol].
      Derives [remaining_qty] from [order_qty - cum_qty] and coerces status
      to [Filled] when remaining quantity reaches zero to handle cases where
      the WS event status lags behind the cumulative fill quantity. *)
  let read_execution_events ~symbol ~start_pos =
    let events = Kraken_executions_feed.read_execution_events symbol start_pos in
    List.map (fun (e : Kraken_executions_feed.execution_event) ->
      let remaining_qty = e.order_qty -. e.cum_qty in
      let effective_status =
        if remaining_qty <= 0.0 && e.order_qty > 0.0 then Types.Filled
        else status_of_kraken_status e.order_status
      in
      { Types.
        order_id = e.order_id;
        order_status = effective_status;
        limit_price = e.limit_price;
        side = side_of_kraken_side e.side;
        remaining_qty;
        filled_qty = e.cum_qty;
        avg_price = e.avg_price;
        timestamp = e.timestamp;
        is_amended = (e.exec_type = Kraken_executions_feed.Amended);
      }
    ) events

  (** Iterate execution events from [start_pos] for [symbol], applying [f]
      to each event after converting to unified [Types.execution_event].
      Uses the same fill-derived status coercion as [read_execution_events]. *)
  let iter_execution_events ~symbol ~start_pos f =
    Kraken_executions_feed.iter_execution_events symbol start_pos (fun (e : Kraken_executions_feed.execution_event) ->
      let remaining_qty = e.order_qty -. e.cum_qty in
      let effective_status =
        if remaining_qty <= 0.0 && e.order_qty > 0.0 then Types.Filled
        else status_of_kraken_status e.order_status
      in
      f { Types.
        order_id = e.order_id;
        order_status = effective_status;
        limit_price = e.limit_price;
        side = side_of_kraken_side e.side;
        remaining_qty;
        filled_qty = e.cum_qty;
        avg_price = e.avg_price;
        timestamp = e.timestamp;
        is_amended = (e.exec_type = Kraken_executions_feed.Amended);
      }
    )

  (** Return the current ring-buffer position for ticker events on [symbol]. *)
  let get_ticker_position ~symbol =
    Kraken_ticker_feed.get_current_position symbol

  (** Read ticker events from [start_pos] onward for [symbol], mapped to
      unified [Types.ticker_event] records containing bid, ask, timestamp. *)
  let read_ticker_events ~symbol ~start_pos =
    let events = Kraken_ticker_feed.read_ticker_events symbol start_pos in
    List.map (fun (t : Kraken_ticker_feed.ticker) ->
      { Types.
        bid = t.bid;
        ask = t.ask;
        timestamp = t.timestamp;
      }
    ) events

  (** Iterate ticker events from [start_pos] for [symbol], applying [f]. *)
  let iter_ticker_events ~symbol ~start_pos f =
    Kraken_ticker_feed.iter_ticker_events symbol start_pos (fun (t : Kraken_ticker_feed.ticker) ->
      f { Types. bid = t.bid; ask = t.ask; timestamp = t.timestamp }
    )

  (** Return the current ring-buffer position for orderbook events on [symbol]. *)
  let get_orderbook_position ~symbol =
    Kraken_orderbook_feed.get_current_position symbol

  (** Read orderbook snapshots from [start_pos] for [symbol]. Each snapshot's
      levels are converted from [Kraken_orderbook_feed.level] (with separate
      string/float fields) to [(price, size)] float tuples in arrays. *)
  let read_orderbook_events ~symbol ~start_pos =
    let events = Kraken_orderbook_feed.read_orderbook_events symbol start_pos in
    List.map (fun (ob : Kraken_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Kraken_orderbook_feed.level) ->
          l.price_float,
          l.size_float
        ) levels
      in
      { Types.
        bids = map_levels ob.bids;
        asks = map_levels ob.asks;
        timestamp = ob.timestamp;
      }
    ) events

  (** Iterate orderbook snapshots from [start_pos] for [symbol], applying [f]
      to each snapshot after level conversion. *)
  let iter_orderbook_events ~symbol ~start_pos f =
    Kraken_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Kraken_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Kraken_orderbook_feed.level) ->
          l.price_float,
          l.size_float
        ) levels
      in
      f { Types. bids = map_levels ob.bids; asks = map_levels ob.asks; timestamp = ob.timestamp }
    )

  (** Zero-allocation top-of-book iterator. Reads price_float/size_float
      directly from Kraken level records without building converted arrays. *)
  let iter_top_of_book_events ~symbol ~start_pos f =
    Kraken_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Kraken_orderbook_feed.orderbook) ->
      if Array.length ob.bids > 0 && Array.length ob.asks > 0 then begin
        let bid = ob.bids.(0) in
        let ask = ob.asks.(0) in
        f bid.price_float bid.size_float ask.price_float ask.size_float
      end
    )

  (** Fold over all open orders for [symbol] with accumulator [init] and
      function [f], converting each order to unified [Types.open_order]. *)
  let fold_open_orders ~symbol ~init ~f =
    Kraken_executions_feed.fold_open_orders symbol ~init ~f:(fun acc (o : Kraken_executions_feed.open_order) ->
      f acc { Types.
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
    )

  let iter_open_orders_fast ~symbol f =
    Kraken_executions_feed.fold_open_orders symbol ~init:() ~f:(fun () (o : Kraken_executions_feed.open_order) ->
      let limit_price = match o.limit_price with Some p -> p | None -> 0.0 in
      let side_str = match o.side with Kraken_executions_feed.Buy -> "buy" | Sell -> "sell" in
      f o.order_id limit_price o.remaining_qty side_str o.order_userref
    )

  (* -- Instrument metadata accessors ----------------------------------- *)


  (** Return the minimum price tick size for [symbol], or [None] if unknown. *)
  let get_price_increment ~symbol =
    Kraken_instruments_feed.get_price_increment symbol

  (** Return the minimum quantity step for [symbol], or [None] if unknown. *)
  let get_qty_increment ~symbol =
    Kraken_instruments_feed.get_qty_increment symbol

  (** Return the minimum order quantity for [symbol], or [None] if unknown. *)
  let get_qty_min ~symbol =
    Kraken_instruments_feed.get_qty_min symbol

  (** Round [price] to the nearest valid tick for [symbol].
      Falls back to the original value if no price increment is known. *)
  let round_price ~symbol ~price =
    match Kraken_instruments_feed.get_price_increment symbol with
    | Some inc -> Float.round (price /. inc) *. inc
    | None -> price

  (** Return [(maker_fee option, taker_fee option)] from the local fee cache. *)
  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some f -> (Some (fst f), Some (snd f))
    | None -> (None, None)

  (** Fetch fee schedules for [symbols] via [Kraken_get_fee] and populate
      [fee_cache]. Intended to be called once at application startup. *)
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

(* Register Kraken_impl into the global exchange registry at load time. *)
let () =
  Exchange.Registry.register (module Kraken_impl)
