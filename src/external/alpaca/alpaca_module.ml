(** Alpaca Exchange Adapter. Implements Exchange_intf.S. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Config = struct
  include Alpaca_types.Config

  let set_testnet testnet =
    Alpaca_types.Config.set_testnet testnet;
    Alpaca_market_hours.paper_mode := testnet
  ;;
end

module Alpaca_impl = struct
  let name = "alpaca"
  let section = "alpaca_module"
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16

  let string_of_order_type = function
    | Types.Limit -> "limit"
    | Types.Market -> "market"
    | Types.StopLoss -> "stop"
    | Types.TakeProfit -> "limit"
    | Types.StopLossLimit -> "stop_limit"
    | Types.TakeProfitLimit -> "limit"
    | Types.Other s -> s
    | _ -> "limit"
  ;;

  let alpaca_side_of_side = function
    | Types.Buy -> Alpaca_types.Buy
    | Types.Sell -> Alpaca_types.Sell
  ;;

  let string_of_time_in_force = function
    | Types.GTC -> "GTC"
    | Types.IOC -> "IOC"
    | Types.FOK -> "FOK"
  ;;

  let place_order
        ~token:_
        ~order_type
        ~side
        ~qty
        ~symbol
        ?limit_price
        ?time_in_force
        ?post_only:_
        ?reduce_only:_
        ?order_userref:_
        ?cl_ord_id
        ?trigger_price:_
        ?display_qty:_
        ?retry_config:_
        ()
    =
    let alpaca_side = alpaca_side_of_side side in
    let type_str = string_of_order_type order_type in
    let tif_str = Option.map string_of_time_in_force time_in_force in
    Alpaca_rest.place_order
      ~symbol
      ~qty
      ~side:alpaca_side
      ~order_type:type_str
      ?limit_price
      ?time_in_force:tif_str
      ?cl_ord_id
      ()
    >|= function
    | Ok res ->
      Ok
        { Types.order_id = res.order_id
        ; cl_ord_id = res.cl_ord_id
        ; order_userref = res.order_userref
        }
    | Error e -> Error e
  ;;

  let amend_order
        ~token:_
        ~order_id
        ?cl_ord_id
        ?qty
        ?limit_price
        ?post_only:_
        ?trigger_price:_
        ?display_qty:_
        ?symbol
        ?retry_config:_
        ()
    =
    let is_fractional =
      match qty with
      | Some q -> Float.floor q <> q
      | None -> false
    in
    let remove_old_order orig_id =
      match symbol with
      | Some s -> Alpaca_executions.remove_open_order s orig_id
      | None ->
        List.iter
          (fun s -> Alpaca_executions.remove_open_order s orig_id)
          (Alpaca_executions.get_all_symbols ())
    in
    let perform_rest_amend () =
      Alpaca_rest.amend_order ~order_id ?qty ?limit_price ?cl_ord_id ()
      >|= function
      | Ok res ->
        remove_old_order res.original_order_id;
        Ok
          { Types.original_order_id = res.original_order_id
          ; new_order_id = res.new_order_id
          ; amend_id = res.amend_id
          ; cl_ord_id = res.cl_ord_id
          }
      | Error e -> Error e
    in
    let fallback_cancel_replace sym q lp side =
      Logging.debug_f
        ~section
        "Alpaca fallback cancel & replace for fractional order %s (%s %s %.4f @ %.4f)"
        order_id
        sym
        (match side with
         | Alpaca_types.Buy -> "buy"
         | Sell -> "sell")
        q
        lp;
      Alpaca_rest.cancel_order order_id
      >>= function
      | Ok _ ->
        remove_old_order order_id;
        Alpaca_rest.place_order
          ~symbol:sym
          ~qty:q
          ~side
          ~order_type:"limit"
          ~limit_price:lp
          ?cl_ord_id
          ()
        >|= (function
         | Ok res ->
           Ok
             { Types.original_order_id = order_id
             ; new_order_id = res.order_id
             ; amend_id = Some res.order_id
             ; cl_ord_id = res.cl_ord_id
             }
         | Error e -> Error (Printf.sprintf "Fallback place_order failed: %s" e))
      | Error e ->
        Lwt.return (Error (Printf.sprintf "Fallback cancel_order failed: %s" e))
    in
    let find_existing () =
      match symbol with
      | Some s -> Alpaca_executions.get_open_order s order_id
      | None ->
        let all_syms = Alpaca_executions.get_all_symbols () in
        List.find_map (fun s -> Alpaca_executions.get_open_order s order_id) all_syms
    in
    if is_fractional
    then (
      match find_existing (), qty, limit_price with
      | Some existing, Some q, Some lp ->
        fallback_cancel_replace
          existing.symbol
          q
          lp
          (match existing.side with
           | Buy -> Alpaca_types.Buy
           | Sell -> Sell)
      | _ -> perform_rest_amend ())
    else
      perform_rest_amend ()
      >>= function
      | Ok res -> Lwt.return (Ok res)
      | Error e ->
        (match find_existing (), qty, limit_price with
         | Some existing, Some q, Some lp ->
           fallback_cancel_replace
             existing.symbol
             q
             lp
             (match existing.side with
              | Buy -> Alpaca_types.Buy
              | Sell -> Sell)
         | _ -> Lwt.return (Error e))
  ;;

  let cancel_orders
        ~token:_
        ?order_ids
        ?cl_ord_ids:_
        ?order_userrefs:_
        ?symbol:_
        ?retry_config:_
        ()
    =
    match order_ids with
    | Some ids ->
      Lwt_list.map_s Alpaca_rest.cancel_order ids
      >|= fun results ->
      let succs =
        List.filter_map
          (function
            | Ok r -> Some r
            | Error _ -> None)
          results
        |> List.concat
      in
      let mapped =
        List.map
          (fun (r : Alpaca_types.cancel_order_result) ->
             { Types.order_id = r.order_id; cl_ord_id = r.cl_ord_id })
          succs
      in
      if mapped <> [] then Ok mapped else Error "Failed to cancel orders"
    | None -> Lwt.return (Error "No order_ids provided for Alpaca cancellation")
  ;;

  let get_top_of_book ~symbol = Alpaca_orderbook.get_best_bid_ask symbol
  let get_top_of_book_fast ~symbol = Alpaca_orderbook.get_best_bid_ask_fast symbol
  let get_tradeable_balance ~asset = Alpaca_balances.get_balance asset
  let get_tradeable_balance_fast ~asset = fun () -> Alpaca_balances.get_balance asset
  let get_balance_age_fast ~asset:_ = fun () -> Alpaca_balances.get_balance_age ()
  let get_total_balance ~asset = Alpaca_balances.get_total_balance asset
  let get_staked_balance ~asset:_ = 0.0
  let get_all_balances () = Alpaca_balances.get_all_balances ()

  let get_open_order ~symbol ~order_id =
    match Alpaca_executions.get_open_order symbol order_id with
    | Some o ->
      Some
        { Types.order_id = o.order_id
        ; symbol = o.symbol
        ; side = o.side
        ; qty = o.qty
        ; cum_qty = o.cum_qty
        ; remaining_qty = o.remaining_qty
        ; limit_price = o.limit_price
        ; status = o.status
        ; user_ref = o.user_ref
        ; cl_ord_id = o.cl_ord_id
        }
    | None -> None
  ;;

  let get_open_orders ~symbol =
    let orders = Alpaca_executions.get_open_orders symbol in
    List.map
      (fun (o : Alpaca_executions.open_order_internal) ->
         { Types.order_id = o.order_id
         ; symbol = o.symbol
         ; side = o.side
         ; qty = o.qty
         ; cum_qty = o.cum_qty
         ; remaining_qty = o.remaining_qty
         ; limit_price = o.limit_price
         ; status = o.status
         ; user_ref = o.user_ref
         ; cl_ord_id = o.cl_ord_id
         })
      orders
  ;;

  let get_all_orders_for_asset ~asset =
    let all_symbols = Alpaca_executions.get_all_symbols () in
    let matching =
      List.filter
        (fun sym -> sym = asset || String.starts_with ~prefix:(asset ^ "/") sym)
        all_symbols
    in
    List.concat_map (fun symbol -> get_open_orders ~symbol) matching
  ;;

  let subscribe_orderbook ~symbols = Alpaca_orderbook.subscribe_symbols symbols
  let get_orderbook_position ~symbol = Alpaca_orderbook.get_current_position symbol

  let get_orderbook_position_fast ~symbol =
    Alpaca_orderbook.get_current_position_fast symbol
  ;;

  let read_orderbook_events ~symbol ~start_pos =
    Alpaca_orderbook.read_orderbook_events symbol start_pos
  ;;

  let iter_orderbook_events ~symbol ~start_pos f =
    Alpaca_orderbook.iter_orderbook_events symbol start_pos f
  ;;

  let iter_top_of_book_events ~symbol ~start_pos f =
    Alpaca_orderbook.iter_orderbook_events symbol start_pos (fun ob ->
      if Array.length ob.bids > 0 && Array.length ob.asks > 0
      then f (fst ob.bids.(0)) (snd ob.bids.(0)) (fst ob.asks.(0)) (snd ob.asks.(0)))
  ;;

  let get_execution_feed_position ~symbol = Alpaca_executions.get_current_position symbol

  let get_execution_feed_position_fast ~symbol =
    Alpaca_executions.get_current_position_fast symbol
  ;;

  let has_execution_data ~symbol = Alpaca_executions.has_execution_data symbol
  let has_execution_data_fast ~symbol = Alpaca_executions.has_execution_data_fast symbol

  let read_execution_events ~symbol ~start_pos =
    let events = Alpaca_executions.read_execution_events symbol start_pos in
    List.map
      (fun (e : Alpaca_executions.execution_event_internal) ->
         { Types.order_id = e.order_id
         ; order_status = e.order_status
         ; limit_price = e.limit_price
         ; side = e.side
         ; remaining_qty = e.remaining_qty
         ; filled_qty = e.filled_qty
         ; avg_price = e.avg_price
         ; timestamp = e.timestamp
         ; is_amended = e.is_amended
         ; cl_ord_id = e.cl_ord_id
         })
      events
  ;;

  let iter_execution_events ~symbol ~start_pos f =
    Alpaca_executions.iter_execution_events
      symbol
      start_pos
      (fun (e : Alpaca_executions.execution_event_internal) ->
         f
           { Types.order_id = e.order_id
           ; order_status = e.order_status
           ; limit_price = e.limit_price
           ; side = e.side
           ; remaining_qty = e.remaining_qty
           ; filled_qty = e.filled_qty
           ; avg_price = e.avg_price
           ; timestamp = e.timestamp
           ; is_amended = e.is_amended
           ; cl_ord_id = e.cl_ord_id
           })
  ;;

  let fold_open_orders ~symbol ~init ~f =
    Alpaca_executions.fold_open_orders
      symbol
      ~init
      ~f:(fun acc (o : Alpaca_executions.open_order_internal) ->
        f
          acc
          { Types.order_id = o.order_id
          ; symbol = o.symbol
          ; side = o.side
          ; qty = o.qty
          ; cum_qty = o.cum_qty
          ; remaining_qty = o.remaining_qty
          ; limit_price = o.limit_price
          ; status = o.status
          ; user_ref = o.user_ref
          ; cl_ord_id = o.cl_ord_id
          })
  ;;

  let iter_open_orders_fast ~symbol f =
    Alpaca_executions.fold_open_orders
      symbol
      ~init:()
      ~f:(fun () (o : Alpaca_executions.open_order_internal) ->
        let limit_price =
          match o.limit_price with
          | Some p -> p
          | None -> 0.0
        in
        let side_str =
          match o.side with
          | Types.Buy -> "buy"
          | Sell -> "sell"
        in
        f o.order_id limit_price o.remaining_qty side_str o.user_ref)
  ;;

  let get_price_increment ~symbol:_ = Some 0.01

  let get_qty_increment ~symbol:_ =
    Some 0.000000001 (* Native Alpaca 9-decimal fractional precision *)
  ;;

  let get_qty_min ~symbol:_ = Some 0.000000001
  let fetch_open_orders () = Alpaca_executions.bootstrap_open_orders ()
  let round_price ~symbol:_ ~price = Float.round (price *. 100.0) /. 100.0
  let get_fees ~symbol:_ = Some 0.0, Some 0.0 (* Alpaca commission-free *)
end

let () = Exchange.Registry.register (module Alpaca_impl)

(* Register the oracle data-venue adapter (historical bars, market calendar,
   balances, fees for the capital oracle) at load time. *)
let () = Exchange.Oracle.Registry.register (module Alpaca_oracle)
