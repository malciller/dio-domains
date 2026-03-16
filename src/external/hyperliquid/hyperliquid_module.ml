(** Hyperliquid Exchange Implementation *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Hyperliquid_impl = struct
  let name = "hyperliquid"
  let section = "hyperliquid_module"

  (* Configuration *)
  let is_testnet = ref true
  let set_testnet testnet =
    is_testnet := testnet;
    Logging.info_f ~section "Hyperliquid module configured for %s" (if testnet then "testnet" else "mainnet")

  (* Internal cache for fees *)
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16

  (* Helpers for type conversion *)
  let string_of_order_type = function
    | Types.Limit -> "limit"
    | Types.Market -> "market"
    | Types.Other s -> s
    | _ -> "limit" (* Default to limit for others for now *)

  let string_of_side = function
    | Types.Buy -> "buy"
    | Types.Sell -> "sell"

  let status_of_hyperliquid_status = function
    | Hyperliquid_executions_feed.PendingNewStatus -> Types.New
    | Hyperliquid_executions_feed.NewStatus -> Types.New
    | Hyperliquid_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Hyperliquid_executions_feed.FilledStatus -> Types.Filled
    | Hyperliquid_executions_feed.CanceledStatus -> Types.Canceled
    | Hyperliquid_executions_feed.ExpiredStatus -> Types.Expired
    | Hyperliquid_executions_feed.RejectedStatus -> Types.Rejected
    | Hyperliquid_executions_feed.UnknownStatus s -> Types.Unknown s

  let side_of_hyperliquid_side = function
    | Hyperliquid_executions_feed.Buy -> Types.Buy
    | Hyperliquid_executions_feed.Sell -> Types.Sell

  (** Place a new order *)
  let place_order
      ~token:_ (* Hyperliquid uses wallet address and private key from env *)
      ~order_type
      ~side
      ~qty
      ~symbol
      ?limit_price
      ?time_in_force:_
      ?post_only
      ?reduce_only
      ?order_userref
      ?cl_ord_id
      ?trigger_price:_
      ?display_qty:_
      ?retry_config:_
      () =
    
    let is_limit = match order_type with Types.Limit -> true | _ -> false in
    let px = Option.value limit_price ~default:0.0 in
    let side_bool = match side with Types.Buy -> true | Types.Sell -> false in

    let px_rounded = Hyperliquid_instruments_feed.round_price_to_tick_for_symbol symbol px in
    let sz_rounded = Hyperliquid_instruments_feed.round_qty_to_lot symbol qty in

    let constructed_cl_ord_id = match order_userref with
      | Some uref -> Some (Printf.sprintf "0x%032x" uref)
      | None -> cl_ord_id
    in

    Hyperliquid_actions.place_order
      ~symbol
      ~is_buy:side_bool
      ~sz:sz_rounded
      ~px:px_rounded
      ~is_limit
      ?post_only
      ?reduce_only
      ?cl_ord_id:constructed_cl_ord_id
      ~testnet:!is_testnet
      ()
    >|= function
    | Ok res ->
        let order_id_str = Int64.to_string res.Hyperliquid_actions.order_id in
        (* Proactively populate open_orders so that find_order_everywhere succeeds
           when the strategy tries to amend immediately after placement.
           Without this, open_orders stays empty until the next webData2 (~1.5s),
           causing every amendment in that window to fail with "Order not found". *)
        let hl_side = match side with Types.Buy -> Hyperliquid_executions_feed.Buy | _ -> Hyperliquid_executions_feed.Sell in
        Hyperliquid_executions_feed.inject_order
          ~symbol
          ~order_id:order_id_str
          ~side:hl_side
          ~qty:sz_rounded
          ~price:px_rounded
          ?user_ref:(match order_userref with Some u -> Some u | None -> None)
          ?cl_ord_id:constructed_cl_ord_id
          ();
        Ok {
          Types.order_id = order_id_str;
          cl_ord_id = None;
          order_userref = None;
        }
    | Error e -> Error e

  (** Amend an existing order *)
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
      () =
    
    match symbol with
    | None -> Lwt.return (Error "Symbol is required for Hyperliquid amendment")
    | Some sym ->
        (* Lookup existing order to get side and current values if not provided *)
        match Hyperliquid_executions_feed.find_order_everywhere order_id with
        | None -> Lwt.return (Error (Printf.sprintf "Order not found for amendment: %s" order_id))
        | Some existing ->
            let is_buy = match existing.side with 
              | Hyperliquid_executions_feed.Buy -> true 
              | Hyperliquid_executions_feed.Sell -> false 
            in
            let px = match limit_price with
              | Some p -> p
              | None -> Option.value existing.limit_price ~default:0.0
            in
            let sz = Option.value qty ~default:existing.order_qty in
            
            let px_rounded = Hyperliquid_instruments_feed.round_price_to_tick_for_symbol sym px in
            let sz_rounded = Hyperliquid_instruments_feed.round_qty_to_lot sym sz in

            let constructed_cl_ord_id = match existing.order_userref with
              | Some uref -> Some (Printf.sprintf "0x%032x" uref)
              | None -> cl_ord_id
            in

            Hyperliquid_actions.amend_order
              ~symbol:sym
              ~order_id:(Int64.of_string order_id)
              ~is_buy
              ~px:px_rounded
              ~sz:sz_rounded
              ?cl_ord_id:constructed_cl_ord_id
              ~testnet:!is_testnet
              ()
        >|= function
        | Ok res ->
            Ok {
              Types.original_order_id = Int64.to_string res.order_id;
              Types.new_order_id = Int64.to_string res.amend_id;
              Types.amend_id = None;
              Types.cl_ord_id = constructed_cl_ord_id;
            }
        | Error e -> Error e

  (** Cancel orders *)
  let cancel_orders
      ~token:_
      ?order_ids
      ?cl_ord_ids:_
      ?order_userrefs:_
      ?retry_config:_
      () =
    
    match order_ids with
    | None -> Lwt.return (Ok [])
    | Some ids ->
        (* Hyperliquid cancel takes symbol and list of ids? 
           Actually Hyperliquid_actions.cancel_orders takes coin and list of ids. 
           But Exchange_intf doesn't pass coin here! 
           Wait, Kraken_actions.cancel_orders doesn't either, but it might not need it.
           Hyperliquid REQUIRES coin for cancellation.
           Wait, there's another cancel that takes just IDs? No.
           I'll need to look at how Kraken handles this or if I need to change the interface (unlikely).
           Kraken module just calls Kraken_actions.cancel_orders.
        *)
        (* Group IDs by symbol *)
        let symbol_map = Hashtbl.create 4 in
        List.iter (fun id ->
          match Hyperliquid_executions_feed.find_order_everywhere id with
          | Some o ->
              let existing = try Hashtbl.find symbol_map o.symbol with _ -> [] in
              Hashtbl.replace symbol_map o.symbol (id :: existing)
          | None -> () (* Skip or handle error *)
        ) ids;
        
        let results = ref [] in
        let errors = ref [] in
        
        Hashtbl.fold (fun symbol ids_for_symbol acc ->
          (Hyperliquid_actions.cancel_orders
            ~symbol
            ~order_ids:(List.map Int64.of_string ids_for_symbol)
            ~testnet:!is_testnet
          >|= function
          | Ok () -> 
              results := !results @ (List.map (fun id -> { Types.order_id = id; cl_ord_id = None }) ids_for_symbol)
          | Error e -> errors := e :: !errors
          ) :: acc
        ) symbol_map [] |> Lwt.join >>= fun () ->
        
        if !errors <> [] then
          Lwt.return (Error (String.concat "; " !errors))
        else
          Lwt.return (Ok !results)

  (** Market Data Access *)
  
  let get_ticker ~symbol =
    match Hyperliquid_ticker_feed.get_latest_ticker symbol with
    | Some t -> Some (t.bid, t.ask)
    | None -> None

  let get_top_of_book ~symbol =
    match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) -> Some (bp, bs, ap, as_val)
    | None -> None

  let get_balance ~asset =
    Hyperliquid_balances.get_balance asset

  let get_open_order ~symbol ~order_id =
    match Hyperliquid_executions_feed.get_open_order symbol order_id with
    | Some o -> Some {
        Types.order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_hyperliquid_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_hyperliquid_status o.order_status;
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
        side = side_of_hyperliquid_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_hyperliquid_status o.order_status;
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
        order_status = status_of_hyperliquid_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_hyperliquid_side e.side;
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
          l.price, l.size
        ) levels
      in
      { Types.
        bids = map_levels ob.bids;
        asks = map_levels ob.asks;
        timestamp = ob.timestamp;
      }
    ) events

  (** Metadata Access *)
  
  let get_price_increment ~symbol = Hyperliquid_instruments_feed.get_price_increment symbol
  let get_qty_increment ~symbol = Hyperliquid_instruments_feed.get_qty_increment symbol
  let get_qty_min ~symbol = Hyperliquid_instruments_feed.get_qty_min symbol
  let round_price ~symbol ~price = Hyperliquid_instruments_feed.round_price_to_tick_for_symbol symbol price

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some f -> (Some (fst f), Some (snd f))
    | None -> (None, None)

  let initialize_fees symbols =
    Lwt_list.iter_p (fun symbol ->
      Hyperliquid_get_fee.get_fee_info ~testnet:!is_testnet symbol >|= function
      | Some info ->
          let maker = Option.value info.maker_fee ~default:0.0 in
          let taker = Option.value info.taker_fee ~default:0.0 in
          Hashtbl.replace fee_cache symbol (maker, taker)
      | None -> ()
    ) symbols
end

(* Register the module *)
let () =
  Exchange.Registry.register (module Hyperliquid_impl)
