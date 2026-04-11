(** Exchange_intf implementation for the Lighter L2 DEX.
    Bridges Lighter-specific WebSocket feeds, FFI signer, REST/WS actions,
    and instrument metadata into the exchange-agnostic interface consumed
    by strategies. *)

open Lwt.Infix

module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Lighter_impl = struct
  let name = "lighter"
  let section = "lighter_module"

  (* Per-symbol fee cache. Maps symbol to (maker_fee, taker_fee). *)
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16

  (* Type conversion helpers *)
  let status_of_lighter_status = function
    | Lighter_executions_feed.PendingStatus -> Types.Pending
    | Lighter_executions_feed.NewStatus -> Types.New
    | Lighter_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Lighter_executions_feed.FilledStatus -> Types.Filled
    | Lighter_executions_feed.CanceledStatus -> Types.Canceled
    | Lighter_executions_feed.ExpiredStatus -> Types.Expired
    | Lighter_executions_feed.RejectedStatus -> Types.Rejected
    | Lighter_executions_feed.UnknownStatus s -> Types.Unknown s

  let side_of_lighter_side = function
    | Lighter_executions_feed.Buy -> Types.Buy
    | Lighter_executions_feed.Sell -> Types.Sell

  (** Place a new order on Lighter. *)
  let place_order
      ~token:_
      ~order_type
      ~side
      ~qty
      ~symbol
      ?limit_price
      ?time_in_force
      ?post_only
      ?reduce_only
      ?order_userref:_
      ?cl_ord_id:_
      ?trigger_price:_
      ?display_qty:_
      ?retry_config:_
      () =

    let is_buy = match side with Types.Buy -> true | Types.Sell -> false in
    let px = match limit_price with
      | Some p -> p
      | None ->
          match order_type with
          | Types.Market ->
              (* Emulate market order with slippage from BBO *)
              (match Lighter_ticker_feed.get_latest_ticker symbol with
               | Some t ->
                   if is_buy then t.ask *. 1.05
                   else t.bid *. 0.95
               | None ->
                   Logging.warn_f ~section "No ticker for market order on %s" symbol;
                   0.0)
          | _ -> 0.0
    in

    let tif = match time_in_force with Some t -> t | None -> Types.GTC in
    let post_only_val = Option.value post_only ~default:false in
    let reduce_only_val = Option.value reduce_only ~default:false in

    let px_rounded = Lighter_instruments_feed.round_price_for_symbol symbol px in
    let sz_rounded = Lighter_instruments_feed.round_qty_for_symbol symbol qty in

    Lighter_actions.place_order
      ~symbol
      ~is_buy
      ~qty:sz_rounded
      ~price:px_rounded
      ~order_type
      ~tif
      ~post_only:post_only_val
      ~reduce_only:reduce_only_val
      ()
    >|= function
    | Ok res -> Ok res
    | Error msg -> Error msg

  (** Amend an existing order. Lighter supports in-place modify. *)
  let amend_order
      ~token:_
      ~order_id
      ?cl_ord_id:_
      ?qty
      ?limit_price
      ?post_only:_
      ?trigger_price:_
      ?display_qty:_
      ?symbol
      ?retry_config:_
      () =

    let sym = match symbol with
      | Some s -> s
      | None ->
          match Lighter_executions_feed.find_order_everywhere order_id with
          | Some o -> o.symbol
          | None -> ""
    in
    if sym = "" then
      Lwt.return (Error "Cannot amend: symbol unknown and order not found")
    else
      let existing = Lighter_executions_feed.find_order_everywhere order_id in
      if Option.is_some symbol && Option.is_none existing then begin
        Logging.warn_f ~section "Amend rejected at module level: order %s [%s] not in local state" order_id sym;
        Lwt.return (Error (Printf.sprintf "Order not found for amendment: %s" order_id))
      end else
      let new_qty = match qty with
        | Some q -> Lighter_instruments_feed.round_qty_for_symbol sym q
        | None -> (match existing with Some o -> o.order_qty | None -> 0.0)
      in
      let new_price = match limit_price with
        | Some p -> Lighter_instruments_feed.round_price_for_symbol sym p
        | None -> (match existing with
            | Some o -> (match o.limit_price with Some p -> p | None -> 0.0)
            | None -> 0.0)
      in
      Lighter_actions.modify_order ~symbol:sym ~order_id ~new_qty ~new_price
      >|= function
      | Ok res -> Ok res
      | Error msg -> Error msg

  (** Cancel one or more orders. *)
  let cancel_orders
      ~token:_
      ?order_ids
      ?cl_ord_ids:_
      ?order_userrefs:_
      ?retry_config:_
      () =

    let ids = match order_ids with Some ids -> ids | None -> [] in
    if ids = [] then
      Lwt.return (Error "No order IDs to cancel")
    else
      let results = Lwt_list.map_s (fun order_id ->
        (* Resolve symbol from the executions feed *)
        let symbol = match Lighter_executions_feed.find_order_everywhere order_id with
          | Some o -> o.symbol
          | None -> ""
        in
        if symbol = "" then
          Lwt.return (Error (Printf.sprintf "Cannot cancel %s: symbol unknown" order_id))
        else
          Lighter_actions.cancel_order ~symbol ~order_id
      ) ids in
      results >|= fun res_list ->
      let successes = List.filter_map (function Ok r -> Some r | Error _ -> None) res_list in
      let errors = List.filter_map (function Error msg -> Some msg | Ok _ -> None) res_list in
      if errors = [] then Ok successes
      else Error (String.concat "; " errors)

  (* ---- Market data accessors ---- *)

  let get_ticker ~symbol =
    match Lighter_ticker_feed.get_latest_ticker symbol with
    | Some t -> Some (t.bid, t.ask)
    | None -> None

  let subscribe_ticker ~symbol =
    let _ = Lighter_ticker_feed.get_latest_ticker symbol in
    Lwt.return_unit

  let get_top_of_book ~symbol =
    Lighter_orderbook_feed.get_best_bid_ask symbol

  let get_balance ~asset =
    Lighter_balances.get_balance asset

  let get_all_balances () =
    Lighter_balances.get_all_balances ()

  let get_open_order ~symbol ~order_id =
    match Lighter_executions_feed.get_open_order symbol order_id with
    | Some o ->
        Some { Types.
          order_id = o.order_id;
          symbol = o.symbol;
          side = side_of_lighter_side o.side;
          qty = o.order_qty;
          cum_qty = o.cum_qty;
          remaining_qty = o.remaining_qty;
          limit_price = o.limit_price;
          status = status_of_lighter_status o.order_status;
          user_ref = o.order_userref;
          cl_ord_id = o.cl_ord_id;
        }
    | None -> None

  let get_open_orders ~symbol =
    let orders = Lighter_executions_feed.get_open_orders symbol in
    List.map (fun (o : Lighter_executions_feed.open_order) ->
      { Types.
        order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_lighter_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_lighter_status o.order_status;
        user_ref = o.order_userref;
        cl_ord_id = o.cl_ord_id;
      }
    ) orders

  let fold_open_orders ~symbol ~init ~f =
    Lighter_executions_feed.fold_open_orders symbol ~init ~f:(fun acc (o : Lighter_executions_feed.open_order) ->
      f acc { Types.
        order_id = o.order_id;
        symbol = o.symbol;
        side = side_of_lighter_side o.side;
        qty = o.order_qty;
        cum_qty = o.cum_qty;
        remaining_qty = o.remaining_qty;
        limit_price = o.limit_price;
        status = status_of_lighter_status o.order_status;
        user_ref = o.order_userref;
        cl_ord_id = o.cl_ord_id;
      }
    )

  let iter_open_orders_fast ~symbol f =
    Lighter_executions_feed.fold_open_orders symbol ~init:() ~f:(fun () (o : Lighter_executions_feed.open_order) ->
      let limit_price = match o.limit_price with Some p -> p | None -> 0.0 in
      let side_str = match o.side with Lighter_executions_feed.Buy -> "buy" | Sell -> "sell" in
      f o.order_id limit_price o.remaining_qty side_str o.order_userref
    )

  let get_execution_feed_position ~symbol =
    Lighter_executions_feed.get_current_position symbol

  let has_execution_data ~symbol =
    Lighter_executions_feed.has_execution_data symbol

  let read_execution_events ~symbol ~start_pos =
    let events = Lighter_executions_feed.read_execution_events symbol start_pos in
    List.map (fun (e : Lighter_executions_feed.execution_event) ->
      { Types.
        order_id = e.order_id;
        order_status = status_of_lighter_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_lighter_side e.side;
        remaining_qty = max 0.0 (e.order_qty -. e.cum_qty);
        filled_qty = e.cum_qty;
        avg_price = e.avg_price;
        timestamp = e.timestamp;
        is_amended = e.is_amended;
        cl_ord_id = e.cl_ord_id;
      }
    ) events

  let iter_execution_events ~symbol ~start_pos f =
    Lighter_executions_feed.iter_execution_events symbol start_pos (fun (e : Lighter_executions_feed.execution_event) ->
      f { Types.
        order_id = e.order_id;
        order_status = status_of_lighter_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_lighter_side e.side;
        remaining_qty = max 0.0 (e.order_qty -. e.cum_qty);
        filled_qty = e.cum_qty;
        avg_price = e.avg_price;
        timestamp = e.timestamp;
        is_amended = e.is_amended;
        cl_ord_id = e.cl_ord_id;
      }
    )

  let get_ticker_position ~symbol =
    Lighter_ticker_feed.get_current_position symbol

  let read_ticker_events ~symbol ~start_pos =
    let events = Lighter_ticker_feed.read_ticker_events symbol start_pos in
    List.map (fun (t : Lighter_ticker_feed.ticker) ->
      { Types. bid = t.bid; ask = t.ask; timestamp = t.timestamp }
    ) events

  let iter_ticker_events ~symbol ~start_pos f =
    Lighter_ticker_feed.iter_ticker_events symbol start_pos (fun (t : Lighter_ticker_feed.ticker) ->
      f { Types. bid = t.bid; ask = t.ask; timestamp = t.timestamp }
    )

  let get_orderbook_position ~symbol =
    Lighter_orderbook_feed.get_current_position symbol

  let read_orderbook_events ~symbol ~start_pos =
    let events = Lighter_orderbook_feed.read_orderbook_events symbol start_pos in
    List.map (fun (ob : Lighter_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Lighter_orderbook_feed.level) ->
          l.price, l.size
        ) levels
      in
      { Types.
        bids = map_levels ob.bids;
        asks = map_levels ob.asks;
        timestamp = ob.timestamp;
      }
    ) events

  let iter_orderbook_events ~symbol ~start_pos f =
    Lighter_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Lighter_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Lighter_orderbook_feed.level) ->
          l.price, l.size
        ) levels
      in
      f { Types. bids = map_levels ob.bids; asks = map_levels ob.asks; timestamp = ob.timestamp }
    )

  (** Zero-allocation top-of-book iterator. *)
  let iter_top_of_book_events ~symbol ~start_pos f =
    Lighter_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Lighter_orderbook_feed.orderbook) ->
      if Array.length ob.bids > 0 && Array.length ob.asks > 0 then begin
        let bid = ob.bids.(0) in
        let ask = ob.asks.(0) in
        f bid.price bid.size ask.price ask.size
      end
    )

  (** Instrument metadata accessors *)

  let get_price_increment ~symbol = Lighter_instruments_feed.get_price_increment symbol
  let get_qty_increment ~symbol = Lighter_instruments_feed.get_qty_increment symbol
  let get_qty_min ~symbol = Lighter_instruments_feed.get_qty_min symbol
  let round_price ~symbol ~price = Lighter_instruments_feed.round_price_for_symbol symbol price

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some (maker, taker) -> (Some maker, Some taker)
    | None ->
        (* Populate from instruments feed *)
        (match Lighter_instruments_feed.lookup_info symbol with
         | Some info ->
             Hashtbl.replace fee_cache symbol (info.maker_fee, info.taker_fee);
             (Some info.maker_fee, Some info.taker_fee)
         | None -> (None, None))
end

(** WebSocket-based initialization routines. *)
let wait_for_ws_connected () =
  Lighter_ws.wait_for_connected ()

let initialize_signer () =
  let section = "lighter_startup" in
  let private_key = match Sys.getenv_opt "LIGHTER_API_PRIVATE_KEY" |> Option.map String.trim with
    | Some k when k <> "" -> k
    | _ -> failwith "LIGHTER_API_PRIVATE_KEY env var is required"
  in
  let api_key_index = match Sys.getenv_opt "LIGHTER_API_KEY_INDEX" |> Option.map String.trim with
    | Some s -> (try int_of_string s with _ -> 0)
    | None -> 0
  in
  let account_index = match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
    | Some s -> (try int_of_string s with _ -> 0)
    | None -> 0
  in
  let base_url = Lighter_proxy.api_base_url () in
  match Lighter_signer.initialize ~base_url ~private_key ~key_index:api_key_index ~acct_index:account_index with
  | Ok () ->
      Logging.info_f ~section "Lighter signer initialized successfully";
      Lighter_signer.initialize_nonce ~base_url ~api_key_index ~account_index
  | Error msg ->
      Logging.error_f ~section "Failed to initialize Lighter signer: %s" msg;
      Lwt.return_unit

(** Initialize instruments from REST + signer + auth. *)
let initialize_instruments ~symbols =
  let base_url = Lighter_proxy.api_base_url () in
  Lighter_instruments_feed.fetch_and_initialize ~base_url ~required_symbols:symbols

(** Fetch open orders via REST on reconnection. *)
let fetch_open_orders () =
  let section = "lighter_startup" in
  let account_index = match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
    | Some s -> (try int_of_string s with _ -> 0)
    | None -> 0
  in
  let base_url = Lighter_proxy.api_base_url () in
  let token = Lighter_signer.get_auth_token () in
  (* Lighter SDK uses /api/v1/accountActiveOrders with auth as a query param *)
  let url = Printf.sprintf "%s/api/v1/accountActiveOrders?account_index=%d&auth=%s"
    base_url account_index token in
  Lwt.catch (fun () ->
    let uri = Uri.of_string url in
    let%lwt (resp, body) = Cohttp_lwt_unix.Client.get uri in
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    let%lwt body_str = Cohttp_lwt.Body.to_string body in
    if status < 200 || status >= 300 then begin
      Logging.error_f ~section "openOrders request failed: HTTP %d (body=%s)" status
        (if String.length body_str > 200 then String.sub body_str 0 200 ^ "..." else body_str);
      Lighter_executions_feed.set_startup_snapshot_done ();
      Lwt.return_unit
    end else begin
      let trimmed = String.trim body_str in
      if trimmed = "" || trimmed = "{}" || trimmed = "[]" then begin
        Logging.info_f ~section "No open orders on Lighter (empty response)";
        Lighter_executions_feed.set_startup_snapshot_done ();
        Lwt.return_unit
      end else begin
        let json = Yojson.Safe.from_string trimmed in
        Lighter_executions_feed.process_account_orders_update json;
        Lighter_executions_feed.set_startup_snapshot_done ();
        Logging.debug_f ~section "Fetched and injected open orders";
        Lwt.return_unit
      end
    end
  ) (fun exn ->
    Logging.error_f ~section "Failed to fetch open orders: %s" (Printexc.to_string exn);
    Lighter_executions_feed.set_startup_snapshot_done ();
    Lwt.return_unit
  )

(* Register Lighter_impl with the exchange registry at module load time *)
let () =
  Exchange.Registry.register (module Lighter_impl)
