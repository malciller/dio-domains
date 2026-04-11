(** IBKR exchange adapter.

    Implements [Exchange_intf.S] for Interactive Brokers via the TWS API.
    Converts between TWS-specific types and the unified Dio [Types] domain.
    Delegates order actions to [Ibkr_actions] and reads market data from
    per-symbol feed caches.

    Configuration via environment variables:
    - IBKR_GATEWAY_HOST:  IB Gateway host (default: 127.0.0.1)
    - IBKR_GATEWAY_PORT:  IB Gateway port (default: 4002)
    - IBKR_ACCOUNT_ID:    Trading account ID (default: auto-detected)
    - IBKR_TRADING_MODE:  "paper" or "live" (default: paper)
    - IBKR_CLIENT_ID:     API client ID (default: 0)

    Registered into [Exchange.Registry] at module load time. *)

open Lwt.Infix

module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

(** Configuration loaded from environment, with runtime override via [set_testnet].

    Environment variables set the initial defaults.  The per-symbol [testnet]
    flag in config.json is applied later by the supervisor before the gateway
    connection is established, overriding [trading_mode], [is_paper], and
    [gateway_port]. *)
module Config = struct
  let section = "ibkr_config"

  let gateway_host =
    try Sys.getenv "IBKR_GATEWAY_HOST"
    with Not_found -> "127.0.0.1"

  (* Mutable so set_testnet can override after config is parsed *)
  let gateway_port = ref (
    try int_of_string (Sys.getenv "IBKR_GATEWAY_PORT")
    with _ -> 4002
  )

  let account_id =
    try Some (Sys.getenv "IBKR_ACCOUNT_ID")
    with Not_found -> None

  let trading_mode = ref (
    try
      let mode = Sys.getenv "IBKR_TRADING_MODE" in
      match String.lowercase_ascii mode with
      | "live" -> "live"
      | "paper" | _ -> "paper"
    with Not_found -> "paper"
  )

  let client_id =
    try int_of_string (Sys.getenv "IBKR_CLIENT_ID")
    with _ -> Ibkr_types.default_client_id

  let is_paper = ref (!trading_mode = "paper")

  (** Override trading mode from the config.json [testnet] flag.
      [testnet = true] → paper trading (port 4002).
      [testnet = false] → live trading (port 4001).
      Only overrides the port when no explicit IBKR_GATEWAY_PORT env var is set. *)
  let set_testnet testnet =
    let mode = if testnet then "paper" else "live" in
    trading_mode := mode;
    is_paper := testnet;
    (* Only override port if the user didn't set it explicitly via env *)
    if Sys.getenv_opt "IBKR_GATEWAY_PORT" = None then
      gateway_port := (if testnet then 4002 else 4001);
    Logging.info_f ~section "IBKR trading mode set to %s (testnet=%b, port=%d)"
      mode testnet !gateway_port

  let () =
    Logging.info_f ~section "IBKR config: host=%s port=%d mode=%s clientId=%d"
      gateway_host !gateway_port !trading_mode client_id;
    if !is_paper then
      Logging.info ~section "Running in PAPER trading mode"
    else
      Logging.warn ~section "Running in LIVE trading mode — real money at risk"
end

(** Shared connection handle. *)
let connection = ref None

let get_conn () =
  match !connection with
  | Some c -> c
  | None -> failwith "IBKR connection not initialized"

module Ibkr_impl = struct
  let name = "ibkr"
  let section = "ibkr_module"

  (* ---- Type conversions ---- *)

  let string_of_order_type = function
    | Types.Limit -> "LMT"
    | Types.Market -> "MKT"
    | Types.StopLoss -> "STP"
    | Types.TakeProfit -> "LMT"
    | Types.StopLossLimit -> "STP LMT"
    | Types.TakeProfitLimit -> "LMT"
    | Types.SettlPosition -> "MKT"
    | Types.Other s -> s

  let string_of_side = function
    | Types.Buy -> "BUY"
    | Types.Sell -> "SELL"

  let string_of_tif = function
    | Types.GTC -> "GTC"
    | Types.IOC -> "IOC"
    | Types.FOK -> "FOK"

  let side_of_string = function
    | "BUY" | "BOT" -> Types.Buy
    | "SELL" | "SLD" -> Types.Sell
    | _ -> Types.Buy

  (* ---- Order lifecycle ---- *)

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
      ?cl_ord_id:_
      ?trigger_price:_
      ?display_qty:_
      ?retry_config:_
      () =
    let conn = get_conn () in
    let tws_order_type = string_of_order_type order_type in
    let tws_side = string_of_side side in
    let tif = match time_in_force with
      | Some t -> string_of_tif t
      | None -> "DAY"
    in
    Lwt.catch (fun () ->
      Ibkr_actions.place_order conn
        ~symbol ~action:tws_side ~qty
        ~order_type:tws_order_type
        ?limit_price ~tif ()
      >|= fun order_id ->
      Ok {
        Types.order_id = string_of_int order_id;
        cl_ord_id = None;
        order_userref = None;
      }
    ) (fun exn ->
      Lwt.return (Error (Printexc.to_string exn))
    )

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
    let conn = get_conn () in
    let oid = try int_of_string order_id with _ -> 0 in
    let sym = match symbol with Some s -> s | None -> "" in
    if sym = "" then
      Lwt.return (Error "Symbol required for IBKR order amendment")
    else
      (* Look up existing order to fill in missing fields *)
      let existing = Ibkr_executions_feed.get_open_order sym order_id in
      let action = match existing with
        | Some oo -> oo.Ibkr_executions_feed.oo_side
        | None -> "BUY"
      in
      let effective_qty = match qty with
        | Some q -> q
        | None ->
            (match existing with
             | Some oo -> oo.Ibkr_executions_feed.oo_qty
             | None -> 0.0)
      in
      let order_type = match limit_price with
        | Some _ -> "LMT"
        | None -> "MKT"
      in
      Lwt.catch (fun () ->
        Ibkr_actions.modify_order conn
          ~order_id:oid ~symbol:sym
          ~action ~qty:effective_qty
          ~order_type ?limit_price ()
        >|= fun () ->
        Ok {
          Types.original_order_id = order_id;
          new_order_id = order_id;  (* TWS modifies in-place *)
          amend_id = None;
          cl_ord_id = None;
        }
      ) (fun exn ->
        Lwt.return (Error (Printexc.to_string exn))
      )

  let cancel_orders
      ~token:_
      ?order_ids
      ?cl_ord_ids:_
      ?order_userrefs:_
      ?symbol:_
      ?retry_config:_
      () =
    let conn = get_conn () in
    let ids = match order_ids with Some ids -> ids | None -> [] in
    Lwt_list.map_s (fun oid_str ->
      let oid = try int_of_string oid_str with _ -> 0 in
      Lwt.catch (fun () ->
        Ibkr_actions.cancel_order conn ~order_id:oid >|= fun () ->
        Ok { Types.order_id = oid_str; cl_ord_id = None }
      ) (fun exn ->
        Lwt.return (Error (Printexc.to_string exn))
      )
    ) ids >|= fun results ->
    let successes = List.filter_map (function Ok r -> Some r | Error _ -> None) results in
    let errors = List.filter_map (function Error e -> Some e | Ok _ -> None) results in
    if List.length errors > 0 then
      Error (String.concat "; " errors)
    else
      Ok successes

  (* ---- Market data accessors ---- *)



  let subscribe_orderbook ~symbols =
    let conn = get_conn () in
    Lwt_list.iter_s (fun symbol ->
      Ibkr_contracts.resolve conn ~symbol >>= fun contract ->
      Ibkr_orderbook_feed.subscribe conn ~contract
    ) symbols

  let get_top_of_book ~symbol =
    match Ibkr_orderbook_feed.store_opt symbol with
    | Some store ->
        (match Concurrency.Ring_buffer.RingBuffer.read_latest store.buffer with
         | Some ob when Array.length ob.Ibkr_orderbook_feed.bids > 0
                     && Array.length ob.Ibkr_orderbook_feed.asks > 0 ->
             let bid = ob.bids.(0) in
             let ask = ob.asks.(0) in
             Some (bid.Ibkr_orderbook_feed.price, bid.size, ask.price, ask.size)
         | _ -> None)
    | None -> None

  let get_balance ~asset = Ibkr_balances.get_balance ~asset

  let get_all_balances () = Ibkr_balances.get_all_balances ()

  let get_open_order ~symbol ~order_id =
    match Ibkr_executions_feed.get_open_order symbol order_id with
    | Some oo ->
        Some {
          Types.order_id = oo.Ibkr_executions_feed.oo_order_id;
          symbol = oo.oo_symbol;
          side = side_of_string oo.oo_side;
          qty = oo.oo_qty;
          cum_qty = oo.oo_filled_qty;
          remaining_qty = oo.oo_remaining_qty;
          limit_price = oo.oo_limit_price;
          status = Ibkr_types.to_exchange_order_status oo.oo_status;
          user_ref = None;
          cl_ord_id = None;
        }
    | None -> None

  let get_open_orders ~symbol =
    List.map (fun (oo : Ibkr_executions_feed.open_order) ->
      { Types.order_id = oo.oo_order_id;
        symbol = oo.oo_symbol;
        side = side_of_string oo.oo_side;
        qty = oo.oo_qty;
        cum_qty = oo.oo_filled_qty;
        remaining_qty = oo.oo_remaining_qty;
        limit_price = oo.oo_limit_price;
        status = Ibkr_types.to_exchange_order_status oo.oo_status;
        user_ref = None;
        cl_ord_id = None;
      }
    ) (Ibkr_executions_feed.get_open_orders symbol)

  (* ---- Ring buffer event feeds ---- *)



  let get_orderbook_position ~symbol =
    Ibkr_orderbook_feed.get_current_position symbol

  let read_orderbook_events ~symbol ~start_pos =
    List.map (fun (ob : Ibkr_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Ibkr_orderbook_feed.level) -> (l.price, l.size)) levels
      in
      { Types.bids = map_levels ob.bids; asks = map_levels ob.asks; timestamp = ob.timestamp }
    ) (Ibkr_orderbook_feed.read_orderbook_events symbol start_pos)

  let iter_orderbook_events ~symbol ~start_pos f =
    Ibkr_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Ibkr_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Ibkr_orderbook_feed.level) -> (l.price, l.size)) levels
      in
      f { Types.bids = map_levels ob.bids; asks = map_levels ob.asks; timestamp = ob.timestamp }
    )

  let iter_top_of_book_events ~symbol ~start_pos f =
    Ibkr_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Ibkr_orderbook_feed.orderbook) ->
      if Array.length ob.bids > 0 && Array.length ob.asks > 0 then begin
        let bid = ob.bids.(0) in
        let ask = ob.asks.(0) in
        f bid.Ibkr_orderbook_feed.price bid.size ask.price ask.size
      end
    )

  let get_execution_feed_position ~symbol =
    Ibkr_executions_feed.get_current_position symbol

  (** Return [true] once execution data has been received for [symbol]. *)
  let has_execution_data ~symbol =
    Ibkr_executions_feed.has_execution_data symbol

  let read_execution_events ~symbol ~start_pos =
    List.map (fun (e : Ibkr_executions_feed.execution_event) ->
      { Types.order_id = e.order_id;
        order_status = Ibkr_types.to_exchange_order_status e.status;
        limit_price = None;
        side = side_of_string e.side;
        remaining_qty = e.remaining_qty;
        filled_qty = e.filled_qty;
        avg_price = e.avg_fill_price;
        timestamp = e.timestamp;
        is_amended = false;
        cl_ord_id = None;
      }
    ) (Ibkr_executions_feed.read_execution_events symbol start_pos)

  let iter_execution_events ~symbol ~start_pos f =
    Ibkr_executions_feed.iter_execution_events symbol start_pos (fun (e : Ibkr_executions_feed.execution_event) ->
      f { Types.order_id = e.order_id;
          order_status = Ibkr_types.to_exchange_order_status e.status;
          limit_price = None;
          side = side_of_string e.side;
          remaining_qty = e.remaining_qty;
          filled_qty = e.filled_qty;
          avg_price = e.avg_fill_price;
          timestamp = e.timestamp;
          is_amended = false;
          cl_ord_id = None;
        }
    )

  let fold_open_orders ~symbol ~init ~f =
    Ibkr_executions_feed.fold_open_orders symbol ~init ~f:(fun acc (oo : Ibkr_executions_feed.open_order) ->
      f acc { Types.order_id = oo.oo_order_id;
              symbol = oo.oo_symbol;
              side = side_of_string oo.oo_side;
              qty = oo.oo_qty;
              cum_qty = oo.oo_filled_qty;
              remaining_qty = oo.oo_remaining_qty;
              limit_price = oo.oo_limit_price;
              status = Ibkr_types.to_exchange_order_status oo.oo_status;
              user_ref = None;
              cl_ord_id = None;
            }
    )

  let iter_open_orders_fast ~symbol f =
    Ibkr_executions_feed.fold_open_orders symbol ~init:() ~f:(fun () (oo : Ibkr_executions_feed.open_order) ->
      let limit_price = match oo.oo_limit_price with Some p -> p | None -> 0.0 in
      f oo.oo_order_id limit_price oo.oo_remaining_qty (String.lowercase_ascii oo.oo_side) None
    )

  (* ---- Instrument metadata ---- *)

  let get_price_increment ~symbol =
    match Ibkr_contracts.get_cached ~symbol with
    | Some c when c.Ibkr_types.min_tick > 0.0 -> Some c.min_tick
    | _ -> None

  let get_qty_increment ~symbol:_ = Some 1.0  (* TWS API: whole shares only — fractional not supported via API *)

  let get_qty_min ~symbol:_ = Some 1.0  (* TWS API: whole shares only *)

  let round_price ~symbol ~price =
    match get_price_increment ~symbol with
    | Some inc -> Float.round (price /. inc) *. inc
    | None -> Float.round (price *. 100.0) /. 100.0  (* Default to 2 decimals *)

  let get_fees ~symbol:_ =
    (* IBKR uses a tiered/fixed commission structure, not per-pair fees.
       Return None to indicate fees are not available per-symbol. *)
    (None, None)
end

(* Register into the global exchange registry at load time. *)
let () =
  Exchange.Registry.register (module Ibkr_impl)
