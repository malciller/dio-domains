(** Exchange_intf implementation for the Hyperliquid L1 DEX.
    Bridges Hyperliquid-specific WebSocket feeds, REST actions, and instrument
    metadata into the exchange-agnostic interface consumed by strategies. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Hyperliquid_impl = struct
  let name = "hyperliquid"
  let section = "hyperliquid_module"

  (* Testnet/mainnet toggle. Atomic for safe concurrent reads from feed threads. *)
  let is_testnet = Atomic.make true
  let set_testnet testnet =
    Atomic.set is_testnet testnet;
    Logging.info_f ~section "Hyperliquid module configured for %s" (if testnet then "testnet" else "mainnet")

  (* Per-symbol fee cache. Maps symbol to (perp_maker, perp_taker, spot_maker, spot_taker).
     Populated once during initialize_fees and read on every get_fees call. *)
  let fee_cache : (string, float * float * float * float) Hashtbl.t = Hashtbl.create 16

  (* Type conversion helpers: Hyperliquid-specific variants to Exchange_intf.Types *)
  let string_of_order_type = function
    | Types.Limit -> "limit"
    | Types.Market -> "market"
    | Types.Other s -> s
    | _ -> "limit" (* Fallback: unsupported order types default to limit *)

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

  (** Place a new order on Hyperliquid.
      Market orders are emulated as aggressive IOC limit orders with 5% slippage.
      Prices and quantities are rounded to the instrument's tick and lot size
      before submission. *)
  let place_order
      ~token:_ (* Unused: Hyperliquid authenticates via wallet address and private key from env *)
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
      ?trigger_price:_
      ?display_qty:_
      ?retry_config:_
      () =
    
    let is_limit = match order_type with Types.Limit -> true | _ -> false in
    let side_bool = match side with Types.Buy -> true | Types.Sell -> false in
    let px = match limit_price with
      | Some p -> p
      | None ->
          match order_type with
          | Types.Market ->
              (* Hyperliquid has no native market order type.
                 Emulate via IOC limit order with 5% slippage from current BBO. *)
              (match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
              | Some (bid, _, ask, _) ->
                  if side_bool then ask *. 1.05 (* Buy: 5% above ask *)
                  else bid *. 0.95 (* Sell: 5% below bid *)
              | None ->
                  Logging.warn_f ~section "No orderbook data available for market order on %s, using 0.0" symbol;
                  0.0)
          | _ -> 0.0
    in

    let hl_tif = match time_in_force with
      | Some Types.IOC -> Hyperliquid_types.Ioc
      | Some Types.GTC -> Hyperliquid_types.Gtc
      | Some Types.FOK -> Hyperliquid_types.Ioc
      | None -> if Option.value post_only ~default:false then Hyperliquid_types.Alo else Hyperliquid_types.Gtc
    in

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
      ~time_in_force:hl_tif
      ~testnet:(Atomic.get is_testnet)
      ()
    >|= function
    | Ok res ->
        let order_id_str = Int64.to_string res.Hyperliquid_actions.order_id in
        (* Proactively inject into open_orders so find_order_everywhere succeeds
           for immediate post-placement amendments. Without this, the order is
           invisible until the next webData2 push (~1.5s), and any amendment
           in that window would fail with "Order not found". *)
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

  (** Amend an existing order (cancel-replace on Hyperliquid).
      Looks up the current order state from the executions feed to fill in
      any parameters not explicitly provided by the caller. *)
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
        (* Resolve current order state for side, price, and qty defaults *)
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
              ~testnet:(Atomic.get is_testnet)
              ()
        >|= function
        | Ok res ->
            let new_order_id_str = Int64.to_string res.amend_id in
            (* Do NOT inject the new OID into open_orders. Previous attempts
               created phantom duplicates: the new OID was added while the old
               OID persisted (WS cancel not yet received), causing get_open_orders
               to return two entries and triggering spurious cancel-all logic.
               The inflight_amend counter now bridges the REST-to-WS gap.
               Only the old OID is removed (on ID change) to prevent a late
               WS "open" event from re-adding it as a ghost order. *)
            if new_order_id_str <> order_id then
              Hyperliquid_executions_feed.remove_open_order ~symbol:sym ~order_id;
            Ok {
              Types.original_order_id = Int64.to_string res.order_id;
              Types.new_order_id = new_order_id_str;
              Types.amend_id = None;
              Types.cl_ord_id = constructed_cl_ord_id;
            }
        | Error e -> Error e

  (** Cancel one or more orders by order ID.
      Orders are grouped by symbol for Hyperliquid's per-coin cancel endpoint.
      Each symbol group is processed sequentially to avoid shared-state races. *)
  let cancel_orders
      ~token:_
      ?order_ids
      ?cl_ord_ids:_
      ?order_userrefs:_
      ?symbol:_
      ?retry_config:_
      () =
    
    match order_ids with
    | None -> Lwt.return (Ok [])
    | Some ids ->
        (* Group order IDs by symbol; Hyperliquid requires per-coin cancel requests *)
        let symbol_map = Hashtbl.create 4 in
        List.iter (fun id ->
          match Hyperliquid_executions_feed.find_order_everywhere id with
          | Some o ->
              let existing = try Hashtbl.find symbol_map o.symbol with _ -> [] in
              Hashtbl.replace symbol_map o.symbol (id :: existing)
          | None -> ()
        ) ids;
        
        (* Process symbol groups sequentially to avoid concurrent mutation of shared state *)
        let symbol_groups = Hashtbl.fold (fun sym ids acc -> (sym, ids) :: acc) symbol_map [] in
        Lwt_list.fold_left_s (fun (results_acc, errors_acc) (symbol, ids_for_symbol) ->
          Hyperliquid_actions.cancel_orders
            ~symbol
            ~order_ids:(List.map Int64.of_string ids_for_symbol)
            ~testnet:(Atomic.get is_testnet)
          >|= function
          | Ok () ->
              (* Proactively remove cancelled orders from open_orders.
                 Prevents ghost entries if the orderUpdates WS message is delayed or lost. *)
              List.iter (fun id ->
                Hyperliquid_executions_feed.remove_open_order ~symbol ~order_id:id
              ) ids_for_symbol;
              let new_results = List.map (fun id -> { Types.order_id = id; cl_ord_id = None }) ids_for_symbol in
              (new_results @ results_acc, errors_acc)
          | Error e ->
              (results_acc, e :: errors_acc)
        ) ([], []) symbol_groups >>= fun (results, errors) ->
        
        if errors <> [] then
          Lwt.return (Error (String.concat "; " errors))
        else
          Lwt.return (Ok results)

  (** Market data accessors. Delegate to the corresponding feed modules. *)
  


  let get_top_of_book ~symbol =
    match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) -> Some (bp, bs, ap, as_val)
    | None -> None

  let get_balance ~asset =
    Hyperliquid_balances.get_balance asset

  let get_all_balances () =
    let assets = Hyperliquid_balances.get_all_assets () in
    List.filter_map (fun asset ->
      let bal = Hyperliquid_balances.get_balance asset in
      if bal > 0.0 then Some (asset, bal) else None
    ) assets

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

  let fold_open_orders ~symbol ~init ~f =
    Hyperliquid_executions_feed.fold_open_orders symbol ~init ~f:(fun acc (o : Hyperliquid_executions_feed.open_order) ->
      f acc { Types.
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
    )

  let iter_open_orders_fast ~symbol f =
    Hyperliquid_executions_feed.fold_open_orders symbol ~init:() ~f:(fun () (o : Hyperliquid_executions_feed.open_order) ->
      let limit_price = match o.limit_price with Some p -> p | None -> 0.0 in
      let side_str = match o.side with Hyperliquid_executions_feed.Buy -> "buy" | Sell -> "sell" in
      f o.order_id limit_price o.remaining_qty side_str o.order_userref
    )

  let get_execution_feed_position ~symbol =
    Hyperliquid_executions_feed.get_current_position symbol

  (** Return [true] once the initial execution snapshot has been ingested for [symbol]. *)
  let has_execution_data ~symbol =
    Hyperliquid_executions_feed.has_execution_data symbol

  let read_execution_events ~symbol ~start_pos =
    let events = Hyperliquid_executions_feed.read_execution_events symbol start_pos in
    List.map (fun (e : Hyperliquid_executions_feed.execution_event) ->
      { Types.
        order_id = e.order_id;
        order_status = status_of_hyperliquid_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_hyperliquid_side e.side;
        remaining_qty = max 0.0 (e.order_qty -. e.cum_qty);
        filled_qty = e.cum_qty;
        avg_price = e.avg_price;
        timestamp = e.timestamp;
        is_amended = false;
        cl_ord_id = e.cl_ord_id;
      }
    ) events

  let iter_execution_events ~symbol ~start_pos f =
    Hyperliquid_executions_feed.iter_execution_events symbol start_pos (fun (e : Hyperliquid_executions_feed.execution_event) ->
      f { Types.
        order_id = e.order_id;
        order_status = status_of_hyperliquid_status e.order_status;
        limit_price = e.limit_price;
        side = side_of_hyperliquid_side e.side;
        remaining_qty = max 0.0 (e.order_qty -. e.cum_qty);
        filled_qty = e.cum_qty;
        avg_price = e.avg_price;
        timestamp = e.timestamp;
        is_amended = false;
        cl_ord_id = e.cl_ord_id;
      }
    )



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

  let iter_orderbook_events ~symbol ~start_pos f =
    Hyperliquid_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Hyperliquid_orderbook_feed.orderbook) ->
      let map_levels levels =
        Array.map (fun (l : Hyperliquid_orderbook_feed.level) ->
          l.price, l.size
        ) levels
      in
      f { Types. bids = map_levels ob.bids; asks = map_levels ob.asks; timestamp = ob.timestamp }
    )

  (** Zero-allocation top-of-book iterator. Reads price/size directly
      from Hyperliquid level records without building converted arrays. *)
  let iter_top_of_book_events ~symbol ~start_pos f =
    Hyperliquid_orderbook_feed.iter_orderbook_events symbol start_pos (fun (ob : Hyperliquid_orderbook_feed.orderbook) ->
      if Array.length ob.bids > 0 && Array.length ob.asks > 0 then begin
        let bid = ob.bids.(0) in
        let ask = ob.asks.(0) in
        f bid.price bid.size ask.price ask.size
      end
    )

  (** Instrument metadata and fee accessors *)
  
  let get_price_increment ~symbol = Hyperliquid_instruments_feed.get_price_increment symbol
  let get_qty_increment ~symbol = Hyperliquid_instruments_feed.get_qty_increment symbol
  let get_qty_min ~symbol = Hyperliquid_instruments_feed.get_qty_min symbol
  let round_price ~symbol ~price = Hyperliquid_instruments_feed.round_price_to_tick_for_symbol symbol price

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some (pm, pt, _sm, _st) -> (Some pm, Some pt)
    | None -> (None, None)
    
  let get_spot_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some (_pm, _pt, sm, st) -> (Some sm, Some st)
    | None -> (None, None)

  let initialize_fees symbols =
    if symbols = [] then Lwt.return_unit
    else
      Hyperliquid_get_fee.get_fee_info ~testnet:(Atomic.get is_testnet) () >|= function
      | Some info ->
          let maker = Option.value info.maker_fee ~default:0.0 in
          let taker = Option.value info.taker_fee ~default:0.0 in
          let spot_m = Option.value info.spot_maker_fee ~default:0.0 in
          let spot_t = Option.value info.spot_taker_fee ~default:0.0 in
          List.iter (fun symbol ->
            Hashtbl.replace fee_cache symbol (maker, taker, spot_m, spot_t)
          ) symbols
      | None -> ()

  (** Transfer USDC between spot and perp sub-accounts *)
  let transfer_usd ~amount ~to_perp =
    Hyperliquid_actions.usd_class_transfer
      ~amount
      ~to_perp
      ~testnet:(Atomic.get is_testnet)
end

(** WebSocket-based initialization routines.
    These helpers coordinate between the WS transport layer and the
    domain-specific feed modules during startup. *)
let wait_for_ws_connected () =
  (* Delegates to the Lwt_mvar-based wait in hyperliquid_ws. No polling. *)
  Hyperliquid_ws.wait_for_connected ()

let initialize_instruments_ws () =
  let section = "hyperliquid_startup" in
  Lwt.catch (fun () ->
    wait_for_ws_connected () >>= fun () ->
    let open Yojson.Safe.Util in
    let req_id_perp = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame_perp = `Assoc [
      "method", `String "post";
      "id", `Int req_id_perp;
      "request", `Assoc [
        "type", `String "info";
        "payload", `Assoc [ "type", `String "meta" ]
      ]
    ] in
    let extract_ws_payload resp =
      try 
        let p = resp |> member "data" |> member "response" |> member "payload" in
        let d = member "data" p in
        if d = `Null then p else d
      with _ -> resp
    in
    
    Hyperliquid_ws.send_request ~json:ws_frame_perp ~req_id:req_id_perp ~timeout_ms:5000 >>= fun resp_perp ->
    let payload_perp = extract_ws_payload resp_perp in
    
    let req_id_spot = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame_spot = `Assoc [
      "method", `String "post";
      "id", `Int req_id_spot;
      "request", `Assoc [
        "type", `String "info";
        "payload", `Assoc [ "type", `String "spotMeta" ]
      ]
    ] in
    Hyperliquid_ws.send_request ~json:ws_frame_spot ~req_id:req_id_spot ~timeout_ms:5000 >>= fun resp_spot ->
    let payload_spot = extract_ws_payload resp_spot in
    
    Hyperliquid_instruments_feed.process_meta_response payload_perp payload_spot
  ) (fun exn ->
    Logging.error_f ~section "Failed to fetch instruments via WS: %s" (Printexc.to_string exn);
    Hyperliquid_instruments_feed.notify_ready ();
    Lwt.return_unit
  )

let fetch_spot_balances_ws () =
  let section = "hyperliquid_startup" in
  let parse_json_float json =
    match json with
    | `String s -> (try float_of_string s with _ -> 0.0)
    | `Float f -> f
    | `Int i -> float_of_int i
    | _ -> 0.0
  in
  let wallet = match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> ""
  in
  if wallet = "" then Lwt.return_unit
  else begin
    let req_id = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame = `Assoc [
      "method", `String "post";
      "id", `Int req_id;
      "request", `Assoc [
        "type", `String "info";
        "payload", `Assoc [
          "type", `String "spotClearinghouseState";
          "user", `String wallet
        ]
      ]
    ] in
    Lwt.catch (fun () ->
      wait_for_ws_connected () >>= fun () ->
      Hyperliquid_ws.send_request ~json:ws_frame ~req_id ~timeout_ms:5000 >>= fun resp_frame ->
      let open Yojson.Safe.Util in
      let response_node = try resp_frame |> member "data" |> member "response" with _ -> `Null in
      let payload = try response_node |> member "payload" with _ -> `Null in
      
      let balances_node = try payload |> member "data" |> member "balances" with _ -> `Null in
      
      if balances_node = `Null then begin
        Logging.warn_f ~section "Balances node is null! Raw WS response: %s" (Yojson.Safe.to_string resp_frame);
        Hyperliquid_balances.notify_ready ();
        Lwt.return_unit
      end else begin
        let balances = balances_node |> to_list in
        List.iter (fun item ->
          try
            let raw_coin = member "coin" item |> to_string in
            let coin = Hyperliquid_balances.canonicalize_coin raw_coin in
            let total = parse_json_float (member "total" item) in
            let store = Hyperliquid_balances.get_balance_store coin in
            Hyperliquid_balances.BalanceStore.update_wallet store total "spot" "account";
            
            Logging.info_f ~section "Initial Spot %s balance: %.6f" coin total
          with exn ->
            Logging.warn_f ~section "Failed to parse spot balance entry: %s" (Printexc.to_string exn)
        ) balances;
        Hyperliquid_balances.notify_ready ();
        Lwt.return_unit
      end
    ) (fun exn ->
      Logging.error_f ~section "Failed to fetch spot balances via WS: %s" (Printexc.to_string exn);
      Hyperliquid_balances.notify_ready ();
      Lwt.return_unit
    )
  end

let fetch_open_orders_ws () =
  let section = "hyperliquid_startup" in
  let wallet = match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None -> ""
  in
  if wallet = "" then Lwt.return_unit
  else begin
    let req_id = Hyperliquid_actions.next_ws_req_id () in
    let ws_frame = `Assoc [
      "method", `String "post";
      "id", `Int req_id;
      "request", `Assoc [
        "type", `String "info";
        "payload", `Assoc [
          "type", `String "openOrders";
          "user", `String wallet
        ]
      ]
    ] in
    Lwt.catch (fun () ->
      wait_for_ws_connected () >>= fun () ->
      Hyperliquid_ws.send_request ~json:ws_frame ~req_id ~timeout_ms:5000 >>= fun resp_frame ->
      let open Yojson.Safe.Util in
      (* Extract payload from data.response.payload.data, falling back to data.response.payload *)
      let response_node = try resp_frame |> member "data" |> member "response" with _ -> `Null in
      let payload = try response_node |> member "payload" with _ -> `Null in
      let data_node = try payload |> member "data" with _ -> `Null in
      let orders_json = if data_node <> `Null then data_node else payload in
      Hyperliquid_executions_feed.inject_open_orders orders_json;
      (* Signal that the open-order snapshot has been fully injected.
         Domain workers block on this event instead of a fixed wall-clock delay. *)
      Hyperliquid_executions_feed.set_startup_snapshot_done ();
      Lwt.return_unit
    ) (fun exn ->
      Logging.error_f ~section "Failed to fetch open orders via WS: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  end

(* Register Hyperliquid_impl with the exchange registry at module load time *)
let () =
  Exchange.Registry.register (module Hyperliquid_impl)
