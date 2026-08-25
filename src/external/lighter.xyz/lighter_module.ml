(** [Exchange_intf] implementation for Lighter L2: wires the WebSocket feeds,
    FFI signer, action endpoints, and instrument metadata behind the
    venue-agnostic interface consumed by strategies. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

module Lighter_impl = struct
  let name = "lighter"
  let section = "lighter_module"

  (* symbol -> (maker_fee, taker_fee) cache. *)
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16

  (* Conversions between Lighter feed types and the agnostic [Types]. *)
  let status_of_lighter_status = function
    | Lighter_executions_feed.PendingStatus -> Types.Pending
    | Lighter_executions_feed.NewStatus -> Types.New
    | Lighter_executions_feed.PartiallyFilledStatus -> Types.PartiallyFilled
    | Lighter_executions_feed.FilledStatus -> Types.Filled
    | Lighter_executions_feed.CanceledStatus -> Types.Canceled
    | Lighter_executions_feed.ExpiredStatus -> Types.Expired
    | Lighter_executions_feed.RejectedStatus -> Types.Rejected
    | Lighter_executions_feed.UnknownStatus s -> Types.Unknown s
  ;;

  let side_of_lighter_side = function
    | Lighter_executions_feed.Buy -> Types.Buy
    | Lighter_executions_feed.Sell -> Types.Sell
  ;;

  (** Places an order; rounds price/qty to the instrument's tick and lot sizes
      before submission. *)
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
        ()
    =
    let is_buy =
      match side with
      | Types.Buy -> true
      | Types.Sell -> false
    in
    let px =
      match limit_price with
      | Some p -> p
      | None ->
        (match order_type with
         | Types.Market ->
           (* No true market orders: cross the spread with a 5% slippage
               tolerance to maximize fill odds. *)
           (match Lighter_orderbook_feed.get_best_bid_ask symbol with
            | Some (bid, _, ask, _) -> if is_buy then ask *. 1.05 else bid *. 0.95
            | None ->
              Logging.warn_f ~section "No orderbook data for market order on %s" symbol;
              0.0)
         | _ -> 0.0)
    in
    let tif =
      match time_in_force with
      | Some t -> t
      | None -> Types.GTC
    in
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
  ;;

  (** Amends qty/price of an existing order in place, without a
      cancel-and-replace cycle. *)
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
        ()
    =
    let existing = Lighter_executions_feed.find_order_everywhere order_id in
    let sym =
      match symbol with
      | Some s -> s
      | None ->
        (match existing with
         | Some o -> o.symbol
         | None -> "")
    in
    if sym = ""
    then Lwt.return (Error "Cannot amend: symbol unknown and order not found")
    else if Option.is_none existing
    then
      (* Reject amends for orders missing from the live cache (e.g. after a
         WS reconnect): retrying against an untracked phantom order would
         spam L2 modify txs; surfacing the error engages the upstream amend
         failure cooldown instead. *)
      Lwt.return (Error (Printf.sprintf "Order not found for amendment: %s" order_id))
    else (
      let new_qty =
        match qty with
        | Some q -> Lighter_instruments_feed.round_qty_for_symbol sym q
        | None ->
          (match existing with
           | Some o -> o.order_qty
           | None -> 0.0)
      in
      let new_price =
        match limit_price with
        | Some p -> Lighter_instruments_feed.round_price_for_symbol sym p
        | None ->
          (match existing with
           | Some o ->
             (match o.limit_price with
              | Some p -> p
              | None -> 0.0)
           | None -> 0.0)
      in
      Lighter_actions.modify_order ~symbol:sym ~order_id ~new_qty ~new_price
      >|= function
      | Ok res -> Ok res
      | Error msg -> Error msg)
  ;;

  (** Cancels a list of orders, resolving each symbol from the executions feed
      when not supplied. *)
  let cancel_orders
        ~token:_
        ?order_ids
        ?cl_ord_ids:_
        ?order_userrefs:_
        ?symbol
        ?retry_config:_
        ()
    =
    let ids =
      match order_ids with
      | Some ids -> ids
      | None -> []
    in
    if ids = []
    then Lwt.return (Error "No order IDs to cancel")
    else (
      let results =
        Lwt_list.map_s
          (fun order_id ->
             (* Use the given symbol, else look the order up in the
                executions feed. *)
             let sym =
               match symbol with
               | Some s -> s
               | None ->
                 (match Lighter_executions_feed.find_order_everywhere order_id with
                  | Some o -> o.symbol
                  | None -> "")
             in
             if sym = ""
             then
               Lwt.return
                 (Error (Printf.sprintf "Cannot cancel %s: symbol unknown" order_id))
             else Lighter_actions.cancel_order ~symbol:sym ~order_id)
          ids
      in
      results
      >|= fun res_list ->
      let successes =
        List.filter_map
          (function
            | Ok r -> Some r
            | Error _ -> None)
          res_list
      in
      let errors =
        List.filter_map
          (function
            | Error msg -> Some msg
            | Ok _ -> None)
          res_list
      in
      if errors = [] then Ok successes else Error (String.concat "; " errors))
  ;;

  (* Read-only market data accessors. *)

  let subscribe_orderbook ~symbols = Lighter_ws.subscribe_public_orderbook ~symbols
  let get_top_of_book ~symbol = Lighter_orderbook_feed.get_best_bid_ask symbol
  let get_top_of_book_fast ~symbol = Lighter_orderbook_feed.get_best_bid_ask_fast symbol
  let get_tradeable_balance ~asset = Lighter_balances.get_balance asset

  let get_tradeable_balance_fast ~asset =
    let store = Lighter_balances.get_balance_store asset in
    fun () -> Lighter_balances.BalanceStore.get_balance store
  ;;

  (* No freshness tracking: unknown age (treated as stale by strategies, which
     preserves the previous attempt-anyway behavior). *)
  let get_balance_age_fast ~asset:_ = fun () -> None
  let get_total_balance ~asset = Lighter_balances.get_balance asset
  let get_staked_balance ~asset:_ = 0.0
  let get_all_balances () = Lighter_balances.get_all_balances ()

  let get_open_order ~symbol ~order_id =
    match Lighter_executions_feed.get_open_order symbol order_id with
    | Some o ->
      Some
        { Types.order_id = o.order_id
        ; symbol = o.symbol
        ; side = side_of_lighter_side o.side
        ; qty = o.order_qty
        ; cum_qty = o.cum_qty
        ; remaining_qty = o.remaining_qty
        ; limit_price = o.limit_price
        ; status = status_of_lighter_status o.order_status
        ; user_ref = o.order_userref
        ; cl_ord_id = o.cl_ord_id
        }
    | None -> None
  ;;

  let get_open_orders ~symbol =
    let orders = Lighter_executions_feed.get_open_orders symbol in
    List.map
      (fun (o : Lighter_executions_feed.open_order) ->
         { Types.order_id = o.order_id
         ; symbol = o.symbol
         ; side = side_of_lighter_side o.side
         ; qty = o.order_qty
         ; cum_qty = o.cum_qty
         ; remaining_qty = o.remaining_qty
         ; limit_price = o.limit_price
         ; status = status_of_lighter_status o.order_status
         ; user_ref = o.order_userref
         ; cl_ord_id = o.cl_ord_id
         })
      orders
  ;;

  let get_all_orders_for_asset ~asset =
    let prefix = asset ^ "/" in
    let all_symbols = Lighter_executions_feed.get_all_symbols () in
    let matching = List.filter (fun sym -> String.starts_with ~prefix sym) all_symbols in
    List.concat_map (fun symbol -> get_open_orders ~symbol) matching
  ;;

  let fold_open_orders ~symbol ~init ~f =
    Lighter_executions_feed.fold_open_orders
      symbol
      ~init
      ~f:(fun acc (o : Lighter_executions_feed.open_order) ->
        f
          acc
          { Types.order_id = o.order_id
          ; symbol = o.symbol
          ; side = side_of_lighter_side o.side
          ; qty = o.order_qty
          ; cum_qty = o.cum_qty
          ; remaining_qty = o.remaining_qty
          ; limit_price = o.limit_price
          ; status = status_of_lighter_status o.order_status
          ; user_ref = o.order_userref
          ; cl_ord_id = o.cl_ord_id
          })
  ;;

  let iter_open_orders_fast ~symbol f =
    Lighter_executions_feed.fold_open_orders
      symbol
      ~init:()
      ~f:(fun () (o : Lighter_executions_feed.open_order) ->
        let limit_price =
          match o.limit_price with
          | Some p -> p
          | None -> 0.0
        in
        let side_str =
          match o.side with
          | Lighter_executions_feed.Buy -> "buy"
          | Sell -> "sell"
        in
        f o.order_id limit_price o.remaining_qty side_str o.order_userref)
  ;;

  let get_execution_feed_position ~symbol =
    Lighter_executions_feed.get_current_position symbol
  ;;

  let get_execution_feed_position_fast ~symbol =
    Lighter_executions_feed.get_current_position_fast symbol
  ;;

  let has_execution_data ~symbol = Lighter_executions_feed.has_execution_data symbol

  let has_execution_data_fast ~symbol =
    Lighter_executions_feed.has_execution_data_fast symbol
  ;;

  let read_execution_events ~symbol ~start_pos =
    let events = Lighter_executions_feed.read_execution_events symbol start_pos in
    List.map
      (fun (e : Lighter_executions_feed.execution_event) ->
         { Types.order_id = e.order_id
         ; order_status = status_of_lighter_status e.order_status
         ; limit_price = e.limit_price
         ; side = side_of_lighter_side e.side
         ; remaining_qty = max 0.0 (e.order_qty -. e.cum_qty)
         ; filled_qty = e.cum_qty
         ; avg_price = e.avg_price
         ; timestamp = e.timestamp
         ; is_amended = e.is_amended
         ; cl_ord_id = e.cl_ord_id
         })
      events
  ;;

  let iter_execution_events ~symbol ~start_pos f =
    Lighter_executions_feed.iter_execution_events
      symbol
      start_pos
      (fun (e : Lighter_executions_feed.execution_event) ->
         f
           { Types.order_id = e.order_id
           ; order_status = status_of_lighter_status e.order_status
           ; limit_price = e.limit_price
           ; side = side_of_lighter_side e.side
           ; remaining_qty = max 0.0 (e.order_qty -. e.cum_qty)
           ; filled_qty = e.cum_qty
           ; avg_price = e.avg_price
           ; timestamp = e.timestamp
           ; is_amended = e.is_amended
           ; cl_ord_id = e.cl_ord_id
           })
  ;;

  let get_orderbook_position ~symbol = Lighter_orderbook_feed.get_current_position symbol

  let get_orderbook_position_fast ~symbol =
    Lighter_orderbook_feed.get_current_position_fast symbol
  ;;

  let read_orderbook_events ~symbol ~start_pos =
    let events = Lighter_orderbook_feed.read_orderbook_events symbol start_pos in
    List.map
      (fun (ob : Lighter_orderbook_feed.orderbook) ->
         let map_levels levels =
           Array.map (fun (l : Lighter_orderbook_feed.level) -> l.price, l.size) levels
         in
         { Types.bids = map_levels ob.bids
         ; asks = map_levels ob.asks
         ; timestamp = ob.timestamp
         })
      events
  ;;

  let iter_orderbook_events ~symbol ~start_pos f =
    Lighter_orderbook_feed.iter_orderbook_events
      symbol
      start_pos
      (fun (ob : Lighter_orderbook_feed.orderbook) ->
         let map_levels levels =
           Array.map (fun (l : Lighter_orderbook_feed.level) -> l.price, l.size) levels
         in
         f
           { Types.bids = map_levels ob.bids
           ; asks = map_levels ob.asks
           ; timestamp = ob.timestamp
           })
  ;;

  (** Iterates best bid/ask (price, size) per snapshot since [start_pos]. *)
  let iter_top_of_book_events ~symbol ~start_pos f =
    Lighter_orderbook_feed.iter_orderbook_events
      symbol
      start_pos
      (fun (ob : Lighter_orderbook_feed.orderbook) ->
         if Array.length ob.bids > 0 && Array.length ob.asks > 0
         then (
           let bid = ob.bids.(0) in
           let ask = ob.asks.(0) in
           f bid.price bid.size ask.price ask.size))
  ;;

  (** Instrument spec queries: tick/lot increments, minimums, and rounding. *)

  let get_price_increment ~symbol = Lighter_instruments_feed.get_price_increment symbol
  let get_qty_increment ~symbol = Lighter_instruments_feed.get_qty_increment symbol
  let get_qty_min ~symbol = Lighter_instruments_feed.get_qty_min symbol

  let round_price ~symbol ~price =
    Lighter_instruments_feed.round_price_for_symbol symbol price
  ;;

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some (maker, taker) -> Some maker, Some taker
    | None ->
      (* Cache miss: load fees from the instruments feed. *)
      (match Lighter_instruments_feed.lookup_info symbol with
       | Some info ->
         Hashtbl.replace fee_cache symbol (info.maker_fee, info.taker_fee);
         Some info.maker_fee, Some info.taker_fee
       | None -> None, None)
  ;;
end

(** Blocks until both Lighter WS connections are up. *)
let wait_for_ws_connected () = Lighter_ws.wait_for_connected ()

let initialize_signer () =
  let section = "lighter_startup" in
  let private_key =
    match Sys.getenv_opt "LIGHTER_API_PRIVATE_KEY" |> Option.map String.trim with
    | Some k when k <> "" -> k
    | _ -> failwith "LIGHTER_API_PRIVATE_KEY env var is required"
  in
  let api_key_index =
    match Sys.getenv_opt "LIGHTER_API_KEY_INDEX" |> Option.map String.trim with
    | Some s ->
      (try int_of_string s with
       | _ -> 0)
    | None -> 0
  in
  let account_index =
    match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
    | Some s ->
      (try int_of_string s with
       | _ -> 0)
    | None -> 0
  in
  let base_url = Lighter_proxy.api_base_url () in
  match
    Lighter_signer.initialize
      ~base_url
      ~private_key
      ~key_index:api_key_index
      ~acct_index:account_index
  with
  | Ok () ->
    Logging.info_f ~section "Lighter signer initialized successfully";
    Lighter_signer.initialize_nonce ~base_url ~api_key_index ~account_index
  | Error msg ->
    Logging.error_f ~section "Failed to initialize Lighter signer: %s" msg;
    Lwt.return_unit
;;

(** Fetches instrument metadata over REST for [symbols] into the instruments
    feed. *)
let initialize_instruments ~symbols =
  let base_url = Lighter_proxy.api_base_url () in
  Lighter_instruments_feed.fetch_and_initialize ~base_url ~required_symbols:symbols
;;

(** Pulls open orders for the account over REST and feeds them into the
    executions feed to reconcile state after a reconnect. *)
let fetch_open_orders () =
  let section = "lighter_startup" in
  let account_index =
    match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
    | Some s ->
      (try int_of_string s with
       | _ -> 0)
    | None -> 0
  in
  let base_url = Lighter_proxy.api_base_url () in
  let token = Lighter_signer.get_auth_token () in
  (* accountActiveOrders takes account_index and auth token query params. *)
  let url =
    Printf.sprintf
      "%s/api/v1/accountActiveOrders?account_index=%d&auth=%s"
      base_url
      account_index
      token
  in
  Lwt.catch
    (fun () ->
       let uri = Uri.of_string url in
       let%lwt resp, body = Cohttp_lwt_unix.Client.get uri in
       let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
       let%lwt body_str = Cohttp_lwt.Body.to_string body in
       if status < 200 || status >= 300
       then (
         Logging.error_f
           ~section
           "openOrders request failed: HTTP %d (body=%s)"
           status
           (if String.length body_str > 200
            then String.sub body_str 0 200 ^ "..."
            else body_str);
         if status >= 500 then Lighter_proxy.rotate_proxy ();
         Lighter_executions_feed.set_startup_snapshot_done ();
         Lwt.return_unit)
       else (
         let trimmed = String.trim body_str in
         if trimmed = "" || trimmed = "{}" || trimmed = "[]"
         then (
           Logging.info_f ~section "No open orders on Lighter (empty response)";
           Lighter_executions_feed.set_startup_snapshot_done ();
           Lwt.return_unit)
         else (
           let json = Yojson.Safe.from_string trimmed in
           Lighter_executions_feed.handle_snapshot json;
           Lighter_executions_feed.set_startup_snapshot_done ();
           Logging.debug_f ~section "Fetched and injected open orders";
           Lwt.return_unit)))
    (fun exn ->
       Logging.error_f ~section "Failed to fetch open orders: %s" (Printexc.to_string exn);
       Lighter_executions_feed.set_startup_snapshot_done ();
       Lwt.return_unit)
;;

(** Fetches all asset balances via the REST API at startup, ensuring balances
    (including USDC) are populated before domains start. Handles both unified
    accounts (USDC is account-level collateral) and split accounts (USDC is
    in the assets array). *)
let fetch_balances () =
  let section = "lighter_startup" in
  let account_index =
    match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
    | Some s ->
      (try int_of_string s with
       | _ -> 0)
    | None -> 0
  in
  let base_url = Lighter_proxy.api_base_url () in
  let url = Printf.sprintf "%s/api/v1/account?by=index&value=%d" base_url account_index in
  Lwt.catch
    (fun () ->
       let uri = Uri.of_string url in
       let%lwt resp, body = Cohttp_lwt_unix.Client.get uri in
       let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
       let%lwt body_str = Cohttp_lwt.Body.to_string body in
       if status < 200 || status >= 300
       then (
         Logging.error_f
           ~section
           "Lighter account request failed: HTTP %d (body=%s)"
           status
           (if String.length body_str > 300
            then String.sub body_str 0 300 ^ "..."
            else body_str);
         if status >= 500 then Lighter_proxy.rotate_proxy ();
         Lwt.return_unit)
       else (
         let trimmed = String.trim body_str in
         Logging.info_f
           ~section
           "Lighter account response: %s"
           (if String.length trimmed > 500
            then String.sub trimmed 0 500 ^ "..."
            else trimmed);
         let json = Yojson.Safe.from_string trimmed in
         let open Yojson.Safe.Util in
         let accounts =
           try member "accounts" json |> to_list with
           | _ -> []
         in
         match accounts with
         | [] ->
           Logging.error_f ~section "Lighter account response has no accounts";
           Lwt.return_unit
         | account :: _ ->
           let assets =
             try member "assets" account |> to_list with
             | _ -> []
           in
           let assets_assoc =
             List.map
               (fun asset_json ->
                  let asset_id =
                    try member "asset_id" asset_json |> to_int |> string_of_int with
                    | _ -> "?"
                  in
                  asset_id, asset_json)
               assets
           in
           (* For unified accounts, USDC lives at the account level as
             collateral/available_balance, not in the assets array.
             Check if assets already has a positive USDC entry; if not,
             inject the account-level collateral as a synthetic USDC asset. *)
           let has_usdc_in_assets =
             List.exists
               (fun (_id, aj) ->
                  let sym =
                    try member "symbol" aj |> to_string with
                    | _ -> ""
                  in
                  let bal =
                    try Lighter_types.parse_json_float (member "balance" aj) with
                    | _ -> 0.0
                  in
                  sym = "USDC" && bal > 0.0)
               assets_assoc
           in
           let final_assets =
             if has_usdc_in_assets
             then assets_assoc
             else (
               (* Read account-level collateral (unified account USDC balance) *)
               let collateral =
                 try Lighter_types.parse_json_float (member "collateral" account) with
                 | _ -> 0.0
               in
               let available =
                 try
                   Lighter_types.parse_json_float (member "available_balance" account)
                 with
                 | _ -> 0.0
               in
               let usdc_balance = max collateral available in
               Logging.info_f
                 ~section
                 "Unified account detected: injecting USDC from account-level \
                  collateral=%.6f available=%.6f -> %.6f"
                 collateral
                 available
                 usdc_balance;
               if usdc_balance > 0.0
               then
                 assets_assoc
                 @ [ ( "3"
                     , `Assoc
                         [ "symbol", `String "USDC"
                         ; "asset_id", `Int 3
                         ; "balance", `String (Printf.sprintf "%.6f" usdc_balance)
                         ; "locked_balance", `String "0.000000"
                         ] )
                   ]
               else assets_assoc)
           in
           let synthetic_json =
             `Assoc
               [ "type", `String "snapshot/account_all_assets"
               ; "channel", `String (Printf.sprintf "account_all_assets/%d" account_index)
               ; "account_all", `Assoc [ "assets", `Assoc final_assets ]
               ]
           in
           Lighter_balances.process_market_data synthetic_json;
           let usdc_bal = Lighter_balances.get_balance "USDC" in
           Logging.info_f
             ~section
             "Lighter balances fetched via REST: USDC=%.6f (%d assets from array, %d \
              total injected)"
             usdc_bal
             (List.length assets)
             (List.length final_assets);
           Lwt.return_unit))
    (fun exn ->
       Logging.error_f
         ~section
         "Failed to fetch Lighter balances: %s"
         (Printexc.to_string exn);
       Lwt.return_unit)
;;

(* Register the implementation with the exchange registry at module load. *)
let () = Exchange.Registry.register (module Lighter_impl)
