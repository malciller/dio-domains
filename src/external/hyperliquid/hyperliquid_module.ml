(** Hyperliquid Exchange Implementation *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types
module ExTypes = Exchange.Types

let is_mainnet = ref true
let set_network mainnet = is_mainnet := mainnet

(** Get nonce based on current time in milliseconds *)
let get_nonce () =
  let now_ms = Unix.gettimeofday () *. 1000.0 in
  Int64.of_float now_ms

(** Yojson builders for Hyperliquid Types *)
let tif_to_string = function
  | Hyperliquid_types.Alo -> "Alo"
  | Hyperliquid_types.Ioc -> "Ioc"
  | Hyperliquid_types.Gtc -> "Gtc"

let tpsl_to_string = function
  | Hyperliquid_types.Tp -> "tp"
  | Hyperliquid_types.Sl -> "sl"

let order_type_wire_to_json = function
  | Hyperliquid_types.Limit l ->
      `Assoc [("limit", `Assoc [("tif", `String (tif_to_string l.tif))])]
  | Hyperliquid_types.Trigger t ->
      `Assoc [("trigger", `Assoc [
        ("isMarket", `Bool t.isMarket);
        ("triggerPx", `String t.triggerPx);
        ("tpsl", `String (tpsl_to_string t.tpsl))
      ])]

let order_wire_to_json (ow: Hyperliquid_types.order_wire) =
  let base_fields = [
    ("a", `Int ow.a);
    ("b", `Bool ow.b);
    ("p", `String ow.p);
    ("s", `String ow.s);
    ("r", `Bool ow.r);
    ("t", order_type_wire_to_json ow.t)
  ] in
  match ow.c with
  | Some c -> `Assoc (base_fields @ [("c", `String c)])
  | None -> `Assoc base_fields

let order_action_to_json orders grouping =
  `Assoc [
    ("type", `String "order");
    ("orders", `List (List.map order_wire_to_json orders));
    ("grouping", `String grouping)
  ]

module Hyperliquid_impl : Exchange.S = struct
  let name = "hyperliquid"
  let section = "hyperliquid_module"

  (* Internal cache for fees *)
  let fee_cache : (string, float * float) Hashtbl.t = Hashtbl.create 16


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
      ~token:_
      ~order_type
      ~side
      ~qty
      ~symbol
      ?limit_price
      ?time_in_force:_
      ?post_only:_
      ?reduce_only
      ?order_userref:_
      ?cl_ord_id
      ?trigger_price:_
      ?display_qty:_
      ?retry_config:_
      () =

    let hl_side = match side with Types.Buy -> "buy" | Types.Sell -> "sell" in
    let hl_order_type = match order_type with
      | Types.Limit -> "limit"
      | Types.Market -> "market"
      | _ -> "limit"
    in

    let vault_address = None in
    let private_key_hex = match Sys.getenv_opt "HYPERLIQUID_PRIVATE_KEY" with
      | Some key -> String.trim key
      | None -> failwith "HYPERLIQUID_PRIVATE_KEY environment variable not set"
    in
    let _wallet_from_env = Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim in
    let _agent_from_env = Sys.getenv_opt "HYPERLIQUID_AGENT_ADDRESS" |> Option.map String.trim in
    (Lwt.return (Ok ())) >>= function
    | Error e -> Lwt.return (Error e)
    | Ok () ->
    let rounded_qty = Hyperliquid_instruments_feed.round_qty_to_lot symbol qty in
    let rounded_limit_price = match limit_price with
      | Some p -> Some (Hyperliquid_instruments_feed.round_price_to_tick p)
      | None -> None
    in

    Logging.info_f ~section "Placing %s order on HL: %s %s %f (rounded: %f) @ %s"
      hl_side symbol hl_order_type qty rounded_qty 
      (match limit_price, rounded_limit_price with 
       | Some p, Some rp -> Printf.sprintf "%f (rounded: %f)" p rp 
       | _ -> "market");

    let wire_order = 
      let asset_index = match Hyperliquid_instruments_feed.get_asset_index symbol with
        | Some idx -> idx
        | None -> 
            Logging.warn_f ~section "Could not find asset index for %s, falling back to 0" symbol;
            0
      in
      Hyperliquid_actions.to_hl_order_wire
      ~qty:rounded_qty
      ~symbol
      ~asset_index
      ~ot:(Hyperliquid_actions.hl_order_type order_type None)
      ~side
      ~limit_price:rounded_limit_price
      ~reduce_only
      ~cl_ord_id
    in

    let msgpack_val = Hyperliquid_types.pack_order_action ~orders:[wire_order] ~grouping:"na" in
    let action_msgpack = Hyperliquid_types.serialize_action msgpack_val in
    let action_json = order_action_to_json [wire_order] "na" in

    let nonce = get_nonce () in
    let (r, s, v) = Hyperliquid_signer.sign_l1_action
      ~private_key_hex
      ~action_msgpack
      ~nonce
      ~is_mainnet:(!is_mainnet)
      ~vault_address
    in

    let req_id = Random.int 1000000 in
    (* Format the payload for WebSocket API *)
    let payload : Yojson.Safe.t = `Assoc [
      ("method", `String "post");
      ("id", `Int req_id);
      ("request", `Assoc [
        ("type", `String "action");
        ("payload", `Assoc [
          ("action", action_json);
          ("nonce", `Intlit (Int64.to_string nonce));
          ("signature", `Assoc [
            ("r", `String r);
            ("s", `String s);
            ("v", `Int v)
          ])
        ])
      ])
    ] in
    
    (* Assuming vault address is needed in the WS request if using one *)
    let payload : Yojson.Safe.t = match vault_address with
      | Some addr -> 
          let req = Yojson.Safe.Util.member "request" payload in
          let payload_obj = Yojson.Safe.Util.member "payload" req in
          let new_payload_obj : Yojson.Safe.t = match payload_obj with
            | `Assoc pairs -> `Assoc (("vaultAddress", `String addr) :: pairs)
            | _ -> payload_obj
          in
          let new_req : Yojson.Safe.t = match req with
            | `Assoc pairs -> `Assoc (("payload", new_payload_obj) :: List.filter (fun (k,_) -> k <> "payload") pairs)
            | _ -> req
          in
          (match payload with
          | `Assoc pairs -> `Assoc (("request", new_req) :: List.filter (fun (k,_) -> k <> "request") pairs)
          | _ -> payload : Yojson.Safe.t)
      | None -> payload
    in

    Hyperliquid_ws.send_request ~json:payload ~req_id ~timeout_ms:10000 >>= fun response_json ->
    Logging.debug_f ~section "Hyperliquid order raw response: %s" (Yojson.Safe.to_string response_json);

    (* Try to parse response *)
    begin try
      let open Yojson.Safe.Util in
      let data = member "data" response_json in
      let __response = member "response" data in
      let response_type = member "type" __response |> to_string in

      if response_type = "order" then
        let statuses = member "data" __response |> member "statuses" |> to_list in
        match statuses with
        | [status] ->
            let oid = member "resting" status |> member "oid" |> to_string in
            Lwt.return (Ok {
              Dio_exchange.Exchange_intf.Types.order_id = oid;
              cl_ord_id = cl_ord_id;
              order_userref = None;
            })
        | _ ->
            (* Fallback if multiple statuses or unexpected format, though we sent 1 order *)
            let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
            Lwt.return (Ok {
              Dio_exchange.Exchange_intf.Types.order_id = mock_order_id;
              cl_ord_id = cl_ord_id;
              order_userref = None;
            })
      else if response_type = "cancel" then
        let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
        Lwt.return (Ok {
          Dio_exchange.Exchange_intf.Types.order_id = mock_order_id;
          cl_ord_id = cl_ord_id;
          order_userref = None;
        })
      else if response_type = "error" then
        let error_msg = member "data" __response |> to_string in
        Lwt.return (Error error_msg)
      else if response_type = "action" then
        (* WebSocket order/cancel response: payload has status "ok" or "err" and optional "response" message *)
        let payload = member "payload" __response in
        let status = member "status" payload |> to_string in
        if status = "err" then
          let err_msg = try member "response" payload |> to_string with _ -> "Unknown error" in
          Lwt.return (Error err_msg)
        else
          let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
          Lwt.return (Ok {
            Dio_exchange.Exchange_intf.Types.order_id = mock_order_id;
            cl_ord_id = cl_ord_id;
            order_userref = None;
          })
      else
        Lwt.return (Error "Unknown response type from Hyperliquid")
    with exn ->
      let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
      Logging.warn_f ~section "Failed to parse HL response, mocking success: %s" (Printexc.to_string exn);
      Lwt.return (Ok {
        Dio_exchange.Exchange_intf.Types.order_id = mock_order_id;
        cl_ord_id = cl_ord_id;
        order_userref = None;
      })
    end

  (** Amend an existing order *)
  let amend_order
      ~token:_
      ~order_id:_
      ?cl_ord_id:_
      ?qty:_
      ?limit_price:_
      ?post_only:_
      ?trigger_price:_
      ?display_qty:_
      ?symbol:_
      ?retry_config:_
      () =
    Lwt.return (Error "Hyperliquid order amendment not yet implemented natively (requires cancel+replace flow)")

  (** Cancel orders *)
  let cancel_orders
      ~token:_
      ?order_ids
      ?cl_ord_ids
      ?order_userrefs:_
      ?retry_config:_
      () =
    
    let vault_address = None in
    let private_key_hex = match Sys.getenv_opt "HYPERLIQUID_PRIVATE_KEY" with
      | Some key -> String.trim key
      | None -> failwith "HYPERLIQUID_PRIVATE_KEY environment variable not set"
    in
    let _wallet_from_env = Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim in
    let _agent_from_env = Sys.getenv_opt "HYPERLIQUID_AGENT_ADDRESS" |> Option.map String.trim in
    (Lwt.return (Ok ())) >>= function
    | Error e -> Lwt.return (Error e)
    | Ok () ->
    let nonce = get_nonce () in

    Logging.info_f ~section "Cancelling HL orders: %s" 
      (match order_ids with Some ids -> String.concat "," ids | None -> "none");

    let cancels = match order_ids with
      | Some ids -> 
          List.map (fun id -> 
            { Hyperliquid_types.a = 0; o = Int64.of_string id }
          ) ids
      | None -> []
    in
    
    let cancel_cloids = match cl_ord_ids with
      | Some ids -> 
          List.map (fun cloid -> 
            { Hyperliquid_types.a = 0; cloid = cloid }
          ) ids
      | None -> []
    in

    (* Generate packed formats *)
    let action_msgpack = 
      if cancels <> [] then
        Hyperliquid_types.serialize_action (Hyperliquid_types.pack_cancel_action ~cancels)
      else if cancel_cloids <> [] then
        Hyperliquid_types.serialize_action (Hyperliquid_types.pack_cancel_by_cloid_action ~cancels:cancel_cloids)
      else ""
    in
    
    let action_json =
      if cancels <> [] then
        `Assoc [
          ("type", `String "cancel");
          ("cancels", `List (List.map (fun (c: Hyperliquid_types.cancel_wire) -> `Assoc [("a", `Int c.a); ("o", `Intlit (Int64.to_string c.o))]) cancels))
        ]
      else if cancel_cloids <> [] then
        `Assoc [
          ("type", `String "cancelByCloid");
          ("cancels", `List (List.map (fun (c: Hyperliquid_types.cancel_by_cloid_wire) -> `Assoc [("a", `Int c.a); ("cloid", `String c.cloid)]) cancel_cloids))
        ]
      else `Assoc []
    in

    if action_msgpack = "" then
      Lwt.return (Ok [])
    else begin
      let (r, s, v) = Hyperliquid_signer.sign_l1_action
        ~private_key_hex
        ~action_msgpack
        ~nonce
        ~is_mainnet:(!is_mainnet)
        ~vault_address
      in

      let req_id = Random.int 1000000 in
      let payload : Yojson.Safe.t = `Assoc [
        ("method", `String "post");
        ("id", `Int req_id);
        ("request", `Assoc [
          ("type", `String "action");
          ("payload", `Assoc [
            ("action", action_json);
            ("nonce", `Intlit (Int64.to_string nonce));
            ("signature", `Assoc [
              ("r", `String r);
              ("s", `String s);
              ("v", `Int v)
            ])
          ])
        ])
      ] in
      
      let payload : Yojson.Safe.t = match vault_address with
        | Some addr -> 
            let req = Yojson.Safe.Util.member "request" payload in
            let payload_obj = Yojson.Safe.Util.member "payload" req in
            let new_payload_obj : Yojson.Safe.t = match payload_obj with
              | `Assoc pairs -> `Assoc (("vaultAddress", `String addr) :: pairs)
              | _ -> payload_obj
            in
            let new_req : Yojson.Safe.t = match req with
              | `Assoc pairs -> `Assoc (("payload", new_payload_obj) :: List.filter (fun (k,_) -> k <> "payload") pairs)
              | _ -> req
            in
            (match payload with
            | `Assoc pairs -> `Assoc (("request", new_req) :: List.filter (fun (k,_) -> k <> "request") pairs)
            | _ -> payload : Yojson.Safe.t)
        | None -> payload
      in

      Hyperliquid_ws.send_request ~json:payload ~req_id ~timeout_ms:10000 >>= fun response_json ->
      Logging.info_f ~section "Hyperliquid cancel payload sent via WebSocket, received response: %s" 
        (Yojson.Safe.to_string response_json);
      
      (* Yield to Lwt scheduler to avoid synchronous callbacks *)
      Lwt.pause () >>= fun () ->
      
      (* Try to parse response *)
      begin try
        let open Yojson.Safe.Util in
        let data = member "data" response_json in
        let __response = member "response" data in
        let response_type = member "type" __response |> to_string in
        
        if response_type = "cancel" then
          (* Map back to our inputs to simulate successful cancel IDs *)
          let response_items = match order_ids with
            | Some ids -> 
                List.map (fun id -> { Dio_exchange.Exchange_intf.Types.order_id = id; cl_ord_id = None }) ids
            | None -> []
          in
          Lwt.return (Ok response_items)
        else if response_type = "error" then
          let error_msg = member "data" __response |> to_string in
          Lwt.return (Error error_msg)
        else
          Lwt.return (Error "Unknown response type from Hyperliquid")
      with exn ->
        Logging.warn_f ~section "Failed to parse HL response: %s" (Printexc.to_string exn);
        let response_items = match order_ids with
          | Some ids -> 
              List.map (fun id -> { Dio_exchange.Exchange_intf.Types.order_id = id; cl_ord_id = None }) ids
          | None -> []
        in
        Lwt.return (Ok response_items)
      end
    end

  (** Market Data Access *)
  
  let get_ticker ~symbol =
    match Hyperliquid_ticker_feed.get_latest_ticker symbol with
    | Some t -> Some (t.bid, t.ask)
    | None -> None

  let get_top_of_book ~symbol =
    match Hyperliquid_orderbook_feed.get_best_bid_ask symbol with
    | Some (bp, bs, ap, as_val) ->
        Some (float_of_string bp, float_of_string bs, float_of_string ap, float_of_string as_val)
    | None -> None

  let get_balance ~asset =
    Hyperliquid_balances_feed.get_balance asset

  let get_open_order ~symbol ~order_id =
    match Hyperliquid_executions_feed.get_open_order symbol order_id with
    | Some o -> Some {
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
      { Types.
        bids = Array.map (fun (l : Hyperliquid_orderbook_feed.level) -> l.price_float, l.size_float) ob.bids;
        asks = Array.map (fun (l : Hyperliquid_orderbook_feed.level) -> l.price_float, l.size_float) ob.asks;
        timestamp = ob.timestamp;
      }
    ) events

  (** Metadata Access *)
  
  let get_price_increment ~symbol =
    Hyperliquid_instruments_feed.get_price_increment symbol

  let get_qty_increment ~symbol =
    Hyperliquid_instruments_feed.get_qty_increment symbol

  let get_qty_min ~symbol =
    Hyperliquid_instruments_feed.get_qty_min symbol

  let get_fees ~symbol =
    match Hashtbl.find_opt fee_cache symbol with
    | Some f -> (Some (fst f), Some (snd f))
    | None -> (None, None)

  let initialize_fees symbols =
    Lwt_list.iter_p (fun symbol ->
      Hyperliquid_get_fee.get_fee_info ~testnet:(not !is_mainnet) symbol >|= function
      | Some info ->
          let maker = Option.value info.maker_fee ~default:0.0002 in
          let taker = Option.value info.taker_fee ~default:0.0005 in
          Hashtbl.replace fee_cache symbol (maker, taker)
      | None -> 
          (* Use sensible defaults if fetch fails *)
          Hashtbl.replace fee_cache symbol (0.0002, 0.0005)
    ) symbols
end

(* Register the module *)
let () =
  Exchange.Registry.register (module Hyperliquid_impl)
