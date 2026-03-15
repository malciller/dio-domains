(** Hyperliquid Exchange Implementation *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types
module ExTypes = Exchange.Types

let section = "hyperliquid_module"

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

let batch_modify_action_to_json modifies grouping =
  let modifies_json = `List (List.map (fun (oid_str, order) ->
    `Assoc [
      ("oid", `Intlit oid_str);
      ("order", order_wire_to_json order)
    ]
  ) modifies) in
  `Assoc [
    ("type", `String "batchModify");
    ("modifies", modifies_json);
    ("grouping", `String grouping)
  ]

let modify_action_to_json oid order =
  `Assoc [
    ("type", `String "modify");
    ("oid", `Intlit oid);
    ("order", order_wire_to_json order)
  ]

let parse_order_response _symbol nonce cl_ord_id _rounded_qty _rounded_limit_price response_json =
  let open Yojson.Safe.Util in
  try
    let data = member "data" response_json in
    let hl_response = member "response" data in
    let hl_type = member "type" hl_response |> to_string_option in
    let hl_payload = member "payload" hl_response in

    match hl_type with
    | Some "error" -> Error (to_string hl_payload)
    | Some "action" ->
        begin
          let status = member "status" hl_payload |> to_string_option in
          if status = Some "err" then
            let err_msg = 
              match member "response" hl_payload with
              | `String s -> s
              | `Assoc _ as resp -> member "data" resp |> to_string_option |> Option.value ~default:(Yojson.Safe.to_string resp)
              | _ -> "Unknown error"
            in
            Error err_msg
          else
            let __response = member "response" hl_payload in
            let response_type = member "type" __response |> to_string_option in

            match response_type with
            | Some "order"
            | Some "modify"
            | Some "cancel"
            | Some "batchModify" ->
                begin
                  let raw_data = member "data" __response in
                  let statuses = match raw_data with
                    | `Assoc pairs -> (match List.assoc_opt "statuses" pairs with Some (`List l) -> l | _ -> [])
                    | _ -> []
                  in
                  match statuses with
                  | [status] ->
                      (* Try resting first, then filled *)
                      let resting = member "resting" status in
                      let filled = member "filled" status in
                      
                      let oid_json = 
                        if resting <> `Null then member "oid" resting
                        else if filled <> `Null then member "oid" filled
                        else `Null
                      in
                      
                      let oid = match oid_json with
                        | `Int i -> string_of_int i
                        | `Intlit s -> s
                        | `String s -> s
                        | _ -> 
                            Logging.debug_f ~section "Could not find oid in order status: %s" (Yojson.Safe.to_string status);
                            Printf.sprintf "hl-%Ld" nonce
                      in
                      Ok (oid, cl_ord_id)
                  | [] ->
                      let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
                      Logging.debug_f ~section "Empty statuses list in success response, using mock ID: %s" mock_order_id;
                      Ok (mock_order_id, cl_ord_id)
                  | statuses -> 
                      Logging.debug_f ~section "Unexpected number of statuses in response: %d. JSON: %s" (List.length statuses) (Yojson.Safe.to_string response_json);
                      Error "Unexpected number of statuses in response"
                end
            | Some t -> Error (Printf.sprintf "Unknown action response type: %s" t)
            | None -> 
                Logging.debug_f ~section "No type found in action response: %s" (Yojson.Safe.to_string __response);
                if status = Some "ok" then
                   let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
                   Ok (mock_order_id, cl_ord_id)
                else
                   Error "No type found in action response"
        end
    | Some "info" ->
        (* We don't expect 'info' in place_order, but handle for completeness *)
        let mock_order_id = Printf.sprintf "hl-%Ld" nonce in
        Ok (mock_order_id, cl_ord_id)
    | Some t -> Error (Printf.sprintf "Unknown outer response type: %s" t)
    | None -> Error "No type found in outer response"
  with exn ->
    Logging.error_f ~section "Parsing error in Hyperliquid order response: %s. JSON: %s" 
      (Printexc.to_string exn) (Yojson.Safe.to_string response_json);
    Error (Printf.sprintf "Parsing error: %s" (Printexc.to_string exn))

module Hyperliquid_impl : Exchange.S = struct
  let name = "hyperliquid"
  let section = section

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
      ?order_userref
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
      | Some p -> Hyperliquid_instruments_feed.round_price_to_tick p
      | None -> 0.0 (* Hyperliquid requires a price for limit orders, for market we should handle separately if needed *)
    in

    Logging.info_f ~section "Placing %s order on HL: %s %s %f (rounded: %f) @ %f"
      hl_side symbol hl_order_type qty rounded_qty rounded_limit_price;

    let cl_ord_id = match cl_ord_id with
      | Some id -> Some id
      | None ->
          match order_userref with
          | Some ur -> Some (Printf.sprintf "0x%032x" ur)
          | None -> None
    in

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
      ~limit_price:(Some rounded_limit_price)
      ~reduce_only
      ~cl_ord_id
    in

    let msgpack_val = Hyperliquid_types.pack_order_action ~orders:[wire_order] ~grouping:"na" in
    let action_msgpack = Hyperliquid_types.serialize_action msgpack_val in
    let action_json = order_action_to_json [wire_order] "na" in

    let nonce = get_nonce () in
    let expires_after = Int64.add nonce (Int64.of_int 300_000) in
    let (r, s, v) = Hyperliquid_signer.sign_l1_action
      ~expires_after
      ~private_key_hex
      ~action_msgpack
      ~nonce
      ~is_mainnet:(!is_mainnet)
      ~vault_address
      ()
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
          ]);
          ("expiresAfter", `Intlit (Int64.to_string expires_after))
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
    Logging.debug_f ~section "Hyperliquid order raw response: %s" (Yojson.Safe.to_string response_json);

    match parse_order_response symbol nonce cl_ord_id rounded_qty (Some rounded_limit_price) response_json with
    | Ok (oid, cl_ord_id) ->
        let hl_side = match side with Types.Buy -> Hyperliquid_executions_feed.Buy | Types.Sell -> Hyperliquid_executions_feed.Sell in
        Hyperliquid_executions_feed.inject_order
          ~symbol
          ~order_id:oid
          ~side:hl_side
          ~qty:rounded_qty
          ~price:rounded_limit_price
          ?user_ref:order_userref
          ?cl_ord_id
          ();

        Lwt.return (Ok {
          Dio_exchange.Exchange_intf.Types.order_id = oid;
          cl_ord_id = cl_ord_id;
          order_userref = order_userref;
        })
    | Error err -> Lwt.return (Error err)

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


(** Amend an existing order – single "modify" action *)
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

  if symbol = None then
    Lwt.return (Error "Symbol required for Hyperliquid amendment")
  else
    let symbol = Option.get symbol in
    
    match get_open_order ~symbol ~order_id with
    | None -> 
        Logging.error_f ~section "Cannot amend: order %s not found for %s" order_id symbol;
        Lwt.return (Error (Printf.sprintf "Order %s not found" order_id))

    | Some current ->

        let rounded_qty =
          match qty with
          | Some q -> Hyperliquid_instruments_feed.round_qty_to_lot symbol q
          | None -> current.qty
        in

        let rounded_limit_price =
          match limit_price with
          | Some p -> Hyperliquid_instruments_feed.round_price_to_tick p
          | None -> Option.value ~default:0.0 current.limit_price
        in

        let private_key_hex =
          match Sys.getenv_opt "HYPERLIQUID_PRIVATE_KEY" with
          | Some k -> String.trim k
          | None -> failwith "HYPERLIQUID_PRIVATE_KEY not set"
        in

        let vault_address = None in

        let asset_index =
          match Hyperliquid_instruments_feed.get_asset_index symbol with
          | Some i -> i
          | None ->
              Logging.warn_f ~section "No asset index for %s" symbol;
              0
        in

        let wire_order =
          Hyperliquid_actions.to_hl_order_wire
            ~qty:rounded_qty
            ~symbol
            ~asset_index
            ~ot:(Hyperliquid_actions.hl_order_type ExTypes.Limit None)
            ~side:current.side
            ~limit_price:(Some rounded_limit_price)
            ~reduce_only:None
            ~cl_ord_id:(match cl_ord_id with
              | Some id -> Some id
              | None -> 
                  match current.user_ref with
                  | Some ur -> Some (Printf.sprintf "0x%032x" ur)
                  | None -> current.cl_ord_id)
        in

        let oid_int64 =
          try Int64.of_string order_id
          with _ ->
            failwith (Printf.sprintf "order_id must be numeric string, got: %s" order_id)
        in

        (* Canonical MsgPack action — same as SDK *)
        let msgpack_val =
          Hyperliquid_types.pack_modify_action
            ~oid:oid_int64
            ~order:wire_order
        in

        let action_msgpack = Hyperliquid_types.serialize_action msgpack_val in  

        let action_json = modify_action_to_json (Int64.to_string oid_int64) wire_order in

        let nonce = get_nonce () in
        let expires_after = Int64.add nonce (Int64.of_int 300_000) in
        let (r, s, v) =
          Hyperliquid_signer.sign_l1_action
            ~expires_after
            ~private_key_hex
            ~action_msgpack
            ~nonce
            ~is_mainnet:(!is_mainnet)
            ~vault_address
            ()
        in

        let req_id = Random.int 1_000_000 in

        let payload : Yojson.Safe.t =
          `Assoc [
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
                ]);
                ("expiresAfter", `Intlit (Int64.to_string expires_after))
              ])
            ])
          ]
        in

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

        Logging.info_f ~section "Amending %s on %s: qty=%.4f @ %.4f"
          order_id symbol rounded_qty rounded_limit_price;

        Logging.debug_f ~section "Signed modify action: %s"
          (Yojson.Safe.to_string action_json);

        Hyperliquid_ws.send_request
          ~json:payload
          ~req_id
          ~timeout_ms:15000
        >>= fun response_json ->

        Logging.debug_f ~section "Raw modify response: %s"
          (Yojson.Safe.to_string response_json);

        match parse_order_response symbol nonce cl_ord_id rounded_qty (Some rounded_limit_price) response_json with
        | Ok (new_oid, new_cl_ord_id) ->
            Logging.info_f ~section "Modify OK → new oid %s" new_oid;

            (* Proactively inject the amended order state *)
            let hl_side = match current.side with Types.Buy -> Hyperliquid_executions_feed.Buy | Types.Sell -> Hyperliquid_executions_feed.Sell in
            Hyperliquid_executions_feed.inject_order
              ~symbol
              ~order_id:new_oid
              ~side:hl_side
              ~qty:rounded_qty
              ~price:rounded_limit_price
              ?user_ref:current.user_ref
              ?cl_ord_id:new_cl_ord_id
              ();

            Lwt.return (Ok {
              ExTypes.amend_id = new_oid;
              order_id = new_oid;
              cl_ord_id = new_cl_ord_id
            })

        | Error e ->
            Logging.error_f ~section "Modify failed: %s" e;
            Lwt.return (Error e)


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
      let expires_after = Int64.add nonce (Int64.of_int 300_000) in
      let (r, s, v) = Hyperliquid_signer.sign_l1_action
        ~expires_after
        ~private_key_hex
        ~action_msgpack
        ~nonce
        ~is_mainnet:(!is_mainnet)
        ~vault_address
        ()
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
            ]);
            ("expiresAfter", `Intlit (Int64.to_string expires_after))
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
        let hl_response = member "response" data in
        let hl_type = member "type" hl_response |> to_string_option in
        let hl_payload = member "payload" hl_response in

        match hl_type with
        | Some "error" -> Lwt.return (Error (to_string hl_payload))
        | Some "action" ->
            begin
              let status = member "status" hl_payload |> to_string_option in
              if status = Some "ok" then
                let inner_response = member "response" hl_payload in
                let response_type = member "type" inner_response |> to_string in
                
                if response_type = "cancel" then
                  (* Map back to our inputs to simulate successful cancel IDs *)
                  let response_items = match order_ids with
                    | Some ids -> 
                        List.map (fun id -> { Dio_exchange.Exchange_intf.Types.order_id = id; cl_ord_id = None }) ids
                    | None -> []
                  in
                  Lwt.return (Ok response_items)
                else
                  Lwt.return (Error ("Unknown inner response type: " ^ response_type))
              else
                let error_msg = member "response" hl_payload |> to_string in
                Lwt.return (Error error_msg)
            end
        | Some t -> Lwt.return (Error ("Unknown outer response type: " ^ t))
        | None -> Lwt.return (Error "No type found in outer response")
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

  let round_price_to_tick ~symbol:_ price =
    Hyperliquid_instruments_feed.round_price_to_tick price

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
