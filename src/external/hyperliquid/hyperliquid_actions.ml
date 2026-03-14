(** Hyperliquid L1 Actions API via WebSocket *)

open Lwt.Infix
module Types = Dio_exchange.Exchange_intf.Types

let section = "hyperliquid_actions"

let current_time_ms () =
  let now = Mtime_clock.now () in
  let ns = Mtime.to_uint64_ns now in
  Int64.to_float ns /. 1_000_000.0 |> int_of_float

(** Generate signed transaction payload *)
let signed_payload ~action_yojson ~action_msgpack ~wallet ~is_mainnet ~vault_address =
  let nonce = current_time_ms () |> Int64.of_int in
  let (r, s, v) = Hyperliquid_signer.sign_l1_action 
    ~private_key_hex:wallet 
    ~action_msgpack 
    ~nonce 
    ~is_mainnet 
    ~vault_address 
  in
  let signature_yojson = `Assoc [
    ("r", `String ("0x" ^ r));
    ("s", `String ("0x" ^ s));
    ("v", `Int v)
  ] in
  `Assoc [
    ("action", action_yojson);
    ("nonce", `Int (Int64.to_int nonce));
    ("signature", signature_yojson);
    ("vaultAddress", match vault_address with Some addr -> `String addr | None -> `Null)
  ]

(** Map our internal Types.order_type to Hyperliquid types *)
let hl_order_type (ot : Types.order_type) (tif : Types.time_in_force option) : Hyperliquid_types.order_type_wire =
  let tif_enum = match tif with
    | Some Types.IOC -> Hyperliquid_types.Ioc
    | Some Types.FOK -> Hyperliquid_types.Ioc (* HL Doesn't technically have FOK distinct from IOC in basic limit *)
    | Some Types.GTC -> Hyperliquid_types.Gtc
    | _ -> Hyperliquid_types.Gtc
  in
  match ot with
  | Types.Limit | Types.Market ->
      Limit { tif = tif_enum }
  | Types.StopLoss ->
      Trigger { triggerPx = "0.0"; (* needs real trigger *) isMarket = true; tpsl = Sl }
  | Types.TakeProfit ->
      Trigger { triggerPx = "0.0"; isMarket = true; tpsl = Tp }
  | _ -> Limit { tif = tif_enum }

let to_hl_order_wire ~qty ~symbol:_ ~ot ~side ~limit_price ~reduce_only ~cl_ord_id =
  (* Need a symbol to asset index mapping or fetched from instruments feed *)
  (* For simplicity, assuming asset=0 for now until integrated with feed *)
  let asset = 0 in
  let is_buy = (side = Types.Buy) in
  let px = match limit_price with Some p -> Printf.sprintf "%.8f" p | None -> "0.0" in
  let sz = Printf.sprintf "%.8f" qty in
  let r = match reduce_only with Some x -> x | None -> false in
  {
    Hyperliquid_types.a = asset;
    b = is_buy;
    p = px;
    s = sz;
    r = r;
    t = ot;
    c = cl_ord_id;
  }

let place_order
    ~token
    ~order_type:_
    ~side
    ~order_qty
    ~symbol
    ?limit_price
    ?time_in_force
    ?post_only:_
    ?reduce_only
    ?order_userref:_
    ?cl_ord_id
    ?trigger_price:_
    ?display_qty:_
    ?retry_config:_
    () =
  let hl_ot = hl_order_type (Dio_exchange.Exchange_intf.Types.Limit) time_in_force in (* Quick mock *)
  let wire = to_hl_order_wire ~qty:order_qty ~symbol ~ot:hl_ot ~side ~limit_price ~reduce_only ~cl_ord_id in
  
  let msgpck_val = Hyperliquid_types.pack_order_action ~orders:[wire] ~grouping:"na" in
  let action_msgpack = Hyperliquid_types.serialize_action msgpck_val in
  
  (* We should actually build the action JSON directly too to pair with the msgpack payload *)
  let action_yojson = `Assoc [
    ("type", `String "order");
    ("grouping", `String "na");
    ("orders", `List [
      `Assoc [
        ("a", `Int wire.a);
        ("b", `Bool wire.b);
        ("p", `String wire.p);
        ("s", `String wire.s);
        ("r", `Bool wire.r);
        ("t", `Assoc [("limit", `Assoc [("tif", `String "Gtc")])]) (* Map properly later *)
      ]
    ])
  ] in
  
  let payload = signed_payload ~action_yojson ~action_msgpack ~wallet:token ~is_mainnet:true ~vault_address:None in
  
  Hyperliquid_ws.send_post_request payload ~timeout_ms:5000 >|= function
  | `Null -> Error "Timeout waiting for WS response"
  | json -> 
      Logging.info_f ~section "Order placed raw response: %s" (Yojson.Safe.to_string json);
      let open Yojson.Safe.Util in
      try
        let action_data = member "data" json |> member "data" in
        let statuses = member "statuses" action_data |> to_list in
        match statuses with
        | st :: _ ->
            let get_oid node = member "oid" node |> to_int |> string_of_int in
            (match member "resting" st |> to_option (fun x -> x) with
             | Some r -> Ok { Types.order_id = get_oid r; cl_ord_id = None; order_userref = None }
             | None ->
                 match member "filled" st |> to_option (fun x -> x) with
                 | Some f -> Ok { Types.order_id = get_oid f; cl_ord_id = None; order_userref = None }
                 | None ->
                     match member "error" st |> to_option to_string with
                     | Some err -> Error ("Order error: " ^ err)
                     | None -> Error "Order status not resting or filled")
        | [] -> Error "Empty statuses array"
      with exn ->
        Error ("Failed to parse order response: " ^ Printexc.to_string exn)

let amend_order ~token:_ ~order_id:_ ?cl_ord_id:_ ?order_qty:_ ?limit_price:_ ?post_only:_ ?trigger_price:_ ?display_qty:_ ?symbol:_ ?retry_config:_ () =
  Lwt.return (Error "amend_order not fully implemented")

let cancel_orders ~token:_ ?order_ids:_ ?cl_ord_ids:_ ?order_userrefs:_ ?retry_config:_ () =
  Lwt.return (Error "cancel_orders not fully implemented")
