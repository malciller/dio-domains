let test_tif_to_string () =
  Alcotest.(check string)
    "Alo"
    "Alo"
    (Hyperliquid.Types.tif_to_string Hyperliquid.Types.Alo);
  Alcotest.(check string)
    "Ioc"
    "Ioc"
    (Hyperliquid.Types.tif_to_string Hyperliquid.Types.Ioc);
  Alcotest.(check string)
    "Gtc"
    "Gtc"
    (Hyperliquid.Types.tif_to_string Hyperliquid.Types.Gtc)
;;

let test_tpsl_to_string () =
  Alcotest.(check string)
    "Tp"
    "tp"
    (Hyperliquid.Types.tpsl_to_string Hyperliquid.Types.Tp);
  Alcotest.(check string)
    "Sl"
    "sl"
    (Hyperliquid.Types.tpsl_to_string Hyperliquid.Types.Sl)
;;

let test_pack_id () =
  (* ID that fits in int. *)
  let id1 = 12345L in
  let packed1 = Hyperliquid.Types.pack_id id1 in
  (match packed1 with
   | Msgpck.Int i -> Alcotest.(check int) "fits in int" 12345 i
   | _ -> Alcotest.fail "Expected Int");
  (* Large ID: may not fit a 63-bit int on 32-bit systems, but fits int64. *)
  let id2 = 9223372036854775807L (* Int64.max_int *) in
  let packed2 = Hyperliquid.Types.pack_id id2 in
  match packed2 with
  | Msgpck.Int i ->
    (try
       let _ = Int64.of_int i in
       ()
     with
     | _ -> Alcotest.fail "Int conversion overflowed")
  | Msgpck.Int64 i -> Alcotest.(check int64) "fits in int64" id2 i
  | _ -> Alcotest.fail "Expected Int or Int64"
;;

let test_pack_order_wire () =
  let order =
    { Hyperliquid.Types.a = 1
    ; Hyperliquid.Types.b = true
    ; Hyperliquid.Types.p = "123.45"
    ; Hyperliquid.Types.s = "1.5"
    ; Hyperliquid.Types.r = false
    ; Hyperliquid.Types.t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo }
    ; Hyperliquid.Types.c = Some "client123"
    }
  in
  let packed = Hyperliquid.Types.pack_order_wire order in
  match packed with
  | Msgpck.Map kv ->
    let has_a =
      List.exists (fun (k, v) -> k = Msgpck.String "a" && v = Msgpck.Int 1) kv
    in
    Alcotest.(check bool) "has a" true has_a;
    let has_b =
      List.exists (fun (k, v) -> k = Msgpck.String "b" && v = Msgpck.Bool true) kv
    in
    Alcotest.(check bool) "has b" true has_b;
    let has_c =
      List.exists
        (fun (k, v) -> k = Msgpck.String "c" && v = Msgpck.String "client123")
        kv
    in
    Alcotest.(check bool) "has c" true has_c
  | _ -> Alcotest.fail "Expected Map"
;;

let test_pack_order_type_wire_limit () =
  let ot = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo } in
  let packed = Hyperliquid.Types.pack_order_type_wire ot in
  match packed with
  | Msgpck.Map _ -> ()
  | _ -> Alcotest.fail "Expected Map for Limit order type"
;;

let test_pack_order_type_wire_trigger () =
  let ot =
    Hyperliquid.Types.Trigger
      { triggerPx = "100.0"; isMarket = true; tpsl = Hyperliquid.Types.Sl }
  in
  let packed = Hyperliquid.Types.pack_order_type_wire ot in
  match packed with
  | Msgpck.Map kvs ->
    let has_trigger = List.exists (fun (k, _) -> k = Msgpck.String "trigger") kvs in
    Alcotest.(check bool) "has trigger key" true has_trigger
  | _ -> Alcotest.fail "Expected Map for Trigger order type"
;;

let test_pack_order_wire_no_cloid () =
  let order =
    { Hyperliquid.Types.a = 2
    ; Hyperliquid.Types.b = false
    ; Hyperliquid.Types.p = "50.0"
    ; Hyperliquid.Types.s = "0.5"
    ; Hyperliquid.Types.r = true
    ; Hyperliquid.Types.t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Gtc }
    ; Hyperliquid.Types.c = None
    }
  in
  let packed = Hyperliquid.Types.pack_order_wire order in
  match packed with
  | Msgpck.Map kv ->
    let has_c = List.exists (fun (k, _) -> k = Msgpck.String "c") kv in
    Alcotest.(check bool) "no c field" false has_c;
    let has_r =
      List.exists (fun (k, v) -> k = Msgpck.String "r" && v = Msgpck.Bool true) kv
    in
    Alcotest.(check bool) "r is true" true has_r
  | _ -> Alcotest.fail "Expected Map"
;;

let test_pack_modify_action () =
  let order =
    { Hyperliquid.Types.a = 1
    ; b = true
    ; p = "100.0"
    ; s = "1.0"
    ; r = false
    ; t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo }
    ; c = None
    }
  in
  let packed = Hyperliquid.Types.pack_modify_action ~oid:42L ~order in
  match packed with
  | Msgpck.Map kv ->
    let has_type =
      List.exists
        (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "modify")
        kv
    in
    Alcotest.(check bool) "type=modify" true has_type;
    let has_oid = List.exists (fun (k, _) -> k = Msgpck.String "oid") kv in
    Alcotest.(check bool) "has oid" true has_oid;
    let has_order = List.exists (fun (k, _) -> k = Msgpck.String "order") kv in
    Alcotest.(check bool) "has order" true has_order
  | _ -> Alcotest.fail "Expected Map"
;;

let test_pack_cancel_action () =
  let cancels =
    [ { Hyperliquid.Types.a = 0; o = 999L }; { Hyperliquid.Types.a = 1; o = 1000L } ]
  in
  let packed = Hyperliquid.Types.pack_cancel_action ~cancels in
  match packed with
  | Msgpck.Map kv ->
    let has_type =
      List.exists
        (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "cancel")
        kv
    in
    Alcotest.(check bool) "type=cancel" true has_type;
    let cancels_list = List.assoc (Msgpck.String "cancels") kv in
    (match cancels_list with
     | Msgpck.List l -> Alcotest.(check int) "2 cancels" 2 (List.length l)
     | _ -> Alcotest.fail "Expected List for cancels")
  | _ -> Alcotest.fail "Expected Map"
;;

let test_pack_batch_modify_action () =
  let order =
    { Hyperliquid.Types.a = 0
    ; b = true
    ; p = "50.0"
    ; s = "1.0"
    ; r = false
    ; t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo }
    ; c = None
    }
  in
  let packed =
    Hyperliquid.Types.pack_batch_modify_action
      ~modifies:[ 10L, order; 20L, order ]
      ~grouping:"na"
  in
  match packed with
  | Msgpck.Map kv ->
    let has_type =
      List.exists
        (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "batchModify")
        kv
    in
    Alcotest.(check bool) "type=batchModify" true has_type;
    let modifies_list = List.assoc (Msgpck.String "modifies") kv in
    (match modifies_list with
     | Msgpck.List l -> Alcotest.(check int) "2 modifies" 2 (List.length l)
     | _ -> Alcotest.fail "Expected List for modifies")
  | _ -> Alcotest.fail "Expected Map"
;;

let test_pack_cancel_by_cloid_action () =
  let cancels = [ { Hyperliquid.Types.a = 0; cloid = "0x001" } ] in
  let packed = Hyperliquid.Types.pack_cancel_by_cloid_action ~cancels in
  match packed with
  | Msgpck.Map kv ->
    let has_type =
      List.exists
        (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "cancelByCloid")
        kv
    in
    Alcotest.(check bool) "type=cancelByCloid" true has_type
  | _ -> Alcotest.fail "Expected Map"
;;

let test_serialize_action () =
  let action = Hyperliquid.Types.pack_order_action ~orders:[] ~grouping:"na" in
  let bytes = Hyperliquid.Types.serialize_action action in
  Alcotest.(check bool) "non-empty" true (String.length bytes > 0)
;;

let json_of f =
  let buf = Buffer.create 256 in
  f buf;
  Buffer.contents buf
;;

let limit_order ?cloid ~p ~s () : Hyperliquid.Types.order_wire =
  { Hyperliquid.Types.a = 7
  ; b = true
  ; p
  ; s
  ; r = false
  ; t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Gtc }
  ; c = cloid
  }
;;

let test_json_order_action () =
  let order = limit_order ~p:"100.25" ~s:"2" () in
  let json =
    json_of (fun buf ->
      Hyperliquid.Types.add_order_action_json buf ~orders:[ order ] ~grouping:"na")
  in
  Alcotest.(check string)
    "exact wire"
    {|{"type":"order","orders":[{"a":7,"b":true,"p":"100.25","s":"2","r":false,"t":{"limit":{"tif":"Gtc"}}}],"grouping":"na"}|}
    json
;;

let test_json_order_action_cloid_escapes () =
  let cloid = "cid\"x\\y\tz" in
  let order = limit_order ~cloid ~p:"1" ~s:"2" () in
  let json =
    json_of (fun buf ->
      Hyperliquid.Types.add_order_action_json buf ~orders:[ order ] ~grouping:"na")
  in
  let open Yojson.Safe.Util in
  let parsed = Yojson.Safe.from_string json in
  let c = member "orders" parsed |> to_list |> List.hd |> member "c" |> to_string in
  Alcotest.(check string) "cloid round-trips through JSON" cloid c
;;

let test_json_modify_action () =
  let order = limit_order ~p:"99.5" ~s:"3" () in
  let json =
    json_of (fun buf -> Hyperliquid.Types.add_modify_action_json buf ~oid:12345L ~order)
  in
  Alcotest.(check string)
    "exact wire"
    {|{"type":"modify","oid":12345,"order":{"a":7,"b":true,"p":"99.5","s":"3","r":false,"t":{"limit":{"tif":"Gtc"}}}}|}
    json
;;

let test_json_cancel_action () =
  let json =
    json_of (fun buf ->
      Hyperliquid.Types.add_cancel_action_json
        buf
        ~asset_index:7
        ~order_ids:[ 12345L; 67890L ])
  in
  Alcotest.(check string)
    "exact wire"
    {|{"type":"cancel","cancels":[{"a":7,"o":12345},{"a":7,"o":67890}]}|}
    json
;;

let test_json_trigger_order () =
  let order : Hyperliquid.Types.order_wire =
    { a = 3
    ; b = false
    ; p = "0"
    ; s = "1"
    ; r = true
    ; t =
        Hyperliquid.Types.Trigger
          { triggerPx = "100"; isMarket = true; tpsl = Hyperliquid.Types.Sl }
    ; c = None
    }
  in
  let json =
    json_of (fun buf ->
      Hyperliquid.Types.add_order_action_json buf ~orders:[ order ] ~grouping:"na")
  in
  Alcotest.(check string)
    "exact wire"
    {|{"type":"order","orders":[{"a":3,"b":false,"p":"0","s":"1","r":true,"t":{"trigger":{"isMarket":true,"triggerPx":"100","tpsl":"sl"}}}],"grouping":"na"}|}
    json
;;

let () =
  Alcotest.run
    "Hyperliquid Types"
    [ ( "conversion"
      , [ Alcotest.test_case "tif_to_string" `Quick test_tif_to_string
        ; Alcotest.test_case "tpsl_to_string" `Quick test_tpsl_to_string
        ] )
    ; ( "pack"
      , [ Alcotest.test_case "pack_id" `Quick test_pack_id
        ; Alcotest.test_case "pack_order_wire" `Quick test_pack_order_wire
        ; Alcotest.test_case
            "pack_order_wire no cloid"
            `Quick
            test_pack_order_wire_no_cloid
        ; Alcotest.test_case
            "pack_order_type_wire limit"
            `Quick
            test_pack_order_type_wire_limit
        ; Alcotest.test_case
            "pack_order_type_wire trigger"
            `Quick
            test_pack_order_type_wire_trigger
        ] )
    ; ( "actions"
      , [ Alcotest.test_case "pack_modify_action" `Quick test_pack_modify_action
        ; Alcotest.test_case "pack_cancel_action" `Quick test_pack_cancel_action
        ; Alcotest.test_case
            "pack_batch_modify_action"
            `Quick
            test_pack_batch_modify_action
        ; Alcotest.test_case
            "pack_cancel_by_cloid_action"
            `Quick
            test_pack_cancel_by_cloid_action
        ; Alcotest.test_case "serialize_action" `Quick test_serialize_action
        ] )
    ; ( "json"
      , [ Alcotest.test_case "order action" `Quick test_json_order_action
        ; Alcotest.test_case
            "order action cloid escaping"
            `Quick
            test_json_order_action_cloid_escapes
        ; Alcotest.test_case "modify action" `Quick test_json_modify_action
        ; Alcotest.test_case "cancel action" `Quick test_json_cancel_action
        ; Alcotest.test_case "trigger order" `Quick test_json_trigger_order
        ] )
    ]
;;
