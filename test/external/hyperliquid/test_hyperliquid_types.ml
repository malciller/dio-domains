let test_tif_to_string () =
  Alcotest.(check string) "Alo" "Alo" (Hyperliquid.Types.tif_to_string Hyperliquid.Types.Alo);
  Alcotest.(check string) "Ioc" "Ioc" (Hyperliquid.Types.tif_to_string Hyperliquid.Types.Ioc);
  Alcotest.(check string) "Gtc" "Gtc" (Hyperliquid.Types.tif_to_string Hyperliquid.Types.Gtc)

let test_tpsl_to_string () =
  Alcotest.(check string) "Tp" "tp" (Hyperliquid.Types.tpsl_to_string Hyperliquid.Types.Tp);
  Alcotest.(check string) "Sl" "sl" (Hyperliquid.Types.tpsl_to_string Hyperliquid.Types.Sl)

let test_pack_id () =
  (* Test packing an ID that fits in int *)
  let id1 = 12345L in
  let packed1 = Hyperliquid.Types.pack_id id1 in
  (match packed1 with
  | Msgpck.Int i -> Alcotest.(check int) "fits in int" 12345 i
  | _ -> Alcotest.fail "Expected Int");

  (* Test packing a large ID (might not fit in 63-bit int on 32-bit systems, but should fit in int64) *)
  let id2 = 9223372036854775807L (* Int64.max_int *) in
  let packed2 = Hyperliquid.Types.pack_id id2 in
  (match packed2 with
  | Msgpck.Int i -> 
      (try 
        let _ = Int64.of_int i in
        (* If it fits, it's ok, maybe 64-bit system *)
        ()
       with _ -> Alcotest.fail "Int conversion overflowed")
  | Msgpck.Int64 i -> Alcotest.(check int64) "fits in int64" id2 i
  | _ -> Alcotest.fail "Expected Int or Int64")

let test_pack_order_wire () =
  let order = {
    Hyperliquid.Types.a = 1;
    Hyperliquid.Types.b = true;
    Hyperliquid.Types.p = "123.45";
    Hyperliquid.Types.s = "1.5";
    Hyperliquid.Types.r = false;
    Hyperliquid.Types.t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo };
    Hyperliquid.Types.c = Some "client123";
  } in
  let packed = Hyperliquid.Types.pack_order_wire order in
  match packed with
  | Msgpck.Map kv ->
      let has_a = List.exists (fun (k, v) -> k = Msgpck.String "a" && v = Msgpck.Int 1) kv in
      Alcotest.(check bool) "has a" true has_a;
      let has_b = List.exists (fun (k, v) -> k = Msgpck.String "b" && v = Msgpck.Bool true) kv in
      Alcotest.(check bool) "has b" true has_b;
      let has_c = List.exists (fun (k, v) -> k = Msgpck.String "c" && v = Msgpck.String "client123") kv in
      Alcotest.(check bool) "has c" true has_c
  | _ -> Alcotest.fail "Expected Map"

let test_pack_order_type_wire_limit () =
  let ot = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo } in
  let packed = Hyperliquid.Types.pack_order_type_wire ot in
  match packed with
  | Msgpck.Map _ -> () (* Valid map structure *)
  | _ -> Alcotest.fail "Expected Map for Limit order type"

let test_pack_order_type_wire_trigger () =
  let ot = Hyperliquid.Types.Trigger { triggerPx = "100.0"; isMarket = true; tpsl = Hyperliquid.Types.Sl } in
  let packed = Hyperliquid.Types.pack_order_type_wire ot in
  match packed with
  | Msgpck.Map kvs ->
      let has_trigger = List.exists (fun (k, _) -> k = Msgpck.String "trigger") kvs in
      Alcotest.(check bool) "has trigger key" true has_trigger
  | _ -> Alcotest.fail "Expected Map for Trigger order type"

let test_pack_order_wire_no_cloid () =
  let order = {
    Hyperliquid.Types.a = 2;
    Hyperliquid.Types.b = false;
    Hyperliquid.Types.p = "50.0";
    Hyperliquid.Types.s = "0.5";
    Hyperliquid.Types.r = true;
    Hyperliquid.Types.t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Gtc };
    Hyperliquid.Types.c = None;
  } in
  let packed = Hyperliquid.Types.pack_order_wire order in
  match packed with
  | Msgpck.Map kv ->
      let has_c = List.exists (fun (k, _) -> k = Msgpck.String "c") kv in
      Alcotest.(check bool) "no c field" false has_c;
      let has_r = List.exists (fun (k, v) -> k = Msgpck.String "r" && v = Msgpck.Bool true) kv in
      Alcotest.(check bool) "r is true" true has_r
  | _ -> Alcotest.fail "Expected Map"

let test_pack_modify_action () =
  let order = {
    Hyperliquid.Types.a = 1; b = true; p = "100.0"; s = "1.0";
    r = false; t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo }; c = None;
  } in
  let packed = Hyperliquid.Types.pack_modify_action ~oid:42L ~order in
  match packed with
  | Msgpck.Map kv ->
      let has_type = List.exists (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "modify") kv in
      Alcotest.(check bool) "type=modify" true has_type;
      let has_oid = List.exists (fun (k, _) -> k = Msgpck.String "oid") kv in
      Alcotest.(check bool) "has oid" true has_oid;
      let has_order = List.exists (fun (k, _) -> k = Msgpck.String "order") kv in
      Alcotest.(check bool) "has order" true has_order
  | _ -> Alcotest.fail "Expected Map"

let test_pack_cancel_action () =
  let cancels = [ { Hyperliquid.Types.a = 0; o = 999L }; { Hyperliquid.Types.a = 1; o = 1000L } ] in
  let packed = Hyperliquid.Types.pack_cancel_action ~cancels in
  match packed with
  | Msgpck.Map kv ->
      let has_type = List.exists (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "cancel") kv in
      Alcotest.(check bool) "type=cancel" true has_type;
      let cancels_list = List.assoc (Msgpck.String "cancels") kv in
      (match cancels_list with
       | Msgpck.List l -> Alcotest.(check int) "2 cancels" 2 (List.length l)
       | _ -> Alcotest.fail "Expected List for cancels")
  | _ -> Alcotest.fail "Expected Map"

let test_pack_batch_modify_action () =
  let order = {
    Hyperliquid.Types.a = 0; b = true; p = "50.0"; s = "1.0";
    r = false; t = Hyperliquid.Types.Limit { tif = Hyperliquid.Types.Alo }; c = None;
  } in
  let packed = Hyperliquid.Types.pack_batch_modify_action ~modifies:[(10L, order); (20L, order)] ~grouping:"na" in
  match packed with
  | Msgpck.Map kv ->
      let has_type = List.exists (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "batchModify") kv in
      Alcotest.(check bool) "type=batchModify" true has_type;
      let modifies_list = List.assoc (Msgpck.String "modifies") kv in
      (match modifies_list with
       | Msgpck.List l -> Alcotest.(check int) "2 modifies" 2 (List.length l)
       | _ -> Alcotest.fail "Expected List for modifies")
  | _ -> Alcotest.fail "Expected Map"

let test_pack_cancel_by_cloid_action () =
  let cancels = [ { Hyperliquid.Types.a = 0; cloid = "0x001" } ] in
  let packed = Hyperliquid.Types.pack_cancel_by_cloid_action ~cancels in
  match packed with
  | Msgpck.Map kv ->
      let has_type = List.exists (fun (k, v) -> k = Msgpck.String "type" && v = Msgpck.String "cancelByCloid") kv in
      Alcotest.(check bool) "type=cancelByCloid" true has_type
  | _ -> Alcotest.fail "Expected Map"

let test_serialize_action () =
  let action = Hyperliquid.Types.pack_order_action
    ~orders:[]
    ~grouping:"na"
  in
  let bytes = Hyperliquid.Types.serialize_action action in
  Alcotest.(check bool) "non-empty" true (String.length bytes > 0)

let () =
  Alcotest.run "Hyperliquid Types" [
    "conversion", [
      Alcotest.test_case "tif_to_string" `Quick test_tif_to_string;
      Alcotest.test_case "tpsl_to_string" `Quick test_tpsl_to_string;
    ];
    "pack", [
      Alcotest.test_case "pack_id" `Quick test_pack_id;
      Alcotest.test_case "pack_order_wire" `Quick test_pack_order_wire;
      Alcotest.test_case "pack_order_wire no cloid" `Quick test_pack_order_wire_no_cloid;
      Alcotest.test_case "pack_order_type_wire limit" `Quick test_pack_order_type_wire_limit;
      Alcotest.test_case "pack_order_type_wire trigger" `Quick test_pack_order_type_wire_trigger;
    ];
    "actions", [
      Alcotest.test_case "pack_modify_action" `Quick test_pack_modify_action;
      Alcotest.test_case "pack_cancel_action" `Quick test_pack_cancel_action;
      Alcotest.test_case "pack_batch_modify_action" `Quick test_pack_batch_modify_action;
      Alcotest.test_case "pack_cancel_by_cloid_action" `Quick test_pack_cancel_by_cloid_action;
      Alcotest.test_case "serialize_action" `Quick test_serialize_action;
    ]
  ]
