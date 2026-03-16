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

let () =
  Alcotest.run "Hyperliquid Types" [
    "conversion", [
      Alcotest.test_case "tif_to_string" `Quick test_tif_to_string;
      Alcotest.test_case "tpsl_to_string" `Quick test_tpsl_to_string;
    ];
    "pack", [
      Alcotest.test_case "pack_id" `Quick test_pack_id;
      Alcotest.test_case "pack_order_wire" `Quick test_pack_order_wire;
    ]
  ]
