let test_order_wire_packing () =
  let order = {
    Hyperliquid.Hyperliquid_types.a = 0;
    b = true;
    p = "50000.0";
    s = "1.5";
    r = false;
    t = Limit { tif = Gtc };
    c = Some "c_1234";
  } in

  let packed = Hyperliquid.Hyperliquid_types.pack_order_wire order in
  let as_str = Hyperliquid.Hyperliquid_types.serialize_action packed in
  
  (* We just do simple verification that it's a map and has length 7 (a, b, p, s, r, t, c) *)
  (match packed with
   | Msgpck.Map l -> 
       Alcotest.(check int) "has 7 fields" 7 (List.length l)
   | _ -> Alcotest.fail "Expected Map"
  );
  Alcotest.(check bool) "serialization successful" true (String.length as_str > 0)

let test_cancel_wire_packing () =
  let cancel = {
    Hyperliquid.Hyperliquid_types.a = 0;
    o = 123456789L;
  } in

  let packed = Hyperliquid.Hyperliquid_types.pack_cancel_wire cancel in
  (match packed with
   | Msgpck.Map l -> 
       Alcotest.(check int) "has 2 fields" 2 (List.length l)
   | _ -> Alcotest.fail "Expected Map"
  )

let () =
  Alcotest.run "Hyperliquid Types" [
    "packing", [
      Alcotest.test_case "pack_order_wire" `Quick test_order_wire_packing;
      Alcotest.test_case "pack_cancel_wire" `Quick test_cancel_wire_packing;
    ];
  ]
