module Types = Dio_exchange.Exchange_intf.Types

(* --- status_of_lighter_int --- *)

let test_status_of_lighter_int_in_progress () =
  Alcotest.(check bool) "0 -> Pending" true
    (Lighter.Types.status_of_lighter_int 0 = Types.Pending)

let test_status_of_lighter_int_pending () =
  Alcotest.(check bool) "1 -> Pending" true
    (Lighter.Types.status_of_lighter_int 1 = Types.Pending)

let test_status_of_lighter_int_active () =
  Alcotest.(check bool) "2 -> New" true
    (Lighter.Types.status_of_lighter_int 2 = Types.New)

let test_status_of_lighter_int_filled () =
  Alcotest.(check bool) "3 -> Filled" true
    (Lighter.Types.status_of_lighter_int 3 = Types.Filled)

let test_status_of_lighter_int_canceled_by_owner () =
  Alcotest.(check bool) "4 -> Canceled" true
    (Lighter.Types.status_of_lighter_int 4 = Types.Canceled)

let test_status_of_lighter_int_canceled_by_engine () =
  Alcotest.(check bool) "5 -> Canceled" true
    (Lighter.Types.status_of_lighter_int 5 = Types.Canceled)

let test_status_of_lighter_int_canceled_by_protocol () =
  Alcotest.(check bool) "6 -> Canceled" true
    (Lighter.Types.status_of_lighter_int 6 = Types.Canceled)

let test_status_of_lighter_int_expired () =
  Alcotest.(check bool) "7 -> Expired" true
    (Lighter.Types.status_of_lighter_int 7 = Types.Expired)

let test_status_of_lighter_int_unknown () =
  match Lighter.Types.status_of_lighter_int 99 with
  | Types.Unknown s ->
      Alcotest.(check string) "static variant" "lighter_unknown_status" s
  | _ -> Alcotest.fail "Expected Unknown"

(* --- status_of_lighter_string --- *)

let test_status_of_lighter_string_active () =
  Alcotest.(check bool) "Active -> New" true
    (Lighter.Types.status_of_lighter_string "Active" = Types.New)

let test_status_of_lighter_string_filled () =
  Alcotest.(check bool) "Filled -> Filled" true
    (Lighter.Types.status_of_lighter_string "Filled" = Types.Filled)

let test_status_of_lighter_string_canceled_variants () =
  Alcotest.(check bool) "CanceledByOwner" true
    (Lighter.Types.status_of_lighter_string "CanceledByOwner" = Types.Canceled);
  Alcotest.(check bool) "CanceledByEngine" true
    (Lighter.Types.status_of_lighter_string "CanceledByEngine" = Types.Canceled);
  Alcotest.(check bool) "CanceledByProtocol" true
    (Lighter.Types.status_of_lighter_string "CanceledByProtocol" = Types.Canceled)

let test_status_of_lighter_string_expired () =
  Alcotest.(check bool) "Expired -> Expired" true
    (Lighter.Types.status_of_lighter_string "Expired" = Types.Expired)

let test_status_of_lighter_string_unknown () =
  match Lighter.Types.status_of_lighter_string "SomethingElse" with
  | Types.Unknown s -> Alcotest.(check string) "static variant" "lighter_unknown_status" s
  | _ -> Alcotest.fail "Expected Unknown"

(* --- order type mapping --- *)

let test_lighter_order_type_int () =
  Alcotest.(check int) "Limit" 0 (Lighter.Types.lighter_order_type_int Types.Limit);
  Alcotest.(check int) "Market" 1 (Lighter.Types.lighter_order_type_int Types.Market);
  Alcotest.(check int) "StopLoss" 2 (Lighter.Types.lighter_order_type_int Types.StopLoss);
  Alcotest.(check int) "TakeProfit" 3 (Lighter.Types.lighter_order_type_int Types.TakeProfit);
  Alcotest.(check int) "StopLossLimit -> StopLoss" 2 (Lighter.Types.lighter_order_type_int Types.StopLossLimit);
  Alcotest.(check int) "TakeProfitLimit -> TakeProfit" 3 (Lighter.Types.lighter_order_type_int Types.TakeProfitLimit)

(* --- TIF mapping --- *)

let test_lighter_tif_int () =
  Alcotest.(check int) "IOC" 0 (Lighter.Types.lighter_tif_int Types.IOC);
  Alcotest.(check int) "FOK -> IOC" 0 (Lighter.Types.lighter_tif_int Types.FOK);
  Alcotest.(check int) "GTC" 1 (Lighter.Types.lighter_tif_int Types.GTC);
  Alcotest.(check int) "GTC post_only" 2 (Lighter.Types.lighter_tif_int ~post_only:true Types.GTC)

(* --- side conversions --- *)

let test_is_ask_of_side () =
  Alcotest.(check bool) "Sell -> true" true (Lighter.Types.is_ask_of_side Types.Sell);
  Alcotest.(check bool) "Buy -> false" false (Lighter.Types.is_ask_of_side Types.Buy)

let test_side_of_is_ask () =
  Alcotest.(check bool) "true -> Sell" true (Lighter.Types.side_of_is_ask true = Types.Sell);
  Alcotest.(check bool) "false -> Buy" true (Lighter.Types.side_of_is_ask false = Types.Buy)

(* --- int <-> float conversions --- *)

let test_float_to_lighter_int () =
  (* 2181.83 with 2 decimals -> 218183 *)
  Alcotest.(check int64) "price 2 dec" 218183L (Lighter.Types.float_to_lighter_int ~decimals:2 2181.83);
  (* 1.5 with 4 decimals -> 15000 *)
  Alcotest.(check int64) "qty 4 dec" 15000L (Lighter.Types.float_to_lighter_int ~decimals:4 1.5);
  (* 0.0 -> 0 *)
  Alcotest.(check int64) "zero" 0L (Lighter.Types.float_to_lighter_int ~decimals:2 0.0)

let test_lighter_int_to_float () =
  Alcotest.(check (float 0.000001)) "218183 2 dec" 2181.83
    (Lighter.Types.lighter_int_to_float ~decimals:2 218183L);
  Alcotest.(check (float 0.000001)) "15000 4 dec" 1.5
    (Lighter.Types.lighter_int_to_float ~decimals:4 15000L)

let test_price_increment_of_decimals () =
  Alcotest.(check (float 0.000001)) "2 dec" 0.01
    (Lighter.Types.price_increment_of_decimals 2);
  Alcotest.(check (float 0.000001)) "4 dec" 0.0001
    (Lighter.Types.price_increment_of_decimals 4)

let test_qty_increment_of_decimals () =
  Alcotest.(check (float 0.000001)) "3 dec" 0.001
    (Lighter.Types.qty_increment_of_decimals 3)

(* --- rounding --- *)

let test_round_price () =
  Alcotest.(check (float 0.000001)) "round 2 dec" 123.46
    (Lighter.Types.round_price ~price_decimals:2 123.456);
  (* idempotency *)
  let r1 = Lighter.Types.round_price ~price_decimals:2 123.456 in
  let r2 = Lighter.Types.round_price ~price_decimals:2 r1 in
  Alcotest.(check (float 0.000000001)) "idempotent" r1 r2;
  (* zero passthrough *)
  Alcotest.(check (float 0.000001)) "zero" 0.0
    (Lighter.Types.round_price ~price_decimals:2 0.0);
  (* negative passthrough *)
  Alcotest.(check (float 0.000001)) "negative" (-5.0)
    (Lighter.Types.round_price ~price_decimals:2 (-5.0))

let test_round_qty () =
  (* floor-based: 1.2369 with 2 dec -> 1.23 *)
  Alcotest.(check (float 0.000001)) "floor 2 dec" 1.23
    (Lighter.Types.round_qty ~size_decimals:2 1.2369);
  (* idempotency *)
  let q1 = Lighter.Types.round_qty ~size_decimals:4 1.23456789 in
  let q2 = Lighter.Types.round_qty ~size_decimals:4 q1 in
  Alcotest.(check (float 0.000000001)) "idempotent" q1 q2

(* --- JSON parsers --- *)

let test_parse_json_float () =
  Alcotest.(check (float 0.000001)) "from string" 1.5
    (Lighter.Types.parse_json_float (`String "1.5"));
  Alcotest.(check (float 0.000001)) "from float" 2.5
    (Lighter.Types.parse_json_float (`Float 2.5));
  Alcotest.(check (float 0.000001)) "from int" 3.0
    (Lighter.Types.parse_json_float (`Int 3));
  Alcotest.(check (float 0.000001)) "bad string" 0.0
    (Lighter.Types.parse_json_float (`String "notanumber"))

let test_parse_json_int () =
  Alcotest.(check int) "from string" 42
    (Lighter.Types.parse_json_int (`String "42"));
  Alcotest.(check int) "from int" 7
    (Lighter.Types.parse_json_int (`Int 7));
  Alcotest.(check int) "from float" 3
    (Lighter.Types.parse_json_int (`Float 3.9));
  Alcotest.(check int) "bad string" 0
    (Lighter.Types.parse_json_int (`String "abc"))

let test_parse_json_int64 () =
  Alcotest.(check int64) "from string" 123456789012345L
    (Lighter.Types.parse_json_int64 (`String "123456789012345"));
  Alcotest.(check int64) "from int" 42L
    (Lighter.Types.parse_json_int64 (`Int 42));
  Alcotest.(check int64) "bad string" 0L
    (Lighter.Types.parse_json_int64 (`String "notint"));
  Alcotest.(check int64) "null" 0L
    (Lighter.Types.parse_json_int64 `Null)

(* --- tx type constants --- *)

let test_tx_type_constants () =
  Alcotest.(check int) "create_order" 14 Lighter.Types.tx_type_create_order;
  Alcotest.(check int) "cancel_order" 15 Lighter.Types.tx_type_cancel_order;
  Alcotest.(check int) "modify_order" 17 Lighter.Types.tx_type_modify_order;
  Alcotest.(check int) "cancel_all" 18 Lighter.Types.tx_type_cancel_all_orders

let () =
  Alcotest.run "Lighter Types" [
    "status_of_lighter_int", [
      Alcotest.test_case "InProgress (0)" `Quick test_status_of_lighter_int_in_progress;
      Alcotest.test_case "Pending (1)" `Quick test_status_of_lighter_int_pending;
      Alcotest.test_case "Active (2)" `Quick test_status_of_lighter_int_active;
      Alcotest.test_case "Filled (3)" `Quick test_status_of_lighter_int_filled;
      Alcotest.test_case "CanceledByOwner (4)" `Quick test_status_of_lighter_int_canceled_by_owner;
      Alcotest.test_case "CanceledByEngine (5)" `Quick test_status_of_lighter_int_canceled_by_engine;
      Alcotest.test_case "CanceledByProtocol (6)" `Quick test_status_of_lighter_int_canceled_by_protocol;
      Alcotest.test_case "Expired (7)" `Quick test_status_of_lighter_int_expired;
      Alcotest.test_case "Unknown (99)" `Quick test_status_of_lighter_int_unknown;
    ];
    "status_of_lighter_string", [
      Alcotest.test_case "Active" `Quick test_status_of_lighter_string_active;
      Alcotest.test_case "Filled" `Quick test_status_of_lighter_string_filled;
      Alcotest.test_case "Canceled variants" `Quick test_status_of_lighter_string_canceled_variants;
      Alcotest.test_case "Expired" `Quick test_status_of_lighter_string_expired;
      Alcotest.test_case "Unknown" `Quick test_status_of_lighter_string_unknown;
    ];
    "order_type", [
      Alcotest.test_case "lighter_order_type_int" `Quick test_lighter_order_type_int;
    ];
    "tif", [
      Alcotest.test_case "lighter_tif_int" `Quick test_lighter_tif_int;
    ];
    "side", [
      Alcotest.test_case "is_ask_of_side" `Quick test_is_ask_of_side;
      Alcotest.test_case "side_of_is_ask" `Quick test_side_of_is_ask;
    ];
    "int_float", [
      Alcotest.test_case "float_to_lighter_int" `Quick test_float_to_lighter_int;
      Alcotest.test_case "lighter_int_to_float" `Quick test_lighter_int_to_float;
      Alcotest.test_case "price_increment_of_decimals" `Quick test_price_increment_of_decimals;
      Alcotest.test_case "qty_increment_of_decimals" `Quick test_qty_increment_of_decimals;
    ];
    "rounding", [
      Alcotest.test_case "round_price" `Quick test_round_price;
      Alcotest.test_case "round_qty" `Quick test_round_qty;
    ];
    "json_parsers", [
      Alcotest.test_case "parse_json_float" `Quick test_parse_json_float;
      Alcotest.test_case "parse_json_int" `Quick test_parse_json_int;
      Alcotest.test_case "parse_json_int64" `Quick test_parse_json_int64;
    ];
    "constants", [
      Alcotest.test_case "tx_type constants" `Quick test_tx_type_constants;
    ]
  ]
