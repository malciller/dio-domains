module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types
module ExecFeed = Lighter.Executions_feed

let test_module_name () =
  Alcotest.(check string) "module name is lighter" "lighter"
    Lighter.Module.Lighter_impl.name

let test_status_conversions () =
  let check_status exec_status expected_name =
    let result = Lighter.Module.Lighter_impl.status_of_lighter_status exec_status in
    let name = match result with
      | Types.Pending -> "Pending"
      | Types.New -> "New"
      | Types.PartiallyFilled -> "PartiallyFilled"
      | Types.Filled -> "Filled"
      | Types.Canceled -> "Canceled"
      | Types.Expired -> "Expired"
      | Types.Rejected -> "Rejected"
      | Types.Unknown s -> "Unknown:" ^ s
    in
    Alcotest.(check string) expected_name expected_name name
  in
  check_status ExecFeed.PendingStatus "Pending";
  check_status ExecFeed.NewStatus "New";
  check_status ExecFeed.PartiallyFilledStatus "PartiallyFilled";
  check_status ExecFeed.FilledStatus "Filled";
  check_status ExecFeed.CanceledStatus "Canceled";
  check_status ExecFeed.ExpiredStatus "Expired";
  check_status ExecFeed.RejectedStatus "Rejected";
  check_status (ExecFeed.UnknownStatus "foo") "Unknown:foo"

let test_side_conversions () =
  let buy = Lighter.Module.Lighter_impl.side_of_lighter_side ExecFeed.Buy in
  Alcotest.(check bool) "Buy -> Buy" true (buy = Types.Buy);
  let sell = Lighter.Module.Lighter_impl.side_of_lighter_side ExecFeed.Sell in
  Alcotest.(check bool) "Sell -> Sell" true (sell = Types.Sell)

let test_data_accessors_empty () =
  let ticker = Lighter.Module.Lighter_impl.get_ticker ~symbol:"NODATA_L_TICKER" in
  Alcotest.(check bool) "no ticker" true (Option.is_none ticker);
  let tob = Lighter.Module.Lighter_impl.get_top_of_book ~symbol:"NODATA_L_TOB" in
  Alcotest.(check bool) "no top of book" true (Option.is_none tob);
  let bal = Lighter.Module.Lighter_impl.get_balance ~asset:"NODATA_L_ASSET" in
  Alcotest.(check (float 0.000001)) "no balance" 0.0 bal

let test_get_fees_with_instruments () =
  (* Initialize instruments with mock data (these have 0.0 maker/taker fees) *)
  Lighter.Instruments_feed.initialize ["FEETEST"];
  let (maker, taker) = Lighter.Module.Lighter_impl.get_fees ~symbol:"FEETEST" in
  Alcotest.(check bool) "maker fee found" true (Option.is_some maker);
  Alcotest.(check bool) "taker fee found" true (Option.is_some taker);
  (* Unknown symbol *)
  let (mk_miss, tk_miss) = Lighter.Module.Lighter_impl.get_fees ~symbol:"NO_FEE_SYM" in
  Alcotest.(check bool) "maker None" true (Option.is_none mk_miss);
  Alcotest.(check bool) "taker None" true (Option.is_none tk_miss)

let () =
  Alcotest.run "Lighter Module" [
    "basics", [
      Alcotest.test_case "module name" `Quick test_module_name;
    ];
    "conversions", [
      Alcotest.test_case "status conversions" `Quick test_status_conversions;
      Alcotest.test_case "side conversions" `Quick test_side_conversions;
    ];
    "data_access", [
      Alcotest.test_case "empty data accessors" `Quick test_data_accessors_empty;
      Alcotest.test_case "get_fees with instruments" `Quick test_get_fees_with_instruments;
    ]
  ]
