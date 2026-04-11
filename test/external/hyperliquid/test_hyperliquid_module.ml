module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types
module ExecFeed = Hyperliquid.Executions_feed

let test_module_registration () =
  (* Check that the module name is correctly defined *)
  Alcotest.(check string) "module name is hyperliquid" "hyperliquid" Hyperliquid.Module.Hyperliquid_impl.name

let test_type_conversions () =
  let side_buy = Hyperliquid.Module.Hyperliquid_impl.string_of_side Types.Buy in
  Alcotest.(check string) "Buy" "buy" side_buy;

  let side_sell = Hyperliquid.Module.Hyperliquid_impl.string_of_side Types.Sell in
  Alcotest.(check string) "Sell" "sell" side_sell;

  let order_limit = Hyperliquid.Module.Hyperliquid_impl.string_of_order_type Types.Limit in
  Alcotest.(check string) "Limit" "limit" order_limit;

  let order_market = Hyperliquid.Module.Hyperliquid_impl.string_of_order_type Types.Market in
  Alcotest.(check string) "Market" "market" order_market

let test_status_conversions () =
  let check_status hl_status expected_name =
    let result = Hyperliquid.Module.Hyperliquid_impl.status_of_hyperliquid_status hl_status in
    let name = match result with
      | Types.New -> "New"
      | Types.PartiallyFilled -> "PartiallyFilled"
      | Types.Filled -> "Filled"
      | Types.Canceled -> "Canceled"
      | Types.Expired -> "Expired"
      | Types.Rejected -> "Rejected"
      | Types.Unknown s -> "Unknown:" ^ s
      | _ -> "other"
    in
    Alcotest.(check string) expected_name expected_name name
  in
  check_status ExecFeed.PendingNewStatus "New";
  check_status ExecFeed.NewStatus "New";
  check_status ExecFeed.PartiallyFilledStatus "PartiallyFilled";
  check_status ExecFeed.FilledStatus "Filled";
  check_status ExecFeed.CanceledStatus "Canceled";
  check_status ExecFeed.ExpiredStatus "Expired";
  check_status ExecFeed.RejectedStatus "Rejected";
  check_status (ExecFeed.UnknownStatus "foo") "Unknown:foo"

let test_side_conversions () =
  let buy = Hyperliquid.Module.Hyperliquid_impl.side_of_hyperliquid_side ExecFeed.Buy in
  Alcotest.(check bool) "Buy -> Buy" true (buy = Types.Buy);
  let sell = Hyperliquid.Module.Hyperliquid_impl.side_of_hyperliquid_side ExecFeed.Sell in
  Alcotest.(check bool) "Sell -> Sell" true (sell = Types.Sell)

let test_data_accessors_empty () =

  let tob = Hyperliquid.Module.Hyperliquid_impl.get_top_of_book ~symbol:"NODATA_TOB" in
  Alcotest.(check bool) "no top of book" true (Option.is_none tob);
  let bal = Hyperliquid.Module.Hyperliquid_impl.get_balance ~asset:"NODATA_ASSET" in
  Alcotest.(check (float 0.000001)) "no balance" 0.0 bal

let () =
  Alcotest.run "Hyperliquid Module" [
    "basics", [
      Alcotest.test_case "registration" `Quick test_module_registration;
      Alcotest.test_case "type conversions" `Quick test_type_conversions;
    ];
    "conversions", [
      Alcotest.test_case "status conversions" `Quick test_status_conversions;
      Alcotest.test_case "side conversions" `Quick test_side_conversions;
    ];
    "data_access", [
      Alcotest.test_case "empty data accessors" `Quick test_data_accessors_empty;
    ]
  ]
