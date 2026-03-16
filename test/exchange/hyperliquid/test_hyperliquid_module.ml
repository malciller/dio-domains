module Exchange = Dio_exchange.Exchange_intf
module Types = Exchange.Types

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

let () =
  Alcotest.run "Hyperliquid Module" [
    "basics", [
      Alcotest.test_case "registration" `Quick test_module_registration;
      Alcotest.test_case "type conversions" `Quick test_type_conversions;
    ]
  ]
