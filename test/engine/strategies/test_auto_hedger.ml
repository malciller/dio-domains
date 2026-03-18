open Dio_strategies
open Dio_strategies.Strategy_common

let setup () =
  (* Clear the ring buffer by reading until empty *)
  ignore (Auto_hedger.get_pending_orders 1000)

let test_ignore_wrong_exchange () =
  setup ();
  Auto_hedger.handle_order_filled false "kraken" "HYPE" Buy 100.0 42.50 15.0;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Buffer should be empty" 0 (List.length pending)

let test_hedge_valid_spot () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50 15.0;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One hedge order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Order symbol" "HYPE" order.symbol;
  Alcotest.(check string) "Order side" "sell" (string_of_order_side order.side);
  Alcotest.(check (float 0.0001)) "Order qty" 100.0 order.qty;
  Alcotest.(check string) "Order strategy" "Hedger" order.strategy

let test_hedge_close_short () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 50.0 43.00 15.0;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One hedge order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Order symbol" "HYPE" order.symbol;
  Alcotest.(check string) "Order side" "buy" (string_of_order_side order.side);
  Alcotest.(check (float 0.0001)) "Order qty" 50.0 order.qty;
  Alcotest.(check string) "Order strategy" "Hedger" order.strategy
  
let () =
  Alcotest.run "Auto Hedger" [
    "gatekeeper", [
      Alcotest.test_case "Ignores wrong exchange" `Quick test_ignore_wrong_exchange;
    ];
    "execution", [
      Alcotest.test_case "Hedges Buy to Sell" `Quick test_hedge_valid_spot;
      Alcotest.test_case "Hedges Sell to Buy" `Quick test_hedge_close_short;
    ];
  ]
