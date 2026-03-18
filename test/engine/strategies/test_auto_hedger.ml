open Dio_strategies
open Dio_strategies.Strategy_common

let setup () =
  (* Clear the ring buffer and reset state *)
  ignore (Auto_hedger.get_pending_orders 1000);
  Auto_hedger.reset_state ()

let test_ignore_wrong_exchange () =
  setup ();
  Auto_hedger.handle_order_filled false "kraken" "HYPE" Buy 100.0 42.50;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Buffer should be empty" 0 (List.length pending)

let test_hedge_buy () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One hedge order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Order symbol" "HYPE" order.symbol;
  Alcotest.(check string) "Order side" "sell" (string_of_order_side order.side);
  Alcotest.(check (float 0.0001)) "Order qty" 100.0 order.qty;
  Alcotest.(check string) "Order strategy" "Hedger" order.strategy

let test_sell_no_short_skips () =
  setup ();
  (* Sell with no existing short → no hedge order *)
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 50.0 43.00;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "No hedge on sell without short" 0 (List.length pending)

let test_sell_closes_short () =
  setup ();
  (* Buy first to build short position, then sell to close it *)
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50;
  ignore (Auto_hedger.get_pending_orders 10);  (* drain the buy hedge *)
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 50.0 43.00;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One close order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Order side" "buy" (string_of_order_side order.side);
  Alcotest.(check (float 0.0001)) "Close qty capped to short" 50.0 order.qty

let test_every_fill_hedged () =
  setup ();
  (* Multiple buys in same direction - ALL should produce hedge orders *)
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 200.0 42.80;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 50.0 42.60;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Three hedge orders for three fills" 3 (List.length pending);
  (* Verify quantities match 1:1 *)
  let qtys = List.map (fun o -> o.qty) pending in
  Alcotest.(check (list (float 0.0001))) "Quantities match fills" [100.0; 200.0; 50.0] qtys;
  (* All should be sell (opposite of buy) *)
  List.iter (fun o ->
    Alcotest.(check string) "Hedge side is sell" "sell" (string_of_order_side o.side)
  ) pending

let test_direction_change_closes_short () =
  setup ();
  (* Buy builds short, sell closes partial short *)
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 75.0 43.00;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Two hedge orders" 2 (List.length pending);
  let first = List.nth pending 0 in
  let second = List.nth pending 1 in
  Alcotest.(check string) "First hedge is sell (open short)" "sell" (string_of_order_side first.side);
  Alcotest.(check (float 0.0001)) "First qty" 100.0 first.qty;
  Alcotest.(check string) "Second hedge is buy (close short)" "buy" (string_of_order_side second.side);
  Alcotest.(check (float 0.0001)) "Second qty capped" 75.0 second.qty

let test_skip_zero_price () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 0.0;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "No hedge on zero price" 0 (List.length pending)

let () =
  Alcotest.run "Auto Hedger" [
    "gatekeeper", [
      Alcotest.test_case "Ignores wrong exchange" `Quick test_ignore_wrong_exchange;
      Alcotest.test_case "Skips zero price fills" `Quick test_skip_zero_price;
    ];
    "delta_neutral", [
      Alcotest.test_case "Hedges buy with perp short" `Quick test_hedge_buy;
      Alcotest.test_case "Sell without short skips hedge" `Quick test_sell_no_short_skips;
      Alcotest.test_case "Sell closes existing short" `Quick test_sell_closes_short;
      Alcotest.test_case "Every fill hedged independently" `Quick test_every_fill_hedged;
      Alcotest.test_case "Direction change closes short" `Quick test_direction_change_closes_short;
    ];
  ]
