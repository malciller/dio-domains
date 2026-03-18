open Dio_strategies
open Dio_strategies.Strategy_common

let setup () =
  (* Clear the ring buffer and reset state *)
  ignore (Auto_hedger.get_pending_orders 1000);
  Auto_hedger.reset_state ()

(* Simulated perp top-of-book: (bid_price, bid_size, ask_price, ask_size) *)
let mock_tob = Some (42.20, 100.0, 42.22, 50.0)

let test_ignore_wrong_exchange () =
  setup ();
  Auto_hedger.handle_order_filled false "kraken" "HYPE" Buy 100.0 42.50 None;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Buffer should be empty" 0 (List.length pending)

let test_hedge_buy_with_tob () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50 mock_tob;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One hedge order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Order symbol" "HYPE" order.symbol;
  Alcotest.(check string) "Order side" "sell" (string_of_order_side order.side);
  Alcotest.(check (float 0.0001)) "Order qty" 100.0 order.qty;
  (* Sell hedge should use best BID as limit price *)
  Alcotest.(check string) "Order type is limit" "limit" order.order_type;
  Alcotest.(check (option (float 0.0001))) "Limit price is best bid" (Some 42.20) order.price

let test_hedge_buy_no_tob_fallback () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50 None;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One hedge order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Falls back to market" "market" order.order_type;
  Alcotest.(check (option (float 0.0001))) "No limit price" None order.price

let test_sell_no_short_skips () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 50.0 43.00 mock_tob;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "No hedge on sell without short" 0 (List.length pending)

let test_sell_closes_full_short () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50 mock_tob;
  ignore (Auto_hedger.get_pending_orders 10);
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 50.0 43.00 mock_tob;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One close order generated" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check string) "Order side" "buy" (string_of_order_side order.side);
  (* Close qty should be the full hedge qty (100.0), NOT the sell qty (50.0) *)
  Alcotest.(check (float 0.0001)) "Close qty is full hedge" 100.0 order.qty;
  (* Buy hedge (close) should use best ASK as limit price *)
  Alcotest.(check string) "Order type is limit" "limit" order.order_type;
  Alcotest.(check (option (float 0.0001))) "Limit price is best ask" (Some 42.22) order.price

let test_second_buy_skipped () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50 mock_tob;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 200.0 42.19 mock_tob;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 50.0 42.05 mock_tob;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Only one hedge order from first buy" 1 (List.length pending);
  let order = List.hd pending in
  Alcotest.(check (float 0.0001)) "Qty from first buy" 100.0 order.qty;
  Alcotest.(check string) "Side is sell" "sell" (string_of_order_side order.side)

let test_full_cycle () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.50 mock_tob;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.19 mock_tob;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.05 mock_tob;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "Only one short opened" 1 (List.length pending);
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Sell 100.0 42.21 mock_tob;
  let pending2 = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "One close order" 1 (List.length pending2);
  let close = List.hd pending2 in
  Alcotest.(check string) "Close side is buy" "buy" (string_of_order_side close.side);
  Alcotest.(check (float 0.0001)) "Close qty is full hedge" 100.0 close.qty;
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 42.05 mock_tob;
  let pending3 = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "New short opened after close" 1 (List.length pending3);
  let new_short = List.hd pending3 in
  Alcotest.(check string) "New short side is sell" "sell" (string_of_order_side new_short.side)

let test_skip_zero_price () =
  setup ();
  Auto_hedger.handle_order_filled false "hyperliquid" "HYPE" Buy 100.0 0.0 mock_tob;
  let pending = Auto_hedger.get_pending_orders 10 in
  Alcotest.(check int) "No hedge on zero price" 0 (List.length pending)

let () =
  Alcotest.run "Auto Hedger" [
    "gatekeeper", [
      Alcotest.test_case "Ignores wrong exchange" `Quick test_ignore_wrong_exchange;
      Alcotest.test_case "Skips zero price fills" `Quick test_skip_zero_price;
    ];
    "single_short", [
      Alcotest.test_case "Buy uses perp best bid for limit" `Quick test_hedge_buy_with_tob;
      Alcotest.test_case "No TOB falls back to market" `Quick test_hedge_buy_no_tob_fallback;
      Alcotest.test_case "Sell without hedge skips" `Quick test_sell_no_short_skips;
      Alcotest.test_case "Sell closes full hedge at best ask" `Quick test_sell_closes_full_short;
      Alcotest.test_case "Second buy skipped while hedge open" `Quick test_second_buy_skipped;
      Alcotest.test_case "Full cycle: open, skip, close, reopen" `Quick test_full_cycle;
    ];
  ]
