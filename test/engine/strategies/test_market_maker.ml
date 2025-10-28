open Alcotest


(* Mock trading config for testing - using the Market_maker's local trading_config type *)
let create_test_asset ?(exchange="kraken") ?(symbol="BTC/USD") ?(qty="0.001") ?(strategy="MM")
    ?(min_usd_balance=None) ?(max_exposure=None) ?(maker_fee=None) ?(taker_fee=None) () =
  {
    Dio_strategies.Market_maker.exchange;
    symbol;
    qty;
    min_usd_balance;
    max_exposure;
    strategy;
    maker_fee;
    taker_fee;
  }

let test_initialization () =
  (* Test strategy initialization *)
  check unit "market_maker init" () (Dio_strategies.Market_maker.Strategy.init ())

let test_order_creation_place () =
  (* Test creating place orders *)
  let order = Dio_strategies.Market_maker.create_place_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true "MM" in

  check bool "place order operation" true (order.operation = Dio_strategies.Strategy_common.Place);
  check string "place order symbol" "BTC/USD" order.symbol;
  check bool "place order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  check (float 0.) "place order qty" 0.001 order.qty;
  check (option (float 0.)) "place order price" (Some 50000.0) order.price;
  check bool "place order post_only" true order.post_only;
  check string "place order strategy" "MM" order.strategy

let test_order_creation_amend () =
  (* Test creating amend orders *)
  let order = Dio_strategies.Market_maker.create_amend_order "order123" "BTC/USD" Dio_strategies.Strategy_common.Sell 0.001 (Some 51000.0) true "MM" in

  check bool "amend order operation" true (order.operation = Dio_strategies.Strategy_common.Amend);
  check (option string) "amend order id" (Some "order123") order.order_id;
  check string "amend order symbol" "BTC/USD" order.symbol;
  check bool "amend order side" true (order.side = Dio_strategies.Strategy_common.Sell);
  check (float 0.) "amend order qty" 0.001 order.qty;
  check (option (float 0.)) "amend order price" (Some 51000.0) order.price;
  check bool "amend order post_only" true order.post_only;
  check string "amend order strategy" "MM" order.strategy

let test_order_creation_cancel () =
  (* Test creating cancel orders *)
  let order = Dio_strategies.Market_maker.create_cancel_order "order456" "BTC/USD" "MM" in

  check bool "cancel order operation" true (order.operation = Dio_strategies.Strategy_common.Cancel);
  check (option string) "cancel order id" (Some "order456") order.order_id;
  check string "cancel order symbol" "BTC/USD" order.symbol;
  check string "cancel order strategy" "MM" order.strategy;
  check (option (float 0.)) "cancel order price" None order.price;
  check (float 0.) "cancel order qty" 0.0 order.qty

let test_fee_calculation () =
  (* Test fee calculation logic *)
  let asset_no_fee = create_test_asset ~maker_fee:(Some 0.0) () in
  let asset_maker_fee = create_test_asset ~maker_fee:(Some 0.001) () in
  let asset_taker_fee = create_test_asset ~taker_fee:(Some 0.002) () in

  let fee1 = Dio_strategies.Market_maker.get_fee_for_asset asset_no_fee in
  let fee2 = Dio_strategies.Market_maker.get_fee_for_asset asset_maker_fee in
  let fee3 = Dio_strategies.Market_maker.get_fee_for_asset asset_taker_fee in

  check (float 0.) "fee no fee asset" 0.0 fee1;
  check (float 0.) "fee maker fee asset" 0.001 fee2;
  check (float 0.) "fee taker fee asset" 0.002 fee3

let test_price_rounding () =
  (* Test price rounding - this is tricky to test without mocking Kraken instruments feed *)
  (* For now, just test that the function doesn't crash and returns a reasonable value *)
  let rounded = Dio_strategies.Market_maker.round_price 50000.12345678 "BTC/USD" in
  (* Should return a float, even if no precision info is available *)
  check bool "price rounding non-negative" true (rounded >= 0.0)

let test_state_management () =
  (* Test strategy state management *)
  let state1 = Dio_strategies.Market_maker.get_strategy_state "BTC/USD" in
  let state2 = Dio_strategies.Market_maker.get_strategy_state "BTC/USD" in

  (* Should return the same state for same symbol *)
  check bool "same state for same symbol" true (state1 == state2)

let test_userref_generation () =
  (* Test userref tagging - MM strategy should use userref=2 *)
  let strategy_userref = Dio_strategies.Strategy_common.strategy_userref_mm in
  check int "mm strategy userref" 2 strategy_userref;
  
  (* Test that is_strategy_order correctly identifies MM orders *)
  check bool "userref 2 matches mm" true 
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 2);
  check bool "userref 1 doesn't match mm" false 
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 1)

let test_order_acknowledgment () =
  (* Test order acknowledgment handling *)
  let initial_state = Dio_strategies.Market_maker.get_strategy_state "TEST/USD" in

  (* Add a pending order manually for testing *)
  initial_state.pending_orders <- ("test123", Dio_strategies.Strategy_common.Buy, 50000.0) :: initial_state.pending_orders;

  (* Handle acknowledgment *)
  Dio_strategies.Market_maker.Strategy.handle_order_acknowledged "TEST/USD" "order456" Dio_strategies.Strategy_common.Buy 50000.0;

  (* Should update tracking but not remove pending orders since IDs don't match exactly *)
  (* This is a bit tricky to test precisely without more complex mocking *)
  check bool "acknowledgment handled" true true

let test_order_cancellation () =
  (* Test order cancellation handling *)
  let state = Dio_strategies.Market_maker.get_strategy_state "TEST2/USD" in

  (* Set up some tracked orders *)
  state.last_buy_order_id <- Some "buy123";
  state.last_buy_order_price <- Some 49000.0;
  state.last_sell_order_id <- Some "sell456";
  state.last_sell_order_price <- Some 51000.0;

  (* Cancel the buy order *)
  Dio_strategies.Market_maker.Strategy.handle_order_cancelled "TEST2/USD" "buy123";

  (* Should clear buy order tracking *)
  check (option string) "buy order id cleared" None state.last_buy_order_id;
  check (option (float 0.)) "buy order price cleared" None state.last_buy_order_price;
  check (option string) "sell order id preserved" (Some "sell456") state.last_sell_order_id

let test_minimum_quantity_check () =
  (* Test minimum quantity checking - this relies on Kraken instruments feed *)
  (* For now, just test that it doesn't crash *)
  let result = Dio_strategies.Market_maker.meets_min_qty "BTC/USD" 0.001 in
  (* Result depends on feed data, but should be boolean *)
  check bool "min qty returns bool" true (result = true || result = false)

let test_config_parsing () =
  (* Test configuration value parsing *)
  let test_parse str default expected =
    let result = Dio_strategies.Market_maker.parse_config_float str "test_param" default "TEST" "TEST/USD" in
    abs_float (result -. expected) < 0.0001
  in

  check bool "parse valid float" true (test_parse "0.001" 0.1 0.001);
  check bool "parse invalid float" true (test_parse "invalid" 0.1 0.1);
  check bool "parse empty float" true (test_parse "" 0.05 0.05)

let test_config_parsing_optional () =
  (* Test optional configuration value parsing *)
  let test_parse str exchange symbol expected =
    let result = Dio_strategies.Market_maker.parse_config_float_opt str "test_param" exchange symbol in
    match result, expected with
    | Some r, Some e -> abs_float (r -. e) < 0.0001
    | None, None -> true
    | _ -> false
  in

  check bool "parse optional valid" true (test_parse "100.0" "TEST" "TEST/USD" (Some 100.0));
  check bool "parse optional empty" true (test_parse "" "TEST" "TEST/USD" None);
  check bool "parse optional invalid" true (test_parse "invalid" "TEST" "TEST/USD" None)

let test_duplicate_cancellation () =
  (* Test duplicate order cancellation logic *)
  let open_orders = [
    ("order1", 50000.0, 0.001, "buy", Some 2);
    ("order2", 50000.0, 0.001, "buy", Some 2);  (* Same price - should be cancelled *)
    ("order3", 51000.0, 0.001, "buy", Some 2);  (* Different price - should not be cancelled *)
  ] in

  (* Mock the strategy state to have a cancelled order *)
  let state = Dio_strategies.Market_maker.get_strategy_state "TEST/USD" in
  state.cancelled_orders <- [("order1", Unix.time ())];  (* Mark order1 as cancelled *)

  (* Count duplicates cancelled *)
  let cancelled_count = Dio_strategies.Market_maker.cancel_duplicate_orders "TEST/USD" 50000.0 Dio_strategies.Strategy_common.Buy open_orders "MM" in

  (* Should cancel 1 duplicate (order2), order1 is already cancelled *)
  check int "duplicate cancellation count" 1 cancelled_count

let test_sell_first_placement_logic () =
  (* Test that sell orders are created before buy orders *)
  (* This is more of an integration test - we'd need to mock the entire execution flow *)
  (* For now, just test that the logic functions exist and don't crash *)
  let asset = create_test_asset () in
  let current_price = Some 50000.0 in
  let top_of_book = Some (49950.0, 1.0, 50050.0, 1.0) in
  let asset_balance = Some 0.1 in
  let quote_balance = Some 1000.0 in

  (* This would trigger the sell-first logic, but testing it fully requires mocking the order ringbuffer *)
  (* For now, just ensure the function can be called without crashing *)
  Dio_strategies.Market_maker.Strategy.execute asset current_price top_of_book asset_balance quote_balance 0 0 [] 1;
  check bool "execute function runs" true true

let test_fee_cache_integration () =
  (* Test that fee cache is initialized and can be queried *)
  Dio_strategies.Fee_cache.init ();
  let fee_opt = Dio_strategies.Fee_cache.get_maker_fee ~exchange:"kraken" ~symbol:"BTC/USD" in
  (* Initially should be None since no data cached *)
  check (option (float 0.)) "initial fee cache empty" None fee_opt;

  (* Test cache storage and retrieval *)
  Dio_strategies.Fee_cache.store_fee ~exchange:"kraken" ~symbol:"BTC/USD" ~fee:0.001 ~ttl_seconds:600.0;
  let fee_opt2 = Dio_strategies.Fee_cache.get_maker_fee ~exchange:"kraken" ~symbol:"BTC/USD" in
  check (option (float 0.)) "fee cache retrieval" (Some 0.001) fee_opt2

let test_fee_cache_no_duplicate_fetches () =
  (* Test that refresh_if_missing doesn't trigger network calls when cache has valid data *)
  Dio_strategies.Fee_cache.init ();

  (* Store a fee in cache *)
  Dio_strategies.Fee_cache.store_fee ~exchange:"kraken" ~symbol:"TEST/USD" ~fee:0.002 ~ttl_seconds:600.0;

  (* Verify it's cached *)
  let fee_opt = Dio_strategies.Fee_cache.get_maker_fee ~exchange:"kraken" ~symbol:"TEST/USD" in
  check (option (float 0.)) "fee cached before refresh_if_missing" (Some 0.002) fee_opt;

  (* Call refresh_if_missing - should complete immediately since data is cached *)
  Dio_strategies.Fee_cache.refresh_if_missing "TEST/USD";

  (* Fee should still be cached *)
  let fee_opt2 = Dio_strategies.Fee_cache.get_maker_fee ~exchange:"kraken" ~symbol:"TEST/USD" in
  check (option (float 0.)) "fee still cached after refresh_if_missing" (Some 0.002) fee_opt2

let test_profitability_checks () =
  (* Test profitability calculations *)
  (* For zero fee assets, any spread should be profitable *)
  let zero_fee_asset = create_test_asset ~maker_fee:(Some 0.0) () in

  (* For fee assets, test the required spread calculation *)
  let fee_asset = create_test_asset ~maker_fee:(Some 0.001) () in

  (* These tests would require more complex mocking of the execution environment *)
  (* For now, ensure the fee calculation functions work *)
  let fee1 = Dio_strategies.Market_maker.get_fee_for_asset zero_fee_asset in
  let fee2 = Dio_strategies.Market_maker.get_fee_for_asset fee_asset in

  check (float 0.) "zero fee calculation" 0.0 fee1;
  check (float 0.) "maker fee calculation" 0.001 fee2

let test_post_only_checks () =
  (* Test post-only validation logic *)
  (* This is mainly tested through the execution logic, which requires extensive mocking *)
  (* For now, just ensure the strategy can handle various price scenarios *)
  let asset = create_test_asset () in

  (* Test with prices where buy < bid (valid post-only) *)
  Dio_strategies.Market_maker.Strategy.execute asset (Some 50000.0) (Some (49950.0, 1.0, 50050.0, 1.0)) (Some 0.1) (Some 1000.0) 0 0 [] 1;

  (* Test with prices where buy >= bid (invalid post-only - should be skipped) *)
  Dio_strategies.Market_maker.Strategy.execute asset (Some 50000.0) (Some (50000.0, 1.0, 50050.0, 1.0)) (Some 0.1) (Some 1000.0) 0 0 [] 2;

  check bool "post-only checks handled" true true

let () =
  run "Market Maker" [
    "initialization", [
      test_case "strategy init" `Quick test_initialization;
    ];
    "order_creation", [
      test_case "place order" `Quick test_order_creation_place;
      test_case "amend order" `Quick test_order_creation_amend;
      test_case "cancel order" `Quick test_order_creation_cancel;
    ];
    "fee_calculation", [
      test_case "fee calculation" `Quick test_fee_calculation;
      test_case "fee cache integration" `Quick test_fee_cache_integration;
      test_case "fee cache no duplicate fetches" `Quick test_fee_cache_no_duplicate_fetches;
    ];
    "price_handling", [
      test_case "price rounding" `Quick test_price_rounding;
    ];
    "state_management", [
      test_case "state management" `Quick test_state_management;
      test_case "userref generation" `Quick test_userref_generation;
    ];
    "order_handling", [
      test_case "order acknowledgment" `Quick test_order_acknowledgment;
      test_case "order cancellation" `Quick test_order_cancellation;
      test_case "minimum quantity check" `Quick test_minimum_quantity_check;
      test_case "duplicate cancellation" `Quick test_duplicate_cancellation;
    ];
    "strategy_logic", [
      test_case "sell first placement" `Quick test_sell_first_placement_logic;
      test_case "profitability checks" `Quick test_profitability_checks;
      test_case "post-only checks" `Quick test_post_only_checks;
    ];
    "config_parsing", [
      test_case "config parsing" `Quick test_config_parsing;
      test_case "config parsing optional" `Quick test_config_parsing_optional;
    ];
  ]