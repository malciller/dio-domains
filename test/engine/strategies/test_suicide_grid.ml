open Alcotest


let test_initialization () =
  (* Test strategy initialization *)
  check unit "suicide_grid init" () (Dio_strategies.Suicide_grid.Strategy.init ())

let test_order_creation_place () =
  (* Test creating place orders *)
  let order = Dio_strategies.Suicide_grid.create_place_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true "Grid" in

  check bool "place order operation" true (order.operation = Dio_strategies.Strategy_common.Place);
  check string "place order symbol" "BTC/USD" order.symbol;
  check bool "place order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  check (float 0.) "place order qty" 0.001 order.qty;
  check (option (float 0.)) "place order price" (Some 50000.0) order.price;
  check bool "place order post_only" true order.post_only;
  check string "place order strategy" "Grid" order.strategy

let test_order_creation_amend () =
  (* Test creating amend orders *)
  let order = Dio_strategies.Suicide_grid.create_amend_order "order123" "BTC/USD" Dio_strategies.Strategy_common.Sell 0.001 (Some 51000.0) true "Grid" in

  check bool "amend order operation" true (order.operation = Dio_strategies.Strategy_common.Amend);
  check (option string) "amend order id" (Some "order123") order.order_id;
  check string "amend order symbol" "BTC/USD" order.symbol;
  check bool "amend order side" true (order.side = Dio_strategies.Strategy_common.Sell);
  check (float 0.) "amend order qty" 0.001 order.qty;
  check (option (float 0.)) "amend order price" (Some 51000.0) order.price;
  check bool "amend order post_only" true order.post_only;
  check string "amend order strategy" "Grid" order.strategy

let test_order_creation_cancel () =
  (* Test creating cancel orders *)
  let order = Dio_strategies.Suicide_grid.create_cancel_order "order456" "BTC/USD" "Grid" in

  check bool "cancel order operation" true (order.operation = Dio_strategies.Strategy_common.Cancel);
  check (option string) "cancel order id" (Some "order456") order.order_id;
  check string "cancel order symbol" "BTC/USD" order.symbol;
  check string "cancel order strategy" "Grid" order.strategy;
  check (option (float 0.)) "cancel order price" None order.price;
  check (float 0.) "cancel order qty" 0.0 order.qty

let test_legacy_order_creation () =
  (* Test legacy create_order function for backwards compatibility *)
  let order = Dio_strategies.Suicide_grid.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in

  check bool "legacy order operation" true (order.operation = Dio_strategies.Strategy_common.Place);
  check string "legacy order symbol" "BTC/USD" order.symbol;
  check bool "legacy order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  check (float 0.) "legacy order qty" 0.001 order.qty;
  check (option (float 0.)) "legacy order price" (Some 50000.0) order.price;
  check bool "legacy order post_only" true order.post_only;
  check string "legacy order strategy" "Grid" order.strategy

let test_duplicate_key_per_side () =
  (* Ensure duplicate key is per asset+side, not price/qty *)
  let open Dio_strategies in
  let buy1 = Suicide_grid.create_place_order "BTC/USD" Strategy_common.Buy 0.001 (Some 50000.0) true "Grid" in
  let buy2 = Suicide_grid.create_place_order "BTC/USD" Strategy_common.Buy 0.002 (Some 51000.0) true "Grid" in
  let sell1 = Suicide_grid.create_place_order "BTC/USD" Strategy_common.Sell 0.003 (Some 52000.0) true "Grid" in

  check string "same key for buy side" buy1.duplicate_key buy2.duplicate_key;
  check bool "different key for opposite side" true (buy1.duplicate_key <> sell1.duplicate_key)

let test_config_parsing () =
  (* Test configuration value parsing *)
  let test_parse str default expected =
    let result = Dio_strategies.Suicide_grid.parse_config_float str "test_param" default "TEST" "TEST/USD" in
    abs_float (result -. expected) < 0.0001
  in

  check bool "parse valid float" true (test_parse "0.001" 0.1 0.001);
  check bool "parse invalid float" true (test_parse "invalid" 0.1 0.1);
  check bool "parse empty float" true (test_parse "" 0.05 0.05)

let test_price_rounding () =
  (* Test price rounding - this relies on Kraken instruments feed *)
  (* For now, just test that the function doesn't crash and returns a reasonable value *)
  let rounded = Dio_strategies.Suicide_grid.round_price 50000.12345678 "BTC/USD" in
  check bool "price rounding non-negative" true (rounded >= 0.0)

let test_price_increment () =
  (* Test price increment retrieval - this relies on Kraken instruments feed *)
  let increment = Dio_strategies.Suicide_grid.get_price_increment "BTC/USD" in
  check bool "price increment positive" true (increment > 0.0)

let test_grid_price_calculation () =
  (* Test grid price calculations *)
  let above_price = Dio_strategies.Suicide_grid.calculate_grid_price 50000.0 1.0 true "TEST/USD" in
  let below_price = Dio_strategies.Suicide_grid.calculate_grid_price 50000.0 1.0 false "TEST/USD" in

  (* Should be above and below 50000 with 1% grid *)
  check bool "above price correct" true (above_price >= 50499.0 && above_price <= 50501.0);
  check bool "below price correct" true (below_price >= 49499.0 && below_price <= 49501.0)

let test_state_management () =
  (* Test strategy state management *)
  let state1 = Dio_strategies.Suicide_grid.get_strategy_state "BTC/USD" in
  let state2 = Dio_strategies.Suicide_grid.get_strategy_state "BTC/USD" in

  (* Should return the same state for same symbol *)
  check bool "same state for same symbol" true (state1 == state2)

let test_userref_generation () =
  (* Test userref tagging - Grid strategy should use userref=1 *)
  let strategy_userref = Dio_strategies.Strategy_common.strategy_userref_grid in
  check int "grid strategy userref" 1 strategy_userref;
  
  (* Test that is_strategy_order correctly identifies Grid orders *)
  check bool "userref 1 matches grid" true 
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 1);
  check bool "userref 2 doesn't match grid" false 
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 2)

let test_balance_checking () =
  (* Test balance checking logic *)
  check bool "sufficient buy balance" true (Dio_strategies.Suicide_grid.can_place_buy_order 0.001 100.0 50.0);
  check bool "insufficient buy balance" false (Dio_strategies.Suicide_grid.can_place_buy_order 0.001 10.0 50.0);
  check bool "sufficient sell balance" true (Dio_strategies.Suicide_grid.can_place_sell_order 0.001 1.0 1.0 0.001);
  check bool "insufficient sell balance" false (Dio_strategies.Suicide_grid.can_place_sell_order 0.001 1.0 0.0005 0.001)

let test_order_acknowledgment () =
  (* Test order acknowledgment handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST/USD" in

  (* Add a pending order manually for testing *)
  state.pending_orders <- ("test123", Dio_strategies.Strategy_common.Buy, 50000.0, Unix.time ()) :: state.pending_orders;

  (* Handle acknowledgment *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged "TEST/USD" "order456" Dio_strategies.Strategy_common.Buy 50000.0;

  (* Should update buy order ID tracking *)
  check (option string) "buy order id updated" (Some "order456") state.last_buy_order_id

let test_order_cancellation () =
  (* Test order cancellation handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST2/USD" in

  (* Set up some tracked orders *)
  state.last_buy_order_id <- Some "buy123";
  state.last_buy_order_price <- Some 49000.0;
  state.open_sell_orders <- [("sell456", 51000.0); ("sell789", 52000.0)];

  (* Cancel the buy order *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled "TEST2/USD" "buy123";

  (* Should clear buy order tracking *)
  check (option string) "buy order id cleared" None state.last_buy_order_id;
  check (option (float 0.)) "buy order price cleared" None state.last_buy_order_price

let test_order_rejection () =
  (* Test order rejection handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST3/USD" in

  (* Add a pending order manually for testing *)
  state.pending_orders <- [("test123", Dio_strategies.Strategy_common.Sell, 51000.0, Unix.time ())];

  (* Handle rejection *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_rejected "TEST3/USD" Dio_strategies.Strategy_common.Sell 51000.0;

  (* Should remove from pending orders *)
  check bool "pending orders cleared" true (List.length state.pending_orders = 0)

let () =
  run "Suicide Grid" [
    "initialization", [
      test_case "strategy init" `Quick test_initialization;
    ];
    "order_creation", [
      test_case "place order" `Quick test_order_creation_place;
      test_case "amend order" `Quick test_order_creation_amend;
      test_case "cancel order" `Quick test_order_creation_cancel;
      test_case "legacy order" `Quick test_legacy_order_creation;
      test_case "duplicate key per side" `Quick test_duplicate_key_per_side;
    ];
    "config", [
      test_case "config parsing" `Quick test_config_parsing;
      test_case "price rounding" `Quick test_price_rounding;
      test_case "price increment" `Quick test_price_increment;
      test_case "grid price calculation" `Quick test_grid_price_calculation;
    ];
    "state", [
      test_case "state management" `Quick test_state_management;
      test_case "userref generation" `Quick test_userref_generation;
    ];
    "balance", [
      test_case "balance checking" `Quick test_balance_checking;
    ];
    "events", [
      test_case "order acknowledgment" `Quick test_order_acknowledgment;
      test_case "order cancellation" `Quick test_order_cancellation;
      test_case "order rejection" `Quick test_order_rejection;
    ];
  ]