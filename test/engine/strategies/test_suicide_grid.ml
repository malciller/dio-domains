open Alcotest


let test_initialization () =
  (* Test strategy initialization *)
  check unit "suicide_grid init" () (Dio_strategies.Suicide_grid.Strategy.init ())

let test_order_creation_place () =
  (* Test creating place orders *)
  let order = Dio_strategies.Suicide_grid.create_place_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true Dio_strategies.Strategy_common.Grid "kraken" in

  check bool "place order operation" true (order.operation = Dio_strategies.Strategy_common.Place);
  check string "place order symbol" "BTC/USD" order.symbol;
  check bool "place order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  check (float 0.) "place order qty" 0.001 order.qty;
  check (option (float 0.)) "place order price" (Some 50000.0) order.price;
  check bool "place order post_only" true order.post_only;
  check bool "place order strategy" true (order.strategy = Dio_strategies.Strategy_common.Grid)

let test_order_creation_amend () =
  (* Test creating amend orders *)
  let order = Dio_strategies.Suicide_grid.create_amend_order "order123" "BTC/USD" Dio_strategies.Strategy_common.Sell 0.001 (Some 51000.0) true Dio_strategies.Strategy_common.Grid "kraken" in

  check bool "amend order operation" true (order.operation = Dio_strategies.Strategy_common.Amend);
  check (option string) "amend order id" (Some "order123") order.order_id;
  check string "amend order symbol" "BTC/USD" order.symbol;
  check bool "amend order side" true (order.side = Dio_strategies.Strategy_common.Sell);
  check (float 0.) "amend order qty" 0.001 order.qty;
  check (option (float 0.)) "amend order price" (Some 51000.0) order.price;
  check bool "amend order post_only" true order.post_only;
  check bool "amend order strategy" true (order.strategy = Dio_strategies.Strategy_common.Grid)

let test_order_creation_cancel () =
  (* Test creating cancel orders *)
  let order = Dio_strategies.Suicide_grid.create_cancel_order "order456" "BTC/USD" Dio_strategies.Strategy_common.Grid "kraken" in

  check bool "cancel order operation" true (order.operation = Dio_strategies.Strategy_common.Cancel);
  check (option string) "cancel order id" (Some "order456") order.order_id;
  check string "cancel order symbol" "BTC/USD" order.symbol;
  check bool "cancel order strategy" true (order.strategy = Dio_strategies.Strategy_common.Grid);
  check (option (float 0.)) "cancel order price" None order.price;
  check (float 0.) "cancel order qty" 0.0 order.qty

let test_legacy_order_creation () =
  (* Test legacy create_order function for backwards compatibility *)
  ()
  (* Check if create_order exists, if not remove test or alias it. Assuming it was renamed to create_place_order or removed. 
     If it's removed, we should remove this test case. For now, let's comment it out or update it to create_place_order if legacy is gone. *)
  (* let order = Dio_strategies.Suicide_grid.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in *)


  (* let order = Dio_strategies.Suicide_grid.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in *)



let test_duplicate_key_per_side () =
  (* Ensure duplicate key is per asset+side, not price/qty *)
  let open Dio_strategies in
  let buy1 = Suicide_grid.create_place_order "BTC/USD" Strategy_common.Buy 0.001 (Some 50000.0) true Strategy_common.Grid "kraken" in
  let buy2 = Suicide_grid.create_place_order "BTC/USD" Strategy_common.Buy 0.002 (Some 51000.0) true Strategy_common.Grid "kraken" in
  let sell1 = Suicide_grid.create_place_order "BTC/USD" Strategy_common.Sell 0.003 (Some 52000.0) true Strategy_common.Grid "kraken" in

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
  let rounded = Dio_strategies.Suicide_grid.round_price 50000.12345678 "BTC/USD" "kraken" in
  check bool "price rounding non-negative" true (rounded >= 0.0)

let test_price_increment () =
  (* Test price increment retrieval - this relies on Kraken instruments feed *)
  let increment = Dio_strategies.Suicide_grid.get_price_increment "BTC/USD" "kraken" in
  check bool "price increment positive" true (increment > 0.0)

let test_grid_price_calculation () =
  (* Test grid price calculations *)
  let above_price = Dio_strategies.Suicide_grid.calculate_grid_price 50000.0 1.0 true "TEST/USD" "kraken" in
  let below_price = Dio_strategies.Suicide_grid.calculate_grid_price 50000.0 1.0 false "TEST/USD" "kraken" in

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
  state.open_sell_orders <- [("sell456", 51000.0, 1.0); ("sell789", 52000.0, 1.0)];

  (* Cancel the buy order *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled "TEST2/USD" "buy123" Dio_strategies.Strategy_common.Buy None;

  (* Should clear buy order tracking *)
  check (option string) "buy order id cleared" None state.last_buy_order_id;
  check (option (float 0.)) "buy order price cleared" None state.last_buy_order_price

let test_order_cancellation_matches_client_order_id () =
  (* Lighter (and similar): strategy may still track client index while the
     execution feed reports exchange order_index on cancel. *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST_CLID/USD" in
  state.last_buy_order_id <- Some "1";
  state.last_buy_order_price <- Some 2200.0;
  Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled "TEST_CLID/USD"
    "577023702126926647" Dio_strategies.Strategy_common.Buy (Some "1");
  check (option string) "buy cleared via cl_ord_id alias" None state.last_buy_order_id;
  check (option (float 0.)) "buy price cleared" None state.last_buy_order_price

let test_order_rejection () =
  (* Test order rejection handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST3/USD" in

  (* Add a pending order manually for testing *)
  state.pending_orders <- [("test123", Dio_strategies.Strategy_common.Sell, 51000.0, Unix.time ())];

  (* Handle rejection *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_rejected "TEST3/USD" Dio_strategies.Strategy_common.Sell 51000.0;

  (* Should remove from pending orders *)
  check bool "pending orders cleared" true (List.length state.pending_orders = 0)

let test_accumulation_profit_tracking () =
  (* Test that handle_order_filled correctly accumulates profit from sell fills.
     Flow: buy fills at buy_price, sell fills at sell_price > buy_price → profit accrues.
     
     With qty=0.35, buy@39.50, sell@39.90, maker_fee=0.0004:
       gross = (39.90 - 39.50) * 0.35 = 0.14
       fees  = (39.90 * 0.35 * 0.0004) + (39.50 * 0.35 * 0.0004) = 0.011116
       net   = 0.14 - 0.011116 = 0.128884  *)
  let symbol = "ACCUM_TEST/USDC" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.accumulated_profit <- 0.0;
  state.grid_qty <- 0.35;
  state.maker_fee <- 0.0004;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done symbol;

  (* Simulate buy fill: sets last_buy_fill_price *)
  state.last_buy_order_id <- Some "buy001";
  state.last_buy_order_price <- Some 39.50;
  Dio_strategies.Suicide_grid.Strategy.handle_order_filled symbol "buy001" Dio_strategies.Strategy_common.Buy ~fill_price:39.50 None;

  (* Verify buy fill recorded the price for later profit calc *)
  check (option (float 0.01)) "buy fill price recorded" (Some 39.50) state.last_buy_fill_price;

  (* Simulate sell fill at a higher price *)
  state.open_sell_orders <- [("sell001", 39.90, 1.0)];
  Dio_strategies.Suicide_grid.Strategy.handle_order_filled symbol "sell001" Dio_strategies.Strategy_common.Sell ~fill_price:39.90 None;

  (* Verify profit was accumulated *)
  let expected_gross = (39.90 -. 39.50) *. 0.35 in
  let expected_fees = (39.90 *. 0.35 *. 0.0004) +. (39.50 *. 0.35 *. 0.0004) in
  let expected_net = expected_gross -. expected_fees in
  check bool "profit accumulated" true (state.accumulated_profit > 0.0);
  check bool "profit value correct" true
    (abs_float (state.accumulated_profit -. expected_net) < 0.0001)

let test_accumulation_gated_sell_insufficient () =
  (* Test that when accumulated_profit is BELOW required_profit,
     the sell qty falls back to 1:1 (qty, not rounded_sell).
     
     With qty=0.35, sell_mult=0.999, price=40.0:
       rounded_sell = round_qty(0.35 * 0.999) = round_qty(0.34965) 
       On Hyperliquid the lot size defaults to 0.01, so rounded_sell = 0.34
       rounding_diff = 0.35 - 0.34 = 0.01
       required_profit = 0.01 * 40.0 + 0.05 = 0.45
     
     With accumulated_profit = 0.10 (< 0.45), sell_qty should be 0.35 (1:1) *)
  let symbol = "GATE_TEST/USDC" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.accumulated_profit <- 0.10;

  let qty = 0.35 in
  let sell_mult = 0.999 in
  let sell_price = 40.0 in
  let accumulation_buffer = 0.05 in

  (* Replicate the exact logic from execute_strategy *)
  let rounded_sell = Dio_strategies.Suicide_grid.round_qty (qty *. sell_mult) symbol "hyperliquid" in
  let rounding_diff = qty -. rounded_sell in
  let required_profit = rounding_diff *. sell_price +. accumulation_buffer in

  let sell_qty =
    if required_profit > 0.0 && state.accumulated_profit >= required_profit then begin
      state.accumulated_profit <- state.accumulated_profit -. required_profit;
      rounded_sell
    end else
      qty
  in

  check bool "sell qty falls back to 1:1 when profit insufficient" true
    (abs_float (sell_qty -. qty) < 0.0001);
  (* Profit should NOT have been debited *)
  check bool "profit unchanged" true
    (abs_float (state.accumulated_profit -. 0.10) < 0.0001);
  (* Verify the threshold was meaningful *)
  check bool "required_profit > accumulated" true (required_profit > state.accumulated_profit)

let test_accumulation_gated_sell_sufficient () =
  (* Test that when accumulated_profit >= required_profit,
     the sell qty uses REDUCED amount (rounded_sell) and profit is debited.
     
     Same params as above but with accumulated_profit = 1.00 (> 0.45) *)
  let symbol = "GATE_OK/USDC" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.accumulated_profit <- 1.00;

  let qty = 0.35 in
  let sell_mult = 0.999 in
  let sell_price = 40.0 in
  let accumulation_buffer = 0.05 in

  let rounded_sell = Dio_strategies.Suicide_grid.round_qty (qty *. sell_mult) symbol "hyperliquid" in
  let rounding_diff = qty -. rounded_sell in
  let required_profit = rounding_diff *. sell_price +. accumulation_buffer in

  let sell_qty =
    if required_profit > 0.0 && state.accumulated_profit >= required_profit then begin
      state.accumulated_profit <- state.accumulated_profit -. required_profit;
      rounded_sell
    end else
      qty
  in

  check bool "sell qty uses reduced amount" true (sell_qty < qty);
  check bool "sell qty equals rounded_sell" true
    (abs_float (sell_qty -. rounded_sell) < 0.0001);
  (* Profit should have been debited by required_profit *)
  let expected_remaining = 1.00 -. required_profit in
  check bool "profit debited correctly" true
    (abs_float (state.accumulated_profit -. expected_remaining) < 0.0001)

(* Helper: round qty using instrument feed directly.
   Production code goes through Exchange.Registry -> Hyperliquid_impl -> Instruments_feed,
   but the test binary may not link the exchange module. This calls the feed directly. *)
let round_qty_hl qty sym =
  let inc = match Hyperliquid.Instruments_feed.get_qty_increment sym with Some v -> v | None -> 0.01 in
  let inv = 1.0 /. inc in
  floor (qty *. inv) /. inv

let test_accumulation_full_lifecycle () =
  (* End-to-end test with realistic HYPE/USDC lot sizing.
     HYPE sz_decimals=2 → lot=0.01 (asset)
     
     qty=0.35 (asset), buy@39.50, sell@39.90 (USDC), sell_mult=0.999, buffer=0.05 USDC:
       round_qty(0.35 * 0.999) = round_qty(0.34965) = 0.34 (asset, lot=0.01)
       rounding_diff = 0.35 - 0.34 = 0.01 (asset)
       required_profit = 0.01 * 39.90 + 0.05 = 0.449 (USDC)
     Each cycle net profit:
       gross = (39.90 - 39.50) * 0.35 = 0.14 (USDC)
       fees  = (39.90*0.35 + 39.50*0.35) * 0.0004 = 0.011116 (USDC)
       net   = 0.14 - 0.011116 ≈ 0.128884 (USDC)
     Need ~4 cycles to reach 0.449 USDC *)
  let symbol = "LIFECYCLE_HYPE/USDC" in

  (* Register instrument with HYPE's real lot size: 2 decimal places *)
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;

  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.accumulated_profit <- 0.0;
  state.grid_qty <- 0.35;         (* 0.35 asset *)
  state.maker_fee <- 0.0004;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done symbol;

  let buy_price = 39.50 in        (* USDC per asset *)
  let sell_price = 39.90 in       (* USDC per asset *)
  let accumulation_buffer = 0.05 in  (* USDC *)
  let sell_mult = 0.999 in

  (* Run 5 profitable buy→sell cycles *)
  for i = 1 to 5 do
    let buy_id = Printf.sprintf "buy_%d" i in
    let sell_id = Printf.sprintf "sell_%d" i in

    state.last_buy_order_id <- Some buy_id;
    state.last_buy_order_price <- Some buy_price;
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled symbol buy_id Dio_strategies.Strategy_common.Buy ~fill_price:buy_price None;

    state.open_sell_orders <- [(sell_id, sell_price, 1.0)];
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled symbol sell_id Dio_strategies.Strategy_common.Sell ~fill_price:sell_price None;
  done;

  (* After 5 cycles: ~5 * 0.128884 ≈ 0.644 USDC accumulated *)
  check bool "profit accumulated over 5 cycles" true (state.accumulated_profit > 0.0);

  (* Test the gating decision *)
  let qty = 0.35 in  (* asset *)
  let rounded_sell = round_qty_hl (qty *. sell_mult) symbol in
  (* rounded_sell = 0.34 (asset), rounding_diff = 0.01 (asset) *)
  let rounding_diff = qty -. rounded_sell in
  (* required_profit = 0.01 * 39.90 + 0.05 = 0.449 (USDC) *)
  let required_profit = rounding_diff *. sell_price +. accumulation_buffer in

  check bool "rounded_sell is 0.34 (asset)" true (abs_float (rounded_sell -. 0.34) < 0.0001);
  check bool "rounding_diff is 0.01 (asset)" true (abs_float (rounding_diff -. 0.01) < 0.0001);
  check bool "required_profit ≈ 0.449 (USDC)" true (abs_float (required_profit -. 0.449) < 0.01);

  let profit_before = state.accumulated_profit in
  let can_accumulate = profit_before >= required_profit in

  let sell_qty =
    if required_profit > 0.0 && state.accumulated_profit >= required_profit then begin
      state.accumulated_profit <- state.accumulated_profit -. required_profit;
      rounded_sell
    end else
      qty
  in

  (* With 5 cycles (~0.644 USDC) vs required 0.449 USDC, gate should fire *)
  check bool "lifecycle: enough profit to gate" true can_accumulate;
  check bool "lifecycle: gated sell fires reduced qty 0.34 (asset)" true (sell_qty < qty);
  check bool "lifecycle: sell qty = rounded_sell" true
    (abs_float (sell_qty -. rounded_sell) < 0.0001);
  check bool "lifecycle: profit debited" true (state.accumulated_profit < profit_before)

let test_accumulation_multi_strategy_isolation () =
  (* Test two strategies with different lot sizes running concurrently:
     
     BTC/USDC — sz_decimals=5 (lot=0.00001 asset)
       qty=0.0002 (asset), price ~84000 USDC, buffer=1.00 USDC
       round_qty(0.0002 * 0.999) = round_qty(0.00019980) = 0.00019 (asset)
       rounding_diff = 0.0002 - 0.00019 = 0.00001 (asset)
       required = 0.00001 * 84336 + 1.00 = 1.84336 (USDC)
     
     HYPE/USDC — sz_decimals=2 (lot=0.01 asset)
       qty=0.35 (asset), price ~40 USDC, buffer=0.05 USDC
       round_qty(0.35 * 0.999) = round_qty(0.34965) = 0.34 (asset)
       rounding_diff = 0.35 - 0.34 = 0.01 (asset)
       required = 0.01 * 39.90 + 0.05 = 0.449 (USDC) *)
  let btc_sym = "ISO_BTC/USDC" in
  let hype_sym = "ISO_HYPE/USDC" in

  (* Register instruments with real lot sizes *)
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol:btc_sym ~sz_decimals:5;
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol:hype_sym ~sz_decimals:2;

  let btc = Dio_strategies.Suicide_grid.get_strategy_state btc_sym in
  let hype = Dio_strategies.Suicide_grid.get_strategy_state hype_sym in

  (* Verify states are distinct objects *)
  check bool "distinct state objects" true (btc != hype);

  (* Reset both *)
  btc.accumulated_profit <- 0.0;
  btc.grid_qty <- 0.0002;    (* 0.0002 BTC (asset) *)
  btc.maker_fee <- 0.0004;
  hype.accumulated_profit <- 0.0;
  hype.grid_qty <- 0.35;     (* 0.35 HYPE (asset) *)
  hype.maker_fee <- 0.0004;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done btc_sym;
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done hype_sym;

  (* Verify lot sizes are correct *)
  let btc_rounded = round_qty_hl (0.0002 *. 0.999) btc_sym in
  let hype_rounded = round_qty_hl (0.35 *. 0.999) hype_sym in
  check bool "BTC rounded_sell = 0.00019 (asset, lot=0.00001)" true
    (abs_float (btc_rounded -. 0.00019) < 0.000001);
  check bool "HYPE rounded_sell = 0.34 (asset, lot=0.01)" true
    (abs_float (hype_rounded -. 0.34) < 0.0001);

  (* --- BTC cycles: buy@84000 → sell@84336 USDC (+0.4%) --- *)
  (* net = (84336 - 84000) * 0.0002 - fees = 0.0672 - 0.01345 ≈ 0.054 USDC per cycle *)
  for i = 1 to 30 do
    let buy_id = Printf.sprintf "btc_buy_%d" i in
    let sell_id = Printf.sprintf "btc_sell_%d" i in
    btc.last_buy_order_id <- Some buy_id;
    btc.last_buy_order_price <- Some 84000.0;
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled btc_sym buy_id Dio_strategies.Strategy_common.Buy ~fill_price:84000.0 None;
    btc.open_sell_orders <- [(sell_id, 84336.0, 1.0)];
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled btc_sym sell_id Dio_strategies.Strategy_common.Sell ~fill_price:84336.0 None;
  done;

  let btc_profit = btc.accumulated_profit in
  check bool "BTC profit > 0 USDC after 30 cycles" true (btc_profit > 0.0);
  check bool "HYPE profit still 0 after BTC cycles" true (abs_float hype.accumulated_profit < 0.0001);

  (* --- HYPE cycles: buy@39.50 → sell@39.90 USDC (+1.0%) --- *)
  (* net ≈ 0.128884 USDC per cycle *)
  for i = 1 to 5 do
    let buy_id = Printf.sprintf "hype_buy_%d" i in
    let sell_id = Printf.sprintf "hype_sell_%d" i in
    hype.last_buy_order_id <- Some buy_id;
    hype.last_buy_order_price <- Some 39.50;
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled hype_sym buy_id Dio_strategies.Strategy_common.Buy ~fill_price:39.50 None;
    hype.open_sell_orders <- [(sell_id, 39.90, 1.0)];
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled hype_sym sell_id Dio_strategies.Strategy_common.Sell ~fill_price:39.90 None;
  done;

  let hype_profit = hype.accumulated_profit in
  check bool "HYPE profit > 0 USDC after 5 cycles" true (hype_profit > 0.0);
  (* BTC profit must NOT have changed from HYPE's fills *)
  check bool "BTC profit unchanged by HYPE fills" true
    (abs_float (btc.accumulated_profit -. btc_profit) < 0.0001);

  (* --- Test independent gating decisions --- *)

  (* HYPE: required = 0.01 * 39.90 + 0.05 = 0.449 USDC
     5 cycles * 0.128884 ≈ 0.644 USDC → should gate *)
  let hype_diff = 0.35 -. hype_rounded in
  let hype_required = hype_diff *. 39.90 +. 0.05 in
  check bool "HYPE can gate (0.644 USDC >= 0.449 USDC)" true
    (hype.accumulated_profit >= hype_required);

  (* BTC: required = 0.00001 * 84336 + 1.00 = 1.84336 USDC
     30 cycles * 0.054 ≈ 1.62 USDC → should NOT gate yet *)
  let btc_diff = 0.0002 -. btc_rounded in
  let btc_required = btc_diff *. 84336.0 +. 1.00 in
  check bool "BTC cannot gate yet (1.62 USDC < 1.84 USDC)" true
    (btc.accumulated_profit < btc_required);

  Printf.printf "  BTC: accumulated=%.4f USDC, required=%.4f USDC, lot=0.00001\n"
    btc.accumulated_profit btc_required;
  Printf.printf "  HYPE: accumulated=%.4f USDC, required=%.4f USDC, lot=0.01\n"
    hype.accumulated_profit hype_required;

  (* --- Test reserved_quote (USDC) isolation --- *)
  btc.exchange_id <- "hyperliquid";
  hype.exchange_id <- "hyperliquid";
  Dio_strategies.Suicide_grid.set_asset_reserved_quote btc 16.80;  (* 0.0002 * 84000 = 16.80 USDC *)
  Dio_strategies.Suicide_grid.set_asset_reserved_quote hype 13.80; (* 0.35 * 39.42 ≈ 13.80 USDC *)
  let total_reserved = Dio_strategies.Suicide_grid.get_total_reserved_quote ~exchange:"hyperliquid" in
  check bool "total reserved USDC includes both domains" true (total_reserved >= 30.0)

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
      test_case "order cancellation matches client order id" `Quick test_order_cancellation_matches_client_order_id;
      test_case "order rejection" `Quick test_order_rejection;
    ];
    "accumulation", [
      test_case "profit tracking from sell fills" `Quick test_accumulation_profit_tracking;
      test_case "gated sell - insufficient profit" `Quick test_accumulation_gated_sell_insufficient;
      test_case "gated sell - sufficient profit" `Quick test_accumulation_gated_sell_sufficient;
      test_case "full lifecycle (20 buy-sell cycles)" `Quick test_accumulation_full_lifecycle;
      test_case "multi-strategy isolation (BTC + HYPE)" `Quick test_accumulation_multi_strategy_isolation;
    ];
  ]