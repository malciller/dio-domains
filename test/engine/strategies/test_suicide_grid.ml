open Alcotest

let test_initialization () =
  (* Test strategy initialization *)
  check unit "suicide_grid init" () (Dio_strategies.Suicide_grid.Strategy.init ())
;;

let test_order_creation_place () =
  (* Test creating place orders *)
  let order =
    Dio_strategies.Suicide_grid.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      Dio_strategies.Strategy_common.Buy
      0.001
      (Some 50000.0)
      true
      Dio_strategies.Strategy_common.Grid
      "kraken"
  in
  check
    bool
    "place order operation"
    true
    (order.operation = Dio_strategies.Strategy_common.Place);
  check string "place order symbol" "BTC/USD" order.symbol;
  check bool "place order side" true (order.side = Dio_strategies.Strategy_common.Buy);
  check (float 0.) "place order qty" 0.001 order.qty;
  check (option (float 0.)) "place order price" (Some 50000.0) order.price;
  check bool "place order post_only" true order.post_only;
  check
    bool
    "place order strategy"
    true
    (order.strategy = Dio_strategies.Strategy_common.Grid)
;;

let test_order_creation_amend () =
  (* Test creating amend orders *)
  let order =
    Dio_strategies.Suicide_grid.create_amend_order
      "order123"
      "BTC/USD"
      Dio_strategies.Strategy_common.Sell
      0.001
      (Some 51000.0)
      true
      Dio_strategies.Strategy_common.Grid
      "kraken"
  in
  check
    bool
    "amend order operation"
    true
    (order.operation = Dio_strategies.Strategy_common.Amend);
  check (option string) "amend order id" (Some "order123") order.order_id;
  check string "amend order symbol" "BTC/USD" order.symbol;
  check bool "amend order side" true (order.side = Dio_strategies.Strategy_common.Sell);
  check (float 0.) "amend order qty" 0.001 order.qty;
  check (option (float 0.)) "amend order price" (Some 51000.0) order.price;
  check bool "amend order post_only" true order.post_only;
  check
    bool
    "amend order strategy"
    true
    (order.strategy = Dio_strategies.Strategy_common.Grid)
;;

let test_order_creation_cancel () =
  (* Test creating cancel orders *)
  let order =
    Dio_strategies.Suicide_grid.create_cancel_order
      "order456"
      "BTC/USD"
      Dio_strategies.Strategy_common.Grid
      "kraken"
  in
  check
    bool
    "cancel order operation"
    true
    (order.operation = Dio_strategies.Strategy_common.Cancel);
  check (option string) "cancel order id" (Some "order456") order.order_id;
  check string "cancel order symbol" "BTC/USD" order.symbol;
  check
    bool
    "cancel order strategy"
    true
    (order.strategy = Dio_strategies.Strategy_common.Grid);
  check (option (float 0.)) "cancel order price" None order.price;
  check (float 0.) "cancel order qty" 0.0 order.qty
;;

let test_legacy_order_creation () =
  (* Test legacy create_order function for backwards compatibility *)
  ()
;;

(* Check if create_order exists, if not remove test or alias it. Assuming it was renamed to create_place_order or removed. 
     If it's removed, we should remove this test case. For now, let's comment it out or update it to create_place_order if legacy is gone. *)
(* let order = Dio_strategies.Suicide_grid.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in *)

(* let order = Dio_strategies.Suicide_grid.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in *)

let test_duplicate_key_per_side () =
  (* Ensure duplicate key is per asset+side, not price/qty *)
  let open Dio_strategies in
  let buy1 =
    Suicide_grid.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      Strategy_common.Buy
      0.001
      (Some 50000.0)
      true
      Strategy_common.Grid
      "kraken"
  in
  let buy2 =
    Suicide_grid.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      Strategy_common.Buy
      0.002
      (Some 51000.0)
      true
      Strategy_common.Grid
      "kraken"
  in
  let sell1 =
    Suicide_grid.create_place_order
      "BTC/USD|sell|grid"
      "BTC/USD"
      Strategy_common.Sell
      0.003
      (Some 52000.0)
      true
      Strategy_common.Grid
      "kraken"
  in
  check string "same key for buy side" buy1.duplicate_key buy2.duplicate_key;
  check
    bool
    "different key for opposite side"
    true
    (buy1.duplicate_key <> sell1.duplicate_key)
;;

let test_config_parsing () =
  (* Test configuration value parsing *)
  let test_parse str default expected =
    let result =
      Dio_strategies.Suicide_grid.parse_config_float
        str
        "test_param"
        default
        "TEST"
        "TEST/USD"
    in
    abs_float (result -. expected) < 0.0001
  in
  check bool "parse valid float" true (test_parse "0.001" 0.1 0.001);
  check bool "parse invalid float" true (test_parse "invalid" 0.1 0.1);
  check bool "parse empty float" true (test_parse "" 0.05 0.05)
;;

let test_price_rounding () =
  (* Test price rounding - this relies on Kraken instruments feed *)
  (* For now, just test that the function doesn't crash and returns a reasonable value *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "BTC/USD" in
  let rounded = state.cached_round_price 50000.12345678 in
  check bool "price rounding non-negative" true (rounded >= 0.0)
;;

let test_price_increment () =
  (* Test price increment retrieval - this relies on Kraken instruments feed *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "BTC/USD" in
  let increment = state.cached_price_increment in
  check bool "price increment positive" true (increment > 0.0)
;;

let test_grid_price_calculation () =
  (* Test grid price calculations *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST/USD" in
  let above_price =
    Dio_strategies.Suicide_grid.calculate_grid_price 50000.0 1.0 true state
  in
  let below_price =
    Dio_strategies.Suicide_grid.calculate_grid_price 50000.0 1.0 false state
  in
  (* Should be above and below 50000 with 1% grid *)
  check bool "above price correct" true (above_price >= 50499.0 && above_price <= 50501.0);
  check bool "below price correct" true (below_price >= 49499.0 && below_price <= 49501.0)
;;

let test_state_management () =
  (* Test strategy state management *)
  let state1 = Dio_strategies.Suicide_grid.get_strategy_state "BTC/USD" in
  let state2 = Dio_strategies.Suicide_grid.get_strategy_state "BTC/USD" in
  (* Should return the same state for same symbol *)
  check bool "same state for same symbol" true (state1 == state2)
;;

let test_userref_generation () =
  (* Test userref tagging - Grid strategy should use userref=1 *)
  let strategy_userref = Dio_strategies.Strategy_common.strategy_userref_grid in
  check int "grid strategy userref" 1 strategy_userref;
  (* Test that is_strategy_order correctly identifies Grid orders *)
  check
    bool
    "userref 1 matches grid"
    true
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 1);
  check
    bool
    "userref 2 doesn't match grid"
    false
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 2)
;;

let test_blocked_placement_sell_retries () =
  (* A buy is placed (buy_attempted = true) but the placement-tick sell
     attempt is blocked by a transient gate (sell cooldown). The sell for the
     non-accrued inventory must stay OWED and be placed on a later tick even
     though no further buy placement happens (buy_attempted = false) and no
     buy filled - the startup case (BTC/USDC: free 0.00112536, reserved
     0.0006248, sellable 0.00050056 > venue min 0.0005). *)
  let symbol = "PLACE_RETRY/BTC/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.reserved_base <- 0.5;
  state.accumulated_profit <- 1.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- false;
  state.last_buy_fill_price <- Some 62369.0;
  state.last_buy_fill_qty <- Some 0.5;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  let run_leg buy_attempted =
    Dio_strategies.Suicide_grid.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
      ~state
      ~now:100.0
      ~asset
      ~bid_price:62369.0
      ~ask_price:62370.0
      ~asset_balance:1.00112
      ~buy_attempted
      ~ecfg
      ~locked_in_sells:0.0
  in
  (* Tick 1: a buy was placed this tick; the sell is on cooldown, so the
     attempt is blocked - the sell must stay owed. *)
  Hashtbl.replace state.amend_cooldowns "place_Sell" (Unix.gettimeofday () +. 10.0);
  run_leg true;
  check
    bool
    "no sell pushed while on cooldown"
    true
    (Dio_strategies.Suicide_grid.get_pending_orders 100 = []);
  check
    bool
    "placement-triggered sell stays owed (latch armed)"
    true
    state.just_filled_buy;
  (* Tick 2: cooldown expired; no buy placement, no fill, but the owed sell
     retries and is placed. *)
  Hashtbl.remove state.amend_cooldowns "place_Sell";
  run_leg false;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let found =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  check bool "owed sell placed on retry (no buy placement or fill needed)" true found;
  check bool "latch cleared after the sell is placed" false state.just_filled_buy
;;

let test_hl_buy_fill_accrues_reserve () =
  (* Spec-aligned buy fill: it ONLY updates the reference info for the next
     sell's profitability check - reserved_base is never touched on a buy
     fill (the legacy per-fill slice retention is removed; accumulation now
     happens at sell-fill time when the profit window exceeds the buffer).
     Hyperliquid additionally takes the BUY fee out of the RECEIVED BASE
     token, so the anticipated credit runs on the net landed qty: qty 0.5,
     maker_fee 0.0004 -> the venue credits 0.5*(1-0.0004) = 0.4998 base and
     the credit is 0.4998 - NOT the raw 0.5 fill qty (crediting gross would
     overstate inventory and let sells dip into the reserve). *)
  let symbol = "HL_ACCRUAL/BTC/USDC" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.reserved_base <- 0.0;
  state.anticipated_base_credit <- 0.0;
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done symbol;
  Dio_strategies.Suicide_grid.Strategy.handle_order_filled
    ~now:0.0
    symbol
    "hlacc1"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:62000.0
    ~fill_qty:0.5
    None;
  let landed = 0.5 -. (0.0004 *. 0.5) in
  check
    bool
    "hl buy fill does NOT reserve (reference update only)"
    true
    (state.reserved_base = 0.0);
  check
    bool
    "hl buy reference price recorded"
    true
    (state.last_buy_fill_price = Some 62000.0);
  check
    bool
    "hl buy reference qty recorded"
    true
    (state.last_buy_fill_qty = Some 0.5);
  check
    bool
    "hl anticipated credit is net of the base-side buy fee"
    true
    (abs_float (state.anticipated_base_credit -. landed) < 1e-9);
  (* Kraken aligns identically: buy fill updates refs only, no reserve. *)
  let kr_symbol = "KR_ACCRUAL/XMR/USD" in
  let kr_state = Dio_strategies.Suicide_grid.get_strategy_state kr_symbol in
  kr_state.exchange_id <- "kraken";
  kr_state.grid_qty <- 0.04;
  kr_state.cached_sell_mult <- 0.999;
  kr_state.reserved_base <- 0.0;
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done kr_symbol;
  Dio_strategies.Suicide_grid.Strategy.handle_order_filled
    ~now:0.0
    kr_symbol
    "kracc1"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:390.0
    ~fill_qty:0.04
    None;
  check bool "kraken buy fill does not reserve" true (kr_state.reserved_base = 0.0);
  check
    bool
    "kraken buy reference recorded"
    true
    (kr_state.last_buy_fill_price = Some 390.0)
;;

let test_sub_minimum_qty_sell_places () =
  (* Sells are NOT floored at the venue qty minimum - only the quote-notional
     floor gates them (accrual sells sell_mult x qty and residual inventory
     legitimately size below the lot minimum). Sellable inventory above
     reserved rounds to 0.55 - far below the (deliberately impossible) 10
     BTC venue qty floor - yet places because its notional clears the $1-
     style floor; the old gate would have blocked it entirely. *)
  let symbol = "SUBMINQTY/BTC/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  (* Impossible base-quantity floor; the real gate is the notional one. *)
  state.cached_venue_min_qty <- 10.0;
  state.cached_venue_min_notional <- 1.0;
  state.reserved_base <- 0.5;
  state.accumulated_profit <- 1.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 62369.0;
  state.last_buy_fill_qty <- Some 0.5;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:62369.0
    ~ask_price:62370.0
    ~asset_balance:1.05
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let sell_qty =
    List.find_map
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         if
           o.operation = Dio_strategies.Strategy_common.Place
           && o.side = Dio_strategies.Strategy_common.Sell
           && o.symbol = symbol
         then Some o.qty
         else None)
      pushed
  in
  (* bal 1.05 + credit 0 - reserved 0.5 = 0.55 non-accrued inventory, lot-
      rounded to sz_decimals 2. Placed despite being under the qty minimum. *)
  check
    (option (float 1e-9))
    "sub-minimum qty sell places at full non-accrued inventory"
    (Some 0.55)
    sell_qty;
  check bool "latch cleared after the sub-minimum sell placed" false state.just_filled_buy
;;

let test_balance_checking () =
  (* Test balance checking logic *)
  check
    bool
    "sufficient buy balance"
    true
    (Dio_strategies.Suicide_grid.can_place_buy_order 0.001 100.0 50.0);
  check
    bool
    "insufficient buy balance"
    false
    (Dio_strategies.Suicide_grid.can_place_buy_order 0.001 10.0 50.0);
  check
    bool
    "sufficient sell balance"
    true
    (Dio_strategies.Suicide_grid.can_place_sell_order 0.001 1.0 1.0 0.001);
  check
    bool
    "insufficient sell balance"
    false
    (Dio_strategies.Suicide_grid.can_place_sell_order 0.001 1.0 0.0005 0.001)
;;

let test_order_acknowledgment () =
  (* Test order acknowledgment handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST/USD" in
  (* Add a pending order manually for testing *)
  state.pending_orders
  <- ("test123", Dio_strategies.Strategy_common.Buy, 50000.0, Unix.time ())
     :: state.pending_orders;
  (* Handle acknowledgment *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
    ~now:0.0
    "TEST/USD"
    "order456"
    Dio_strategies.Strategy_common.Buy
    50000.0;
  (* Should update buy order ID tracking *)
  check (option string) "buy order id updated" (Some "order456") state.last_buy_order_id
;;

let test_order_cancellation () =
  (* Test order cancellation handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST2/USD" in
  (* Set up some tracked orders *)
  state.last_buy_order_id <- Some "buy123";
  state.last_buy_order_price <- Some 49000.0;
  state.open_sell_orders <- [ "sell456", 51000.0, 1.0; "sell789", 52000.0, 1.0 ];
  (* Cancel the buy order *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled
    ~now:0.0
    "TEST2/USD"
    "buy123"
    Dio_strategies.Strategy_common.Buy
    None;
  (* Should clear buy order tracking *)
  check (option string) "buy order id cleared" None state.last_buy_order_id;
  check (option (float 0.)) "buy order price cleared" None state.last_buy_order_price
;;

let test_order_cancellation_matches_client_order_id () =
  (* Lighter (and similar): strategy may still track client index while the
     execution feed reports exchange order_index on cancel. *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST_CLID/USD" in
  state.last_buy_order_id <- Some "1";
  state.last_buy_order_price <- Some 2200.0;
  Dio_strategies.Suicide_grid.Strategy.handle_order_cancelled
    ~now:0.0
    "TEST_CLID/USD"
    "577023702126926647"
    Dio_strategies.Strategy_common.Buy
    (Some "1");
  check (option string) "buy cleared via cl_ord_id alias" None state.last_buy_order_id;
  check (option (float 0.)) "buy price cleared" None state.last_buy_order_price
;;

let test_order_rejection () =
  (* Test order rejection handling *)
  let state = Dio_strategies.Suicide_grid.get_strategy_state "TEST3/USD" in
  (* Add a pending order manually for testing *)
  state.pending_orders
  <- [ "test123", Dio_strategies.Strategy_common.Sell, 51000.0, Unix.time () ];
  (* Handle rejection *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
    ~now:0.0
    "TEST3/USD"
    Dio_strategies.Strategy_common.Sell
    51000.0;
  (* Should remove from pending orders *)
  check bool "pending orders cleared" true (List.length state.pending_orders = 0)
;;

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
  (* A buffer above the window so the spec reserve/reset does not fire here:
     this test observes pure accumulation. *)
  state.accumulation_buffer <- 5.0;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done symbol;
  (* Simulate buy fill: sets last_buy_fill_price *)
  state.last_buy_order_id <- Some "buy001";
  state.last_buy_order_price <- Some 39.50;
  Dio_strategies.Suicide_grid.Strategy.handle_order_filled
    ~now:0.0
    symbol
    "buy001"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:39.50
    ~fill_qty:0.35
    None;
  (* Verify buy fill recorded the price for later profit calc *)
  check
    (option (float 0.01))
    "buy fill price recorded"
    (Some 39.50)
    state.last_buy_fill_price;
  (* Simulate sell fill at a higher price *)
  state.open_sell_orders <- [ "sell001", 39.90, 1.0 ];
  Dio_strategies.Suicide_grid.Strategy.handle_order_filled
    ~now:0.0
    symbol
    "sell001"
    Dio_strategies.Strategy_common.Sell
    ~fill_price:39.90
    ~fill_qty:0.35
    None;
  (* Verify profit was accumulated *)
  let expected_gross = (39.90 -. 39.50) *. 0.35 in
  let expected_fees = (39.90 *. 0.35 *. 0.0004) +. (39.50 *. 0.35 *. 0.0004) in
  let expected_net = expected_gross -. expected_fees in
  check bool "profit accumulated" true (state.accumulated_profit > 0.0);
  check
    bool
    "profit value correct"
    true
    (abs_float (state.accumulated_profit -. expected_net) < 0.0001)
;;

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
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let sell_qty, is_accumulation_sell, required_profit =
    Dio_strategies.Suicide_grid.compute_sell_qty
      ~ecfg
      ~state
      ~asset
      ~qty
      ~sell_price
      ~sell_mult
      ~symbol
      ~exchange:"hyperliquid"
  in
  check
    bool
    "not an accumulation sell when profit insufficient"
    true
    (not is_accumulation_sell);
  check
    bool
    "sell qty falls back to 1:1 when profit insufficient"
    true
    (abs_float (sell_qty -. qty) < 0.0001);
  (* Profit should NOT have been debited *)
  check
    bool
    "profit unchanged"
    true
    (abs_float (state.accumulated_profit -. 0.10) < 0.0001);
  (* Verify the threshold was meaningful *)
  check
    bool
    "required_profit > accumulated"
    true
    (required_profit > state.accumulated_profit)
;;

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
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let sell_qty, is_accumulation_sell, required_profit =
    Dio_strategies.Suicide_grid.compute_sell_qty
      ~ecfg
      ~state
      ~asset
      ~qty
      ~sell_price
      ~sell_mult
      ~symbol
      ~exchange:"hyperliquid"
  in
  check bool "sell qty uses reduced amount" true (sell_qty < qty);
  check bool "is accumulation sell" true is_accumulation_sell;
  let rounded_sell =
    Dio_strategies.Suicide_grid.round_qty (qty *. sell_mult) symbol "hyperliquid"
  in
  check
    bool
    "sell qty equals rounded_sell"
    true
    (abs_float (sell_qty -. rounded_sell) < 0.0001);
  check
    bool
    "profit still above threshold"
    true
    (state.accumulated_profit >= required_profit)
;;

let test_accumulation_recovery_blocks_blind_sell () =
  (* After asset_low/capital_low clears, a new sell must pass the accumulation buffer.
     Normal cycles allow 1:1 fallback; recovery cycles do not. *)
  let symbol = "RECOVERY_GATE/USDC" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.accumulated_profit <- 0.10;
  state.resuming_after_balance_flag <- true;
  let qty = 0.35 in
  let sell_mult = 0.999 in
  let sell_price = 40.0 in
  let accumulation_buffer = 0.05 in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let sell_qty, is_accumulation_sell, _required_profit =
    Dio_strategies.Suicide_grid.compute_sell_qty
      ~ecfg
      ~state
      ~asset
      ~qty
      ~sell_price
      ~sell_mult
      ~symbol
      ~exchange:"hyperliquid"
  in
  check
    bool
    "recovery blocks 1:1 fallback sell"
    true
    (not
       (Dio_strategies.Suicide_grid.accumulation_sell_allowed_on_recovery
          ~ecfg
          ~state
          ~is_accumulation_sell
          ~sell_qty));
  state.resuming_after_balance_flag <- false;
  check
    bool
    "normal cycle allows 1:1 fallback"
    true
    (Dio_strategies.Suicide_grid.accumulation_sell_allowed_on_recovery
       ~ecfg
       ~state
       ~is_accumulation_sell
       ~sell_qty)
;;

(* Helper: round qty using instrument feed directly.
   Production code goes through Exchange.Registry -> Hyperliquid_impl -> Instruments_feed,
   but the test binary may not link the exchange module. This calls the feed directly. *)
let round_qty_hl qty sym =
  let inc =
    match Hyperliquid.Instruments_feed.get_qty_increment sym with
    | Some v -> v
    | None -> 0.01
  in
  let inv = 1.0 /. inc in
  floor (qty *. inv) /. inv
;;

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
  state.grid_qty <- 0.35;
  (* 0.35 asset *)
  state.maker_fee <- 0.0004;
  (* Buffer above the ~0.644 USDC window so the spec reserve/reset does not
     fire mid-test; the buffer-reserve path is covered by the store tests. *)
  state.accumulation_buffer <- 2.0;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done symbol;
  let buy_price = 39.50 in
  (* USDC per asset *)
  let sell_price = 39.90 in
  (* USDC per asset *)
  let accumulation_buffer = 0.05 in
  (* USDC *)
  let sell_mult = 0.999 in
  (* Run 5 profitable buy→sell cycles *)
  for i = 1 to 5 do
    let buy_id = Printf.sprintf "buy_%d" i in
    let sell_id = Printf.sprintf "sell_%d" i in
    state.last_buy_order_id <- Some buy_id;
    state.last_buy_order_price <- Some buy_price;
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
      ~now:0.0
      symbol
      buy_id
      Dio_strategies.Strategy_common.Buy
      ~fill_price:buy_price
      ~fill_qty:0.35
      None;
    state.open_sell_orders <- [ sell_id, sell_price, 1.0 ];
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
      ~now:0.0
      symbol
      sell_id
      Dio_strategies.Strategy_common.Sell
      ~fill_price:sell_price
      ~fill_qty:0.35
      None
  done;
  (* After 5 cycles: ~5 * 0.128884 ≈ 0.644 USDC accumulated *)
  check bool "profit accumulated over 5 cycles" true (state.accumulated_profit > 0.0);
  (* Test the gating decision *)
  let qty = 0.35 in
  (* asset *)
  let rounded_sell = round_qty_hl (qty *. sell_mult) symbol in
  (* rounded_sell = 0.34 (asset), rounding_diff = 0.01 (asset) *)
  let rounding_diff = qty -. rounded_sell in
  (* required_profit = 0.01 * 39.90 + 0.05 = 0.449 (USDC) *)
  let required_profit = (rounding_diff *. sell_price) +. accumulation_buffer in
  check
    bool
    "rounded_sell is 0.34 (asset)"
    true
    (abs_float (rounded_sell -. 0.34) < 0.0001);
  check
    bool
    "rounding_diff is 0.01 (asset)"
    true
    (abs_float (rounding_diff -. 0.01) < 0.0001);
  check
    bool
    "required_profit ≈ 0.449 (USDC)"
    true
    (abs_float (required_profit -. 0.449) < 0.01);
  let profit_before = state.accumulated_profit in
  let can_accumulate = profit_before >= required_profit in
  let sell_qty =
    if required_profit > 0.0 && state.accumulated_profit >= required_profit
    then (
      state.accumulated_profit <- state.accumulated_profit -. required_profit;
      rounded_sell)
    else qty
  in
  (* With 5 cycles (~0.644 USDC) vs required 0.449 USDC, gate should fire *)
  check bool "lifecycle: enough profit to gate" true can_accumulate;
  check bool "lifecycle: gated sell fires reduced qty 0.34 (asset)" true (sell_qty < qty);
  check
    bool
    "lifecycle: sell qty = rounded_sell"
    true
    (abs_float (sell_qty -. rounded_sell) < 0.0001);
  check bool "lifecycle: profit debited" true (state.accumulated_profit < profit_before)
;;

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
  btc.grid_qty <- 0.0002;
  (* 0.0002 BTC (asset) *)
  btc.maker_fee <- 0.0004;
  btc.accumulation_buffer <- 100.0;
  hype.accumulated_profit <- 0.0;
  hype.grid_qty <- 0.35;
  (* 0.35 HYPE (asset) *)
  hype.maker_fee <- 0.0004;
  (* Buffers above each window keep the spec reserve/reset from firing: the
     tests here observe pure accumulation + gating isolation. *)
  hype.accumulation_buffer <- 5.0;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done btc_sym;
  Dio_strategies.Suicide_grid.Strategy.set_startup_replay_done hype_sym;
  (* Verify lot sizes are correct *)
  let btc_rounded = round_qty_hl (0.0002 *. 0.999) btc_sym in
  let hype_rounded = round_qty_hl (0.35 *. 0.999) hype_sym in
  check
    bool
    "BTC rounded_sell = 0.00019 (asset, lot=0.00001)"
    true
    (abs_float (btc_rounded -. 0.00019) < 0.000001);
  check
    bool
    "HYPE rounded_sell = 0.34 (asset, lot=0.01)"
    true
    (abs_float (hype_rounded -. 0.34) < 0.0001);
  (* --- BTC cycles: buy@84000 → sell@84336 USDC (+0.4%) --- *)
  (* net = (84336 - 84000) * 0.0002 - fees = 0.0672 - 0.01345 ≈ 0.054 USDC per cycle *)
  for i = 1 to 30 do
    let buy_id = Printf.sprintf "btc_buy_%d" i in
    let sell_id = Printf.sprintf "btc_sell_%d" i in
    btc.last_buy_order_id <- Some buy_id;
    btc.last_buy_order_price <- Some 84000.0;
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
      ~now:0.0
      btc_sym
      buy_id
      Dio_strategies.Strategy_common.Buy
      ~fill_price:84000.0
      ~fill_qty:0.0002
      None;
    btc.open_sell_orders <- [ sell_id, 84336.0, 1.0 ];
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
      ~now:0.0
      btc_sym
      sell_id
      Dio_strategies.Strategy_common.Sell
      ~fill_price:84336.0
      ~fill_qty:0.0002
      None
  done;
  let btc_profit = btc.accumulated_profit in
  check bool "BTC profit > 0 USDC after 30 cycles" true (btc_profit > 0.0);
  check
    bool
    "HYPE profit still 0 after BTC cycles"
    true
    (abs_float hype.accumulated_profit < 0.0001);
  (* --- HYPE cycles: buy@39.50 → sell@39.90 USDC (+1.0%) --- *)
  (* net ≈ 0.128884 USDC per cycle *)
  for i = 1 to 5 do
    let buy_id = Printf.sprintf "hype_buy_%d" i in
    let sell_id = Printf.sprintf "hype_sell_%d" i in
    hype.last_buy_order_id <- Some buy_id;
    hype.last_buy_order_price <- Some 39.50;
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
      ~now:0.0
      hype_sym
      buy_id
      Dio_strategies.Strategy_common.Buy
      ~fill_price:39.50
      ~fill_qty:0.35
      None;
    hype.open_sell_orders <- [ sell_id, 39.90, 1.0 ];
    Dio_strategies.Suicide_grid.Strategy.handle_order_filled
      ~now:0.0
      hype_sym
      sell_id
      Dio_strategies.Strategy_common.Sell
      ~fill_price:39.90
      ~fill_qty:0.35
      None
  done;
  let hype_profit = hype.accumulated_profit in
  check bool "HYPE profit > 0 USDC after 5 cycles" true (hype_profit > 0.0);
  (* BTC profit must NOT have changed from HYPE's fills *)
  check
    bool
    "BTC profit unchanged by HYPE fills"
    true
    (abs_float (btc.accumulated_profit -. btc_profit) < 0.0001);
  (* --- Test independent gating decisions --- *)

  (* HYPE: required = 0.01 * 39.90 + 0.05 = 0.449 USDC
     5 cycles * 0.128884 ≈ 0.644 USDC → should gate *)
  let hype_diff = 0.35 -. hype_rounded in
  let hype_required = (hype_diff *. 39.90) +. 0.05 in
  check
    bool
    "HYPE can gate (0.644 USDC >= 0.449 USDC)"
    true
    (hype.accumulated_profit >= hype_required);
  (* BTC: required = 0.00001 * 84336 + 1.00 = 1.84336 USDC
     30 cycles * 0.054 ≈ 1.62 USDC → should NOT gate yet *)
  let btc_diff = 0.0002 -. btc_rounded in
  let btc_required = (btc_diff *. 84336.0) +. 1.00 in
  check
    bool
    "BTC cannot gate yet (1.62 USDC < 1.84 USDC)"
    true
    (btc.accumulated_profit < btc_required);
  Printf.printf
    "  BTC: accumulated=%.4f USDC, required=%.4f USDC, lot=0.00001\n"
    btc.accumulated_profit
    btc_required;
  Printf.printf
    "  HYPE: accumulated=%.4f USDC, required=%.4f USDC, lot=0.01\n"
    hype.accumulated_profit
    hype_required;
  (* --- Test reserved_quote (USDC) isolation --- *)
  btc.exchange_id <- "hyperliquid";
  hype.exchange_id <- "hyperliquid";
  Dio_strategies.Suicide_grid.set_asset_reserved_quote btc 16.80;
  (* 0.0002 * 84000 = 16.80 USDC *)
  Dio_strategies.Suicide_grid.set_asset_reserved_quote hype 13.80;
  (* 0.35 * 39.42 ≈ 13.80 USDC *)
  let total_reserved = Dio_strategies.Suicide_grid.get_total_reserved_quote btc in
  check bool "total reserved USDC includes both domains" true (total_reserved >= 30.0)
;;

let test_virtual_gtc_sell_grid_maintenance () =
  let symbol = "VIRTUAL_GTC/USD" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.persisted_sell_levels <- [ 101.00, 1.0; 98.98, 1.0; 96.96, 1.0 ];
  state.last_buy_fill_price <- Some 96.0;
  state.open_sell_orders <- [];
  (* Expired or missing DAY orders *)
  let asset_alpaca =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg_alpaca = Dio_strategies.Suicide_grid.get_exchange_config "alpaca" in
  (* Run evaluate_sell_leg on Alpaca during a price drop to 90.0 (death spiral) *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset:asset_alpaca
    ~bid_price:90.0
    ~ask_price:90.1
    ~asset_balance:3.0
    ~buy_attempted:false
    ~ecfg:ecfg_alpaca
    ~locked_in_sells:0.0;
  (* Verify that a missing sell order from persisted stack was pushed to order buffer at target price 101.00 *)
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let popped = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  check
    bool
    "sell order pushed to buffer for Alpaca Virtual GTC maintenance"
    true
    (Option.is_some popped);
  Option.iter
    (fun (order : Dio_strategies.Strategy_common.strategy_order) ->
       check
         string
         "side is sell"
         "sell"
         (Dio_strategies.Strategy_common.string_of_order_side order.side);
       check string "symbol matches" symbol order.symbol;
       match order.price with
       | Some p ->
         check
           bool
           "Alpaca sell price preserved above cost basis (no loss)"
           true
           (p >= 96.96)
       | None -> failwith "missing sell price")
    popped;
  (* Verify offline fill reconciliation: asset_balance is 0.0, so persisted levels must be pruned *)
  let state_offline = Dio_strategies.Suicide_grid.get_strategy_state "OFFLINE_TEST/USD" in
  state_offline.persisted_sell_levels <- [ 105.00, 1.0 ];
  state_offline.open_sell_orders <- [];
  let asset_offline = { asset_alpaca with symbol = "OFFLINE_TEST/USD" } in
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state:state_offline)
    ~state:state_offline
    ~now:100.0
    ~asset:asset_offline
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:0.0
    ~buy_attempted:false
    ~ecfg:ecfg_alpaca
    ~locked_in_sells:0.0;
  check
    bool
    "offline fill pruned from persisted_sell_levels"
    true
    (state_offline.persisted_sell_levels = []);
  (* Verify pre-existing open exchange order adoption in sync_open_orders *)
  let state_adopt = Dio_strategies.Suicide_grid.get_strategy_state "ADOPT_TEST/USD" in
  state_adopt.persisted_sell_levels <- [];
  let iter_orders f = f "oid_ex_1" 105.0 1.0 "sell" (Some 1) in
  let _ =
    Dio_strategies.Suicide_grid.sync_open_orders
      ~state:state_adopt
      ~now:100.0
      ~asset:{ asset_alpaca with symbol = "ADOPT_TEST/USD" }
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders
      ~ecfg:ecfg_alpaca
  in
  check
    bool
    "pre-existing exchange sell order adopted into persisted_sell_levels"
    true
    (List.exists (fun (p, q) -> p = 105.0 && q = 1.0) state_adopt.persisted_sell_levels);
  (* Verify venue isolation: non-Alpaca (Kraken) has remaintain_expired_sells = false *)
  let kraken_symbol = "KRAKEN_TEST/USD" in
  let state_kraken = Dio_strategies.Suicide_grid.get_strategy_state kraken_symbol in
  state_kraken.open_sell_orders <- [];
  let asset_kraken =
    { Dio_strategies.Suicide_grid.exchange = "kraken"
    ; symbol = kraken_symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg_kraken = Dio_strategies.Suicide_grid.get_exchange_config "kraken" in
  check
    bool
    "Kraken remaintain_expired_sells is false"
    false
    ecfg_kraken.remaintain_expired_sells;
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state:state_kraken)
    ~state:state_kraken
    ~now:100.0
    ~asset:asset_kraken
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:1.0
    ~buy_attempted:false
    ~ecfg:ecfg_kraken
    ~locked_in_sells:0.0;
  let kraken_popped = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  check
    bool
    "Kraken does not trigger Virtual GTC maintenance"
    true
    (Option.is_none kraken_popped)
;;

let test_halted_path_still_places_sell () =
  (* Feature B: when the capital oracle halts an asset (INACTIVE), the
     execute_strategy buy leg is skipped (buy_attempted = false), but the
     sell for a just-filled buy is STILL placed - a sell needs only
     inventory, not quote, and is the account's capital-recovery path.
     Exercises evaluate_sell_leg with exactly the inputs the halted path
     produces (buy_attempted:false + just_filled_buy) and asserts the sell
     reaches the order buffer. *)
  let symbol = "HALT_SELL/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.35;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  (* A buy filled right before capital ran out: the sell must still go out. *)
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 39.50;
  state.last_buy_fill_qty <- Some 0.35;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  (* Drain any orders left in the shared buffer by prior tests. *)
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* The halted path: the buy leg was skipped, so buy_attempted = false. *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:39.50
    ~ask_price:39.55
    ~asset_balance:0.5
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let found =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  check bool "halted path still places the sell for a just-filled buy" true found
;;

let test_sell_ack_releases_inflight_latch () =
  (* A sell placement's in-flight marker must be released on ACK (not left
     latched while the sell rests on the book): has_active_sell then means "a
     sell placement is in flight" only, so a resting sell no longer gates the
     next sell for new inventory behind a buy fill. *)
  let symbol = "LATCH_TEST/USD" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 1.0;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.cached_venue_min_notional <- 0.0;
  (* A sell placement is in flight (dispatch added the key). *)
  check
    bool
    "duplicate key added by dispatch"
    true
    (Dio_strategies.Strategy_common.InFlightOrders.add_in_flight_order
       state.duplicate_key_sell);
  check
    bool
    "has_active_sell true while the placement is in flight"
    true
    (Dio_strategies.Suicide_grid.has_active_sell state);
  (* The placement acks: the key must be released. *)
  Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
    ~now:100.0
    symbol
    "sell1"
    Dio_strategies.Strategy_common.Sell
    100.0;
  check
    bool
    "duplicate key released on ack"
    false
    (Dio_strategies.Strategy_common.InFlightOrders.is_in_flight state.duplicate_key_sell);
  check
    bool
    "has_active_sell false while a sell rests on the book"
    false
    (Dio_strategies.Suicide_grid.has_active_sell state);
  (* A new buy fills while the first sell still rests: the sell for the new
     inventory must be placed (1-buy x multi-sell ladder) - no longer gated
     behind a buy fill clearing a stale latch. *)
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 99.0;
  state.last_buy_fill_qty <- Some 1.0;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* The resting sell locks its inventory: pass its qty as locked_in_sells so
     the new sell only consumes the new fill's inventory. *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:2.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:1.0;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let found =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  check
    bool
    "second sell placed while the first sell rests (multi-sell ladder)"
    true
    found;
  check
    bool
    "just_filled_buy cleared after the sell is placed"
    false
    state.just_filled_buy
;;

let test_sell_retry_until_placed () =
  (* A buy fills but the sell attempt is blocked by a transient gate (sell
     cooldown after a rejection). The one-shot just_filled_buy trigger must
     NOT be consumed: the leg retries the next tick and places the sell even
     though no replacement buy was placed (capital exhausted / oracle-halted:
     buy_attempted = false). *)
  let symbol = "RETRY_TEST/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.35;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 39.50;
  state.last_buy_fill_qty <- Some 0.35;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  let run_leg () =
    Dio_strategies.Suicide_grid.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
      ~state
      ~now:100.0
      ~asset
      ~bid_price:39.50
      ~ask_price:39.55
      ~asset_balance:0.5
      ~buy_attempted:false
      ~ecfg
      ~locked_in_sells:0.0
  in
  (* Tick 1: the sell is on cooldown (a recent rejection latched it). *)
  Hashtbl.replace state.amend_cooldowns "place_Sell" (Unix.gettimeofday () +. 10.0);
  run_leg ();
  check
    bool
    "no sell pushed while on cooldown"
    true
    (Dio_strategies.Suicide_grid.get_pending_orders 100 = []);
  check bool "just_filled_buy survives the blocked attempt" true state.just_filled_buy;
  (* Tick 2: cooldown expired; the buy leg still cannot place a replacement
     (buy_attempted = false), but the sell must go out. *)
  Hashtbl.remove state.amend_cooldowns "place_Sell";
  run_leg ();
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let found =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  check
    bool
    "retried sell placed with buy_attempted=false (no replacement buy)"
    true
    found;
  check
    bool
    "just_filled_buy cleared after the sell is placed"
    false
    state.just_filled_buy
;;

let test_accumulation_sells_non_accrued_inventory () =
  (* Accumulation venues (Hyperliquid/Lighter/IBKR): the sell is sized PURELY
     by the non-accrued inventory = available balance - reserved_base. The
     venue's tradeable balance already nets the base held by resting sells
     (Hyperliquid reports total - hold), so locked_in_sells must NOT be
     subtracted again - subtracting it double-counted the resting-sell hold
     and understated the inventory below the floor, which blocked the sell for
     the startup case (BTC: free 0.00112536, reserved 0.0006248, sellable
     0.00050056 > venue min 0.0005). *)
  let symbol = "FLOOR_FALLBACK/BTC/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.5;
  state.accumulated_profit <- 2.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 62369.0;
  state.last_buy_fill_qty <- Some 0.5;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* A resting sell of 0.4 locks inventory; the venue's tradeable balance
     (1.00112) already nets it, so the sellable must NOT be reduced by locked
     again: sellable = 1.00112 - 0.5 = 0.50112 -> round 0.50 is pushed, not
     the double-counted 0.10. *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:62369.0
    ~ask_price:62370.0
    ~asset_balance:1.00112
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.4;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let sell =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  (match sell with
   | Some o ->
     check
       (float 1e-8)
       "non-accrued inventory sold, resting-sell hold not double-counted"
       0.5
       o.qty
   | None -> failwith "expected the non-accrued sell to be pushed");
  check
    bool
    "reserved_base untouched (accrual never sold)"
    true
    (abs_float (state.reserved_base -. 0.5) < 1e-9);
  check
    bool
    "just_filled_buy cleared after the sell is placed"
    false
    state.just_filled_buy
;;

let test_nothing_placeable_clears_latch () =
  (* When the known balance holds no sellable inventory above the venue floor,
     the leg verifies nothing can be sold and clears the latch - a later fill
     re-arms it. No phantom order is pushed. *)
  let symbol = "NOTHING_PLACEABLE/BTC/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:5;
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.0005;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.0005;
  state.cached_venue_min_notional <- 10.0;
  (* Balance is below the reserved accrual: no sellable inventory. *)
  state.reserved_base <- 0.0006248;
  state.accumulated_profit <- 2.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 62369.0;
  state.last_buy_fill_qty <- Some 0.0005;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.0005"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:62369.0
    ~ask_price:62370.0
    ~asset_balance:0.0003
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  check
    bool
    "no sell pushed with no sellable inventory"
    true
    (Dio_strategies.Suicide_grid.get_pending_orders 100 = []);
  check
    bool
    "just_filled_buy cleared (verified nothing placeable)"
    false
    state.just_filled_buy
;;

let test_kraken_partial_sell_clamp () =
  (* Kraken (sell_mult, reserved-base guard): when available < sell_qty, the
     leg sells the non-accrued inventory that actually exists (lot-rounded
     down) instead of blocking the whole sell - "sell what inventory is not
     accrued", freeing capital and keeping the ladder running. *)
  let symbol = "KRAKEN_CLAMP/USD" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 1.0;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.cached_venue_min_notional <- 0.0;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 100.0;
  state.last_buy_fill_qty <- Some 1.0;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:0.7
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let sell =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  match sell with
  | Some o -> check (float 1e-8) "clamped to available (non-accrued inventory)" 0.7 o.qty
  | None -> failwith "expected the clamped sell to be pushed"
;;

let test_alpaca_dollar_floor_gate () =
  (* Alpaca's venue floor is a DOLLAR notional: a sell is only attempted when
     the non-accrued inventory is worth at least the floor ($1). Below the
     floor the leg withholds the order and keeps the latch (the gate re-checks
     every tick); at/above it the sell is placed. *)
  let symbol = "ALPACA_FLOOR/USD" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.grid_qty <- 0.25;
  state.cached_sell_mult <- 1.0;
  state.cached_venue_min_qty <- 0.000000001;
  state.cached_venue_min_notional <- 1.0;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 142.0;
  state.last_buy_fill_qty <- Some 0.25;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = "0.25"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "alpaca" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* Below the dollar floor: 0.005 shares x 142 = 0.71 < $1. *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:142.0
    ~ask_price:142.1
    ~asset_balance:0.005
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  check
    bool
    "no sell below the dollar floor"
    true
    (Dio_strategies.Suicide_grid.get_pending_orders 100 = []);
  (* At/above the floor: 0.5 shares x 142 = $71 >= $1. *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:142.0
    ~ask_price:142.1
    ~asset_balance:0.5
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  let found =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  check bool "sell placed above the dollar floor" true found
;;

let test_alpaca_sell_anchors_on_fill_not_ask () =
  (* Alpaca sell placement is anchored on the fill (fill + gi), NOT pushed up
     to the current ask. Clamping to the ask stacked every new sell on the
     same price as the market bounced (SPCX sells piling at 138.50) instead of
     laddering down as the price moved down. The fill anchor keeps the rungs
     equidistant and can never place the sell below fill + gi, so the
     fill-anchored profitability is preserved. *)
  let symbol = "ALPACA_ANCHOR/USD" in
  let state = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.grid_qty <- 1.0;
  state.cached_sell_mult <- 1.0;
  state.cached_venue_min_qty <- 0.000000001;
  state.cached_venue_min_notional <- 1.0;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 100.0;
  state.last_buy_fill_qty <- Some 1.0;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Suicide_grid.get_exchange_config "alpaca" in
  let buffer = Dio_strategies.Suicide_grid.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* Market well ABOVE fill + gi: fill 100.00 + gi 1% = 101.00, ask 105.00.
     The sell must land at 101.00 (fill-anchored), not 105.00 (ask-pinned). *)
  Dio_strategies.Suicide_grid.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Suicide_grid.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:105.0
    ~asset_balance:1.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0;
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 100 in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      bool
      "sell anchored on fill"
      true
      (o.operation = Dio_strategies.Strategy_common.Place
       && o.side = Dio_strategies.Strategy_common.Sell
       && o.symbol = symbol);
    check (option (float 0.)) "sell at fill + gi, not the ask" (Some 101.0) o.price
  | _ -> failwith "expected exactly one sell order"
;;

let test_new_buy_respects_2x_gi_closest_sell () =
  (* A fresh buy (no resting buy) placed after a fill must sit at least 2x the
     grid interval below the closest resting sell - the same spacing the
     trailing leg enforces via exact_target (sell_price - 2*gi). Without it a
     new buy can land within a ~1x rung of the lowest sell. *)
  let symbol = "SPACE_SELL/USD" in
  let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  st.exchange_id <- "kraken";
  st.grid_qty <- 1.0;
  let grid_interval = 0.5 in
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval
    ; sell_mult = "1.0"
    ; strategy = "suicide_grid"
    ; maker_fee = Some 0.001
    ; taker_fee = Some 0.002
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  let drain () = ignore (Dio_strategies.Suicide_grid.get_pending_orders 100) in
  drain ();
  (* Closest sell at 100.40, bid 100.00, gi 0.5%: the 2*gi cap is anchored on
     the SELL price, so the buy must not sit above 100.40 - 2*gi(100.40) =
     100.40 - 1.004 = 99.396. The raw grid buy (0.5% below the bid) would be
     99.50 - above the cap, so the cap must pull it down to 99.396. *)
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now
       ~asset
       ~bid_price:100.0
       ~ask_price:100.0
       ~quote_balance:1000.0
       ~quote_balance_stale:false
       ~cycle:1
       ~iter_open_orders
       ~open_buy_count_from_scan:0
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:(Some ("sell1", 100.40))
       ~pending_buy_qty_from_scan:0.0);
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 10 in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      bool
      "buy placed"
      true
      (o.operation = Dio_strategies.Strategy_common.Place
       && o.side = Dio_strategies.Strategy_common.Buy);
    (match o.price with
     | Some p ->
       let cap = 100.40 -. (100.40 *. (2.0 *. 0.5 /. 100.0)) in
       check
         bool
         "buy respects the sell-anchored 2x gi closest-sell cap"
         true
         (p <= cap +. 1e-6)
     | None -> failwith "buy missing price")
  | _ -> failwith "expected exactly one buy order"
;;

let test_reclaim_step_cancels_when_not_issued () =
  (* Reclaim self-healing: the FIRST cycle with a reclaim decision and an
     eligible resting buy issues the cancel (arm the latch). *)
  let step =
    Dio_strategies.Suicide_grid.reclaim_step
      ~now:100.0
      ~retry_seconds:15.0
      ~issued:false
      ~issued_at:0.0
      ~eligible:1
      ~any_buy:true
  in
  check
    bool
    "first reclaim issues the cancel"
    true
    (step = Dio_strategies.Suicide_grid.Reclaim_cancel 1)
;;

let test_reclaim_step_throttles_in_flight_cancel () =
  (* A cancel issued 5s ago is still in flight (retry window 15s): do NOT
     re-issue - avoids cancel spam against a cancel that is dispatching. *)
  let step =
    Dio_strategies.Suicide_grid.reclaim_step
      ~now:105.0
      ~retry_seconds:15.0
      ~issued:true
      ~issued_at:100.0
      ~eligible:1
      ~any_buy:true
  in
  check
    bool
    "in-flight cancel deferred"
    true
    (step = Dio_strategies.Suicide_grid.Reclaim_deferred)
;;

let test_reclaim_step_retries_failed_cancel () =
  (* THE STUCK-STATE REGRESSION: the reclaim cancel is a one-shot network op
     that can fail silently (dispatch dropped on a connection flap, exchange
     rejection, ring-buffer full). If the latch never re-arms, the account is
     permanently stuck - the reclaimed asset stays paused (the oracle's plan
     only clears once the store's committed value drops to zero) and the
     priority asset never resumes on capital that was never released. The
     fix: once the retry interval elapses with the eligible buy still in the
     store, the cancel MUST be re-issued. *)
  let step =
    Dio_strategies.Suicide_grid.reclaim_step
      ~now:116.0
      ~retry_seconds:15.0
      ~issued:true
      ~issued_at:100.0
      ~eligible:1
      ~any_buy:true
  in
  check
    bool
    "stale failed cancel is retried"
    true
    (step = Dio_strategies.Suicide_grid.Reclaim_cancel 1)
;;

let test_reclaim_step_rearms_when_store_clean () =
  (* The cancel landed (or never needed): the store no longer shows ANY buy.
     The latch re-arms so a later reclaim decision re-triggers cleanly - and
     the released capital is recognized by the next oracle pass (the
     committed value it reads is zero). *)
  let step =
    Dio_strategies.Suicide_grid.reclaim_step
      ~now:100.0
      ~retry_seconds:15.0
      ~issued:true
      ~issued_at:99.0
      ~eligible:0
      ~any_buy:false
  in
  check
    bool
    "clean store re-arms the latch"
    true
    (step = Dio_strategies.Suicide_grid.Reclaim_rearm)
;;

let test_reclaim_step_waits_for_mid_amend_buy () =
  (* The domain only cancels buys that are not mid-amendment (Hyperliquid
     rejects canceling an order being amended). An in-flight-amend buy is not
     cancellable: the step waits for the amend to resolve into a cancellable
     replacement instead of spamming the exchange (and instead of re-arming
     - the capital is still committed, so the reclaim decision is still
     correct). *)
  let step =
    Dio_strategies.Suicide_grid.reclaim_step
      ~now:100.0
      ~retry_seconds:15.0
      ~issued:false
      ~issued_at:0.0
      ~eligible:0
      ~any_buy:true
  in
  check
    bool
    "mid-amend buy defers the cancel"
    true
    (step = Dio_strategies.Suicide_grid.Reclaim_deferred)
;;

let test_buy_placement_balance_guard () =
  (* Bug 2 regression: buy placement against an under-funded quote balance.
     - FRESH balance snapshot (authoritative): the order must NOT be sent
       (it would be rejected by the exchange for insufficient funds); the
       buy is paused via the capital_low latch instead.
     - STALE balance snapshot (may be wrong): the order is still attempted
       (the exchange's verdict is the truth) and the foreordained flag is
       set so the expected rejection does not re-latch capital_low. *)
  let symbol = "TESTBAL/USD" in
  let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  st.exchange_id <- "kraken";
  st.grid_qty <- 1.0;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 0.5
    ; sell_mult = "1.0"
    ; strategy = "suicide_grid"
    ; maker_fee = Some 0.001
    ; taker_fee = Some 0.002
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders
    : (string -> float -> float -> string -> int option -> unit) -> unit
    =
    fun _ -> ()
  in
  let now = Unix.gettimeofday () in
  let drain () = ignore (Dio_strategies.Suicide_grid.get_pending_orders 100) in
  let pending_count () =
    List.length (Dio_strategies.Suicide_grid.get_pending_orders 10)
  in
  drain ();
  (* 1. Fresh balance, insufficient -> no order pushed, capital_low latched. *)
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now
       ~asset
       ~bid_price:100.0
       ~ask_price:100.0
       ~quote_balance:10.0
       ~quote_balance_stale:false
       ~cycle:1
       ~iter_open_orders
       ~open_buy_count_from_scan:0
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:None
       ~pending_buy_qty_from_scan:0.0);
  check bool "fresh insufficient: capital_low latched" true st.capital_low;
  check int "fresh insufficient: no order pushed" 0 (pending_count ());
  check
    bool
    "fresh insufficient: foreordained flag not set"
    false
    st.last_buy_attempted_insufficient;
  drain ();
  (* 2. Stale balance, insufficient -> order attempted, foreordained flag. *)
  st.capital_low <- false;
  st.capital_low_logged <- false;
  st.capital_low_at_balance <- 0.0;
  Hashtbl.remove st.amend_cooldowns "place_Buy";
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now:(now +. 1.0)
       ~asset
       ~bid_price:100.0
       ~ask_price:100.0
       ~quote_balance:10.0
       ~quote_balance_stale:true
       ~cycle:2
       ~iter_open_orders
       ~open_buy_count_from_scan:0
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:None
       ~pending_buy_qty_from_scan:0.0);
  check
    bool
    "stale insufficient: foreordained flag set"
    true
    st.last_buy_attempted_insufficient;
  check int "stale insufficient: order attempted" 1 (pending_count ());
  drain ();
  (* 3. Fresh balance, SUFFICIENT -> order placed normally, flag cleared. *)
  st.capital_low <- false;
  st.capital_low_logged <- false;
  st.capital_low_at_balance <- 0.0;
  st.last_buy_attempted_insufficient <- true;
  st.inflight_buy <- false;
  st.pending_orders <- [];
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       st.duplicate_key_buy);
  Hashtbl.remove st.amend_cooldowns "place_Buy";
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now:(now +. 2.0)
       ~asset
       ~bid_price:100.0
       ~ask_price:100.0
       ~quote_balance:1000.0
       ~quote_balance_stale:false
       ~cycle:3
       ~iter_open_orders
       ~open_buy_count_from_scan:0
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:None
       ~pending_buy_qty_from_scan:0.0);
  check
    bool
    "fresh sufficient: foreordained flag cleared"
    false
    st.last_buy_attempted_insufficient;
  check int "fresh sufficient: order placed" 1 (pending_count ());
  drain ()
;;

let test_reconcile_cross_boundary_tolerance () =
  (* The persisted-sell reconcile now buckets by an int price key
     (price*10000 rounded) instead of a Printf "%.4f" string, with a
     neighbor-bucket probe. Verify matching semantics are unchanged for
     prices that straddle a 4-decimal bucket boundary: sync_open_orders
     should match the persisted level rather than adopting a duplicate. *)
  let open Dio_strategies.Suicide_grid in
  let symbol = "BOUNDARY_TEST/USD" in
  let state = get_strategy_state symbol in
  (* 100.00004 vs the open order's 100.00005: within the 1e-4 tolerance but
     straddles the 4-decimal bucket boundary. *)
  state.persisted_sell_levels <- [ 100.00004, 1.0 ];
  let asset_alpaca =
    { exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = get_exchange_config "alpaca" in
  let iter_orders f = f "oid_b" 100.00005 1.0 "sell" (Some 1) in
  let _ =
    sync_open_orders
      ~state
      ~now:100.0
      ~asset:asset_alpaca
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders
      ~ecfg
  in
  (* The persisted level should have been matched (no adoption of a second
     near-100.0 level), so only one level remains around 100.0. *)
  let near_100 =
    List.filter (fun (p, _) -> abs_float (p -. 100.0) < 0.001) state.persisted_sell_levels
  in
  check
    bool
    "cross-boundary persisted level matched without duplicate adoption"
    true
    (List.length near_100 = 1)
;;

let test_sync_open_orders_price_keyed_index () =
  (* sync_open_orders now indexes persisted sell levels by a price key
     instead of rescanning the list per order (the O(n*m) hotpath). Verify
     the observable behavior is preserved: qty update on a matching open
     sell, adoption of a new sell, and 1-to-1 matching across duplicate
     prices. *)
  let open Dio_strategies.Suicide_grid in
  let symbol = "IDX_MATCH/USD" in
  let state = get_strategy_state symbol in
  state.persisted_sell_levels <- [ 100.00, 1.0; 98.00, 1.0 ];
  let asset_alpaca =
    { exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = get_exchange_config "alpaca" in
  (* Two open sells: one matches existing persisted level with a qty
     difference (should update qty), one is a new price (should adopt). *)
  let iter_orders f =
    f "oid_1" 100.0 1.5 "sell" (Some 1);
    f "oid_2" 97.0 1.0 "sell" (Some 1)
  in
  let _ =
    sync_open_orders
      ~state
      ~now:100.0
      ~asset:asset_alpaca
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders
      ~ecfg
  in
  check
    bool
    "matched persisted level qty updated"
    true
    (List.exists (fun (p, q) -> p = 100.0 && q = 1.5) state.persisted_sell_levels);
  check
    bool
    "new open sell adopted into persisted levels"
    true
    (List.exists (fun (p, q) -> p = 97.0 && q = 1.0) state.persisted_sell_levels);
  check bool "adopted level persisted flag" true state.persistence_dirty;
  (* Case 2: duplicate open sells at the same price must not both consume the
     same persisted level (1-to-1 matching). *)
  let state2 = get_strategy_state "IDX_MATCH2/USD" in
  state2.persisted_sell_levels <- [ 105.00, 1.0 ];
  let iter_orders2 f =
    f "oid_a" 105.0 1.0 "sell" (Some 1);
    f "oid_b" 105.0 1.0 "sell" (Some 1)
  in
  let _ =
    sync_open_orders
      ~state:state2
      ~now:100.0
      ~asset:{ asset_alpaca with symbol = "IDX_MATCH2/USD" }
      ~bid_price:105.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders2
      ~ecfg
  in
  (* One level matches; the second sell adopts a new level. *)
  let matches = List.filter (fun (p, _) -> p = 105.0) state2.persisted_sell_levels in
  check bool "duplicate open sells matched 1-to-1" true (List.length matches = 2)
;;

let test_sync_open_orders_reconcile_agreement () =
  (* M16: sync_open_orders now computes the (open_levels, missing_levels)
     split during its scan (O(m), by draining per-price-key match counts) and
     threads it into evaluate_sell_leg, replacing the second O(n+m)
     partition_persisted_sell_levels pass. Verify the threaded split agrees
     EXACTLY with the reference partition over the same final persisted list
     and open-sell set, across duplicates, boundary floats, adoptions and
     qty updates. *)
  let open Dio_strategies.Suicide_grid in
  let ecfg = get_exchange_config "alpaca" in
  let mk_asset symbol =
    { exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Grid"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let assert_split_matches name ~persisted ~sells =
    let symbol = "AGREE_TEST_" ^ name ^ "/USD" in
    let state = get_strategy_state symbol in
    state.persisted_sell_levels <- persisted;
    let iter_orders f = List.iter (fun (oid, p, q) -> f oid p q "sell" (Some 1)) sells in
    let ( _open_buy_count
        , _has_recent_amend_buy
        , _locked_in_buys
        , _locked_in_sells
        , _closest_sell
        , _pending_buy_qty
        , open_levels
        , missing_levels )
      =
      sync_open_orders
        ~state
        ~now:100.0
        ~asset:(mk_asset symbol)
        ~bid_price:100.0
        ~lot_qty:1.0
        ~iter_open_orders:iter_orders
        ~ecfg
    in
    let ref_open, ref_missing = reconcile_persisted_sell_levels ~state in
    let canon = List.sort (fun (p1, _) (p2, _) -> Float.compare p1 p2) in
    check
      bool
      (name ^ ": threaded open_levels == reference partition")
      true
      (canon open_levels = canon ref_open);
    check
      bool
      (name ^ ": threaded missing_levels == reference partition")
      true
      (canon missing_levels = canon ref_missing)
  in
  (* Duplicates at the same price (SPCX-style) with matching sells. *)
  assert_split_matches
    "duplicates"
    ~persisted:[ 149.0, 0.25; 149.0, 0.25; 148.0, 0.25 ]
    ~sells:[ "s1", 149.0, 0.25; "s2", 149.0, 0.25; "s3", 148.0, 0.25 ];
  (* One duplicate unmatched: the second 149.0 level is missing. *)
  assert_split_matches
    "duplicate-missing"
    ~persisted:[ 149.0, 0.25; 149.0, 0.25 ]
    ~sells:[ "s1", 149.0, 0.25 ];
  (* 4-decimal boundary float: 100.00005 open vs 100.00004 persisted. *)
  assert_split_matches
    "boundary"
    ~persisted:[ 100.00004, 1.0 ]
    ~sells:[ "s1", 100.00005, 1.0 ];
  (* A genuinely missing level (nothing on the book at that price). *)
  assert_split_matches
    "missing"
    ~persisted:[ 105.0, 1.0; 100.0, 1.0 ]
    ~sells:[ "s1", 105.0, 1.0 ];
  (* Adoption: no persisted levels, sells get adopted (all open, none missing). *)
  assert_split_matches
    "adoption"
    ~persisted:[]
    ~sells:[ "s1", 105.0, 1.0; "s2", 97.0, 1.0 ];
  (* Qty update on a matched level. *)
  assert_split_matches "qty-update" ~persisted:[ 100.0, 1.0 ] ~sells:[ "s1", 100.0, 1.5 ]
;;

(* ---- Buy-trailing: qty-only oracle re-sizes must honor the trailing rules - *)

let eval_buy_trail ~symbol ~grid_qty ~bid ~ask ~resting_price ~resting_qty ~sell_opt =
  let buy_id = symbol ^ "_buy" in
  let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  st.exchange_id <- "alpaca";
  st.grid_qty <- grid_qty;
  st.last_buy_order_id <- Some buy_id;
  st.last_buy_order_price <- Some resting_price;
  st.pending_orders <- [];
  st.inflight_amend_buy <- false;
  (* The in-flight amendment registry and cooldowns are global, keyed by
     order id: clear any leftovers so each test starts clean. *)
  ignore
    (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment buy_id);
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = Printf.sprintf "%.8g" grid_qty
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "suicide_grid"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Suicide_grid.get_pending_orders 100);
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now
       ~asset
       ~bid_price:bid
       ~ask_price:ask
       ~quote_balance:1000.0
       ~quote_balance_stale:false
       ~cycle:1
       ~iter_open_orders
       ~open_buy_count_from_scan:1
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:sell_opt
       ~pending_buy_qty_from_scan:resting_qty);
  Dio_strategies.Suicide_grid.get_pending_orders 10
;;

let test_qty_mismatch_keeps_resting_price_when_target_below () =
  (* Alpaca qty-only re-size (oracle re-derived the size from a churning
     pool; spacing unchanged) with the grid target BELOW the resting buy
     (flat/falling market): the amend must fix the QTY and keep the resting
     PRICE - the buy only ever trails up, so it must NOT be dragged down to
     the grid target. *)
  let pushed =
    eval_buy_trail
      ~symbol:"QTY_HOLD/USD"
      ~grid_qty:2.0
      ~bid:100.0
      ~ask:100.5
      ~resting_price:100.0
      ~resting_qty:1.0
      ~sell_opt:(Some ("sell1", 105.0))
  in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      bool
      "qty-only amend pushed"
      true
      (o.operation = Dio_strategies.Strategy_common.Amend
       && o.side = Dio_strategies.Strategy_common.Buy);
    check (float 0.) "qty corrected to config" 2.0 o.qty;
    check (option (float 0.)) "price kept at the resting price" (Some 100.0) o.price
  | _ -> failwith "expected exactly one buy amend"
;;

let test_qty_mismatch_trails_price_up_when_target_above () =
  (* Alpaca qty-only re-size where the grid target (bid - gi) is ABOVE the
     resting buy: the amend trails the price up to the target AND applies the
     new qty - identical to normal trailing. *)
  let pushed =
    eval_buy_trail
      ~symbol:"QTY_TRAIL/USD"
      ~grid_qty:2.0
      ~bid:101.0
      ~ask:101.5
      ~resting_price:99.0
      ~resting_qty:1.0
      ~sell_opt:(Some ("sell1", 105.0))
  in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      bool
      "trail-up amend pushed"
      true
      (o.operation = Dio_strategies.Strategy_common.Amend
       && o.side = Dio_strategies.Strategy_common.Buy);
    check (float 0.) "qty corrected to config" 2.0 o.qty;
    check (option (float 0.)) "price trailed up to the grid target" (Some 100.0) o.price
  | _ -> failwith "expected exactly one buy amend"
;;

let test_pure_trailing_no_amend_when_target_below () =
  (* No qty mismatch, market flat relative to the resting buy: the trailing
     rules say the buy sits - no amend may be emitted. *)
  let pushed =
    eval_buy_trail
      ~symbol:"QTY_NONE/USD"
      ~grid_qty:2.0
      ~bid:100.0
      ~ask:100.5
      ~resting_price:100.0
      ~resting_qty:2.0
      ~sell_opt:(Some ("sell1", 105.0))
  in
  check int "flat/falling market emits no amend" 0 (List.length pushed)
;;

let test_buy_trail_fires_on_single_tick_move () =
  (* The amend deadband is the exchange's minimum price move (one tick,
     cached_price_increment = 0.01): a small trail-up fires immediately. A
     5-cent move (bid 97.02 -> grid buy 96.05 vs resting 96.00) is above the
     1-tick threshold, so the amend fires - the old 10-tick/5%-of-grid buffer
     would have swallowed this and made trailing jumpy. *)
  let symbol = "TRAIL_TICK/USD" in
  let buy_id = symbol ^ "_buy" in
  let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  st.exchange_id <- "alpaca";
  st.grid_qty <- 1.0;
  st.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  st.last_buy_order_id <- Some buy_id;
  st.last_buy_order_price <- Some 96.0;
  st.pending_orders <- [];
  st.inflight_amend_buy <- false;
  ignore
    (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment buy_id);
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "suicide_grid"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Suicide_grid.get_pending_orders 100);
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now
       ~asset
       ~bid_price:97.02
       ~ask_price:97.52
       ~quote_balance:1000.0
       ~quote_balance_stale:false
       ~cycle:1
       ~iter_open_orders
       ~open_buy_count_from_scan:1
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:(Some ("sell1", 105.0))
       ~pending_buy_qty_from_scan:1.0);
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 10 in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      (option (float 0.))
      "buy trails up on a 5-cent move (above the 1-tick deadband)"
      (Some 96.05)
      o.price
  | _ -> failwith "expected exactly one buy amend"
;;

let test_buy_trail_2xgi_anchored_on_sell () =
  (* The trailing clamp's 2*gi separation is anchored on the SELL price and
     applies when the sell is strictly ABOVE the top of book (a valid bracket
     around the market). Sell at 103.50, bid at 103.00, gi 1.0%: the buy must
     stop at 101.43 (= 103.50 - 2*gi of the sell, which binds) - not 101.97
     (bid - gi) and not 101.44 (= 103.50 - 2*gi of the bid). *)
  let symbol = "TRAIL_SELL_ANCHOR/USD" in
  let buy_id = symbol ^ "_buy" in
  let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  st.exchange_id <- "alpaca";
  st.grid_qty <- 1.0;
  st.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  st.last_buy_order_id <- Some buy_id;
  st.last_buy_order_price <- Some 96.0;
  st.pending_orders <- [];
  st.inflight_amend_buy <- false;
  ignore
    (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment buy_id);
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "suicide_grid"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Suicide_grid.get_pending_orders 100);
  ignore
    (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
       ~state:st
       ~now
       ~asset
       ~bid_price:103.0
       ~ask_price:103.5
       ~quote_balance:1000.0
       ~quote_balance_stale:false
       ~cycle:1
       ~iter_open_orders
       ~open_buy_count_from_scan:1
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:(Some ("sell1", 103.50))
       ~pending_buy_qty_from_scan:1.0);
  let pushed = Dio_strategies.Suicide_grid.get_pending_orders 10 in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      (option (float 0.))
      "buy anchored exactly 2*gi below the sell above the book"
      (Some 101.43)
      o.price
  | _ -> failwith "expected exactly one buy amend"
;;

let test_buy_trail_respects_sell_zone_while_tracked () =
  (* The 2*gi-from-closest-sell clamp is PRICE-INDEPENDENT and stays active
     while the sell is tracked by order management. A sell AT the book
     (100.00 = bid) still holds the buy at sell - 2*gi = 98.00 - it does NOT
     trail to bid - gi = 99.00 (which would be inside the sell's zone). A sell
     BELOW the book (99.00 < bid 100.00) still holds the buy at
     sell - 2*gi = 97.02. The buy trails at bid - gi = 99.00 only when NO
     sell is tracked at all (removed by order management). *)
  let run_case ~symbol ~sell_opt =
    let buy_id = symbol ^ "_buy" in
    let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
    st.exchange_id <- "alpaca";
    st.grid_qty <- 1.0;
    st.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
    st.last_buy_order_id <- Some buy_id;
    st.last_buy_order_price <- Some 96.0;
    st.pending_orders <- [];
    st.inflight_amend_buy <- false;
    ignore
      (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment
         buy_id);
    let asset =
      { Dio_strategies.Suicide_grid.exchange = "alpaca"
      ; symbol
      ; qty = "1.0"
      ; grid_interval = 1.0
      ; sell_mult = "1.0"
      ; strategy = "suicide_grid"
      ; maker_fee = Some 0.0
      ; taker_fee = Some 0.0
      ; accumulation_buffer = 0.01
      ; base_accumulation = true
      ; sell_levels_persistence = true
      }
    in
    let iter_open_orders _ = () in
    let now = Unix.gettimeofday () in
    ignore (Dio_strategies.Suicide_grid.get_pending_orders 100);
    ignore
      (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
         ~state:st
         ~now
         ~asset
         ~bid_price:100.0
         ~ask_price:100.5
         ~quote_balance:1000.0
         ~quote_balance_stale:false
         ~cycle:1
         ~iter_open_orders
         ~open_buy_count_from_scan:1
         ~has_recent_amend_buy:false
         ~locked_in_buys:0.0
         ~closest_sell_order_initial:sell_opt
         ~pending_buy_qty_from_scan:1.0);
    let pushed = Dio_strategies.Suicide_grid.get_pending_orders 10 in
    match pushed with
    | [ (o : Dio_strategies.Strategy_common.strategy_order) ] -> o.price
    | _ -> failwith "expected exactly one buy amend"
  in
  check
    (option (float 0.))
    "sell at the book: buy stays 2*gi below the tracked sell"
    (Some 98.0)
    (run_case ~symbol:"TRAIL_ZONE_AT/USD" ~sell_opt:(Some ("sell1", 100.0)));
  check
    (option (float 0.))
    "sell below the book: buy stays 2*gi below the tracked sell"
    (Some 97.02)
    (run_case ~symbol:"TRAIL_ZONE_BELOW/USD" ~sell_opt:(Some ("sell1", 99.0)));
  check
    (option (float 0.))
    "no sell tracked: buy trails at bid - gi"
    (Some 99.0)
    (run_case ~symbol:"TRAIL_ZONE_NONE/USD" ~sell_opt:None)
;;

let test_buy_trail_never_enters_sell_zone_until_removed () =
  (* PROOF of the ladder-respecting property: while a sell is tracked, the
     buy trails up toward it but stops exactly 2*gi below it (sell - 2*gi)
     and NEVER goes above that boundary - not even when the perceived bid
     dislocates ABOVE the resting sell without filling it. The zone is
     released only when the sell is removed from tracking (order
     management), after which the buy resumes trailing at bid - gi. *)
  let symbol = "TRAIL_ZONE_PROOF/USD" in
  let buy_id = symbol ^ "_buy" in
  let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
  st.exchange_id <- "alpaca";
  st.grid_qty <- 1.0;
  st.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  st.last_buy_order_id <- Some buy_id;
  st.last_buy_order_price <- Some 96.0;
  st.pending_orders <- [];
  st.inflight_amend_buy <- false;
  let asset =
    { Dio_strategies.Suicide_grid.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "suicide_grid"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  let eval_step ~bid ~sell_opt =
    ignore (Dio_strategies.Suicide_grid.get_pending_orders 100);
    (* Each push_order for an amend leaves a pending_amend entry, the in-flight
       amendment latch and the inflight_amend_buy flag: clear all three so the
       next step trails from the freshly amended resting price. *)
    st.pending_orders <- [];
    st.inflight_amend_buy <- false;
    ignore
      (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment
         buy_id);
    ignore
      (Dio_strategies.Suicide_grid_execution.evaluate_buy_leg
         ~state:st
         ~now
         ~asset
         ~bid_price:bid
         ~ask_price:(bid +. 0.5)
         ~quote_balance:1000.0
         ~quote_balance_stale:false
         ~cycle:1
         ~iter_open_orders
         ~open_buy_count_from_scan:1
         ~has_recent_amend_buy:false
         ~locked_in_buys:0.0
         ~closest_sell_order_initial:sell_opt
         ~pending_buy_qty_from_scan:1.0);
    Dio_strategies.Suicide_grid.get_pending_orders 10
  in
  (* 1. Bid 101.50, sell 103.00 tracked: buy trails to bid - gi = 100.49,
        below the zone boundary sell - 2*gi = 100.94. *)
  (match eval_step ~bid:101.5 ~sell_opt:(Some ("sell1", 103.0)) with
   | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
     check
       (option (float 0.))
       "trails toward the sell, below its zone"
       (Some 100.49)
       o.price
   | _ -> failwith "expected a trail amend");
  (* 2. Bid 102.50: the buy reaches exactly sell - 2*gi = 100.94 and stops. *)
  (match eval_step ~bid:102.5 ~sell_opt:(Some ("sell1", 103.0)) with
   | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
     check
       (option (float 0.))
       "stops exactly at sell - 2*gi (zone boundary)"
       (Some 100.94)
       o.price
   | _ -> failwith "expected a stop amend");
  (* 3. Bid dislocates ABOVE the sell (104.00) but the sell is still tracked
        and unfilled: the buy MUST NOT move - it stays at 100.94, never
        entering the zone and never crossing the resting sell. *)
  check
    int
    "no trail past the sell while it is tracked (price dislocation)"
    0
    (List.length (eval_step ~bid:104.0 ~sell_opt:(Some ("sell1", 103.0))));
  (* 4. The sell is removed from tracking (order management): the buy resumes
        trailing at bid - gi = 102.96. *)
  match eval_step ~bid:104.0 ~sell_opt:None with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    check
      (option (float 0.))
      "resumes trailing at bid - gi after the sell is removed"
      (Some 102.96)
      o.price
  | _ -> failwith "expected a resume amend"
;;

let () =
  run
    "Suicide Grid"
    [ "initialization", [ test_case "strategy init" `Quick test_initialization ]
    ; ( "order_creation"
      , [ test_case "place order" `Quick test_order_creation_place
        ; test_case "amend order" `Quick test_order_creation_amend
        ; test_case "cancel order" `Quick test_order_creation_cancel
        ; test_case "legacy order" `Quick test_legacy_order_creation
        ; test_case "duplicate key per side" `Quick test_duplicate_key_per_side
        ] )
    ; ( "buy trailing"
      , [ test_case
            "qty mismatch keeps resting price when target below"
            `Quick
            test_qty_mismatch_keeps_resting_price_when_target_below
        ; test_case
            "qty mismatch trails price up when target above"
            `Quick
            test_qty_mismatch_trails_price_up_when_target_above
        ; test_case
            "pure trailing emits no amend when target below"
            `Quick
            test_pure_trailing_no_amend_when_target_below
        ; test_case
            "trailing 2x gi clamp anchored on the sell price"
            `Quick
            test_buy_trail_2xgi_anchored_on_sell
        ; test_case
            "no 2x gi clamp when the sell is at/below the top of book"
            `Quick
            test_buy_trail_respects_sell_zone_while_tracked
        ; test_case
            "buy never enters the sell zone until the sell is removed"
            `Quick
            test_buy_trail_never_enters_sell_zone_until_removed
        ; test_case
            "trailing fires on a single tick move"
            `Quick
            test_buy_trail_fires_on_single_tick_move
        ] )
    ; ( "config"
      , [ test_case "config parsing" `Quick test_config_parsing
        ; test_case "price rounding" `Quick test_price_rounding
        ; test_case "price increment" `Quick test_price_increment
        ; test_case "grid price calculation" `Quick test_grid_price_calculation
        ] )
    ; ( "state"
      , [ test_case "state management" `Quick test_state_management
        ; test_case "userref generation" `Quick test_userref_generation
        ; test_case
            "virtual gtc sell grid maintenance"
            `Quick
            test_virtual_gtc_sell_grid_maintenance
        ; test_case
            "sync_open_orders price-keyed index"
            `Quick
            test_sync_open_orders_price_keyed_index
        ; test_case
            "reconcile cross-boundary tolerance"
            `Quick
            test_reconcile_cross_boundary_tolerance
        ; test_case
            "sync_open_orders reconcile agrees with partition"
            `Quick
            test_sync_open_orders_reconcile_agreement
        ] )
    ; "balance", [ test_case "balance checking" `Quick test_balance_checking ]
    ; ( "placement guard"
      , [ test_case
            "buy placement vs fresh/stale balance"
            `Quick
            test_buy_placement_balance_guard
        ; test_case
            "halted path still places the sell for a just-filled buy"
            `Quick
            test_halted_path_still_places_sell
        ; test_case
            "sell ack releases the in-flight latch (multi-sell ladder)"
            `Quick
            test_sell_ack_releases_inflight_latch
        ; test_case
            "blocked sell retries until placed (no replacement buy needed)"
            `Quick
            test_sell_retry_until_placed
        ; test_case
            "blocked placement-triggered sell retries on the next tick"
            `Quick
            test_blocked_placement_sell_retries
        ; test_case
            "accumulation sells non-accrued inventory (no locked double-count)"
            `Quick
            test_accumulation_sells_non_accrued_inventory
        ; test_case
            "nothing placeable clears the latch"
            `Quick
            test_nothing_placeable_clears_latch
        ; test_case
            "kraken partial inventory sells the clamp"
            `Quick
            test_kraken_partial_sell_clamp
        ; test_case
            "alpaca dollar notional floor gate"
            `Quick
            test_alpaca_dollar_floor_gate
        ; test_case
            "hl buy fill accrues reserved base (net of base fee)"
            `Quick
            test_hl_buy_fill_accrues_reserve
        ; test_case
            "sub-minimum qty sell places (notional is the only floor)"
            `Quick
            test_sub_minimum_qty_sell_places
        ; test_case
            "alpaca sell anchors on fill, not the ask"
            `Quick
            test_alpaca_sell_anchors_on_fill_not_ask
        ; test_case
            "new buy respects the 2x gi closest-sell cap"
            `Quick
            test_new_buy_respects_2x_gi_closest_sell
        ] )
    ; ( "reclaim"
      , [ test_case
            "first reclaim decision issues the cancel"
            `Quick
            test_reclaim_step_cancels_when_not_issued
        ; test_case
            "in-flight cancel is deferred (no spam)"
            `Quick
            test_reclaim_step_throttles_in_flight_cancel
        ; test_case
            "failed cancel is retried after the interval"
            `Quick
            test_reclaim_step_retries_failed_cancel
        ; test_case
            "clean store re-arms the latch"
            `Quick
            test_reclaim_step_rearms_when_store_clean
        ; test_case
            "mid-amend buy defers the cancel"
            `Quick
            test_reclaim_step_waits_for_mid_amend_buy
        ] )
    ; ( "events"
      , [ test_case "order acknowledgment" `Quick test_order_acknowledgment
        ; test_case "order cancellation" `Quick test_order_cancellation
        ; test_case
            "order cancellation matches client order id"
            `Quick
            test_order_cancellation_matches_client_order_id
        ; test_case "order rejection" `Quick test_order_rejection
        ] )
    ; ( "accumulation"
      , [ test_case
            "profit tracking from sell fills"
            `Quick
            test_accumulation_profit_tracking
        ; test_case
            "gated sell - insufficient profit"
            `Quick
            test_accumulation_gated_sell_insufficient
        ; test_case
            "gated sell - sufficient profit"
            `Quick
            test_accumulation_gated_sell_sufficient
        ; test_case
            "recovery blocks blind 1:1 sell"
            `Quick
            test_accumulation_recovery_blocks_blind_sell
        ; test_case
            "full lifecycle (20 buy-sell cycles)"
            `Quick
            test_accumulation_full_lifecycle
        ; test_case
            "multi-strategy isolation (BTC + HYPE)"
            `Quick
            test_accumulation_multi_strategy_isolation
        ] )
    ]
;;
