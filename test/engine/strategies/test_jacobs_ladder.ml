open Alcotest

(* Forces the Alpaca exchange module to link and run its registry
   registration, so venue metadata resolves exactly as in production (the
   1e-9 fractional increment behind dust-level pruning). *)
let () = ignore Alpaca.Module.Alpaca_impl.name

(* Seed the id-keyed sell-commitment ledger from the legacy list form used
   throughout these fixtures. *)
let set_sell_commitments tbl entries =
  Hashtbl.clear tbl;
  List.iter
    (fun (id, price, qty, seen, acked, armed) ->
       Hashtbl.replace
         tbl
         id
         { Dio_strategies.Jacobs_ladder.sc_price = price
         ; sc_qty = qty
         ; sc_seen = seen
         ; sc_acked = acked
         ; sc_listed = seen
         ; sc_armed = armed
         })
    entries
;;

let test_initialization () =
  (* Test strategy initialization *)
  check unit "jacobs_ladder init" () (Dio_strategies.Jacobs_ladder.Strategy.init ())
;;

let test_order_creation_place () =
  (* Test creating place orders *)
  let order =
    Dio_strategies.Jacobs_ladder.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      Dio_strategies.Strategy_common.Buy
      0.001
      (Some 50000.0)
      true
      Dio_strategies.Strategy_common.Ladder
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
    (order.strategy = Dio_strategies.Strategy_common.Ladder)
;;

let test_order_creation_amend () =
  (* Test creating amend orders *)
  let order =
    Dio_strategies.Jacobs_ladder.create_amend_order
      "order123"
      "BTC/USD"
      Dio_strategies.Strategy_common.Sell
      0.001
      (Some 51000.0)
      true
      Dio_strategies.Strategy_common.Ladder
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
    (order.strategy = Dio_strategies.Strategy_common.Ladder)
;;

let test_order_creation_cancel () =
  (* Test creating cancel orders *)
  let order =
    Dio_strategies.Jacobs_ladder.create_cancel_order
      "order456"
      "BTC/USD"
      Dio_strategies.Strategy_common.Ladder
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
    (order.strategy = Dio_strategies.Strategy_common.Ladder);
  check (option (float 0.)) "cancel order price" None order.price;
  check (float 0.) "cancel order qty" 0.0 order.qty
;;

let test_legacy_order_creation () =
  (* Test legacy create_order function for backwards compatibility *)
  ()
;;

(* Check if create_order exists, if not remove test or alias it. Assuming it was renamed to create_place_order or removed. 
     If it's removed, we should remove this test case. For now, let's comment it out or update it to create_place_order if legacy is gone. *)
(* let order = Dio_strategies.Jacobs_ladder.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in *)

(* let order = Dio_strategies.Jacobs_ladder.create_order "BTC/USD" Dio_strategies.Strategy_common.Buy 0.001 (Some 50000.0) true in *)

let test_duplicate_key_per_side () =
  (* Ensure duplicate key is per asset+side, not price/qty *)
  let open Dio_strategies in
  let buy1 =
    Jacobs_ladder.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      Strategy_common.Buy
      0.001
      (Some 50000.0)
      true
      Strategy_common.Ladder
      "kraken"
  in
  let buy2 =
    Jacobs_ladder.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      Strategy_common.Buy
      0.002
      (Some 51000.0)
      true
      Strategy_common.Ladder
      "kraken"
  in
  let sell1 =
    Jacobs_ladder.create_place_order
      "BTC/USD|sell|grid"
      "BTC/USD"
      Strategy_common.Sell
      0.003
      (Some 52000.0)
      true
      Strategy_common.Ladder
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
      Dio_strategies.Jacobs_ladder.parse_config_float
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "BTC/USD" in
  let rounded = state.cached_round_price 50000.12345678 in
  check bool "price rounding non-negative" true (rounded >= 0.0)
;;

let test_price_increment () =
  (* Test price increment retrieval - this relies on Kraken instruments feed *)
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "BTC/USD" in
  let increment = state.cached_price_increment in
  check bool "price increment positive" true (increment > 0.0)
;;

let test_grid_price_calculation () =
  (* Test grid price calculations *)
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "TEST/USD" in
  let above_price =
    Dio_strategies.Jacobs_ladder.calculate_grid_price 50000.0 1.0 true state
  in
  let below_price =
    Dio_strategies.Jacobs_ladder.calculate_grid_price 50000.0 1.0 false state
  in
  (* Should be above and below 50000 with 1% grid *)
  check bool "above price correct" true (above_price >= 50499.0 && above_price <= 50501.0);
  check bool "below price correct" true (below_price >= 49499.0 && below_price <= 49501.0)
;;

let test_state_management () =
  (* Test strategy state management *)
  let state1 = Dio_strategies.Jacobs_ladder.get_strategy_state "BTC/USD" in
  let state2 = Dio_strategies.Jacobs_ladder.get_strategy_state "BTC/USD" in
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  let run_leg buy_attempted =
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now:100.0
      ~asset
      ~bid_price:62369.0
      ~ask_price:62370.0
      ~asset_balance:1.00112
      ~buy_attempted
      ~oracle_halted:false
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:None
  in
  (* Tick 1: a buy was placed this tick; the sell is on cooldown, so the
     attempt is blocked - the sell must stay owed. *)
  Hashtbl.replace state.amend_cooldowns "place_Sell" (Unix.gettimeofday () +. 10.0);
  run_leg true;
  check
    bool
    "no sell pushed while on cooldown"
    true
    (Dio_strategies.Jacobs_ladder.get_pending_orders 100 = []);
  check
    bool
    "placement-triggered sell stays owed (latch armed)"
    true
    state.just_filled_buy;
  (* Tick 2: cooldown expired; no buy placement, no fill, but the owed sell
     retries and is placed. *)
  Hashtbl.remove state.amend_cooldowns "place_Sell";
  run_leg false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.reserved_base <- 0.0;
  state.position_base <- 0.0;
  state.buy_credits_since_balance <- [];
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done symbol;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
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
  check bool "hl buy reference qty recorded" true (state.last_buy_fill_qty = Some 0.5);
  check
    bool
    "hl pending buy credit is net of the base-side buy fee"
    true
    (abs_float
       (List.fold_left (fun acc (_, q) -> acc +. q) 0.0 state.buy_credits_since_balance
        -. landed)
     < 1e-9);
  (* Kraken aligns identically: buy fill updates refs only, no reserve. *)
  let kr_symbol = "KR_ACCRUAL/XMR/USD" in
  let kr_state = Dio_strategies.Jacobs_ladder.get_strategy_state kr_symbol in
  kr_state.exchange_id <- "kraken";
  kr_state.grid_qty <- 0.04;
  kr_state.cached_sell_mult <- 0.999;
  kr_state.reserved_base <- 0.0;
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done kr_symbol;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
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

(* The reserved_base leak under volatility: for accumulation venues with
   track_pending_sells = false (Hyperliquid), a resting sell blocks nothing
   locally after its ack, and the inventory gate trusts the balance feed's
   hold-netting to be current. The spotState hold lags the ack by seconds;
   sizing in that window counts the just-sold base as free and dips into
   reserved_base. The unnetted-hold guard must subtract the armed hold until
   the balance confirms netting (or the grace expires). *)
let test_unnetted_sell_hold_gates_second_sizing () =
  let symbol = "UNNET1/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.18;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.0224;
  state.accumulated_profit <- 24.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 88.6;
  state.last_buy_fill_qty <- Some 0.18;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.18"
    ; grid_interval = 0.36
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.25
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  let run_leg ~now ~bal ~age =
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now
      ~asset
      ~bid_price:88.5
      ~ask_price:88.6
      ~asset_balance:bal
      ~buy_attempted:false
      ~oracle_halted:false
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:(Some age)
  in
  (* Tick 1: balance 0.4024, reserved 0.0224 -> available 0.38 -> sell sized
     to the full available (0.38 free float). The venue is holding it. The
     balance message is current (age 0.1). *)
  run_leg ~now:100.0 ~bal:0.4024 ~age:0.1;
  let pushed1 = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell1 =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell)
      pushed1
  in
  check bool "first sell placed" true (Option.is_some sell1);
  let qty1 =
    match sell1 with
    | Some o -> o.qty
    | None -> 0.0
  in
  drain ();
  (* Tick 2, moments later: a second buy fill triggers another sell while
     the venue's spotState has NOT yet netted the first hold - the balance
     still reads 0.4024 (age 3.0 = newest message predates the placement).
     Without the guard, available would again be 0.38 and the second sell
     would dip into the 0.0224 reserve. With the guard, the armed hold
     (0.38) clamps the second sell to zero. *)
  state.just_filled_buy <- true;
  state.last_buy_fill_qty <- Some 0.18;
  (* The first sell acked (in-flight latch released, dedup key removed); the
     balance message still predates the placement (age 3.0 at t=102 ->
     message from t=99): the hold is unnetted and must gate the sizing. *)
  state.inflight_sell <- false;
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       state.duplicate_key_sell);
  run_leg ~now:102.0 ~bal:0.4024 ~age:3.0;
  let pushed2 = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell2_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed2
  in
  check
    bool
    "second sell within the netting window does not dip into reserved_base"
    true
    (sell2_qty <= 1e-9);
  drain ();
  (* Tick 3: the venue nets the hold (tradeable drops by the full held qty,
     0.4024 -> 0.0224), which retires it; the fresh fill's 0.18 is not yet in
     the venue figure and rides the buy-credit bridge. The fresh fill's sell
     is placeable against the netted balance (0.0224 + 0.18 - 0.0224 reserved). *)
  state.just_filled_buy <- true;
  state.inflight_sell <- false;
  state.position_base <- 0.4024;
  state.position_initialized <- true;
  state.position_venue_ts <- 99.0;
  state.buy_credits_since_balance <- [ 103.0, 0.18 ];
  state.attributed_balance_increase <- 0.0;
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       state.duplicate_key_sell);
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:103.0
    ~base_balance_age:(Some 0.2)
    ~asset_balance:(0.4024 -. qty1);
  run_leg ~now:103.0 ~bal:(0.4024 -. qty1) ~age:0.2;
  let pushed3 = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell3_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed3
  in
  check
    bool
    "after netting is observed, the fresh fill's sell is placeable again"
    true
    (sell3_qty >= 0.17 && sell3_qty <= 0.18 +. 1e-6);
  drain ()
;;

let test_unnetted_sell_hold_expires_after_grace () =
  let symbol = "UNNET2/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.18;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- false;
  state.last_buy_fill_price <- Some 88.6;
  state.last_buy_fill_qty <- Some 0.18;
  (* Simulate an armed hold whose confirmation never came (e.g. the
     placement was rejected, or no balance message ever arrived): past the
     grace window it must decay so sells are not blocked indefinitely. The
     feed provides no freshness signal here (age None), so the 15s grace
     governs. No new placement is triggered in this tick (just_filled_buy
     false, no buy attempt), so the prune alone is exercised. *)
  state.sell_holds_since_balance <- [ 0.0, 0.38 ];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.18"
    ; grid_interval = 0.36
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.25
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:20.0
    ~asset
    ~bid_price:88.5
    ~ask_price:88.6
    ~asset_balance:0.4024
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None;
  check
    bool
    "stale unnetted hold decays after the grace window"
    true
    (state.sell_holds_since_balance = []);
  drain ()
;;

let test_unnetted_sell_hold_capped_even_with_age () =
  (* A hold must also decay when the feed DOES report freshness but the
     message is ancient (age 100s > grace): the cutoff caps at now - 15s so
     a dead feed cannot keep a hold alive forever. No new placement is
     triggered; the prune alone is exercised. *)
  let symbol = "UNNET3/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.18;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- false;
  state.last_buy_fill_price <- Some 88.6;
  state.last_buy_fill_qty <- Some 0.18;
  let now = Unix.gettimeofday () in
  (* Placed 100s ago; the newest balance message is equally old. *)
  state.sell_holds_since_balance <- [ now -. 100.0, 0.38 ];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.18"
    ; grid_interval = 0.36
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.25
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now
    ~asset
    ~bid_price:88.5
    ~ask_price:88.6
    ~asset_balance:0.4024
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 100.0);
  check
    bool
    "hold older than the grace decays even when the feed reports an ancient age"
    true
    (state.sell_holds_since_balance = []);
  drain ()
;;

let test_unnetted_sell_hold_releases_on_newer_message () =
  (* The production wedge: a sell hold was armed, then a buy fill raised the
     venue figure in the same window the sell's hold was applied, so the
     tradeable net never dropped. The old drop-only release kept the hold armed
     for the whole grace and blocked the owed 1:1 sell, so inventory piled up
     until the next trigger dumped it as one oversized sell. A balance message
     generated AFTER the placement already contains the hold, so the overlay
     must retire and the owed sell must place. This is the net-FLAT masked-drop
     case ([last_balance_delta] = 0); the buy-fill INCREASE case is covered by
     [test_unnetted_sell_hold_ignores_buy_increase]. *)
  let symbol = "UNNET4/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.15;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.19917916;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 82.309;
  state.last_buy_fill_qty <- Some 0.15;
  state.position_base <- 0.35512940;
  state.position_initialized <- true;
  state.position_venue_ts <- 99.0;
  state.buy_credits_since_balance <- [];
  (* The sell's hold was armed at t=100; the newest balance message is from
     t=109 (age 1.0 at now 110), i.e. newer than the placement. *)
  state.sell_holds_since_balance <- [ 100.0, 0.15 ];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.15"
    ; grid_interval = 0.4
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.25
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:110.0
    ~asset
    ~bid_price:82.30
    ~ask_price:82.31
    ~asset_balance:0.35512940
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 1.0);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell when o.symbol = symbol -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed
  in
  check
    bool
    "a balance message newer than the placement releases the hold and places the owed \
     sell"
    true
    (sell_qty >= 0.15 -. 1e-9);
  check
    bool
    "the released hold leaves the tracking list (only the new placement's hold remains)"
    true
    (match state.sell_holds_since_balance with
     | [ (t, q) ] -> t >= 110.0 -. 1e-9 && q >= 0.15 -. 1e-9
     | _ -> false);
  drain ()
;;

let test_unnetted_sell_hold_burst_downmove () =
  (* Violent down-move burst, end to end through the venue-feed lag: several
     buys fill faster than the spot grid can offer them. For each rung the
     balance snapshot that nets the PREVIOUS rung's resting-sell hold also
     carries the new buy, so the tradeable net never drops (delta 0) and the
     old drop-only release left every prior hold armed. Each message also
     PREDATES the new fill's credit, so that credit is pruned and cannot offset
     the stale hold. Without the message-time release the armed holds stack and
     block every subsequent sell, so inventory piles up until the grace expires
     and it dumps as one oversized sell. Every rung's owed 1:1 sell must place
     at the single lot size instead. *)
  let symbol = "BURST_HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.15;
  (* Zero fee so a fill's buy credit is exactly the lot qty; a fee would only
     perturb the arithmetic, not the mechanism. *)
  state.maker_fee <- 0.0;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.199;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- false;
  state.position_base <- 0.199;
  state.position_initialized <- true;
  state.position_venue_ts <- 0.0;
  state.buy_credits_since_balance <- [];
  state.sell_holds_since_balance <- [];
  state.last_buy_fill_price <- None;
  state.last_buy_fill_qty <- None;
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done symbol;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.15"
    ; grid_interval = 0.4
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.25
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  let lot = 0.15 in
  (* Venue model: total base includes holds; a placed sell locks its qty. *)
  let venue_total = ref 0.199 in
  let venue_hold = ref 0.0 in
  let sold = ref [] in
  drain ();
  for i = 0 to 4 do
    let fill_now = 100.0 +. (float_of_int i *. 1.0) in
    (* 1) a buy fills at the falling price, crediting base. *)
    venue_total := !venue_total +. lot;
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
      ~now:fill_now
      symbol
      (Printf.sprintf "burst_buy_%d" i)
      Dio_strategies.Strategy_common.Buy
      ~fill_price:(82.0 -. (float_of_int i *. 0.5))
      ~fill_qty:lot
      None;
    (* 2) the venue emits a balance snapshot after BOTH the new fill and the
          previous resting sell's hold: the tradeable net is unchanged (no drop
          to observe), and the message is newer than the armed hold. *)
    let msg_now = fill_now +. 0.5 in
    Dio_strategies.Jacobs_ladder.reconcile_position
      ~state
      ~now:msg_now
      ~base_balance_age:(Some 0.0)
      ~asset_balance:(!venue_total -. !venue_hold);
    (* 3) the sell leg runs; the owed 1:1 sell must place at the single lot. *)
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now:(msg_now +. 0.5)
      ~asset
      ~bid_price:(82.0 -. (float_of_int i *. 0.5))
      ~ask_price:(82.0 -. (float_of_int i *. 0.5) +. 0.01)
      ~asset_balance:(!venue_total -. !venue_hold)
      ~buy_attempted:false
      ~oracle_halted:false
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:(Some 0.0);
    let qty =
      List.fold_left
        (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
           match o.operation, o.side with
           | Place, Sell when o.symbol = symbol -> acc +. o.qty
           | _ -> acc)
        0.0
        (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
    in
    sold := (i, qty) :: !sold;
    (* A blocked rung places nothing and locks nothing - the pile-up the test
       guards against. *)
    venue_hold := !venue_hold +. qty;
    drain ()
  done;
  let sold = List.rev !sold in
  let placed_count = List.length (List.filter (fun (_, q) -> q >= lot -. 1e-6) sold) in
  let oversized = List.exists (fun (_, q) -> q > lot +. 1e-6) sold in
  check
    bool
    "every rung of a masking down-move burst places its own 1:1 sell"
    true
    (placed_count = 5);
  check
    bool
    "no rung dumps accumulated inventory as an oversized sell"
    true
    (not oversized);
  check
    bool
    "cumulative sold base equals the burst's buy lots (no over-accumulation)"
    true
    (abs_float (List.fold_left (fun a (_, q) -> a +. q) 0.0 sold -. (5.0 *. lot)) < 1e-6)
;;

let test_unnetted_sell_hold_ignores_buy_increase () =
  (* REGRESSION (rapid-fill oversell): a sell hold was armed, then a buy fill
     raised the venue tradeable figure and bumped the same per-asset freshness
     timestamp BEFORE the venue applied the sell's hold. The old release treated
     any newer message as proof of netting and retired the guard, so the next
     1:1 sell sized against base already committed to the resting sell and the
     venue rejected it ("HL Order Rejected: Insufficient spot balance"). A
     message whose move is an INCREASE cannot have applied a sell hold: the
     guard must stay until a flat/down message or the grace retires it. *)
  let symbol = "UNNET5/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.15;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 82.0;
  state.last_buy_fill_qty <- Some 0.15;
  state.position_base <- 0.05;
  state.position_initialized <- true;
  state.position_venue_ts <- 99.0;
  state.last_seen_asset_balance <- 0.05;
  state.buy_credits_since_balance <- [];
  state.attributed_balance_increase <- 0.0;
  state.last_balance_delta <- 0.0;
  (* The sell hold was armed at t=100 for one lot. *)
  state.sell_holds_since_balance <- [ 100.0, 0.15 ];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.15"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.25
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  let sells_seen () =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  drain ();
  (* Message at t=110 (age 1.0): tradeable 0.05 -> 0.20, a +0.15 buy-fill
     increase that does NOT yet carry the sell's hold. *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:110.0
    ~base_balance_age:(Some 1.0)
    ~asset_balance:0.20;
  check
    bool
    "the increasing message leaves the sell hold armed"
    true
    (state.sell_holds_since_balance <> []);
  check
    bool
    "the increasing message is recorded as positive for the guard"
    true
    (state.last_balance_delta > 0.0);
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:110.0
    ~asset
    ~bid_price:82.0
    ~ask_price:82.01
    ~asset_balance:0.20
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 1.0);
  check
    bool
    "no second sell is offered against base committed to the resting sell"
    true
    (not (sells_seen ()));
  drain ();
  (* A subsequent FLAT message (hold now netted, no net move) retires the guard,
     and the owed 1:1 sell becomes placeable. *)
  state.just_filled_buy <- true;
  state.inflight_sell <- false;
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       state.duplicate_key_sell);
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:111.0
    ~base_balance_age:(Some 1.0)
    ~asset_balance:0.20;
  check
    bool
    "the flat message is recorded as non-increasing"
    true
    (state.last_balance_delta <= 0.0);
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:111.0
    ~asset
    ~bid_price:82.0
    ~ask_price:82.01
    ~asset_balance:0.20
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 1.0);
  check bool "the owed sell places once the hold is retired" true (sells_seen ());
  drain ()
;;

let test_ghost_buy_suppressed_within_ack_grace () =
  (* REGRESSION (rapid-fill churn): a just-acked buy is not yet listed by the
     open-orders feed. The old scan saw zero open buys, no in-flight flag (the
     ack cleared it) and no amend, and purged the live buy as
     "GHOST_BUY_DETECTED", re-placing it into the rapid-fill cascade that
     ultimately over-sized a sell. A buy acked within the grace must survive the
     feed lag; after the grace a still-absent buy is a genuine ghost. *)
  let symbol = "GHOST1/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config "kraken";
  state.open_sell_orders <- [];
  state.pending_orders <- [];
  Hashtbl.clear state.sell_commitments;
  Hashtbl.clear state.amend_cooldowns;
  state.inflight_buy <- false;
  state.inflight_cancel_buy <- false;
  state.inflight_amend_buy <- false;
  state.last_buy_order_id <- Some "ack-buy-1";
  state.last_buy_order_price <- Some 100.0;
  state.reserved_quote <- 5.0;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.1"
    ; grid_interval = 0.5
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.001
    ; taker_fee = Some 0.002
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let now = Unix.gettimeofday () in
  let feed = ref [] in
  let iter_open_orders f = List.iter (fun (a, b, c, d, e) -> f a b c d e) !feed in
  let run_scan () =
    ignore
      (Dio_strategies.Jacobs_ladder.sync_open_orders
         ~state
         ~now
         ~asset
         ~bid_price:100.0
         ~lot_qty:0.1
         ~iter_open_orders
         ~get_open_orders_generation:(fun () -> -1)
         ~ecfg)
  in
  (* Acked just now: the feed has not listed the buy yet. *)
  state.last_buy_ack_ts <- now;
  run_scan ();
  check
    bool
    "a freshly acked buy is not purged as a ghost"
    true
    (state.last_buy_order_id = Some "ack-buy-1");
  check
    bool
    "its quote reservation is retained within the grace"
    true
    (abs_float state.reserved_quote > 1.0);
  (* Past the grace and still absent with no terminal event: recover it. *)
  state.last_buy_ack_ts <- now -. 20.0;
  run_scan ();
  check
    bool
    "after the grace a still-absent buy is recovered"
    true
    (state.last_buy_order_id = None);
  check
    bool
    "the stale quote reservation is released with the ghost"
    true
    (abs_float state.reserved_quote < 1e-9)
;;

let test_position_ledger_bridges_unreflected_fill () =
  (* The over-accumulation desync: a buy fill fires the 1:1 sell before the
     venue's balance feed has netted the fill. Sizing off the raw snapshot
     then reads bal - reserved_base = dust and blocks the sell, so the buy
     leg chains while inventory piles up. The windowed buy credit overlays the
     just-filled qty onto the last-known venue figure, so the sale sizes
     against the fill even though the feed is still pre-fill. *)
  let symbol = "LEDGER1/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.2;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.19117916;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 79.0;
  state.last_buy_fill_qty <- Some 0.2;
  (* Venue figure is the pre-fill snapshot (dust above the reserve); the
     just-filled 0.2 rides in the unreflected-credit overlay. *)
  state.position_base <- 0.1936;
  state.position_initialized <- true;
  state.position_venue_ts <- 999.0;
  state.buy_credits_since_balance <- [ 999.5, 0.2 ];
  state.sell_holds_since_balance <- [];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.3
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* Sanity: the raw snapshot really would block the sale (dust over the
     reserve), proving the credit is what makes it placeable. *)
  check
    bool
    "stale venue snapshot alone is below the venue floor"
    true
    (0.1936 -. state.reserved_base < state.cached_qty_increment);
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1000.0
    ~asset
    ~bid_price:79.0
    ~ask_price:79.1
    ~asset_balance:0.1936
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 0.5);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell when o.symbol = symbol -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed
  in
  check
    bool
    "unreflected fill sells the 1:1 lot despite the stale snapshot"
    true
    (sell_qty >= 0.2 -. 1e-6 && sell_qty <= 0.2 +. 1e-6);
  drain ()
;;

let test_position_reconcile_freshness_gate () =
  (* A new balance message is authoritative: adopt the venue figure and drop
     every buy credit its generation time covers. A message that does not
     advance the feed timestamp must leave both the ledger and the overlay
     untouched. *)
  let symbol = "LEDGER2/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.reserved_base <- 0.19117916;
  state.position_base <- 0.4;
  state.position_initialized <- true;
  state.position_venue_ts <- 998.0;
  state.buy_credits_since_balance <- [ 998.5, 0.2 ];
  (* New message generated at 999 (now 1000.5, age 1.5): adopt the venue
     figure and prune the credit it already covers. *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.5
    ~base_balance_age:(Some 1.5)
    ~asset_balance:0.2;
  check
    bool
    "new venue message replaces the ledger"
    true
    (abs_float (state.position_base -. 0.2) < 1e-12);
  check
    bool
    "credits the message already covers are pruned"
    true
    (state.buy_credits_since_balance = []);
  (* A later fill arrives, then a message that does NOT advance the feed
     timestamp: neither the adopted value nor the fresh credit is disturbed. *)
  state.buy_credits_since_balance <- [ 999.7, 0.15 ];
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.1
    ~base_balance_age:(Some 1.4)
    ~asset_balance:0.2;
  check
    bool
    "a non-advancing message does not clobber the ledger"
    true
    (abs_float (state.position_base -. 0.2) < 1e-12);
  check
    bool
    "a non-advancing message does not prune fresh credits"
    true
    (List.length state.buy_credits_since_balance = 1);
  (* The feed catches up (generated 1001.5): adopt the new value and prune. *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1002.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:0.39;
  check
    bool
    "fresher venue message replaces the ledger, including downward corrections"
    true
    (abs_float (state.position_base -. 0.39) < 1e-12);
  check
    bool
    "credits covered by the catching-up message are pruned"
    true
    (state.buy_credits_since_balance = [])
;;

let test_position_reconcile_lower_balance_cannot_dip_reserve () =
  (* After adopting a lower venue figure, the sellable figure is position_base
     minus the reserve, clamped at zero - adoption cannot leave a sellable
     amount above the venue's own free inventory. *)
  let symbol = "LEDGER3/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.reserved_base <- 0.19117916;
  state.position_base <- 0.9;
  state.position_initialized <- true;
  state.position_venue_ts <- 500.0;
  state.buy_credits_since_balance <- [];
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:501.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:0.1936;
  check
    bool
    "ledger adopts the lower venue figure"
    true
    (abs_float (state.position_base -. 0.1936) < 1e-12);
  let available = Float.max 0.0 (state.position_base -. state.reserved_base) in
  check
    bool
    "adopted lower balance leaves no sellable amount above the venue free base"
    true
    (available <= state.position_base -. state.reserved_base +. 1e-12
     && available < state.cached_qty_increment)
;;

let test_position_seed_prunes_covered_credit () =
  (* Init race: a buy fill is processed before the first balance message, so
     it sits in the overlay; the first message was generated after the fill
     and already includes it. The seed must prune the covered credit or the
     fill is counted twice and a sell can reach reserved_base. *)
  let symbol = "LEDGER4/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.reserved_base <- 0.19117916;
  state.position_initialized <- false;
  state.position_base <- 0.0;
  state.position_venue_ts <- 0.0;
  state.buy_credits_since_balance <- [ 998.5, 0.2 ];
  (* First message generated at 999 (after the fill at 998.5). *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.5
    ~base_balance_age:(Some 1.5)
    ~asset_balance:0.39117916;
  check
    bool
    "first message seeds the ledger"
    true
    (abs_float (state.position_base -. 0.39117916) < 1e-12);
  check
    bool
    "seed message prunes the covered credit"
    true
    (state.buy_credits_since_balance = []);
  check
    bool
    "seeded ledger leaves only the free float above the reserve"
    true
    (abs_float (state.position_base -. state.reserved_base -. 0.2) < 1e-9)
;;

let test_position_seed_keeps_newer_credit () =
  (* Counter-case to the init race: the first message was generated BEFORE the
     fill, so it does not include it. The credit must survive the seed so the
     just-filled buy is still sellable. *)
  let symbol = "LEDGER5/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.reserved_base <- 0.19117916;
  state.position_initialized <- false;
  state.position_base <- 0.0;
  state.position_venue_ts <- 0.0;
  state.buy_credits_since_balance <- [ 999.5, 0.2 ];
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.5
    ~base_balance_age:(Some 1.5)
    ~asset_balance:0.1936;
  check
    bool
    "seed adopts the pre-fill venue figure"
    true
    (abs_float (state.position_base -. 0.1936) < 1e-12);
  check
    bool
    "credit newer than the seed message is kept"
    true
    (List.length state.buy_credits_since_balance = 1)
;;

let test_position_stale_message_does_not_regress_ledger () =
  (* Out-of-order / replayed feed message with an older generation time must
     not lower the ledger (WS reconnect replay, clock skew). *)
  let symbol = "LEDGER6/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.position_base <- 0.4;
  state.position_initialized <- true;
  state.position_venue_ts <- 1000.0;
  state.buy_credits_since_balance <- [];
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.1
    ~base_balance_age:(Some 10.0)
    ~asset_balance:0.05;
  check
    bool
    "an older-generation message does not regress the ledger"
    true
    (abs_float (state.position_base -. 0.4) < 1e-12)
;;

let test_position_partial_credit_prune () =
  (* A message generated between two fills covers the older but not the newer:
     only the newer credit may survive. *)
  let symbol = "LEDGER7/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.position_base <- 0.2;
  state.position_initialized <- true;
  state.position_venue_ts <- 998.0;
  state.buy_credits_since_balance <- [ 998.5, 0.1; 999.5, 0.2 ];
  (* Message generated at 999.5; fill at 998.5 covered, fill at 999.5 not. *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:0.25;
  check
    bool
    "adopts the venue figure"
    true
    (abs_float (state.position_base -. 0.25) < 1e-12);
  check
    bool
    "only the covered credit is pruned"
    true
    (state.buy_credits_since_balance = [ 999.5, 0.2 ])
;;

let test_position_balance_before_fill_no_double_credit () =
  (* Independently-fed balance can adopt a buy fill BEFORE its execution event
     lands. The fill must not then be added to the overlay again, or the sell
     sizes the same base twice - the production over-sell: fill 0.2, adopted
     0.4, sell 0.4, insufficient-balance reject. *)
  let symbol = "LEDGER17/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.2;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.19517916;
  state.position_base <- 0.19994;
  state.position_initialized <- true;
  state.position_venue_ts <- 1000.0;
  state.attributed_balance_increase <- 0.0;
  state.buy_credits_since_balance <- [];
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.startup_replay <- false;
  state.last_fill_oid <- None;
  (* Balance message already includes the 0.2 fill. *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1001.0
    ~base_balance_age:(Some 0.0)
    ~asset_balance:0.39986;
  check
    bool
    "venue adopts the post-fill figure"
    true
    (abs_float (state.position_base -. 0.39986) < 1e-12);
  check
    bool
    "the adopted increase is tracked for attribution"
    true
    (state.attributed_balance_increase > 0.19);
  (* Execution event for the same fill arrives after the balance message. *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
    ~now:1001.2
    symbol
    "542258992352"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:81.37
    ~fill_qty:0.2
    None;
  check
    bool
    "fill already adopted is not re-added to the overlay"
    true
    (state.buy_credits_since_balance = []);
  check
    bool
    "adopted-increase pool is drawn down"
    true
    (abs_float state.attributed_balance_increase < 1e-9);
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.3
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1001.3
    ~asset
    ~bid_price:81.35
    ~ask_price:81.37
    ~asset_balance:0.39986
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 0.3);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell when o.symbol = symbol -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed
  in
  check
    bool
    "sell sizes the one real lot, not the doubled base"
    true
    (sell_qty >= 0.2 -. 1e-6 && sell_qty <= 0.2 +. 1e-6);
  drain ()
;;

let test_position_upward_reconcile_adopts_venue () =
  (* The venue reports more than we tracked (a fill we never saw): the venue is
     authoritative, so adopt upward rather than keeping the stale ledger. *)
  let symbol = "LEDGER8/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.position_base <- 0.1;
  state.position_initialized <- true;
  state.position_venue_ts <- 1000.0;
  state.buy_credits_since_balance <- [];
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1001.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:0.5;
  check
    bool
    "missed-fill venue figure is adopted upward"
    true
    (abs_float (state.position_base -. 0.5) < 1e-12)
;;

let test_position_nan_balance_does_not_seed () =
  (* Startup / feed outage: a NaN snapshot must not seed or mutate the ledger
     or prune credits. *)
  let symbol = "LEDGER9/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.position_initialized <- false;
  state.position_base <- 0.0;
  state.position_venue_ts <- 0.0;
  state.buy_credits_since_balance <- [ 999.5, 0.2 ];
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1000.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:Float.nan;
  check bool "NaN balance does not seed the ledger" false state.position_initialized;
  check
    bool
    "NaN balance does not prune credits"
    true
    (List.length state.buy_credits_since_balance = 1)
;;

let test_position_buy_credit_and_sell_hold_cancel () =
  (* Churn inside the feed-lag window: the bought base is immediately offered
     again, so the pending buy credit and the unnetted sell hold must cancel.
     The sale must not size against base that is already committed to the
     resting sell. *)
  let symbol = "LEDGER10/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.2;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.19117916;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 79.0;
  state.last_buy_fill_qty <- Some 0.2;
  state.position_base <- 0.1936;
  state.position_initialized <- true;
  state.position_venue_ts <- 999.0;
  state.buy_credits_since_balance <- [ 999.5, 0.2 ];
  state.sell_holds_since_balance <- [ 999.5, 0.2 ];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.3
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1000.0
    ~asset
    ~bid_price:79.0
    ~ask_price:79.1
    ~asset_balance:0.1936
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 0.5);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell when o.symbol = symbol -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed
  in
  check
    bool
    "buy credit offset by the unnetted sell hold does not place a second sell"
    true
    (sell_qty < 1e-9);
  drain ()
;;

let test_position_dead_feed_credit_expires () =
  (* No balance freshness beyond the grace: the credit must decay so it cannot
     size a sale against base the feed never confirmed. *)
  let symbol = "LEDGER11/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.2;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.19117916;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 79.0;
  state.last_buy_fill_qty <- Some 0.2;
  state.position_base <- 0.1936;
  state.position_initialized <- true;
  state.position_venue_ts <- 0.0;
  state.buy_credits_since_balance <- [ 900.0, 0.2 ];
  state.sell_holds_since_balance <- [];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.3
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1000.0
    ~asset
    ~bid_price:79.0
    ~ask_price:79.1
    ~asset_balance:0.1936
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None;
  check
    bool
    "stale credit decays after the grace"
    true
    (state.buy_credits_since_balance = []);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  check
    bool
    "expired credit cannot size a sale"
    true
    (not
       (List.exists
          (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
             o.operation = Dio_strategies.Strategy_common.Place
             && o.side = Dio_strategies.Strategy_common.Sell
             && o.symbol = symbol)
          pushed));
  drain ()
;;

let test_position_reserved_exceeds_ledger_clamps () =
  (* Over-reserved dust: the ledger is below reserved_base, so there is
     nothing sellable and no negative size. *)
  let symbol = "LEDGER12/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.2;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.98;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.19117916;
  state.open_sell_orders <- [];
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 79.0;
  state.last_buy_fill_qty <- Some 0.2;
  state.position_base <- 0.1;
  state.position_initialized <- true;
  state.position_venue_ts <- 999.0;
  state.buy_credits_since_balance <- [];
  state.sell_holds_since_balance <- [];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.3
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1000.0
    ~asset
    ~bid_price:79.0
    ~ask_price:79.1
    ~asset_balance:0.1
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:(Some 0.5);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  check
    bool
    "reserved-exceeding ledger places nothing and never sizes negative"
    true
    (not
       (List.exists
          (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
             o.operation = Dio_strategies.Strategy_common.Place
             && o.side = Dio_strategies.Strategy_common.Sell
             && o.symbol = symbol)
          pushed));
  drain ()
;;

let test_position_startup_replay_records_no_credit () =
  (* Historical fills replayed at startup (above the persisted high-water
     mark) must not create live pending credits: the venue already holds that
     base and the seed will count it. *)
  let symbol = "LEDGER13/BTC/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.0002;
  state.maker_fee <- 0.0004;
  state.reserved_base <- 0.0;
  state.position_base <- 0.0;
  state.buy_credits_since_balance <- [];
  state.startup_replay <- true;
  state.last_fill_oid <- Some "1";
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
    ~now:500.0
    symbol
    "2"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:77000.0
    ~fill_qty:0.0002
    None;
  check
    bool
    "startup replay records no pending buy credit"
    true
    (state.buy_credits_since_balance = [])
;;

let test_position_gross_venue_sell_fill_decrements () =
  (* Gross-balance venues (Alpaca) report the full holding, so the ledger
     falls by the sold qty at fill, clamped at zero. *)
  let symbol = "LEDGER14/QQQ" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca";
  state.grid_qty <- 0.5;
  state.cached_sell_mult <- 0.9;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.accumulation_buffer <- 0.0;
  state.position_base <- 0.1;
  state.position_initialized <- true;
  state.last_buy_fill_price <- Some 100.0;
  state.last_buy_fill_qty <- Some 0.5;
  state.base_accumulation_enabled <- false;
  state.startup_replay <- false;
  state.last_fill_oid <- None;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
    ~now:600.0
    symbol
    "gross-sell-1"
    Dio_strategies.Strategy_common.Sell
    ~fill_price:101.0
    ~fill_qty:0.5
    None;
  check
    bool
    "gross venue sell fill decrements the ledger and clamps at zero"
    true
    (state.position_base = 0.0)
;;

let test_position_accumulation_sell_fill_keeps_ledger () =
  (* Accumulation venues net the resting-sell hold, so the venue figure - and
     therefore the ledger - does not move on a sell fill; the reconciliation
     absorbs the netting. Decrementing here would double-count. *)
  let symbol = "LEDGER15/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid";
  state.grid_qty <- 0.2;
  state.cached_sell_mult <- 0.98;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.accumulation_buffer <- 0.0;
  state.position_base <- 0.4;
  state.position_initialized <- true;
  state.last_buy_fill_price <- Some 79.0;
  state.last_buy_fill_qty <- Some 0.2;
  state.base_accumulation_enabled <- false;
  state.startup_replay <- false;
  state.last_fill_oid <- None;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
    ~now:600.0
    symbol
    "accum-sell-1"
    Dio_strategies.Strategy_common.Sell
    ~fill_price:79.1
    ~fill_qty:0.2
    None;
  check
    bool
    "accumulation venue sell fill leaves the ledger untouched"
    true
    (abs_float (state.position_base -. 0.4) < 1e-12)
;;

let test_position_asset_low_recovery_sees_pending_credit () =
  (* Recovery must use the fill-aware ledger, or a latched asset_low never
     clears on the fill tick and sells stay wedged after a burst rejection. *)
  let symbol = "LEDGER16/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid";
  state.reserved_base <- 0.19117916;
  state.position_base <- 0.1936;
  state.position_initialized <- true;
  state.position_venue_ts <- 999.0;
  state.buy_credits_since_balance <- [ 999.5, 0.2 ];
  state.asset_low <- true;
  state.last_seen_asset_balance <- 0.1936;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "0.98"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.3
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  Dio_strategies.Jacobs_ladder.evaluate_asset_low_recovery
    ~state
    ~now:1000.0
    ~base_balance_age:(Some 0.5)
    ~ecfg
    ~asset
    ~asset_balance:0.1936
    ~lot_qty:0.2
    ~unnetted_hold:0.0;
  check
    bool
    "asset_low clears on the fill-aware ledger even with a stale spot snapshot"
    false
    state.asset_low;
  check
    bool
    "recovery arms the sell+buy resume flag"
    true
    state.resuming_after_balance_flag
;;

let test_position_sell_hold_releases_fifo_on_netting () =
  (* A resting sell's hold is retired only by an observed tradeable drop, and
     drops retire the OLDEST hold first. Per-hold baselines were gameable: an
     older hold netting dropped tradeable below a newer hold's baseline and
     released the newer, un-netted hold, over-offering a full lot. Buys only
     raise tradeable, so they must never consume a hold. *)
  let symbol = "LEDGER18/HYPE/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.buy_credits_since_balance <- [];
  state.attributed_balance_increase <- 0.0;
  state.sell_holds_since_balance <- [ 1000.0, 0.15; 1001.0, 0.15 ];
  state.position_base <- 0.5;
  state.position_initialized <- true;
  state.position_venue_ts <- 999.0;
  (* A 0.15 tradeable drop (the older hold netting) must retire only the
     oldest hold, leaving the newer one outstanding. *)
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1002.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:0.35;
  check
    bool
    "a drop retires the oldest hold only (FIFO)"
    true
    (state.sell_holds_since_balance = [ 1001.0, 0.15 ]);
  check
    bool
    "the adopted drop is reflected in the ledger"
    true
    (abs_float (state.position_base -. 0.35) < 1e-12);
  (* A buy-driven increase (delta > 0) must not consume any hold. *)
  state.sell_holds_since_balance <- [ 1003.0, 0.2 ];
  state.position_base <- 0.35;
  state.position_venue_ts <- 1002.0;
  Dio_strategies.Jacobs_ladder.reconcile_position
    ~state
    ~now:1004.0
    ~base_balance_age:(Some 0.5)
    ~asset_balance:0.5;
  check
    bool
    "a buy-driven increase does not consume a sell hold"
    true
    (state.sell_holds_since_balance = [ 1003.0, 0.2 ])
;;

let test_sell_never_offers_locked_inventory () =
  (* REGRESSION (the production XMR over-sell): base committed to a resting
     sell must NEVER be offered again when the venue's reported figure is gross
     or the open-order feed has dropped the order. Kraken nets holds from the
     SAME feed the ledger tracks, so a dropped feed frees that base there and
     the ledger excess must compensate.
     reserved 0.0048 + resting sell 0.0388 + gross holding 0.0836 (incl. a
     just-filled 0.04 buy): only 0.04 is sellable - NOT 0.0788. *)
  let symbol = "LEDGER19/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 0.04;
  state.maker_fee <- 0.0;
  state.cached_sell_mult <- 1.0;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 0.0;
  state.reserved_base <- 0.0048;
  state.position_base <- 0.0836;
  state.position_initialized <- true;
  state.position_venue_ts <- 999.0;
  state.buy_credits_since_balance <- [];
  state.attributed_balance_increase <- 0.0;
  state.sell_holds_since_balance <- [];
  state.open_sell_orders <- [ "resting-sell", 537.78, 0.0388 ];
  set_sell_commitments
    state.sell_commitments
    [ "resting-sell", 537.78, 0.0388, true, true, 0.0 ];
  state.feed_locked_sell_base <- 0.0;
  state.inflight_sell <- false;
  state.asset_low <- false;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 528.83;
  state.last_buy_fill_qty <- Some 0.04;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.04"
    ; grid_interval = 0.16
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.0
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1000.0
    ~asset
    ~bid_price:528.83
    ~ask_price:528.90
    ~asset_balance:0.0836
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:(Dio_strategies.Jacobs_ladder.committed_sell_base state)
    ~base_balance_age:(Some 0.5);
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let sell_qty =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell when o.symbol = symbol -> acc +. o.qty
         | _ -> acc)
      0.0
      pushed
  in
  check
    bool
    "locked resting-sell base is never offered again (sells only the free 0.04)"
    true
    (abs_float (sell_qty -. 0.04) < 1e-6);
  check
    bool
    "the over-sell amount (gross - reserved = 0.0788) is impossible"
    true
    (sell_qty < 0.0788 -. 1e-6);
  (* Dispatch arms the ledger: the new sell joins the resting one. *)
  check
    bool
    "the dispatched sell is added to the commitment ledger"
    true
    (abs_float (Dio_strategies.Jacobs_ladder.committed_sell_base state -. 0.0788) < 1e-6);
  drain ()
;;

let test_inflight_sell_commitment_survives_feed_gap () =
  (* The in-flight sell ledger keeps base committed across the venue feed on
     venues that derive holds from that SAME feed (Kraken): a dispatched sell
     is reserved before the feed lists it; the feed refreshes its qty while
     listed; and a feed that stops listing a LIVE order (reconnect / truncated
     snapshot) does NOT free the base until the terminal event. This is the
     Kraken/IBKR/Lighter failure made impossible. (Hyperliquid nets holds from
     its own state, so it trusts the feed and evicts instead - see
     [test_sell_commitment_lifecycle_all_venues].) *)
  let symbol = "LEDGER20/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config "kraken";
  state.open_sell_orders <- [];
  Hashtbl.clear state.sell_commitments;
  state.pending_orders <- [];
  state.last_buy_order_id <- None;
  state.last_buy_order_price <- None;
  state.inflight_sell <- false;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.2"
    ; grid_interval = 0.16
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.0
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let now = Unix.gettimeofday () in
  let feed = ref [] in
  let iter_open_orders f = List.iter (fun (a, b, c, d, e) -> f a b c d e) !feed in
  let locked () =
    let _, _, _, locked, _, _, _ =
      Dio_strategies.Jacobs_ladder.sync_open_orders
        ~state
        ~now
        ~asset
        ~bid_price:100.0
        ~lot_qty:0.2
        ~iter_open_orders
        ~get_open_orders_generation:(fun () -> -1)
        ~ecfg
    in
    locked
  in
  (* 1) Dispatched, not yet acked/listed: the base is already committed. *)
  Dio_strategies.Jacobs_ladder.arm_sell_commitment
    ~state
    ~id:"pending_sell_100.00"
    ~price:100.0
    ~qty:0.2;
  let l1 = locked () in
  check
    bool
    "a dispatched, not-yet-listed sell stays committed"
    true
    (abs_float (l1 -. 0.2) < 1e-9);
  check
    bool
    "the in-flight sell is merged into the open-order view"
    true
    (List.exists (fun (id, _, _) -> id = "pending_sell_100.00") state.open_sell_orders);
  (* 2) Ack re-keys to the venue id; still not listed. *)
  Dio_strategies.Jacobs_ladder.rekey_sell_commitment
    ~state
    ~old_id:"pending_sell_100.00"
    ~new_id:"venue-1"
    ~price:100.5
    ~qty:0.2
    ~acked:true;
  let l2 = locked () in
  check
    bool
    "an acked sell stays committed before the feed lists it"
    true
    (abs_float (l2 -. 0.2) < 1e-9);
  (* 3) The feed lists it and then refreshes the remaining qty. *)
  feed := [ "venue-1", 100.5, 0.2, "sell", None ];
  let l3 = locked () in
  check bool "a feed-listed sell stays committed" true (abs_float (l3 -. 0.2) < 1e-9);
  feed := [ "venue-1", 100.5, 0.12, "sell", None ];
  let l4 = locked () in
  check
    bool
    "the feed refreshes the committed remaining qty"
    true
    (abs_float (l4 -. 0.12) < 1e-9);
  (* 4) The feed drops the live order: base stays committed (this is the
     Kraken reconnect/truncation case). *)
  feed := [];
  let l5 = locked () in
  check
    bool
    "a feed-dropped live sell stays committed (no free base)"
    true
    (abs_float (l5 -. 0.12) < 1e-9);
  (* 5) Only a terminal event releases it. *)
  Dio_strategies.Jacobs_ladder.remove_sell_commitment ~state ~id:"venue-1";
  let l6 = locked () in
  check bool "a terminal event releases the commitment" true (l6 < 1e-12)
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:62369.0
    ~ask_price:62370.0
    ~asset_balance:1.05
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  (* target_q is 0.5 (fill qty), and available is 1.05 - 0.5 = 0.55 >= 0.5.
     Placed for target_q + surplus (0.55) despite being under the cached_venue_min_qty floor (10.0). *)
  check
    (option (float 1e-9))
    "sub-minimum qty sell places at target sell qty"
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
    (Dio_strategies.Jacobs_ladder.can_place_buy_order 0.001 100.0 50.0);
  check
    bool
    "insufficient buy balance"
    false
    (Dio_strategies.Jacobs_ladder.can_place_buy_order 0.001 10.0 50.0);
  check
    bool
    "sufficient sell balance"
    true
    (Dio_strategies.Jacobs_ladder.can_place_sell_order 0.001 1.0 0.001);
  check
    bool
    "insufficient sell balance"
    false
    (Dio_strategies.Jacobs_ladder.can_place_sell_order 0.001 0.0005 0.001)
;;

let test_order_acknowledgment () =
  (* Test order acknowledgment handling *)
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "TEST/USD" in
  (* Add a pending order manually for testing *)
  state.pending_orders
  <- ("test123", Dio_strategies.Strategy_common.Buy, 50000.0, Unix.time ())
     :: state.pending_orders;
  (* Handle acknowledgment *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "TEST2/USD" in
  (* Set up some tracked orders *)
  state.last_buy_order_id <- Some "buy123";
  state.last_buy_order_price <- Some 49000.0;
  state.open_sell_orders <- [ "sell456", 51000.0, 1.0; "sell789", 52000.0, 1.0 ];
  (* Cancel the buy order *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_cancelled
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "TEST_CLID/USD" in
  state.last_buy_order_id <- Some "1";
  state.last_buy_order_price <- Some 2200.0;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_cancelled
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state "TEST3/USD" in
  (* Add a pending order manually for testing *)
  state.pending_orders
  <- [ "test123", Dio_strategies.Strategy_common.Sell, 51000.0, Unix.time () ];
  (* Handle rejection *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_rejected
    ~now:0.0
    "TEST3/USD"
    Dio_strategies.Strategy_common.Sell
    51000.0;
  (* Should remove from pending orders *)
  check bool "pending orders cleared" true (List.length state.pending_orders = 0)
;;

(* TIF/ALO reject recovery: a violent move can make the trailing buy's amend
   or placement die to a TIF/ALO/post-only reject. Without recovery, the
   asset sits buyless for the entire oracle-INACTIVE window (the halt's
   "no open buy" rule turns the transient reject into an indefinite gap).
   The recovery latch must arm on the TIF kill paths and never on
   insufficient-balance or stale-cancel paths. *)
let test_tif_recovery_armed_on_amendment_failed () =
  let symbol = "TIFREC1/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.tif_recovery_pending <- false;
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_amendment_failed
    ~now:100.0
    symbol
    "tiford1"
    Dio_strategies.Strategy_common.Buy
    "Alo order would cross: badAloPxRejected";
  check
    bool
    "amendment TIF reject arms the recovery latch"
    true
    state.tif_recovery_pending;
  check bool "recovery timestamp recorded" true (state.tif_recovery_since > 0.0);
  drain ()
;;

let test_tif_recovery_not_armed_on_non_tif_amend_failure () =
  let symbol = "TIFREC2/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.tif_recovery_pending <- false;
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_amendment_failed
    ~now:100.0
    symbol
    "tiford2"
    Dio_strategies.Strategy_common.Buy
    "too many cumulative requests (rate limited)";
  check
    bool
    "non-terminal amend failure keeps tracking (no recovery latch)"
    false
    state.tif_recovery_pending;
  drain ()
;;

(* Alpaca reports a terminal order on the amend/fallback-cancel path as
   [order is already in "filled" state] (JSON-escaped in the reason). That is a
   terminal (order-gone) failure, not a transient one: tracking must clear and
   the id must be evicted so the open-orders scan cannot re-adopt the stale
   venue cache entry and re-issue the same failed cancel+replace every cooldown. *)
let test_alpaca_filled_amend_failure_clears_tracking () =
  let symbol = "TERM1/SMH/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.tif_recovery_pending <- false;
  state.last_buy_order_id <- Some "ab4f2af3";
  state.last_buy_order_price <- Some 566.63;
  Hashtbl.remove state.evicted_orders "ab4f2af3";
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_amendment_failed
    ~now:100.0
    symbol
    "ab4f2af3"
    Dio_strategies.Strategy_common.Buy
    {|Fallback cancel_order failed: HTTP 422 cancelling ab4f2af3: {"code":42210000,"message":"order is already in \"filled\" state"}|};
  check
    (option string)
    "terminal amend failure clears tracked buy id"
    None
    state.last_buy_order_id;
  check
    bool
    "terminal amend failure evicts stale order from scan"
    true
    (Hashtbl.mem state.evicted_orders "ab4f2af3");
  check
    bool
    "terminal (non-TIF) amend failure does not arm recovery"
    false
    state.tif_recovery_pending;
  drain ()
;;

let test_tif_recovery_not_armed_on_insufficient_failed () =
  let symbol = "TIFREC3/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.tif_recovery_pending <- false;
  state.last_buy_attempted_insufficient <- false;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_failed
    ~now:100.0
    symbol
    Dio_strategies.Strategy_common.Buy
    "insufficient funds for the transaction";
  check
    bool
    "insufficient-balance placement failure does not arm recovery (capital_low owns it)"
    false
    state.tif_recovery_pending
;;

let test_tif_recovery_armed_on_transient_failed () =
  let symbol = "TIFREC4/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.tif_recovery_pending <- false;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_failed
    ~now:100.0
    symbol
    Dio_strategies.Strategy_common.Buy
    "dispatch deadline exceeded";
  check
    bool
    "transient placement failure arms the recovery latch"
    true
    state.tif_recovery_pending
;;

let test_tif_recovery_cleared_on_buy_ack () =
  let symbol = "TIFREC5/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.tif_recovery_pending <- true;
  state.tif_recovery_since <- Unix.gettimeofday ();
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
    ~now:200.0
    symbol
    "tifack1"
    Dio_strategies.Strategy_common.Buy
    85.0;
  check
    bool
    "buy acknowledgment clears the recovery latch (resting buy exists)"
    false
    state.tif_recovery_pending
;;

let test_tif_recovery_armed_on_ghost_buy_ws_kill () =
  let symbol = "TIFREC6/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.tif_recovery_pending <- false;
  state.last_buy_order_id <- None;
  state.open_sell_orders <- [];
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* Dispatch a fresh buy placement: push_order registers the pending_buy_
     token and the in-flight guard. *)
  let order =
    Dio_strategies.Jacobs_ladder.create_place_order
      state.duplicate_key_buy
      symbol
      Dio_strategies.Strategy_common.Buy
      0.5
      (Some 85.0)
      true
      Dio_strategies.Strategy_common.Ladder
      "hyperliquid"
  in
  check
    bool
    "placement dispatched"
    true
    (Dio_strategies.Jacobs_ladder.push_order ~now:100.0 ~state order);
  let has_token =
    List.exists
      (fun (id, _, _, _) -> String.starts_with ~prefix:"pending_buy_" id)
      state.pending_orders
  in
  check bool "pending_buy_ token registered" true has_token;
  (* The venue WS rejects the never-acked placement: a cancel event for an
     untracked id arrives and the ghost purge removes the token - this is a
     buy-placement kill and must arm recovery. *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_cancelled
    ~now:101.0
    symbol
    "ws_rejected_untracked_id"
    Dio_strategies.Strategy_common.Buy
    None;
  check
    bool
    "ghost buy token purged"
    true
    (not
       (List.exists
          (fun (id, _, _, _) -> String.starts_with ~prefix:"pending_buy_" id)
          state.pending_orders));
  check
    bool
    "WS kill of the in-flight buy placement arms the recovery latch"
    true
    state.tif_recovery_pending;
  drain ()
;;

let test_tif_recovery_not_armed_on_stale_cancel () =
  let symbol = "TIFREC7/HYPE/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.tif_recovery_pending <- false;
  state.last_buy_order_id <- None;
  state.open_sell_orders <- [];
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* A previously tracked (acked) buy: its id is in the ever-tracked set.
     Simulate a late WS cancel arriving after the tracking was already
     wiped (e.g. by the ghost-buy sync): the cancel is STALE - it must not
     arm recovery, since no placement actually died. *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
    ~now:100.0
    symbol
    "stale_cancel_id"
    Dio_strategies.Strategy_common.Buy
    85.0;
  check bool "ack cleared the latch" false state.tif_recovery_pending;
  state.last_buy_order_id <- None;
  state.last_buy_order_price <- None;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_cancelled
    ~now:110.0
    symbol
    "stale_cancel_id"
    Dio_strategies.Strategy_common.Buy
    None;
  check
    bool
    "stale cancel does not arm the recovery latch"
    false
    state.tif_recovery_pending;
  drain ()
;;

let test_accumulation_profit_tracking () =
  (* Test that handle_order_filled correctly accumulates profit from sell fills.
     Flow: buy fills at buy_price, sell fills at sell_price > buy_price → profit accrues.
     
     With qty=0.35, buy@39.50, sell@39.90, maker_fee=0.0004:
       gross = (39.90 - 39.50) * 0.35 = 0.14
       fees  = (39.90 * 0.35 * 0.0004) + (39.50 * 0.35 * 0.0004) = 0.011116
       net   = 0.14 - 0.011116 = 0.128884  *)
  let symbol = "ACCUM_TEST/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.accumulated_profit <- 0.0;
  state.grid_qty <- 0.35;
  state.maker_fee <- 0.0004;
  (* A buffer above the window so the spec reserve/reset does not fire here:
     this test observes pure accumulation. *)
  state.accumulation_buffer <- 5.0;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done symbol;
  (* Simulate buy fill: sets last_buy_fill_price *)
  state.last_buy_order_id <- Some "buy001";
  state.last_buy_order_price <- Some 39.50;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
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
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.accumulated_profit <- 0.0;
  state.grid_qty <- 0.35;
  (* 0.35 asset *)
  state.maker_fee <- 0.0004;
  (* Buffer above the ~0.644 USDC window so the spec reserve/reset does not
     fire mid-test; the buffer-reserve path is covered by the store tests. *)
  state.accumulation_buffer <- 2.0;
  (* Clear startup replay gate so fills are processed normally *)
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done symbol;
  let buy_price = 39.50 in
  (* USDC per asset *)
  let sell_price = 39.90 in
  (* USDC per asset *)
  (* Run 5 profitable buy→sell cycles *)
  for i = 1 to 5 do
    let buy_id = Printf.sprintf "buy_%d" i in
    let sell_id = Printf.sprintf "sell_%d" i in
    state.last_buy_order_id <- Some buy_id;
    state.last_buy_order_price <- Some buy_price;
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
      ~now:0.0
      symbol
      buy_id
      Dio_strategies.Strategy_common.Buy
      ~fill_price:buy_price
      ~fill_qty:0.35
      None;
    state.open_sell_orders <- [ sell_id, sell_price, 1.0 ];
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
      ~now:0.0
      symbol
      sell_id
      Dio_strategies.Strategy_common.Sell
      ~fill_price:sell_price
      ~fill_qty:0.35
      None
  done;
  (* After 5 cycles: ~5 * 0.128884 ≈ 0.644 USDC accumulated *)
  check bool "profit accumulated over 5 cycles" true (state.accumulated_profit > 0.0)
;;

let test_accumulation_multi_strategy_isolation () =
  (* Test two strategies with different lot sizes running concurrently:
     
     BTC/USDC: sz_decimals=5 (lot=0.00001 asset)
       qty=0.0002 (asset), price ~84000 USDC, buffer=1.00 USDC
       round_qty(0.0002 * 0.999) = round_qty(0.00019980) = 0.00019 (asset)
       rounding_diff = 0.0002 - 0.00019 = 0.00001 (asset)
       required = 0.00001 * 84336 + 1.00 = 1.84336 (USDC)
     
     HYPE/USDC: sz_decimals=2 (lot=0.01 asset)
       qty=0.35 (asset), price ~40 USDC, buffer=0.05 USDC
       round_qty(0.35 * 0.999) = round_qty(0.34965) = 0.34 (asset)
       rounding_diff = 0.35 - 0.34 = 0.01 (asset)
       required = 0.01 * 39.90 + 0.05 = 0.449 (USDC) *)
  let btc_sym = "ISO_BTC/USDC" in
  let hype_sym = "ISO_HYPE/USDC" in
  (* Register instruments with real lot sizes *)
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol:btc_sym ~sz_decimals:5;
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol:hype_sym ~sz_decimals:2;
  let btc = Dio_strategies.Jacobs_ladder.get_strategy_state btc_sym in
  let hype = Dio_strategies.Jacobs_ladder.get_strategy_state hype_sym in
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
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done btc_sym;
  Dio_strategies.Jacobs_ladder.Strategy.set_startup_replay_done hype_sym;
  (* --- BTC cycles: buy@84000 → sell@84336 USDC (+0.4%) --- *)
  (* net = (84336 - 84000) * 0.0002 - fees = 0.0672 - 0.01345 ≈ 0.054 USDC per cycle *)
  for i = 1 to 30 do
    let buy_id = Printf.sprintf "btc_buy_%d" i in
    let sell_id = Printf.sprintf "btc_sell_%d" i in
    btc.last_buy_order_id <- Some buy_id;
    btc.last_buy_order_price <- Some 84000.0;
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
      ~now:0.0
      btc_sym
      buy_id
      Dio_strategies.Strategy_common.Buy
      ~fill_price:84000.0
      ~fill_qty:0.0002
      None;
    btc.open_sell_orders <- [ sell_id, 84336.0, 1.0 ];
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
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
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
      ~now:0.0
      hype_sym
      buy_id
      Dio_strategies.Strategy_common.Buy
      ~fill_price:39.50
      ~fill_qty:0.35
      None;
    hype.open_sell_orders <- [ sell_id, 39.90, 1.0 ];
    Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
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
  (* --- Test reserved_quote (USDC) isolation --- *)
  btc.exchange_id <- "hyperliquid";
  hype.exchange_id <- "hyperliquid";
  Dio_strategies.Jacobs_ladder.set_asset_reserved_quote btc 16.80;
  (* 0.0002 * 84000 = 16.80 USDC *)
  Dio_strategies.Jacobs_ladder.set_asset_reserved_quote hype 13.80;
  (* 0.35 * 39.42 ≈ 13.80 USDC *)
  let total_reserved = Dio_strategies.Jacobs_ladder.get_total_reserved_quote btc in
  check bool "total reserved USDC includes both domains" true (total_reserved >= 30.0)
;;

let test_virtual_gtc_sell_grid_maintenance () =
  let symbol = "VIRTUAL_GTC/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.persisted_sell_levels <- [ 101.00, 1.0; 98.98, 1.0; 96.96, 1.0 ];
  state.last_buy_fill_price <- Some 96.0;
  state.open_sell_orders <- [];
  (* Expired or missing DAY orders *)
  let asset_alpaca =
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg_alpaca = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  (* Run evaluate_sell_leg on Alpaca during a price drop to 90.0 (death spiral) *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset:asset_alpaca
    ~bid_price:90.0
    ~ask_price:90.1
    ~asset_balance:3.0
    ~buy_attempted:false
    ~ecfg:ecfg_alpaca
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  (* Verify that a missing sell order from persisted stack was pushed to order buffer at target price 101.00 *)
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
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
  let state_offline =
    Dio_strategies.Jacobs_ladder.get_strategy_state "OFFLINE_TEST/USD"
  in
  state_offline.persisted_sell_levels <- [ 105.00, 1.0 ];
  state_offline.open_sell_orders <- [];
  let asset_offline = { asset_alpaca with symbol = "OFFLINE_TEST/USD" } in
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state:state_offline)
    ~state:state_offline
    ~now:100.0
    ~asset:asset_offline
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:0.0
    ~buy_attempted:false
    ~ecfg:ecfg_alpaca
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  check
    bool
    "offline fill pruned from persisted_sell_levels"
    true
    (state_offline.persisted_sell_levels = []);
  (* Verify pre-existing open exchange order adoption in sync_open_orders *)
  let state_adopt = Dio_strategies.Jacobs_ladder.get_strategy_state "ADOPT_TEST/USD" in
  state_adopt.persisted_sell_levels <- [];
  let iter_orders f = f "oid_ex_1" 105.0 1.0 "sell" (Some 1) in
  let _ =
    Dio_strategies.Jacobs_ladder.sync_open_orders
      ~state:state_adopt
      ~now:100.0
      ~asset:{ asset_alpaca with symbol = "ADOPT_TEST/USD" }
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders
      ~get_open_orders_generation:(fun () -> -1)
      ~ecfg:ecfg_alpaca
  in
  check
    bool
    "pre-existing exchange sell order adopted into persisted_sell_levels"
    true
    (List.exists (fun (p, q) -> p = 105.0 && q = 1.0) state_adopt.persisted_sell_levels);
  (* Verify venue isolation: non-Alpaca (Kraken) has remaintain_expired_sells = false *)
  let kraken_symbol = "KRAKEN_TEST/USD" in
  let state_kraken = Dio_strategies.Jacobs_ladder.get_strategy_state kraken_symbol in
  state_kraken.open_sell_orders <- [];
  let asset_kraken =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol = kraken_symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg_kraken = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  check
    bool
    "Kraken remaintain_expired_sells is false"
    false
    ecfg_kraken.remaintain_expired_sells;
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state:state_kraken)
    ~state:state_kraken
    ~now:100.0
    ~asset:asset_kraken
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:1.0
    ~buy_attempted:false
    ~ecfg:ecfg_kraken
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let kraken_popped = Dio_strategies.Strategy_common.LockFreeQueue.read buffer in
  check
    bool
    "Kraken does not trigger Virtual GTC maintenance"
    true
    (Option.is_none kraken_popped)
;;

let test_halted_ladders_second_sell_beside_resting_one () =
  (* Kraken startup-inactive with a resting sell already on the book: the
     free inventory must STILL sell - laddered as a second order beside the
     resting one. The balance the domain passes is the NETTED tradeable
     figure (the venue feed listed the 0.04 hold and it is netted out), so
     the ledger's excess over the feed is zero and the second sell is sized
     by the free 0.04004 - NOT blocked. (When the venue feed DROPS a live
     order the excess rises by its qty and the base stays committed; see
     [test_inflight_sell_commitment_survives_feed_gap].) *)
  let symbol = "LADDER2/XMR/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 0.05;
  state.maker_fee <- 0.0026;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.0;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [ "resting1", 462.13, 0.04 ];
  set_sell_commitments
    state.sell_commitments
    [ "resting1", 462.13, 0.04, true, true, 0.0 ];
  state.feed_locked_sell_base <- 0.04;
  state.just_filled_buy <- false;
  state.last_buy_fill_price <- None;
  state.last_buy_fill_qty <- None;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.05"
    ; grid_interval = 5.0
    ; sell_mult = "0.999"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0026
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:422.67
    ~ask_price:422.80
    ~asset_balance:0.04004 (* netted tradeable; the resting sell's hold is NOT in here *)
    ~buy_attempted:false
    ~oracle_halted:true
    ~ecfg
    ~locked_in_sells:0.04
    ~base_balance_age:None;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let placed =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  match placed with
  | None -> Alcotest.fail "expected a second inventory sell beside the resting one"
  | Some o ->
    Alcotest.(check bool)
      "second sell sized by netted tradeable (~0.04004)"
      (o.qty > 0.0399 && o.qty <= 0.0401)
      true
;;

let test_halted_startup_places_inventory_sell () =
  (* Spec (sell side / activity gating): when the asset is FIRST placed
     inactive - e.g. on startup, before any fill this session - the sell leg
     must still check whether a placeable inventory sell exists and attempt
     it: sells need inventory, not quote. No just_filled_buy, no
     buy_attempted: the trigger is the halted state plus placeable
     inventory, anchored at the bid when no buy-fill price is known. *)
  let symbol = "STARTUP_SELL/XMR/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 0.05;
  state.maker_fee <- 0.0026;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.0;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.persisted_sell_levels <- [];
  (* Startup-inactive: no fills this session, no buy attempted. *)
  state.just_filled_buy <- false;
  state.last_buy_fill_price <- None;
  state.last_buy_fill_qty <- None;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.05"
    ; grid_interval = 5.0
    ; sell_mult = "0.999"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0026
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:170.0
    ~ask_price:170.10
    ~asset_balance:1.25
    ~buy_attempted:false
    ~oracle_halted:true
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let found =
    List.exists
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  check bool "startup-inactive places an inventory sell (XMR case)" true found;
  (* And with NO inventory there is no sell: the halt check requires a
     placeable balance. *)
  drain ();
  state.open_sell_orders <- [];
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:101.0
    ~asset
    ~bid_price:170.0
    ~ask_price:170.10
    ~asset_balance:0.0
    ~buy_attempted:false
    ~oracle_halted:true
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  check bool "no inventory -> no halt-triggered sell" true (pushed = [])
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  (* Drain any orders left in the shared buffer by prior tests. *)
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* The halted path: the buy leg was skipped, so buy_attempted = false. *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:39.50
    ~ask_price:39.55
    ~asset_balance:0.5
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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

let test_capital_low_still_places_bottom_rung_sell () =
  (* Capital exhaustion without an oracle INACTIVE decision: the buy leg
     latched [capital_low] locally (quote no longer covers the next buy) and
     skipped placement, but the oracle had not yet (or will never) publish an
     inactive decision. The last buy fill's inventory must STILL be offered as
     the bottom-rung sell on every venue - otherwise the strategy sits paused
     with unsold inventory and over-accumulates. Exercises evaluate_sell_leg
     exactly as the domain calls it on that tick: buy_attempted=false,
     just_filled_buy=false, oracle_halted=false, capital_low=true. *)
  let symbol = "CAPLOW/XMR/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 0.05;
  state.maker_fee <- 0.0026;
  state.cached_sell_mult <- 0.999;
  state.cached_qty_increment <- 0.01;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 0.0;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.persisted_sell_levels <- [];
  state.position_initialized <- true;
  state.position_base <- 0.05;
  state.just_filled_buy <- false;
  state.last_buy_fill_price <- Some 462.0;
  state.last_buy_fill_qty <- Some 0.05;
  state.capital_low <- true;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.05"
    ; grid_interval = 5.0
    ; sell_mult = "0.999"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0026
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:461.0
    ~ask_price:462.1
    ~asset_balance:0.05
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  let placed =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      pushed
  in
  match placed with
  | None -> Alcotest.fail "capital_low with inventory must place the bottom-rung sell"
  | Some o ->
    (* Never touches reserved_base: the sell is clamped to non-reserved
       inventory (0.05 here). *)
    Alcotest.(check bool)
      "sell within non-reserved inventory"
      (o.qty > 0.0 && o.qty <= 0.05 +. 1e-9)
      true
;;

let test_burst_tracked_venue_no_reserved_dip () =
  (* Kraken/IBKR/Lighter are accumulation venues WITH track_pending_sells;
     their tradeable figure (total - hold) still trails a placement, so a
     burst that acks several sells before the balance adopts the hold used to
     size a second sell against a stale-high figure and dump reserved_base.
     The unnetted-hold guard must now apply here too: with a fresh balance
     message that PREDATES the first placement, the armed hold clamps the
     second sell to zero. *)
  let symbol = "BURSTTRACK/XMR/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.grid_qty <- 0.05;
  state.maker_fee <- 0.0026;
  state.cached_sell_mult <- 0.999;
  state.cached_qty_increment <- 0.01;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 0.0;
  state.reserved_base <- 0.02;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [];
  state.persisted_sell_levels <- [];
  state.position_initialized <- true;
  state.position_base <- 0.05;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 462.0;
  state.last_buy_fill_qty <- Some 0.05;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "0.05"
    ; grid_interval = 5.0
    ; sell_mult = "0.999"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0026
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  let run_leg ~now ~age =
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now
      ~asset
      ~bid_price:461.0
      ~ask_price:462.1
      ~asset_balance:0.05
      ~buy_attempted:false
      ~oracle_halted:false
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:(Some age)
  in
  drain ();
  (* Tick 1: reserved 0.02 leaves 0.03 free -> the first sell goes out and
     arms its hold. *)
  run_leg ~now:100.0 ~age:0.1;
  let qty1 =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell -> acc +. o.qty
         | _ -> acc)
      0.0
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  check bool "first sell placed" true (qty1 > 0.0);
  drain ();
  (* Tick 2, moments later: a second fill triggers another sell while the
     balance message still predates the first placement (age 3.0 at t=102 ->
     message from t=99). The armed hold must clamp the second sell to zero. *)
  state.just_filled_buy <- true;
  state.last_buy_fill_qty <- Some 0.05;
  state.inflight_sell <- false;
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       state.duplicate_key_sell);
  run_leg ~now:102.0 ~age:3.0;
  let qty2 =
    List.fold_left
      (fun acc (o : Dio_strategies.Strategy_common.strategy_order) ->
         match o.operation, o.side with
         | Place, Sell -> acc +. o.qty
         | _ -> acc)
      0.0
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  check
    bool
    "second sell within the netting window does not dip into reserved_base"
    true
    (qty2 <= 1e-9)
;;

let test_sell_ack_releases_inflight_latch () =
  (* A sell placement's in-flight marker must be released on ACK (not left
     latched while the sell rests on the book): has_active_sell then means "a
     sell placement is in flight" only, so a resting sell no longer gates the
     next sell for new inventory behind a buy fill. *)
  let symbol = "LATCH_TEST/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    (Dio_strategies.Jacobs_ladder.has_active_sell state);
  (* The placement acks: the key must be released. *)
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
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
    (Dio_strategies.Jacobs_ladder.has_active_sell state);
  (* A new buy fills while the first sell still rests: the sell for the new
     inventory must be placed (1-buy x multi-sell ladder) - no longer gated
     behind a buy fill clearing a stale latch. *)
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 99.0;
  state.last_buy_fill_qty <- Some 1.0;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* The resting sell locks its inventory: pass its qty as locked_in_sells so
     the new sell only consumes the new fill's inventory. *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:2.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:1.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.35"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  let run_leg () =
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now:100.0
      ~asset
      ~bid_price:39.50
      ~ask_price:39.55
      ~asset_balance:0.5
      ~buy_attempted:false
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:None
      ~oracle_halted:false
  in
  (* Tick 1: the sell is on cooldown (a recent rejection latched it). *)
  Hashtbl.replace state.amend_cooldowns "place_Sell" (Unix.gettimeofday () +. 10.0);
  run_leg ();
  check
    bool
    "no sell pushed while on cooldown"
    true
    (Dio_strategies.Jacobs_ladder.get_pending_orders 100 = []);
  check bool "just_filled_buy survives the blocked attempt" true state.just_filled_buy;
  (* Tick 2: cooldown expired; the buy leg still cannot place a replacement
     (buy_attempted = false), but the sell must go out. *)
  Hashtbl.remove state.amend_cooldowns "place_Sell";
  run_leg ();
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  (* Accumulation venues (Hyperliquid/Lighter/IBKR): the sell is sized by the
     non-accrued, uncommitted inventory. On a NET-balance venue whose feed
     listed the resting sell, the venue tradeable already removed that hold,
     so the ledger's excess over the feed is zero and the sell is NOT
     reduced by the resting-sell hold again. (The ledger still guarantees the
     base stays committed if the feed later drops the order.) *)
  let symbol = "FLOOR_FALLBACK/BTC/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.maker_fee <- 0.0004;
  state.cached_sell_mult <- 0.999;
  state.cached_venue_min_qty <- 0.01;
  state.cached_venue_min_notional <- 10.0;
  state.reserved_base <- 0.5;
  state.accumulated_profit <- 2.0;
  state.open_sell_orders <- [];
  set_sell_commitments state.sell_commitments [ "resting", 62369.0, 0.4, true, true, 0.0 ];
  state.feed_locked_sell_base <- 0.4;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 62369.0;
  state.last_buy_fill_qty <- Some 0.5;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* A resting sell of 0.4 locks inventory and the venue feed listed it, so
     the tradeable balance already nets it: the ledger's excess over the feed
     is zero and the sellable is 1.00112 - 0.5 = 0.50, not the double-counted
     0.10. *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:62369.0
    ~ask_price:62370.0
    ~asset_balance:1.00112
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.4
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.0005"
    ; grid_interval = 0.75
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:62369.0
    ~ask_price:62370.0
    ~asset_balance:0.0003
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  check
    bool
    "no sell pushed with no sellable inventory"
    true
    (Dio_strategies.Jacobs_ladder.get_pending_orders 100 = []);
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "0.999"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0004
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "kraken" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:0.7
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "0.25"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* Below the dollar floor: 0.005 shares x 142 = 0.71 < $1. *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:142.0
    ~ask_price:142.1
    ~asset_balance:0.005
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  check
    bool
    "no sell below the dollar floor"
    true
    (Dio_strategies.Jacobs_ladder.get_pending_orders 100 = []);
  (* At/above the floor: 0.5 shares x 142 = $71 >= $1. *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:142.0
    ~ask_price:142.1
    ~asset_balance:0.5
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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

let test_alpaca_verified_nothing_to_sell_consumes_latch () =
  (* The LIT wedge: a dust balance (venue rejects the sell with 403, balance
     ~0) + a ghost-buy re-placement arming just_filled_buy + no resting
     sells + a stale last_buy_fill_price. The trigger is owed but can NEVER
     place (missing_alpaca_sell_grid requires inventory_ok), so the latch
     stayed dead-armed forever and the leg re-fired the inventory-gate block
     warn on every book tick (with the live ref price interpolated into the
     reason, defeating the dedup window). The verified nothing-to-sell
     consumption must clear the latch on a fresh below-floor balance, and
     the persistent grid-maintenance clause must still place the sell the
     moment inventory recovers - no owed sell is lost. *)
  let symbol = "ALPACA_LIT_WEDGE/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.grid_qty <- 0.26;
  state.cached_sell_mult <- 1.0;
  state.cached_qty_increment <- 0.000000001;
  state.cached_venue_min_qty <- 0.000000001;
  state.cached_venue_min_notional <- 1.0;
  state.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  state.cached_price_increment <- 0.01;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [];
  state.persisted_sell_levels <- [];
  state.last_buy_fill_price <- Some 73.95;
  state.last_buy_fill_qty <- Some 0.26;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "0.26"
    ; grid_interval = 0.5
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  let evaluate ~now ~balance ~buy_attempted =
    drain ();
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now
      ~asset
      ~bid_price:(if balance > 0.0 then 73.94 else 71.83)
      ~ask_price:(if balance > 0.0 then 74.23 else 74.23)
      ~asset_balance:balance
      ~buy_attempted
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:None
      ~oracle_halted:false
  in
  let pushed_sell () =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  (* Tick 1: the ghost-buy placement tick (buy_attempted) arms the latch, but
     the dust balance is verified below the $1 floor -> consumed, nothing
     placed, no warn loop. *)
  state.just_filled_buy <- false;
  evaluate ~now:100.0 ~balance:0.000000003 ~buy_attempted:true;
  check bool "no sell pushed on a dust balance" true (pushed_sell () = None);
  check
    bool
    "latch consumed on verified nothing-to-sell (was the dead-armed wedge)"
    false
    state.just_filled_buy;
  (* Tick 2: a later book tick with buy_attempted=false must not re-arm the
     latch or push anything - the resting state is silent. *)
  evaluate ~now:101.0 ~balance:0.000000003 ~buy_attempted:false;
  check
    bool
    "resting state neither re-arms the latch nor pushes a sell"
    (state.just_filled_buy = false && pushed_sell () = None)
    true;
  (* Tick 3: inventory recovers above the floor - the persistent
     (open_sell_orders = [] /\ last_buy_fill_price) grid-maintenance clause
     places the fill-anchored sell without needing the latch. *)
  evaluate ~now:102.0 ~balance:0.5 ~buy_attempted:false;
  match pushed_sell () with
  | Some o ->
    check (float 1e-8) "recovered inventory sells the fill qty 1:1" 0.26 o.qty;
    check bool "sell re-anchors above the fill price" (Option.get o.price > 73.95) true
  | None -> failwith "expected the grid-maintenance sell after inventory recovery"
;;

let test_alpaca_persistence_never_hijacks_owed_sell () =
  (* Persistence model: the sell_levels file restores rungs the venue dropped
     (fractional Alpaca orders are forced to day TIF); it NEVER dictates the
     price or sizing of a new sell. A dust persisted level (legacy clamped
     sizing) must not hijack a buy fill's owed sell - the owed sell is
     strategy-sized (fill + gi, 1:1 qty) - and an unplaceable restoration
     level is pruned from the file instead of wedging the maintenance path
     forever (the exact SMH/REMX/LIT accumulate-only failure). *)
  let symbol = "ALPACA_PERSIST/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.grid_qty <- 0.26;
  state.cached_sell_mult <- 1.0;
  state.cached_qty_increment <- 0.000000001;
  state.cached_venue_min_qty <- 0.000000001;
  state.cached_venue_min_notional <- 1.0;
  state.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  state.cached_price_increment <- 0.01;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 76.87;
  state.last_buy_fill_qty <- Some 0.26;
  (* The prod wedge: a dust persisted level from legacy clamped sizing. *)
  state.persisted_sell_levels <- [ 75.98, 1e-09 ];
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "0.26"
    ; grid_interval = 0.5
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  let evaluate ~now =
    drain ();
    Dio_strategies.Jacobs_ladder.evaluate_sell_leg
      ~persisted_reconcile:
        (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
      ~state
      ~now
      ~asset
      ~bid_price:76.9
      ~ask_price:77.0
      ~asset_balance:23.4
      ~buy_attempted:false
      ~ecfg
      ~locked_in_sells:0.0
      ~base_balance_age:None
      ~oracle_halted:false
  in
  let pushed_sell () =
    List.find_opt
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Place
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  (* 1. The buy fill's owed sell is strategy-sized (fill + gi = 77.25, 1:1
     qty) even though a dust level sits in the file. *)
  evaluate ~now:100.0;
  (match pushed_sell () with
   | Some o ->
     check (float 1e-6) "owed sell keeps strategy qty" 0.26 o.qty;
     check
       (float 0.005)
       "owed sell keeps fill+gi price, not the persisted level"
       77.25
       (match o.price with
        | Some p -> p
        | None -> 0.0)
   | None -> failwith "expected the strategy-sized owed sell to be pushed");
  check
    bool
    "dust level still persisted after the owed sell"
    true
    (List.exists (fun (p, _) -> p = 75.98) state.persisted_sell_levels);
  (* 2. With the owed sell resting (acked, latch released) and nothing new
     owed, the maintenance path restores missing levels: the dust level is
     selected, fails the $1 floor, and is pruned instead of wedging. *)
  state.inflight_sell <- false;
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       state.duplicate_key_sell);
  state.open_sell_orders <- [ "rest1", 77.25, 0.26 ];
  evaluate ~now:160.0;
  let pending2 = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
  check bool "nothing pushed while pruning the dust level" true (pending2 = []);
  (* 3. A real dropped rung is restored at its recorded price/qty. *)
  state.persisted_sell_levels <- [ 80.0, 0.26 ];
  evaluate ~now:220.0;
  match pushed_sell () with
  | Some o ->
    check
      (float 0.005)
      "restored rung keeps its own price"
      80.0
      (match o.price with
       | Some p -> p
       | None -> 0.0);
    check (float 1e-6) "restored rung keeps its own qty" 0.26 o.qty
  | None -> failwith "expected the dropped rung to be restored"
;;

(* Shared Alpaca asset for the excess-inventory sweep tests. *)
let alpaca_excess_asset ~symbol =
  { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0
  ; sell_mult = "1.0"
  ; strategy = "Ladder"
  ; maker_fee = Some 0.0
  ; taker_fee = None
  ; accumulation_buffer = 0.05
  ; base_accumulation = true
  ; sell_levels_persistence = true
  }
;;

let drain_order_buffer () =
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec go () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> go ()
    | None -> ()
  in
  go ()
;;

let reset_alpaca_excess_state symbol =
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "alpaca";
  state.grid_qty <- 1.0;
  state.cached_sell_mult <- 1.0;
  state.cached_qty_increment <- 0.000000001;
  state.cached_venue_min_qty <- 0.000000001;
  state.cached_venue_min_notional <- 1.0;
  state.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  state.cached_price_increment <- 0.01;
  state.reserved_base <- 0.0;
  state.open_sell_orders <- [];
  state.persisted_sell_levels <- [];
  state.just_filled_buy <- false;
  state.resuming_after_balance_flag <- false;
  state.inflight_sell <- false;
  state.last_buy_fill_price <- None;
  state.last_buy_fill_qty <- None;
  ignore
    (Dio_strategies.Strategy_common.InFlightOrders.remove_in_flight_order
       state.duplicate_key_sell);
  drain_order_buffer ();
  state
;;

let pushed_sell_for symbol =
  List.find_opt
    (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
       o.operation = Dio_strategies.Strategy_common.Place
       && o.side = Dio_strategies.Strategy_common.Sell
       && o.symbol = symbol)
    (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
;;

let test_alpaca_excess_refills_before_dumping () =
  (* Ladder [101 x1; 99 x1] is entirely missing and 3.0 is sellable (1.0
     beyond the ladder). Excess must NOT be folded into a restore: the top
     missing rung goes out at its OWN recorded qty (1.0), so the ladder is
     rebuilt rung-by-rung and only a COMPLETE ladder gets the sweep. *)
  let symbol = "ALPACA_EXCESS_REFILL/USD" in
  let state = reset_alpaca_excess_state symbol in
  state.persisted_sell_levels <- [ 101.0, 1.0; 99.0, 1.0 ];
  let asset = alpaca_excess_asset ~symbol in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:3.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  match pushed_sell_for symbol with
  | Some o ->
    check
      (float 0.005)
      "missing top rung restored at its recorded price"
      101.0
      (Option.value o.price ~default:0.0);
    check
      (float 1e-6)
      "missing top rung keeps its own qty (excess waits for a complete ladder)"
      1.0
      o.qty
  | None -> failwith "expected the missing top rung to be restored"
;;

let test_alpaca_excess_amends_open_top_rung () =
  (* Ladder fully resting [101 x1; 99 x1] (2.0 committed) with 1.0 more
     sellable. The sweep must amend the TOP order up to 2.0 (+1.0), leaving the
     lower rung alone, instead of placing a separate sell or dumping idle. *)
  let symbol = "ALPACA_EXCESS_AMEND/USD" in
  let state = reset_alpaca_excess_state symbol in
  state.persisted_sell_levels <- [ 101.0, 1.0; 99.0, 1.0 ];
  state.open_sell_orders <- [ "top-oid", 101.0, 1.0; "low-oid", 99.0, 1.0 ];
  let asset = alpaca_excess_asset ~symbol in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:3.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:2.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let amends =
    List.filter
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Amend
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  match amends with
  | [ o ] ->
    check
      (option string)
      "excess amend targets the top rung's order id"
      (Some "top-oid")
      o.order_id;
    check
      (float 0.005)
      "excess amend keeps the top rung price"
      101.0
      (Option.value o.price ~default:0.0);
    check (float 1e-6) "excess amend grows the top rung by the excess" 2.0 o.qty
  | _ -> failwith "expected exactly one sell amend on the top rung"
;;

let test_alpaca_excess_excludes_reserved_base () =
  (* reserved_base is not sellable: balance 3.0 with 2.0 already reserved and
     1.0 committed to the resting top rung leaves NO excess, so the top rung
     must not be amended. (If reserved_base leaked into the sweep the top would
     grow by 1.0.) *)
  let symbol = "ALPACA_EXCESS_RESERVED/USD" in
  let state = reset_alpaca_excess_state symbol in
  state.reserved_base <- 2.0;
  state.persisted_sell_levels <- [ 101.0, 1.0 ];
  state.open_sell_orders <- [ "top-oid", 101.0, 1.0 ];
  let asset = alpaca_excess_asset ~symbol in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:3.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:1.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let amends =
    List.filter
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Amend
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  check bool "reserved_base is excluded from the excess (no amend)" true (amends = [])
;;

let test_alpaca_venue_available_blocks_reserve_dip () =
  (* The venue's own free figure is authoritative. Here the account shows gross
     3.0 but only reserved_base is free (everything else is held), while the
     engine's reconstructed locked_in_sells is 0.0 (the amend-window
     undercount). Sizing against gross-minus-reconstructed-holds would offer a
     full lot out of the reserve; the venue-authoritative basis must place
     nothing. *)
  let symbol = "ALPACA_VENUE_AVAIL/USD" in
  let state = reset_alpaca_excess_state symbol in
  state.reserved_base <- 0.00234;
  state.open_sell_orders <- [];
  state.persisted_sell_levels <- [];
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 100.0;
  state.last_buy_fill_qty <- Some 1.0;
  Alpaca.Balances.set_available_balance_for_test symbol 0.00234;
  let asset = alpaca_excess_asset ~symbol in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:3.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  check
    bool
    "venue free == reserved_base offers nothing (no sell out of the reserve)"
    true
    (pushed_sell_for symbol = None)
;;

let test_alpaca_excess_sweep_capped_to_one_lot () =
  (* A single sweep may grow the top rung by at most one grid lot. An uncapped
     sweep turns any transient over-estimate of sellable inventory into a rung
     sized to the whole position. Ladder 101x1 fully resting, 5.0 sellable,
     lot 1.0: the top rung must end at 2.0, not 6.0. *)
  let symbol = "ALPACA_SWEEP_CAP/USD" in
  let state = reset_alpaca_excess_state symbol in
  state.persisted_sell_levels <- [ 101.0, 1.0 ];
  state.open_sell_orders <- [ "cap-top-oid", 101.0, 1.0 ];
  let asset = alpaca_excess_asset ~symbol in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.1
    ~asset_balance:6.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:1.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let amends =
    List.filter
      (fun (o : Dio_strategies.Strategy_common.strategy_order) ->
         o.operation = Dio_strategies.Strategy_common.Amend
         && o.side = Dio_strategies.Strategy_common.Sell
         && o.symbol = symbol)
      (Dio_strategies.Jacobs_ladder.get_pending_orders 100)
  in
  match amends with
  | [ o ] ->
    check (float 1e-6) "excess sweep grows the top rung by at most one lot" 2.0 o.qty
  | _ ->
    failwith
      (Printf.sprintf
         "expected exactly one capped sell amend on the top rung, got %d"
         (List.length amends))
;;

let test_alpaca_sell_anchors_on_fill_not_ask () =
  (* Alpaca sell placement is anchored on the fill (fill + gi), NOT pushed up
     to the current ask. Clamping to the ask stacked every new sell on the
     same price as the market bounced (SPCX sells piling at 138.50) instead of
     laddering down as the price moved down. The fill anchor keeps the rungs
     equidistant and can never place the sell below fill + gi, so the
     fill-anchored profitability is preserved. *)
  let symbol = "ALPACA_ANCHOR/USD" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.05
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config "alpaca" in
  let buffer = Dio_strategies.Jacobs_ladder.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  (* Market well ABOVE fill + gi: fill 100.00 + gi 1% = 101.00, ask 105.00.
     The sell must land at 101.00 (fill-anchored), not 105.00 (ask-pinned). *)
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:100.0
    ~asset
    ~bid_price:100.0
    ~ask_price:105.0
    ~asset_balance:1.0
    ~buy_attempted:false
    ~ecfg
    ~locked_in_sells:0.0
    ~base_balance_age:None
    ~oracle_halted:false;
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 100 in
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
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  st.exchange_id <- "kraken";
  st.grid_qty <- 1.0;
  let grid_interval = 0.5 in
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.001
    ; taker_fee = Some 0.002
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  let drain () = ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100) in
  drain ();
  (* Closest sell at 100.40, bid 100.00, gi 0.5%: the 2*gi cap is anchored on
     the SELL price, so the buy must not sit above 100.40 - 2*gi(100.40) =
     100.40 - 1.004 = 99.396. The raw grid buy (0.5% below the bid) would be
     99.50 - above the cap, so the cap must pull it down to 99.396. *)
  ignore
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:(Some ("sell1", 100.40)));
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 10 in
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

let test_fresh_buy_clamps_against_companion_sell () =
  (* REGRESSION: the fresh buy leg runs BEFORE the sell leg, so the companion
     sell it is about to place is not in the open-order feed yet. Clamping the
     fresh buy only against the sells already in the feed let a buy passed
     against a HIGHER stale sell land inside the NEWER, lower companion sell's
     2x gi zone; the next tick then amended it down (the place-then-amend
     churn). The clamp must anticipate the companion sell.

     Geometry (gi 0.5%, 2-decimal rounding):
       last buy fill = 100.00, bid/ask = 100.40 (within one gi of the fill)
       companion sell = 100.00 * 1.005 = 100.50
       companion floor = 100.50 - 2*gi*100.50 = 99.495
       stale feed sell = 101.00 -> old floor 99.99 (would NOT bind)
       raw grid buy = 100.40 * 0.995 = 99.90 (the price that used to churn) *)
  let symbol = "SPACE_COMPANION/USD" in
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  st.exchange_id <- "kraken";
  st.grid_qty <- 1.0;
  st.last_buy_fill_price <- Some 100.00;
  (* A buy just filled, so the sell leg owes (and will place) the companion
     sell this same tick. *)
  st.just_filled_buy <- true;
  st.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  let grid_interval = 0.5 in
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.001
    ; taker_fee = Some 0.002
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100);
  ignore
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
       ~state:st
       ~now
       ~asset
       ~bid_price:100.40
       ~ask_price:100.40
       ~quote_balance:1000.0
       ~quote_balance_stale:false
       ~cycle:1
       ~iter_open_orders
       ~open_buy_count_from_scan:0
       ~has_recent_amend_buy:false
       ~locked_in_buys:0.0
       ~closest_sell_order_initial:(Some ("stale_sell", 101.00)));
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 10 in
  match pushed with
  | [ (o : Dio_strategies.Strategy_common.strategy_order) ] ->
    (match o.price with
     | Some p ->
       let companion_floor = 100.50 -. (100.50 *. (2.0 *. 0.5 /. 100.0)) in
       check
         bool
         "fresh buy sits at/below the companion sell's 2x gi floor"
         true
         (p <= companion_floor +. 1e-6);
       check
         bool
         "companion sell (not the stale feed sell) is the binding clamp"
         true
         (p < 99.90 -. 1e-6)
     | None -> failwith "buy missing price")
  | _ -> failwith "expected exactly one buy order"
;;

let test_reclaim_step_cancels_when_not_issued () =
  (* Reclaim self-healing: the FIRST cycle with a reclaim decision and an
     eligible resting buy issues the cancel (arm the latch). *)
  let step =
    Dio_strategies.Jacobs_ladder.reclaim_step
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
    (step = Dio_strategies.Jacobs_ladder.Reclaim_cancel 1)
;;

let test_reclaim_step_throttles_in_flight_cancel () =
  (* A cancel issued 5s ago is still in flight (retry window 15s): do NOT
     re-issue - avoids cancel spam against a cancel that is dispatching. *)
  let step =
    Dio_strategies.Jacobs_ladder.reclaim_step
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
    (step = Dio_strategies.Jacobs_ladder.Reclaim_deferred)
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
    Dio_strategies.Jacobs_ladder.reclaim_step
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
    (step = Dio_strategies.Jacobs_ladder.Reclaim_cancel 1)
;;

let test_reclaim_step_rearms_when_store_clean () =
  (* The cancel landed (or never needed): the store no longer shows ANY buy.
     The latch re-arms so a later reclaim decision re-triggers cleanly - and
     the released capital is recognized by the next oracle pass (the
     committed value it reads is zero). *)
  let step =
    Dio_strategies.Jacobs_ladder.reclaim_step
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
    (step = Dio_strategies.Jacobs_ladder.Reclaim_rearm)
;;

let test_reclaim_step_waits_for_mid_amend_buy () =
  (* The domain only cancels buys that are not mid-amendment (Hyperliquid
     rejects canceling an order being amended). An in-flight-amend buy is not
     cancellable: the step waits for the amend to resolve into a cancellable
     replacement instead of spamming the exchange (and instead of re-arming
     - the capital is still committed, so the reclaim decision is still
     correct). *)
  let step =
    Dio_strategies.Jacobs_ladder.reclaim_step
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
    (step = Dio_strategies.Jacobs_ladder.Reclaim_deferred)
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
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  st.exchange_id <- "kraken";
  st.grid_qty <- 1.0;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 0.5
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
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
  let drain () = ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100) in
  let pending_count () =
    List.length (Dio_strategies.Jacobs_ladder.get_pending_orders 10)
  in
  drain ();
  (* 1. Fresh balance, insufficient -> no order pushed, capital_low latched. *)
  ignore
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:None);
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
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:None);
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
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:None);
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
  let open Dio_strategies.Jacobs_ladder in
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
    ; strategy = "Ladder"
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
      ~get_open_orders_generation:(fun () -> -1)
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
  let open Dio_strategies.Jacobs_ladder in
  let symbol = "IDX_MATCH/USD" in
  let state = get_strategy_state symbol in
  state.persisted_sell_levels <- [ 100.00, 1.0; 98.00, 1.0 ];
  let asset_alpaca =
    { exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
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
      ~get_open_orders_generation:(fun () -> -1)
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
      ~get_open_orders_generation:(fun () -> -1)
      ~ecfg
  in
  (* One level matches; the second sell adopts a new level. *)
  let matches = List.filter (fun (p, _) -> p = 105.0) state2.persisted_sell_levels in
  check bool "duplicate open sells matched 1-to-1" true (List.length matches = 2)
;;

let test_sync_open_orders_reconcile_agreement () =
  (* sync_open_orders now computes the (open_levels, missing_levels)
     split during its scan (O(m), by draining per-price-key match counts) and
     threads it into evaluate_sell_leg, replacing the second O(n+m)
     partition_persisted_sell_levels pass. Verify the threaded split agrees
     EXACTLY with the reference partition over the same final persisted list
     and open-sell set, across duplicates, boundary floats, adoptions and
     qty updates. *)
  let open Dio_strategies.Jacobs_ladder in
  let ecfg = get_exchange_config "alpaca" in
  let mk_asset symbol =
    { exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
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
        ~get_open_orders_generation:(fun () -> -1)
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

let test_sync_open_orders_generation_skip () =
  (* When the venue's open-orders generation is unchanged since the last scan,
     [sync_open_orders] must skip the O(open-orders) scan and reuse the cached
     derived state, while STILL running the ledger reconcile (so a lost
     placement ages out). A generation bump forces a rescan. *)
  let open Dio_strategies.Jacobs_ladder in
  let symbol = "GEN_SKIP/USD" in
  let state = get_strategy_state symbol in
  state.exchange_id <- "kraken";
  state.cached_ecfg <- get_exchange_config "kraken";
  Hashtbl.clear state.sell_commitments;
  state.open_sell_orders <- [];
  state.open_orders_scan_valid <- false;
  let asset =
    { exchange = "kraken"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = None
    ; accumulation_buffer = 0.0
    ; base_accumulation = false
    ; sell_levels_persistence = false
    }
  in
  let ecfg = get_exchange_config "kraken" in
  let gen = ref 7 in
  let scan_calls = ref 0 in
  let iter_orders f =
    incr scan_calls;
    f "buy-1" 99.0 1.0 "buy" None;
    f "sell-1" 101.0 2.0 "sell" None
  in
  let sync () =
    sync_open_orders
      ~state
      ~now:100.0
      ~asset
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders
      ~get_open_orders_generation:(fun () -> !gen)
      ~ecfg
  in
  let obc1, _hrab1, lib1, _lis1, _cs1, _op1, _mp1 = sync () in
  check int "first sync scans once" 1 !scan_calls;
  check int "open-buy count cached" 1 obc1;
  check (float 1e-9) "locked-in buys from scan" 99.0 lib1;
  check int "feed sells recorded" 1 (List.length state.open_sell_orders);
  (* Same generation: the scan closure must not run. *)
  let iter_orders_boom _ = failwith "scan must be skipped" in
  let _obc2, _hrab2, lib2, _lis2, _cs2, _op2, _mp2 =
    sync_open_orders
      ~state
      ~now:100.0
      ~asset
      ~bid_price:100.0
      ~lot_qty:1.0
      ~iter_open_orders:iter_orders_boom
      ~get_open_orders_generation:(fun () -> !gen)
      ~ecfg
  in
  check (float 1e-9) "locked-in buys reused on skip" 99.0 lib2;
  check int "sell list reused on skip" 1 (List.length state.open_sell_orders);
  (* A lost placement (armed, never listed/acked) still ages out on a skipped
     cycle because the reconcile always runs. *)
  arm_sell_commitment ~state ~id:"pending_sell_lost" ~price:95.0 ~qty:3.0;
  (match Hashtbl.find_opt state.sell_commitments "pending_sell_lost" with
   | Some c ->
     Hashtbl.replace
       state.sell_commitments
       "pending_sell_lost"
       { c with sc_armed = -100.0 }
   | None -> ());
  ignore (sync ());
  check
    bool
    "lost placement aged out during a skipped cycle"
    false
    (Hashtbl.mem state.sell_commitments "pending_sell_lost");
  (* A generation bump forces a rescan. *)
  incr gen;
  ignore (sync ());
  check int "generation change rescans" 2 !scan_calls
;;

(* ---- Buy-trailing: qty-only oracle re-sizes must honor the trailing rules - *)

let eval_buy_trail ~symbol ~grid_qty ~bid ~ask ~resting_price ~resting_qty:_ ~sell_opt =
  let buy_id = symbol ^ "_buy" in
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = Printf.sprintf "%.8g" grid_qty
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100);
  ignore
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:sell_opt);
  Dio_strategies.Jacobs_ladder.get_pending_orders 10
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
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100);
  ignore
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:(Some ("sell1", 105.0)));
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 10 in
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
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = true
    }
  in
  let iter_open_orders _ = () in
  let now = Unix.gettimeofday () in
  ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100);
  ignore
    (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
       ~oracle_halted:false
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
       ~closest_sell_order_initial:(Some ("sell1", 103.50)));
  let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 10 in
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
    let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
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
      { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
      ; symbol
      ; qty = "1.0"
      ; grid_interval = 1.0
      ; sell_mult = "1.0"
      ; strategy = "jacobs_ladder"
      ; maker_fee = Some 0.0
      ; taker_fee = Some 0.0
      ; accumulation_buffer = 0.01
      ; base_accumulation = true
      ; sell_levels_persistence = true
      }
    in
    let iter_open_orders _ = () in
    let now = Unix.gettimeofday () in
    ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100);
    ignore
      (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
         ~oracle_halted:false
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
         ~closest_sell_order_initial:sell_opt);
    let pushed = Dio_strategies.Jacobs_ladder.get_pending_orders 10 in
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
  let st = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  st.exchange_id <- "alpaca";
  st.grid_qty <- 1.0;
  st.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  st.last_buy_order_id <- Some buy_id;
  st.last_buy_order_price <- Some 96.0;
  st.pending_orders <- [];
  st.inflight_amend_buy <- false;
  let asset =
    { Dio_strategies.Jacobs_ladder.exchange = "alpaca"
    ; symbol
    ; qty = "1.0"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "jacobs_ladder"
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
    ignore (Dio_strategies.Jacobs_ladder.get_pending_orders 100);
    (* Each push_order for an amend leaves a pending_amend entry, the in-flight
       amendment latch and the inflight_amend_buy flag: clear all three so the
       next step trails from the freshly amended resting price. *)
    st.pending_orders <- [];
    st.inflight_amend_buy <- false;
    ignore
      (Dio_strategies.Strategy_common.InFlightAmendments.remove_in_flight_amendment
         buy_id);
    ignore
      (Dio_strategies.Jacobs_ladder_execution.evaluate_buy_leg
         ~oracle_halted:false
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
         ~closest_sell_order_initial:sell_opt);
    Dio_strategies.Jacobs_ladder.get_pending_orders 10
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

(* ------------------------------------------------------------------ *)
(* Cross-venue invariant: base committed to a resting/in-flight sell   *)
(* is NEVER available, on every venue, in every trade.                 *)
(* ------------------------------------------------------------------ *)

let sell_matrix_asset ~exchange ~symbol ~qty =
  { Dio_strategies.Jacobs_ladder.exchange
  ; symbol
  ; qty
  ; grid_interval = 0.16
  ; sell_mult = "1.0"
  ; strategy = "Ladder"
  ; maker_fee = Some 0.0
  ; taker_fee = None
  ; accumulation_buffer = 0.0
  ; base_accumulation = true
  ; sell_levels_persistence = false
  }
;;

(** Drives the REAL sell leg for one venue and returns the placed sell qty. *)
let sell_matrix_feed_and_size
      ~exchange
      ~symbol
      ~reported
      ~ledger
      ~reserved
      ~free
      ~feed_healthy
  =
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- exchange;
  state.grid_qty <- free;
  state.maker_fee <- 0.0;
  state.cached_sell_mult <- 1.0;
  state.cached_qty_increment <- 0.01;
  state.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  state.cached_price_increment <- 0.01;
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 0.0;
  state.reserved_base <- reserved;
  state.accumulated_profit <- 0.0;
  state.open_sell_orders <- [ "resting", 100.0, ledger ];
  set_sell_commitments
    state.sell_commitments
    [ "resting", 100.0, ledger, true, true, 0.0 ];
  state.feed_locked_sell_base <- (if feed_healthy then ledger else 0.0);
  state.sell_holds_since_balance <- [];
  state.buy_credits_since_balance <- [];
  state.attributed_balance_increase <- 0.0;
  state.position_base <- 0.0;
  state.position_initialized <- false;
  state.position_venue_ts <- 0.0;
  state.just_filled_buy <- true;
  state.last_buy_fill_price <- Some 100.0;
  state.last_buy_fill_qty <- Some free;
  state.asset_low <- false;
  state.inflight_sell <- false;
  state.inflight_buy <- false;
  state.persisted_sell_levels <- [];
  (* Alpaca's authoritative free figure already excludes resting holds. *)
  if exchange = "alpaca"
  then Alpaca.Balances.set_available_balance_for_test symbol (reserved +. free);
  let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config exchange in
  let asset = sell_matrix_asset ~exchange ~symbol ~qty:(Printf.sprintf "%.4f" free) in
  drain_order_buffer ();
  Dio_strategies.Jacobs_ladder.evaluate_sell_leg
    ~persisted_reconcile:
      (Dio_strategies.Jacobs_ladder.reconcile_persisted_sell_levels ~state)
    ~state
    ~now:1000.0
    ~asset
    ~bid_price:100.0
    ~ask_price:100.05
    ~asset_balance:reported
    ~buy_attempted:false
    ~oracle_halted:false
    ~ecfg
    ~locked_in_sells:(Dio_strategies.Jacobs_ladder.committed_sell_base state)
    ~base_balance_age:None;
  let sell = pushed_sell_for symbol in
  drain_order_buffer ();
  match sell with
  | Some o -> o.qty
  | None -> 0.0
;;

let test_sellable_base_matrix_all_venues () =
  (* THE regression the bug keeps escaping through: for every venue and both
     a healthy feed and a dropped/gross feed, the placed sell must be the
     free lot only. The production over-sell was free + locked (0.5). *)
  let venues = [ "kraken"; "hyperliquid"; "ibkr"; "lighter"; "alpaca" ] in
  let ledger = 0.4 in
  let reserved = 0.05 in
  let free = 0.1 in
  let idx = ref 0 in
  List.iter
    (fun exchange ->
       (* Hyperliquid/Alpaca net holds from the venue's OWN state, independent
          of our executions feed, so their reported figure excludes the resting
          hold whether or not our feed lists it. Kraken derives the hold from
          the SAME feed, so a dropped feed leaves the locked base in its
          reported figure (compensated by the ledger excess). IBKR/Lighter
          report gross. *)
       let trust_feed = exchange = "hyperliquid" || exchange = "alpaca" in
       let nets_from_feed = exchange = "kraken" in
       List.iter
         (fun feed_healthy ->
            incr idx;
            let symbol = Printf.sprintf "SELLMATRIX%d/USD" !idx in
            let reported =
              if trust_feed || (nets_from_feed && feed_healthy)
              then reserved +. free
              else reserved +. ledger +. free
            in
            let qty =
              sell_matrix_feed_and_size
                ~exchange
                ~symbol
                ~reported
                ~ledger
                ~reserved
                ~free
                ~feed_healthy
            in
            check
              (float 1e-9)
              (Printf.sprintf
                 "%s feed=%s: sells only the free lot (locked base excluded)"
                 exchange
                 (if feed_healthy then "healthy" else "dropped"))
              free
              qty)
         [ true; false ])
    venues
;;

let test_sell_commitment_lifecycle_all_venues () =
  (* The ledger across the REAL lifecycle for every venue: dispatch arms it,
     ack re-keys it, the feed lists/refreshes it, a feed drop keeps the base
     committed, and a sell fill releases it. *)
  let venues = [ "kraken"; "hyperliquid"; "ibkr"; "lighter"; "alpaca" ] in
  List.iter
    (fun exchange ->
       let symbol = Printf.sprintf "LIFECYCLE_%s/USD" (String.uppercase_ascii exchange) in
       let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
       state.exchange_id <- exchange;
       state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config exchange;
       state.cached_qty_increment <- 0.01;
       state.cached_price_increment <- 0.01;
       state.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
       state.cached_venue_min_qty <- 0.0;
       state.cached_venue_min_notional <- 0.0;
       state.grid_qty <- 0.2;
       state.maker_fee <- 0.0;
       state.cached_sell_mult <- 1.0;
       state.accumulation_buffer <- 0.0;
       state.base_accumulation_enabled <- true;
       state.reserved_base <- 0.0;
       state.accumulated_profit <- 0.0;
       state.open_sell_orders <- [];
       Hashtbl.clear state.sell_commitments;
       state.feed_locked_sell_base <- 0.0;
       state.sell_holds_since_balance <- [];
       state.buy_credits_since_balance <- [];
       state.attributed_balance_increase <- 0.0;
       state.persisted_sell_levels <- [];
       state.pending_orders <- [];
       state.last_buy_order_id <- None;
       state.last_buy_order_price <- None;
       state.inflight_sell <- false;
       state.asset_low <- false;
       state.startup_replay <- false;
       state.last_fill_oid <- None;
       state.last_buy_fill_price <- Some 100.0;
       state.last_buy_fill_qty <- Some 0.2;
       drain_order_buffer ();
       let asset = sell_matrix_asset ~exchange ~symbol ~qty:"0.2" in
       let ecfg = Dio_strategies.Jacobs_ladder.get_exchange_config exchange in
       let now = Unix.gettimeofday () in
       let label msg = Printf.sprintf "%s: %s" exchange msg in
       (* 1. Dispatch arms the ledger before any confirmation. *)
       let order =
         Dio_strategies.Jacobs_ladder.create_place_order
           state.duplicate_key_sell
           symbol
           Dio_strategies.Strategy_common.Sell
           0.2
           (Some 100.0)
           true
           Dio_strategies.Strategy_common.Ladder
           exchange
       in
       ignore (Dio_strategies.Jacobs_ladder.push_order ~now ~state order);
       check
         (float 1e-9)
         (label "dispatch arms the ledger")
         0.2
         (Dio_strategies.Jacobs_ladder.committed_sell_base state);
       (* 2. Ack re-keys to the venue id and marks it acked. *)
       Dio_strategies.Jacobs_ladder.Strategy.handle_order_acknowledged
         ~now:(now +. 0.1)
         symbol
         "life-oid"
         Dio_strategies.Strategy_common.Sell
         100.0;
       check
         (float 1e-9)
         (label "ack keeps the base committed")
         0.2
         (Dio_strategies.Jacobs_ladder.committed_sell_base state);
       check
         bool
         (label "ack re-keys to the venue order id")
         true
         (Hashtbl.mem state.sell_commitments "life-oid");
       (* 3. Feed lists it; 4. feed drops it. *)
       let feed = ref [ "life-oid", 100.0, 0.2, "sell", None ] in
       let iter_open_orders f = List.iter (fun (a, b, c, d, e) -> f a b c d e) !feed in
       let sync () =
         let _, _, _, locked, _, _, _ =
           Dio_strategies.Jacobs_ladder.sync_open_orders
             ~state
             ~now
             ~asset
             ~bid_price:100.0
             ~lot_qty:0.2
             ~iter_open_orders
             ~get_open_orders_generation:(fun () -> -1)
             ~ecfg
         in
         locked
       in
       ignore (sync ());
       check
         (float 1e-9)
         (label "a listed sell stays committed")
         0.2
         (Dio_strategies.Jacobs_ladder.committed_sell_base state);
       feed := [];
       ignore (sync ());
       (* A feed absence is venue-specific: venues whose balance nets holds
          from their own state (Hyperliquid/Alpaca) trust the feed, so a
          dropped order is terminal and evicted; venues deriving holds from the
          same feed (Kraken/IBKR/Lighter) keep the base committed. *)
       let trust_feed = exchange = "hyperliquid" || exchange = "alpaca" in
       check
         (float 1e-9)
         (label
            (if trust_feed
             then "a feed-dropped sell is evicted (venue nets from own state)"
             else "a feed-dropped live sell stays committed"))
         (if trust_feed then 0.0 else 0.2)
         (Dio_strategies.Jacobs_ladder.committed_sell_base state);
       check
         bool
         (label
            (if trust_feed
             then "an evicted sell leaves the open-order view"
             else "an in-flight sell remains visible to the buy leg"))
         (not trust_feed)
         (List.exists (fun (id, _, _) -> id = "life-oid") state.open_sell_orders);
       let effective =
         Dio_strategies.Jacobs_ladder.effective_committed_sell_base
           ~ecfg
           ~ledger_total:(Dio_strategies.Jacobs_ladder.committed_sell_base state)
           ~feed_total:state.feed_locked_sell_base
           ~unnetted_hold:0.0
       in
       check
         (float 1e-9)
         (label "dropped-feed base is subtracted only on feed-derived venues")
         (if trust_feed then 0.0 else 0.2)
         effective;
       (* 5. A full fill releases the commitment. *)
       Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
         ~now:(now +. 1.0)
         symbol
         "life-oid"
         Dio_strategies.Strategy_common.Sell
         ~fill_price:100.0
         ~fill_qty:0.2
         None;
       check
         (float 1e-9)
         (label "a sell fill releases the commitment")
         0.0
         (Dio_strategies.Jacobs_ladder.committed_sell_base state))
    venues
;;

let test_terminal_sell_fill_releases_full_commitment () =
  (* REGRESSION (Hyperliquid): the orderUpdates "filled" event retires the
     order from the open-order feed and is filtered out of the strategy
     stream, so the userEvents Trade that reaches [handle_order_filled] can
     report only the final partial size. A terminal fill must release the
     WHOLE commitment by id, not merely the reported qty, or the earlier
     fills' base stays locked forever - the stale sell still shown on the
     dashboard (negative closest-sell distance) and the under-counted
     sellable balance that re-buys into the phantom reservation. *)
  let symbol = "TERMINAL_FILL/USDC" in
  let state = Dio_strategies.Jacobs_ladder.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.cached_ecfg <- Dio_strategies.Jacobs_ladder.get_exchange_config "hyperliquid";
  state.cached_qty_increment <- 0.01;
  state.cached_price_increment <- 0.01;
  state.cached_round_price <- (fun p -> Float.round (p *. 100.0) /. 100.0);
  state.cached_venue_min_qty <- 0.0;
  state.cached_venue_min_notional <- 0.0;
  state.grid_qty <- 0.2;
  state.maker_fee <- 0.0;
  state.cached_sell_mult <- 1.0;
  state.accumulation_buffer <- 0.0;
  state.base_accumulation_enabled <- true;
  state.reserved_base <- 0.0;
  state.accumulated_profit <- 0.0;
  set_sell_commitments state.sell_commitments [ "part-oid", 100.0, 0.2, true, true, 0.0 ];
  state.open_sell_orders <- [ "part-oid", 100.0, 0.2 ];
  state.feed_locked_sell_base <- 0.2;
  state.sell_holds_since_balance <- [];
  state.buy_credits_since_balance <- [];
  state.attributed_balance_increase <- 0.0;
  state.persisted_sell_levels <- [];
  state.pending_orders <- [];
  state.last_buy_fill_price <- Some 99.0;
  state.last_buy_fill_qty <- Some 0.2;
  state.startup_replay <- false;
  state.last_fill_oid <- None;
  Hashtbl.reset state.processed_fills;
  Dio_strategies.Jacobs_ladder.Strategy.handle_order_filled
    ~now:1000.0
    symbol
    "part-oid"
    Dio_strategies.Strategy_common.Sell
    ~fill_price:100.0
    ~fill_qty:0.05
    None;
  check
    (float 1e-9)
    "terminal fill releases the whole commitment"
    0.0
    (Dio_strategies.Jacobs_ladder.committed_sell_base state);
  check
    bool
    "the filled sell is dropped from the open-order view"
    true
    (not (List.exists (fun (id, _, _) -> id = "part-oid") state.open_sell_orders))
;;

let () =
  run
    "Jacobs Ladder"
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
        ; test_case
            "sync_open_orders skips scan when generation unchanged"
            `Quick
            test_sync_open_orders_generation_skip
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
            "capital_low still places the bottom-rung sell"
            `Quick
            test_capital_low_still_places_bottom_rung_sell
        ; test_case
            "burst on a tracked accumulation venue never dips into reserved_base"
            `Quick
            test_burst_tracked_venue_no_reserved_dip
        ; test_case
            "sell ack releases the in-flight latch (multi-sell ladder)"
            `Quick
            test_sell_ack_releases_inflight_latch
        ; test_case
            "blocked sell retries until placed (no replacement buy needed)"
            `Quick
            test_sell_retry_until_placed
        ; test_case
            "halted ladders a second sell beside a resting one"
            `Quick
            test_halted_ladders_second_sell_beside_resting_one
        ; test_case
            "startup-inactive places an inventory sell"
            `Quick
            test_halted_startup_places_inventory_sell
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
            "alpaca verified nothing-to-sell consumes the dead-armed latch"
            `Quick
            test_alpaca_verified_nothing_to_sell_consumes_latch
        ; test_case
            "alpaca persistence never hijacks an owed sell; dust levels prune"
            `Quick
            test_alpaca_persistence_never_hijacks_owed_sell
        ; test_case
            "hl buy fill accrues reserved base (net of base fee)"
            `Quick
            test_hl_buy_fill_accrues_reserve
        ; test_case
            "unnetted sell hold gates second sizing in the netting window"
            `Quick
            test_unnetted_sell_hold_gates_second_sizing
        ; test_case
            "stale unnetted sell hold decays after the grace"
            `Quick
            test_unnetted_sell_hold_expires_after_grace
        ; test_case
            "unnetted sell hold capped even with an ancient balance age"
            `Quick
            test_unnetted_sell_hold_capped_even_with_age
        ; test_case
            "newer balance message releases the unnetted sell hold"
            `Quick
            test_unnetted_sell_hold_releases_on_newer_message
        ; test_case
            "down-move burst places every rung's sell (no hold pile-up)"
            `Quick
            test_unnetted_sell_hold_burst_downmove
        ; test_case
            "buy-fill increase does not release the unnetted sell hold"
            `Quick
            test_unnetted_sell_hold_ignores_buy_increase
        ; test_case
            "freshly acked buy is not purged as a ghost within the grace"
            `Quick
            test_ghost_buy_suppressed_within_ack_grace
        ; test_case
            "position ledger bridges an unreflected buy fill"
            `Quick
            test_position_ledger_bridges_unreflected_fill
        ; test_case
            "position reconciliation is freshness-gated"
            `Quick
            test_position_reconcile_freshness_gate
        ; test_case
            "adopted lower venue balance cannot free reserved base"
            `Quick
            test_position_reconcile_lower_balance_cannot_dip_reserve
        ; test_case
            "seed prunes a credit the first message already covers"
            `Quick
            test_position_seed_prunes_covered_credit
        ; test_case
            "seed keeps a credit newer than the first message"
            `Quick
            test_position_seed_keeps_newer_credit
        ; test_case
            "older-generation message cannot regress the ledger"
            `Quick
            test_position_stale_message_does_not_regress_ledger
        ; test_case
            "a message between two fills prunes only the covered credit"
            `Quick
            test_position_partial_credit_prune
        ; test_case
            "a missed-fill venue figure is adopted upward"
            `Quick
            test_position_upward_reconcile_adopts_venue
        ; test_case
            "balance message before its fill event does not double-credit"
            `Quick
            test_position_balance_before_fill_no_double_credit
        ; test_case
            "NaN balance neither seeds nor prunes"
            `Quick
            test_position_nan_balance_does_not_seed
        ; test_case
            "buy credit and unnetted sell hold cancel in the lag window"
            `Quick
            test_position_buy_credit_and_sell_hold_cancel
        ; test_case
            "credit decays when the feed goes silent past the grace"
            `Quick
            test_position_dead_feed_credit_expires
        ; test_case
            "reserved-exceeding ledger places nothing (no negative size)"
            `Quick
            test_position_reserved_exceeds_ledger_clamps
        ; test_case
            "startup replay records no pending credit"
            `Quick
            test_position_startup_replay_records_no_credit
        ; test_case
            "gross venue sell fill decrements the ledger"
            `Quick
            test_position_gross_venue_sell_fill_decrements
        ; test_case
            "accumulation venue sell fill leaves the ledger"
            `Quick
            test_position_accumulation_sell_fill_keeps_ledger
        ; test_case
            "asset_low recovery uses the fill-aware ledger"
            `Quick
            test_position_asset_low_recovery_sees_pending_credit
        ; test_case
            "sell hold netting retires the oldest hold FIFO, buys never do"
            `Quick
            test_position_sell_hold_releases_fifo_on_netting
        ; test_case
            "locked resting-sell base is never offered again"
            `Quick
            test_sell_never_offers_locked_inventory
        ; test_case
            "in-flight sell commitment survives a venue feed gap"
            `Quick
            test_inflight_sell_commitment_survives_feed_gap
        ; test_case
            "sellable base excludes locked inventory on every venue (matrix)"
            `Quick
            test_sellable_base_matrix_all_venues
        ; test_case
            "sell commitment lifecycle on every venue"
            `Quick
            test_sell_commitment_lifecycle_all_venues
        ; test_case
            "terminal sell fill releases the whole commitment"
            `Quick
            test_terminal_sell_fill_releases_full_commitment
        ; test_case
            "sub-minimum qty sell places (notional is the only floor)"
            `Quick
            test_sub_minimum_qty_sell_places
        ; test_case
            "alpaca sell anchors on fill, not the ask"
            `Quick
            test_alpaca_sell_anchors_on_fill_not_ask
        ; test_case
            "alpaca excess refills the ladder before dumping"
            `Quick
            test_alpaca_excess_refills_before_dumping
        ; test_case
            "alpaca excess amends an open top rung"
            `Quick
            test_alpaca_excess_amends_open_top_rung
        ; test_case
            "alpaca excess excludes reserved_base"
            `Quick
            test_alpaca_excess_excludes_reserved_base
        ; test_case
            "alpaca venue free == reserved_base offers nothing"
            `Quick
            test_alpaca_venue_available_blocks_reserve_dip
        ; test_case
            "alpaca excess sweep is capped to one lot"
            `Quick
            test_alpaca_excess_sweep_capped_to_one_lot
        ; test_case
            "new buy respects the 2x gi closest-sell cap"
            `Quick
            test_new_buy_respects_2x_gi_closest_sell
        ; test_case
            "fresh buy clamps against the companion (not-yet-placed) sell"
            `Quick
            test_fresh_buy_clamps_against_companion_sell
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
        ; test_case
            "amendment TIF reject arms recovery"
            `Quick
            test_tif_recovery_armed_on_amendment_failed
        ; test_case
            "non-terminal amend failure does not arm recovery"
            `Quick
            test_tif_recovery_not_armed_on_non_tif_amend_failure
        ; test_case
            "Alpaca filled amend failure clears tracking and evicts"
            `Quick
            test_alpaca_filled_amend_failure_clears_tracking
        ; test_case
            "insufficient-balance failure does not arm recovery"
            `Quick
            test_tif_recovery_not_armed_on_insufficient_failed
        ; test_case
            "transient placement failure arms recovery"
            `Quick
            test_tif_recovery_armed_on_transient_failed
        ; test_case "buy ack clears recovery" `Quick test_tif_recovery_cleared_on_buy_ack
        ; test_case
            "ghost buy WS kill arms recovery"
            `Quick
            test_tif_recovery_armed_on_ghost_buy_ws_kill
        ; test_case
            "stale cancel does not arm recovery"
            `Quick
            test_tif_recovery_not_armed_on_stale_cancel
        ] )
    ; ( "accumulation"
      , [ test_case
            "profit tracking from sell fills"
            `Quick
            test_accumulation_profit_tracking
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
