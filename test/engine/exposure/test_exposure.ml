module Limits = Dio_strategies.Strategy_common

let () = ignore Hyperliquid.Module.Hyperliquid_impl.name
let limits ?portfolio_pct ?venue_pct () = Some { Limits.portfolio_pct; venue_pct }

let check name expected position_value portfolio_value venue_value limits =
  Alcotest.(check bool)
    name
    expected
    (Dio_engine.Exposure.breached ~position_value ~portfolio_value ~venue_value ~limits)
;;

let test_no_limit_fails_open () = check "no limit" false 500.0 0.0 0.0 None

let test_portfolio_ratio () =
  let l = limits ~portfolio_pct:10.0 () in
  check "below portfolio cap" false 99.0 1000.0 0.0 l;
  check "at portfolio cap passes" false 100.0 1000.0 0.0 l;
  check "above portfolio cap pauses" true 100.01 1000.0 0.0 l
;;

let test_venue_ratio () =
  let l = limits ~venue_pct:25.0 () in
  check "below venue cap" false 4_000.0 0.0 20_000.0 l;
  check "at venue cap passes" false 5_000.0 0.0 20_000.0 l;
  check "above venue cap pauses" true 5_001.0 0.0 20_000.0 l
;;

let test_either_limit_wins () =
  let l = limits ~portfolio_pct:10.0 ~venue_pct:25.0 () in
  check "portfolio binds first" true 1_001.0 10_000.0 20_000.0 l;
  check "venue binds first" true 5_001.0 100_000.0 20_000.0 l;
  check "both clear" false 1_000.0 10_000.0 20_000.0 l
;;

let test_unknown_denominator_fails_open () =
  let l = limits ~portfolio_pct:10.0 ~venue_pct:25.0 () in
  check "portfolio unknown" false 100.0 0.0 20_000.0 l;
  check "venue unknown" false 100.0 10_000.0 0.0 l;
  check "both unknown" false 100.0 0.0 0.0 l
;;

let test_ratios () =
  let portfolio, venue =
    Dio_engine.Exposure.ratios
      ~position_value:1_000.0
      ~portfolio_value:10_000.0
      ~venue_value:20_000.0
  in
  Alcotest.(check (float 1e-9)) "portfolio pct" 10.0 portfolio;
  Alcotest.(check (float 1e-9)) "venue pct" 5.0 venue;
  let portfolio, venue =
    Dio_engine.Exposure.ratios
      ~position_value:1_000.0
      ~portfolio_value:0.0
      ~venue_value:0.0
  in
  Alcotest.(check bool) "unknown portfolio is nan" true (Float.is_nan portfolio);
  Alcotest.(check bool) "unknown venue is nan" true (Float.is_nan venue)
;;

let test_parent_unfunded_truth_table () =
  let p = Dio_engine.Exposure.parent_unfunded in
  Alcotest.(check bool)
    "healthy"
    false
    (p ~oracle_active:true ~capital_low:false ~exposure_high:false);
  Alcotest.(check bool)
    "oracle inactive"
    true
    (p ~oracle_active:false ~capital_low:false ~exposure_high:false);
  Alcotest.(check bool)
    "capital low"
    true
    (p ~oracle_active:true ~capital_low:true ~exposure_high:false);
  Alcotest.(check bool)
    "exposure high"
    true
    (p ~oracle_active:true ~capital_low:false ~exposure_high:true);
  Alcotest.(check bool)
    "both local"
    true
    (p ~oracle_active:true ~capital_low:true ~exposure_high:true)
;;

let test_exposure_blocks_buy_not_sell_state () =
  let symbol = "EXPO_TEST/USDC" in
  Hyperliquid.Instruments_feed.register_test_instrument ~symbol ~sz_decimals:2;
  let state = Dio_strategies.Strategy_api.get_strategy_state symbol in
  state.exchange_id <- "hyperliquid";
  state.grid_qty <- 0.5;
  state.cached_venue_min_qty <- 0.01;
  state.cached_ecfg <- Dio_strategies.Strategy_api.get_exchange_config "hyperliquid";
  state.last_buy_order_id <- None;
  state.last_buy_order_price <- None;
  state.inflight_buy <- false;
  state.capital_low <- false;
  state.exposure_high <- true;
  Hashtbl.remove state.amend_cooldowns "place_Buy";
  let asset =
    { Dio_strategies.Strategy_api.exchange = "hyperliquid"
    ; symbol
    ; qty = "0.5"
    ; grid_interval = 1.0
    ; sell_mult = "1.0"
    ; strategy = "Ladder"
    ; maker_fee = Some 0.0
    ; taker_fee = Some 0.0
    ; accumulation_buffer = 0.01
    ; base_accumulation = true
    ; sell_levels_persistence = false
    }
  in
  let buffer = Dio_strategies.Strategy_api.get_order_buffer () in
  let rec drain () =
    match Dio_strategies.Strategy_common.LockFreeQueue.read buffer with
    | Some _ -> drain ()
    | None -> ()
  in
  drain ();
  let place () =
    Dio_strategies.Strategy_api.buy_place_initial
      ~state
      ~now:100.0
      ~asset
      ~bid_price:100.0
      ~ask_price:100.1
      ~quote_balance:10_000.0
      ~quote_balance_stale:false
      ~oracle_halted:false
      ~cycle:1
      ~locked_in_buys:0.0
      ~closest_sell_order_initial:None
  in
  Alcotest.(check bool) "blocked while exposure high" false (place ());
  Alcotest.(check bool)
    "no order pushed"
    true
    (Dio_strategies.Strategy_common.LockFreeQueue.read buffer = None);
  Alcotest.(check bool) "capital_low untouched" false state.capital_low;
  state.exposure_high <- false;
  Alcotest.(check bool) "allowed after clear" true (place ())
;;

let () =
  Alcotest.run
    "Exposure"
    [ ( "limits"
      , [ Alcotest.test_case "no limit fails open" `Quick test_no_limit_fails_open
        ; Alcotest.test_case "portfolio ratio" `Quick test_portfolio_ratio
        ; Alcotest.test_case "venue ratio" `Quick test_venue_ratio
        ; Alcotest.test_case "either limit wins" `Quick test_either_limit_wins
        ; Alcotest.test_case
            "unknown denominator fails open"
            `Quick
            test_unknown_denominator_fails_open
        ; Alcotest.test_case
            "blocks buys but not capital_low"
            `Quick
            test_exposure_blocks_buy_not_sell_state
        ; Alcotest.test_case
            "parent unfunded truth table"
            `Quick
            test_parent_unfunded_truth_table
        ] )
    ; "logging", [ Alcotest.test_case "ratios" `Quick test_ratios ]
    ]
;;
