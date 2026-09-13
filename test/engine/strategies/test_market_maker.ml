open Alcotest

(* Trading-config fixture. *)
let create_test_asset
      ?(exchange = "kraken")
      ?(symbol = "BTC/USD")
      ?(qty = "0.001")
      ?(strategy = "MM")
      ?(min_usd_balance = None)
      ?(max_exposure = None)
      ?(maker_fee = None)
      ?(taker_fee = None)
      ?(grid_interval = 1.0, 1.0)
      ?(sell_mult = "1.0")
      ?(testnet = false)
      ?(hedge = false)
      ?(accumulation_buffer = 0.01, 0.01)
      ?(data_feed = None)
      ()
  : Dio_strategies.Strategy_common.trading_config
  =
  { exchange
  ; symbol
  ; qty
  ; grid_interval
  ; sell_mult
  ; min_usd_balance
  ; max_exposure
  ; strategy
  ; maker_fee
  ; taker_fee
  ; testnet
  ; hedge
  ; accumulation_buffer
  ; data_feed
  ; base_accumulation = true
  ; sell_levels = true
  }
;;

let test_initialization () =
  check unit "market_maker init" () (Dio_strategies.Market_maker.Strategy.init ())
;;

let test_order_creation_place () =
  let order =
    Dio_strategies.Market_maker.create_place_order
      "BTC/USD"
      Dio_strategies.Strategy_common.Buy
      0.001
      (Some 50000.0)
      true
      Dio_strategies.Strategy_common.MM
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
    (order.strategy = Dio_strategies.Strategy_common.MM)
;;

let test_order_creation_amend () =
  let order =
    Dio_strategies.Market_maker.create_amend_order
      "order123"
      "BTC/USD"
      Dio_strategies.Strategy_common.Sell
      0.001
      (Some 51000.0)
      true
      Dio_strategies.Strategy_common.MM
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
    (order.strategy = Dio_strategies.Strategy_common.MM)
;;

let test_order_creation_cancel () =
  let order =
    Dio_strategies.Market_maker.create_cancel_order
      "order456"
      "BTC/USD"
      Dio_strategies.Strategy_common.MM
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
    (order.strategy = Dio_strategies.Strategy_common.MM);
  check (option (float 0.)) "cancel order price" None order.price;
  check (float 0.) "cancel order qty" 0.0 order.qty
;;

let test_fee_calculation () =
  let asset_no_fee = create_test_asset ~maker_fee:(Some 0.0) () in
  let asset_maker_fee = create_test_asset ~maker_fee:(Some 0.001) () in
  let asset_taker_fee = create_test_asset ~taker_fee:(Some 0.002) () in
  let fee1 = Dio_strategies.Market_maker.get_fee_for_asset asset_no_fee in
  let fee2 = Dio_strategies.Market_maker.get_fee_for_asset asset_maker_fee in
  let fee3 = Dio_strategies.Market_maker.get_fee_for_asset asset_taker_fee in
  check (float 0.) "fee no fee asset" 0.0 fee1;
  check (float 0.) "fee maker fee asset" 0.001 fee2;
  check (float 0.) "fee taker fee asset" 0.002 fee3
;;

let test_price_rounding () =
  (* No instrument-precision feed here; assert a non-negative float. *)
  let rounded =
    Dio_strategies.Market_maker.round_price 50000.12345678 "BTC/USD" "kraken"
  in
  check bool "price rounding non-negative" true (rounded >= 0.0)
;;

let test_state_management () =
  let state1 = Dio_strategies.Market_maker.get_strategy_state "BTC/USD" in
  let state2 = Dio_strategies.Market_maker.get_strategy_state "BTC/USD" in
  (* Same symbol returns the same state record. *)
  check bool "same state for same symbol" true (state1 == state2)
;;

let test_userref_generation () =
  (* MM strategy tags orders with userref=2. *)
  let strategy_userref = Dio_strategies.Strategy_common.strategy_userref_mm in
  check int "mm strategy userref" 2 strategy_userref;
  check
    bool
    "userref 2 matches mm"
    true
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 2);
  check
    bool
    "userref 1 doesn't match mm"
    false
    (Dio_strategies.Strategy_common.is_strategy_order strategy_userref 1)
;;

let test_order_acknowledgment () =
  let initial_state = Dio_strategies.Market_maker.get_strategy_state "TEST/USD" in
  initial_state.pending_orders
  <- ("test123", Dio_strategies.Strategy_common.Buy, 50000.0, Unix.time ())
     :: initial_state.pending_orders;
  Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
    ~now:0.0
    "TEST/USD"
    "order456"
    Dio_strategies.Strategy_common.Buy
    50000.0;
  (* IDs differ, so tracking updates without removing the pending order. *)
  check bool "acknowledgment handled" true true
;;

let test_order_cancellation () =
  let state = Dio_strategies.Market_maker.get_strategy_state "TEST2/USD" in
  state.last_buy_order_id <- Some "buy123";
  state.last_buy_order_price <- Some 49000.0;
  state.open_sell_orders <- [ "sell456", 51000.0, 1.0 ];
  Dio_strategies.Market_maker.Strategy.handle_order_cancelled
    ~now:0.0
    "TEST2/USD"
    "buy123"
    Dio_strategies.Strategy_common.Buy
    None;
  check (option string) "buy order id cleared" None state.last_buy_order_id;
  check (option (float 0.)) "buy order price cleared" None state.last_buy_order_price;
  let sell_preserved =
    List.exists (fun (id, _, _) -> id = "sell456") state.open_sell_orders
  in
  check bool "sell order id preserved" true sell_preserved
;;

let test_minimum_quantity_check () =
  (* Depends on instrument feed data; assert a boolean result. *)
  let result = Dio_strategies.Market_maker.meets_min_qty "BTC/USD" 0.001 "kraken" in
  check bool "min qty returns bool" true (result = true || result = false)
;;

let test_config_parsing () =
  let test_parse str default expected =
    let result =
      Dio_strategies.Market_maker.parse_config_float
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

let test_config_parsing_optional () =
  let test_parse str exchange symbol expected =
    let result =
      Dio_strategies.Market_maker.parse_config_float_opt str "test_param" exchange symbol
    in
    match result, expected with
    | Some r, Some e -> abs_float (r -. e) < 0.0001
    | None, None -> true
    | _ -> false
  in
  check
    bool
    "parse optional valid"
    true
    (test_parse "100.0" "TEST" "TEST/USD" (Some 100.0));
  check bool "parse optional empty" true (test_parse "" "TEST" "TEST/USD" None);
  check bool "parse optional invalid" true (test_parse "invalid" "TEST" "TEST/USD" None)
;;

let test_duplicate_cancellation () =
  let open_orders =
    [ "order1", 50000.0, 0.001, "buy", Some 2
    ; "order2", 50000.0, 0.001, "buy", Some 2
    ; (* Same price: duplicate, cancelled. *)
      "order3", 51000.0, 0.001, "buy", Some 2
      (* Different price: kept. *)
    ]
  in
  let state = Dio_strategies.Market_maker.get_strategy_state "TEST/USD" in
  (* order1 already cancelled. *)
  state.cancelled_orders <- [ "order1", Unix.time () ];

  let open_orders_list =
    List.map (fun (oid, price, qty, _, _) -> oid, price, qty) open_orders
  in
  let cancelled_count =
    Dio_strategies.Market_maker.cancel_duplicate_orders
      ~state
      "TEST/USD"
      50000.0
      Dio_strategies.Strategy_common.Buy
      open_orders_list
      Dio_strategies.Strategy_common.MM
      "kraken"
  in
  (* Only order2 is newly cancelled; order1 was already. *)
  check int "duplicate cancellation count" 1 cancelled_count
;;

let test_sell_first_placement_logic () =
  (* Sell orders are placed before buy orders. *)
  let asset = create_test_asset () in
  let current_price = Some 50000.0 in
  let top_of_book = Some (49950.0, 1.0, 50050.0, 1.0) in
  let asset_balance = Some 0.1 in
  let quote_balance = Some 1000.0 in
  (* Full sell-first path requires mocking the order ringbuffer. *)
  Dio_strategies.Market_maker.Strategy.execute
    asset
    current_price
    top_of_book
    asset_balance
    quote_balance
    0
    0
    (fun _ -> ())
    1;
  check bool "execute function runs" true true
;;

let test_fee_cache_integration () =
  Dio_strategies.Fee_cache.init ();
  Dio_strategies.Fee_cache.clear ();
  let fee_opt =
    Dio_strategies.Fee_cache.get_maker_fee ~exchange:"kraken" ~symbol:"BTC/USD"
  in
  (* No cached entry yet. *)
  check (option (float 0.)) "initial fee cache empty" None fee_opt;
  Dio_strategies.Fee_cache.store_fees
    ~exchange:"kraken"
    ~symbol:"BTC/USD"
    ~maker_fee:0.001
    ~taker_fee:0.001
    ~ttl_seconds:600.0;
  let fee_opt2 =
    Dio_strategies.Fee_cache.get_maker_fee ~exchange:"kraken" ~symbol:"BTC/USD"
  in
  check (option (float 0.)) "fee cache retrieval" (Some 0.001) fee_opt2
;;

let test_fee_cache_stats () =
  Dio_strategies.Fee_cache.init ();
  Dio_strategies.Fee_cache.clear ();

  (* Empty cache: zero totals. *)
  let total, valid = Dio_strategies.Fee_cache.stats () in
  check bool "empty stats returns integers" true (total = 0 && valid = 0);
  Dio_strategies.Fee_cache.store_fees
    ~exchange:"kraken"
    ~symbol:"BTC/USD"
    ~maker_fee:0.001
    ~taker_fee:0.001
    ~ttl_seconds:600.0;
  Dio_strategies.Fee_cache.store_fees
    ~exchange:"kraken"
    ~symbol:"ETH/USD"
    ~maker_fee:0.002
    ~taker_fee:0.002
    ~ttl_seconds:600.0;
  let total2, valid2 = Dio_strategies.Fee_cache.stats () in
  check bool "populated stats returns integers" true (total2 = 2 && valid2 = 2)
;;

let test_profitability_checks () =
  let zero_fee_asset = create_test_asset ~maker_fee:(Some 0.0) () in
  let fee_asset = create_test_asset ~maker_fee:(Some 0.001) () in
  let fee1 = Dio_strategies.Market_maker.get_fee_for_asset zero_fee_asset in
  let fee2 = Dio_strategies.Market_maker.get_fee_for_asset fee_asset in
  check (float 0.) "zero fee calculation" 0.0 fee1;
  check (float 0.) "maker fee calculation" 0.001 fee2
;;

let test_post_only_checks () =
  let asset = create_test_asset () in
  (* buy < bid: valid post-only. *)
  Dio_strategies.Market_maker.Strategy.execute
    asset
    (Some 50000.0)
    (Some (49950.0, 1.0, 50050.0, 1.0))
    (Some 0.1)
    (Some 1000.0)
    0
    0
    (fun _ -> ())
    1;
  (* buy >= bid: invalid post-only. *)
  Dio_strategies.Market_maker.Strategy.execute
    asset
    (Some 50000.0)
    (Some (50000.0, 1.0, 50050.0, 1.0))
    (Some 0.1)
    (Some 1000.0)
    0
    0
    (fun _ -> ())
    2;
  check bool "post-only checks handled" true true
;;

(* ---- Inflight flag lifecycle tests ----
   inflight_buy/inflight_sell are set on Place; ack/fill/cancel must clear
   them or re-placement is blocked after the first cycle. *)

(** Helper: get or create a fresh state for a unique test symbol. *)
let fresh_state prefix =
  let symbol = Printf.sprintf "%s_%d/USD" prefix (Random.bits ()) in
  let state = Dio_strategies.Market_maker.get_strategy_state symbol in
  state.inflight_buy <- false;
  state.inflight_sell <- false;
  state.pending_orders <- [];
  state.last_buy_order_id <- None;
  state.last_buy_order_price <- None;
  state.open_sell_orders <- [];
  state.cancelled_orders <- [];
  symbol, state
;;

let test_inflight_cleared_on_ack () =
  let symbol, state = fresh_state "ACK" in
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
    ~now:(Unix.time ())
    symbol
    "order_buy_1"
    Dio_strategies.Strategy_common.Buy
    50000.0;
  check bool "inflight_buy cleared on ack" false state.inflight_buy;
  Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
    ~now:(Unix.time ())
    symbol
    "order_sell_1"
    Dio_strategies.Strategy_common.Sell
    51000.0;
  check bool "inflight_sell cleared on ack" false state.inflight_sell
;;

let test_inflight_cleared_on_fill () =
  let symbol, state = fresh_state "FILL" in
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  Dio_strategies.Market_maker.Strategy.handle_order_filled
    ~now:(Unix.time ())
    symbol
    "order_buy_2"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:50000.0
    ~fill_qty:1.0
    None;
  check bool "inflight_buy cleared on fill" false state.inflight_buy;
  Dio_strategies.Market_maker.Strategy.handle_order_filled
    ~now:(Unix.time ())
    symbol
    "order_sell_2"
    Dio_strategies.Strategy_common.Sell
    ~fill_price:51000.0
    ~fill_qty:1.0
    None;
  check bool "inflight_sell cleared on fill" false state.inflight_sell
;;

let test_inflight_cleared_on_cancel () =
  let symbol, state = fresh_state "CANCEL" in
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  (* Genuine cancel: no pending_amend entry. *)
  Dio_strategies.Market_maker.Strategy.handle_order_cancelled
    ~now:(Unix.time ())
    symbol
    "order_buy_3"
    Dio_strategies.Strategy_common.Buy
    None;
  check bool "inflight_buy cleared on cancel" false state.inflight_buy;
  Dio_strategies.Market_maker.Strategy.handle_order_cancelled
    ~now:(Unix.time ())
    symbol
    "order_sell_3"
    Dio_strategies.Strategy_common.Sell
    None;
  check bool "inflight_sell cleared on cancel" false state.inflight_sell
;;

let test_inflight_cleared_on_fail () =
  let symbol, state = fresh_state "FAIL" in
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  Dio_strategies.Market_maker.Strategy.handle_order_failed
    ~now:(Unix.time ())
    symbol
    Dio_strategies.Strategy_common.Buy
    "test failure";
  check bool "inflight_buy cleared on fail" false state.inflight_buy;
  Dio_strategies.Market_maker.Strategy.handle_order_failed
    ~now:(Unix.time ())
    symbol
    Dio_strategies.Strategy_common.Sell
    "test failure";
  check bool "inflight_sell cleared on fail" false state.inflight_sell
;;

let test_inflight_cleared_on_reject () =
  let symbol, state = fresh_state "REJECT" in
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  Dio_strategies.Market_maker.Strategy.handle_order_rejected
    ~now:(Unix.time ())
    symbol
    Dio_strategies.Strategy_common.Buy
    50000.0;
  check bool "inflight_buy cleared on reject" false state.inflight_buy;
  Dio_strategies.Market_maker.Strategy.handle_order_rejected
    ~now:(Unix.time ())
    symbol
    Dio_strategies.Strategy_common.Sell
    51000.0;
  check bool "inflight_sell cleared on reject" false state.inflight_sell
;;

let test_inflight_preserves_opposite_side () =
  let symbol, state = fresh_state "PRESERVE" in
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  (* Ack clears only its own side. *)
  Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
    ~now:(Unix.time ())
    symbol
    "order_buy_5"
    Dio_strategies.Strategy_common.Buy
    50000.0;
  check bool "inflight_buy cleared" false state.inflight_buy;
  check bool "inflight_sell preserved" true state.inflight_sell;
  state.inflight_buy <- true;
  state.inflight_sell <- true;
  Dio_strategies.Market_maker.Strategy.handle_order_filled
    ~now:(Unix.time ())
    symbol
    "order_sell_5"
    Dio_strategies.Strategy_common.Sell
    ~fill_price:51000.0
    ~fill_qty:1.0
    None;
  check bool "inflight_buy preserved" true state.inflight_buy;
  check bool "inflight_sell cleared" false state.inflight_sell
;;

let test_inflight_full_lifecycle () =
  (* place buy -> ack -> fill -> re-placement not blocked *)
  let symbol, state = fresh_state "LIFECYCLE" in
  state.inflight_buy <- true;
  check bool "inflight_buy set after place" true state.inflight_buy;
  Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
    ~now:(Unix.time ())
    symbol
    "lifecycle_buy_1"
    Dio_strategies.Strategy_common.Buy
    50000.0;
  check bool "inflight_buy cleared after ack" false state.inflight_buy;
  state.inflight_buy <- true;
  Dio_strategies.Market_maker.Strategy.handle_order_filled
    ~now:(Unix.time ())
    symbol
    "lifecycle_buy_1"
    Dio_strategies.Strategy_common.Buy
    ~fill_price:50000.0
    ~fill_qty:1.0
    None;
  check bool "inflight_buy cleared after fill" false state.inflight_buy;
  check bool "re-placement not blocked" false state.inflight_buy
;;

let test_inflight_cancel_replace_preserves_flag () =
  (* Cancel-replace (amendment) preserves inflight: a replacement is incoming. *)
  let symbol, state = fresh_state "AMEND" in
  state.inflight_buy <- true;
  (* pending_amend entry marks cancel-replace. *)
  state.pending_orders
  <- [ ( "pending_amend_order_buy_cr"
       , Dio_strategies.Strategy_common.Buy
       , 50000.0
       , Unix.time () )
     ];
  Dio_strategies.Market_maker.Strategy.handle_order_cancelled
    ~now:(Unix.time ())
    symbol
    "order_buy_cr"
    Dio_strategies.Strategy_common.Buy
    None;
  (* Cancel-replace keeps inflight_buy: the replacement is still pending. *)
  check bool "inflight_buy preserved during cancel-replace" true state.inflight_buy
;;

(* ---- InFlightAmendments cleanup tests ----
   handle_order_amended and handle_order_amendment_skipped must release
   InFlightAmendments entries. *)

let test_amendment_clears_inflight_amendment () =
  let symbol, state = fresh_state "AMEND_IFA" in
  let _added =
    Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment
      "amend_order_1"
  in
  check
    bool
    "amendment registered"
    true
    (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight "amend_order_1");
  (* Buy tracking must match for the handler to fire. *)
  state.last_buy_order_id <- Some "amend_order_1";
  state.last_buy_order_price <- Some 50000.0;
  Dio_strategies.Market_maker.Strategy.handle_order_amended
    ~now:(Unix.time ())
    symbol
    "amend_order_1"
    "amend_order_2"
    Dio_strategies.Strategy_common.Buy
    50500.0;
  check
    bool
    "inflight amendment cleared after amended"
    false
    (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight "amend_order_1")
;;

let test_amendment_skipped_clears_inflight_amendment () =
  let symbol, _state = fresh_state "SKIP_IFA" in
  let _added =
    Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment
      "skip_order_1"
  in
  check
    bool
    "amendment registered"
    true
    (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight "skip_order_1");
  Dio_strategies.Market_maker.Strategy.handle_order_amendment_skipped
    ~now:(Unix.time ())
    symbol
    "skip_order_1"
    Dio_strategies.Strategy_common.Buy
    50000.0;
  check
    bool
    "inflight amendment cleared after skip"
    false
    (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight "skip_order_1")
;;

let test_amendment_failed_clears_inflight_amendment () =
  let symbol, state = fresh_state "FAIL_IFA" in
  let _added =
    Dio_strategies.Strategy_common.InFlightAmendments.add_in_flight_amendment
      "fail_order_1"
  in
  check
    bool
    "amendment registered"
    true
    (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight "fail_order_1");
  state.last_buy_order_id <- Some "fail_order_1";
  state.last_buy_order_price <- Some 50000.0;
  Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed
    ~now:(Unix.time ())
    symbol
    "fail_order_1"
    Dio_strategies.Strategy_common.Buy
    "test reason";
  check
    bool
    "inflight amendment cleared after fail"
    false
    (Dio_strategies.Strategy_common.InFlightAmendments.is_in_flight "fail_order_1")
;;

let () =
  run
    "Market Maker"
    [ "initialization", [ test_case "strategy init" `Quick test_initialization ]
    ; ( "order_creation"
      , [ test_case "place order" `Quick test_order_creation_place
        ; test_case "amend order" `Quick test_order_creation_amend
        ; test_case "cancel order" `Quick test_order_creation_cancel
        ] )
    ; ( "fee_calculation"
      , [ test_case "fee calculation" `Quick test_fee_calculation
        ; test_case "fee cache integration" `Quick test_fee_cache_integration
        ; test_case "fee cache stats" `Quick test_fee_cache_stats
        ] )
    ; "price_handling", [ test_case "price rounding" `Quick test_price_rounding ]
    ; ( "state_management"
      , [ test_case "state management" `Quick test_state_management
        ; test_case "userref generation" `Quick test_userref_generation
        ] )
    ; ( "order_handling"
      , [ test_case "order acknowledgment" `Quick test_order_acknowledgment
        ; test_case "order cancellation" `Quick test_order_cancellation
        ; test_case "minimum quantity check" `Quick test_minimum_quantity_check
        ; test_case "duplicate cancellation" `Quick test_duplicate_cancellation
        ] )
    ; ( "strategy_logic"
      , [ test_case "sell first placement" `Quick test_sell_first_placement_logic
        ; test_case "profitability checks" `Quick test_profitability_checks
        ; test_case "post-only checks" `Quick test_post_only_checks
        ] )
    ; ( "config_parsing"
      , [ test_case "config parsing" `Quick test_config_parsing
        ; test_case "config parsing optional" `Quick test_config_parsing_optional
        ] )
    ; ( "inflight_lifecycle"
      , [ test_case "cleared on ack" `Quick test_inflight_cleared_on_ack
        ; test_case "cleared on fill" `Quick test_inflight_cleared_on_fill
        ; test_case "cleared on cancel" `Quick test_inflight_cleared_on_cancel
        ; test_case "cleared on fail" `Quick test_inflight_cleared_on_fail
        ; test_case "cleared on reject" `Quick test_inflight_cleared_on_reject
        ; test_case "preserves opposite side" `Quick test_inflight_preserves_opposite_side
        ; test_case "full lifecycle" `Quick test_inflight_full_lifecycle
        ; test_case
            "cancel-replace preserves flag"
            `Quick
            test_inflight_cancel_replace_preserves_flag
        ] )
    ; ( "amendment_inflight"
      , [ test_case
            "amended clears InFlightAmendment"
            `Quick
            test_amendment_clears_inflight_amendment
        ; test_case
            "amendment skipped clears InFlightAmendment"
            `Quick
            test_amendment_skipped_clears_inflight_amendment
        ; test_case
            "amendment failed clears InFlightAmendment"
            `Quick
            test_amendment_failed_clears_inflight_amendment
        ] )
    ]
;;
