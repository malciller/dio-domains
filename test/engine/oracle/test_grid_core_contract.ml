(* Contract tests: Grid_core level/rounding helpers must agree with the live
   Suicide_grid implementation. *)

let identity_state () =
  let open Dio_strategies.Suicide_grid_types in
  { last_buy_order_price = None
  ; last_buy_order_id = None
  ; open_sell_orders = []
  ; persisted_sell_levels = []
  ; recently_injected_sells = []
  ; pending_orders = []
  ; last_cycle = 0
  ; last_order_time = 0.0
  ; inflight_cancel_buy = false
  ; inflight_amend_buy = false
  ; amend_cooldowns = Hashtbl.create 1
  ; last_cleanup_time = 0.0
  ; inflight_buy = false
  ; inflight_sell = false
  ; evicted_orders = Hashtbl.create 1
  ; asset_low = false
  ; capital_low = false
  ; capital_low_logged = false
  ; capital_low_at_balance = 0.0
  ; last_buy_attempted_insufficient = false
  ; resuming_after_balance_flag = false
  ; just_filled_buy = false
  ; force_buy_reanchor = false
  ; reserved_quote = 0.0
  ; accumulated_profit = 0.0
  ; reserved_base = 0.0
  ; last_buy_fill_price = None
  ; last_sell_fill_price = None
  ; last_buy_fill_qty = None
  ; last_sell_fill_qty = None
  ; grid_qty = 0.0
  ; cached_sell_mult = 1.0
  ; cached_ecfg = default_kraken_config
  ; maker_fee = 0.0
  ; exchange_id = ""
  ; startup_replay = true
  ; last_fill_oid = None
  ; highest_startup_oid = None
  ; anticipated_base_credit = 0.0
  ; last_seen_asset_balance = 0.0
  ; persistence_dirty = false
  ; last_cycle_orders_hash = 0
  ; last_cycle_buy_count = 0
  ; duplicate_key_buy = "t|buy|grid"
  ; duplicate_key_sell = "t|sell|grid"
  ; cached_round_price = (fun p -> p)
  ; cached_price_increment = 0.01
  ; cached_qty_increment = 0.01
  ; cached_venue_min_qty = 1.0
  ; cached_venue_min_notional = 0.0
  ; exchange_reserved_atomic = None
  ; processed_fills = Hashtbl.create 1
  ; processed_fills_queue = Queue.create ()
  ; mutex = Mutex.create ()
  }
;;

let cfg ?(price_increment = 0.01) ?(qty_increment = 0.01) () =
  let open Dio_strategies.Grid_core in
  { qty = 1.0
  ; sell_mult = 1.0
  ; grid_interval_pct = 1.0
  ; maker_fee = 0.0004
  ; accumulation_buffer = 0.0
  ; price_increment
  ; qty_increment
  ; qty_min = 0.0
  ; min_notional = 0.0
  ; exchange_model = Hyperliquid
  ; start_price = 100.0
  ; start_quote = 10_000.0
  ; cash_hook = None
  }
;;

let near a b = Alcotest.(check (float 1e-6)) "approx" a b

let test_buy_level_matches_live () =
  let st = identity_state () in
  let open Dio_strategies in
  List.iter
    (fun (price, gi) ->
       let live = Suicide_grid_config.calculate_grid_price price gi false st in
       let core =
         Grid_core.buy_level
           ~ref:price
           Grid_core.{ (cfg ~price_increment:1e-9 ()) with grid_interval_pct = gi }
       in
       near live core)
    [ 100.0, 1.0; 100.0, 0.5; 500.0, 2.0; 0.0032, 1.0; 1_234.56, 1.25 ]
;;

let test_sell_level_matches_live () =
  let st = identity_state () in
  let open Dio_strategies in
  List.iter
    (fun (price, gi) ->
       let live = Suicide_grid_config.calculate_grid_price price gi true st in
       let core =
         Grid_core.sell_level
           ~ref:price
           Grid_core.{ (cfg ~price_increment:1e-9 ()) with grid_interval_pct = gi }
       in
       near live core)
    [ 100.0, 1.0; 100.0, 0.5; 500.0, 2.0; 0.0032, 1.0; 1_234.56, 1.25 ]
;;

let test_trail_buy_matches_live () =
  (* exact_target rule: target = min(ref*(1-gi/100), sell - 2*gi/100*ref) *)
  let open Dio_strategies in
  let core_cfg =
    Grid_core.{ (cfg ~price_increment:1e-9 ()) with grid_interval_pct = 1.0 }
  in
  let ref = 100.0 in
  let sell = Grid_core.sell_level core_cfg ~ref in
  let grid_buy = Grid_core.buy_level core_cfg ~ref in
  let exact = sell -. (ref *. (2.0 /. 100.0)) in
  near (Float.min grid_buy exact) (Grid_core.trail_buy_level core_cfg ~bid:ref ~sell)
;;

let test_min_move_matches_live () =
  let st = identity_state () in
  let open Dio_strategies in
  let price = 100.0 in
  let live = Suicide_grid_config.get_min_move_threshold price 1.0 st in
  let core = Grid_core.min_move_threshold (cfg ~price_increment:0.01 ()) price in
  near live core
;;

let test_round_lot_matches_formula () =
  (* Suicide_grid_config.round_qty is floor(qty*inv)/inv with inv = 1/incr;
     Grid_core.round_lot must reproduce the same formula. *)
  let open Dio_strategies in
  let c = cfg ~qty_increment:0.0005 () in
  let inv = 1.0 /. 0.0005 in
  near (Float.floor (0.00123 *. inv) /. inv) (Grid_core.round_lot c 0.00123);
  let c2 = cfg ~qty_increment:0.01 () in
  near 0.0 (Grid_core.round_lot c2 0.004)
;;

let () =
  Alcotest.run
    "grid_core_contract"
    [ ( "levels"
      , [ Alcotest.test_case "buy level matches live" `Quick test_buy_level_matches_live
        ; Alcotest.test_case "sell level matches live" `Quick test_sell_level_matches_live
        ; Alcotest.test_case
            "trail buy matches live rule"
            `Quick
            test_trail_buy_matches_live
        ; Alcotest.test_case "min move matches live" `Quick test_min_move_matches_live
        ; Alcotest.test_case
            "round lot matches formula"
            `Quick
            test_round_lot_matches_formula
        ] )
    ]
;;
