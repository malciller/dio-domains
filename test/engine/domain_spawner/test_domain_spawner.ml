(* Fee fetcher stub: returns the asset with maker/taker fees set. *)
let mock_fee_fetcher (asset : Dio_engine.Config.trading_config)
  : Dio_engine.Config.trading_config
  =
  { asset with maker_fee = Some 0.001; taker_fee = Some 0.002 }
;;

let test_spawn_domains_basic () =
  let assets =
    [ { Dio_engine.Config.exchange = "kraken"
      ; symbol = "BTC/USD"
      ; qty = "0.001"
      ; grid_interval = 1.0, 1.0
      ; sell_mult = "1.0"
      ; min_usd_balance = None
      ; max_exposure = None
      ; strategy = "jacobs_ladder"
      ; maker_fee = None
      ; taker_fee = None
      ; testnet = false
      ; hedge = false
      ; accumulation_buffer = 0.01, 0.01
      ; data_feed = None
      ; base_accumulation = true
      ; sell_levels = true
      ; cpu_priority = 0
      }
    ; { Dio_engine.Config.exchange = "kraken"
      ; symbol = "ETH/USD"
      ; qty = "0.01"
      ; grid_interval = 0.5, 0.5
      ; sell_mult = "1.1"
      ; min_usd_balance = Some "100.0"
      ; max_exposure = Some "500.0"
      ; strategy = "jacobs_ladder"
      ; maker_fee = None
      ; taker_fee = None
      ; testnet = false
      ; hedge = false
      ; accumulation_buffer = 0.01, 0.01
      ; data_feed = None
      ; base_accumulation = true
      ; sell_levels = true
      ; cpu_priority = 0
      }
    ]
  in
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; flow_log =
        { Dio_engine.Config.enabled = false
        ; min_interval_seconds = 0.2
        ; heartbeat_seconds = 5.0
        ; pulse_seconds = 5.0
        ; price_ticks = true
        }
    ; gc = None
    ; oracle = None
    ; trading = assets
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Dio_engine.Config.Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; fng_check_threshold = 1.5
    ; theme = None
    ; strategy_trace = false
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      assets
  in
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  Alcotest.(check int)
    "correct number of domains"
    (List.length assets)
    (List.length status)
;;

let test_spawn_domains_empty () =
  (* Reset registry state from prior tests. *)
  Dio_engine.Domain_spawner.clear_domain_registry ();
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; flow_log =
        { Dio_engine.Config.enabled = false
        ; min_interval_seconds = 0.2
        ; heartbeat_seconds = 5.0
        ; pulse_seconds = 5.0
        ; price_ticks = true
        }
    ; gc = None
    ; oracle = None
    ; trading = []
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Dio_engine.Config.Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; fng_check_threshold = 1.5
    ; theme = None
    ; strategy_trace = false
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      []
  in
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  Alcotest.(check int) "empty domains list length" 0 (List.length status)
;;

let test_fee_fetcher_integration () =
  let asset =
    { Dio_engine.Config.exchange = "kraken"
    ; symbol = "LTC/USD"
    ; qty = "0.1"
    ; grid_interval = 2.0, 2.0
    ; sell_mult = "1.05"
    ; min_usd_balance = None
    ; max_exposure = None
    ; strategy = "jacobs_ladder"
    ; maker_fee = None
    ; taker_fee = None
    ; testnet = false
    ; hedge = false
    ; accumulation_buffer = 0.01, 0.01
    ; data_feed = None
    ; base_accumulation = true
    ; sell_levels = true
    ; cpu_priority = 0
    }
  in
  let asset_with_fees = mock_fee_fetcher asset in
  Alcotest.(check (option (float 0.001)))
    "maker_fee added"
    (Some 0.001)
    asset_with_fees.maker_fee;
  Alcotest.(check (option (float 0.001)))
    "taker_fee added"
    (Some 0.002)
    asset_with_fees.taker_fee
;;

let test_strategy_initialization () =
  Alcotest.(check unit)
    "jacobs_ladder init"
    ()
    (Dio_strategies.Strategy_api.Strategy.init ())
;;

let test_domain_error_handling () =
  (* Reset registry state from prior tests. *)
  Dio_engine.Domain_spawner.clear_domain_registry ();
  let failing_asset =
    { Dio_engine.Config.exchange = "invalid_exchange"
    ; symbol = "TEST/USD"
    ; qty = "0.001"
    ; grid_interval = 1.0, 1.0
    ; sell_mult = "1.0"
    ; min_usd_balance = None
    ; max_exposure = None
    ; strategy = "invalid_strategy"
    ; maker_fee = None
    ; taker_fee = None
    ; testnet = false
    ; hedge = false
    ; accumulation_buffer = 0.01, 0.01
    ; data_feed = None
    ; base_accumulation = true
    ; sell_levels = true
    ; cpu_priority = 0
    }
  in
  (* Domains handle errors internally; the runner must not crash. *)
  let config =
    { Dio_engine.Config.cycle_mod = 10000
    ; logging = { level = Logging.INFO; sections = []; width = None }
    ; flow_log =
        { Dio_engine.Config.enabled = false
        ; min_interval_seconds = 0.2
        ; heartbeat_seconds = 5.0
        ; pulse_seconds = 5.0
        ; price_ticks = true
        }
    ; gc = None
    ; oracle = None
    ; trading = [ failing_asset ]
    ; latency_window_seconds = 5.0
    ; latency_spike_threshold_us = 10.0
    ; latency_spike_report = Dio_engine.Config.Spike_report_internal
    ; latency_spike_report_seconds = 30.0
    ; latency_network_spike_threshold_us = 20_000.0
    ; fng_check_threshold = 1.5
    ; theme = None
    ; strategy_trace = false
    }
  in
  let _supervisor_thread =
    Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets
      config
      mock_fee_fetcher
      [ failing_asset ]
  in
  (* Allow domains time to fail. *)
  Unix.sleepf 0.1;
  let status = Dio_engine.Domain_spawner.get_domain_status () in
  Alcotest.(check int) "domain created for failing asset" 1 (List.length status)
;;

let mk_asset symbol cpu_priority : Dio_engine.Config.trading_config =
  { Dio_engine.Config.exchange = "kraken"
  ; symbol
  ; qty = "1.0"
  ; grid_interval = 1.0, 1.0
  ; sell_mult = "1.0"
  ; min_usd_balance = None
  ; max_exposure = None
  ; strategy = "jacobs_ladder"
  ; maker_fee = None
  ; taker_fee = None
  ; testnet = false
  ; hedge = false
  ; accumulation_buffer = 0.01, 0.01
  ; data_feed = None
  ; base_accumulation = true
  ; sell_levels = true
  ; cpu_priority
  }
;;

let test_order_by_cpu_priority () =
  let symbols assets =
    List.map (fun (a : Dio_engine.Config.trading_config) -> a.symbol) assets
  in
  Alcotest.(check (list string))
    "descending priority, ties keep config order"
    [ "D"; "B"; "A"; "C" ]
    (symbols
       (Dio_engine.Domain_spawner.order_by_cpu_priority
          [ mk_asset "A" 0; mk_asset "B" 5; mk_asset "C" 0; mk_asset "D" 10 ]));
  Alcotest.(check (list string))
    "equal priorities preserve config order"
    [ "A"; "C" ]
    (symbols
       (Dio_engine.Domain_spawner.order_by_cpu_priority
          [ mk_asset "A" 0; mk_asset "C" 0 ]))
;;

open Dio_engine.Domain_spawner

let ladder_params : Dio_engine.Config.overlay_params =
  { start_ratio_pct = 5.0
  ; add_threshold_pct = 2.5
  ; max_ratio_pct = 75.0
  ; short_tp_roe = 5.0
  ; recycle_base_share_pct = 50.0
  ; min_entry_margin_ratio = 1.0
  ; min_liq_mult = 1.0
  ; full_balance_ref = true
  }
;;

let test_ladder () = derive_overlay_ladder ~leverage:20.0 ladder_params
let close name expected actual = Alcotest.(check (float 1e-6)) name expected actual
let step () = (test_ladder ()).ol_add_threshold /. 20.0

(* Average entry and short of a ladder that filled [filled] rungs from [anchor]. *)
let ladder_position ~filled ~anchor ~balance =
  let l = test_ladder () in
  let step = step () in
  let size k = l.ol_add_first *. (l.ol_add_growth ** float_of_int k) in
  let ratio = ref l.ol_start_ratio in
  let weighted = ref l.ol_start_ratio in
  for k = 0 to filled - 1 do
    let s = size k in
    ratio := !ratio +. s;
    weighted := !weighted +. (s *. (1.0 +. (float_of_int (k + 1) *. step)))
  done;
  !weighted /. !ratio *. anchor, !ratio *. balance
;;

let test_reconstruct_fresh () =
  (* No adds: the entry is the origin and the first band is one ROE step above it. *)
  let l = test_ladder () in
  let entry = 80_000.0 in
  let short_qty = l.ol_start_ratio *. 1.0 in
  let seed =
    reconstruct_ladder l ~leverage:20.0 ~entry ~short_qty ~accumulated:1.0 ~mark:80_100.0
  in
  let size k = l.ol_add_first *. (l.ol_add_growth ** float_of_int k) in
  close "fresh anchor is the entry" entry seed.ls_anchor;
  Alcotest.(check int) "fresh starts at rung 0" 0 seed.ls_add_index;
  close "fresh carries nothing" 0.0 seed.ls_carry;
  close "first rung size" (size 0) seed.ls_rung_qty;
  close "first band one step above the entry" (entry *. (1.0 +. step ())) seed.ls_next_add
;;

let test_reconstruct_catches_up () =
  (* Five rungs on the book, price now at rung 8: rungs 6-7 move into the carry, the
     index stops at rung 7 so the next arm places 6-8 at the current mark. *)
  let l = test_ladder () in
  let anchor = 80_000.0 in
  let size k = l.ol_add_first *. (l.ol_add_growth ** float_of_int k) in
  let entry, short_qty = ladder_position ~filled:5 ~anchor ~balance:1.0 in
  let mark = anchor *. (1.0 +. (8.0 *. step ())) in
  let seed =
    reconstruct_ladder l ~leverage:20.0 ~entry ~short_qty ~accumulated:1.0 ~mark
  in
  close "mid-ladder anchor recovered" anchor seed.ls_anchor;
  Alcotest.(check int) "stopped one rung below the price" 7 seed.ls_add_index;
  close "missed sizes are carried" (size 5 +. size 6) seed.ls_carry;
  close "next rung is the price rung" (size 7) seed.ls_rung_qty;
  close "next band is the price band" mark seed.ls_next_add
;;

let test_reconstruct_retrace_rearms () =
  (* A price below the last filled band re-arms the retraced rungs: their size is added
     again on the way back up, ratcheting the average entry toward the mark. *)
  let l = test_ladder () in
  let anchor = 80_000.0 in
  let entry, short_qty = ladder_position ~filled:5 ~anchor ~balance:1.0 in
  let mark = anchor *. (1.0 +. (2.0 *. step ())) in
  let seed =
    reconstruct_ladder l ~leverage:20.0 ~entry ~short_qty ~accumulated:1.0 ~mark
  in
  close "anchor still recovered" anchor seed.ls_anchor;
  Alcotest.(check int) "retrace re-arms down to the price rung" 2 seed.ls_add_index;
  close "retrace carries nothing" 0.0 seed.ls_carry;
  close
    "next band is one step above the retrace"
    (anchor *. (1.0 +. (3.0 *. step ())))
    seed.ls_next_add
;;

let () =
  Alcotest.run
    "Domain Spawner"
    [ ( "spawning"
      , [ Alcotest.test_case "basic spawning" `Quick test_spawn_domains_basic
        ; Alcotest.test_case "empty list" `Quick test_spawn_domains_empty
        ; Alcotest.test_case "error handling" `Quick test_domain_error_handling
        ] )
    ; ( "affinity"
      , [ Alcotest.test_case "cpu priority ordering" `Quick test_order_by_cpu_priority ] )
    ; ( "ladder"
      , [ Alcotest.test_case
            "fresh short anchors at the entry"
            `Quick
            test_reconstruct_fresh
        ; Alcotest.test_case
            "mid-ladder catch-up carries the crossed rungs"
            `Quick
            test_reconstruct_catches_up
        ; Alcotest.test_case
            "retrace re-arms the rungs below the price"
            `Quick
            test_reconstruct_retrace_rearms
        ] )
    ; ( "integration"
      , [ Alcotest.test_case "fee fetcher" `Quick test_fee_fetcher_integration
        ; Alcotest.test_case "strategy init" `Quick test_strategy_initialization
        ] )
    ]
;;
