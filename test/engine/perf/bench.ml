module LP = Latency_profiler
module SC = Dio_strategies.Strategy_common
module SG = Dio_strategies.Strategy_api
module FC = Dio_strategies.Fee_cache
module SR = Dio_strategies.Strategy_runtime
module SF = Dio_strategies.Strategy_file
module SA = Dio_strategies.Strategy_actions_cycle
module SCE = Dio_strategies.Strategy_cycle_engine
module SS = Dio_strategies.Strategy_state

(* ── helpers ──────────────────────────────────────────────────────────────── *)

(** Run [f] for [n] iterations, recording each call in profiler [p]. *)
let run_bench p n f =
  for _ = 1 to n do
    LP.time_it p f |> ignore
  done
;;

(** Measure total wall time in ms for a block. *)
let wall_ms f =
  let t0 = Mtime_clock.now_ns () in
  f ();
  let t1 = Mtime_clock.now_ns () in
  Int64.to_float (Int64.sub t1 t0) /. 1_000_000.0
;;

(** Pretty-print the results table. *)
let print_results results =
  let sep = String.make 78 '-' in
  Printf.eprintf "\n%s\n" sep;
  Printf.eprintf
    "%-40s %8s %10s %10s %10s %10s\n"
    "Benchmark"
    "N"
    "p50 (µs)"
    "p90 (µs)"
    "p99 (µs)"
    "total (ms)";
  Printf.eprintf "%s\n" sep;
  List.iter
    (fun (name, n, p50, p90, p99, total_ms) ->
      Printf.eprintf "%-40s %8d %10.2f %10.2f %10.2f %10.2f\n" name n p50 p90 p99 total_ms)
    results;
  Printf.eprintf "%s\n\n" sep
;;

(* ── benchmarks ───────────────────────────────────────────────────────────── *)

let bench_lockfreequeue () =
  let name = "lockfreequeue_write_read" in
  let n = 10_000 in
  let buf = SC.LockFreeQueue.create () in
  let order =
    SG.create_place_order
      "BTC/USD|buy|grid"
      "BTC/USD"
      SC.Buy
      0.001
      (Some 50000.0)
      true
      SC.Ladder
      "kraken"
  in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let _ = SC.LockFreeQueue.write buf order in
        let _ = SC.LockFreeQueue.read buf in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_inflight_orders () =
  let name = "inflight_orders_ops" in
  let n = 10_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let key = "BTC/USD|buy|0.00100000|50000.00000000" in
        let _ = SC.InFlightOrders.add_in_flight_order key in
        let _ = SC.InFlightOrders.remove_in_flight_order key in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_inflight_amendments () =
  let name = "inflight_amendments_ops" in
  let n = 10_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let oid = "order-abc-123" in
        let _ = SC.InFlightAmendments.add_in_flight_amendment oid in
        let _ = SC.InFlightAmendments.is_in_flight oid in
        let _ = SC.InFlightAmendments.remove_in_flight_amendment oid in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_fee_cache () =
  let name = "fee_cache_store_get" in
  let n = 5_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  FC.init ();
  FC.clear ();
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        FC.store_fees
          ~exchange:"kraken"
          ~symbol:"BTC/USD"
          ~maker_fee:0.001
          ~taker_fee:0.001
          ~ttl_seconds:600.0;
        let _ = FC.get_maker_fee ~exchange:"kraken" ~symbol:"BTC/USD" in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_order_creation_place () =
  let name = "order_creation_place" in
  let n = 5_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let _ =
          SG.create_place_order
            "BTC/USD|buy|grid"
            "BTC/USD"
            SC.Buy
            0.001
            (Some 50000.0)
            true
            SC.Ladder
            "kraken"
        in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_order_creation_amend () =
  let name = "order_creation_amend" in
  let n = 5_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let _ =
          SG.create_amend_order
            "order-xyz"
            "BTC/USD"
            SC.Sell
            0.001
            (Some 51000.0)
            true
            SC.Ladder
            "kraken"
        in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_config_parse () =
  let name = "config_parse_float" in
  let n = 10_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let _ = SG.parse_config_float "0.001" "grid_pct" 0.01 "kraken" "BTC/USD" in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_grid_price_calc () =
  let name = "price_calc_grid" in
  let n = 10_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let state = SG.get_strategy_state "BTC/USD" in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let _ = SG.calculate_grid_price 50000.0 1.0 true state in
        let _ = SG.calculate_grid_price 50000.0 1.0 false state in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_state_warmup () =
  let name = "state_get_100_symbols" in
  let n = 100 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let sym = Printf.sprintf "SYM%03d/USD" (Random.int 100) in
        let _ = SG.get_strategy_state sym in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

let bench_duplicate_key_gen () =
  let name = "generate_duplicate_key" in
  let n = 10_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        let _ = SC.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0) in
        ()))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

(* Per-cycle interpreter overhead for the real strategy file: the file's steps, guards,
   gate/fact publication and dispatch, with a no-op engine context (the stateful action
   bodies need live state and are measured separately via the dashboard phases). *)
let bench_strategy_cycle () =
  let name = "strategy_run_cycle_real_file" in
  match SF.parse_file "strategies/jacobs_ladder.json" with
  | Error e ->
    Printf.eprintf "bench: %s\n%!" e;
    name, 0, 0.0, 0.0, 0.0, 0.0
  | Ok file ->
    let ctx = SCE.create () in
    let module H = SA.Make (SCE) in
    let handlers = H.handler ctx in
    let rt = SR.create ~handlers file in
    let event = SR.make_event "book_update" [] in
    let n = 200_000 in
    let p = LP.create ~max_latency_us:100_000 name in
    let w0 = Gc.minor_words () in
    let total_ms =
      wall_ms (fun () ->
        run_bench p n (fun () -> ignore (SR.run_cycle rt ~price:100.0 ~now:1.0 ~event)))
    in
    let words = (Gc.minor_words () -. w0) /. float n in
    Printf.eprintf "  %s: %.1f words/cycle (interpreter core)\n%!" name words;
    name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

(* Synthetic asset used by the strategy-body benchmarks. *)
let bench_asset : SS.trading_config =
  { exchange = "kraken"
  ; symbol = "BENCH/USD"
  ; qty = "1.0"
  ; grid_interval = 1.0
  ; sell_mult = "1.0"
  ; strategy = "jacobs_ladder"
  ; maker_fee = None
  ; taker_fee = None
  ; accumulation_buffer = 0.0
  ; base_accumulation = false
  ; sell_levels_persistence = false
  }
;;

(* Feed scan + ledger reconcile with 11 resting orders (10 sells, 1 buy). *)
let bench_sync_scan () =
  let name = "sync_open_orders_48_orders" in
  let state = SG.get_strategy_state "BENCH/USD" in
  let ecfg = SG.get_exchange_config "kraken" in
  state.persisted_sell_levels <- List.init 48 (fun i -> 100.0 +. float i, 1.0);
  (* Seed a realistic ledger: 48 live sell commitments + resting open sells. *)
  for i = 1 to 48 do
    SG.upsert_sell_commitment
      ~state
      ~id:(Printf.sprintf "sell-%d" i)
      ~price:(100.0 +. float i)
      ~qty:1.0
      ~seen:true
      ~acked:true
  done;
  for i = 1 to 48 do
    Dio_strategies.Strategy_sell_orders.push
      state.open_sell_orders
      (Printf.sprintf "sell-%d" i)
      (100.0 +. float i)
      1.0
  done;
  let it f =
    for i = 1 to 48 do
      f (Printf.sprintf "sell-%d" i) 1.0 (100.0 +. float i) "sell" None
    done;
    f "buy-1" 1.0 99.0 "buy" None
  in
  let n = 200_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        ignore
          (SG.sync_open_orders
             ~state
             ~now:1.0
             ~asset:bench_asset
             ~bid_price:99.0
             ~lot_qty:1.0
             ~iter_open_orders:it
             ~get_open_orders_generation:
               (let g = ref 0 in
                fun () ->
                  incr g;
                  !g)
             ~ecfg)))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

(* Sell-leg fact derivation with 3 persisted levels to reconcile. *)
let bench_sell_prepare () =
  let name = "sell_leg_prepare" in
  let state = SG.get_strategy_state "BENCH/USD" in
  let ecfg = SG.get_exchange_config "kraken" in
  let persisted_reconcile = [], [ 100.0, 1.0; 101.0, 1.0; 102.0, 1.0 ] in
  let n = 200_000 in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms =
    wall_ms (fun () ->
      run_bench p n (fun () ->
        ignore
          (SG.sell_leg_prepare
             ~persisted_reconcile
             ~state
             ~now:1.0
             ~asset:bench_asset
             ~bid_price:99.0
             ~ask_price:100.0
             ~asset_balance:10.0
             ~buy_attempted:false
             ~oracle_halted:false
             ~ecfg
             ~locked_in_sells:0.0
             ~base_balance_age:(Some 1.0))))
  in
  name, n, LP.percentile p 0.50, LP.percentile p 0.90, LP.percentile p 0.99, total_ms
;;

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Random.self_init ();
  (* Preserve real stderr for results; mute instrument-feed WARN spam during benchmarks. *)
  let real_err = Unix.dup Unix.stderr in
  let devnull = Unix.openfile "/dev/null" [ Unix.O_WRONLY ] 0 in
  Unix.dup2 devnull Unix.stderr;
  Unix.close devnull;
  SG.Strategy.init ();
  FC.init ();
  let results =
    [ bench_lockfreequeue ()
    ; bench_inflight_orders ()
    ; bench_inflight_amendments ()
    ; bench_fee_cache ()
    ; bench_order_creation_place ()
    ; bench_order_creation_amend ()
    ; bench_config_parse ()
    ; bench_grid_price_calc ()
    ; bench_state_warmup ()
    ; bench_duplicate_key_gen ()
    ; bench_strategy_cycle ()
    ; bench_sync_scan ()
    ; bench_sell_prepare ()
    ]
  in
  (* Restore stderr so benchmark output is not suppressed by the test runner. *)
  Unix.dup2 real_err Unix.stderr;
  Unix.close real_err;
  Printf.eprintf "Running performance benchmarks...\n%!";
  print_results results
;;
