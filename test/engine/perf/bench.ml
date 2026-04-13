module LP  = Latency_profiler
module SC  = Dio_strategies.Strategy_common
module SG  = Dio_strategies.Suicide_grid
module FC  = Dio_strategies.Fee_cache


(* ── helpers ──────────────────────────────────────────────────────────────── *)

(** Run [f] for [n] iterations, recording each call in profiler [p]. *)
let run_bench p n f =
  for _ = 1 to n do
    LP.time_it p f |> ignore
  done

(** Measure total wall time in ms for a block. *)
let wall_ms f =
  let t0 = Mtime_clock.now_ns () in
  f ();
  let t1 = Mtime_clock.now_ns () in
  Int64.to_float (Int64.sub t1 t0) /. 1_000_000.0

(** Pretty-print the results table. *)
let print_results results =
  let sep = String.make 78 '-' in
  Printf.eprintf "\n%s\n" sep;
  Printf.eprintf "%-40s %8s %10s %10s %10s %10s\n"
    "Benchmark" "N" "p50 (µs)" "p90 (µs)" "p99 (µs)" "total (ms)";
  Printf.eprintf "%s\n" sep;
  List.iter (fun (name, n, p50, p90, p99, total_ms) ->
    Printf.eprintf "%-40s %8d %10.2f %10.2f %10.2f %10.2f\n"
      name n p50 p90 p99 total_ms
  ) results;
  Printf.eprintf "%s\n\n" sep

(* ── benchmarks ───────────────────────────────────────────────────────────── *)

let bench_ringbuffer () =
  let name = "ringbuffer_write_read" in
  let n    = 10_000 in
  let buf  = SC.OrderRingBuffer.create 16 in
  let order =
    SG.create_place_order "BTC/USD|buy|grid" "BTC/USD" SC.Buy 0.001 (Some 50000.0) true SC.Grid "kraken"
  in
  let p = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let _ = SC.OrderRingBuffer.write buf order in
      let _ = SC.OrderRingBuffer.read  buf       in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_inflight_orders () =
  let name = "inflight_orders_ops" in
  let n    = 10_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let key = "BTC/USD|buy|0.00100000|50000.00000000" in
      let _   = SC.InFlightOrders.add_in_flight_order    key in
      let _   = SC.InFlightOrders.remove_in_flight_order key in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_inflight_amendments () =
  let name = "inflight_amendments_ops" in
  let n    = 10_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let oid = "order-abc-123" in
      let _   = SC.InFlightAmendments.add_in_flight_amendment    oid in
      let _   = SC.InFlightAmendments.is_in_flight               oid in
      let _   = SC.InFlightAmendments.remove_in_flight_amendment oid in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_fee_cache () =
  let name = "fee_cache_store_get" in
  let n    = 5_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  FC.init ();
  FC.clear ();
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      FC.store_fees
        ~exchange:"kraken" ~symbol:"BTC/USD"
        ~maker_fee:0.001 ~taker_fee:0.001 ~ttl_seconds:600.0;
      let _ = FC.get_maker_fee ~exchange:"kraken" ~symbol:"BTC/USD" in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_order_creation_place () =
  let name = "order_creation_place" in
  let n    = 5_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let _ =
        SG.create_place_order "BTC/USD|buy|grid" "BTC/USD" SC.Buy 0.001 (Some 50000.0) true SC.Grid "kraken"
      in ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_order_creation_amend () =
  let name = "order_creation_amend" in
  let n    = 5_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let _ =
        SG.create_amend_order
          "order-xyz" "BTC/USD" SC.Sell 0.001 (Some 51000.0) true SC.Grid "kraken"
      in ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_config_parse () =
  let name = "config_parse_float" in
  let n    = 10_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let _ = SG.parse_config_float "0.001" "grid_pct" 0.01 "kraken" "BTC/USD" in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_grid_price_calc () =
  let name = "price_calc_grid" in
  let n    = 10_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let state = SG.get_strategy_state "BTC/USD" in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let _ = SG.calculate_grid_price 50000.0 1.0 true  state in
      let _ = SG.calculate_grid_price 50000.0 1.0 false state in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_state_warmup () =
  let name = "state_get_100_symbols" in
  let n    = 100 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let sym = Printf.sprintf "SYM%03d/USD" (Random.int 100) in
      let _   = SG.get_strategy_state sym in
      ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

let bench_duplicate_key_gen () =
  let name = "generate_duplicate_key" in
  let n    = 10_000 in
  let p    = LP.create ~max_latency_us:100_000 name in
  let total_ms = wall_ms (fun () ->
    run_bench p n (fun () ->
      let _ =
        SC.generate_duplicate_key "BTC/USD" "buy" 0.001 (Some 50000.0)
      in ()
    )
  ) in
  (name, n,
   LP.percentile p 0.50,
   LP.percentile p 0.90,
   LP.percentile p 0.99,
   total_ms)

(* ── entry point ──────────────────────────────────────────────────────────── *)

let () =
  Random.self_init ();
  (* Save real stderr so we can print bench results through it.
     Then mute the library's instrument-feed WARN spam during benchmarks. *)
  let real_err = Unix.dup Unix.stderr in
  let devnull  = Unix.openfile "/dev/null" [Unix.O_WRONLY] 0 in
  Unix.dup2 devnull Unix.stderr;
  Unix.close devnull;
  SG.Strategy.init ();
  FC.init ();

  let results = [
    bench_ringbuffer          ();
    bench_inflight_orders     ();
    bench_inflight_amendments ();
    bench_fee_cache           ();
    bench_order_creation_place ();
    bench_order_creation_amend ();
    bench_config_parse        ();
    bench_grid_price_calc     ();
    bench_state_warmup        ();
    bench_duplicate_key_gen   ();
  ] in

  (* Restore stderr so our output is always visible, even under dune runtest *)
  Unix.dup2 real_err Unix.stderr;
  Unix.close real_err;

  Printf.eprintf "Running performance benchmarks...\n%!";

  print_results results;
  (* Sleep 1s so Dune finishes all parallel tests before flushing this output last Unix.sleep 1 *)

