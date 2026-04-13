module LP = Latency_profiler

let setup_huge_heap id =
  let local_cache = Hashtbl.create 50_000 in
  for i = 1 to 50_000 do
    let key = Printf.sprintf "order-%d" i in
    let data = Printf.sprintf "massive_payload_data_string_that_will_live_forever_%d_%d" id i in
    Hashtbl.replace local_cache key data
  done;
  local_cache

let simulate_tick local_cache profiler tick_id =
  let span_start = Mtime_clock.now_ns () in

  (* 1. High Velocity Minor Allocation *)
  let msgs = List.init 250 (fun i ->
    let price = 50000.0 +. float_of_int (i mod 100) in
    let size = 0.5 +. float_of_int (i mod 10) in
    Printf.sprintf "{\"e\":\"trade\",\"p\":\"%.2f\",\"q\":\"%.2f\",\"T\":%Ld}" price size span_start
  ) in

  let parsed = List.map String.length msgs in
  let _sum = List.fold_left (+) 0 parsed in

  (* 2. Write Barrier Trigger *)
  (* Mutate old generation with new generation pointers continuously *)
  let target_index = tick_id mod 50_000 in
  let key = Printf.sprintf "order-%d" target_index in
  let new_dynamic_str = Printf.sprintf "updated-value-%Ld" span_start in
  Hashtbl.replace local_cache key new_dynamic_str;

  let span_end = Mtime_clock.now_ns () in
  LP.record profiler (Mtime.Span.of_uint64_ns (Int64.sub span_end span_start))

let worker_domain id total_ticks =
  let prof_name = Printf.sprintf "hammer_%d" id in
  let profiler = LP.create ~bucket_us:10 ~max_latency_us:5000_000 prof_name in
  
  let local_cache = setup_huge_heap id in
  
  (* Force local GC to promote cache before hot loop *)
  Gc.minor ();

  for i = 1 to total_ticks do
    simulate_tick local_cache profiler i
  done;

  LP.percentile profiler 0.99, LP.percentile profiler 0.999

let () =
  let domains_count = 6 in
  let ticks_per_domain = 40_000 in
  
  Printf.printf "Starting GC Hammer with %d domains, %d ticks each...\n%!" domains_count ticks_per_domain;

  let start_time = Unix.gettimeofday () in

  let handles = List.init domains_count (fun id ->
    Domain.spawn (fun () -> worker_domain id ticks_per_domain)
  ) in

  let results = List.map Domain.join handles in

  let end_time = Unix.gettimeofday () in

  Printf.printf "\n============= GC HAMMER SUMMARY =============\n";
  Printf.printf "Total Wall Time: %.2fs\n" (end_time -. start_time);
  
  let p99s = List.map fst results in
  let p999s = List.map snd results in
  
  let avg_p99 = List.fold_left (+.) 0.0 p99s /. float_of_int domains_count in
  let avg_p999 = List.fold_left (+.) 0.0 p999s /. float_of_int domains_count in
  let max_p999 = List.fold_left max 0.0 p999s in

  Printf.printf "AVG P99 Latency:  %10.2f us\n" avg_p99;
  Printf.printf "AVG P999 Latency: %10.2f us\n" avg_p999;
  Printf.printf "MAX P999 Latency: %10.2f us\n" max_p999;
  
  (* We output a machine-readable line line MAX_P999: value so the bash script can grep it *)
  Printf.printf "SWEEP_METRIC_P99: %10.2f\n" avg_p99;
  Printf.printf "SWEEP_METRIC_MAX_P999: %10.2f\n" max_p999;
  Printf.printf "=============================================\n%!"
