(** Canonical per-stage latency rows, for the latency journal.

    This is the one place that enumerates every profiler the engine keeps, so the journal
    covers exactly the stages the dashboard renders. Sources, in precedence order (the
    first row for a [domain ^ stage] wins, matching the dashboard's "full segment set
    beats the controller's single span"):

    - per-asset domains: [orderbook], [execution], [prep], [strategy], [cycle];
    - carry controllers and overlay instances, same five stages, keyed by
      ["<venue>:<instrument>"];
    - the capital oracle's per-asset pipeline, stage [oracle];
    - the capital oracle's engine-global passes, domain [_oracle];
    - per-venue network windows: [ws_ping], [ws_feed], [rest_request], [signer], keyed by
      venue;
    - per-venue parse pipeline: [feed_queue] (receive -> dequeue) and [feed_parse]
      (decode + book apply), keyed by venue;
    - order-operation windows per symbol: [place], [amend], [cancel];
    - the stop-the-world canary, domain [_engine], stage [canary].

    Readers only: every source is a published atomic snapshot, so a caller cannot observe
    a torn histogram or block a trading cycle. *)

(** Synthetic canary row: the canary reports one maximum per window, not a distribution,
    so every percentile field carries that maximum. Use the peak over every window since
    the journal's previous tick, not just the newest window: the journal runs slower than
    the canary, and sampling one window in six dropped most spikes. Marking the row this
    way keeps [tail] and [compare] arithmetic sane instead of reporting a p99 of zero. *)
let canary_snapshot () =
  let peak_ns, cause = Canary.take_peak () in
  let max_us = float_of_int peak_ns /. 1000.0 in
  let now = Unix.gettimeofday () in
  { Latency_profiler.name = "canary"
  ; p50 = max_us
  ; p90 = max_us
  ; p95 = max_us
  ; p99 = max_us
  ; p999 = max_us
  ; samples = 1
  ; union_samples = 1
  ; sub_us_samples = 0
  ; overflow = 0
  ; max_us
  ; max_time = now
  ; over_threshold = 0
  ; max_cause = cause
  ; executions = 0
  ; last_exec_time = 0.0
  ; window_start = now
  ; window_end = now
  }
;;

let all () : Latency_journal.row list =
  let seen = Hashtbl.create 64 in
  let acc = ref [] in
  let add (domain, stages) =
    List.iter
      (fun (stage, snap_opt) ->
        match snap_opt with
        | None -> ()
        | Some snap ->
          let key = domain ^ "\000" ^ stage in
          if not (Hashtbl.mem seen key)
          then (
            Hashtbl.replace seen key ();
            acc := { Latency_journal.domain; stage; snap } :: !acc))
      stages
  in
  List.iter add (Domain_spawner.get_domain_profiler_snapshots ());
  List.iter add (Domain_spawner.get_controller_profiler_snapshots ());
  List.iter add (Domain_spawner.get_overlay_profiler_snapshots ());
  List.iter
    (fun (symbol, snap_opt) -> add (symbol, [ "oracle", snap_opt ]))
    (Dio_oracle.Oracle_runtime.asset_profiler_snapshots ());
  add ("_oracle", Dio_oracle.Oracle_runtime.profiler_snapshots ());
  List.iter
    (fun (venue, stages) -> add (venue, stages))
    (Network_latency.all_venue_snapshots ());
  List.iter add (Order_executor.all_profiler_snapshots ());
  (match Canary.is_running () with
   | true -> add ("_engine", [ "canary", Some (canary_snapshot ()) ])
   | false -> ());
  List.rev !acc
;;
