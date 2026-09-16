(* Per-cycle allocation profile of the *live* config-driven strategy loop.

   The production domain attributes minor-word allocation to each fine phase
   ([reset_phase_metrics] + [Strategy_cycle_engine.measure]) and the dashboard prints it.
   This harness drives the real strategy file through the real handler with a populated
   ledger so every buy/sell phase actually runs, then reports the per-phase words for a
   single profiled cycle after a steady-state warmup. Build with the release profile
   (flambda -O3), matching production:

   dune build --profile release test/engine/perf/cycle_alloc_profile.exe
   ./_build/default/test/engine/perf/cycle_alloc_profile.exe *)

module SG = Dio_strategies.Strategy_api
module SR = Dio_strategies.Strategy_runtime
module SF = Dio_strategies.Strategy_file
module SA = Dio_strategies.Strategy_actions_cycle
module SCE = Dio_strategies.Strategy_cycle_engine
module SS = Dio_strategies.Strategy_state
module Sell_orders = Dio_strategies.Strategy_sell_orders

let asset : SS.trading_config =
  { exchange = "kraken"
  ; symbol = "BENCH/USD"
  ; qty = "1.0"
  ; grid_interval = 1.0
  ; sell_mult = "1.0"
  ; strategy = "jacobs_ladder"
  ; maker_fee = Some 0.0004
  ; taker_fee = None
  ; accumulation_buffer = 0.0
  ; base_accumulation = false
  ; sell_levels_persistence = false
  }
;;

let path () =
  match Sys.getenv_opt "DUNE_SOURCEROOT" with
  | Some root -> Filename.concat root "strategies/jacobs_ladder.json"
  | None -> "strategies/jacobs_ladder.json"
;;

let () =
  SG.Strategy.init ();
  (match Sys.getenv_opt "DIO_MINOR_HEAP" with
   | Some s ->
     (try
        let w = int_of_string (String.trim s) in
        let c = Gc.get () in
        Gc.set { c with minor_heap_size = w };
        Printf.printf "minor_heap_size set to %d words\n%!" w
      with
      | _ -> ())
   | None -> ());
  match SF.parse_file (path ()) with
  | Error e -> Printf.eprintf "cycle_alloc_profile: %s\n%!" e
  | Ok file ->
    let state = SG.get_strategy_state "BENCH/USD" in
    state.grid_qty <- 1.0;
    state.position_base <- 10.0;
    state.position_initialized <- true;
    state.last_buy_fill_price <- Some 100.0;
    state.last_buy_fill_qty <- Some 1.0;
    (* A resting buy so the buy-amend leg runs. *)
    state.last_buy_order_id <- Some "buy-1";
    state.last_buy_order_price <- Some 99.0;
    (* A resting ladder: persisted levels + live feed sells + ledger commitments. *)
    state.persisted_sell_levels <- List.init 8 (fun i -> 100.0 +. float i, 1.0);
    for i = 1 to 8 do
      SG.upsert_sell_commitment
        ~state
        ~id:(Printf.sprintf "sell-%d" i)
        ~price:(100.0 +. float i)
        ~qty:1.0
        ~seen:true
        ~acked:true
    done;
    for i = 1 to 8 do
      Sell_orders.push
        state.open_sell_orders
        (Printf.sprintf "sell-%d" i)
        (100.0 +. float i)
        1.0
    done;
    let ecfg = SG.get_exchange_config "kraken" in
    let ctx = SCE.create () in
    ctx.cg_asset <- Some asset;
    ctx.cg_state <- Some state;
    ctx.cg_ecfg <- Some ecfg;
    ctx.cg_price <- 100.0;
    ctx.cg_bid <- 99.9;
    ctx.cg_ask <- 100.1;
    ctx.cg_abal <- 10.0;
    ctx.cg_qbal <- 1_000_000.0;
    ctx.cg_base_age <- Some 1.0;
    ctx.cg_gen <- 0;
    ctx.cg_iter
    <- (let orders =
          ("buy-1", 99.0, 1.0, "buy", None)
          :: List.init 8 (fun i ->
            Printf.sprintf "sell-%d" i, 100.0 +. float i, 1.0, "sell", None)
        in
        fun f -> List.iter (fun (id, p, q, s, u) -> f id p q s u) orders);
    (* Representative delta: one same-price sell amend per cycle, no overflow, so the
       measured cycle is the production incremental path rather than a full rescan. *)
    ctx.cg_drain
    <- (fun ~symbol:_ -> [ "sell-0", Some (Some 100.0, 1.0, "sell", None) ], false);
    let module H = SA.Make (SCE) in
    let rt = SR.create ~handlers:(H.handler ctx) file in
    let event = SR.make_event "book_update" [] in
    let run i =
      ctx.cg_gen <- i;
      ignore (SR.run_cycle ~collect:false rt ~price:100.0 ~now:(float i) ~event)
    in
    (* Warm up so platform facts, caches and the ledger reach steady state. *)
    for i = 1 to 2000 do
      run i
    done;
    let n = 20_000 in
    let times = Array.make n 0.0 in
    let before = Gc.minor_words () in
    for i = 1 to n do
      let t0 = Monotonic_clock.now_ns () in
      run (3000 + i);
      times.(i - 1) <- float (Monotonic_clock.now_ns () - t0) /. 1000.0
    done;
    let after = Gc.minor_words () in
    (* index of the worst cycle and the next-worst, to see if spikes are periodic *)
    let argmax = ref 0 in
    Array.iteri (fun i v -> if v > times.(!argmax) then argmax := i) times;
    Array.sort compare times;
    Printf.printf "worst cycle index=%d (period?)\n%!" !argmax;
    Printf.printf
      "steady-state live cycle: %.1f minor words/cycle (%.1f bytes)  p50=%.1fus \
       p99=%.1fus max=%.1fus\n\
       %!"
      ((after -. before) /. float n)
      ((after -. before) /. float n *. 8.0)
      times.(n / 2)
      times.(n * 99 / 100)
      times.(n - 1);
    (* Same loop with per-phase profiling ON, which is exactly the mode the production
       dashboard samples (it records STRATEGY only on [latency_this_cycle]). This isolates
       the cost of the [measure]/[sub_start] instrumentation itself. *)
    let ptimes = Array.make n 0.0 in
    ctx.cg_profile <- true;
    state.profiling <- true;
    rt.prof_enabled <- true;
    let gmax = ref 0
    and amax = ref 0
    and missmax = ref 0 in
    for i = 1 to n do
      let t0 = Monotonic_clock.now_ns () in
      run (3000 + i);
      ptimes.(i - 1) <- float (Monotonic_clock.now_ns () - t0) /. 1000.0;
      if rt.prof_guard_ns > !gmax then gmax := rt.prof_guard_ns;
      if rt.prof_args_ns > !amax then amax := rt.prof_args_ns;
      if rt.prof_missing > !missmax then missmax := rt.prof_missing
    done;
    rt.prof_enabled <- false;
    ctx.cg_profile <- false;
    state.profiling <- false;
    Array.sort compare ptimes;
    Printf.printf
      "profiling ON cycle: p50=%.1fus p99=%.1fus max=%.1fus  guardmax=%.1fus \
       argsmax=%.1fus missmax=%d\n\
       %!"
      ptimes.(n / 2)
      ptimes.(n * 99 / 100)
      ptimes.(n - 1)
      (float !gmax /. 1000.0)
      (float !amax /. 1000.0)
      !missmax;
    (* Steady-state per-phase allocation: reset the counters, run N cycles with profiling
       ON and report the per-phase average. A single profiled cycle is too noisy to target
       (the same phase varies by 5-8x run to run); this is the attribution to act on. *)
    SS.reset_phase_metrics ~profiling:true state;
    ctx.cg_profile <- true;
    rt.prof_enabled <- true;
    let pn = 20_000 in
    for i = 1 to pn do
      run (500_000 + i)
    done;
    rt.prof_enabled <- false;
    ctx.cg_profile <- false;
    let avg w = float w /. float pn in
    Printf.printf "steady-state per-phase allocation (words/cycle):\n";
    let p name w = Printf.printf "  %-14s %8.2f w\n" name (avg w) in
    p "preamble" state.alloc_preamble_words;
    p "facts" state.alloc_facts_words;
    p "cleanup" state.alloc_cleanup_words;
    p "sync" state.alloc_sync_words;
    p "buy" state.alloc_buy_words;
    p "buy_plan" state.alloc_buy_plan_words;
    p "buy_amend" state.alloc_buy_amend_words;
    p "sell" state.alloc_sell_words;
    p "sell_plan" state.alloc_sell_plan_words;
    p "sell_place" state.alloc_sell_place_words;
    p "sell_finalize" state.alloc_sell_finalize_words;
    p "sfin_latch" state.alloc_sfin_latch_words;
    p "sfin_sweep" state.alloc_sfin_sweep_words;
    p "sfin_end" state.alloc_sfin_end_words;
    Printf.printf
      "  sell-plan sub-timers: overlays=%dus reconcile=%dus\n%!"
      (state.time_splan_overlays_ns / 1000)
      (state.time_splan_reconcile_ns / 1000);
    (* Populated interpreter-only cycle: the real file + real facts/state, but a no-op
       action handler, so the words are guard eval + arg eval + step plumbing exactly as
       the live loop pays them (no body work, no I/O). This is the metric to optimize. *)
    let rt2 = SR.create ~handlers:SR.noop_handler file in
    rt2.platform <- Array.copy rt.platform;
    rt2.state <- Array.copy rt.state;
    let interp_words =
      let b = Gc.minor_words () in
      let n = 200_000 in
      for i = 1 to n do
        ignore (SR.run_cycle ~collect:false rt2 ~price:100.0 ~now:(float i) ~event)
      done;
      (Gc.minor_words () -. b) /. float n
    in
    let interp_us =
      let n = 50_000 in
      let t0 = Monotonic_clock.now_ns () in
      for i = 1 to n do
        ignore (SR.run_cycle ~collect:false rt2 ~price:100.0 ~now:(float i) ~event)
      done;
      float (Monotonic_clock.now_ns () - t0) /. float n /. 1000.0
    in
    Printf.printf
      "  interpreter-only cycle: %.1f w/cycle, %.2fus/cycle\n%!"
      interp_words
      interp_us;
    (* Attribute the interpreter plumbing: guard evaluation (live [eval_exn], no per-guard
       [Ok]) and action-argument evaluation. *)
    let e = SR.env_of rt in
    let facts = SR.facts_of rt in
    let parse = SR.parse_guard_expr rt in
    let guards = List.filter_map (fun (s : SF.step) -> s.st_when) file.steps in
    let guard_words =
      let b = Gc.minor_words () in
      let n = 200_000 in
      for _ = 1 to n do
        List.iter
          (fun g ->
            try
              ignore (Dio_strategies.Strategy_guard.eval_exn ~parse_expr:parse e facts g)
            with
            | _ -> ())
          guards
      done;
      (Gc.minor_words () -. b) /. float n
    in
    Printf.printf "  guard eval (all steps): %.1f w/cycle\n%!" guard_words;
    let string_args =
      List.concat_map
        (fun (s : SF.step) ->
          List.concat_map
            (fun (a : SF.action) ->
              List.filter_map
                (fun (_k, j) ->
                  match j with
                  | `String _ -> Some j
                  | _ -> None)
                a.a_args)
            (s.st_then @ s.st_else))
        file.steps
    in
    let arg_words =
      let b = Gc.minor_words () in
      let n = 200_000 in
      for _ = 1 to n do
        List.iter (fun j -> ignore (SR.value_of_json rt e j)) string_args
      done;
      (Gc.minor_words () -. b) /. float n
    in
    Printf.printf
      "  arg eval (%d string args): %.1f w/cycle\n%!"
      (List.length string_args)
      arg_words;
    (* Simulate the live "fact not published yet" case: clear the platform table so every
       [$platform.*] reference misses. Before the single-pass fix this double-evaluated
       and built a message string per miss. *)
    let saved = Array.copy rt.platform in
    rt.platform <- Array.make (Array.length rt.platform) None;
    rt.prof_missing <- 0;
    rt.prof_enabled <- true;
    let miss_words =
      let b = Gc.minor_words () in
      let n = 200_000 in
      for _ = 1 to n do
        List.iter (fun j -> ignore (SR.value_of_json rt e j)) string_args
      done;
      (Gc.minor_words () -. b) /. float n
    in
    let miss_missing = rt.prof_missing in
    rt.prof_enabled <- false;
    let miss_us =
      let n = 50_000 in
      let t0 = Monotonic_clock.now_ns () in
      for _ = 1 to n do
        List.iter (fun j -> ignore (SR.value_of_json rt e j)) string_args
      done;
      float (Monotonic_clock.now_ns () - t0) /. float n /. 1000.0
    in
    rt.platform <- saved;
    Printf.printf
      "  arg eval (all facts missing): %.1f w/cycle, %.2fus/cycle, misses=%d/iter\n%!"
      miss_words
      miss_us
      (miss_missing / 200_000);
    (* Optional: authoritative allocation-site attribution via Gc.Memprof. Sampling is per
       allocated word, so a site's [n_samples * size] estimates the words it allocates.
       Run with DIO_MEMPROF=1 to dump the steady-state cycle's top allocation sites with
       source locations, instead of guessing from a phase counter. *)
    (match Sys.getenv_opt "DIO_MEMPROF" with
     | None -> ()
     | Some _ ->
       let sites : (string, int) Hashtbl.t = Hashtbl.create 512 in
       let frame_of (a : Gc.Memprof.allocation) =
         let entries = Printexc.raw_backtrace_entries a.callstack in
         let rec go i =
           if i >= Array.length entries
           then "<no-debug-info>"
           else (
             match Printexc.backtrace_slots_of_raw_entry entries.(i) with
             | Some slots when Array.length slots > 0 ->
               let s = slots.(0) in
               let name =
                 match Printexc.Slot.name s with
                 | Some n -> n
                 | None -> "?"
               in
               let loc =
                 match Printexc.Slot.location s with
                 | Some l ->
                   Printf.sprintf "%s:%d" (Filename.basename l.filename) l.line_number
                 | None -> "?"
               in
               name ^ " @ " ^ loc
             | _ -> go (i + 1))
         in
         go 0
       in
       let on_alloc (a : Gc.Memprof.allocation) =
         let k = frame_of a in
         let w = a.Gc.Memprof.n_samples in
         Hashtbl.replace
           sites
           k
           (w
            +
            try Hashtbl.find sites k with
            | Not_found -> 0)
       in
       let tracker : (unit, unit) Gc.Memprof.tracker =
         { alloc_minor =
             (fun a ->
               on_alloc a;
               None)
         ; alloc_major = (fun _ -> None)
         ; promote = (fun _ -> None)
         ; dealloc_minor = (fun _ -> ())
         ; dealloc_major = (fun _ -> ())
         }
       in
       let mn = 20_000 in
       let rate = 1.0 in
       let prof = Gc.Memprof.start ~sampling_rate:rate ~callstack_size:12 tracker in
       for i = 1 to mn do
         run (900_000 + i)
       done;
       Gc.Memprof.stop ();
       Gc.Memprof.discard prof;
       let rows = Hashtbl.fold (fun k v acc -> (k, v) :: acc) sites [] in
       let rows = List.sort (fun (_, a) (_, b) -> compare b a) rows in
       Printf.printf "memprof top allocation sites (words/cycle):\n";
       List.iteri
         (fun i (k, v) ->
           if i < 25 then Printf.printf "  %7.1f  %s\n" (float v /. float mn) k)
         rows)
;;
