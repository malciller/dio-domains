(* Tests for Dio_oracle.Oracle_runtime: the fast-poll cadence used while no
   asset is active (so a fully deployed account recognizes capital returns
   quickly), the default runtime knobs, and the lock-free event trigger
   ([request_pass] wakes [wait_until] early). *)

let default_config () = Dio_oracle.Oracle_runtime.default_config ()

let make_decision ~active =
  { Dio_oracle.Oracle_runtime.exchange = "kraken"
  ; symbol = "X/USD"
  ; active
  ; reason = "test"
  ; qty = 1.0
  ; grid_interval = 1.0
  ; d_surv = 0.99
  ; d_gov = 0.1
  ; governing_horizon = "1d"
  ; deployed = 100.0
  ; pool_share = 100.0
  ; remainder = 0.0
  ; range = None
  ; warnings = []
  ; updated_at = 0.0
  }
;;

let test_poll_while_all_inactive () =
  (* A fully deployed account (every decision inactive, e.g. "cannot fund the
     first buy" while awaiting sell fills) polls at the fast cadence. *)
  let config = { (default_config ()) with poll_seconds = 5.0; refresh_seconds = 300.0 } in
  let decisions = [ make_decision ~active:false; make_decision ~active:false ] in
  Alcotest.(check (float 0.0001))
    "fast poll while all inactive"
    5.0
    (Dio_oracle.Oracle_runtime.next_sleep ~config ~decisions)
;;

let test_normal_cadence_while_active () =
  (* At least one active asset keeps the normal refresh cadence. *)
  let config = { (default_config ()) with poll_seconds = 5.0; refresh_seconds = 300.0 } in
  let decisions = [ make_decision ~active:true; make_decision ~active:false ] in
  Alcotest.(check (float 0.0001))
    "refresh cadence while active"
    300.0
    (Dio_oracle.Oracle_runtime.next_sleep ~config ~decisions)
;;

let test_normal_cadence_on_empty_snapshot () =
  (* Nothing published yet: keep the normal cadence, do not spin. *)
  let config = { (default_config ()) with poll_seconds = 5.0; refresh_seconds = 300.0 } in
  Alcotest.(check (float 0.0001))
    "refresh cadence on empty snapshot"
    300.0
    (Dio_oracle.Oracle_runtime.next_sleep ~config ~decisions:[])
;;

let test_default_qty_cap_mult_uncapped () =
  (* Default 0.0 = uncapped: each asset grows its qty to deploy the whole pool
     share it is handed (the user scenario: qty grows to survive max drawdown
     with all capital deployed). *)
  let d = default_config () in
  Alcotest.(check (float 0.0001)) "qty_cap_mult default" 0.0 d.qty_cap_mult;
  Alcotest.(check (float 0.0001)) "poll_seconds default" 30.0 d.poll_seconds;
  Alcotest.(check (float 0.0001))
    "startup_wait_seconds default"
    60.0
    d.startup_wait_seconds
;;

let test_tracks_asset () =
  (* Only assets on the exchanges the runtime models (kraken, hyperliquid,
     alpaca) get a published decision, so only they are startup-gated. *)
  Alcotest.(check bool)
    "kraken tracked"
    true
    (Dio_oracle.Oracle_runtime.tracks_asset ~exchange:"kraken" ~symbol:"BTC/USD");
  Alcotest.(check bool)
    "hyperliquid tracked"
    true
    (Dio_oracle.Oracle_runtime.tracks_asset ~exchange:"hyperliquid" ~symbol:"BTC");
  Alcotest.(check bool)
    "alpaca tracked"
    true
    (Dio_oracle.Oracle_runtime.tracks_asset ~exchange:"alpaca" ~symbol:"QQQ");
  Alcotest.(check bool)
    "ibkr not tracked"
    false
    (Dio_oracle.Oracle_runtime.tracks_asset ~exchange:"ibkr" ~symbol:"AAPL");
  Alcotest.(check bool)
    "lighter not tracked"
    false
    (Dio_oracle.Oracle_runtime.tracks_asset ~exchange:"lighter" ~symbol:"BTC")
;;

let test_first_pass_attempt_done_fresh () =
  (* Fresh process, no runtime loop running: no pass attempt has finished, so
     the flag is false (domains stay gated on the startup wait). *)
  Alcotest.(check bool)
    "first pass attempt not done"
    false
    (Dio_oracle.Oracle_runtime.first_pass_attempt_done ())
;;

let test_jitter_bounded () =
  (* Jitter stays within [base, base + min(15, base/2)] and never goes below base. *)
  let base = 10.0 in
  List.iter
    (fun _ ->
       let v = Dio_oracle.Oracle_runtime.jittered base in
       Alcotest.(check bool)
         "jitter in range"
         (v >= base && v <= base +. Float.min 15.0 (base /. 2.0))
         true)
    (List.init 200 (fun i -> i))
;;

let test_trigger_wakes_early () =
  (* A [request_pass] (one lock-free Atomic increment) wakes [wait_until] on
     the next slice instead of sleeping out the deadline: a fill or a
     canceled/rejected/expired order re-sizes the asset within ~50ms. The
     generation captured before the wait makes the wake one-shot. *)
  let gen = Atomic.get Dio_oracle.Oracle_runtime.pass_requested in
  let t0 = Unix.gettimeofday () in
  let deadline = t0 +. 60.0 in
  Dio_oracle.Oracle_runtime.request_pass ();
  Lwt_main.run (Dio_oracle.Oracle_runtime.wait_until ~deadline ~generation:gen ());
  let elapsed = Unix.gettimeofday () -. t0 in
  Alcotest.(check bool) "woke well before the 60s deadline" (elapsed < 2.0) true
;;

let test_trigger_is_one_shot () =
  (* The next wait captures the post-trigger generation: without a NEW
     [request_pass] it holds to the deadline (no repeated pass on the same
     event). *)
  let gen = Atomic.get Dio_oracle.Oracle_runtime.pass_requested in
  Dio_oracle.Oracle_runtime.request_pass ();
  Lwt_main.run
    (Dio_oracle.Oracle_runtime.wait_until
       ~deadline:(Unix.gettimeofday () +. 60.0)
       ~generation:gen
       ());
  let gen_after = Atomic.get Dio_oracle.Oracle_runtime.pass_requested in
  Alcotest.(check int) "request incremented the generation" (gen + 1) gen_after;
  let t0 = Unix.gettimeofday () in
  let deadline = t0 +. 0.05 in
  Lwt_main.run (Dio_oracle.Oracle_runtime.wait_until ~deadline ~generation:gen_after ());
  let elapsed = Unix.gettimeofday () -. t0 in
  Alcotest.(check bool)
    "held to the deadline without a new trigger"
    (elapsed >= 0.045)
    true
;;

let () =
  Alcotest.run
    "Oracle_runtime"
    [ ( "next_sleep"
      , [ Alcotest.test_case "poll while all inactive" `Quick test_poll_while_all_inactive
        ; Alcotest.test_case
            "normal cadence while active"
            `Quick
            test_normal_cadence_while_active
        ; Alcotest.test_case
            "normal cadence on empty snapshot"
            `Quick
            test_normal_cadence_on_empty_snapshot
        ] )
    ; ( "defaults"
      , [ Alcotest.test_case
            "qty_cap_mult uncapped by default"
            `Quick
            test_default_qty_cap_mult_uncapped
        ; Alcotest.test_case "jitter bounded" `Quick test_jitter_bounded
        ] )
    ; ( "startup-gate support"
      , [ Alcotest.test_case "tracks_asset" `Quick test_tracks_asset
        ; Alcotest.test_case
            "first pass attempt not done fresh"
            `Quick
            test_first_pass_attempt_done_fresh
        ] )
    ; ( "trigger"
      , [ Alcotest.test_case "request_pass wakes early" `Quick test_trigger_wakes_early
        ; Alcotest.test_case "wake is one-shot" `Quick test_trigger_is_one_shot
        ] )
    ]
;;
