(* Tests for Dio_oracle.Oracle_runtime: the event-driven decision path - the
   cadence is only a safety net ([next_sleep] is always the refresh cadence
   because fills/cancels apply their pool deltas at event time), the lock-free
   event trigger ([request_pass] wakes [wait_until] immediately), the
   in-process pool-delta channel ([notify_fill] / [notify_order_cancel] feed
   [resolve_account_pool] without a network wait), and the changed-only
   per-symbol publish set. *)

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
  ; d_cover = 0.1
  ; governing_horizon = "1d"
  ; deployed = 100.0
  ; pool_share = 100.0
  ; remainder = 0.0
  ; reclaim_capital = false
  ; reclaim_target = ""
  ; range = None
  ; p2v = None
  ; parameter_components =
      { Dio_oracle.Oracle_types.fng = Some 50.0
      ; fng_parameter = None
      ; survival_parameter = 1.0
      ; resolved_parameter = 1.0
      ; fng_weight = 0.5
      ; range_parameter = None
      ; range_weight = 0.25
      }
  ; gi_reason = "test"
  ; qty_reason = "test"
  ; warnings = []
  ; updated_at = 0.0
  }
;;

let test_next_sleep_is_refresh_cadence () =
  (* The decision path is event-driven: fills/cancels apply their pool deltas
     at event time, so [next_sleep] is always the refresh cadence (a safety
     net for conditions that emit no event), regardless of how many assets are
     active - the fast poll cadence no longer gates decisions. *)
  let config = { (default_config ()) with poll_seconds = 5.0; refresh_seconds = 300.0 } in
  let decisions = [ make_decision ~active:false; make_decision ~active:false ] in
  Alcotest.(check (float 0.0001))
    "refresh cadence even while all inactive"
    300.0
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

let test_class_member_source () =
  (* Class members are blend inputs, never decision subjects: they gather
     their history purely from Yahoo (whitelisted) UNLESS the member IS the
     active asset on that exchange, which uses the exchange itself. Symbols
     with no trusted Yahoo mapping contribute nothing. *)
  let src exchange asset member =
    Dio_oracle.Oracle_fetch.class_member_source ~exchange ~asset_symbol:asset member
  in
  (* The active asset itself -> the exchange, whatever the venue. *)
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "SOL/USD asset on kraken uses the exchange"
    `Exchange
    (src "kraken" "SOL/USD" "SOL/USD");
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "case-insensitive active asset match"
    `Exchange
    (src "kraken" "sol/usd" "SOL/USD");
  (* Any other member -> pure Yahoo, never the venue's view of it. *)
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "SOL/USD member of another asset's class comes from Yahoo"
    (`Yahoo "SOL-USD")
    (src "kraken" "XMR/USD" "SOL/USD");
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "alt member of a Hyperliquid class comes from Yahoo, not Hyperliquid"
    (`Yahoo "ADA-USD")
    (src "hyperliquid" "HYPE/USDC" "ADA/USD");
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "equity member comes from Yahoo"
    (`Yahoo "SPY")
    (src "alpaca" "QQQ" "SPY");
  (* No trusted Yahoo mapping (dead-token collisions) -> contributes nothing. *)
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "HYPE has no trusted Yahoo mapping"
    `None
    (src "hyperliquid" "BTC/USDC" "HYPE/USD");
  Alcotest.(
    check
      (of_pp (fun fmt v ->
         Format.fprintf
           fmt
           "%s"
           (match v with
            | `Exchange -> "exchange"
            | `Yahoo y -> "yahoo " ^ y
            | `None -> "none"))))
    "BNB has no trusted Yahoo mapping"
    `None
    (src "kraken" "XMR/USD" "BNB/USD")
;;

let test_first_pass_attempt_done_fresh () =
  (* Fresh process, no runtime loop running: no pass attempt has finished, so
     the flag is false (domains stay gated on the startup wait). *)
  Alcotest.(check bool)
    "first pass attempt not done"
    false
    (Dio_oracle.Oracle_runtime.first_pass_attempt_done ())
;;

let test_cold_start_pass_not_an_attempt () =
  (* A pass that cannot reach the decision phase (no runnable tasks / no
     materialized state yet) must NOT count as a pass attempt: if it did, the
     domains' F&G-only fallback gate would open while the oracle's first real
     decisions are still seconds away (cold start), letting capital-unaware
     sizing place orders the first real pass immediately declares INACTIVE
     (rejected by the exchanges for insufficient funds). *)
  let was_done = Dio_oracle.Oracle_runtime.first_pass_attempt_done () in
  let cfg = Dio_oracle.Oracle_runtime.default_config () in
  ignore
    (Lwt_main.run
       (Dio_oracle.Oracle_runtime.run_pass ~trading:[] ~classes:[] ~config:cfg ()));
  Alcotest.(check bool)
    "cold-start pass does not count as an attempt"
    was_done
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
  Lwt_main.run
    (Dio_oracle.Oracle_runtime.wait_until
       ~deadline
       ~generation:gen
       ~refresh_gen:(Atomic.get Dio_oracle.Oracle_runtime.refresh_generation)
       ());
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
       ~refresh_gen:(Atomic.get Dio_oracle.Oracle_runtime.refresh_generation)
       ());
  let gen_after = Atomic.get Dio_oracle.Oracle_runtime.pass_requested in
  Alcotest.(check int) "request incremented the generation" (gen + 1) gen_after;
  let t0 = Unix.gettimeofday () in
  let deadline = t0 +. 0.05 in
  Lwt_main.run
    (Dio_oracle.Oracle_runtime.wait_until
       ~deadline
       ~generation:gen_after
       ~refresh_gen:(Atomic.get Dio_oracle.Oracle_runtime.refresh_generation)
       ());
  let elapsed = Unix.gettimeofday () -. t0 in
  Alcotest.(check bool)
    "held to the deadline without a new trigger"
    (elapsed >= 0.045)
    true
;;

let test_resolve_for_no_override () =
  (* No overrides configured: the resolved config is the global one (sizing
     knobs fall through untouched). *)
  let config = { (default_config ()) with target_survival = 0.95 } in
  let resolved =
    Dio_oracle.Oracle_runtime.resolve_for config ~exchange:"hyperliquid" "HYPE/USDC"
  in
  Alcotest.(check (float 0.0001)) "target_survival global" 0.95 resolved.target_survival;
  Alcotest.(check (float 0.0001)) "fng_weight global" 0.5 resolved.fng_weight;
  Alcotest.(check (option (float 0.0001))) "max_capital global" None resolved.max_capital
;;

let test_resolve_for_merges_present_keys () =
  (* A per-asset entry overrides only the present keys; everything else stays
     on the global config. *)
  let config =
    { (default_config ()) with
      target_survival = 0.95
    ; assets =
        [ ( "HYPE/USDC"
          , { target_survival = Some 0.98
            ; fng_weight = None
            ; range_weight = None
            ; min_active_dsurv = None
            ; qty_cap_mult = Some 3.0
            ; no_deep_history = None
            ; weight_by_sessions = None
            ; horizons = Some [ 90; 180 ]
            } )
        ]
    }
  in
  let resolved =
    Dio_oracle.Oracle_runtime.resolve_for config ~exchange:"hyperliquid" "HYPE/USDC"
  in
  Alcotest.(check (float 0.0001))
    "overridden target_survival"
    0.98
    resolved.target_survival;
  Alcotest.(check (float 0.0001)) "inherited fng_weight" 0.5 resolved.fng_weight;
  Alcotest.(check (float 0.0001)) "overridden qty_cap_mult" 3.0 resolved.qty_cap_mult;
  Alcotest.(check (option (list int)))
    "overridden horizons"
    (Some [ 90; 180 ])
    resolved.horizons;
  Alcotest.(check (option (float 0.0001)))
    "inherited max_capital"
    None
    resolved.max_capital
;;

let test_resolve_for_case_insensitive () =
  (* Keys match the trading-config symbol case-insensitively. *)
  let config =
    { (default_config ()) with
      target_survival = 0.95
    ; assets =
        [ ( "hype/usdc"
          , { target_survival = Some 0.98
            ; fng_weight = None
            ; range_weight = None
            ; min_active_dsurv = None
            ; qty_cap_mult = None
            ; no_deep_history = None
            ; weight_by_sessions = None
            ; horizons = None
            } )
        ]
    }
  in
  let resolved =
    Dio_oracle.Oracle_runtime.resolve_for config ~exchange:"hyperliquid" "HYPE/USDC"
  in
  Alcotest.(check (float 0.0001))
    "case-insensitive override"
    0.98
    resolved.target_survival
;;

let test_resolve_for_venue_key_wins () =
  (* A "venue/symbol" key wins over the bare symbol for that venue; the bare
     symbol still applies to other venues. *)
  let config =
    { (default_config ()) with
      target_survival = 0.95
    ; assets =
        [ ( "HYPE/USDC"
          , { target_survival = Some 0.98
            ; fng_weight = None
            ; range_weight = None
            ; min_active_dsurv = None
            ; qty_cap_mult = None
            ; no_deep_history = None
            ; weight_by_sessions = None
            ; horizons = None
            } )
        ; ( "hyperliquid/HYPE/USDC"
          , { target_survival = Some 0.90
            ; fng_weight = None
            ; range_weight = None
            ; min_active_dsurv = None
            ; qty_cap_mult = None
            ; no_deep_history = None
            ; weight_by_sessions = None
            ; horizons = None
            } )
        ]
    }
  in
  let on_hl =
    Dio_oracle.Oracle_runtime.resolve_for config ~exchange:"hyperliquid" "HYPE/USDC"
  in
  let on_kraken =
    Dio_oracle.Oracle_runtime.resolve_for config ~exchange:"kraken" "HYPE/USDC"
  in
  Alcotest.(check (float 0.0001))
    "venue key wins on hyperliquid"
    0.90
    on_hl.target_survival;
  Alcotest.(check (float 0.0001))
    "bare key applies on kraken"
    0.98
    on_kraken.target_survival
;;

let test_resolve_for_unknown_key_global () =
  (* An override key that matches no trading symbol never matches: the asset
     runs on the global config. *)
  let config =
    { (default_config ()) with
      target_survival = 0.95
    ; assets =
        [ ( "SOMEOTHER/USD"
          , { target_survival = Some 0.10
            ; fng_weight = None
            ; range_weight = None
            ; min_active_dsurv = None
            ; qty_cap_mult = None
            ; no_deep_history = None
            ; weight_by_sessions = None
            ; horizons = None
            } )
        ]
    }
  in
  let resolved =
    Dio_oracle.Oracle_runtime.resolve_for config ~exchange:"hyperliquid" "HYPE/USDC"
  in
  Alcotest.(check (float 0.0001)) "no match keeps global" 0.95 resolved.target_survival
;;

let test_override_fields_all_none () =
  (* Regression: the startup summary used to crash with
     Invalid_argument("option is None") when every knob was absent (eager
     Option.get on a None field). The all-None record must yield []. *)
  let o : Dio_oracle.Oracle_runtime.asset_overrides =
    { target_survival = None
    ; fng_weight = None
    ; range_weight = None
    ; min_active_dsurv = None
    ; qty_cap_mult = None
    ; no_deep_history = None
    ; weight_by_sessions = None
    ; horizons = None
    }
  in
  Alcotest.(check (list string)) "empty" [] (Dio_oracle.Oracle_runtime.override_fields o)
;;

let test_override_fields_present_only () =
  (* Only Some knobs are listed, formatted for the startup summary line. *)
  let o : Dio_oracle.Oracle_runtime.asset_overrides =
    { target_survival = Some 0.98
    ; fng_weight = None
    ; range_weight = None
    ; min_active_dsurv = None
    ; qty_cap_mult = Some 3.0
    ; no_deep_history = None
    ; weight_by_sessions = Some false
    ; horizons = Some [ 90; 180 ]
    }
  in
  Alcotest.(check (list string))
    "only present keys, in record order"
    [ "target_survival 0.98"
    ; "qty_cap_mult 3.00"
    ; "weight_by_sessions false"
    ; "horizons [90,180]"
    ]
    (Dio_oracle.Oracle_runtime.override_fields o)
;;

(* ================= Memoization / background-refresh support ============ *)

let bar ~date ~close =
  { Dio_oracle.Oracle_types.date
  ; open_ = close
  ; high = close
  ; low = close
  ; close
  ; volume = 100.0
  }
;;

let mk_bars n =
  Array.init n (fun i ->
    bar
      ~date:(Dio_oracle.Oracle_calendar.add_days "2024-01-01" i)
      ~close:(100.0 +. float_of_int i))
;;

let test_same_bars () =
  let a = mk_bars 100 in
  Alcotest.(check bool) "physical identity" true (Dio_oracle.Oracle_runtime.same_bars a a);
  (* Equal content, different objects. *)
  let b = Array.copy a in
  Alcotest.(check bool)
    "structural equality"
    true
    (Dio_oracle.Oracle_runtime.same_bars a b);
  (* A delta append invalidates. *)
  let c = Array.append a [| bar ~date:"2025-01-01" ~close:101.0 |] in
  Alcotest.(check bool)
    "append invalidates"
    false
    (Dio_oracle.Oracle_runtime.same_bars a c);
  (* A deep-history prepend invalidates. *)
  let d = Array.append [| bar ~date:"2019-01-01" ~close:50.0 |] a in
  Alcotest.(check bool)
    "prepend invalidates"
    false
    (Dio_oracle.Oracle_runtime.same_bars a d);
  (* A middle correction invalidates (exact compare, not sampled). *)
  let e = Array.copy a in
  e.(50)
  <- bar
       ~date:e.(50).Dio_oracle.Oracle_types.date
       ~close:(e.(50).Dio_oracle.Oracle_types.close +. 5.0);
  Alcotest.(check bool)
    "middle correction invalidates"
    false
    (Dio_oracle.Oracle_runtime.same_bars a e);
  (* Different lengths invalidate. *)
  Alcotest.(check bool)
    "length differs"
    false
    (Dio_oracle.Oracle_runtime.same_bars a (mk_bars 99))
;;

let test_same_fng () =
  Alcotest.(check bool) "none equal" true (Dio_oracle.Oracle_runtime.same_fng None None);
  Alcotest.(check bool)
    "some equal within tolerance"
    true
    (Dio_oracle.Oracle_runtime.same_fng (Some 37.0) (Some 37.0000001));
  Alcotest.(check bool)
    "different values differ"
    false
    (Dio_oracle.Oracle_runtime.same_fng (Some 37.0) (Some 38.0));
  Alcotest.(check bool)
    "none vs some differ"
    false
    (Dio_oracle.Oracle_runtime.same_fng None (Some 37.0))
;;

let mk_gi ~symbol ~first_buy ~has_committed =
  { Dio_oracle.Oracle_runtime.g_exchange = "kraken"
  ; g_symbol = symbol
  ; g_first_buy = first_buy
  ; g_committed = 0.0
  ; g_has_committed_buy = has_committed
  ; g_pool = 0.0
  ; g_capital_blocked = true
  }
;;

let test_account_gate_token_flip () =
  (* A pool move inside the sizing-cache bucket can still cross an asset's
     first-buy gate: 99.9 vs 100.1 on a 100.0 gate is a 0.2% relative move -
     well inside the 0.5% fingerprint bucket. The token must flip, so the
     cache bypass re-sizes instead of reusing a stale INACTIVE decision (the
     "capital returned but the strategy did not resume" stall). *)
  let inputs = [ mk_gi ~symbol:"A" ~first_buy:100.0 ~has_committed:false ] in
  Alcotest.(check (list bool))
    "not fundable at 99.9"
    [ false ]
    (Dio_oracle.Oracle_runtime.account_gate_token ~pool:99.9 inputs ~deployed_of:(fun _ ->
       0.0));
  Alcotest.(check (list bool))
    "fundable at 100.1"
    [ true ]
    (Dio_oracle.Oracle_runtime.account_gate_token
       ~pool:100.1
       inputs
       ~deployed_of:(fun _ -> 0.0))
;;

let test_account_gate_token_respects_pass_down () =
  (* A fully-funded priority asset consumes its deployed share before the
     next asset's gate is evaluated: with A deployed 40 of a 100 pool, B is
     sized against the 60 remainder and cannot fund a 70 first buy - even
     though the raw pool reads above it. This is the pass-down budget the
     reclaim plan's target test uses. *)
  let a = mk_gi ~symbol:"A" ~first_buy:40.0 ~has_committed:false in
  let b = mk_gi ~symbol:"B" ~first_buy:70.0 ~has_committed:false in
  let deployed_of s = if s = "A" then 40.0 else 0.0 in
  Alcotest.(check (list bool))
    "B starved by A's deployment"
    [ true; false ]
    (Dio_oracle.Oracle_runtime.account_gate_token ~pool:100.0 [ a; b ] ~deployed_of)
;;

let test_account_gate_token_committed_buy_exempt () =
  (* A committed resting buy exempts the asset from the first-buy gate even
     when the pass-down budget cannot fund a fresh one - its first buy is
     already funded and locked in the account balance. *)
  let a = mk_gi ~symbol:"A" ~first_buy:100.0 ~has_committed:true in
  Alcotest.(check (list bool))
    "committed buy fundable despite the budget"
    [ true ]
    (Dio_oracle.Oracle_runtime.account_gate_token ~pool:10.0 [ a ] ~deployed_of:(fun _ ->
       0.0))
;;

let test_account_fp_eq () =
  let fp ~analyses ~pool ~fng ~state =
    { Dio_oracle.Oracle_runtime.af_analyses = analyses
    ; af_pool = pool
    ; af_fng = fng
    ; af_state = state
    }
  in
  let base = fp ~analyses:[ "1"; "2" ] ~pool:1000.0 ~fng:(Some 37.0) ~state:"a:1:2:3" in
  (* Identical -> equal. *)
  Alcotest.(check bool)
    "identical fingerprints equal"
    true
    (Dio_oracle.Oracle_runtime.account_fp_eq base base);
  (* Sub-0.5% pool drift -> equal (quantized sizing cannot move). *)
  Alcotest.(check bool)
    "pool drift under bucket equal"
    true
    (Dio_oracle.Oracle_runtime.account_fp_eq
       base
       (fp ~analyses:[ "1"; "2" ] ~pool:1003.0 ~fng:(Some 37.0) ~state:"a:1:2:3"));
  (* >0.5% pool drift -> re-size. *)
  Alcotest.(check bool)
    "pool drift over bucket differs"
    false
    (Dio_oracle.Oracle_runtime.account_fp_eq
       base
       (fp ~analyses:[ "1"; "2" ] ~pool:1010.0 ~fng:(Some 37.0) ~state:"a:1:2:3"));
  (* A recomputed analysis (new id) -> re-size. *)
  Alcotest.(check bool)
    "analysis id change differs"
    false
    (Dio_oracle.Oracle_runtime.account_fp_eq
       base
       (fp ~analyses:[ "1"; "3" ] ~pool:1000.0 ~fng:(Some 37.0) ~state:"a:1:2:3"));
  (* A fill (strategy state change) -> re-size even at the same pool. *)
  Alcotest.(check bool)
    "state change differs"
    false
    (Dio_oracle.Oracle_runtime.account_fp_eq
       base
       (fp ~analyses:[ "1"; "2" ] ~pool:1000.0 ~fng:(Some 37.0) ~state:"a:1.5:2:3"));
  (* F&G change -> re-size. *)
  Alcotest.(check bool)
    "fng change differs"
    false
    (Dio_oracle.Oracle_runtime.account_fp_eq
       base
       (fp ~analyses:[ "1"; "2" ] ~pool:1000.0 ~fng:(Some 38.0) ~state:"a:1:2:3"))
;;

let empty_materialized ~fng ~assets =
  let balances = Hashtbl.create 1 in
  { Dio_oracle.Oracle_runtime.m_assets = assets
  ; m_balances = balances
  ; m_fng = fng
  ; m_epoch = 0
  ; m_last_history_at = 0.0
  }
;;

let test_history_changed () =
  let m = empty_materialized ~fng:(Some 37.0) ~assets:[] in
  (* First cycle is always a change (cold start). *)
  Alcotest.(check bool)
    "cold start is a change"
    true
    (Dio_oracle.Oracle_runtime.history_changed None m);
  (* A balance-only cycle (same fng, same assets) is NOT a change: the pass
     loop must not wake for it. *)
  Alcotest.(check bool)
    "balance-only cycle unchanged"
    false
    (Dio_oracle.Oracle_runtime.history_changed
       (Some m)
       (empty_materialized ~fng:(Some 37.0) ~assets:[]));
  (* A f&g move is a change. *)
  Alcotest.(check bool)
    "fng change is a change"
    true
    (Dio_oracle.Oracle_runtime.history_changed
       (Some m)
       (empty_materialized ~fng:(Some 38.0) ~assets:[]))
;;

let test_analysis_memoization_roundtrip () =
  (* The same materialized inputs return the SAME analysis record (reused =
     true), so the pass skips the replay entirely and account sizing sees
     stable analysis ids; any input change recomputes (reused = false). *)
  let tc = Dio_oracle.Oracle_tasks.default_trading_config "kraken" "TEST/USD" in
  let bars = mk_bars 100 in
  let series =
    { Dio_oracle.Oracle_types.symbol = "TEST/USD"
    ; calendar_kind = Dio_oracle.Oracle_types.Crypto
    ; bars
    ; gaps = []
    }
  in
  let am =
    { Dio_oracle.Oracle_runtime.am_exchange = "kraken"
    ; am_symbol = "TEST/USD"
    ; am_tc = tc
    ; am_bars = bars
    ; am_deep_bars = 0
    ; am_members = [ series ]
    ; am_calendar = None
    ; am_calendar_fp = "none"
    }
  in
  let task =
    { Dio_oracle.Oracle_tasks.symbol = "TEST/USD"; exchange = "kraken"; config = tc }
  in
  let run () =
    Lwt_main.run
      (Dio_oracle.Oracle_runtime.analyze_asset
         (Dio_oracle.Oracle_runtime.default_config ())
         []
         task
         ~index:1
         ~n_tasks:1
         ~am
         ~fng:(Some 37.0))
  in
  let first, reused1 = run () in
  Alcotest.(check bool) "first analysis computes" false reused1;
  let second, reused2 = run () in
  Alcotest.(check bool) "second analysis reused" true reused2;
  Alcotest.(check bool) "same record identity" (first == second) true;
  Alcotest.(check int) "same analysis id" first.id second.id;
  (* A changed input (new bar appended) recomputes. *)
  let am2 =
    { am with
      am_bars =
        Array.append
          bars
          [| bar ~date:(Dio_oracle.Oracle_calendar.add_days "2024-01-01" 100) ~close:101.0
          |]
    }
  in
  let third, reused3 =
    Lwt_main.run
      (Dio_oracle.Oracle_runtime.analyze_asset
         (Dio_oracle.Oracle_runtime.default_config ())
         []
         task
         ~index:1
         ~n_tasks:1
         ~am:am2
         ~fng:(Some 37.0))
  in
  Alcotest.(check bool) "changed input recomputes" false reused3;
  Alcotest.(check bool) "recomputed record differs" (first == third) false
;;

let test_publish_generation_bumps_per_pass () =
  (* The domain decision cache keys on [get_publish_generation], so a new
     pass's decisions are adopted on the domain's next cycle - the reclaim
     decision (and the later re-activation) must reach the domain promptly,
     not at the next background-refresh cycle. Every publish bumps it. *)
  let before = Dio_oracle.Oracle_runtime.get_publish_generation () in
  Dio_oracle.Oracle_runtime.publish [ make_decision ~active:true ];
  let after_one = Dio_oracle.Oracle_runtime.get_publish_generation () in
  Dio_oracle.Oracle_runtime.publish [ make_decision ~active:false ];
  let after_two = Dio_oracle.Oracle_runtime.get_publish_generation () in
  Alcotest.(check int) "first publish bumps" (before + 1) after_one;
  Alcotest.(check int) "second publish bumps again" (after_one + 1) after_two
;;

(* ---- Pool-delta channel (network-independent decision path) ----------- *)

let kraken_account () = Dio_oracle.Oracle_topology.key ~venue:"kraken" ~symbol:"X/USD" ()

let pool_materialized ~epoch ~pool =
  let account = kraken_account () in
  let aid = Dio_oracle.Oracle_runtime.account_id account in
  let balances = Hashtbl.create 1 in
  let snapshot =
    { Dio_oracle.Oracle_balances.exchange = "kraken"
    ; testnet = false
    ; balances =
        [ { Dio_oracle.Oracle_balances.asset = "USD"
          ; available = pool
          ; total = pool
          ; wallet_type = "rest"
          ; wallet_id = "account"
          }
        ]
    ; fetched_at = 0.0
    }
  in
  Hashtbl.replace balances aid (Some (pool, snapshot));
  { Dio_oracle.Oracle_runtime.m_assets = []
  ; m_balances = balances
  ; m_fng = None
  ; m_epoch = epoch
  ; m_last_history_at = 0.0
  }
;;

let test_pool_delta_fill_applied () =
  (* A buy fill consumes value+fee from the materialized pool at decision
     time - the decision path never waits on a network balance refresh. *)
  let gen = Atomic.get Dio_oracle.Oracle_runtime.refresh_generation in
  Dio_oracle.Oracle_runtime.notify_fill
    ~exchange:"kraken"
    ~symbol:"X/USD"
    ~testnet:false
    ~side:Dio_exchange.Exchange_intf.Types.Buy
    ~filled_qty:10.0
    ~avg_price:5.0
    ~fee:0.05;
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let m = pool_materialized ~epoch:gen ~pool:1000.0 in
  match Dio_oracle.Oracle_runtime.resolve_account_pool ~m (kraken_account ()) with
  | Some (pool, _) ->
    (* 10 x 5 = 50 value + 0.05 fee consumed: 1000 - 50.05 = 949.95 *)
    Alcotest.(check (float 0.01)) "fill delta applied" 949.95 pool
  | None -> Alcotest.fail "expected a resolved pool"
;;

let test_pool_delta_cancel_applied () =
  (* A canceled BUY returns its remaining committed value to the pool. *)
  let gen = Atomic.get Dio_oracle.Oracle_runtime.refresh_generation in
  Dio_oracle.Oracle_runtime.notify_order_cancel
    ~exchange:"kraken"
    ~symbol:"X/USD"
    ~testnet:false
    ~side:Dio_exchange.Exchange_intf.Types.Buy
    ~value:50.0;
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let m = pool_materialized ~epoch:gen ~pool:1000.0 in
  match Dio_oracle.Oracle_runtime.resolve_account_pool ~m (kraken_account ()) with
  | Some (pool, _) -> Alcotest.(check (float 0.01)) "cancel delta applied" 1050.0 pool
  | None -> Alcotest.fail "expected a resolved pool"
;;

let test_pool_delta_sell_cancel_ignored () =
  (* A canceled SELL releases base inventory, not quote: the pool is
     untouched. *)
  let gen = Atomic.get Dio_oracle.Oracle_runtime.refresh_generation in
  Dio_oracle.Oracle_runtime.notify_order_cancel
    ~exchange:"kraken"
    ~symbol:"X/USD"
    ~testnet:false
    ~side:Dio_exchange.Exchange_intf.Types.Sell
    ~value:500.0;
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  let m = pool_materialized ~epoch:gen ~pool:1000.0 in
  match Dio_oracle.Oracle_runtime.resolve_account_pool ~m (kraken_account ()) with
  | Some (pool, _) -> Alcotest.(check (float 0.01)) "sell cancel ignored" 1000.0 pool
  | None -> Alcotest.fail "expected a resolved pool"
;;

let test_pool_delta_superseded_by_newer_epoch () =
  (* A delta whose baseline does not match the consumed materialized epoch is
     dropped: a fresher authoritative pool (fetched after the event) already
     includes it, so applying it again would double-count. *)
  let gen = Atomic.get Dio_oracle.Oracle_runtime.refresh_generation in
  Dio_oracle.Oracle_runtime.notify_fill
    ~exchange:"kraken"
    ~symbol:"X/USD"
    ~testnet:false
    ~side:Dio_exchange.Exchange_intf.Types.Buy
    ~filled_qty:10.0
    ~avg_price:5.0
    ~fee:0.0;
  Dio_oracle.Oracle_runtime.drain_pool_events ();
  (* The materialized record was published AFTER the fill (epoch advanced):
     its pool already reflects the fill. *)
  let m = pool_materialized ~epoch:(gen + 1) ~pool:949.0 in
  match Dio_oracle.Oracle_runtime.resolve_account_pool ~m (kraken_account ()) with
  | Some (pool, _) -> Alcotest.(check (float 0.01)) "delta superseded" 949.0 pool
  | None -> Alcotest.fail "expected a resolved pool"
;;

let test_publish_changed_symbols () =
  (* [publish] exposes the symbols whose decision changed this pass, so the
     engine wakes only those domains (per-symbol) instead of broadcasting. *)
  Dio_oracle.Oracle_runtime.publish [ make_decision ~active:true ];
  Alcotest.(check (list string))
    "first publish changes the symbol"
    [ "X/USD" ]
    !Dio_oracle.Oracle_runtime.last_changed_symbols;
  (* Re-publishing an identical decision changes nothing. *)
  Dio_oracle.Oracle_runtime.publish [ make_decision ~active:true ];
  Alcotest.(check (list string))
    "identical re-publish changes nothing"
    []
    !Dio_oracle.Oracle_runtime.last_changed_symbols;
  (* A flipped decision changes it again. *)
  Dio_oracle.Oracle_runtime.publish [ make_decision ~active:false ];
  Alcotest.(check (list string))
    "flipped decision changes the symbol"
    [ "X/USD" ]
    !Dio_oracle.Oracle_runtime.last_changed_symbols
;;

let () =
  Alcotest.run
    "Oracle_runtime"
    [ ( "next_sleep"
      , [ Alcotest.test_case
            "refresh cadence regardless of activity"
            `Quick
            test_next_sleep_is_refresh_cadence
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
    ; ( "per-asset overrides"
      , [ Alcotest.test_case
            "no override keeps global"
            `Quick
            test_resolve_for_no_override
        ; Alcotest.test_case
            "present keys merge, absent inherit"
            `Quick
            test_resolve_for_merges_present_keys
        ; Alcotest.test_case
            "case-insensitive key match"
            `Quick
            test_resolve_for_case_insensitive
        ; Alcotest.test_case
            "venue key wins over symbol"
            `Quick
            test_resolve_for_venue_key_wins
        ; Alcotest.test_case
            "unknown key falls through to global"
            `Quick
            test_resolve_for_unknown_key_global
        ; Alcotest.test_case
            "override summary with no knobs set"
            `Quick
            test_override_fields_all_none
        ; Alcotest.test_case
            "override summary lists only present knobs"
            `Quick
            test_override_fields_present_only
        ] )
    ; ( "startup-gate support"
      , [ Alcotest.test_case "tracks_asset" `Quick test_tracks_asset
        ; Alcotest.test_case
            "first pass attempt not done fresh"
            `Quick
            test_first_pass_attempt_done_fresh
        ; Alcotest.test_case
            "cold-start pass not an attempt"
            `Quick
            test_cold_start_pass_not_an_attempt
        ; Alcotest.test_case "class member source policy" `Quick test_class_member_source
        ] )
    ; ( "trigger"
      , [ Alcotest.test_case "request_pass wakes early" `Quick test_trigger_wakes_early
        ; Alcotest.test_case "wake is one-shot" `Quick test_trigger_is_one_shot
        ] )
    ; ( "memoization"
      , [ Alcotest.test_case "same_bars exact compare" `Quick test_same_bars
        ; Alcotest.test_case "same_fng" `Quick test_same_fng
        ; Alcotest.test_case "account sizing fingerprint" `Quick test_account_fp_eq
        ; Alcotest.test_case "history_changed semantics" `Quick test_history_changed
        ; Alcotest.test_case
            "analysis memoization roundtrip"
            `Quick
            test_analysis_memoization_roundtrip
        ; Alcotest.test_case
            "publish generation bumps per pass"
            `Quick
            test_publish_generation_bumps_per_pass
        ] )
    ; ( "pool-delta channel"
      , [ Alcotest.test_case
            "fill delta applied in process"
            `Quick
            test_pool_delta_fill_applied
        ; Alcotest.test_case
            "buy cancel delta applied"
            `Quick
            test_pool_delta_cancel_applied
        ; Alcotest.test_case
            "sell cancel ignored"
            `Quick
            test_pool_delta_sell_cancel_ignored
        ; Alcotest.test_case
            "delta superseded by newer epoch"
            `Quick
            test_pool_delta_superseded_by_newer_epoch
        ; Alcotest.test_case
            "publish changed-only symbols"
            `Quick
            test_publish_changed_symbols
        ] )
    ; ( "capital-gate"
      , [ Alcotest.test_case
            "gate flips within the pool bucket"
            `Quick
            test_account_gate_token_flip
        ; Alcotest.test_case
            "gate respects the pass-down budget"
            `Quick
            test_account_gate_token_respects_pass_down
        ; Alcotest.test_case
            "committed buy exempts the gate"
            `Quick
            test_account_gate_token_committed_buy_exempt
        ] )
    ]
;;
