(* Tests for Dio_oracle.Oracle_reclaim: the priority-capital-reclamation
   planning - which lower-priority resting buys to cancel so a
   higher-priority asset that cannot fund its first buy after a fill can
   resume. Also tests Oracle_runtime.apply_reclaim (the decision patch the
   domain acts on). *)

open Alcotest

let inp ~symbol ~cost ~committed =
  { Dio_oracle.Oracle_reclaim.symbol; first_buy_cost = cost; committed_value = committed }
;;

let plan ~pool assets = Dio_oracle.Oracle_reclaim.plan ~pool assets

(* The user scenario: BTC/USDC (priority 1) filled and cannot fund a
   replacement from the pool; HYPE/USDC (priority 2) holds committed capital
   that - combined with the pool - funds BTC's first buy. *)
let test_example_reclaims_lower_priority () =
  let assets =
    [ inp ~symbol:"BTC/USDC" ~cost:31.54 ~committed:0.0
    ; inp ~symbol:"HYPE/USDC" ~cost:19.73 ~committed:19.73
    ]
  in
  let p = plan ~pool:14.14 assets in
  check
    (list (pair string string))
    "reclaim HYPE to fund BTC"
    [ "HYPE/USDC", "BTC/USDC" ]
    p
;;

let test_insufficient_capital_keeps_lower_active () =
  (* HYPE's committed capital + pool cannot fund BTC's first buy: no
     deallocation - the lower-priority asset stays active. *)
  let assets =
    [ inp ~symbol:"BTC/USDC" ~cost:31.54 ~committed:0.0
    ; inp ~symbol:"HYPE/USDC" ~cost:19.73 ~committed:10.0
    ]
  in
  check (list (pair string string)) "no reclaim" [] (plan ~pool:14.14 assets)
;;

let test_boundary_just_enough () =
  (* HYPE committed exactly closes the gap (with the 1e-9 tolerance). *)
  let gap = 31.54 -. 14.14 in
  let assets =
    [ inp ~symbol:"BTC/USDC" ~cost:31.54 ~committed:0.0
    ; inp ~symbol:"HYPE/USDC" ~cost:19.73 ~committed:gap
    ]
  in
  check
    (list (pair string string))
    "boundary committed capital reclaims"
    [ "HYPE/USDC", "BTC/USDC" ]
    (plan ~pool:14.14 assets)
;;

let test_no_targets () =
  (* The pool already funds the priority asset's first buy: nothing to
     reclaim. *)
  let assets =
    [ inp ~symbol:"BTC/USDC" ~cost:31.54 ~committed:0.0
    ; inp ~symbol:"HYPE/USDC" ~cost:19.73 ~committed:19.73
    ]
  in
  check (list (pair string string)) "no reclaim when funded" [] (plan ~pool:40.0 assets)
;;

let test_target_with_committed_buy_not_a_target () =
  (* An asset holding its own committed buy is never a target (its first buy
     is already funded). *)
  let assets =
    [ inp ~symbol:"BTC/USDC" ~cost:31.54 ~committed:5.0
    ; inp ~symbol:"HYPE/USDC" ~cost:19.73 ~committed:0.0
    ]
  in
  check (list (pair string string)) "no reclaim" [] (plan ~pool:10.0 assets)
;;

let test_min_cardinality_tie_breaks_lowest_priority () =
  (* Priority A fills, gap 20 (pool 10, cost 30). Lower assets B(25), C(20),
     D(15). Both B and C alone cover the gap; the plan cancels the FEWEST (1)
     and among 1-element sets the LOWEST priority (C, the one farther down
     the priority order). *)
  let assets =
    [ inp ~symbol:"A" ~cost:30.0 ~committed:0.0
    ; inp ~symbol:"B" ~cost:1.0 ~committed:25.0
    ; inp ~symbol:"C" ~cost:1.0 ~committed:20.0
    ; inp ~symbol:"D" ~cost:1.0 ~committed:15.0
    ]
  in
  check
    (list (pair string string))
    "cancel C for A (fewest, then lowest priority)"
    [ "C", "A" ]
    (plan ~pool:10.0 assets)
;;

let test_multi_asset_cumulative () =
  (* A single target needs 40 (pool 10, cost 50); neither B(25) nor C(20)
     alone covers, together they do. The plan cancels both (fewest covering
     set of size 2). *)
  let assets =
    [ inp ~symbol:"A" ~cost:50.0 ~committed:0.0
    ; inp ~symbol:"B" ~cost:1.0 ~committed:25.0
    ; inp ~symbol:"C" ~cost:1.0 ~committed:20.0
    ]
  in
  let p = plan ~pool:10.0 assets in
  check bool "reclaims B" true (List.mem ("B", "A") p);
  check bool "reclaims C" true (List.mem ("C", "A") p);
  check int "exactly two reclaimed" 2 (List.length p)
;;

let test_any_committed_lower_asset_reclaimed () =
  (* Candidate scope: ANY lower-priority asset holding committed buy capital
     is reclaimed when its release closes a higher-priority funding gap - a
     fully-funded running lower grid is not exempt (committed capital always
     flows toward the highest-priority asset that needs it). *)
  let assets =
    [ inp ~symbol:"A" ~cost:50.0 ~committed:0.0
    ; inp ~symbol:"B" ~cost:1.0 ~committed:100.0
    ]
  in
  check
    (list (pair string string))
    "fully-funded committed asset reclaimed"
    [ "B", "A" ]
    (plan ~pool:10.0 assets)
;;

let test_multi_target_sequential_release () =
  (* A(50, no committed) cannot be funded even by C(30) + pool 10 (40 < 50).
     B(30, no committed) CAN be funded by C's 30 (10 + 30 >= 30), so C is
     reclaimed for B - released capital funds the highest priority target it
     actually covers. *)
  let assets =
    [ inp ~symbol:"A" ~cost:50.0 ~committed:0.0
    ; inp ~symbol:"B" ~cost:30.0 ~committed:0.0
    ; inp ~symbol:"C" ~cost:1.0 ~committed:30.0
    ]
  in
  check
    (list (pair string string))
    "C reclaimed for B only"
    [ "C", "B" ]
    (plan ~pool:10.0 assets)
;;

(* The full reclaim lifecycle, driven exactly as the engine drives it:
   pass -> plan (oracle) interleaved with the domain's cancel step
   (Dio_strategies.Suicide_grid.reclaim_step). This is the regression for
   the stuck state: a single failed cancel used to leave the account
   permanently halted - the reclaimed asset stayed paused (the plan only
   cleared once the store's committed value dropped to zero) and the
   priority asset never resumed on capital that was never released. With the
   retry, a failed cancel is re-issued after the retry interval and the
   account resolves. *)
let test_reclaim_lifecycle_retries_failed_cancel () =
  let pool = ref 14.14 in
  let hype_committed = ref 19.73 in
  let assets () =
    [ inp ~symbol:"BTC/USDC" ~cost:31.54 ~committed:0.0
    ; inp ~symbol:"HYPE/USDC" ~cost:19.73 ~committed:!hype_committed
    ]
  in
  let step ~now ~issued ~issued_at ~eligible ~any_buy =
    Dio_strategies.Suicide_grid.reclaim_step
      ~now
      ~retry_seconds:15.0
      ~issued
      ~issued_at
      ~eligible
      ~any_buy
  in
  (* Pass 1: the plan targets HYPE for BTC. *)
  let p1 = plan ~pool:!pool (assets ()) in
  check
    (list (pair string string))
    "pass 1 reclaims HYPE for BTC"
    [ "HYPE/USDC", "BTC/USDC" ]
    p1;
  (* Domain sees the reclaim; first cancel attempt FAILS (store unchanged). *)
  check
    bool
    "first attempt issues the cancel"
    true
    (step ~now:100.0 ~issued:false ~issued_at:0.0 ~eligible:1 ~any_buy:true
     = Dio_strategies.Suicide_grid.Reclaim_cancel 1);
  (* Pass 2: nothing changed (the failed cancel left the store intact), so
     the plan persists and the decision stays reclaim. *)
  let p2 = plan ~pool:!pool (assets ()) in
  check
    (list (pair string string))
    "pass 2 plan persists after failed cancel"
    [ "HYPE/USDC", "BTC/USDC" ]
    p2;
  (* The domain stays latched through the retry window (no cancel spam)... *)
  check
    bool
    "in-flight cancel deferred"
    true
    (step ~now:105.0 ~issued:true ~issued_at:100.0 ~eligible:1 ~any_buy:true
     = Dio_strategies.Suicide_grid.Reclaim_deferred);
  (* ...but after the interval the cancel is RETRIED, and this time it lands:
     the store clears and the pool reflects the released capital. *)
  check
    bool
    "stale failed cancel is retried"
    true
    (step ~now:116.0 ~issued:true ~issued_at:100.0 ~eligible:1 ~any_buy:true
     = Dio_strategies.Suicide_grid.Reclaim_cancel 1);
  hype_committed := 0.0;
  pool := 33.87;
  check
    bool
    "clean store re-arms the latch"
    true
    (step ~now:116.5 ~issued:true ~issued_at:116.0 ~eligible:0 ~any_buy:false
     = Dio_strategies.Suicide_grid.Reclaim_rearm);
  (* Pass 3: the plan clears (BTC is funded from the pool) and the priority
     asset re-activates - the account is no longer stuck. *)
  let p3 = plan ~pool:!pool (assets ()) in
  check (list (pair string string)) "pass 3 plan clears" [] p3
;;

(* ================= apply_reclaim (runtime patch) ==================== *)
let make_decision ~symbol ~active =
  { Dio_oracle.Oracle_runtime.exchange = "hyperliquid"
  ; symbol
  ; active
  ; reason = (if active then "" else "pool 14.14 cannot fund the first buy")
  ; qty = 0.0005
  ; grid_interval = 0.75
  ; d_surv = 0.99
  ; d_gov = 0.5
  ; d_cover = 0.6
  ; governing_horizon = "365d"
  ; deployed = 0.0
  ; pool_share = 14.14
  ; remainder = 14.14
  ; reclaim_capital = false
  ; reclaim_target = ""
  ; range = None
  ; p2v = None
  ; parameter_components =
      { Dio_oracle.Oracle_types.fng = Some 37.0
      ; fng_parameter = None
      ; survival_parameter = 0.75
      ; resolved_parameter = 0.75
      ; fng_weight = 0.5
      ; range_parameter = None
      ; range_weight = 0.25
      }
  ; gi_reason = "grid max 0.75% (100% survival unreachable at any gi)"
  ; qty_reason = "minimum qty 0.0005 (stretch: 100% survival unreachable)"
  ; warnings = []
  ; updated_at = 0.0
  }
;;

let test_apply_reclaim_patches () =
  let btc = make_decision ~symbol:"BTC/USDC" ~active:false in
  let hype = make_decision ~symbol:"HYPE/USDC" ~active:true in
  let patched =
    Dio_oracle.Oracle_runtime.apply_reclaim
      ~plan:[ "HYPE/USDC", "BTC/USDC" ]
      [ btc; hype ]
  in
  (* The non-reclaimed asset is untouched. *)
  check bool "BTC active unchanged" false (List.nth patched 0).active;
  check bool "BTC not marked reclaim" false (List.nth patched 0).reclaim_capital;
  (* The reclaimed asset is INACTIVE-with-reclaim, deployed 0, remainder kept. *)
  let h = List.nth patched 1 in
  check bool "HYPE inactive" false h.active;
  check bool "HYPE reclaim flag" true h.reclaim_capital;
  check string "HYPE reclaim target" "BTC/USDC" h.reclaim_target;
  check
    string
    "HYPE reason names the target"
    "capital reallocated to BTC/USDC (higher priority)"
    h.reason;
  check (float 1e-9) "HYPE deployed zeroed" 0.0 h.deployed;
  check (float 1e-9) "HYPE remainder kept" 14.14 h.remainder
;;

let test_apply_reclaim_idempotent_and_reversible () =
  (* Applied fresh each pass on top of the unpatched cached decisions: the
     plan is never baked into the cache, so a plan change reverses cleanly. *)
  let btc = make_decision ~symbol:"BTC/USDC" ~active:false in
  let hype = make_decision ~symbol:"HYPE/USDC" ~active:true in
  let with_plan plan = Dio_oracle.Oracle_runtime.apply_reclaim ~plan [ btc; hype ] in
  let patched = with_plan [ "HYPE/USDC", "BTC/USDC" ] in
  check bool "patched inactive" false (List.nth patched 1).active;
  check
    bool
    "unpatched (plan gone) restores active"
    true
    (List.nth (with_plan []) 1).active;
  check
    bool
    "unpatched (plan gone) clears reclaim"
    false
    (List.nth (with_plan []) 1).reclaim_capital
;;

let () =
  Alcotest.run
    "Oracle_reclaim"
    [ ( "plan"
      , [ Alcotest.test_case
            "example reclaims lower-priority order"
            `Quick
            test_example_reclaims_lower_priority
        ; Alcotest.test_case
            "insufficient capital keeps the lower asset active"
            `Quick
            test_insufficient_capital_keeps_lower_active
        ; Alcotest.test_case
            "boundary just-enough reclaims"
            `Quick
            test_boundary_just_enough
        ; Alcotest.test_case "no targets, no reclaim" `Quick test_no_targets
        ; Alcotest.test_case
            "asset with its own committed buy is never a target"
            `Quick
            test_target_with_committed_buy_not_a_target
        ; Alcotest.test_case
            "min cardinality, then lowest priority"
            `Quick
            test_min_cardinality_tie_breaks_lowest_priority
        ; Alcotest.test_case
            "multi-asset cumulative coverage"
            `Quick
            test_multi_asset_cumulative
        ; Alcotest.test_case
            "any committed lower asset is reclaimed"
            `Quick
            test_any_committed_lower_asset_reclaimed
        ; Alcotest.test_case
            "multi-target sequential release"
            `Quick
            test_multi_target_sequential_release
        ; Alcotest.test_case
            "lifecycle retries a failed cancel and resolves"
            `Quick
            test_reclaim_lifecycle_retries_failed_cancel
        ] )
    ; ( "apply_reclaim"
      , [ Alcotest.test_case "patches the decision" `Quick test_apply_reclaim_patches
        ; Alcotest.test_case
            "idempotent and reversible"
            `Quick
            test_apply_reclaim_idempotent_and_reversible
        ] )
    ]
;;
