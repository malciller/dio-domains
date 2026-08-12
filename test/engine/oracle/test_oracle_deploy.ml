(* Oracle_deploy tests: the capital deployment engine.

   The engine's contract, tested on a synthetic deterministic history:
   - qty_for_pool inverts the floor-aware runway cost (round trip, qty_min
     floor),
   - governing_drawdown picks the deepest reachable horizon target and is
     None when no horizon can clear the target,
   - the ATH-anchored survival drawdown d_cover caps the ladder's scale
     absolutely: at the ATH d_cover = d_gov, below the target level
     ATH*(1-d_gov) the required runway is zero (maximally aggressive),
   - the verification replay is funded with the asset's pool budget, so a
     well-funded asset passes (no false "under-funded" verdicts) and the
     sizing grows to the qty_cap,
   - the deployment is as aggressive as possible within the constraints:
     equities are pure oracle (parameter = survival_parameter, qty =
     survival-max, F&G never blends in), crypto blends the F&G contrarian
     signal on both gi and qty (fear tightens and up-sizes, greed loosens and
     pulls back) clamped to never break the survival constraint,
   - under-funded active assets run at qty_min with the shortfall warned and
     keep their whole share (config-order priority),
   - fallback (immature-history) assets are sized on the deepest observed
     drawdown, ATH-anchored; qty still grows up to the qty_cap while the
     static funding check holds (volume-driven),
   - inactive when the pool cannot fund the first buy, no horizon clears the
     target, or the replayed D_surv stays below --min-active-dsurv. *)

open Dio_oracle

(* The deployment engine instantiated over the grid strategy model: the
   engine under test is the generic functor, exercised through its only
   concrete strategy today. *)
module D = Oracle_deploy.Engine (Oracle_strategy.Grid)

let near ?(eps = 1e-6) a b = Alcotest.(check (float eps)) "approx" a b

(* Substring check for warning assertions. *)
let contains haystack needle =
  let hl = String.length haystack in
  let nl = String.length needle in
  let rec go i = i + nl <= hl && (String.sub haystack i nl = needle || go (i + 1)) in
  nl > 0 && go 0
;;

(* Deterministic synthetic history: a gentle oscillation with a deep crash in
   the middle and a recovery, so the MFD distribution has real mass in the
   tail but the grid path is replayable. *)
let synth_series ~n =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let price = ref 100.0 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  for i = 0 to n - 1 do
    (* Trend -0.05%/day, oscillation +/-0.3% around it. *)
    let osc = 0.003 *. sin (float_of_int i /. 9.0) in
    let drift = -0.0005 in
    let crash =
      if i >= 300 && i < 320
      then 0.980 (* 20 bars x -2%: a ~33% drawdown before recovery. *)
      else if i >= 320 && i < 380
      then 1.010
      else 1.0
    in
    price := !price *. (1.0 +. drift +. osc) *. crash;
    let p = !price in
    bars.(i)
    <- { Oracle_types.date = iso i
       ; open_ = p
       ; high = p *. 1.006
       ; low = p *. 0.994
       ; close = p
       ; volume = 1000.0
       }
  done;
  { Oracle_types.symbol = "SYNTH"; calendar_kind = Oracle_types.Crypto; bars; gaps = [] }
;;

let asset = synth_series ~n:500

let horizon h =
  { Oracle_types.label = Oracle_types.horizon_label Oracle_types.Crypto h
  ; sessions = h
  ; calendar_days = h
  }
;;

let horizons = List.map horizon [ 30; 90; 180 ]
let warmup = 60

let models ~(asset : Oracle_types.series) =
  List.map
    (fun h ->
       Oracle_replay.blend_model_of
         ~horizon:h
         ~asset
         ~class_members:[ asset ]
         ~kappa:10
         ~warmup
         ())
    horizons
;;

let grid ?(min_notional = 0.0) ?(qty_min = 0.01) ?(gi = 1.0) ~(start_price : float) ()
  : Dio_strategies.Grid_core.config
  =
  let open Dio_strategies.Grid_core in
  { qty = 0.1
  ; sell_mult = 1.0
  ; grid_interval_pct = gi
  ; maker_fee = 0.0004
  ; accumulation_buffer = 0.0
  ; price_increment = 0.01
  ; qty_increment = 0.01
  ; qty_min
  ; min_notional
  ; exchange_model = Dio_strategies.Grid_core_types.Hyperliquid
  ; start_price
  ; start_quote = 0.0
  ; cash_hook = None
  }
;;

let deploy
      ?(asset = asset)
      ?(pool = 10_000.0)
      ?(fng = Some 50.0)
      ?(fng_weight = 0.5)
      ?(range_weight = 0.0)
      ?(min_active_dsurv = 0.0)
      ?(use_fng = true)
      ?(lo = 0.5)
      ?(hi = 2.0)
      ?(target = 0.99)
      ?(qty_cap_mult = 1.0)
      ?(g = grid ~start_price:100.0 ())
      ~(models : Oracle_replay.blend_model list)
      ()
  =
  D.deploy_asset
    ~asset
    ~cfg:g
    ~lo
    ~hi
    ~models
    ~target_survival:target
    ~pool
    ~fng
    ~fng_weight
    ~range_weight
    ~min_active_dsurv
    ~use_fng
    ~param_steps:5
    ~scan_points:12
    ~qty_cap_mult
;;

let test_qty_for_pool_round_trip () =
  let g = grid ~start_price:100.0 () in
  let n_fills = 20 in
  let pool = 1_000.0 in
  let qty = D.qty_for_pool ~cfg:g ~n_fills ~pool in
  (* The floor-aware cost at the returned qty fits the pool (the inversion
     rounds down to the lot, so it never overshoots)... *)
  let cost q =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q
      ~grid_interval_pct:g.grid_interval_pct
      ~fee:g.maker_fee
      ~start_price:g.start_price
      ~min_notional:g.min_notional
      ~price_increment:g.price_increment
      ~qty_increment:g.qty_increment
      ~n_fills
  in
  Alcotest.(check bool) "cost(qty) <= pool" (cost qty <= pool) true;
  (* ...and one lot more exceeds it (the inversion is at the boundary). *)
  Alcotest.(check bool)
    "cost(qty + lot) > pool"
    (cost (qty +. g.qty_increment) > pool)
    true
;;

let test_qty_for_pool_under_funded () =
  let g = grid ~start_price:100.0 () in
  let qty = D.qty_for_pool ~cfg:g ~n_fills:30 ~pool:1.0 in
  (* The floor is the venue lot OR the config qty, whichever is larger. *)
  near (Float.max (Float.max g.qty_min g.qty_increment) g.qty) qty
;;

let test_governing_drawdown () =
  let ms = models ~asset in
  let d, label =
    match Oracle_deploy.governing_drawdown ~models:ms ~target_survival:0.99 with
    | Some x -> x
    | None -> Alcotest.fail "expected a reachable governing drawdown"
  in
  Alcotest.(check bool) "d in (0,1)" (d > 0.0 && d < 1.0) true;
  Alcotest.(check bool) "label non-empty" (String.length label > 0) true;
  (* The governing drawdown must clear the target on every REACHABLE horizon
     (horizons whose target sits in a coverage gap are excluded by the
     engine, exactly as in deploy_asset). *)
  List.iter
    (fun (m : Oracle_replay.blend_model) ->
       match Oracle_deploy.horizon_target_drawdown m ~target_survival:0.99 with
       | None -> ()
       | Some _ ->
         let c = (Oracle_replay.blended_coverage m ~d_surv:d).blended in
         Alcotest.(check bool)
           (Printf.sprintf "governing clears %s" m.horizon.label)
           (c +. 1e-9 >= 0.99)
           true)
    ms;
  (* No reachable horizon on a too-short history (the empty-distribution guard
     raises inside every model): governing is None. *)
  let short = synth_series ~n:80 in
  let short_models =
    List.map
      (fun h ->
         Oracle_replay.blend_model_of
           ~horizon:h
           ~asset:short
           ~class_members:[ short ]
           ~kappa:10
           ~warmup
           ())
      horizons
  in
  match Oracle_deploy.governing_drawdown ~models:short_models ~target_survival:0.99 with
  | Some _ -> Alcotest.fail "expected unreachable governing drawdown"
  | None -> ()
;;

let test_deploy_fully_funded () =
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  (* Crypto, neutral F&G (50): the qty is blended toward the middle of
     [q_min, survival-max], so roughly half the pool deploys; the row passes
     (the replay, funded with the full pool budget, clears the target). *)
  let d = deploy ~pool:10_000.0 ~qty_cap_mult:0.0 ~g ~models:ms () in
  Alcotest.(check bool) "active" d.Oracle_types.active true;
  Alcotest.(check bool) "row passed" d.Oracle_types.row.passed true;
  (* Every horizon clears the target at the replayed D_surv. *)
  List.iter
    (fun (c : Oracle_types.deployment_coverage) ->
       Alcotest.(check bool)
         (Printf.sprintf "coverage %s" c.horizon_label)
         (c.blended_coverage +. 1e-9 >= 0.99)
         true)
    d.coverage;
  (* D_surv covers the governing drawdown (the replay is pool-funded, so a
     well-capitalized asset survives the whole path). *)
  Alcotest.(check bool) "d_surv >= d_gov" (d.d_surv +. 0.02 >= d.d_gov) true;
  (* gi is within the config range and never tighter than the survival side. *)
  Alcotest.(check bool)
    "gi in range"
    (d.parameter >= 0.5 -. 1e-9 && d.parameter <= 2.0 +. 1e-9)
    true;
  Alcotest.(check bool)
    "gi >= gi_survival"
    (d.parameter +. 1e-9 >= d.parameter_components.survival_parameter)
    true;
  (* At neutral F&G the qty blend sits in the middle of [q_min, survival-max]:
     the deployed capital is roughly half the pool, not the whole pool. *)
  Alcotest.(check bool)
    "deployed ~ half the pool at neutral F&G"
    (d.deployed > 0.4 *. d.pool_share && d.deployed < 0.6 *. d.pool_share)
    true;
  (* Equities are pure oracle: no F&G qty blend, so the survival-max qty
     deploys (almost) the whole pool. *)
  let d_eq =
    deploy ~pool:10_000.0 ~qty_cap_mult:0.0 ~g ~fng:None ~use_fng:false ~models:ms ()
  in
  Alcotest.(check bool) "equity active" d_eq.active true;
  Alcotest.(check bool)
    "equity row passed"
    (d_eq.row.passed || d_eq.d_surv +. 0.02 >= d_eq.d_gov)
    true;
  Alcotest.(check bool)
    "equity deploys >= 99% of pool (pure oracle, no F&G blend)"
    (d_eq.deployed >= 0.99 *. d_eq.pool_share)
    true
;;

let test_deploy_gi_blend () =
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  (* With a huge pool every gi passes, so the survival side allows the
     tightest config gi; the blend is then 0.5 * gi_fng + 0.5 * gi_lo. *)
  let d = deploy ~pool:100_000.0 ~g ~fng:(Some 50.0) ~fng_weight:0.5 ~models:ms () in
  near d.parameter_components.survival_parameter 0.5;
  near d.parameter 0.875;
  (* Equities (use_fng = false): pure survival side, no F&G. *)
  let d_eq = deploy ~pool:100_000.0 ~g ~fng:None ~use_fng:false ~models:ms () in
  Alcotest.(check bool)
    "equity gi_fng is None"
    (d_eq.parameter_components.fng_parameter = None)
    true;
  near d_eq.parameter d_eq.parameter_components.survival_parameter;
  near d_eq.parameter 0.5
;;

let test_deploy_under_funded () =
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let gi = 2.0 in
  let d_gov, _ =
    match Oracle_deploy.governing_drawdown ~models:ms ~target_survival:0.99 with
    | Some x -> x
    | None -> Alcotest.fail "governing drawdown unreachable"
  in
  let n_fills =
    Oracle_strategy.Grid.fills_for_drawdown { g with grid_interval_pct = gi } ~d:d_gov
  in
  (* A pool below the sizing-floor runway through d_gov: under-funded. The
     floor is the venue lot or the config qty, whichever is larger. *)
  let q_min = Float.max (Float.max g.qty_min g.qty_increment) g.qty in
  let full =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q_min
      ~grid_interval_pct:gi
      ~fee:g.maker_fee
      ~start_price:g.start_price
      ~min_notional:g.min_notional
      ~price_increment:g.price_increment
      ~qty_increment:g.qty_increment
      ~n_fills
  in
  let cost_one =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q_min
      ~grid_interval_pct:gi
      ~fee:g.maker_fee
      ~start_price:g.start_price
      ~min_notional:g.min_notional
      ~price_increment:g.price_increment
      ~qty_increment:g.qty_increment
      ~n_fills:1
  in
  let d =
    deploy ~pool:(full *. 0.5) ~g ~lo:gi ~hi:gi ~fng:None ~use_fng:false ~models:ms ()
  in
  Alcotest.(check bool) "under-funded still active" d.active true;
  Alcotest.(check bool) "qty at qty_min" (d.qty <= q_min +. 1e-9) true;
  Alcotest.(check bool) "deployed = whole pool" (d.deployed >= d.pool_share -. 1e-9) true;
  Alcotest.(check bool) "remainder 0 (priority keeps the pool)" (d.remainder = 0.0) true;
  Alcotest.(check bool) "under-funded warned" (d.warnings <> []) true;
  Alcotest.(check bool) "pool can still fund the first buy" (full *. 0.5 > cost_one) true
;;

let test_deploy_qty_cap () =
  (* With the default cap (qty_cap_mult = 1.0) the qty never exceeds the
     config qty: a pool larger than the design capital deploys only the
     design capital and passes the excess down the priority order. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d = deploy ~pool:10_000.0 ~g ~models:ms () in
  Alcotest.(check bool) "active" d.Oracle_types.active true;
  Alcotest.(check bool) "qty capped at config qty" (d.qty <= g.qty +. 1e-9) true;
  (* The design capital (cost at the config qty through d_gov) is far below
     the pool, so the excess passes down. *)
  Alcotest.(check bool)
    "deployed is the design capital, not the pool"
    (d.deployed < 0.5 *. d.pool_share)
    true;
  Alcotest.(check bool) "excess passes down" (d.remainder > 0.5 *. d.pool_share) true
;;

(** A series with a ~99.97% crash over a whisper-quiet drift: its windows
    carry a tiny (but non-zero) trailing vol, so the crash window's z-score
    sits far beyond anything the asset's drawdowns map to and the blended
    curve can never clear an extreme target - the fallback-sizing trigger. *)
let crash_series ~n =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let price = ref 100.0 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  for i = 0 to n - 1 do
    let osc = 0.0002 *. sin (float_of_int i /. 7.0) in
    let crash =
      if i >= 150 && i < 155 then 0.20 (* 5 bars x -80%: a ~99.97% collapse *) else 1.0
    in
    price := !price *. (1.0 +. osc) *. crash;
    let p = !price in
    bars.(i)
    <- { Oracle_types.date = iso i
       ; open_ = p
       ; high = p *. 1.001
       ; low = p *. 0.999
       ; close = p
       ; volume = 1000.0
       }
  done;
  { Oracle_types.symbol = "CRASH"; calendar_kind = Oracle_types.Crypto; bars; gaps = [] }
;;

(** A short series that ends at the trough of its own ~60% crash: with the
    ladder anchored at the path's first close and trailing the market, the
    replay now honestly realizes the crash (D_surv ~ the observed drawdown).
    This is the SPCX-shaped immature asset: the observed max drawdown is the
    only real signal. The gentle oscillation keeps the pre-crash segment
    volatile so the MFD window is not excluded as flat data. *)
let trough_series ~n =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let price = ref 100.0 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  for i = 0 to n - 1 do
    (* 18 consecutive -5% bars ending the series: a ~60% crash that starts
       inside the single MFD window (bars 61-81 at n=90, warmup 60) and ends
       at the trough. *)
    let osc = 0.003 *. sin (float_of_int i /. 9.0) in
    let crash = if i >= n - 18 then 0.95 else 1.0 in
    price := !price *. (1.0 +. osc) *. crash;
    let p = !price in
    bars.(i)
    <- { Oracle_types.date = iso i
       ; open_ = p
       ; high = p *. 1.001
       ; low = p *. 0.999
       ; close = p
       ; volume = 1000.0
       }
  done;
  { Oracle_types.symbol = "TROUGH"; calendar_kind = Oracle_types.Crypto; bars; gaps = [] }
;;

(** A series that crashes hard in the middle (bars 60-69, -5%/bar: ~40%
    drawdown) and then RECOVERS to near its peak: the price ends in the upper
    part of its range (d_from_ath small), so the ATH-anchored runway is a
    real positive fraction of the drawdown - the fallback parameter must
    loosen until the pool funds that runway. *)
let recover_series ~n =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let price = ref 100.0 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  for i = 0 to n - 1 do
    let osc = 0.003 *. sin (float_of_int i /. 9.0) in
    let crash = if i >= 60 && i < 70 then 0.95 else 1.0 in
    let recover = if i >= 70 then 1.025 else 1.0 in
    price := !price *. (1.0 +. osc) *. crash *. recover;
    let p = !price in
    bars.(i)
    <- { Oracle_types.date = iso i
       ; open_ = p
       ; high = p *. 1.001
       ; low = p *. 0.999
       ; close = p
       ; volume = 1000.0
       }
  done;
  { Oracle_types.symbol = "RECOVER"
  ; calendar_kind = Oracle_types.Crypto
  ; bars
  ; gaps = []
  }
;;

let test_deepest_observed_drawdown () =
  let ms = models ~asset in
  let d, label =
    match Oracle_deploy.deepest_observed_drawdown ms with
    | Some x -> x
    | None -> Alcotest.fail "expected a deepest observed drawdown"
  in
  Alcotest.(check bool) "d in (0,1)" (d > 0.0 && d < 1.0) true;
  Alcotest.(check bool) "label non-empty" (String.length label > 0) true;
  (* No windows anywhere -> None. *)
  let short = synth_series ~n:80 in
  let short_models =
    List.map
      (fun h ->
         Oracle_replay.blend_model_of
           ~horizon:h
           ~asset:short
           ~class_members:[ short ]
           ~kappa:10
           ~warmup
           ())
      horizons
  in
  match Oracle_deploy.deepest_observed_drawdown short_models with
  | Some _ -> Alcotest.fail "expected None on an empty history"
  | None -> ()
;;

let test_deploy_fallback_immature () =
  (* An immature asset (one window) whose blended model cannot clear the
     target (the class carries a far deeper z-tail) must still deploy: it is
     sized to the deepest drawdown its own history has actually observed,
     with the not-authoritative caveat flagged - never skipped. *)
  let g = grid ~start_price:100.0 () in
  let h21 = { Oracle_types.label = "21s"; sessions = 21; calendar_days = 21 } in
  (* High-vol, no crash: one window, shallow observed drawdowns, large
     trailing vol (small tau) so the class tail never saturates. *)
  let immature = synth_series ~n:90 in
  let crash = crash_series ~n:200 in
  let m =
    Oracle_replay.blend_model_of
      ~horizon:h21
      ~asset:immature
      ~class_members:[ crash ]
      ~kappa:10
      ~warmup
      ()
  in
  (* Target 1.0 sits above the blend's ceiling (the crash class never
     saturates at any achievable drawdown): unreachable on the only horizon. *)
  (match Oracle_deploy.governing_drawdown ~models:[ m ] ~target_survival:1.0 with
   | Some _ -> Alcotest.fail "expected unreachable governing drawdown"
   | None -> ());
  let d =
    deploy ~pool:5_000.0 ~asset:immature ~qty_cap_mult:0.0 ~g ~target:1.0 ~models:[ m ] ()
  in
  Alcotest.(check bool) "immature asset still active" d.Oracle_types.active true;
  Alcotest.(check bool)
    "d_gov is the deepest observed drawdown"
    (d.d_gov > 0.001 && d.d_gov < 0.5)
    true;
  Alcotest.(check bool) "governing horizon is 21s" (d.governing_horizon = "21s") true;
  Alcotest.(check bool)
    "fallback caveat warned"
    (List.exists (fun (w : string) -> contains w "deepest observed") d.warnings)
    true;
  (* The replay is funded and the sizing is still pool-constrained. (At the
     artificial target of 1.0 nothing can "pass": the blend ceiling sits
     below 1.0 - that is exactly why the fallback fired. In production the
     target is 0.99 and a normal class lets the raw sizing pass.) *)
  Alcotest.(check bool) "tuning surface present" (d.tuning_rows <> []) true;
  Alcotest.(check bool) "deployed fits the pool" (d.deployed <= 5_000.0 +. 1e-9) true
;;

let test_fallback_tunes_parameter_to_funding () =
  (* The fallback (immature-history) parameter must be tuned by the static
     funding check against the ATH-anchored runway - not collapse onto
     gi_lo. A replay-based fallback criterion is unusable on an immature
     history (the replay is not authoritative), so the criterion is the
     static funding check: the pool must fund the ATH-anchored drawdown at
     the floor qty at the chosen gi. The grid loosens until the pool can
     fund it. *)
  let recover = recover_series ~n:90 in
  let crash = crash_series ~n:200 in
  let h21 = { Oracle_types.label = "21s"; sessions = 21; calendar_days = 21 } in
  let m =
    Oracle_replay.blend_model_of
      ~horizon:h21
      ~asset:recover
      ~class_members:[ crash ]
      ~kappa:10
      ~warmup
      ()
  in
  (* Fallback: the blend cannot clear the extreme target. *)
  (match Oracle_deploy.governing_drawdown ~models:[ m ] ~target_survival:1.0 with
   | Some _ -> Alcotest.fail "expected unreachable governing drawdown"
   | None -> ());
  let d_gov, _ =
    match Oracle_deploy.deepest_observed_drawdown [ m ] with
    | Some x -> x
    | None -> Alcotest.fail "expected a deepest observed drawdown"
  in
  Alcotest.(check bool) "observed crash drawdown is real" (d_gov > 0.3) true;
  let lo, hi = 0.5, 2.0 in
  let sp = recover.bars.(Array.length recover.bars - 1).Oracle_types.close in
  let g = grid ~start_price:sp () in
  let r = Option.get (Oracle_math.range_stats_of recover) in
  let d_cover = Oracle_math.ath_anchored_drawdown ~d_gov r in
  (* The recover series ends near its peak, so the ATH-anchored runway is a
     real positive fraction of the observed drawdown (not collapsed to 0). *)
  Alcotest.(check bool) "runway positive" (d_cover > 0.05 && d_cover < d_gov) true;
  let q_min = D.sizing_floor ~cfg:g in
  let fills_at gi =
    let gi_frac = Float.min (gi /. 100.0) 0.99 in
    max
      1
      (int_of_float
         (Float.ceil (Float.log (1.0 -. d_cover) /. Float.log (1.0 -. gi_frac))))
  in
  let cost_at gi =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q_min
      ~grid_interval_pct:gi
      ~fee:0.0004
      ~start_price:sp
      ~min_notional:0.0
      ~price_increment:0.01
      ~qty_increment:0.01
      ~n_fills:(fills_at gi)
  in
  let cost_lo = cost_at lo in
  let cost_hi = cost_at hi in
  Alcotest.(check bool) "tighter grid costs more" (cost_lo > cost_hi) true;
  (* Pool between the two: un-fundable at gi_lo, fundable at gi_hi. *)
  let pool = (cost_lo +. cost_hi) /. 2.0 in
  let d =
    deploy
      ~asset:recover
      ~pool
      ~g
      ~lo
      ~hi
      ~fng:None
      ~use_fng:false
      ~target:1.0
      ~models:[ m ]
      ()
  in
  Alcotest.(check bool) "fallback asset still active" d.active true;
  Alcotest.(check bool)
    "grid loosened to the fundable parameter (not gi_lo)"
    (d.parameter > lo +. 0.001)
    true;
  Alcotest.(check bool)
    "the ATH-anchored runway is the sizing basis"
    (abs_float (d.d_cover -. d_cover) < 1e-9)
    true;
  Alcotest.(check bool) "deployed fits the pool" (d.deployed <= pool +. 1e-9) true;
  Alcotest.(check bool)
    "fallback caveat warned"
    (List.exists (fun (w : string) -> contains w "deepest observed") d.warnings)
    true
;;

let test_fallback_abundant_pool_squeezes_to_lo () =
  (* With capital far beyond the ATH-anchored runway's worst-case cost, the
     static funding check funds even the tightest config grid: the squeeze
     onto gi_lo is a computed outcome of the observed drawdown + the pool,
     not the degenerate replay collapse. *)
  let trough = trough_series ~n:90 in
  let crash = crash_series ~n:200 in
  let h21 = { Oracle_types.label = "21s"; sessions = 21; calendar_days = 21 } in
  let m =
    Oracle_replay.blend_model_of
      ~horizon:h21
      ~asset:trough
      ~class_members:[ crash ]
      ~kappa:10
      ~warmup
      ()
  in
  (match Oracle_deploy.governing_drawdown ~models:[ m ] ~target_survival:1.0 with
   | Some _ -> Alcotest.fail "expected unreachable governing drawdown"
   | None -> ());
  let d =
    let trough_close = trough.bars.(Array.length trough.bars - 1).Oracle_types.close in
    deploy
      ~asset:trough
      ~pool:1_000_000.0
      ~g:(grid ~start_price:trough_close ())
      ~lo:0.5
      ~hi:2.0
      ~fng:None
      ~use_fng:false
      ~target:1.0
      ~models:[ m ]
      ()
  in
  Alcotest.(check bool) "active with abundant capital" d.active true;
  Alcotest.(check bool)
    "tightest config grid funded by the observed drawdown"
    (abs_float (d.parameter -. 0.5) < 1e-9)
    true
;;

let test_fallback_grows_qty_while_funded () =
  (* An immature asset is volume driven too: the order qty grows up to the
     qty_cap while the static funding check (the pool funds the ATH-anchored
     drawdown at the sizing floor) holds. The trough asset ended below its
     ATH-anchored target level (d_cover = 0), so only the first buy needs
     funding and the qty squeezes to the cap; the excess passes down the
     priority order. *)
  let trough = trough_series ~n:90 in
  let crash = crash_series ~n:200 in
  let h21 = { Oracle_types.label = "21s"; sessions = 21; calendar_days = 21 } in
  let m =
    Oracle_replay.blend_model_of
      ~horizon:h21
      ~asset:trough
      ~class_members:[ crash ]
      ~kappa:10
      ~warmup
      ()
  in
  (match Oracle_deploy.governing_drawdown ~models:[ m ] ~target_survival:1.0 with
   | Some _ -> Alcotest.fail "expected unreachable governing drawdown"
   | None -> ());
  let sp = trough.bars.(Array.length trough.bars - 1).Oracle_types.close in
  let g = grid ~start_price:sp () in
  let q_min = D.sizing_floor ~cfg:g in
  let d =
    deploy
      ~asset:trough
      ~pool:1_000_000.0
      ~g
      ~lo:0.5
      ~hi:2.0
      ~fng:None
      ~use_fng:false
      ~target:1.0
      ~qty_cap_mult:10.0
      ~models:[ m ]
      ()
  in
  Alcotest.(check bool) "fallback asset active" d.active true;
  Alcotest.(check bool)
    "fallback qty grows to the cap while the funding check holds"
    (abs_float (d.qty -. (10.0 *. q_min)) < 1e-9)
    true;
  Alcotest.(check bool)
    "fallback deployed is the first-buy funding at the cap qty"
    (d.deployed > q_min *. sp *. 0.5 && d.deployed < 10.0 *. q_min *. sp)
    true;
  Alcotest.(check bool)
    "the rest of the pool passes down"
    (d.remainder > 0.9 *. d.pool_share)
    true
;;

let test_governing_basis () =
  (* Reachable history: the basis is the target-clearing governing drawdown,
     flagged as authoritative (not fallback), identical to governing_drawdown. *)
  let ms = models ~asset in
  (match Oracle_deploy.governing_basis ~models:ms ~target_survival:0.99 with
   | Some (d, h, fallback) ->
     Alcotest.(check bool) "target basis not fallback" (not fallback) true;
     (match Oracle_deploy.governing_drawdown ~models:ms ~target_survival:0.99 with
      | Some (d2, h2) ->
        near d d2;
        Alcotest.(check string) "same governing horizon" h h2
      | None -> Alcotest.fail "governing_drawdown should be reachable")
   | None -> Alcotest.fail "expected a target basis");
  (* Immature history (one window) with an unreachable target: the basis falls
     back to the deepest observed drawdown and is flagged as such. *)
  let immature = synth_series ~n:90 in
  let crash = crash_series ~n:200 in
  let h21 = { Oracle_types.label = "21s"; sessions = 21; calendar_days = 21 } in
  let m =
    Oracle_replay.blend_model_of
      ~horizon:h21
      ~asset:immature
      ~class_members:[ crash ]
      ~kappa:10
      ~warmup
      ()
  in
  (match Oracle_deploy.governing_basis ~models:[ m ] ~target_survival:1.0 with
   | Some (d, h, fallback) ->
     Alcotest.(check bool) "fallback flagged" fallback true;
     Alcotest.(check string) "fallback horizon is 21s" h "21s";
     Alcotest.(check bool) "fallback d in (0,1)" (d > 0.0 && d < 1.0) true;
     (match Oracle_deploy.deepest_observed_drawdown [ m ] with
      | Some (d2, _) -> near d d2
      | None -> Alcotest.fail "deepest observed should exist")
   | None -> Alcotest.fail "expected a fallback basis");
  (* No models at all: nothing computable. *)
  match Oracle_deploy.governing_basis ~models:[] ~target_survival:0.99 with
  | Some _ -> Alcotest.fail "expected no basis for an empty model set"
  | None -> ()
;;

let test_deploy_inactive () =
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  (* Pool below the first buy at qty_min. *)
  let d = deploy ~pool:0.01 ~g ~models:ms () in
  Alcotest.(check bool) "inactive on un-fundable pool" (not d.active) true;
  Alcotest.(check bool) "inactive passes the pool down" (d.remainder = d.pool_share) true;
  (* No reachable horizon at all: a history too short for warmup + horizon
     raises the empty-distribution guard, so every horizon is dropped and the
     asset cannot be sized. *)
  let short = synth_series ~n:80 in
  let short_models =
    List.map
      (fun h ->
         Oracle_replay.blend_model_of
           ~horizon:h
           ~asset:short
           ~class_members:[ short ]
           ~kappa:10
           ~warmup
           ())
      horizons
  in
  let d = deploy ~pool:10_000.0 ~g ~models:short_models () in
  Alcotest.(check bool) "inactive on no reachable horizon" (not d.active) true;
  (* --min-active-dsurv above the achievable runway: a small pool funds only a
     shallow ladder, so the replayed D_surv stays far below 99%. *)
  let d = deploy ~pool:5.0 ~g ~min_active_dsurv:0.99 ~models:ms () in
  Alcotest.(check bool) "inactive below min-active-dsurv" (not d.active) true
;;

let test_floor_aware_shrink () =
  (* A binding min_notional makes the replayed path burn faster than the
     closed form deep in the ladder; the verification loop must down-size qty
     (or flag under-funding at qty_min) instead of returning a sizing whose
     coverage misses the target. *)
  let g = grid ~start_price:100.0 ~min_notional:10.0 ~qty_min:0.05 () in
  let ms = models ~asset in
  let d = deploy ~pool:800.0 ~g ~lo:0.5 ~hi:0.5 ~fng:None ~use_fng:false ~models:ms () in
  if d.Oracle_types.active
  then (
    Alcotest.(check bool)
      "row passes or under-funded warning"
      (d.row.passed || d.warnings <> [])
      true;
    (* Whatever the outcome, the returned sizing must not overstate coverage:
       every horizon either clears the target or is flagged. *)
    List.iter
      (fun (c : Oracle_types.deployment_coverage) ->
         Alcotest.(check bool)
           (Printf.sprintf "coverage %s honest" c.horizon_label)
           (c.blended_coverage +. 1e-9 >= 0.99 || d.warnings <> [])
           true)
      d.coverage)
;;

(** A strictly monotone series (high = low = close): every close is its own
    ATH on the way up and its own all-time low on the way down, so the range
    position is exactly 0 (at the ATH) or 1 (at the low). *)
let linear_series ~n ~slope =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let price = ref 100.0 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  for i = 0 to n - 1 do
    price := !price *. (1.0 +. slope);
    let p = !price in
    bars.(i)
    <- { Oracle_types.date = iso i
       ; open_ = p
       ; high = p
       ; low = p
       ; close = p
       ; volume = 1000.0
       }
  done;
  { Oracle_types.symbol = "LIN"; calendar_kind = Oracle_types.Crypto; bars; gaps = [] }
;;

let test_range_stats () =
  (* ATH / all-time low / price and the derived stats over the synthetic
     history: the reference for the per-asset potential price range. *)
  let r =
    match Oracle_math.range_stats_of asset with
    | Some r -> r
    | None -> Alcotest.fail "expected range stats"
  in
  Alcotest.(check bool) "ath is the max high" (r.ath > 100.0) true;
  Alcotest.(check bool) "low is below the price" (r.all_time_low < r.price) true;
  Alcotest.(check bool)
    "price is the last close"
    (r.price = asset.bars.(Array.length asset.bars - 1).Oracle_types.close)
    true;
  Alcotest.(check bool)
    "d_from_ath in [0, range_span]"
    (r.d_from_ath >= 0.0 && r.d_from_ath <= r.range_span)
    true;
  Alcotest.(check bool) "span positive" (r.range_span > 0.0) true;
  Alcotest.(check bool) "d_to_low in [0, range_span]" (r.d_to_low >= 0.0) true;
  (* Empty history -> None. *)
  Alcotest.(check bool)
    "empty history -> None"
    (Oracle_math.range_stats_of
       { Oracle_types.symbol = "E"
       ; calendar_kind = Oracle_types.Crypto
       ; bars = [||]
       ; gaps = []
       }
     = None)
    true
;;

let test_range_parameter_direction () =
  (* The range side of the blend widens spacing near the ATH (the whole
     historical fall is still ahead: preserve runway) and tightens it near
     the all-time low (bounded remaining downside: the aggressive
     accumulation zone), working with the F&G contrarian convention. *)
  let lo, hi = 0.5, 2.0 in
  let r_ath =
    match Oracle_math.range_stats_of (linear_series ~n:60 ~slope:0.02) with
    | Some r -> r
    | None -> Alcotest.fail "expected stats for the rising series"
  in
  let r_low =
    match Oracle_math.range_stats_of (linear_series ~n:60 ~slope:(-0.02)) with
    | Some r -> r
    | None -> Alcotest.fail "expected stats for the falling series"
  in
  near r_ath.d_from_ath 0.0;
  near r_low.d_from_ath r_low.range_span;
  (* At the ATH (position 0) the range side is hi; at the low (position 1)
     it is lo. *)
  (match Oracle_deploy.range_parameter ~lo ~hi r_ath with
   | Some p -> near p hi
   | None -> Alcotest.fail "expected a range parameter at the ATH");
  match Oracle_deploy.range_parameter ~lo ~hi r_low with
  | Some p -> near p lo
  | None -> Alcotest.fail "expected a range parameter at the low"
;;

let test_deploy_range_blend () =
  (* With a huge pool the survival side allows gi_lo and F&G is neutral: the
     range side joins the blend. The synth asset ends in the UPPER part of
     its historical range (position ~0.2), so its range side sits above the
     config midpoint - spacing widens while the asset is still far from its
     lows (runway for the potential fall to the historical low). The final
     parameter is never tighter than the survival side: when the blended row
     itself fails the replayed path, the engine falls back to
     survival_parameter (runway wins over sentiment and range aggression). *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d =
    deploy
      ~pool:100_000.0
      ~g
      ~fng:(Some 50.0)
      ~fng_weight:0.5
      ~range_weight:0.25
      ~models:ms
      ()
  in
  Alcotest.(check bool) "active" d.active true;
  (match d.parameter_components.range_parameter with
   | Some rp ->
     Alcotest.(check bool)
       "range side above the midpoint (asset in the upper part of its range)"
       (rp > 1.25)
       true
   | None -> Alcotest.fail "expected a range parameter");
  Alcotest.(check bool)
    "range_weight carried"
    (d.parameter_components.range_weight = 0.25)
    true;
  Alcotest.(check bool)
    "gi never tighter than the survival side"
    (d.parameter >= d.parameter_components.survival_parameter -. 1e-9)
    true;
  Alcotest.(check bool) "gi within the config range" (d.parameter <= 2.0 +. 1e-9) true;
  Alcotest.(check bool)
    "blend row passes or falls back to survival_parameter"
    (d.row.passed
     || abs_float (d.parameter -. d.parameter_components.survival_parameter) < 1e-9)
    true
;;

let test_deploy_range_equity () =
  (* Equities (use_fng = false) are pure oracle: no F&G side AND no sentiment
     blend - the parameter is exactly the survival-constrained value (the
     tightest density that clears the target / the funding the capital
     allows). The range side stays computed for the record but never loosens
     the equity grid: survival is the only constraint and aggression the
     only tune. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d =
    deploy
      ~pool:100_000.0
      ~g
      ~fng:None
      ~use_fng:false
      ~fng_weight:0.5
      ~range_weight:0.25
      ~models:ms
      ()
  in
  Alcotest.(check bool) "active" d.active true;
  Alcotest.(check bool)
    "equity fng_parameter is None"
    (d.parameter_components.fng_parameter = None)
    true;
  Alcotest.(check bool)
    "equity gi = survival parameter (pure oracle, no sentiment blend)"
    (abs_float (d.parameter -. d.parameter_components.survival_parameter) < 1e-9)
    true;
  Alcotest.(check bool)
    "equity range side still computed for the record"
    (Option.is_some d.parameter_components.range_parameter)
    true
;;

let test_ath_anchored_drawdown () =
  (* The ATH-anchored survival drawdown d_cover caps the ladder's scale
     absolutely: the grid always covers down to the absolute target level
     ATH*(1 - d_gov), so the scale never shrinks endlessly as the market
     grinds down. *)
  let mk ~ath ~low ~price =
    { Oracle_types.ath
    ; all_time_low = low
    ; price
    ; d_from_ath = (if ath > 0.0 then (ath -. price) /. ath else 0.0)
    ; d_to_low = (if price > 0.0 then (price -. low) /. price else 0.0)
    ; range_span = (if ath > 0.0 then (ath -. low) /. ath else 0.0)
    }
  in
  let d_gov = 0.5 in
  (* At the ATH: the fall from the current price to ATH*(1-d_gov) is exactly
     d_gov. *)
  let r_ath = mk ~ath:100.0 ~low:10.0 ~price:100.0 in
  near (Oracle_math.ath_anchored_drawdown ~d_gov r_ath) d_gov;
  (* 20% below the ATH: part of the fall to the target level has already
     happened, so the remaining runway is smaller than d_gov (but positive). *)
  let r_mid = mk ~ath:100.0 ~low:10.0 ~price:80.0 in
  let d_mid = Oracle_math.ath_anchored_drawdown ~d_gov r_mid in
  Alcotest.(check bool) "middle runway in (0, d_gov)" (d_mid > 0.0 && d_mid < d_gov) true;
  (* At or below the target level ATH*(1-d_gov) = 50: the whole span has been
     traversed - the required runway is zero (only the first buy needs
     funding), so the model can be maximally aggressive. *)
  let r_deep = mk ~ath:100.0 ~low:10.0 ~price:50.0 in
  near (Oracle_math.ath_anchored_drawdown ~d_gov r_deep) 0.0;
  let r_below = mk ~ath:100.0 ~low:10.0 ~price:30.0 in
  near (Oracle_math.ath_anchored_drawdown ~d_gov r_below) 0.0;
  (* A new ATH (d_from_ath = 0 via the clamp) still sizes at d_gov. *)
  let r_new = mk ~ath:110.0 ~low:10.0 ~price:115.0 in
  near (Oracle_math.ath_anchored_drawdown ~d_gov r_new) d_gov;
  (* Monotone: the deeper the price sits below the ATH, the smaller the
     remaining runway. *)
  Alcotest.(check bool)
    "monotone in d_from_ath"
    (Oracle_math.ath_anchored_drawdown ~d_gov r_mid
     > Oracle_math.ath_anchored_drawdown ~d_gov r_deep)
    true
;;

let test_deploy_replay_pool_funded () =
  (* The verification replay is funded with the asset's pool budget, not the
     static ladder cost: a well-funded asset must pass and grow its qty to
     the cap instead of being falsely branded under-funded. (The old funding
     rule - min(pool, static ladder cost) - systematically under-stated the
     burn of a long replayed path and produced absurd "pool cannot fund"
     verdicts for deep-pooled assets like QQQ.) *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d_gov, _ =
    match Oracle_deploy.governing_drawdown ~models:ms ~target_survival:0.99 with
    | Some x -> x
    | None -> Alcotest.fail "governing drawdown unreachable"
  in
  let r = Option.get (Oracle_math.range_stats_of asset) in
  let d_cover = Oracle_math.ath_anchored_drawdown ~d_gov r in
  let n_fills =
    Oracle_strategy.Grid.fills_for_drawdown { g with grid_interval_pct = 0.5 } ~d:d_cover
  in
  let cost_floor =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:(D.sizing_floor ~cfg:g)
      ~grid_interval_pct:0.5
      ~fee:g.maker_fee
      ~start_price:g.start_price
      ~min_notional:g.min_notional
      ~price_increment:g.price_increment
      ~qty_increment:g.qty_increment
      ~n_fills
  in
  (* A pool far beyond the static runway cost (the old funding cap): the
     replay must still clear the target - the pool is the honest budget. *)
  let d = deploy ~pool:(50.0 *. cost_floor) ~g ~fng:None ~use_fng:false ~models:ms () in
  Alcotest.(check bool) "active with a deep pool" d.active true;
  Alcotest.(check bool)
    "row passes with the pool-funded replay"
    (d.row.passed || d.d_surv +. 0.02 >= d.d_gov)
    true;
  Alcotest.(check bool)
    "no under-funded warning"
    (not (List.exists (fun (w : string) -> contains w "under-funded") d.warnings))
    true;
  (* Equity, pure oracle: the qty grows to the cap (the whole headroom the
     config allows) and the gi squeezes to the survival-constrained lo. *)
  Alcotest.(check bool)
    "qty at the cap (aggressive within the qty range)"
    (abs_float (d.qty -. D.sizing_floor ~cfg:g) < 1e-9)
    true;
  Alcotest.(check bool)
    "gi at the survival-constrained lo"
    (abs_float (d.parameter -. d.parameter_components.survival_parameter) < 1e-9)
    true
;;

let test_deploy_crypto_qty_blend () =
  (* Crypto blends BOTH the grid interval and the qty with the F&G
     contrarian signal: max fear (fng 0) tightens the grid to the
     survival-constrained lo and up-sizes the qty to the survival-max; max
     greed (fng 100) loosens the grid and pulls the qty back toward the
     floor. Neither may break the survival constraint. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d_fear =
    deploy ~pool:100_000.0 ~g ~fng:(Some 0.0) ~qty_cap_mult:2.0 ~models:ms ()
  in
  let d_greed =
    deploy ~pool:100_000.0 ~g ~fng:(Some 100.0) ~qty_cap_mult:2.0 ~models:ms ()
  in
  Alcotest.(check bool) "fear active" d_fear.active true;
  Alcotest.(check bool) "greed active" d_greed.active true;
  (* Fear: qty = survival-max = the cap (q_min * 2). *)
  let q_min = D.sizing_floor ~cfg:g in
  Alcotest.(check bool)
    "fear qty at the survival-max (the cap)"
    (abs_float (d_fear.qty -. (2.0 *. q_min)) < 1e-9)
    true;
  (* Greed: qty pulled back to the floor. *)
  Alcotest.(check bool)
    "greed qty at the floor"
    (abs_float (d_greed.qty -. q_min) < 1e-9)
    true;
  (* Fear tightens the grid (fng side = lo), greed loosens it. *)
  Alcotest.(check bool)
    "fear gi tighter than greed gi"
    (d_fear.parameter < d_greed.parameter)
    true;
  (* The qty blend never exceeds the survival-max. *)
  Alcotest.(check bool)
    "blended qty bounded by the survival-max"
    (d_fear.qty +. 1e-9 >= q_min && d_fear.qty <= (2.0 *. q_min) +. 1e-9)
    true
;;

let test_deploy_equity_ignores_fng () =
  (* Equities are pure oracle: the F&G value never enters the sizing, so the
     parameter and qty are identical at fng 0 and fng 100. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d0 = deploy ~pool:100_000.0 ~g ~fng:(Some 0.0) ~use_fng:false ~models:ms () in
  let d100 = deploy ~pool:100_000.0 ~g ~fng:(Some 100.0) ~use_fng:false ~models:ms () in
  Alcotest.(check bool) "equity active" (d0.active && d100.active) true;
  Alcotest.(check bool)
    "equity fng_parameter None in both"
    (d0.parameter_components.fng_parameter = None
     && d100.parameter_components.fng_parameter = None)
    true;
  Alcotest.(check bool)
    "equity parameter identical across fng"
    (abs_float (d0.parameter -. d100.parameter) < 1e-9)
    true;
  Alcotest.(check bool)
    "equity qty identical across fng"
    (abs_float (d0.qty -. d100.qty) < 1e-9)
    true;
  Alcotest.(check bool)
    "equity sizing is the pure-oracle survival-max"
    (abs_float (d0.parameter -. d0.parameter_components.survival_parameter) < 1e-9)
    true
;;

let test_deploy_ath_cap_below_target () =
  (* Once the price is at or below the ATH-anchored target level
     ATH*(1-d_gov), the required runway is zero: only the first buy needs
     funding, so a small pool keeps the asset active and the sizing is
     maximally aggressive (gi_lo) instead of clamping to the widest grid. *)
  let trough = trough_series ~n:90 in
  let crash = crash_series ~n:200 in
  let h21 = { Oracle_types.label = "21s"; sessions = 21; calendar_days = 21 } in
  let m =
    Oracle_replay.blend_model_of
      ~horizon:h21
      ~asset:trough
      ~class_members:[ crash ]
      ~kappa:10
      ~warmup
      ()
  in
  let sp = trough.bars.(Array.length trough.bars - 1).Oracle_types.close in
  let g = grid ~start_price:sp () in
  let q_min = D.sizing_floor ~cfg:g in
  let cost_one =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q_min
      ~grid_interval_pct:2.0
      ~fee:g.maker_fee
      ~start_price:sp
      ~min_notional:0.0
      ~price_increment:0.01
      ~qty_increment:0.01
      ~n_fills:1
  in
  let d =
    deploy
      ~asset:trough
      ~pool:(1.5 *. cost_one)
      ~g
      ~lo:0.5
      ~hi:2.0
      ~fng:None
      ~use_fng:false
      ~target:1.0
      ~models:[ m ]
      ()
  in
  Alcotest.(check bool) "active funded by the first buy alone" d.active true;
  Alcotest.(check bool)
    "d_cover 0 (price below the ATH-anchored target level)"
    (d.d_cover = 0.0)
    true;
  Alcotest.(check bool)
    "gi squeezed to the tightest config value (max aggression)"
    (abs_float (d.parameter -. 0.5) < 1e-9)
    true;
  Alcotest.(check bool) "deployed fits the pool" (d.deployed <= d.pool_share +. 1e-9) true
;;

let () =
  Alcotest.run
    "oracle_deploy"
    [ ( "qty_for_pool"
      , [ Alcotest.test_case "round trip" `Quick test_qty_for_pool_round_trip
        ; Alcotest.test_case
            "under-funded returns qty_min"
            `Quick
            test_qty_for_pool_under_funded
        ] )
    ; ( "governing_drawdown"
      , [ Alcotest.test_case "deepest reachable horizon" `Quick test_governing_drawdown ]
      )
    ; ( "governing_basis"
      , [ Alcotest.test_case "target vs fallback vs none" `Quick test_governing_basis ] )
    ; ( "ath_anchored_drawdown"
      , [ Alcotest.test_case "absolute cap on the scale" `Quick test_ath_anchored_drawdown
        ] )
    ; ( "deploy_asset"
      , [ Alcotest.test_case "fully funded" `Quick test_deploy_fully_funded
        ; Alcotest.test_case "gi blend and equity rule" `Quick test_deploy_gi_blend
        ; Alcotest.test_case "under-funded priority" `Quick test_deploy_under_funded
        ; Alcotest.test_case "qty cap passes excess down" `Quick test_deploy_qty_cap
        ; Alcotest.test_case
            "replay funded by the pool budget"
            `Quick
            test_deploy_replay_pool_funded
        ; Alcotest.test_case
            "crypto qty blended with F&G"
            `Quick
            test_deploy_crypto_qty_blend
        ; Alcotest.test_case
            "equity ignores F&G entirely"
            `Quick
            test_deploy_equity_ignores_fng
        ; Alcotest.test_case
            "ATH cap: below-target price runs at max aggression"
            `Quick
            test_deploy_ath_cap_below_target
        ; Alcotest.test_case
            "immature fallback sizing"
            `Quick
            test_deploy_fallback_immature
        ; Alcotest.test_case
            "fallback tunes parameter to funding"
            `Quick
            test_fallback_tunes_parameter_to_funding
        ; Alcotest.test_case
            "fallback abundant pool squeezes to lo"
            `Quick
            test_fallback_abundant_pool_squeezes_to_lo
        ; Alcotest.test_case
            "fallback grows qty while funded"
            `Quick
            test_fallback_grows_qty_while_funded
        ; Alcotest.test_case "inactive cases" `Quick test_deploy_inactive
        ; Alcotest.test_case "floor-aware down-sizing" `Quick test_floor_aware_shrink
        ; Alcotest.test_case "range blend" `Quick test_deploy_range_blend
        ; Alcotest.test_case "range equity blend" `Quick test_deploy_range_equity
        ] )
    ; ( "deepest_observed_drawdown"
      , [ Alcotest.test_case
            "deepest window, None on empty"
            `Quick
            test_deepest_observed_drawdown
        ] )
    ; ( "range"
      , [ Alcotest.test_case "range stats" `Quick test_range_stats
        ; Alcotest.test_case
            "range parameter direction"
            `Quick
            test_range_parameter_direction
        ] )
    ]
;;
