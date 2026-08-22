(* Oracle_deploy tests: the capital deployment engine.

   The engine's contract, tested on a synthetic deterministic history:
   - qty_for_pool inverts the floor-aware runway cost (round trip, qty_min
     floor),
   - governing_drawdown picks the deepest reachable horizon target and is
     None when no horizon can clear the target,
   - the sizing drawdown d_cover is the largest ACTUAL peak-to-valley
     drawdown of the asset's history (the fall the grid must fund from the
     current price) - never an ATH-anchored / ATH-to-ATL construction, so a
     1000x run-up can never read as a phantom 99.9% drawdown,
   - the verification replay is funded with the asset's pool budget, so a
     well-funded asset passes (no false "under-funded" verdicts) and the
     sizing grows to the qty_cap,
   - the deployment is as aggressive as possible within the constraints:
     equities are pure oracle (parameter = survival_parameter, qty =
     survival-max, F&G never blends in), crypto blends the F&G contrarian
     signal on both gi and qty (fear tightens and up-sizes, greed loosens and
     pulls back) clamped to never break the survival constraint,
   - under-funded active assets run at qty_min with the shortfall warned and
     keep their whole share (config-order priority) - EXCEPT a committed grid
     (resting buy funded and locked in the account balance), which keeps
     running on that committed capital and passes its whole share down so a
     lower-priority asset can fund its own first order,
   - fallback (immature-history) assets are sized on the deepest observed
     drawdown, peak-to-valley; qty still grows up to the qty_cap while the
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
  ; accumulation_buffer = 100.0
    (* A buffer far above the in-window profit: the spec's sell-fill reserve
       never fires within a modeled window, isolating the survival-driven gi
       selection (the reserve path itself is covered by the grid_core and
       store tests). *)
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
      ?(seed = None)
      ?(has_committed_buy = false)
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
    ~seed
    ~has_committed_buy
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
  (* Coverage mode with the qty cap at 0 (no growth): the sizing targets 100%
     replay survival - the most aggressive (tightest) grid interval that
     survives the whole replayed history at the minimum order size. The
     order qty stays at the minimum (the cap is 0 = no growth), so the
     deployment consumes only the minimum-order runway through d_cover and
     the surplus passes down the priority order. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d = deploy ~pool:10_000.0 ~qty_cap_mult:0.0 ~g ~models:ms () in
  Alcotest.(check bool) "active" d.Oracle_types.active true;
  Alcotest.(check bool) "row passed" d.Oracle_types.row.passed true;
  (* The sizing is survival-driven: 100% replay survival (the whole history
     survived), gi = the tightest config parameter (0.5) that reaches it. *)
  Alcotest.(check bool) "100% replay survival" (d.d_surv >= 1.0 -. 1e-9) true;
  Alcotest.(check bool)
    "gi at the most aggressive config value with 100% survival"
    (abs_float (d.parameter -. 0.5) < 1e-9)
    true;
  Alcotest.(check bool)
    "resolved gi = the survival-driven gi (no blend)"
    (d.parameter = d.parameter_components.resolved_parameter
     && d.parameter = d.parameter_components.survival_parameter)
    true;
  (* qty cap 0 = the qty never grows: the minimum order size deploys. *)
  let q_min = D.sizing_floor ~cfg:g in
  Alcotest.(check bool) "qty at the minimum (cap 0 = no growth)" (d.qty = q_min) true;
  (* The deployment consumes only the minimum-order runway through d_cover
     (the synth asset's 15% floor-overshoot reference); the surplus passes
     down the priority order - the priority asset does NOT model against
     100% of the pool. *)
  Alcotest.(check bool)
    "deployed is the minimum-order runway, not the pool"
    (d.deployed > 0.0 && d.deployed < 0.1 *. d.pool_share)
    true;
  Alcotest.(check bool)
    "surplus passes down the priority order"
    (d.remainder > 0.9 *. d.pool_share)
    true;
  (* F&G is inert: the sizing is identical with or without it (the equity
     variant produces the same sizing). *)
  let d_eq =
    deploy ~pool:10_000.0 ~qty_cap_mult:0.0 ~g ~fng:None ~use_fng:false ~models:ms ()
  in
  Alcotest.(check bool)
    "fng does not move the sizing"
    (abs_float (d_eq.parameter -. d.parameter) < 1e-9 && d_eq.qty = d.qty)
    true
;;

let test_deploy_gi_selection () =
  (* The gi search over the FULL config range at the minimum order size: the
     most aggressive (tightest) parameter with 100% replay survival. With a
     huge pool every parameter survives, so the tightest config gi (0.5) is
     chosen; with a tighter pool the tightest parameters stop surviving and
     the search lands on the first passing one; with a pool so small that
     nothing survives, the sizing stretches to the grid maximum. F&G never
     moves the gi (the sizing is survival-driven, no blend). *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let d = deploy ~pool:100_000.0 ~g ~fng:(Some 50.0) ~models:ms () in
  Alcotest.(check bool) "active" d.Oracle_types.active true;
  near d.parameter 0.5;
  (* The F&G contrarian signal is gone from the sizing: no fng side, the
     survival-driven value is the resolved value. *)
  Alcotest.(check bool)
    "fng_parameter is None (no sentiment blend)"
    (d.parameter_components.fng_parameter = None)
    true;
  Alcotest.(check bool)
    "gi identical at max fear and max greed"
    (abs_float (deploy ~pool:100_000.0 ~g ~fng:(Some 0.0) ~models:ms ()).parameter
     -. d.parameter
     < 1e-9)
    true;
  (* A tighter pool: 0.5 no longer survives the whole history, the search
     lands on the tightest parameter that does (0.875). *)
  let d300 = deploy ~pool:300.0 ~g ~models:ms () in
  Alcotest.(check bool) "active at pool 300" d300.active true;
  Alcotest.(check bool)
    "100% survival at the selected gi"
    (d300.d_surv >= 1.0 -. 1e-9)
    true;
  near d300.parameter 0.875;
  (* A tiny pool (above the first-buy gate): no parameter survives the whole
     history - the sizing stretches: gi = the grid maximum, qty = minimum. *)
  let ds = deploy ~pool:15.0 ~g ~models:ms () in
  Alcotest.(check bool) "active in stretch mode" ds.active true;
  near ds.parameter 2.0;
  Alcotest.(check bool) "stretch qty at the minimum" (ds.qty = D.sizing_floor ~cfg:g) true;
  Alcotest.(check bool) "stretch cannot reach 100% survival" (ds.d_surv < 1.0) true;
  Alcotest.(check bool)
    "stretch mode warned"
    (List.exists (fun (w : string) -> contains w "unreachable") ds.warnings)
    true
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
  (* The synth asset is in the OUTLIER regime: the risk reference is the 15%
     floor-overshoot fallback, so the floor qty's runway through it costs
     less than the share - the asset consumes most (but not all) of its
     pool and the remainder passes down the priority order. *)
  Alcotest.(check bool)
    "deployed consumes most of the share (floor-qty runway through the 15% reference)"
    (d.deployed > 0.5 *. d.pool_share && d.deployed < d.pool_share)
    true;
  Alcotest.(check bool)
    "remainder passes down the priority order"
    (d.remainder > 0.0 && abs_float (d.remainder -. (d.pool_share -. d.deployed)) < 1e-6)
    true;
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
    part of its range, so the sizing drawdown is the full actual
    peak-to-valley fall of the crash - the fallback parameter must loosen
    until the pool funds that runway. *)
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

let test_deploy_committed_buy_gate () =
  (* The first-buy gate: an asset whose pool cannot fund its first buy at the
     minimum order size is inactive - UNLESS a committed resting buy is
     already in place (the first buy is funded and resting on the exchange;
     its cost is locked in the account balance). A committed grid keeps
     running - the grid's own capital gates pause it when the pool cannot
     extend another rung - instead of the oracle freezing a live grid. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let q_min = D.sizing_floor ~cfg:g in
  let cost_one =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q_min
      ~grid_interval_pct:2.0
      ~fee:g.maker_fee
      ~start_price:100.0
      ~min_notional:0.0
      ~price_increment:0.01
      ~qty_increment:0.01
      ~n_fills:1
  in
  (* Un-fundable pool, no committed buy: INACTIVE with the gate reason. *)
  let d = deploy ~pool:(0.5 *. cost_one) ~g ~models:ms () in
  Alcotest.(check bool) "inactive without a committed buy" (not d.active) true;
  Alcotest.(check bool)
    "reason names the first-buy gate"
    (contains d.reason "cannot fund the first buy")
    true;
  (* Same pool, committed buy in place: the grid keeps running. *)
  let dc = deploy ~pool:(0.5 *. cost_one) ~g ~has_committed_buy:true ~models:ms () in
  Alcotest.(check bool) "active with a committed buy" dc.active true;
  (* The sizing is still the honest survival-driven sizing on the pool: qty
     at the minimum (stretch - the pool cannot fund a fresh ladder) and the
     grid interval at the config maximum. *)
  Alcotest.(check bool)
    "committed-buy sizing stays within the qty range"
    (dc.qty >= q_min)
    true;
  Alcotest.(check bool)
    "committed-buy gi within the config range"
    (dc.parameter >= 0.5 -. 1e-9 && dc.parameter <= 2.0 +. 1e-9)
    true
;;

let test_deploy_committed_buy_passes_pool_down () =
  (* The committed-grid allocation: an under-funded ACTIVE grid with a
     committed resting buy keeps running on its committed capital and draws
     NOTHING new from the available pool - its whole share passes down the
     priority order so a lower-priority asset can fund its own first order.
     A committed grid whose pool DOES fund the ladder still consumes its
     ladder cost and passes only the surplus; and an under-funded grid
     WITHOUT a committed buy keeps its whole share (unchanged). *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let q_min = D.sizing_floor ~cfg:g in
  let cost_one =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:q_min
      ~grid_interval_pct:2.0
      ~fee:g.maker_fee
      ~start_price:g.start_price
      ~min_notional:g.min_notional
      ~price_increment:g.price_increment
      ~qty_increment:g.qty_increment
      ~n_fills:1
  in
  (* An under-funded pool: above the first-buy gate (so it would run without
     a committed buy too) but far below the full ladder cost. *)
  let pool = 1.5 *. cost_one in
  (* Control, no committed buy: the under-funded asset keeps its whole share
     (deployed = pool, nothing passes down). *)
  let d0 = deploy ~pool ~g ~models:ms () in
  Alcotest.(check bool) "under-funded (no committed) is active" d0.active true;
  Alcotest.(check bool) "under-funded (no committed) not passed" (not d0.row.passed) true;
  Alcotest.(check bool)
    "under-funded (no committed) consumes the whole share"
    (abs_float (d0.deployed -. pool) < 1e-6 && d0.remainder = 0.0)
    true;
  (* The committed grid on the SAME pool: still active, draws nothing, passes
     the whole pool down. *)
  let dc = deploy ~pool ~g ~has_committed_buy:true ~models:ms () in
  Alcotest.(check bool) "committed under-funded stays active" dc.active true;
  Alcotest.(check bool) "committed under-funded not passed" (not dc.row.passed) true;
  Alcotest.(check bool)
    "committed under-funded draws nothing from the pool"
    (dc.deployed = 0.0)
    true;
  Alcotest.(check bool)
    "committed under-funded passes the whole share down"
    (abs_float (dc.remainder -. pool) < 1e-6)
    true;
  (* A funded committed grid (pool >> ladder) still consumes its ladder cost
     and passes only the surplus - unchanged from before the fix. *)
  let df = deploy ~pool:10_000.0 ~g ~has_committed_buy:true ~models:ms () in
  Alcotest.(check bool) "committed funded stays active" df.active true;
  Alcotest.(check bool) "committed funded row passed" df.row.passed true;
  Alcotest.(check bool)
    "committed funded consumes its ladder cost"
    (df.deployed > 0.0 && df.deployed < 0.5 *. df.pool_share)
    true;
  Alcotest.(check bool)
    "committed funded passes only the surplus"
    (abs_float (df.remainder -. (df.pool_share -. df.deployed)) < 1e-6)
    true
;;

let test_deploy_seeded_replay () =
  (* The sizing replay starts from the account's accumulated state (held
     base / reserved base / accumulated profit) when a seed is provided: on
     an accumulation venue the seeded grid can sell reduced from the start,
     so the survival verdict models the strategy as it actually runs. The
     seed must flow through without disturbing the sizing contract. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let seed =
    Some
      Dio_strategies.Grid_core_types.
        { initial_base = 1.0
        ; initial_reserved_base = 0.0
        ; initial_accumulated_profit = 0.0
        }
  in
  let d = deploy ~pool:10_000.0 ~seed ~g ~models:ms () in
  Alcotest.(check bool) "seeded deployment active" d.active true;
  Alcotest.(check bool)
    "seeded sizing keeps the survival target (100% survival at qty min)"
    (d.d_surv >= 1.0 -. 1e-9)
    true;
  Alcotest.(check bool)
    "seeded gi is the most aggressive with 100% survival"
    (abs_float (d.parameter -. 0.5) < 1e-9)
    true
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
     funding check against the actual peak-to-valley runway - not collapse
     onto gi_lo. A replay-based fallback criterion is unusable on an immature
     history (the replay is not authoritative), so the criterion is the
     static funding check: the pool must fund the actual peak-to-valley
     drawdown at the floor qty at the chosen gi. The grid loosens until the
     pool can fund it. *)
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
  let d_cover =
    let p = Option.get (Oracle_math.peak_to_valley_stats_of recover) in
    p.Oracle_types.max_drawdown
  in
  (* The recover series ends near its peak, so the sizing drawdown is the
     full actual peak-to-valley fall the crash produced - a real positive
     fraction of the observed drawdown, at least as deep as the deepest MFD
     window (the p2v measurement starts from the running peak, which is
     >= every window start close). *)
  Alcotest.(check bool) "runway positive" (d_cover > 0.05 && d_cover < 1.0) true;
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
    "the actual peak-to-valley drawdown is the sizing basis"
    (abs_float (d.d_cover -. d_cover) < 1e-9)
    true;
  Alcotest.(check bool) "deployed fits the pool" (d.deployed <= pool +. 1e-9) true;
  Alcotest.(check bool)
    "fallback caveat warned"
    (List.exists (fun (w : string) -> contains w "deepest observed") d.warnings)
    true
;;

let test_fallback_abundant_pool_squeezes_to_lo () =
  (* With capital far beyond the actual peak-to-valley runway's worst-case
     cost, the
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
     qty_cap while the static funding check (the pool funds the actual
     peak-to-valley drawdown at the sizing floor) holds. The trough asset
     ended at the valley of its ~60% actual max drawdown - funding it at the
     cap qty costs the full ladder through that drawdown (no "first buy
     only" shortcut: the actual drawdown must be funded from the current
     price), and the excess passes down the priority order. *)
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
  let d_cover =
    let p = Option.get (Oracle_math.peak_to_valley_stats_of trough) in
    p.Oracle_types.max_drawdown
  in
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
  (* Deployed = min(pool, floor-aware ladder cost through the actual max
     drawdown at the cap qty) - the full peak-to-valley runway, not just the
     first buy. *)
  let n_fills =
    Oracle_strategy.Grid.fills_for_drawdown { g with grid_interval_pct = 0.5 } ~d:d_cover
  in
  let cost_cap =
    Oracle_mfd.floor_aware_runway_cost
      ~qty:(10.0 *. q_min)
      ~grid_interval_pct:0.5
      ~fee:g.maker_fee
      ~start_price:sp
      ~min_notional:0.0
      ~price_increment:0.01
      ~qty_increment:0.01
      ~n_fills
  in
  Alcotest.(check bool)
    "fallback deployed funds the actual peak-to-valley drawdown at the cap qty"
    (d.deployed > 0.9 *. cost_cap && d.deployed <= cost_cap +. 1e-6)
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

(** A flat series with a mid-history dip (~11% peak-to-valley) and a partial
    recovery to a price strictly between the event valley and the event peak:
    the range side must land strictly between lo and hi, on the formula
    position = (peak - price) / (peak - valley). *)
let dip_series ~n =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let price = ref 100.0 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  for i = 0 to n - 1 do
    let mult =
      if i < 40
      then 1.0
      else if i < 60
      then 0.994 (* 20 x -0.6%: an ~11% peak-to-valley dip *)
      else if i < 70
      then 1.0069 (* partial recovery *)
      else 1.0
    in
    price := !price *. mult;
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
  { Oracle_types.symbol = "DIP"; calendar_kind = Oracle_types.Crypto; bars; gaps = [] }
;;

let test_range_parameter_direction () =
  (* The range side of the blend is anchored on the largest ACTUAL
     peak-to-valley drawdown event, not the ATH/ATL span: above the event
     peak the full max drawdown is still ahead (widen: preserve runway), at
     the event valley the downside is bounded by what actually happened
     (tighten: the aggressive accumulation zone), and a strictly monotone
     series - which never drew down - carries no range information at all. *)
  let lo, hi = 0.5, 2.0 in
  (* Strictly rising: no drawdown ever -> no p2v event, so the range side is
     absent (the strictly falling series below provides the bound cases). *)
  (match Oracle_math.peak_to_valley_stats_of (linear_series ~n:60 ~slope:0.02) with
   | Some _ -> Alcotest.fail "expected no p2v event on a monotone rising series"
   | None -> ());
  (* Strictly falling: the price sits at the event valley (position 1) -> lo. *)
  let p_fall =
    Option.get (Oracle_math.peak_to_valley_stats_of (linear_series ~n:60 ~slope:(-0.02)))
  in
  Alcotest.(check bool)
    "falling series ends at the valley"
    (p_fall.price = p_fall.valley)
    true;
  (match Oracle_deploy.range_parameter ~lo ~hi p_fall with
   | Some p -> near p lo
   | None -> Alcotest.fail "expected a range parameter at the event valley");
  (* Mid-way between the event peak and valley: the formula position. *)
  let dip = dip_series ~n:120 in
  let p_dip = Option.get (Oracle_math.peak_to_valley_stats_of dip) in
  Alcotest.(check bool)
    "dip price strictly between valley and peak"
    (p_dip.valley < p_dip.price && p_dip.price < p_dip.peak)
    true;
  let position = (p_dip.peak -. p_dip.price) /. (p_dip.peak -. p_dip.valley) in
  match Oracle_deploy.range_parameter ~lo ~hi p_dip with
  | Some p -> near p (lo +. ((1.0 -. position) *. (hi -. lo)))
  | None -> Alcotest.fail "expected a range parameter for the dip"
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

let test_peak_to_valley_stats () =
  (* The sizing drawdown is the largest ACTUAL peak-to-valley drawdown of the
     history: each bar's fall from the running peak of closes down to that
     bar's low, maximized. A 1000x run-up only registers the falls that
     actually happened - never the ATH-to-ATL span. *)
  let stats asset_ =
    match Oracle_math.peak_to_valley_stats_of asset_ with
    | Some p -> p
    | None -> Alcotest.fail "expected p2v stats"
  in
  (* The synth series: a ~33% crash after a gentle decline from its start
     peak, then recovery. The max drawdown is measured from the running peak
     (the first close, ~100) to the crash trough low. *)
  let p_synth = stats asset in
  Alcotest.(check bool)
    "synth p2v in (0.4, 0.46)"
    (p_synth.max_drawdown > 0.4 && p_synth.max_drawdown < 0.46)
    true;
  Alcotest.(check bool)
    "peak precedes valley"
    (p_synth.peak_idx < p_synth.valley_idx)
    true;
  Alcotest.(check bool) "peak above valley" (p_synth.peak > p_synth.valley) true;
  Alcotest.(check bool)
    "price is the last close"
    (p_synth.price = asset.bars.(Array.length asset.bars - 1).Oracle_types.close)
    true;
  Alcotest.(check bool)
    "dates non-empty"
    (p_synth.peak_date <> "" && p_synth.valley_date <> "")
    true;
  (* The crash series: a ~99.97% collapse registers as a ~99.97% drawdown -
     the actual event, never inflated beyond what happened. *)
  let p_crash = stats (crash_series ~n:200) in
  Alcotest.(check bool) "crash p2v above 99%" (p_crash.max_drawdown > 0.99) true;
  (* The trough series: ends at the valley of its ~60% crash. *)
  let p_trough = stats (trough_series ~n:90) in
  Alcotest.(check bool)
    "trough p2v in (0.55, 0.65)"
    (p_trough.max_drawdown > 0.55 && p_trough.max_drawdown < 0.65)
    true;
  Alcotest.(check bool)
    "trough price sits at the valley (within the 0.1% bar low)"
    (p_trough.price >= p_trough.valley
     && p_trough.price -. p_trough.valley < p_trough.peak *. 0.01)
    true;
  (* The recover series: crash then recovery near the peak - the drawdown is
     the crash itself, not the ATH-to-ATL span (which is tiny here). *)
  let p_recover = stats (recover_series ~n:90) in
  Alcotest.(check bool)
    "recover p2v in (0.38, 0.43)"
    (p_recover.max_drawdown > 0.38 && p_recover.max_drawdown < 0.43)
    true;
  (* A strictly monotone rising series never drew down -> None. *)
  Alcotest.(check bool)
    "monotone rising -> None"
    (Oracle_math.peak_to_valley_stats_of (linear_series ~n:60 ~slope:0.02) = None)
    true;
  (* Empty history -> None. *)
  Alcotest.(check bool)
    "empty history -> None"
    (Oracle_math.peak_to_valley_stats_of
       { Oracle_types.symbol = "E"
       ; calendar_kind = Oracle_types.Crypto
       ; bars = [||]
       ; gaps = []
       }
     = None)
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
  let d_cover =
    let p = Option.get (Oracle_math.peak_to_valley_stats_of asset) in
    p.Oracle_types.max_drawdown
  in
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

let test_deploy_qty_survival_scale () =
  (* The qty scale-up rule: the order qty grows ONLY to deploy residual
     capital BEHIND 100% survival, bounded by the qty_cap (qty_cap_mult is
     the cap, not a rule). With a deep pool the largest qty keeping 100%
     replay survival is the cap itself; with a tighter pool the replay
     bounds it below the cap; with the cap at 0 the qty never grows. F&G is
     inert - the sizing is identical at max fear and max greed. *)
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset in
  let q_min = D.sizing_floor ~cfg:g in
  (* Deep pool, cap 2x: the qty reaches the cap, 100% survival holds. *)
  let d = deploy ~pool:100_000.0 ~g ~fng:(Some 0.0) ~qty_cap_mult:2.0 ~models:ms () in
  Alcotest.(check bool) "active" d.active true;
  Alcotest.(check bool)
    "qty at the cap (the qty range ceiling)"
    (abs_float (d.qty -. (2.0 *. q_min)) < 1e-9)
    true;
  Alcotest.(check bool) "100% survival at the cap" (d.d_surv >= 1.0 -. 1e-9) true;
  (* Tighter pool: the largest qty that still survives the whole history is
     below the cap - the survival replay (funded with the pool) bounds the
     "deploy all capital" scale-up. *)
  let d800 = deploy ~pool:800.0 ~g ~qty_cap_mult:2.0 ~models:ms () in
  Alcotest.(check bool) "active at pool 800" d800.active true;
  Alcotest.(check bool)
    "qty bounded below the cap by 100% survival"
    (d800.qty < 2.0 *. q_min && d800.qty > q_min)
    true;
  Alcotest.(check bool)
    "100% survival at the bounded qty"
    (d800.d_surv >= 1.0 -. 1e-9)
    true;
  (* The cap is a ceiling, not a rule: with the cap at 0 (no growth) the qty
     stays at the minimum even with a deep pool. *)
  let d0 = deploy ~pool:100_000.0 ~g ~qty_cap_mult:0.0 ~models:ms () in
  Alcotest.(check bool)
    "cap 0 = qty never grows"
    (abs_float (d0.qty -. q_min) < 1e-9)
    true;
  (* F&G inert: identical sizing at max greed. *)
  let dg = deploy ~pool:100_000.0 ~g ~fng:(Some 100.0) ~qty_cap_mult:2.0 ~models:ms () in
  Alcotest.(check bool)
    "fng does not move the qty"
    (abs_float (dg.qty -. d.qty) < 1e-9 && abs_float (dg.parameter -. d.parameter) < 1e-9)
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

let test_deploy_valley_still_funds_drawdown () =
  (* The p2v sizing has no "below the ATH-anchored target level -> zero
     runway" rule: an asset sitting AT the valley of its largest actual
     peak-to-valley drawdown still requires that drawdown to be funded from
     the current price. A pool that only covers the first buy keeps the
     asset active but under-funded - the full actual drawdown is the sizing
     basis, not the ATH relationship. *)
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
  let d_cover =
    let p = Option.get (Oracle_math.peak_to_valley_stats_of trough) in
    p.Oracle_types.max_drawdown
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
    "d_cover is the full actual peak-to-valley drawdown (not 0 at the valley)"
    (abs_float (d.d_cover -. d_cover) < 1e-9)
    true;
  Alcotest.(check bool)
    "gi loosened to the widest value (the pool cannot fund the actual drawdown)"
    (abs_float (d.parameter -. 2.0) < 1e-9)
    true;
  Alcotest.(check bool)
    "under-funded warning names the drawdown gap"
    (List.exists (fun (w : string) -> contains w "drawdown") d.warnings)
    true;
  Alcotest.(check bool) "deployed fits the pool" (d.deployed <= d.pool_share +. 1e-9) true
;;

let test_peak_to_valley_1000x_runup () =
  (* The user's exact complaint: a 1000x run-up must NOT make the sizing read
     as a 99.9% drawdown. This series runs 0.01 -> 10 (1000x) then crashes
     to 1.0: the ATH-to-ATL span is ~99.9%, but the largest ACTUAL
     peak-to-valley fall is only the crash (~90%). The p2v stats must report
     the real event - and the range side must see the price at the event
     valley (position 1 -> tight, the aggressive accumulation zone). *)
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let n = 260 in
  let bars =
    Array.init n (fun i ->
      let p =
        if i < 200
        then
          0.01 *. (1000.0 ** (float_of_int i /. 200.0)) (* 0.01 -> ~10: a 1000x run-up *)
        else if i = 200
        then 10.0 (* the peak close *)
        else if i = 201
        then 1.0 (* the crash to 1/10 of the peak: a ~90% actual drawdown *)
        else 1.0
      in
      { Oracle_types.date = iso i
      ; open_ = p
      ; high = p *. 1.001
      ; low = p *. 0.999
      ; close = p
      ; volume = 1000.0
      })
  in
  let runup =
    { Oracle_types.symbol = "RUNUP"
    ; calendar_kind = Oracle_types.Crypto
    ; bars
    ; gaps = []
    }
  in
  let p = Option.get (Oracle_math.peak_to_valley_stats_of runup) in
  Alcotest.(check bool)
    "actual p2v drawdown is the crash (~90%), not the 99.9% ATH-to-ATL span"
    (p.max_drawdown > 0.89 && p.max_drawdown < 0.91)
    true;
  Alcotest.(check bool) "peak is the run-up top" (p.peak > 9.9 && p.peak < 10.1) true;
  Alcotest.(check bool)
    "valley is the crash bottom"
    (p.valley > 0.99 && p.valley < 1.01)
    true;
  Alcotest.(check bool)
    "range side: price at the event valley -> tight (near lo)"
    (match Oracle_deploy.range_parameter ~lo:0.5 ~hi:2.0 p with
     | Some rp -> rp < 0.6
     | None -> false)
    true;
  (* The span context stays ~99.9% - but it is display context only. *)
  let r = Option.get (Oracle_math.range_stats_of runup) in
  Alcotest.(check bool)
    "span context is ~99.9% (the phantom the p2v sizing rejects)"
    (r.range_span > 0.99)
    true
;;

let test_peak_to_valley_ordering () =
  (* The venue feeds can return bars newest-first (the Hyperliquid
     pagination reverses its pages; only the deepen step re-sorts). The p2v
     computation must sort chronologically first - a backwards series would
     fabricate a "peak -> valley" event whose valley PREDATES the peak. The
     stats on the reversed series must be identical to the sorted one. *)
  let ascending = recover_series ~n:90 in
  let reversed =
    Array.init (Array.length ascending.bars) (fun i ->
      ascending.bars.(Array.length ascending.bars - 1 - i))
  in
  let descending = { ascending with Oracle_types.bars = reversed } in
  let p_asc = Option.get (Oracle_math.peak_to_valley_stats_of ascending) in
  let p_desc = Option.get (Oracle_math.peak_to_valley_stats_of descending) in
  Alcotest.(check bool)
    "identical max drawdown regardless of bar order"
    (abs_float (p_asc.max_drawdown -. p_desc.max_drawdown) < 1e-12)
    true;
  Alcotest.(check bool)
    "identical peak regardless of bar order"
    (abs_float (p_asc.peak -. p_desc.peak) < 1e-9)
    true;
  Alcotest.(check bool)
    "identical valley regardless of bar order"
    (abs_float (p_asc.valley -. p_desc.valley) < 1e-9)
    true;
  Alcotest.(check bool)
    "peak precedes valley in time (the bug this guards against)"
    (p_asc.peak_date < p_asc.valley_date)
    true
;;

(* A tiny hand-built series: (date, close, low) per day; high = close. *)
let bars_of (rows : (string * float * float) list) : Oracle_types.series =
  { Oracle_types.symbol = "HAND"
  ; calendar_kind = Oracle_types.Crypto
  ; bars =
      Array.of_list
        (List.map
           (fun (date, c, l) ->
              Oracle_types.
                { date; open_ = c; high = c; low = l; close = c; volume = 1000.0 })
           rows)
  ; gaps = []
  }
;;

let test_p2v_recovered_flag () =
  (* A crash that fully retraced (a later close >= the peak) anchors the
     ATH-scaled reference; a crash the asset is still inside does not - it
     falls to the outlier policy. *)
  let mk = bars_of in
  let p =
    Option.get
      (Oracle_math.peak_to_valley_stats_of
         (mk [ "d1", 100.0, 100.0; "d2", 40.0, 40.0; "d3", 110.0, 110.0 ]))
  in
  Alcotest.(check bool) "retraced crash recovered" p.recovered true;
  let p2 =
    Option.get
      (Oracle_math.peak_to_valley_stats_of
         (mk [ "d1", 100.0, 100.0; "d2", 40.0, 40.0; "d3", 60.0, 60.0 ]))
  in
  Alcotest.(check bool) "still-inside crash not recovered" (not p2.recovered) true;
  (* The synth asset's crash retraced to ~99.6% of its peak but never closed
     at or above it: strictly unrecovered (the recovered-only rule). *)
  Alcotest.(check bool)
    "synth asset not recovered (retrace stopped short of the peak)"
    (not (Option.get (Oracle_math.peak_to_valley_stats_of asset)).recovered)
    true;
  Alcotest.(check bool)
    "trough series not recovered (ends at its valley)"
    (not
       (Option.get (Oracle_math.peak_to_valley_stats_of (trough_series ~n:90))).recovered)
    true
;;

let test_floor_overshoot_p90 () =
  let mk = bars_of in
  (* W-bottom: a floor at 80 is established (bounce to 85) and then broken
     to 75 before the recovery - one 6.25% break. *)
  let w =
    mk
      [ "d1", 100.0, 100.0
      ; "d2", 80.0, 80.0
      ; "d3", 85.0, 85.0
      ; "d4", 75.0, 75.0
      ; "d5", 100.0, 100.0
      ]
  in
  Alcotest.(check (option (float 1e-9)))
    "W-bottom break measured"
    (Some 0.0625)
    (Oracle_math.floor_overshoot_p90_of w);
  (* Two breaks: the p90 is the deeper one. *)
  let w2 =
    mk
      [ "d1", 100.0, 100.0
      ; "d2", 80.0, 80.0
      ; "d3", 85.0, 85.0
      ; "d4", 75.0, 75.0
      ; "d5", 100.0, 100.0
      ; "d6", 90.0, 90.0
      ; "d7", 95.0, 95.0
      ; "d8", 70.0, 70.0
      ; "d9", 100.0, 100.0
      ]
  in
  Alcotest.(check (option (float 1e-9)))
    "deeper break drives the p90"
    (Some 0.06625)
    (Oracle_math.floor_overshoot_p90_of w2);
  (* A V-bottom: no floor was broken - nothing measured. *)
  let v = mk [ "d1", 100.0, 100.0; "d2", 75.0, 75.0; "d3", 100.0, 100.0 ] in
  Alcotest.(check bool)
    "V-bottom: no break"
    (Oracle_math.floor_overshoot_p90_of v = None)
    true;
  (* A continuous fall: no floor was ever established - nothing measured. *)
  let fall =
    mk
      [ "d1", 100.0, 100.0
      ; "d2", 90.0, 90.0
      ; "d3", 80.0, 80.0
      ; "d4", 70.0, 70.0
      ; "d5", 100.0, 100.0
      ]
  in
  Alcotest.(check bool)
    "continuous fall: no floor established"
    (Oracle_math.floor_overshoot_p90_of fall = None)
    true;
  (* A break that never recovered is discarded (no proof of recovery). *)
  let unproven =
    mk [ "d1", 100.0, 100.0; "d2", 80.0, 80.0; "d3", 85.0, 85.0; "d4", 75.0, 75.0 ]
  in
  Alcotest.(check bool)
    "break without recovery discarded"
    (Oracle_math.floor_overshoot_p90_of unproven = None)
    true
;;

let test_sizing_reference () =
  let mk = bars_of in
  (* Recovered crash, price above the ATH-scaled floor: fund the remainder.
     ATH 110, dd (100 -> 40) = 60% -> floor 44; price 80 -> (80-44)/80. *)
  let a =
    mk [ "d1", 100.0, 100.0; "d2", 40.0, 40.0; "d3", 110.0, 110.0; "d4", 80.0, 80.0 ]
  in
  let r = Option.get (Oracle_math.sizing_reference_of ~fallback:false a) in
  Alcotest.(check bool) "recovered: not at the floor" (not r.at_floor) true;
  Alcotest.(check bool) "recovered: not an outlier" (not r.outlier) true;
  near r.d_cover 0.45;
  (match r.floor_ref with
   | Some f -> near f 44.0
   | None -> Alcotest.fail "expected floor_ref");
  (* Price at/below the floor ("living in the max drawdown"): the remainder
     is exhausted - the 0.15 fallback funds it (nothing measured here). The
     second drop lands exactly ON the scaled floor (44 = ATH 110 x 0.4), so
     its drawdown ties the first crash and the recovered first crash stays
     the anchor. *)
  let b =
    mk [ "d1", 100.0, 100.0; "d2", 40.0, 40.0; "d3", 110.0, 110.0; "d4", 44.0, 44.0 ]
  in
  let rb = Option.get (Oracle_math.sizing_reference_of ~fallback:false b) in
  Alcotest.(check bool) "at the floor" rb.at_floor true;
  Alcotest.(check bool) "at floor: not an outlier" (not rb.outlier) true;
  near rb.d_cover 0.15;
  (* Unrecovered deepest event: no recovered anchor - outlier policy. *)
  let c = mk [ "d1", 100.0, 100.0; "d2", 40.0, 40.0; "d3", 60.0, 60.0 ] in
  let rc = Option.get (Oracle_math.sizing_reference_of ~fallback:false c) in
  Alcotest.(check bool) "outlier" rc.outlier true;
  Alcotest.(check bool) "outlier: no floor anchor" (rc.floor_ref = None) true;
  near rc.d_cover 0.15;
  (* An unrecovered deepest event WITH measured floor-break history: the
     measured overshoot funds it (not the fallback). *)
  let e =
    mk
      [ "d1", 100.0, 100.0
      ; "d2", 80.0, 80.0
      ; "d3", 85.0, 85.0
      ; "d4", 75.0, 75.0
      ; "d5", 100.0, 100.0
      ; "d6", 40.0, 40.0
      ]
  in
  let re = Option.get (Oracle_math.sizing_reference_of ~fallback:false e) in
  Alcotest.(check bool) "unrecovered with measured overshoot" re.outlier true;
  near re.d_cover 0.0625;
  (match re.overshoot_p90 with
   | Some o -> near o 0.0625
   | None -> Alcotest.fail "expected the measured overshoot");
  (* Fallback (immature history) keeps the raw event drawdown regardless of
     where the price sits - the discount is a matured-regime feature. *)
  let rfa = Option.get (Oracle_math.sizing_reference_of ~fallback:true a) in
  near rfa.d_cover 0.6;
  Alcotest.(check bool) "fallback: raw, no floor context" (rfa.floor_ref = None) true
;;

(** A series whose deepest crash fully recovered (a later close exceeded the
    peak) and that ends BELOW its ATH: the authoritative ATH-scaled remainder
    regime. Crash 100 -> ~29 (30 bars x -4%), recovery +1%/bar past the peak
    (~102), then a slow -0.15%/day decline to ~78. *)
let recov_below_ath_series () =
  let iso day = Oracle_calendar.add_days "2020-01-01" day in
  let n = 330 in
  let bars =
    Array.make
      n
      Oracle_types.{ date = ""; open_ = 0.; high = 0.; low = 0.; close = 0.; volume = 0. }
  in
  let price = ref 100.0 in
  for i = 0 to n - 1 do
    let phase = if i < 30 then 0.96 else if i < 155 then 1.01 else 0.9985 in
    price := !price *. phase;
    let p = !price in
    bars.(i)
    <- { Oracle_types.date = iso i
       ; open_ = p
       ; high = p *. 1.002
       ; low = p *. 0.998
       ; close = p
       ; volume = 1000.0
       }
  done;
  { Oracle_types.symbol = "RECOV"; calendar_kind = Oracle_types.Crypto; bars; gaps = [] }
;;

let test_deploy_sizing_reference () =
  (* A recovered crash with the price below the ATH: the authoritative
     deployment funds the ATH-scaled remainder, not the full event. *)
  let a = recov_below_ath_series () in
  let g = grid ~start_price:100.0 () in
  let ms = models ~asset:a in
  let d = deploy ~asset:a ~pool:100_000.0 ~g ~fng:None ~use_fng:false ~models:ms () in
  let p = Option.get (Oracle_math.peak_to_valley_stats_of a) in
  match d.Oracle_types.sizing with
  | Some r ->
    Alcotest.(check bool) "recovered anchor" (not r.Oracle_types.outlier) true;
    Alcotest.(check bool) "not at the floor" (not r.Oracle_types.at_floor) true;
    Alcotest.(check bool)
      "remainder below the full event drawdown"
      (d.Oracle_types.d_cover < p.Oracle_types.max_drawdown -. 0.01)
      true;
    (match r.Oracle_types.floor_ref with
     | Some f ->
       Alcotest.(check bool)
         "floor between the valley and the peak"
         (f > p.Oracle_types.valley && f < p.Oracle_types.peak)
         true
     | None -> Alcotest.fail "expected floor_ref")
  | None -> Alcotest.fail "expected an ATH-scaled sizing reference"
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
    ; ( "peak_to_valley"
      , [ Alcotest.test_case "actual max drawdown" `Quick test_peak_to_valley_stats
        ; Alcotest.test_case
            "1000x run-up reports the real crash"
            `Quick
            test_peak_to_valley_1000x_runup
        ; Alcotest.test_case
            "order-independent (newest-first feeds)"
            `Quick
            test_peak_to_valley_ordering
        ; Alcotest.test_case "recovered flag" `Quick test_p2v_recovered_flag
        ] )
    ; ( "sizing reference"
      , [ Alcotest.test_case "floor overshoot measurement" `Quick test_floor_overshoot_p90
        ; Alcotest.test_case
            "ATH-scaled regimes (remainder / at-floor / outlier / fallback)"
            `Quick
            test_sizing_reference
        ; Alcotest.test_case
            "deploy funds the ATH-scaled remainder"
            `Quick
            test_deploy_sizing_reference
        ] )
    ; ( "deploy_asset"
      , [ Alcotest.test_case
            "fully funded (coverage mode, min qty)"
            `Quick
            test_deploy_fully_funded
        ; Alcotest.test_case
            "gi selection: tightest with 100% survival, else stretch"
            `Quick
            test_deploy_gi_selection
        ; Alcotest.test_case "under-funded priority" `Quick test_deploy_under_funded
        ; Alcotest.test_case "qty cap passes excess down" `Quick test_deploy_qty_cap
        ; Alcotest.test_case
            "replay funded by the pool budget"
            `Quick
            test_deploy_replay_pool_funded
        ; Alcotest.test_case
            "qty scale-up bounded by 100% survival and the cap"
            `Quick
            test_deploy_qty_survival_scale
        ; Alcotest.test_case
            "equity ignores F&G entirely"
            `Quick
            test_deploy_equity_ignores_fng
        ; Alcotest.test_case
            "valley still funds the actual drawdown"
            `Quick
            test_deploy_valley_still_funds_drawdown
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
        ; Alcotest.test_case
            "committed buy keeps a running grid alive"
            `Quick
            test_deploy_committed_buy_gate
        ; Alcotest.test_case
            "committed buy passes the pool down while under-funded"
            `Quick
            test_deploy_committed_buy_passes_pool_down
        ; Alcotest.test_case
            "seeded replay models the accumulated state"
            `Quick
            test_deploy_seeded_replay
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
