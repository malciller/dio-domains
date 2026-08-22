(* Oracle_deploy - the capital deployment engine (strategy-generic).

   This is the core of the oracle tool. Given a venue-locked capital pool,
   the blend models of an asset, and the asset's strategy template (parameter
   range [lo, hi], qty gates, fees), it decides:

     - the tuned parameter (grid: the grid interval gi),
     - the position size qty,
     - whether the asset should stay active at all.

   The sizing drawdown is the largest drawdown the asset has ACTUALLY
   experienced, measured peak to valley over its whole (deepened) history
   (see Oracle_math.peak_to_valley_stats_of): the grid must fund the worst
   peak-to-valley fall that really took place, from wherever the price sits
   today. A 1000x run-up only registers the falls that actually happened -
   never the ATH-to-ATL span, which would read as a phantom 99.9% drawdown.

   Sizing rules (the deployment contract):

     - The grid interval and the order qty each use their FULL config ranges
       for modeling AND placement: gi in [lo, hi], qty in
       [qty_min, qty_min * qty_cap_mult] (the cap is a ceiling, not a rule:
       qty_cap_mult <= 0 means the qty never grows beyond the minimum).
     - Goal: 100% survivability first. The most aggressive (tightest) grid
       interval in [lo, hi] that reaches 100% replay survival (the replayed
       path never runs dry: D_surv = 1.0) at the minimum order size is
       chosen; the order qty then grows - only while 100% survival holds -
       to deploy the pool: the largest qty in [qty_min, qty_min * qty_cap]
       that still survives the whole replayed history. Whatever a deployment
       does not consume passes down the account's priority order.
     - If NO grid interval in [lo, hi] reaches 100% survival (the pool cannot
       survive the whole history even at the minimum order size), the
       deployment stretches: the minimum qty at the grid interval MAXIMUM -
       the widest spacing the config allows stretches the capital's survival
       as far as possible. The order qty is never increased in this mode:
       growth is reserved for deploying residual capital behind 100% survival.
      - An asset whose pool cannot fund its first buy at the minimum order
        size is inactive and its whole share passes down the priority order.
      - EXCEPTION: a grid with a committed resting buy (its first buy is
        already funded and locked in the account balance) keeps running on
        that committed capital and, while the pool cannot fund its FULL
        ladder, draws nothing new from the available pool - its whole share
        passes down so a lower-priority asset can still fund its own first
        order. Only a fully-funded committed grid consumes its ladder cost
        from the pool (and passes the surplus down).

   The verification replay is funded with the asset's actual pool budget
   (the capital the allocation hands it), not the static ladder cost: the
   honest question is "can the strategy survive this history with the capital
   it is entitled to?" Sizing the static runway cost instead systematically
   under-states the burn of a long replayed path (the ladder buys far more
   levels over years than the d_cover fill count) and falsely brands
   well-funded assets as under-funded.

   Resolution order:
     1. governing drawdown from the blend models (None -> inactive),
     2. sizing drawdown d_cover: mature assets fund the ATH-scaled remaining
        drop to the expected floor (see Oracle_math.sizing_reference_of) -
        the fall still ahead from the current price, capped by the worst-ever
        drop and floored by the measured floor overshoot; immature fallback
        assets keep the raw largest ACTUAL peak-to-valley drawdown (no ATH
        anchoring: a 1000x run-up must not turn into a phantom 99.9%
        drawdown; only falls that really happened count),
     3. gi search over the full [lo, hi]: the tightest parameter reaching
        100% replay survival at the minimum order size; when no parameter can
        (the pool cannot survive the whole history), stretch: gi = hi,
     4. qty: the minimum order size in stretch mode; otherwise the largest
        qty in [qty_min, qty_min * qty_cap_mult] that keeps 100% replay
        survival (deploying the pool, capped by the qty ceiling),
     5. final row at the resolved (gi, qty) - the verification replay reports
        the honest D_surv / coverage / deployed for the sizing that runs.

   The F&G / range sentiment blend is GONE from the sizing: the grid interval
   is survival-driven over the config range, and the qty follows the
   "100% first, then deploy residual capital" rule. The F&G value and the
   weights are still carried in [parameter_components] for the record, but
   they never influence the resolved sizing.

   Everything here is pure (no IO) and strategy-generic: instantiate
   [Engine] with an Oracle_strategy.S model (Oracle_strategy.Grid today) and
   the deployment, allocation and reserve semantics are shared as-is. The CLI
   resolves pools, F&G and series. *)

open Oracle_types

let section = "oracle_deploy"

(** F&G-resolved parameter on the config range: lo + (hi-lo)*fng/100. Fear
    (low fng) tightens the parameter (for the grid: densifies levels and
    accumulates base at depressed prices) - the contrarian convention the live
    jacobs_ladder uses. *)
let fng_parameter ~(lo : float) ~(hi : float) ~(fng : float) =
  Cmc.Fear_and_greed.grid_value_for_fng ~grid_interval:(lo, hi) ~fear_and_greed:fng
;;

(** Per-horizon target drawdown: the smallest d with blended coverage >=
    target. [None] when the horizon cannot clear the target - either the
    distribution is empty (history too short for warmup + horizon) or the
    target sits in a coverage gap the blended history cannot reach with
    certainty (surviving the whole history is not achievable). *)
let horizon_target_drawdown (m : Oracle_replay.blend_model) ~(target_survival : float)
  : float option
  =
  try
    let d = Oracle_replay.drawdown_for_target ~model:m ~target_survival in
    let coverage = (Oracle_replay.blended_coverage m ~d_surv:d).blended in
    if coverage +. 1e-12 >= target_survival then Some d else None
  with
  | _ -> None
;;

(** Governing drawdown across the horizons: the deepest reachable d* (the
    binding horizon). Surviving it clears the target on every reachable
    horizon. [None] when no horizon can clear the target. *)
let governing_drawdown
      ~(models : Oracle_replay.blend_model list)
      ~(target_survival : float)
  : (float * string) option
  =
  let best = ref None in
  List.iter
    (fun (m : Oracle_replay.blend_model) ->
       match horizon_target_drawdown m ~target_survival with
       | Some d ->
         (match !best with
          | Some (bd, _) when bd >= d -> ()
          | _ -> best := Some (d, m.horizon.label))
       | None -> ())
    models;
  !best
;;

(** Deepest observed asset MFD across the models that have windows: the raw
    worst drawdown the asset's own history has actually produced on any
    horizon. This is the fallback sizing basis for immature assets whose
    blended history cannot clear the target survival - a new asset gets sized
    to survive what it has actually done ("raw"), with the not-authoritative
    caveat carried in the deployment warnings. *)
let deepest_observed_drawdown (models : Oracle_replay.blend_model list)
  : (float * string) option
  =
  let best = ref None in
  List.iter
    (fun (m : Oracle_replay.blend_model) ->
       let n = Array.length m.index.mfd_sorted in
       if n > 0
       then (
         let d = m.index.mfd_sorted.(n - 1) in
         match !best with
         | Some (bd, _) when bd >= d -> ()
         | _ -> best := Some (d, m.horizon.label)))
    models;
  !best
;;

(** The sizing basis, shared by [deploy_asset] and the runtime's allocation
    layer so both always agree: the target-clearing governing drawdown when
    any horizon can reach it, otherwise (immature asset / coverage gap on
    every horizon) the deepest drawdown the asset's own history has actually
    observed - the "raw" fallback, carrying a not-authoritative caveat.
    Returns [(d_gov, governing_horizon, is_fallback)]; [None] only when no
    model has a single MFD window (history shorter than warmup + horizon + 2
    on every horizon), i.e. nothing can be computed. *)
let governing_basis ~(models : Oracle_replay.blend_model list) ~(target_survival : float)
  : (float * string * bool) option
  =
  match governing_drawdown ~models ~target_survival with
  | Some (d, h) -> Some (d, h, false)
  | None ->
    (match deepest_observed_drawdown models with
     | Some (d, h) -> Some (d, h, true)
     | None -> None)
;;

(* The drawdown references ([range_stats_of], [peak_to_valley_stats_of])
   live in Oracle_math (dependency-free) so the sizing inversions in
   Oracle_replay can share the same actual peak-to-valley reference without a
   module cycle. *)

(** The range side of the parameter blend: [lo + (1 - position) * (hi - lo)]
    with position = (peak - price) / (peak - valley) clamped to [0, 1],
    where (peak, valley) is the largest ACTUAL peak-to-valley drawdown event
    of the asset's history. Above the event peak (position ~ 0) a fall of the
    full max drawdown is still possible, so spacing widens toward hi
    (preserve runway); at the event valley (position ~ 1) the remaining
    downside is bounded by what actually happened, so spacing tightens toward
    lo - an aggressive accumulator zone that works with the F&G contrarian
    convention. Anchoring on the real event instead of the ATH/ATL span means
    a 1000x run-up never distorts the side into a phantom range. [None] when
    the asset never actually drew down (strictly monotone history). *)
let range_parameter ~(lo : float) ~(hi : float) (p : p2v_stats) : float option =
  let span = p.peak -. p.valley in
  if span <= 0.0
  then None
  else (
    let position = Float.max 0.0 (Float.min 1.0 ((p.peak -. p.price) /. span)) in
    Some (lo +. ((1.0 -. position) *. (hi -. lo))))
;;

(** The deployment engine, parameterized by a strategy model [M] (see
    Oracle_strategy.S). All deployment math goes through the model's funding
    function, replay and qty floors. *)
module Engine (M : Oracle_strategy.S) = struct
  (** The sizing floor: the venue's minimum order qty OR the configured design
      qty, whichever is larger. A venue qty_min is a lot precision (Alpaca
      reports 1e-9 for fractional shares), not a minimum order size: sizing an
      order to it produces a sub-minimum buy whose cost basis the venue rejects
      ("cost basis must be >= minimal amount"). The floor therefore never drops
      below the config qty, and an asset that cannot fund even that goes
      inactive instead of emitting an un-placeable order. *)
  let sizing_floor ~(cfg : M.config) = Float.max (M.min_qty cfg) (M.design_qty cfg)

  (** Largest lot-rounded qty (>= sizing_floor) whose floor-aware cost through
      the target drawdown fits the pool. The cost is monotone non-decreasing
      in qty (the floor up-size term max(qty, ceil_lot(min_notional/level))
      is), so a bisection is sound after an exponential upper-bound search.
      Returns the sizing floor when the pool cannot fund the full runway at
      it. *)
  let qty_for_pool ~(cfg : M.config) ~(n_fills : int) ~(pool : float) =
    let q_min = sizing_floor ~cfg in
    if pool <= 0.0
    then q_min
    else (
      let rec grow q =
        if M.cost_at cfg ~qty:q ~n_fills >= pool then q else grow (q *. 2.0)
      in
      let q_hi = grow (Float.max q_min 1e-9) in
      let rec bisect lo hi i =
        if i = 0
        then hi
        else (
          let mid = (lo +. hi) /. 2.0 in
          if M.cost_at cfg ~qty:mid ~n_fills <= pool
          then bisect mid hi (i - 1)
          else bisect lo mid (i - 1))
      in
      let q = bisect q_min q_hi 60 in
      M.round_qty cfg q)
  ;;

  (** Replay the strategy at a qty and report per-horizon blended coverage at
      the replayed D_surv. The replay is funded with the asset's actual pool
      budget - the capital the allocation hands this asset - so the survival
      verdict answers the honest question: "can the strategy survive this
      history with the capital it is entitled to?" (The static ladder cost
      through d_cover systematically under-states the burn of a long replayed
      path - the ladder buys far more levels over years than the d_cover fill
      count - and would falsely brand well-funded assets as under-funded.) *)
  let verify_at_qty
        ~(seed : Dio_strategies.Grid_core_types.seed option)
        ~(cfg : M.config)
        ~(pool : float)
        ~(asset : series)
        ~(models : Oracle_replay.blend_model list)
        ~(qty : float)
    =
    let cfg = M.set_qty (M.set_start_quote cfg pool) qty in
    let out = M.replay ?seed cfg asset in
    let coverage =
      List.map
        (fun (m : Oracle_replay.blend_model) ->
           { horizon_label = m.horizon.label
           ; blended_coverage =
               (Oracle_replay.blended_coverage m ~d_surv:out.d_surv).blended
           })
        models
    in
    cfg, out, coverage
  ;;

  (** The 100% survivability criterion: the replayed path never ran dry
      (D_surv = 1.0). This is the sizing goal - "first get 100%
      survivability" - and the bound on the qty scale-up: the order qty only
      grows while the whole replayed history still survives. *)
  let survives_all (out : M.outcome) = out.M.d_surv >= 1.0 -. 1e-9

  (** Largest qty in [q_lo, q_hi] whose replayed path still survives the
      WHOLE history (D_surv = 1.0), scanning a log-spaced qty grid and
      bisection-refining the boundary cell; a second pass above the first hit
      bounds non-monotone islands. [q_lo] is the sizing floor and is expected
      to survive (the gi search guarantees it); if even it does not, it is
      returned anyway and the caller reports the shortfall.

      The replay is funded with the asset's pool budget, so the survival
      verdict already encodes the pool: a qty too large for the pool exhausts
      it and fails the criterion, which is what bounds the "deploy all
      capital" scale-up. *)
  let max_qty_for_survival
        ~(seed : Dio_strategies.Grid_core_types.seed option)
        ~(cfg : M.config)
        ~(pool : float)
        ~(asset : series)
        ~(models : Oracle_replay.blend_model list)
        ~(q_lo : float)
        ~(q_hi : float)
        ~(scan_points : int)
    : float
    =
    let passes qty =
      let _, out, _ = verify_at_qty ~seed ~cfg ~pool ~asset ~models ~qty in
      survives_all out
    in
    if q_hi <= q_lo
    then q_lo
    else if passes q_hi
    then q_hi
    else (
      let pts ~lo ~hi =
        Array.init scan_points (fun i ->
          lo *. ((hi /. lo) ** (float_of_int i /. float_of_int (scan_points - 1))))
      in
      (* First failing qty scanning from q_lo upward (passing is monotone in
         the typical case: a larger qty burns deeper per dollar). *)
      let first_failing (arr : float array) =
        let rec go i =
          if i >= Array.length arr
          then None
          else if passes arr.(i)
          then go (i + 1)
          else Some i
        in
        go 0
      in
      let arr = pts ~lo:q_lo ~hi:q_hi in
      match first_failing arr with
      | None -> q_lo (* unreachable: q_hi passes but the scan missed it *)
      | Some idx ->
        let lo_cap = if idx = 0 then q_lo else arr.(idx - 1) in
        let hi_cap = arr.(idx) in
        (* Largest passing qty in [lo_cap, hi_cap]: [lo_cap] passes and
           [hi_cap] fails, so the bracket keeps [lo] = passing on every
           halving and the final [lo] is the largest qty that clears. *)
        let rec bisect lo hi i =
          if i = 0
          then lo
          else (
            let mid = (lo +. hi) /. 2.0 in
            if passes mid then bisect mid hi (i - 1) else bisect lo mid (i - 1))
        in
        let refined = bisect lo_cap hi_cap 40 in
        (* Second pass above the first hit, at the same resolution, to bound
           non-monotone islands (a larger qty that also clears). *)
        let upper = ref refined in
        let rec rescan i =
          if i < Array.length arr
          then (
            if i > idx && passes arr.(i) then upper := Float.max !upper arr.(i);
            rescan (i + 1))
        in
        rescan idx;
        !upper)
  ;;

  (** The deployment row at an explicit (parameter, qty): verifies the sizing
      on the replayed path (funded with the pool) and reports the static
      ladder cost through [d_cover]. [passed] = the deployment is fully
      funded: either the path survived the whole history (100% survival) or
      the pool funds the whole static runway at the minimum order size.
      [deployed] is the floor-aware cost through [d_cover] at the final qty,
      capped at the pool. *)
  let row_at
        ~(seed : Dio_strategies.Grid_core_types.seed option)
        ~(asset : series)
        ~(cfg : M.config)
        ~(models : Oracle_replay.blend_model list)
        ~(pool : float)
        ~(d_cover : float)
        ~(parameter : float)
        ~(qty : float)
    : deployment_row
    =
    let cfg = M.set_parameter cfg parameter in
    let n_fills = M.fills_for_drawdown cfg ~d:d_cover in
    let q_min = sizing_floor ~cfg in
    let d_surv_static = M.drawdown_of_fills cfg ~n_fills in
    let static_funded = M.cost_at cfg ~qty:q_min ~n_fills <= pool +. 1e-9 in
    let _, out, coverage = verify_at_qty ~seed ~cfg ~pool ~asset ~models ~qty in
    let deployed = Float.min pool (M.cost_at cfg ~qty ~n_fills) in
    { parameter
    ; qty
    ; deployed
    ; d_surv_static
    ; d_surv_replay = out.M.d_surv
    ; min_quote_drawdown = out.M.min_quote_drawdown
    ; coverage
    ; static_funded
    ; passed = survives_all out || static_funded
    ; profit_proxy = M.profit_proxy cfg ~qty ~deployed
    }
  ;;

  (** The full deployment for one asset against its venue pool share. Pure:
      [asset], [models], [cfg] and the pool are resolved by the caller.

       Resolution order:
        1. governing drawdown from the blend models (None -> inactive),
        2. sizing drawdown d_cover: the largest ACTUAL peak-to-valley
          drawdown of the asset's history - the fall from the current price
          the grid must fund. No ATH anchoring: a 1000x run-up only
          registers the falls that actually happened, never the ATH-to-ATL
          span (a phantom 99.9% drawdown),
        3. gi search over the full [lo, hi]: the tightest parameter that
          reaches 100% replay survival at the minimum order size (the
          "most aggressive grid_interval(min,max)"); when no parameter can
          (the pool cannot survive the whole replayed history even at the
          minimum qty), the sizing stretches: gi = hi,
        4. qty: the minimum order size in stretch mode; otherwise the
          largest qty in [qty_min, qty_min * qty_cap_mult] that keeps 100%
          replay survival - deploying the pool by adjusting the qty, capped
          by the qty ceiling (qty_cap_mult is the cap, not a rule),
        5. final row at the resolved (gi, qty): the verification replay
          reports the honest D_surv / coverage / deployed for the sizing
          that actually runs.

       Inactive reasons: no reachable horizon, a pool that cannot fund even the
       first buy at the sizing floor (the venue lot or the config qty,
       whichever is larger - sizing never drops below the configured qty), or a
       replayed D_surv below [min_active_dsurv]. An under-funded ACTIVE asset
       keeps its whole share (config-order priority) and runs at the floor with
       the shortfall flagged in [warnings]. EXCEPTION: an under-funded ACTIVE
       grid that has a committed resting buy keeps running on that committed
       capital and draws NOTHING new from the available pool (deployed = 0,
       remainder = the whole pool): its first buy is already funded, so
       hoarding the pool would starve every lower-priority asset of its own
       first order.

       [qty_cap_mult] is the deployment ceiling as a multiple of the template
       qty (the config's design qty): the order qty never grows beyond
       qty_min * qty_cap_mult, and qty_cap_mult <= 0 means the qty never grows
       beyond the minimum at all. The cap is a ceiling, not a rule: the qty
       only grows to deploy residual capital while 100% survival holds.

       [use_fng] / [fng_weight] / [range_weight] are kept in the signature for
       caller compatibility but are INERT: the sizing is survival-driven and
       no sentiment blend is applied. *)
  let deploy_asset
        ~(seed : Dio_strategies.Grid_core_types.seed option)
        ~(has_committed_buy : bool)
        ~(asset : series)
        ~(cfg : M.config)
        ~(lo : float)
        ~(hi : float)
        ~(models : Oracle_replay.blend_model list)
        ~(target_survival : float)
        ~(pool : float)
        ~(fng : float option)
        ~(fng_weight : float)
        ~(range_weight : float)
        ~(min_active_dsurv : float)
        ~(use_fng : bool)
        ~(param_steps : int)
        ~(scan_points : int)
        ~(qty_cap_mult : float)
    : asset_deployment
    =
    let _ = use_fng in
    let q_min = sizing_floor ~cfg in
    let lo = Float.min lo hi in
    let hi = Float.max lo hi in
    (* Per-asset historical price-range reference (ATH -> all-time low,
       display context) and the largest ACTUAL peak-to-valley drawdown event
       (the sizing drawdown). The range side of the parameter blend uses the
       p2v event: where the price sits within the worst drawdown's
       [peak, valley] band. Above the event peak the full max drawdown is
       still ahead, so spacing widens (preserve runway); at the event valley
       the downside is bounded by what actually happened, so spacing tightens
       - an aggressive accumulator working with the F&G contrarian signal.
       A 1000x run-up only registers the falls that actually happened, never
       the ATH-to-ATL span (which would read as a phantom 99.9% drawdown). *)
    let range = Oracle_math.range_stats_of asset in
    let p2v = Oracle_math.peak_to_valley_stats_of asset in
    let range_parameter =
      match p2v with
      | Some p -> range_parameter ~lo ~hi p
      | None -> None
    in
    let empty_row parameter =
      { parameter
      ; qty = 0.0
      ; deployed = 0.0
      ; d_surv_static = 0.0
      ; d_surv_replay = 0.0
      ; min_quote_drawdown = 0.0
      ; coverage = []
      ; static_funded = false
      ; passed = false
      ; profit_proxy = 0.0
      }
    in
    let inactive reason =
      { active = false
      ; reason
      ; pool_share = pool
      ; deployed = 0.0
      ; remainder = pool
      ; governing_horizon = ""
      ; d_gov = 0.0
      ; d_cover = 0.0
      ; sizing = None
      ; parameter_components =
          { fng
          ; fng_parameter = None
          ; survival_parameter = hi
          ; resolved_parameter = hi
          ; fng_weight
          ; range_parameter
          ; range_weight
          }
      ; gi_reason = ""
      ; qty_reason = ""
      ; qty = 0.0
      ; parameter = hi
      ; d_surv = 0.0
      ; min_quote_drawdown = 0.0
      ; range
      ; p2v
      ; coverage = []
      ; warnings = []
      ; tuning_rows = []
      ; row = empty_row hi
      }
    in
    (* The sizing basis, via the shared [governing_basis] so the runtime's
       allocation layer and this sizing always agree: the target-clearing
       governing drawdown when any horizon can reach it, otherwise (immature
       assets / coverage gap on every horizon) the deepest drawdown the
       asset's own history has actually observed - the "raw" sizing, carrying
       a not-authoritative caveat. Only when NO model has a single MFD window
       (history shorter than warmup + horizon + 2 on every horizon) does the
       asset become inactive: nothing can be computed. *)
    let basis = governing_basis ~models ~target_survival in
    match basis with
    | None ->
      inactive
        "no usable history: no MFD windows on any horizon (each horizon needs warmup + \
         horizon + 2 bars)"
    | Some (d_gov, governing_horizon, fallback) ->
      (* Sizing drawdown: mature (authoritative) assets fund the ATH-scaled
         remaining drop to the expected floor - the worst-ever drawdown
         applied to the current regime's top, so an asset below its ATH only
         funds the fall that is still ahead (see
         Oracle_math.sizing_reference_of). Immature fallback assets keep the
         raw largest ACTUAL peak-to-valley drawdown (the discount is a
         matured-regime feature; a floor from a thin history is not a
         meaningful support). No ATH-to-ATL anchoring: a 1000x run-up must
         not inflate the sizing into a phantom 99.9% drawdown; only falls
         that really took place count. Falls back to the statistical
         governing drawdown when the series never drew down (strictly
         monotone history). *)
      let sizing_ref = Oracle_math.sizing_reference_of ~fallback asset in
      let d_cover =
        match sizing_ref with
        | Some r -> r.d_cover
        | None -> d_gov
      in
      let dropped_horizons =
        if fallback
        then []
        else
          List.filter
            (fun (m : Oracle_replay.blend_model) ->
               Option.is_none (horizon_target_drawdown m ~target_survival))
            models
      in
      let cost_one = M.cost_at (M.set_parameter cfg hi) ~qty:q_min ~n_fills:1 in
      (* The first-buy gate: an asset whose pool cannot fund its first buy at
         the minimum order size is inactive and its whole share passes down
         the priority order. EXCEPTION: a grid with a committed resting buy
         (the first buy is already funded and resting on the exchange - its
         cost is locked in the account balance) is never "cannot fund the
         first buy": the committed grid keeps running, the grid's own
         capital gates pause it when the pool cannot extend another rung. *)
      if pool +. 1e-9 < cost_one && not has_committed_buy
      then
        inactive
          (Printf.sprintf
             "pool %.2f cannot fund the first buy at qty_min (needs %.2f)"
             pool
             cost_one)
      else (
        (* 1. The gi search over the FULL config range at the minimum order
           size: the most aggressive (tightest) parameter in [lo, hi] whose
           deployment reaches 100% replay survival - the replayed path, funded
           with the pool, never runs dry. When NO parameter can (the pool
           cannot survive the whole history even at the minimum qty), the
           sizing stretches: gi = hi, qty = q_min - the widest spacing the
           config allows stretches the capital's survival as far as possible. *)
        let candidates =
          Array.init param_steps (fun i ->
            lo +. ((hi -. lo) *. (float_of_int i /. float_of_int (param_steps - 1))))
        in
        let rows =
          Array.map
            (fun parameter ->
               row_at ~seed ~asset ~cfg ~models ~pool ~d_cover ~parameter ~qty:q_min)
            candidates
          |> Array.to_list
        in
        let gi_100 =
          List.find_opt (fun (r : deployment_row) -> r.d_surv_replay >= 1.0 -. 1e-9) rows
        in
        let parameter, stretch =
          match gi_100 with
          | Some r -> r.parameter, false
          | None -> hi, true
        in
        (* 2. The qty: the minimum in stretch mode - the order size only grows
           to deploy residual capital BEHIND 100% survival, and qty_cap_mult
           is the ceiling, not a rule (qty_cap_mult <= 0 means the qty never
           grows). In coverage mode: the largest qty in
           [q_min, q_min * qty_cap_mult] that keeps 100% replay survival -
           "deploy all capital by adjusting qty", bounded by the survival
           replay (the replay is funded with the pool, so a qty the pool
           cannot carry fails the criterion by itself). *)
        let qty_cap = if qty_cap_mult > 0.0 then q_min *. qty_cap_mult else q_min in
        let qty, qty_reason =
          if stretch
          then
            ( q_min
            , Printf.sprintf
                "minimum qty %.6g (stretch: 100%% survival unreachable)"
                q_min )
          else (
            let cfg_at = M.set_parameter cfg parameter in
            let q =
              max_qty_for_survival
                ~seed
                ~cfg:cfg_at
                ~pool
                ~asset
                ~models
                ~q_lo:q_min
                ~q_hi:qty_cap
                ~scan_points
            in
            if q >= qty_cap -. 1e-12
            then
              ( qty_cap
              , Printf.sprintf
                  "capped at config qty %.6g x qty_cap_mult %.2f"
                  q_min
                  qty_cap_mult )
            else q, Printf.sprintf "largest qty %.6g keeping 100%% survival" q)
        in
        let gi_reason =
          if stretch
          then Printf.sprintf "grid max %.2f%% (100%% survival unreachable at any gi)" hi
          else
            Printf.sprintf
              "tightest gi %.2f%% with 100%% survival at minimum qty"
              parameter
        in
        (* 3. The final row at the resolved (gi, qty): the verification replay
           reports the honest D_surv / coverage / deployed for the sizing that
           actually runs. *)
        let row = row_at ~seed ~asset ~cfg ~models ~pool ~d_cover ~parameter ~qty in
        let warnings = ref [] in
        if stretch
        then
          warnings
          := Printf.sprintf
               "100%% survival unreachable at any grid interval in [%.2f%%, %.2f%%] \
                (best D_surv %.1f%% at minimum order size); stretching at grid interval \
                max %.2f%% with minimum qty %.6g - the deepest coverage this capital \
                allows; increase the pool for more"
               lo
               hi
               (row.d_surv_replay *. 100.0)
               hi
               q_min
             :: !warnings;
        if fallback
        then
          warnings
          := Printf.sprintf
               "immature history: the blended model cannot clear the %.1f%% target \
                survival (thin or gapped coverage); sized to the deepest observed \
                drawdown %.1f%% on %s - raw, not authoritative - tighten \
                --target-survival as history grows"
               (target_survival *. 100.0)
               (d_gov *. 100.0)
               governing_horizon
             :: !warnings;
        (match dropped_horizons with
         | [] -> ()
         | dropped ->
           warnings
           := Printf.sprintf
                "horizon(s) %s cannot clear the target (coverage gap); sized on the \
                 reachable horizons only"
                (String.concat
                   ", "
                   (List.map
                      (fun (m : Oracle_replay.blend_model) -> m.horizon.label)
                      dropped))
              :: !warnings);
        if (not row.passed) && not stretch
        then
          warnings
          := Printf.sprintf
               "under-funded: pool share $%.2f can't fund the %.1f%% drawdown - needs \
                $%.2f at the minimum order size (grid %.2f%%); increase the pool or \
                loosen the grid_interval config"
               pool
               (d_cover *. 100.0)
               (M.cost_at
                  (M.set_parameter cfg parameter)
                  ~qty:q_min
                  ~n_fills:
                    (M.fills_for_drawdown (M.set_parameter cfg parameter) ~d:d_cover))
               parameter
             :: !warnings;
        (* At/below the scaled floor, or no recovered anchor: the price
           position cannot fund the fall (the remainder is exhausted / the
           deepest event is still in progress) - the measured floor overshoot
           funds the asset instead. *)
        let p2v_dd =
          match p2v with
          | Some p -> p.max_drawdown
          | None -> d_gov
        in
        let p2v_price =
          match p2v with
          | Some p -> p.price
          | None -> 0.0
        in
        let p2v_ath =
          match range with
          | Some r -> r.ath
          | None ->
            (match p2v with
             | Some p -> p.peak
             | None -> 0.0)
        in
        let p2v_dates () =
          match p2v with
          | Some p -> Printf.sprintf "%s->%s" p.peak_date p.valley_date
          | None -> "-"
        in
        let overshoot_tail (r : Oracle_types.sizing_reference) =
          if Option.is_none r.overshoot_p90
          then " (no floor-break history: 15% fallback)"
          else ""
        in
        (match sizing_ref with
         | Some r when r.outlier ->
           warnings
           := Printf.sprintf
                "deepest drawdown (%.1f%% on %s) has not recovered - still living in it, \
                 no recovered anchor; funding the measured 90th-pct floor overshoot \
                 %.1f%%%s"
                (p2v_dd *. 100.0)
                (p2v_dates ())
                (r.d_cover *. 100.0)
                (overshoot_tail r)
              :: !warnings
         | Some r when r.at_floor ->
           (match r.floor_ref with
            | Some floor_ref ->
              warnings
              := Printf.sprintf
                   "price $%.2f at/below the ATH-scaled floor $%.2f (ATH $%.2f - %.1f%% \
                    worst): the remaining drop is exhausted - funding the measured \
                    90th-pct floor overshoot %.1f%%%s"
                   p2v_price
                   floor_ref
                   p2v_ath
                   (p2v_dd *. 100.0)
                   (r.d_cover *. 100.0)
                   (overshoot_tail r)
                 :: !warnings
            | None -> ())
         | _ -> ());
        let active, reason =
          if row.d_surv_replay +. 1e-9 < min_active_dsurv
          then
            ( false
            , Printf.sprintf
                "replayed D_surv %.1f%% below --min-active-dsurv %.1f%%"
                (row.d_surv_replay *. 100.0)
                (min_active_dsurv *. 100.0) )
          else true, ""
        in
        (* A grid with a committed resting buy is already running on committed
           capital: its first buy is funded and resting on the exchange, and
           that cost is locked in the account balance - which is exactly why
           the available pool reads low. While the pool cannot fund the grid's
           full ladder (under-funded: [not row.passed]) the committed grid
           draws NOTHING new from the available pool: the committed capital
           keeps it running, so hoarding the whole pool (deployed = pool,
           remainder = 0) would starve every lower-priority asset of its own
           first order. Only a fully-funded committed grid consumes its ladder
           cost from the pool and passes the surplus down; an under-funded
           grid WITHOUT a committed buy keeps its whole share as before. *)
        let committed_running = has_committed_buy && not row.passed in
        let deployed = if active && not committed_running then row.deployed else 0.0 in
        let remainder =
          if not active
          then pool
          else if committed_running
          then pool
          else if row.deployed +. 1e-9 < pool
          then pool -. row.deployed
          else 0.0
        in
        { active
        ; reason
        ; pool_share = pool
        ; deployed
        ; remainder
        ; governing_horizon
        ; d_gov
        ; d_cover
        ; sizing = sizing_ref
        ; parameter_components =
            { fng
            ; fng_parameter = None
            ; survival_parameter = parameter
            ; resolved_parameter = parameter
            ; fng_weight
            ; range_parameter
            ; range_weight
            }
        ; gi_reason
        ; qty_reason
        ; qty = row.qty
        ; parameter
        ; d_surv = row.d_surv_replay
        ; min_quote_drawdown = row.min_quote_drawdown
        ; range
        ; p2v
        ; coverage = row.coverage
        ; warnings = List.rev !warnings
        ; tuning_rows = rows
        ; row
        })
  ;;
end
