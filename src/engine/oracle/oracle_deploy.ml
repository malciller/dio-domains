(* Oracle_deploy - the capital deployment engine (strategy-generic).

   This is the core of the oracle tool. Given a venue-locked capital pool,
   the blend models of an asset, and the asset's strategy template (parameter
   range [lo, hi], qty gates, fees), it decides:

     - the tuned parameter (grid: the grid interval gi): a weighted blend of
       the Fear & Greed contrarian signal (crypto only) and the oracle's
       capital-constrained tightness,
     - the position size qty: the largest lot-rounded quantity whose floor-
       aware cost through the governing drawdown d_gov fits the pool
       (deploy maximum capital, reserved for drawdowns),
     - whether the asset should stay active at all.

   The governing drawdown is d_gov = max_h d*_h over the reachable horizons,
   where d*_h is the smallest drawdown with F_blend_h(d) >= target: surviving
   d_gov clears the target survival on every horizon at once (F is monotone in
   d). The qty inversion uses the floor-aware runway cost (walking the ladder
   with the strategy model's dynamic buy up-sizing) because a binding
   min_notional makes the closed-form geometric sum understate the true
   capital burn.

   The survival side of the parameter blend is survival_parameter: the
   tightest parameter in the config range whose deployment clears the target
   survival on the ACTUAL replayed path (the strategy model's replay at the
   pool). Tighter parameters burn more capital per point of drawdown, so this
   is "how tight can the available capital and the asset's historical
   volatility afford to go". Path replay D_surv is not monotone in qty
   (intermediate sells shift which rung exhausts the grid), so the qty
   down-sizing inside the verification loop scans a log-spaced qty grid and
   bisection-refines the boundary cell, mirroring
   Oracle_replay.Sizing.empirical_min_capital.

   In fallback mode (immature history: no horizon can clear the target
   survival) survival_parameter is the tightest parameter whose static ladder
   cost through the deepest drawdown the history has actually observed fits
   the pool. The observed max drawdown is a real signal even when the
   coverage curve is not authoritative, and the replay D_surv is not a usable
   tuning signal on such a short history (the strategy can never exhaust on
   it), so the static funding check drives the grid instead of collapsing
   onto the tightest config value.

   Everything here is pure (no IO) and strategy-generic: instantiate
   [Engine] with an Oracle_strategy.S model (Oracle_strategy.Grid today) and
   the deployment, allocation and reserve semantics are shared as-is. The CLI
   resolves pools, F&G and series. *)

open Oracle_types

let section = "oracle_deploy"

(** F&G-resolved parameter on the config range: lo + (hi-lo)*fng/100. Fear
    (low fng) tightens the parameter (for the grid: densifies levels and
    accumulates base at depressed prices) - the contrarian convention the live
    suicide_grid uses. *)
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
      the replayed D_surv. The replay is funded with the capital the sizing
      actually commits - the cost at the tested qty, capped at the pool - so
      the verification is not made artificially lenient by pool capital that
      the greedy allocation passes down to the next asset. *)
  let verify_at_qty
        ~(cfg : M.config)
        ~(pool : float)
        ~(n_fills : int)
        ~(asset : series)
        ~(models : Oracle_replay.blend_model list)
        ~(qty : float)
    =
    let funding = Float.min pool (M.cost_at cfg ~qty ~n_fills) in
    let cfg = M.set_qty (M.set_start_quote cfg funding) qty in
    let out = M.replay cfg asset in
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

  (** Largest qty in [q_lo, q_hi] that clears the survival criterion on the
      replayed path, scanning a log-spaced qty grid and bisection-refining the
      boundary cell; a second pass above the first hit bounds non-monotone
      islands (the existing empirical_min_capital pattern). [None] when even
      [q_lo] (min_qty) cannot clear the criterion.

      The criterion is [fallback]-aware: in fallback (immature) mode the
      target survival is unattainable by construction, so the criterion
      becomes the static funding check - the pool can fund the deepest
      drawdown the history has actually observed at this parameter and qty
      (the ladder cost through the observed d_gov fits the pool). The replay
      D_surv is deliberately not used there: on a short, quiet (or trough-
      ending) history the strategy never exhausts on the replayed path
      (d_surv = 1.0), so a replay criterion would pass at every parameter and
      collapse the tuning onto the tightest config value for the wrong reason.
      The static check is a real function of the limited history - its
      observed max drawdown - and the pool; otherwise the criterion is the
      usual per-horizon target coverage. *)
  let shrink_qty
        ~(cfg : M.config)
        ~(pool : float)
        ~(n_fills : int)
        ~(fallback : bool)
        ~(asset : series)
        ~(models : Oracle_replay.blend_model list)
        ~(target_survival : float)
        ~(q_lo : float)
        ~(q_hi : float)
        ~(scan_points : int)
    : (M.config * M.outcome * deployment_coverage list * float) option
    =
    let criterion_met coverage =
      List.for_all
        (fun (c : deployment_coverage) -> c.blended_coverage +. 1e-12 >= target_survival)
        coverage
    in
    let passes qty =
      if fallback
      then M.cost_at cfg ~qty ~n_fills <= pool +. 1e-9
      else (
        let _, _, coverage = verify_at_qty ~cfg ~pool ~n_fills ~asset ~models ~qty in
        criterion_met coverage)
    in
    if q_hi <= q_lo
    then
      if passes q_lo
      then (
        let cfg, out, coverage =
          verify_at_qty ~cfg ~pool ~n_fills ~asset ~models ~qty:q_lo
        in
        Some (cfg, out, coverage, q_lo))
      else None
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
      | None ->
        (* Everything up to q_hi passes. *)
        let cfg, out, coverage =
          verify_at_qty ~cfg ~pool ~n_fills ~asset ~models ~qty:q_hi
        in
        Some (cfg, out, coverage, q_hi)
      | Some idx ->
        let lo_cap = if idx = 0 then q_lo else arr.(idx - 1) in
        let hi_cap = arr.(idx) in
        let rec bisect lo hi i =
          if i = 0
          then hi
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
        let cfg, out, coverage =
          verify_at_qty ~cfg ~pool ~n_fills ~asset ~models ~qty:!upper
        in
        Some (cfg, out, coverage, !upper))
  ;;

  (** The deployment row for one candidate parameter: qty inverted from the
      pool, then down-sized by the verification loop if the replayed path
      cannot clear the target. [deployed] is the floor-aware cost through the
      governing drawdown at the final qty, capped at the pool. A row that does
      not pass has its qty pinned to min_qty (the shrink found nothing that
      clears).

      [qty_cap] is the deployment ceiling (the design qty, i.e. the config qty
      scaled by --qty-cap-mult): a pool larger than the design capital passes
      the excess down the priority order instead of letting the first asset
      absorb the whole venue pool (the greedy allocation needs the ceiling to
      know when an asset is "done"). [None] = uncapped (full deployment). *)
  let row_for_parameter
        ~(asset : series)
        ~(cfg : M.config)
        ~(models : Oracle_replay.blend_model list)
        ~(target_survival : float)
        ~(pool : float)
        ~(d_gov : float)
        ~(fallback : bool)
        ~(parameter : float)
        ~(qty_cap : float option)
        ~(scan_points : int)
    : deployment_row
    =
    let cfg = M.set_parameter cfg parameter in
    let n_fills = M.fills_for_drawdown cfg ~d:d_gov in
    let q_min = sizing_floor ~cfg in
    let qty_full =
      if fallback
      then
        (* Immature history: no survival signal justifies growing the order
           qty beyond the floor. The observed drawdown is funded at the floor
           (larger qtys fund the same drawdown at the same D_surv, so the
           extra capital buys no survival), so the fallback deployment is
           exactly the funding through the observed drawdown at the floor -
           the asset's reservation - and the rest of the pool passes down the
           priority order instead of being absorbed by an asset whose target
           it cannot improve on. *)
        q_min
      else (
        let qty = qty_for_pool ~cfg ~n_fills ~pool in
        match qty_cap with
        | Some cap when cap >= q_min -> Float.min qty cap
        | _ -> qty)
    in
    let d_surv_static = M.drawdown_of_fills cfg ~n_fills in
    let out, coverage, qty =
      match
        shrink_qty
          ~cfg
          ~pool
          ~n_fills
          ~fallback
          ~asset
          ~models
          ~target_survival
          ~q_lo:q_min
          ~q_hi:qty_full
          ~scan_points
      with
      | Some (_, out, coverage, qty) -> out, coverage, qty
      | None ->
        (* min_qty cannot clear the survival criterion (target coverage on the
           replayed path, or the fallback funding check): keep min_qty and
           report the shortfall. *)
        let _, out, coverage =
          verify_at_qty ~cfg ~pool ~n_fills ~asset ~models ~qty:q_min
        in
        out, coverage, q_min
    in
    let deployed = Float.min pool (M.cost_at cfg ~qty ~n_fills) in
    let passed =
      if fallback
      then
        (* The grid at this density can fund the observed drawdown even at
           the sizing floor (the binding qty): any larger qty only costs
           more. *)
        M.cost_at cfg ~qty:q_min ~n_fills <= pool +. 1e-9
      else
        List.for_all
          (fun (c : deployment_coverage) ->
             c.blended_coverage +. 1e-12 >= target_survival)
          coverage
    in
    { parameter
    ; qty
    ; deployed
    ; d_surv_static
    ; d_surv_replay = out.M.d_surv
    ; min_quote_drawdown = out.M.min_quote_drawdown
    ; coverage
    ; passed
    ; profit_proxy = M.profit_proxy cfg ~qty ~deployed
    }
  ;;

  (** The full deployment for one asset against its venue pool share. Pure:
      [asset], [models], [cfg] and the pool are resolved by the caller.

      Resolution order:
       1. governing drawdown from the blend models (None -> inactive),
       2. parameter scan over [lo, hi]: survival_parameter = tightest parameter
          that clears the target on the replayed path (in fallback mode: the
          tightest parameter whose static ladder cost through the observed
          drawdown fits the pool - the replay cannot tune on an immature
          history),
      3. resolved_parameter = fng_weight * fng_parameter + (1 - fng_weight) *
         survival_parameter for crypto (use_fng), survival_parameter alone for
         equities; clamped to the range and never tighter than
         survival_parameter (runway wins over sentiment),
      4. final row at the resolved parameter (verification down-sizes qty if
         needed).

       Inactive reasons: no reachable horizon, a pool that cannot fund even the
       first buy at the sizing floor (the venue lot or the config qty,
       whichever is larger - sizing never drops below the configured qty), or a
       replayed D_surv below [min_active_dsurv]. An under-funded ACTIVE asset
       keeps its whole share (config-order priority) and runs at the floor with
       the shortfall flagged in [warnings].

       In fallback mode (immature history) the order qty is pinned at the
       sizing floor: the observed drawdown is the only signal, it is fully
       funded at the floor, and any larger qty deploys more capital for zero
       additional survival - so the fallback deployment is exactly the asset's
       reservation and the rest of the pool passes down the priority order to
       assets that can still use it to meet the target.

       [qty_cap_mult] is the deployment ceiling as a multiple of the template
       qty (the config's design qty): the default 1.0 caps each asset's
       deployment at its design capital so a surplus passes down the priority
       order instead of letting the highest-priority asset absorb the whole
       venue pool; 0.0 disables the cap (full deployment of whatever pool the
       asset is handed). The ceiling never applies in fallback mode (the floor
       pin is stricter). *)
  let deploy_asset
        ~(asset : series)
        ~(cfg : M.config)
        ~(lo : float)
        ~(hi : float)
        ~(models : Oracle_replay.blend_model list)
        ~(target_survival : float)
        ~(pool : float)
        ~(fng : float option)
        ~(fng_weight : float)
        ~(min_active_dsurv : float)
        ~(use_fng : bool)
        ~(param_steps : int)
        ~(scan_points : int)
        ~(qty_cap_mult : float)
    : asset_deployment
    =
    let qty_cap =
      if qty_cap_mult > 0.0 then Some (M.design_qty cfg *. qty_cap_mult) else None
    in
    let q_min = sizing_floor ~cfg in
    let lo = Float.min lo hi in
    let hi = Float.max lo hi in
    let empty_row parameter =
      { parameter
      ; qty = 0.0
      ; deployed = 0.0
      ; d_surv_static = 0.0
      ; d_surv_replay = 0.0
      ; min_quote_drawdown = 0.0
      ; coverage = []
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
      ; parameter_components =
          { fng
          ; fng_parameter = None
          ; survival_parameter = hi
          ; resolved_parameter = hi
          ; fng_weight
          }
      ; qty = 0.0
      ; parameter = hi
      ; d_surv = 0.0
      ; min_quote_drawdown = 0.0
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
      let evaluable_models =
        if fallback
        then
          List.filter
            (fun (m : Oracle_replay.blend_model) -> Array.length m.index.mfd_sorted > 0)
            models
        else
          List.filter
            (fun (m : Oracle_replay.blend_model) ->
               Option.is_some (horizon_target_drawdown m ~target_survival))
            models
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
      if pool +. 1e-9 < cost_one
      then
        inactive
          (Printf.sprintf
             "pool %.2f cannot fund the first buy at qty_min (needs %.2f)"
             pool
             cost_one)
      else (
        (* 1. parameter scan: tightest parameter clearing the target on the
           replayed path. *)
        let candidates =
          Array.init param_steps (fun i ->
            lo +. ((hi -. lo) *. (float_of_int i /. float_of_int (param_steps - 1))))
        in
        let rows =
          Array.map
            (fun parameter ->
               row_for_parameter
                 ~asset
                 ~cfg
                 ~models:evaluable_models
                 ~target_survival
                 ~pool
                 ~d_gov
                 ~fallback
                 ~parameter
                 ~qty_cap
                 ~scan_points)
            candidates
          |> Array.to_list
        in
        let first_passing = List.find_opt (fun (r : deployment_row) -> r.passed) rows in
        let survival_parameter =
          match first_passing with
          | Some r -> r.parameter
          | None -> hi
        in
        (* 2. resolve the parameter: F&G blend for crypto, pure survival for
           equity. *)
        let fng_parameter_opt =
          if use_fng then Option.map (fun f -> fng_parameter ~lo ~hi ~fng:f) fng else None
        in
        let blended =
          match fng_parameter_opt with
          | Some p -> (fng_weight *. p) +. ((1.0 -. fng_weight) *. survival_parameter)
          | None -> survival_parameter
        in
        let resolved_parameter = Float.min (Float.max blended lo) hi in
        let clamped, warn_clamp =
          if resolved_parameter +. 1e-9 < survival_parameter
          then survival_parameter, true
          else resolved_parameter, false
        in
        (* 3. final row at the resolved parameter; if the blend landed between
           scan points and fails while a scan point passes, fall back to the
           known-passing survival parameter. *)
        let row =
          row_for_parameter
            ~asset
            ~cfg
            ~models:evaluable_models
            ~target_survival
            ~pool
            ~d_gov
            ~fallback
            ~parameter:clamped
            ~qty_cap
            ~scan_points
        in
        let parameter_final =
          if row.passed || first_passing = None then clamped else survival_parameter
        in
        let row =
          if parameter_final = clamped
          then row
          else
            row_for_parameter
              ~asset
              ~cfg
              ~models:evaluable_models
              ~target_survival
              ~pool
              ~d_gov
              ~fallback
              ~parameter:parameter_final
              ~qty_cap
              ~scan_points
        in
        let warnings = ref [] in
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
        if warn_clamp
        then
          warnings
          := Printf.sprintf
               "parameter clamped from %.2f to the survival-constrained %.2f (runway \
                wins over sentiment)"
               resolved_parameter
               survival_parameter
             :: !warnings;
        if not row.passed
        then
          warnings
          := (if fallback
              then (
                let cfg_final = M.set_parameter cfg parameter_final in
                let n_fills_final = M.fills_for_drawdown cfg_final ~d:d_gov in
                Printf.sprintf
                  "cannot fund the deepest observed drawdown %.1f%% at qty_min at gi \
                   %.2f%% (ladder cost %.2f > pool %.2f); increase the pool or loosen \
                   the grid_interval config"
                  (d_gov *. 100.0)
                  parameter_final
                  (M.cost_at cfg_final ~qty:q_min ~n_fills:n_fills_final)
                  pool)
              else
                Printf.sprintf
                  "under-funded: pool %.2f cannot fund the %.1f%% target drawdown at \
                   qty_min on the replayed path (D_surv %.1f%%); coverage is below \
                   target - increase the pool or lower --target-survival"
                  pool
                  (d_gov *. 100.0)
                  (row.d_surv_replay *. 100.0))
             :: !warnings;
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
        let deployed = if active then row.deployed else 0.0 in
        let remainder =
          if not active
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
        ; parameter_components =
            { fng
            ; fng_parameter = fng_parameter_opt
            ; survival_parameter
            ; resolved_parameter = parameter_final
            ; fng_weight
            }
        ; qty = row.qty
        ; parameter = parameter_final
        ; d_surv = row.d_surv_replay
        ; min_quote_drawdown = row.min_quote_drawdown
        ; coverage = row.coverage
        ; warnings = List.rev !warnings
        ; tuning_rows = rows
        ; row
        })
  ;;
end
