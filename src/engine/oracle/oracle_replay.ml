(* Oracle_replay - the drawdown blend model and the generic sizing inversions.

   The blend (F_blend_h(d)) is the unified z-blend shared by sizing, surfaces
   and percentile tables:
     F_asset(d)     = share of the asset's own windows with MFD <= d (raw)
     F_class_avg(d) = (1/n) * sum_s F_class^z( d / (sigma_s * sqrt(h)) )
                      - the pooled class z-CDF evaluated at each asset start's
                        own volatility regime, so a low-vol asset is not
                        punished for a classmate's raw swing size
     F_blend(d)     = (n_a * F_asset(d) + kappa * F_class_avg(d)) / (n_a + kappa)
   All three are estimated over the same window set: starts with a defined
   volatility regime (sigma > 0) only. Flat/gap-adjacent windows carry no
   volatility information (sigma = 0 would map to tau = +infinity and false
   100% class certainty), so they are excluded from F_asset, n_a and
   F_class_avg alike - the blend is a true weighted average of the valid
   windows, never an inconsistent mix of differently-sized denominators.

   All of F_asset, F_class_avg and F_blend are monotone non-decreasing in d
   (composition of empirical CDFs), so bisection over d is sound even though
   path-replay D_surv is not monotone in capital.

   Per (asset, horizon, stride) the blend model precomputes once: the asset's
   MFD samples + trailing vols (Oracle_stats.asset_regime_of) and the pooled
   class z-index (Oracle_classes.z_index_of). Coverage evaluations then cost
   O(n * log n_class) instead of rescanning windows, keeping the bisection
   fast. [stride] selects the sampling basis: the default is horizon sessions
   (non-overlapping windows) for every coverage evaluation - target-survival
   inversion, blended surfaces and percentile tables alike - so a single
   contiguous crash is counted once, not once per overlapping start.
   [n_asset] (the kappa blend weight) is the window count on the model's own
   sampling basis: F_asset is estimated from exactly those windows, so
   weighting the blend by the same count makes kappa a true pseudocount
   against the asset's effective sample size (n_eff on the default basis). A
   short non-overlapping sample therefore shrinks toward the class instead of
   pretending the asset's overlapping window count is independent
   information.

   The sizing inversions ([Sizing]) are strategy-generic: they run against an
   Oracle_strategy.S model (Grid today), so "what capital does surviving the
   target drawdown cost" is answered for any strategy that supplies the
   funding function (cost_at / fills_for_drawdown / drawdown_of_fills) and the
   path replay. *)

open Oracle_types

let section = "oracle_replay"

(* Dedupe keys for the per-(asset, horizon) diagnostics below: the oracle
   re-analyzes the same history on every pass, so thin-sample warnings would
   otherwise re-log every refresh. Warn once per (asset, horizon) per run;
   identical repeats drop to debug. *)
let warned_flat_windows : (string, unit) Hashtbl.t = Hashtbl.create 64
let warned_thin_windows : (string, unit) Hashtbl.t = Hashtbl.create 64

(** Everything needed to evaluate F_blend_h(d) for one horizon on one sampling
    basis. The index is precomputed eagerly by [blend_model_of]. *)
type blend_model =
  { horizon : horizon
  ; asset : series
  ; class_members : series list
  ; kappa : int
  ; warmup : int
  ; weight_by_sessions : bool
  ; stride : int
    (** Sampling basis: horizon sessions by default (non-overlapping windows)
        for all coverage evaluations; stride 1 only when explicitly requested. *)
  ; index : blend_index
  }

and blend_index =
  { mfd_sorted : float array
    (** Asset MFD samples (stride basis, sigma > 0 starts only), sorted
        ascending, for F_asset. *)
  ; sigma : float array
    (** Asset trailing vol per start, aligned with the MFD samples (same loop
        and stride), for the per-start class-z evaluation. Only starts with a
        defined volatility regime (sigma > 0) are included - flat/gap-adjacent
        windows carry no volatility information and are excluded from both
        F_asset and the blend weight. *)
  ; n_asset : int
    (** Blend weight: the window count on the model's own sampling basis
        ([stride] = horizon sessions by default, i.e. the effective sample
        size [n_eff]). F_asset is estimated from exactly these windows, so the
        kappa pseudocount is measured against the asset's true independent
        information content; a thin non-overlapping sample shrinks toward the
        class rather than being weighted as if every overlapping start were an
        independent observation. *)
  ; class_z : Oracle_classes.z_index
  }

let asset_closes_lows (s : series) =
  let bars = Oracle_calendar.sort_bars s.bars |> Oracle_calendar.dedup in
  Array.map (fun b -> b.close) bars, Array.map (fun b -> b.low) bars
;;

(** Count of sorted samples <= [x]. *)
let count_le (sorted : float array) (x : float) =
  let lo = ref 0 in
  let hi = ref (Array.length sorted) in
  while !lo < !hi do
    let mid = (!lo + !hi) / 2 in
    if sorted.(mid) <= x then lo := mid + 1 else hi := mid
  done;
  !lo
;;

let blend_index_of
      ~(horizon : horizon)
      ~(asset : series)
      ~(class_members : series list)
      ~(warmup : int)
      ~weight_by_sessions
      ~(stride : int)
  : blend_index
  =
  let closes, lows = asset_closes_lows asset in
  let regime =
    Oracle_stats.asset_regime_of
      ~closes
      ~lows
      ~horizon:horizon.sessions
      ~w:warmup
      ~warmup
      ~stride
      ()
  in
  (* Exclude starts with sigma = 0 (flat or gap-adjacent windows): they carry
     no volatility information, so they must not contribute to either side of
     the blend. Mapping them to tau = +infinity would inject false 100% class
     certainty, and keeping them in F_asset while excluding them from
     F_class_avg would make the blend an inconsistent weighted average (the
     class side averaged over fewer starts than the asset side). Filtering here
     keeps [mfd_sorted], [sigma] and [n_asset] aligned with the class
     component's valid starts, so the blend is a true weighted average of the
     valid, non-zero-volatility periods. *)
  let n_flat = ref 0 in
  let mfd = ref [] in
  let sigma = ref [] in
  Array.iteri
    (fun i s ->
       if s > 0.0
       then (
         mfd := regime.mfd.(i) :: !mfd;
         sigma := s :: !sigma)
       else incr n_flat)
    regime.sigma;
  if !n_flat > 0
  then (
    let key = Printf.sprintf "%s@%d" asset.symbol horizon.sessions in
    let first = not (Hashtbl.mem warned_flat_windows key) in
    if first then Hashtbl.add warned_flat_windows key ();
    if first
    then
      Logging.warn_f
        ~section
        "Oracle_replay: %d/%d start windows for %s @%d have zero trailing volatility \
         (flat or gap-adjacent data); they are excluded from both the asset CDF and the \
         class contribution"
        !n_flat
        (Array.length regime.sigma)
        asset.symbol
        horizon.sessions
    else
      Logging.debug_f
        ~section
        "Oracle_replay: %d/%d flat windows for %s @%d (already reported this run)"
        !n_flat
        (Array.length regime.sigma)
        asset.symbol
        horizon.sessions);
  let mfd = Array.of_list (List.rev !mfd) in
  let sigma = Array.of_list (List.rev !sigma) in
  let mfd_sorted = Array.copy mfd in
  Array.sort Float.compare mfd_sorted;
  let class_z =
    Oracle_classes.z_index_of
      ~weight_by_sessions
      ~members:class_members
      ~horizon:horizon.sessions
      ~vol_window:warmup
      ~warmup
      ~stride
      ()
  in
  (* The blend weight is the window count on this model's sampling basis (the
     effective sample size when stride = horizon): F_asset is estimated from
     exactly [sigma] windows (all with a defined volatility regime), so kappa
     is a true pseudocount against the asset's independent information
     content. *)
  { mfd_sorted; sigma; n_asset = Array.length sigma; class_z }
;;

type coverage_at_d =
  { n_asset : int
    (** Blend weight on the model's sampling basis (see [blend_index.n_asset]). *)
  ; asset : float
  ; class_ : float
  ; blended : float
  }

(** Pooled class z-coverage averaged over the asset starts with a defined
    trailing volatility regime (sigma > 0). Starts with sigma = 0 (flat/gap
    windows) carry no volatility information; mapping them to tau = +infinity
    would inject false 100% class certainty, so they are excluded up-front in
    [blend_index_of] from the asset CDF, the blend weight and this average -
    the blend is a true weighted average over the valid windows only. *)
let class_fraction (m : blend_model) ~(d : float) =
  let sqrt_h = sqrt (float_of_int m.horizon.sessions) in
  let cls = ref 0.0 in
  let n_valid = ref 0 in
  Array.iter
    (fun sigma ->
       if sigma > 0.0
       then (
         incr n_valid;
         let tau = d /. (sigma *. sqrt_h) in
         cls := !cls +. Oracle_classes.z_cdf_of m.index.class_z ~tau))
    m.index.sigma;
  if !n_valid = 0 then 0.0 else !cls /. float_of_int !n_valid
;;

(** The unified z-blend F_blend(d). Monotone non-decreasing in [d]. Raises on
    an empty distribution (no MFD windows on this horizon/warmup/stride basis)
    instead of returning 0.0 - an empty distribution must never masquerade as
    "zero coverage" or feed a bogus inverse-size. *)
let blended_f (m : blend_model) ~(d : float) =
  let n = Array.length m.index.sigma in
  if n = 0
  then
    invalid_arg
      (Printf.sprintf
         "Oracle_replay.blended_f: empty distribution for %s (horizon %d, warmup %d, \
          stride %d): no MFD windows to evaluate coverage on"
         m.asset.symbol
         m.horizon.sessions
         m.warmup
         m.stride)
  else (
    let f_asset = float_of_int (count_le m.index.mfd_sorted d) /. float_of_int n in
    let f_class = class_fraction m ~d in
    Oracle_stats.blend
      ~n_asset:(float_of_int m.index.n_asset)
      ~asset_f:f_asset
      ~kappa:(float_of_int m.kappa)
      ~class_f:f_class)
;;

(** F_asset_h(d), translated F_class_h(d) and the kappa blend at drawdown [d].
    Raises on an empty distribution (see [blended_f]). *)
let blended_coverage (m : blend_model) ~(d_surv : float) : coverage_at_d =
  let n = Array.length m.index.sigma in
  if n = 0
  then
    invalid_arg
      (Printf.sprintf
         "Oracle_replay.blended_coverage: empty distribution for %s (horizon %d, warmup \
          %d, stride %d): no MFD windows to evaluate coverage on"
         m.asset.symbol
         m.horizon.sessions
         m.warmup
         m.stride)
  else (
    let f_asset = float_of_int (count_le m.index.mfd_sorted d_surv) /. float_of_int n in
    let f_class = class_fraction m ~d:d_surv in
    let f_blend =
      Oracle_stats.blend
        ~n_asset:(float_of_int m.index.n_asset)
        ~asset_f:f_asset
        ~kappa:(float_of_int m.kappa)
        ~class_f:f_class
    in
    { n_asset = m.index.n_asset; asset = f_asset; class_ = f_class; blended = f_blend })
;;

(** Headline: historical path coverage for the strategy's own D_surv. *)
let historical_path_coverage (m : blend_model) ~(d_surv : float)
  : historical_path_coverage
  =
  let c = blended_coverage m ~d_surv in
  { horizon = m.horizon
  ; asset_coverage = c.asset
  ; class_coverage = c.class_
  ; blended_coverage = c.blended
  }
;;

(** Builds the coverage model. [stride] defaults to the horizon (non-
    overlapping windows) for every coverage evaluation: overlapping (stride-1)
    samples would count one contiguous crash once per rolling start and let a
    single severe regime dominate the target-survival inversion. Pass
    ~stride:1 explicitly only when an overlapping basis is wanted. Warns when
    the effective sample is thin (n_eff < 5, the asset history cannot support
    an authoritative tail). Windows with zero trailing volatility (flat or
    gap-adjacent data) are excluded from the asset CDF, the blend weight and
    the class contribution inside [blend_index_of] (which also warns). *)
let blend_model_of
      ?(weight_by_sessions = true)
      ?stride
      ~(horizon : horizon)
      ~(asset : series)
      ~(class_members : series list)
      ~(kappa : int)
      ~(warmup : int)
      ()
  : blend_model
  =
  let stride = Option.value stride ~default:horizon.sessions in
  let index =
    blend_index_of ~horizon ~asset ~class_members ~warmup ~weight_by_sessions ~stride
  in
  let n_eff = Array.length index.sigma in
  if n_eff > 0 && n_eff < 5
  then (
    let key = Printf.sprintf "%s@%d" asset.symbol horizon.sessions in
    let first = not (Hashtbl.mem warned_thin_windows key) in
    if first then Hashtbl.add warned_thin_windows key ();
    if first
    then
      Logging.warn_f
        ~section
        "Oracle_replay: only %d independent %d-session windows for %s (warmup %d); \
         coverage/sizing is not authoritative below 5 windows"
        n_eff
        horizon.sessions
        asset.symbol
        warmup
    else
      Logging.debug_f
        ~section
        "Oracle_replay: only %d independent %d-session windows for %s (already reported \
         this run)"
        n_eff
        horizon.sessions
        asset.symbol);
  { horizon; asset; class_members; kappa; warmup; weight_by_sessions; stride; index }
;;

(** Smallest drawdown d in (0, 1) whose coverage [f](d) reaches [target].
    Empirical coverage functions used here are monotone non-decreasing in d,
    so a bisection is sound - unlike replay D_surv, which is path-dependent
    and not monotone in capital. The bisection keeps the upper bound [hi]
    with f(hi) >= target and returns it: a CDF is a step function, so the
    lower bound can sit on a step edge whose value is still below target, and
    downstream sizing must consume the coverage the grid actually achieves.
    40 iterations narrow the bracket to ~1e-12 - far past any float
    distinguishability on the [0, 1] axis - so the returned edge is exact to
    machine precision regardless of where the empirical step lies.

    Coupling caveat: [hi] is capped at 0.999999, so for a target above
    f(0.999999) (the target sits in the gap between the deepest exhausting
    coverage and the never-exhausted coverage of 1.0) the bisection returns
    0.999999 with f(hi) < target. Callers must re-check the achieved coverage
    (as [Sizing.find_min_capital] / [Sizing.max_qty] do) instead of trusting
    the value; the cap deliberately stays below 1.0 so the returned drawdown
    can never propagate a d = 1.0 into [fills_for_drawdown] (whose log would
    saturate). *)
let d_for_coverage ~(f : float -> float) ~(target : float) =
  if target <= 0.0
  then 0.0
  else (
    let rec bisect lo hi i =
      if i = 0
      then hi
      else (
        let mid = (lo +. hi) /. 2.0 in
        if f mid >= target then bisect lo mid (i - 1) else bisect mid hi (i - 1))
    in
    bisect 0.0 0.999999 40)
;;

(** Smallest drawdown d in (0, 1) whose blended coverage F_blend(d) reaches
    [target]. F_blend is monotone non-decreasing in d (an empirical CDF), so a
    bisection is sound - unlike replay D_surv, which is path-dependent and not
    monotone in capital. *)
let drawdown_for_target ~(model : blend_model) ~(target_survival : float) =
  d_for_coverage
    ~f:(fun d -> (blended_coverage model ~d_surv:d).blended)
    ~target:target_survival
;;

(** Strategy-generic sizing inversions. Instantiate with a strategy model
    (Oracle_strategy.Grid today): the inversions only need the funding
    function and the path replay the model supplies.

    Static inversions ([find_min_capital], [max_qty]) evaluate the closed-form
    worst case - N consecutive buy steps with no intermediate sells - which is
    the theoretical lower bound a path could hit, so they are structurally
    pessimistic: real paths bounce, sells free quote, and the strategy
    survives deeper than the static runway predicts. The empirical number
    ([empirical_min_capital]) measures the "capital buffer" the static sizing
    pays for on the actual replayed path. *)
module Sizing (M : Oracle_strategy.S) = struct
  (** Inverse sizing: smallest [capital] whose static runway survives the
      drawdown d-star (the smallest d with F_blend(d) >= target). The
      drawdown is the ATH-scaled survival reference of the asset's history
      (see Oracle_math.sizing_reference_of): mature assets fund the remaining
      fall to the expected floor from the current price (capped by the
      deepest actual peak-to-valley drop, floored by the measured floor
      overshoot) - never a hypothetical fall from the ATH. The CDF is
      monotone in d, and the runway cost (floor-aware; see
      [cost_at]) is a monotone function of the fill count, so this is
      well-defined even though path-replay D_surv is not monotone in
      capital. Returns [reachable = false] when the required capital
      exceeds [hi] (or the target would need surviving the entire history
      with certainty). *)
  let find_min_capital
        ?(hi = 1e9)
        ~(grid : M.config)
        ~(model : blend_model)
        ~(target_survival : float)
        ()
    : sizing_result
    =
    let d = drawdown_for_target ~model ~target_survival in
    let d_cover =
      match Oracle_math.sizing_reference_of ~fallback:false model.asset with
      | Some r -> r.d_cover
      | None -> d
    in
    let n = M.fills_for_drawdown grid ~d:d_cover in
    let capital = M.cost_at grid ~qty:(M.design_qty grid) ~n_fills:n in
    if capital > hi
    then
      { parameter = "capital"
      ; value = hi
      ; d_surv = 1.0
      ; coverage = 0.0
      ; reachable = false
      }
    else (
      let d_surv = M.drawdown_of_fills grid ~n_fills:n in
      let coverage = (blended_coverage model ~d_surv).blended in
      (* The bisection returns the CDF step top (f(hi) >= target), so a blended
         coverage still below target here means the target sits in a gap the
         blended history cannot reach (surviving the whole history is not
         achievable with certainty) - explicitly unreachable, not a capital
         number. *)
      if coverage +. 1e-12 < target_survival
      then
        { parameter = "capital"
        ; value = hi
        ; d_surv = 1.0
        ; coverage = 0.0
        ; reachable = false
        }
      else { parameter = "capital"; value = capital; d_surv; coverage; reachable = true })
  ;;

  (** Static min capital for a set of horizons: the max over horizons of
      [find_min_capital] (the model's own "safe recommendation" for the target
      survival). This is the default [start_quote] the CLI replays when no
      --capital is given: the strategy needs capital only to place buy orders
      (sell inventory is not required to run it), so the sizing's own
      recommendation - which funds the ladder through the target drawdown on
      the deepest horizon - is a meaningful replay capital instead of an
      unrelated live account balance. Returns [None] when no horizon reaches a
      finite sizing within [hi] (e.g. the target sits in a coverage gap);
      callers should then require an explicit capital. *)
  let min_capital_for_horizons
        ?(hi = 1e9)
        ~(grid : M.config)
        ~(models : blend_model list)
        ~(target_survival : float)
        ()
    =
    let best =
      List.fold_left
        (fun acc (m : blend_model) ->
           let r = find_min_capital ~hi ~grid ~model:m ~target_survival () in
           if r.reachable then Float.max acc r.value else acc)
        0.0
        models
    in
    if best > 0.0 then Some best else None
  ;;

  (** Largest [qty] whose static runway (given the grid's [start_quote])
      survives the drawdown d-star (the smallest d with F_blend(d) >= target,
      sized on the ATH-scaled survival reference as in [find_min_capital]).
      The runway cost is monotone non-decreasing in qty (the floor up-size
      term max(qty, ceil_lot(min_notional/level)) is), so the boundary is
      found by bisection over an exponentially grown upper bound - exact when
      the venue floor does not bind, and conservative under a binding floor
      (the per-unit cost understates the true cost of large qtys, so the
      result is advisory there - the replay-based empirical sizing is
      authoritative). Returns [reachable = false] when even [min_qty] cannot
      fit the budget (or the target would need surviving the entire history
      with certainty). *)
  let max_qty
        ?(hi = 1e6)
        ~(grid : M.config)
        ~(model : blend_model)
        ~(target_survival : float)
        ()
    : sizing_result
    =
    let d = drawdown_for_target ~model ~target_survival in
    let d_cover =
      match Oracle_math.sizing_reference_of ~fallback:false model.asset with
      | Some r -> r.d_cover
      | None -> d
    in
    let n = M.fills_for_drawdown grid ~d:d_cover in
    let d_surv = M.drawdown_of_fills grid ~n_fills:n in
    let coverage = (blended_coverage model ~d_surv).blended in
    (* Same unreachable detection as [find_min_capital]: the bisection returns
       a CDF step top, so a coverage below target here is a coverage gap the
       blended history cannot clear with certainty. *)
    if coverage +. 1e-12 < target_survival
    then
      { parameter = "qty"; value = hi; d_surv = 1.0; coverage = 0.0; reachable = false }
    else (
      let budget = M.start_quote grid in
      let q_min = M.min_qty grid in
      let fits qty = qty >= q_min && M.cost_at grid ~qty ~n_fills:n <= budget +. 1e-9 in
      let qty =
        if budget <= 0.0
        then q_min
        else (
          let rec grow q =
            if M.cost_at grid ~qty:q ~n_fills:n >= budget then q else grow (q *. 2.0)
          in
          let q_hi = grow (Float.max q_min 1e-9) in
          (* The largest qty that fits the budget: keep the passing lower
             bound [lo] (the cost at the returned qty never overshoots - the
             floor-aware inversion is exact, unlike the closed form, which
             understates the true cost under a binding notional floor). *)
          let rec bisect lo hi i =
            if i = 0
            then lo
            else (
              let mid = (lo +. hi) /. 2.0 in
              if M.cost_at grid ~qty:mid ~n_fills:n <= budget
              then bisect mid hi (i - 1)
              else bisect lo mid (i - 1))
          in
          bisect q_min q_hi 60)
      in
      if (not (fits qty)) || qty > hi
      then (
        let qty = Float.max qty q_min |> Float.min hi in
        { parameter = "qty"; value = qty; d_surv; coverage; reachable = false })
      else { parameter = "qty"; value = qty; d_surv; coverage; reachable = true })
  ;;


  (** Multi-horizon variant of [empirical_min_capital]: the smallest capital
      per horizon whose actual path replay clears [target_survival].

      A path replay is horizon-independent - the same replayed path yields one
      d_surv, which each horizon's blend CDF scores separately - so the
      log-spaced scan shares ONE replay per probed capital across all models
      and the first crossings come from the same samples. Everything after the
      shared scan (bisection refinement, the densified island re-scan below
      the first hit) runs per horizon exactly as in the single-horizon
      version; with a one-element model list the probe sequence is identical
      to [empirical_min_capital]. The scan bracket stops at the largest
      static requirement * 1.05 across models (or [hi] when any horizon is
      statically unreachable), with the same rescan-to-[hi] fallback when a
      horizon crosses nowhere inside the bracket. *)
  let empirical_min_capitals
        ?(scan_points = 96)
        ?(hi = 1e9)
        ~(grid : M.config)
        ~(models : blend_model list)
        ~(target_survival : float)
        ()
    : sizing_result array
    =
    let ms = Array.of_list models in
    let n = Array.length ms in
    if n = 0
    then [||]
    else (
      let asset = ms.(0).asset in
      (* One replay per probed capital; every horizon's blend CDF scores the
         same replayed d_surv. *)
      let covs_at capital =
        let out = M.replay (M.set_start_quote grid capital) asset in
        Array.map (fun m -> (blended_coverage m ~d_surv:out.d_surv).blended) ms
      in
      let cov_of j capital = (covs_at capital).(j) in
      let statics =
        Array.map (fun m -> find_min_capital ~grid ~model:m ~target_survival ~hi ()) ms
      in
      let c_min = M.cost_at grid ~qty:(M.design_qty grid) ~n_fills:1 in
      let c_max =
        Float.min
          hi
          (Array.fold_left
             (fun acc s -> if s.reachable then Float.max acc (s.value *. 1.05) else acc)
             hi
             statics)
      in
      if c_max <= c_min
      then statics
      else (
        let pts ~c_lo ~c_hi =
          Array.init scan_points (fun i ->
            c_lo *. ((c_hi /. c_lo) ** (float_of_int i /. float_of_int (scan_points - 1))))
        in
        (* Shared log-spaced scan over one bracket: every probe is replayed
           once and scored against all horizons. *)
        let scan ~c_lo ~c_hi =
          let caps = pts ~c_lo ~c_hi in
          let covs = Array.init n (fun _ -> Array.make scan_points 0.0) in
          for i = 0 to scan_points - 1 do
            let cs = covs_at caps.(i) in
            for j = 0 to n - 1 do
              covs.(j).(i) <- cs.(j)
            done
          done;
          caps, covs
        in
        let idx_of_first_crossing cov =
          let rec go i =
            if i >= scan_points
            then None
            else if cov.(i) >= target_survival
            then Some i
            else go (i + 1)
          in
          go 0
        in
        (* Bisection-refine a bracket whose upper endpoint passes: keeps the
           passing side, 40 iterations, exactly as the single-horizon version. *)
        let refine_one j ~c_lo ~c_hi ~idx caps =
          let lo_cap =
            if idx = 0
            then c_lo
            else
              c_lo
              *. ((c_hi /. c_lo)
                  ** (float_of_int (idx - 1) /. float_of_int (scan_points - 1)))
          in
          let rec bisect lo hi_ i =
            if i = 0
            then hi_
            else (
              let mid = (lo +. hi_) /. 2.0 in
              if cov_of j mid >= target_survival
              then bisect lo mid (i - 1)
              else bisect mid hi_ (i - 1))
          in
          bisect lo_cap caps.(idx) 40
        in
        let caps, covs = scan ~c_lo:c_min ~c_hi:c_max in
        (* Final assembly: one replay at the chosen boundary fills the
           achieved d_surv/coverage (as the single-horizon version does). *)
        let finish j best =
          let out = M.replay (M.set_start_quote grid best) asset in
          { parameter = "capital"
          ; value = best
          ; d_surv = out.d_surv
          ; coverage = (blended_coverage ms.(j) ~d_surv:out.d_surv).blended
          ; reachable = true
          }
        in
        let results = Array.init n (fun _ -> ref None) in
        for j = 0 to n - 1 do
          match idx_of_first_crossing covs.(j) with
          | None ->
            (* No crossing in the bracket: when the bracket stopped short of
               [hi], rescan the full range to the user's bound before giving
               up (as the single-horizon version does). *)
            if c_max < hi -. 1e-9
            then (
              let caps2, covs2 = scan ~c_lo:c_min ~c_hi:hi in
              match idx_of_first_crossing covs2.(j) with
              | Some idx ->
                results.(j)
                := Some (finish j (refine_one j ~c_lo:c_min ~c_hi:hi ~idx caps2))
              | None -> ())
            else ()
          | Some idx ->
            let r1 = refine_one j ~c_lo:c_min ~c_hi:c_max ~idx caps in
            (* Densified re-scan strictly below the first hit bounds
               non-monotone islands (a smaller capital that also clears). *)
            let r2 =
              let cap_island = caps.(idx) in
              let caps2, covs2 = scan ~c_lo:c_min ~c_hi:cap_island in
              match idx_of_first_crossing covs2.(j) with
              | None -> None
              | Some idx2 ->
                Some (refine_one j ~c_lo:c_min ~c_hi:cap_island ~idx:idx2 caps2)
            in
            let best =
              match r2 with
              | Some v when v < r1 -> v
              | _ -> r1
            in
            results.(j) := Some (finish j best)
        done;
        Array.map
          (fun r ->
             match !r with
             | Some v -> v
             | None ->
               { parameter = "capital"
               ; value = hi
               ; d_surv = 1.0
               ; coverage = 0.0
               ; reachable = false
               })
          results))
  ;;

  (* Thin re-export of the multi-horizon inversion for the single-model
     callers (CLI inverse-sizing table, tests): identical probe sequence,
     identical result - see [empirical_min_capitals]. *)
  let empirical_min_capital
        ?(scan_points = 96)
        ?(hi = 1e9)
        ~(grid : M.config)
        ~(model : blend_model)
        ~(target_survival : float)
        ()
    : sizing_result
    =
    (empirical_min_capitals ~scan_points ~hi ~grid ~models:[ model ] ~target_survival ()).(
    0)
  ;;

  (** Rollup of one cell's per-horizon sizing results into the "clears every
      horizon" number: the largest reachable requirement - or [None] when ANY
      horizon cannot clear the target within the search bound (no finite
      capital clears all horizons then). *)
  let combined_sizing (rs : sizing_result array) : sizing_result option =
    if
      Array.exists (fun r -> not r.reachable) rs
      || Array.exists (fun r -> Float.is_nan r.value) rs
    then None
    else if Array.length rs = 0
    then None
    else
      Some
        (Array.fold_left (fun acc r -> if r.value > acc.value then r else acc) rs.(0) rs)
  ;;

  (** One cash-requirement-surface row: the (gi, qty) cell identity plus the
      static and empirical per-horizon minimum-capital inversions. *)
  type cash_cell =
    { gi : float
    ; qty : float
    ; static : sizing_result array
    ; empirical : sizing_result array
    }

  (** Cash requirement sweep over pre-built grid variants: for each cell, the
      closed-form static min-capital per horizon and the replay-verified
      empirical min-capital per horizon. Cells are caller-built (gi x qty)
      configs carrying their own venue gates; results preserve input order.
      [empirical_scan_points] trades accuracy for runtime inside the dense
      sweep (the single-grid inverse-sizing table keeps the default 96). *)
  let surface_rows
        ?(empirical_scan_points = 24)
        ?hi
        ~(target_survival : float)
        ~(models : blend_model list)
        (cells : (float * float * M.config) list)
    : cash_cell list
    =
    List.map
      (fun (gi, qty, grid) ->
         let static =
           Array.of_list
             (List.map
                (fun m -> find_min_capital ?hi ~grid ~model:m ~target_survival ())
                models)
         in
         let empirical =
           empirical_min_capitals
             ~scan_points:empirical_scan_points
             ?hi
             ~grid
             ~models
             ~target_survival
             ()
         in
         { gi; qty; static; empirical })
      cells
  ;;
end
