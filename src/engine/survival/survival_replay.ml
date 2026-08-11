(* Survival_replay - Grid_core path replay and historical path coverage.

   Replays the grid over the asset's OHLC history (pessimistic Buy_first
   ordering) and extracts the survival event: D_surv =
   first_exhaustion_price_drawdown, or 100% when the grid never runs dry (it
   then survived every drawdown the history produced, so F_h(1.0) = 1.0). The
   headline number is historical_path_coverage = F_blend_h(D_surv): the share
   of the asset's own history (blended toward the class) whose max drawdown the
   grid would have survived, with a target-survival probability.

   The blend (F_blend_h(d)) is the unified z-blend shared by sizing, surfaces
   and percentile tables:
     F_asset(d)     = share of the asset's own windows with MFD <= d (raw)
     F_class_avg(d) = (1/n) * sum_s F_class^z( d / (sigma_s * sqrt(h)) )
                      - the pooled class z-CDF evaluated at each asset start's
                      own volatility regime, so a low-vol asset is not punished
                      for a classmate's raw swing size
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
   MFD samples + trailing vols (Survival_stats.asset_regime_of) and the pooled
   class z-index (Survival_classes.z_index_of). Coverage evaluations then cost
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
   information. *)

open Survival_types
open Dio_strategies

let section = "survival_replay"

type outcome =
  { d_surv : float
  ; exhausted : bool
  ; halt_cause : Grid_core_types.halt_cause option
  ; min_quote_drawdown : float
  ; buy_fills : int
  ; sell_fills : int
  }

let d_surv_of_result (r : Grid_core.result) =
  match r.first_exhaustion_price_drawdown with
  | Some d -> d
  (* The grid never ran dry: it survived every drawdown the history produced,
     so the survival threshold is 100% and F_h(1.0) = 1.0. This keeps coverage
     monotone in capital/qty, which the inverse-sizing binary searches rely on;
     the realized quote dip is still reported as min_quote_drawdown. *)
  | None -> 1.0
;;

let replay_series (cfg : Grid_core.config) (s : series) : outcome =
  let bars =
    s.bars
    |> Survival_calendar.sort_bars
    |> Survival_calendar.dedup
    |> Array.map (fun (b : Survival_types.bar) ->
      Grid_core_types.{ high = b.high; low = b.low; close = b.close })
  in
  let r = Grid_core.replay cfg ~bars ~ordering:Grid_core_types.Buy_first in
  { d_surv = d_surv_of_result r
  ; exhausted = r.exhausted
  ; halt_cause = r.halt_cause
  ; min_quote_drawdown = r.min_quote_drawdown
  ; buy_fills = r.buy_fills
  ; sell_fills = r.sell_fills
  }
;;

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
  ; class_z : Survival_classes.z_index
  }

let asset_closes_lows (s : series) =
  let bars = Survival_calendar.sort_bars s.bars |> Survival_calendar.dedup in
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
    Survival_stats.asset_regime_of
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
  then
    Logging.warn_f
      ~section
      "Survival_replay: %d/%d start windows for %s @%d have zero trailing volatility \
       (flat or gap-adjacent data); they are excluded from both the asset CDF and the \
       class contribution"
      !n_flat
      (Array.length regime.sigma)
      asset.symbol
      horizon.sessions;
  let mfd = Array.of_list (List.rev !mfd) in
  let sigma = Array.of_list (List.rev !sigma) in
  let mfd_sorted = Array.copy mfd in
  Array.sort Float.compare mfd_sorted;
  let class_z =
    Survival_classes.z_index_of
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
         cls := !cls +. Survival_classes.z_cdf_of m.index.class_z ~tau))
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
         "Survival_replay.blended_f: empty distribution for %s (horizon %d, warmup %d, \
          stride %d): no MFD windows to evaluate coverage on"
         m.asset.symbol
         m.horizon.sessions
         m.warmup
         m.stride)
  else (
    let f_asset = float_of_int (count_le m.index.mfd_sorted d) /. float_of_int n in
    let f_class = class_fraction m ~d in
    Survival_stats.blend
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
         "Survival_replay.blended_coverage: empty distribution for %s (horizon %d, \
          warmup %d, stride %d): no MFD windows to evaluate coverage on"
         m.asset.symbol
         m.horizon.sessions
         m.warmup
         m.stride)
  else (
    let f_asset = float_of_int (count_le m.index.mfd_sorted d_surv) /. float_of_int n in
    let f_class = class_fraction m ~d:d_surv in
    let f_blend =
      Survival_stats.blend
        ~n_asset:(float_of_int m.index.n_asset)
        ~asset_f:f_asset
        ~kappa:(float_of_int m.kappa)
        ~class_f:f_class
    in
    { n_asset = m.index.n_asset; asset = f_asset; class_ = f_class; blended = f_blend })
;;

(** Headline: historical path coverage for the grid's own D_surv. *)
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

(** Coverage of a candidate capital: replay with [start_quote = capital]. *)
let coverage_of_capital
      (base : Grid_core.config)
      ~(series : series)
      (m : blend_model)
      (capital : float)
  : coverage_at_d
  =
  let cfg = { base with start_quote = capital } in
  let out = replay_series cfg series in
  blended_coverage m ~d_surv:out.d_surv
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
  then
    Logging.warn_f
      ~section
      "Survival_replay: only %d independent %d-session windows for %s (warmup %d); \
       coverage/sizing is not authoritative below 5 windows"
      n_eff
      horizon.sessions
      asset.symbol
      warmup;
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
    machine precision regardless of where the empirical step lies. *)
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

(** Smallest number of ladder fills whose static runway drawdown
    1-(1-gi)^n reaches [d]. At least one fill is required (a grid below the
    first buy is exhausted at the first level). *)
let fills_for_drawdown ~(grid : Grid_core.config) ~(d : float) =
  let gi = Float.min (grid.grid_interval_pct /. 100.0) 0.99 in
  if d <= 0.0
  then 1
  else max 1 (int_of_float (Float.ceil (Float.log (1.0 -. d) /. Float.log (1.0 -. gi))))
;;

(** Quote capital that exactly funds [n_fills] ladder buys. Floor-aware: walks
    the ladder with dynamic buy up-sizing (Grid_core.required_buy_qty), so a
    binding min_notional cannot make the sizing understate the true cost. With
    no floor this reduces to the closed-form geometric sum. *)
let capital_for_fills ~(grid : Grid_core.config) ~(n_fills : int) =
  Survival_mfd.floor_aware_runway_cost
    ~qty:grid.qty
    ~grid_interval_pct:grid.grid_interval_pct
    ~fee:grid.maker_fee
    ~start_price:grid.start_price
    ~min_notional:grid.min_notional
    ~price_increment:grid.price_increment
    ~qty_increment:grid.qty_increment
    ~n_fills
;;

(** Static drawdown survived by [n_fills] ladder steps. *)
let drawdown_of_fills ~(grid : Grid_core.config) ~(n_fills : int) =
  let gi = grid.grid_interval_pct /. 100.0 in
  1.0 -. ((1.0 -. gi) ** float_of_int n_fills)
;;

(** Inverse sizing: smallest [capital] whose static runway survives the
    drawdown d* (the smallest d with F_blend(d) >= target). The CDF is monotone
    in d, and the runway cost (floor-aware; see [capital_for_fills]) is a
    monotone function of the fill count, so this is well-defined even though
    path-replay D_surv is not monotone in capital. Returns
    [reachable = false] when the required capital exceeds [hi] (or the target
    would need surviving the entire history with certainty). *)
let find_min_capital
      ?(hi = 1e9)
      ~(grid : Grid_core.config)
      ~(model : blend_model)
      ~(target_survival : float)
      ()
  : sizing_result
  =
  let d = drawdown_for_target ~model ~target_survival in
  let n = fills_for_drawdown ~grid ~d in
  let capital = capital_for_fills ~grid ~n_fills:n in
  if capital > hi
  then
    { parameter = "capital"; value = hi; d_surv = 1.0; coverage = 0.0; reachable = false }
  else (
    let d_surv = drawdown_of_fills ~grid ~n_fills:n in
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
    --capital is given: the grid needs capital only to place buy orders (sell
    inventory is not required to run it), so the sizing's own recommendation -
    which funds the ladder through the target drawdown on the deepest horizon -
    is a meaningful replay capital instead of an unrelated live account
    balance. Returns [None] when no horizon reaches a finite sizing within
    [hi] (e.g. the target sits in a coverage gap); callers should then require
    an explicit capital. *)
let min_capital_for_horizons
      ?(hi = 1e9)
      ~(grid : Grid_core.config)
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

(** Largest [qty] whose static runway (given the grid's [start_quote]) survives
    the drawdown d* (the smallest d with F_blend(d) >= target). The runway cost
    is linear in qty when the venue floor does not bind, so the boundary is
    closed form; under a binding floor the per-unit cost understates the true
    cost of large qtys (the floor caps the cost of small ones), so the result
    is advisory there - the replay-based empirical sizing is authoritative.
    Returns [reachable = false] when even [qty_increment] is too large (or the
    target would need surviving the entire history with certainty). *)
let max_qty
      ?(hi = 1e6)
      ~(grid : Grid_core.config)
      ~(model : blend_model)
      ~(target_survival : float)
      ()
  : sizing_result
  =
  let d = drawdown_for_target ~model ~target_survival in
  let n = fills_for_drawdown ~grid ~d in
  let d_surv = drawdown_of_fills ~grid ~n_fills:n in
  let coverage = (blended_coverage model ~d_surv).blended in
  (* Same unreachable detection as [find_min_capital]: the bisection returns a
     CDF step top, so a coverage below target here is a coverage gap the
     blended history cannot clear with certainty. *)
  if coverage +. 1e-12 < target_survival
  then { parameter = "qty"; value = hi; d_surv = 1.0; coverage = 0.0; reachable = false }
  else (
    let gi = grid.grid_interval_pct /. 100.0 in
    let per_unit =
      (1.0 +. grid.maker_fee)
      *. grid.start_price
      *. (1.0 -. gi)
      *. ((1.0 -. ((1.0 -. gi) ** float_of_int n)) /. gi)
    in
    let qty = grid.start_quote /. per_unit in
    if qty < grid.qty_increment || qty > hi
    then (
      let qty = Float.max qty grid.qty_increment |> Float.min hi in
      { parameter = "qty"; value = qty; d_surv; coverage; reachable = false })
    else { parameter = "qty"; value = qty; d_surv; coverage; reachable = true })
;;

(** Empirical min capital (advisory): the smallest [start_quote] whose actual
    Grid_core path replay clears [target_survival] on the asset's own history.

    The static sizing ([find_min_capital]) evaluates the closed-form worst
    case - N consecutive ladder buys with no intermediate sells - which is the
    theoretical lower bound a path could hit, so it is structurally
    pessimistic: real paths bounce, sells free quote, and the grid survives
    deeper than the static runway predicts. The empirical number measures the
    "capital buffer" the static sizing pays for. Caveat: the static runway is
    a pure geometric ladder, so when the venue's min_notional binds (dynamic
    buy up-sizing), the replayed path can burn capital faster per rung than
    the closed form - the empirical number may then exceed the static one
    instead of landing below it.

    Path-replay D_surv is NOT monotone in capital (intermediate sells shift
    which ladder level exhausts the grid), so this cannot be a plain binary
    search: it scans a log-spaced capital grid, then bisection-refines the
    boundary cell of the first crossing under the local-monotonicity
    assumption, and repeats the scan below the first hit at the same
    resolution to bound non-monotone islands. Returns [reachable = false] when
    even [hi] does not clear the target. Advisory only: the static result
    remains the sizing recommendation. *)
let empirical_min_capital
      ?(scan_points = 96)
      ?(hi = 1e9)
      ~(grid : Grid_core.config)
      ~(model : blend_model)
      ~(target_survival : float)
      ()
  : sizing_result
  =
  let coverage_at capital =
    let cfg = { grid with start_quote = capital } in
    let out = replay_series cfg model.asset in
    (blended_coverage model ~d_surv:out.d_surv).blended
  in
  let c_min = capital_for_fills ~grid ~n_fills:1 in
  let static = find_min_capital ~grid ~model ~target_survival ~hi () in
  let c_max = if static.reachable then Float.min hi (static.value *. 1.05) else hi in
  if c_max <= c_min
  then static
  else (
    let pts ~c_lo ~c_hi =
      Array.init scan_points (fun i ->
        c_lo *. ((c_hi /. c_lo) ** (float_of_int i /. float_of_int (scan_points - 1))))
    in
    let first_crossing ~c_lo ~c_hi =
      let arr = pts ~c_lo ~c_hi in
      let rec go i =
        if i >= Array.length arr
        then None
        else if coverage_at arr.(i) >= target_survival
        then Some (i, arr.(i))
        else go (i + 1)
      in
      go 0
    in
    let refine ~c_lo ~c_hi = function
      | None -> None
      | Some (idx, cap) ->
        let lo_cap =
          if idx = 0
          then c_lo
          else
            c_lo
            *. ((c_hi /. c_lo)
                ** (float_of_int (idx - 1) /. float_of_int (scan_points - 1)))
        in
        let rec bisect lo hi i =
          if i = 0
          then hi
          else (
            let mid = (lo +. hi) /. 2.0 in
            if coverage_at mid >= target_survival
            then bisect lo mid (i - 1)
            else bisect mid hi (i - 1))
        in
        Some (bisect lo_cap cap 40)
    in
    let first = first_crossing ~c_lo:c_min ~c_hi:c_max in
    let refined = refine ~c_lo:c_min ~c_hi:c_max first in
    let refined =
      match first with
      | Some (_, cap) ->
        (* Second, same-resolution pass below the first hit to bound
           non-monotone islands (a smaller capital that also clears). *)
        (match refine ~c_lo:c_min ~c_hi:cap (first_crossing ~c_lo:c_min ~c_hi:cap) with
         | Some r2 ->
           (match refined with
            | Some r1 -> Some (Float.min r1 r2)
            | None -> Some r2)
         | None -> refined)
      | None -> refined
    in
    match refined with
    | None when c_max < hi -. 1e-9 ->
      (* Static unreachable: rescan to the user's bound. *)
      (match refine ~c_lo:c_min ~c_hi:hi (first_crossing ~c_lo:c_min ~c_hi:hi) with
       | Some r ->
         let out = replay_series { grid with start_quote = r } model.asset in
         let coverage = (blended_coverage model ~d_surv:out.d_surv).blended in
         { parameter = "capital"
         ; value = r
         ; d_surv = out.d_surv
         ; coverage
         ; reachable = true
         }
       | None ->
         { parameter = "capital"
         ; value = hi
         ; d_surv = 1.0
         ; coverage = 0.0
         ; reachable = false
         })
    | Some r ->
      let out = replay_series { grid with start_quote = r } model.asset in
      let coverage = (blended_coverage model ~d_surv:out.d_surv).blended in
      { parameter = "capital"
      ; value = r
      ; d_surv = out.d_surv
      ; coverage
      ; reachable = true
      }
    | None ->
      { parameter = "capital"
      ; value = hi
      ; d_surv = 1.0
      ; coverage = 0.0
      ; reachable = false
      })
;;
