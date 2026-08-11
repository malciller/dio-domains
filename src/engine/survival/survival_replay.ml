(* Survival_replay - Grid_core path replay and historical path coverage.

   Replays the grid over the asset's OHLC history (pessimistic Buy_first
   ordering) and extracts the survival event: D_surv =
   first_capital_low_drawdown, or 100% when the grid never runs dry (it then
   survived every drawdown the history produced, so F_h(1.0) = 1.0). The
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

   All of F_asset, F_class_avg and F_blend are monotone non-decreasing in d
   (composition of empirical CDFs), so bisection over d is sound even though
   path-replay D_surv is not monotone in capital.

   Per (asset, horizon, stride) the blend model precomputes once: the asset's
   MFD samples + trailing vols (Survival_stats.asset_regime_of) and the pooled
   class z-index (Survival_classes.z_index_of). Coverage evaluations then cost
   O(n * log n_class) instead of rescanning windows, keeping the bisection
   fast. [stride] selects the sampling basis: 1 for coverage/sizing (unbiased
   mean estimate), horizon for percentile tables (non-overlapping tail). *)

open Survival_types
open Dio_strategies

type outcome =
  { d_surv : float
  ; exhausted : bool
  ; min_quote_drawdown : float
  ; buy_fills : int
  ; sell_fills : int
  }

let d_surv_of_result (r : Grid_core.result) =
  match r.first_capital_low_drawdown with
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
    (** Sampling basis: 1 for coverage/sizing, horizon for percentile tables. *)
  ; index : blend_index
  }

and blend_index =
  { mfd_sorted : float array
    (** Asset MFD samples (stride basis), sorted ascending, for F_asset. *)
  ; sigma : float array
    (** Asset trailing vol per start, aligned with the MFD samples (same loop
        and stride), for the per-start class-z evaluation. *)
  ; n_asset : int
    (** Blend weight: the stride-1 (overlapping) window count, so kappa keeps
        its "pseudo-sessions against all the asset's own data" meaning even
        when the sample basis is non-overlapping. *)
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
  let n_asset =
    Survival_mfd.n_starts ~closes ~lows ~horizon:horizon.sessions ~warmup ()
  in
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
  let mfd_sorted = Array.copy regime.mfd in
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
  { mfd_sorted; sigma = regime.sigma; n_asset; class_z }
;;

type coverage_at_d =
  { n_asset : int
  ; asset : float
  ; class_ : float
  ; blended : float
  }

(** The unified z-blend F_blend(d). Monotone non-decreasing in [d]. *)
let blended_f (m : blend_model) ~(d : float) =
  let n = Array.length m.index.sigma in
  if n = 0
  then 0.0
  else (
    let f_asset = float_of_int (count_le m.index.mfd_sorted d) /. float_of_int n in
    let sqrt_h = sqrt (float_of_int m.horizon.sessions) in
    let cls = ref 0.0 in
    Array.iter
      (fun sigma ->
         let tau = if sigma > 0.0 then d /. (sigma *. sqrt_h) else Float.infinity in
         cls := !cls +. Survival_classes.z_cdf_of m.index.class_z ~tau)
      m.index.sigma;
    let f_class = !cls /. float_of_int n in
    Survival_stats.blend
      ~n_asset:(float_of_int m.index.n_asset)
      ~asset_f:f_asset
      ~kappa:(float_of_int m.kappa)
      ~class_f:f_class)
;;

(** F_asset_h(d), translated F_class_h(d) and the kappa blend at drawdown [d]. *)
let blended_coverage (m : blend_model) ~(d_surv : float) : coverage_at_d =
  let n = Array.length m.index.sigma in
  if n = 0
  then { n_asset = m.index.n_asset; asset = 0.0; class_ = 0.0; blended = 0.0 }
  else (
    let sqrt_h = sqrt (float_of_int m.horizon.sessions) in
    let f_asset = float_of_int (count_le m.index.mfd_sorted d_surv) /. float_of_int n in
    let cls = ref 0.0 in
    Array.iter
      (fun sigma ->
         let tau = if sigma > 0.0 then d_surv /. (sigma *. sqrt_h) else Float.infinity in
         cls := !cls +. Survival_classes.z_cdf_of m.index.class_z ~tau)
      m.index.sigma;
    let f_class = !cls /. float_of_int n in
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

let blend_model_of
      ?(weight_by_sessions = true)
      ?(stride = 1)
      ~(horizon : horizon)
      ~(asset : series)
      ~(class_members : series list)
      ~(kappa : int)
      ~(warmup : int)
      ()
  : blend_model
  =
  let index =
    blend_index_of ~horizon ~asset ~class_members ~warmup ~weight_by_sessions ~stride
  in
  { horizon; asset; class_members; kappa; warmup; weight_by_sessions; stride; index }
;;

(** Smallest drawdown d in (0, 1) whose coverage [f](d) reaches [target].
    Empirical coverage functions used here are monotone non-decreasing in d,
    so a bisection is sound - unlike replay D_surv, which is path-dependent
    and not monotone in capital. *)
let d_for_coverage ~(f : float -> float) ~(target : float) =
  if target <= 0.0
  then 0.0
  else (
    let rec bisect lo hi i =
      if i = 0
      then lo
      else (
        let mid = (lo +. hi) /. 2.0 in
        if f mid >= target then bisect lo mid (i - 1) else bisect mid hi (i - 1))
    in
    bisect 0.0 0.999999 60)
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

(** Quote capital that exactly funds [n_fills] ladder buys (closed form). *)
let capital_for_fills ~(grid : Grid_core.config) ~(n_fills : int) =
  Survival_mfd.static_runway_cost
    ~qty:grid.qty
    ~grid_interval_pct:grid.grid_interval_pct
    ~fee:grid.maker_fee
    ~start_price:grid.start_price
    ~n_fills
;;

(** Static drawdown survived by [n_fills] ladder steps. *)
let drawdown_of_fills ~(grid : Grid_core.config) ~(n_fills : int) =
  let gi = grid.grid_interval_pct /. 100.0 in
  1.0 -. ((1.0 -. gi) ** float_of_int n_fills)
;;

(** Inverse sizing: smallest [capital] whose static runway survives the
    drawdown d* (the smallest d with F_blend(d) >= target). The CDF is monotone
    in d, and the runway cost is a closed-form monotone function of the fill
    count, so this is well-defined even though path-replay D_surv is not
    monotone in capital. Returns [reachable = false] when the required capital
    exceeds [hi] (or the target would need surviving the entire history with
    certainty). *)
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
    { parameter = "capital"; value = capital; d_surv; coverage; reachable = true })
;;

(** Inverse sizing: largest [qty] whose static runway (given the grid's
    [start_quote]) survives the drawdown d* (the smallest d with
    F_blend(d) >= target). The runway cost is linear in qty, so the boundary is
    closed form. Returns [reachable = false] when even [qty_increment] is too
    large (or the target would need surviving the entire history with
    certainty). *)
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
  else { parameter = "qty"; value = qty; d_surv; coverage; reachable = true }
;;

(** Empirical min capital (advisory): the smallest [start_quote] whose actual
    Grid_core path replay clears [target_survival] on the asset's own history.

    The static sizing ([find_min_capital]) evaluates the closed-form worst
    case - N consecutive ladder buys with no intermediate sells - which is the
    theoretical lower bound a path could hit, so it is structurally
    pessimistic: real paths bounce, sells free quote, and the grid survives
    deeper than the static runway predicts. The empirical number measures the
    "capital buffer" the static sizing pays for.

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
