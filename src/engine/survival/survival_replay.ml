(* Survival_replay - Grid_core path replay and historical path coverage.

   Replays the grid over the asset's OHLC history (pessimistic Buy_first
   ordering) and extracts the survival event: D_surv =
   first_capital_low_drawdown, or 100% when the grid never runs dry (it then
   survived every drawdown the history produced, so F_h(1.0) = 1.0). The
   headline number is historical_path_coverage = F_blend_h(D_surv): the share
   of the asset's own history (blended toward the class) whose max drawdown the
   grid would have survived, with a target-survival probability.

   Inverse sizing: path-replay D_surv is not monotone in capital (intermediate
   sells shift which ladder level exhausts the grid), so sizing inverts the
   drawdown CDF instead - the smallest d with F_blend(d) >= target, then the
   closed-form static runway capital/qty that funds enough ladder fills to
   survive that drawdown ("how much runway do I need"). *)

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

(** Everything needed to evaluate F_blend_h(d) for one horizon. *)
type blend_model =
  { horizon : horizon
  ; asset : series
  ; class_members : series list
  ; kappa : int
  ; warmup : int
  ; weight_by_sessions : bool
  }

let asset_closes_lows (s : series) =
  let bars = Survival_calendar.sort_bars s.bars |> Survival_calendar.dedup in
  Array.map (fun b -> b.close) bars, Array.map (fun b -> b.low) bars
;;

type coverage_at_d =
  { n_asset : int
  ; asset : float
  ; class_ : float
  ; blended : float
  }

(** F_asset_h(d), pooled F_class_h(d) and the kappa blend at drawdown [d]. *)
let blended_coverage (m : blend_model) ~(d_surv : float) : coverage_at_d =
  let closes, lows = asset_closes_lows m.asset in
  let f_asset =
    Survival_mfd.f_h
      ~closes
      ~lows
      ~horizon:m.horizon.sessions
      ~threshold:d_surv
      ~warmup:m.warmup
  in
  let f_class =
    Survival_classes.pooled_cdf
      ~weight_by_sessions:m.weight_by_sessions
      ~members:m.class_members
      ~horizon:m.horizon.sessions
      ~threshold:d_surv
      ~warmup:m.warmup
      ()
  in
  let n_asset =
    Survival_mfd.n_starts ~closes ~lows ~horizon:m.horizon.sessions ~warmup:m.warmup
  in
  let f_blend =
    Survival_stats.blend
      ~n_asset:(float_of_int n_asset)
      ~asset_f:f_asset
      ~kappa:(float_of_int m.kappa)
      ~class_f:f_class
  in
  { n_asset; asset = f_asset; class_ = f_class; blended = f_blend }
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
      ~(horizon : horizon)
      ~(asset : series)
      ~(class_members : series list)
      ~(kappa : int)
      ~(warmup : int)
      ()
  : blend_model
  =
  { horizon; asset; class_members; kappa; warmup; weight_by_sessions }
;;

(** Smallest drawdown d in (0, 1) whose blended coverage F_blend(d) reaches
    [target]. F_blend is monotone non-decreasing in d (an empirical CDF), so a
    bisection is sound - unlike replay D_surv, which is path-dependent and not
    monotone in capital. *)
let drawdown_for_target ~(model : blend_model) ~(target_survival : float) =
  if target_survival <= 0.0
  then 0.0
  else (
    let f d = (blended_coverage model ~d_surv:d).blended in
    let rec bisect lo hi i =
      if i = 0
      then lo
      else (
        let mid = (lo +. hi) /. 2.0 in
        if f mid >= target_survival then bisect lo mid (i - 1) else bisect mid hi (i - 1))
    in
    bisect 0.0 0.999999 60)
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
