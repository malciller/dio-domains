(* Survival MFD - maximum fractional drawdown and its empirical CDF/survival.

   Definitions (per the plan and review):
     MFD(s,h) = 1 - min{ L_u : u in (s, s+h] } / C_s      (half-open, lows only)
     F_h(d)   = P(MFD_h <= d)                              (coverage, a CDF)
     S_h(d)   = 1 - F_h(d)                                 (survival)

   All windows are index-space over a session series built by Survival_calendar.
   The no-lookahead sigma invariant lives in Survival_stats: volatility windows
   only use bars <= s.

   Also hosts the closed-form static drawdown runway used to cross-check the
   Grid_core replay:
     C_used(N) = (1+fee) * q * C_s * (1-gi) * (1-(1-gi)^N) / gi
     N* = max N with C_used(N) <= C  =>  D_surv = 1-(1-gi)^N*
   and the floor-aware variant [floor_aware_runway_cost], which walks the same
   ladder with Grid_core's dynamic buy up-sizing (q_k = max(qty, ceil_lot
   (min_notional / level_k))) so the static sizing stays conservative when the
   venue's notional floor binds. *)

open Survival_types

(** MFD from start session [s] over [horizon] sessions. None when the window is
    out of range or [horizon] <= 0. *)
let mfd ~closes ~lows ~start ~horizon =
  let n = Array.length closes in
  if n = 0 || start < 0 || start >= n || horizon <= 0
  then None
  else (
    let c = closes.(start) in
    if c <= 0.0
    then None
    else (
      let fin = min n (start + horizon + 1) in
      if fin <= start + 1
      then None
      else (
        let m = ref lows.(start + 1) in
        for i = start + 2 to fin - 1 do
          if lows.(i) < !m then m := lows.(i)
        done;
        Some (1.0 -. (!m /. c)))))
;;

(** Empirical F_h(d): share of valid start sessions in [warmup, n-horizon-1]
    whose MFD over the next [horizon] sessions is <= [d]. Raises when no valid
    start exists: a coverage of 0.0 from zero observations is meaningless, not
    informative, and must not masquerade as "every window drew down more than
    [d]". [stride] steps through starts (default 1 = every session); stride =
    horizon gives non-overlapping windows. *)
let f_h ~closes ~lows ~horizon ~threshold ~warmup ?(stride = 1) () =
  let n = Array.length closes in
  let stride = max 1 stride in
  let hits = ref 0 in
  let total = ref 0 in
  let s = ref warmup in
  while !s <= n - 1 do
    (match mfd ~closes ~lows ~start:!s ~horizon with
     | Some m ->
       incr total;
       if m <= threshold then incr hits
     | None -> ());
    s := !s + stride
  done;
  if !total = 0
  then
    invalid_arg
      (Printf.sprintf
         "Survival_mfd.f_h: empty distribution for horizon %d warmup %d: no valid start \
          window (needs >= warmup + horizon + 2 sessions)"
         horizon
         warmup)
  else float_of_int !hits /. float_of_int !total
;;

let survival ~closes ~lows ~horizon ~threshold ~warmup ?(stride = 1) () =
  1.0 -. f_h ~closes ~lows ~horizon ~threshold ~warmup ~stride ()
;;

(** Number of valid per-start MFD windows in [warmup, n-horizon-1]. This is the
    [n_asset] weight for the kappa blend. [stride] mirrors [f_h]. *)
let n_starts ~closes ~lows ~horizon ~warmup ?(stride = 1) () =
  let n = Array.length closes in
  let stride = max 1 stride in
  let total = ref 0 in
  let s = ref warmup in
  while !s <= n - 1 do
    (match mfd ~closes ~lows ~start:!s ~horizon with
     | Some _ -> incr total
     | None -> ());
    s := !s + stride
  done;
  !total
;;

(** Per-start MFD samples over [warmup, n-horizon-1]. Shared by the asset
    percentile table and the blended-percentile inversion. [stride] mirrors
    [f_h]. *)
let samples ~closes ~lows ~horizon ~warmup ?(stride = 1) () =
  let n = Array.length closes in
  let stride = max 1 stride in
  let xs = ref [] in
  let s = ref warmup in
  while !s <= n - 1 do
    (match mfd ~closes ~lows ~start:!s ~horizon with
     | Some m -> xs := m :: !xs
     | None -> ());
    s := !s + stride
  done;
  Array.of_list (List.rev !xs)
;;

(** Survival surface for one horizon across drawdown thresholds. *)
let surface ~closes ~lows ~horizon:(h : horizon) ~thresholds_pct ~warmup
  : survival_surface
  =
  let n_starts = n_starts ~closes ~lows ~horizon:h.sessions ~warmup () in
  let rows =
    List.map
      (fun d ->
         let d_frac = d /. 100.0 in
         let f = f_h ~closes ~lows ~horizon:h.sessions ~threshold:d_frac ~warmup () in
         { drawdown_pct = d; coverage = f; survival = 1.0 -. f })
      thresholds_pct
  in
  { horizon_label = h.label; calendar_days = h.calendar_days; n_starts; rows }
;;

(** Percentile table for one horizon: the MFD level not exceeded with
    probability p. Uses the empirical CDF of per-start MFDs. Rows are
    estimated from non-overlapping windows (stride = horizon sessions) so tail
    percentiles are not dominated by one contiguous crash counted once per
    overlapping start; [n_starts] reports the full (overlapping) window count
    and [n_eff] the number of independent windows actually used. *)
let percentile_table ~closes ~lows ~horizon:(h : horizon) ~percentiles ~warmup
  : percentile_table
  =
  let n_starts = n_starts ~closes ~lows ~horizon:h.sessions ~warmup () in
  let arr = samples ~closes ~lows ~horizon:h.sessions ~warmup ~stride:h.sessions () in
  let n_eff = Array.length arr in
  let rows =
    List.map
      (fun p -> { percentile = p; mfd = Survival_math.percentile arr p })
      percentiles
  in
  { horizon_label = h.label; calendar_days = h.calendar_days; n_starts; n_eff; rows }
;;

(* ---- Closed-form static drawdown runway ---- *)

let static_runway_cost ~qty ~grid_interval_pct ~fee ~start_price ~n_fills =
  let gi = grid_interval_pct /. 100.0 in
  let first = start_price *. (1.0 -. gi) in
  (1.0 +. fee) *. qty *. first *. ((1.0 -. ((1.0 -. gi) ** float_of_int n_fills)) /. gi)
;;

(** Number of consecutive ladder buys affordable from [capital]; >= 0. The
    closed-form ladder cost converges to the total geometric sum
    (1+fee)*qty*first/gi as the fill count grows, so capital at or above that
    limit funds the whole ladder - the recursion must terminate there instead
    of searching forever. *)
let max_affordable_fills ~qty ~grid_interval_pct ~fee ~start_price ~capital =
  if capital <= 0.0
  then 0
  else (
    let gi = grid_interval_pct /. 100.0 in
    let first = start_price *. (1.0 -. gi) in
    let total_ladder = (1.0 +. fee) *. qty *. first /. gi in
    if capital >= total_ladder
    then
      (* Capital funds the entire ladder: cap at the fill count whose drawdown
         is within 1e-12 of 100% - beyond it the runways differ by less than
         the closed form's own float precision, so the count is arbitrary. *)
      max 1 (int_of_float (Float.ceil (Float.log 1e-12 /. Float.log (1.0 -. gi))))
    else (
      (* capital < total_ladder, so the cost strictly increases to a limit
         above capital and the recursion terminates. *)
      let rec go n =
        let cost =
          static_runway_cost ~qty ~grid_interval_pct ~fee ~start_price ~n_fills:(n + 1)
        in
        if cost <= capital then go (n + 1) else n
      in
      go 0))
;;

(** Static (closed-form) drawdown the grid survives before quote capital can no
    longer fund the next ladder buy. This is the D_surv behind
    historical_path_coverage = F_h(D_surv). *)
let static_drawdown_runway ~qty ~grid_interval_pct ~fee ~start_price ~capital =
  let n = max_affordable_fills ~qty ~grid_interval_pct ~fee ~start_price ~capital in
  let gi = grid_interval_pct /. 100.0 in
  let drawdown = 1.0 -. ((1.0 -. gi) ** float_of_int n) in
  n, drawdown
;;

(** Floor-aware runway cost of [n_fills] ladder buys: walks the grid's own
    ladder (same rounding as Grid_core.buy_level / sell_level /
    trail_buy_level) with dynamic buy up-sizing (q_k = max(qty,
    ceil_lot(min_notional / level_k)) per Grid_core.required_buy_qty), so the
    cost is exact when the venue floor binds and reduces to the closed-form
    geometric sum when it does not. The closed-form [static_runway_cost]
    assumes a fixed qty at every rung and therefore UNDERSTATES the true
    requirement whenever min_notional forces up-sizing deep in the ladder;
    this variant is the conservative bound used by the sizing layer. *)
let floor_aware_runway_cost
      ~qty
      ~grid_interval_pct
      ~fee
      ~start_price
      ~min_notional
      ~price_increment
      ~qty_increment
      ~n_fills
  =
  let gi = grid_interval_pct /. 100.0 in
  let inv_p = 1.0 /. price_increment in
  let round_price p = Float.round (p *. inv_p) /. inv_p in
  let inv_q = 1.0 /. qty_increment in
  let ceil_lot q = Float.ceil ((q *. inv_q) -. 1e-9) /. inv_q in
  let level = ref (round_price (start_price *. (1.0 -. gi))) in
  let acc = ref 0.0 in
  for _ = 1 to n_fills do
    let floor_q = if min_notional > 0.0 then ceil_lot (min_notional /. !level) else 0.0 in
    let q = Float.max qty floor_q in
    acc := !acc +. (q *. !level *. (1.0 +. fee));
    let sell = round_price (!level *. (1.0 +. gi)) in
    let grid_buy = round_price (!level *. (1.0 -. gi)) in
    let exact = round_price (sell -. (!level *. (2.0 *. gi))) in
    level := Float.min grid_buy exact
  done;
  !acc
;;
