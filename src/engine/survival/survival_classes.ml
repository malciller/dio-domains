(* Survival classes - risk-class definitions and pooled curve estimation
   (Phase 2).

   Class membership is not hardcoded here: the asset -> class assignment lives
   on each config.json trading entry ("asset_class"), and the class -> member
   pool comes from the top-level config.json "classes" map (or the CLI
   --members override). Each class curve is estimated from the pooled per-start
   MFD samples of all its member series (Survival_mfd.samples), reusing the
   same empirical-CDF machinery as the asset curve. Two weighting modes (a
   flagged tunable):
   - session-count weighting (default): every member start is one sample, so
     longer histories dominate;
   - equal-weight per member: each member's history contributes weight 1/m
     (1/n_member per start).

   The same pooled sample set backs the class surface, the pooled percentile
   table, and the vol-normalized z-index (Survival_classes.z_index_of) that
   drives the kappa blend: the class contributes F_class^z evaluated at each
   asset start's own vol regime (Survival_replay.blended_f), so a low-vol
   asset is not punished for a high-vol classmate's raw swing size. *)

open Survival_types

(** Pooled weighted (value, weight) samples across members. *)
type pooled =
  { samples : (float * float) array
  ; n_members : int
  }

let member_bars (s : series) =
  s.bars |> Survival_calendar.sort_bars |> Survival_calendar.dedup
;;

let pooled
      ?(weight_by_sessions = true)
      ~(members : series list)
      ~horizon
      ~warmup
      ?(stride = 1)
      ()
  : pooled
  =
  let acc = ref [] in
  List.iter
    (fun s ->
       let bars = member_bars s in
       let closes = Array.map (fun b -> b.close) bars in
       let lows = Array.map (fun b -> b.low) bars in
       let xs = Survival_mfd.samples ~closes ~lows ~horizon ~warmup ~stride () in
       let n = Array.length xs in
       let w =
         if n = 0 then 0.0 else if weight_by_sessions then 1.0 else 1.0 /. float_of_int n
       in
       Array.iter (fun x -> acc := (x, w) :: !acc) xs)
    members;
  { samples = Array.of_list (List.rev !acc); n_members = List.length members }
;;

(** Weighted empirical CDF of a pooled sample set at [threshold]. *)
let cdf_of (p : pooled) ~threshold =
  let hits = ref 0.0 in
  let total = ref 0.0 in
  Array.iter
    (fun (v, w) ->
       total := !total +. w;
       if v <= threshold then hits := !hits +. w)
    p.samples;
  if !total <= 0.0 then 0.0 else !hits /. !total
;;

(** Pooled class surface: empirical CDF over the pooled member starts. *)
let class_surface
      ?weight_by_sessions
      ~(members : series list)
      ~horizon:(h : horizon)
      ~thresholds_pct
      ~warmup
      ()
  : survival_surface
  =
  let p = pooled ?weight_by_sessions ~members ~horizon:h.sessions ~warmup () in
  let rows =
    List.map
      (fun d ->
         let f = cdf_of p ~threshold:(d /. 100.0) in
         { drawdown_pct = d; coverage = f; survival = 1.0 -. f })
      thresholds_pct
  in
  { horizon_label = h.label
  ; calendar_days = h.calendar_days
  ; n_starts = Array.length p.samples
  ; rows
  }
;;

(** Pooled class percentile table over the same sample set, estimated from
    non-overlapping windows (stride = horizon sessions) so tail percentiles
    are not autocorrelation-dominated; [n_starts] is the full overlapping
    window count and [n_eff] the independent-window count. *)
let class_percentile_table
      ?weight_by_sessions
      ~(members : series list)
      ~horizon:(h : horizon)
      ~percentiles
      ~warmup
      ()
  : percentile_table
  =
  let n_starts =
    (pooled ?weight_by_sessions ~members ~horizon:h.sessions ~warmup ()).samples
    |> Array.length
  in
  let p =
    pooled ?weight_by_sessions ~members ~horizon:h.sessions ~warmup ~stride:h.sessions ()
  in
  let n_eff = Array.length p.samples in
  let rows =
    List.map
      (fun pt ->
         { percentile = pt; mfd = Survival_math.weighted_percentile p.samples pt })
      percentiles
  in
  { horizon_label = h.label; calendar_days = h.calendar_days; n_starts; n_eff; rows }
;;

(* ---- Vol-normalized pooled z-CDF (for the vol-normalized blend) ---- *)

(** Pooled class z-CDF, precomputed as a sorted weighted z-array with prefix
    sums so evaluation is O(log n) per threshold (the z-blend evaluates it once
    per asset start per drawdown, so linear scans would be too slow inside the
    coverage bisection). [stride] mirrors Survival_mfd.samples. *)
type z_index =
  { sorted : (float * float) array (** (z, weight) pairs, sorted ascending by z. *)
  ; prefix : float array (** prefix.(i) = sum of weights of pairs 0..i. *)
  ; total : float
  ; n : int
  }

let z_index_of
      ?(weight_by_sessions = true)
      ~(members : series list)
      ~horizon
      ~vol_window
      ~warmup
      ?(stride = 1)
      ()
  : z_index
  =
  let stride = max 1 stride in
  let acc = ref [] in
  List.iter
    (fun s ->
       let bars = member_bars s in
       let closes = Array.map (fun b -> b.close) bars in
       let lows = Array.map (fun b -> b.low) bars in
       let zs = ref [] in
       let i = ref warmup in
       while !i <= Array.length closes - 1 do
         (match Survival_stats.z_mfd ~closes ~lows ~s:!i ~horizon ~w:vol_window with
          | Some z -> zs := z :: !zs
          | None -> ());
         i := !i + stride
       done;
       let arr = Array.of_list (List.rev !zs) in
       let n = Array.length arr in
       let w =
         if n = 0 then 0.0 else if weight_by_sessions then 1.0 else 1.0 /. float_of_int n
       in
       Array.iter (fun z -> acc := (z, w) :: !acc) arr)
    members;
  let pairs = Array.of_list (List.rev !acc) in
  Array.sort (fun (a, _) (b, _) -> Float.compare a b) pairs;
  let n = Array.length pairs in
  let prefix = Array.make n 0.0 in
  let total = ref 0.0 in
  Array.iteri
    (fun i (_, w) ->
       total := !total +. w;
       prefix.(i) <- !total)
    pairs;
  { sorted = pairs; prefix; total = !total; n }
;;

(** F_class^z(tau): weighted share of pooled class z-samples <= [tau]. O(log n)
    via the prefix sums; returns 0.0 for an empty pool. *)
let z_cdf_of (i : z_index) ~(tau : float) =
  if i.total <= 0.0
  then 0.0
  else (
    (* Upper bound: first index with z > tau. *)
    let lo = ref 0 in
    let hi = ref i.n in
    while !lo < !hi do
      let mid = (!lo + !hi) / 2 in
      if fst i.sorted.(mid) <= tau then lo := mid + 1 else hi := mid
    done;
    if !lo = 0 then 0.0 else i.prefix.(!lo - 1) /. i.total)
;;

let pooled_z_cdf
      ?weight_by_sessions
      ~(members : series list)
      ~horizon
      ~threshold
      ~vol_window
      ~warmup
      ()
  =
  let p = z_index_of ?weight_by_sessions ~members ~horizon ~vol_window ~warmup () in
  z_cdf_of p ~tau:threshold
;;
