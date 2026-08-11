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
   table, and the vol-normalized z-CDF (Survival_stats.z_mfd), which are the
   inputs to the kappa blend. *)

open Survival_types

(** Pooled weighted (value, weight) samples across members. *)
type pooled =
  { samples : (float * float) array
  ; n_members : int
  }

let member_bars (s : series) =
  s.bars |> Survival_calendar.sort_bars |> Survival_calendar.dedup
;;

let pooled ?(weight_by_sessions = true) ~(members : series list) ~horizon ~warmup ()
  : pooled
  =
  let acc = ref [] in
  List.iter
    (fun s ->
       let bars = member_bars s in
       let closes = Array.map (fun b -> b.close) bars in
       let lows = Array.map (fun b -> b.low) bars in
       let xs = Survival_mfd.samples ~closes ~lows ~horizon ~warmup in
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

let pooled_cdf ?weight_by_sessions ~(members : series list) ~horizon ~threshold ~warmup ()
  =
  let p = pooled ?weight_by_sessions ~members ~horizon ~warmup () in
  cdf_of p ~threshold
;;

let pooled_survival
      ?weight_by_sessions
      ~(members : series list)
      ~horizon
      ~threshold
      ~warmup
      ()
  =
  1.0 -. pooled_cdf ?weight_by_sessions ~members ~horizon ~threshold ~warmup ()
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

(** Pooled class percentile table over the same sample set. *)
let class_percentile_table
      ?weight_by_sessions
      ~(members : series list)
      ~horizon:(h : horizon)
      ~percentiles
      ~warmup
      ()
  : percentile_table
  =
  let p = pooled ?weight_by_sessions ~members ~horizon:h.sessions ~warmup () in
  let rows =
    List.map
      (fun pt ->
         { percentile = pt; mfd = Survival_math.weighted_percentile p.samples pt })
      percentiles
  in
  { horizon_label = h.label
  ; calendar_days = h.calendar_days
  ; n_starts = Array.length p.samples
  ; rows
  }
;;

(* ---- Vol-normalized pooled z-CDF (for the vol-normalized blend) ---- *)

let pooled_z
      ?(weight_by_sessions = true)
      ~(members : series list)
      ~horizon
      ~vol_window
      ~warmup
      ()
  : pooled
  =
  let acc = ref [] in
  List.iter
    (fun s ->
       let bars = member_bars s in
       let closes = Array.map (fun b -> b.close) bars in
       let lows = Array.map (fun b -> b.low) bars in
       let zs = ref [] in
       for i = warmup to Array.length closes - 1 do
         match Survival_stats.z_mfd ~closes ~lows ~s:i ~horizon ~w:vol_window with
         | Some z -> zs := z :: !zs
         | None -> ()
       done;
       let arr = Array.of_list (List.rev !zs) in
       let n = Array.length arr in
       let w =
         if n = 0 then 0.0 else if weight_by_sessions then 1.0 else 1.0 /. float_of_int n
       in
       Array.iter (fun z -> acc := (z, w) :: !acc) arr)
    members;
  { samples = Array.of_list (List.rev !acc); n_members = List.length members }
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
  let p = pooled_z ?weight_by_sessions ~members ~horizon ~vol_window ~warmup () in
  cdf_of p ~threshold
;;
