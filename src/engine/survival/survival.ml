(* DIO Capital Survival Engine - facade.

   analyze : series -> config -> result
     computes, for each horizon: the survival surface (F_h(d) per threshold),
     the MFD percentile table, the pooled class surfaces, and the kappa-blended
     surfaces + percentile tables. Fails-fast on excessive gaps (max_gap >
     tolerance), enforcing the "never forward-fill" rule.

   Phase 2 lands on top of Phase 1: class curves estimated from pooled member
   history (Survival_classes), the kappa blend (Survival_stats.blend), and the
   equity expected-session model (Survival_sessions) drive equity gap
   detection. Grid path replay / historical path coverage live in
   Survival_replay; live-engine wiring stays out of scope (Phase 3). *)

open Survival_types

type class_input =
  { name : string
  ; kappa : int
  ; members : series list
  }

type config =
  { horizons : horizon list
  ; thresholds_pct : float list
  ; percentiles : float list
  ; vol_window : int
  ; gap_tolerance : int
  ; classes : class_input list
  ; equity_sessions : Survival_sessions.model option
  ; weight_by_sessions : bool
  }

type result =
  { series : series
  ; n_sessions : int
  ; first_date : string
  ; last_date : string
  ; max_gap : int
  ; gaps : gap list
  ; surfaces : survival_surface list
  ; percentile_tables : percentile_table list
  ; class_estimates : class_estimate list
  ; blended_surfaces : blended_surface list
  ; blended_percentile_tables : blended_percentile_table list
  }

let default_horizons ~(calendar_kind : calendar_kind) =
  let mk n =
    { label = horizon_label calendar_kind n
    ; sessions = n
    ; calendar_days = calendar_days_of_sessions calendar_kind n
    }
  in
  match calendar_kind with
  | Crypto -> List.map mk [ 30; 90; 180; 365 ]
  | Equity -> List.map mk [ 21; 63; 126; 252 ]
;;

let default_thresholds_pct = [ 5.; 10.; 20.; 30.; 40.; 50. ]
let default_percentiles = [ 50.; 75.; 90.; 95.; 99. ]

let default_config ~(calendar_kind : calendar_kind) =
  { horizons = default_horizons ~calendar_kind
  ; thresholds_pct = default_thresholds_pct
  ; percentiles = default_percentiles
  ; vol_window = 60
  ; gap_tolerance = 5
  ; classes = []
  ; equity_sessions = None
  ; weight_by_sessions = true
  }
;;

(** Per-start MFD samples of a bar series over [warmup, n-horizon-1]. *)
let asset_samples ~(series : series) ~horizon ~warmup ?(stride = 1) () =
  let bars = series.bars |> Survival_calendar.sort_bars |> Survival_calendar.dedup in
  let closes = Array.map (fun b -> b.close) bars in
  let lows = Array.map (fun b -> b.low) bars in
  Survival_mfd.samples ~closes ~lows ~horizon ~warmup ~stride ()
;;

(** Blended percentile table for one horizon: invert the unified z-blend
    (Survival_replay.blended_f on a non-overlapping stride model) per
    percentile - the smallest d with F_blend(d) >= p/100. The blend is
    monotone in d, so bisection is exact for the empirical CDF. Rows are
    estimated on non-overlapping windows so the tail is autocorrelation-honest;
    [n_starts]/[n_eff] mirror the asset percentile tables. *)
let blended_percentile_table
      ~(asset : series)
      ~(class_input : class_input)
      ~(h : horizon)
      ~percentiles
      ~warmup
      ~weight_by_sessions
  : blended_percentile_table
  =
  let model =
    Survival_replay.blend_model_of
      ~horizon:h
      ~asset
      ~class_members:class_input.members
      ~kappa:class_input.kappa
      ~warmup
      ~weight_by_sessions
      ~stride:h.sessions
      ()
  in
  let rows =
    List.map
      (fun p ->
         { percentile = p
         ; mfd =
             Survival_replay.d_for_coverage
               ~f:(fun d -> Survival_replay.blended_f model ~d)
               ~target:(p /. 100.0)
         })
      percentiles
  in
  { class_name = class_input.name
  ; table =
      { horizon_label = h.label
      ; calendar_days = h.calendar_days
      ; n_starts =
          asset_samples ~series:asset ~horizon:h.sessions ~warmup () |> Array.length
      ; n_eff =
          asset_samples ~series:asset ~horizon:h.sessions ~warmup ~stride:h.sessions ()
          |> Array.length
      ; rows
      }
  }
;;

(** One class across all horizons: pooled class curves plus the blended surface
    and blended percentile table per horizon. [warmup] is the (already
    series-adapted) warmup the asset surfaces were computed with, so the class
    and blend estimates sit on the same window basis as the asset curves. *)
let estimate_class
      ~(asset : series)
      ~(class_input : class_input)
      ~(config : config)
      ~warmup
      ~(asset_surfaces : survival_surface list)
  : class_estimate * blended_surface list * blended_percentile_table list
  =
  let cs = ref [] in
  let cts = ref [] in
  let bs = ref [] in
  let bts = ref [] in
  List.iter2
    (fun (h : horizon) (asset_surface : survival_surface) ->
       let class_surface =
         Survival_classes.class_surface
           ~weight_by_sessions:config.weight_by_sessions
           ~members:class_input.members
           ~horizon:h
           ~thresholds_pct:config.thresholds_pct
           ~warmup
           ()
       in
       let class_table =
         Survival_classes.class_percentile_table
           ~weight_by_sessions:config.weight_by_sessions
           ~members:class_input.members
           ~horizon:h
           ~percentiles:config.percentiles
           ~warmup
           ()
       in
       let n_asset = asset_surface.n_starts in
       let model =
         Survival_replay.blend_model_of
           ~horizon:h
           ~asset
           ~class_members:class_input.members
           ~kappa:class_input.kappa
           ~warmup
           ~weight_by_sessions:config.weight_by_sessions
           ()
       in
       let blended_rows =
         List.map
           (fun (ar : surface_row) ->
              let f = Survival_replay.blended_f model ~d:(ar.drawdown_pct /. 100.0) in
              { drawdown_pct = ar.drawdown_pct; coverage = f; survival = 1.0 -. f })
           asset_surface.rows
       in
       let blended_surface =
         { class_name = class_input.name
         ; surface =
             { horizon_label = h.label
             ; calendar_days = h.calendar_days
             ; n_starts = n_asset
             ; rows = blended_rows
             }
         }
       in
       let blended_table =
         blended_percentile_table
           ~asset
           ~class_input
           ~h
           ~percentiles:config.percentiles
           ~warmup
           ~weight_by_sessions:config.weight_by_sessions
       in
       cs := class_surface :: !cs;
       cts := class_table :: !cts;
       bs := blended_surface :: !bs;
       bts := blended_table :: !bts)
    config.horizons
    asset_surfaces;
  ( { class_name = class_input.name
    ; kappa = class_input.kappa
    ; member_count = List.length class_input.members
    ; surfaces = List.rev !cs
    ; percentile_tables = List.rev !cts
    }
  , List.rev !bs
  , List.rev !bts )
;;

let analyze series config =
  let bars = series.bars |> Survival_calendar.sort_bars |> Survival_calendar.dedup in
  let is_session = Option.map Survival_sessions.is_session config.equity_sessions in
  let gaps =
    Survival_calendar.detect_gaps ~calendar_kind:series.calendar_kind ?is_session bars
  in
  let max_gap = Survival_calendar.max_gap gaps in
  if max_gap > config.gap_tolerance
  then
    failwith
      (Printf.sprintf
         "Survival.analyze: %s has %d-session gap(s) (tolerance %d); refusing to \
          forward-fill. Last: %s -> %s"
         series.symbol
         max_gap
         config.gap_tolerance
         (if gaps = [] then "-" else (List.hd gaps).after)
         (if gaps = [] then "-" else (List.hd gaps).before));
  let closes = Array.map (fun b -> b.close) bars in
  let lows = Array.map (fun b -> b.low) bars in
  let n = Array.length bars in
  let warmup = min (n - 1) config.vol_window in
  let surfaces =
    List.map
      (fun h ->
         Survival_mfd.surface
           ~closes
           ~lows
           ~horizon:h
           ~thresholds_pct:config.thresholds_pct
           ~warmup)
      config.horizons
  in
  let percentile_tables =
    List.map
      (fun h ->
         Survival_mfd.percentile_table
           ~closes
           ~lows
           ~horizon:h
           ~percentiles:config.percentiles
           ~warmup)
      config.horizons
  in
  let class_estimates = ref [] in
  let blended_surfaces = ref [] in
  let blended_tables = ref [] in
  List.iter
    (fun (ci : class_input) ->
       let est, bs, bts =
         estimate_class
           ~asset:series
           ~class_input:ci
           ~config
           ~warmup
           ~asset_surfaces:surfaces
       in
       class_estimates := est :: !class_estimates;
       blended_surfaces := !blended_surfaces @ bs;
       blended_tables := !blended_tables @ bts)
    config.classes;
  { series
  ; n_sessions = n
  ; first_date = (if n = 0 then "" else bars.(0).date)
  ; last_date = (if n = 0 then "" else bars.(n - 1).date)
  ; max_gap
  ; gaps
  ; surfaces
  ; percentile_tables
  ; class_estimates = List.rev !class_estimates
  ; blended_surfaces = !blended_surfaces
  ; blended_percentile_tables = !blended_tables
  }
;;
