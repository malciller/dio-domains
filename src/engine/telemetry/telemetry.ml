(** Lightweight Telemetry System for Performance Tracking *)

let section = "telemetry"

(** Sliding window counter for high-frequency metrics *)
type sliding_counter = {
  window_size: int;  (* Maximum number of samples to keep *)
  current_window: (float * int) list ref;  (* (timestamp, increment) pairs *)
  total_count: int ref;  (* Running total within window *)
  last_cleanup: float ref;  (* Last time we cleaned old entries *)
}

(** Rate tracking for counters *)
type rate_sample = {
  timestamp: float;
  value: int;
}

type rate_tracker = {
  samples: rate_sample list ref;
  window_seconds: float;
  mutable samples_since_cleanup: int;  (* Track samples added since last cleanup *)
}

(** Metric types *)
type metric_type =
  | Counter of int ref
  | SlidingCounter of sliding_counter
  | Gauge of float ref
  | Histogram of float list ref

type metric = {
  name: string;
  metric_type: metric_type;
  labels: (string * string) list;
  created_at: float;
  mutable last_updated: float;
  rate_tracker: rate_tracker option;
  mutable cached_rate: float;  (* Cached rate for UI display *)
}

(** Global metrics store *)
let metrics : (string, metric) Hashtbl.t = Hashtbl.create 128
let metrics_mutex = Mutex.create ()
let metrics_changed_condition = Lwt_condition.create ()

(** Helper to generate metric key *)
let metric_key name labels =
  let label_str = 
    labels 
    |> List.sort (fun (k1, _) (k2, _) -> String.compare k1 k2)
    |> List.map (fun (k, v) -> k ^ "=" ^ v)
    |> String.concat ","
  in
  if label_str = "" then name else name ^ "{" ^ label_str ^ "}"

(** Sliding window counter for high-frequency metrics *)
let sliding_counter name ?(labels=[]) ?(window_size=1000) ?(track_rate=false) ?(rate_window=10.0) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric =
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let rate_tracker = if track_rate then
          Some {
            samples = ref [];
            window_seconds = rate_window;
            samples_since_cleanup = 0;
          }
        else None in
        let sliding = {
          window_size;
          current_window = ref [];
          total_count = ref 0;
          last_cleanup = ref (Unix.time ());
        } in
        let m = {
          name;
          metric_type = SlidingCounter sliding;
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
          rate_tracker;
          cached_rate = 0.0;
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

(** Increment sliding window counter with automatic cleanup *)
let inc_sliding_counter metric ?(value=1) () =
  match metric.metric_type with
  | SlidingCounter sliding ->
      (* Fast path: just increment without cleanup most of the time *)
      Mutex.lock metrics_mutex;
      let now = Unix.time () in

      (* Add new increment to window *)
      sliding.current_window := (now, value) :: !(sliding.current_window);
      sliding.total_count := !(sliding.total_count) + value;
      metric.last_updated <- now;

      (* Update rate tracker if enabled *)
      (match metric.rate_tracker with
       | Some tracker ->
           let sample = { timestamp = now; value = !(sliding.total_count) } in
           tracker.samples := sample :: !(tracker.samples);
           tracker.samples_since_cleanup <- tracker.samples_since_cleanup + 1;
           (* Only filter periodically to avoid expensive operations on every increment *)
           if tracker.samples_since_cleanup >= 100 then begin
             tracker.samples_since_cleanup <- 0;
             (* Keep only samples within the time window and limit to 200 samples max *)
             let cutoff = now -. tracker.window_seconds in
             let filtered = List.filter (fun s -> s.timestamp >= cutoff) !(tracker.samples) in
             let limited = if List.length filtered > 200 then
               List.rev (List.tl (List.rev filtered))  (* Keep most recent 200 *)
             else filtered in
             tracker.samples := limited
           end
       | None -> ());

      (* Check if cleanup is needed - do this quickly *)
      let should_cleanup = now -. !(sliding.last_cleanup) > 10.0 ||
                          List.length !(sliding.current_window) > sliding.window_size in

      Mutex.unlock metrics_mutex;

      (* Do expensive cleanup outside of the main mutex if needed *)
      if should_cleanup then
        begin
          Mutex.lock metrics_mutex;
          (* Re-check condition in case another thread already cleaned up *)
          let now_check = Unix.time () in
          if now_check -. !(sliding.last_cleanup) > 10.0 ||
             List.length !(sliding.current_window) > sliding.window_size then
            begin
              sliding.last_cleanup := now_check;
              let cutoff = now_check -. 30.0 in  (* Keep last 30 seconds for rate calculation *)
              let new_window = ref [] in
              let new_total = ref 0 in

              List.iter (fun (timestamp, count) ->
                if timestamp >= cutoff then begin
                  new_window := (timestamp, count) :: !new_window;
                  new_total := !new_total + count;
                end
              ) !(sliding.current_window);

              sliding.current_window := !new_window;
              sliding.total_count := !new_total;

              (* If still too many entries, keep only the most recent ones *)
              if List.length !new_window > sliding.window_size then begin
                let sorted = List.sort (fun (t1, _) (t2, _) -> Float.compare t2 t1) !new_window in
                let recent = ref [] in
                let count = ref 0 in
                List.iter (fun (timestamp, inc) ->
                  if !count < sliding.window_size then begin
                    recent := (timestamp, inc) :: !recent;
                    count := !count + inc;
                  end
                ) sorted;
                sliding.current_window := !recent;
                sliding.total_count := !count;
              end
            end;
          Mutex.unlock metrics_mutex;
        end;

      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

(** Get current value of sliding window counter *)
let get_sliding_counter metric =
  match metric.metric_type with
  | SlidingCounter sliding -> !(sliding.total_count)
  | _ -> 0


(** Counter - incrementing metric *)
let counter name ?(labels=[]) ?(track_rate=false) ?(rate_window=10.0) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric =
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let rate_tracker = if track_rate then
          Some {
            samples = ref [];
            window_seconds = rate_window;
            samples_since_cleanup = 0;
          }
        else None in
        let m = {
          name;
          metric_type = Counter (ref 0);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
          rate_tracker;
          cached_rate = 0.0;
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

let inc_counter metric ?(value=1) () =
  match metric.metric_type with
  | Counter r ->
      Mutex.lock metrics_mutex;
      let now = Unix.time () in
      r := !r + value;
      metric.last_updated <- now;

      (* Update rate tracker if enabled *)
      (match metric.rate_tracker with
       | Some tracker ->
           let sample = { timestamp = now; value = !r } in
           tracker.samples := sample :: !(tracker.samples);
           tracker.samples_since_cleanup <- tracker.samples_since_cleanup + 1;
           (* Only filter periodically to avoid expensive operations on every increment *)
           if tracker.samples_since_cleanup >= 100 then begin
             tracker.samples_since_cleanup <- 0;
             (* Keep only samples within the time window and limit to 200 samples max *)
             let cutoff = now -. tracker.window_seconds in
             let filtered = List.filter (fun s -> s.timestamp >= cutoff) !(tracker.samples) in
             let limited = if List.length filtered > 200 then
               List.rev (List.tl (List.rev filtered))  (* Keep most recent 200 *)
             else filtered in
             tracker.samples := limited
           end
       | None -> ());

      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | SlidingCounter _ ->
      inc_sliding_counter metric ~value ()
  | _ -> ()

let get_counter metric =
  match metric.metric_type with
  | Counter r -> !r
  | SlidingCounter sliding -> !(sliding.total_count)
  | _ -> 0

(** Update cached rates and snapshot values for all metrics (called during snapshot creation) *)
let update_cached_rates () =
  Mutex.lock metrics_mutex;
  Hashtbl.iter (fun _ metric ->
    (* Cache current sliding counter values and rates for UI snapshots *)
    (match metric.metric_type with
     | SlidingCounter sliding ->
         (* Calculate rate from sliding window data for display *)
         let window = !(sliding.current_window) in
         let rate = if window = [] then 0.0
                   else
                     let total_increments = List.fold_left (fun acc (_, inc) -> acc + inc) 0 window in
                     let earliest_time = List.fold_left (fun acc (t, _) -> min acc t) (fst (List.hd window)) window in
                     let latest_time = List.fold_left (fun acc (t, _) -> max acc t) earliest_time window in
                     let time_span = max 1.0 (latest_time -. earliest_time) in  (* Avoid division by zero *)
                     float_of_int total_increments /. time_span in
         metric.cached_rate <- rate
     | _ -> ());

    (* Update rate calculations *)
    match metric.metric_type, metric.rate_tracker with
    | Counter _, Some tracker ->
        let samples = !(tracker.samples) in
        if samples = [] then ()
        else if List.length samples = 1 then ()  (* Need at least 2 samples for rate *)
        else
          (* Find min and max timestamps efficiently without full sort *)
          let min_sample = ref (List.hd samples) in
          let max_sample = ref (List.hd samples) in
          List.iter (fun sample ->
            if sample.timestamp < (!min_sample).timestamp then min_sample := sample;
            if sample.timestamp > (!max_sample).timestamp then max_sample := sample;
          ) samples;
          let time_diff = (!max_sample).timestamp -. (!min_sample).timestamp in
          let value_diff = (!max_sample).value - (!min_sample).value in
          if time_diff > 0.0 && time_diff <= tracker.window_seconds then
            metric.cached_rate <- float_of_int value_diff /. time_diff
    | SlidingCounter _, Some tracker ->
        (* For sliding counters with rate tracking, calculate rate from samples *)
        let samples = !(tracker.samples) in
        if samples <> [] && List.length samples >= 2 then
          let sorted_samples = List.sort (fun s1 s2 -> Float.compare s1.timestamp s2.timestamp) samples in
          let earliest = List.hd sorted_samples in
          let latest = List.hd (List.rev sorted_samples) in
          let time_diff = latest.timestamp -. earliest.timestamp in
          let value_diff = latest.value - earliest.value in
          if time_diff > 0.0 && time_diff <= tracker.window_seconds then
            metric.cached_rate <- float_of_int value_diff /. time_diff
    | _ -> ()
  ) metrics;
  Mutex.unlock metrics_mutex

(** Calculate current rate for a counter with rate tracking enabled *)
let get_counter_rate metric =
  metric.cached_rate

(** Gauge - arbitrary value that can go up or down *)
let gauge name ?(labels=[]) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric =
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let m = {
          name;
          metric_type = Gauge (ref 0.0);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
          rate_tracker = None;
          cached_rate = 0.0;
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

let set_gauge metric value =
  match metric.metric_type with
  | Gauge r ->
      Mutex.lock metrics_mutex;
      r := value;
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let inc_gauge metric value =
  match metric.metric_type with
  | Gauge r ->
      Mutex.lock metrics_mutex;
      r := !r +. value;
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let get_gauge metric =
  match metric.metric_type with
  | Gauge r -> !r
  | _ -> 0.0

(** Histogram - track distribution of values *)
let histogram name ?(labels=[]) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric =
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let m = {
          name;
          metric_type = Histogram (ref []);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
          rate_tracker = None;
          cached_rate = 0.0;
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

let observe_histogram metric value =
  match metric.metric_type with
  | Histogram r ->
      Mutex.lock metrics_mutex;
      r := value :: !r;
      (* Keep only most recent 1000 samples to avoid memory bloat *)
      if List.length !r > 1000 then
        r := List.filteri (fun i _ -> i < 1000) !r;  (* Keep first 1000 elements (most recent since we prepend) *)
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let histogram_stats metric =
  match metric.metric_type with
  | Histogram r ->
      let values = !r in
      if values = [] then (0.0, 0.0, 0.0, 0.0, 0)
      else
        let sorted = List.sort Float.compare values in
        let len = List.length sorted in
        let sum = List.fold_left (+.) 0.0 sorted in
        let mean = sum /. float_of_int len in
        (* For median: average of two middle elements for even length *)
        let p50 = if len mod 2 = 0 then
          (List.nth sorted (len / 2 - 1) +. List.nth sorted (len / 2)) /. 2.0
        else
          List.nth sorted (len / 2) in
        let p95_idx = min (len - 1) (len * 95 / 100) in
        let p95 = List.nth sorted p95_idx in
        let p99_idx = min (len - 1) (len * 99 / 100) in
        let p99 = List.nth sorted p99_idx in
        (mean, p50, p95, p99, len)
  | _ -> (0.0, 0.0, 0.0, 0.0, 0)

(** Timing utilities *)
let start_timer () = Unix.gettimeofday ()

let record_duration metric start_time =
  let duration = Unix.gettimeofday () -. start_time in
  observe_histogram metric duration;
  duration

(** Global start time *)
let start_time = ref (Unix.time ())

let () = start_time := Unix.time ()

(** Initialize basic system metrics *)
let () =
  (* Create some initial metrics to ensure the dashboard has data *)
  ignore (counter "system_startups" ());
  ignore (gauge "uptime_seconds" ());
  ignore (counter "dashboard_views" ())

(** Group metrics by category for better organization *)
let categorize_metric metric =
  let name = metric.name in
  if String.starts_with ~prefix:"connection_" name ||
     String.starts_with ~prefix:"circuit_breaker" name then "Connections"
  else if String.starts_with ~prefix:"order" name ||
          String.starts_with ~prefix:"api_" name ||
          name = "orders_placed" || name = "orders_failed" || name = "orders_amended" then "Trading"
  else if String.starts_with ~prefix:"domain_" name ||
          String.starts_with ~prefix:"strategy_" name then "Domains"
  else if String.starts_with ~prefix:"ticker" name ||
          String.starts_with ~prefix:"orderbook" name ||
          String.starts_with ~prefix:"balance" name ||
          String.starts_with ~prefix:"execution" name then "Market Data"
  else if String.starts_with ~prefix:"open_" name then "Positions"
  else "Other"

(** Format metric value for display *)
let format_metric_value metric =
  match metric.metric_type with
  | Counter r -> Printf.sprintf "%d" !r
  | SlidingCounter sliding -> Printf.sprintf "%d" !(sliding.total_count)
  | Gauge r -> Printf.sprintf "%.1f" !r
  | Histogram _ ->
      let (_, p50, _, _, count) = histogram_stats metric in
      if count > 0 then
        Printf.sprintf "%d samples, p50=%.1fµs" count (p50 *. 1_000_000.0)
      else "no samples"

(** Report all metrics organized by category *)
let report_metrics () =
  Mutex.lock metrics_mutex;
  let now = Unix.time () in
  let uptime = now -. !start_time in

  (* Collect all metrics by category *)
  let categories = Hashtbl.create 16 in
  Hashtbl.iter (fun _key metric ->
    let category = categorize_metric metric in
    let existing = Hashtbl.find_opt categories category |> Option.value ~default:[] in
    Hashtbl.replace categories category (metric :: existing)
  ) metrics;

  Logging.info ~section "=== Telemetry Report ===";
  Logging.info_f ~section "System uptime: %.0f seconds" uptime;
  Logging.info ~section "";

  (* Define category order *)
  let category_order = ["Connections"; "Trading"; "Positions"; "Domains"; "Market Data"; "Other"] in

  List.iter (fun category ->
    match Hashtbl.find_opt categories category with
    | Some metrics_list when metrics_list <> [] ->
        Logging.info_f ~section "[%s]" category;
        let separator = String.make (String.length category + 3) '-' in
        Logging.info_f ~section "%s" separator;

        (* Sort metrics within category by name *)
        let sorted_metrics = List.sort (fun m1 m2 -> String.compare m1.name m2.name) metrics_list in

        List.iter (fun metric ->
          let label_str =
            if metric.labels = [] then ""
            else " (" ^ (metric.labels |> List.map (fun (k, v) -> k ^ "=" ^ v) |> String.concat ", ") ^ ")"
          in

          let value_str = format_metric_value metric in
          Logging.info_f ~section "  %-25s %s%s" (metric.name ^ ":") value_str label_str;

          (* Special handling for domain_cycles rate *)
          match metric.metric_type, metric.name with
          | Counter _, "domain_cycles" ->
              let rate = get_counter_rate metric in
              Logging.info_f ~section "    └─ %.0f cycles/sec" rate
          | _ -> ()
        ) sorted_metrics;
        Logging.info ~section ""
    | _ -> ()
  ) category_order;

  Logging.info ~section "=== End Report ===";
  Mutex.unlock metrics_mutex

(** Reset all metrics *)
let reset_metrics () =
  Mutex.lock metrics_mutex;
  Hashtbl.clear metrics;
  Mutex.unlock metrics_mutex;
  Logging.info ~section "All metrics reset"

(** Common metrics *)
module Common = struct
  let api_requests = counter "api_requests" ()
  let api_errors = counter "api_errors" ()
  let api_duration = histogram "api_duration_seconds" ()
  
  let orders_placed = counter "orders_placed" ()
  let orders_failed = counter "orders_failed" ()


end

(** Per-asset metrics helper *)
let asset_counter name asset ?(track_rate=false) ?(rate_window=10.0) () = counter name ~labels:["asset", asset] ~track_rate ~rate_window ()
let asset_sliding_counter name asset ?(window_size=10000) ?(track_rate=false) ?(rate_window=10.0) () =
  sliding_counter name ~labels:["asset", asset] ~window_size ~track_rate ~rate_window ()
let asset_gauge name asset = gauge name ~labels:["asset", asset] ()
let asset_histogram name asset = histogram name ~labels:["asset", asset] ()

(** Update system metrics *)
let update_system_metrics () =
  let uptime_metric = gauge "uptime_seconds" () in
  let uptime = Unix.time () -. !start_time in
  set_gauge uptime_metric uptime

(** Wait for metric changes (event-driven) *)
let wait_for_metric_changes () = Lwt_condition.wait metrics_changed_condition

