(** Lightweight Telemetry System for Performance Tracking *)

open Lwt.Infix
open Concurrency

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

(** Bounded circular buffer for rate samples to prevent memory leaks *)
type rate_buffer = {
  buffer_type: [`Rate];
  data: rate_sample option array;
  mutable write_pos: int;
  mutable size: int;
  capacity: int;
}

type rate_tracker = {
  samples: rate_buffer;
  window_seconds: float;
  mutable samples_since_cleanup: int;  (* Track samples added since last cleanup *)
}

(** Bounded circular buffer for histogram samples *)
type histogram_buffer = {
  hist_type: [`Histogram];
  hist_data: float option array;
  mutable hist_write_pos: int;
  mutable hist_size: int;
  hist_capacity: int;
}

(** Memory-aware buffer capacity management *)
type memory_pressure = Low | Medium | High

type memory_config = {
  mutable rate_buffer_capacity: int;
  mutable histogram_capacity: int;
  mutable last_adjustment: float;
}

let memory_config = {
  rate_buffer_capacity = 50;
  histogram_capacity = 2000;
  last_adjustment = 0.0;
}

(** Mutex to protect memory_config access from race conditions *)
let memory_config_mutex = Mutex.create ()

(** Metric lifecycle management *)
let metric_stale_ttl_seconds = 3600.0  (* Remove metrics not updated in 1 hour *)
let max_metrics_count = 5000  (* Cap total metrics to prevent unbounded growth *)

(** Get current memory pressure level based on OCaml heap usage *)
let get_memory_pressure () : memory_pressure =
  let stat = Gc.stat () in
  let heap_words = stat.heap_words in
  let word_size = Sys.word_size / 8 in  (* bytes per word *)
  let heap_mb = float_of_int (heap_words * word_size) /. (1024.0 *. 1024.0) in
  let heap_percent = heap_mb /. 1024.0 *. 100.0 in  (* Assume 1GB max heap for calculation *)
  if heap_percent < 60.0 then Low
  else if heap_percent < 80.0 then Medium
  else High

(** Rate buffer management functions *)
let create_rate_buffer () : rate_buffer =
  Mutex.lock memory_config_mutex;
  let capacity = memory_config.rate_buffer_capacity in
  Mutex.unlock memory_config_mutex;
  {
    buffer_type = `Rate;
    data = Array.make capacity None;
    write_pos = 0;
    size = 0;
    capacity;
  }

let add_rate_sample buffer sample =
  let was_full = buffer.size >= buffer.capacity in
  buffer.data.(buffer.write_pos) <- Some sample;
  buffer.write_pos <- (buffer.write_pos + 1) mod buffer.capacity;
  if buffer.size < buffer.capacity then
    buffer.size <- buffer.size + 1
  else if was_full then
    (* Buffer was already full, oldest sample was overwritten *)
    Logging.debug ~section:"telemetry" "Rate buffer overflow: overwriting oldest sample"

let get_rate_samples buffer =
  let rec collect acc pos remaining =
    if remaining = 0 then acc
    else
      let actual_pos = (buffer.write_pos - remaining) mod buffer.capacity in
      if actual_pos < 0 then
        collect acc pos (remaining - 1)  (* Wrap around *)
      else
        match buffer.data.(actual_pos) with
        | Some sample -> collect (sample :: acc) pos (remaining - 1)
        | None -> collect acc pos (remaining - 1)
  in
  collect [] 0 buffer.size

(** Histogram buffer management functions *)
let create_histogram_buffer () : histogram_buffer =
  Mutex.lock memory_config_mutex;
  let capacity = memory_config.histogram_capacity in
  Mutex.unlock memory_config_mutex;
  {
    hist_type = `Histogram;
    hist_data = Array.make capacity None;
    hist_write_pos = 0;
    hist_size = 0;
    hist_capacity = capacity;
  }

let add_histogram_sample buffer sample =
  let was_full = buffer.hist_size >= buffer.hist_capacity in
  buffer.hist_data.(buffer.hist_write_pos) <- Some sample;
  buffer.hist_write_pos <- (buffer.hist_write_pos + 1) mod buffer.hist_capacity;
  if buffer.hist_size < buffer.hist_capacity then
    buffer.hist_size <- buffer.hist_size + 1
  else if was_full then
    (* Buffer was already full, oldest sample was overwritten *)
    Logging.debug ~section:"telemetry" "Histogram buffer overflow: overwriting oldest sample"

let get_histogram_samples buffer =
  let rec collect acc pos remaining =
    if remaining = 0 then acc
    else
      let actual_pos = (buffer.hist_write_pos - remaining) mod buffer.hist_capacity in
      if actual_pos < 0 then
        collect acc pos (remaining - 1)  (* Wrap around *)
      else
        match buffer.hist_data.(actual_pos) with
        | Some sample -> collect (sample :: acc) pos (remaining - 1)
        | None -> collect acc pos (remaining - 1)
  in
  collect [] 0 buffer.hist_size

(** Metric types *)
type metric_type =
  | Counter of int ref
  | SlidingCounter of sliding_counter
  | Gauge of float ref
  | Histogram of histogram_buffer

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

(** Event bus for metric changes (stream-based subscription) *)
module MetricChangeEventBus = Event_bus.Make(struct
  type t = unit  (* Metric changes don't need data, just notification *)
end)

(** Global metric change event bus instance *)
let metric_change_event_bus = MetricChangeEventBus.create "metric_change"

(** Safe broadcast helper that handles Invalid_argument exceptions from concurrent access *)
let safe_broadcast_metrics_changed () =
  try
    Lwt_condition.broadcast metrics_changed_condition ()
  with
  | Invalid_argument _ ->
      (* Promise already resolved - this can happen with concurrent broadcasts *)
      (* Not fatal, just means a waiter was already woken up *)
      ()
  | exn ->
      (* Other errors should still be logged *)
      Logging.warn_f ~section:"telemetry" "Error broadcasting metrics_changed: %s" (Printexc.to_string exn);

  (* Also publish to event bus for stream-based subscribers *)
  MetricChangeEventBus.publish metric_change_event_bus ()


(** Helper to generate metric key *)
let metric_key name labels =
  let label_str = 
    labels 
    |> List.sort (fun (k1, _) (k2, _) -> String.compare k1 k2)
    |> List.map (fun (k, v) -> k ^ "=" ^ v)
    |> String.concat ","
  in
  if label_str = "" then name else name ^ "{" ^ label_str ^ "}"




(** Update cached rates and snapshot values for all metrics (called during snapshot creation) *)
let update_cached_rates () =
  Mutex.lock metrics_mutex;
  Hashtbl.iter (fun _ metric ->
    (* Only calculate expensive rates for metrics that actually have rate tracking enabled *)
    match metric.metric_type, metric.rate_tracker with
    | Counter _, Some tracker ->
        (* For counters with rate tracking, calculate from rate samples *)
        let samples = get_rate_samples tracker.samples in
        let sample_count = List.length samples in
        if sample_count >= 2 then
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
          else
            metric.cached_rate <- 0.0
        else
          metric.cached_rate <- 0.0
    | SlidingCounter _, Some tracker ->
        (* For sliding counters with rate tracking, calculate rate from rate samples *)
        let samples = get_rate_samples tracker.samples in
        let sample_count = List.length samples in
        if sample_count >= 2 then
          let sorted_samples = List.sort (fun s1 s2 -> Float.compare s1.timestamp s2.timestamp) samples in
          let earliest = List.hd sorted_samples in
          let latest = List.hd (List.rev sorted_samples) in
          let time_diff = latest.timestamp -. earliest.timestamp in
          let value_diff = latest.value - earliest.value in
          if time_diff > 0.0 && time_diff <= tracker.window_seconds then
            metric.cached_rate <- float_of_int value_diff /. time_diff
          else
            metric.cached_rate <- 0.0
        else
          metric.cached_rate <- 0.0
    | SlidingCounter sliding, None ->
        (* For sliding counters without rate tracking, skip expensive window iteration *)
        (* Use a simple heuristic: rate = total_count / 30 seconds (our cleanup window) *)
        let rate = if !(sliding.total_count) > 0 then
          float_of_int !(sliding.total_count) /. 30.0
        else 0.0 in
        metric.cached_rate <- rate
    | _ ->
        (* For other metric types without rate tracking, set rate to 0 *)
        metric.cached_rate <- 0.0
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
          last_updated = Unix.gettimeofday ();
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
      metric.last_updated <- Unix.gettimeofday ();
      Mutex.unlock metrics_mutex;
      safe_broadcast_metrics_changed ()
  | _ -> ()

(** Silent gauge update - updates metric without broadcasting changes *)
let set_gauge_silent metric value =
  match metric.metric_type with
  | Gauge r ->
      Mutex.lock metrics_mutex;
      r := value;
      metric.last_updated <- Unix.gettimeofday ();
      Mutex.unlock metrics_mutex
  | _ -> ()

let inc_gauge metric value =
  match metric.metric_type with
  | Gauge r ->
      Mutex.lock metrics_mutex;
      r := !r +. value;
      metric.last_updated <- Unix.gettimeofday ();
      Mutex.unlock metrics_mutex;
      safe_broadcast_metrics_changed ()
  | _ -> ()

let get_gauge metric =
  match metric.metric_type with
  | Gauge r -> !r
  | _ -> 0.0

(** Adjust buffer capacities based on memory pressure *)
let adjust_buffer_capacities () =
  let now = Unix.gettimeofday () in
  let pressure = get_memory_pressure () in

  (* Only adjust capacities every 60 seconds to avoid excessive churn *)
  Mutex.lock memory_config_mutex;
  let time_since_last = now -. memory_config.last_adjustment in
  if time_since_last >= 60.0 then begin
    Logging.warn_f ~section:"telemetry" "Adjusting buffer capacities (pressure: %s)"
      (match pressure with Low -> "Low" | Medium -> "Medium" | High -> "High");
    let old_rate_capacity = memory_config.rate_buffer_capacity in
    let old_histogram_capacity = memory_config.histogram_capacity in

    (match pressure with
     | Low ->
         (* Increase capacities up to max limits *)
         memory_config.rate_buffer_capacity <- min 500 (memory_config.rate_buffer_capacity * 6 / 5);
         memory_config.histogram_capacity <- min 30000 (memory_config.histogram_capacity * 6 / 5)
     | Medium ->
         (* Keep current capacities *)
         ()
     | High ->
         (* Decrease capacities with minimum limits - ensure enough samples for smooth metrics *)
         (* At 2k samples/sec (1/100 sampling), 10000 samples = ~5s, 15000 samples = ~7.5s *)
         memory_config.rate_buffer_capacity <- max 50 (memory_config.rate_buffer_capacity * 7 / 10);
         memory_config.histogram_capacity <- max 10000 (memory_config.histogram_capacity * 7 / 10);

         Logging.debug_f ~section:"telemetry" "Reduced buffer capacities due to high memory pressure: rate %d->%d, histogram %d->%d"
           old_rate_capacity memory_config.rate_buffer_capacity
           old_histogram_capacity memory_config.histogram_capacity
    );

    memory_config.last_adjustment <- now;
  end;

  (* Get current capacities for metrics (within mutex) *)
  let current_rate_capacity = memory_config.rate_buffer_capacity in
  let current_histogram_capacity = memory_config.histogram_capacity in
  Mutex.unlock memory_config_mutex;

  (* Always update metrics (outside throttle) *)
  let rate_capacity_metric = gauge "telemetry_rate_buffer_capacity" () in
  let histogram_capacity_metric = gauge "telemetry_histogram_buffer_capacity" () in

  (* Reset ALL pressure gauges to 0, then set current to 1 *)
  let mem_low = gauge "telemetry_memory_pressure_low" () in
  let mem_med = gauge "telemetry_memory_pressure_medium" () in
  let mem_high = gauge "telemetry_memory_pressure_high" () in

  set_gauge mem_low 0.0;
  set_gauge mem_med 0.0;
  set_gauge mem_high 0.0;

  (match pressure with
   | Low -> set_gauge mem_low 1.0
   | Medium -> set_gauge mem_med 1.0
   | High -> set_gauge mem_high 1.0);

  set_gauge rate_capacity_metric (float_of_int current_rate_capacity);
  set_gauge histogram_capacity_metric (float_of_int current_histogram_capacity)

(** Remove a metric explicitly *)
let remove_metric name ?(labels=[]) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  Hashtbl.remove metrics key;
  Mutex.unlock metrics_mutex;
  Logging.debug_f ~section:"telemetry" "Explicitly removed metric: %s" key

(** Prune stale metrics and cap total count to prevent unbounded growth *)
let prune_stale_metrics () =
  Mutex.lock metrics_mutex;
  let now = Unix.gettimeofday () in
  let stale_cutoff = now -. metric_stale_ttl_seconds in

  (* Collect keys to remove *)
  let keys_to_remove = ref [] in
  let total_count = Hashtbl.length metrics in

  Hashtbl.iter (fun key metric ->
    if metric.last_updated < stale_cutoff then
      keys_to_remove := key :: !keys_to_remove
  ) metrics;

  (* Remove stale metrics *)
  let stale_removed = List.length !keys_to_remove in
  List.iter (Hashtbl.remove metrics) !keys_to_remove;

  (* If still over limit, remove oldest metrics *)
  let current_count = Hashtbl.length metrics in
  let to_remove_more = max 0 (current_count - max_metrics_count) in
  if to_remove_more > 0 then begin
    (* Collect all metrics with creation times *)
    let metrics_with_times = ref [] in
    Hashtbl.iter (fun key metric ->
      metrics_with_times := (key, metric.created_at) :: !metrics_with_times
    ) metrics;

    (* Sort by creation time (oldest first) and remove oldest *)
    let sorted = List.sort (fun (_, t1) (_, t2) -> Float.compare t1 t2) !metrics_with_times in
    let oldest_to_remove = List.map fst (List.filteri (fun i _ -> i < to_remove_more) sorted) in
    List.iter (Hashtbl.remove metrics) oldest_to_remove;
  end;

  let final_count = Hashtbl.length metrics in
  Mutex.unlock metrics_mutex;

  (* Log summary if we removed anything *)
  if stale_removed > 0 || to_remove_more > 0 then
    Logging.info_f ~section "Pruned stale metrics: removed %d stale + %d oldest, total metrics: %d/%d"
      stale_removed to_remove_more final_count total_count

(** Start event-driven capacity adjustment loop *)
let start_capacity_adjuster () =
  Lwt.async (fun () ->
    let rec adjustment_loop () =
      (* Wait for any metric change *)
      Lwt_condition.wait metrics_changed_condition >>= fun () ->

      (* Check if 60 seconds have passed since last adjustment *)
      let now = Unix.gettimeofday () in
      Mutex.lock memory_config_mutex;
      let time_since_last = now -. memory_config.last_adjustment in
      Mutex.unlock memory_config_mutex;

      if time_since_last >= 60.0 then begin
        (* Perform adjustment *)
        adjust_buffer_capacities ();
        (* Also prune stale metrics *)
        prune_stale_metrics ()
      end;

      (* Continue listening *)
      adjustment_loop ()
    in
    adjustment_loop ()
  )

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
          metric_type = Histogram (create_histogram_buffer ());  (* Capacity managed by memory_config *)
          labels;
          created_at = Unix.time ();
          last_updated = Unix.gettimeofday ();
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
  | Histogram buffer ->
      Mutex.lock metrics_mutex;
      add_histogram_sample buffer value;
      metric.last_updated <- Unix.gettimeofday ();
      Mutex.unlock metrics_mutex;
      safe_broadcast_metrics_changed ()
  | _ -> ()

let histogram_stats metric =
  match metric.metric_type with
  | Histogram buffer ->
      let values = get_histogram_samples buffer in
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

(** Hybrid timing utilities for high-precision performance measurement *)
type timer = {
  start_mtime: Mtime.t;
  start_unix: float;
}

let start_timer_v2 () = {
  start_mtime = Mtime_clock.now ();
  start_unix = Unix.gettimeofday ();
}

let record_duration_v2 metric timer =
  let now_mtime = Mtime_clock.now () in
  let duration_ns = Mtime.span timer.start_mtime now_mtime |> Mtime.Span.to_uint64_ns in
  let duration_seconds = Int64.to_float duration_ns /. 1_000_000_000.0 in
  observe_histogram metric duration_seconds;
  duration_seconds

(** Global start time *)
let start_time = ref (Unix.time ())

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
            samples = create_rate_buffer ();  (* Capacity managed by memory_config *)
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
          last_updated = Unix.gettimeofday ();
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
      let now = Unix.gettimeofday () in

      (* Check window size BEFORE adding new entry to prevent unbounded growth *)
      let current_len = List.length !(sliding.current_window) in
      
      (* Emergency truncation if window is way over limit *)
      if current_len >= sliding.window_size * 3 / 2 then begin  (* 1.5x target size *)
        Logging.warn_f ~section:"telemetry" "EMERGENCY: Sliding window for metric exceeded 1.5x limit (%d/%d), forcing truncation" 
          current_len sliding.window_size;
        let cutoff = now -. 15.0 in  (* Keep last 15 seconds *)
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
        sliding.last_cleanup := now;
      end else if current_len >= sliding.window_size then begin
        (* Window is at or beyond limit - perform immediate truncation *)
        let cutoff = now -. 30.0 in  (* Keep last 30 seconds for rate calculation *)
        let new_window = ref [] in
        let new_total = ref 0 in

        List.iter (fun (timestamp, count) ->
          if timestamp >= cutoff then begin
            new_window := (timestamp, count) :: !new_window;
            new_total := !new_total + count;
          end
        ) !(sliding.current_window);

        (* If still too many entries after time-based filtering, keep only most recent *)
        let filtered_window = !new_window in
        if List.length filtered_window > sliding.window_size then begin
          let sorted = List.sort (fun (t1, _) (t2, _) -> Float.compare t2 t1) filtered_window in
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
        end else begin
          sliding.current_window := filtered_window;
          sliding.total_count := !new_total;
        end;
        sliding.last_cleanup := now;
      end;

      (* Now add new increment to window *)
      sliding.current_window := (now, value) :: !(sliding.current_window);
      sliding.total_count := !(sliding.total_count) + value;
      metric.last_updated <- now;

      (* Update rate tracker if enabled *)
      (match metric.rate_tracker with
       | Some tracker ->
           let sample = { timestamp = now; value = !(sliding.total_count) } in
           add_rate_sample tracker.samples sample;
           tracker.samples_since_cleanup <- tracker.samples_since_cleanup + 1;
           (* Filter old samples periodically *)
           if tracker.samples_since_cleanup >= 100 then begin
             tracker.samples_since_cleanup <- 0;
             (* Remove samples older than window_seconds *)
             let cutoff = now -. tracker.window_seconds in
             let samples = get_rate_samples tracker.samples in
             let filtered = List.filter (fun s -> s.timestamp >= cutoff) samples in
             (* Rebuild buffer with only recent samples *)
             tracker.samples.size <- 0;
             tracker.samples.write_pos <- 0;
             List.iter (add_rate_sample tracker.samples) (List.rev filtered)
           end
       | None -> ());

      (* Periodic cleanup: trigger every 15 seconds (reduced from 30) *)
      let should_cleanup = now -. !(sliding.last_cleanup) > 15.0 in

      Mutex.unlock metrics_mutex;

      (* Do periodic cleanup outside of the main mutex *)
      if should_cleanup then
        begin
          Mutex.lock metrics_mutex;
          (* Re-check condition in case another thread already cleaned up *)
          let now_check = Unix.time () in
          if now_check -. !(sliding.last_cleanup) > 30.0 then
            begin
              sliding.last_cleanup := now_check;
              let cutoff = now_check -. 15.0 in  (* Keep last 15 seconds (reduced from 30) *)
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

              (* Ensure we don't exceed window size *)
              let window_len = List.length !new_window in
              if window_len > sliding.window_size then begin
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

      safe_broadcast_metrics_changed ()
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
            samples = create_rate_buffer ();  (* Capacity managed by memory_config *)
            window_seconds = rate_window;
            samples_since_cleanup = 0;
          }
        else None in
        let m = {
          name;
          metric_type = Counter (ref 0);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.gettimeofday ();
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
      let now = Unix.gettimeofday () in
      r := !r + value;
      metric.last_updated <- now;

      (* Update rate tracker if enabled *)
      (match metric.rate_tracker with
       | Some tracker ->
           let sample = { timestamp = now; value = !r } in
           add_rate_sample tracker.samples sample;
           tracker.samples_since_cleanup <- tracker.samples_since_cleanup + 1;
           (* Filter old samples periodically *)
           if tracker.samples_since_cleanup >= 100 then begin
             tracker.samples_since_cleanup <- 0;
             (* Remove samples older than window_seconds *)
             let cutoff = now -. tracker.window_seconds in
             let samples = get_rate_samples tracker.samples in
             let filtered = List.filter (fun s -> s.timestamp >= cutoff) samples in
             (* Rebuild buffer with only recent samples *)
             tracker.samples.size <- 0;
             tracker.samples.write_pos <- 0;
             List.iter (add_rate_sample tracker.samples) (List.rev filtered)
           end
       | None -> ());

      Mutex.unlock metrics_mutex;
      safe_broadcast_metrics_changed ()
  | _ -> ()

let get_counter metric =
  match metric.metric_type with
  | Counter r -> !r
  | SlidingCounter sliding -> !(sliding.total_count)
  | _ -> 0

(** Initialize basic system metrics *)
let () =
  (* Create some initial metrics to ensure the dashboard has data *)
  ignore (gauge "uptime_seconds" ())

(** Start capacity adjustment loop *)
let () = start_capacity_adjuster ()

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
  else if String.starts_with ~prefix:"heap_memory" name ||
          String.starts_with ~prefix:"rss_memory" name ||
          String.starts_with ~prefix:"telemetry_" name ||
          String.starts_with ~prefix:"event_bus_" name then "System"
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
let asset_histogram name asset () = histogram name ~labels:["asset", asset] ()

(** Get current OCaml heap usage in MB *)
let get_ocaml_heap_usage () =
  let stat = Gc.stat () in
  let heap_words = stat.heap_words in
  let word_size = Sys.word_size / 8 in  (* bytes per word *)
  float_of_int (heap_words * word_size) /. (1024.0 *. 1024.0)

(** Get allocation rate (minor words allocated per second) *)
let get_allocation_rate () =
  let stat = Gc.quick_stat () in
  let minor_words = stat.minor_words in
  (* Estimate allocation rate based on recent activity *)
  (* This is a rough approximation - in practice you'd want time-based sampling *)
  minor_words /. 1000000.0  (* Convert to millions of words per sample *)

(** Get current RSS (Resident Set Size) in MB *)
let get_rss_usage () =
  try
    let pid = Unix.getpid () in
    let cmd = Printf.sprintf "ps -o rss= -p %d" pid in
    let ic = Unix.open_process_in cmd in
    let line = input_line ic in
    let _ = Unix.close_process_in ic in
    let rss_kb = int_of_string (String.trim line) in
    float_of_int rss_kb /. 1024.0  (* Convert KB to MB *)
  with _ -> 0.0  (* Fallback if ps command fails *)

(** Memory monitoring gauge metrics - created on demand *)
let logs_dropped_gauge = ref None
let event_bus_subscribers_total_gauge = ref None
let event_bus_subscribers_active_gauge = ref None
let telemetry_rate_buffers_gauge = ref None
let telemetry_histogram_buffers_gauge = ref None
let heap_memory_gauge = ref None
let rss_memory_gauge = ref None
let registry_size_gauge = ref None
let amendments_registry_size_gauge = ref None



(** Silent memory usage updates - update metrics without broadcasting changes *)
let set_heap_memory_usage_silent usage =
  let metric = match !heap_memory_gauge with
    | Some m -> m
    | None ->
        let m = gauge "heap_memory_mb" () in
        heap_memory_gauge := Some m;
        m
  in
  set_gauge_silent metric usage

let set_rss_memory_usage_silent usage =
  let metric = match !rss_memory_gauge with
    | Some m -> m
    | None ->
        let m = gauge "rss_memory_mb" () in
        rss_memory_gauge := Some m;
        m
  in
  set_gauge_silent metric usage

(** Performance monitoring metrics for telemetry system health *)
let snapshot_creation_time_histogram = ref None
let max_sliding_window_size_gauge = ref None
let total_sliding_entries_gauge = ref None

(** Track snapshot creation performance *)
let record_snapshot_creation_time duration_seconds =
  let metric = match !snapshot_creation_time_histogram with
    | Some m -> m
    | None ->
        let m = histogram "telemetry_snapshot_creation_seconds" () in
        snapshot_creation_time_histogram := Some m;
        m
  in
  observe_histogram metric duration_seconds

(** Track sliding window statistics for memory monitoring *)
let update_sliding_window_stats () =
  Mutex.lock metrics_mutex;
  let max_window_size = ref 0 in
  let total_entries = ref 0 in

  Hashtbl.iter (fun _ metric ->
    match metric.metric_type with
    | SlidingCounter sliding ->
        let window_size = List.length !(sliding.current_window) in
        max_window_size := max !max_window_size window_size;
        total_entries := !total_entries + window_size
    | _ -> ()
  ) metrics;

  Mutex.unlock metrics_mutex;

  (* Update gauges *)
  let max_gauge = match !max_sliding_window_size_gauge with
    | Some m -> m
    | None ->
        let m = gauge "telemetry_max_sliding_window_size" () in
        max_sliding_window_size_gauge := Some m;
        m
  in
  set_gauge max_gauge (float_of_int !max_window_size);

  let total_gauge = match !total_sliding_entries_gauge with
    | Some m -> m
    | None ->
        let m = gauge "telemetry_total_sliding_entries" () in
        total_sliding_entries_gauge := Some m;
        m
  in
  set_gauge total_gauge (float_of_int !total_entries)

(** Update system metrics *)
let update_system_metrics () =
  (* Update all metrics silently to batch changes *)
  let uptime_metric = gauge "uptime_seconds" () in
  let uptime = Unix.time () -. !start_time in
  set_gauge_silent uptime_metric uptime;

  (* Update memory usage gauges silently *)
  let heap_mb = get_ocaml_heap_usage () in
  let rss_mb = get_rss_usage () in
  let alloc_rate = get_allocation_rate () in

  set_heap_memory_usage_silent heap_mb;
  set_rss_memory_usage_silent rss_mb;

  (* Add allocation rate metric silently *)
  let alloc_rate_metric = gauge "allocation_rate_mwords_per_sample" () in
  set_gauge_silent alloc_rate_metric alloc_rate;

  (* Broadcast once at the end to trigger telemetry_cache update *)
  safe_broadcast_metrics_changed ()

(** Update memory monitoring metrics *)

(** Helper functions to update memory monitoring metrics *)
let set_logs_dropped_count count =
  let metric = match !logs_dropped_gauge with
    | Some m -> m
    | None ->
        let m = gauge "logs_dropped_total" () in
        logs_dropped_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int count)

let set_event_bus_subscribers_total count =
  let metric = match !event_bus_subscribers_total_gauge with
    | Some m -> m
    | None ->
        let m = gauge "event_bus_subscribers_total" () in
        event_bus_subscribers_total_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int count)

let set_event_bus_subscribers_active count =
  let metric = match !event_bus_subscribers_active_gauge with
    | Some m -> m
    | None ->
        let m = gauge "event_bus_subscribers_active" () in
        event_bus_subscribers_active_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int count)

let set_telemetry_rate_buffers_size size =
  let metric = match !telemetry_rate_buffers_gauge with
    | Some m -> m
    | None ->
        let m = gauge "telemetry_rate_buffers_total_size" () in
        telemetry_rate_buffers_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int size)

let set_telemetry_histogram_buffers_size size =
  let metric = match !telemetry_histogram_buffers_gauge with
    | Some m -> m
    | None ->
        let m = gauge "telemetry_histogram_buffers_total_size" () in
        telemetry_histogram_buffers_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int size)

let set_registry_size size =
  let metric = match !registry_size_gauge with
    | Some m -> m
    | None ->
        let m = gauge "registry_size_total" () in
        registry_size_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int size)

let set_amendments_registry_size size =
  let metric = match !amendments_registry_size_gauge with
    | Some m -> m
    | None ->
        let m = gauge "amendments_registry_size_total" () in
        amendments_registry_size_gauge := Some m;
        m
  in
  set_gauge metric (float_of_int size)



(** Update memory monitoring metrics *)
let update_memory_monitoring_metrics () =
  (* Calculate and update internal telemetry buffer sizes *)
  let rate_buffer_total = Hashtbl.fold (fun _ metric acc ->
    match metric.metric_type with
    | Counter _ when Option.is_some metric.rate_tracker ->
        acc + (Option.get metric.rate_tracker).samples.size
    | SlidingCounter _ when Option.is_some metric.rate_tracker ->
        acc + (Option.get metric.rate_tracker).samples.size
    | _ -> acc
  ) metrics 0 in

  let histogram_buffer_total = Hashtbl.fold (fun _ metric acc ->
    match metric.metric_type with
    | Histogram buffer -> acc + buffer.hist_size
    | _ -> acc
  ) metrics 0 in

  set_telemetry_rate_buffers_size rate_buffer_total;
  set_telemetry_histogram_buffers_size histogram_buffer_total;
  ()

(** Report all metrics organized by category *)
let report_metrics () =
  Mutex.lock metrics_mutex;

  (* Update memory monitoring metrics first *)
   update_memory_monitoring_metrics ();

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

(** Wait for metric changes (event-driven) *)
let wait_for_metric_changes () = Lwt_condition.wait metrics_changed_condition

(** Subscribe to metric changes (stream-based) *)
let subscribe_metric_changes () =
  let subscription = MetricChangeEventBus.subscribe metric_change_event_bus in
  (subscription.stream, subscription.close)

