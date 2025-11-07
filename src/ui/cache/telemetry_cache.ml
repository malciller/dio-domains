(** Telemetry-specific cache for the telemetry view

    Provides reactive access to telemetry data for the telemetry dashboard UI,
    respecting the concurrency model.
*)

open Lwt.Infix
open Telemetry
open Concurrency
open Ring_buffer

open Ui_types

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Telemetry update event for ring buffer storage *)
type telemetry_update_event = {
  timestamp: float;
  event_type: string;  (* "metric_updated", "metric_added", "metric_removed" *)
  metric_key: string;  (* Telemetry metric key *)
}

(** Event bus for telemetry snapshots *)
module TelemetrySnapshotEventBus = Event_bus.Make(struct
  type t = telemetry_snapshot
end)

(** Telemetry cache state - Two Layer Architecture *)
type t = {
  (* Layer 1: Bounded ring buffer for individual metric updates *)
  layer1_ring_buffer: telemetry_update_event RingBuffer.t;

  (* Layer 2: Current and previous snapshots for retry/recovery *)
  mutable current_snapshot: telemetry_snapshot option;
  mutable previous_snapshot: telemetry_snapshot option;

  telemetry_snapshot_event_bus: TelemetrySnapshotEventBus.t;
  update_condition: unit Lwt_condition.t;
  mutable last_metrics_hash: string;  (* Track hash of metrics for deduplication *)
  mutable last_update: float;  (* Track when we last published a snapshot *)
  update_interval: float;  (* Minimum time between periodic updates *)
}

(** Global shutdown flag for background tasks *)
let shutdown_requested = Atomic.make false

(** Signal shutdown to background tasks *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

(** Global telemetry cache instance - Two Layer Architecture *)
let cache = {
  layer1_ring_buffer = RingBuffer.create 1000;  (* Bounded ring buffer for metric updates *)
  current_snapshot = None;
  previous_snapshot = None;
  telemetry_snapshot_event_bus = TelemetrySnapshotEventBus.create "telemetry_snapshot";
  update_condition = Lwt_condition.create ();
  last_metrics_hash = "";
  last_update = 0.0;
  update_interval = 1.0;  (* Update telemetry snapshots at most every 1.0 seconds for dashboard mode *)
}

(** Safe broadcast helper for cache update condition *)
let safe_broadcast_update_condition cache =
  try
    Lwt_condition.broadcast cache.update_condition ()
  with
  | Invalid_argument _ ->
      (* Promise already resolved - this can happen with concurrent broadcasts *)
      (* Not fatal, just means a waiter was already woken up *)
      ()
  | exn ->
      (* Other errors should still be logged *)
      Logging.warn_f ~section:"telemetry_cache" "Error broadcasting update_condition: %s" (Printexc.to_string exn)

(** Initialization guard with mutex protection *)
let initialized = ref false
let init_mutex = Mutex.create ()

(** Atomic counter for unique snapshot versions *)
let snapshot_version_counter = Atomic.make 1

(** System initialization ready barrier *)
let system_ready = ref false
let system_ready_condition = Lwt_condition.create ()

(** Signal that system is ready for cache access *)
let signal_system_ready () =
  if not !system_ready then (
    system_ready := true;
    Lwt_condition.broadcast system_ready_condition ()
  )

(** Rebuild telemetry snapshot from Layer 1 events (triggered by metric updates) *)
let rebuild_snapshot_from_layer1 () : telemetry_snapshot =
  let start_time = Unix.gettimeofday () in
  let now = Unix.time () in
  let uptime = now -. !Telemetry.start_time in

  Logging.debug_f ~section:"telemetry_cache" "Rebuilding snapshot from Layer 1 at %.3f (uptime: %.1f)" now uptime;

  (* Update cached rates and sliding counter snapshots before copying metrics *)
  Telemetry.update_cached_rates ();

  (* Update sliding window statistics for monitoring *)
  Telemetry.update_sliding_window_stats ();

  (* Get current metrics hash for tracking changes *)
  let current_hash = Telemetry.(
    Mutex.lock metrics_mutex;
    let hash = Hashtbl.fold (fun key metric acc ->
      let metric_str = Printf.sprintf "%s:%s:%s:%.3f:%.3f"
        key metric.name (String.concat "," (List.map (fun (k,v) -> k^"="^v) metric.labels))
        metric.last_updated metric.cached_rate in
      Digest.string (acc ^ metric_str) |> Digest.to_hex
    ) metrics "" in
    Mutex.unlock metrics_mutex;
    hash
  ) in

  (* Update last hash for future comparisons *)
  cache.last_metrics_hash <- current_hash;

    (* Safely copy metrics from telemetry system - minimize allocations *)
    (* Strip heavy data structures like sliding window lists to prevent memory bloat *)
    let metrics_list = Telemetry.(
      Mutex.lock metrics_mutex;
      let metric_count = Hashtbl.length metrics in
      Logging.debug_f ~section:"telemetry_cache" "Copying %d metrics from telemetry system" metric_count;

      (* Create lightweight copies of metrics for dashboard, stripping heavy data *)
      let lightweight_metrics = Hashtbl.fold (fun _ metric acc ->
        let lightweight_metric = match metric.metric_type with
        | SlidingCounter sliding ->
            (* For sliding counters, create a lightweight version without the current_window list *)
            (* Create a lightweight sliding_counter with empty current_window *)
            let lightweight_sliding = {
              Telemetry.window_size = sliding.Telemetry.window_size;
              current_window = ref [];  (* Empty list to save memory *)
              total_count = ref !(sliding.Telemetry.total_count);  (* Copy the current total *)
              last_cleanup = ref !(sliding.Telemetry.last_cleanup);
            } in
            (* Create completely new metric record to avoid retaining mutable references *)
            {
              Telemetry.name = metric.Telemetry.name;
              metric_type = SlidingCounter lightweight_sliding;
              labels = metric.Telemetry.labels;  (* Labels are immutable, safe to share *)
              created_at = metric.Telemetry.created_at;
              last_updated = metric.Telemetry.last_updated;
              rate_tracker = None;  (* Don't copy rate tracker to save memory *)
              cached_rate = metric.Telemetry.cached_rate;
            }
        | Counter r ->
            (* Create completely new metric record to avoid retaining mutable references *)
            {
              Telemetry.name = metric.Telemetry.name;
              metric_type = Counter (ref !r);  (* Copy the counter value *)
              labels = metric.Telemetry.labels;
              created_at = metric.Telemetry.created_at;
              last_updated = metric.Telemetry.last_updated;
              rate_tracker = None;
              cached_rate = metric.Telemetry.cached_rate;
            }
        | Gauge r ->
            (* Create completely new metric record to avoid retaining mutable references *)
            {
              Telemetry.name = metric.Telemetry.name;
              metric_type = Gauge (ref !r);  (* Copy the gauge value *)
              labels = metric.Telemetry.labels;
              created_at = metric.Telemetry.created_at;
              last_updated = metric.Telemetry.last_updated;
              rate_tracker = None;
              cached_rate = metric.Telemetry.cached_rate;
            }
        | Histogram hist ->
            (* Create completely new metric record to avoid retaining mutable references *)
            (* Note: histogram_stats function handles histogram data safely *)
            {
              Telemetry.name = metric.Telemetry.name;
              metric_type = Histogram hist;  (* Histograms are immutable once created *)
              labels = metric.Telemetry.labels;
              created_at = metric.Telemetry.created_at;
              last_updated = metric.Telemetry.last_updated;
              rate_tracker = None;
              cached_rate = metric.Telemetry.cached_rate;
            }
        in
        lightweight_metric :: acc
      ) metrics [] in

      lightweight_metrics
    ) in
    Mutex.unlock metrics_mutex;

    (* Limit snapshot size to prevent memory issues - keep most recent metrics *)
    let max_metrics_per_snapshot = 1000 in
    let final_metrics =
      if List.length metrics_list > max_metrics_per_snapshot then (
        Logging.warn_f ~section:"telemetry_cache" "Limiting snapshot size from %d to %d metrics to prevent memory issues"
          (List.length metrics_list) max_metrics_per_snapshot;
        (* Take the first N metrics (most recent by insertion order) *)
        take max_metrics_per_snapshot metrics_list
      ) else
        metrics_list
    in
    Logging.debug_f ~section:"telemetry_cache" "Snapshot contains %d metrics" (List.length final_metrics);

    (* Compute categories efficiently - use single pass *)
    let categories_hashtable = Hashtbl.create 16 in
    List.iter (fun metric ->
      let category = Telemetry.categorize_metric metric in
      let existing = Hashtbl.find_opt categories_hashtable category |> Option.value ~default:[] in
      Hashtbl.replace categories_hashtable category (metric :: existing)
    ) final_metrics;

    let category_list =
      [("Connections", []); ("Trading", []); ("Positions", []); ("Domains", []); ("Market Data", []); ("System", []); ("Other", [])]
      |> List.map (fun (cat, _) ->
           match Hashtbl.find_opt categories_hashtable cat with
           | Some metrics -> (cat, List.sort (fun m1 m2 -> String.compare m1.name m2.name) metrics)
           | None -> (cat, [])
         )
    in

    let version = Atomic.incr snapshot_version_counter; Atomic.get snapshot_version_counter in
    let snapshot = {
      uptime;
      metrics = final_metrics;
      categories = category_list;
      timestamp = now;
      version;
    } in

    (* Record snapshot creation time for performance monitoring *)
    let end_time = Unix.gettimeofday () in
    let duration = end_time -. start_time in
    Telemetry.record_snapshot_creation_time duration;

    snapshot

(** Update Layer 2 snapshots - move current to previous, set new current *)
let update_layer2_snapshots new_snapshot =
  (* Move current to previous (destroying old previous) *)
  cache.previous_snapshot <- cache.current_snapshot;
  (* Set new current *)
  cache.current_snapshot <- Some new_snapshot;

  Logging.debug_f ~section:"telemetry_cache" "Layer 2 snapshots updated - current version: %d, previous exists: %b"
    new_snapshot.version (Option.is_some cache.previous_snapshot)

(** Add a metric update event to Layer 1 ring buffer and immediately trigger Layer 2 snapshot *)
let add_metric_update_event metric_key event_type =
  let event = {
    timestamp = Unix.gettimeofday ();
    event_type;
    metric_key;
  } in
  RingBuffer.write cache.layer1_ring_buffer event;
  Logging.debug_f ~section:"telemetry_cache" "Added %s event for metric %s to Layer 1 ring buffer"
    event_type metric_key;

  (* Immediately rebuild Layer 2 snapshot and publish when any event is added to Layer 1 *)
  try
    let snapshot = rebuild_snapshot_from_layer1 () in
    update_layer2_snapshots snapshot;

    (* Update last_update timestamp *)
    cache.last_update <- Unix.time ();

    (* Publish snapshot to event bus for streaming *)
    TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus snapshot;
    safe_broadcast_update_condition cache;

    Logging.debug_f ~section:"telemetry_cache" "Snapshot published immediately after Layer 1 update (version: %d) at %.2f"
      snapshot.version snapshot.timestamp
  with exn ->
    Logging.warn_f ~section:"telemetry_cache" "Failed to rebuild snapshot after Layer 1 update: %s"
      (Printexc.to_string exn)

(** Periodic update function that publishes snapshots even when metrics haven't changed *)
let update_telemetry_cache_periodic () =
  let now = Unix.time () in
  let time_since_last = now -. cache.last_update in

  (* Always try to update if enough time has passed, OR if it's been too long since last publish (keepalive) *)
  let should_update = time_since_last >= cache.update_interval in
  let needs_keepalive = time_since_last >= 30.0 in  (* Keepalive every 30 seconds max *)

  if should_update || needs_keepalive then (
    try
      let snapshot = rebuild_snapshot_from_layer1 () in
      update_layer2_snapshots snapshot;
      cache.last_update <- now;

      (* Publish snapshot to event bus for streaming *)
      TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus snapshot;
      safe_broadcast_update_condition cache;

      Logging.debug_f ~section:"telemetry_cache" "Periodic snapshot published (version: %d) at %.2f"
        snapshot.version snapshot.timestamp
    with exn ->
      Logging.warn_f ~section:"telemetry_cache" "Failed to rebuild snapshot during periodic update: %s"
        (Printexc.to_string exn)
  )

(** Wait for next telemetry update *)
let wait_for_update () = Lwt_condition.wait cache.update_condition

(** Subscribe to telemetry snapshot updates *)
let subscribe_telemetry_snapshot () =
  let subscription = TelemetrySnapshotEventBus.subscribe ~persistent:true cache.telemetry_snapshot_event_bus in
  (subscription.stream, subscription.close)

(** Get telemetry event bus subscriber statistics *)
let get_telemetry_subscriber_stats () =
  TelemetrySnapshotEventBus.get_subscriber_stats cache.telemetry_snapshot_event_bus

(** Get current telemetry snapshot (returns cached data, never blocks) *)
let current_telemetry_snapshot () =
  match cache.current_snapshot with
  | Some snapshot -> snapshot
  | None ->
      (* Return empty snapshot if cache not initialized yet *)
      {
        uptime = 0.0;
        metrics = [];
        categories = [];
        timestamp = Unix.time ();
        version = 0;
      }

(** Publish initial telemetry snapshot synchronously *)
let publish_initial_snapshot () =
  Logging.debug ~section:"telemetry_cache" "Publishing initial telemetry snapshot";
  let initial_snapshot = rebuild_snapshot_from_layer1 () in
  update_layer2_snapshots initial_snapshot;
  cache.last_update <- Unix.time ();
  TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus initial_snapshot;
  safe_broadcast_update_condition cache;
  Logging.debug_f ~section:"telemetry_cache" "Initial telemetry snapshot published (version: %d)" initial_snapshot.version

(** Start event-driven telemetry updater (subscribes to telemetry changes) *)
let start_telemetry_updater () =
  Logging.debug ~section:"telemetry_cache" "Starting event-driven telemetry updater";

  (* Start event-driven updater that reacts to telemetry changes *)
  Lwt.async (fun () ->
    Logging.debug ~section:"telemetry_cache" "Starting telemetry event-driven updater";

    (* Wait for system to be ready before starting subscription *)
    (if !system_ready then
      Lwt.return_unit
    else
      Lwt_condition.wait system_ready_condition) >>= fun () ->
    Logging.debug ~section:"telemetry_cache" "Telemetry event-driven updater starting after system ready";

    (* Subscribe to metric changes using stream-based approach *)
    Logging.debug ~section:"telemetry_cache" "Subscribing to metric changes";
    let (metric_stream, _metric_close) = Telemetry.subscribe_metric_changes () in

    let rec process_metric_updates () =
      (* Check for shutdown request *)
      if Atomic.get shutdown_requested then (
        Logging.debug ~section:"telemetry_cache" "Telemetry cache updater shutting down due to shutdown request";
        Lwt.return_unit
      ) else (
        Lwt.catch
          (fun () -> Lwt_stream.get metric_stream >>= function
            | Some () ->
                (* Metrics changed - add event to Layer 1 (this will immediately trigger Layer 2 snapshot rebuild) *)
                Logging.debug ~section:"telemetry_cache" "Telemetry metrics changed, adding event to Layer 1";
                (* Add generic metric update event to Layer 1 - this will immediately trigger snapshot rebuild *)
                add_metric_update_event "all" "metrics_changed";
                (* Continue listening for next change *)
                process_metric_updates ()
            | None ->
                (* Stream ended - wait a bit and try to resubscribe *)
                Logging.debug ~section:"telemetry_cache" "Metric changes subscription stream ended, will resubscribe";
                Lwt_unix.sleep 1.0 >>= process_metric_updates)
          (fun exn ->
            Logging.warn_f ~section:"telemetry_cache" "Metric changes subscription error: %s, will retry" (Printexc.to_string exn);
            Lwt_unix.sleep 1.0 >>= process_metric_updates)
      )
    in
    process_metric_updates ()
  );

  (* Start periodic updater that publishes snapshots even when metrics haven't changed *)
  Lwt.async (fun () ->
    Logging.debug ~section:"telemetry_cache" "Starting periodic telemetry updater";
    
    (* Wait for system to be ready *)
    (if !system_ready then
      Lwt.return_unit
    else
      Lwt_condition.wait system_ready_condition) >>= fun () ->
    
    let rec periodic_loop () =
      (* Check for shutdown request *)
      if Atomic.get shutdown_requested then (
        Logging.debug ~section:"telemetry_cache" "Periodic telemetry updater shutting down due to shutdown request";
        Lwt.return_unit
      ) else (
        (* Update cache periodically (this will check if enough time has passed) *)
        update_telemetry_cache_periodic ();
        (* Sleep 1 second and continue *)
        Lwt_unix.sleep 1.0 >>= periodic_loop
      )
    in
    periodic_loop ()
  )

(** Force cleanup stale subscribers for dashboard memory management *)
let force_cleanup_stale_subscribers () =
  TelemetrySnapshotEventBus.force_cleanup_stale_subscribers cache.telemetry_snapshot_event_bus ()

(** Initialize the telemetry cache system with double-checked locking *)
let init () =
  (* First check without locking (fast path) *)
  if !initialized then (
    Logging.debug ~section:"telemetry_cache" "Telemetry cache already initialized, skipping";
    ()
  ) else (
    (* Acquire lock for initialization *)
    Mutex.lock init_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock init_mutex)
      (fun () ->
        (* Double-check after acquiring lock *)
        if !initialized then (
          Logging.debug ~section:"telemetry_cache" "Telemetry cache already initialized during lock acquisition";
          ()
        ) else (
          initialized := true;
          try
            (* Publish initial snapshot synchronously before starting background updater *)
            publish_initial_snapshot ();
            start_telemetry_updater ();
            Logging.info ~section:"telemetry_cache" "Telemetry cache initialized"
          with exn ->
            Logging.error_f ~section:"telemetry_cache" "Failed to initialize telemetry cache: %s" (Printexc.to_string exn);
            Logging.debug ~section:"telemetry_cache" "Telemetry cache initialization failed"
        )
      )
  )
