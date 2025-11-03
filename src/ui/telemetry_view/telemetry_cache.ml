(** Telemetry-specific cache for the telemetry view

    Provides reactive access to telemetry data for the telemetry dashboard UI,
    respecting the concurrency model.
*)

open Lwt.Infix
open Telemetry
open Concurrency

open Ui_types

(** Event bus for telemetry snapshots *)
module TelemetrySnapshotEventBus = Event_bus.Make(struct
  type t = telemetry_snapshot
end)

(** Telemetry cache state *)
type t = {
  mutable current_snapshot: telemetry_snapshot option;
  telemetry_snapshot_event_bus: TelemetrySnapshotEventBus.t;
  update_condition: unit Lwt_condition.t;
  mutable last_update: float;
  update_interval: float;  (* Minimum time between updates *)
  mutable consecutive_failures: int;  (* Track consecutive failures for backoff *)
  mutable backoff_until: float;  (* When to resume updates after backoff *)
  mutable last_metrics_hash: string;  (* Track hash of metrics for deduplication *)
}

(** Global telemetry cache instance *)
let cache = {
  current_snapshot = None;
  telemetry_snapshot_event_bus = TelemetrySnapshotEventBus.create "telemetry_snapshot";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 3.0;  (* Update every 3 seconds for dashboard mode - reduced frequency *)
  consecutive_failures = 0;
  backoff_until = 0.0;
  last_metrics_hash = "";
}

(** Initialization guard with mutex protection *)
let initialized = ref false
let init_mutex = Mutex.create ()

(** System initialization ready barrier *)
let system_ready = ref false
let system_ready_condition = Lwt_condition.create ()

(** Signal that system is ready for cache access *)
let signal_system_ready () =
  if not !system_ready then (
    system_ready := true;
    Lwt_condition.broadcast system_ready_condition ()
  )

(** Create a fresh telemetry snapshot with deduplication *)
let create_telemetry_snapshot () : telemetry_snapshot option =
  let start_time = Unix.gettimeofday () in
  let now = Unix.time () in
  let uptime = now -. !Telemetry.start_time in

  (* Update cached rates and sliding counter snapshots before copying metrics *)
  Telemetry.update_cached_rates ();

  (* Update sliding window statistics for monitoring *)
  Telemetry.update_sliding_window_stats ();

  (* Compute hash of current metrics for deduplication *)
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

  (* Check if metrics have changed since last snapshot *)
  if current_hash = cache.last_metrics_hash then (
    Logging.debug ~section:"telemetry_cache" "Metrics unchanged, skipping snapshot creation";
    None  (* No change, don't create new snapshot *)
  ) else (
    Logging.debug_f ~section:"telemetry_cache" "Creating snapshot at %.3f (uptime: %.1f)" now uptime;
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
            { metric with
              metric_type = SlidingCounter lightweight_sliding;
              rate_tracker = None;  (* Don't copy rate tracker to save memory *)
            }
        | Counter _ | Gauge _ | Histogram _ ->
            (* For other metric types, copy as-is but without rate_tracker to save memory *)
            { metric with rate_tracker = None }
        in
        lightweight_metric :: acc
      ) metrics [] in

      lightweight_metrics
    ) in
    Mutex.unlock metrics_mutex;

    let final_metrics = metrics_list in
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

    let result = Some {
      uptime;
      metrics = final_metrics;
      categories = category_list;
      timestamp = now;
    } in

    (* Record snapshot creation time for performance monitoring *)
    let end_time = Unix.gettimeofday () in
    let duration = end_time -. start_time in
    Telemetry.record_snapshot_creation_time duration;

    result
  )

(** Wait for next telemetry update *)
let wait_for_update () = Lwt_condition.wait cache.update_condition

(** Subscribe to telemetry snapshot updates *)
let subscribe_telemetry_snapshot () = TelemetrySnapshotEventBus.subscribe cache.telemetry_snapshot_event_bus

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
      }

(** Start background telemetry updater (polling-based) *)
let start_telemetry_updater () =
  Logging.debug ~section:"telemetry_cache" "Starting background telemetry updater";
  (* Ensure we have an initial update *)
  (match create_telemetry_snapshot () with
   | Some initial_snapshot ->
       cache.current_snapshot <- Some initial_snapshot;
       cache.last_update <- Unix.time ();
       TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus initial_snapshot;
       Lwt_condition.broadcast cache.update_condition ()
   | None ->
       (* No metrics yet, create empty snapshot *)
       let empty_snapshot = {
         uptime = 0.0;
         metrics = [];
         categories = [];
         timestamp = Unix.time ();
       } in
       cache.current_snapshot <- Some empty_snapshot;
       cache.last_update <- Unix.time ();
       TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus empty_snapshot;
       Lwt_condition.broadcast cache.update_condition ()
  );

  (* Start periodic polling updater *)
  Lwt.async (fun () ->
    Logging.debug_f ~section:"telemetry_cache" "Starting telemetry polling updater";

    (* Wait for system to be ready before starting polling *)
    (if !system_ready then
      Lwt.return_unit
    else
      Lwt_condition.wait system_ready_condition) >>= fun () ->
    Logging.debug_f ~section:"telemetry_cache" "Telemetry polling updater starting after system ready";

    let rec polling_loop () =
      let now = Unix.time () in
      let time_since_last = now -. cache.last_update in

      (* Check if we're in backoff period *)
      let in_backoff = now < cache.backoff_until in

      if in_backoff then (
        Lwt_unix.sleep 1.0 >>= polling_loop
      ) else (
        if time_since_last >= cache.update_interval then (
          (try
            match create_telemetry_snapshot () with
            | Some snapshot ->
                (* Publish snapshot to event bus first *)
                TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus snapshot;
                Lwt_condition.broadcast cache.update_condition ();
                (* Clear cache.current_snapshot immediately after publishing to prevent memory accumulation *)
                cache.current_snapshot <- None;
                cache.last_update <- now;
                cache.consecutive_failures <- 0;
                cache.backoff_until <- 0.0;
                Logging.debug_f ~section:"telemetry_cache" "Telemetry snapshot published and cache cleared at %.2f" snapshot.timestamp
            | None ->
                (* Metrics unchanged, just update timestamp *)
                cache.last_update <- now;
                Logging.debug_f ~section:"telemetry_cache" "Metrics unchanged, skipping snapshot update at %.2f" now
          with exn ->
            cache.consecutive_failures <- cache.consecutive_failures + 1;
            let backoff_seconds = min 60.0 (2.0 *. (2.0 ** float_of_int (min cache.consecutive_failures 5))) in
            cache.backoff_until <- now +. backoff_seconds;
            Logging.warn_f ~section:"telemetry_cache" "Failed to update telemetry snapshot (attempt %d): %s, backing off for %.1f seconds"
              cache.consecutive_failures (Printexc.to_string exn) backoff_seconds
          );
        );
        Lwt_unix.sleep 1.0 >>= polling_loop (* Poll every second *)
      )
    in
    polling_loop () (* No initial delay once system is ready *)
  );

  (* Add periodic event bus cleanup - every 5 minutes *)
  Lwt.async (fun () ->
    Logging.debug ~section:"telemetry_cache" "Starting event bus cleanup loop";
    let rec cleanup_loop () =
      let%lwt () = Lwt_unix.sleep 300.0 in (* Clean up every 5 minutes *)
      let removed_opt = TelemetrySnapshotEventBus.cleanup_stale_subscribers cache.telemetry_snapshot_event_bus () in
      (match removed_opt with
       | Some removed_count ->
           if removed_count > 0 then
             Logging.info_f ~section:"telemetry_cache" "Cleaned up %d stale telemetry subscribers" removed_count
       | None -> ());

      (* Report subscriber statistics to telemetry *)
      let (total, active, _) = TelemetrySnapshotEventBus.get_subscriber_stats cache.telemetry_snapshot_event_bus in
      Telemetry.set_event_bus_subscribers_total total;
      Telemetry.set_event_bus_subscribers_active active;
      cleanup_loop ()
    in
    cleanup_loop ()
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
            start_telemetry_updater ();
            Logging.info ~section:"telemetry_cache" "Telemetry cache initialized"
          with exn ->
            Logging.error_f ~section:"telemetry_cache" "Failed to initialize telemetry cache: %s" (Printexc.to_string exn);
            Logging.debug ~section:"telemetry_cache" "Telemetry cache initialization failed"
        )
      )
  )
