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
}

(** Global telemetry cache instance *)
let cache = {
  current_snapshot = None;
  telemetry_snapshot_event_bus = TelemetrySnapshotEventBus.create "telemetry_snapshot";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 1.0;  (* Update every 1 second for telemetry *)
  consecutive_failures = 0;
  backoff_until = 0.0;
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

(** Create a fresh telemetry snapshot *)
let create_telemetry_snapshot () : telemetry_snapshot =
  let now = Unix.time () in
  let uptime = now -. !Telemetry.start_time in

  Logging.debug_f ~section:"telemetry_cache" "Creating snapshot at %.3f (uptime: %.1f)" now uptime;

  (* Update cached rates and sliding counter snapshots before copying metrics *)
  Telemetry.update_cached_rates ();

  (* Safely copy metrics from telemetry system *)
  let metrics_list = ref [] in
  Telemetry.(
    Mutex.lock metrics_mutex;
    let metric_count = Hashtbl.length metrics in
    Logging.debug_f ~section:"telemetry_cache" "Copying %d metrics from telemetry system" metric_count;
    Hashtbl.iter (fun _ metric -> metrics_list := metric :: !metrics_list) metrics;
    Mutex.unlock metrics_mutex
  );

  let final_metrics = !metrics_list in
  Logging.debug_f ~section:"telemetry_cache" "Snapshot contains %d metrics" (List.length final_metrics);

  (* Group by categories *)
  let categories = Hashtbl.create 16 in
  List.iter (fun metric ->
    let category = Telemetry.categorize_metric metric in
    let existing = Hashtbl.find_opt categories category |> Option.value ~default:[] in
    Hashtbl.replace categories category (metric :: existing)
  ) !metrics_list;

  let category_list =
    [("Connections", []); ("Trading", []); ("Positions", []); ("Domains", []); ("Market Data", []); ("Other", [])]
    |> List.map (fun (cat, _) ->
         match Hashtbl.find_opt categories cat with
         | Some metrics -> (cat, List.sort (fun m1 m2 -> String.compare m1.name m2.name) metrics)
         | None -> (cat, [])
       )
  in

  {
    uptime;
    metrics = !metrics_list;
    categories = category_list;
    timestamp = now;
  }

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
  let initial_snapshot = create_telemetry_snapshot () in
  cache.current_snapshot <- Some initial_snapshot;
  cache.last_update <- Unix.time ();
  TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus initial_snapshot;
  Lwt_condition.broadcast cache.update_condition ();

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
            let snapshot = create_telemetry_snapshot () in
            cache.current_snapshot <- Some snapshot;
            cache.last_update <- now;
            cache.consecutive_failures <- 0;
            cache.backoff_until <- 0.0;
            TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus snapshot;
            Lwt_condition.broadcast cache.update_condition ();
            Logging.debug_f ~section:"telemetry_cache" "Telemetry snapshot updated at %.2f" snapshot.timestamp
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
  )

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
