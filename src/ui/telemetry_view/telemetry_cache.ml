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
  mutable pending_update: bool;  (* Whether we have pending changes *)
}

(** Global telemetry cache instance *)
let cache = {
  current_snapshot = None;
  telemetry_snapshot_event_bus = TelemetrySnapshotEventBus.create "telemetry_snapshot";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 0.1;  (* Update at most every 100ms *)
  pending_update = false;
}

(** Initialization guard *)
let initialized = ref false

(** Create a fresh telemetry snapshot *)
let create_telemetry_snapshot () : telemetry_snapshot =
  let now = Unix.time () in
  let uptime = now -. !Telemetry.start_time in

  (* Safely copy metrics from telemetry system *)
  let metrics_list = ref [] in
  Telemetry.(
    Mutex.lock metrics_mutex;
    Hashtbl.iter (fun _ metric -> metrics_list := metric :: !metrics_list) metrics;
    Mutex.unlock metrics_mutex
  );

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
    timestamp = cache.last_update;  (* Use last update time instead of current time *)
  }

(** Update telemetry cache with throttling *)
let update_telemetry_cache () =
  cache.pending_update <- true;
  let now = Unix.time () in
  if now -. cache.last_update >= cache.update_interval then (
    let snapshot = create_telemetry_snapshot () in
    cache.current_snapshot <- Some snapshot;
    cache.last_update <- now;
    cache.pending_update <- false;
    TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus snapshot;
    Lwt_condition.broadcast cache.update_condition ()
  )

(** Wait for next telemetry update *)
let wait_for_update () = Lwt_condition.wait cache.update_condition

(** Subscribe to telemetry snapshot updates *)
let subscribe_telemetry_snapshot () = TelemetrySnapshotEventBus.subscribe cache.telemetry_snapshot_event_bus

(** Get current telemetry snapshot (blocking) *)
let current_telemetry_snapshot () =
  (* Always create fresh snapshot for UI - uptime and timestamp must be current *)
  let snapshot = create_telemetry_snapshot () in
  cache.current_snapshot <- Some snapshot;
  snapshot

(** Start background telemetry updater (event-driven with throttling) *)
let start_telemetry_updater () =
  (* Ensure we have an initial update *)
  let initial_snapshot = create_telemetry_snapshot () in
  cache.current_snapshot <- Some initial_snapshot;
  cache.last_update <- Unix.time ();
  TelemetrySnapshotEventBus.publish cache.telemetry_snapshot_event_bus initial_snapshot;
  Lwt_condition.broadcast cache.update_condition ();

  let rec loop () =
    Telemetry.wait_for_metric_changes () >>= fun () ->
    update_telemetry_cache ();
    loop ()
  in
  Lwt.async loop

(** Initialize the telemetry cache system *)
let init () =
  if !initialized then (
    Logging.debug ~section:"telemetry_cache" "Telemetry cache already initialized, skipping";
    ()
  ) else (
    initialized := true;
    start_telemetry_updater ();
    Logging.info ~section:"telemetry_cache" "Telemetry cache initialized"
  )
