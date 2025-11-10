(** Memory Cleanup Event Bus - Coordinates event-driven memory cleanup operations

    Provides event-driven coordination for memory cleanup operations across the system.
    Handles batch processing triggers, cleanup coordination, and memory pressure responses.
*)

(** Import memory tracing for allocation tracker access *)
let () = try ignore (Sys.getenv "DIO_MEMORY_TRACING") with Not_found -> ()

(** Memory cleanup event types for coordinating batch operations *)
type cleanup_event =
  | BatchMapUpdate of {
      module_name: string;
      operation_count: int;
      trigger_reason: string;
      priority: int;  (* Higher numbers = higher priority *)
    }
  | BatchListUpdate of {
      module_name: string;
      operation_count: int;
      trigger_reason: string;
      priority: int;
    }
  | DeferredCleanup of {
      module_name: string;
      cleanup_type: string;
      estimated_savings: int;  (* Estimated bytes to be freed *)
      urgency: float;  (* 0.0 = low, 1.0 = critical *)
    }
  | MemoryPressureEvent of {
      pressure_level: string;
      active_allocations: int;
      allocation_threshold: int;
      recommended_actions: string list;
    }
  | AllocationThresholdExceeded of {
      structure_type: string;
      allocation_site: string;
      frequency_per_minute: int;
      threshold: int;
      suggested_batch_size: int;
    }
  | StructureReplaced of {
      old_id: int;
      new_id: int;
      structure_type: string;
      estimated_memory_freed: int;
    }
  | DeferredDestruction of {
      allocation_ids: int list;
      batch_size: int;
      estimated_memory_freed: int;
    }

(** Event bus for memory cleanup coordination *)
module MemoryCleanupEventBus = Concurrency.Event_bus.Make(struct
  type t = cleanup_event
end)

(** Global memory cleanup event bus instance *)
let cleanup_event_bus = MemoryCleanupEventBus.create "memory_cleanup"

(** Publish batch map update event *)
let publish_batch_map_update module_name operation_count trigger_reason priority =
  let event = BatchMapUpdate {
    module_name;
    operation_count;
    trigger_reason;
    priority;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Publish batch list update event *)
let publish_batch_list_update module_name operation_count trigger_reason priority =
  let event = BatchListUpdate {
    module_name;
    operation_count;
    trigger_reason;
    priority;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Publish deferred cleanup event *)
let publish_deferred_cleanup module_name cleanup_type estimated_savings urgency =
  let event = DeferredCleanup {
    module_name;
    cleanup_type;
    estimated_savings;
    urgency;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Publish memory pressure event *)
let publish_memory_pressure pressure_level active_allocations allocation_threshold recommended_actions =
  let event = MemoryPressureEvent {
    pressure_level;
    active_allocations;
    allocation_threshold;
    recommended_actions;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Publish allocation threshold exceeded event *)
let publish_allocation_threshold_exceeded structure_type allocation_site frequency_per_minute threshold suggested_batch_size =
  let event = AllocationThresholdExceeded {
    structure_type;
    allocation_site;
    frequency_per_minute;
    threshold;
    suggested_batch_size;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Publish structure replaced event *)
let publish_structure_replaced old_id new_id structure_type estimated_memory_freed =
  let event = StructureReplaced {
    old_id;
    new_id;
    structure_type;
    estimated_memory_freed;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Publish deferred destruction event *)
let publish_deferred_destruction allocation_ids batch_size estimated_memory_freed =
  let event = DeferredDestruction {
    allocation_ids;
    batch_size;
    estimated_memory_freed;
  } in
  MemoryCleanupEventBus.publish cleanup_event_bus event

(** Subscribe to cleanup events *)
let subscribe_cleanup_events () =
  let subscription = MemoryCleanupEventBus.subscribe cleanup_event_bus in
  (subscription.stream, subscription.close)

(** Get cleanup event bus subscriber statistics *)
let get_cleanup_subscriber_stats () =
  MemoryCleanupEventBus.get_subscriber_stats cleanup_event_bus

(** Force cleanup stale subscribers *)
let force_cleanup_stale_subscribers () =
  MemoryCleanupEventBus.force_cleanup_stale_subscribers cleanup_event_bus ()

(** Allocation frequency tracking for threshold detection - now tracks timestamps *)
let allocation_timestamps = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.create 32  (* key -> timestamp list *)
let frequency_mutex = Mutex.create ()
let last_cleanup_time = ref (Unix.time ())
let allocation_window_seconds = 10.0  (* Only count allocations within last 10 seconds *)

(** Track total allocation sizes by structure type *)
let allocation_sizes = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.create 32
let sizes_mutex = Mutex.create ()

(** Track last memory pressure event time per structure type to prevent spam *)
let last_memory_pressure_time = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.create 32
let memory_pressure_cooldown = 60.0  (* Minimum seconds between memory pressure events per structure type *)

(** Track allocations by domain *)
let domain_allocations = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.create 16
let domain_mutex = Mutex.create ()

(** Initialize cleanup event system and subscribe to allocation events *)
let init () =
  Logging.debug ~section:"memory_cleanup_events" "Initialized memory cleanup event system"

(** Generic allocation event processing - to be called externally to avoid circular dependencies *)
let process_allocation_event_generic structure_type site_file site_function line size domain_id timestamp =
  let key = structure_type ^ ":" ^ site_file ^ ":" ^ site_function in
  Mutex.lock frequency_mutex;
  let now = Unix.time () in
  if now -. !last_cleanup_time > 60.0 then begin
    Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.clear allocation_timestamps;
    Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.clear last_memory_pressure_time;  (* Also clear memory pressure cooldown tracking *)
    last_cleanup_time := now
  end;
  let current_timestamps = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.find_opt allocation_timestamps key |> Option.value ~default:[] in
  let updated_timestamps = now :: current_timestamps in
  Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace allocation_timestamps key updated_timestamps;
  let recent_timestamps = List.filter (fun ts -> now -. ts <= allocation_window_seconds) updated_timestamps in
  Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace allocation_timestamps key recent_timestamps;
  let current_count = List.length recent_timestamps in
  if current_count >= 200 then begin
    let frequency_per_minute = current_count * 6 in
    publish_allocation_threshold_exceeded
      structure_type
      (Printf.sprintf "%s:%s:%d" site_file site_function line)
      frequency_per_minute
      200
      (min 100 (current_count / 2));
    Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.remove allocation_timestamps key
  end;
  Mutex.unlock frequency_mutex;
  Mutex.lock sizes_mutex;
  let current_size = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.find_opt allocation_sizes structure_type |> Option.value ~default:0 in
  Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace allocation_sizes structure_type (current_size + size);
  if current_size + size > 10_000_000 then begin
    (* Check cooldown period to prevent spam *)
    let last_event_time = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.find_opt last_memory_pressure_time structure_type |> Option.value ~default:0.0 in
    let now = Unix.time () in
    if now -. last_event_time >= memory_pressure_cooldown then begin
      publish_memory_pressure
        "high"
        0
        10_000_000
        [Printf.sprintf "High memory usage for %s: %d bytes" structure_type (current_size + size)];
      Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace last_memory_pressure_time structure_type now
    end
  end;
  Mutex.unlock sizes_mutex;
  Mutex.lock domain_mutex;
  let domain_count = Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.find_opt domain_allocations domain_id |> Option.value ~default:0 in
  Dio_memory_tracing.Memory_tracing.Tracked.Hashtbl.replace domain_allocations domain_id (domain_count + 1);
  Logging.debug_f ~section:"memory_cleanup_events"
    "Allocation: %s (%d bytes) in domain %d at %s:%s:%d (timestamp: %.2f)"
    structure_type size domain_id site_file site_function line timestamp;
  Mutex.unlock domain_mutex;
  ()

let ensure_allocation_tracker_subscription () =
  try
    Dio_memory_tracing.Allocation_tracker.register_allocation_event_processor process_allocation_event_generic;
    Logging.debug ~section:"memory_cleanup_events" "Registered allocation event processor with allocation tracker";
    ()
  with exn ->
    Logging.debug_f ~section:"memory_cleanup_events" "Failed to register allocation event processor: %s" (Printexc.to_string exn);
    ()

