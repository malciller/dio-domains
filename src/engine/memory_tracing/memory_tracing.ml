(** Memory Tracing - Public API for comprehensive memory leak detection

    Provides initialization and control functions for the memory tracing system.
    Only active when DIO_MEMORY_TRACING=true environment variable is set.
*)

(** Initialize memory tracing system *)
let init () =
  (* Initialize configuration first to load environment variables *)
  Config.init ();

  if Config.is_memory_tracing_enabled () then begin
    (* Ensure report directory exists early *)
    let report_dir = Config.get_report_directory () in
    (try Unix.mkdir report_dir 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ());

    Logging.info ~section:"memory_tracing" "Initializing memory tracing system...";

    (* Initialize components in dependency order *)
    if Config.should_enable_runtime_events () then
      Runtime_events_tracer.start ();

    if Config.should_enable_domain_profiling () then
      Domain_profiler.init ();

    Leak_detector.init ();
    Reporter.init ();

    (* Start periodic size sampling for dynamic size tracking *)
    let _size_sampling_thread = Allocation_tracker.start_periodic_size_sampling () in

    (* Start periodic reporting *)
    let interval = Config.get_reporting_interval () in
    Reporter.start_periodic_reporting interval;

    Logging.info_f ~section:"memory_tracing" "Memory tracing system initialized (reporting every %d seconds)" interval;
    true
  end else begin
    Logging.debug ~section:"memory_tracing" "Memory tracing disabled (set DIO_MEMORY_TRACING=true to enable)";
    false
  end

(** Shutdown memory tracing system *)
let shutdown () =
  if Config.is_memory_tracing_enabled () then begin
    Logging.info ~section:"memory_tracing" "Shutting down memory tracing system...";

    (* Stop runtime events tracer *)
    Runtime_events_tracer.stop ();

    Logging.info ~section:"memory_tracing" "Memory tracing system shut down";
  end

(** Generate immediate memory report *)
let generate_report format =
  if Config.is_memory_tracing_enabled () then
    Reporter.generate_report_now format
  else
    Lwt.return "Memory tracing is disabled"

(** Get current leak status summary *)
let get_leak_status () =
  if Config.is_memory_tracing_enabled () then
    Some (Leak_detector.get_leak_status_summary ())
  else
    None

(** Force garbage collection and memory analysis *)
let trigger_gc_analysis () =
  if Config.is_memory_tracing_enabled () then begin
    Logging.info ~section:"memory_tracing" "Triggering manual GC and analysis...";
    Runtime_events_tracer.trigger_gc_and_capture ();
    Lwt.return_unit
  end else
    Lwt.return_unit

(** Get runtime events statistics *)
let get_runtime_stats () =
  if Config.is_memory_tracing_enabled () then
    Some (Runtime_events_tracer.get_stats ())
  else
    None

(** Check if memory tracing is currently enabled *)
let is_enabled () = Config.is_memory_tracing_enabled ()

(** Reconfigure tracing (for runtime adjustments) *)
let reconfigure () =
  if Config.is_memory_tracing_enabled () then begin
    Config.load_from_environment ();
    Logging.info ~section:"memory_tracing" "Memory tracing reconfigured";
  end

(** Export types for external use *)
type leak_result = Leak_detector.leak_result = {
  structure_type: string;
  allocation_site: Allocation_tracker.allocation_site;
  domain_id: int;
  severity: [`Low | `Medium | `High | `Critical];
  growth_rate: float;
  current_size: int;
  time_window: float;
  description: string;
  recommendations: string list;
}

type allocation_site = Allocation_tracker.allocation_site = {
  file: string;
  function_name: string;
  line: int;
  backtrace: string;
}

(** Tracked data structure wrappers for manual instrumentation *)
module Tracked = struct
  module Hashtbl = Allocation_tracker.TrackedHashtbl
  module SafeHashtbl = Allocation_tracker.SafeTrackedHashtbl
  module Array = Allocation_tracker.TrackedArray
  module List = Allocation_tracker.TrackedList
  module Map = Allocation_tracker.TrackedMap
  module Queue = Allocation_tracker.TrackedQueue
  module RingBuffer = Allocation_tracker.TrackedRingBuffer
end

(** Manual allocation tracking functions *)
let track_custom_structure = Allocation_tracker.track_custom_structure
let untrack_custom_structure = Allocation_tracker.untrack_custom_structure
let record_reuse = Allocation_tracker.record_reuse

(** Tracking deferral control - for avoiding deadlocks during critical phases *)
let defer_allocation_tracking = Allocation_tracker.defer_tracking
let resume_allocation_tracking = Allocation_tracker.resume_tracking
let is_allocation_tracking_deferred = Allocation_tracker.is_tracking_deferred

(** Domain memory information *)
let get_domain_memory_usage = Domain_profiler.get_domain_memory_mb
let get_domain_allocation_rate = Domain_profiler.get_domain_allocation_rate

(** Utility functions for backtrace capture *)
let get_allocation_site = Allocation_tracker.get_allocation_site

(** Advanced: Direct access to components (use with caution) *)
module Internal = struct
  module Config = Config
  module Runtime_events_tracer = Runtime_events_tracer
  module Allocation_tracker = Allocation_tracker
  module Domain_profiler = Domain_profiler
  module Leak_detector = Leak_detector
  module Reporter = Reporter
  module SafeTrackedHashtbl = Allocation_tracker.SafeTrackedHashtbl
end
