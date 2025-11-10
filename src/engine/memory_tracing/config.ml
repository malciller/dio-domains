(** Memory Tracing Configuration

    Controls enabling/disabling of memory tracing and configures tracing granularity.
    Uses environment variables for easy development/production separation.
*)

(** Tracing configuration *)
type t = {
  mutable enabled: bool;
  mutable sample_rate: float;  (* 1.0 = sample everything, 0.1 = sample 10% *)
  mutable track_allocations: bool;
  mutable track_deallocations: bool;
  mutable track_hashtables: bool;
  mutable track_arrays: bool;
  mutable track_lists: bool;  (* Only large lists *)
  mutable track_custom_structures: bool;
  mutable enable_runtime_events: bool;
  mutable enable_domain_profiling: bool;
  mutable enable_leak_detection: bool;
  mutable reporting_interval_seconds: int;
  mutable max_allocation_history: int;
  mutable leak_detection_sensitivity: [`Low | `Medium | `High];
  mutable max_report_allocations: int;  (* 0 = unlimited, show all allocations *)
  mutable report_directory: string;
  mutable report_file_prefix: string;
  mutable runtime_events_directory: string;
}

(** Global configuration instance *)
let config = {
  enabled = false;
  sample_rate = 0.01;  (* Sample only 1% of allocations to reduce overhead *)
  track_allocations = true;
  track_deallocations = true;
  track_hashtables = true;  (* Disable hashtable tracking to reduce overhead *)
  track_arrays = true;  (* Disable array tracking to reduce overhead *)
  track_lists = true;  (* Only track large lists to avoid overhead *)
  track_custom_structures = true;  (* Disable custom structure tracking *)
  enable_runtime_events = true;  (* Disable runtime events *)
  enable_domain_profiling = true;  (* Disable domain profiling *)
  enable_leak_detection = true;  (* Keep leak detection but with reduced tracking *)
  reporting_interval_seconds = 60;  (* Report every minute *)
  max_allocation_history = 1000;  (* Keep only 1000 allocations in history *)
  leak_detection_sensitivity = `Low;  (* Reduce sensitivity to avoid false positives *)
  max_report_allocations = 100;  (* Limit reports to 100 allocations *)
  report_directory = "memory_trace";
  report_file_prefix = "memory_trace";
  runtime_events_directory = "memory_trace";
}

(** Environment variable names *)
let env_enabled = "DIO_MEMORY_TRACING"
let env_sample_rate = "DIO_MEMORY_SAMPLE_RATE"
let env_track_hashtables = "DIO_MEMORY_TRACK_HASHTABLES"
let env_track_arrays = "DIO_MEMORY_TRACK_ARRAYS"
let env_track_lists = "DIO_MEMORY_TRACK_LISTS"
let env_track_custom = "DIO_MEMORY_TRACK_CUSTOM"
let env_runtime_events = "DIO_MEMORY_RUNTIME_EVENTS"
let env_domain_profiling = "DIO_MEMORY_DOMAIN_PROFILING"
let env_leak_detection = "DIO_MEMORY_LEAK_DETECTION"
let env_reporting_interval = "DIO_MEMORY_REPORT_INTERVAL"
let env_max_history = "DIO_MEMORY_MAX_HISTORY"
let env_max_report_allocations = "DIO_MEMORY_MAX_REPORT_ALLOCATIONS"
let env_sensitivity = "DIO_MEMORY_SENSITIVITY"
let env_report_directory = "DIO_MEMORY_REPORT_DIRECTORY"
let env_report_prefix = "DIO_MEMORY_REPORT_PREFIX"
let env_runtime_events_directory = "DIO_MEMORY_RUNTIME_EVENTS_DIRECTORY"

(** Parse boolean environment variable *)
let parse_bool_env var_name default =
  match Sys.getenv_opt var_name with
  | Some "true" | Some "1" | Some "yes" | Some "on" -> true
  | Some "false" | Some "0" | Some "no" | Some "off" -> false
  | Some _ -> Logging.warn_f ~section:"memory_config" "Invalid boolean value for %s, using default" var_name; default
  | None -> default

(** Parse float environment variable *)
let parse_float_env var_name default =
  match Sys.getenv_opt var_name with
  | Some str ->
      (try float_of_string str
       with _ -> Logging.warn_f ~section:"memory_config" "Invalid float value for %s, using default" var_name; default)
  | None -> default

(** Parse int environment variable *)
let parse_int_env var_name default =
  match Sys.getenv_opt var_name with
  | Some str ->
      (try int_of_string str
       with _ -> Logging.warn_f ~section:"memory_config" "Invalid int value for %s, using default" var_name; default)
  | None -> default

(** Parse sensitivity environment variable *)
let parse_sensitivity_env var_name default =
  match Sys.getenv_opt var_name with
  | Some "low" | Some "Low" -> `Low
  | Some "medium" | Some "Medium" -> `Medium
  | Some "high" | Some "High" -> `High
  | Some _ -> Logging.warn_f ~section:"memory_config" "Invalid sensitivity value for %s, using default" var_name; default
  | None -> default

(** Load configuration from environment variables *)
let load_from_environment () =
  config.enabled <- parse_bool_env env_enabled false;
  config.sample_rate <- parse_float_env env_sample_rate 1.0;
  config.track_hashtables <- parse_bool_env env_track_hashtables true;
  config.track_arrays <- parse_bool_env env_track_arrays true;
  config.track_lists <- parse_bool_env env_track_lists false;
  config.track_custom_structures <- parse_bool_env env_track_custom true;
  config.enable_runtime_events <- parse_bool_env env_runtime_events true;
  config.enable_domain_profiling <- parse_bool_env env_domain_profiling true;
  config.enable_leak_detection <- parse_bool_env env_leak_detection true;
  config.reporting_interval_seconds <- parse_int_env env_reporting_interval 300;
  config.max_allocation_history <- parse_int_env env_max_history 10000;
  config.max_report_allocations <- parse_int_env env_max_report_allocations 0;
  config.leak_detection_sensitivity <- parse_sensitivity_env env_sensitivity `Medium;

  (* Report directory can be any string *)
  (match Sys.getenv_opt env_report_directory with
   | Some dir -> config.report_directory <- dir
   | None -> ());

  (* Report prefix can be any string *)
  (match Sys.getenv_opt env_report_prefix with
   | Some prefix -> config.report_file_prefix <- prefix
   | None -> ());

  (* Runtime events directory can be any string *)
  (match Sys.getenv_opt env_runtime_events_directory with
   | Some dir -> config.runtime_events_directory <- dir
   | None -> ());

  (* Derived settings *)
  config.track_allocations <- config.track_hashtables || config.track_arrays || config.track_lists || config.track_custom_structures;
  config.track_deallocations <- config.track_allocations;

  (* Log configuration if enabled *)
  if config.enabled then begin
    Logging.info_f ~section:"memory_config" "Memory tracing enabled with sample rate %.2f" config.sample_rate;
    Logging.debug_f ~section:"memory_config" "Tracking: hashtables=%B, arrays=%B, lists=%B, custom=%B"
      config.track_hashtables config.track_arrays config.track_lists config.track_custom_structures;
    Logging.debug_f ~section:"memory_config" "Features: runtime_events=%B, domain_profiling=%B, leak_detection=%B"
      config.enable_runtime_events config.enable_domain_profiling config.enable_leak_detection;
    Logging.debug_f ~section:"memory_config" "Reporting: interval=%ds, max_history=%d, sensitivity=%s, directory=%s"
      config.reporting_interval_seconds config.max_allocation_history
      (match config.leak_detection_sensitivity with `Low -> "low" | `Medium -> "medium" | `High -> "high")
      config.report_directory;
    Logging.debug_f ~section:"memory_config" "Runtime events directory: %s" config.runtime_events_directory;
  end

(** Check if memory tracing is enabled *)
let is_memory_tracing_enabled () = config.enabled

(** Check if sampling should be applied (for performance) *)
let should_sample () =
  if config.sample_rate >= 1.0 then true
  else Random.float 1.0 <= config.sample_rate

(** Check if hashtable tracking is enabled *)
let should_track_hashtables () = config.enabled && config.track_hashtables

(** Check if array tracking is enabled *)
let should_track_arrays () = config.enabled && config.track_arrays

(** Check if list tracking is enabled *)
let should_track_lists () = config.enabled && config.track_lists

(** Check if custom structure tracking is enabled *)
let should_track_custom_structures () = config.enabled && config.track_custom_structures

(** Check if runtime events are enabled *)
let should_enable_runtime_events () = config.enabled && config.enable_runtime_events

(** Check if domain profiling is enabled *)
let should_enable_domain_profiling () = config.enabled && config.enable_domain_profiling

(** Check if leak detection is enabled *)
let should_enable_leak_detection () = config.enabled && config.enable_leak_detection

(** Get reporting interval in seconds *)
let get_reporting_interval () = config.reporting_interval_seconds

(** Get maximum allocation history size *)
let get_max_allocation_history () = config.max_allocation_history

(** Get leak detection sensitivity *)
let get_leak_detection_sensitivity () = config.leak_detection_sensitivity

(** Get maximum report allocations (0 = unlimited) *)
let get_max_report_allocations () = config.max_report_allocations

(** Sensitivity thresholds record *)
type sensitivity_thresholds = {
  growth_rate_low: float;
  growth_rate_medium: float;
  growth_rate_high: float;
  size_low: int;
  size_medium: int;
  size_high: int;
}

(** Get report directory - ensure it's an absolute path *)
let get_report_directory () =
  let dir = config.report_directory in
  if Filename.is_relative dir then
    Filename.concat (Sys.getcwd ()) dir
  else
    dir

(** Get report file prefix *)
let get_report_file_prefix () = config.report_file_prefix

(** Get runtime events directory - use the same directory as reports for consistency *)
let get_runtime_events_directory () =
  get_report_directory ()  (* Always use the same directory as reports *)

(** Adjust sensitivity thresholds based on configuration *)
let get_sensitivity_thresholds () =
  match config.leak_detection_sensitivity with
  | `Low ->
      { growth_rate_low = 10.0; growth_rate_medium = 100.0; growth_rate_high = 1000.0;
        size_low = 1000; size_medium = 10000; size_high = 100000 }
  | `Medium ->
      { growth_rate_low = 1.0; growth_rate_medium = 10.0; growth_rate_high = 100.0;
        size_low = 100; size_medium = 1000; size_high = 10000 }
  | `High ->
      { growth_rate_low = 0.1; growth_rate_medium = 1.0; growth_rate_high = 10.0;
        size_low = 10; size_medium = 100; size_high = 1000 }

(** Programmatically enable/disable tracing (for testing) *)
let set_enabled enabled =
  config.enabled <- enabled;
  if enabled then load_from_environment ()

(** Set sample rate programmatically *)
let set_sample_rate rate =
  config.sample_rate <- max 0.0 (min 1.0 rate)

(** Initialize configuration - called at startup *)
let init () =
  load_from_environment ()
