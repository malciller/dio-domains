(** Structured logging system with ANSI formatting, per-section level filtering,
    and domain-safe synchronous output. *)

type level = DEBUG | INFO | WARN | ERROR | CRITICAL

let level_to_string = function
  | DEBUG -> "DEBUG" | INFO -> "INFO" | WARN -> "WARN"
  | ERROR -> "ERROR" | CRITICAL -> "CRITICAL"

let level_of_string s = match String.lowercase_ascii s with
  | "debug" -> Some DEBUG | "info" -> Some INFO | "warn" -> Some WARN
  | "error" -> Some ERROR | "critical" -> Some CRITICAL | _ -> None

let level_to_int = function DEBUG -> 0 | INFO -> 1 | WARN -> 2 | ERROR -> 3 | CRITICAL -> 4

(* ANSI escape sequences for terminal color output per log level. *)
let reset = "\027[0m"
let level_color = function
  | DEBUG -> "\027[2m\027[36m" | INFO -> "\027[32m" | WARN -> "\027[33m"
  | ERROR -> "\027[31m" | CRITICAL -> "\027[1m\027[41m\027[37m"

(* Per-section log level configuration. *)
type section = { name: string; mutable min_level: level }

(* Global mutable configuration state. *)
let global_min_level = ref INFO
let sections = Hashtbl.create 32
let use_colors = ref true
let output_channel = ref stderr
let enabled_sections = ref []
let quiet_mode = ref false
let log_callback : (level -> string -> string -> unit Lwt.t) ref = ref (fun _level _section _message -> Lwt.return_unit)
let set_enabled_sections secs = enabled_sections := secs
let set_quiet_mode quiet = quiet_mode := quiet
let set_log_callback callback = log_callback := callback

(** Mutex serializing output_channel writes across OCaml 5.x domains
    to prevent interleaved log lines from concurrent workers. *)
let output_mutex = Mutex.create ()

let get_section name =
  match Hashtbl.find_opt sections name with
  | Some s -> s
  | None ->
      Mutex.lock output_mutex;
      let s = match Hashtbl.find_opt sections name with
        | Some s -> s
        | None ->
            let s = { name; min_level = !global_min_level } in
            Hashtbl.replace sections name s;
            s
      in
      Mutex.unlock output_mutex;
      s

(** Returns true if [level] passes both the section and global minimum
    level filters. Used as a guard to skip allocation on disabled paths. *)
let will_log level section_name =
  let section = get_section section_name in
  (!enabled_sections = [] || List.mem section_name !enabled_sections) &&
  level_to_int level >= level_to_int section.min_level &&
  level_to_int level >= level_to_int !global_min_level

(* Formats the current wall-clock time as "YYYY-MM-DD HH:MM:SS.mmm".
   Caches the date/time prefix per second to avoid repeated localtime calls. *)
let format_timestamp =
  let last_sec = ref 0.0 in
  let last_ts = ref "" in
  fun () ->
    let time = Unix.gettimeofday () in
    let sec = floor time in
    let ms = int_of_float ((time -. sec) *. 1000.) in
    if sec <> !last_sec then begin
      let tm = Unix.localtime time in
      last_ts := Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
        (tm.Unix.tm_year + 1900) (tm.Unix.tm_mon + 1) tm.Unix.tm_mday
        tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec;
      last_sec := sec
    end;
    !last_ts ^ Printf.sprintf ".%03d" ms

(* Core synchronous logging function. Domain-safe via [output_mutex].
   Intentionally avoids Lwt: domain workers lack a Lwt scheduler,
   and Lwt.async from domains would leak promises into the main scheduler. *)
let log_sync level section_name message =
  let section = get_section section_name in
  if (!enabled_sections <> [] && not (List.mem section_name !enabled_sections)) ||
     level_to_int level < level_to_int section.min_level ||
     level_to_int level < level_to_int !global_min_level then
    ()
  else if !quiet_mode then
    (* Quiet mode: suppress all console output. Callback dispatch, if needed,
       is restricted to the Lwt domain to avoid thread-safety violations. *)
    ()
  else begin
    let timestamp = format_timestamp () in
    let level_str = level_to_string level in
    let formatted =
      if !use_colors then
        Printf.sprintf "%s %s%s%s [%s] %s"
          timestamp (level_color level) level_str reset section_name message
      else
        Printf.sprintf "%s %s [%s] %s" timestamp level_str section_name message
    in
    Mutex.lock output_mutex;
    (try
       output_string !output_channel formatted;
       output_char !output_channel '\n';
       flush !output_channel;
       Mutex.unlock output_mutex
     with exn ->
       Mutex.unlock output_mutex;
       ignore exn)
  end

(* Lwt wrapper: delegates to [log_sync] then returns [Lwt.return_unit]. *)
let log level section_name message =
  log_sync level section_name message;
  Lwt.return_unit

(* Format-string log API. Zero-allocation when the level is disabled:
   [Printf.ifprintf] consumes format arguments without allocating a string;
   [Printf.ksprintf] allocates a buffer only when the message will be emitted. *)
let debug_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log DEBUG section then
    Printf.ksprintf (fun msg -> log_sync DEBUG section msg) fmt
  else
    Printf.ifprintf () fmt

let info_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log INFO section then
    Printf.ksprintf (fun msg -> log_sync INFO section msg) fmt
  else
    Printf.ifprintf () fmt

let warn_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log WARN section then
    Printf.ksprintf (fun msg -> log_sync WARN section msg) fmt
  else
    Printf.ifprintf () fmt

let error_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log ERROR section then
    Printf.ksprintf (fun msg -> log_sync ERROR section msg) fmt
  else
    Printf.ifprintf () fmt

let critical_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log CRITICAL section then
    Printf.ksprintf (fun msg -> log_sync CRITICAL section msg) fmt
  else
    Printf.ifprintf () fmt

let debug ~section msg =
  if will_log DEBUG section then log_sync DEBUG section msg
let info ~section msg =
  if will_log INFO section then log_sync INFO section msg
let warn ~section msg =
  if will_log WARN section then log_sync WARN section msg
let error ~section msg =
  if will_log ERROR section then log_sync ERROR section msg
let critical ~section msg =
  if will_log CRITICAL section then log_sync CRITICAL section msg

(* Global and per-section configuration accessors. *)
let init () = ()
let set_level level = global_min_level := level
let set_section_level name level = (get_section name).min_level <- level
let set_colors enabled = use_colors := enabled
let set_output channel = output_channel := channel
let get_level () = !global_min_level
let get_section_level name = (get_section name).min_level

(* Re-exported utility. *)
let level_to_string = level_to_string
