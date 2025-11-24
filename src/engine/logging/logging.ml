(** Advanced Logging System with Formatting, Styling and Granular Control *)

type level = DEBUG | INFO | WARN | ERROR | CRITICAL

let level_to_string = function
  | DEBUG -> "DEBUG" | INFO -> "INFO" | WARN -> "WARN"
  | ERROR -> "ERROR" | CRITICAL -> "CRITICAL"

let level_of_string s = match String.lowercase_ascii s with
  | "debug" -> Some DEBUG | "info" -> Some INFO | "warn" -> Some WARN
  | "error" -> Some ERROR | "critical" -> Some CRITICAL | _ -> None

let level_to_int = function DEBUG -> 0 | INFO -> 1 | WARN -> 2 | ERROR -> 3 | CRITICAL -> 4

(* ANSI color codes *)
let reset = "\027[0m"
let level_color = function
  | DEBUG -> "\027[2m\027[36m" | INFO -> "\027[32m" | WARN -> "\027[33m"
  | ERROR -> "\027[31m" | CRITICAL -> "\027[1m\027[41m\027[37m"

(* Logging section management *)
type section = { name: string; mutable min_level: level }

(* Global configuration *)
let global_min_level = ref INFO
let sections = Hashtbl.create 32
let use_colors = ref true
let output_channel = ref stderr
let enabled_sections = ref []
let quiet_mode = ref false
let log_callback = ref (fun _level _section _message -> Lwt.return_unit)
let set_enabled_sections secs = enabled_sections := secs
let set_quiet_mode quiet = quiet_mode := quiet
let set_log_callback callback = log_callback := callback

let get_section name =
  match Hashtbl.find_opt sections name with
  | Some s -> s
  | None -> let s = { name; min_level = !global_min_level } in Hashtbl.add sections name s; s

(** Check if a log level will be enabled for a section (avoids allocation) *)
let will_log level section_name =
  let section = get_section section_name in
  (!enabled_sections = [] || List.mem section_name !enabled_sections) &&
  level_to_int level >= level_to_int section.min_level &&
  level_to_int level >= level_to_int !global_min_level

(* Format timestamp - performance optimized *)
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

(* Core logging function with inlined checks for performance *)
let log level section_name message =
  let section = get_section section_name in
  if (!enabled_sections <> [] && not (List.mem section_name !enabled_sections)) ||
     level_to_int level < level_to_int section.min_level ||
     level_to_int level < level_to_int !global_min_level then
    Lwt.return_unit
  else begin
    (* Always call the callback if set (for dashboard log streaming) *)
    let callback_lwt = !log_callback level section_name message in

    if !quiet_mode then callback_lwt
    else begin
      let%lwt () = callback_lwt in
      let timestamp = format_timestamp () in
      let level_str = level_to_string level in
      let formatted =
        if !use_colors then
          Printf.sprintf "%s %s%s%s [%s] %s"
            timestamp (level_color level) level_str reset section_name message
        else
          Printf.sprintf "%s %s [%s] %s" timestamp level_str section_name message
      in
      output_string !output_channel formatted;
      output_char !output_channel '\n';
      flush !output_channel;
      Lwt.return_unit
    end
  end

(* Public API *)
let debug_f ~section fmt =
  Printf.ksprintf (fun msg ->
    if will_log DEBUG section then
      Lwt.async (fun () -> log DEBUG section msg)
    else
      ()
  ) fmt

let info_f ~section fmt =
  Printf.ksprintf (fun msg ->
    if will_log INFO section then
      Lwt.async (fun () -> log INFO section msg)
    else
      ()
  ) fmt

let warn_f ~section fmt =
  Printf.ksprintf (fun msg ->
    if will_log WARN section then
      Lwt.async (fun () -> log WARN section msg)
    else
      ()
  ) fmt

let error_f ~section fmt =
  Printf.ksprintf (fun msg ->
    if will_log ERROR section then
      Lwt.async (fun () -> log ERROR section msg)
    else
      ()
  ) fmt

let critical_f ~section fmt =
  Printf.ksprintf (fun msg ->
    if will_log CRITICAL section then
      Lwt.async (fun () -> log CRITICAL section msg)
    else
      ()
  ) fmt

let debug ~section msg = Lwt.async (fun () -> log DEBUG section msg)
let info ~section msg = Lwt.async (fun () -> log INFO section msg)
let warn ~section msg = Lwt.async (fun () -> log WARN section msg)
let error ~section msg = Lwt.async (fun () -> log ERROR section msg)
let critical ~section msg = Lwt.async (fun () -> log CRITICAL section msg)

(* Configuration *)
let init () = ()
let set_level level = global_min_level := level
let set_section_level name level = (get_section name).min_level <- level
let set_colors enabled = use_colors := enabled
let set_output channel = output_channel := channel
let get_level () = !global_min_level
let get_section_level name = (get_section name).min_level

(* Utility functions *)
let level_to_string = level_to_string
