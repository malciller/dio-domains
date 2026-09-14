(** Structured logging system with ANSI formatting, per-section level filtering,
    and domain-safe asynchronous output.

    Line layout (aligned):
      HH:MM:SS.mmm LVL  SECTION         message
      19:17:23.672 INFO oracle_runtime  [2/8] hyperliquid/BTC/USDC ACTIVE ...
      19:17:23.672 INFO oracle_runtime   ┆ worst drop 83.6% (peak $19497.40 ...
    - Time-only timestamp, gray; the date is available from
      `docker logs --timestamps`.
    - Level column fixed at 5 chars, colored by severity.
    - Section column fixed at 20 chars so the message column never shifts, and
      colored with a stable per-section hue.
    - Multi-line messages render continuation lines under a dim ┆ gutter at the
      message column instead of a repeated prefix.
    - Lines are word-wrapped to the terminal width (auto-detected or a fixed
      `logging_width` override). Non-terminal output uses `COLUMNS` when set,
      else 200, so wrapped lines keep the gutter without being cramped.
    - With colors disabled the layout and alignment are preserved without ANSI
      escapes.

    Hot-path contract:
    - All levels except CRITICAL are formatted and pushed to an async queue; the
      caller performs zero I/O and never drains the queue.
    - The single background drain thread owns every write and flush, at ~50ms
      cadence (DEBUG/INFO) or ~1ms while a WARN/ERROR requests prompt flush.
    - A log line costs roughly a microsecond and a handful of allocations on the
      caller (timestamp, colored line, queue push), independent of buffered lines. *)

(* OxCaml marks [Domain.DLS] and [Unix.putenv] as [unsafe_multidomain].
   - [Domain.DLS] caches the last section lookup per domain, storing a shared
     [section] record whose [min_level] may be mutated via [set_section_level].
     [Domain.Safe.DLS] requires a portable stored value, which this record is
     not; the record is only mutated during configuration changes.
   - [Unix.putenv] is called once at startup by [load_dotenv], before any domain
     is spawned. *)
[@@@alert "-unsafe_multidomain"]

type level =
  | DEBUG
  | INFO
  | WARN
  | ERROR
  | CRITICAL

let level_to_string = function
  | DEBUG -> "DEBUG"
  | INFO -> "INFO"
  | WARN -> "WARN"
  | ERROR -> "ERROR"
  | CRITICAL -> "CRITICAL"
;;

let level_of_string s =
  match String.lowercase_ascii s with
  | "debug" -> Some DEBUG
  | "info" -> Some INFO
  | "warn" -> Some WARN
  | "error" -> Some ERROR
  | "critical" -> Some CRITICAL
  | _ -> None
;;

let level_to_int = function
  | DEBUG -> 0
  | INFO -> 1
  | WARN -> 2
  | ERROR -> 3
  | CRITICAL -> 4
;;

(* ANSI escape sequences for terminal color output per log level. *)
let reset = "\027[0m"

let level_color = function
  | DEBUG -> "\027[2m\027[36m"
  | INFO -> "\027[32m"
  | WARN -> "\027[33m"
  | ERROR -> "\027[31m"
  | CRITICAL -> "\027[1m\027[41m\027[37m"
;;

(* Timestamp gray so the level and section columns stand out. *)
let timestamp_color = "\027[90m"
let ansi code = "\027[" ^ string_of_int code ^ "m"

(* Per-section identity colors for the most frequent sections, keeping the
   interleaved components mutually distinct. The oracle_* family shares
   bright-yellow so a block of oracle lines reads as one subsystem. Unlisted
   sections fall back to a stable hash into [section_color_palette]. *)
let section_color_overrides =
  let tbl = Hashtbl.create 16 in
  List.iter
    (fun (name, code) -> Hashtbl.replace tbl name code)
    [ "main", 94 (* bright blue *)
    ; "supervisor", 92 (* bright green *)
    ; "domain_spawner", 36 (* cyan *)
    ; "domain_supervisor", 95 (* bright magenta *)
    ; "oracle_runtime", 93 (* bright yellow *)
    ; "oracle_replay", 93
    ; "oracle_yahoo", 93
    ; "jacobs_ladder", 96 (* bright cyan *)
    ; "order_processor", 34 (* blue *)
    ; "dashboard_server", 35 (* magenta *)
    ; "discord_notifier", 97 (* bright white *)
    ; "market_maker", 90 (* gray *)
    ; "config", 90
    ; "memory", 90
    ];
  tbl
;;

(* Palette excludes the severity colors (31 red, 32 green, 33 yellow) so the
   level column keeps its meaning. *)
let section_color_palette = [| 34; 35; 36; 90; 92; 93; 94; 95; 96; 97 |]

let hash_string s =
  let h = ref 0 in
  String.iter (fun c -> h := ((!h * 31) + Char.code c) land 0x7fffffff) s;
  !h
;;

let section_color_code name =
  match Hashtbl.find_opt section_color_overrides name with
  | Some code -> code
  | None ->
    section_color_palette.(hash_string name mod Array.length section_color_palette)
;;

(* Per-section log level configuration. *)
type section =
  { name : string
  ; mutable min_level : level
  }

(* Global mutable configuration state. *)
let global_min_level = ref INFO
let sections = Hashtbl.create 32
let use_colors = ref true
let output_channel = ref stderr
let enabled_sections = ref []
let enabled_set : (string, unit) Hashtbl.t = Hashtbl.create 16
let quiet_mode = ref false

(* ---- Line width ----
   [configured_width] overrides all (None = auto). Auto mode detects the width
   from the output fd via [Notty_unix.winsize], cached ~1s so terminal resizes
   are picked up without an ioctl per line. Non-terminal output (pipes,
   `docker logs`, files) falls back to `COLUMNS` when set, else [default_width];
   wrapping still applies. *)

let default_width = 200
let configured_width : int option ref = ref None

let width_cache : (float * Unix.file_descr * int option) Atomic.t =
  Atomic.make (0.0, Unix.stdout, None)
;;

let width_ttl = 1.0

let detect_terminal_width () =
  try
    let fd = Unix.descr_of_out_channel !output_channel in
    match Notty_unix.winsize fd with
    | Some (w, _) when w > 0 -> Some w
    | _ -> None
  with
  | _ -> None
;;

(* Width hint for non-terminal output (e.g. `COLUMNS=200 app | less`); [None]
   when unset or malformed, so [default_width] applies. *)
let env_width () =
  match Sys.getenv_opt "COLUMNS" with
  | Some s ->
    (match int_of_string_opt s with
     | Some n when n > 0 -> Some n
     | _ -> None)
  | None -> None
;;

(* Always [Some] in practice: [configured_width], a detected terminal, COLUMNS,
   or [default_width]. Wrapping is always on. *)
let current_width () =
  match !configured_width with
  | Some w -> Some w
  | None ->
    let fd = Unix.descr_of_out_channel !output_channel in
    let now = Unix.gettimeofday () in
    let checked_at, cached_fd, cached = Atomic.get width_cache in
    if now -. checked_at < width_ttl && cached_fd = fd
    then cached
    else (
      let w =
        match detect_terminal_width () with
        | Some _ as w -> w
        | None ->
          (match env_width () with
           | Some _ as w -> w
           | None -> Some default_width)
      in
      Atomic.set width_cache (now, fd, w);
      w)
;;

let set_width width = configured_width := width

let log_callback : (level -> string -> string -> unit Lwt.t) ref =
  ref (fun _level _section _message -> Lwt.return_unit)
;;

let set_enabled_sections secs =
  enabled_sections := secs;
  Hashtbl.reset enabled_set;
  List.iter (fun s -> Hashtbl.replace enabled_set s ()) secs
;;

let set_quiet_mode quiet = quiet_mode := quiet
let set_log_callback callback = log_callback := callback

(** Serializes output_channel writes across domains to prevent interleaved log
    lines from concurrent workers. *)
let output_mutex = Mutex.create ()

(** Guards the [sections] registry only; separate from [output_mutex] so
    [get_section] on the trading hot path (via [will_log], on a Domain.DLS cache
    miss) never blocks behind the drain thread's [output_mutex], held across a
    potentially blocking [flush]. *)
let sections_mutex = Mutex.create ()

let get_section name =
  match Hashtbl.find_opt sections name with
  | Some s -> s
  | None ->
    Mutex.lock sections_mutex;
    let s =
      match Hashtbl.find_opt sections name with
      | Some s -> s
      | None ->
        let s = { name; min_level = !global_min_level } in
        Hashtbl.replace sections name s;
        s
    in
    Mutex.unlock sections_mutex;
    s
;;

let dummy_section = { name = ""; min_level = CRITICAL }
let tls_section_cache = Domain.DLS.new_key (fun () -> "", dummy_section)

(** [true] when [level] passes both the section and global minimum filters;
    guards allocation on disabled paths. Domain.DLS caches the last section
    lookup, eliminating Hashtbl overhead on the hot path. *)
let will_log level section_name =
  let last_name, section = Domain.DLS.get tls_section_cache in
  let sec =
    if section_name == last_name || String.equal section_name last_name
    then section
    else (
      let s = get_section section_name in
      Domain.DLS.set tls_section_cache (section_name, s);
      s)
  in
  (Hashtbl.length enabled_set = 0 || Hashtbl.mem enabled_set section_name)
  && level_to_int level >= level_to_int sec.min_level
  && level_to_int level >= level_to_int !global_min_level
;;

(* Format the current wall-clock time as "HH:MM:SS.mmm" (time only; the date is
   available from `docker logs --timestamps`). Caches the time prefix per second
   via Atomic for lock-free thread safety. *)
let timestamp_cache = Atomic.make (0.0, "")

let format_timestamp () =
  let time = Unix.gettimeofday () in
  let sec = floor time in
  let ms = int_of_float ((time -. sec) *. 1000.) in
  let last_sec, ts = Atomic.get timestamp_cache in
  let ts_prefix =
    if sec <> last_sec
    then (
      let tm = Unix.localtime time in
      let new_ts =
        Printf.sprintf "%02d:%02d:%02d" tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
      in
      ignore (Atomic.compare_and_set timestamp_cache (last_sec, ts) (sec, new_ts));
      new_ts)
    else ts
  in
  ts_prefix ^ Printf.sprintf ".%03d" ms
;;

(* ---- Column alignment ----
   Level column width 5; section column width 20 (fits every section that logs
   in practice, including dashboard_server, domain_supervisor, discord_notifier,
   hyperliquid_startup). Both fixed so the message column never shifts
   mid-stream: every message starts at the same column, which makes the log
   scannable. A section longer than 20 runs on. Multi-line messages render
   continuation lines under a dim ┆ gutter at the message column (see
   [render_message]). *)

let level_width = 5
let section_width = 20

let pad_to n s =
  let len = String.length s in
  if len >= n then s else s ^ String.make (n - len) ' '
;;

(* ---- Word wrapping ----
   Wrap [text] at word boundaries so no physical line exceeds [width] columns.
   Multi-space runs are collapsed; an over-long word is emitted on its own line
   rather than lost. *)
let wrap_text ~width text =
  let width = max 16 width in
  if String.length text <= width
  then text
  else (
    let words = text |> String.split_on_char ' ' |> List.filter (( <> ) "") in
    let buf = Buffer.create (String.length text + 32) in
    let line = Buffer.create 64 in
    let flush () =
      if Buffer.length line > 0
      then (
        Buffer.add_string buf (Buffer.contents line);
        Buffer.add_char buf '\n';
        Buffer.clear line)
    in
    List.iter
      (fun w ->
         let wlen = String.length w in
         if Buffer.length line = 0
         then Buffer.add_string line w
         else if Buffer.length line + 1 + wlen <= width
         then (
           Buffer.add_char line ' ';
           Buffer.add_string line w)
         else (
           flush ();
           Buffer.add_string line w))
      words;
    flush ();
    let s = Buffer.contents buf in
    String.sub s 0 (String.length s - 1))
;;

(* Gutter glyph for continuation lines of a multi-line message; rendered dim.
   Blank lines in a block are dropped. *)
let gutter_glyph = "┆"

(* Split a message into physical lines. The first is the header; the rest are
   detail/gutter lines with caller-embedded leading whitespace stripped. Lines
   are word-wrapped to the terminal width ([width] = [None] leaves them
   unwrapped); the header gets [width - prefix], continuation lines two columns
   less for the gutter. *)
let render_message ~prefix_len ~width message =
  let lines = String.split_on_char '\n' message in
  let head, rest =
    match lines with
    | [] -> "", []
    | h :: t -> h, t
  in
  let avail = Option.map (fun w -> max 16 (w - prefix_len - 1)) width in
  let avail_cont = Option.map (fun w -> max 16 (w - prefix_len - 3)) width in
  let wrap avail text =
    match avail with
    | None -> text
    | Some a -> wrap_text ~width:a text
  in
  let head_lines = String.split_on_char '\n' (wrap avail head) in
  let cont_lines =
    rest
    |> List.map String.trim
    |> List.filter (( <> ) "")
    |> List.concat_map (fun line -> String.split_on_char '\n' (wrap avail_cont line))
  in
  head_lines @ cont_lines
;;

(* Render one log line. Pure: no I/O or queue; callable directly. *)
let format_line level section_name message =
  let timestamp = format_timestamp () in
  let level_str = pad_to level_width (level_to_string level) in
  let section_str = pad_to section_width section_name in
  let prefix_len = String.length timestamp + 1 + level_width + 1 + section_width + 1 in
  let width = current_width () in
  let lines = render_message ~prefix_len ~width message in
  let head, cont =
    match lines with
    | [] -> "", []
    | h :: t -> h, t
  in
  let buf = Buffer.create (String.length message + 64) in
  if !use_colors
  then (
    Buffer.add_string buf timestamp_color;
    Buffer.add_string buf timestamp;
    Buffer.add_string buf reset;
    Buffer.add_char buf ' ';
    Buffer.add_string buf (level_color level);
    Buffer.add_string buf level_str;
    Buffer.add_string buf reset;
    Buffer.add_char buf ' ';
    Buffer.add_string buf (ansi (section_color_code section_name));
    Buffer.add_string buf section_str;
    Buffer.add_string buf reset;
    Buffer.add_char buf ' ')
  else (
    Buffer.add_string buf timestamp;
    Buffer.add_char buf ' ';
    Buffer.add_string buf level_str;
    Buffer.add_char buf ' ';
    Buffer.add_string buf section_str;
    Buffer.add_char buf ' ');
  Buffer.add_string buf head;
  List.iter
    (fun line ->
       Buffer.add_char buf '\n';
       Buffer.add_string buf (String.make prefix_len ' ');
       if !use_colors
       then (
         Buffer.add_string buf timestamp_color;
         Buffer.add_string buf gutter_glyph;
         Buffer.add_string buf reset)
       else Buffer.add_string buf gutter_glyph;
       Buffer.add_char buf ' ';
       Buffer.add_string buf line)
    cont;
  Buffer.contents buf
;;

(* ---- Async log drain (all levels except CRITICAL) ----
   Hot path: format the message and push onto async_queue under async_mutex.
   Cost ~50ns (mutex + Queue.push); zero I/O and no output_mutex contention.

   Background drain thread: writes all queued messages to output_channel,
   batching flushes. The thread owns every flush, so no caller does I/O. It
   idles at 50ms cadence, dropping to ~1ms while a WARN/ERROR requests prompt
   flush, so urgent lines appear within ~ms without blocking the caller.

   CRITICAL: drains the async queue first (preserving order), then writes
   synchronously with flush; the one emergency path allowed to block. *)

let async_queue : string Queue.t = Queue.create ()
let async_mutex = Mutex.create ()
let async_drain_started = Atomic.make false

(* Set when a WARN/ERROR line is queued and should be flushed promptly. Cleared
   by the drain thread; set by any domain. *)
let flush_requested = Atomic.make false

(** Push a pre-formatted log line onto the async queue. No I/O. *)
let[@inline always] log_async formatted =
  Mutex.lock async_mutex;
  Queue.push formatted async_queue;
  Mutex.unlock async_mutex
;;

(** Push and flag the drain thread for a prompt flush (~1ms cadence). *)
let[@inline always] log_async_urgent formatted =
  log_async formatted;
  Atomic.set flush_requested true
;;

(** Drain all pending async messages to output_channel. Caller MUST NOT hold
    [output_mutex]. *)
let drain_async_queue () =
  Mutex.lock async_mutex;
  if Queue.is_empty async_queue
  then Mutex.unlock async_mutex
  else (
    (* Transfer pending messages out of the async queue in O(1), minimizing
       async_mutex hold time so producers can push immediately after unlock. *)
    let batch = Queue.create () in
    Queue.transfer async_queue batch;
    Mutex.unlock async_mutex;
    (* Write the batch under output_mutex with one flush per batch: a flush per
       line cost N write plus N flush syscalls per burst. Batch-flush keeps lines
       equally prompt (the whole batch lands this iteration) at one write call
       each and a single flush. *)
    Mutex.lock output_mutex;
    try
      Queue.iter
        (fun msg ->
           output_string !output_channel msg;
           output_char !output_channel '\n')
        batch;
      if not (Queue.is_empty batch) then flush !output_channel;
      Mutex.unlock output_mutex
    with
    | exn ->
      Mutex.unlock output_mutex;
      ignore exn)
;;

(** Start the background drain thread; called once, idempotent. Uses
    [Thread.create] (not [Domain.spawn]) to avoid consuming a core. The thread
    owns all output flushing, so no caller performs I/O. While [flush_requested]
    is set it polls at ~1ms so WARN/ERROR lines surface promptly; else 50ms. *)
let start_async_drain () =
  if Atomic.compare_and_set async_drain_started false true
  then (
    let _drain_thread =
      Thread.create
        (fun () ->
           while true do
             drain_async_queue ();
             if Atomic.get flush_requested
             then (
               Atomic.set flush_requested false;
               (* ~1ms cadence while an urgent line is pending. *)
               Thread.delay 0.001)
             else Thread.delay 0.05
           done)
        ()
    in
    ())
;;

(* Core logging function. Domain-safe. All levels except CRITICAL are pushed to
   the async queue with no synchronous I/O and no caller-side draining. CRITICAL
   drains the queue and writes + flushes synchronously for immediate visibility. *)
let log_sync level section_name message =
  let section = get_section section_name in
  if
    (Hashtbl.length enabled_set <> 0 && not (Hashtbl.mem enabled_set section_name))
    || level_to_int level < level_to_int section.min_level
    || level_to_int level < level_to_int !global_min_level
  then ()
  else if !quiet_mode
  then ()
  else (
    let formatted = format_line level section_name message in
    if level = CRITICAL
    then (
      (* Emergency path: drain the async queue first to preserve ordering, then
         write with immediate flush. *)
      drain_async_queue ();
      Mutex.lock output_mutex;
      try
        output_string !output_channel formatted;
        output_char !output_channel '\n';
        flush !output_channel;
        Mutex.unlock output_mutex
      with
      | exn ->
        Mutex.unlock output_mutex;
        ignore exn)
    else (
      (* Async path for every other level (including WARN/ERROR): buffer the
         formatted line. The drain thread flushes within ~50ms, or ~1ms for
         WARN/ERROR via flush_requested. *)
      start_async_drain ();
      if level_to_int level >= level_to_int WARN
      then log_async_urgent formatted
      else log_async formatted))
;;

(* Lwt wrapper: delegates to [log_sync] then returns [Lwt.return_unit]. *)
let log level section_name message =
  log_sync level section_name message;
  Lwt.return_unit
;;

(* Format-string log API. Zero-allocation when disabled: [Printf.ifprintf]
   consumes format arguments without allocating a string, and [Printf.ksprintf]
   allocates only when the message is emitted. *)
let debug_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log DEBUG section
  then Printf.ksprintf (fun msg -> log_sync DEBUG section msg) fmt
  else Printf.ifprintf () fmt
;;

let info_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log INFO section
  then Printf.ksprintf (fun msg -> log_sync INFO section msg) fmt
  else Printf.ifprintf () fmt
;;

let warn_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log WARN section
  then Printf.ksprintf (fun msg -> log_sync WARN section msg) fmt
  else Printf.ifprintf () fmt
;;

let error_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log ERROR section
  then Printf.ksprintf (fun msg -> log_sync ERROR section msg) fmt
  else Printf.ifprintf () fmt
;;

let critical_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log CRITICAL section
  then Printf.ksprintf (fun msg -> log_sync CRITICAL section msg) fmt
  else Printf.ifprintf () fmt
;;

(** Deferred (non-blocking) sibling of [critical_f].

    Formats at CRITICAL but enqueues on the async drain queue with a
    prompt-flush request, as WARN/ERROR do, instead of draining and flushing
    synchronously under [output_mutex]. Use from latency-sensitive paths (e.g. an
    order-fill handler inside a domain's execution cycle): a full or slow output
    pipe would otherwise block the caller in [flush] for the duration of the
    backpressure. The line still reaches the output within ~1ms. *)
let critical_async_f ~section (fmt : ('a, unit, string, unit) format4) =
  if will_log CRITICAL section && not !quiet_mode
  then
    Printf.ksprintf
      (fun msg ->
         let formatted = format_line CRITICAL section msg in
         start_async_drain ();
         log_async_urgent formatted)
      fmt
  else Printf.ifprintf () fmt
;;

let debug ~section msg = if will_log DEBUG section then log_sync DEBUG section msg
let info ~section msg = if will_log INFO section then log_sync INFO section msg
let warn ~section msg = if will_log WARN section then log_sync WARN section msg
let error ~section msg = if will_log ERROR section then log_sync ERROR section msg

let critical ~section msg =
  if will_log CRITICAL section then log_sync CRITICAL section msg
;;

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

(** Minimal [.env] loader. When [path] exists, every [KEY=VALUE] line is exported
    into the process environment. Blank lines and [#] lines are skipped, a leading
    [export ] is ignored, and matching single/double quotes around a value are
    stripped. Existing variables are never overwritten. A missing file is a
    no-op. *)
let load_dotenv ?(path = ".env") () =
  match
    try Some (open_in path) with
    | Sys_error _ -> None
  with
  | None -> ()
  | Some ic ->
    Fun.protect
      ~finally:(fun () -> close_in_noerr ic)
      (fun () ->
         try
           while true do
             let line = String.trim (input_line ic) in
             if line <> "" && line.[0] <> '#'
             then (
               let line =
                 if String.starts_with ~prefix:"export " line
                 then String.trim (String.sub line 7 (String.length line - 7))
                 else line
               in
               match String.index_opt line '=' with
               | None -> ()
               | Some i ->
                 let key = String.trim (String.sub line 0 i) in
                 let value =
                   String.trim (String.sub line (i + 1) (String.length line - i - 1))
                 in
                 let value =
                   let n = String.length value in
                   if
                     n >= 2
                     && ((value.[0] = '"' && value.[n - 1] = '"')
                         || (value.[0] = '\'' && value.[n - 1] = '\''))
                   then String.sub value 1 (n - 2)
                   else value
                 in
                 if key <> "" && Sys.getenv_opt key = None then Unix.putenv key value)
           done
         with
         | End_of_file -> ())
;;
