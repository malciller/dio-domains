(** Latency journal: an append-only JSONL record of every published per-stage latency
    window, with bounded retention.

    Why this exists: {!Latency_profiler} keeps only its most recent published window, so a
    percentile regression is visible in the live dashboard and then gone. The journal
    retains those windows so a regression can be located after the fact and compared
    against an earlier run (`dio latency compare`).

    Operating contract — the journal must never cost a trade:

    - It runs on one detached diagnostic domain, pinned to background affinity and set to
      the idle scheduling policy, exactly like {!Canary}. It competes with no trading
      core.
    - It only reads state the trading domains already publish: lock-free {!Atomic.get} on
      profiler snapshots, plus the short profiler-table mutex that domain startup also
      takes. It never records into a profiler and never touches the cycle path.
    - Every write is wrapped. A failed write, a missing directory, or low disk space
      increments a dropped counter and leaves the engine running. Telemetry yields to
      capital state: the shared data directory also holds position state, so the journal
      stops before free space gets tight.
    - A truncated final line (SIGKILL, `docker rm -f`) is tolerated by the reader, which
      is the point of line-oriented JSON.

    File lifecycle: one file per engine start, [latency-<YYYYMMDD-HHMMSS>-<pid>.jsonl]. A
    nightly restart is therefore the natural rotation boundary, and two engines sharing a
    host cannot interleave. Retention deletes whole files, oldest first; it never rewrites
    a live one. *)

(* OxCaml marks [Domain.spawn] as [do_not_spawn_domains]. The journal runs at most one
   detached domain for background telemetry, never a trading domain. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let section = "latency_journal"

(** Default journal directory: [<data_dir>/latency]. The config module resolves the same
    path ({!Dio_engine.Config.latency_journal_config}) and passes it explicitly, so this
    is only the fallback for tooling and tests. *)
let default_dir () = Ds.Paths.subdir "latency"

(** One journalled window: the stage it belongs to, the row it decorates, and the profiler
    snapshot itself. *)
type row =
  { domain : string
  ; stage : string
  ; snap : Latency_profiler.snapshot
  }

(** Runtime knobs, resolved by the caller from {!Dio_engine.Config.latency_journal_config}
    so this library stays free of a config dependency. *)
type options =
  { enabled : bool
  ; interval_s : float
  ; dir : string
  ; retain_days : int
  ; max_total_mb : int
  ; min_free_mb : int
  }

let rows_written = Atomic.make 0
let rows_dropped = Atomic.make 0
let bytes_written = Atomic.make 0
let running = Atomic.make false
let stop_requested = Atomic.make false
let file_path = Atomic.make ""
let last_error = Atomic.make ""

(** Build identity from [DIO_GIT_SHA] when the deploy sets it. Optional: without it rows
    carry no attribution and reports print "-". *)
let git_sha = Atomic.make ""

let is_running () = Atomic.get running
let rows_written_count () = Atomic.get rows_written
let rows_dropped_count () = Atomic.get rows_dropped
let bytes_written_count () = Atomic.get bytes_written
let current_file () = Atomic.get file_path
let last_error_message () = Atomic.get last_error

(** mkdir -p: create [dir] and any missing parents; an existing component is fine
    (idempotent, tolerates a racing writer). *)
let mkdir_p (dir : string) =
  let rec create path =
    if not (Sys.file_exists path)
    then (
      create (Filename.dirname path);
      try Unix.mkdir path 0o755 with
      | Unix.Unix_error (Unix.EEXIST, _, _) -> ())
  in
  create dir
;;

(** Free bytes available on the filesystem holding [dir]. A failed or unsupported query
    reports "plenty", so the journal keeps writing instead of silently stopping. *)
let free_bytes (dir : string) =
  match Disk_space.free_bytes dir with
  | -1 -> Int64.max_int
  | n -> Int64.of_int n
;;

let journal_prefix = "latency-"
let journal_suffix = ".jsonl"

(** Journal files in [dir], oldest first (names carry a sortable timestamp). *)
let journal_files (dir : string) =
  try
    Sys.readdir dir
    |> Array.to_list
    |> List.filter (fun f ->
      String.length f > String.length journal_prefix
      && String.sub f 0 (String.length journal_prefix) = journal_prefix
      && Filename.check_suffix f journal_suffix)
    |> List.map (Filename.concat dir)
    |> List.sort String.compare
  with
  | _ -> []
;;

(** Delete whole files, oldest first, until the directory is within [retain_days] and
    [max_total_bytes]. [current] is never deleted. A day-based limit alone cannot bound
    size when the asset count grows, so both limits apply and whichever bites first wins. *)
let prune
  ~(dir : string)
  ~(retain_days : int)
  ~(max_total_bytes : int)
  ~(current : string option)
  =
  let now = Unix.gettimeofday () in
  let max_age = float_of_int retain_days *. 86_400.0 in
  let stat p =
    try Some (Unix.stat p) with
    | _ -> None
  in
  let total =
    List.fold_left
      (fun acc p ->
        match stat p with
        | Some st -> acc + st.Unix.st_size
        | None -> acc)
      0
      (journal_files dir)
  in
  let rec go total = function
    | [] -> ()
    | p :: rest ->
      let too_old =
        match stat p with
        | Some st -> now -. st.Unix.st_mtime > max_age
        | None -> false
      in
      if (not too_old) && total <= max_total_bytes
      then ()
      else (
        let size =
          match stat p with
          | Some st -> st.Unix.st_size
          | None -> 0
        in
        let is_current =
          match current with
          | Some c -> String.equal p c
          | None -> false
        in
        if is_current
        then go total rest
        else (
          (try Unix.unlink p with
           | _ -> ());
          go (total - size) rest))
  in
  go total (journal_files dir)
;;

let timestamp_slug () =
  let tm = Unix.localtime (Unix.gettimeofday ()) in
  Printf.sprintf
    "%04d%02d%02d-%02d%02d%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
    tm.Unix.tm_hour
    tm.Unix.tm_min
    tm.Unix.tm_sec
;;

(** One JSONL record. Deliberately narrow: p90/p95 are dropped because a reader comparing
    runs needs p50/p99/p999 and the max, and bytes are the binding constraint on the prod
    data directory. [w] is the profiler window end, used to ignore re-sampled windows.

    [mx] is the maximum of the profiler's retained window union, so one spike stays
    visible for every window in the union and would otherwise read as a fresh spike per
    row. [mt] is the end (Unix ms) of the window that holds it: rows sharing an [mt] are
    the same event, so readers dedupe on it and never mistake a carried-forward maximum
    for a new one. *)
let json_of_row ~(now : float) ~(sha : string) (r : row) =
  let s = r.snap in
  let fields =
    [ "t", `Float now
    ; "d", `String r.domain
    ; "s", `String r.stage
    ; ( "n"
      , `Int
          (if s.Latency_profiler.union_samples > 0
           then s.Latency_profiler.union_samples
           else s.Latency_profiler.samples) )
    ; "p50", `Float s.Latency_profiler.p50
    ; "p99", `Float s.Latency_profiler.p99
    ; "p999", `Float s.Latency_profiler.p999
    ; "mx", `Float s.Latency_profiler.max_us
    ; ( "mt"
      , if s.Latency_profiler.max_time > 0.0
        then `Int (int_of_float (s.Latency_profiler.max_time *. 1000.0))
        else `Null )
    ; "ov", `Int s.Latency_profiler.overflow
    ; "w", `Float s.Latency_profiler.window_end
    ; ( "c"
      , match s.Latency_profiler.max_cause with
        | Some c -> `String c
        | None -> `Null )
    ]
  in
  `Assoc (if String.equal sha "" then fields else ("g", `String sha) :: fields)
;;

type t =
  { opts : options
  ; mutable produce : unit -> row list
  ; seen : (string, float) Hashtbl.t
  (** [domain \\000 stage -> last window_end], so a re-sampled window is written once. *)
  ; mutable fd : Unix.file_descr option
  ; mutable path : string
  ; mutable ticks : int
  ; mutable space_warned : bool
  }

let dedupe_key (r : row) = r.domain ^ "\000" ^ r.stage

let open_file (t : t) : Unix.file_descr option =
  match t.fd with
  | Some fd -> Some fd
  | None ->
    (try
       mkdir_p t.opts.dir;
       let path =
         Filename.concat
           t.opts.dir
           (Printf.sprintf "latency-%s-%d.jsonl" (timestamp_slug ()) (Unix.getpid ()))
       in
       let fd =
         Unix.openfile
           path
           [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_APPEND; Unix.O_CLOEXEC ]
           0o644
       in
       t.path <- path;
       t.fd <- Some fd;
       Atomic.set file_path path;
       Logging.info_f ~section "writing to %s" path;
       Some fd
     with
     | exn ->
       Atomic.set last_error (Printexc.to_string exn);
       None)
;;

let write_all fd (s : string) =
  let len = String.length s in
  let rec go off =
    if off < len
    then (
      let n = Unix.write_substring fd s off (len - off) in
      if n > 0 then go (off + n))
  in
  go 0
;;

(** One sampling tick: read the producer, keep rows whose window is new, and append them.
    Never raises. *)
let tick (t : t) =
  let rows =
    try t.produce () with
    | _ -> []
  in
  let now = Unix.gettimeofday () in
  let fresh =
    List.filter
      (fun (r : row) ->
        if r.snap.Latency_profiler.samples <= 0
        then false
        else (
          let key = dedupe_key r in
          let w = r.snap.Latency_profiler.window_end in
          match Hashtbl.find_opt t.seen key with
          | Some prev when Float.equal prev w -> false
          | _ ->
            Hashtbl.replace t.seen key w;
            true))
      rows
  in
  if fresh <> []
  then (
    let floor_bytes = Int64.mul (Int64.of_int t.opts.min_free_mb) 1_048_576L in
    if Int64.compare (free_bytes t.opts.dir) floor_bytes < 0
    then (
      if not t.space_warned
      then (
        t.space_warned <- true;
        Logging.warn_f
          ~section
          "below %d MB free in %s; pausing the latency journal (position state has \
           priority)"
          t.opts.min_free_mb
          t.opts.dir))
    else (
      t.space_warned <- false;
      match open_file t with
      | None -> ignore (Atomic.fetch_and_add rows_dropped (List.length fresh))
      | Some fd ->
        let sha = Atomic.get git_sha in
        let buf = Buffer.create 4096 in
        List.iter
          (fun r ->
            Buffer.add_string buf (Yojson.Basic.to_string (json_of_row ~now ~sha r));
            Buffer.add_char buf '\n')
          fresh;
        let payload = Buffer.contents buf in
        (try
           write_all fd payload;
           ignore (Atomic.fetch_and_add bytes_written (String.length payload));
           ignore (Atomic.fetch_and_add rows_written (List.length fresh))
         with
         | exn ->
           Atomic.set last_error (Printexc.to_string exn);
           ignore (Atomic.fetch_and_add rows_dropped (List.length fresh))));
    t.ticks <- t.ticks + 1;
    if t.ticks mod 100 = 0
    then (
      prune
        ~dir:t.opts.dir
        ~retain_days:t.opts.retain_days
        ~max_total_bytes:(t.opts.max_total_mb * 1_048_576)
        ~current:(Some t.path);
      Logging.debug_f
        ~section
        "%d rows written, %d dropped, %d bytes%s"
        (Atomic.get rows_written)
        (Atomic.get rows_dropped)
        (Atomic.get bytes_written)
        (match Atomic.get last_error with
         | "" -> ""
         | e -> ", last error: " ^ e)))
;;

let loop (t : t) =
  let rec go () =
    if Atomic.get stop_requested
    then ()
    else (
      (try tick t with
       | _ -> ());
      try
        Unix.sleepf (Float.max 1.0 t.opts.interval_s);
        go ()
      with
      | _ -> ())
  in
  go ()
;;

(** [start opts ~produce] begins journalling unless [opts.enabled] is false.

    Startup is fail-soft by design: a directory that cannot be created, or a filesystem
    already below [min_free_mb], logs one warning and leaves the journal off. It never
    aborts or delays engine startup, because the journal is the least important thing the
    process does. *)
let start (opts : options) ~(produce : unit -> row list) =
  if not opts.enabled
  then Logging.info_f ~section "disabled"
  else (
    let usable =
      try
        mkdir_p opts.dir;
        true
      with
      | exn ->
        Logging.warn_f
          ~section
          "cannot create %s (%s); latency journal off"
          opts.dir
          (Printexc.to_string exn);
        false
    in
    if usable
    then (
      Atomic.set stop_requested false;
      Atomic.set running true;
      prune
        ~dir:opts.dir
        ~retain_days:opts.retain_days
        ~max_total_bytes:(opts.max_total_mb * 1_048_576)
        ~current:None;
      (match Sys.getenv_opt "DIO_GIT_SHA" with
       | Some sha when String.trim sha <> "" -> Atomic.set git_sha (String.trim sha)
       | _ -> ());
      Logging.info_f
        ~section
        "started: %s every %.0fs (retain %d days or %d MB, stop below %d MB free)"
        opts.dir
        opts.interval_s
        opts.retain_days
        opts.max_total_mb
        opts.min_free_mb;
      ignore
        (Domain.spawn (fun () ->
           Gc_config.apply ();
           Thread_affinity.pin_self_background ();
           Thread_affinity.set_self_idle ();
           let t =
             { opts
             ; produce
             ; seen = Hashtbl.create 64
             ; fd = None
             ; path = ""
             ; ticks = 0
             ; space_warned = false
             }
           in
           loop t))))
;;

(** [stop ()] asks the loop to exit after its current tick. Tests only; the production
    process exits with the file open, and every completed tick is already written. *)
let stop () =
  Atomic.set stop_requested true;
  Atomic.set running false
;;
