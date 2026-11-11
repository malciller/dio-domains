(** Off-domain latency window publication.

    Why this exists: {!Latency_profiler.snapshot_and_reset} folds the rolling window
    union, scans every histogram tier for percentiles, zeroes the arrays, and formats the
    spike log line. That is O(buckets) work per profiler — hundreds of microseconds per
    domain per window — and it used to run on each trading domain, inside the very loop
    whose latency it measures.

    Instead the trading domain only calls {!Latency_profiler.rotate}, which closes the
    live window and hands it over in constant time. One detached diagnostic domain
    ({!start}) drains those windows, does the fold and percentile work, publishes the
    snapshot the dashboard and journal already read, and emits the spike log line.

    Handoff contract:

    - Rotation happens on the recording domain, so no record can be in flight against a
      window that was just closed.
    - A closed window is handed over by reference through an atomic cell; the publisher is
      the only reader. It recycles the buffers once published, and rotation skips a window
      while the previous one is still waiting, so a stalled publisher widens a window
      rather than dropping samples.
    - If no publisher domain is running (tests, embedders), {!rotate} publishes
      synchronously instead, preserving the old behavior: never silent, just slower.
    - Every failure is contained: a publish error is logged and the next poll retries. *)

(* OxCaml marks [Domain.spawn] as [do_not_spawn_domains]. This module runs at most one
   detached diagnostic domain and never a trading domain. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let section = "latency_publisher"

(** One domain's publication group: the stages that make up its spike log line, plus the
    overlay/controller/order profilers that share its cadence but are not logged. *)
type group =
  { key : string
  ; window_seconds : float
  ; threshold_us : float
  ; report_seconds : float
  ; report : bool
  ; stages : (string * Latency_profiler.t) list
  ; silent : Latency_profiler.t list Atomic.t
  ; mutable last_report : float
  }

(** True while the publisher domain is alive. *)
let running = Atomic.make false

let groups : (string, group) Hashtbl.t = Hashtbl.create 16
let groups_mutex = Mutex.create ()

(** Idempotent by key: a restarted domain replaces its predecessor's group. *)
let register g =
  g.last_report <- Unix.gettimeofday ();
  Mutex.lock groups_mutex;
  Hashtbl.replace groups g.key g;
  Mutex.unlock groups_mutex;
  g
;;

(** Windows published on this group's cadence but omitted from its spike line. *)
let add_silent g profs = Atomic.set g.silent (Atomic.get g.silent @ profs)

(** At most one INFO line per [report_seconds]. *)
let maybe_log g snapshots =
  if g.report && snapshots <> []
  then (
    let now = Unix.gettimeofday () in
    if now -. g.last_report >= g.report_seconds
    then (
      g.last_report <- now;
      match
        Latency_profiler.spike_message
          ~key:g.key
          ~window_seconds:g.window_seconds
          ~threshold_us:g.threshold_us
          snapshots
      with
      | None -> ()
      | Some msg -> Logging.info_f ~section "%s" msg))
;;

(** Publish the windows {!rotate} closed. *)
let publish_group g =
  let snapshots =
    List.filter_map
      (fun (label, p) ->
        match Latency_profiler.drain ~spike_threshold_us:g.threshold_us p with
        | Some snap -> Some (label, snap)
        | None -> None)
      g.stages
  in
  List.iter (fun p -> ignore (Latency_profiler.drain p)) (Atomic.get g.silent);
  maybe_log g snapshots
;;

(** Fallback when no publisher domain is running: publish on the caller's domain. *)
let publish_now g =
  let snapshots =
    List.map
      (fun (label, p) ->
        label, Latency_profiler.snapshot_and_reset ~spike_threshold_us:g.threshold_us p)
      g.stages
  in
  List.iter
    (fun p -> ignore (Latency_profiler.snapshot_and_reset p))
    (Atomic.get g.silent);
  maybe_log g snapshots
;;

(** Close this group's windows. O(1) per profiler on the recording domain when the
    publisher is running; otherwise publishes synchronously. *)
let rotate g =
  if Atomic.get running
  then (
    List.iter (fun (_, p) -> Latency_profiler.rotate p) g.stages;
    List.iter Latency_profiler.rotate (Atomic.get g.silent))
  else publish_now g
;;

(** Drain every registered group once. *)
let publish_all () =
  let gs =
    Mutex.lock groups_mutex;
    let l = Hashtbl.to_seq_values groups |> List.of_seq in
    Mutex.unlock groups_mutex;
    l
  in
  List.iter
    (fun g ->
      try publish_group g with
      | exn ->
        Logging.warn_f ~section "publish for %s failed: %s" g.key (Printexc.to_string exn))
    gs
;;

(** Well below the per-domain window, so a closed window is visible within a second. *)
let publish_interval_seconds = 1.0

let run () =
  try
    Gc_config.apply ();
    Thread_affinity.pin_self_background ();
    Thread_affinity.set_self_idle ();
    let rec loop () =
      (try publish_all () with
       | _ -> ());
      Unix.sleepf publish_interval_seconds;
      loop ()
    in
    loop ()
  with
  | exn ->
    (* Leaving [running] set would turn every rotation into a handoff nobody drains. *)
    Atomic.set running false;
    Logging.warn_f
      ~section
      "publisher stopped (%s); publishing inline"
      (Printexc.to_string exn)
;;

(** [start ()] spawns the publisher domain. Detached, pinned to background affinity at
    idle priority, like the canary and the journal: it competes with no trading core. *)
let start () =
  if not (Atomic.get running)
  then (
    Atomic.set running true;
    match Domain.spawn run with
    | _ ->
      Logging.info_f
        ~section
        "started: draining trading-domain windows every %.0fs"
        publish_interval_seconds
    | exception exn ->
      (* Leave [running] false so [rotate] publishes synchronously: slower, but a trading
         domain never records into a window nobody drains. *)
      Atomic.set running false;
      Logging.warn_f
        ~section
        "cannot spawn publisher (%s); publishing inline"
        (Printexc.to_string exn))
;;
