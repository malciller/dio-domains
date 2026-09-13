(** Main-loop watchdog.

    The main domain's Lwt event loop runs every supervised WebSocket read loop,
    the order-processing loop, the supervisor health monitor, the dashboard UDS
    server and the memory reporter, and is the only signaler of the
    [Exchange_wakeup] conditions per-asset domains park on. A main loop wedged
    in an unbounded blocking operation therefore freezes the whole process, and
    the health monitor with it.

    A native thread reads a beat timestamp refreshed every [beat_interval_s] by a
    main-loop fiber. If no beat arrives within [stall_threshold_s]:

    1. Log CRITICAL (the async writer thread still runs).
    2. Raise SIGABRT: the fatal-signal handler prints heap diagnostics and a
       backtrace only if the wedge is at the OCaml level; a wedge inside a
       blocking syscall defers the handler on OCaml 5. Best-effort.
    3. After [abort_grace_s], force-exit (status 2) so the process supervisor
       restarts a live engine rather than one holding unamendable orders.

    The threshold is generous (twelve missed beats): major GC pauses are
    sub-second and several sequential bounded TLS operations stay under it.
    [DIO_WATCHDOG_OFF] disables the thread (e.g. for debugger attachment). *)

let section = "watchdog"

(** Beat cadence and staleness threshold (both seconds). *)
let beat_interval_s = 5.0

let stall_threshold_s = 60.0

(** Seconds between SIGABRT and force-exit, allowing the fatal-signal handler
    to flush diagnostics. *)
let abort_grace_s = 3.0

(** Last main-loop beat as a Unix timestamp. Written only by the beat fiber,
    read only by the watchdog thread. Seeded at [start]. *)
let last_beat = Atomic.make 0.0

(** Latched once per stall so a wedged loop produces exactly one abort. *)
let triggered = Atomic.make false

(** Latched at [start] so repeated calls are no-ops. *)
let started = Atomic.make false

(** Refreshed by the main loop's beat fiber. *)
let beat () = Atomic.set last_beat (Unix.gettimeofday ())

(** Pure stall predicate: [true] when the last beat is older than the
    threshold. Exposed for tests. *)
let is_stalled ~(last_beat : float) ~(now : float) : bool =
  now -. last_beat > stall_threshold_s
;;

(** Main-loop side: refresh the beat every [beat_interval_s]. MUST run on the
    main domain's Lwt scheduler ([Lwt.async]); if it wedges, beats stop, which
    is the signal the watchdog waits for. Nested [Lwt_main.run] calls during
    startup pump this fiber, so startup cannot false-trigger. *)
let beat_loop () : unit Lwt.t =
  let open Lwt.Infix in
  let rec loop () =
    beat ();
    Lwt_unix.sleep beat_interval_s
    >>= fun () ->
    Lwt.async loop;
    Lwt.return_unit
  in
  Lwt.async loop;
  Lwt.return_unit
;;

(** Watchdog thread body: poll the beat timestamp every [beat_interval_s]
    and respond to a stall. Never returns. *)
let watchdog_loop () =
  while true do
    Thread.delay beat_interval_s;
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_beat in
    if is_stalled ~last_beat:last ~now && Atomic.compare_and_set triggered false true
    then (
      Logging.critical_f
        ~section
        "MAIN LOOP STALLED: no beat for %.0fs (threshold %.0fs). The engine cannot \
         process orders, prices or dashboard clients. Raising SIGABRT for diagnostics, \
         then force-exiting for supervised restart."
        (now -. last)
        stall_threshold_s;
      Thread.delay 0.5;
      (* Best effort: the fatal-signal handler prints diagnostics only for an
         OCaml-level wedge; a blocking syscall defers it. The force-exit below
         is the guaranteed action. *)
      (try Unix.kill (Unix.getpid ()) Sys.sigabrt with
       | _ -> ());
      Thread.delay abort_grace_s;
      Logging.critical_f
        ~section
        "Main loop still unresponsive after SIGABRT - watchdog force-exiting (exit 2)";
      exit 2)
  done
;;

(** Idempotent: seed the beat and spawn the watchdog thread unless
    [DIO_WATCHDOG_OFF] is set. *)
let start () : unit =
  if Atomic.compare_and_set started false true
  then (
    beat ();
    match Sys.getenv_opt "DIO_WATCHDOG_OFF" with
    | Some _ ->
      Logging.warn_f
        ~section
        "Watchdog DISABLED via DIO_WATCHDOG_OFF - a stalled main loop will hang silently"
    | None ->
      ignore (Thread.create watchdog_loop ());
      Logging.debug_f
        ~section
        "Watchdog started (beat %.0fs, stall threshold %.0fs)"
        beat_interval_s
        stall_threshold_s)
  else ()
;;
