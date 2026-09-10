(** Main-loop watchdog.

    The engine's responsiveness lives and dies with the main domain's Lwt
    event loop: every supervised WebSocket read loop, the order-processing
    loop, the supervisor health monitor, the dashboard UDS server and the
    memory reporter all run there, and every per-asset domain parks on an
    [Exchange_wakeup] condition that only the main loop signals. A main
    loop that wedges inside an unbounded blocking operation therefore
    freezes the entire process - orders, prices, dashboard reconnect -
    silently, because the health monitor itself lives on the frozen loop.

    The watchdog is a native thread reading a beat timestamp that a
    main-loop fiber refreshes every [beat_interval_s]. When no beat
    arrives within [stall_threshold_s], the loop is presumed wedged:

    1. A CRITICAL report is logged (the async writer thread still runs).
    2. SIGABRT is raised to the process itself: the installed
       fatal-signal handler prints heap diagnostics and a backtrace IF the
       wedge is at the OCaml level. A wedge inside a blocking syscall
       defers the handler on OCaml 5, so this is best-effort.
    3. After [abort_grace_s] the process is force-exited (exit 2) so the
       process supervisor restarts a live engine instead of a frozen one
       holding resting orders it can no longer amend or cancel.

    The threshold is deliberately generous (twelve missed beats): a healthy
    loop beats every [beat_interval_s] with no observable jitter, major GC
    pauses are sub-second, and several sequential bounded TLS operations
    (each now capped at ~30s) still stay under it. The point is to convert
    an indefinite silent freeze into a bounded, visible, restartable one.

    Set [DIO_WATCHDOG_OFF] to disable the thread entirely (e.g. when
    attaching a debugger to a suspect process for repeated inspection). *)

let section = "watchdog"

(** Main-loop beat cadence and the staleness threshold that triggers the
    stall response. *)
let beat_interval_s = 5.0

let stall_threshold_s = 60.0

(** Grace period between raising SIGABRT and the force-exit, giving the
    fatal-signal handler time to flush its diagnostics. *)
let abort_grace_s = 3.0

(** Last main-loop beat, as a Unix timestamp. Only the beat fiber writes;
    the watchdog thread only reads. Seeded at [start] so the counter starts
    from process launch. *)
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

(** The main-loop side: refresh the beat every [beat_interval_s]. Must run
    on the main domain's Lwt scheduler (schedule with [Lwt.async]); if that
    scheduler wedges, beats stop - which is exactly the signal the
    watchdog thread waits for. The nested [Lwt_main.run] calls during
    engine startup pump this fiber too, so startup cannot false-trigger. *)
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
      (* Best effort: the fatal-signal handler prints diagnostics only if
         the wedge is at the OCaml level; a wedge inside a blocking syscall
         defers the handler. The force-exit below is the guaranteed action. *)
      (try Unix.kill (Unix.getpid ()) Sys.sigabrt with
       | _ -> ());
      Thread.delay abort_grace_s;
      Logging.critical_f
        ~section
        "Main loop still unresponsive after SIGABRT - watchdog force-exiting (exit 2)";
      exit 2)
  done
;;

(** Idempotent: seeds the beat and spawns the watchdog thread unless
    disabled via [DIO_WATCHDOG_OFF]. *)
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
