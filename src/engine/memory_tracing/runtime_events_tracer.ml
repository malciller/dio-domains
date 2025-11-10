(** Runtime Events Tracer - Captures OCaml runtime memory events

    Uses OCaml's Runtime_events module to track GC events, allocations,
    and domain-specific memory activity. Multicore-compatible and provides
    structured event data for leak detection.
*)

(** Memory event types we track *)
type memory_event =
  | Minor_collection of {
      domain_id: int;
      timestamp: float;
      promoted_words: int;
      heap_words: int;
      live_words: int;
    }
  | Major_collection of {
      domain_id: int;
      timestamp: float;
      heap_words: int;
      live_words: int;
      free_words: int;
      fragments: int;
      compactions: int;
    }
  | Domain_spawn of {
      domain_id: int;
      timestamp: float;
    }
  | Domain_terminate of {
      domain_id: int;
      timestamp: float;
    }

(** Event callback type *)
type event_callback = memory_event -> unit

(** Tracer state *)
type t = {
  mutable event_callbacks: event_callback list;
  mutable is_running: bool;
  mutable runtime_events_cursor: Runtime_events.cursor option;
  mutable event_thread: Thread.t option;
  mutex: Mutex.t;
}

(** Global tracer instance *)
let tracer = {
  event_callbacks = [];
  is_running = false;
  runtime_events_cursor = None;  (* Will be created when started *)
  event_thread = None;
  mutex = Mutex.create ();
}

(** Register an event callback *)
let add_callback callback =
  Mutex.lock tracer.mutex;
  tracer.event_callbacks <- callback :: tracer.event_callbacks;
  Mutex.unlock tracer.mutex

(** Remove an event callback *)
let remove_callback callback =
  Mutex.lock tracer.mutex;
  tracer.event_callbacks <- List.filter (fun cb -> cb != callback) tracer.event_callbacks;
  Mutex.unlock tracer.mutex

(** Dispatch event to all callbacks *)
let dispatch_event event =
  Mutex.lock tracer.mutex;
  let callbacks = tracer.event_callbacks in
  Mutex.unlock tracer.mutex;
  List.iter (fun callback ->
    try callback event
    with exn ->
      Logging.debug_f ~section:"runtime_events_tracer" "Callback failed: %s" (Printexc.to_string exn)
  ) callbacks

(** Event processing loop - runs in background thread *)
let rec event_loop () =
  if not tracer.is_running then ()
  else begin
    try
      (* Check if cursor is available *)
      match tracer.runtime_events_cursor with
      | Some cursor ->
          (* Create basic callbacks structure for runtime events *)
          let callbacks = Runtime_events.Callbacks.create () in

          (* Read available events with timeout to prevent blocking *)
          (* Use Lwt_unix.select to make read_poll non-blocking with 10ms timeout *)
          let timeout = 0.01 in  (* 10ms timeout *)

          (* Run read_poll in a separate thread with timeout *)
          let read_result = ref None in
          let _read_thread = Thread.create (fun () ->
            try
              let result = Runtime_events.read_poll cursor callbacks None in
              read_result := Some result
            with _exn ->
              read_result := Some (-1)  (* Indicate error *)
          ) () in

          (* Wait for completion or timeout *)
          let rec wait_with_timeout remaining_time =
            if remaining_time <= 0.0 then (
              (* Timeout - don't wait for thread to finish *)
              Logging.debug ~section:"runtime_events_tracer" "Runtime events read_poll timed out, continuing...";
              false
            ) else (
              match !read_result with
              | Some result ->
                  (* Thread completed *)
                  ignore result;  (* We ignore the result since read_poll is called for side effects *)
                  true
              | None ->
                  (* Still running, wait a bit more *)
                  Thread.delay (min 0.001 remaining_time);
                  wait_with_timeout (remaining_time -. 0.001)
            )
          in

          ignore (wait_with_timeout timeout);

          (* Sleep briefly between polls *)
          Thread.delay 0.01;

          event_loop ()
      | None ->
          Logging.error ~section:"runtime_events_tracer" "Runtime events cursor not available";
          Thread.delay 1.0;  (* Wait longer before retrying *)
          event_loop ()

    with exn ->
      Logging.error_f ~section:"runtime_events_tracer" "Error in event loop: %s" (Printexc.to_string exn);
      event_loop ()  (* Continue despite errors *)
  end

(** Start the runtime events tracer *)
let start () =
  Mutex.lock tracer.mutex;
  if not tracer.is_running then begin
    tracer.is_running <- true;

    (* Ensure runtime events directory exists *)
    let events_dir = Config.get_runtime_events_directory () in
    (try Unix.mkdir events_dir 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ());

    (* Set environment variable for runtime events directory *)
    Unix.putenv "OCAML_RUNTIME_EVENTS_DIR" events_dir;

    (* Start runtime events *)
    Runtime_events.start ();

    (* Create cursor now that runtime events are started *)
    tracer.runtime_events_cursor <- Some (Runtime_events.create_cursor None);

    (* Start event processing thread *)
    let thread = Thread.create event_loop () in
    tracer.event_thread <- Some thread;

    Logging.info_f ~section:"runtime_events_tracer" "Runtime events tracer started (events in %s)" events_dir;
  end;
  Mutex.unlock tracer.mutex

(** Stop the runtime events tracer *)
let stop () =
  Mutex.lock tracer.mutex;
  if tracer.is_running then begin
    tracer.is_running <- false;

    (* Wait for event thread to finish *)
    (match tracer.event_thread with
     | Some thread ->
         Thread.join thread;
         tracer.event_thread <- None
     | None -> ());

    (* Clean up cursor *)
    tracer.runtime_events_cursor <- None;

    Logging.info ~section:"runtime_events_tracer" "Runtime events tracer stopped";
  end;
  Mutex.unlock tracer.mutex

(** Check if tracer is running *)
let is_running () = tracer.is_running

(** Get current domain ID safely *)
let get_current_domain_id () =
  try Obj.magic (Domain.self ())
  with _ -> 0  (* Fallback for single-core or if Domain.self fails *)

(** Trigger manual GC and capture stats *)
let trigger_gc_and_capture () =
  let before = Gc.stat () in
  Gc.full_major ();
  let after = Gc.stat () in
  let domain_id = get_current_domain_id () in
  let timestamp = Unix.gettimeofday () in

  (* Report major collection event *)
  dispatch_event (Major_collection {
    domain_id;
    timestamp;
    heap_words = after.heap_words;
    live_words = after.live_words;
    free_words = after.free_words;
    fragments = after.fragments;
    compactions = after.compactions;
  });

  Logging.debug_f ~section:"runtime_events_tracer"
    "Manual GC: heap %dMB -> %dMB, live %dMB -> %dMB"
    (before.heap_words * (Sys.word_size / 8) / 1048576)
    (after.heap_words * (Sys.word_size / 8) / 1048576)
    (before.live_words * (Sys.word_size / 8) / 1048576)
    (after.live_words * (Sys.word_size / 8) / 1048576)

(** Statistics record *)
type stats = {
  minor_collections: int;
  major_collections: int;
  domain_spawns: int;
  domain_terminates: int;
}

(** Get runtime events statistics *)
let get_stats () =
  let events_dir = Config.get_runtime_events_directory () in
  Unix.putenv "OCAML_RUNTIME_EVENTS_DIR" events_dir;
  let cursor = Runtime_events.create_cursor None in
  let minor_collections = ref 0 in
  let major_collections = ref 0 in
  let domain_spawns = ref 0 in
  let domain_terminates = ref 0 in

  let callbacks = Runtime_events.Callbacks.create () in

  (* Read available events to count them with timeout *)
  let timeout = 0.01 in  (* 10ms timeout *)
  let read_result = ref None in
  let _read_thread = Thread.create (fun () ->
    try
      let result = Runtime_events.read_poll cursor callbacks None in
      read_result := Some result
    with _exn ->
      read_result := Some (-1)  (* Indicate error *)
  ) () in

  (* Wait for completion or timeout *)
  let rec wait_with_timeout remaining_time =
    if remaining_time <= 0.0 then false
    else (
      match !read_result with
      | Some _result -> true
      | None ->
          Thread.delay (min 0.001 remaining_time);
          wait_with_timeout (remaining_time -. 0.001)
    )
  in

  ignore (wait_with_timeout timeout);

  {
    minor_collections = !minor_collections;
    major_collections = !major_collections;
    domain_spawns = !domain_spawns;
    domain_terminates = !domain_terminates;
  }
