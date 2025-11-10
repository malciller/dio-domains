(** Startup Coordinator - Event-driven coordination for startup phases *)

open Lwt.Infix

(** Startup phases that components can wait for or signal completion of *)
type startup_phase =
  | FeedsInit      (* All websocket feeds have been initialized and connected *)
  | DomainsInit    (* All supervised domains have been started *)
  | ServerStart    (* Metrics broadcast server has been started *)

(** Convert phase to string for logging *)
let phase_to_string = function
  | FeedsInit -> "feeds_init"
  | DomainsInit -> "domains_init"
  | ServerStart -> "server_start"

(** Event bus for startup phase completion notifications *)
module StartupEventBus = Event_bus.Make(struct
  type t = startup_phase
end)

(** Global startup event bus instance *)
let startup_event_bus = StartupEventBus.create "startup_coordinator"

(** Conditions for components that need to wait synchronously *)
let feeds_init_condition = Lwt_condition.create ()
let domains_init_condition = Lwt_condition.create ()
let server_start_condition = Lwt_condition.create ()

(** Track which phases have been completed *)
let feeds_init_completed = Atomic.make false
let domains_init_completed = Atomic.make false
let server_start_completed = Atomic.make false

(** Signal completion of a startup phase *)
let signal_phase_complete phase =
  let phase_name = phase_to_string phase in
  Logging.info ~section:"startup_coordinator" ("Phase completed: " ^ phase_name);

  (* Set completion flag atomically *)
  let completed_flag = match phase with
    | FeedsInit -> feeds_init_completed
    | DomainsInit -> domains_init_completed
    | ServerStart -> server_start_completed
  in
  Atomic.set completed_flag true;

  (* Broadcast to synchronous waiters *)
  let condition = match phase with
    | FeedsInit -> feeds_init_condition
    | DomainsInit -> domains_init_condition
    | ServerStart -> server_start_condition
  in
  Lwt_condition.broadcast condition ();

  (* Publish to event bus for stream-based subscribers *)
  StartupEventBus.publish startup_event_bus phase

(** Wait for a startup phase to complete (synchronous) *)
let wait_for_phase phase timeout_seconds =
  let condition = match phase with
    | FeedsInit -> feeds_init_condition
    | DomainsInit -> domains_init_condition
    | ServerStart -> server_start_condition
  in
  let completed_flag = match phase with
    | FeedsInit -> feeds_init_completed
    | DomainsInit -> domains_init_completed
    | ServerStart -> server_start_completed
  in

  (* Check if already completed *)
  if Atomic.get completed_flag then
    Lwt.return_true
  else begin
    let phase_name = phase_to_string phase in
    Logging.debug ~section:"startup_coordinator" ("Waiting for phase: " ^ phase_name);

    (* Wait with timeout *)
    let timeout_promise = Lwt_unix.sleep timeout_seconds >|= fun () -> false in
    let wait_promise = Lwt_condition.wait condition >|= fun () -> true in

    Lwt.pick [timeout_promise; wait_promise] >>= fun success ->
    if not success then
      Logging.warn ~section:"startup_coordinator" ("Timeout waiting for phase: " ^ phase_name);
    Lwt.return success
  end

(** Subscribe to startup phase completion events (stream-based) *)
let subscribe_to_phases () = StartupEventBus.subscribe startup_event_bus

(** Check if a phase has been completed *)
let is_phase_completed phase =
  let completed_flag = match phase with
    | FeedsInit -> feeds_init_completed
    | DomainsInit -> domains_init_completed
    | ServerStart -> server_start_completed
  in
  Atomic.get completed_flag
