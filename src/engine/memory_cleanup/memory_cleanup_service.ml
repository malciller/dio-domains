(** Memory Cleanup Service - Background service for processing replacement events and triggering deferred destruction

    Subscribes to StructureReplaced events, batches them, and triggers deferred destruction
    of replaced allocations without disrupting operations.
*)

open Lwt.Infix

(** Configuration - balanced approach for stable operation *)
let cleanup_batch_size = 200  (* Allocations destroyed per batch - balanced *)
let cleanup_interval_seconds = 3.0  (* Seconds between cleanup cycles - moderate *)
let immediate_cleanup_threshold = 500  (* Trigger immediate cleanup when queue exceeds this *)
let startup_grace_period_seconds = 60.0  (* Don't be aggressive during first minute *)

(** Service state *)
type service_state = {
  mutable running: bool;
  mutable replacement_count: int;
  mutable destroyed_count: int;
  mutable memory_freed: int;
  mutable startup_time: float;
}

let service_state = {
  running = false;
  replacement_count = 0;
  destroyed_count = 0;
  memory_freed = 0;
  startup_time = Unix.gettimeofday ();
}

(** Process replacement events and trigger deferred destruction *)
let process_replacement_events () =
  (* Get count of allocations waiting to be destroyed *)
  let queue_size = Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.get_replaced_allocations_count () in
  let now = Unix.gettimeofday () in
  let startup_elapsed = now -. service_state.startup_time in
  let in_startup_grace_period = startup_elapsed < startup_grace_period_seconds in

  if queue_size > 0 then begin
    (* Use conservative cleanup during startup grace period *)
    let destroyed =
      if in_startup_grace_period then begin
        (* Conservative cleanup during startup - smaller batches, longer timeouts *)
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.destroy_replaced_allocations
          ~batch_size:(cleanup_batch_size / 4) ()  (* 50 allocations max during startup *)
      end else if queue_size >= immediate_cleanup_threshold then begin
        (* Large queue after startup - use normal cleanup *)
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.destroy_replaced_allocations ~batch_size:cleanup_batch_size ()
      end else begin
        (* Normal operation - moderate cleanup *)
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.destroy_replaced_allocations ~batch_size:(cleanup_batch_size / 2) ()
      end
    in

    if destroyed > 0 then begin
      service_state.destroyed_count <- service_state.destroyed_count + destroyed;
      let phase = if in_startup_grace_period then " (startup phase)" else "" in
      Logging.info_f ~section:"memory_cleanup_service"
        "Processed replacement events%s: destroyed %d allocations (queue size: %d)" phase destroyed queue_size
    end
  end;

  Lwt.return_unit

(** Background cleanup loop *)
let rec cleanup_loop () =
  if service_state.running then begin
    Lwt.catch
      (fun () ->
        (* Wait for interval *)
        Lwt_unix.sleep cleanup_interval_seconds >>= fun () ->
        (* Process replacement events *)
        process_replacement_events () >>= fun () ->
        (* Continue loop *)
        cleanup_loop ()
      )
      (fun exn ->
        Logging.warn_f ~section:"memory_cleanup_service"
          "Error in cleanup loop: %s" (Printexc.to_string exn);
        (* Continue despite errors *)
        Lwt_unix.sleep 1.0 >>= fun () ->
        cleanup_loop ()
      )
  end else
    Lwt.return_unit

(** Event processing loop - handles StructureReplaced events *)
let rec event_loop stream =
  Lwt.catch
    (fun () ->
      Lwt.bind (Lwt_stream.get stream) (function
        | Some event ->
            (match event with
            | Memory_cleanup_events.StructureReplaced { old_id; new_id; structure_type; estimated_memory_freed } ->
                service_state.replacement_count <- service_state.replacement_count + 1;
                service_state.memory_freed <- service_state.memory_freed + estimated_memory_freed;
                Logging.debug_f ~section:"memory_cleanup_service"
                  "Received StructureReplaced event: %d -> %d (%s, ~%d bytes)"
                  old_id new_id structure_type estimated_memory_freed;
                (* Check queue size and trigger immediate cleanup if needed (but not during startup) *)
                let now = Unix.gettimeofday () in
                let startup_elapsed = now -. service_state.startup_time in
                let in_startup_grace_period = startup_elapsed < startup_grace_period_seconds in

                if not in_startup_grace_period then begin
                  let queue_size = Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.get_replaced_allocations_count () in
                  if queue_size >= immediate_cleanup_threshold then begin
                    Logging.info_f ~section:"memory_cleanup_service"
                      "Queue size %d exceeds threshold %d, triggering immediate cleanup" queue_size immediate_cleanup_threshold;
                    Lwt.async process_replacement_events
                  end
                end;
                event_loop stream
            | Memory_cleanup_events.DeferredDestruction { allocation_ids; batch_size; estimated_memory_freed } ->
                Logging.debug_f ~section:"memory_cleanup_service"
                  "Received DeferredDestruction event: %d allocations (~%d bytes)"
                  (List.length allocation_ids) estimated_memory_freed;
                (* Trigger destruction *)
                let destroyed = Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.destroy_replaced_allocations ~batch_size () in
                if destroyed > 0 then begin
                  service_state.destroyed_count <- service_state.destroyed_count + destroyed;
                  Logging.info_f ~section:"memory_cleanup_service"
                    "Destroyed %d allocations from deferred destruction event" destroyed
                end;
                event_loop stream
            | _ ->
                (* Ignore other event types *)
                event_loop stream)
        | None ->
            (* Stream closed *)
            Logging.info ~section:"memory_cleanup_service" "Cleanup event stream closed";
            Lwt.return_unit
      )
    )
    (fun exn ->
      Logging.warn_f ~section:"memory_cleanup_service"
        "Error processing cleanup event: %s" (Printexc.to_string exn);
      (* Continue processing *)
      Lwt_unix.sleep 0.1 >>= fun () ->
      event_loop stream
    )

(** Start the memory cleanup service *)
let start () =
  service_state.running <- true;
  Logging.info_f ~section:"memory_cleanup_service"
    "Starting memory cleanup service (batch_size=%d, interval=%.1fs, immediate_threshold=%d, startup_grace=%.0fs)"
    cleanup_batch_size cleanup_interval_seconds immediate_cleanup_threshold startup_grace_period_seconds;
  
  (* Subscribe to cleanup events *)
  let (stream, _close) = Memory_cleanup_events.subscribe_cleanup_events () in
  
  (* Start background cleanup loop *)
  Lwt.async cleanup_loop;
  
  (* Start event processing loop *)
  Lwt.async (fun () -> event_loop stream);
  
  Logging.info ~section:"memory_cleanup_service" "Memory cleanup service started";
  Lwt.return_unit

(** Stop the memory cleanup service *)
let stop () =
  service_state.running <- false;
  Logging.info ~section:"memory_cleanup_service" "Memory cleanup service stopped"

(** Get service statistics *)
let get_stats () =
  {
    running = service_state.running;
    replacement_count = service_state.replacement_count;
    destroyed_count = service_state.destroyed_count;
    memory_freed = service_state.memory_freed;
    startup_time = service_state.startup_time;
  }

(** Initialize the service (called from main) *)
let init () =
  Logging.info_f ~section:"memory_cleanup_service"
    "Memory cleanup service initialized (batch_size=%d, interval=%.1fs, startup_grace=%.0fs)"
    cleanup_batch_size cleanup_interval_seconds startup_grace_period_seconds

