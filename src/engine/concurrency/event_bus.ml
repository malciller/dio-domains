(** Lightweight event bus for lock-free, snapshot-based fan-out.

    Producers publish immutable payloads; subscribers receive a stream of
    snapshots via `Lwt_stream`. Internally each topic maintains an
    atomically-swapped snapshot (`Atomic.t`) and a list of Lwt push
    functions. This keeps the hot path lock-free while delivering events to
    cooperative consumers.

    Enhanced with automatic subscriber cleanup and monitoring to prevent memory leaks.
*)

open Lwt.Infix

module type PAYLOAD = sig
  type t
end

module Make (Payload : PAYLOAD) = struct
  type snapshot = Payload.t

  type subscription = {
    stream: snapshot Lwt_stream.t;
    close: unit -> unit;  (* Function to close this subscription's stream *)
  }

  type subscriber = {
    push: snapshot -> unit;
    close: unit -> unit;  (* Function to close this subscriber's stream *)
    mutable closed: bool;
    created_at: float;  (* Track when subscriber was created *)
    mutable last_used: float;  (* Track when subscriber was last used *)
    persistent: bool;  (* Mark persistent subscribers that should not be force cleaned *)
  }

  type t = {
    topic: string;
    latest: snapshot option Atomic.t;
    subscribers: subscriber list Atomic.t;
    mutable last_cleanup: float;  (* Track when we last cleaned up subscribers *)
    mutable cleanup_enabled: bool;  (* Enable/disable automatic cleanup *)
  }

  let create ?initial ?(cleanup_enabled=true) topic =
    {
      topic;
      latest = Atomic.make initial;
      subscribers = Atomic.make [];
      last_cleanup = Unix.time ();
      cleanup_enabled;
    }

  let topic bus = bus.topic

  let latest bus = Atomic.get bus.latest

  (** Clear the latest snapshot to prevent memory retention *)
  let clear_latest bus = Atomic.set bus.latest None

  (** Clean up stale subscribers (older than max_age_seconds or unused for too long) *)
  let cleanup_stale_subscribers bus ?(max_age_seconds=30.0) ?(max_unused_seconds=10.0) () =
    let now = Unix.time () in
    let rec try_cleanup () =
      let current = Atomic.get bus.subscribers in
      let filtered = List.filter (fun sub ->
        (* Never clean up persistent subscribers (like broadcast streams) *)
        sub.persistent ||
        (not sub.closed &&
         (now -. sub.created_at) <= max_age_seconds &&
         (now -. sub.last_used) <= max_unused_seconds &&
         (* Additional aggressive cleanup: remove subscribers that have been unused for extended periods *)
         (now -. sub.last_used) <= 300.0  (* Hard limit: 5 minutes max unused time *)
        )
      ) current in
      let removed_count = List.length current - List.length filtered in
      if removed_count > 0 then begin
        (* Log cleanup activity for monitoring *)
        Logging.debug_f ~section:"event_bus" "Cleaning %d stale subscribers for topic '%s' (max_age: %.1fs, max_unused: %.1fs)"
          removed_count bus.topic max_age_seconds max_unused_seconds;
        if Atomic.compare_and_set bus.subscribers current filtered then begin
          bus.last_cleanup <- now;
          Some removed_count
        end else try_cleanup ()
      end else None
    in
    try_cleanup ()

  let publish bus payload =
    Atomic.set bus.latest (Some payload);
    let now = Unix.time () in

    (* Automatic cleanup check - run periodically during publish operations *)
    if bus.cleanup_enabled && (now -. bus.last_cleanup) > 15.0 then begin
      (* Non-blocking cleanup - don't block the publish operation *)
      Lwt.async (fun () ->
        match cleanup_stale_subscribers bus ~max_age_seconds:30.0 ~max_unused_seconds:10.0 () with
        | Some cleaned_count when cleaned_count > 0 ->
            Logging.debug_f ~section:"event_bus" "Auto-cleaned %d stale subscribers for topic '%s'" cleaned_count bus.topic;
            bus.last_cleanup <- Unix.time ();
            Lwt.return_unit
        | _ ->
            bus.last_cleanup <- now;
            Lwt.return_unit
      )
    end;

    let subs = Atomic.get bus.subscribers in
    (* Filter out closed subscribers and validate each one before pushing *)
    let active_subs = List.filter (fun sub -> not sub.closed) subs in
    List.iter (fun sub ->
      (* Double-check subscriber is still active before pushing *)
      if not sub.closed then begin
        sub.last_used <- now;
        (* Make pushes non-blocking to prevent backpressure *)
        Lwt.async (fun () ->
          Lwt.catch
            (fun () ->
              (* Use very short timeout to detect blocking immediately *)
              Lwt.pick [
                (Lwt.return (sub.push payload) >|= fun () -> ());
                (Lwt_unix.sleep 0.001 >|= fun () -> ())  (* 1ms timeout - fail fast on blocking *)
              ]
            )
            (fun exn ->
              (* Mark subscriber as closed on push failure (including timeout) *)
              Logging.debug_f ~section:"event_bus" "Subscriber push failed/timed out, marking closed: %s" (Printexc.to_string exn);
              sub.closed <- true;
              Lwt.return_unit
            )
        )
      end
    ) active_subs

  let subscribe ?(persistent=false) bus =
    let stream, push = Lwt_stream.create () in
    let now = Unix.time () in
    let subscriber = {
      push = (fun payload -> push (Some payload));
      close = (fun () -> push None);  (* Close the stream by sending None *)
      closed = false;
      created_at = now;
      last_used = now;
      persistent;
    } in
    let rec try_add () =
      let current = Atomic.get bus.subscribers in
      if List.exists (fun s -> s == subscriber) current then ()
      else if Atomic.compare_and_set bus.subscribers current (subscriber :: current) then ()
      else try_add ()
    in
    let rec try_remove () =
      let current = Atomic.get bus.subscribers in
      let filtered = List.filter (fun s -> s != subscriber) current in
      if Atomic.compare_and_set bus.subscribers current filtered then () else try_remove ()
    in
    try_add ();
    Lwt.async (fun () ->
      Lwt_stream.closed stream >|= fun () ->
      subscriber.closed <- true;
      try_remove ()
    );
    (match Atomic.get bus.latest with
    | Some payload -> push (Some payload)
    | None -> ());
    { stream; close = subscriber.close }

  let await_next bus timeout =
    let subscription = subscribe bus in
    Lwt.finalize
      (fun () ->
        Lwt.pick [
          (Lwt_stream.get subscription.stream >|= fun evt -> evt);
          (Lwt_unix.sleep timeout >|= fun () -> None)
        ])
      (fun () -> subscription.close (); Lwt.return_unit)

  (** Force cleanup all stale subscribers regardless of age (for dashboard memory management) *)
  let force_cleanup_stale_subscribers bus () =
    let now = Unix.time () in
    let rec try_cleanup () =
      let current = Atomic.get bus.subscribers in
      let filtered = List.filter (fun sub ->
        (* Never force cleanup persistent subscribers *)
        sub.persistent ||
        (not sub.closed && (now -. sub.last_used) <= 10.0)  (* Keep only very recently used subscribers - reduced from 30s to 10s *)
      ) current in
      let removed_count = List.length current - List.length filtered in
      if removed_count > 0 then begin
        Logging.info_f ~section:"event_bus" "Force cleaned %d stale subscribers for topic '%s' (kept %d active)"
          removed_count bus.topic (List.length filtered);
        if Atomic.compare_and_set bus.subscribers current filtered then begin
          bus.last_cleanup <- now;
          Some removed_count
        end else try_cleanup ()
      end else None
    in
    try_cleanup ()

  (** Get subscriber statistics for monitoring *)
  let get_subscriber_count bus =
    let subs = Atomic.get bus.subscribers in
    List.length subs

  let get_subscriber_stats bus =
    let subs = Atomic.get bus.subscribers in
    let now = Unix.time () in
    let total = List.length subs in
    let active = List.length (List.filter (fun sub -> not sub.closed) subs) in
    let stale = List.length (List.filter (fun sub ->
      sub.closed || (now -. sub.last_used) > 300.0
    ) subs) in
    let persistent = List.length (List.filter (fun sub -> sub.persistent) subs) in
    let unused_60s = List.length (List.filter (fun sub ->
      not sub.closed && not sub.persistent && (now -. sub.last_used) > 60.0
    ) subs) in
    let unused_300s = List.length (List.filter (fun sub ->
      not sub.closed && not sub.persistent && (now -. sub.last_used) > 300.0
    ) subs) in
    (total, active, stale, persistent, unused_60s, unused_300s)

  (** Monitor subscriber counts and log warnings for high counts *)
  let monitor_subscriber_count bus ?(warning_threshold=50) ?(critical_threshold=100) () =
    let (total, active, stale, persistent, unused_60s, unused_300s) = get_subscriber_stats bus in

    (* Log warnings for high subscriber counts *)
    if total >= critical_threshold then
      Logging.warn_f ~section:"event_bus" "CRITICAL: Event bus '%s' has %d subscribers (threshold: %d)"
        bus.topic total critical_threshold
    else if total >= warning_threshold then
      Logging.warn_f ~section:"event_bus" "WARNING: Event bus '%s' has %d subscribers (approaching %d limit)"
        bus.topic total critical_threshold;

    (* Log detailed stats for monitoring *)
    if total > 10 then  (* Only log detailed stats for buses with many subscribers *)
      Logging.debug_f ~section:"event_bus" "Event bus '%s' stats: total=%d, active=%d, stale=%d, persistent=%d, unused_60s=%d, unused_300s=%d"
        bus.topic total active stale persistent unused_60s unused_300s;

    (total, active, stale, persistent, unused_60s, unused_300s)
end


