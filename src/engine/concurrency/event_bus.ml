(** Lock-free, snapshot-based event bus with bounded fan-out.

    Producers publish immutable payloads to a named topic. Subscribers receive
    copies via bounded [Lwt_stream] channels. Each topic maintains an atomic
    subscriber list. The publish path is lock-free: it iterates the subscriber
    list under a single [Atomic.get] and pushes non-blockingly. Slow subscribers
    whose bounded streams are full are closed immediately to prevent backpressure
    propagation. Periodic cleanup of closed subscribers runs every 100 publishes.
*)

open Lwt.Infix

module type PAYLOAD = sig
  type t
end

(** Existential record exposing per-bus operations to the global registry.
    [cleanup] triggers stale subscriber removal and returns the count removed.
    [stats] returns (total, active, closed) subscriber counts. *)
type bus_ops = {
  topic: string;
  cleanup: unit -> int option;
  stats: unit -> (int * int * int);
}

(** Global atomic list of all instantiated buses. Used for cross-bus
    monitoring and coordinated cleanup from external callers. *)
let registry : bus_ops list Atomic.t = Atomic.make []

(** CAS-loop insertion into the global bus registry. *)
let register ops =
  let rec loop () =
    let old = Atomic.get registry in
    if Atomic.compare_and_set registry old (ops :: old) then ()
    else loop ()
  in
  loop ()

(** Iterate over all registered buses. Snapshot is taken once via [Atomic.get]. *)
let iter_buses f =
  List.iter f (Atomic.get registry)

module Make (Payload : PAYLOAD) = struct
  type snapshot = Payload.t

  type subscription = {
    stream: snapshot Lwt_stream.t;
    close: unit -> unit;  (** Closes this subscription's bounded stream. *)
  }

  type subscriber = {
    push: snapshot -> unit Lwt.t;
    close: unit -> unit;  (** Closes the underlying [Lwt_stream.bounded_push]. *)
    mutable closed: bool;
    persistent: bool;  (** Persistent subscribers are exempt from forced cleanup. *)
  }

  type t = {
    topic: string;
    subscribers: subscriber list Atomic.t;
    mutable publish_count: int;  (** Monotonic counter driving periodic cleanup. *)
  }

  (** Remove non-persistent subscribers whose [closed] flag is set. Uses a
      CAS loop to atomically swap the filtered list. Returns [Some n] if
      [n] subscribers were removed, [None] if none were stale. The
      [max_age_seconds] and [max_unused_seconds] parameters are accepted
      for interface compatibility but are currently unused. *)
  let cleanup_stale_subscribers bus ?(max_age_seconds=60.0) ?(max_unused_seconds=30.0) () =
    ignore max_age_seconds; ignore max_unused_seconds;
    let current = Atomic.get bus.subscribers in
    let has_closed = List.exists (fun sub -> not sub.persistent && sub.closed) current in
    if not has_closed then None
    else begin
      let rec try_cleanup () =
        let current = Atomic.get bus.subscribers in
        let to_keep = List.filter (fun sub -> sub.persistent || not sub.closed) current in
        let removed_count = List.length current - List.length to_keep in
        if removed_count > 0 then begin
          let to_remove = List.filter (fun sub -> not (List.memq sub to_keep)) current in
          List.iter (fun sub ->
            sub.closed <- true;
            (try sub.close () with _ -> ())
          ) to_remove;
          if Atomic.compare_and_set bus.subscribers current to_keep then
            Some removed_count
          else try_cleanup ()
        end else None
      in
      try_cleanup ()
    end

  (** Alias for [cleanup_stale_subscribers] with default parameters.
      Retained for backward API compatibility. *)
  let force_cleanup_stale_subscribers bus () = cleanup_stale_subscribers bus ()

  (** Returns [(total, active, closed)] subscriber counts for this bus. *)
  let get_subscriber_stats bus =
    let subs = Atomic.get bus.subscribers in
    let total = List.length subs in
    let active = List.length (List.filter (fun sub -> not sub.closed) subs) in
    let closed = List.length (List.filter (fun sub -> sub.closed) subs) in
    (total, active, closed)

  (** Allocate a new bus for the given [topic]. Registers the bus in the
      global registry for external monitoring. The [?initial] parameter is
      accepted but ignored; payloads are not retained in an [Atomic.t] to
      avoid unbounded memory growth from large structures. *)
  let create ?initial:_ topic =
    let bus = {
      topic;
      subscribers = Atomic.make [];
      publish_count = 0;
    } in
    register {
      topic;
      cleanup = (fun () -> cleanup_stale_subscribers bus ());
      stats = (fun () -> get_subscriber_stats bus);
    };
    bus

  let topic bus = bus.topic

  (** Publish [payload] to all active subscribers. Each subscriber's push
      is attempted non-blockingly. If the push promise is sleeping (i.e. the
      bounded stream buffer is full), the subscriber is marked closed and its
      stream is terminated to avoid backpressure. Cleanup of stale subscribers
      is triggered every 100 publishes. No payload is retained after dispatch. *)
  let publish bus payload =
    let subs = Atomic.get bus.subscribers in
    List.iter (fun sub ->
      if not sub.closed then begin
        let push_result = sub.push payload in
        if Lwt.is_sleeping push_result then begin
          (* Bounded stream is full; subscriber cannot keep up. *)
          Lwt.cancel push_result;
          sub.closed <- true;
          (try sub.close () with _ -> ())
        end
      end
    ) subs;

    bus.publish_count <- bus.publish_count + 1;
    if bus.publish_count mod 100 = 0 then
      ignore (cleanup_stale_subscribers bus ())

  (** Create a new subscription backed by a bounded (capacity 4) [Lwt_stream].
      The subscriber is atomically prepended to the bus's subscriber list via
      a CAS loop. An [Lwt.finalize] handler on [Lwt_stream.closed] ensures
      the subscriber is removed from the list when the stream is closed.
      If [persistent] is [true], the subscriber is exempt from forced cleanup.
      No initial snapshot is pushed; payloads are not retained by the bus. *)
  let subscribe ?(persistent=false) bus =
    let stream, push_source = Lwt_stream.create_bounded 4 in
    let subscriber = {
      push = (fun payload -> push_source#push payload);
      close = (fun () -> push_source#close);
      closed = false;
      persistent;
    } in
    (* CAS loop: atomically prepend subscriber to the list. *)
    let rec try_add () =
      let current = Atomic.get bus.subscribers in
      if List.exists (fun s -> s == subscriber) current then ()
      else if Atomic.compare_and_set bus.subscribers current (subscriber :: current) then ()
      else try_add ()
    in
    (* CAS loop: atomically remove subscriber from the list. *)
    let rec try_remove () =
      let current = Atomic.get bus.subscribers in
      let filtered = List.filter (fun s -> s != subscriber) current in
      if Atomic.compare_and_set bus.subscribers current filtered then () else try_remove ()
    in
    try_add ();
    (* Guarantee cleanup on stream closure via Lwt finalizer. *)
    Lwt.async (fun () ->
      Lwt.finalize
        (fun () -> Lwt_stream.closed stream)
        (fun () ->
          subscriber.closed <- true;
          try_remove ();
          Lwt.return_unit
        )
    );
    { stream; close = subscriber.close }

  (** Subscribe, await a single event or timeout, then close. Returns
      [Some payload] if an event arrived within [timeout] seconds,
      [None] on timeout. The subscription is cleaned up in the finalizer. *)
  let await_next bus timeout =
    let subscription = subscribe bus in
    Lwt.finalize
      (fun () ->
        Lwt.pick [
          (Lwt_stream.get subscription.stream >|= fun evt -> evt);
          (Lwt_unix.sleep timeout >|= fun () -> None)
        ])
      (fun () -> subscription.close (); Lwt.return_unit)
end
