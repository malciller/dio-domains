(** Lightweight event bus for lock-free, snapshot-based fan-out.

    Producers publish immutable payloads; subscribers receive a stream of
    snapshots via `Lwt_stream`. Internally each topic maintains an
    atomically-swapped snapshot (`Atomic.t`) and a list of Lwt push
    functions. This keeps the hot path lock-free while delivering events to
    cooperative consumers.
*)

open Lwt.Infix

module type PAYLOAD = sig
  type t
end

(** Global registry of all event buses for monitoring and cleanup *)
type bus_ops = {
  topic: string;
  cleanup: unit -> int option;
  stats: unit -> (int * int * int);
}

let registry : bus_ops list Atomic.t = Atomic.make []

let register ops =
  let rec loop () =
    let old = Atomic.get registry in
    if Atomic.compare_and_set registry old (ops :: old) then ()
    else loop ()
  in
  loop ()

let iter_buses f =
  List.iter f (Atomic.get registry)

module Make (Payload : PAYLOAD) = struct
  type snapshot = Payload.t

  type subscription = {
    stream: snapshot Lwt_stream.t;
    close: unit -> unit;  (* Function to close this subscription's stream *)
  }

  type subscriber = {
    push: snapshot -> unit Lwt.t;
    close: unit -> unit;  (* Function to close this subscriber's stream *)
    mutable closed: bool;
    persistent: bool;  (* Mark persistent subscribers that should not be force cleaned *)
  }

  type t = {
    topic: string;
    subscribers: subscriber list Atomic.t;
    mutable publish_count: int;  (* Counter for event-driven cleanup *)
  }

  (** Clean up closed subscribers — only checks the closed flag, no syscalls *)
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
          Logging.debug_f ~section:"event_bus" "Cleaned up %d closed subscribers for topic %s (kept %d)" removed_count bus.topic (List.length to_keep);
          if Atomic.compare_and_set bus.subscribers current to_keep then
            Some removed_count
          else try_cleanup ()
        end else None
      in
      try_cleanup ()
    end

  (** Force cleanup all closed subscribers (alias kept for API compatibility) **)
  let force_cleanup_stale_subscribers bus () = cleanup_stale_subscribers bus ()

  (** Get subscriber statistics for monitoring *)
  let get_subscriber_stats bus =
    let subs = Atomic.get bus.subscribers in
    let total = List.length subs in
    let active = List.length (List.filter (fun sub -> not sub.closed) subs) in
    let closed = List.length (List.filter (fun sub -> sub.closed) subs) in
    (total, active, closed)

  let create ?initial:_ topic =
    let bus = {
      topic;
      subscribers = Atomic.make [];
      publish_count = 0;
    } in
    (* Register with global registry *)
    register {
      topic;
      cleanup = (fun () -> cleanup_stale_subscribers bus ());
      stats = (fun () -> get_subscriber_stats bus);
    };
    bus

  let topic bus = bus.topic

  let publish bus payload =
    (* Do NOT store payload in latest — retaining large Yojson trees / records causes
       unbounded memory growth when payloads are large (e.g. l2Book with full bid/ask
       arrays, webData2 snapshots, execution_event records). *)
    let subs = Atomic.get bus.subscribers in
    (* Filter out closed subscribers inline — avoids allocating a new list *)
    List.iter (fun sub ->
      if not sub.closed then begin
        (* Try non-blocking push first to avoid Lwt.async/Lwt.pick/timer overhead *)
        let push_result = sub.push payload in
        if Lwt.is_sleeping push_result then begin
          (* Stream is full — subscriber is too slow, close it immediately *)
          Lwt.cancel push_result;
          sub.closed <- true;
          (try sub.close () with _ -> ())
        end
        (* last_used tracking removed: Unix.time() syscall on every hot-path publish
           was measured as significant overhead at high WS frame rates. *)
      end
    ) subs;

    (* Event-driven cleanup: run cleanup every 100 publications *)
    bus.publish_count <- bus.publish_count + 1;
    if bus.publish_count mod 100 = 0 then
      ignore (cleanup_stale_subscribers bus ())

  let subscribe ?(persistent=false) bus =
    (* Use bounded stream to prevent unbounded memory growth *)
    let stream, push_source = Lwt_stream.create_bounded 64 in
    let subscriber = {
      push = (fun payload -> push_source#push payload);
      close = (fun () -> push_source#close);
      closed = false;
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
    (* Use finalize pattern to guarantee cleanup when stream closes *)
    Lwt.async (fun () ->
      Lwt.finalize
        (fun () -> Lwt_stream.closed stream)
        (fun () ->
          subscriber.closed <- true;
          try_remove ();
          Lwt.return_unit
        )
    );
    (* Snapshot-on-subscribe removed: we no longer retain latest payloads since
       large Yojson trees / records would be held indefinitely in the Atomic. *)
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
end


