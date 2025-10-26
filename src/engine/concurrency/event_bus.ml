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

module Make (Payload : PAYLOAD) = struct
  type snapshot = Payload.t

  type subscriber = {
    push: snapshot -> unit;
    mutable closed: bool;
    created_at: float;  (* Track when subscriber was created *)
    mutable last_used: float;  (* Track when subscriber was last used *)
  }

  type t = {
    topic: string;
    latest: snapshot option Atomic.t;
    subscribers: subscriber list Atomic.t;
  }

  let create ?initial topic =
    {
      topic;
      latest = Atomic.make initial;
      subscribers = Atomic.make [];
    }

  let topic bus = bus.topic

  let latest bus = Atomic.get bus.latest

  let publish bus payload =
    Atomic.set bus.latest (Some payload);
    let now = Unix.time () in
    let subs = Atomic.get bus.subscribers in
    List.iter (fun sub ->
      if not sub.closed then begin
        sub.last_used <- now;
        sub.push payload
      end
    ) subs

  let subscribe bus =
    let stream, push = Lwt_stream.create () in
    let now = Unix.time () in
    let subscriber = {
      push = (fun payload -> push (Some payload));
      closed = false;
      created_at = now;
      last_used = now;
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
    stream

  let await_next bus timeout =
    let stream = subscribe bus in
    Lwt.pick [
      (Lwt_stream.get stream >|= fun evt -> evt);
      (Lwt_unix.sleep timeout >|= fun () -> None)
    ]

  (** Clean up stale subscribers (older than max_age_seconds or unused for too long) *)
  let cleanup_stale_subscribers bus ?(max_age_seconds=3600.0) ?(max_unused_seconds=300.0) () =
    let now = Unix.time () in
    let rec try_cleanup () =
      let current = Atomic.get bus.subscribers in
      let filtered = List.filter (fun sub ->
        not sub.closed &&
        (now -. sub.created_at) <= max_age_seconds &&
        (now -. sub.last_used) <= max_unused_seconds
      ) current in
      let removed_count = List.length current - List.length filtered in
      if removed_count > 0 then begin
        if Atomic.compare_and_set bus.subscribers current filtered then
          Some removed_count
        else try_cleanup ()
      end else None
    in
    try_cleanup ()

  (** Get subscriber statistics for monitoring *)
  let get_subscriber_stats bus =
    let subs = Atomic.get bus.subscribers in
    let now = Unix.time () in
    let total = List.length subs in
    let active = List.length (List.filter (fun sub -> not sub.closed) subs) in
    let stale = List.length (List.filter (fun sub ->
      sub.closed || (now -. sub.last_used) > 300.0
    ) subs) in
    (total, active, stale)
end


