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
    let subs = Atomic.get bus.subscribers in
    List.iter (fun sub -> if not sub.closed then sub.push payload) subs

  let subscribe bus =
    let stream, push = Lwt_stream.create () in
    let subscriber = { push = (fun payload -> push (Some payload)); closed = false } in
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
end


