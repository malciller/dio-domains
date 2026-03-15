(** Generic publish/subscribe event bus using Lwt_stream *)

module type PAYLOAD = sig
  type t
end

module Make (P : PAYLOAD) = struct
  type payload = P.t

  type subscription = {
    stream: payload Lwt_stream.t;
    close: unit -> unit;
  }

  type t = {
    name: string;
    mutable pushers: (payload option -> unit) list;
    mutex: Mutex.t;
  }

  let create name = { name; pushers = []; mutex = Mutex.create () }

  let topic bus = bus.name

  let subscribe bus =
    let (stream, push) = Lwt_stream.create () in
    Mutex.lock bus.mutex;
    bus.pushers <- push :: bus.pushers;
    Mutex.unlock bus.mutex;
    let close () =
      Mutex.lock bus.mutex;
      bus.pushers <- List.filter (fun p -> p != push) bus.pushers;
      Mutex.unlock bus.mutex;
      push None
    in
    { stream; close }

  let publish bus payload =
    Mutex.lock bus.mutex;
    let pushers = bus.pushers in
    Mutex.unlock bus.mutex;
    List.iter (fun push -> push (Some payload)) pushers
end
