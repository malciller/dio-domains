(** Lwt Utilities — memory-safe alternatives to standard Lwt combinators.

    [Lwt_stream.iter] and recursive [>>= fun () -> loop ()] patterns create
    a growing chain of Lwt [Forward] nodes because [>>=] links each
    iteration's promise to the next one.  Even [Lwt.pause ()] does not help:
    it merely adds another [Forward] node to the chain.

    The ONLY way to break the chain is to resolve the current promise
    immediately (via [Lwt.return_unit]) and spawn the next iteration as
    an independent task (via [Lwt.async]).  This ensures no [Forward] node
    links old iterations to new ones, so old promise nodes become unreachable
    and can be garbage-collected. *)

open Lwt.Infix

(** Consume a stream, calling [f] on each item synchronously.
    Unlike [Lwt_stream.iter], this does NOT accumulate Lwt [Forward] nodes.
    Each iteration is spawned independently via [Lwt.async], breaking the chain.
    Returns a promise that resolves when the stream ends ([None]). *)
let consume_stream f stream =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    Lwt_stream.get stream >>= function
    | None ->
        Lwt.wakeup_later done_u ();
        Lwt.return_unit
    | Some x ->
        f x;
        (* Spawn next iteration independently — breaks the Forward chain.
           The current promise resolves immediately with unit. *)
        Lwt.async loop;
        Lwt.return_unit
  in
  Lwt.async loop;
  done_p
