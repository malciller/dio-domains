(** Memory-safe alternatives to standard Lwt combinators.

    [Lwt_stream.iter] and recursive [>>= fun () -> loop ()] patterns
    accumulate Lwt [Forward] nodes because [>>=] chains each iteration's
    promise to its successor. [Lwt.pause ()] does not mitigate this; it
    appends an additional [Forward] node.

    The solution is to resolve the current promise immediately via
    [Lwt.return_unit] and spawn the next iteration as an independent
    task via [Lwt.async]. This severs the [Forward] chain so prior
    promise nodes become unreachable and eligible for collection. *)

open Lwt.Infix

(** Synchronously applies [f] to each element of [stream] without
    accumulating Lwt [Forward] nodes. Each iteration is spawned via
    [Lwt.async] to sever the promise chain. Returns a promise that
    resolves when the stream is closed ([None]). *)
let consume_stream f stream =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    Lwt_stream.get stream >>= function
    | None ->
        Lwt.wakeup_later done_u ();
        Lwt.return_unit
    | Some x ->
        f x;
        (* Spawn next iteration independently to sever the Forward chain.
           The current promise resolves immediately with unit. *)
        Lwt.async loop;
        Lwt.return_unit
  in
  Lwt.async loop;
  done_p
