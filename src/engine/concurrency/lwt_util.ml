(** Lwt Utilities — memory-safe alternatives to standard Lwt combinators.

    [Lwt_stream.iter] and recursive [>>= fun () -> loop ()] patterns create
    a growing chain of Lwt [Forward] nodes.  When any root holds a reference
    to the first promise in the chain, old nodes cannot be collected and
    memory grows linearly with the number of iterations.

    The helpers below break the forwarding chain by periodically yielding
    to the scheduler via [Lwt.pause ()], creating a fresh promise root. *)

open Lwt.Infix

(** Consume a stream, calling [f] on each item synchronously.
    Unlike [Lwt_stream.iter], this periodically breaks the Lwt forwarding
    chain so old promise nodes can be garbage-collected.
    [batch] controls how many items are processed before yielding (default 128). *)
let consume_stream ?(batch=128) f stream =
  let rec outer () =
    inner 0
  and inner n =
    Lwt_stream.get stream >>= function
    | None -> Lwt.return_unit
    | Some x ->
        f x;
        if n + 1 >= batch then
          (* Yield to scheduler — breaks the forwarding chain *)
          Lwt.pause () >>= fun () -> outer ()
        else
          inner (n + 1)
  in
  outer ()

(** Run [f] in an infinite loop, breaking the forwarding chain between
    each invocation.  Unlike [let rec loop () = f () >>= fun () -> loop ()],
    old promise nodes are collectible between cycles.
    Exceptions from [f] are caught and ignored (the loop continues). *)
let rec run_forever f =
  Lwt.catch f (fun _exn -> Lwt.return_unit)
  >>= fun () ->
  Lwt.pause () >>= fun () ->
  run_forever f
