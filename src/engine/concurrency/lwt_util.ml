(** Memory-safe alternatives to standard Lwt combinators.

    [Lwt_stream.iter] and recursive [>>= fun () -> loop ()] patterns accumulate
    Lwt [Forward] nodes: [>>=] chains each iteration's promise to its successor,
    and [Lwt.pause ()] appends another. These combinators resolve the current
    promise immediately with [Lwt.return_unit] and spawn the next iteration via
    [Lwt.async], severing the [Forward] chain so prior nodes become collectable. *)

open Lwt.Infix

(** Apply [f] synchronously to each element of [stream] without accumulating
    Lwt [Forward] nodes. Each iteration is spawned via [Lwt.async].
    @return a promise resolved when the stream closes ([None]). *)
let consume_stream f stream =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    Lwt.catch
      (fun () ->
         Lwt_stream.get stream
         >>= function
         | None ->
           Lwt.wakeup_later done_u ();
           Lwt.return_unit
         | Some x ->
           f x;
           (* Spawn the next iteration independently to sever the Forward chain. *)
           Lwt.async loop;
           Lwt.return_unit)
      (fun exn ->
         Lwt.wakeup_later_exn done_u exn;
         Lwt.return_unit)
  in
  Lwt.async loop;
  done_p
;;

(** Apply async [f] sequentially to each element of [stream] without
    accumulating [Forward] nodes. Unlike [consume_stream], awaits [f x] before
    scheduling the next iteration, providing backpressure for hot paths such as
    WebSocket frame processing. *)
let consume_stream_s f stream =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    Lwt.catch
      (fun () ->
         Lwt_stream.get stream
         >>= function
         | None ->
           Lwt.wakeup_later done_u ();
           Lwt.return_unit
         | Some x ->
           f x
           >>= fun () ->
           Lwt.async loop;
           Lwt.return_unit)
      (fun exn ->
         Lwt.wakeup_later_exn done_u exn;
         Lwt.return_unit)
  in
  Lwt.async loop;
  done_p
;;

(** Run [f ()] every [interval] seconds until [stop ()] returns [true]. Each
    iteration is spawned via [Lwt.async] to sever the promise chain.
    @param initial_delay Seconds to wait before the first iteration (default 0.0).
    @return a promise resolved when the loop exits. *)
let run_periodic ?(initial_delay = 0.0) ~interval ~stop f =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    if stop ()
    then (
      Lwt.wakeup_later done_u ();
      Lwt.return_unit)
    else
      Lwt.catch (fun () -> f ()) (fun _exn -> Lwt.return_unit)
      >>= fun () ->
      Lwt_unix.sleep interval
      >>= fun () ->
      if stop ()
      then (
        Lwt.wakeup_later done_u ();
        Lwt.return_unit)
      else (
        Lwt.async loop;
        Lwt.return_unit)
  in
  if initial_delay > 0.0
  then
    Lwt.async (fun () ->
      Lwt_unix.sleep initial_delay
      >>= fun () ->
      Lwt.async loop;
      Lwt.return_unit)
  else Lwt.async loop;
  done_p
;;

(** Poll [check ()] on each wakeup until it returns [true] or [timeout]
    expires. [wait_signal] must return a promise that resolves whenever
    downstream data may have changed (e.g. an [Lwt_condition.wait]). Each
    iteration is spawned via [Lwt.async] to sever the forward chain.
    @return [true] if [check ()] passed, [false] on timeout. *)
let poll_until ~timeout ~wait_signal ~check =
  let done_p, done_u = Lwt.wait () in
  let deadline = Unix.gettimeofday () +. timeout in
  let rec loop () =
    if check ()
    then (
      Lwt.wakeup_later done_u true;
      Lwt.return_unit)
    else (
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0
      then (
        Lwt.wakeup_later done_u (check ());
        Lwt.return_unit)
      else
        Lwt.pick
          [ (wait_signal () >|= fun () -> `Again)
          ; (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
          ]
        >>= function
        | `Again ->
          Lwt.async loop;
          Lwt.return_unit
        | `Timeout ->
          Lwt.wakeup_later done_u (check ());
          Lwt.return_unit)
  in
  Lwt.async loop;
  done_p
;;
