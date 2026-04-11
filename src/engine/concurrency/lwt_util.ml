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

(** Sequentially applies async [f] to each element of [stream] without
    accumulating [Forward] nodes. Unlike [consume_stream], this awaits [f x]
    before scheduling the next iteration, which provides backpressure for
    hot paths such as WebSocket frame processing. *)
let consume_stream_s f stream =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    Lwt_stream.get stream >>= function
    | None ->
        Lwt.wakeup_later done_u ();
        Lwt.return_unit
    | Some x ->
        Lwt.catch
          (fun () -> f x)
          (fun _exn -> Lwt.return_unit)
        >>= fun () ->
        Lwt.async loop;
        Lwt.return_unit
  in
  Lwt.async loop;
  done_p

(** Runs [f ()] every [interval] seconds until [stop ()] returns [true].
    Each iteration is spawned via [Lwt.async] to sever the promise chain,
    identical to the pattern used in [consume_stream].

    If [initial_delay] is provided, waits that many seconds before the
    first iteration.

    Returns a promise that resolves when the loop exits (i.e. [stop ()]
    returns [true]). *)
let run_periodic ?(initial_delay=0.0) ~interval ~stop f =
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    if stop () then begin
      Lwt.wakeup_later done_u ();
      Lwt.return_unit
    end else
      Lwt.catch
        (fun () -> f ())
        (fun _exn -> Lwt.return_unit)
      >>= fun () ->
      Lwt_unix.sleep interval >>= fun () ->
      if stop () then begin
        Lwt.wakeup_later done_u ();
        Lwt.return_unit
      end else begin
        Lwt.async loop;
        Lwt.return_unit
      end
  in
  if initial_delay > 0.0 then
    Lwt.async (fun () -> Lwt_unix.sleep initial_delay >>= fun () ->
      Lwt.async loop; Lwt.return_unit)
  else
    Lwt.async loop;
  done_p

(** Polls [check ()] on each wakeup until it returns [true] or the
    [timeout] expires.  [wait_signal] should return a promise that
    resolves whenever downstream data may have changed (e.g. an
    [Lwt_condition.wait]).

    Unlike a raw [>>= fun () -> loop ()], each iteration is spawned via
    [Lwt.async] so the forward chain is severed.  Returns a promise that
    resolves to [true] if [check ()] passed, [false] on timeout. *)
let poll_until ~timeout ~wait_signal ~check =
  let done_p, done_u = Lwt.wait () in
  let deadline = Unix.gettimeofday () +. timeout in
  let rec loop () =
    if check () then begin
      Lwt.wakeup_later done_u true;
      Lwt.return_unit
    end else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then begin
        Lwt.wakeup_later done_u (check ());
        Lwt.return_unit
      end else
        Lwt.pick [
          (wait_signal () >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again ->
            Lwt.async loop;
            Lwt.return_unit
        | `Timeout ->
            Lwt.wakeup_later done_u (check ());
            Lwt.return_unit
  in
  Lwt.async loop;
  done_p
