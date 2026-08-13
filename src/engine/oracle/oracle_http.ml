(* Oracle_http - HTTP with timeouts for the oracle's network fetches.

   Every oracle fetch (venue OHLC, deep history, balances, fees) goes through
   this wrapper: a request that the upstream blackholes must not freeze the
   oracle pass forever. [Lwt_unix.with_timeout] cancels the wait after
   [timeout] seconds and the caller's existing failure handling (warn + fall
   back / return what it has) applies. The underlying request keeps running
   in the background when the timeout fires; the pass simply moves on.

   Callers that know the venue being fetched pass [~venue] (e.g.
   ~venue:"kraken"); the round trip is then recorded in that venue's
   rest_request profiler for the dashboard's NETWORK page. *)

open Lwt.Infix

let default_timeout = 10.0
let fee_timeout = 5.0

let fail_timeout what =
  Lwt.fail (Failure (Printf.sprintf "Oracle_http: %s timed out" what))
;;

(** Records the elapsed time since [start_ns] in [venue]'s rest profiler. *)
let record_rest venue start_ns =
  Network_latency.record_rest
    venue
    (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) start_ns))
;;

(** GET with a timeout; raises on timeout/transport errors like Cohttp does. *)
let get
      ?(timeout = default_timeout)
      ?(headers = Cohttp.Header.init ())
      ?venue
      (uri : Uri.t)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  let start_ns = Mtime_clock.now_ns () in
  let record () =
    match venue with
    | Some v -> record_rest v start_ns
    | None -> ()
  in
  Lwt.catch
    (fun () ->
       Lwt.pick
         [ (Cohttp_lwt_unix.Client.get ~headers uri
            >>= fun r ->
            record ();
            Lwt.return r)
         ; (Lwt_unix.sleep timeout
            >>= fun () ->
            record ();
            fail_timeout "GET")
         ])
    (fun exn ->
       record ();
       Lwt.fail exn)
;;

(** POST with a timeout; raises on timeout/transport errors like Cohttp does. *)
let post
      ?(timeout = default_timeout)
      ?(headers = Cohttp.Header.init ())
      ?(body = Cohttp_lwt.Body.empty)
      ?venue
      (uri : Uri.t)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  let start_ns = Mtime_clock.now_ns () in
  let record () =
    match venue with
    | Some v -> record_rest v start_ns
    | None -> ()
  in
  Lwt.catch
    (fun () ->
       Lwt.pick
         [ (Cohttp_lwt_unix.Client.post ~headers ~body uri
            >>= fun r ->
            record ();
            Lwt.return r)
         ; (Lwt_unix.sleep timeout
            >>= fun () ->
            record ();
            fail_timeout "POST")
         ])
    (fun exn ->
       record ();
       Lwt.fail exn)
;;
