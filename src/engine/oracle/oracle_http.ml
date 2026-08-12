(* Oracle_http - HTTP with timeouts for the oracle's network fetches.

   Every oracle fetch (venue OHLC, deep history, balances, fees) goes through
   this wrapper: a request that the upstream blackholes must not freeze the
   oracle pass forever. [Lwt_unix.with_timeout] cancels the wait after
   [timeout] seconds and the caller's existing failure handling (warn + fall
   back / return what it has) applies. The underlying request keeps running
   in the background when the timeout fires; the pass simply moves on. *)

open Lwt.Infix

let default_timeout = 10.0
let fee_timeout = 5.0

let fail_timeout what =
  Lwt.fail (Failure (Printf.sprintf "Oracle_http: %s timed out" what))
;;

(** GET with a timeout; raises on timeout/transport errors like Cohttp does. *)
let get ?(timeout = default_timeout) ?(headers = Cohttp.Header.init ()) (uri : Uri.t)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  Lwt.catch
    (fun () ->
       Lwt.pick
         [ Cohttp_lwt_unix.Client.get ~headers uri
         ; (Lwt_unix.sleep timeout >>= fun () -> fail_timeout "GET")
         ])
    (fun exn -> Lwt.fail exn)
;;

(** POST with a timeout; raises on timeout/transport errors like Cohttp does. *)
let post
      ?(timeout = default_timeout)
      ?(headers = Cohttp.Header.init ())
      ?(body = Cohttp_lwt.Body.empty)
      (uri : Uri.t)
  : (Cohttp.Response.t * Cohttp_lwt.Body.t) Lwt.t
  =
  Lwt.catch
    (fun () ->
       Lwt.pick
         [ Cohttp_lwt_unix.Client.post ~headers ~body uri
         ; (Lwt_unix.sleep timeout >>= fun () -> fail_timeout "POST")
         ])
    (fun exn -> Lwt.fail exn)
;;
