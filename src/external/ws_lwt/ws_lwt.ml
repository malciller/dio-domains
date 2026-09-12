(* Internal, client-only replacement for the lwt websocket wrapper.
   Derived from the websocket 2.17 lwt wrapper (ISC, Vincent Bernardoff); the framing
   is delegated to [Websocket.Make (Cohttp_lwt_unix.IO)] so the wire format is
   byte-identical. Differences from the reference:
   - no [Lwt_log] dependency (removed the [lwt < 6] cap);
   - no server-side code (we only ever connect as a client).
   [set_tcp_nodelay] and [Conduit_lwt_unix.connect] are reproduced verbatim. *)

open Websocket
open Lwt.Infix

module Impl = Websocket.Make (Cohttp_lwt_unix.IO)

exception HTTP_Error of string

let http_error msg = Lwt.fail (HTTP_Error msg)
let protocol_error msg = Lwt.fail (Protocol_error msg)

let set_tcp_nodelay flow =
  let open Conduit_lwt_unix in
  match flow with
  | TCP { fd; _ } -> Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY true
  | _ -> ()
;;

let fail_unless eq f = if not eq then f () else Lwt.return_unit
let fail_if eq f = if eq then f () else Lwt.return_unit

let drain_handshake req ic oc nonce =
  Impl.Request.write (fun _writer -> Lwt.return ()) req oc
  >>= fun () ->
  (Impl.Response.read ic
   >>= function
   | `Ok r -> Lwt.return r
   | `Eof -> Lwt.fail End_of_file
   | `Invalid s -> Lwt.fail @@ Failure s)
  >>= fun response ->
  let open Cohttp in
  let status = Response.status response in
  let headers = Response.headers response in
  fail_if
    Code.(is_error @@ code_of_status status)
    (fun () -> http_error Code.(string_of_status status))
  >>= fun () ->
  fail_unless
    (Response.version response = `HTTP_1_1)
    (fun () -> protocol_error "wrong http version")
  >>= fun () ->
  fail_unless (status = `Switching_protocols) (fun () ->
    protocol_error "wrong status")
  >>= fun () ->
  (match Header.get headers "upgrade" with
   | Some a when String.lowercase_ascii a = "websocket" -> Lwt.return_unit
   | _ -> protocol_error "wrong upgrade")
  >>= fun () ->
  fail_unless (upgrade_present headers) (fun () ->
    protocol_error "upgrade header not present")
  >>= fun () ->
  match Header.get headers "sec-websocket-accept" with
  | Some accept when accept = b64_encoded_sha1sum (nonce ^ websocket_uuid) ->
    Lwt.return_unit
  | _ -> protocol_error "wrong accept"
;;

let open_connection ctx client url nonce extra_headers =
  let open Cohttp in
  let headers =
    Header.add_list
      extra_headers
      [ "Upgrade", "websocket"
      ; "Connection", "Upgrade"
      ; "Sec-WebSocket-Key", nonce
      ; "Sec-WebSocket-Version", "13"
      ]
  in
  let req = Request.make ~headers url in
  Conduit_lwt_unix.connect ~ctx client
  >>= fun (flow, ic, oc) ->
  set_tcp_nodelay flow;
  Lwt.catch
    (fun () -> drain_handshake req ic oc nonce)
    (fun exn ->
       Lwt_io.close ic >>= fun () -> Lwt.fail exn)
  >>= fun () ->
  Lwt.return (ic, oc)
;;

type conn =
  { read_frame : unit -> Frame.t Lwt.t
  ; write_frame : Websocket.Frame.t -> unit Lwt.t
  ; oc : Lwt_io.output_channel
  }

let read { read_frame; _ } = read_frame ()
let write { write_frame; _ } frame = write_frame frame
let close_transport { oc; _ } = Lwt_io.close oc
let resolve_ctx () = Ctx.resolve ()

let connect
  ?(extra_headers = Cohttp.Header.init ())
  ?(random_string = Websocket.Rng.init ())
  ?(ctx = Ctx.resolve ())
  ?buf
  client
  url
  =
  let nonce = Base64.encode_exn (random_string 16) in
  open_connection ctx client url nonce extra_headers
  >|= fun (ic, oc) ->
  let read_frame = Impl.make_read_frame ?buf ~mode:(Impl.Client random_string) ic oc in
  let read_frame () =
    Lwt.catch read_frame (fun exn ->
      Lwt.async (fun () -> Lwt_io.close ic);
      Lwt.fail exn)
  in
  let buf = Buffer.create 128 in
  let write_frame frame =
    Buffer.clear buf;
    Lwt.wrap2 (Impl.write_frame_to_buf ~mode:(Impl.Client random_string)) buf frame
    >>= fun () ->
    Lwt.catch
      (fun () ->
         Lwt_io.write oc (Buffer.contents buf)
         >>= fun () -> Lwt_io.flush oc)
      (fun exn ->
         Lwt.async (fun () -> Lwt_io.close oc);
         Lwt.fail exn)
  in
  { read_frame; write_frame; oc }
;;
