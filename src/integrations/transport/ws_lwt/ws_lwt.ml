(* Client-only WebSocket connection over the in-tree Conduit/Cohttp/Lwt stack.
   [Websocket.Make] owns RFC 6455 framing; this file owns the opening handshake, channel
   lifetime and TCP tuning. *)

open Lwt.Infix

(* A peer controls the payload length in a frame header; the frame reader would otherwise
   allocate it in full. No exchange message is close to this size. *)
let max_frame_bytes = 64 * 1024 * 1024

module Io = struct
  include Cohttp_lwt_unix.IO

  let read ic count =
    if count <= max_frame_bytes
    then Cohttp_lwt_unix.IO.read ic count
    else
      Lwt.fail
        (Failure
           (Printf.sprintf
              "websocket frame of %d bytes exceeds the %d byte cap"
              count
              max_frame_bytes))
  ;;
end

module Wire = Websocket.Make (Io)

exception HTTP_Error of string

let protocol_error message = Lwt.fail (Websocket.Protocol_error message)

(* RFC 6455 §4.1: a successful upgrade is HTTP/1.1 101, with an [Upgrade] token in
   [Connection] and an accept token derived from the client nonce. *)
let check_upgrade_response ~nonce response =
  let open Cohttp in
  let status = Response.status response in
  let headers = Response.headers response in
  if Code.is_error (Code.code_of_status status)
  then Lwt.fail (HTTP_Error (Code.string_of_status status))
  else if Response.version response <> `HTTP_1_1
  then protocol_error "unexpected HTTP version"
  else if status <> `Switching_protocols
  then protocol_error "server did not switch protocols"
  else (
    match Header.get headers "upgrade" with
    | Some value when String.lowercase_ascii value = "websocket" ->
      if not (Websocket.upgrade_present headers)
      then protocol_error "connection header does not include upgrade"
      else (
        let expected = Websocket.b64_encoded_sha1sum (nonce ^ Websocket.websocket_uuid) in
        match Header.get headers "sec-websocket-accept" with
        | Some accept when String.equal accept expected -> Lwt.return_unit
        | _ -> protocol_error "invalid sec-websocket-accept")
    | _ -> protocol_error "missing upgrade header")
;;

let upgrade_headers extra nonce =
  Cohttp.Header.add_list
    extra
    [ "Upgrade", "websocket"
    ; "Connection", "Upgrade"
    ; "Sec-WebSocket-Key", nonce
    ; "Sec-WebSocket-Version", "13"
    ]
;;

(* A close is best-effort: it must not replace the error that caused it. *)
let close_quietly channel =
  Lwt.catch (fun () -> Lwt_io.close channel) (fun _ -> Lwt.return_unit)
;;

let close_channels ic oc = Lwt.join [ close_quietly ic; close_quietly oc ]

let set_tcp_nodelay = function
  | Conduit_lwt_unix.TCP { fd; _ } -> Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY true
  | Conduit_lwt_unix.Domain_socket _ -> ()
;;

let establish ~ctx ~client ~uri ~headers ~nonce =
  Conduit_lwt_unix.connect ~ctx client
  >>= fun (flow, ic, oc) ->
  set_tcp_nodelay flow;
  let request = Cohttp.Request.make ~headers:(upgrade_headers headers nonce) uri in
  Lwt.catch
    (fun () ->
      Wire.Request.write (fun () -> Lwt.return_unit) request oc
      >>= fun () ->
      Wire.Response.read ic
      >>= function
      | `Ok response ->
        check_upgrade_response ~nonce response >>= fun () -> Lwt.return (ic, oc)
      | `Eof -> Lwt.fail End_of_file
      | `Invalid message -> Lwt.fail (Failure message))
    (fun exn -> close_channels ic oc >>= fun () -> Lwt.fail exn)
;;

type conn =
  { input : Lwt_io.input_channel
  ; output : Lwt_io.output_channel
  ; next_frame : unit -> Websocket.Frame.t Lwt.t
  ; send_frame : Websocket.Frame.t -> unit Lwt.t
  }

let read conn = conn.next_frame ()
let write conn frame = conn.send_frame frame
let close_transport conn = close_channels conn.input conn.output
let resolve_ctx () = Ctx.resolve ()

let connect
  ?(extra_headers = Cohttp.Header.init ())
  ?(random_string = Websocket.Rng.init ())
  ?(ctx = Ctx.resolve ())
  ?buf
  client
  uri
  =
  let nonce = Base64.encode_exn (random_string 16) in
  establish ~ctx ~client ~uri ~headers:extra_headers ~nonce
  >|= fun (ic, oc) ->
  let next_frame = Wire.make_read_frame ?buf ~mode:(Wire.Client random_string) ic oc in
  let next_frame () =
    Lwt.catch next_frame (fun exn -> close_channels ic oc >>= fun () -> Lwt.fail exn)
  in
  let out = Buffer.create 256 in
  let send_frame frame =
    Buffer.clear out;
    Lwt.wrap2 (Wire.write_frame_to_buf ~mode:(Wire.Client random_string)) out frame
    >>= fun () ->
    Lwt.catch
      (fun () -> Lwt_io.write oc (Buffer.contents out) >>= fun () -> Lwt_io.flush oc)
      (fun exn -> close_channels ic oc >>= fun () -> Lwt.fail exn)
  in
  { input = ic; output = oc; next_frame; send_frame }
;;
