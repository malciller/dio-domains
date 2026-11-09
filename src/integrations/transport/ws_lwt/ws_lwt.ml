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

  (* Frame reads copy straight into the destination buffer; the old path built an
     intermediate [Buffer] and then a string. *)
  let read_into ic buf off len =
    if len <= max_frame_bytes
    then Lwt_io.read_into ic buf off len
    else
      Lwt.fail
        (Failure
           (Printf.sprintf
              "websocket frame of %d bytes exceeds the %d byte cap"
              len
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

(* Drops buffered output instead of flushing it: [close] flushes first, which blocks
   forever on a peer that stopped draining. The only teardown that cannot hang. *)
let abort_transport conn = Lwt.join [ Lwt_io.abort conn.output; Lwt_io.abort conn.input ]
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
    (* [to_bytes] + [write_from_exactly] avoids the string round-trip [Lwt_io.write] adds. *)
    Lwt.catch
      (fun () ->
        Lwt_io.write_from_exactly oc (Buffer.to_bytes out) 0 (Buffer.length out)
        >>= fun () -> Lwt_io.flush oc)
      (fun exn -> close_channels ic oc >>= fun () -> Lwt.fail exn)
  in
  { input = ic; output = oc; next_frame; send_frame }
;;

(* [getaddrinfo] usually yields several A records and only one may be reachable; dialing
   just the first turns a rotated or blackholed address into an endless reconnect timeout.
   Each address gets a bounded slice of one budget, and failures name every one tried. *)
let connect_failover
  ?(ctx = resolve_ctx ())
  ?(timeout_s = 20.0)
  ?dial_host
  ~tls_host
  ~port
  uri
  =
  let dial_host = Option.value dial_host ~default:tls_host in
  let deadline = Unix.gettimeofday () +. timeout_s in
  let failf fmt = Printf.ksprintf (fun msg -> Lwt.fail (Failure msg)) fmt in
  Lwt.catch
    (fun () ->
      Lwt_unix.with_timeout timeout_s (fun () ->
        Lwt_unix.getaddrinfo
          dial_host
          (string_of_int port)
          [ Unix.AI_FAMILY Unix.PF_INET ]))
    (function
      | Lwt_unix.Timeout ->
        failf "connect %s:%d: resolution timed out after %.0fs" dial_host port timeout_s
      | exn -> Lwt.fail exn)
  >>= fun addresses ->
  let ips =
    List.filter_map
      (fun { Unix.ai_addr; _ } ->
        match ai_addr with
        | Unix.ADDR_INET (addr, _) -> Some addr
        | Unix.ADDR_UNIX _ -> None)
      addresses
    |> List.sort_uniq compare
  in
  let total = List.length ips in
  if total = 0
  then failf "connect %s:%d: no IPv4 address" dial_host port
  else (
    (* A reachable address completes TCP+TLS in well under a second; the cap stops one
       blackholed address from consuming the whole budget. *)
    let slice =
      if total = 1 then timeout_s else Float.min 5.0 (timeout_s /. float_of_int total)
    in
    let rec try_ips tried = function
      | [] ->
        failf
          "connect %s:%d failed on all %d address(es) [%s]"
          dial_host
          port
          total
          (String.concat ", " (List.rev tried))
      | ip :: rest ->
        let budget = Float.min slice (deadline -. Unix.gettimeofday ()) in
        if budget <= 0.0
        then
          failf
            "connect %s:%d: %.0fs budget exhausted, untried [%s]"
            dial_host
            port
            timeout_s
            (String.concat
               ", "
               (List.map (fun a -> Unix.string_of_inet_addr a) (ip :: rest)))
        else (
          let client =
            `TLS (`Hostname tls_host, `IP (Ipaddr_unix.of_inet_addr ip), `Port port)
          in
          Lwt.catch
            (fun () ->
              Lwt_unix.with_timeout budget (fun () -> connect ~ctx client uri)
              >>= fun conn -> Lwt.return (conn, Unix.string_of_inet_addr ip))
            (fun exn ->
              try_ips
                (Printf.sprintf
                   "%s: %s"
                   (Unix.string_of_inet_addr ip)
                   (Printexc.to_string exn)
                 :: tried)
                rest))
    in
    try_ips [] ips)
;;
