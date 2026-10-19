open Lwt.Infix

let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let strip_cr s =
  let n = String.length s in
  if n > 0 && s.[n - 1] = '\r' then String.sub s 0 (n - 1) else s
;;

let read_headers ic =
  let rec loop acc =
    Lwt_io.read_line_opt ic
    >>= fun line_opt ->
    match line_opt with
    | None -> Lwt.return (List.rev acc)
    | Some line ->
      let line = strip_cr line in
      if line = ""
      then Lwt.return (List.rev acc)
      else (
        match String.index_opt line ':' with
        | None -> loop acc
        | Some i ->
          let k = String.sub line 0 i |> String.trim |> String.lowercase_ascii in
          let v = String.sub line (i + 1) (String.length line - i - 1) |> String.trim in
          loop ((k, v) :: acc))
  in
  loop []
;;

let server_text_header payload =
  let len = String.length payload in
  let hdr = Buffer.create 10 in
  Buffer.add_char hdr (Char.chr 0x81);
  if len < 126
  then Buffer.add_char hdr (Char.chr len)
  else if len <= 0xffff
  then (
    Buffer.add_char hdr (Char.chr 126);
    Buffer.add_char hdr (Char.chr ((len lsr 8) land 0xff));
    Buffer.add_char hdr (Char.chr (len land 0xff)))
  else (
    Buffer.add_char hdr (Char.chr 127);
    for k = 0 to 7 do
      Buffer.add_char
        hdr
        (Char.chr
           (Int64.to_int
              (Int64.logand
                 (Int64.shift_right_logical (Int64.of_int len) (56 - (8 * k)))
                 0xffL)))
    done);
  Buffer.contents hdr
;;

let read_exactly ic n =
  let buf = Bytes.create n in
  let rec loop offset =
    if offset = n
    then Lwt.return (Bytes.unsafe_to_string buf)
    else Lwt_io.read_into_exactly ic buf offset (n - offset) >>= fun () -> loop n
  in
  loop 0
;;

let unmask key payload =
  String.mapi (fun i c -> Char.chr (Char.code c lxor Char.code key.[i mod 4])) payload
;;

let read_client_frame ic =
  read_exactly ic 2
  >>= fun header ->
  let b0 = Char.code header.[0] in
  let b1 = Char.code header.[1] in
  let opcode = b0 land 0xf in
  let masked = b1 land 0x80 <> 0 in
  let len7 = b1 land 0x7f in
  (if len7 < 126
   then Lwt.return len7
   else if len7 = 126
   then read_exactly ic 2 >|= fun s -> (Char.code s.[0] lsl 8) lor Char.code s.[1]
   else Lwt.fail (Failure "test client frame too large"))
  >>= fun len ->
  (if masked then read_exactly ic 4 else Lwt.return "")
  >>= fun key ->
  read_exactly ic len
  >|= fun payload ->
  let payload = if masked then unmask key payload else payload in
  opcode, masked, payload
;;

let serve_once ~payload () =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
  >>= fun () ->
  Lwt_unix.listen sock 1;
  let port =
    match Lwt_unix.getsockname sock with
    | Unix.ADDR_INET (_, port) -> port
    | _ -> assert false
  in
  let server () =
    Lwt_unix.accept sock
    >>= fun (fd, _) ->
    let ic = Lwt_io.of_fd ~mode:Lwt_io.Input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.Output fd in
    read_headers ic
    >>= fun headers ->
    let key = List.assoc_opt "sec-websocket-key" headers |> Option.value ~default:"" in
    let accept =
      Base64.encode_exn
        Digestif.SHA1.(digest_string (key ^ Websocket.websocket_uuid) |> to_raw_string)
    in
    let response =
      Printf.sprintf
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: %s\r\n\
         \r\n"
        accept
    in
    Lwt_io.write oc response
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    (* Deliberately split the frame across multiple writes so the client sees short reads
       and must reassemble the header and payload. *)
    let hdr = server_text_header payload in
    let body = payload in
    let half = String.length body / 2 in
    let first = String.sub body 0 half in
    let second = String.sub body half (String.length body - half) in
    Lwt_io.write oc hdr
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    Lwt_unix.sleep 0.01
    >>= fun () ->
    Lwt_io.write oc first
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    Lwt_unix.sleep 0.01
    >>= fun () ->
    Lwt_io.write oc second
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    Lwt_unix.sleep 0.05
    >>= fun () ->
    Lwt_io.close ic >>= fun () -> Lwt_io.close oc >>= fun () -> Lwt_unix.close sock
  in
  Lwt.async (fun () -> Lwt.catch server (fun _ -> Lwt.return_unit));
  Lwt.return port
;;

(* Accept one connection, complete the handshake, read one client frame and echo its
   payload back as a text frame. This exercises the write path, including client-side
   masking. *)
let echo_once ~payload:_ () =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0))
  >>= fun () ->
  Lwt_unix.listen sock 1;
  let port =
    match Lwt_unix.getsockname sock with
    | Unix.ADDR_INET (_, port) -> port
    | _ -> assert false
  in
  let server () =
    Lwt_unix.accept sock
    >>= fun (fd, _) ->
    let ic = Lwt_io.of_fd ~mode:Lwt_io.Input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.Output fd in
    read_headers ic
    >>= fun headers ->
    let key = List.assoc_opt "sec-websocket-key" headers |> Option.value ~default:"" in
    let accept =
      Base64.encode_exn
        Digestif.SHA1.(digest_string (key ^ Websocket.websocket_uuid) |> to_raw_string)
    in
    let response =
      Printf.sprintf
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: %s\r\n\
         \r\n"
        accept
    in
    Lwt_io.write oc response
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    read_client_frame ic
    >>= fun (opcode, masked, received) ->
    let reply = if masked && opcode = 1 then received else "invalid-client-frame" in
    let hdr = server_text_header reply in
    Lwt_io.write oc hdr
    >>= fun () ->
    Lwt_io.write oc reply
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    Lwt_unix.sleep 0.05
    >>= fun () ->
    Lwt_io.close ic >>= fun () -> Lwt_io.close oc >>= fun () -> Lwt_unix.close sock
  in
  Lwt.async (fun () -> Lwt.catch server (fun _ -> Lwt.return_unit));
  Lwt.return port
;;

let read_one ~payload () =
  serve_once ~payload ()
  >>= fun port ->
  let uri = Uri.of_string (Printf.sprintf "ws://127.0.0.1:%d/" port) in
  let client : Conduit_lwt_unix.client =
    `TCP (`IP (Ipaddr.of_string "127.0.0.1"), `Port port)
  in
  Lwt_unix.with_timeout 5.0 (fun () ->
    Ws_lwt.connect ~ctx:(Ws_lwt.resolve_ctx ()) client uri)
  >>= fun conn ->
  Lwt_unix.with_timeout 5.0 (fun () -> Ws_lwt.read conn)
  >>= fun frame ->
  check "opcode" "text" (Websocket.Frame.Opcode.to_string frame.Websocket.Frame.opcode);
  check "content" payload frame.Websocket.Frame.content;
  Ws_lwt.close_transport conn
;;

let write_one ~payload () =
  echo_once ~payload ()
  >>= fun port ->
  let uri = Uri.of_string (Printf.sprintf "ws://127.0.0.1:%d/" port) in
  let client : Conduit_lwt_unix.client =
    `TCP (`IP (Ipaddr.of_string "127.0.0.1"), `Port port)
  in
  Lwt_unix.with_timeout 5.0 (fun () ->
    Ws_lwt.connect ~ctx:(Ws_lwt.resolve_ctx ()) client uri)
  >>= fun conn ->
  Lwt_unix.with_timeout 5.0 (fun () ->
    Ws_lwt.write conn (Websocket.Frame.create ~content:payload ()))
  >>= fun () ->
  Lwt_unix.with_timeout 5.0 (fun () -> Ws_lwt.read conn)
  >>= fun frame ->
  check
    "echo opcode"
    "text"
    (Websocket.Frame.Opcode.to_string frame.Websocket.Frame.opcode);
  check "echo content" payload frame.Websocket.Frame.content;
  Ws_lwt.close_transport conn
;;

let () =
  Lwt_main.run
    (read_one ~payload:"hello" ()
     >>= fun () ->
     read_one ~payload:(String.make 200_000 'x') ()
     >>= fun () -> write_one ~payload:"client-frame" ());
  if !failures > 0 then exit 1
;;
