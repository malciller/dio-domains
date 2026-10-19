open Lwt.Infix

let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let serve_once response =
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
    let rec drain () =
      Lwt_io.read_line_opt ic
      >>= function
      | None | Some "" -> Lwt.return_unit
      | Some _ -> drain ()
    in
    drain ()
    >>= fun () ->
    Lwt_io.write oc response
    >>= fun () ->
    Lwt_io.flush oc
    >>= fun () ->
    Lwt_io.close ic >>= fun () -> Lwt_io.close oc >>= fun () -> Lwt_unix.close sock
  in
  Lwt.async (fun () -> Lwt.catch server (fun _ -> Lwt.return_unit));
  Lwt.return port
;;

let get port =
  let uri = Uri.of_string (Printf.sprintf "http://127.0.0.1:%d/test" port) in
  Cohttp_lwt_unix.Client.get uri
  >>= fun (resp, body) ->
  Cohttp_lwt.Body.to_string body
  >>= fun body ->
  Lwt.return (Cohttp.Code.code_of_status (Cohttp.Response.status resp), body)
;;

let test_content_length () =
  serve_once "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
  >>= fun port ->
  Lwt_unix.with_timeout 5.0 (fun () -> get port)
  >>= fun (code, body) ->
  check "content-length status" "200" (string_of_int code);
  check "content-length body" "hello" body;
  Lwt.return_unit
;;

let test_chunked () =
  serve_once
    "HTTP/1.1 200 OK\r\n\
     Transfer-Encoding: chunked\r\n\
     Connection: close\r\n\
     \r\n\
     5\r\n\
     hello\r\n\
     6\r\n\
    \ world\r\n\
     0\r\n\
     \r\n"
  >>= fun port ->
  Lwt_unix.with_timeout 5.0 (fun () -> get port)
  >>= fun (code, body) ->
  check "chunked status" "200" (string_of_int code);
  check "chunked body" "hello world" body;
  Lwt.return_unit
;;

let () =
  Lwt_main.run (test_content_length () >>= fun () -> test_chunked ());
  if !failures > 0 then exit 1
;;
