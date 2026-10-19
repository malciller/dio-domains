(** In-tree HTTP/1.1 client over the vendored Lwt/OpenSSL stack.

    Supports the operations the exchange adapters use: GET/POST/PATCH/DELETE, request
    headers, string bodies, chunked and content-length responses, and incremental body
    streaming for Server-Sent Events. *)

open Lwt.Infix

module IO = struct
  type ic = Lwt_io.input_channel
  type oc = Lwt_io.output_channel

  let read ic count = Lwt_io.read ~count ic
  let write oc s = Lwt_io.write oc s
  let flush oc = Lwt_io.flush oc
  let close oc = Lwt_io.close oc
end

let close_quietly f = Lwt.catch f (fun _ -> Lwt.return_unit)

let close_both ic oc =
  close_quietly (fun () -> Lwt_io.close ic)
  >>= fun () -> close_quietly (fun () -> Lwt_io.close oc)
;;

let strip_cr s =
  let n = String.length s in
  if n > 0 && s.[n - 1] = '\r' then String.sub s 0 (n - 1) else s
;;

let read_status_line ic =
  Lwt_io.read_line_opt ic
  >>= fun line_opt ->
  match line_opt with
  | None -> Lwt.fail End_of_file
  | Some line ->
    let line = strip_cr line in
    (match String.split_on_char ' ' line with
     | version :: code :: _ ->
       let version = if version = "HTTP/1.0" then `HTTP_1_0 else `HTTP_1_1 in
       let code = int_of_string code in
       Lwt.return (version, Cohttp.Code.status_of_code code)
     | _ -> Lwt.fail (Failure ("malformed HTTP status line: " ^ line)))
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
          let k = String.sub line 0 i |> String.trim in
          let v = String.sub line (i + 1) (String.length line - i - 1) |> String.trim in
          loop ((k, v) :: acc))
  in
  loop []
;;

let read_exact ic n =
  let buf = Buffer.create n in
  let rec loop remaining =
    if remaining <= 0
    then Lwt.return (Buffer.contents buf)
    else
      Lwt_io.read ~count:remaining ic
      >>= fun chunk ->
      let got = String.length chunk in
      if got = 0
      then Lwt.return (Buffer.contents buf)
      else (
        Buffer.add_string buf chunk;
        loop (remaining - got))
  in
  loop n
;;

let read_until_eof ic push =
  let rec loop () =
    Lwt_io.read ~count:65536 ic
    >>= fun chunk ->
    if String.length chunk = 0
    then Lwt.return_unit
    else (
      push chunk;
      loop ())
  in
  loop ()
;;

let read_chunked ic push =
  let rec read_chunk () =
    Lwt_io.read_line_opt ic
    >>= fun line_opt ->
    match line_opt with
    | None -> Lwt.return_unit
    | Some line ->
      let line = strip_cr line in
      let size_part = String.split_on_char ';' line |> List.hd |> String.trim in
      let size = int_of_string ("0x" ^ size_part) in
      if size = 0
      then (
        (* Trailers, terminated by an empty line. *)
        let rec trailers () =
          Lwt_io.read_line_opt ic
          >>= fun l_opt ->
          match l_opt with
          | None -> Lwt.return_unit
          | Some l -> if strip_cr l = "" then Lwt.return_unit else trailers ()
        in
        trailers ())
      else
        read_exact ic size
        >>= fun data ->
        push data;
        (* Consume the CRLF after the chunk. *)
        Lwt_io.read_line_opt ic >>= fun _ -> read_chunk ()
  in
  read_chunk ()
;;

let make_body_stream ic oc headers ~is_head ~status_code : Cohttp_lwt.Body.t Lwt.t =
  let no_body = is_head || status_code < 200 || status_code = 204 || status_code = 304 in
  if no_body
  then close_both ic oc >|= fun () -> `String ""
  else (
    let stream, push = Lwt_stream.create () in
    let finish () =
      push None;
      close_both ic oc
    in
    let transfer_encoding =
      Cohttp.Header.get headers "transfer-encoding" |> Option.map String.lowercase_ascii
    in
    let content_length =
      Cohttp.Header.get headers "content-length"
      |> Option.map String.trim
      |> Option.map int_of_string_opt
      |> Option.join
    in
    let push_chunk s = push (Some s) in
    let reader () =
      match transfer_encoding with
      | Some te when String.length te >= 7 ->
        let has_chunked =
          let parts = String.split_on_char ',' te in
          List.exists (fun p -> String.trim p = "chunked") parts
        in
        if has_chunked
        then read_chunked ic push_chunk
        else (
          match content_length with
          | Some n ->
            read_exact ic n
            >>= fun data ->
            if data <> "" then push_chunk data;
            Lwt.return_unit
          | None -> read_until_eof ic push_chunk)
      | _ ->
        (match content_length with
         | Some n ->
           read_exact ic n
           >>= fun data ->
           if data <> "" then push_chunk data;
           Lwt.return_unit
         | None -> read_until_eof ic push_chunk)
    in
    Lwt.async (fun () ->
      Lwt.catch (fun () -> reader () >>= finish) (fun _exn -> finish ()));
    Lwt.return (`Stream stream))
;;

let request
  ~meth
  ?(headers = Cohttp.Header.init ())
  ?(body = Cohttp_lwt.Body.of_string "")
  uri
  =
  let scheme =
    Uri.scheme uri |> Option.map String.lowercase_ascii |> Option.value ~default:"http"
  in
  let is_tls = scheme = "https" || scheme = "wss" in
  let host = Uri.host uri |> Option.value ~default:"" in
  let port = Uri.port uri |> Option.value ~default:(if is_tls then 443 else 80) in
  let host_header =
    if (is_tls && port = 443) || ((not is_tls) && port = 80)
    then host
    else Printf.sprintf "%s:%d" host port
  in
  Lwt_unix.getaddrinfo host (string_of_int port) [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ]
  >>= fun addresses ->
  let ip =
    match addresses with
    | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ -> Ipaddr_unix.of_inet_addr addr
    | _ -> failwith (Printf.sprintf "cannot resolve %s:%d" host port)
  in
  let client : Conduit_lwt_unix.client =
    if is_tls then `TLS (`Hostname host, `IP ip, `Port port) else `TCP (`IP ip, `Port port)
  in
  Conduit_lwt_unix.connect ~ctx:Conduit_lwt_unix.default_ctx client
  >>= fun (_flow, ic, oc) ->
  Lwt.catch
    (fun () ->
      Cohttp_lwt.Body.to_string body
      >>= fun body_str ->
      let headers =
        headers
        |> fun h ->
        Cohttp.Header.set h "Host" host_header
        |> fun h ->
        Cohttp.Header.set h "Connection" "close"
        |> fun h ->
        Cohttp.Header.set h "Content-Length" (string_of_int (String.length body_str))
      in
      let request_line =
        Printf.sprintf
          "%s %s HTTP/1.1\r\n"
          (Cohttp.Request.string_of_meth meth)
          (Uri.path_and_query uri)
      in
      Lwt_io.write oc request_line
      >>= fun () ->
      Lwt_io.write oc (Cohttp.Header.to_string headers)
      >>= fun () ->
      Lwt_io.write oc "\r\n"
      >>= fun () ->
      Lwt_io.write oc body_str
      >>= fun () ->
      Lwt_io.flush oc
      >>= fun () ->
      read_status_line ic
      >>= fun (version, status) ->
      read_headers ic
      >>= fun response_headers ->
      let response = { Cohttp.Response.status; headers = response_headers; version } in
      make_body_stream
        ic
        oc
        response_headers
        ~is_head:(meth = `HEAD)
        ~status_code:(Cohttp.Code.code_of_status status)
      >>= fun body -> Lwt.return (response, body))
    (fun exn -> close_both ic oc >>= fun () -> Lwt.fail exn)
;;

module Client = struct
  let get ?headers uri = request ~meth:`GET ?headers uri
  let post ?headers ?body uri = request ~meth:`POST ?headers ?body uri
  let patch ?headers ?body uri = request ~meth:`PATCH ?headers ?body uri
  let delete ?headers uri = request ~meth:`DELETE ?headers uri
end
