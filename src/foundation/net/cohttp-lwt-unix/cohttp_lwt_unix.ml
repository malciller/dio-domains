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

(* [on_finish] owns connection disposal: the pool returns the channels for reuse, or
   closes them when the response is not reusable. Every exit path must call it exactly
   once. *)
let make_body_stream ic headers ~is_head ~status_code ~reusable ~on_finish
  : Cohttp_lwt.Body.t Lwt.t
  =
  let no_body = is_head || status_code < 200 || status_code = 204 || status_code = 304 in
  if no_body
  then (
    on_finish ~reusable;
    Lwt.return (`String ""))
  else (
    let stream, push = Lwt_stream.create () in
    let finished = ref false in
    let finish ~reusable =
      if not !finished
      then (
        finished := true;
        push None;
        on_finish ~reusable)
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
      Lwt.catch
        (fun () -> reader () >|= fun () -> finish ~reusable)
        (fun _exn ->
          finish ~reusable:false;
          Lwt.return_unit));
    Lwt.return (`Stream stream))
;;

(* ---- keep-alive pool ------------------------------------------------------

   One idle connection list per authority. All access happens on the Lwt thread (the same
   one that owns the channels), so no lock is needed; channels are not domain-safe and
   never cross domains here.

   Policy:
   - reuse an idle connection only while it is younger than [idle_timeout_s];
   - at most [max_idle_per_key] idle connections per authority;
   - a reuse attempt that fails before any response byte arrives is retried exactly once
     on a fresh connection, which cannot duplicate a request the venue already accepted. *)

let idle_timeout_s = 30.0
let max_idle_per_key = 4

type pooled =
  { p_ic : Lwt_io.input_channel
  ; p_oc : Lwt_io.output_channel
  ; mutable p_busy : bool
  ; mutable p_last_used : float
  }

let pool : (string, pooled list ref) Hashtbl.t = Hashtbl.create 16
let pool_key ~is_tls ~host ~port = Printf.sprintf "%s|%d|%b" host port is_tls
let discard conn = Lwt.async (fun () -> close_both conn.p_ic conn.p_oc)

let take key =
  match Hashtbl.find_opt pool key with
  | None -> None
  | Some cell ->
    let now = Unix.gettimeofday () in
    let rec pick = function
      | [] -> None
      | conn :: rest ->
        if conn.p_busy
        then pick rest
        else if now -. conn.p_last_used > idle_timeout_s
        then (
          discard conn;
          cell := rest;
          pick rest)
        else (
          cell := rest;
          conn.p_busy <- true;
          Some conn)
    in
    pick !cell
;;

let give_back key conn ~reusable =
  if not reusable
  then (
    conn.p_busy <- false;
    discard conn)
  else (
    conn.p_busy <- false;
    conn.p_last_used <- Unix.gettimeofday ();
    let cell =
      match Hashtbl.find_opt pool key with
      | Some cell -> cell
      | None ->
        let cell = ref [] in
        Hashtbl.replace pool key cell;
        cell
    in
    cell := conn :: !cell;
    let overflow =
      List.length (List.filter (fun c -> not c.p_busy) !cell) - max_idle_per_key
    in
    if overflow > 0
    then (
      let seen = ref 0 in
      cell
      := List.filter
           (fun c ->
             if c.p_busy
             then true
             else (
               incr seen;
               if !seen > max_idle_per_key
               then (
                 discard c;
                 false)
               else true))
           !cell))
;;

let request
  ?connect_host
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
  (* Dial target may differ from the URL host: a pinned egress relay answers for the real
     host, so TLS SNI and [Host] stay on [host] while DNS resolves [connect_host]. *)
  let dial_host = Option.value connect_host ~default:host in
  let key = pool_key ~is_tls ~host ~port in
  let connect () =
    Lwt_unix.getaddrinfo
      dial_host
      (string_of_int port)
      [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ]
    >>= fun addresses ->
    let ip =
      match addresses with
      | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ ->
        Ipaddr_unix.of_inet_addr addr
      | _ -> failwith (Printf.sprintf "cannot resolve %s:%d" dial_host port)
    in
    let client : Conduit_lwt_unix.client =
      if is_tls
      then `TLS (`Hostname host, `IP ip, `Port port)
      else `TCP (`IP ip, `Port port)
    in
    Conduit_lwt_unix.connect ~ctx:Conduit_lwt_unix.default_ctx client
    >|= fun (_flow, ic, oc) ->
    { p_ic = ic; p_oc = oc; p_busy = true; p_last_used = Unix.gettimeofday () }
  in
  Cohttp_lwt.Body.to_string body
  >>= fun body_str ->
  let request_headers =
    headers
    |> fun h ->
    Cohttp.Header.set h "Host" host_header
    |> fun h ->
    Cohttp.Header.set h "Connection" "keep-alive"
    |> fun h ->
    Cohttp.Header.set h "Content-Length" (string_of_int (String.length body_str))
  in
  let request_line =
    Printf.sprintf
      "%s %s HTTP/1.1\r\n"
      (Cohttp.Request.string_of_meth meth)
      (Uri.path_and_query uri)
  in
  let send conn =
    Lwt_io.write conn.p_oc request_line
    >>= fun () ->
    Lwt_io.write conn.p_oc (Cohttp.Header.to_string request_headers)
    >>= fun () ->
    Lwt_io.write conn.p_oc "\r\n"
    >>= fun () -> Lwt_io.write conn.p_oc body_str >>= fun () -> Lwt_io.flush conn.p_oc
  in
  let retried = ref false in
  let rec exchange conn ~from_pool =
    let got_response = ref false in
    Lwt.catch
      (fun () ->
        send conn
        >>= fun () ->
        read_status_line conn.p_ic
        >>= fun (version, status) ->
        got_response := true;
        read_headers conn.p_ic
        >>= fun response_headers ->
        let no_body =
          meth = `HEAD
          || Cohttp.Code.code_of_status status < 200
          || Cohttp.Code.code_of_status status = 204
          || Cohttp.Code.code_of_status status = 304
        in
        let has_framing =
          no_body
          || (match Cohttp.Header.get response_headers "transfer-encoding" with
              | Some te ->
                let te = String.lowercase_ascii te in
                String.split_on_char ',' te
                |> List.exists (fun p -> String.trim p = "chunked")
              | None -> false)
          || Cohttp.Header.get response_headers "content-length" <> None
        in
        let connection_close =
          match Cohttp.Header.get response_headers "connection" with
          | Some value -> String.lowercase_ascii (String.trim value) = "close"
          | None -> version = `HTTP_1_0
        in
        let reusable = (not connection_close) && has_framing in
        let response = { Cohttp.Response.status; headers = response_headers; version } in
        make_body_stream
          conn.p_ic
          response_headers
          ~is_head:(meth = `HEAD)
          ~status_code:(Cohttp.Code.code_of_status status)
          ~reusable
          ~on_finish:(fun ~reusable -> give_back key conn ~reusable)
        >|= fun body -> response, body)
      (fun exn ->
        conn.p_busy <- false;
        discard conn;
        (* Only a pooled connection that failed before any response byte can be retried: a
           fresh dial cannot be stale, and a request that produced a status line may
           already have been accepted. *)
        if from_pool && (not !got_response) && not !retried
        then (
          retried := true;
          connect () >>= fun fresh -> exchange fresh ~from_pool:false)
        else Lwt.fail exn)
  in
  match take key with
  | Some conn -> exchange conn ~from_pool:true
  | None -> connect () >>= fun conn -> exchange conn ~from_pool:false
;;

module Client = struct
  let get ?headers uri = request ~meth:`GET ?headers uri

  let post ?connect_host ?headers ?body uri =
    request ?connect_host ~meth:`POST ?headers ?body uri
  ;;

  let patch ?headers ?body uri = request ~meth:`PATCH ?headers ?body uri
  let delete ?headers uri = request ~meth:`DELETE ?headers uri
end
