(** In-tree RFC 6455 WebSocket client framing.

    The module keeps the API of the [websocket] package that [ws_lwt.ml] uses: [Make(IO)],
    [Frame], [Rng], [b64_encoded_sha1sum], [upgrade_present], and [websocket_uuid]. Only
    client connections are implemented; the engine has no WebSocket server. *)

open Lwt.Infix

let websocket_uuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

let b64_encoded_sha1sum s =
  Base64.encode_exn Digestif.SHA1.(digest_string s |> to_raw_string)
;;

module Rng = struct
  let init ?(state = Random.get_state ()) () len =
    String.init len (fun _ -> Char.chr (Random.State.bits state land 0xff))
  ;;
end

module Frame = struct
  module Opcode = struct
    type t =
      | Continuation
      | Text
      | Binary
      | Close
      | Ping
      | Pong
      | Ctrl of int
      | Nonctrl of int

    let to_string = function
      | Continuation -> "continuation"
      | Text -> "text"
      | Binary -> "binary"
      | Close -> "close"
      | Ping -> "ping"
      | Pong -> "pong"
      | Ctrl i -> "ctrl " ^ string_of_int i
      | Nonctrl i -> "nonctrl " ^ string_of_int i
    ;;

    let pp ppf t = Format.fprintf ppf "%s" (to_string t)

    let of_enum = function
      | i when i < 0 || i > 0xf -> invalid_arg "Frame.Opcode.of_enum"
      | 0 -> Continuation
      | 1 -> Text
      | 2 -> Binary
      | 8 -> Close
      | 9 -> Ping
      | 10 -> Pong
      | i when i < 8 -> Nonctrl i
      | i -> Ctrl i
    ;;

    let to_enum = function
      | Continuation -> 0
      | Text -> 1
      | Binary -> 2
      | Close -> 8
      | Ping -> 9
      | Pong -> 10
      | Ctrl i -> i
      | Nonctrl i -> i
    ;;

    let is_ctrl opcode = to_enum opcode > 7
  end

  type t =
    { opcode : Opcode.t
    ; extension : int
    ; final : bool
    ; content : string
    }

  let create ?(opcode = Opcode.Text) ?(extension = 0) ?(final = true) ?(content = "") () =
    { opcode; extension; final; content }
  ;;
end

let upgrade_present headers =
  match Cohttp.Header.get headers "connection" with
  | None -> false
  | Some value ->
    String.split_on_char ',' value
    |> List.exists (fun token -> String.lowercase_ascii (String.trim token) = "upgrade")
;;

exception Protocol_error of string

module type IO = sig
  type ic
  type oc

  val read : ic -> int -> string Lwt.t
  val write : oc -> string -> unit Lwt.t
  val flush : oc -> unit Lwt.t
end

module Make (IO : IO) = struct
  type mode =
    | Client of (int -> string)
    | Server

  let strip_cr s =
    let n = String.length s in
    if n > 0 && s.[n - 1] = '\r' then String.sub s 0 (n - 1) else s
  ;;

  let read_line ic =
    let buf = Buffer.create 64 in
    let rec loop () =
      Lwt.catch
        (fun () ->
          IO.read ic 1
          >>= fun s ->
          if s = ""
          then Lwt.fail End_of_file
          else if s.[0] = '\n'
          then Lwt.return (strip_cr (Buffer.contents buf))
          else (
            Buffer.add_char buf s.[0];
            loop ()))
        (function
          | End_of_file ->
            if Buffer.length buf = 0
            then Lwt.fail End_of_file
            else Lwt.return (strip_cr (Buffer.contents buf))
          | exn -> Lwt.fail exn)
    in
    loop ()
  ;;

  let read_headers ic =
    let rec loop acc =
      Lwt.catch
        (fun () -> read_line ic >|= fun l -> Some l)
        (function
          | End_of_file -> Lwt.return None
          | exn -> Lwt.fail exn)
      >>= fun line_opt ->
      match line_opt with
      | None -> Lwt.return (List.rev acc)
      | Some line ->
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

  let host_header uri =
    let scheme = Uri.scheme uri |> Option.map String.lowercase_ascii in
    let tls =
      match scheme with
      | Some "https" | Some "wss" -> true
      | _ -> false
    in
    let host = Uri.host uri |> Option.value ~default:"" in
    let port = Uri.port uri |> Option.value ~default:(if tls then 443 else 80) in
    if (tls && port = 443) || ((not tls) && port = 80)
    then host
    else Printf.sprintf "%s:%d" host port
  ;;

  module Request = struct
    let write body_writer req oc =
      let headers = Cohttp.Request.headers req in
      let headers =
        match Cohttp.Header.get headers "host" with
        | Some _ -> headers
        | None -> Cohttp.Header.add headers "Host" (host_header (Cohttp.Request.uri req))
      in
      let request_line =
        Printf.sprintf
          "%s %s HTTP/1.1\r\n"
          (Cohttp.Request.string_of_meth (Cohttp.Request.meth req))
          (Uri.path_and_query (Cohttp.Request.uri req))
      in
      IO.write oc request_line
      >>= fun () ->
      IO.write oc (Cohttp.Header.to_string headers)
      >>= fun () ->
      IO.write oc "\r\n" >>= fun () -> body_writer () >>= fun () -> IO.flush oc
    ;;
  end

  module Response = struct
    let read ic =
      Lwt.catch
        (fun () ->
          read_line ic
          >>= fun status_line ->
          let version, code =
            match String.split_on_char ' ' (strip_cr status_line) with
            | version :: code :: _ ->
              let version = if version = "HTTP/1.0" then `HTTP_1_0 else `HTTP_1_1 in
              version, int_of_string code
            | _ -> failwith ("malformed HTTP status line: " ^ status_line)
          in
          read_headers ic
          >>= fun headers ->
          Lwt.return
            (`Ok
              { Cohttp.Response.status = Cohttp.Code.status_of_code code
              ; headers
              ; version
              }))
        (function
          | End_of_file -> Lwt.return `Eof
          | Failure s -> Lwt.return (`Invalid s)
          | exn -> Lwt.fail exn)
    ;;
  end

  let int_of_be16 s = (Char.code s.[0] lsl 8) lor Char.code s.[1]

  let int_of_be64 s =
    let byte i = Int64.of_int (Char.code s.[i]) in
    let v =
      Int64.logor
        (Int64.shift_left (byte 0) 56)
        (Int64.logor
           (Int64.shift_left (byte 1) 48)
           (Int64.logor
              (Int64.shift_left (byte 2) 40)
              (Int64.logor
                 (Int64.shift_left (byte 3) 32)
                 (Int64.logor
                    (Int64.shift_left (byte 4) 24)
                    (Int64.logor
                       (Int64.shift_left (byte 5) 16)
                       (Int64.logor (Int64.shift_left (byte 6) 8) (byte 7)))))))
    in
    if Int64.compare v (Int64.of_int max_int) > 0
    then failwith "websocket frame too large"
    else Int64.to_int v
  ;;

  let unmask key s =
    let n = String.length s in
    let b = Bytes.of_string s in
    for i = 0 to n - 1 do
      Bytes.set b i (Char.chr (Char.code (Bytes.get b i) lxor Char.code key.[i mod 4]))
    done;
    Bytes.unsafe_to_string b
  ;;

  let read_exactly ic n =
    if n <= 0
    then Lwt.return ""
    else (
      let buf = Buffer.create n in
      let rec loop remaining =
        if remaining = 0
        then Lwt.return (Buffer.contents buf)
        else
          IO.read ic remaining
          >>= fun chunk ->
          let got = String.length chunk in
          if got = 0
          then Lwt.fail End_of_file
          else (
            Buffer.add_string buf chunk;
            loop (remaining - got))
      in
      loop n)
  ;;

  let make_read_frame ?buf:_ ~mode:_ ic _oc () =
    read_exactly ic 2
    >>= fun hdr ->
    if String.length hdr < 2
    then Lwt.fail End_of_file
    else (
      let b0 = Char.code hdr.[0] in
      let b1 = Char.code hdr.[1] in
      let final = b0 land 0x80 <> 0 in
      let extension = (b0 lsr 4) land 0x7 in
      let opcode = Frame.Opcode.of_enum (b0 land 0xf) in
      let masked = b1 land 0x80 <> 0 in
      let len7 = b1 land 0x7f in
      let read_len () =
        if len7 < 126
        then Lwt.return len7
        else if len7 = 126
        then read_exactly ic 2 >|= int_of_be16
        else read_exactly ic 8 >|= int_of_be64
      in
      read_len ()
      >>= fun payload_len ->
      let read_mask () =
        if masked then read_exactly ic 4 >|= fun k -> Some k else Lwt.return None
      in
      read_mask ()
      >>= fun key ->
      let read_payload () =
        if payload_len = 0 then Lwt.return "" else read_exactly ic payload_len
      in
      read_payload ()
      >|= fun content ->
      let content =
        match key with
        | Some key -> unmask key content
        | None -> content
      in
      { Frame.opcode; extension; final; content })
  ;;

  let write_frame_to_buf ~mode buf frame =
    let masked =
      match mode with
      | Client _ -> true
      | Server -> false
    in
    let b0 =
      (if frame.Frame.final then 0x80 else 0)
      lor ((frame.Frame.extension land 0x7) lsl 4)
      lor Frame.Opcode.to_enum frame.Frame.opcode
    in
    Buffer.add_char buf (Char.chr b0);
    let len = String.length frame.Frame.content in
    let mask_bit = if masked then 0x80 else 0 in
    if len < 126
    then Buffer.add_char buf (Char.chr (mask_bit lor len))
    else if len <= 0xffff
    then (
      Buffer.add_char buf (Char.chr (mask_bit lor 126));
      Buffer.add_char buf (Char.chr ((len lsr 8) land 0xff));
      Buffer.add_char buf (Char.chr (len land 0xff)))
    else (
      Buffer.add_char buf (Char.chr (mask_bit lor 127));
      for k = 0 to 7 do
        Buffer.add_char
          buf
          (Char.chr
             (Int64.to_int (Int64.shift_right_logical (Int64.of_int len) (56 - (8 * k)))
              land 0xff))
      done);
    match mode with
    | Client random_string ->
      let key = random_string 4 in
      Buffer.add_string buf key;
      String.iteri
        (fun i c ->
          Buffer.add_char buf (Char.chr (Char.code c lxor Char.code key.[i mod 4])))
        frame.Frame.content
    | Server -> Buffer.add_string buf frame.Frame.content
  ;;
end
