(** [Strategy_lsp] — minimal in-process Language Server Protocol server for `.strategy`
    files over stdio, spawned via `dio strategy lsp`.

    It implements just enough LSP to light up an editor against the existing script parser
    ([Strategy_script.parse_string]) and static validator ([Strategy_compile.validate]):
    full-text sync, then diagnostics published on open/change/save. No completion or code
    actions — validation only.

    Diagnostics are positioned by the `where` field the validator attaches to each
    diagnostic (a step id); the step label is located in the source by re-normalising
    label lines exactly the way [Strategy_script] does. Parse failures already carry
    `line:col`, so those are exact. *)

open Yojson.Safe

let sev_to_severity = function
  | Strategy_compile.Error -> 1
  | Strategy_compile.Warning -> 2
;;

(* ───────────────────────── LSP framing ───────────────────────── *)

(** Read exactly [n] bytes from stdin. *)
let read_exact n =
  let buf = Bytes.create n in
  let filled = ref 0 in
  while !filled < n do
    match input stdin buf !filled (n - !filled) with
    | 0 -> raise End_of_file
    | k -> filled := !filled + k
  done;
  Bytes.to_string buf
;;

(** Read one frame (headers + body) from stdin. Returns [None] on EOF. *)
let read_frame () : string option =
  let hdr = Buffer.create 128 in
  let rec read_headers () =
    if Buffer.length hdr >= 4 && Buffer.sub hdr (Buffer.length hdr - 4) 4 = "\r\n\r\n"
    then ()
    else (
      match input_char stdin with
      | exception End_of_file -> ()
      | c ->
        Buffer.add_char hdr c;
        read_headers ())
  in
  read_headers ();
  if Buffer.length hdr = 0
  then None
  else (
    let len =
      let content_length = ref None in
      List.iter
        (fun line ->
          let line = String.trim line in
          let prefix = "content-length:" in
          if String.length line > String.length prefix
             && String.lowercase_ascii (String.sub line 0 (String.length prefix)) = prefix
          then
            content_length
            := Some
                 (int_of_string
                    (String.trim
                       (String.sub
                          line
                          (String.length prefix)
                          (String.length line - String.length prefix)))))
        (String.split_on_char '\n' (Buffer.contents hdr));
      match !content_length with
      | Some l -> l
      | None -> 0
    in
    Some (read_exact len))
;;

(** Write a JSON-RPC message with the LSP Content-Length framing. *)
let write_msg (msg : Yojson.Safe.t) : unit =
  let body = Yojson.Safe.to_string msg in
  Printf.printf "Content-Length: %d\r\n\r\n%s" (String.length body) body;
  flush stdout
;;

(* ───────────────────────── JSON helpers ───────────────────────── *)

let pos_obj line character = `Assoc [ "line", `Int line; "character", `Int character ]
let range_obj sl sc el ec = `Assoc [ "start", pos_obj sl sc; "end", pos_obj el ec ]

let diagnostic ~range ~severity ~message =
  `Assoc
    [ "range", range
    ; "severity", `Int severity
    ; "source", `String "dio"
    ; "message", `String message
    ]
;;

let publish (uri : string) (diags : Yojson.Safe.t list) : unit =
  write_msg
    (`Assoc
      [ "jsonrpc", `String "2.0"
      ; "method", `String "textDocument/publishDiagnostics"
      ; "params", `Assoc [ "uri", `String uri; "diagnostics", `List diags ]
      ])
;;

(* ───────────────────── diagnostic localization ───────────────────── *)

(** Order lifecycle kinds, mirrored from [Strategy_script]; a multi-kind `when` group
    suffixes each generated step id with the kind. *)
let order_kinds =
  [ "filled"
  ; "cancelled"
  ; "acknowledged"
  ; "amended"
  ; "failed"
  ; "rejected"
  ; "amendment_skipped"
  ; "amendment_failed"
  ; "cancel_cleanup"
  ]
;;

(** Truncate a line at the first comment marker (`--`, `//`, `#`). *)
let strip_comment (line : string) : string =
  let n = String.length line in
  let rec go i =
    if i >= n
    then i
    else if line.[i] = '#'
    then i
    else if i + 1 < n
            && ((line.[i] = '-' && line.[i + 1] = '-')
                || (line.[i] = '/' && line.[i + 1] = '/'))
    then i
    else go (i + 1)
  in
  String.sub line 0 (go 0)
;;

(** Re-derive the normalized step id a label line would produce (dots and spaces to
    underscores, lowercased), matching [Strategy_script.normalize]. *)
let normalize_label (body : string) : string =
  body
  |> String.trim
  |> String.map (fun c -> if c = '.' || c = ' ' || c = '\t' then '_' else c)
  |> String.lowercase_ascii
;;

(** Does normalized label [norm] match step id [id]? Direct, or as the base of a
    multi-kind group id (`label_kind`). *)
let label_matches (id : string) (norm : string) : bool =
  String.equal norm id
  ||
  let base = norm ^ "_" in
  let bl = String.length base
  and il = String.length id in
  il > bl
  && String.sub id 0 bl = base
  && List.mem (String.sub id bl (il - bl)) order_kinds
;;

(** Locate the source line whose step label corresponds to [id]. Returns the 0-based line
    and the label width, if found. *)
let find_step_line (id : string) (text : string) : (int * int) option =
  let header_words = [ "strategy"; "version"; "remembers"; "tunable"; "when" ] in
  let candidate (line : string) : int option =
    let line = String.trim (strip_comment line) in
    let n = String.length line in
    if n = 0 || line.[n - 1] <> ':'
    then None
    else (
      let body = String.sub line 0 (n - 1) in
      let first =
        match String.index_opt body ' ' with
        | Some i -> String.sub body 0 i
        | None -> body
      in
      if List.mem first header_words
      then None
      else (
        let norm = normalize_label body in
        if String.equal norm "" || not (label_matches id norm)
        then None
        else Some (String.length body)))
  in
  let rec go i = function
    | [] -> None
    | l :: rest ->
      (match candidate l with
       | Some w -> Some (i, w)
       | None -> go (i + 1) rest)
  in
  go 0 (String.split_on_char '\n' text)
;;

(** Map a validator `where` string to a source range. Returns start line, start char, end
    line, end char. *)
let locate (where : string) (text : string) : int * int * int * int =
  let step_prefix = "step " in
  if String.length where > String.length step_prefix
     && String.sub where 0 (String.length step_prefix) = step_prefix
  then (
    let id =
      String.sub
        where
        (String.length step_prefix)
        (String.length where - String.length step_prefix)
    in
    match find_step_line id text with
    | Some (l, w) -> l, 0, l, w
    | None -> 0, 0, 0, 0)
  else 0, 0, 0, 0
;;

(* ───────────────────────── validation ───────────────────────── *)

(** Validate [text] and publish diagnostics for [uri]. *)
let validate_text (uri : string) (text : string) : unit =
  match Strategy_script.parse_string text with
  | Error msg ->
    let line, col, m =
      match String.split_on_char ':' msg with
      | "script" :: l :: c :: rest ->
        (try int_of_string l - 1, max 0 (int_of_string c - 1), String.concat ":" rest with
         | _ -> 0, 0, msg)
      | _ -> 0, 0, msg
    in
    publish
      uri
      [ diagnostic ~range:(range_obj line col line (col + 1)) ~severity:1 ~message:m ]
  | Ok f ->
    Strategy_actions_builtin.register_all ();
    let diags = Strategy_compile.validate f in
    let lsp_diags =
      List.rev_map
        (fun (d : Strategy_compile.diagnostic) ->
          let sl, sc, el, ec = locate d.where text in
          diagnostic
            ~range:(range_obj sl sc el ec)
            ~severity:(sev_to_severity d.sev)
            ~message:(d.where ^ ": " ^ d.msg))
        diags
    in
    publish uri lsp_diags
;;

(* ───────────────────────── message handling ───────────────────────── *)

let docs : (string, string) Hashtbl.t = Hashtbl.create 16
let exit_requested = ref false

let text_document_uri (params : Yojson.Safe.t) : string =
  let open Util in
  params |> member "textDocument" |> member "uri" |> to_string
;;

let handle_notification (method_ : string) (params : Yojson.Safe.t) : unit =
  let open Util in
  match method_ with
  | "textDocument/didOpen" ->
    let uri = text_document_uri params in
    let text = params |> member "textDocument" |> member "text" |> to_string in
    Hashtbl.replace docs uri text;
    validate_text uri text
  | "textDocument/didChange" ->
    let uri = text_document_uri params in
    (match params |> member "contentChanges" |> to_list |> List.rev with
     | c :: _ ->
       let text = member "text" c |> to_string in
       Hashtbl.replace docs uri text;
       validate_text uri text
     | [] -> ())
  | "textDocument/didSave" ->
    let uri = text_document_uri params in
    let text =
      match Hashtbl.find_opt docs uri with
      | Some t -> t
      | None -> ""
    in
    if String.length text > 0 then validate_text uri text
  | "textDocument/didClose" ->
    let uri = text_document_uri params in
    Hashtbl.remove docs uri;
    publish uri []
  | "exit" -> Stdlib.exit 0
  | _ -> ()
;;

let handle_request (id : Yojson.Safe.t) (method_ : string) (_params : Yojson.Safe.t)
  : unit
  =
  match method_ with
  | "initialize" ->
    write_msg
      (`Assoc
        [ "jsonrpc", `String "2.0"
        ; "id", id
        ; ( "result"
          , `Assoc
              [ ( "capabilities"
                , `Assoc
                    [ ( "textDocumentSync"
                      , `Assoc
                          [ "openClose", `Bool true
                          ; "change", `Int 1
                          ; "save", `Assoc [ "includeText", `Bool true ]
                          ] )
                    ] )
              ; ( "serverInfo"
                , `Assoc [ "name", `String "dio-strategy"; "version", `String "0.1.0" ] )
              ] )
        ])
  | "shutdown" ->
    exit_requested := true;
    write_msg (`Assoc [ "jsonrpc", `String "2.0"; "id", id; "result", `Null ])
  | _ -> write_msg (`Assoc [ "jsonrpc", `String "2.0"; "id", id; "result", `Null ])
;;

let handle_frame (json : Yojson.Safe.t) : unit =
  let open Util in
  match member "method" json |> to_string_option with
  | None -> ()
  | Some method_ ->
    let id = member "id" json in
    let params = member "params" json in
    if id <> `Null
    then handle_request id method_ params
    else handle_notification method_ params
;;

(* ───────────────────────── main loop ───────────────────────── *)

let run () : int =
  exit_requested := false;
  let rec loop () =
    if !exit_requested
    then ()
    else (
      match read_frame () with
      | None -> ()
      | Some body ->
        (try handle_frame (Yojson.Safe.from_string body) with
         | Yojson.Json_error msg -> prerr_endline ("strategy lsp: malformed frame: " ^ msg)
         | exn -> prerr_endline ("strategy lsp: " ^ Printexc.to_string exn));
        loop ())
  in
  try
    loop ();
    0
  with
  | End_of_file -> 0
;;
