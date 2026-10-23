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

let publish ?version (uri : string) (diags : Yojson.Safe.t list) : unit =
  let version_fields =
    match version with
    | Some v -> [ "version", `Int v ]
    | None -> []
  in
  write_msg
    (`Assoc
      [ "jsonrpc", `String "2.0"
      ; "method", `String "textDocument/publishDiagnostics"
      ; ( "params"
        , `Assoc ([ "uri", `String uri; "diagnostics", `List diags ] @ version_fields) )
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

(* ───────────────────── UTF-16 position mapping ───────────────────── *)

(** LSP positions are UTF-16 code units; the engine reports UTF-8 byte offsets, so a
    diagnostic after a non-ASCII character on the same line would land short. *)
let utf16_length (s : string) : int =
  let n = String.length s in
  let rec go i acc =
    if i >= n
    then acc
    else (
      let b = Char.code s.[i] in
      if b < 0x80
      then go (i + 1) (acc + 1)
      else if b < 0xE0
      then go (min n (i + 2)) (acc + 1)
      else if b < 0xF0
      then go (min n (i + 3)) (acc + 1)
      else go (min n (i + 4)) (acc + 2))
  in
  go 0 0
;;

let line_starts (text : string) : int array =
  let starts = ref [ 0 ] in
  String.iteri (fun i c -> if c = '\n' then starts := (i + 1) :: !starts) text;
  Array.of_list (List.rev !starts)
;;

let line_bounds (text : string) (starts : int array) (line : int) : int * int =
  if line < 0 || line >= Array.length starts
  then 0, 0
  else (
    let start = starts.(line) in
    let stop =
      match String.index_from_opt text start '\n' with
      | Some i -> i
      | None -> String.length text
    in
    start, stop)
;;

let byte_offset_of_utf16 (line : string) (units : int) : int =
  let n = String.length line in
  let rec go i u =
    if i >= n || u <= 0
    then i
    else (
      let b = Char.code line.[i] in
      let bytes =
        if b < 0x80 then 1 else if b < 0xE0 then 2 else if b < 0xF0 then 3 else 4
      in
      let width = if bytes = 4 then 2 else 1 in
      if u < width then i else go (min n (i + bytes)) (u - width))
  in
  go 0 (max 0 units)
;;

let utf16_col (text : string) (starts : int array) (line : int) (byte_col : int) : int =
  let start, stop = line_bounds text starts line in
  if start = 0 && stop = 0
  then 0
  else (
    let line_text = String.sub text start (stop - start) in
    let byte_col = max 0 (min byte_col (String.length line_text)) in
    utf16_length (String.sub line_text 0 byte_col))
;;

(* ───────────────────────── validation ───────────────────────── *)

(** Validate [text] and publish diagnostics for [uri]. *)
let validate_text ?version (uri : string) (text : string) : unit =
  let starts = line_starts text in
  match Strategy_script.parse_string text with
  | Error msg ->
    let line, col, m =
      match String.split_on_char ':' msg with
      | "script" :: l :: c :: rest ->
        (try int_of_string l - 1, max 0 (int_of_string c - 1), String.concat ":" rest with
         | _ -> 0, 0, msg)
      | _ -> 0, 0, msg
    in
    let col = utf16_col text starts line col in
    publish
      ?version
      uri
      [ diagnostic ~range:(range_obj line col line (col + 1)) ~severity:1 ~message:m ]
  | Ok f ->
    Strategy_actions_builtin.register_all ();
    let diags = Strategy_compile.validate f in
    let lsp_diags =
      List.rev_map
        (fun (d : Strategy_compile.diagnostic) ->
          let sl, sc, el, ec = locate d.where text in
          let sc = utf16_col text starts sl sc in
          let ec = utf16_col text starts el ec in
          diagnostic
            ~range:(range_obj sl sc el ec)
            ~severity:(sev_to_severity d.sev)
            ~message:(d.where ^ ": " ^ d.msg))
        diags
    in
    publish ?version uri lsp_diags
;;

(* ───────────────────────── message handling ───────────────────────── *)

type doc =
  { mutable text : string
  ; mutable version : int option
  }

let docs : (string, doc) Hashtbl.t = Hashtbl.create 16
let exit_requested = ref false

let assoc_member key json =
  match json with
  | `Assoc fields -> List.assoc_opt key fields
  | _ -> None
;;

let text_document (params : Yojson.Safe.t) : Yojson.Safe.t =
  match assoc_member "textDocument" params with
  | Some td -> td
  | None -> `Null
;;

let text_document_uri (params : Yojson.Safe.t) : string =
  match assoc_member "uri" (text_document params) with
  | Some (`String uri) -> uri
  | _ -> ""
;;

let text_document_version (params : Yojson.Safe.t) : int option =
  match assoc_member "version" (text_document params) with
  | Some (`Int version) -> Some version
  | _ -> None
;;

let position_of_json (json : Yojson.Safe.t) : int * int =
  match assoc_member "line" json, assoc_member "character" json with
  | Some (`Int line), Some (`Int character) -> line, character
  | _ -> 0, 0
;;

let byte_offset_of_position (text : string) (starts : int array) (line, character) : int =
  let start, stop = line_bounds text starts line in
  if start = 0 && stop = 0
  then 0
  else (
    let line_text = String.sub text start (stop - start) in
    start + byte_offset_of_utf16 line_text character)
;;

(** Apply one LSP [TextDocumentContentChangeEvent]. Range-delimited changes are accepted
    even though the server advertises full sync, so clients that send them still work. *)
let apply_change (text : string) (change : Yojson.Safe.t) : string =
  let replacement =
    match assoc_member "text" change with
    | Some (`String text) -> text
    | _ -> ""
  in
  match assoc_member "range" change with
  | None | Some `Null -> replacement
  | Some range ->
    let bound key =
      match assoc_member key range with
      | Some position -> position_of_json position
      | None -> 0, 0
    in
    let starts = line_starts text in
    let start = byte_offset_of_position text starts (bound "start") in
    let stop = byte_offset_of_position text starts (bound "end") in
    let len = String.length text in
    let start = max 0 (min start len) in
    let stop = max start (min stop len) in
    String.sub text 0 start ^ replacement ^ String.sub text stop (len - stop)
;;

let store_doc uri text version = Hashtbl.replace docs uri { text; version }

let handle_notification (method_ : string) (params : Yojson.Safe.t) : unit =
  match method_ with
  | "textDocument/didOpen" ->
    let uri = text_document_uri params in
    let text =
      match assoc_member "text" (text_document params) with
      | Some (`String text) -> text
      | _ -> ""
    in
    let version = text_document_version params in
    store_doc uri text version;
    validate_text ?version uri text
  | "textDocument/didChange" ->
    let uri = text_document_uri params in
    let current =
      match Hashtbl.find_opt docs uri with
      | Some doc -> doc.text
      | None -> ""
    in
    let changes =
      match assoc_member "contentChanges" params with
      | Some (`List changes) -> changes
      | _ -> []
    in
    let text = List.fold_left apply_change current changes in
    let version = text_document_version params in
    store_doc uri text version;
    validate_text ?version uri text
  | "textDocument/didSave" ->
    let uri = text_document_uri params in
    let text =
      match assoc_member "text" params with
      | Some (`String text) -> Some text
      | _ ->
        (match Hashtbl.find_opt docs uri with
         | Some doc -> Some doc.text
         | None -> None)
    in
    let version =
      match Hashtbl.find_opt docs uri with
      | Some doc -> doc.version
      | None -> text_document_version params
    in
    (match text with
     | Some text when String.length text > 0 -> validate_text ?version uri text
     | _ -> ())
  | "textDocument/didClose" ->
    let uri = text_document_uri params in
    let version =
      match Hashtbl.find_opt docs uri with
      | Some doc -> doc.version
      | None -> None
    in
    Hashtbl.remove docs uri;
    publish ?version uri []
  | "exit" -> exit_requested := true
  | "initialized"
  | "$/cancelRequest"
  | "workspace/didChangeConfiguration"
  | "workspace/didChangeWatchedFiles" -> ()
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
                    ; "positionEncoding", `String "utf-16"
                    ] )
              ; ( "serverInfo"
                , `Assoc [ "name", `String "dio-strategy"; "version", `String "0.2.0" ] )
              ] )
        ])
  | "shutdown" ->
    (* Respond first; the client then sends `exit` or closes the pipe. Closing the stream
       here races its pending write. *)
    write_msg (`Assoc [ "jsonrpc", `String "2.0"; "id", id; "result", `Null ])
  | _ ->
    write_msg
      (`Assoc
        [ "jsonrpc", `String "2.0"
        ; "id", id
        ; ( "error"
          , `Assoc
              [ "code", `Int (-32601)
              ; "message", `String ("method not found: " ^ method_)
              ] )
        ])
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
