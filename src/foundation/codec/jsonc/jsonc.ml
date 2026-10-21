(** JSON with comments (JSONC) reader.

    [config.json] is hand-edited, so it accepts the same conveniences as VS Code's
    [settings.json]:
    - [// line comments] and [/* block comments */],
    - trailing commas before a closing brace or bracket,
    - an optional UTF-8 byte-order mark.

    Comments inside strings are preserved; the scanner tracks quoted strings and escapes.
    Everything else is passed through to {!Yojson.Basic}. *)

let strip_bom s =
  let len = String.length s in
  if len >= 3 && s.[0] = '\xef' && s.[1] = '\xbb' && s.[2] = '\xbf'
  then String.sub s 3 (len - 3)
  else s
;;

(** Removes [//] and [/* */] comments, preserving string contents. *)
let strip_comments s =
  let len = String.length s in
  let buf = Buffer.create len in
  let rec loop i =
    if i >= len
    then ()
    else (
      match s.[i] with
      | '"' ->
        Buffer.add_char buf '"';
        let rec in_string j =
          if j >= len
          then ()
          else (
            let c = s.[j] in
            Buffer.add_char buf c;
            if c = '\\' && j + 1 < len
            then (
              Buffer.add_char buf s.[j + 1];
              in_string (j + 2))
            else if c = '"'
            then loop (j + 1)
            else in_string (j + 1))
        in
        in_string (i + 1)
      | '/' when i + 1 < len && s.[i + 1] = '/' ->
        let rec skip j =
          if j >= len then () else if s.[j] = '\n' then loop j else skip (j + 1)
        in
        skip (i + 2)
      | '/' when i + 1 < len && s.[i + 1] = '*' ->
        let rec skip j =
          if j + 1 >= len
          then ()
          else if s.[j] = '*' && s.[j + 1] = '/'
          then loop (j + 2)
          else skip (j + 1)
        in
        skip (i + 2)
      | c ->
        Buffer.add_char buf c;
        loop (i + 1))
  in
  loop 0;
  Buffer.contents buf
;;

(** Drops a comma that is followed only by whitespace and a closing bracket. *)
let strip_trailing_commas s =
  let len = String.length s in
  let buf = Buffer.create len in
  let rec next_non_space j =
    if j >= len
    then j
    else (
      match s.[j] with
      | ' ' | '\t' | '\n' | '\r' -> next_non_space (j + 1)
      | _ -> j)
  in
  let rec loop i in_string =
    if i >= len
    then ()
    else (
      let c = s.[i] in
      if in_string
      then (
        Buffer.add_char buf c;
        if c = '\\' && i + 1 < len
        then (
          Buffer.add_char buf s.[i + 1];
          loop (i + 2) true)
        else if c = '"'
        then loop (i + 1) false
        else loop (i + 1) true)
      else if c = '"'
      then (
        Buffer.add_char buf c;
        loop (i + 1) true)
      else if c = ','
      then (
        let j = next_non_space (i + 1) in
        if j < len && (s.[j] = '}' || s.[j] = ']')
        then loop j false
        else (
          Buffer.add_char buf c;
          loop (i + 1) false))
      else (
        Buffer.add_char buf c;
        loop (i + 1) false))
  in
  loop 0 false;
  Buffer.contents buf
;;

let strip s = s |> strip_bom |> strip_comments |> strip_trailing_commas
let from_string s = Yojson.Basic.from_string (strip s)

let from_file path =
  let raw = In_channel.with_open_bin path In_channel.input_all in
  from_string raw
;;
