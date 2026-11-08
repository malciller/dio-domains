(** Minimal JSON writer for hot paths.

    Appends JSON directly to a [Buffer], with the escapes the wire format requires, so
    callers do not build and serialize a [Yojson] AST per message. *)

(** Append [s] as a JSON string, escaping quotes, backslashes and control characters. *)
let add_string buf s =
  Buffer.add_char buf '"';
  String.iter
    (fun c ->
      match c with
      | '"' -> Buffer.add_string buf "\\\""
      | '\\' -> Buffer.add_string buf "\\\\"
      | '\n' -> Buffer.add_string buf "\\n"
      | '\r' -> Buffer.add_string buf "\\r"
      | '\t' -> Buffer.add_string buf "\\t"
      | '\b' -> Buffer.add_string buf "\\b"
      | '\012' -> Buffer.add_string buf "\\f"
      | c when Char.code c < 0x20 ->
        Buffer.add_string buf (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char buf c)
    s;
  Buffer.add_char buf '"'
;;

let add_bool buf b = Buffer.add_string buf (if b then "true" else "false")
