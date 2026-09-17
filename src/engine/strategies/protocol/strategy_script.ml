(** Strategy script (.strategy) — a human-readable, indentation-based frontend over
    [Strategy_file.t].

    The script lowers to the exact same AST the JSON parser produces, so the validator,
    runtime, harness, and replay are unchanged. The surface reads like short written
    instructions:

    {v
      strategy jacobs.ladder
      version 1

      remembers:
        cycle.ok: bool
        buy.active: bool

      when book.updates:
        prepare:
          reset the venue
          read the book
          cycle.ok = not $platform.price.nan

        skip.nan.price:
          if $platform.price.nan:
            stop
    v}

    Naming rules (this is the whole trick):
    - Dotted names map to underscores, so `cancel.excess.buys` is the action
      `cancel_excess_buys` and `skip.nan.price` is the id `skip_nan_price`. Names are
      whitespace-insensitive: `cycle.ok` is one token, never two.
    - Your own remembered variables and `let` bindings are written bare: `cycle.ok` lowers
      to `$state.cycle_ok`.
    - Engine facts are references: `$platform.*`, `$event.*`, `$params.*`, `$local.*`,
      `$signal.*`, `$price`, `$now`. Dots past the namespace also map to underscores, so
      `$platform.price.nan` and `$platform.price_nan` are the same fact.

    Blocks are newline + colon + indentation; no braces. A step is a label (a name, then
    `:`) whose body is either actions or a single `if <condition>:` / `otherwise:` pair. A
    top-level `when <phrase>:` opens a trigger group; the trigger guard is injected into
    every step inside it, exactly as the JSON `event` guard. *)

open Strategy_file

(* ────────────────────────── tokens ────────────────────────── *)

type tk =
  | T_ident of string
  | T_int of int
  | T_float of float
  | T_bool of bool
  | T_string of string
  | T_ref of string
  | T_lbrace
  | T_rbrace
  | T_lparen
  | T_rparen
  | T_lbracket
  | T_rbracket
  | T_comma
  | T_colon
  | T_qmark
  | T_assign
  | T_eq
  | T_ne
  | T_lt
  | T_le
  | T_gt
  | T_ge
  | T_plus
  | T_minus
  | T_star
  | T_slash
  | T_and
  | T_or
  | T_not
  | T_eof

type tok =
  { t : tk
  ; line : int
  ; col : int
  }

let tk_eq a b =
  match a, b with
  | T_ident a, T_ident b -> String.equal a b
  | T_int a, T_int b -> a = b
  | T_float a, T_float b -> Float.equal a b
  | T_bool a, T_bool b -> a = b
  | T_string a, T_string b -> String.equal a b
  | T_ref a, T_ref b -> String.equal a b
  | _ ->
    (match a, b with
     | T_lbrace, T_lbrace
     | T_rbrace, T_rbrace
     | T_lparen, T_lparen
     | T_rparen, T_rparen
     | T_lbracket, T_lbracket
     | T_rbracket, T_rbracket
     | T_comma, T_comma
     | T_colon, T_colon
     | T_qmark, T_qmark
     | T_assign, T_assign
     | T_eq, T_eq
     | T_ne, T_ne
     | T_lt, T_lt
     | T_le, T_le
     | T_gt, T_gt
     | T_ge, T_ge
     | T_plus, T_plus
     | T_minus, T_minus
     | T_star, T_star
     | T_slash, T_slash
     | T_and, T_and
     | T_or, T_or
     | T_not, T_not
     | T_eof, T_eof -> true
     | _ -> false)
;;

let is_digit c = c >= '0' && c <= '9'
let is_id_start c = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c = '_'
let is_id_char c = is_id_start c || is_digit c || c = '.'
let is_ref_char c = is_id_char c

exception Script_error of string * int * int

let err ?(line = 0) ?(col = 0) msg = raise (Script_error (msg, line, col))

let tk_to_string = function
  | T_ident s | T_ref s -> s
  | T_int i -> string_of_int i
  | T_float f ->
    let s = string_of_float f in
    if String.length s > 0 && s.[String.length s - 1] = '.' then s ^ "0" else s
  | T_bool b -> if b then "true" else "false"
  | T_string s -> Printf.sprintf "%S" s
  | T_assign -> "="
  | T_eq -> "=="
  | T_ne -> "!="
  | T_lt -> "<"
  | T_le -> "<="
  | T_gt -> ">"
  | T_ge -> ">="
  | T_plus -> "+"
  | T_minus -> "-"
  | T_star -> "*"
  | T_slash -> "/"
  | T_and -> "and"
  | T_or -> "or"
  | T_not -> "not"
  | T_lparen -> "("
  | T_rparen -> ")"
  | T_lbrace -> "{"
  | T_rbrace -> "}"
  | T_lbracket -> "["
  | T_rbracket -> "]"
  | T_comma -> ","
  | T_colon -> ":"
  | T_qmark -> "?"
  | T_eof -> ""
;;

let tk_list_to_string toks = String.concat " " (List.map tk_to_string toks)

(* ────────────────────────── character lexer ────────────────────────── *)

let lex_tokens (src : string) : tok list =
  let n = String.length src in
  let toks = ref []
  and i = ref 0
  and line = ref 1
  and col = ref 0 in
  let advance () =
    incr i;
    incr col
  in
  let newline () =
    incr line;
    col := 0
  in
  let c () = if !i < n then Some src.[!i] else None in
  let peek2 () = if !i + 1 < n then Some src.[!i + 1] else None in
  let start_col = ref 0 in
  let emit t = toks := { t; line = !line; col = !start_col } :: !toks in
  let error msg = raise (Script_error (msg, !line, !col)) in
  let skip_to_eol () =
    while
      !i < n
      &&
      match c () with
      | Some '\n' -> false
      | _ -> true
    do
      advance ()
    done
  in
  while !i < n do
    start_col := !col;
    match c () with
    | None -> ()
    | Some ch ->
      if ch = '\n'
      then (
        advance ();
        newline ())
      else if ch = ' ' || ch = '\t' || ch = '\r'
      then advance ()
      else if ch = '-'
              &&
              match peek2 () with
              | Some '-' -> true
              | _ -> false
      then (
        advance ();
        advance ();
        skip_to_eol ())
      else if ch = '/'
              &&
              match peek2 () with
              | Some '/' -> true
              | _ -> false
      then (
        advance ();
        advance ();
        skip_to_eol ())
      else if ch = '#'
      then skip_to_eol ()
      else if ch = '"'
      then (
        advance ();
        let buf = Buffer.create 64 in
        let started_line = !line in
        let rec loop () =
          match c () with
          | None -> error (Printf.sprintf "unterminated string (line %d)" started_line)
          | Some '\\' ->
            advance ();
            (match c () with
             | Some '"' ->
               Buffer.add_char buf '"';
               advance ();
               loop ()
             | Some '\\' ->
               Buffer.add_char buf '\\';
               advance ();
               loop ()
             | Some 'n' ->
               Buffer.add_char buf '\n';
               advance ();
               loop ()
             | Some other ->
               Buffer.add_char buf '\\';
               Buffer.add_char buf other;
               advance ();
               loop ()
             | None -> error "unterminated escape in string")
          | Some '"' -> advance ()
          | Some '\n' -> error "newline in string literal"
          | Some other ->
            Buffer.add_char buf other;
            advance ();
            loop ()
        in
        loop ();
        emit (T_string (Buffer.contents buf)))
      else if is_id_start ch
      then (
        let start = !i in
        while !i < n && is_id_char src.[!i] do
          advance ()
        done;
        let word = String.sub src start (!i - start) in
        match word with
        | "and" -> emit T_and
        | "or" -> emit T_or
        | "not" -> emit T_not
        | "true" -> emit (T_bool true)
        | "false" -> emit (T_bool false)
        | _ -> emit (T_ident word))
      else if ch = '$'
      then (
        let start = !i in
        advance ();
        while !i < n && is_ref_char src.[!i] do
          advance ()
        done;
        emit (T_ref (String.sub src start (!i - start))))
      else if is_digit ch
      then (
        let start = !i in
        while !i < n && is_digit src.[!i] do
          advance ()
        done;
        let is_float =
          (match c () with
           | Some '.' -> true
           | _ -> false)
          ||
          match c () with
          | Some ('e' | 'E') -> true
          | _ -> false
        in
        if is_float
        then (
          (match c () with
           | Some '.' ->
             advance ();
             while !i < n && is_digit src.[!i] do
               advance ()
             done
           | _ -> ());
          (match c () with
           | Some ('e' | 'E') ->
             advance ();
             (match c () with
              | Some ('+' | '-') -> advance ()
              | _ -> ());
             while !i < n && is_digit src.[!i] do
               advance ()
             done
           | _ -> ());
          emit (T_float (float_of_string (String.sub src start (!i - start)))))
        else emit (T_int (int_of_string (String.sub src start (!i - start)))))
      else (
        let two = if !i + 1 < n then String.sub src !i 2 else String.make 1 ch in
        match two with
        | "==" ->
          advance ();
          advance ();
          emit T_eq
        | "!=" ->
          advance ();
          advance ();
          emit T_ne
        | "<=" ->
          advance ();
          advance ();
          emit T_le
        | ">=" ->
          advance ();
          advance ();
          emit T_ge
        | _ ->
          advance ();
          (match ch with
           | '(' -> emit T_lparen
           | ')' -> emit T_rparen
           | '{' -> emit T_lbrace
           | '}' -> emit T_rbrace
           | '[' -> emit T_lbracket
           | ']' -> emit T_rbracket
           | ',' -> emit T_comma
           | ':' -> emit T_colon
           | '?' -> emit T_qmark
           | '=' -> emit T_assign
           | '<' -> emit T_lt
           | '>' -> emit T_gt
           | '+' -> emit T_plus
           | '-' -> emit T_minus
           | '*' -> emit T_star
           | '/' -> emit T_slash
           | '!' -> emit T_not
           | _ -> error (Printf.sprintf "unexpected character %C" ch)))
  done;
  toks := { t = T_eof; line = !line; col = !col } :: !toks;
  List.rev !toks
;;

(* ────────────────────────── logical lines ────────────────────────── *)

(** A physical line's tokens become one logical line unless the brackets are open or the
    line ends on a connective/operator (in which case it continues). *)
type lline =
  { indent : int
  ; toks : tok list
  ; ln : int
  }

let continues_with = function
  | T_and
  | T_or
  | T_assign
  | T_eq
  | T_ne
  | T_lt
  | T_le
  | T_gt
  | T_ge
  | T_plus
  | T_minus
  | T_star
  | T_slash
  | T_comma -> true
  | _ -> false
;;

let logical_lines (toks : tok list) : lline list =
  (* Split by physical line first. *)
  let physical = ref [] in
  let cur = ref []
  and cur_line = ref (-1) in
  List.iter
    (fun tok ->
      if tok.line <> !cur_line && !cur <> []
      then (
        physical := List.rev !cur :: !physical;
        cur := []);
      cur_line := tok.line;
      match tok.t with
      | T_eof -> ()
      | _ -> cur := tok :: !cur)
    toks;
  if !cur <> [] then physical := List.rev !cur :: !physical;
  let physical = List.rev !physical in
  let out = ref [] in
  let depth = ref 0 in
  let cont = ref false in
  let buf = ref None in
  (* (indent, rev toks, ln) *)
  let flush () =
    match !buf with
    | Some (indent, rev, ln) ->
      out := { indent; toks = List.rev rev; ln } :: !out;
      buf := None
    | None -> ()
  in
  List.iter
    (fun line_toks ->
      if line_toks = []
      then ()
      else (
        if not !cont
        then (
          flush ();
          let indent = (List.hd line_toks).col in
          buf := Some (indent, [], (List.hd line_toks).line));
        (match !buf with
         | Some (indent, rev, ln) ->
           buf := Some (indent, List.rev_append line_toks rev, ln)
         | None -> ());
        (* update bracket depth and trailing-operator state *)
        List.iter
          (fun tok ->
            match tok.t with
            | T_lparen | T_lbracket -> incr depth
            | T_rparen | T_rbracket -> decr depth
            | _ -> ())
          line_toks;
        let last = List.nth line_toks (List.length line_toks - 1) in
        cont := !depth > 0 || continues_with last.t))
    physical;
  flush ();
  List.rev !out
;;

(* ────────────────────────── token-slice parser ────────────────────────── *)

type tp =
  { a : tok array
  ; mutable p : int
  }

let tp_make toks = { a = Array.of_list toks; p = 0 }
let tp_peek tp = if tp.p < Array.length tp.a then tp.a.(tp.p).t else T_eof
let tp_at_end tp = tp.p >= Array.length tp.a
let tp_advance tp = tp.p <- tp.p + 1
let tp_eq tp k = tk_eq (tp_peek tp) k
let tp_line tp = if tp.p < Array.length tp.a then tp.a.(tp.p).line else 0
let tp_col tp = if tp.p < Array.length tp.a then tp.a.(tp.p).col else 0
let tp_err tp msg = err ~line:(tp_line tp) ~col:(tp_col tp) msg

let tp_take tp n =
  let out = ref [] in
  for _ = 1 to n do
    if not (tp_at_end tp)
    then (
      out := tp.a.(tp.p) :: !out;
      tp_advance tp)
  done;
  List.rev !out
;;

(** Collect tokens until [stop] holds at bracket depth 0. Does not consume the stopper. *)
let tp_take_until tp stop =
  let out = ref []
  and depth = ref 0
  and go = ref true in
  while !go && not (tp_at_end tp) do
    let k = tp_peek tp in
    if !depth = 0 && stop k
    then go := false
    else (
      (match k with
       | T_lparen | T_lbracket -> incr depth
       | T_rparen | T_rbracket -> decr depth
       | _ -> ());
      out := tp.a.(tp.p) :: !out;
      tp_advance tp)
  done;
  List.rev !out
;;

(* ────────────────────────── names and scope ────────────────────────── *)

let words_of toks =
  List.filter_map
    (fun tok ->
      match tok.t with
      | T_ident s -> Some (String.lowercase_ascii s)
      | T_int i -> Some (string_of_int i)
      | _ -> None)
    toks
;;

let normalize words =
  String.concat
    "_"
    (List.map
       (fun w ->
         String.map (fun c -> if c = '.' then '_' else c) (String.lowercase_ascii w))
       words)
;;

(** Normalise an engine reference: keep the namespace, turn later dots into underscores,
    so [$platform.price.nan] and [$platform.price_nan] both mean the same fact. *)
let normalize_ref s =
  match String.index_opt s '.' with
  | None -> s
  | Some i ->
    let ns = String.sub s 0 (i + 1) in
    let field = String.sub s (i + 1) (String.length s - i - 1) in
    ns ^ String.map (fun c -> if c = '.' then '_' else c) field
;;

let norm_key s =
  String.map (fun c -> if c = '.' then '_' else c) (String.lowercase_ascii s)
;;

type scope =
  { state_names : string list
  ; mutable locals : string list
  }

let all_own scope _ = scope.state_names @ scope.locals

(** Longest prefix of [words] that joins (with "_") to a declared own-name. Returns
    (canonical_name, words_consumed). *)
let match_name names words =
  let arr = Array.of_list words in
  let n = Array.length arr in
  let best = ref None in
  for k = 1 to n do
    let candidate = normalize (Array.to_list (Array.sub arr 0 k)) in
    if List.mem candidate names then best := Some (candidate, k)
  done;
  !best
;;

let ref_of scope name =
  if List.mem name scope.state_names then "$state." ^ name else "$local." ^ name
;;

(** Render an expression token slice to engine expression text, rewriting bare own-names
    to [$state.*]/[$local.*] and keeping [$refs], literals, and operators verbatim. *)
let render_expr scope ?(line = 0) ?(col = 0) toks =
  let a = Array.of_list toks in
  let n = Array.length a in
  let buf = Buffer.create 64 in
  let started = ref false in
  let suppress = ref false in
  let push s =
    if !started && not !suppress then Buffer.add_char buf ' ';
    Buffer.add_string buf s;
    started := true;
    suppress := false
  in
  let i = ref 0 in
  while !i < n do
    match a.(!i).t with
    | T_ident _ ->
      let j = ref !i
      and words = ref [] in
      while
        !j < n
        &&
        match a.(!j).t with
        | T_ident _ -> true
        | _ -> false
      do
        (match a.(!j).t with
         | T_ident w -> words := w :: !words
         | _ -> ());
        incr j
      done;
      let words = List.rev !words in
      (match match_name (all_own scope ()) words with
       | Some (name, consumed) ->
         push (ref_of scope name);
         i := !i + consumed
       | None -> err ~line ~col (Printf.sprintf "unknown name: %s" (normalize words)))
    | T_ref r ->
      push (normalize_ref r);
      incr i
    | tk ->
      (match tk with
       | T_lparen ->
         push "(";
         suppress := true
       | T_rparen ->
         suppress := true;
         push ")"
       | T_comma ->
         suppress := true;
         push ","
       | _ -> push (tk_to_string tk));
      incr i
  done;
  if not !started then err ~line ~col "empty expression";
  Buffer.contents buf
;;

(** Render a single reference (bare own-name or [$ref]). *)
let render_ref scope ?(line = 0) ?(col = 0) toks =
  match toks with
  | [ { t = T_ref r; _ } ] -> normalize_ref r
  | _ ->
    let words = words_of toks in
    (match match_name (all_own scope ()) words with
     | Some (name, _) -> ref_of scope name
     | None ->
       err ~line ~col (Printf.sprintf "unknown reference: %s" (String.concat " " words)))
;;

(* ────────────────────────── conditions ────────────────────────── *)

let guard_functions =
  [ "is.none"
  ; "is.some"
  ; "cooldown.elapsed"
  ; "pending"
  ; "capacity"
  ; "engine"
  ; "order"
  ; "signal"
  ]
;;

(** If the token at [tp] is a guard function name followed by '(' return (canonical_name,
    1). *)
let match_guard tp =
  match tp_peek tp with
  | T_ident s
    when List.mem (String.lowercase_ascii s) guard_functions
         && tp.p + 1 < Array.length tp.a
         &&
         match tp.a.(tp.p + 1).t with
         | T_lparen -> true
         | _ -> false -> Some (String.lowercase_ascii s, 1)
  | _ -> None
;;

let rec parse_condition scope tp : guard = parse_or scope tp

and parse_or scope tp =
  let left = parse_and scope tp in
  let rec loop left =
    if tp_eq tp T_or
    then (
      tp_advance tp;
      loop (G_any [ left; parse_and scope tp ]))
    else left
  in
  loop left

and parse_and scope tp =
  let left = parse_unary scope tp in
  let rec loop left =
    if tp_eq tp T_and
    then (
      tp_advance tp;
      loop (G_all [ left; parse_unary scope tp ]))
    else left
  in
  loop left

and parse_unary scope tp =
  if tp_eq tp T_not
  then (
    tp_advance tp;
    match parse_unary scope tp with
    | G_expr s -> G_expr ("not " ^ s)
    | g -> G_not g)
  else parse_primary scope tp

and parse_primary scope tp =
  let line = tp_line tp in
  let col = tp_col tp in
  if tp_eq tp T_lparen
  then (
    tp_advance tp;
    let g = parse_condition scope tp in
    if not (tp_eq tp T_rparen) then tp_err tp "expected ')'";
    tp_advance tp;
    g)
  else if tp_eq tp (T_ident "event")
  then (
    tp_advance tp;
    let neg =
      if tp_eq tp T_ne
      then (
        tp_advance tp;
        true)
      else if tp_eq tp T_assign || tp_eq tp T_eq
      then (
        tp_advance tp;
        false)
      else tp_err tp "expected '==' or '!=' after 'event'"
    in
    let s =
      match tp_peek tp with
      | T_string s ->
        tp_advance tp;
        s
      | _ -> tp_err tp "expected event name string"
    in
    if neg then G_not (G_event s) else G_event s)
  else if tp_eq tp (T_ident "side")
  then (
    tp_advance tp;
    let neg =
      if tp_eq tp T_ne
      then (
        tp_advance tp;
        true)
      else if tp_eq tp T_assign || tp_eq tp T_eq
      then (
        tp_advance tp;
        false)
      else tp_err tp "expected '==' or '!=' after 'side'"
    in
    let s =
      match tp_peek tp with
      | T_string s ->
        tp_advance tp;
        s
      | _ -> tp_err tp "expected side string"
    in
    if neg then G_not (G_side s) else G_side s)
  else (
    match match_guard tp with
    | Some (name, arity) -> parse_guard_call scope tp name arity
    | None ->
      let toks =
        tp_take_until tp (fun k ->
          match k with
          | T_and | T_or | T_not | T_rparen | T_comma | T_colon -> true
          | _ -> false)
      in
      if toks = [] then err ~line ~col "expected condition";
      G_expr (render_expr scope ~line ~col toks))

and parse_guard_call scope tp name arity =
  let line = tp_line tp in
  let col = tp_col tp in
  ignore (tp_take tp arity);
  if not (tp_eq tp T_lparen) then tp_err tp (Printf.sprintf "expected '(' after %s" name);
  tp_advance tp;
  let pairs () =
    let out = ref [] in
    let first = ref true in
    while (not (tp_eq tp T_rparen)) && not (tp_at_end tp) do
      if not !first
      then if tp_eq tp T_comma then tp_advance tp else tp_err tp "expected ','";
      first := false;
      let key =
        match tp_peek tp with
        | T_ident s ->
          tp_advance tp;
          norm_key s
        | _ -> tp_err tp "expected argument name"
      in
      if not (tp_eq tp T_colon || tp_eq tp T_assign) then tp_err tp "expected ':'";
      tp_advance tp;
      let toks =
        tp_take_until tp (fun k ->
          match k with
          | T_comma | T_rparen -> true
          | _ -> false)
      in
      out := (key, render_expr scope ~line ~col toks) :: !out
    done;
    List.rev !out
  in
  let expect_rparen () =
    if not (tp_eq tp T_rparen)
    then tp_err tp (Printf.sprintf "expected ')' after %s(..." name);
    tp_advance tp
  in
  match name with
  | "is.none" | "is.some" ->
    let toks = tp_take_until tp (fun k -> tk_eq k T_rparen) in
    let r = render_ref scope ~line ~col toks in
    expect_rparen ();
    if String.equal name "is.none" then G_is_none r else G_is_some r
  | "pending" ->
    let s =
      match tp_peek tp with
      | T_string s ->
        tp_advance tp;
        s
      | _ -> tp_err tp "pending expects a string"
    in
    expect_rparen ();
    G_pending s
  | "capacity" ->
    let p = pairs () in
    expect_rparen ();
    G_capacity p
  | "engine" ->
    let p = pairs () in
    expect_rparen ();
    G_engine p
  | "order" ->
    let p = pairs () in
    expect_rparen ();
    G_order p
  | "signal" ->
    let p = pairs () in
    expect_rparen ();
    G_signal p
  | "cooldown.elapsed" ->
    let p = pairs () in
    expect_rparen ();
    let since =
      match List.assoc_opt "since" p with
      | Some s -> s
      | None -> tp_err tp "cooldown.elapsed needs 'since'"
    in
    let seconds =
      match List.assoc_opt "seconds" p with
      | Some s -> s
      | None -> tp_err tp "cooldown elapsed needs 'seconds'"
    in
    G_cooldown { since; seconds }
  | _ -> tp_err tp (Printf.sprintf "unknown guard: %s" name)
;;

(* ────────────────────────── actions ────────────────────────── *)

(** Value of an action argument: a single literal becomes its JSON scalar, anything else
    becomes the rendered expression string. *)
let arg_value scope toks =
  match toks with
  | [ { t = T_int i; _ } ] -> `Int i
  | [ { t = T_float f; _ } ] -> `Float f
  | [ { t = T_bool b; _ } ] -> `Bool b
  | [ { t = T_string s; _ } ] -> `String s
  | [ { t = T_ref r; _ } ] -> `String (normalize_ref r)
  | _ ->
    let line =
      match toks with
      | t :: _ -> t.line
      | [] -> 0
    in
    `String (render_expr scope ~line toks)
;;

(** Parse the parenthesised argument list (and any bind clause) at [tp]. *)
let parse_action_args scope tp =
  let args = ref []
  and binds = ref [] in
  if tp_eq tp T_lparen
  then (
    tp_advance tp;
    let first = ref true in
    while (not (tp_eq tp T_rparen)) && not (tp_at_end tp) do
      if not !first
      then if tp_eq tp T_comma then tp_advance tp else tp_err tp "expected ','";
      first := false;
      let line = tp_line tp in
      if tp_eq tp (T_ident "bind")
      then (
        tp_advance tp;
        if not (tp_eq tp T_lparen) then tp_err tp "expected '(' after 'bind'";
        tp_advance tp;
        let bfirst = ref true in
        while (not (tp_eq tp T_rparen)) && not (tp_at_end tp) do
          if not !bfirst
          then if tp_eq tp T_comma then tp_advance tp else tp_err tp "expected ','";
          bfirst := false;
          let var_toks =
            tp_take_until tp (fun k ->
              match k with
              | T_colon | T_assign | T_comma | T_rparen -> true
              | _ -> false)
          in
          let var = normalize (words_of var_toks) in
          if not (tp_eq tp T_colon || tp_eq tp T_assign)
          then tp_err tp "expected ':' in bind";
          tp_advance tp;
          let field_toks =
            tp_take_until tp (fun k ->
              match k with
              | T_comma | T_rparen -> true
              | _ -> false)
          in
          let field = normalize (words_of field_toks) in
          binds := (var, "$out." ^ field) :: !binds
        done;
        if not (tp_eq tp T_rparen) then tp_err tp "expected ')' after bind";
        tp_advance tp)
      else (
        match tp_peek tp with
        | T_ident _
          when tp.p + 1 < Array.length tp.a
               &&
               match tp.a.(tp.p + 1).t with
               | T_colon | T_assign -> true
               | _ -> false ->
          let name =
            match tp_peek tp with
            | T_ident s -> norm_key s
            | _ -> assert false
          in
          tp_advance tp;
          tp_advance tp;
          let toks =
            tp_take_until tp (fun k ->
              match k with
              | T_comma | T_rparen -> true
              | _ -> false)
          in
          args := (name, arg_value scope toks) :: !args
        | _ ->
          ignore line;
          tp_err tp "arguments must be named (name: value)")
    done;
    if not (tp_eq tp T_rparen) then tp_err tp "expected ')'";
    tp_advance tp)
  else ();
  List.rev !args, List.rev !binds
;;

type statement =
  | S_action of action
  | S_assign of string * string (* gate name, rendered value *)
  | S_let of string * string
  | S_stop

let parse_statement scope toks =
  match toks with
  | [ { t = T_ident "stop"; _ } ] -> S_stop
  | { t = T_ident "let"; _ } :: rest ->
    let tp = tp_make rest in
    let name_toks = tp_take_until tp (fun k -> tk_eq k T_assign) in
    if not (tp_eq tp T_assign) then tp_err tp "expected '=' in let";
    tp_advance tp;
    let rhs = tp_take_until tp (fun _ -> false) in
    let name = normalize (words_of name_toks) in
    S_let (name, render_expr scope rhs)
  | _ ->
    let tp = tp_make toks in
    let lhs = tp_take_until tp (fun k -> tk_eq k T_assign) in
    if tp_eq tp T_assign
    then (
      (* gate assignment: lhs must be a declared own-name *)
      tp_advance tp;
      let rhs = tp_take_until tp (fun _ -> false) in
      let name =
        match match_name scope.state_names (words_of lhs) with
        | Some (n, _) -> n
        | None ->
          err
            ~line:(tp_line tp)
            (Printf.sprintf
               "'%s' is not a remembered variable; declare it under 'remembers:'"
               (normalize (words_of lhs)))
      in
      S_assign (name, render_expr scope rhs))
    else (
      (* action call: name is the words up to '(' or end of line *)
      let tp = tp_make toks in
      let name_toks = tp_take_until tp (fun k -> tk_eq k T_lparen) in
      let name = normalize (words_of name_toks) in
      if String.equal name "" then tp_err tp "expected an action";
      let args, binds = parse_action_args scope tp in
      List.iter (fun (v, _) -> scope.locals <- v :: scope.locals) binds;
      S_action { a_name = name; a_args = args; a_bind = binds; a_on_error = O_stop })
;;

(* ────────────────────────── program structure ────────────────────────── *)

let line_colon l =
  match List.rev l.toks with
  | { t = T_colon; _ } :: _ -> true
  | _ -> false
;;

let strip_colon toks =
  match List.rev toks with
  | { t = T_colon; _ } :: rest -> List.rev rest
  | _ -> toks
;;

let first_word l =
  match l.toks with
  | { t = T_ident w; _ } :: _ -> Some (String.lowercase_ascii w)
  | _ -> None
;;

let child_indent lines i parent =
  if i < Array.length lines && lines.(i).indent > parent
  then Some lines.(i).indent
  else None
;;

let parse_state_block scope lines i header_indent =
  ignore scope;
  match child_indent lines i header_indent with
  | None -> err ~line:lines.(i - 1).ln "remembers: needs at least one variable"
  | Some body_indent ->
    let decls = ref []
    and j = ref i in
    while !j < Array.length lines && lines.(!j).indent = body_indent do
      let l = lines.(!j) in
      let name_toks, kind_toks =
        let rec split acc = function
          | [] -> List.rev acc, []
          | { t = T_colon; _ } :: rest -> List.rev acc, rest
          | t :: rest -> split (t :: acc) rest
        in
        split [] l.toks
      in
      if kind_toks = [] then err ~line:l.ln "expected '<type>' after ':'";
      let persist =
        List.exists (fun w -> String.equal w "persist") (words_of kind_toks)
      in
      let kind_words =
        List.filter (fun w -> not (String.equal w "persist")) (words_of kind_toks)
      in
      let has_q = List.exists (fun t -> tk_eq t.t T_qmark) kind_toks in
      let name = normalize (words_of name_toks) in
      let kind_name = normalize kind_words ^ if has_q then "?" else "" in
      (match state_kind_of_string kind_name with
       | k -> decls := { s_name = name; s_kind = k; s_persist = persist } :: !decls
       | exception _ -> err ~line:l.ln (Printf.sprintf "unknown type: %s" kind_name));
      incr j
    done;
    List.rev !decls, !j
;;

let sep_toks toks =
  let rec split acc = function
    | [] -> List.rev acc, None, []
    | { t = T_colon; _ } :: rest -> List.rev acc, Some ':', rest
    | { t = T_assign; _ } :: rest -> List.rev acc, Some '=', rest
    | t :: rest -> split (t :: acc) rest
  in
  split [] toks
;;

let parse_param_kind line kind_toks =
  let words = words_of kind_toks in
  let canonical = normalize words in
  match words with
  | "decimal_str" :: _ -> P_decimal_str
  | "range" :: _ -> P_range
  | "float" :: _ -> P_float
  | "int" :: _ -> P_int
  | "bool" :: _ -> P_bool
  | "string" :: _ -> P_string
  | "enum" :: _ ->
    let vals =
      List.filter_map
        (fun t ->
          match t.t with
          | T_string s -> Some s
          | _ -> None)
        kind_toks
    in
    if vals = [] then err ~line "enum needs at least one value (enum [\"a\", \"b\"])";
    P_enum vals
  | _ -> err ~line (Printf.sprintf "unknown parameter type: %s" canonical)
;;

let parse_param_default kind toks =
  let scalar =
    match toks with
    | [ { t = T_int i; _ } ] -> Some (`Int i)
    | [ { t = T_float f; _ } ] -> Some (`Float f)
    | [ { t = T_bool b; _ } ] -> Some (`Bool b)
    | [ { t = T_string s; _ } ] -> Some (`String s)
    | [] -> None
    | _ ->
      (match kind with
       | P_range ->
         let floats =
           List.filter_map
             (fun t ->
               match t.t with
               | T_float f -> Some (`Float f)
               | T_int i -> Some (`Float (float_of_int i))
               | _ -> None)
             toks
         in
         Some (`List floats)
       | _ -> None)
  in
  scalar
;;

let parse_params_block lines i header_indent =
  match child_indent lines i header_indent with
  | None -> [], i
  | Some body_indent ->
    let decls = ref []
    and j = ref i in
    while !j < Array.length lines && lines.(!j).indent = body_indent do
      let l = lines.(!j) in
      let name_toks, colon, after = sep_toks l.toks in
      if colon = None then err ~line:l.ln "expected ':' in parameter declaration";
      let kind_toks, default_toks =
        let rec split acc = function
          | [] -> List.rev acc, []
          | { t = T_assign; _ } :: rest -> List.rev acc, rest
          | t :: rest -> split (t :: acc) rest
        in
        split [] after
      in
      let kind = parse_param_kind l.ln kind_toks in
      let default = parse_param_default kind default_toks in
      decls
      := { p_name = normalize (words_of name_toks); p_kind = kind; p_default = default }
         :: !decls;
      incr j
    done;
    List.rev !decls, !j
;;

let lifecycle_kinds =
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

let kind_to_group = function
  | "filled" -> "fill"
  | k when List.mem k lifecycle_kinds -> "order_lifecycle"
  | k -> k
;;

let valid_kind k = List.mem k ("filled" :: lifecycle_kinds)

let trigger_phrases =
  [ "book.updates", [ "book_update" ]
  ; "book.update", [ "book_update" ]
  ; "balance.updates", [ "balance_update" ]
  ; "balance.update", [ "balance_update" ]
  ; "oracle.publishes", [ "oracle_publish" ]
  ; "oracle.publish", [ "oracle_publish" ]
  ; "order.fills", [ "filled" ]
  ; "fill.happens", [ "filled" ]
  ; "any.order.event", lifecycle_kinds
  ; "order.event", lifecycle_kinds
  ]
;;

let resolve_trigger words =
  let phrase = String.concat "." (List.map String.lowercase_ascii words) in
  match List.assoc_opt phrase trigger_phrases with
  | Some kinds -> kinds
  | None ->
    let prefix = "order.is." in
    let pl = String.length prefix in
    if String.length phrase > pl && String.equal (String.sub phrase 0 pl) prefix
    then (
      let kind = normalize [ String.sub phrase pl (String.length phrase - pl) ] in
      if valid_kind kind
      then [ kind ]
      else err (Printf.sprintf "unknown order kind: %s" kind))
    else err (Printf.sprintf "unrecognised trigger: 'when %s'" (String.concat " " words))
;;

type raw_step =
  { r_id : string
  ; r_lets : (string * string) list
  ; r_when : guard option
  ; r_then : action list
  ; r_else : action list
  ; r_stop : bool
  }

let set_gate_action name value =
  { a_name = "set_gate"
  ; a_args = [ "name", `String name; "value", `String value ]
  ; a_bind = []
  ; a_on_error = O_stop
  }
;;

let parse_action_lines scope lines i parent_indent =
  match child_indent lines i parent_indent with
  | None -> [], false, i
  | Some body_indent ->
    let acts = ref []
    and stop = ref false
    and j = ref i in
    while !j < Array.length lines && lines.(!j).indent = body_indent do
      let l = lines.(!j) in
      (match parse_statement scope l.toks with
       | S_action a -> acts := a :: !acts
       | S_assign (n, v) -> acts := set_gate_action n v :: !acts
       | S_stop -> stop := true
       | S_let _ -> err ~line:l.ln "'let' is only allowed directly in a step");
      incr j
    done;
    List.rev !acts, !stop, !j
;;

let parse_step_body scope lines i header_indent header_ln =
  let lets = ref []
  and when_ = ref None
  and then_ = ref []
  and else_ = ref []
  and stop = ref false in
  let body_indent =
    match child_indent lines i header_indent with
    | None -> err ~line:header_ln "step has an empty body"
    | Some bi -> bi
  in
  let j = ref i in
  let finished = ref false in
  while (not !finished) && !j < Array.length lines && lines.(!j).indent = body_indent do
    let l = lines.(!j) in
    let fw = first_word l in
    if fw = Some "let"
    then (
      (match parse_statement scope l.toks with
       | S_let (n, e) ->
         lets := (n, e) :: !lets;
         scope.locals <- n :: scope.locals
       | _ -> err ~line:l.ln "expected 'let' binding");
      incr j)
    else if fw = Some "if" || fw = Some "when"
    then (
      let cond_toks =
        match l.toks with
        | _ :: rest -> rest
        | [] -> []
      in
      when_ := Some (parse_condition scope (tp_make (strip_colon cond_toks)));
      let acts, st, nj = parse_action_lines scope lines (!j + 1) body_indent in
      then_ := List.rev acts;
      if st then stop := true;
      j := nj;
      if !j < Array.length lines
         && lines.(!j).indent = body_indent
         && (first_word lines.(!j) = Some "otherwise"
             || first_word lines.(!j) = Some "else")
      then (
        let acts2, st2, nj2 = parse_action_lines scope lines (!j + 1) body_indent in
        else_ := List.rev acts2;
        if st2 then stop := true;
        j := nj2);
      finished := true)
    else (
      (match parse_statement scope l.toks with
       | S_action a -> then_ := a :: !then_
       | S_assign (n, v) -> then_ := set_gate_action n v :: !then_
       | S_stop -> stop := true
       | S_let (n, e) ->
         lets := (n, e) :: !lets;
         scope.locals <- n :: scope.locals);
      incr j)
  done;
  ( { r_id = ""
    ; r_lets = List.rev !lets
    ; r_when = !when_
    ; r_then = List.rev !then_
    ; r_else = List.rev !else_
    ; r_stop = !stop
    }
  , !j )
;;

let parse_group scope lines i kinds =
  let header = lines.(i) in
  match child_indent lines (i + 1) header.indent with
  | None -> err ~line:header.ln "trigger group has an empty body"
  | Some step_indent ->
    let steps = ref []
    and j = ref (i + 1) in
    while !j < Array.length lines && lines.(!j).indent = step_indent do
      let sl = lines.(!j) in
      if not (line_colon sl) then err ~line:sl.ln "expected a step label ending in ':'";
      let label_toks = strip_colon sl.toks in
      let label = normalize (words_of label_toks) in
      if String.equal label "" then err ~line:sl.ln "step label cannot be empty";
      (* fresh local scope for each step, seeded with the labels that bind in it *)
      let step_scope = { scope with locals = [] } in
      let body, nj = parse_step_body step_scope lines (!j + 1) step_indent sl.ln in
      List.iter
        (fun kind ->
          let id =
            if List.length kinds = 1 then label else Printf.sprintf "%s_%s" label kind
          in
          let event_guard = G_event kind in
          let when_ =
            match body.r_when with
            | Some cond -> Some (G_all [ event_guard; cond ])
            | None -> Some event_guard
          in
          steps
          := { st_id = id
             ; st_let = body.r_lets
             ; st_when = when_
             ; st_then = body.r_then
             ; st_else = body.r_else
             ; st_stop = body.r_stop
             }
             :: !steps)
        kinds;
      j := nj
    done;
    List.rev !steps, !j
;;

let parse_program (lines : lline array) : t =
  if Array.length lines = 0 then err "empty strategy file";
  let n = Array.length lines in
  let i = ref 0 in
  (* header *)
  let header = lines.(0) in
  let htoks = header.toks in
  (match htoks with
   | { t = T_ident "strategy"; _ } :: rest -> ignore rest
   | _ -> err ~line:header.ln "expected 'strategy <name>' on the first line");
  let name_words, version_inline =
    let rec split acc = function
      | [] -> List.rev acc, None
      | { t = T_ident "version"; _ } :: { t = T_int v; _ } :: _ -> List.rev acc, Some v
      | t :: rest -> split (t :: acc) rest
    in
    split [] (List.tl htoks)
  in
  let name = normalize (words_of name_words) in
  if String.equal name "" then err ~line:header.ln "strategy name is empty";
  incr i;
  let version =
    match version_inline with
    | Some v -> v
    | None ->
      if !i < n && lines.(!i).indent = 0 && first_word lines.(!i) = Some "version"
      then (
        let v =
          match
            List.filter_map
              (fun t ->
                match t.t with
                | T_int v -> Some v
                | _ -> None)
              lines.(!i).toks
          with
          | v :: _ -> v
          | [] -> 1
        in
        incr i;
        v)
      else 1
  in
  let state = ref []
  and params = ref [] in
  if !i < n && lines.(!i).indent = 0 && first_word lines.(!i) = Some "remembers"
  then (
    let decls, nj =
      parse_state_block { state_names = []; locals = [] } lines (!i + 1) lines.(!i).indent
    in
    state := decls;
    i := nj);
  let scope =
    { state_names = List.map (fun (s : state_decl) -> s.s_name) !state; locals = [] }
  in
  if !i < n && lines.(!i).indent = 0 && first_word lines.(!i) = Some "tunable"
  then (
    let decls, nj = parse_params_block lines (!i + 1) lines.(!i).indent in
    params := decls;
    i := nj);
  let steps = ref []
  and trigger_groups = ref [] in
  while !i < n do
    let l = lines.(!i) in
    if l.indent <> 0 then err ~line:l.ln "unexpected indentation at top level";
    match first_word l with
    | Some "when" when line_colon l ->
      let phrase = words_of (strip_colon (List.tl l.toks)) in
      let kinds = resolve_trigger phrase in
      List.iter
        (fun kind ->
          let grp = kind_to_group kind in
          if not (List.mem grp !trigger_groups)
          then trigger_groups := !trigger_groups @ [ grp ])
        kinds;
      let gsteps, nj = parse_group scope lines !i kinds in
      steps := !steps @ gsteps;
      i := nj
    | Some w -> err ~line:l.ln (Printf.sprintf "unexpected top-level statement: %s" w)
    | None -> err ~line:l.ln "expected a 'when ...:' group"
  done;
  { name
  ; version
  ; triggers = !trigger_groups
  ; params = !params
  ; state = !state
  ; steps = !steps
  }
;;

(* ────────────────────────── public API ────────────────────────── *)

let parse_string (s : string) : (t, string) result =
  try Ok (parse_program (Array.of_list (logical_lines (lex_tokens s)))) with
  | Script_error (msg, line, col) ->
    Error (Printf.sprintf "script:%d:%d: %s" line col msg)
  | Failure msg -> Error ("script: " ^ msg)
  | exn -> Error ("script: " ^ Printexc.to_string exn)
;;

let parse_file (path : string) : (t, string) result =
  try
    let ch = open_in path in
    let len = in_channel_length ch in
    let buf = Buffer.create len in
    Buffer.add_channel buf ch len;
    close_in ch;
    parse_string (Buffer.contents buf)
  with
  | Sys_error msg -> Error ("script: " ^ msg)
  | exn -> Error ("script: " ^ Printexc.to_string exn)
;;
