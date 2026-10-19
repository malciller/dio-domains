(** In-tree JSON parser and printer.

    Implements the [Yojson.Safe], [Yojson.Basic] and [Util] surface the engine uses.
    Standard JSON, UTF-8, escaped strings and large integers are handled; [Intlit]
    preserves integers outside OCaml's [int] range. *)

exception Json_error of string

module Safe = struct
  type t =
    [ `Null
    | `Bool of bool
    | `Int of int
    | `Intlit of string
    | `Float of float
    | `String of string
    | `Assoc of (string * t) list
    | `List of t list
    | `Tuple of t list
    | `Variant of string * t option
    ]

  let error msg = raise (Json_error msg)

  let add_utf8 b u =
    if u < 0x80
    then Buffer.add_char b (Char.chr u)
    else if u < 0x800
    then (
      Buffer.add_char b (Char.chr (0xC0 lor (u lsr 6)));
      Buffer.add_char b (Char.chr (0x80 lor (u land 0x3F))))
    else if u < 0x10000
    then (
      Buffer.add_char b (Char.chr (0xE0 lor (u lsr 12)));
      Buffer.add_char b (Char.chr (0x80 lor ((u lsr 6) land 0x3F)));
      Buffer.add_char b (Char.chr (0x80 lor (u land 0x3F))))
    else (
      Buffer.add_char b (Char.chr (0xF0 lor (u lsr 18)));
      Buffer.add_char b (Char.chr (0x80 lor ((u lsr 12) land 0x3F)));
      Buffer.add_char b (Char.chr (0x80 lor ((u lsr 6) land 0x3F)));
      Buffer.add_char b (Char.chr (0x80 lor (u land 0x3F))))
  ;;

  let from_string (s : string) : t =
    let n = String.length s in
    let i = ref 0 in
    let fail msg = error (Printf.sprintf "Line 1, bytes %d: %s" !i msg) in
    let peek () = if !i < n then Some s.[!i] else None in
    let skip_ws () =
      while
        !i < n
        &&
        match s.[!i] with
        | ' ' | '\t' | '\r' | '\n' -> true
        | _ -> false
      do
        incr i
      done
    in
    let expect c =
      if !i < n && s.[!i] = c then incr i else fail (Printf.sprintf "expected %C" c)
    in
    let hex_value c =
      match c with
      | '0' .. '9' -> Char.code c - Char.code '0'
      | 'a' .. 'f' -> Char.code c - Char.code 'a' + 10
      | 'A' .. 'F' -> Char.code c - Char.code 'A' + 10
      | _ -> fail "invalid hex digit"
    in
    let hex4 () =
      if !i + 4 > n then fail "truncated unicode escape";
      let v =
        (hex_value s.[!i] lsl 12)
        lor (hex_value s.[!i + 1] lsl 8)
        lor (hex_value s.[!i + 2] lsl 4)
        lor hex_value s.[!i + 3]
      in
      i := !i + 4;
      v
    in
    let parse_string () =
      expect '"';
      let b = Buffer.create 32 in
      let rec loop () =
        if !i >= n then fail "unterminated string";
        match s.[!i] with
        | '"' ->
          incr i;
          Buffer.contents b
        | '\\' ->
          incr i;
          if !i >= n then fail "unterminated escape";
          if s.[!i] = 'u'
          then (
            incr i;
            let u = hex4 () in
            if u >= 0xD800 && u <= 0xDBFF
            then (
              if !i + 2 > n || s.[!i] <> '\\' || s.[!i + 1] <> 'u'
              then fail "missing low surrogate";
              i := !i + 2;
              let lo = hex4 () in
              if lo < 0xDC00 || lo > 0xDFFF then fail "invalid low surrogate";
              add_utf8 b (0x10000 + ((u - 0xD800) lsl 10) + (lo - 0xDC00)))
            else if u >= 0xDC00 && u <= 0xDFFF
            then fail "unexpected low surrogate"
            else add_utf8 b u;
            loop ())
          else (
            (match s.[!i] with
             | '"' -> Buffer.add_char b '"'
             | '\\' -> Buffer.add_char b '\\'
             | '/' -> Buffer.add_char b '/'
             | 'b' -> Buffer.add_char b '\b'
             | 'f' -> Buffer.add_char b '\012'
             | 'n' -> Buffer.add_char b '\n'
             | 'r' -> Buffer.add_char b '\r'
             | 't' -> Buffer.add_char b '\t'
             | _ -> fail "invalid escape");
            incr i;
            loop ())
        | c when Char.code c < 0x20 -> fail "unescaped control character"
        | c ->
          Buffer.add_char b c;
          incr i;
          loop ()
      in
      loop ()
    in
    let parse_number () =
      let start = !i in
      (match peek () with
       | Some '-' -> incr i
       | _ -> ());
      let digits_seen = ref false in
      while
        !i < n
        &&
        match s.[!i] with
        | '0' .. '9' ->
          digits_seen := true;
          true
        | _ -> false
      do
        incr i
      done;
      if not !digits_seen then fail "invalid number";
      let is_float = ref false in
      (match peek () with
       | Some '.' ->
         is_float := true;
         incr i;
         let d = ref false in
         while
           !i < n
           &&
           match s.[!i] with
           | '0' .. '9' ->
             d := true;
             true
           | _ -> false
         do
           incr i
         done;
         if not !d then fail "invalid fraction"
       | _ -> ());
      (match peek () with
       | Some ('e' | 'E') ->
         is_float := true;
         incr i;
         (match peek () with
          | Some ('+' | '-') -> incr i
          | _ -> ());
         let d = ref false in
         while
           !i < n
           &&
           match s.[!i] with
           | '0' .. '9' ->
             d := true;
             true
           | _ -> false
         do
           incr i
         done;
         if not !d then fail "invalid exponent"
       | _ -> ());
      let token = String.sub s start (!i - start) in
      if !is_float
      then `Float (float_of_string token)
      else (
        match int_of_string_opt token with
        | Some v -> `Int v
        | None -> `Intlit token)
    in
    let rec parse_value () : t =
      skip_ws ();
      match peek () with
      | None -> fail "unexpected end of input"
      | Some '{' ->
        incr i;
        skip_ws ();
        (match peek () with
         | Some '}' ->
           incr i;
           `Assoc []
         | _ ->
           let rec fields acc =
             skip_ws ();
             match peek () with
             | Some '"' ->
               let k = parse_string () in
               skip_ws ();
               expect ':';
               let v = parse_value () in
               skip_ws ();
               (match peek () with
                | Some ',' ->
                  incr i;
                  fields ((k, v) :: acc)
                | Some '}' ->
                  incr i;
                  `Assoc (List.rev ((k, v) :: acc))
                | _ -> fail "expected ',' or '}'")
             | _ -> fail "expected object key"
           in
           fields [])
      | Some '[' ->
        incr i;
        skip_ws ();
        (match peek () with
         | Some ']' ->
           incr i;
           `List []
         | _ ->
           let rec items acc =
             let v = parse_value () in
             skip_ws ();
             match peek () with
             | Some ',' ->
               incr i;
               items (v :: acc)
             | Some ']' ->
               incr i;
               `List (List.rev (v :: acc))
             | _ -> fail "expected ',' or ']'"
           in
           items [])
      | Some '"' -> `String (parse_string ())
      | Some 't' ->
        if !i + 4 <= n && String.sub s !i 4 = "true"
        then (
          i := !i + 4;
          `Bool true)
        else fail "invalid token"
      | Some 'f' ->
        if !i + 5 <= n && String.sub s !i 5 = "false"
        then (
          i := !i + 5;
          `Bool false)
        else fail "invalid token"
      | Some 'n' ->
        if !i + 4 <= n && String.sub s !i 4 = "null"
        then (
          i := !i + 4;
          `Null)
        else fail "invalid token"
      | Some ('-' | '0' .. '9') -> parse_number ()
      | Some c -> fail (Printf.sprintf "invalid token %C" c)
    in
    let v = parse_value () in
    skip_ws ();
    if !i <> n then fail "junk after end of JSON value";
    v
  ;;

  let escape_string b s =
    Buffer.add_char b '"';
    String.iter
      (fun c ->
        match c with
        | '"' -> Buffer.add_string b "\\\""
        | '\\' -> Buffer.add_string b "\\\\"
        | '\b' -> Buffer.add_string b "\\b"
        | '\012' -> Buffer.add_string b "\\f"
        | '\n' -> Buffer.add_string b "\\n"
        | '\r' -> Buffer.add_string b "\\r"
        | '\t' -> Buffer.add_string b "\\t"
        | c when Char.code c < 0x20 ->
          Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
        | c -> Buffer.add_char b c)
      s;
    Buffer.add_char b '"'
  ;;

  let float_to_string f =
    if Float.is_nan f || Float.is_infinite f
    then "null"
    else (
      let s = Printf.sprintf "%.17g" f in
      if String.contains s '.' || String.contains s 'e' || String.contains s 'E'
      then s
      else s ^ ".0")
  ;;

  let rec write (b : Buffer.t) (json : t) : unit =
    match json with
    | `Null -> Buffer.add_string b "null"
    | `Bool true -> Buffer.add_string b "true"
    | `Bool false -> Buffer.add_string b "false"
    | `Int i -> Buffer.add_string b (string_of_int i)
    | `Intlit s -> Buffer.add_string b s
    | `Float f -> Buffer.add_string b (float_to_string f)
    | `String s -> escape_string b s
    | `Assoc xs ->
      Buffer.add_char b '{';
      List.iteri
        (fun i (k, v) ->
          if i > 0 then Buffer.add_char b ',';
          escape_string b k;
          Buffer.add_char b ':';
          write b v)
        xs;
      Buffer.add_char b '}'
    | `List xs | `Tuple xs ->
      Buffer.add_char b '[';
      List.iteri
        (fun i v ->
          if i > 0 then Buffer.add_char b ',';
          write b v)
        xs;
      Buffer.add_char b ']'
    | `Variant (name, None) -> escape_string b name
    | `Variant (name, Some v) ->
      Buffer.add_char b '[';
      escape_string b name;
      Buffer.add_char b ',';
      write b v;
      Buffer.add_char b ']'
  ;;

  let to_string (t : t) : string =
    let b = Buffer.create 256 in
    write b t;
    Buffer.contents b
  ;;

  let pretty_to_string (t : t) : string =
    let b = Buffer.create 512 in
    let rec pretty indent = function
      | `Assoc [] -> Buffer.add_string b "{}"
      | `List [] -> Buffer.add_string b "[]"
      | `Assoc xs ->
        Buffer.add_string b "{\n";
        List.iteri
          (fun i (k, v) ->
            if i > 0 then Buffer.add_string b ",\n";
            Buffer.add_string b (String.make (indent + 2) ' ');
            escape_string b k;
            Buffer.add_string b ": ";
            pretty (indent + 2) v)
          xs;
        Buffer.add_char b '\n';
        Buffer.add_string b (String.make indent ' ');
        Buffer.add_char b '}'
      | `List xs | `Tuple xs ->
        Buffer.add_string b "[\n";
        List.iteri
          (fun i v ->
            if i > 0 then Buffer.add_string b ",\n";
            Buffer.add_string b (String.make (indent + 2) ' ');
            pretty (indent + 2) v)
          xs;
        Buffer.add_char b '\n';
        Buffer.add_string b (String.make indent ' ');
        Buffer.add_char b ']'
      | v -> write b v
    in
    pretty 0 t;
    Buffer.contents b
  ;;

  let from_file (path : string) : t =
    let ic = open_in_bin path in
    Fun.protect
      ~finally:(fun () -> close_in_noerr ic)
      (fun () -> really_input_string ic (in_channel_length ic))
    |> from_string
  ;;

  let to_file (path : string) (t : t) : unit =
    let oc = open_out_bin path in
    Fun.protect
      ~finally:(fun () -> close_out_noerr oc)
      (fun () -> output_string oc (to_string t))
  ;;

  module Util = struct
    exception Type_error of string * t
    exception Undefined of string * t

    let typeof = function
      | `Assoc _ -> "object"
      | `Bool _ -> "bool"
      | `Float _ -> "float"
      | `Int _ -> "int"
      | `Intlit _ -> "intlit"
      | `List _ -> "array"
      | `Null -> "null"
      | `String _ -> "string"
      | `Tuple _ -> "tuple"
      | `Variant _ -> "variant"
    ;;

    let typerr msg js = raise (Type_error (msg ^ typeof js, js))

    let assoc name obj =
      try List.assoc name obj with
      | Not_found -> `Null
    ;;

    let member name = function
      | `Assoc obj -> assoc name obj
      | js -> typerr ("Can't get member '" ^ name ^ "' of non-object type ") js
    ;;

    let rec path l obj =
      match l with
      | [] -> Some obj
      | key :: rest ->
        (match obj with
         | `Assoc xs ->
           (match List.assoc_opt key xs with
            | Some v -> path rest v
            | None -> None)
         | _ -> None)
    ;;

    let index i = function
      | `List l as js ->
        let len = List.length l in
        let wrapped = if i < 0 then len + i else i in
        if wrapped < 0 || wrapped >= len
        then raise (Undefined ("Index " ^ string_of_int i ^ " out of bounds", js))
        else List.nth l wrapped
      | js -> typerr ("Can't get index " ^ string_of_int i ^ " of non-array type ") js
    ;;

    let map f = function
      | `List l -> `List (List.map f l)
      | js -> typerr "Can't map function over non-array type " js
    ;;

    let to_assoc = function
      | `Assoc obj -> obj
      | js -> typerr "Expected object, got " js
    ;;

    let to_option f = function
      | `Null -> None
      | x -> Some (f x)
    ;;

    let to_bool = function
      | `Bool b -> b
      | js -> typerr "Expected bool, got " js
    ;;

    let to_bool_option = function
      | `Bool b -> Some b
      | `Null -> None
      | js -> typerr "Expected bool or null, got " js
    ;;

    let to_number = function
      | `Int i -> float_of_int i
      | `Float f -> f
      | js -> typerr "Expected number, got " js
    ;;

    let to_number_option = function
      | `Int i -> Some (float_of_int i)
      | `Float f -> Some f
      | `Null -> None
      | js -> typerr "Expected number or null, got " js
    ;;

    let to_float = function
      | `Float f -> f
      | js -> typerr "Expected float, got " js
    ;;

    let to_float_option = function
      | `Float f -> Some f
      | `Null -> None
      | js -> typerr "Expected float or null, got " js
    ;;

    let to_int = function
      | `Int i -> i
      | js -> typerr "Expected int, got " js
    ;;

    let to_int_option = function
      | `Int i -> Some i
      | `Null -> None
      | js -> typerr "Expected int or null, got " js
    ;;

    let to_list = function
      | `List l -> l
      | js -> typerr "Expected array, got " js
    ;;

    let to_string = function
      | `String s -> s
      | js -> typerr "Expected string, got " js
    ;;

    let to_string_option = function
      | `String s -> Some s
      | `Null -> None
      | js -> typerr "Expected string or null, got " js
    ;;

    let convert_each f = function
      | `List l -> List.map f l
      | js -> typerr "Can't convert each element of non-array type " js
    ;;

    let rec rev_filter_map f acc l =
      match l with
      | [] -> acc
      | x :: tl ->
        (match f x with
         | None -> rev_filter_map f acc tl
         | Some y -> rev_filter_map f (y :: acc) tl)
    ;;

    let filter_map f l = List.rev (rev_filter_map f [] l)

    let rec rev_flatten acc l =
      match l with
      | [] -> acc
      | x :: tl ->
        (match x with
         | `List l2 -> rev_flatten (List.rev_append l2 acc) tl
         | _ -> rev_flatten acc tl)
    ;;

    let flatten l = List.rev (rev_flatten [] l)

    let filter_index i l =
      filter_map
        (function
          | `List l ->
            (try Some (List.nth l i) with
             | _ -> None)
          | _ -> None)
        l
    ;;

    let filter_list l =
      filter_map
        (function
          | `List l -> Some l
          | _ -> None)
        l
    ;;

    let filter_member k l =
      filter_map
        (function
          | `Assoc o ->
            (try Some (List.assoc k o) with
             | _ -> None)
          | _ -> None)
        l
    ;;

    let filter_assoc l =
      filter_map
        (function
          | `Assoc o -> Some o
          | _ -> None)
        l
    ;;

    let filter_bool l =
      filter_map
        (function
          | `Bool b -> Some b
          | _ -> None)
        l
    ;;

    let filter_int l =
      filter_map
        (function
          | `Int i -> Some i
          | _ -> None)
        l
    ;;

    let filter_float l =
      filter_map
        (function
          | `Float f -> Some f
          | _ -> None)
        l
    ;;

    let filter_number l =
      filter_map
        (function
          | `Int i -> Some (float_of_int i)
          | `Float f -> Some f
          | _ -> None)
        l
    ;;

    let filter_string l =
      filter_map
        (function
          | `String s -> Some s
          | _ -> None)
        l
    ;;

    let keys o = to_assoc o |> List.map (fun (k, _) -> k)
    let values o = to_assoc o |> List.map (fun (_, v) -> v)

    let combine first second =
      match first, second with
      | `Assoc a, `Assoc b -> `Assoc (a @ b)
      | _ -> invalid_arg "Expected two objects, check inputs"
    ;;
  end
end

module Basic = struct
  type t =
    [ `Null
    | `Bool of bool
    | `Int of int
    | `Intlit of string
    | `Float of float
    | `String of string
    | `Assoc of (string * t) list
    | `List of t list
    ]

  let rec of_safe : Safe.t -> t = function
    | `Null -> `Null
    | `Bool b -> `Bool b
    | `Int i -> `Int i
    | `Intlit s -> `Intlit s
    | `Float f -> `Float f
    | `String s -> `String s
    | `Assoc xs -> `Assoc (List.map (fun (k, v) -> k, of_safe v) xs)
    | `List xs -> `List (List.map of_safe xs)
    | `Tuple _ | `Variant _ -> raise (Json_error "Basic: tuple or variant in JSON")
  ;;

  let rec to_safe : t -> Safe.t = function
    | `Null -> `Null
    | `Bool b -> `Bool b
    | `Int i -> `Int i
    | `Intlit s -> `Intlit s
    | `Float f -> `Float f
    | `String s -> `String s
    | `Assoc xs -> `Assoc (List.map (fun (k, v) -> k, to_safe v) xs)
    | `List xs -> `List (List.map to_safe xs)
  ;;

  let from_string (s : string) : t = of_safe (Safe.from_string s)
  let from_file (path : string) : t = of_safe (Safe.from_file path)
  let to_string (t : t) : string = Safe.to_string (to_safe t)
  let pretty_to_string (t : t) : string = Safe.pretty_to_string (to_safe t)
  let to_file (path : string) (t : t) : unit = Safe.to_file path (to_safe t)

  module Util = struct
    exception Type_error of string * t
    exception Undefined of string * t

    let typeof = function
      | `Assoc _ -> "object"
      | `Bool _ -> "bool"
      | `Float _ -> "float"
      | `Int _ -> "int"
      | `Intlit _ -> "intlit"
      | `List _ -> "array"
      | `Null -> "null"
      | `String _ -> "string"
    ;;

    let typerr msg js = raise (Type_error (msg ^ typeof js, js))

    let assoc name obj =
      try List.assoc name obj with
      | Not_found -> `Null
    ;;

    let member name = function
      | `Assoc obj -> assoc name obj
      | js -> typerr ("Can't get member '" ^ name ^ "' of non-object type ") js
    ;;

    let rec path l obj =
      match l with
      | [] -> Some obj
      | key :: rest ->
        (match obj with
         | `Assoc xs ->
           (match List.assoc_opt key xs with
            | Some v -> path rest v
            | None -> None)
         | _ -> None)
    ;;

    let index i = function
      | `List l as js ->
        let len = List.length l in
        let wrapped = if i < 0 then len + i else i in
        if wrapped < 0 || wrapped >= len
        then raise (Undefined ("Index " ^ string_of_int i ^ " out of bounds", js))
        else List.nth l wrapped
      | js -> typerr ("Can't get index " ^ string_of_int i ^ " of non-array type ") js
    ;;

    let map f = function
      | `List l -> `List (List.map f l)
      | js -> typerr "Can't map function over non-array type " js
    ;;

    let to_assoc = function
      | `Assoc obj -> obj
      | js -> typerr "Expected object, got " js
    ;;

    let to_option f = function
      | `Null -> None
      | x -> Some (f x)
    ;;

    let to_bool = function
      | `Bool b -> b
      | js -> typerr "Expected bool, got " js
    ;;

    let to_bool_option = function
      | `Bool b -> Some b
      | `Null -> None
      | js -> typerr "Expected bool or null, got " js
    ;;

    let to_number = function
      | `Int i -> float_of_int i
      | `Float f -> f
      | js -> typerr "Expected number, got " js
    ;;

    let to_number_option = function
      | `Int i -> Some (float_of_int i)
      | `Float f -> Some f
      | `Null -> None
      | js -> typerr "Expected number or null, got " js
    ;;

    let to_float = function
      | `Float f -> f
      | js -> typerr "Expected float, got " js
    ;;

    let to_float_option = function
      | `Float f -> Some f
      | `Null -> None
      | js -> typerr "Expected float or null, got " js
    ;;

    let to_int = function
      | `Int i -> i
      | js -> typerr "Expected int, got " js
    ;;

    let to_int_option = function
      | `Int i -> Some i
      | `Null -> None
      | js -> typerr "Expected int or null, got " js
    ;;

    let to_list = function
      | `List l -> l
      | js -> typerr "Expected array, got " js
    ;;

    let to_string = function
      | `String s -> s
      | js -> typerr "Expected string, got " js
    ;;

    let to_string_option = function
      | `String s -> Some s
      | `Null -> None
      | js -> typerr "Expected string or null, got " js
    ;;

    let convert_each f = function
      | `List l -> List.map f l
      | js -> typerr "Can't convert each element of non-array type " js
    ;;

    let rec rev_filter_map f acc l =
      match l with
      | [] -> acc
      | x :: tl ->
        (match f x with
         | None -> rev_filter_map f acc tl
         | Some y -> rev_filter_map f (y :: acc) tl)
    ;;

    let filter_map f l = List.rev (rev_filter_map f [] l)

    let rec rev_flatten acc l =
      match l with
      | [] -> acc
      | x :: tl ->
        (match x with
         | `List l2 -> rev_flatten (List.rev_append l2 acc) tl
         | _ -> rev_flatten acc tl)
    ;;

    let flatten l = List.rev (rev_flatten [] l)

    let filter_index i l =
      filter_map
        (function
          | `List l ->
            (try Some (List.nth l i) with
             | _ -> None)
          | _ -> None)
        l
    ;;

    let filter_list l =
      filter_map
        (function
          | `List l -> Some l
          | _ -> None)
        l
    ;;

    let filter_member k l =
      filter_map
        (function
          | `Assoc o ->
            (try Some (List.assoc k o) with
             | _ -> None)
          | _ -> None)
        l
    ;;

    let filter_assoc l =
      filter_map
        (function
          | `Assoc o -> Some o
          | _ -> None)
        l
    ;;

    let filter_bool l =
      filter_map
        (function
          | `Bool b -> Some b
          | _ -> None)
        l
    ;;

    let filter_int l =
      filter_map
        (function
          | `Int i -> Some i
          | _ -> None)
        l
    ;;

    let filter_float l =
      filter_map
        (function
          | `Float f -> Some f
          | _ -> None)
        l
    ;;

    let filter_number l =
      filter_map
        (function
          | `Int i -> Some (float_of_int i)
          | `Float f -> Some f
          | _ -> None)
        l
    ;;

    let filter_string l =
      filter_map
        (function
          | `String s -> Some s
          | _ -> None)
        l
    ;;

    let keys o = to_assoc o |> List.map (fun (k, _) -> k)
    let values o = to_assoc o |> List.map (fun (_, v) -> v)

    let combine first second =
      match first, second with
      | `Assoc a, `Assoc b -> `Assoc (a @ b)
      | _ -> invalid_arg "Expected two objects, check inputs"
    ;;
  end
end
