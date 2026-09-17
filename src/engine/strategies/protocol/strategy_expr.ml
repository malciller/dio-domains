(** Expressions and references for strategy files.

    Provides [$ref] scanning (used by the validator) and a small expression engine —
    literals, references, arithmetic, comparison and boolean operators — compiled to a
    result-returning evaluator over an [env]. String arguments that do not parse as
    expressions are treated as templates: [$refs] are substituted into the text. *)

type scope =
  | Price
  | Event
  | State
  | Param
  | Signal
  | Local
  | Now
  | Platform

type ref_ =
  { scope : scope
  ; path : string list
  ; slot : int (* interned index of the dotted key, for slot-addressed env scopes *)
  }

(** Process-wide string->slot intern table. Strategy files use a small, fixed set of fact
    and state keys; resolving each to an integer once (at parse time) lets the hot
    evaluator read them by array index instead of hashing the key string on every cycle
    (the same shared predicate appears in many guards).

    All domains load strategy files and mutate runtime state concurrently, so the table
    cannot be mutated in place: an unsynchronized [Hashtbl] shared across OCaml 5 domains
    is undefined behavior (a concurrent resize can wedge a lookup in a long probe), which
    shows up as exactly the kind of intermittent per-thread CPU stall we were chasing.

    The table is therefore append-only and published as an immutable snapshot via
    [Atomic]. Hits (the entire runtime path) are lock-free: they read the current snapshot
    and never touch the mutex, so concurrent domains cannot convoy on each other. Only a
    miss (a genuinely new key, which happens while files are parsed and on first use)
    takes [key_intern_mutex] and republishes a copy; a published table is never mutated
    again, so a reader that captured the previous snapshot still reads it safely. *)
let key_intern : (string, int) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 256)

let key_intern_mutex = Mutex.create ()
let key_next = Atomic.make 0

let intern_key (s : string) : int =
  let t = Atomic.get key_intern in
  match Hashtbl.find t s with
  | i -> i
  | exception Not_found ->
    Mutex.lock key_intern_mutex;
    let t = Atomic.get key_intern in
    let i =
      match Hashtbl.find t s with
      | i -> i
      | exception Not_found ->
        let i = Atomic.get key_next in
        Atomic.set key_next (i + 1);
        let t' = Hashtbl.copy t in
        Hashtbl.replace t' s i;
        Atomic.set key_intern t';
        i
    in
    Mutex.unlock key_intern_mutex;
    i
;;

let interned_key_count () = Atomic.get key_next

type value = Strategy_actions.value =
  | V_unset
  | V_none
  | V_float of float
  | V_int of int
  | V_bool of bool
  | V_string of string

let scope_of_string = function
  | "price" -> Some Price
  | "event" -> Some Event
  | "state" -> Some State
  | "params" -> Some Param
  | "signal" -> Some Signal
  | "local" -> Some Local
  | "now" -> Some Now
  | "platform" -> Some Platform
  | _ -> None
;;

let is_ref_char c =
  (c >= 'a' && c <= 'z')
  || (c >= 'A' && c <= 'Z')
  || (c >= '0' && c <= '9')
  || c = '_'
  || c = '.'
;;

(** Extract raw "$..." tokens (without the leading '$'). *)
let raw_refs (s : string) : string list =
  let n = String.length s in
  let out = ref [] in
  let i = ref 0 in
  while !i < n do
    if s.[!i] = '$'
    then (
      let j = ref (!i + 1) in
      while !j < n && is_ref_char s.[!j] do
        incr j
      done;
      if !j > !i + 1 then out := String.sub s (!i + 1) (!j - !i - 1) :: !out;
      i := !j)
    else incr i
  done;
  List.rev !out
;;

let parse_ref (raw : string) : (ref_, string) result =
  match String.split_on_char '.' raw with
  | [] -> Error ("empty reference: " ^ raw)
  | head :: path ->
    (match scope_of_string head with
     | Some scope ->
       let slot =
         match scope, path with
         | (State | Platform | Local | Signal), _ -> intern_key (String.concat "." path)
         | Param, [ n ] -> intern_key n
         | Param, _ -> intern_key (String.concat "." path)
         | _ -> 0
       in
       Ok { scope; path; slot }
     | None -> Error ("unknown reference scope: $" ^ head))
;;

let refs_in (s : string) : (ref_, string) result list = List.map parse_ref (raw_refs s)

let string_of_value = function
  | V_unset -> ""
  | V_none -> ""
  | V_float f -> string_of_float f
  | V_int i -> string_of_int i
  | V_bool b -> string_of_bool b
  | V_string s -> s
;;

let value_of_literal (s : string) : value =
  match s with
  | "true" -> V_bool true
  | "false" -> V_bool false
  | _ ->
    (try V_float (float_of_string s) with
     | _ -> V_string s)
;;

let value_eq a b =
  match a, b with
  | V_none, V_none -> true
  | V_bool x, V_bool y -> x = y
  | V_string x, V_string y -> String.equal x y
  | V_int x, V_int y -> x = y
  | V_float x, V_float y -> x = y
  | V_int x, V_float y | V_float y, V_int x -> float_of_int x = y
  | _ -> false
;;

let to_float = function
  | V_float f -> Ok f
  | V_int i -> Ok (float_of_int i)
  | V_string s ->
    (try Ok (float_of_string s) with
     | _ -> Error ("expected number, got string " ^ s))
  | _ -> Error "expected number"
;;

(** Raised when a reference cannot be resolved (unknown fact/param, absent event field,
    malformed expression). The evaluator is direct-style so that no [Ok] wrapper is
    allocated per reference on the hot path. *)
exception Eval_error of string

(** Resolution hooks for references. Slot-addressed scopes ([state]/[param]/[local]/
    [signal]/[platform]) take the interned key index, so the hot evaluator never hashes a
    key string. Each accessor returns the resolved value directly and raises [Eval_error]
    when the reference is unresolved. *)
type env =
  { price : unit -> value
  ; event : string -> value
  ; state : int -> value
  ; param : int -> value
  ; local : string -> value
  ; signal : int -> value
  ; now : unit -> value
  ; platform : int -> value
  }

type expr =
  | Lit_float of float
  | Lit_int of int
  | Lit_bool of bool
  | Lit_string of string
  | Ref of ref_
  | Neg of expr
  | Add of expr * expr
  | Sub of expr * expr
  | Mul of expr * expr
  | Div of expr * expr
  | Lt of expr * expr
  | Le of expr * expr
  | Gt of expr * expr
  | Ge of expr * expr
  | Eq of expr * expr
  | Ne of expr * expr
  | And of expr * expr
  | Or of expr * expr
  | Not of expr

exception Lex_error of string
exception Parse_error of string

type token =
  | Tk_int of int
  | Tk_float of float
  | Tk_bool of bool
  | Tk_ref of string
  | Tk_ident of string
  | Tk_plus
  | Tk_minus
  | Tk_star
  | Tk_slash
  | Tk_lt
  | Tk_le
  | Tk_gt
  | Tk_ge
  | Tk_eq
  | Tk_ne
  | Tk_and
  | Tk_or
  | Tk_not
  | Tk_lparen
  | Tk_rparen

let tokenize (s : string) : token list =
  let n = String.length s in
  let toks = ref [] in
  let i = ref 0 in
  let is_digit c = c >= '0' && c <= '9' in
  let is_alpha c = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c = '_' in
  let is_alnum c = is_alpha c || is_digit c in
  while !i < n do
    let c = s.[!i] in
    if c = ' ' || c = '\t' || c = '\n' || c = '\r'
    then incr i
    else if c = '$'
    then (
      let j = ref (!i + 1) in
      while !j < n && is_ref_char s.[!j] do
        incr j
      done;
      if !j = !i + 1 then raise (Lex_error "empty reference");
      toks := Tk_ref (String.sub s (!i + 1) (!j - !i - 1)) :: !toks;
      i := !j)
    else if is_digit c || (c = '.' && !i + 1 < n && is_digit s.[!i + 1])
    then (
      let j = ref !i in
      let is_float = ref false in
      while !j < n && (is_digit s.[!j] || s.[!j] = '.') do
        if s.[!j] = '.' then is_float := true;
        incr j
      done;
      let text = String.sub s !i (!j - !i) in
      (try
         toks
         := (if !is_float
             then Tk_float (float_of_string text)
             else Tk_int (int_of_string text))
            :: !toks
       with
       | _ -> raise (Lex_error ("bad number: " ^ text)));
      i := !j)
    else if is_alpha c
    then (
      let j = ref !i in
      while !j < n && is_alnum s.[!j] do
        incr j
      done;
      let word = String.sub s !i (!j - !i) in
      let tk =
        match word with
        | "true" -> Tk_bool true
        | "false" -> Tk_bool false
        | "and" -> Tk_and
        | "or" -> Tk_or
        | "not" -> Tk_not
        | w -> Tk_ident w
      in
      toks := tk :: !toks;
      i := !j)
    else (
      let two = if !i + 1 < n then String.sub s !i 2 else "" in
      match two with
      | "<=" ->
        toks := Tk_le :: !toks;
        i := !i + 2
      | ">=" ->
        toks := Tk_ge :: !toks;
        i := !i + 2
      | "==" ->
        toks := Tk_eq :: !toks;
        i := !i + 2
      | "!=" ->
        toks := Tk_ne :: !toks;
        i := !i + 2
      | _ ->
        (match c with
         | '+' ->
           toks := Tk_plus :: !toks;
           incr i
         | '-' ->
           toks := Tk_minus :: !toks;
           incr i
         | '*' ->
           toks := Tk_star :: !toks;
           incr i
         | '/' ->
           toks := Tk_slash :: !toks;
           incr i
         | '<' ->
           toks := Tk_lt :: !toks;
           incr i
         | '>' ->
           toks := Tk_gt :: !toks;
           incr i
         | '=' ->
           toks := Tk_eq :: !toks;
           incr i
         | '(' ->
           toks := Tk_lparen :: !toks;
           incr i
         | ')' ->
           toks := Tk_rparen :: !toks;
           incr i
         | '!' ->
           toks := Tk_not :: !toks;
           incr i
         | _ -> raise (Lex_error (Printf.sprintf "unexpected character %C" c))))
  done;
  List.rev !toks
;;

let parse_tokens (toks : token list) : expr =
  let arr = Array.of_list toks in
  let n = Array.length arr in
  let pos = ref 0 in
  let peek () = if !pos < n then Some arr.(!pos) else None in
  let advance () = incr pos in
  let rec parse_or () =
    let l = parse_and () in
    let rec loop l =
      match peek () with
      | Some Tk_or ->
        advance ();
        loop (Or (l, parse_and ()))
      | _ -> l
    in
    loop l
  and parse_and () =
    let l = parse_cmp () in
    let rec loop l =
      match peek () with
      | Some Tk_and ->
        advance ();
        loop (And (l, parse_cmp ()))
      | _ -> l
    in
    loop l
  and parse_cmp () =
    let l = parse_add () in
    match peek () with
    | Some Tk_lt ->
      advance ();
      Lt (l, parse_add ())
    | Some Tk_le ->
      advance ();
      Le (l, parse_add ())
    | Some Tk_gt ->
      advance ();
      Gt (l, parse_add ())
    | Some Tk_ge ->
      advance ();
      Ge (l, parse_add ())
    | Some Tk_eq ->
      advance ();
      Eq (l, parse_add ())
    | Some Tk_ne ->
      advance ();
      Ne (l, parse_add ())
    | _ -> l
  and parse_add () =
    let l = parse_mul () in
    let rec loop l =
      match peek () with
      | Some Tk_plus ->
        advance ();
        loop (Add (l, parse_mul ()))
      | Some Tk_minus ->
        advance ();
        loop (Sub (l, parse_mul ()))
      | _ -> l
    in
    loop l
  and parse_mul () =
    let l = parse_unary () in
    let rec loop l =
      match peek () with
      | Some Tk_star ->
        advance ();
        loop (Mul (l, parse_unary ()))
      | Some Tk_slash ->
        advance ();
        loop (Div (l, parse_unary ()))
      | _ -> l
    in
    loop l
  and parse_unary () =
    match peek () with
    | Some Tk_minus ->
      advance ();
      Neg (parse_unary ())
    | Some Tk_not ->
      advance ();
      Not (parse_unary ())
    | _ -> parse_primary ()
  and parse_primary () =
    match peek () with
    | Some (Tk_int i) ->
      advance ();
      Lit_int i
    | Some (Tk_float f) ->
      advance ();
      Lit_float f
    | Some (Tk_bool b) ->
      advance ();
      Lit_bool b
    | Some (Tk_ref r) ->
      advance ();
      (match parse_ref r with
       | Ok rr -> Ref rr
       | Error m -> raise (Parse_error m))
    | Some Tk_lparen ->
      advance ();
      let e = parse_or () in
      (match peek () with
       | Some Tk_rparen -> advance ()
       | _ -> raise (Parse_error "expected )"));
      e
    | Some (Tk_ident w) -> raise (Parse_error ("unexpected identifier: " ^ w))
    | _ -> raise (Parse_error "expected expression")
  in
  let e = parse_or () in
  (match peek () with
   | Some _ -> raise (Parse_error "trailing tokens")
   | None -> ());
  e
;;

let parse (s : string) : (expr, string) result =
  try Ok (parse_tokens (tokenize s)) with
  | Lex_error m -> Error m
  | Parse_error m -> Error m
;;

(** Direct-style evaluator. Guards only need a bool, so the old result-returning recursion
    boxed a [V_float]/[V_bool] and an [Ok] for every operator node and every [env]
    accessor
    - the dominant per-tick allocation on a file with many [expr] guards. This recursion
      threads booleans and floats unboxed and raises [Eval_error] on failure; [eval] wraps
      it for callers that want the explicit error channel. *)
let value_to_float_exn = function
  | V_float f -> f
  | V_int i -> float_of_int i
  | V_string s ->
    (try float_of_string s with
     | _ -> raise (Eval_error ("expected number, got string " ^ s)))
  | _ -> raise (Eval_error "expected number")
;;

let eval_ref (env : env) (r : ref_) : value =
  match r.scope with
  | Price -> env.price ()
  | Now -> env.now ()
  | Event ->
    (match r.path with
     | [] -> raise (Eval_error "empty $event reference")
     | f :: _ -> env.event f)
  | State ->
    (match r.path with
     | [] -> raise (Eval_error "empty $state reference")
     | _ -> env.state r.slot)
  | Param ->
    (match r.path with
     | [] -> raise (Eval_error "empty $params reference")
     | _ -> env.param r.slot)
  | Local ->
    (match r.path with
     | [] -> raise (Eval_error "empty $local reference")
     | n :: _ -> env.local n)
  | Signal ->
    (match r.path with
     | [] -> raise (Eval_error "empty $signal reference")
     | _ -> env.signal r.slot)
  | Platform ->
    (match r.path with
     | [] -> raise (Eval_error "empty $platform reference")
     | _ -> env.platform r.slot)
;;

let rec eval_bool env (e : expr) : bool =
  match e with
  | Lit_bool b -> b
  | Not a -> not (eval_bool env a)
  | And (a, b) -> eval_bool env a && eval_bool env b
  | Or (a, b) -> eval_bool env a || eval_bool env b
  | Lt (a, b) -> eval_float env a < eval_float env b
  | Le (a, b) -> eval_float env a <= eval_float env b
  | Gt (a, b) -> eval_float env a > eval_float env b
  | Ge (a, b) -> eval_float env a >= eval_float env b
  | Eq (a, b) -> value_eq (eval_value env a) (eval_value env b)
  | Ne (a, b) -> not (value_eq (eval_value env a) (eval_value env b))
  | _ ->
    (match eval_value env e with
     | V_bool b -> b
     | _ -> raise (Eval_error "expected bool"))

and eval_float env (e : expr) : float =
  match e with
  | Lit_float f -> f
  | Lit_int i -> float_of_int i
  | Neg a -> -.eval_float env a
  | Add (a, b) -> eval_float env a +. eval_float env b
  | Sub (a, b) -> eval_float env a -. eval_float env b
  | Mul (a, b) -> eval_float env a *. eval_float env b
  | Div (a, b) -> eval_float env a /. eval_float env b
  | _ -> value_to_float_exn (eval_value env e)

and eval_value env (e : expr) : value =
  match e with
  | Lit_float f -> V_float f
  | Lit_int i -> V_int i
  | Lit_bool b -> V_bool b
  | Lit_string s -> V_string s
  | Ref r -> eval_ref env r
  | Neg a -> V_float (-.eval_float env a)
  | Add (a, b) -> V_float (eval_float env a +. eval_float env b)
  | Sub (a, b) -> V_float (eval_float env a -. eval_float env b)
  | Mul (a, b) -> V_float (eval_float env a *. eval_float env b)
  | Div (a, b) -> V_float (eval_float env a /. eval_float env b)
  | Lt (a, b) -> V_bool (eval_float env a < eval_float env b)
  | Le (a, b) -> V_bool (eval_float env a <= eval_float env b)
  | Gt (a, b) -> V_bool (eval_float env a > eval_float env b)
  | Ge (a, b) -> V_bool (eval_float env a >= eval_float env b)
  | Eq (a, b) -> V_bool (value_eq (eval_value env a) (eval_value env b))
  | Ne (a, b) -> V_bool (not (value_eq (eval_value env a) (eval_value env b)))
  | And (a, b) -> V_bool (eval_bool env a && eval_bool env b)
  | Or (a, b) -> V_bool (eval_bool env a || eval_bool env b)
  | Not a -> V_bool (not (eval_bool env a))
;;

(** Result-returning wrapper over the direct evaluator. *)
let eval (env : env) (e : expr) : (value, string) result =
  try Ok (eval_value env e) with
  | Eval_error m -> Error m
;;

(** Substitute [$refs] into a string template. A template with no [$] is returned as-is,
    so a literal string argument (e.g. a [set_gate] [name]) costs no buffer. *)
let interpolate (env : env) (s : string) : (value, string) result =
  if not (String.contains s '$')
  then Ok (V_string s)
  else (
    let n = String.length s in
    let buf = Buffer.create n in
    let i = ref 0 in
    let err = ref None in
    while !i < n && !err = None do
      if s.[!i] = '$'
      then (
        let j = ref (!i + 1) in
        while !j < n && is_ref_char s.[!j] do
          incr j
        done;
        if !j > !i + 1
        then (
          let raw = String.sub s (!i + 1) (!j - !i - 1) in
          (match parse_ref raw with
           | Error m -> err := Some m
           | Ok r ->
             (try Buffer.add_string buf (string_of_value (eval_ref env r)) with
              | Eval_error m -> err := Some m));
          i := !j)
        else (
          Buffer.add_char buf '$';
          incr i))
      else (
        Buffer.add_char buf s.[!i];
        incr i)
    done;
    match !err with
    | Some m -> Error m
    | None -> Ok (V_string (Buffer.contents buf)))
;;

(** Evaluate a string argument: as an expression if it parses, else as a template. *)
let eval_arg (env : env) (s : string) : (value, string) result =
  match parse s with
  | Ok e -> eval env e
  | Error _ -> interpolate env s
;;
