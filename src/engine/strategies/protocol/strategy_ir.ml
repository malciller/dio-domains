(** Typed expression backend for strategy files.

    The runtime interpreter evaluates [Strategy_expr.expr] over a boxed [value] array.
    This module is the first half of the replacement compiler: the strategy file is still
    the source, but each expression is lowered once, at load time, to a closure over
    unboxed float/int/bool/string arrays. The hot loop then calls those closures; it never
    walks the AST, allocates a [value], or hashes a fact key.

    Slot assignment is compile-time only (a [Hashtbl]); compiled closures capture the
    typed array index, so evaluation is allocation-free. *)

type kind =
  | K_float
  | K_int
  | K_bool
  | K_string

type env =
  { floats : float array
  ; ints : int array
  ; bools : bool array
  ; strings : string array
  ; mutable price : float
  ; mutable now : float
  ; mutable event_kind : string
  ; mutable event_side : string
  }

type layout =
  { kinds : (Strategy_expr.scope * string, kind * int) Hashtbl.t
  ; keys : (Strategy_expr.scope * string, string) Hashtbl.t
  (** Insertion-ordered copy of [kinds]. The compiled runtime walks it every cycle to
      fill the env; a preallocated array avoids both the [Hashtbl.iter] cursor and the
      [scope, key] tuple allocation per entry. *)
  ; mutable entries : (Strategy_expr.scope * string * kind * int) list
  ; mutable float_count : int
  ; mutable int_count : int
  ; mutable bool_count : int
  ; mutable string_count : int
  }

let create_layout () =
  { kinds = Hashtbl.create 64
  ; keys = Hashtbl.create 64
  ; entries = []
  ; float_count = 0
  ; int_count = 0
  ; bool_count = 0
  ; string_count = 0
  }
;;

let slot_for layout ~kind_of scope ~key =
  match Hashtbl.find_opt layout.kinds (scope, key) with
  | Some s -> s
  | None ->
    let kind = kind_of scope ~key in
    let index =
      match kind with
      | K_float ->
        let i = layout.float_count in
        layout.float_count <- i + 1;
        i
      | K_int ->
        let i = layout.int_count in
        layout.int_count <- i + 1;
        i
      | K_bool ->
        let i = layout.bool_count in
        layout.bool_count <- i + 1;
        i
      | K_string ->
        let i = layout.string_count in
        layout.string_count <- i + 1;
        i
    in
    Hashtbl.replace layout.kinds (scope, key) (kind, index);
    Hashtbl.replace layout.keys (scope, key) key;
    layout.entries <- (scope, key, kind, index) :: layout.entries;
    kind, index
;;

type t =
  | Float of (env -> float)
  | Bool of (env -> bool)
  | String of (env -> string)

let as_float = function
  | Float f -> f
  | Bool b -> fun env -> if b env then 1.0 else 0.0
  | String s ->
    fun env ->
      (match float_of_string_opt (s env) with
       | Some f -> f
       | None -> nan)
;;

let as_bool = function
  | Bool b -> b
  | Float f -> fun env -> f env <> 0.0
  | String s -> fun env -> s env <> ""
;;

let as_string = function
  | String s -> s
  | Float f -> fun env -> string_of_float (f env)
  | Bool b -> fun env -> if b env then "true" else "false"
;;

(** Compile an expression. [locals] binds a step's [let] names to the parsed expression they
    were assigned: a reference to a local is substituted inline, so a let costs nothing at
    run time and the compiled program never needs a locals table. *)
let compile ?locals layout ~kind_of e =
  let rec go = function
    | Strategy_expr.Lit_float f -> Float (fun _ -> f)
    | Strategy_expr.Lit_int i -> Float (fun _ -> float_of_int i)
    | Strategy_expr.Lit_bool b -> Bool (fun _ -> b)
    | Strategy_expr.Lit_string s -> String (fun _ -> s)
    | Strategy_expr.Ref r ->
      let dotted = String.concat "." r.path in
      let underscored = String.map (fun c -> if c = '.' then '_' else c) dotted in
      (match locals with
       | Some table when Hashtbl.mem table dotted ->
         go (Hashtbl.find table dotted)
       | Some table when Hashtbl.mem table underscored ->
         go (Hashtbl.find table underscored)
       | _ ->
         (match r.scope with
          | Strategy_expr.Price -> Float (fun env -> env.price)
          | Strategy_expr.Now -> Float (fun env -> env.now)
          | scope ->
            let kind, index =
              slot_for layout ~kind_of scope ~key:(String.concat "." r.path)
            in
            (match kind with
             | K_float -> Float (fun env -> env.floats.(index))
             | K_int -> Float (fun env -> float_of_int env.ints.(index))
             | K_bool -> Bool (fun env -> env.bools.(index))
             | K_string -> String (fun env -> env.strings.(index)))))
    | Strategy_expr.Neg a ->
      let f = as_float (go a) in
      Float (fun env -> -.f env)
    | Strategy_expr.Add (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Float (fun env -> f env +. g env)
    | Strategy_expr.Sub (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Float (fun env -> f env -. g env)
    | Strategy_expr.Mul (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Float (fun env -> f env *. g env)
    | Strategy_expr.Div (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Float (fun env -> f env /. g env)
    | Strategy_expr.Lt (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Bool (fun env -> f env < g env)
    | Strategy_expr.Le (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Bool (fun env -> f env <= g env)
    | Strategy_expr.Gt (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Bool (fun env -> f env > g env)
    | Strategy_expr.Ge (a, b) ->
      let f = as_float (go a)
      and g = as_float (go b) in
      Bool (fun env -> f env >= g env)
    | Strategy_expr.Eq (a, b) ->
      (match go a, go b with
       | String f, String g -> Bool (fun env -> String.equal (f env) (g env))
       | Bool f, Bool g -> Bool (fun env -> f env = g env)
       | a, b ->
         let f = as_float a
         and g = as_float b in
         Bool (fun env -> f env = g env))
    | Strategy_expr.Ne (a, b) ->
      (match go a, go b with
       | String f, String g -> Bool (fun env -> not (String.equal (f env) (g env)))
       | Bool f, Bool g -> Bool (fun env -> f env <> g env)
       | a, b ->
         let f = as_float a
         and g = as_float b in
         Bool (fun env -> f env <> g env))
    | Strategy_expr.And (a, b) ->
      let f = as_bool (go a)
      and g = as_bool (go b) in
      Bool (fun env -> f env && g env)
    | Strategy_expr.Or (a, b) ->
      let f = as_bool (go a)
      and g = as_bool (go b) in
      Bool (fun env -> f env || g env)
    | Strategy_expr.Not a ->
      let f = as_bool (go a) in
      Bool (fun env -> not (f env))
  in
  go e
;;

let compile_bool ?locals layout ~kind_of e = as_bool (compile ?locals layout ~kind_of e)

let compile_float ?locals layout ~kind_of e =
  as_float (compile ?locals layout ~kind_of e)


(** Lower the structured step guards to typed closures. Guards outside this subset are a
    compile error rather than a silent [false]: a compiled strategy must not change
    meaning. *)
let compile_guard ?locals layout ~kind_of g =
  let rec go = function
    | Strategy_file.G_expr s ->
      (match Strategy_expr.parse s with
       | Ok e -> compile_bool ?locals layout ~kind_of e
       | Error m -> invalid_arg ("strategy guard parse: " ^ m))
    | Strategy_file.G_event e -> fun env -> String.equal env.event_kind e
    | Strategy_file.G_side s -> fun env -> String.equal env.event_side s
    | Strategy_file.G_all gs ->
      let fs = Array.of_list (List.map go gs) in
      fun env ->
        let rec loop i =
          i >= Array.length fs || (fs.(i) env && loop (i + 1))
        in
        loop 0
    | Strategy_file.G_any gs ->
      let fs = Array.of_list (List.map go gs) in
      fun env ->
        let rec loop i =
          i < Array.length fs && (fs.(i) env || loop (i + 1))
        in
        loop 0
    | Strategy_file.G_not g ->
      let f = go g in
      fun env -> not (f env)
    | Strategy_file.G_is_none _
    | Strategy_file.G_is_some _
    | Strategy_file.G_capacity _
    | Strategy_file.G_pending _
    | Strategy_file.G_order _
    | Strategy_file.G_signal _
    | Strategy_file.G_engine _
    | Strategy_file.G_cooldown _ -> invalid_arg "strategy_ir: guard form not lowered yet"
  in
  go g
;;

let as_int = function
  | Float f -> fun env -> int_of_float (f env)
  | Bool b -> fun env -> if b env then 1 else 0
  | String s ->
    fun env ->
      (match int_of_string_opt (s env) with
       | Some i -> i
       | None -> (match float_of_string_opt (s env) with
                  | Some f -> int_of_float f
                  | None -> 0))
;;
