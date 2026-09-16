(** Strategy runtime: execute a compiled strategy file against events.

    Owns per-instance state, builds the expression [env], runs steps in file order with
    live-state guards, evaluates action arguments, calls the supplied handler, and applies
    ["bind"] outputs to step-local scope. Handlers are provided by the caller; this layer
    does not talk to an exchange. *)

open Strategy_expr

type event =
  { ev_kind : string
  ; ev_fields : (string * value) list
  }

type action_call =
  { ac_step : string
  ; ac_action : string
  ; ac_args : (string * value) list
  }

(** Engine-provided capabilities for handlers that call reference functions (e.g. price
    rounding, venue identity, venue flag matrix). The engine sets these per instance; the
    defaults are identity/empty so synthetic runs work without an exchange. *)
type caps =
  { round_price : float -> float
  ; exchange : string
  ; remaintain_expired_sells : bool
  }

let default_caps =
  { round_price = (fun x -> x); exchange = ""; remaintain_expired_sells = false }
;;

(** Pre-compiled action argument. The file's JSON args are resolved once at load: scalars
    become [ALit], a [$...] expression is parsed to [AExpr], and any other string becomes
    [ATemplate] (interpolated only at runtime). The per-cycle path therefore never
    re-dispatches JSON, re-hashes an argument string, or allocates an [Ok]. *)
type compiled_arg =
  | ALit of value
  | AExpr of Strategy_expr.expr
  | ATemplate of string

type compiled_action =
  { ca_name : string
  ; ca_args : (string * compiled_arg) list
  ; ca_bind : (string * string) list
  ; ca_gate : (int * compiled_arg) option
  (* Precompiled ["set_gate"] fast path: [Some (slot, value)] when the gate's ["name"] is
     a literal, so the live path can store the value into its slot without building an
     argument assoc list or dispatching through the handler. [None] for every other action
     (and for a dynamic gate name), which falls back to the generic path. *)
  }

(** A step with its guard compiled to a closure and its actions pre-compiled. *)
type compiled_step =
  { cs_id : string
  ; cs_let : (string * string) list
  ; cs_guard : (Strategy_expr.env -> Strategy_guard.facts -> bool) option
  ; cs_then : compiled_action list
  ; cs_else : compiled_action list
  ; cs_stop : bool
  }

(** Action handler. Inputs are argument-name-keyed (the handler matches by name); outputs
    are slot-keyed ([Strategy_fact_slots] indices) so publishing is an array store. *)
type handler = { run : t -> string -> (string * value) list -> (int * value) list }

and t =
  { file : Strategy_file.t
  ; mutable state : value option array
  ; params : value option array
  ; mutable signals : value option array
  ; mutable platform : value option array
  ; locals : (string, value) Hashtbl.t
  ; guard_cache : (string, (Strategy_expr.expr, string) result) Hashtbl.t
  ; mutable compiled : compiled_step list
  ; has_lets : bool
      (* True when any step declares [let] bindings. When false (the common case) the
         per-step locals reset/lookup is skipped entirely. *)
  ; guard_memo : Strategy_guard.memo
      (* Per-cycle memo of compiled [G_expr] guard results; invalidated by [memo_gen]
         whenever a fact is written (the runtime bumps it in the setters). *)
  ; handlers : handler
  ; mutable caps : caps
  ; mutable event : event option
  ; mutable price : float
  ; mutable now : float
  ; mutable env_cache : Strategy_expr.env option
  ; mutable facts_cache : Strategy_guard.facts option
  ; (* Optional instrumentation for the live domain: when [prof_enabled], [run_cycle]
       accumulates where the strategy span goes. [prof_guard_ns]/[prof_args_ns] are wall
       time in guard and action-argument evaluation; [prof_cpu_ns] is the thread-CPU of
       the whole call so contention (wall >> cpu) is distinguishable from real work. *)
    mutable prof_enabled : bool
  ; mutable prof_guard_ns : int
  ; mutable prof_args_ns : int
  ; mutable prof_cpu_ns : int
  ; mutable prof_missing : int
  }

let noop_handler = { run = (fun _ _ _ -> []) }

let default_of_kind = function
  | Strategy_file.S_float -> V_float 0.0
  | S_float_opt -> V_none
  | S_int -> V_int 0
  | S_bool -> V_bool false
  | S_string -> V_string ""
  | S_buy_intent_opt -> V_none
  | S_sell_intent_opt -> V_none
  | S_reserve_policy -> V_none
;;

let default_string = function
  | Some (`String s) -> s
  | Some (`Int i) -> string_of_int i
  | Some (`Float f) -> string_of_float f
  | _ -> ""
;;

let default_float = function
  | Some (`Float f) -> f
  | Some (`Int i) -> float_of_int i
  | Some (`String s) ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

(** Growable slot-addressed value table. [None] means "not set"; [Some V_none] is a real
    value, so a missing fact is distinguishable from a fact explicitly set to none. *)
let set_arr (a : value option array) slot (v : value) : value option array =
  if slot < Array.length a
  then (
    a.(slot) <- Some v;
    a)
  else (
    let n = Array.length a in
    let n' = max (slot + 1) (max 8 (n * 2)) in
    let na = Array.make n' None in
    Array.blit a 0 na 0 n;
    na.(slot) <- Some v;
    na)
;;

let get_arr (a : value option array) slot =
  if slot < Array.length a then a.(slot) else None
;;

let expand_params (file : Strategy_file.t) (overrides : (string * value) list) =
  let arr = ref (Array.make (max 8 (interned_key_count ())) None) in
  let set k v = arr := set_arr !arr (intern_key k) v in
  List.iter
    (fun (p : Strategy_file.param) ->
      let ov = List.assoc_opt p.p_name overrides in
      match p.p_kind with
      | Strategy_file.P_float ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None -> V_float (default_float p.p_default))
      | P_int ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None ->
             (match p.p_default with
              | Some (`Int i) -> V_int i
              | _ -> V_int 0))
      | P_bool ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None ->
             (match p.p_default with
              | Some (`Bool b) -> V_bool b
              | _ -> V_bool false))
      | P_string ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None -> V_string (default_string p.p_default))
      | P_decimal_str ->
        let s =
          match ov with
          | Some (V_string s) -> s
          | _ -> default_string p.p_default
        in
        set p.p_name (V_string s);
        set (p.p_name ^ "_dec") (V_string s);
        set
          (p.p_name ^ "_f")
          (V_float
             (try float_of_string s with
              | _ -> 0.0))
      | P_range ->
        let lo, hi =
          match ov with
          | Some (V_float f) -> f, f
          | _ ->
            (match p.p_default with
             | Some (`List [ `Float a; `Float b ]) -> a, b
             | Some (`List [ `Int a; `Int b ]) -> float_of_int a, float_of_int b
             | Some (`List [ `Float a; `Int b ]) -> a, float_of_int b
             | Some (`List [ `Int a; `Float b ]) -> float_of_int a, b
             | _ -> 0.0, 0.0)
        in
        set (p.p_name ^ "_lo") (V_float lo);
        set (p.p_name ^ "_hi") (V_float hi)
      | P_enum _ ->
        set
          p.p_name
          (match ov with
           | Some v -> v
           | None -> V_string (default_string p.p_default)))
    file.params;
  !arr
;;

(** Memoized guard-expression parser. Expression guards carry their source string; parsing
    it once per guard (instead of once per evaluation) removes the dominant per-tick cost
    of a file with many [expr] guards. The cache is per-runtime, hence thread-confined to
    the owning domain. [Hashtbl.find] + [exception Not_found] keeps the hit path free of
    the [Some] wrapper [find_opt] allocates, which matters because every [expr] guard and
    every string argument hits this per cycle. *)
let parse_guard_expr t s =
  match Hashtbl.find t.guard_cache s with
  | r -> r
  | exception Not_found ->
    let r = parse s in
    Hashtbl.replace t.guard_cache s r;
    r
;;

(** Parse every [expr] guard's source once, at load, so no cycle pays the parse. *)
let rec prewarm_guard t (g : Strategy_file.guard) =
  let open Strategy_file in
  match g with
  | G_expr s -> ignore (parse_guard_expr t s : (Strategy_expr.expr, string) result)
  | G_all gs | G_any gs -> List.iter (prewarm_guard t) gs
  | G_not g -> prewarm_guard t g
  | G_event _
  | G_side _
  | G_is_none _
  | G_is_some _
  | G_capacity _
  | G_pending _
  | G_order _
  | G_signal _
  | G_engine _
  | G_cooldown _ -> ()
;;

(** Serialize one action argument once, at load. Non-expression strings stay as
    [ATemplate] / [ALit] so no cycle re-tokenizes or allocates an [Ok]. *)
let compile_arg t (j : Yojson.Basic.t) : compiled_arg =
  match j with
  | `Int i -> ALit (V_int i)
  | `Float f -> ALit (V_float f)
  | `Bool b -> ALit (V_bool b)
  | `Null -> ALit V_none
  | `String s ->
    (match parse_guard_expr t s with
     | Ok ex -> AExpr ex
     | Error _ -> if String.contains s '$' then ATemplate s else ALit (V_string s))
  | _ -> ALit V_none
;;

let compile_action t (a : Strategy_file.action) : compiled_action =
  (* A ["set_gate"] with a literal ["name"] is by far the most common arg-carrying action.
     Resolve its state slot here, at load, so the live path is a single slot store. *)
  let ca_gate =
    if String.equal a.a_name "set_gate" && a.a_bind = []
    then (
      match List.assoc_opt "name" a.a_args, List.assoc_opt "value" a.a_args with
      | Some (`String k), Some v -> Some (intern_key k, compile_arg t v)
      | _ -> None)
    else None
  in
  { ca_name = a.a_name
  ; ca_args = List.map (fun (k, j) -> k, compile_arg t j) a.a_args
  ; ca_bind = a.a_bind
  ; ca_gate
  }
;;

let compile_step t (st : Strategy_file.step) : compiled_step =
  { cs_id = st.st_id
  ; cs_let = st.st_let
  ; cs_guard =
      Option.map
        (Strategy_guard.compile_exn ~parse_expr:parse ~memo:t.guard_memo)
        st.st_when
  ; cs_then = List.map (compile_action t) st.st_then
  ; cs_else = List.map (compile_action t) st.st_else
  ; cs_stop = st.st_stop
  }
;;

let create ?(handlers = noop_handler) ?(params = []) (file : Strategy_file.t) =
  let state =
    List.fold_left
      (fun a (s : Strategy_file.state_decl) ->
        set_arr a (intern_key s.s_name) (default_of_kind s.s_kind))
      (Array.make (max 8 (interned_key_count ())) None)
      file.state
  in
  let guard_memo : Strategy_guard.memo =
    { memo_ids = Hashtbl.create 64
    ; memo_next = 0
    ; memo_gen = 0
    ; memo_gen_arr = Array.make 64 (-1)
    ; memo_val_arr = Array.make 64 false
    }
  in
  let t =
    { file
    ; state
    ; params = expand_params file params
    ; signals = Array.make (max 8 (interned_key_count ())) None
    ; platform = Array.make (max 8 (interned_key_count ())) None
    ; locals = Hashtbl.create 8
    ; guard_cache = Hashtbl.create 32
    ; compiled = []
    ; has_lets = List.exists (fun (st : Strategy_file.step) -> st.st_let <> []) file.steps
    ; guard_memo
    ; handlers
    ; caps = default_caps
    ; event = None
    ; price = nan
    ; now = 0.0
    ; env_cache = None
    ; facts_cache = None
    ; prof_enabled = false
    ; prof_guard_ns = 0
    ; prof_args_ns = 0
    ; prof_cpu_ns = 0
    ; prof_missing = 0
    }
  in
  List.iter
    (fun (st : Strategy_file.step) -> Option.iter (prewarm_guard t) st.st_when)
    file.steps;
  (* Pre-compile all action args (parses every [$...] expression now so no cycle does). *)
  t.compiled <- List.map (compile_step t) file.steps;
  t
;;

(* Every fact write bumps the guard-memo generation, so a memoised guard result is reused
   only while no fact has changed since it was computed. Cheap (one int increment). *)
let bump_gen t = t.guard_memo.memo_gen <- t.guard_memo.memo_gen + 1

let set_state t k v =
  bump_gen t;
  t.state <- set_arr t.state (intern_key k) v
;;

let get_state t k = get_arr t.state (intern_key k)

let set_platform t k v =
  bump_gen t;
  t.platform <- set_arr t.platform (intern_key k) v
;;

let get_platform t k = get_arr t.platform (intern_key k)

let set_signal t k v =
  bump_gen t;
  t.signals <- set_arr t.signals (intern_key k) v
;;

(* Slot-addressed publication for the fixed fact/gate set. [Strategy_fact_slots] resolves
   each name to one of these indices at startup, so the per-cycle publish path is an array
   store with no string hashing. *)
let set_state_slot t slot v =
  bump_gen t;
  t.state <- set_arr t.state slot v
;;

let set_platform_slot t slot v =
  bump_gen t;
  t.platform <- set_arr t.platform slot v
;;

let set_signal_slot t slot v =
  bump_gen t;
  t.signals <- set_arr t.signals slot v
;;

let set_caps t c = t.caps <- c
let make_event kind fields = { ev_kind = kind; ev_fields = fields }
let current_event t = t.event

(** Preallocated exception value for unresolved references: raising a constant avoids
    building a message string and an exception block on every miss, which matters because
    a guard/argument that references a not-yet-published platform fact misses on every
    cycle. *)
let unresolved = Eval_error "unresolved reference"

let miss t =
  if t.prof_enabled then t.prof_missing <- t.prof_missing + 1;
  raise unresolved
;;

let env_of t : env =
  { price = (fun () -> V_float t.price)
  ; event =
      (fun f ->
        match t.event with
        | None -> miss t
        | Some e ->
          (match List.assoc_opt f e.ev_fields with
           | Some v -> v
           | None -> miss t))
  ; state =
      (fun slot ->
        match get_arr t.state slot with
        | Some v -> v
        | None -> V_none)
  ; param =
      (fun slot ->
        match get_arr t.params slot with
        | Some v -> v
        | None -> miss t)
  ; local =
      (fun n ->
        match Hashtbl.find t.locals n with
        | v -> v
        | exception Not_found -> miss t)
  ; signal =
      (fun slot ->
        match get_arr t.signals slot with
        | Some v -> v
        | None -> miss t)
  ; now = (fun () -> V_float t.now)
  ; platform =
      (fun slot ->
        match get_arr t.platform slot with
        | Some v -> v
        | None -> miss t)
  }
;;

let facts_of t : Strategy_guard.facts =
  { event_kind =
      (fun () ->
        match t.event with
        | Some e -> e.ev_kind
        | None -> "")
  ; side =
      (fun () ->
        match t.event with
        | Some e ->
          (match List.assoc_opt "side" e.ev_fields with
           | Some (V_string s) -> s
           | _ -> "")
        | None -> "")
  ; pending =
      (fun p ->
        match get_arr t.platform (intern_key ("pending:" ^ p)) with
        | Some (V_bool b) -> b
        | _ -> false)
  ; engine_flag = (fun k -> get_arr t.platform (intern_key ("engine:" ^ k)))
  ; capacity = (fun a -> get_arr t.platform (intern_key ("capacity:" ^ a)))
  ; signal = (fun n -> get_arr t.signals (intern_key n))
  ; order_posture = (fun _ _ -> false)
  }
;;

(** Reuse the [env] and [facts] records across cycles. Both are closures over [t], whose
    fields mutate per cycle, so they can be built once per runtime instead of allocating
    ~16 closures every tick. *)
let env_cached t =
  match t.env_cache with
  | Some e -> e
  | None ->
    let e = env_of t in
    t.env_cache <- Some e;
    e
;;

let facts_cached t =
  match t.facts_cache with
  | Some f -> f
  | None ->
    let f = facts_of t in
    t.facts_cache <- Some f;
    f
;;

(** Evaluate one action argument. String args are expressions (or templates); the parse is
    memoized through the same cache as the guard expressions, so a hot action (the six
    [set_gate] calls per cycle) does not re-tokenize and re-build its expression tree
    every tick. The direct [eval_value] handles bool/numeric/string expressions in one
    pass (no per-operator boxing, no retry on a missing reference). *)
let value_of_json t (e : env) (j : Yojson.Basic.t) : (value, string) result =
  match j with
  | `Int i -> Ok (V_int i)
  | `Float f -> Ok (V_float f)
  | `Bool b -> Ok (V_bool b)
  | `Null -> Ok V_none
  | `String s ->
    (match parse_guard_expr t s with
     | Ok ex ->
       (try Ok (eval_value e ex) with
        | Eval_error m -> Error m)
     | Error _ -> interpolate e s)
  | _ -> Error "unsupported argument value"
;;

let field_of_bind (rhs : string) =
  let prefix = "$out." in
  let n = String.length prefix in
  if String.length rhs > n && String.equal (String.sub rhs 0 n) prefix
  then Some (String.sub rhs n (String.length rhs - n))
  else None
;;

(** Evaluate an action's arguments. Most actions carry no args ([{}]); skipping the
    [filter_map] closure and list avoids a per-action allocation. *)
let eval_args t (e : env) (ca : compiled_action) =
  match ca.ca_args with
  | [] -> []
  | _ ->
    (* Every argument was serialized at load, so this is a direct value expression plus,
       for templates, an interpolation — no JSON dispatch, no string hashing, no [Ok]. *)
    let one (k, a) =
      match a with
      | ALit v -> Some (k, v)
      | AExpr ex ->
        (try Some (k, Strategy_expr.eval_value e ex) with
         | Strategy_expr.Eval_error _ -> None)
      | ATemplate s ->
        (match interpolate e s with
         | Ok v -> Some (k, v)
         | Error _ -> None)
    in
    if t.prof_enabled
    then (
      let t0 = Monotonic_clock.now_ns () in
      let r = List.filter_map one ca.ca_args in
      t.prof_args_ns <- t.prof_args_ns + (Monotonic_clock.now_ns () - t0);
      r)
    else List.filter_map one ca.ca_args
;;

(** Apply an action's ["bind"] outputs to the step-local scope. Binds are rare (unused in
    the shipped file), so resolving the field name to its slot here is not on the hot
    path. *)
let apply_binds t (ca : compiled_action) out =
  match ca.ca_bind with
  | [] -> ()
  | _ ->
    List.iter
      (fun (var, rhs) ->
        match field_of_bind rhs with
        | None -> ()
        | Some field ->
          (match List.assoc_opt (intern_key field) out with
           | Some v -> Hashtbl.replace t.locals var v
           | None -> ()))
      ca.ca_bind
;;

(** Evaluate a single serialized argument to its [value] (no [Ok]/tuple/list). *)
let eval_arg_value e (a : compiled_arg) =
  match a with
  | ALit v -> Some v
  | AExpr ex ->
    (try Some (Strategy_expr.eval_value e ex) with
     | Strategy_expr.Eval_error _ -> None)
  | ATemplate s ->
    (match interpolate e s with
     | Ok v -> Some v
     | Error _ -> None)
;;

let run_action t (e : env) step_id (ca : compiled_action) : action_call =
  let args = eval_args t e ca in
  let out = t.handlers.run t ca.ca_name args in
  apply_binds t ca out;
  { ac_step = step_id; ac_action = ca.ca_name; ac_args = args }
;;

(** Live path: run the action without materializing an [action_call] (the caller discards
    the call list). Keeps the [collect = false] cycle allocation-free for the record. *)
let run_action_ignore t (e : env) (ca : compiled_action) =
  match ca.ca_gate with
  | Some (slot, varg) ->
    (* Precompiled ["set_gate"]: no argument assoc list, no handler dispatch, no thunk -
       just evaluate the value and store it into the pre-resolved slot. *)
    (match eval_arg_value e varg with
     | Some v -> set_state_slot t slot v
     | None -> ())
  | None ->
    let args = eval_args t e ca in
    let out = t.handlers.run t ca.ca_name args in
    apply_binds t ca out
;;

(** Top-level loops so the per-step action/let iteration does not allocate a closure on
    every step. *)
let rec iter_ignore t e = function
  | [] -> ()
  | a :: rest ->
    run_action_ignore t e a;
    iter_ignore t e rest
;;

let rec iter_collect t e step_id calls = function
  | [] -> ()
  | a :: rest ->
    calls := run_action t e step_id a :: !calls;
    iter_collect t e step_id calls rest
;;

let rec apply_lets e t = function
  | [] -> ()
  | (name, s) :: rest ->
    (match eval_arg e s with
     | Ok v -> Hashtbl.replace t.locals name v
     | Error _ -> ());
    apply_lets e t rest
;;

(** Run one cycle for a dispatch kind. The snapshot phase is
    [make_event "book_update" []]; execution events carry their fields. Returns the
    ordered action calls made this cycle. *)
let run_cycle ?(collect = true) t ~(price : float) ~(now : float) ~(event : event)
  : action_call list
  =
  let prof = t.prof_enabled in
  let cpu0 = if prof then Monotonic_clock.thread_cpu_ns () else 0 in
  (* Guards also read [$price]/[$now]/[$event], which change every cycle, so the memo can
     only live within a cycle: bump the generation to invalidate every entry. Writes
     during the cycle bump it again, so entries are still invalidated when a fact changes. *)
  t.guard_memo.memo_gen <- t.guard_memo.memo_gen + 1;
  if prof
  then (
    t.prof_guard_ns <- 0;
    t.prof_args_ns <- 0;
    t.prof_missing <- 0);
  t.price <- price;
  t.now <- now;
  t.event <- Some event;
  let e = env_cached t in
  let facts = facts_cached t in
  let calls = ref [] in
  let stop = ref false in
  let run_steps =
    if collect
    then fun step_id calls actions -> iter_collect t e step_id calls actions
    else fun _step_id _calls actions -> iter_ignore t e actions
  in
  List.iter
    (fun (cs : compiled_step) ->
      if not !stop
      then (
        if t.has_lets
        then (
          if Hashtbl.length t.locals > 0 then Hashtbl.reset t.locals;
          apply_lets e t cs.cs_let);
        let passed =
          match cs.cs_guard with
          | None -> true
          | Some g ->
            (* Precompiled closure: no per-cycle parse/cache lookup; an unresolved guard
               is treated as false, matching the previous result-matching behavior. Timed
               only when there is a guard, so guardless steps pay no clock read. *)
            let g0 = if prof then Monotonic_clock.now_ns () else 0 in
            let r =
              try g e facts with
              | Eval_error _ -> false
            in
            if g0 > 0
            then t.prof_guard_ns <- t.prof_guard_ns + (Monotonic_clock.now_ns () - g0);
            r
        in
        let actions = if passed then cs.cs_then else cs.cs_else in
        run_steps cs.cs_id calls actions;
        if passed && cs.cs_stop then stop := true))
    t.compiled;
  if prof then t.prof_cpu_ns <- Monotonic_clock.thread_cpu_ns () - cpu0;
  List.rev !calls
;;
