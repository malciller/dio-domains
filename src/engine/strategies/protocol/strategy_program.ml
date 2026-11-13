(** Compiled strategy step program.

    Compiles the parsed [.strategy] file once, at load, into typed step/action
    descriptors. The hot loop executes those descriptors over unboxed slot arrays; it
    never walks the AST, boxes a [value], or builds an action argument list.

    The order/quote/credit actions write a preallocated [sink]; the engine adapter turns
    that sink into venue calls. State writes update the same typed arrays the guards read. *)

type action =
  | Set_state of Strategy_ir.kind * int * Strategy_ir.t
  | Order_replace of
      { market : string
      ; side : string
      ; qty : Strategy_ir.t
      ; price : Strategy_ir.t
      ; tif : string
      ; reduce_only : bool
      ; dedup_key : string
      }
  | Quote_reserve of Strategy_ir.t
  | Quote_release
  | Credit_profit of
      { qty : Strategy_ir.t
      ; seq : Strategy_ir.t
      }
  | Engine_phase of int
  | Noop

type step =
  { guard : (Strategy_ir.env -> bool) option
  ; then_actions : action array
  ; else_actions : action array
  ; stop : bool
  }

type program =
  { layout : Strategy_ir.layout
  ; steps : step array
  (** Every layout slot in insertion order, materialised once at compile time: the
      per-cycle env fill walks this array instead of hashing [layout.kinds] for each
      [(scope, key)] tuple it would have to allocate. *)
  ; fills : (Strategy_expr.scope * string * Strategy_ir.kind * int) array
  (** Platform facts this program actually reads. A cutover can refresh only these instead
      of the whole platform cache. *)
  ; platform_keys : string array
  (** State slots, for syncing compiled state back to the interpreter runtime after a
      pass. Same prebuilt-array rule as [fills]. *)
  ; state_slots : (string * Strategy_ir.kind * int) array
  (** State slots the file itself assigns, with their key and kind. A shadow can compare
      only these: everything else in a grid's state is written by engine phases, which a
      shadow does not execute (it must not place a second order set). *)
  ; written_state : (string * Strategy_ir.kind * int) array
  (** Platform slot id (interned key) to env index, per value kind. A cutover writes its
      facts straight into the typed env through these, so the per-fact publish that only
      the interpreter consumes costs nothing. *)
  (* Platform slot id -> env index, per value kind, as dense arrays: a Hashtbl lookup
     allocates the [Some] it returns, and this runs on every fact written. An absent slot
     is [-1]. *)
  ; p_float : int array
  ; p_int : int array
  ; p_bool : int array
  ; p_string : int array
  }

type order_intent =
  { mutable market : string
  ; mutable side : string
  ; mutable qty : float
  ; mutable price : float
  ; mutable tif : string
  ; mutable reduce_only : bool
  ; mutable dedup_key : string
  }

(** One entry in the ordered command stream a pass produced. Engine phases and order
    intents must replay in source order, so the sink keeps them in one sequence. *)
type cmd =
  | Cm_order
  | Cm_state (* a state write, so an executor can sync lazily before the next phase *)
  | Cm_phase of int
  | Cm_reserve of float
  | Cm_release
  | Cm_credit of int * float (* seq, qty *)

type sink =
  { mutable orders : order_intent array
  ; mutable order_count : int
  ; mutable cmds : cmd array
  ; mutable cmd_count : int
  ; mutable stopped : bool
  }

let create_sink () =
  { orders =
      Array.init 8 (fun _ ->
        { market = ""
        ; side = ""
        ; qty = 0.0
        ; price = 0.0
        ; tif = ""
        ; reduce_only = false
        ; dedup_key = ""
        })
  ; order_count = 0
  ; cmds = Array.make 32 Cm_release
  ; cmd_count = 0
  ; stopped = false
  }
;;

(** Grow instead of dropping. A dropped command silently truncates the pass: the tail
    stops executing and a dropped [Cm_state] leaves the next phase running against state
    the file just assigned. *)
let push_cmd s c =
  if s.cmd_count = Array.length s.cmds
  then (
    let grown = Array.make (2 * Array.length s.cmds) c in
    Array.blit s.cmds 0 grown 0 s.cmd_count;
    s.cmds <- grown);
  s.cmds.(s.cmd_count) <- c;
  s.cmd_count <- s.cmd_count + 1
;;

let clear_sink s =
  s.order_count <- 0;
  s.cmd_count <- 0;
  s.stopped <- false
;;

let state_kind = function
  | Strategy_file.S_float | S_float_opt -> Strategy_ir.K_float
  | S_int -> K_int
  | S_bool -> K_bool
  | S_string | S_buy_intent_opt | S_sell_intent_opt | S_reserve_policy -> K_string
;;

let param_kind = function
  | Strategy_file.P_float | P_range -> Strategy_ir.K_float
  | P_int -> K_int
  | P_bool -> K_bool
  | P_string | P_decimal_str | P_enum _ -> K_string
;;

let event_kind = function
  | "seq" -> Strategy_ir.K_int
  | "side" -> Strategy_ir.K_string
  | _ -> Strategy_ir.K_float
;;

let platform_kind = function
  | "parent_unfunded"
  | "perp_valid"
  | "funding_valid"
  | "buy_capital_low"
  | "buy_crossing"
  | "buy_cooldown"
  | "buy_inflight"
  | "buy_balance_ok"
  | "quote_balance_stale"
  | "price_nan"
  | "oracle_halted" -> Strategy_ir.K_bool
  | _ -> Strategy_ir.K_float
;;

let json_string = function
  | `String s -> s
  | `Bool b -> string_of_bool b
  | `Int i -> string_of_int i
  | `Float f -> string_of_float f
  | `Null -> ""
  | _ -> invalid_arg "strategy_program: expected scalar argument"
;;

let json_bool = function
  | `Bool b -> b
  | `String "true" -> true
  | `String "false" -> false
  | _ -> invalid_arg "strategy_program: expected bool argument"
;;

let compile_num ?locals layout ~kind_of j =
  match j with
  | `Int i -> Strategy_ir.Float (fun _ -> float_of_int i)
  | `Float f -> Strategy_ir.Float (fun _ -> f)
  | `String s ->
    (match Strategy_expr.parse s with
     | Ok e -> Strategy_ir.compile ?locals layout ~kind_of e
     | Error m -> invalid_arg ("strategy_program: numeric arg " ^ s ^ ": " ^ m))
  | _ -> invalid_arg "strategy_program: expected numeric argument"
;;

let compile_string j = Strategy_ir.String (fun _ -> json_string j)

let compile_arg ?locals layout ~kind_of j =
  match j with
  | `Bool _ -> compile_num ?locals layout ~kind_of j
  | `Int _ | `Float _ -> compile_num ?locals layout ~kind_of j
  | `String s ->
    (match Strategy_expr.parse s with
     | Ok e -> Strategy_ir.compile ?locals layout ~kind_of e
     | Error _ -> compile_string j)
  | _ -> invalid_arg "strategy_program: unsupported argument"
;;

let find_arg args name =
  match List.assoc_opt name args with
  | Some j -> j
  | None -> invalid_arg ("strategy_program: missing argument " ^ name)
;;

(** Engine phases a compiled strategy can call by name. Each maps to one engine entry point
    in the executor; the list is fixed so a phase is an int on the command stream, not a
    string dispatched per pass. *)
let engine_phases =
  [| "apply_order_event"
   ; "init_venue_state"
   ; "prepare_recovery"
   ; "resolve_book"
   ; "cycle_cleanup"
   ; "early_facts"
   ; "cycle_facts"
   ; "expire_amend_cooldowns"
   ; "evict_ghost_orders"
   ; "scan_open_orders"
   ; "refresh_maker_fee"
   ; "expire_tif_recovery"
   ; "cycle_guard"
   ; "buy_gate"
   ; "cancel_excess_buys"
   ; "mark_stale_cycle"
   ; "buy_place_plan"
   ; "buy_place_send"
   ; "buy_place_send_insufficient"
   ; "buy_place_latch_capital_low"
   ; "buy_place_warn_quote"
   ; "buy_amend"
   ; "buy_amend_has_sell"
   ; "buy_amend_with_sell"
   ; "buy_amend_no_sell"
   ; "plan_sell_order"
   ; "sell_place"
   ; "sell_place_should"
   ; "sell_place_body"
   ; "sell_finalize"
   ; "sell_finalize_facts"
   ; "sell_finalize_latch"
   ; "sell_excess_sweep_phase"
   ; "sell_finalize_end"
  |]
;;

let engine_phase_table =
  let t = Hashtbl.create 40 in
  Array.iteri (fun i name -> Hashtbl.replace t name i) engine_phases;
  t
;;

let engine_phase_id name = Hashtbl.find_opt engine_phase_table name

let compile_action ?locals layout ~kind_of ~state_kinds (a : Strategy_file.action) =
  if a.a_bind <> []
  then invalid_arg ("strategy_program: action binds not lowered: " ^ a.a_name);
  match a.a_name with
  | "set_gate" ->
    let name = json_string (find_arg a.a_args "name") in
    let value = json_string (find_arg a.a_args "value") in
    if not (Hashtbl.mem state_kinds name)
    then invalid_arg ("strategy_program: unknown state " ^ name);
    let kind, index =
      Strategy_ir.slot_for layout ~kind_of Strategy_expr.State ~key:name
    in
    let e =
      match Strategy_expr.parse value with
      | Ok e -> Strategy_ir.compile ?locals layout ~kind_of e
      | Error m -> invalid_arg ("strategy_program: set_gate " ^ name ^ ": " ^ m)
    in
    Set_state (kind, index, e)
  | "order_replace" | "order.replace" ->
    let market = json_string (find_arg a.a_args "market") in
    let side = json_string (find_arg a.a_args "side") in
    let qty = compile_num ?locals layout ~kind_of (find_arg a.a_args "qty") in
    let price = compile_num ?locals layout ~kind_of (find_arg a.a_args "price") in
    let tif = json_string (find_arg a.a_args "tif") in
    let reduce_only = json_bool (find_arg a.a_args "reduce_only") in
    let dedup_key = json_string (find_arg a.a_args "dedup_key") in
    Order_replace { market; side; qty; price; tif; reduce_only; dedup_key }
  | "quote_reserve" | "quote.reserve" ->
    Quote_reserve (compile_num layout ~kind_of (find_arg a.a_args "amount"))
  | "quote_release" | "quote.release" -> Quote_release
  | "credit_profit" ->
    let qty = compile_num ?locals layout ~kind_of (find_arg a.a_args "qty") in
    let seq = compile_num layout ~kind_of (find_arg a.a_args "seq") in
    Credit_profit { qty; seq }
  | "market_facts" | "market.facts" | "context" -> Noop
  | name ->
    (match engine_phase_id name with
     | Some id -> Engine_phase id
     | None -> invalid_arg ("strategy_program: action not lowered: " ^ name))
;;

let compile (f : Strategy_file.t) : (program, string) result =
  try
    let layout = Strategy_ir.create_layout () in
    let state_kinds = Hashtbl.create 32 in
    List.iter
      (fun (d : Strategy_file.state_decl) ->
        Hashtbl.replace state_kinds d.s_name (state_kind d.s_kind))
      f.state;
    let param_kinds = Hashtbl.create 16 in
    List.iter
      (fun (p : Strategy_file.param) ->
        Hashtbl.replace param_kinds p.p_name (param_kind p.p_kind))
      f.params;
    let kind_of scope ~key =
      match scope with
      | Strategy_expr.State ->
        (match Hashtbl.find_opt state_kinds key with
         | Some k -> k
         | None -> invalid_arg ("strategy_program: unknown state " ^ key))
      | Strategy_expr.Param ->
        (match Hashtbl.find_opt param_kinds key with
         | Some k -> k
         | None -> invalid_arg ("strategy_program: unknown param " ^ key))
      | Strategy_expr.Event -> event_kind key
      | Strategy_expr.Platform -> platform_kind key
      | Strategy_expr.Local | Strategy_expr.Signal -> Strategy_ir.K_float
      | Strategy_expr.Price | Strategy_expr.Now -> Strategy_ir.K_float
    in
    List.iter
      (fun (d : Strategy_file.state_decl) ->
        ignore (Strategy_ir.slot_for layout ~kind_of Strategy_expr.State ~key:d.s_name))
      f.state;
    List.iter
      (fun (p : Strategy_file.param) ->
        ignore (Strategy_ir.slot_for layout ~kind_of Strategy_expr.Param ~key:p.p_name))
      f.params;
    let steps =
      List.map
        (fun (st : Strategy_file.step) ->
          (* A step's [let] bindings are substituted into the expressions that read them:
             they are step-local, so the compiled program never needs a locals table and a
             binding costs nothing at run time. Bindings are added in source order, so a
             binding may itself read an earlier one. *)
          let locals =
            match st.st_let with
            | [] -> None
            | binds ->
              let table = Hashtbl.create (List.length binds) in
              List.iter
                (fun (name, src) ->
                  match Strategy_expr.parse src with
                  | Ok e -> Hashtbl.replace table name e
                  | Error m ->
                    invalid_arg
                      ("strategy_program: let " ^ name ^ " in " ^ st.st_id ^ ": " ^ m))
                binds;
              Some table
          in
          { guard =
              Option.map (Strategy_ir.compile_guard ?locals layout ~kind_of) st.st_when
          ; then_actions =
              Array.of_list
                (List.map (compile_action ?locals layout ~kind_of ~state_kinds) st.st_then)
          ; else_actions =
              Array.of_list
                (List.map (compile_action ?locals layout ~kind_of ~state_kinds) st.st_else)
          ; stop = st.st_stop
          })
        f.steps
    in
    let fills = Array.of_list (List.rev layout.Strategy_ir.entries) in
    let platform_keys =
      Array.of_list
        (List.filter_map
           (fun (scope, key, _, _) ->
             if scope = Strategy_expr.Platform then Some key else None)
           (Array.to_list fills))
    in
    let written_state =
      let index_key = Hashtbl.create 32 in
      Hashtbl.iter
        (fun (scope, key) (_, index) ->
           if scope = Strategy_expr.State then Hashtbl.replace index_key index key)
        layout.Strategy_ir.kinds;
      let seen = Hashtbl.create 32 in
      let out = ref [] in
      let note (kind, index) =
        if not (Hashtbl.mem seen index)
        then (
          Hashtbl.replace seen index ();
          let key =
            match Hashtbl.find_opt index_key index with
            | Some k -> k
            | None -> ""
          in
          out := (key, kind, index) :: !out)
      in
      let note_actions =
        Array.iter (function
          | Set_state (kind, index, _) -> note (kind, index)
          | _ -> ())
      in
      Array.iter (fun (st : step) -> note_actions st.then_actions; note_actions st.else_actions)
        (Array.of_list steps);
      Array.of_list (List.rev !out)
    in
    let state_slots =
      Array.of_list
        (List.filter_map
           (fun (scope, key, kind, index) ->
              if scope = Strategy_expr.State then Some (key, kind, index) else None)
           (Array.to_list fills))
    in
    let max_slot =
      Array.fold_left
        (fun acc (scope, key, _, _) ->
           if scope = Strategy_expr.Platform
           then max acc (Strategy_runtime.slot_of_key key)
           else acc)
        0
        fills
    in
    let make_table () = Array.make (max_slot + 1) (-1) in
    let p_float = make_table ()
    and p_int = make_table ()
    and p_bool = make_table ()
    and p_string = make_table () in
    Array.iter
      (fun (scope, key, kind, index) ->
         if scope = Strategy_expr.Platform
         then (
           let slot = Strategy_runtime.slot_of_key key in
           let table =
             match kind with
             | Strategy_ir.K_float -> p_float
             | K_int -> p_int
             | K_bool -> p_bool
             | K_string -> p_string
           in
           table.(slot) <- index))
      fills;
    Ok
      { layout
      ; steps = Array.of_list steps
      ; fills
      ; platform_keys
      ; state_slots
      ; written_state
      ; p_float
      ; p_int
      ; p_bool
      ; p_string
      }
  with
  | Invalid_argument m -> Error m
;;

let exec env sink = function
  | Set_state (kind, index, e) ->
    push_cmd sink Cm_state;
    (match kind with
     | Strategy_ir.K_float -> env.Strategy_ir.floats.(index) <- Strategy_ir.as_float e env
     | K_int -> env.Strategy_ir.ints.(index) <- Strategy_ir.as_int e env
     | K_bool -> env.Strategy_ir.bools.(index) <- Strategy_ir.as_bool e env
     | K_string -> env.Strategy_ir.strings.(index) <- Strategy_ir.as_string e env)
  | Order_replace { market; side; qty; price; tif; reduce_only; dedup_key } ->
    (if sink.order_count = Array.length sink.orders
     then (
       let grown =
         Array.init
           (2 * Array.length sink.orders)
           (fun i ->
              if i < sink.order_count
              then sink.orders.(i)
              else
                { market = ""
                ; side = ""
                ; qty = 0.0
                ; price = 0.0
                ; tif = ""
                ; reduce_only = false
                ; dedup_key = ""
                })
       in
       sink.orders <- grown);
     let o = sink.orders.(sink.order_count) in
     o.market <- market;
     o.side <- side;
     o.qty <- Strategy_ir.as_float qty env;
     o.price <- Strategy_ir.as_float price env;
     o.tif <- tif;
     o.reduce_only <- reduce_only;
     o.dedup_key <- dedup_key;
     sink.order_count <- sink.order_count + 1;
     push_cmd sink Cm_order)
  | Quote_reserve e -> push_cmd sink (Cm_reserve (Strategy_ir.as_float e env))
  | Quote_release -> push_cmd sink Cm_release
  | Credit_profit { qty; seq } ->
    push_cmd sink (Cm_credit (Strategy_ir.as_int seq env, Strategy_ir.as_float qty env))
  | Engine_phase id -> push_cmd sink (Cm_phase id)
  | Noop -> ()
;;

let run program env sink =
  clear_sink sink;
  let stopped = ref false in
  Array.iter
    (fun st ->
      if not !stopped
      then (
        let pass =
          match st.guard with
          | None -> true
          | Some g -> g env
        in
        let acts = if pass then st.then_actions else st.else_actions in
        Array.iter (exec env sink) acts;
        if pass && st.stop then stopped := true))
    program.steps;
  sink.stopped <- !stopped
;;

let value_to_float = function
  | Strategy_expr.V_float f -> f
  | V_int i -> float_of_int i
  | V_bool b -> if b then 1.0 else 0.0
  | V_string s ->
    (try float_of_string s with
     | _ -> 0.0)
  | V_none | V_unset -> 0.0
;;

let value_to_int = function
  | Strategy_expr.V_int i -> i
  | V_float f -> int_of_float f
  | V_bool b -> if b then 1 else 0
  | V_string s ->
    (try int_of_string s with
     | _ -> 0)
  | V_none | V_unset -> 0
;;

let value_to_bool = function
  | Strategy_expr.V_bool b -> b
  | V_float f -> f <> 0.0
  | V_int i -> i <> 0
  | V_string s -> String.equal s "true"
  | V_none | V_unset -> false
;;

let value_to_string = function
  | Strategy_expr.V_string s -> s
  | V_float f -> string_of_float f
  | V_int i -> string_of_int i
  | V_bool b -> string_of_bool b
  | V_none | V_unset -> ""
;;

let set_typed env kind index (v : Strategy_expr.value) =
  match kind with
  | Strategy_ir.K_float -> env.Strategy_ir.floats.(index) <- value_to_float v
  | K_int -> env.ints.(index) <- value_to_int v
  | K_bool -> env.bools.(index) <- value_to_bool v
  | K_string -> env.strings.(index) <- value_to_string v
;;

(** A layout slot resolved against the runtime's interned key table once, at instance
    creation. Filling the env from strings costs a hash lookup per slot per cycle; the
    resolution is stable for the process because keys are interned. *)
type resolved_slot =
  { rs_scope : Strategy_expr.scope
  ; rs_key : string
  ; rs_kind : Strategy_ir.kind
  ; rs_env : int
  ; rs_slot : int
  }

let resolve program =
  Array.map
    (fun (scope, key, kind, index) ->
       { rs_scope = scope
       ; rs_key = key
       ; rs_kind = kind
       ; rs_env = index
       ; rs_slot = Strategy_runtime.slot_of_key key
       })
    program.fills
;;

(** Pull the runtime's state into the compiled env. An engine phase writes state through the
    runtime, so the compiled pass must read it back or a later guard would decide on the
    value from before the phase. *)
let fill_state_resolved resolved rt env =
  Array.iter
    (fun (rs : resolved_slot) ->
       if rs.rs_scope = Strategy_expr.State
       then
         set_typed
           env
           rs.rs_kind
           rs.rs_env
           (Strategy_runtime.get_state_slot rt rs.rs_slot))
    resolved
;;

(** Fact sink for a cutover: write straight into the compiled env's typed slots. Facts the
    program does not read are dropped instead of published. *)
let env_facts_io program env rt : Strategy_runtime.facts_io =
  let index table slot =
    if slot >= 0 && slot < Array.length table then Array.unsafe_get table slot else -1
  in
  { io_f =
      (fun slot v ->
        let i = index program.p_float slot in
        if i >= 0 then env.Strategy_ir.floats.(i) <- v)
  ; io_i =
      (fun slot v ->
        let i = index program.p_int slot in
        if i >= 0 then env.ints.(i) <- v)
  ; io_b =
      (fun slot v ->
        let i = index program.p_bool slot in
        if i >= 0 then env.bools.(i) <- v)
  ; io_s =
      (fun slot v ->
        let i = index program.p_string slot in
        if i >= 0 then env.strings.(i) <- v)
  ; io_get_f =
      (fun slot ->
        let i = index program.p_float slot in
        if i >= 0
        then env.floats.(i)
        else (
          match Strategy_runtime.get_platform_slot rt slot with
          | Strategy_expr.V_float f -> f
          | _ -> 0.0))
  }
;;

(** Compiled state value, read by key. The compiled env is the single source of truth for a
    cut-over instance, so its readers (reserve sizing, persistence) read here instead of the
    runtime copy. *)
let has_state_key program key =
  Hashtbl.mem program.layout.kinds (Strategy_expr.State, key)
;;

let has_platform_key program key =
  Hashtbl.mem program.layout.kinds (Strategy_expr.Platform, key)
;;

let env_value program env key =
  match Hashtbl.find_opt program.layout.kinds (Strategy_expr.State, key) with
  | None -> Strategy_expr.V_unset
  | Some (kind, index) ->
    (match kind with
     | Strategy_ir.K_float -> Strategy_expr.V_float env.Strategy_ir.floats.(index)
     | K_int -> V_int env.ints.(index)
     | K_bool -> V_bool env.bools.(index)
     | K_string -> V_string env.strings.(index))

(** Compiled value of a platform fact, read by key. *)
let env_platform_value program env key =
  match Hashtbl.find_opt program.layout.kinds (Strategy_expr.Platform, key) with
  | None -> Strategy_expr.V_unset
  | Some (kind, index) ->
    (match kind with
     | Strategy_ir.K_float -> Strategy_expr.V_float env.Strategy_ir.floats.(index)
     | K_int -> V_int env.ints.(index)
     | K_bool -> V_bool env.bools.(index)
     | K_string -> V_string env.strings.(index))

let env_state_float program env key =
  match env_value program env key with
  | Strategy_expr.V_float f -> f
  | _ -> 0.0
;;

(** Compiled value of a platform fact, for callers that log it. *)
let env_platform_float program env key =
  let slot = Strategy_runtime.slot_of_key key in
  if slot >= 0 && slot < Array.length program.p_float
  then (
    match Array.unsafe_get program.p_float slot with
    | -1 -> nan
    | i -> env.Strategy_ir.floats.(i))
  else nan
;;

(** {!fill_env} over {!resolve}d slots: no string hashing per cycle. *)
let fill_env_resolved
      ?(state = true)
      ?(platform = true)
      resolved
      rt
      env
      ~price
      ~now
      ~event_kind
      ~event_fields
  =
  Array.iter
    (fun (rs : resolved_slot) ->
       let fill =
         (state || rs.rs_scope <> Strategy_expr.State)
         && (platform || rs.rs_scope <> Strategy_expr.Platform)
       in
       if fill
       then (
         let v =
           match rs.rs_scope with
           | Strategy_expr.State -> Strategy_runtime.get_state_slot rt rs.rs_slot
           | Param -> Strategy_runtime.get_param_slot rt rs.rs_slot
           | Platform -> Strategy_runtime.get_platform_slot rt rs.rs_slot
           | Event ->
             (match List.assoc_opt rs.rs_key event_fields with
              | Some v -> v
              | None -> Strategy_expr.V_unset)
           | Local -> Strategy_expr.V_unset
           | Signal -> Strategy_runtime.get_signal_slot rt rs.rs_slot
           | Price | Now -> Strategy_expr.V_unset
         in
         set_typed env rs.rs_kind rs.rs_env v))
    resolved;
  env.Strategy_ir.price <- price;
  env.now <- now;
  env.event_kind <- event_kind;
  env.event_side
  <- (match List.assoc_opt "side" event_fields with
      | Some (Strategy_expr.V_string s) -> s
      | _ -> "")
;;

(** Fill the compiled env from the live runtime. [state=false] keeps the compiled state
    across cycles and refreshes only platform/param/event facts; [state=true] seeds the
    compiled state from the interpreter at instance creation. *)
let fill_env ?(state = true) program env rt ~price ~now ~event_kind ~event_fields =
  let fill_entry (scope, key, kind, index) =
    if state || scope <> Strategy_expr.State
    then (
      let v =
        match scope with
        | Strategy_expr.State -> Strategy_runtime.get_state rt key
        | Param -> Strategy_runtime.get_param rt key
        | Platform -> Strategy_runtime.get_platform rt key
        | Event ->
          (match List.assoc_opt key event_fields with
           | Some v -> v
           | None -> Strategy_expr.V_unset)
        | Local -> Strategy_expr.V_unset
        | Signal -> Strategy_runtime.get_signal rt key
        | Price | Now -> Strategy_expr.V_unset
      in
      set_typed env kind index v)
  in
  Array.iter fill_entry program.fills;
  env.Strategy_ir.price <- price;
  env.now <- now;
  env.event_kind <- event_kind;
  env.event_side
  <- (match List.assoc_opt "side" event_fields with
      | Some (Strategy_expr.V_string s) -> s
      | _ -> "")
;;
