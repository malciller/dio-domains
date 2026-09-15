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

type handler = { run : t -> string -> (string * value) list -> (string * value) list }

and t =
  { file : Strategy_file.t
  ; state : (string, value) Hashtbl.t
  ; params : (string, value) Hashtbl.t
  ; signals : (string, value) Hashtbl.t
  ; platform : (string, value) Hashtbl.t
  ; locals : (string, value) Hashtbl.t
  ; handlers : handler
  ; mutable caps : caps
  ; mutable event : event option
  ; mutable price : float
  ; mutable now : float
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

let expand_params (file : Strategy_file.t) (overrides : (string * value) list) =
  let tbl = Hashtbl.create 32 in
  let set k v = Hashtbl.replace tbl k v in
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
  tbl
;;

let create ?(handlers = noop_handler) ?(params = []) (file : Strategy_file.t) =
  let state = Hashtbl.create 16 in
  List.iter
    (fun (s : Strategy_file.state_decl) ->
      Hashtbl.replace state s.s_name (default_of_kind s.s_kind))
    file.state;
  { file
  ; state
  ; params = expand_params file params
  ; signals = Hashtbl.create 8
  ; platform = Hashtbl.create 8
  ; locals = Hashtbl.create 8
  ; handlers
  ; caps = default_caps
  ; event = None
  ; price = nan
  ; now = 0.0
  }
;;

let set_state t k v = Hashtbl.replace t.state k v
let get_state t k = Hashtbl.find_opt t.state k
let set_platform t k v = Hashtbl.replace t.platform k v
let set_signal t k v = Hashtbl.replace t.signals k v
let set_caps t c = t.caps <- c
let make_event kind fields = { ev_kind = kind; ev_fields = fields }
let current_event t = t.event

let env_of t : env =
  { price = (fun () -> Ok (V_float t.price))
  ; event =
      (fun f ->
        match t.event with
        | None -> Error ("no event in context (field " ^ f ^ ")")
        | Some e ->
          (match List.assoc_opt f e.ev_fields with
           | Some v -> Ok v
           | None -> Error ("unknown event field: " ^ f)))
  ; state =
      (fun dotted ->
        Ok
          (match Hashtbl.find_opt t.state dotted with
           | Some v -> v
           | None -> V_none))
  ; param =
      (fun n ->
        match Hashtbl.find_opt t.params n with
        | Some v -> Ok v
        | None -> Error ("unknown param: " ^ n))
  ; local =
      (fun n ->
        match Hashtbl.find_opt t.locals n with
        | Some v -> Ok v
        | None -> Error ("unknown local: " ^ n))
  ; signal =
      (fun n ->
        match Hashtbl.find_opt t.signals n with
        | Some v -> Ok v
        | None -> Error ("unknown signal: " ^ n))
  ; now = (fun () -> Ok (V_float t.now))
  ; platform =
      (fun c ->
        match Hashtbl.find_opt t.platform c with
        | Some v -> Ok v
        | None -> Error ("unknown platform fact: " ^ c))
  }
;;

let facts_of t : Strategy_guard.facts =
  { event_kind = (fun () -> Option.map (fun e -> e.ev_kind) t.event)
  ; side =
      (fun () ->
        match t.event with
        | Some e ->
          (match List.assoc_opt "side" e.ev_fields with
           | Some (V_string s) -> Some s
           | _ -> None)
        | None -> None)
  ; pending =
      (fun p ->
        match Hashtbl.find_opt t.platform ("pending:" ^ p) with
        | Some (V_bool b) -> b
        | _ -> false)
  ; engine_flag = (fun k -> Hashtbl.find_opt t.platform ("engine:" ^ k))
  ; capacity = (fun a -> Hashtbl.find_opt t.platform ("capacity:" ^ a))
  ; signal = (fun n -> Hashtbl.find_opt t.signals n)
  ; order_posture = (fun _ _ -> false)
  }
;;

let value_of_json (e : env) (j : Yojson.Basic.t) : (value, string) result =
  match j with
  | `Int i -> Ok (V_int i)
  | `Float f -> Ok (V_float f)
  | `Bool b -> Ok (V_bool b)
  | `Null -> Ok V_none
  | `String s -> eval_arg e s
  | _ -> Error "unsupported argument value"
;;

let field_of_bind (rhs : string) =
  let prefix = "$out." in
  let n = String.length prefix in
  if String.length rhs > n && String.equal (String.sub rhs 0 n) prefix
  then Some (String.sub rhs n (String.length rhs - n))
  else None
;;

let run_action t (e : env) step_id (a : Strategy_file.action) : action_call =
  let args =
    List.filter_map
      (fun (k, j) ->
        match value_of_json e j with
        | Ok v -> Some (k, v)
        | Error _ -> None)
      a.a_args
  in
  let call = { ac_step = step_id; ac_action = a.a_name; ac_args = args } in
  let out = t.handlers.run t a.a_name args in
  List.iter
    (fun (var, rhs) ->
      match field_of_bind rhs with
      | None -> ()
      | Some field ->
        (match List.assoc_opt field out with
         | Some v -> Hashtbl.replace t.locals var v
         | None -> ()))
    a.a_bind;
  call
;;

(** Run one cycle for a dispatch kind. The snapshot phase is
    [make_event "book_update" []]; execution events carry their fields. Returns the
    ordered action calls made this cycle. *)
let run_cycle t ~(price : float) ~(now : float) ~(event : event) : action_call list =
  t.price <- price;
  t.now <- now;
  t.event <- Some event;
  let e = env_of t in
  let facts = facts_of t in
  let calls = ref [] in
  let stop = ref false in
  List.iter
    (fun (step : Strategy_file.step) ->
      if not !stop
      then (
        Hashtbl.reset t.locals;
        List.iter
          (fun (name, s) ->
            match eval_arg e s with
            | Ok v -> Hashtbl.replace t.locals name v
            | Error _ -> ())
          step.st_let;
        let passed =
          match step.st_when with
          | None -> true
          | Some g ->
            (match Strategy_guard.eval e facts g with
             | Ok b -> b
             | Error _ -> false)
        in
        let actions = if passed then step.st_then else step.st_else in
        List.iter (fun a -> calls := run_action t e step.st_id a :: !calls) actions;
        if passed && step.st_stop then stop := true))
    t.file.steps;
  List.rev !calls
;;
