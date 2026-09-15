(** Strategy file (JSON) parser. Produces the AST consumed by the validator. *)

exception Parse_error of string

type param_kind =
  | P_float
  | P_int
  | P_bool
  | P_string
  | P_decimal_str
  | P_range
  | P_enum of string list

type param =
  { p_name : string
  ; p_kind : param_kind
  ; p_default : Yojson.Basic.t option
  }

type state_kind =
  | S_float
  | S_float_opt
  | S_int
  | S_bool
  | S_string
  | S_buy_intent_opt
  | S_sell_intent_opt
  | S_reserve_policy

type state_decl =
  { s_name : string
  ; s_kind : state_kind
  ; s_persist : bool
  }

type on_error =
  | O_stop
  | O_continue

type action =
  { a_name : string
  ; a_args : (string * Yojson.Basic.t) list
  ; a_bind : (string * string) list
  ; a_on_error : on_error
  }

type guard =
  | G_event of string
  | G_side of string
  | G_all of guard list
  | G_any of guard list
  | G_not of guard
  | G_is_none of string
  | G_is_some of string
  | G_expr of string
  | G_capacity of (string * string) list
  | G_pending of string
  | G_order of (string * string) list
  | G_signal of (string * string) list
  | G_engine of (string * string) list
  | G_cooldown of
      { since : string
      ; seconds : string
      }

type step =
  { st_id : string
  ; st_let : (string * string) list
  ; st_when : guard option
  ; st_then : action list
  ; st_else : action list
  ; st_stop : bool
  }

type t =
  { name : string
  ; version : int
  ; triggers : string list
  ; params : param list
  ; state : state_decl list
  ; steps : step list
  }

let get (o : Yojson.Basic.t) k =
  match o with
  | `Assoc kvs -> List.assoc_opt k kvs
  | _ -> raise (Parse_error "expected object")
;;

let require o k =
  match get o k with
  | Some v -> v
  | None -> raise (Parse_error ("missing field: " ^ k))
;;

let as_string = function
  | `String s -> s
  | _ -> raise (Parse_error "expected string")
;;

let as_list = function
  | `List l -> l
  | _ -> raise (Parse_error "expected array")
;;

let scalar_to_string (v : Yojson.Basic.t) =
  match v with
  | `String s -> s
  | other -> Yojson.Basic.to_string other
;;

let kind_of_string = function
  | "float" -> P_float
  | "int" -> P_int
  | "bool" -> P_bool
  | "string" -> P_string
  | "decimal_str" -> P_decimal_str
  | "range" -> P_range
  | other -> raise (Parse_error ("unknown param type: " ^ other))
;;

let state_kind_of_string = function
  | "float" -> S_float
  | "float?" -> S_float_opt
  | "int" -> S_int
  | "bool" -> S_bool
  | "string" -> S_string
  | "buy_intent?" -> S_buy_intent_opt
  | "sell_intent?" -> S_sell_intent_opt
  | "reserve_policy" -> S_reserve_policy
  | other -> raise (Parse_error ("unknown state type: " ^ other))
;;

let parse_param (name, j) =
  let ty = as_string (require j "type") in
  let kind =
    if String.equal ty "enum"
    then (
      match get j "values" with
      | Some (`List vs) -> P_enum (List.map as_string vs)
      | _ -> raise (Parse_error "enum param requires a \"values\" array"))
    else kind_of_string ty
  in
  { p_name = name; p_kind = kind; p_default = get j "default" }
;;

let parse_state (name, j) =
  let ty = as_string (require j "type") in
  let s_persist =
    match get j "persist" with
    | Some (`Bool b) -> b
    | None -> false
    | Some _ -> raise (Parse_error "persist must be a bool")
  in
  { s_name = name; s_kind = state_kind_of_string ty; s_persist }
;;

let rec parse_guard (j : Yojson.Basic.t) : guard =
  match j with
  | `Assoc kvs ->
    let guards = List.map (fun (k, v) -> parse_guard_kv k v) kvs in
    (match guards with
     | [ g ] -> g
     | gs -> G_all gs)
  | _ -> raise (Parse_error "guard must be an object")

and parse_guard_kv k v =
  match k with
  | "event" -> G_event (as_string v)
  | "side" -> G_side (as_string v)
  | "all" -> G_all (parse_guards v)
  | "any" -> G_any (parse_guards v)
  | "not" -> G_not (parse_guard v)
  | "is_none" -> G_is_none (as_string v)
  | "is_some" -> G_is_some (as_string v)
  | "expr" -> G_expr (as_string v)
  | "capacity" -> G_capacity (parse_str_map v)
  | "pending" -> G_pending (as_string v)
  | "order" -> G_order (parse_str_map v)
  | "signal" -> G_signal (parse_str_map v)
  | "engine" -> G_engine (parse_str_map v)
  | "cooldown_elapsed" ->
    G_cooldown
      { since = as_string (require v "since"); seconds = as_string (require v "seconds") }
  | other -> raise (Parse_error ("unknown guard key: " ^ other))

and parse_guards j = List.map parse_guard (as_list j)

and parse_str_map j =
  match j with
  | `Assoc kvs -> List.map (fun (k, v) -> k, scalar_to_string v) kvs
  | _ -> raise (Parse_error "expected object")
;;

let parse_action j =
  let a_name = as_string (require j "action") in
  let a_args =
    match get j "args" with
    | Some (`Assoc kvs) -> kvs
    | Some _ -> raise (Parse_error "args must be an object")
    | None -> []
  in
  let a_bind =
    match get j "bind" with
    | Some (`Assoc kvs) -> List.map (fun (k, v) -> k, as_string v) kvs
    | Some _ -> raise (Parse_error "bind must be an object")
    | None -> []
  in
  let a_on_error =
    match get j "on_error" with
    | None -> O_stop
    | Some (`String "stop") -> O_stop
    | Some (`String "continue") -> O_continue
    | Some _ -> raise (Parse_error "on_error must be \"stop\" or \"continue\"")
  in
  { a_name; a_args; a_bind; a_on_error }
;;

let parse_step j =
  let st_id = as_string (require j "id") in
  let st_let =
    match get j "let" with
    | Some (`Assoc kvs) -> List.map (fun (k, v) -> k, as_string v) kvs
    | Some _ -> raise (Parse_error "let must be an object")
    | None -> []
  in
  let st_when = Option.map parse_guard (get j "when") in
  let actions_of k =
    match get j k with
    | Some v -> List.map parse_action (as_list v)
    | None -> []
  in
  let st_stop =
    match get j "stop" with
    | Some (`Bool b) -> b
    | None -> false
    | Some _ -> raise (Parse_error "stop must be a bool")
  in
  { st_id
  ; st_let
  ; st_when
  ; st_then = actions_of "then"
  ; st_else = actions_of "else"
  ; st_stop
  }
;;

let parse_map f j =
  match j with
  | `Assoc kvs -> List.map f kvs
  | _ -> raise (Parse_error "expected object")
;;

let parse (j : Yojson.Basic.t) : t =
  let name = as_string (require j "name") in
  let version =
    match get j "version" with
    | Some (`Int n) -> n
    | Some (`Float f) -> int_of_float f
    | None -> 1
    | Some _ -> raise (Parse_error "version must be an int")
  in
  let triggers =
    match get j "triggers" with
    | Some v -> List.map as_string (as_list v)
    | None -> []
  in
  let params =
    match get j "params" with
    | Some v -> parse_map parse_param v
    | None -> []
  in
  let state =
    match get j "state" with
    | Some v -> parse_map parse_state v
    | None -> []
  in
  let steps =
    match get j "steps" with
    | Some v -> List.map parse_step (as_list v)
    | None -> []
  in
  { name; version; triggers; params; state; steps }
;;

let parse_string (s : string) : (t, string) result =
  try Ok (parse (Yojson.Basic.from_string s)) with
  | Parse_error m -> Error m
  | exn -> Error (Printexc.to_string exn)
;;

let parse_file (path : string) : (t, string) result =
  try Ok (parse (Yojson.Basic.from_file path)) with
  | Parse_error m -> Error m
  | Sys_error m -> Error m
  | exn -> Error (Printexc.to_string exn)
;;
