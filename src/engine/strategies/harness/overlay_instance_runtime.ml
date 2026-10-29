(** Generic glue between an overlay strategy's persisted [remembers] and the overlay
    store.

    The bound strategy file owns the field names and which fields persist. The engine only
    maps file state declarations to JSON values. *)

module Store = Dio_persistence.Overlay_instance_store
module Rt = Strategy_runtime
module Expr = Strategy_expr
module File = Strategy_file

let float_of_json json =
  match json with
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

let int_of_json json =
  match json with
  | `Int i -> i
  | `Float f -> int_of_float f
  | `String s ->
    (try int_of_string s with
     | _ -> 0)
  | _ -> 0
;;

let value_of_json kind json =
  match kind with
  | File.S_float -> Expr.V_float (float_of_json json)
  | File.S_float_opt ->
    (match json with
     | `Null -> Expr.V_none
     | _ -> Expr.V_float (float_of_json json))
  | File.S_int -> Expr.V_int (int_of_json json)
  | File.S_bool ->
    (match json with
     | `Bool b -> Expr.V_bool b
     | `String "true" -> Expr.V_bool true
     | _ -> Expr.V_bool false)
  | File.S_string -> Expr.V_string (Yojson.Basic.Util.to_string json)
  | File.S_buy_intent_opt | File.S_sell_intent_opt | File.S_reserve_policy -> Expr.V_none
;;

let json_of_value = function
  | Expr.V_float f -> `Float f
  | Expr.V_int i -> `Int i
  | Expr.V_bool b -> `Bool b
  | Expr.V_string s -> `String s
  | Expr.V_none | Expr.V_unset -> `Null
;;

let seed rt state =
  let open Yojson.Basic.Util in
  List.iter
    (fun (decl : File.state_decl) ->
      if decl.s_persist
      then (
        match state |> member decl.s_name with
        | `Null -> ()
        | json -> Rt.set_state rt decl.s_name (value_of_json decl.s_kind json)))
    rt.file.state
;;

let snapshot rt =
  `Assoc
    (List.filter_map
       (fun (decl : File.state_decl) ->
         if decl.s_persist
         then Some (decl.s_name, json_of_value (Rt.get_state rt decl.s_name))
         else None)
       rt.file.state)
;;

let save_if_changed ~owner_exchange ~owner_symbol ~id rt (previous : Yojson.Basic.t ref) =
  let next = snapshot rt in
  if next <> !previous
  then (
    previous := next;
    Store.save_async ~owner_exchange ~owner_symbol ~id next)
;;

let params_of_file (file : Strategy_file.t) (json : Yojson.Basic.t) =
  let open Yojson.Basic.Util in
  List.filter_map
    (fun (p : File.param) ->
      match json |> member p.p_name with
      | `Null -> None
      | value ->
        let value =
          match p.p_kind with
          | File.P_float -> Expr.V_float (float_of_json value)
          | File.P_int -> Expr.V_int (int_of_json value)
          | File.P_bool ->
            (match value with
             | `Bool b -> Expr.V_bool b
             | `String "true" -> Expr.V_bool true
             | _ -> Expr.V_bool false)
          | File.P_string | File.P_enum _ -> Expr.V_string (to_string value)
          | File.P_decimal_str -> Expr.V_string (to_string value)
          | File.P_range -> Expr.V_float (float_of_json value)
        in
        Some (p.p_name, value))
    file.params
;;
