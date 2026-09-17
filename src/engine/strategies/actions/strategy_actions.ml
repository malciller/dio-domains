(** Strategy action registry — the single code-side extension surface.

    Actions are named, schema-validated capabilities that user-authored strategy files
    compose into steps. This slice supports declaration and validation; handlers are
    attached when the interpreter runtime lands. *)

type arg_kind =
  | A_float
  | A_int
  | A_bool
  | A_string
  | A_decimal_str
  | A_enum of string list
  | A_expr

type schema_entry =
  { arg : string
  ; kind : arg_kind
  ; required : bool
  }

type class_ =
  | Decision
  | Effectful
  | Read

type value =
  | V_unset
    (* Slot-array sentinel: "this fact has never been published". Never a user value and
       never produced by evaluation; lets a slot hold a [value] directly instead of a
       [value option], so publishing a fact stores it without allocating a [Some] box on
       the hot path. *)
  | V_none
  | V_float of float
  | V_int of int
  | V_bool of bool
  | V_string of string

type event =
  { ev_kind : string
  ; ev_fields : (string * value) list
  }

type instance = { inst_id : string }

type ctx =
  { instance : instance
  ; event : event option
  ; now : float
  }

type args = { slots : value array }
type out = { slots : value array }
type handler = ctx -> args -> out Lwt.t

type t =
  { name : string
  ; class_ : class_
  ; schema : schema_entry list
  ; handler : handler option
  }

let schema_entry ?(required = true) arg kind = { arg; kind; required }
let _registry : (string, t) Hashtbl.t = Hashtbl.create 64

(** Register an action. Raises [Invalid_argument] on a duplicate name. *)
let register (a : t) : unit =
  if Hashtbl.mem _registry a.name
  then invalid_arg (Printf.sprintf "Strategy_actions: duplicate action %S" a.name);
  Hashtbl.replace _registry a.name a
;;

let try_register (a : t) : (unit, string) result =
  if Hashtbl.mem _registry a.name
  then Error (Printf.sprintf "duplicate action %S" a.name)
  else (
    Hashtbl.replace _registry a.name a;
    Ok ())
;;

let find name = Hashtbl.find_opt _registry name
let is_registered name = Hashtbl.mem _registry name

let all_names () =
  Hashtbl.fold (fun k _ acc -> k :: acc) _registry [] |> List.sort String.compare
;;

let clear () = Hashtbl.reset _registry
