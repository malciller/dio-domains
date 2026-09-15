(** Expression reference scanning for strategy files.

    Full expression parsing/compilation lands with the interpreter. This module extracts
    every [$...] reference so the validator can confirm that state, params, events, locals
    and platform facts resolve. *)

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
  }

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
     | Some scope -> Ok { scope; path }
     | None -> Error ("unknown reference scope: $" ^ head))
;;

let refs_in (s : string) : (ref_, string) result list = List.map parse_ref (raw_refs s)
