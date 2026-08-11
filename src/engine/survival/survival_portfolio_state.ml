(* Small, explicit persistence format for survival portfolio allocations.

   This is intentionally separate from the live strategy's
   accumulated_state.json: the survival runner stores account/pool state by
   qualified instrument identity, never by bare symbol. *)

type position =
  { key : Survival_topology.instrument_key
  ; pool : float
  ; base : float
  }

let number json field =
  match Yojson.Safe.Util.member field json with
  | `Float value -> Some value
  | `Int value -> Some (float_of_int value)
  | `Intlit value | `String value ->
    (try Some (float_of_string value) with
     | _ -> None)
  | _ -> None
;;

let parse_position json : (position, string) result =
  let open Yojson.Safe.Util in
  match
    to_string_option (member "venue" json), to_string_option (member "symbol" json)
  with
  | Some venue, Some symbol ->
    let testnet = to_bool_option (member "testnet" json) |> Option.value ~default:false in
    let pool = number json "pool" in
    let base = number json "base" in
    (match pool, base with
     | Some pool, Some base
       when Float.is_finite pool && Float.is_finite base && pool >= 0.0 && base >= 0.0 ->
       Ok { key = Survival_topology.key ~venue ~symbol ~testnet (); pool; base }
     | _ -> Error ("invalid saved position: " ^ venue ^ "/" ^ symbol))
  | _ -> Error "saved position requires venue and symbol"
;;

let load path : (position list, string) result =
  try
    let json = Yojson.Safe.from_file path in
    let values =
      match Yojson.Safe.Util.member "positions" json with
      | `List values -> values
      | _ -> raise (Failure "saved state requires a positions array")
    in
    let parsed = List.map parse_position values in
    let rec collect acc = function
      | [] -> Ok (List.rev acc)
      | Ok value :: rest -> collect (value :: acc) rest
      | Error error :: _ -> Error error
    in
    collect [] parsed
  with
  | exn ->
    Error (Printf.sprintf "cannot load positions %s: %s" path (Printexc.to_string exn))
;;

let position_json (value : position) =
  `Assoc
    [ "venue", `String value.key.venue
    ; "symbol", `String value.key.symbol
    ; "testnet", `Bool value.key.testnet
    ; "pool", `Float value.pool
    ; "base", `Float value.base
    ]
;;

let save path (positions : position list) =
  let json =
    `Assoc
      [ "version", `Int 1
      ; "saved_at", `Float (Unix.gettimeofday ())
      ; "positions", `List (List.map position_json positions)
      ]
  in
  let temporary = path ^ ".tmp" in
  Yojson.Safe.to_file temporary json;
  Sys.rename temporary path
;;
