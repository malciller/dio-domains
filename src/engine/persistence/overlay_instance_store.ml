(** Generic persistent state for additional overlay-strategy instances.

    The state is a JSON object of the strategy's persisted [remembers] fields. The
    strategy file decides which fields persist; the engine does not know their names. *)

let key_of ~owner_exchange ~owner_symbol ~id =
  Printf.sprintf "%s|%s|%s" owner_exchange owner_symbol id
;;

let orchestrator =
  Persistence_orchestrator.create
    ~filename:"overlay_instance_state.json"
    ~parse:(fun tree ->
      try Yojson.Basic.Util.to_assoc tree with
      | _ -> [])
    ~serialize:(fun json -> json)
;;

let load ~owner_exchange ~owner_symbol ~id =
  match
    Persistence_orchestrator.load
      orchestrator
      ~key:(key_of ~owner_exchange ~owner_symbol ~id)
  with
  | Some json -> json
  | None -> `Assoc []
;;

let save ~owner_exchange ~owner_symbol ~id json =
  Persistence_orchestrator.put
    orchestrator
    ~key:(key_of ~owner_exchange ~owner_symbol ~id)
    json
;;

let save_async ~owner_exchange ~owner_symbol ~id json =
  Persistence_orchestrator.put_async
    orchestrator
    ~key:(key_of ~owner_exchange ~owner_symbol ~id)
    json
;;
