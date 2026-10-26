(** Lighter [market_stats] feed: funding, mark, index, and premium per perp market.

    Subscribed per market on the public WebSocket. Parsed snapshots are published to
    [Dio_exchange.Market_context] so strategy code reads them without depending on this
    package. *)

let exchange = "lighter"

let float_of_json json =
  match json with
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

let float_opt = function
  | `Null -> None
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `Intlit s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | `String s ->
    (try Some (float_of_string (String.trim s)) with
     | _ -> None)
  | _ -> None
;;

let process_stats stats =
  let open Yojson.Safe.Util in
  let market_id =
    try member "market_id" stats |> to_int with
    | _ -> -1
  in
  let raw_symbol =
    try member "symbol" stats |> to_string with
    | _ -> ""
  in
  let symbol =
    if raw_symbol <> ""
    then raw_symbol
    else if market_id >= 0
    then
      Option.value
        (Lighter_instruments_feed.get_symbol ~market_index:market_id)
        ~default:""
    else ""
  in
  let coin = Lighter_types.base_coin symbol in
  let mark = float_of_json (member "mark_price" stats) in
  let oracle = float_of_json (member "index_price" stats) in
  if coin <> "" && mark > 0.0
  then (
    let funding =
      match float_opt (member "current_funding_rate" stats) with
      | Some v -> v
      | None -> float_of_json (member "funding_rate" stats)
    in
    let premium_pct = float_of_json (member "premium" stats) in
    Dio_exchange.Market_context.update_funding
      ~exchange
      ~coin
      { funding
      ; premium = premium_pct /. 100.0
      ; mark
      ; oracle
      ; updated_at = Unix.gettimeofday ()
      };
    Concurrency.Exchange_wakeup.signal ~symbol:coin)
;;

let process_market_data json =
  let open Yojson.Safe.Util in
  let process_node = function
    | `Assoc _ as node -> process_stats node
    | _ -> ()
  in
  match member "market_stats" json with
  | `Assoc _ as stats ->
    (match member "symbol" stats with
     | `String _ -> process_stats stats
     | _ -> List.iter (fun (_, v) -> process_node v) (to_assoc stats))
  | `List items -> List.iter process_node items
  | _ -> ()
;;
