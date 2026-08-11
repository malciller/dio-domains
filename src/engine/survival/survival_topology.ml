(* Qualified multi-asset topology for the survival portfolio.

   The live config uses flat symbols and class members, but a survival pool
   must distinguish e.g. hyperliquid/BTC/USDC from kraken/BTC/USD. This module
   owns that identity, validates explicit allocations/transfers, and aligns
   histories on a shared ISO-date timeline without forward-filling gaps. *)

type instrument_key =
  { venue : string
  ; symbol : string
  ; base : string
  ; quote : string
  ; testnet : bool
  }

type position_spec =
  { key : instrument_key
  ; capital : float option
  }

type transfer_spec =
  { session : int
  ; from_key : instrument_key
  ; to_key : instrument_key
  ; amount : float
  }

type definition =
  { positions : position_spec list
  ; transfers : transfer_spec list
  }

let bind result f =
  match result with
  | Ok value -> f value
  | Error error -> Error error
;;

let map_result result f =
  match result with
  | Ok value -> Ok (f value)
  | Error error -> Error error
;;

let default_quote venue =
  match String.lowercase_ascii venue with
  | "hyperliquid" | "lighter" -> "USDC"
  | _ -> "USD"
;;

let split_symbol ~venue symbol =
  let symbol = String.trim symbol in
  match String.index_opt symbol '/' with
  | Some slash when slash > 0 && slash < String.length symbol - 1 ->
    ( String.sub symbol 0 slash |> String.uppercase_ascii
    , String.sub symbol (slash + 1) (String.length symbol - slash - 1)
      |> String.uppercase_ascii )
  | _ -> String.uppercase_ascii symbol, default_quote venue
;;

let key ~venue ~symbol ?(testnet = false) () =
  let base, quote = split_symbol ~venue symbol in
  { venue = String.lowercase_ascii (String.trim venue)
  ; symbol = String.trim symbol
  ; base
  ; quote
  ; testnet
  }
;;

let key_id (key : instrument_key) =
  Printf.sprintf "%s/%s%s" key.venue key.symbol (if key.testnet then "@testnet" else "")
;;

let equal_key (left : instrument_key) (right : instrument_key) =
  String.lowercase_ascii left.venue = String.lowercase_ascii right.venue
  && String.lowercase_ascii left.symbol = String.lowercase_ascii right.symbol
  && left.testnet = right.testnet
;;

let key_of_id value =
  let value = String.trim value in
  let value, testnet =
    if
      String.length value >= 8
      && String.sub value (String.length value - 8) 8 = "@testnet"
    then String.sub value 0 (String.length value - 8), true
    else value, false
  in
  match String.index_opt value '/' with
  | Some slash when slash > 0 && slash < String.length value - 1 ->
    let venue = String.sub value 0 slash in
    let symbol = String.sub value (slash + 1) (String.length value - slash - 1) in
    Ok (key ~venue ~symbol ~testnet ())
  | _ -> Error ("invalid instrument key: " ^ value)
;;

let split_once value delimiter =
  match String.index_opt value delimiter with
  | None -> None
  | Some index ->
    Some
      ( String.sub value 0 index
      , String.sub value (index + 1) (String.length value - index - 1) )
;;

let parse_allocation value : (position_spec, string) result =
  match split_once (String.trim value) '=' with
  | None -> Error "allocation must use VENUE/SYMBOL=AMOUNT"
  | Some (identity, amount) ->
    (match key_of_id (String.trim identity) with
     | Error error -> Error error
     | Ok key ->
       (try
          let capital = float_of_string (String.trim amount) in
          Ok { key; capital = Some capital }
        with
        | _ -> Error ("invalid allocation amount: " ^ amount)))
;;

let parse_transfer value : (transfer_spec, string) result =
  match split_once (String.trim value) '=' with
  | None -> Error "transfer must use SESSION:FROM->TO=AMOUNT"
  | Some (route, amount) ->
    (match split_once route ':' with
     | None -> Error "transfer must include a session before ':'"
     | Some (session, endpoints) ->
       (match split_once endpoints '-' with
        | Some (from_key, rest) when String.length rest > 1 && rest.[0] = '>' ->
          let to_key = String.sub rest 1 (String.length rest - 1) in
          (try
             bind
               (key_of_id (String.trim from_key))
               (fun from_key ->
                  bind
                    (key_of_id (String.trim to_key))
                    (fun to_key ->
                       Ok
                         { session = int_of_string (String.trim session)
                         ; from_key
                         ; to_key
                         ; amount = float_of_string (String.trim amount)
                         }))
           with
           | _ -> Error "invalid transfer session or amount")
        | _ -> Error "transfer route must use FROM->TO"))
;;

let float_field json field =
  match Yojson.Safe.Util.member field json with
  | `Float value -> Some value
  | `Int value -> Some (float_of_int value)
  | `Intlit value | `String value ->
    (try Some (float_of_string value) with
     | _ -> None)
  | _ -> None
;;

let key_from_json json =
  let open Yojson.Safe.Util in
  match
    to_string_option (member "venue" json), to_string_option (member "symbol" json)
  with
  | Some venue, Some symbol ->
    let testnet = to_bool_option (member "testnet" json) |> Option.value ~default:false in
    Ok (key ~venue ~symbol ~testnet ())
  | _ -> Error "topology position requires venue and symbol"
;;

let parse (json : Yojson.Safe.t) : (definition, string) result =
  let open Yojson.Safe.Util in
  let positions_json =
    match member "positions" json with
    | `List values -> Ok values
    | _ -> Error "topology requires a positions array"
  in
  bind positions_json (fun position_values ->
    let positions =
      List.map
        (fun value ->
           bind (key_from_json value) (fun key ->
             let capital =
               match member "capital" value with
               | `Null -> Ok None
               | _ ->
                 (match float_field value "capital" with
                  | Some value -> Ok (Some value)
                  | None -> Error ("invalid capital for " ^ key_id key))
             in
             map_result capital (fun capital -> { key; capital })))
        position_values
    in
    let rec collect_positions acc = function
      | [] -> Ok (List.rev acc)
      | item :: rest -> bind item (fun item -> collect_positions (item :: acc) rest)
    in
    bind (collect_positions [] positions) (fun positions ->
      let transfers_json =
        match member "transfers" json with
        | `Null -> Ok []
        | `List values -> Ok values
        | _ -> Error "topology transfers must be an array"
      in
      bind transfers_json (fun transfer_values ->
        let parse_transfer value =
          let session =
            match member "session" value with
            | `Int value -> Ok value
            | `Intlit value | `String value ->
              (try Ok (int_of_string value) with
               | _ -> Error "invalid transfer session")
            | _ -> Error "transfer requires an integer session"
          in
          let endpoint name =
            match to_string_option (member name value) with
            | None -> Error ("transfer requires " ^ name)
            | Some value -> key_of_id value
          in
          bind session (fun session ->
            bind (endpoint "from") (fun from_key ->
              bind (endpoint "to") (fun to_key ->
                match float_field value "amount" with
                | Some amount -> Ok { session; from_key; to_key; amount }
                | None -> Error "transfer requires a numeric amount")))
        in
        let transfers = List.map parse_transfer transfer_values in
        let rec collect_transfers acc = function
          | [] -> Ok (List.rev acc)
          | item :: rest -> bind item (fun item -> collect_transfers (item :: acc) rest)
        in
        map_result (collect_transfers [] transfers) (fun transfers ->
          { positions; transfers }))))
;;

let load path : (definition, string) result =
  try parse (Yojson.Safe.from_file path) with
  | exn ->
    Error (Printf.sprintf "cannot load topology %s: %s" path (Printexc.to_string exn))
;;

let validate (definition : definition) : (unit, string list) result =
  let errors = ref [] in
  let add error = errors := error :: !errors in
  List.iter
    (fun (position : position_spec) ->
       match position.capital with
       | Some value when (not (Float.is_finite value)) || value < 0.0 ->
         add ("capital must be finite and non-negative: " ^ key_id position.key)
       | _ -> ())
    definition.positions;
  let rec duplicate = function
    | [] -> ()
    | (position : position_spec) :: rest ->
      if List.exists (fun other -> equal_key position.key other.key) rest
      then add ("duplicate topology position: " ^ key_id position.key);
      duplicate rest
  in
  duplicate definition.positions;
  let has_position key =
    List.exists
      (fun (position : position_spec) -> equal_key key position.key)
      definition.positions
  in
  List.iter
    (fun (transfer : transfer_spec) ->
       if transfer.session < 0 then add "transfer session must be non-negative";
       if (not (Float.is_finite transfer.amount)) || transfer.amount < 0.0
       then add "transfer amount must be finite and non-negative";
       if not (has_position transfer.from_key)
       then add ("unknown transfer source: " ^ key_id transfer.from_key);
       if not (has_position transfer.to_key)
       then add ("unknown transfer destination: " ^ key_id transfer.to_key);
       if transfer.from_key.quote <> transfer.to_key.quote
       then
         add
           ("cross-quote transfer is not allowed: "
            ^ key_id transfer.from_key
            ^ " -> "
            ^ key_id transfer.to_key))
    definition.transfers;
  match List.rev !errors with
  | [] -> Ok ()
  | errors -> Error errors
;;

let definition_of_tasks (tasks : Survival_tasks.task list) : definition =
  { positions =
      List.map
        (fun (task : Survival_tasks.task) ->
           { key =
               key
                 ~venue:task.exchange
                 ~symbol:task.symbol
                 ~testnet:task.config.testnet
                 ()
           ; capital = None
           })
        tasks
  ; transfers = []
  }
;;

let timeline_of_series (series : Survival_types.series list) =
  let dates =
    List.fold_left
      (fun dates (value : Survival_types.series) ->
         Array.fold_left
           (fun dates bar -> bar.Survival_types.date :: dates)
           dates
           value.bars)
      []
      series
  in
  List.sort_uniq String.compare dates |> Array.of_list
;;

let align_series (timeline : string array) (series : Survival_types.series) =
  let by_date = Hashtbl.create (Array.length series.bars) in
  Array.iter (fun bar -> Hashtbl.replace by_date bar.Survival_types.date bar) series.bars;
  Array.map (fun date -> Hashtbl.find_opt by_date date) timeline
;;

let to_pool_key (key : instrument_key) : Survival_portfolio.pool_key =
  { venue = key.venue; asset = key.symbol }
;;

let to_portfolio_transfer (transfer : transfer_spec) : Survival_portfolio.transfer =
  { session = transfer.session
  ; from = to_pool_key transfer.from_key
  ; to_ = to_pool_key transfer.to_key
  ; amount = transfer.amount
  }
;;

let position_json (position : position_spec) =
  `Assoc
    ([ "venue", `String position.key.venue
     ; "symbol", `String position.key.symbol
     ; "base", `String position.key.base
     ; "quote", `String position.key.quote
     ; "testnet", `Bool position.key.testnet
     ]
     @
     match position.capital with
     | None -> []
     | Some capital -> [ "capital", `Float capital ])
;;

let transfer_json (transfer : transfer_spec) =
  `Assoc
    [ "session", `Int transfer.session
    ; "from", `String (key_id transfer.from_key)
    ; "to", `String (key_id transfer.to_key)
    ; "amount", `Float transfer.amount
    ]
;;

let to_json (definition : definition) =
  `Assoc
    [ "positions", `List (List.map position_json definition.positions)
    ; "transfers", `List (List.map transfer_json definition.transfers)
    ]
;;
