(* Account balance snapshots for the survival oracle.

   Two sources, chosen by the caller's context:

   1. The live exchange balance stores (websocket-fed by the engine's
      supervisor) - [snapshot_of_live_store]. Preferred by the live runtime
      (oracle_runtime.ml): the data is already in-process over websocket
      channels, so the oracle pass never pays a standalone HTTP round-trip.
      Best-effort: an unregistered exchange or an empty store yields None and
      the caller falls back to REST.

   2. One-shot REST fetches - [fetch_account] / [fetch_task]. Used by the
      standalone CLI (bin/oracle.ml), which has no supervisor, and by the
      runtime when the live store is unavailable. Read-only snapshots with
      pure parsing functions for fixture tests.

   Hyperliquid is always REST: the live balance store aggregates the perp
   clearinghouse USDC into the same "USDC" entry as spot, while the oracle's
   pool must count spot capital only (perp margin is not grid capital), so
   the REST spotClearinghouseState path stays authoritative there. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

type balance =
  { asset : string
  ; available : float
  ; total : float
  ; wallet_type : string
  ; wallet_id : string
  }

type snapshot =
  { exchange : string
  ; testnet : bool
  ; balances : balance list
  ; fetched_at : float
  }

let number = function
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

let field_float json name =
  try Yojson.Safe.Util.member name json |> number with
  | _ -> None
;;

let nonnegative f = if f < 0.0 then 0.0 else f

let normalize_kraken_asset raw =
  let upper = String.uppercase_ascii (String.trim raw) in
  let upper =
    List.fold_left
      (fun value suffix ->
         if
           String.length value > String.length suffix
           && String.sub
                value
                (String.length value - String.length suffix)
                (String.length suffix)
              = suffix
         then String.sub value 0 (String.length value - String.length suffix)
         else value)
      upper
      [ ".HOLD"; ".F"; ".B" ]
  in
  match upper with
  | "XXBT" | "XBT" -> "BTC"
  | "XETH" -> "ETH"
  | "XXDG" -> "DOGE"
  | "ZUSD" -> "USD"
  | "ZEUR" -> "EUR"
  | "ZGBP" -> "GBP"
  | "ZJPY" -> "JPY"
  | value -> value
;;

let normalize_hyperliquid_asset raw =
  match String.uppercase_ascii (String.trim raw) with
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | value -> value
;;

let error_fields json =
  match Yojson.Safe.Util.member "error" json with
  | `List [] | `Null -> None
  | `List errors ->
    Some
      (List.filter_map
         (function
           | `String s -> Some s
           | other ->
             (try Some (Yojson.Safe.to_string other) with
              | _ -> None))
         errors
       |> String.concat "; ")
  | other -> Some (Yojson.Safe.to_string other)
;;

let parse_kraken (json : Yojson.Safe.t) : (balance list, string) result =
  match error_fields json with
  | Some error when error <> "" -> Error ("Kraken API error: " ^ error)
  | _ ->
    (match Yojson.Safe.Util.member "result" json with
     | `Assoc entries ->
       let parsed =
         List.filter_map
           (fun (asset, value) ->
              match number value with
              | Some total when Float.is_finite total ->
                Some
                  { asset = normalize_kraken_asset asset
                  ; available = nonnegative total
                  ; total = nonnegative total
                  ; wallet_type = "trade"
                  ; wallet_id = "account"
                  }
              | _ -> None)
           entries
       in
       if List.length parsed = List.length entries
       then Ok parsed
       else Error "Kraken balance response contained a malformed amount"
     | _ -> Error "Kraken balance response has no result object")
;;

let rec unwrap_hyperliquid_data json =
  match Yojson.Safe.Util.member "data" json with
  | `Null -> json
  | `Assoc _ as data -> unwrap_hyperliquid_data data
  | _ -> json
;;

let parse_hyperliquid_spot (json : Yojson.Safe.t) : (balance list, string) result =
  let json = unwrap_hyperliquid_data json in
  match Yojson.Safe.Util.member "balances" json with
  | `List entries ->
    let parsed =
      List.filter_map
        (fun entry ->
           let open Yojson.Safe.Util in
           match to_string_option (member "coin" entry), field_float entry "total" with
           | Some coin, Some total when Float.is_finite total ->
             let hold = Option.value (field_float entry "hold") ~default:0.0 in
             Some
               { asset = normalize_hyperliquid_asset coin
               ; available = nonnegative (total -. hold)
               ; total = nonnegative total
               ; wallet_type = "spot"
               ; wallet_id = "account"
               }
           | _ -> None)
        entries
    in
    if List.length parsed = List.length entries
    then Ok parsed
    else Error "Hyperliquid spot response contained a malformed balance"
  | _ -> Error "Hyperliquid spot response has no balances list"
;;

let parse_alpaca_account (json : Yojson.Safe.t) : (balance list, string) result =
  match field_float json "cash", field_float json "equity" with
  | Some cash, Some equity when Float.is_finite cash && Float.is_finite equity ->
    let currency =
      Yojson.Safe.Util.member "currency" json
      |> Yojson.Safe.Util.to_string_option
      |> Option.value ~default:"USD"
      |> String.uppercase_ascii
    in
    Ok
      [ { asset = currency
        ; available = nonnegative cash
        ; total = nonnegative equity
        ; wallet_type = "cash"
        ; wallet_id = "account"
        }
      ]
  | _ -> Error "Alpaca account response is missing cash or equity"
;;

let parse_alpaca_positions (json : Yojson.Safe.t) : (balance list, string) result =
  match json with
  | `List entries ->
    let parsed =
      List.filter_map
        (fun entry ->
           let open Yojson.Safe.Util in
           match to_string_option (member "symbol" entry), field_float entry "qty" with
           | Some asset, Some qty when Float.is_finite qty ->
             Some
               { asset = String.uppercase_ascii asset
               ; available = nonnegative qty
               ; total = nonnegative qty
               ; wallet_type = "position"
               ; wallet_id = "account"
               }
           | _ -> None)
        entries
    in
    if List.length parsed = List.length entries
    then Ok parsed
    else Error "Alpaca positions response contained a malformed position"
  | _ -> Error "Alpaca positions response is not an array"
;;

let merge_balances balances =
  let add balance acc =
    match
      List.find_opt
        (fun current ->
           current.asset = balance.asset
           && current.wallet_type = balance.wallet_type
           && current.wallet_id = balance.wallet_id)
        acc
    with
    | None -> balance :: acc
    | Some current ->
      List.map
        (fun value ->
           if value == current
           then
             { value with
               available = value.available +. balance.available
             ; total = value.total +. balance.total
             }
           else value)
        acc
  in
  List.fold_left (fun acc balance -> add balance acc) [] balances |> List.rev
;;

let available_quote (snapshot : snapshot) ~(quote : string) =
  let quote = String.uppercase_ascii (String.trim quote) in
  snapshot.balances
  |> List.fold_left
       (fun total balance ->
          if String.uppercase_ascii balance.asset = quote
          then total +. balance.available
          else total)
       0.0
;;

(** Available (unlocked) balance of one base asset: what the strategy can
    actually sell or the sizing can count as held inventory. The oracle seeds
    its replay grid with this. *)
let available_asset (snapshot : snapshot) ~(asset : string) =
  let asset = String.uppercase_ascii (String.trim asset) in
  snapshot.balances
  |> List.fold_left
       (fun total balance ->
          if String.uppercase_ascii balance.asset = asset
          then total +. balance.available
          else total)
       0.0
;;

let total_asset (snapshot : snapshot) ~(asset : string) =
  let asset = String.uppercase_ascii (String.trim asset) in
  snapshot.balances
  |> List.fold_left
       (fun total balance ->
          if String.uppercase_ascii balance.asset = asset
          then total +. balance.total
          else total)
       0.0
;;

let load_dotenv () =
  try Dotenv.export ~path:".env" () with
  | _ -> ()
;;

let post_json ~url (payload : Yojson.Safe.t) : (Yojson.Safe.t, string) result Lwt.t =
  let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
  let body = Cohttp_lwt.Body.of_string (Yojson.Safe.to_string payload) in
  Lwt.catch
    (fun () ->
       Oracle_http.post ~headers ~body (Uri.of_string url)
       >>= fun (response, response_body) ->
       Cohttp_lwt.Body.to_string response_body
       >|= fun body ->
       let status = Cohttp.Response.status response |> Cohttp.Code.code_of_status in
       if status < 200 || status >= 300
       then Error (Printf.sprintf "HTTP %d: %s" status body)
       else (
         try Ok (Yojson.Safe.from_string body) with
         | exn ->
           Error (Printf.sprintf "invalid JSON response: %s" (Printexc.to_string exn))))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

let fetch_kraken ~testnet:_ () : (balance list, string) result Lwt.t =
  Lwt.catch
    (fun () ->
       Kraken.Kraken_generate_auth_token.get_api_credentials_from_env ()
       >>= fun (api_key, api_secret) ->
       let path = "/0/private/Balance" in
       let nonce = Kraken.Kraken_common_types.nonce () in
       let body = "nonce=" ^ nonce in
       let signature =
         Kraken.Kraken_common_types.sign ~secret:api_secret ~path ~body ~nonce
       in
       let headers =
         Cohttp.Header.of_list
           [ "API-Key", api_key
           ; "API-Sign", signature
           ; "Content-Type", "application/x-www-form-urlencoded"
           ]
       in
       Oracle_http.post
         ~headers
         ~body:(Cohttp_lwt.Body.of_string body)
         (Uri.of_string ("https://api.kraken.com" ^ path))
       >>= fun (response, response_body) ->
       Cohttp_lwt.Body.to_string response_body
       >|= fun body ->
       let status = Cohttp.Response.status response |> Cohttp.Code.code_of_status in
       if status <> 200
       then Error (Printf.sprintf "Kraken HTTP %d: %s" status body)
       else (
         try parse_kraken (Yojson.Safe.from_string body) with
         | exn ->
           Error
             (Printf.sprintf "Kraken response parse failed: %s" (Printexc.to_string exn))))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

let hyperliquid_base_url testnet =
  if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz"
;;

let fetch_hyperliquid ~testnet () : (balance list, string) result Lwt.t =
  match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
  | None | Some "" -> Lwt.return (Error "HYPERLIQUID_WALLET_ADDRESS is not set")
  | Some wallet ->
    let base_url = hyperliquid_base_url testnet in
    (* The suicide_grid strategy trades spot only, so the pool is the spot
       wallet's USDC (and any other spot tokens) exclusively. The perpetual
       clearinghouse balance is deliberately not included: it is margin
       reserved for perp positions, not capital available to the spot grid. *)
    post_json
      ~url:(base_url ^ "/info")
      (`Assoc [ "type", `String "spotClearinghouseState"; "user", `String wallet ])
    >|= (function
     | Error error -> Error ("Hyperliquid spot: " ^ error)
     | Ok spot_json -> parse_hyperliquid_spot spot_json)
;;

let fetch_alpaca ~testnet () : (balance list, string) result Lwt.t =
  load_dotenv ();
  Alpaca.Types.Config.set_testnet testnet;
  Lwt.catch
    (fun () ->
       Alpaca.Rest.get_account ()
       >>= function
       | Error error -> Lwt.return (Error error)
       | Ok account ->
         Alpaca.Rest.get_positions ()
         >|= (function
          | Error error -> Error error
          | Ok positions ->
            let account_balance =
              { asset = String.uppercase_ascii account.currency
              ; available = nonnegative account.cash
              ; total = nonnegative account.equity
              ; wallet_type = "cash"
              ; wallet_id = "account"
              }
            in
            let position_balances =
              List.map
                (fun (position : Alpaca.Types.position_record) ->
                   { asset = String.uppercase_ascii position.symbol
                   ; available = nonnegative position.qty
                   ; total = nonnegative position.qty
                   ; wallet_type = "position"
                   ; wallet_id = "account"
                   })
                positions
            in
            Ok (account_balance :: position_balances)))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

let cache : (string * bool, snapshot) Hashtbl.t = Hashtbl.create 8
let clear_cache () = Hashtbl.clear cache

(** One-shot REST account fetch, cached per (exchange, testnet) for the
    short-lived CLI oracle. The engine runtime prefers the live websocket-fed
    store ([fetch_account_live]) and only hits this path as fallback. *)
let fetch_account ~exchange ~testnet () : (snapshot, string) result Lwt.t =
  let exchange = String.lowercase_ascii exchange in
  match Hashtbl.find_opt cache (exchange, testnet) with
  | Some snapshot -> Lwt.return (Ok snapshot)
  | None ->
    let fetch =
      match exchange with
      | "kraken" -> fetch_kraken ~testnet ()
      | "hyperliquid" -> fetch_hyperliquid ~testnet ()
      | "alpaca" -> fetch_alpaca ~testnet ()
      | _ -> Lwt.return (Error ("unsupported balance venue: " ^ exchange))
    in
    fetch
    >|= (function
     | Error error -> Error error
     | Ok balances ->
       let snapshot =
         { exchange
         ; testnet
         ; balances = merge_balances balances
         ; fetched_at = Unix.gettimeofday ()
         }
       in
       Hashtbl.replace cache (exchange, testnet) snapshot;
       Ok snapshot)
;;

(** Build a balance snapshot from the live exchange registry stores - the
    websocket-fed caches owned by the engine supervisor - instead of a
    standalone REST call. Returns [None] when the exchange is not registered
    (standalone CLI runs, unknown venue) or its store is empty (no websocket
    snapshot received yet), so callers fall back to the REST fetch.

    Live stores only, and only where the store's semantics match the oracle's
    REST path:
    - Kraken: the authenticated balances WS feed tracks the account's wallets;
      tradeable = the WS store's spendable balance.
    - Alpaca: the account feed holds cash (available) / equity (total) plus
      per-symbol positions, mirroring the REST account+positions fetch.
    - Hyperliquid is deliberately excluded: the live "USDC" store aggregates
      the perp clearinghouse USDC with the spot wallet, while the oracle pool
      counts spot only (margin is not grid capital). REST
      spotClearinghouseState stays authoritative there. *)
let snapshot_of_live_store ~(exchange : string) ~(testnet : bool) () : snapshot option =
  let exchange = String.lowercase_ascii exchange in
  match exchange with
  | "kraken" | "alpaca" ->
    (match Exchange.Registry.get exchange with
     | None -> None
     | Some (module Ex) ->
       let balances = Ex.get_all_balances () in
       if balances = []
       then None
       else
         Some
           { exchange
           ; testnet
           ; balances =
               List.map
                 (fun (asset, total) ->
                    let available =
                      try Ex.get_tradeable_balance ~asset with
                      | _ -> 0.0
                    in
                    { asset
                    ; available
                    ; total
                    ; wallet_type = "live"
                    ; wallet_id = "engine"
                    })
                 balances
           ; fetched_at = Unix.gettimeofday ()
           })
  | _ -> None
;;

(** Fetch an account balance snapshot, preferring the live websocket-fed
    exchange store when it has data and falling back to the standalone REST
    fetch (CLI behavior). *)
let fetch_account_live ~exchange ~testnet () : (snapshot, string) result Lwt.t =
  match snapshot_of_live_store ~exchange ~testnet () with
  | Some snapshot -> Lwt.return (Ok snapshot)
  | None -> fetch_account ~exchange ~testnet ()
;;

let fetch_task (task : Oracle_tasks.task) =
  fetch_account ~exchange:task.exchange ~testnet:task.config.testnet ()
;;

(** Live-store-first task fetch for the engine runtime: the websocket-fed
    store when available, else the REST one-shot path. *)
let fetch_task_live (task : Oracle_tasks.task) =
  fetch_account_live ~exchange:task.exchange ~testnet:task.config.testnet ()
;;
