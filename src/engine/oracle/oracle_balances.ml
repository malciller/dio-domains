(* Oracle_balances - account balance snapshots for the survival oracle.

   Two sources, selected by the caller:
   1. Live exchange balance stores (websocket-fed by the engine supervisor) -
      [snapshot_of_live_store]. The live runtime prefers this: data is
      already in-process, so a pass pays no standalone HTTP round-trip.
      Best-effort: an unregistered exchange or empty store yields None and
      the caller falls back to REST.
   2. One-shot REST fetches ([fetch_account] / [fetch_task]), used by the CLI
      (bin/oracle.ml) and as runtime fallback. Per-venue fetch and asset
      normalization live in the venue's oracle adapter
      ([Exchange_intf.Oracle.S.fetch_balances] via
      [Exchange_intf.Oracle.Registry]).

   Hyperliquid is always REST: its live "USDC" store aggregates perp margin
   with spot, while the oracle pool counts spot capital only (perp margin is
   not grid capital), so REST spotClearinghouseState stays authoritative. *)

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
    sell or sizing can count as held inventory. Seeds the replay grid. *)
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

let cache : (string * bool, snapshot) Hashtbl.t = Hashtbl.create 8
let clear_cache () = Hashtbl.clear cache

(** One-shot REST account fetch through the venue registry, cached per
    (exchange, testnet). The runtime prefers the live store
    ([fetch_account_live]) and uses this only as fallback. Each adapter
    returns normalized (asset, available, total) triples. *)
let fetch_account ~exchange ~testnet () : (snapshot, string) result Lwt.t =
  let exchange = String.lowercase_ascii exchange in
  match Hashtbl.find_opt cache (exchange, testnet) with
  | Some snapshot -> Lwt.return (Ok snapshot)
  | None ->
    let fetch =
      match Exchange.Oracle.Registry.get exchange with
      | Some (module V) ->
        V.fetch_balances ~testnet
        >|= (function
         | Error error -> Error error
         | Ok triples ->
           Ok
             (List.map
                (fun (asset, available, total) ->
                   { asset
                   ; available
                   ; total
                   ; wallet_type = "rest"
                   ; wallet_id = "account"
                   })
                triples))
      | None -> Lwt.return (Error ("unsupported balance venue: " ^ exchange))
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

(** Build a snapshot from the live registry stores (websocket-fed caches
    owned by the engine supervisor) instead of a REST call. Returns [None]
    when the venue's adapter has no live-store semantics
    ([Oracle.S.live_balances]), the exchange is unregistered, or the store is
    empty; callers then fall back to REST. Whether a WS-fed store matches the
    oracle's REST balance view is the venue's own answer. *)
let snapshot_of_live_store ~(exchange : string) ~(testnet : bool) () : snapshot option =
  let exchange = String.lowercase_ascii exchange in
  match Exchange.Oracle.Registry.get exchange with
  | Some (module V) ->
    (match V.live_balances () with
     | Some triples ->
       Some
         { exchange
         ; testnet
         ; balances =
             List.map
               (fun (asset, available, total) ->
                  { asset; available; total; wallet_type = "live"; wallet_id = "engine" })
               triples
         ; fetched_at = Unix.gettimeofday ()
         }
     | None -> None)
  | None -> None
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
