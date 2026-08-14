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
      runtime when the live store is unavailable. Read-only snapshots whose
      per-venue fetch + asset normalization now live in the venue's own
      oracle adapter ([Exchange_intf.Oracle.S.fetch_balances], dispatched
      through [Exchange_intf.Oracle.Registry]) - a new venue is plug-and-play
      here too.

   Hyperliquid is always REST: the live balance store used to aggregate the
   perp clearinghouse USDC into the same "USDC" entry as spot, while the
   oracle's pool must count spot capital only (perp margin is not grid
   capital), so the REST spotClearinghouseState path stays authoritative
   there. (The engine's live store itself now tracks spot available = total -
   hold and excludes perp/staking wallets from its tradeable figure, so the
   two sources agree; REST remains the oracle's choice for its own pool.) *)

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

let cache : (string * bool, snapshot) Hashtbl.t = Hashtbl.create 8
let clear_cache () = Hashtbl.clear cache

(** One-shot REST account fetch through the venue registry, cached per
    (exchange, testnet) for the short-lived CLI oracle. The engine runtime
    prefers the live websocket-fed store ([fetch_account_live]) and only hits
    this path as fallback. Each venue's adapter returns already-normalized
    (asset, available, total) triples. *)
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

(** Build a balance snapshot from the live exchange registry stores - the
    websocket-fed caches owned by the engine supervisor - instead of a
    standalone REST call. Returns [None] when the venue's oracle adapter has
    no live store semantics ([Oracle.S.live_balances], e.g. Hyperliquid: its
    live "USDC" store aggregates perp margin with spot, so the oracle keeps
    REST authoritative there), when the exchange is not registered (standalone
    CLI runs, unknown venue), or when its store is empty (no websocket
    snapshot received yet); callers then fall back to the REST fetch.

    Which venues have a WS-fed live store - and whether its semantics match
    the oracle's REST balance view - is the venue's own answer, not this
    module's. *)
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
