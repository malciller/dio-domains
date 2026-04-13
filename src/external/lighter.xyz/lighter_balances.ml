(** Provides the centralized subsystem for tracking real-time asset balances originating from the Lighter exchange.
    Base asset balances (ETH, etc.) are sourced from the [account_all_assets/{ACCOUNT_ID}] WS channel.
    USDC balance handling depends on the account mode:
    - Split accounts: USDC appears in the assets array of [account_all_assets] directly.
    - Unified accounts: USDC is account-level collateral, sourced from [user_stats] WS channel
      and seeded at startup via the REST /api/v1/account endpoint.

    Balance messages are dispatched synchronously from the WS frame handler in [lighter_ws.ml],
    ensuring no messages are lost to bounded-stream backpressure. *)



let section = "lighter_balances"

(** Implements an atomic representation of the balance metrics mapped to a specific digital asset. 
    Maintains localized timestamps to ensure data freshness constraints can be evaluated by external system invariants. *)
type balance_data = {
  asset: string;
  balance: float;
  wallet_type: string;
  wallet_id: string;
  last_updated: float;
}

(** Constructs an isolated, thread-safe memory partition for caching asset balances spanning disparate sub-accounts or wallet architectures.
    Leverages OCaml mutex primitives alongside atomic variables for non-blocking read access to the aggregated balance figures. *)
module BalanceStore = struct
  type wallet_balance = {
    balance: float;
    wallet_type: string;
    wallet_id: string;
    last_updated: float;
  }

  type t = {
    wallets: (string, wallet_balance) Hashtbl.t;
    mutex: Mutex.t;
    total_balance: float Atomic.t;
    last_updated: float Atomic.t;
  }

  let create () = {
    wallets = Hashtbl.create 4;
    mutex = Mutex.create ();
    total_balance = Atomic.make 0.0;
    last_updated = Atomic.make 0.0;
  }

  let update_wallet store balance wallet_type wallet_id =
    let wallet_key = wallet_type ^ "/" ^ wallet_id in
    let now = Unix.time () in
    let wallet_data = {
      balance;
      wallet_type;
      wallet_id;
      last_updated = now;
    } in

    Mutex.lock store.mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock store.mutex) (fun () ->
      Hashtbl.replace store.wallets wallet_key wallet_data;

      let total = Hashtbl.fold (fun _ wallet acc -> acc +. wallet.balance) store.wallets 0.0 in
      Atomic.set store.total_balance total;
      Atomic.set store.last_updated now
    )

  let get_balance store = Atomic.get store.total_balance

  let get_all store =
    {
      asset = "";
      balance = Atomic.get store.total_balance;
      wallet_type = "aggregated";
      wallet_id = "all";
      last_updated = Atomic.get store.last_updated;
    }
end

module BalanceUpdateEventBus = Concurrency.Event_bus.Make(struct
  type t = balance_data
end)

let balance_update_event_bus = BalanceUpdateEventBus.create "balance_update"

let balance_stores : (string, BalanceStore.t) Hashtbl.t = Hashtbl.create 16
let balance_stores_mutex = Mutex.create ()
let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let get_balance_store asset =
  Mutex.lock balance_stores_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock balance_stores_mutex) (fun () ->
    match Hashtbl.find_opt balance_stores asset with
    | Some store -> store
    | None ->
        let store = BalanceStore.create () in
        Hashtbl.add balance_stores asset store;
        store
  )

let get_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_balance store

let get_all_balances () =
  Mutex.lock balance_stores_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock balance_stores_mutex) (fun () ->
    Hashtbl.fold (fun asset store acc ->
      (asset, BalanceStore.get_balance store) :: acc
    ) balance_stores []
  )

let get_balance_data asset =
  let store = get_balance_store asset in
  let data = BalanceStore.get_all store in
  { data with asset }

let has_balance_data asset =
  try
    let store = get_balance_store asset in
    let last_updated = Atomic.get store.last_updated in
    last_updated > 0.0
  with _ -> false

let get_all_assets () =
  Mutex.lock balance_stores_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock balance_stores_mutex) (fun () ->
    let assets = ref [] in
    Hashtbl.iter (fun asset _store -> assets := asset :: !assets) balance_stores;
    !assets
  )

let notify_ready () =
  if not (Atomic.get is_ready) then begin
    Atomic.set is_ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let wait_for_balance_data_lwt assets timeout_seconds =
  Concurrency.Lwt_util.poll_until
    ~timeout:timeout_seconds
    ~wait_signal:(fun () -> Lwt_condition.wait ready_condition)
    ~check:(fun () -> List.for_all has_balance_data assets)

let wait_until_ready () =
  if Atomic.get is_ready then Lwt.return_true
  else
    let open Lwt.Infix in
    Lwt_condition.wait ready_condition >>= fun () ->
    Lwt.return_true

let wait_for_balance_data = wait_for_balance_data_lwt

let subscribe_balance_updates () =
  let subscription = BalanceUpdateEventBus.subscribe balance_update_event_bus in
  (subscription.stream, subscription.close)

(* Execution handlers for propagating balance mutations across the internal concurrent stores and outward to the subscription event bus. *)

let publish_balance_update storage_key balance =
  let store = get_balance_store storage_key in
  BalanceStore.update_wallet store balance "spot" "account";
  let event_data = BalanceStore.get_all store in
  BalanceUpdateEventBus.publish balance_update_event_bus {
    asset = storage_key;
    balance = event_data.balance;
    wallet_type = event_data.wallet_type;
    wallet_id = event_data.wallet_id;
    last_updated = event_data.last_updated;
  }

(** Evaluates and maps incoming JSON payloads routed from the [account_all] or [account_all_assets] channels.
    This function traverses dynamic JSON schema structures to extract all asset allocations,
    including USDC which reports balance directly in coin terms via the account_all_assets channel. *)
let process_asset_balances json =
  let open Yojson.Safe.Util in
  let account_data =
    let v = member "account_all" json in
    if v <> `Null then v
    else let v2 = member "data" json in
      if v2 <> `Null then v2
      else json
  in
  let assets = (try member "assets" account_data |> to_assoc with _ -> []) in
  if assets = [] then
    Logging.debug_f ~section "Balance update has no assets field (type=%s, content=%s)"
      (try member "type" json |> to_string with _ -> "unknown")
      (Yojson.Safe.to_string account_data)
  else begin
    List.iter (fun (asset_id, balance_json) ->
      try
        let storage_key = match balance_json with
          | `Assoc _ ->
              let sym = member "symbol" balance_json in
              if sym <> `Null then (try to_string sym with _ -> asset_id)
              else asset_id
          | _ -> asset_id
        in
        let normalized_key = if String.uppercase_ascii storage_key = "USDC" then "USDC" else storage_key in
        let balance = match balance_json with
          | `Assoc _ ->
              let try_field key =
                let v = member key balance_json in
                if v <> `Null then Some (Lighter_types.parse_json_float v) else None
              in
              (match try_field "balance" with
               | Some b -> b
               | None ->
                   (match try_field "collateral" with
                    | Some b -> b
                    | None ->
                        (match try_field "margin_balance" with
                         | Some b -> b
                         | None ->
                             (match try_field "available" with
                              | Some b -> b
                              | None -> (match try_field "free" with
                                | Some b -> b
                                | None ->
                                    Logging.warn_f ~section "Balance object for %s has unknown structure: %s"
                                      normalized_key (Yojson.Safe.to_string balance_json);
                                    0.0)))))
          | _ -> Lighter_types.parse_json_float balance_json
        in
        publish_balance_update normalized_key balance;
        Logging.debug_f ~section "Balance update: %s = %.8f" normalized_key balance
      with exn ->
        Logging.warn_f ~section "Failed to parse balance for %s: %s" asset_id (Printexc.to_string exn)
    ) assets;
    notify_ready ()
  end

(** Processes user_stats to maintain USDC balance for unified accounts.
    In unified/cross-margin mode, USDC collateral lives at the account level
    (reported via user_stats), not in the assets array (account_all_assets).
    This handler writes collateral to the USDC balance store to keep it current. *)
let process_user_stats json =
  let open Yojson.Safe.Util in
  let stats =
    let s = member "stats" json in
    if s <> `Null then s
    else
      let us = member "user_stats" json in
      if us <> `Null then member "stats" us
      else
        let d = member "data" json in
        if d <> `Null then member "stats" d
        else `Null
  in
  if stats <> `Null then begin
    let extract_float_opt key =
      let v = member key stats in
      if v <> `Null then (try Some (Lighter_types.parse_json_float v) with _ -> None) else None
    in
    let val_of o = Option.value o ~default:0.0 in
    let c = val_of (extract_float_opt "collateral") in
    let m = val_of (extract_float_opt "margin_balance") in
    let w = val_of (extract_float_opt "wallet_balance") in
    let a = val_of (extract_float_opt "account_value") in
    let usdc_balance = max (max c m) (max w a) in
    if usdc_balance > 0.0 then begin
      publish_balance_update "USDC" usdc_balance;
      Logging.debug_f ~section "USDC updated from user_stats: %.6f (col:%.2f mb:%.2f wb:%.2f av:%.2f)"
        usdc_balance c m w a;
      notify_ready ()
    end else begin
      (* Don't overwrite a known positive balance with zero *)
      let existing = get_balance "USDC" in
      if existing <= 0.0 then begin
        publish_balance_update "USDC" 0.0;
        notify_ready ()
      end;
      Logging.debug_f ~section "user_stats zero balance (existing=%.2f, col:%.2f mb:%.2f wb:%.2f av:%.2f)"
        existing c m w a
    end
  end

(** Multiplexes demarshaled JSON frames from the WebSocket fabric into specialized processing functions based on the deterministic message type property. *)
let process_market_data json =
  let open Yojson.Safe.Util in
  let msg_type =
    let raw_type = try member "type" json |> to_string with _ -> "" in
    let channel = try member "channel" json |> to_string with _ -> "" in
    if channel <> "" then
      let ch_prefix = try String.sub channel 0 (min (try String.index channel '/' with Not_found -> String.length channel) (try String.index channel ':' with Not_found -> String.length channel)) with _ -> channel in
      if raw_type = "update" || raw_type = "snapshot" || raw_type = "subscribed" then
        raw_type ^ "/" ^ ch_prefix
      else raw_type
    else raw_type
  in
  match msg_type with
  | "update/account_all" | "snapshot/account_all" | "subscribed/account_all"
  | "update/account_all_assets" | "snapshot/account_all_assets" | "subscribed/account_all_assets" ->
      (try process_asset_balances json
       with exn ->
         Logging.error_f ~section "Failed to process asset balance update: %s" (Printexc.to_string exn))
  | "update/user_stats" | "snapshot/user_stats" | "subscribed/user_stats" ->
      (try process_user_stats json
       with exn ->
         Logging.error_f ~section "Failed to process user_stats update: %s" (Printexc.to_string exn))
  | _ -> ()




let initialize assets =
  Logging.info_f ~section "Initializing Lighter balances feed for %d assets" (List.length assets);
  List.iter (fun asset -> ignore (get_balance_store asset)) assets;
  Logging.info ~section "Lighter balance stores initialized"
