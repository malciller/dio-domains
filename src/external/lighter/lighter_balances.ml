(** Lighter balance tracking module.
    Aggregates balance state from two WebSocket channels:
    - [account_all_assets/{ACCOUNT_ID}]: spot base asset balances (ETH, etc.).
    - [user_stats/{ACCOUNT_ID}]: USDC collateral (the quote/settlement currency).

    USDC is intentionally NOT read from account_all_assets because Lighter reports
    USDC balance=0 there whenever capital is committed to open buy orders — only
    user_stats.stats.collateral reflects the correct total.

    Follows the same background-processor-task pattern as hyperliquid_balances.ml. *)

open Lwt.Infix

let section = "lighter_balances"

(** Per-asset balance record. *)
type balance_data = {
  asset: string;
  balance: float;
  wallet_type: string;
  wallet_id: string;
  last_updated: float;
}

(** Thread-safe per-asset balance store supporting multiple wallet entries. *)
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
    Hashtbl.replace store.wallets wallet_key wallet_data;

    let total = Hashtbl.fold (fun _ wallet acc -> acc +. wallet.balance) store.wallets 0.0 in
    Atomic.set store.total_balance total;
    Atomic.set store.last_updated now;
    Mutex.unlock store.mutex

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
  let store = match Hashtbl.find_opt balance_stores asset with
    | Some store -> store
    | None ->
        let store = BalanceStore.create () in
        Hashtbl.add balance_stores asset store;
        store
  in
  Mutex.unlock balance_stores_mutex;
  store

let get_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_balance store

let get_all_balances () =
  Mutex.lock balance_stores_mutex;
  let balances = Hashtbl.fold (fun asset store acc ->
    (asset, BalanceStore.get_balance store) :: acc
  ) balance_stores [] in
  Mutex.unlock balance_stores_mutex;
  balances

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
  let assets = ref [] in
  Hashtbl.iter (fun asset _store -> assets := asset :: !assets) balance_stores;
  Mutex.unlock balance_stores_mutex;
  !assets

let notify_ready () =
  if not (Atomic.get is_ready) then begin
    Atomic.set is_ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let wait_for_balance_data_lwt assets timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_balance_data assets then
      Lwt.return_true
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then
        Lwt.return_false
      else
        Lwt.pick [
          (Lwt_condition.wait ready_condition >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_balance_data assets)
  in
  loop ()

let wait_for_balance_data = wait_for_balance_data_lwt

let subscribe_balance_updates () =
  let subscription = BalanceUpdateEventBus.subscribe balance_update_event_bus in
  (subscription.stream, subscription.close)

(* ---- Internal balance update helpers ---- *)

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

(** Handle account_all_assets / account_all messages: update base asset balances.
    USDC is intentionally skipped here — its correct total is only available
    via user_stats.collateral (see below). When a buy order is open, Lighter
    reports USDC balance=0 in account_all_assets with the locked amount in
    locked_balance, which would clobber the real value. *)
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
    Logging.debug_f ~section "Balance update has no assets field (type=%s)"
      (try member "type" json |> to_string with _ -> "unknown")
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
        (* Skip USDC — owned exclusively by process_user_stats below. *)
        if storage_key = "USDC" || storage_key = "usdc" then ()
        else begin
          let balance = match balance_json with
            | `Assoc _ ->
                let try_field key =
                  let v = member key balance_json in
                  if v <> `Null then Some (Lighter_types.parse_json_float v) else None
                in
                (match try_field "balance" with
                 | Some b -> b
                 | None ->
                     (match try_field "available" with
                      | Some b -> b
                      | None -> (match try_field "free" with
                        | Some b -> b
                        | None ->
                            Logging.debug_f ~section "Balance object for %s has unknown structure: %s"
                              storage_key (Yojson.Safe.to_string balance_json);
                            0.0)))
            | _ -> Lighter_types.parse_json_float balance_json
          in
          publish_balance_update storage_key balance;
          Logging.debug_f ~section "Balance update: %s = %.8f" storage_key balance
        end
      with exn ->
        Logging.warn_f ~section "Failed to parse balance for %s: %s" asset_id (Printexc.to_string exn)
    ) assets;
    notify_ready ()
  end

(** Handle user_stats messages: extract USDC total from stats.collateral.
    This is the authoritative source for USDC — collateral includes all
    deposited USDC regardless of how much is locked in open orders. *)
let process_user_stats json =
  let open Yojson.Safe.Util in
  let stats = member "stats" json in
  if stats = `Null then
    Logging.debug_f ~section "user_stats message has no stats field (type=%s)"
      (try member "type" json |> to_string with _ -> "unknown")
  else begin
    let collateral_v = member "collateral" stats in
    if collateral_v <> `Null then begin
      let collateral = (try Lighter_types.parse_json_float collateral_v with _ -> 0.0) in
      publish_balance_update "USDC" collateral;
      Logging.debug_f ~section "Balance update: USDC = %.8f (via user_stats collateral)" collateral;
      notify_ready ()
    end else
      Logging.debug_f ~section "user_stats stats.collateral is null"
  end

(** WebSocket message dispatcher. Routes by message type to the appropriate handler. *)
let process_market_data json =
  let open Yojson.Safe.Util in
  let msg_type =
    try member "type" json |> to_string with _ -> ""
  in
  match msg_type with
  | "update/account_all" | "snapshot/account_all" | "subscribed/account_all"
  | "update/account_all_assets" | "snapshot/account_all_assets" | "subscribed/account_all_assets" ->
      (try process_asset_balances json
       with exn ->
         Logging.error_f ~section "Failed to process asset balance update: %s" (Printexc.to_string exn))
  | "update/user_stats" | "subscribed/user_stats" ->
      (try process_user_stats json
       with exn ->
         Logging.error_f ~section "Failed to process user_stats update: %s" (Printexc.to_string exn))
  | _ -> ()

(** Background processor task. Subscribes to the Lighter WS broadcast stream
    and dispatches messages to balance handlers. Mirrors hyperliquid_balances.ml. *)
let _processor_task =
  let rec run () =
    let sub = Lighter_ws.subscribe_market_data () in
    Lwt.catch (fun () ->
      Logging.debug ~section "Starting Lighter balances processor task";
      let%lwt () = Concurrency.Lwt_util.consume_stream process_market_data sub.stream in
      sub.close ();
      Logging.debug ~section "Balances stream ended (disconnect), re-subscribing...";
      Lwt.async run;
      Lwt.return_unit
    ) (fun exn ->
      sub.close ();
      Logging.debug_f ~section "Lighter balances processor task crashed: %s. Re-subscribing..." (Printexc.to_string exn);
      Lwt.async run;
      Lwt.return_unit
    )
  in
  Lwt.async run

let initialize assets =
  Logging.info_f ~section "Initializing Lighter balances feed for %d assets" (List.length assets);
  List.iter (fun asset -> ignore (get_balance_store asset)) assets;
  Logging.info ~section "Lighter balance stores initialized"
