(**
   Kraken authenticated balances WebSocket feed.
   Maintains per-asset balance state via atomic variables for lock-free reads.
   State is mutated in response to WebSocket balance snapshot and update messages.
*)

open Lwt.Infix
open Concurrency

let section = "kraken_balances"

open Kraken_common_types

(** Re-exports conduit context constructor with TLS error handling. *)
let get_conduit_ctx = get_conduit_ctx

(** Per-asset balance record aggregated across wallet types. *)
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

  (* Upper bound on wallet entries per asset to prevent unbounded memory growth.
     Kraken broadcasts events across wallet types: spot, earn, transfer, margin. *)
  let max_wallets = 10

  type t = {
    (* Maps wallet_type/wallet_id keys to their balance records. *)
    wallets: (string, wallet_balance) Hashtbl.t;
    mutex: Mutex.t;
    total_balance: float Atomic.t;  (* Cached sum of all wallet balances for this asset. *)
    last_updated: float Atomic.t;
  }

  let create () = {
    wallets = Hashtbl.create 4;  (* Small initial capacity; most assets have few wallets. *)
    mutex = Mutex.create ();
    total_balance = Atomic.make 0.0;
    last_updated = Atomic.make 0.0;
  }

  (* Updates a single wallet entry and recomputes the aggregate total. *)
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

    (* Evict the oldest wallet entry when capacity is exceeded. *)
    if Hashtbl.length store.wallets > max_wallets then begin
      let oldest_key = ref "" in
      let oldest_time = ref Float.infinity in
      Hashtbl.iter (fun k (v : wallet_balance) ->
        if v.last_updated < !oldest_time then begin
          oldest_time := v.last_updated;
          oldest_key := k
        end
      ) store.wallets;
      if !oldest_key <> "" then Hashtbl.remove store.wallets !oldest_key
    end;

    (* Recompute aggregate balance across all wallets. *)
    let total = Hashtbl.fold (fun _ wallet acc -> acc +. wallet.balance) store.wallets 0.0 in
    Atomic.set store.total_balance total;
    Atomic.set store.last_updated now;
    Mutex.unlock store.mutex

  (* Returns the aggregate balance across all wallets for this asset. *)
  let get_balance store = Atomic.get store.total_balance

  let get_all store =
    {
      asset = "";  (* Populated by the caller with the asset name. *)
      balance = Atomic.get store.total_balance;
      wallet_type = "aggregated";  (* Sentinel indicating a multi-wallet aggregate. *)
      wallet_id = "all";
      last_updated = Atomic.get store.last_updated;
    }
end

(** Event bus for broadcasting balance mutation events to subscribers. *)
module BalanceUpdateEventBus = Event_bus.Make(struct
  type t = balance_data
end)

(** Singleton event bus instance for balance updates. *)
let balance_update_event_bus = BalanceUpdateEventBus.create "balance_update"

(** Global per-asset balance store registry, protected by mutex. *)
let balance_stores : (string, BalanceStore.t) Hashtbl.t = Hashtbl.create 32
let balance_stores_mutex = Mutex.create ()
let initialized = Atomic.make false
let ready_condition = Lwt_condition.create ()

(** Tracks the last update timestamp per asset for staleness detection. *)
let last_balance_update : (string, float) Hashtbl.t = Hashtbl.create 32
let balance_update_mutex = Mutex.create ()

(** Maximum number of dynamically discovered assets to retain in memory. *)
let dynamic_assets_cap = Kraken_common_types.default_dynamic_assets_cap

(** Set of statically configured assets; these are exempt from dynamic eviction. *)
let configured_assets : (string, unit) Hashtbl.t = Hashtbl.create 16
let configured_assets_mutex = Mutex.create ()

(** Guards singleton registration of cleanup event handlers. *)
let cleanup_handlers_started = Atomic.make false

(** Returns true if the given asset has not been updated within [threshold_seconds]. *)
let is_balance_stale asset threshold_seconds =
  try
    Mutex.lock balance_update_mutex;
    match Hashtbl.find_opt last_balance_update asset with
    | Some last_update ->
        let age = Unix.time () -. last_update in
        Mutex.unlock balance_update_mutex;
        age > threshold_seconds
    | None ->
        Mutex.unlock balance_update_mutex;
        true  (* No recorded update; treat as stale. *)
  with _ ->
    true

(** Records the current time as the latest update timestamp for [asset]. *)
let update_balance_timestamp asset =
  Mutex.lock balance_update_mutex;
  Hashtbl.replace last_balance_update asset (Unix.time ());
  Mutex.unlock balance_update_mutex

(** Returns the balance store for [asset], creating one lazily if absent. *)
let get_balance_store asset =
  Mutex.lock balance_stores_mutex;
  let store = match Hashtbl.find_opt balance_stores asset with
  | Some store -> store
  | None ->
      (* Lazy initialization fallback; stores should be pre-created via [initialize]. *)
      let store = BalanceStore.create () in
      Hashtbl.add balance_stores asset store;
      Logging.debug_f ~section "Created balance store on-the-fly for %s (should be pre-initialized)" asset;
      store
  in
  Mutex.unlock balance_stores_mutex;
  store

(** Returns the aggregate balance for [asset]. Inlined for hot-path performance. *)
let[@inline always] get_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_balance store

(** Returns a [balance_data] record with aggregated balances for [asset]. *)
let get_balance_data asset =
  let store = get_balance_store asset in
  let data = BalanceStore.get_all store in
  { data with asset }

(** Returns true if the balance store for [asset] has received at least one update. *)
let has_balance_data asset =
  try
    let store = get_balance_store asset in
    let last_updated = Atomic.get store.last_updated in
    last_updated > 0.0
  with _ -> false

(** Returns a list of all assets currently tracked in the balance store. *)
let get_all_assets () =
  Mutex.lock balance_stores_mutex;
  let assets = ref [] in
  Hashtbl.iter (fun asset _store -> assets := asset :: !assets) balance_stores;
  Mutex.unlock balance_stores_mutex;
  !assets

(** Broadcasts on [ready_condition] to unblock threads waiting for balance data. *)
let notify_ready () =
  (try
    Lwt_condition.broadcast ready_condition ()
  with Invalid_argument _ ->
    (* Suppress exceptions from cancelled Lwt threads. *)
    ())

(** Blocks until all [assets] have balance data or [timeout_seconds] elapses.
    Returns true if all assets were populated, false on timeout. *)
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

(** Evicts dynamically discovered assets exceeding [dynamic_assets_cap].
    Statically configured assets are never removed. *)
let cleanup_dynamic_assets () =
  Mutex.lock balance_stores_mutex;
  Mutex.lock configured_assets_mutex;

  (* Partition assets into configured (static) and dynamic sets. *)
  let all_assets = ref [] in
  Hashtbl.iter (fun asset _ -> all_assets := asset :: !all_assets) balance_stores;

  let configured = ref [] in
  let dynamic = ref [] in

  List.iter (fun asset ->
    if Hashtbl.mem configured_assets asset then
      configured := asset :: !configured
    else
      dynamic := asset :: !dynamic
  ) !all_assets;

  (* Sort dynamic assets by last update time, most recent first. *)
  let dynamic_with_times = List.map (fun asset ->
    let last_update = try
      Mutex.lock balance_update_mutex;
      let time = Hashtbl.find last_balance_update asset in
      Mutex.unlock balance_update_mutex;
      time
    with Not_found ->
      Mutex.unlock balance_update_mutex;
      0.0  (* Never updated; lowest eviction priority. *)
    in
    last_update
  ) !dynamic in

  let dynamic_sorted = List.sort (fun (_, t1) (_, t2) -> Float.compare t2 t1)
    (List.combine !dynamic dynamic_with_times) in

  (* Retain only the most recently updated dynamic assets up to the cap. *)
  let dynamic_to_keep = List.map fst (List.filteri (fun i _ -> i < dynamic_assets_cap) dynamic_sorted) in
  let dynamic_to_remove = List.filter (fun asset -> not (List.mem asset dynamic_to_keep)) !dynamic in

  (* Remove evicted assets from both balance stores and timestamp tracking. *)
  let removed_count = List.length dynamic_to_remove in
  List.iter (fun asset ->
    Hashtbl.remove balance_stores asset;
    Mutex.lock balance_update_mutex;
    Hashtbl.remove last_balance_update asset;
    Mutex.unlock balance_update_mutex;
    Logging.debug_f ~section "Removed dynamic balance asset: %s" asset
  ) dynamic_to_remove;

  Mutex.unlock configured_assets_mutex;
  Mutex.unlock balance_stores_mutex;

  if removed_count > 0 then
    Logging.info_f ~section "Cleaned up %d dynamic balance assets, keeping %d configured + %d recent dynamic"
      removed_count (List.length !configured) (List.length dynamic_to_keep)

(** Schedules asynchronous cleanup of dynamic assets without blocking the caller. *)
let trigger_dynamic_asset_cleanup ~reason () =
  Lwt.async (fun () ->
    Logging.debug_f ~section "Triggering dynamic asset cleanup (reason=%s)" reason;
    cleanup_dynamic_assets ();
    Lwt.return_unit
  )

let maybe_cleanup_after_balance_update () =
  (* Check store size outside the critical section to avoid holding the lock. *)
  let asset_count =
    Mutex.lock balance_stores_mutex;
    let count = Hashtbl.length balance_stores in
    Mutex.unlock balance_stores_mutex;
    count
  in
  if asset_count > dynamic_assets_cap then
    trigger_dynamic_asset_cleanup ~reason:"dynamic_asset_cap_exceeded" ()


(** Parses a balance snapshot message, populating wallet stores for each asset. *)
let parse_snapshot json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    List.iter (fun asset_data ->
      try
        let asset = member "asset" asset_data |> to_string in
        let store = get_balance_store asset in

        (* Iterate over embedded wallet entries and update each. *)
        let wallets = member "wallets" asset_data |> to_list in
        List.iter (fun wallet ->
          try
            let wallet_type = member "type" wallet |> to_string in
            let wallet_id = member "id" wallet |> to_string in
            let balance = member "balance" wallet |> to_float in

            BalanceStore.update_wallet store balance wallet_type wallet_id;

            Logging.debug_f ~section "Balance snapshot wallet: %s %s/%s = %.8f"
              asset wallet_type wallet_id balance;
          with exn ->
            Logging.warn_f ~section "Failed to parse wallet in snapshot: %s"
              (Printexc.to_string exn)
        ) wallets;

        update_balance_timestamp asset;
        notify_ready ();

        let total_balance = BalanceStore.get_balance store in
        Logging.debug_f ~section "Balance snapshot total: %s = %.8f"
          asset total_balance;
        (* Signal heartbeat to confirm connection liveness. *)
        on_heartbeat ()
      with exn ->
        Logging.warn_f ~section "Failed to parse balance snapshot item: %s"
          (Printexc.to_string exn)
    ) data;
    
    (* Mark any pre-initialized assets with zero balance as updated if not yet touched. *)
    let now = Unix.time () in
    Mutex.lock balance_stores_mutex;
    let all_assets = Hashtbl.fold (fun asset _ acc -> asset :: acc) balance_stores [] in
    Mutex.unlock balance_stores_mutex;

    List.iter (fun asset ->
      let store = get_balance_store asset in
      if Atomic.get store.last_updated <= 0.0 then begin
        Atomic.set store.last_updated now;
        update_balance_timestamp asset;
        Logging.debug_f ~section "Balance snapshot: Marked zero-balance asset %s as updated" asset
      end
    ) all_assets;
    notify_ready ();

    (* Trigger cleanup if dynamic asset count exceeds the cap. *)
    maybe_cleanup_after_balance_update ();

    Some ()
  with exn ->
    Logging.warn_f ~section "Failed to parse balance snapshot: %s" 
      (Printexc.to_string exn);
    None

(** Parses incremental balance update (ledger transaction) messages. *)
let parse_update json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    List.iter (fun ledger_tx ->
      try
        let asset = member "asset" ledger_tx |> to_string in
        let balance = member "balance" ledger_tx |> to_float in
        let amount = member "amount" ledger_tx |> to_float in
        let tx_type = member "type" ledger_tx |> to_string in
        let wallet_type = member "wallet_type" ledger_tx |> to_string in
        let wallet_id = member "wallet_id" ledger_tx |> to_string in
        
        let store = get_balance_store asset in
        BalanceStore.update_wallet store balance wallet_type wallet_id;
        update_balance_timestamp asset;
        notify_ready ();

        (* Publish aggregated balance data to event bus subscribers. *)
        let balance_data = BalanceStore.get_all store in
        let event_data = {
          asset;
          balance = balance_data.balance;
          wallet_type = balance_data.wallet_type;
          wallet_id = balance_data.wallet_id;
          last_updated = balance_data.last_updated;
        } in
        BalanceUpdateEventBus.publish balance_update_event_bus event_data;

        Logging.debug_f ~section "Balance update: %s %+.8f (new: %.8f) [%s]"
          asset amount balance tx_type;
        (* Signal heartbeat to confirm connection liveness. *)
        on_heartbeat ()
      with exn ->
        Logging.warn_f ~section "Failed to parse balance update item: %s" 
          (Printexc.to_string exn)
    ) data;
    
    (* Trigger cleanup if dynamic asset count exceeds the cap. *)
    maybe_cleanup_after_balance_update ();

    Some ()
  with exn ->
    Logging.warn_f ~section "Failed to parse balance update: %s" 
      (Printexc.to_string exn);
    None

(** Routes a parsed JSON WebSocket message to the appropriate handler by channel and type. *)
let handle_message_json json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let channel = member "channel" json |> to_string_option in
    let msg_type = member "type" json |> to_string_option in
    let method_type = member "method" json |> to_string_option in
    
    match channel, msg_type, method_type with
    | Some "balances", Some "snapshot", _ ->
        let _ = parse_snapshot json on_heartbeat in ()
    | Some "balances", Some "update", _ ->
        let _ = parse_update json on_heartbeat in ()
    | Some "heartbeat", _, _ ->
        on_heartbeat ()
    | _, _, Some "subscribe" ->
        let open Yojson.Safe.Util in
        let success = member "success" json |> to_bool_option in
        (match success with
        | Some true -> Logging.info ~section "Subscribed to balances feed"
        | Some false -> 
            let error = member "error" json |> to_string_option in
            Logging.error_f ~section "Subscription failed: %s" 
              (Option.value error ~default:"Unknown error")
        | None -> ())
    | Some "status", _, _ ->
        Logging.info ~section "Connected to Kraken authenticated WebSocket"
    | _ -> ()
  with exn ->
    Logging.error_f ~section "Error handling message: %s" 
      (Printexc.to_string exn)

(** Parses a raw text WebSocket message as JSON and dispatches it. *)
let handle_message message on_heartbeat =
  Concurrency.Tick_event_bus.publish_tick ();
  try
    let json = Yojson.Safe.from_string message in
    handle_message_json json on_heartbeat
  with exn ->
    Logging.error_f ~section "Error handling message: %s - %s" 
      (Printexc.to_string exn) message


(** Deprecated message handler stub. Connection management is handled by [Kraken_trading_client]. *)
let start_message_handler _conn _token _on_failure _on_heartbeat =
  Lwt.return_unit

(** Subscribes to the balances channel on the unified authenticated WebSocket connection.
    Registers a ring buffer consumer to process incoming balance messages. *)
let connect_and_subscribe token ~on_failure:_ ~on_heartbeat ~on_connected =
  Logging.info ~section "Registering balances subscription on unified authenticated connection";
  
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "balances");
      ("token", `String token);
      ("snapshot", `Bool true)
    ])
  ] in
  
  Kraken_trading_client.subscribe subscribe_msg >>= fun () ->
  on_connected ();
  
  if not (Atomic.exchange cleanup_handlers_started true) then begin
    let buffer = Kraken_trading_client.get_message_buffer () in
    let condition = Kraken_trading_client.get_message_condition () in
    let read_pos = ref (Ring_buffer.RingBuffer.get_position buffer) in
    
    let rec loop () =
      Lwt_condition.wait condition >>= fun () ->
      let new_messages = Ring_buffer.RingBuffer.read_since buffer !read_pos in
      read_pos := Ring_buffer.RingBuffer.get_position buffer;
      
      List.iter (fun json ->
        handle_message_json json on_heartbeat
      ) new_messages;
      
      if Atomic.get Kraken_trading_client.shutdown_requested then
        Lwt.return_unit
      else begin
        Lwt.async loop;
        Lwt.return_unit
      end
    in
    Lwt.async loop
  end;
  Lwt.return_unit

(** Pre-creates balance stores for the given assets and default fiat currencies.
    Must be called before the WebSocket feed begins producing messages. *)
let initialize assets =
  Logging.info_f ~section "Initializing balances feed for %d assets" (List.length assets);

  (* Merge user-configured assets with default fiat currencies. *)
  let all_assets = List.sort_uniq String.compare (assets @ Kraken_common_types.default_configured_currencies) in

  (* Record which assets are statically configured (exempt from dynamic eviction). *)
  Mutex.lock configured_assets_mutex;
  List.iter (fun asset -> Hashtbl.replace configured_assets asset ()) assets;
  Mutex.unlock configured_assets_mutex;

  (* Pre-allocate balance stores under a single lock acquisition. *)
  Mutex.lock balance_stores_mutex;
  List.iter (fun asset ->
    if not (Hashtbl.mem balance_stores asset) then begin
      let store = BalanceStore.create () in
      Hashtbl.add balance_stores asset store;
      Logging.debug_f ~section "Created thread-safe balance store for %s" asset
    end
  ) all_assets;
  Mutex.unlock balance_stores_mutex;

  Atomic.set initialized true;
  Logging.info ~section "Balance stores initialized - now thread-safe";
  ()

(** Logs warnings for any assets whose balance data exceeds the staleness threshold. *)
let check_stale_balances assets =
  let stale_count = ref 0 in
  List.iter (fun asset ->
    if is_balance_stale asset Kraken_common_types.default_balance_staleness_threshold_s then begin
      Logging.warn_f ~section "Balance data for %s is stale (>5 minutes old)" asset;
      incr stale_count;
    end else
      Logging.debug_f ~section "Balance data for %s is fresh" asset
  ) assets;

  if !stale_count > 0 then
    Logging.warn_f ~section "%d assets have stale balance data" !stale_count

(** Deprecated reconnection stub retained for interface compatibility.
    Reconnection is now managed by [Kraken_trading_client] via heartbeat monitoring. *)
let restart_connection () =
  Lwt.return_unit

(** Returns a stream and close function for subscribing to balance update events. *)
let subscribe_balance_updates () =
  let subscription = BalanceUpdateEventBus.subscribe balance_update_event_bus in
  (subscription.stream, subscription.close)
