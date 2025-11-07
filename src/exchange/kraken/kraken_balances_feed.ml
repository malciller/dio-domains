(** Kraken Balances Feed - WebSocket v2 authenticated balances subscription with lock-free atomics *)

open Lwt.Infix
open Concurrency

let section = "kraken_balances"

(** Safely force Conduit context with error handling *)
let get_conduit_ctx () =
  try
    Lazy.force Conduit_lwt_unix.default_ctx
  with
  | CamlinternalLazy.Undefined ->
      Logging.error ~section "Conduit context was accessed before initialization - this should not happen";
      raise (Failure "Conduit context not initialized - ensure main.ml initializes it before domain spawning")
  | exn ->
      Logging.error_f ~section "Failed to get Conduit context: %s" (Printexc.to_string exn);
      raise exn

(** Balance data per asset - stored atomically *)
type balance_data = {
  asset: string;
  balance: float;
  wallet_type: string;
  wallet_id: string;
  last_updated: float;
}

(** Balance storage that aggregates across multiple wallets *)
module BalanceStore = struct
  type wallet_balance = {
    balance: float;
    wallet_type: string;
    wallet_id: string;
    last_updated: float;
  }

  type t = {
    (* Store balances for all wallets of this asset *)
    wallets: (string, wallet_balance) Hashtbl.t;
    mutex: Mutex.t;
    total_balance: float Atomic.t;  (* Cached total across all wallets *)
    last_updated: float Atomic.t;
  }

  let create () = {
    wallets = Hashtbl.create 4;  (* Most assets won't have many wallets *)
    mutex = Mutex.create ();
    total_balance = Atomic.make 0.0;
    last_updated = Atomic.make 0.0;
  }

  (* Update balance for a specific wallet and recalculate total *)
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

    (* Recalculate total balance across all wallets *)
    let total = Hashtbl.fold (fun _ wallet acc -> acc +. wallet.balance) store.wallets 0.0 in
    Atomic.set store.total_balance total;
    Atomic.set store.last_updated now;
    Mutex.unlock store.mutex

  (* Get total balance across all wallets *)
  let get_balance store = Atomic.get store.total_balance

  let get_all store =
    {
      asset = "";  (* Set by caller *)
      balance = Atomic.get store.total_balance;
      wallet_type = "aggregated";  (* Indicate this is aggregated across wallets *)
      wallet_id = "all";
      last_updated = Atomic.get store.last_updated;
    }
end

(** Event bus for balance updates - publishes individual balance changes *)
module BalanceUpdateEventBus = Event_bus.Make(struct
  type t = balance_data
end)

(** Global balance update event bus instance *)
let balance_update_event_bus = BalanceUpdateEventBus.create "balance_update"

(** Global balance stores per asset - protected by mutex *)
let balance_stores : (string, BalanceStore.t) Hashtbl.t = Hashtbl.create 32
let balance_stores_mutex = Mutex.create ()
let initialized = Atomic.make false
let ready_condition = Lwt_condition.create ()

(** Track last balance update time per asset for staleness detection *)
let last_balance_update : (string, float) Hashtbl.t = Hashtbl.create 32
let balance_update_mutex = Mutex.create ()

(** Track configured assets (from trading config) vs dynamic assets *)
let configured_assets : (string, unit) Hashtbl.t = Hashtbl.create 16
let dynamic_assets_cap = 50  (* Maximum number of non-configured assets to track *)
let configured_assets_mutex = Mutex.create ()

(** Check if balance data is stale (older than threshold) *)
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
        true  (* No update ever received - definitely stale *)
  with _ ->
    true

(** Update last balance update timestamp *)
let update_balance_timestamp asset =
  Mutex.lock balance_update_mutex;
  Hashtbl.replace last_balance_update asset (Unix.time ());
  Mutex.unlock balance_update_mutex

(** Get or create balance store for an asset - thread-safe *)
let get_balance_store asset =
  Mutex.lock balance_stores_mutex;
  let store = match Hashtbl.find_opt balance_stores asset with
  | Some store -> store
  | None ->
      (* This should not happen if initialized correctly, but as a fallback: *)
      let store = BalanceStore.create () in
      Hashtbl.add balance_stores asset store;
      Logging.debug_f ~section "Created balance store on-the-fly for %s (should be pre-initialized)" asset;
      store
  in
  Mutex.unlock balance_stores_mutex;
  store

(** Get current balance for an asset - hot path, inlined *)
let[@inline always] get_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_balance store

(** Get full balance data for an asset *)
let get_balance_data asset =
  let store = get_balance_store asset in
  let data = BalanceStore.get_all store in
  { data with asset }

(** Check if we have balance data for an asset *)
let has_balance_data asset =
  try
    let store = get_balance_store asset in
    let last_updated = Atomic.get store.last_updated in
    last_updated > 0.0
  with _ -> false

(** Get all assets that have balance stores (initialized assets) *)
let get_all_assets () =
  Mutex.lock balance_stores_mutex;
  let assets = ref [] in
  Hashtbl.iter (fun asset _store -> assets := asset :: !assets) balance_stores;
  Mutex.unlock balance_stores_mutex;
  !assets

(** Notify listeners that balance data is available *)
let notify_ready () =
  (try
    Lwt_condition.broadcast ready_condition ()
  with Invalid_argument _ ->
    (* Ignore - some waiters may have timed out or been cancelled *)
    ())

(** Wait until balance data is available for assets *)
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

(** Parse balance snapshot from WebSocket *)
let parse_snapshot json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    List.iter (fun asset_data ->
      try
        let asset = member "asset" asset_data |> to_string in
        let store = get_balance_store asset in

        (* Parse individual wallet balances from the wallets array *)
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
        Telemetry.inc_counter (Telemetry.counter "balance_updates" ()) ();
        (* Update connection heartbeat *)
        on_heartbeat ()
      with exn ->
        Logging.warn_f ~section "Failed to parse balance snapshot item: %s"
          (Printexc.to_string exn)
    ) data;
    
    Some ()
  with exn ->
    Logging.warn_f ~section "Failed to parse balance snapshot: %s" 
      (Printexc.to_string exn);
    None

(** Parse balance update from WebSocket *)
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

        (* Publish balance update event to event bus *)
        let balance_data = BalanceStore.get_all store in
        let event_data = {
          asset;
          balance = balance_data.balance;
          wallet_type = balance_data.wallet_type;
          wallet_id = balance_data.wallet_id;
          last_updated = balance_data.last_updated;
        } in
        BalanceUpdateEventBus.publish balance_update_event_bus event_data;

        Logging.info_f ~section "Balance update: %s %+.8f (new: %.8f) [%s]"
          asset amount balance tx_type;
        Telemetry.inc_counter (Telemetry.counter "balance_updates" ()) ();
        (* Update connection heartbeat *)
        on_heartbeat ()
      with exn ->
        Logging.warn_f ~section "Failed to parse balance update item: %s" 
          (Printexc.to_string exn)
    ) data;
    
    Some ()
  with exn ->
    Logging.warn_f ~section "Failed to parse balance update: %s" 
      (Printexc.to_string exn);
    None

(** WebSocket message handler *)
let handle_message message on_heartbeat =
  try
    let json = Yojson.Safe.from_string message in
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
        on_heartbeat () (* Update connection heartbeat *)
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
    | _ ->
        Logging.debug_f ~section "Unhandled: %s" message
  with exn ->
    Logging.error_f ~section "Error handling message: %s - %s" 
      (Printexc.to_string exn) message


(** Message handling loop - runs in background *)
let start_message_handler conn token on_failure on_heartbeat =
  (* Subscribe to balances *)
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "balances");
      ("token", `String token);
      ("snapshot", `Bool true)
    ])
  ] in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ()) >>= fun () ->

  (* Message loop *)
  let rec msg_loop () =
    Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= function
      | {Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _} ->
          Logging.warn ~section "WebSocket connection closed by server";
          on_failure "Connection closed by server";
          Lwt.return_unit
      | frame ->
          let content = frame.Websocket.Frame.content in
          handle_message content on_heartbeat;
          msg_loop ()
    ) (function
      | End_of_file ->
          Logging.warn ~section "Balances WebSocket connection closed unexpectedly (End_of_file)";
          (* Notify supervisor of connection failure *)
          on_failure "Connection closed unexpectedly (End_of_file)";
          Lwt.return_unit
      | exn ->
          Logging.error_f ~section "Balances WebSocket error during read: %s" (Printexc.to_string exn);
          (* Notify supervisor of connection failure *)
          on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
          Lwt.return_unit
    )
  in
  msg_loop ()

(** WebSocket connection to Kraken authenticated endpoint - establishes connection and starts message handler *)
let connect_and_subscribe token ~on_failure ~on_heartbeat ~on_connected =
  let uri = Uri.of_string "wss://ws-auth.kraken.com/v2" in

  Logging.info_f ~section "Connecting to Kraken authenticated WebSocket...";
  (* Resolve hostname to IP *)
  Lwt_unix.getaddrinfo "ws-auth.kraken.com" "443" [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let ip = match addresses with
    | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
        Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws-auth.kraken.com"
  in
  let client = `TLS (`Hostname "ws-auth.kraken.com", `IP ip, `Port 443) in
  let ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->

    Logging.info ~section "Authenticated WebSocket established, subscribing to balances";
    (* Call on_connected callback after successful connection and before starting message handler *)
    on_connected ();
    start_message_handler conn token on_failure on_heartbeat >>= fun () ->
    Logging.info ~section "Balances WebSocket connection closed";
    Lwt.return_unit

(** Initialize balances feed data stores *)
let initialize assets =
  Logging.info_f ~section "Initializing balances feed for %d assets" (List.length assets);

  (* Also create stores for common quote currencies *)
  let all_assets = List.sort_uniq String.compare (assets @ ["USD"; "EUR"; "USDT"; "USDC"]) in

  (* Mark configured assets (from trading config) *)
  Mutex.lock configured_assets_mutex;
  List.iter (fun asset -> Hashtbl.replace configured_assets asset ()) assets;
  Mutex.unlock configured_assets_mutex;

  (* Pre-create all balance stores during initialization - thread-safe *)
  Mutex.lock balance_stores_mutex;
  List.iter (fun asset ->
    if not (Hashtbl.mem balance_stores asset) then begin
      let store = BalanceStore.create () in
      Hashtbl.add balance_stores asset store;
      Logging.debug_f ~section "Created thread-safe balance store for %s" asset
    end
  ) all_assets;
  Mutex.unlock balance_stores_mutex;

  (* Mark initialization complete *)
  Atomic.set initialized true;
  Logging.info ~section "Balance stores initialized - now thread-safe"

(** Check for stale balance data and log warnings *)
let check_stale_balances assets =
  let stale_count = ref 0 in
  List.iter (fun asset ->
    if is_balance_stale asset 300.0 then begin (* 5 minutes threshold *)
      Logging.warn_f ~section "Balance data for %s is stale (>5 minutes old)" asset;
      incr stale_count;
      Telemetry.inc_counter (Telemetry.counter "balance_stale_assets" ~labels:[("asset", asset)] ()) ()
    end else
      Logging.debug_f ~section "Balance data for %s is fresh" asset
  ) assets;

  (* Update telemetry for stale balance count *)
  if !stale_count > 0 then
    Logging.warn_f ~section "%d assets have stale balance data" !stale_count

(** Cleanup dynamic (non-configured) assets to prevent unbounded growth.
    Configured assets are retained indefinitely regardless of staleness. *)
let cleanup_dynamic_assets () =
  Mutex.lock balance_stores_mutex;
  Mutex.lock configured_assets_mutex;

  (* Collect all assets and classify them *)
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

  (* Sort dynamic assets by last update time (most recent first) *)
  let dynamic_with_times = List.map (fun asset ->
    let last_update = try
      Mutex.lock balance_update_mutex;
      let time = Hashtbl.find last_balance_update asset in
      Mutex.unlock balance_update_mutex;
      time
    with Not_found ->
      Mutex.unlock balance_update_mutex;
      0.0  (* Never updated *)
    in
    last_update
  ) !dynamic in

  let dynamic_sorted = List.sort (fun (_, t1) (_, t2) -> Float.compare t2 t1)
    (List.combine !dynamic dynamic_with_times) in

  (* Keep only the most recently updated dynamic assets up to the cap *)
  let dynamic_to_keep = List.map fst (List.filteri (fun i _ -> i < dynamic_assets_cap) dynamic_sorted) in
  let dynamic_to_remove = List.filter (fun asset -> not (List.mem asset dynamic_to_keep)) !dynamic in

  (* Remove excess dynamic assets *)
  let removed_count = List.length dynamic_to_remove in
  List.iter (fun asset ->
    Hashtbl.remove balance_stores asset;
    Mutex.lock balance_update_mutex;
    Hashtbl.remove last_balance_update asset;
    Mutex.unlock balance_update_mutex;
  ) dynamic_to_remove;

  Mutex.unlock configured_assets_mutex;
  Mutex.unlock balance_stores_mutex;

  if removed_count > 0 then
    Logging.info_f ~section "Cleaned up %d dynamic balance assets, keeping %d configured + %d recent dynamic"
      removed_count (List.length !configured) (List.length dynamic_to_keep)

(** Restart the balance feed connection - useful when feed becomes stale.
    Note: The supervisor handles actual reconnection via heartbeat monitoring.
    This function is kept for backward compatibility but does minimal work. *)
let restart_connection () =
  (* The supervisor's heartbeat monitor will detect connection issues and reconnect automatically *)
  (* We don't need to spam logs or check every asset here *)
  Lwt.return_unit

(** Subscribe to balance update events *)
let subscribe_balance_updates () =
  let subscription = BalanceUpdateEventBus.subscribe balance_update_event_bus in
  (subscription.stream, subscription.close)

(** Force refresh balance data by requesting a new snapshot *)
let force_balance_refresh _token =
  Logging.info ~section "Forcing balance data refresh...";
  (* This would send a request for fresh balance data *)
  (* For now, we'll just log the refresh attempt *)
  Logging.info ~section "Balance refresh requested - implement actual refresh logic";
  Lwt.return_unit

(* Start periodic cleanup of dynamic assets *)
let () =
  Lwt.async (fun () ->
    let rec cleanup_loop () =
      let%lwt () = Lwt_unix.sleep 600.0 in (* Clean up every 10 minutes *)
      cleanup_dynamic_assets ();
      cleanup_loop ()
    in
    cleanup_loop ()
  )

