(** Balance cache for the balance view

    Provides reactive access to balance and open orders data for the balance dashboard UI,
    respecting the concurrency model.
*)

open Lwt.Infix
open Kraken
open Concurrency
open Ring_buffer
open Dio_strategies.Fee_cache

(** Individual wallet balance *)
type wallet_balance = {
  balance: float;
  wallet_type: string;
  wallet_id: string;
  last_updated: float;
}

(** Balance entry for UI display - aggregates across all wallets *)
type balance_entry = {
  asset: string;
  total_balance: float;
  wallets: wallet_balance list;
  last_updated: float;
}

(** Open order entry for UI display *)
type open_order_entry = {
  order_id: string;
  symbol: string;
  side: string;
  order_qty: float;
  remaining_qty: float;
  limit_price: float option;
  avg_price: float;
  order_status: string;
  maker_fee: float option;
  taker_fee: float option;
  last_updated: float;
}

(** Combined balance and orders snapshot for UI *)
type balance_snapshot = {
  balances: balance_entry list;
  open_orders: open_order_entry list;
  timestamp: float;
}

(** Balance/order update event for ring buffer storage *)
type balance_update_event =
  | BalanceUpdate of Kraken_balances_feed.balance_data
  | OrderUpdate of Kraken_executions_feed.execution_event

(** Event bus for balance snapshots *)
module BalanceSnapshotEventBus = Event_bus.Make(struct
  type t = balance_snapshot
end)

(** Balance cache state - Two Layer Architecture *)
type t = {
  (* Layer 1: Bounded ring buffer for individual balance/order updates *)
  layer1_ring_buffer: balance_update_event RingBuffer.t;

  (* Layer 2: Current and previous snapshots for retry/recovery *)
  mutable current_snapshot: balance_snapshot option;
  mutable previous_snapshot: balance_snapshot option;

  balance_snapshot_event_bus: BalanceSnapshotEventBus.t;
  update_condition: unit Lwt_condition.t;
  mutable last_update: float;  (* Track when we last published a snapshot *)
  update_interval: float;  (* Minimum time between periodic updates *)
}

(** Safe broadcast helper for cache update condition *)
let safe_broadcast_update_condition cache =
  try
    Lwt_condition.broadcast cache.update_condition ()
  with
  | Invalid_argument _ ->
      (* Promise already resolved - this can happen with concurrent broadcasts *)
      (* Not fatal, just means a waiter was already woken up *)
      ()
  | exn ->
      (* Other errors should still be logged *)
      Logging.warn_f ~section:"balance_cache" "Error broadcasting update_condition: %s" (Printexc.to_string exn)

(** Global shutdown flag for background tasks *)
let shutdown_requested = Atomic.make false

(** Signal shutdown to background tasks *)
let signal_shutdown () =
  Atomic.set shutdown_requested true

(** Global balance cache instance - Two Layer Architecture *)
let cache = {
  layer1_ring_buffer = RingBuffer.create 2000;  (* Bounded ring buffer for balance/order updates *)
  current_snapshot = None;
  previous_snapshot = None;
  balance_snapshot_event_bus = BalanceSnapshotEventBus.create "balance_snapshot";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 1.0;  (* Update balance snapshots at most every 1.0 seconds for dashboard mode *)
}

(** Initialization guard with mutex protection *)
let initialized = ref false
let init_mutex = Mutex.create ()

(** Kraken feeds ready barrier *)
let kraken_feeds_ready = ref false
let kraken_feeds_condition = Lwt_condition.create ()

(** Signal that Kraken feeds are ready for cache access *)
let signal_feeds_ready () =
  if not !kraken_feeds_ready then (
    kraken_feeds_ready := true;
    Lwt_condition.broadcast kraken_feeds_condition ()
  )

(** Subscribe to balance snapshot updates *)
let subscribe_balance_snapshot () =
  let subscription = BalanceSnapshotEventBus.subscribe ~persistent:true cache.balance_snapshot_event_bus in
  (subscription.stream, subscription.close)

(** Get balance event bus subscriber statistics *)
let get_balance_subscriber_stats () =
  BalanceSnapshotEventBus.get_subscriber_stats cache.balance_snapshot_event_bus

(** Convert balance data to UI entry - now aggregates wallets *)
let balance_data_to_entry asset (balance_data : Kraken_balances_feed.balance_data) =
  let last_updated = balance_data.last_updated in
  {
    asset;
    total_balance = balance_data.balance;
    wallets = [{
      balance = balance_data.balance;
      wallet_type = balance_data.wallet_type;
      wallet_id = balance_data.wallet_id;
      last_updated;
    }];
    last_updated;
  }

(** Convert open order data to UI entry *)
let open_order_to_entry (order : Kraken_executions_feed.open_order) =
  let maker_fee = get_maker_fee ~exchange:"kraken" ~symbol:order.symbol in
  let taker_fee = get_taker_fee ~exchange:"kraken" ~symbol:order.symbol in
  {
    order_id = order.order_id;
    symbol = order.symbol;
    side = (match order.side with Kraken_executions_feed.Buy -> "buy" | Kraken_executions_feed.Sell -> "sell");
    order_qty = order.order_qty;
    remaining_qty = order.remaining_qty;
    limit_price = order.limit_price;
    avg_price = order.avg_price;
    order_status = Kraken_executions_feed.string_of_order_status order.order_status;
    maker_fee;
    taker_fee;
    last_updated = order.last_updated;
  }

(** Aggregate balance data across all wallets for an asset *)
let aggregate_asset_balance asset =
  (* For now, just use the existing balance data - this is a temporary fix *)
  (* TODO: Implement proper multi-wallet aggregation *)
  try
    if Kraken_balances_feed.has_balance_data asset then
      let balance_data = Kraken_balances_feed.get_balance_data asset in
      Some balance_data
    else
      None
  with _ -> None

(** Safely collect all balance data *)
let collect_balances () =
  try
    (* Get all assets that have balance data from Kraken balances feed *)
    let assets = Kraken_balances_feed.get_all_assets () in
    Logging.debug_f ~section:"balance_cache" "Found %d initialized assets" (List.length assets);
    let balances = List.fold_left (fun acc asset ->
      try
        match aggregate_asset_balance asset with
        | Some balance_data ->
            (balance_data_to_entry asset balance_data) :: acc
        | None -> acc
      with exn ->
        Logging.warn_f ~section:"balance_cache" "Error processing asset %s: %s" asset (Printexc.to_string exn);
        acc
    ) [] assets in
    Logging.debug_f ~section:"balance_cache" "Collected %d balance entries" (List.length balances);
    balances
  with exn ->
    (* Return empty list if we can't collect balances *)
    Logging.warn_f ~section:"balance_cache" "Error collecting balances: %s" (Printexc.to_string exn);
    []

(** Safely collect all open orders *)
let collect_open_orders () =
  try
    (* Get open orders from all symbols *)
    let symbols = Kraken_executions_feed.get_all_symbols () in
    Logging.debug_f ~section:"balance_cache" "Found %d initialized symbols" (List.length symbols);
    let all_orders = List.fold_left (fun acc symbol ->
      try
        let open_orders = Kraken_executions_feed.get_open_orders symbol in
        (List.map open_order_to_entry open_orders) @ acc
      with _ -> acc
    ) [] symbols in
    Logging.debug_f ~section:"balance_cache" "Collected %d total open orders" (List.length all_orders);
    all_orders
  with exn ->
    (* Return empty list if we can't collect orders *)
    Logging.warn_f ~section:"balance_cache" "Error collecting open orders: %s" (Printexc.to_string exn);
    []

(** Rebuild balance snapshot from Layer 1 events (triggered by balance/order updates) *)
let rebuild_snapshot_from_layer1 () =
  let timestamp = Unix.time () in  (* Always use current time for fresh timestamp *)
  try
    let balances = collect_balances () in
    let open_orders = collect_open_orders () in
    Logging.debug_f ~section:"balance_cache" "Created snapshot with %d balances and %d orders at timestamp %.2f"
      (List.length balances) (List.length open_orders) timestamp;
    {
      balances;
      open_orders;
      timestamp;
    }
  with exn ->
    Logging.warn_f ~section:"balance_cache" "Failed to rebuild snapshot: %s" (Printexc.to_string exn);
    {
      balances = [];
      open_orders = [];
      timestamp;
    }

(** Update Layer 2 snapshots - move current to previous, set new current *)
let update_layer2_snapshots new_snapshot =
  (* Move current to previous (destroying old previous) *)
  cache.previous_snapshot <- cache.current_snapshot;
  (* Set new current *)
  cache.current_snapshot <- Some new_snapshot;

  Logging.debug_f ~section:"balance_cache" "Layer 2 snapshots updated - current version timestamp: %.2f, previous exists: %b"
    new_snapshot.timestamp (Option.is_some cache.previous_snapshot)

(** Wait for next balance update *)
let wait_for_update () = Lwt_condition.wait cache.update_condition

(** Get current balance snapshot (returns cached data, never blocks) *)
let current_balance_snapshot () =
  match cache.current_snapshot with
  | Some snapshot -> snapshot
  | None ->
    (* Return empty snapshot if cache not initialized yet *)
    {
      balances = [];
      open_orders = [];
      timestamp = Unix.time ();
    }

(** Add a balance/order update event to Layer 1 ring buffer and immediately trigger Layer 2 snapshot *)
let add_balance_update_event event =
  RingBuffer.write cache.layer1_ring_buffer event;
  Logging.debug ~section:"balance_cache" "Added balance/order update event to Layer 1 ring buffer";

  (* Immediately rebuild Layer 2 snapshot and publish when any event is added to Layer 1 *)
  let snapshot = rebuild_snapshot_from_layer1 () in
  update_layer2_snapshots snapshot;

  (* Update last_update timestamp *)
  cache.last_update <- Unix.time ();

  (* Publish snapshot to event bus for streaming *)
  BalanceSnapshotEventBus.publish cache.balance_snapshot_event_bus snapshot;
  safe_broadcast_update_condition cache;

  Logging.debug_f ~section:"balance_cache" "Balance snapshot published immediately after Layer 1 update (timestamp: %.2f)"
    snapshot.timestamp

(** Periodic update function that publishes snapshots even when balances/orders haven't changed *)
let update_balance_cache_periodic () =
  let now = Unix.time () in
  let time_since_last = now -. cache.last_update in

  (* Always try to update if enough time has passed, OR if it's been too long since last publish (keepalive) *)
  let should_update = time_since_last >= cache.update_interval in
  let needs_keepalive = time_since_last >= 30.0 in  (* Keepalive every 30 seconds max *)

  if should_update || needs_keepalive then (
    try
      let snapshot = rebuild_snapshot_from_layer1 () in
      update_layer2_snapshots snapshot;
      cache.last_update <- now;

      (* Publish snapshot to event bus for streaming *)
      BalanceSnapshotEventBus.publish cache.balance_snapshot_event_bus snapshot;
      safe_broadcast_update_condition cache;

      Logging.debug_f ~section:"balance_cache" "Periodic balance snapshot published (timestamp: %.2f)"
        snapshot.timestamp
    with exn ->
      Logging.warn_f ~section:"balance_cache" "Failed to rebuild snapshot during periodic update: %s"
        (Printexc.to_string exn)
  )

(** Clear cache *)
let clear_cache () =
  cache.current_snapshot <- Some {
    balances = [];
    open_orders = [];
    timestamp = Unix.time ();
  };
  let now = Unix.time () in
  BalanceSnapshotEventBus.publish cache.balance_snapshot_event_bus {
    balances = [];
    open_orders = [];
    timestamp = now;
  };
  safe_broadcast_update_condition cache


(** Publish initial balance snapshot synchronously *)
let publish_initial_snapshot () =
  Logging.debug ~section:"balance_cache" "Publishing initial balance snapshot";
  (* Start with empty snapshot - don't try to access Kraken feeds during init *)
  let initial_timestamp = Unix.time () in
  let initial_snapshot = {
    balances = [];
    open_orders = [];
    timestamp = initial_timestamp;
  } in
  cache.current_snapshot <- Some initial_snapshot;
  cache.last_update <- initial_timestamp;
  BalanceSnapshotEventBus.publish cache.balance_snapshot_event_bus initial_snapshot;
  safe_broadcast_update_condition cache;
  Logging.debug_f ~section:"balance_cache" "Initial balance snapshot published with timestamp %.2f" initial_timestamp

(** Start event-driven balance updater (subscribes to balance and order events) *)
let start_balance_updater () =
  Logging.debug_f ~section:"balance_cache" "Starting event-driven balance updater";

  (* Start event-driven updater that reacts to balance and order changes *)
  Lwt.async (fun () ->
    Logging.debug ~section:"balance_cache" "Starting balance event-driven updater";

    (* Wait for Kraken feeds to be ready before starting subscriptions *)
    (if !kraken_feeds_ready then
      Lwt.return_unit
    else
      Lwt_condition.wait kraken_feeds_condition) >>= fun () ->
    Logging.debug ~section:"balance_cache" "Balance event-driven updater starting after feeds ready";

    (* Subscribe to balance updates once *)
    Logging.debug ~section:"balance_cache" "Subscribing to balance updates";
    let (balance_stream, _balance_close) = Kraken_balances_feed.subscribe_balance_updates () in

    (* Subscribe to order updates once *)
    Logging.debug ~section:"balance_cache" "Subscribing to order updates";
    let (order_stream, _order_close) = Kraken_executions_feed.subscribe_order_updates () in

    (* Process balance updates in background *)
    let _balance_processor = Lwt.async (fun () ->
      let rec process_balance_updates () =
        (* Check for shutdown request *)
        if Atomic.get shutdown_requested then (
          Logging.debug ~section:"balance_cache" "Balance processor shutting down due to shutdown request";
          Lwt.return_unit
        ) else (
          Lwt.catch
            (fun () -> Lwt_stream.get balance_stream >>= function
              | Some balance_data ->
                  (* Balance update received - add to Layer 1 (snapshot rebuild happens immediately) *)
                  Logging.debug ~section:"balance_cache" "Balance update received, processing...";
                  add_balance_update_event (BalanceUpdate balance_data);
                  (* Note: Snapshot rebuild and publishing happens immediately in add_balance_update_event *)

                  (* Continue listening *)
                  process_balance_updates ()
              | None ->
                  (* Stream ended - wait a bit and try to resubscribe *)
                  Logging.debug ~section:"balance_cache" "Balance subscription stream ended, will resubscribe";
                  Lwt_unix.sleep 1.0 >>= process_balance_updates)
            (fun exn ->
              Logging.warn_f ~section:"balance_cache" "Balance subscription error: %s, will retry" (Printexc.to_string exn);
              Lwt_unix.sleep 1.0 >>= process_balance_updates)
        )
      in
      process_balance_updates ()
    ) in

    (* Process order updates in background *)
    let _order_processor = Lwt.async (fun () ->
      let rec process_order_updates () =
        (* Check for shutdown request *)
        if Atomic.get shutdown_requested then (
          Logging.debug ~section:"balance_cache" "Order processor shutting down due to shutdown request";
          Lwt.return_unit
        ) else (
          Lwt.catch
            (fun () -> Lwt_stream.get order_stream >>= function
              | Some order_event ->
                  (* Order update received - add to Layer 1 (snapshot rebuild happens immediately) *)
                  Logging.debug ~section:"balance_cache" "Order update received, processing...";
                  add_balance_update_event (OrderUpdate order_event);
                  (* Note: Snapshot rebuild and publishing happens immediately in add_balance_update_event *)

                  (* Continue listening *)
                  process_order_updates ()
              | None ->
                  (* Stream ended - wait a bit and try to resubscribe *)
                  Logging.debug ~section:"balance_cache" "Order subscription stream ended, will resubscribe";
                  Lwt_unix.sleep 1.0 >>= process_order_updates)
            (fun exn ->
              Logging.warn_f ~section:"balance_cache" "Order subscription error: %s, will retry" (Printexc.to_string exn);
              Lwt_unix.sleep 1.0 >>= process_order_updates)
        )
      in
      process_order_updates ()
    ) in

    (* Processors are running in background - return immediately *)
    Lwt.return_unit
  );

  (* Start periodic updater that publishes snapshots even when balances/orders haven't changed *)
  Lwt.async (fun () ->
    Logging.debug ~section:"balance_cache" "Starting periodic balance updater";
    
    (* Wait for Kraken feeds to be ready *)
    (if !kraken_feeds_ready then
      Lwt.return_unit
    else
      Lwt_condition.wait kraken_feeds_condition) >>= fun () ->
    
    let rec periodic_loop () =
      (* Check for shutdown request *)
      if Atomic.get shutdown_requested then (
        Logging.debug ~section:"balance_cache" "Periodic balance updater shutting down due to shutdown request";
        Lwt.return_unit
      ) else (
        (* Update cache periodically (this will check if enough time has passed) *)
        update_balance_cache_periodic ();
        (* Sleep 1 second and continue *)
        Lwt_unix.sleep 1.0 >>= periodic_loop
      )
    in
    periodic_loop ()
  )

(** Force cleanup stale subscribers for dashboard memory management *)
let force_cleanup_stale_subscribers () =
  BalanceSnapshotEventBus.force_cleanup_stale_subscribers cache.balance_snapshot_event_bus ()

(** Initialize the balance cache system with double-checked locking *)
let init () =
  (* First check without locking (fast path) *)
  if !initialized then (
    Logging.debug ~section:"balance_cache" "Balance cache already initialized, skipping";
    ()
  ) else (
    (* Acquire lock for initialization *)
    Mutex.lock init_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock init_mutex)
      (fun () ->
        (* Double-check after acquiring lock *)
        if !initialized then (
          Logging.debug ~section:"balance_cache" "Balance cache already initialized during lock acquisition";
          ()
        ) else (
          Logging.debug_f ~section:"balance_cache" "Initializing balance cache";
          initialized := true;
          try
            (* Publish initial snapshot synchronously before starting background updater *)
            publish_initial_snapshot ();
            start_balance_updater ();
            Logging.debug_f ~section:"balance_cache" "Balance cache initialized successfully"
          with exn ->
            Logging.error_f ~section:"balance_cache" "Failed to initialize balance cache: %s" (Printexc.to_string exn);
            Logging.debug_f ~section:"balance_cache" "Balance cache initialization failed - using empty cache"
        )
      )
  )
