(** Balance cache for the balance view

    Provides reactive access to balance and open orders data for the balance dashboard UI,
    respecting the concurrency model.
*)

open Lwt.Infix
open Kraken
open Concurrency
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

(** Event bus for balance snapshots *)
module BalanceSnapshotEventBus = Event_bus.Make(struct
  type t = balance_snapshot
end)

(** Balance cache state - mirrors telemetry cache pattern *)
type t = {
  mutable current_snapshot: balance_snapshot option;
  balance_snapshot_event_bus: BalanceSnapshotEventBus.t;
  update_condition: unit Lwt_condition.t;
  mutable last_update: float;
  update_interval: float;  (* Minimum time between updates *)
  mutable consecutive_failures: int;  (* Track consecutive failures for backoff *)
  mutable backoff_until: float;  (* When to resume updates after backoff *)
}

(** Global balance cache instance *)
let cache = {
  current_snapshot = None;
  balance_snapshot_event_bus = BalanceSnapshotEventBus.create "balance_snapshot";
  update_condition = Lwt_condition.create ();
  last_update = 0.0;
  update_interval = 5.0;  (* Update every 5 seconds - less aggressive *)
  consecutive_failures = 0;
  backoff_until = 0.0;
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
let subscribe_balance_snapshot () = BalanceSnapshotEventBus.subscribe cache.balance_snapshot_event_bus

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

(** Create a new balance snapshot *)
let create_snapshot () =
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
    Logging.warn_f ~section:"balance_cache" "Failed to create snapshot: %s" (Printexc.to_string exn);
    {
      balances = [];
      open_orders = [];
      timestamp;
    }



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

(** Clear cache *)
let clear_cache () =
  cache.current_snapshot <- Some {
    balances = [];
    open_orders = [];
    timestamp = Unix.time ();
  };
  let now = Unix.time () in
  cache.last_update <- now;
  BalanceSnapshotEventBus.publish cache.balance_snapshot_event_bus {
    balances = [];
    open_orders = [];
    timestamp = now;
  };
  Lwt_condition.broadcast cache.update_condition ()


(** Start background balance updater (mirrors telemetry cache pattern) *)
let start_balance_updater () =
  Logging.debug_f ~section:"balance_cache" "Starting balance updater...";
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
  Lwt_condition.broadcast cache.update_condition ();
  Logging.debug_f ~section:"balance_cache" "Started with empty snapshot, timestamp %.2f" initial_timestamp;

  (* Start periodic balance updater - mirrors telemetry polling pattern *)
  let _polling_updater = Lwt.async (fun () ->
    Logging.debug_f ~section:"balance_cache" "Starting balance polling updater";

    (* Wait for Kraken feeds to be ready before starting polling *)
    (if !kraken_feeds_ready then
      Lwt.return_unit
    else
      Lwt_condition.wait kraken_feeds_condition) >>= fun () ->
    Logging.debug_f ~section:"balance_cache" "Balance polling updater starting after feeds ready";

    let rec polling_loop () =
      Logging.debug_f ~section:"balance_cache" "Balance polling loop iteration";
      let now = Unix.time () in
      let time_since_last = now -. cache.last_update in
      Logging.debug_f ~section:"balance_cache" "Time since last update: %.2f, interval: %.2f" time_since_last cache.update_interval;

      (* Check if we're in backoff period *)
      let in_backoff = now < cache.backoff_until in

      if in_backoff then
        Logging.debug_f ~section:"balance_cache" "In backoff period, skipping update until %.2f" cache.backoff_until
      else (
        (* Always try to update - either if enough time has passed, or if data is getting stale *)
        let should_update = time_since_last >= cache.update_interval ||
                           (match cache.current_snapshot with
                            | Some current -> (now -. current.timestamp) > 30.0  (* Force update if data > 30 seconds old *)
                            | None -> true) in

        if should_update then (
          Logging.debug ~section:"balance_cache" "Updating balance snapshot";
          (try
            let snapshot = create_snapshot () in
            cache.current_snapshot <- Some snapshot;
            cache.last_update <- now;
            cache.consecutive_failures <- 0;  (* Reset failure count on success *)
            cache.backoff_until <- 0.0;  (* Clear backoff *)
            BalanceSnapshotEventBus.publish cache.balance_snapshot_event_bus snapshot;
            Lwt_condition.broadcast cache.update_condition ();
            Logging.debug_f ~section:"balance_cache" "Balance snapshot updated with %d balances, %d orders, timestamp: %.2f"
              (List.length snapshot.balances) (List.length snapshot.open_orders) snapshot.timestamp
          with exn ->
            (* Handle failure with exponential backoff *)
            cache.consecutive_failures <- cache.consecutive_failures + 1;
            let backoff_seconds = min 300.0 (5.0 *. (2.0 ** float_of_int (min cache.consecutive_failures 6))) in
            cache.backoff_until <- now +. backoff_seconds;
            Logging.warn_f ~section:"balance_cache" "Failed to update balance snapshot (attempt %d): %s, backing off for %.1f seconds"
              cache.consecutive_failures (Printexc.to_string exn) backoff_seconds
          )
        )
      );

      Lwt_unix.sleep 2.0 >>= polling_loop  (* Check every 2 seconds to reduce resource usage *)
    in
    polling_loop ()
  ) in

  (* Balance cache updates itself periodically - no external triggers needed *)
  ()

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
            start_balance_updater ();
            Logging.debug_f ~section:"balance_cache" "Balance cache initialized successfully"
          with exn ->
            Logging.error_f ~section:"balance_cache" "Failed to initialize balance cache: %s" (Printexc.to_string exn);
            Logging.debug_f ~section:"balance_cache" "Balance cache initialization failed - using empty cache"
        )
      )
  )
