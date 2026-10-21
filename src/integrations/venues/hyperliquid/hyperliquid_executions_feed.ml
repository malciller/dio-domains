[@@@ocaml.text
  " Real-time order and trade event tracking via WebSocket subscriptions. Per-symbol\n\
  \    execution ring buffers, open-order state, and trade deduplication with bounded FIFO\n\
  \    eviction; per-resource mutexes and atomic flags. "]

open Lwt.Infix

let section = "hyperliquid_executions_feed"

type exec_type =
  | PendingNew
  | New
  | Trade
  | PartiallyFilled
  | Filled
  | Canceled
  | Expired
  | Amended
  | Restated
  | Rejected
  | Unknown of string

type side =
  | Buy
  | Sell

type order_status =
  | PendingNewStatus
  | NewStatus
  | PartiallyFilledStatus
  | FilledStatus
  | CanceledStatus
  | ExpiredStatus
  | RejectedStatus
  | UnknownStatus of string

type open_order =
  { order_id : string
  ; symbol : string
  ; side : side
  ; order_qty : float
  ; cum_qty : float
  ; remaining_qty : float
  ; limit_price : float option
  ; avg_price : float
  ; cum_cost : float
  ; order_status : order_status
  ; order_userref : int option
  ; cl_ord_id : string option
  ; last_updated : float
  }

type execution_event =
  { order_id : string
  ; symbol : string
  ; exec_type : exec_type
  ; order_status : order_status
  ; limit_price : float option
  ; side : side
  ; order_qty : float
  ; cum_qty : float
  ; cum_cost : float
  ; avg_price : float
  ; timestamp : float
  ; trade_id : int64 option
  ; last_qty : float option
  ; last_price : float option
  ; fee : float option
  ; cl_ord_id : string option
  ; venue_time : float
       [@ocaml.doc
         " The venue's own time for this event, or 0.0 for events the feed synthesises \
          locally\n\
         \      (optimistic injection, cancel detection, reconcile sweeps) where no \
          venue time\n\
         \      exists. The capital oracle needs a venue time to judge whether the \
          venue's balance\n\
         \      figure has already incorporated the event; a local clock cannot answer \
          that. "]
  }

module RingBuffer = Concurrency.Ring_buffer.RingBuffer
[@@ocaml.doc " Lock-free ring buffer used for execution event storage. "]

let max_processed_tids = 256
[@@ocaml.doc
  " Maximum trade IDs retained per symbol for deduplication. Eviction follows FIFO order\n\
  \    when the cap is exceeded. "]
;;

type store =
  { events_buffer : execution_event RingBuffer.t
  ; open_orders : (string, open_order) Hashtbl.t
  ; open_orders_cache : open_order list Atomic.t
       [@ocaml.doc " Lock-free snapshot of active open orders for the domain hot path. "]
  ; open_orders_generation : int Atomic.t
       [@ocaml.doc
         " Per-symbol generation bumped when this symbol's snapshot is republished. \
          Consumers\n\
         \      use it to skip a full rescan of this symbol's orders; a process-global \
          counter would\n\
         \      make one symbol's order change invalidate every other symbol's cache. "]
  ; changes : (string * open_order option) list Atomic.t
       [@ocaml.doc
         " Orders mutated since the last drain, newest first. [Some o] is the current \
          record\n\
         \      for an add/replace, [None] a removal. Lets the strategy apply the \
          per-order delta\n\
         \      (O(changes)) instead of an O(open-orders) rescan. Writers prepend under\n\
         \      [orders_mutex]; the reader swaps it out lock-free with {!drain_changes}. "]
  ; changes_overflow : bool Atomic.t
       [@ocaml.doc
         " Set when the change log cannot represent the delta (a full clear/replace), \
          forcing\n\
         \      the consumer to do a full rescan. "]
  ; ready : bool Atomic.t
  ; processed_tids : (int64, float) Hashtbl.t
       [@ocaml.doc " trade_id to arrival_time mapping for deduplication. "]
  ; processed_tids_queue : int64 Queue.t
       [@ocaml.doc " FIFO eviction queue for processed trade IDs. "]
  ; orders_mutex : Mutex.t
  ; tids_mutex : Mutex.t
  }
[@@ocaml.doc " Per-symbol execution state container with readiness signalling. "]

let stores = Ds.Cow_table.create ~shard_count:32 ()
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

let order_index_mutex = Mutex.create ()
[@@ocaml.doc
  " Guards [order_to_symbol]. Separate from [initialization_mutex] (which guards [stores])\n\
  \    to avoid contention between domain workers reading open orders and the WS thread\n\
  \    updating the index per event. "]
;;

let order_to_symbol =
  Concurrency.Fifo_index.create
    ~label:(section ^ "/order_to_symbol")
    ~cap:Concurrency.Cache_limits.order_index_startup_cap
    ()
[@@ocaml.doc
  " order_id to symbol index with adaptive capacity and FIFO eviction. Callers must hold\n\
  \    [order_index_mutex]. "]
;;

let () =
  Concurrency.Fifo_index.publish_metrics
    order_to_symbol
    ~name:"hyperliquid.order_to_symbol"
;;

let order_to_symbol_startup_done = Atomic.make false

let _startup_snapshot_done : bool Atomic.t = Atomic.make false
[@@ocaml.doc
  " Set once after [inject_open_orders]. Domains poll it each cycle;\n\
  \    [Exchange_wakeup.signal_all] on transition wakes sleeping domains without a \
   wall-clock\n\
  \    delay. "]
;;

let is_startup_snapshot_done () = Atomic.get _startup_snapshot_done

let add_to_order_to_symbol order_id symbol =
  Concurrency.Fifo_index.set order_to_symbol ~key:order_id ~value:symbol
[@@ocaml.doc
  " Insert an order_id to symbol mapping, evicting oldest-first past the cap. Caller must\n\
  \    hold order_index_mutex. "]
;;

let retire_from_order_to_symbol order_id =
  Concurrency.Fifo_index.remove order_to_symbol order_id
[@@ocaml.doc " Retires an order_id. Caller must hold order_index_mutex. "]
;;

let mark_startup_complete () =
  if not (Atomic.exchange order_to_symbol_startup_done true)
  then (
    let observed = Concurrency.Fifo_index.length order_to_symbol in
    Concurrency.Fifo_index.lock_cap
      order_to_symbol
      ~observed
      ~floor:Concurrency.Cache_limits.order_index_hyperliquid_min_cap)
[@@ocaml.doc
  " Tune the cap from the volume seen once the startup snapshot is fully consumed:\n\
  \    max(1024, observed * 1.5 + 1). Idempotent; only the first invocation takes \
   effect. The\n\
  \    bound already exists before this runs; this only adjusts it. "]
;;

let amended_blacklist : (string, float) Hashtbl.t = Hashtbl.create 16
[@@ocaml.doc
  " Order IDs removed by cancel-replace amendments, to block late WebSocket orderUpdates\n\
  \    events from re-adding a superseded order. "]
;;

let amended_blacklist_mutex = Mutex.create ()

let get_symbol_store symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store -> store
  | None ->
    Mutex.lock initialization_mutex;
    let store =
      match Ds.Cow_table.find_opt stores symbol with
      | Some store -> store
      | None ->
        let store =
          { events_buffer = RingBuffer.create 512
          ; open_orders = Hashtbl.create 32
          ; open_orders_cache = Atomic.make []
          ; open_orders_generation = Atomic.make 0
          ; changes = Atomic.make []
          ; changes_overflow = Atomic.make false
          ; ready = Atomic.make (Atomic.get _startup_snapshot_done)
          ; processed_tids = Hashtbl.create 32
          ; processed_tids_queue = Queue.create ()
          ; orders_mutex = Mutex.create ()
          ; tids_mutex = Mutex.create ()
          }
        in
        Ds.Cow_table.set stores symbol store;
        store
    in
    Mutex.unlock initialization_mutex;
    store
[@@ocaml.doc
  " Retrieve or lazily create a per-symbol store. Uses double-checked locking under\n\
  \    initialization_mutex for thread-safe initialization. "]
;;

let orders_generation : int Atomic.t = Atomic.make 0
[@@ocaml.doc
  " Bumped on every open-orders snapshot republish so consumers can skip a full rescan\n\
  \    when unchanged. Mirrors [Kraken_executions_feed.orders_generation]. "]
;;

let get_orders_generation () = Atomic.get orders_generation [@@inline]

[@@@ocaml.text
  " Publishes an immutable snapshot of open_orders to the atomic cache. Must be called by\n\
  \    writers under store.orders_mutex. "]

let note_change store id order =
  Atomic.set store.changes ((id, order) :: Atomic.get store.changes)
[@@ocaml.doc
  " Record an order whose state changed, for delta consumption by the strategy. [Some o]\n\
  \    for an add/replace, [None] for a removal. Writers prepend under \
   [store.orders_mutex];\n\
  \    the reader swaps the list out lock-free, so the get/set here never needs a lock. A\n\
  \    writer that lands between a reader's get and its exchange is simply drained next \
   cycle\n\
  \    (the accompanying generation bump guarantees another drain). "]
[@@inline]
;;

let drain_changes store =
  let changes = Atomic.exchange store.changes [] in
  let overflow = Atomic.exchange store.changes_overflow false in
  changes, overflow
[@@ocaml.doc
  " [drain_changes store] swaps out the [(id, order option)] changes since the last drain\n\
  \    (and the overflow flag) lock-free, so the per-cycle drain never blocks on the \
   feed's\n\
  \    order mutex. "]
;;

let publish_open_orders_cache store =
  let snapshot = Hashtbl.fold (fun _id order acc -> order :: acc) store.open_orders [] in
  Atomic.set store.open_orders_cache snapshot;
  Atomic.incr store.open_orders_generation;
  Atomic.incr orders_generation;
  Concurrency.Oracle_wakeup.notify_orders ()
;;

let get_orders_generation_for_symbol symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store -> Atomic.get store.open_orders_generation
  | None -> -1
[@@ocaml.doc
  " Per-symbol generation for [symbol]. [-1] when the store does not exist yet, so the\n\
  \    consumer's rescan gate ([generation >= 0]) forces a scan until the first \
   snapshot. "]
;;

let drain_open_order_changes ~symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store ->
    let changes, overflow = drain_changes store in
    ( List.rev_map
        (fun (id, (o : open_order option)) ->
          let snap =
            match o with
            | Some o ->
              Some
                ( o.limit_price
                , o.remaining_qty
                , (if o.side = Buy then "buy" else "sell")
                , o.order_userref )
            | None -> None
          in
          id, snap)
        changes
    , overflow )
  | None -> [], true
[@@ocaml.doc
  " Drain the per-order change delta for [symbol]:\n\
  \    [(((id, snapshot option) list), overflow)]. [snapshot] is\n\
  \    [(limit_price, remaining_qty, side, order_userref)] for an add/replace, or [None] \
   for\n\
  \    a removal; changes are returned in chronological order. [overflow] (or a missing\n\
  \    store) means the caller must do a full rescan. O(changes). "]
;;

let notify_ready store =
  if not (Atomic.get store.ready)
  then (
    Atomic.set store.ready true;
    try Lwt_condition.broadcast ready_condition () with
    | _ -> ())
;;

let set_startup_snapshot_done () =
  if not (Atomic.exchange _startup_snapshot_done true)
  then (
    Logging.debug_f
      ~section
      "HL open-order snapshot injected \226\128\148 domains may now activate";
    Ds.Cow_table.iter (fun _symbol store -> notify_ready store) stores;
    Concurrency.Exchange_wakeup.signal_all ())
;;

let get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  let orders = Atomic.get store.open_orders_cache in
  List.find_opt (fun (o : open_order) -> o.order_id = order_id) orders
[@@ocaml.doc " Lock-free read of a single open order by ID from the atomic cache. "]
[@@inline always]
;;

let find_order_everywhere order_id =
  Mutex.lock order_index_mutex;
  let symbol_opt = Concurrency.Fifo_index.find_opt order_to_symbol order_id in
  Mutex.unlock order_index_mutex;
  match symbol_opt with
  | Some symbol -> get_open_order symbol order_id
  | None -> None
;;

let get_open_orders symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.open_orders_cache
[@@ocaml.doc
  " Lock-free read of open orders from the atomic cache. Zero mutex contention on domain\n\
  \    hotpath. "]
[@@inline always]
;;

let fold_open_orders symbol ~init ~f =
  let store = get_symbol_store symbol in
  let snapshot = Atomic.get store.open_orders_cache in
  List.fold_left (fun acc order -> f acc order) init snapshot
[@@ocaml.doc
  " Lock-free fold over open orders from the atomic cache. Zero mutex contention on domain\n\
  \    hotpath. "]
[@@inline always]
;;

let remove_open_order ~symbol ~order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let existed = Hashtbl.mem store.open_orders order_id in
  if existed
  then (
    Hashtbl.remove store.open_orders order_id;
    note_change store order_id None;
    publish_open_orders_cache store);
  Mutex.unlock store.orders_mutex;
  if existed
  then (
    Mutex.lock order_index_mutex;
    retire_from_order_to_symbol order_id;
    Mutex.unlock order_index_mutex;
    Mutex.lock amended_blacklist_mutex;
    Hashtbl.replace amended_blacklist order_id (Unix.gettimeofday ());
    Mutex.unlock amended_blacklist_mutex;
    Logging.debug_f
      ~section
      "Removed old order %s [%s] after amendment (blacklisted)"
      order_id
      symbol)
[@@ocaml.doc
  " Remove a single open order by ID. Called after a successful cancel-replace amendment\n\
  \    to prevent the superseded order from appearing as a duplicate in get_open_orders. \
   The\n\
  \    order ID is added to the amendment blacklist. "]
;;

let get_all_symbols () =
  Mutex.lock initialization_mutex;
  let symbols = Ds.Cow_table.fold (fun symbol _ acc -> symbol :: acc) stores ~init:[] in
  Mutex.unlock initialization_mutex;
  symbols
[@@ocaml.doc " Return all symbols that have initialized execution stores. "]
;;

let cleanup_stale_orders () =
  let now = Unix.gettimeofday () in
  let stale_threshold = 24.0 *. 3600.0 in
  let stale_orders = ref [] in
  let all_symbols = get_all_symbols () in
  List.iter
    (fun symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      Hashtbl.iter
        (fun order_id (order : open_order) ->
          if now -. order.last_updated > stale_threshold
          then stale_orders := (symbol, order_id) :: !stale_orders)
        store.open_orders;
      Mutex.unlock store.orders_mutex)
    all_symbols;
  let removed_count = List.length !stale_orders in
  if removed_count > 0
  then (
    Logging.info_f
      ~section
      "Safety cleanup: removing %d orders older than 24h"
      removed_count;
    List.iter
      (fun (_symbol, order_id) ->
        let store = get_symbol_store _symbol in
        Mutex.lock store.orders_mutex;
        let removed = Hashtbl.mem store.open_orders order_id in
        if removed
        then (
          Hashtbl.remove store.open_orders order_id;
          note_change store order_id None;
          publish_open_orders_cache store);
        Mutex.unlock store.orders_mutex;
        if removed
        then (
          Mutex.lock order_index_mutex;
          retire_from_order_to_symbol order_id;
          Mutex.unlock order_index_mutex;
          Logging.debug_f
            ~section
            "Removed stale order during safety cleanup: %s [%s]"
            order_id
            _symbol))
      !stale_orders);
  Mutex.lock amended_blacklist_mutex;
  let blacklist_to_remove = ref [] in
  Hashtbl.iter
    (fun order_id timestamp ->
      if now -. timestamp > 30.0
      then blacklist_to_remove := order_id :: !blacklist_to_remove)
    amended_blacklist;
  List.iter (Hashtbl.remove amended_blacklist) !blacklist_to_remove;
  let bl_removed = List.length !blacklist_to_remove in
  Mutex.unlock amended_blacklist_mutex;
  if bl_removed > 0
  then
    Logging.debug_f ~section "Cleaned up %d stale amendment blacklist entries" bl_removed;
  List.iter
    (fun symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.tids_mutex;
      let tids_to_remove = ref [] in
      Hashtbl.iter
        (fun tid arrival_time ->
          if now -. arrival_time > 600.0 then tids_to_remove := tid :: !tids_to_remove)
        store.processed_tids;
      List.iter (Hashtbl.remove store.processed_tids) !tids_to_remove;
      let original_queue_len = Queue.length store.processed_tids_queue in
      if original_queue_len > 0
      then (
        let temp = Queue.create () in
        Queue.iter
          (fun tid -> if Hashtbl.mem store.processed_tids tid then Queue.push tid temp)
          store.processed_tids_queue;
        Queue.clear store.processed_tids_queue;
        Queue.transfer temp store.processed_tids_queue);
      let tids_removed = List.length !tids_to_remove in
      Mutex.unlock store.tids_mutex;
      if tids_removed > 0
      then
        Logging.debug_f
          ~section
          "Cleaned %d stale processed_tids for %s"
          tids_removed
          symbol)
    all_symbols;
  Mutex.lock order_index_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock order_index_mutex)
    (fun () -> ignore (Concurrency.Fifo_index.trim_queue order_to_symbol))
[@@ocaml.doc
  " Periodic safety cleanup. Removes stale orders (>24h), expired amendment blacklist\n\
  \    entries (>30s), stale processed trade IDs (>10min), and orphaned entries from the\n\
  \    order_to_symbol eviction queue. "]
;;

let clear_all_open_orders () =
  let all_symbols = get_all_symbols () in
  let total_removed = ref 0 in
  List.iter
    (fun symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      let count = Hashtbl.length store.open_orders in
      total_removed := !total_removed + count;
      Hashtbl.clear store.open_orders;
      Atomic.set store.changes_overflow true;
      publish_open_orders_cache store;
      Mutex.unlock store.orders_mutex)
    all_symbols;
  Mutex.lock order_index_mutex;
  Concurrency.Fifo_index.clear order_to_symbol;
  Mutex.unlock order_index_mutex;
  if !total_removed > 0
  then
    Logging.info_f ~section "Cleared %d stale open orders on reconnection" !total_removed
  else Logging.info ~section "No open orders to clear on reconnection"
[@@ocaml.doc
  " Clear all open orders across all symbol stores. Called on WebSocket reconnection to\n\
  \    prevent stale phantom orders from blocking placement. "]
;;

let trigger_stale_order_cleanup ~reason () =
  Lwt.async (fun () ->
    Logging.debug_f ~section "Triggering stale order cleanup (reason=%s)" reason;
    cleanup_stale_orders ();
    Lwt.return_unit)
;;

let get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer
[@@inline always]
;;

let get_current_position_fast symbol =
  let store = get_symbol_store symbol in
  fun () -> RingBuffer.get_position store.events_buffer
[@@inline always]
;;

let read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos
[@@inline always]
;;

let iter_execution_events symbol last_pos f =
  let store = get_symbol_store symbol in
  RingBuffer.iter_since store.events_buffer last_pos f
[@@ocaml.doc
  " Iterate over execution events since last_pos without intermediate allocation. "]
[@@inline always]
;;

let has_execution_data symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.ready
[@@inline always]
;;

let has_execution_data_fast symbol =
  let store = get_symbol_store symbol in
  fun () -> Atomic.get store.ready
[@@inline always]
;;

let wait_for_execution_data symbols timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_execution_data symbols
    then Lwt.return_true
    else (
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0
      then Lwt.return_false
      else
        Lwt.pick
          [ (Lwt_condition.wait ready_condition >|= fun () -> `Again)
          ; (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
          ]
        >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_execution_data symbols))
  in
  loop ()
;;

let apply_index_action (action : [ `None | `Remove of string | `Add of string * string ]) =
  match action with
  | `Remove oid ->
    Mutex.lock order_index_mutex;
    retire_from_order_to_symbol oid;
    Mutex.unlock order_index_mutex
  | `Add (oid, sym) ->
    Mutex.lock order_index_mutex;
    add_to_order_to_symbol oid sym;
    Mutex.unlock order_index_mutex
  | `None -> ()
[@@ocaml.doc
  " Deferred global order_to_symbol index update: applied outside orders_mutex to prevent\n\
  \    nested locking with order_index_mutex. "]
;;

let update_orders_internal_locked ?user_ref store (event : execution_event) =
  let now = Unix.gettimeofday () in
  let is_terminal =
    match event.order_status with
    | FilledStatus | CanceledStatus | RejectedStatus | ExpiredStatus -> true
    | _ -> false
  in
  let existing_order = Hashtbl.find_opt store.open_orders event.order_id in
  let is_superseded =
    if is_terminal
    then false
    else (
      Mutex.lock amended_blacklist_mutex;
      let result = Hashtbl.mem amended_blacklist event.order_id in
      Mutex.unlock amended_blacklist_mutex;
      result)
  in
  if is_superseded
  then (
    Logging.debug_f
      ~section
      "Skipping late WS event for superseded order %s [%s]"
      event.order_id
      event.symbol;
    RingBuffer.write store.events_buffer event;
    notify_ready store;
    Concurrency.Exchange_wakeup.signal ~symbol:event.symbol;
    Concurrency.Oracle_wakeup.notify_exec ();
    `None)
  else (
    let index_action = ref `None in
    let changed = ref false in
    if is_terminal
    then (
      if Hashtbl.mem store.open_orders event.order_id
      then (
        Hashtbl.remove store.open_orders event.order_id;
        note_change store event.order_id None;
        changed := true);
      index_action := `Remove event.order_id)
    else (
      let recovered_user_ref =
        match user_ref with
        | Some _ -> user_ref
        | None ->
          (match existing_order with
           | Some o -> o.order_userref
           | None ->
             (match event.cl_ord_id with
              | Some clid ->
                (try
                   let clean_clid =
                     if String.starts_with ~prefix:"0x" clid
                     then String.sub clid 2 (String.length clid - 2)
                     else clid
                   in
                   if String.length clean_clid >= 16
                   then (
                     let last_part =
                       String.sub clean_clid (String.length clean_clid - 16) 16
                     in
                     let v = Int64.of_string ("0x" ^ last_part) in
                     let uref =
                       let tag = Int64.shift_right_logical v 56 in
                       if Int64.compare tag 0L > 0
                       then Int64.to_int tag
                       else Int64.to_int v
                     in
                     Logging.debug_f
                       ~section
                       "Recovered userref %d from cloid %s"
                       uref
                       clid;
                     Some uref)
                   else (
                     try Some (int_of_string clid) with
                     | _ -> None)
                 with
                 | _ ->
                   Logging.debug_f ~section "Failed to recover userref from cloid %s" clid;
                   None)
              | _ -> None))
      in
      let order : open_order =
        { order_id = event.order_id
        ; symbol = event.symbol
        ; side = event.side
        ; order_qty = event.order_qty
        ; cum_qty = event.cum_qty
        ; remaining_qty = event.order_qty -. event.cum_qty
        ; limit_price = event.limit_price
        ; avg_price = event.avg_price
        ; cum_cost = event.cum_cost
        ; order_status = event.order_status
        ; order_userref = recovered_user_ref
        ; cl_ord_id = event.cl_ord_id
        ; last_updated = now
        }
      in
      let differs =
        match existing_order with
        | Some o ->
          o.remaining_qty <> order.remaining_qty
          || o.order_status <> order.order_status
          || o.limit_price <> order.limit_price
          || o.order_qty <> order.order_qty
          || o.order_userref <> order.order_userref
        | None -> true
      in
      if differs
      then (
        Hashtbl.replace store.open_orders event.order_id order;
        note_change store event.order_id (Some order);
        changed := true);
      index_action := `Add (event.order_id, event.symbol));
    if !changed then publish_open_orders_cache store;
    RingBuffer.write store.events_buffer event;
    notify_ready store;
    Concurrency.Exchange_wakeup.signal ~symbol:event.symbol;
    Concurrency.Oracle_wakeup.notify_exec ();
    !index_action)
[@@ocaml.doc
  " Core order-state transition handler. Updates [open_orders], writes the event to the\n\
  \    ring buffer, and signals the domain. Handles terminal removal, amendment-blacklist\n\
  \    filtering, and userref recovery.\n\n\
  \    Caller must hold [store.orders_mutex]. The full read-modify-write (plus buffer \
   write\n\
  \    and wakeup) runs under the caller's lock, so a merged event computed from table \
   state\n\
  \    (userFills Trade path) is atomic. Returns a deferred [order_to_symbol] action to \
   apply\n\
  \    after unlock, avoiding nested orders_mutex -> order_index_mutex acquisition. "]
;;

let update_orders_internal ?user_ref store (event : execution_event) =
  Mutex.lock store.orders_mutex;
  let index_action =
    Fun.protect
      ~finally:(fun () -> Mutex.unlock store.orders_mutex)
      (fun () -> update_orders_internal_locked ?user_ref store event)
  in
  apply_index_action index_action
;;

let inject_order ~symbol ~order_id ~side ~qty ~price ?user_ref ?cl_ord_id () =
  let store = get_symbol_store symbol in
  let now = Unix.gettimeofday () in
  let event : execution_event =
    { order_id
    ; symbol
    ; exec_type = New
    ; order_status = NewStatus
    ; limit_price = Some price
    ; side
    ; order_qty = qty
    ; cum_qty = 0.0
    ; cum_cost = 0.0
    ; avg_price = 0.0
    ; timestamp = now
    ; venue_time = 0.0
    ; trade_id = None
    ; last_qty = None
    ; last_price = None
    ; fee = None
    ; cl_ord_id
    }
  in
  Mutex.lock store.orders_mutex;
  let index_action =
    Fun.protect
      ~finally:(fun () -> Mutex.unlock store.orders_mutex)
      (fun () ->
        match Hashtbl.find_opt store.open_orders order_id with
        | Some existing when existing.cum_qty > 0.0 ->
          Logging.debug_f
            ~section
            "Skipping proactive inject for %s: fill progress already tracked \
             (cum_qty=%.8f)"
            order_id
            existing.cum_qty;
          `None
        | _ -> update_orders_internal_locked ?user_ref store event)
  in
  apply_index_action index_action;
  Logging.debug_f
    ~section
    "Proactively injected open order: %s [%s] %s side %.8f limit_px=%.2f (cloid: %s, \
     userref: %s)"
    order_id
    symbol
    (if side = Buy then "buy" else "sell")
    qty
    price
    (Option.value cl_ord_id ~default:"none")
    (match user_ref with
     | Some u -> string_of_int u
     | None -> "none")
;;

let find_registered_symbol coin =
  let result = ref None in
  let exact = ref None in
  Mutex.lock initialization_mutex;
  Ds.Cow_table.iter
    (fun registered_symbol _ ->
      if String.starts_with ~prefix:(coin ^ "/") registered_symbol
      then result := Some registered_symbol
      else if registered_symbol = coin
      then exact := Some registered_symbol)
    stores;
  Mutex.unlock initialization_mutex;
  match !result with
  | Some symbol -> Some symbol
  | None ->
    (match !exact with
     | Some symbol -> Some symbol
     | None -> Hyperliquid_instruments_feed.resolve_symbol coin)
;;

let is_rejection_status s =
  let suffix = "Rejected" in
  let slen = String.length s
  and sfxlen = String.length suffix in
  slen > sfxlen && String.sub s (slen - sfxlen) sfxlen = suffix
[@@ocaml.doc
  " Detect Hyperliquid-specific rejection status strings. The exchange uses descriptive\n\
  \    suffixed variants (e.g. \"badAloPxRejected\", \"tickRejected\") rather than a \
   generic\n\
  \    \"rejected\" status. "]
;;

let process_order_updates data_json =
  let open Yojson.Safe.Util in
  match data_json with
  | `List orders ->
    List.iter
      (fun order_update ->
        try
          let order_obj = member "order" order_update in
          let coin = member "coin" order_obj |> to_string in
          let order_id =
            match member "oid" order_obj with
            | `Int i -> string_of_int i
            | `String s -> s
            | _ -> "0"
          in
          let symbol_opt =
            Mutex.lock order_index_mutex;
            let res = Concurrency.Fifo_index.find_opt order_to_symbol order_id in
            Mutex.unlock order_index_mutex;
            match res with
            | Some s -> Some s
            | None -> find_registered_symbol coin
          in
          match symbol_opt with
          | Some symbol ->
            let status = member "status" order_update |> to_string in
            let price =
              match member "limitPx" order_obj with
              | `String s -> float_of_string s
              | `Float f -> f
              | `Int i -> float_of_int i
              | _ -> 0.0
            in
            let qty =
              match member "origSz" order_obj with
              | `String s -> float_of_string s
              | `Float f -> f
              | `Int i -> float_of_int i
              | _ ->
                (match member "sz" order_obj with
                 | `String s -> float_of_string s
                 | `Float f -> f
                 | `Int i -> float_of_int i
                 | _ -> 0.0)
            in
            let side = if member "side" order_obj |> to_string = "B" then Buy else Sell in
            let cl_ord_id = member "cloid" order_obj |> to_string_option in
            let store = get_symbol_store symbol in
            let action_time =
              match member "timestamp" order_obj with
              | `Int i -> float_of_int i /. 1000.0
              | `Float f -> f /. 1000.0
              | `String s ->
                (try float_of_string s /. 1000.0 with
                 | _ -> Unix.gettimeofday ())
              | _ -> Unix.gettimeofday ()
            in
            let now = action_time in
            let order_status =
              match status with
              | "open" -> NewStatus
              | "filled" -> FilledStatus
              | "canceled" -> CanceledStatus
              | "rejected" -> RejectedStatus
              | "marginCanceled" -> CanceledStatus
              | s when is_rejection_status s -> RejectedStatus
              | s -> UnknownStatus s
            in
            let existing_order =
              Mutex.lock store.orders_mutex;
              let o = Hashtbl.find_opt store.open_orders order_id in
              Mutex.unlock store.orders_mutex;
              o
            in
            let is_amended =
              match existing_order with
              | Some prev ->
                let price_changed =
                  match prev.limit_price with
                  | Some p -> abs_float (p -. price) > 1e-12
                  | None -> price > 1e-12
                in
                let qty_changed = abs_float (prev.order_qty -. qty) > 1e-12 in
                price_changed || qty_changed || status = "amended" || status = "modified"
              | None -> status = "amended" || status = "modified"
            in
            let exec_type =
              match status with
              | "open" when is_amended -> Amended
              | "open" -> New
              | "amended" | "modified" -> Amended
              | "filled" -> Filled
              | "canceled" | "marginCanceled" -> Canceled
              | "rejected" -> Rejected
              | s when is_rejection_status s -> Rejected
              | _ -> Unknown status
            in
            let new_cum_qty =
              match status with
              | "filled" -> qty
              | _ ->
                (match existing_order with
                 | Some o -> o.cum_qty
                 | None -> 0.0)
            in
            let new_cum_cost =
              match status with
              | "filled" ->
                (match existing_order with
                 | Some o when o.cum_qty > 0.0 ->
                   let remaining = max 0.0 (qty -. o.cum_qty) in
                   o.cum_cost +. (remaining *. price)
                 | _ -> qty *. price)
              | _ ->
                (match existing_order with
                 | Some o -> o.cum_cost
                 | None -> 0.0)
            in
            let new_avg_price =
              match status with
              | "filled" ->
                (match existing_order with
                 | Some o when o.cum_qty > 0.0 ->
                   let remaining = max 0.0 (qty -. o.cum_qty) in
                   let filled_total = o.cum_qty +. remaining in
                   if filled_total > 0.0
                   then (o.cum_cost +. (remaining *. price)) /. filled_total
                   else price
                 | _ -> price)
              | _ ->
                (match existing_order with
                 | Some o -> o.avg_price
                 | None -> 0.0)
            in
            let event : execution_event =
              { order_id
              ; symbol
              ; exec_type
              ; order_status
              ; limit_price = Some price
              ; side
              ; order_qty = qty
              ; cum_qty = new_cum_qty
              ; cum_cost = new_cum_cost
              ; avg_price = new_avg_price
              ; timestamp = now
              ; venue_time = action_time
              ; trade_id = None
              ; last_qty = (if status = "filled" then Some qty else None)
              ; last_price = (if status = "filled" then Some price else None)
              ; fee = None
              ; cl_ord_id =
                  (match cl_ord_id with
                   | Some _ -> cl_ord_id
                   | None ->
                     (match existing_order with
                      | Some o -> o.cl_ord_id
                      | None -> None))
              }
            in
            let is_terminal_ou =
              match order_status with
              | FilledStatus | CanceledStatus | RejectedStatus | ExpiredStatus -> true
              | _ -> false
            in
            let is_already_terminal = is_terminal_ou && existing_order = None in
            if is_already_terminal
            then
              Logging.debug_f
                ~section
                "Skipping redundant %s event for %s [%s] (already processed via \
                 userFills)"
                status
                order_id
                symbol
            else (
              update_orders_internal store event;
              match order_status with
              | FilledStatus | RejectedStatus ->
                Logging.debug_f
                  ~section
                  "Order %s: %s [%s] (reason: %s)"
                  (String.uppercase_ascii status)
                  order_id
                  symbol
                  status
              | CanceledStatus ->
                Logging.debug_f
                  ~section
                  "Order %s: %s [%s] (reason: %s)"
                  (String.uppercase_ascii status)
                  order_id
                  symbol
                  status
              | NewStatus when is_amended ->
                Logging.info_f
                  ~section
                  "Order AMENDED: %s [%s] %.8f @ %.4f"
                  order_id
                  symbol
                  qty
                  price
              | NewStatus ->
                Logging.debug_f
                  ~section
                  "Order OPEN: %s [%s] %.8f @ %.2f"
                  order_id
                  symbol
                  qty
                  price
              | _ -> ())
          | None -> ()
        with
        | exn ->
          Logging.error_f
            ~section
            "Failed to process HL order update: %s | JSON: %s"
            (Printexc.to_string exn)
            (Yojson.Safe.to_string order_update))
      orders
  | _ -> ()
;;

let process_user_events data_json =
  let open Yojson.Safe.Util in
  let fills =
    try member "fills" data_json |> to_list with
    | _ -> []
  in
  List.iter
    (fun fill ->
      try
        let coin = member "coin" fill |> to_string in
        let order_id =
          match member "oid" fill with
          | `Int i -> string_of_int i
          | `String s -> s
          | _ -> "0"
        in
        let symbol_opt =
          Mutex.lock order_index_mutex;
          let res = Concurrency.Fifo_index.find_opt order_to_symbol order_id in
          Mutex.unlock order_index_mutex;
          match res with
          | Some s -> Some s
          | None -> find_registered_symbol coin
        in
        match symbol_opt with
        | Some symbol ->
          let price = member "px" fill |> to_string |> float_of_string in
          let size = member "sz" fill |> to_string |> float_of_string in
          let side = if member "side" fill |> to_string = "B" then Buy else Sell in
          let tid =
            match member "tid" fill with
            | `Int i -> Int64.of_int i
            | `String s -> Int64.of_string s
            | _ -> 0L
          in
          let fee =
            try member "fee" fill |> to_string |> float_of_string with
            | _ -> 0.0
          in
          let action_time =
            match member "time" fill with
            | `Int i -> float_of_int i /. 1000.0
            | `Float f -> f /. 1000.0
            | `String s ->
              (try float_of_string s /. 1000.0 with
               | _ -> Unix.gettimeofday ())
            | _ -> Unix.gettimeofday ()
          in
          let store = get_symbol_store symbol in
          let now = Unix.gettimeofday () in
          Mutex.lock store.tids_mutex;
          let already_processed = Hashtbl.mem store.processed_tids tid in
          if not already_processed
          then (
            Hashtbl.replace store.processed_tids tid now;
            Queue.push tid store.processed_tids_queue;
            while Hashtbl.length store.processed_tids > max_processed_tids do
              if Queue.is_empty store.processed_tids_queue
              then ignore (Hashtbl.length store.processed_tids)
              else (
                let oldest = Queue.pop store.processed_tids_queue in
                Hashtbl.remove store.processed_tids oldest)
            done);
          Mutex.unlock store.tids_mutex;
          if not already_processed
          then (
            let was_filled = ref false in
            let filled_out = ref 0.0 in
            let avg_out = ref 0.0 in
            let qty_out = ref 0.0 in
            Mutex.lock store.orders_mutex;
            let index_action =
              Fun.protect
                ~finally:(fun () -> Mutex.unlock store.orders_mutex)
                (fun () ->
                  let existing_order : open_order option =
                    Hashtbl.find_opt store.open_orders order_id
                  in
                  let cum_qty =
                    match existing_order with
                    | Some o -> o.cum_qty +. size
                    | None -> size
                  in
                  let order_qty =
                    match existing_order with
                    | Some o -> o.order_qty
                    | None -> size
                  in
                  let is_filled = cum_qty >= order_qty -. 1e-6 in
                  let status =
                    if is_filled then FilledStatus else PartiallyFilledStatus
                  in
                  let limit_price =
                    match existing_order with
                    | Some o -> o.limit_price
                    | None -> Some price
                  in
                  let cl_ord_id =
                    match existing_order with
                    | Some o -> o.cl_ord_id
                    | None -> None
                  in
                  let cum_cost =
                    match existing_order with
                    | Some o -> o.cum_cost +. (size *. price)
                    | None -> size *. price
                  in
                  let avg_price = if cum_qty > 0.0 then cum_cost /. cum_qty else price in
                  let event : execution_event =
                    { order_id
                    ; symbol
                    ; exec_type = Trade
                    ; order_status = status
                    ; limit_price
                    ; side
                    ; order_qty
                    ; cum_qty
                    ; cum_cost
                    ; avg_price
                    ; timestamp = action_time
                    ; venue_time = action_time
                    ; trade_id = Some tid
                    ; last_qty = Some size
                    ; last_price = Some price
                    ; fee = Some fee
                    ; cl_ord_id
                    }
                  in
                  Fun.protect
                    ~finally:(fun () ->
                      was_filled := is_filled;
                      filled_out := cum_qty;
                      avg_out := avg_price;
                      qty_out := order_qty)
                    (fun () -> update_orders_internal_locked store event))
            in
            apply_index_action index_action;
            if !was_filled
            then (
              if is_startup_snapshot_done ()
              then
                Logging.debug_f
                  ~section
                  "Order FILLED: %s [%s] %.8f @ %.2f (trade_id: %Ld)"
                  order_id
                  symbol
                  size
                  price
                  tid
              else
                Logging.debug_f
                  ~section
                  "Order FILLED (startup snapshot): %s [%s] %.8f @ %.2f (trade_id: %Ld)"
                  order_id
                  symbol
                  size
                  price
                  tid;
              let fill_value = !filled_out *. !avg_out in
              let maker_fee_rate =
                match Dio_exchange.Exchange_intf.Registry.get "hyperliquid" with
                | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
                  (match Ex.get_fees ~symbol with
                   | Some f, _ -> f
                   | _ -> 0.0)
                | None -> 0.0
              in
              let estimated_fee = fill_value *. maker_fee_rate in
              Concurrency.Fill_event_bus.publish_fill
                { venue = "hyperliquid"
                ; symbol
                ; side = (if side = Buy then "buy" else "sell")
                ; amount = !filled_out
                ; fill_price = !avg_out
                ; value = fill_value
                ; fee = estimated_fee
                ; timestamp = action_time
                ; order_id
                ; trade_id = Int64.to_string tid
                })
            else if is_startup_snapshot_done ()
            then
              Logging.debug_f
                ~section
                "Order PARTIALLY FILLED: %s [%s] %.8f @ %.2f (filled: %.8f/%.8f)"
                order_id
                symbol
                size
                price
                !filled_out
                !qty_out
            else
              Logging.debug_f
                ~section
                "Order PARTIALLY FILLED (startup snapshot): %s [%s] %.8f @ %.2f (filled: \
                 %.8f/%.8f)"
                order_id
                symbol
                size
                price
                !filled_out
                !qty_out)
        | None -> ()
      with
      | exn ->
        Logging.error_f
          ~section
          "Failed to process HL fill: %s | JSON: %s"
          (Printexc.to_string exn)
          (Yojson.Safe.to_string fill))
    fills;
  let non_user_cancels =
    try member "nonUserCancel" data_json |> to_list with
    | _ -> []
  in
  List.iter
    (fun nuc ->
      let coin = member "coin" nuc |> to_string in
      let order_id =
        match member "oid" nuc with
        | `Int i -> string_of_int i
        | `String s -> s
        | _ -> "0"
      in
      let symbol_opt =
        Mutex.lock order_index_mutex;
        let res = Concurrency.Fifo_index.find_opt order_to_symbol order_id in
        Mutex.unlock order_index_mutex;
        match res with
        | Some s -> Some s
        | None -> find_registered_symbol coin
      in
      match symbol_opt with
      | Some symbol ->
        let store = get_symbol_store symbol in
        let now = Unix.gettimeofday () in
        Mutex.lock store.orders_mutex;
        let existing_opt = Hashtbl.find_opt store.open_orders order_id in
        Mutex.unlock store.orders_mutex;
        (match existing_opt with
         | Some current_order ->
           let event : execution_event =
             { order_id
             ; symbol
             ; exec_type = Canceled
             ; order_status = CanceledStatus
             ; limit_price = current_order.limit_price
             ; side = current_order.side
             ; order_qty = current_order.order_qty
             ; cum_qty = current_order.cum_qty
             ; cum_cost = current_order.cum_cost
             ; avg_price = current_order.avg_price
             ; timestamp = now
             ; venue_time = 0.0
             ; trade_id = None
             ; last_qty = None
             ; last_price = None
             ; fee = None
             ; cl_ord_id = current_order.cl_ord_id
             }
           in
           update_orders_internal store event;
           Logging.info_f ~section "Order NON-USER CANCEL: %s [%s]" order_id symbol
         | None -> ())
      | None -> ())
    non_user_cancels
;;

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  try
    match channel with
    | Some "orderUpdates" ->
      let data = member "data" json in
      process_order_updates data
    | Some "userEvents" | Some "userFills" ->
      let data = member "data" json in
      process_user_events data
    | Some "webData2" -> ()
    | _ -> ()
  with
  | exn ->
    Logging.error_f
      ~section
      "Failed to process Hyperliquid executions data: %s"
      (Printexc.to_string exn)
;;

let cleanup_mvar : unit Lwt_mvar.t = Lwt_mvar.create_empty ()
[@@ocaml.doc
  " Cleanup signal channel: fires on demand (reconnect, manual trigger) or a 120s safety\n\
  \    timer. One-shot Lwt_mvar; [request_cleanup] is idempotent. "]
;;

let request_cleanup () =
  if Lwt_mvar.is_empty cleanup_mvar
  then Lwt.async (fun () -> Lwt_mvar.put cleanup_mvar ())
[@@ocaml.doc
  " Signal the cleanup loop to run immediately. Idempotent: skipped if a signal is already\n\
  \    pending. Uses an is_empty guard to avoid queuing Lwt continuations; the put only\n\
  \    occurs when the mvar is empty (previous signal consumed). "]
;;

let periodic_tasks_started = Atomic.make false

let start_periodic_tasks () =
  if not (Atomic.exchange periodic_tasks_started true)
  then (
    let rec run () =
      Lwt.pick
        [ (Lwt_mvar.take cleanup_mvar >|= fun () -> `Signal)
        ; (Lwt_unix.sleep 120.0 >|= fun () -> `Timeout)
        ]
      >>= fun reason ->
      Logging.debug_f
        ~section
        "Running HL executions cleanup (%s)"
        (match reason with
         | `Signal -> "requested"
         | `Timeout -> "120s fallback");
      cleanup_stale_orders ();
      Lwt.async run;
      Lwt.return_unit
    in
    Lwt.async run)
;;

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_user_data () in
    Lwt.catch
      (fun () ->
        Logging.debug_f ~section "Starting Hyperliquid executions processor task";
        let __ppx_lwt_0 =
          Concurrency.Lwt_util.consume_stream process_market_data sub.stream
        in
        Lwt.backtrace_bind
          "src/integrations/venues/hyperliquid/hyperliquid_executions_feed.ml"
          1404
          (fun exn ->
            try Lwt.reraise exn with
            | exn -> exn)
          __ppx_lwt_0
          (fun () ->
            sub.close ();
            Logging.debug
              ~section
              "Executions stream ended (disconnect), re-subscribing...";
            Lwt.async run;
            Lwt.return_unit))
      (fun exn ->
        sub.close ();
        Logging.error_f
          ~section
          "Hyperliquid executions processor task crashed: %s. Re-subscribing..."
          (Printexc.to_string exn);
        Lwt.async run;
        Lwt.return_unit)
  in
  Lwt.async run
;;

let initialize symbols =
  start_periodic_tasks ();
  Logging.debug ~section "Initializing Hyperliquid executions feed";
  List.iter
    (fun symbol ->
      let _ = get_symbol_store symbol in
      Logging.debug_f ~section "Created Hyperliquid executions buffer for %s" symbol)
    symbols
;;

let inject_open_orders data_json =
  let open Yojson.Safe.Util in
  try
    let orders = to_list data_json in
    let count = ref 0 in
    let snapshot_order_ids = Hashtbl.create 32 in
    List.iter
      (fun order_obj ->
        try
          let coin = member "coin" order_obj |> to_string in
          let order_id =
            match member "oid" order_obj with
            | `Int i -> string_of_int i
            | `String s -> s
            | _ -> "0"
          in
          Hashtbl.replace snapshot_order_ids order_id ();
          let symbol_opt = find_registered_symbol coin in
          match symbol_opt with
          | Some symbol ->
            let price =
              match member "limitPx" order_obj with
              | `String s -> float_of_string s
              | `Float f -> f
              | `Int i -> float_of_int i
              | _ -> 0.0
            in
            let qty =
              match member "origSz" order_obj with
              | `String s -> float_of_string s
              | `Float f -> f
              | `Int i -> float_of_int i
              | _ ->
                (match member "sz" order_obj with
                 | `String s -> float_of_string s
                 | `Float f -> f
                 | `Int i -> float_of_int i
                 | _ -> 0.0)
            in
            let side = if member "side" order_obj |> to_string = "B" then Buy else Sell in
            let cl_ord_id = member "cloid" order_obj |> to_string_option in
            let timestamp_ms =
              match member "timestamp" order_obj with
              | `Int i -> float_of_int i
              | `Float f -> f
              | `String s -> float_of_string s
              | _ -> Unix.gettimeofday () *. 1000.0
            in
            let store = get_symbol_store symbol in
            let event : execution_event =
              { order_id
              ; symbol
              ; exec_type = New
              ; order_status = NewStatus
              ; limit_price = Some price
              ; side
              ; order_qty = qty
              ; cum_qty = 0.0
              ; cum_cost = 0.0
              ; avg_price = 0.0
              ; timestamp = timestamp_ms /. 1000.0
              ; venue_time = timestamp_ms /. 1000.0
              ; trade_id = None
              ; last_qty = None
              ; last_price = None
              ; fee = None
              ; cl_ord_id
              }
            in
            update_orders_internal store event;
            incr count;
            Logging.debug_f
              ~section
              "Injected startup open order: %s [%s] %s %.8f @ %.2f"
              order_id
              symbol
              (if side = Buy then "buy" else "sell")
              qty
              price
          | None -> ()
        with
        | exn ->
          Logging.warn_f
            ~section
            "Failed to parse open order entry: %s"
            (Printexc.to_string exn))
      orders;
    Logging.debug_f ~section "Injected %d initial open orders from snapshot" !count;
    let stale_orders = ref [] in
    Mutex.lock initialization_mutex;
    let all_symbols =
      Ds.Cow_table.fold (fun symbol _ acc -> symbol :: acc) stores ~init:[]
    in
    Mutex.unlock initialization_mutex;
    List.iter
      (fun symbol ->
        let store = get_symbol_store symbol in
        Mutex.lock store.orders_mutex;
        Fun.protect
          ~finally:(fun () -> Mutex.unlock store.orders_mutex)
          (fun () ->
            Hashtbl.iter
              (fun order_id (cached_order : open_order) ->
                if not (Hashtbl.mem snapshot_order_ids order_id)
                then stale_orders := (symbol, store, cached_order) :: !stale_orders)
              store.open_orders))
      all_symbols;
    List.iter
      (fun (symbol, store, (cached_order : open_order)) ->
        let event : execution_event =
          { order_id = cached_order.order_id
          ; symbol
          ; exec_type = Canceled
          ; order_status = CanceledStatus
          ; limit_price = cached_order.limit_price
          ; side = cached_order.side
          ; order_qty = cached_order.order_qty
          ; cum_qty = cached_order.cum_qty
          ; cum_cost = cached_order.cum_cost
          ; avg_price = cached_order.avg_price
          ; timestamp = Unix.gettimeofday ()
          ; venue_time = 0.0
          ; trade_id = None
          ; last_qty = None
          ; last_price = None
          ; fee = None
          ; cl_ord_id = cached_order.cl_ord_id
          }
        in
        update_orders_internal store event;
        Logging.debug_f
          ~section
          "Reconciled stale order: emitted CanceledStatus for %s [%s]"
          cached_order.order_id
          symbol)
      !stale_orders;
    if !stale_orders <> []
    then
      Logging.debug_f
        ~section
        "Reconciled open orders: removed %d stale orders not present in snapshot"
        (List.length !stale_orders);
    Mutex.lock order_index_mutex;
    mark_startup_complete ();
    Mutex.unlock order_index_mutex
  with
  | exn ->
    Logging.error_f ~section "Failed to inject open orders: %s" (Printexc.to_string exn)
;;
