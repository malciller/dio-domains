(** Hyperliquid Executions Feed *)

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

type side = Buy | Sell
type order_status = PendingNewStatus | NewStatus | PartiallyFilledStatus | FilledStatus | CanceledStatus | ExpiredStatus | RejectedStatus | UnknownStatus of string

type open_order = {
  order_id: string; 
  symbol: string; 
  side: side; 
  order_qty: float; 
  cum_qty: float;
  remaining_qty: float; 
  limit_price: float option; 
  avg_price: float;
  cum_cost: float;
  order_status: order_status;
  order_userref: int option; 
  cl_ord_id: string option;
  last_updated: float;
}

type execution_event = {
  order_id: string; 
  symbol: string;
  exec_type: exec_type;
  order_status: order_status; 
  limit_price: float option; 
  side: side;
  order_qty: float; 
  cum_qty: float; 
  cum_cost: float;
  avg_price: float;
  timestamp: float;
  trade_id: int64 option;
  last_qty: float option;
  last_price: float option;
  fee: float option;
  cl_ord_id: string option;
}



(** Lock-free ring buffer for execution data *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Maximum number of trade IDs retained per symbol for deduplication.
    Oldest entries are evicted FIFO when the cap is reached. *)
let max_processed_tids = 256

(** Per-symbol execution storage with readiness signalling *)
type store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  processed_tids: (int64, float) Hashtbl.t; (* trade_id -> arrival_time for deduplication *)
  processed_tids_queue: int64 Queue.t; (* FIFO eviction order *)
  orders_mutex: Mutex.t;
  tids_mutex: Mutex.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Global order_id -> symbol mapping with adaptive startup cap + FIFO eviction.
    Must be accessed under initialization_mutex. *)
let order_to_symbol : (string, string) Hashtbl.t = Hashtbl.create 16
(** FIFO insertion queue for oldest-entry eviction *)
let order_to_symbol_queue : string Queue.t = Queue.create ()
(** Mutable cap: max_int during startup (uncapped), locked after first snapshot *)
let order_to_symbol_cap : int ref = ref max_int
let order_to_symbol_startup_done = Atomic.make false

(** Set once after inject_open_orders completes — replaces the 2s wall-clock gate
    in domain_spawner with a real event. Domains poll this flag in their cycle;
    Exchange_wakeup.signal() is called so sleeping domains wake immediately. *)
let _startup_snapshot_done : bool Atomic.t = Atomic.make false
let set_startup_snapshot_done () =
  if not (Atomic.exchange _startup_snapshot_done true) then begin
    Logging.info ~section "HL open-order snapshot injected — domains may now activate";
    Concurrency.Exchange_wakeup.signal ()
  end
let is_startup_snapshot_done () = Atomic.get _startup_snapshot_done

(** Add order_id->symbol, evicting the oldest entry when over cap.
    Ring-buffer semantics: newest always wins, oldest always goes.
    Must be called while holding initialization_mutex. *)
let add_to_order_to_symbol order_id symbol =
  if not (Hashtbl.mem order_to_symbol order_id) then
    Queue.push order_id order_to_symbol_queue;
  Hashtbl.replace order_to_symbol order_id symbol;
  (* Enforce cap only after startup snapshot is complete *)
  if Atomic.get order_to_symbol_startup_done then
    while Hashtbl.length order_to_symbol > !order_to_symbol_cap do
      if Queue.is_empty order_to_symbol_queue then
        (* Queue and table drifted — just accept current length as new cap *)
        order_to_symbol_cap := Hashtbl.length order_to_symbol
      else begin
        let oldest = Queue.pop order_to_symbol_queue in
        Hashtbl.remove order_to_symbol oldest
      end
    done

(** Lock the adaptive cap after the startup snapshot is fully consumed.
    Cap = observed size rounded up by 50%%, minimum 32.
    Called exactly once; subsequent calls are no-ops. *)
let mark_startup_complete () =
  if not (Atomic.exchange order_to_symbol_startup_done true) then begin
    (* initialization_mutex is already held by the caller (inject_open_orders) *)
    let observed = Hashtbl.length order_to_symbol in
    let cap = max 32 (observed + observed / 2 + 1) in
    order_to_symbol_cap := cap;
    Logging.info_f ~section
      "order_to_symbol adaptive cap locked at %d (observed %d entries at startup)" cap observed
  end

(** Blacklist of order IDs removed by cancel-replace amendments.
    Prevents late WS orderUpdates 'open' events from re-adding
    orders that were already superseded by a newer cancel-replace. *)
let amended_blacklist : (string, float) Hashtbl.t = Hashtbl.create 16
let amended_blacklist_mutex = Mutex.create ()

(** Get or create store for a symbol - thread-safe initialization *)
let get_symbol_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      let store = match Hashtbl.find_opt stores symbol with
        | Some store -> store
        | None ->
            let store = {
              events_buffer = RingBuffer.create 128;
              open_orders = Hashtbl.create 32;
              ready = Atomic.make false;
              processed_tids = Hashtbl.create 32;
              processed_tids_queue = Queue.create ();
              orders_mutex = Mutex.create ();
              tids_mutex = Mutex.create ();
            } in
            Hashtbl.add stores symbol store;
            store
      in
      Mutex.unlock initialization_mutex;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let order = Hashtbl.find_opt store.open_orders order_id in
  Mutex.unlock store.orders_mutex;
  order

let find_order_everywhere order_id =
  (* Use order_to_symbol index for O(1) lookup instead of iterating all stores *)
  Mutex.lock initialization_mutex;
  let symbol_opt = Hashtbl.find_opt order_to_symbol order_id in
  Mutex.unlock initialization_mutex;
  match symbol_opt with
  | Some symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      let order = Hashtbl.find_opt store.open_orders order_id in
      Mutex.unlock store.orders_mutex;
      order
  | None -> None

let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let orders = Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders [] in
  Mutex.unlock store.orders_mutex;
  orders

(** Remove a single open order by ID — used to clean up the old order entry
    after a successful cancel-replace amendment so it doesn't appear as a
    ghost duplicate in get_open_orders. *)
let remove_open_order ~symbol ~order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let existed = Hashtbl.mem store.open_orders order_id in
  if existed then begin
    Hashtbl.remove store.open_orders order_id;
    Mutex.lock initialization_mutex;
    Hashtbl.remove order_to_symbol order_id;
    Mutex.unlock initialization_mutex;
    (* Blacklist this ID so late WS orderUpdates events don't re-add it *)
    Mutex.lock amended_blacklist_mutex;
    Hashtbl.replace amended_blacklist order_id (Unix.gettimeofday ());
    Mutex.unlock amended_blacklist_mutex;
    Logging.debug_f ~section "Removed old order %s [%s] after amendment (blacklisted)" order_id symbol
  end;
  Mutex.unlock store.orders_mutex

(** Get all symbols that have execution stores (initialized symbols) *)
let get_all_symbols () =
  Mutex.lock initialization_mutex;
  let symbols = Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores [] in
  Mutex.unlock initialization_mutex;
  symbols

(** Safety cleanup for stale orders - matches Kraken's cleanup_stale_orders pattern *)
let cleanup_stale_orders () =
  let now = Unix.gettimeofday () in
  let stale_threshold = 24.0 *. 3600.0 in (* 24 hours *)
  let stale_orders = ref [] in
  
  let all_symbols = get_all_symbols () in
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    Hashtbl.iter (fun order_id (order : open_order) ->
      if now -. order.last_updated > stale_threshold then
        stale_orders := (symbol, order_id) :: !stale_orders
    ) store.open_orders;
    Mutex.unlock store.orders_mutex;
  ) all_symbols;
  
  let removed_count = List.length !stale_orders in
  if removed_count > 0 then begin
    Logging.info_f ~section "Safety cleanup: removing %d orders older than 24h" removed_count;
    
    List.iter (fun (_symbol, order_id) ->
      let store = get_symbol_store _symbol in
      Mutex.lock store.orders_mutex;
      if Hashtbl.mem store.open_orders order_id then begin
        Hashtbl.remove store.open_orders order_id;
        Mutex.lock initialization_mutex;
        Hashtbl.remove order_to_symbol order_id;
        Mutex.unlock initialization_mutex;
        Logging.debug_f ~section "Removed stale order during safety cleanup: %s [%s]" order_id _symbol
      end;
      Mutex.unlock store.orders_mutex;
    ) !stale_orders
  end;

  (* Clean up stale amendment blacklist entries (>30s old) *)
  Mutex.lock amended_blacklist_mutex;
  let blacklist_to_remove = ref [] in
  Hashtbl.iter (fun order_id timestamp ->
    if now -. timestamp > 30.0 then
      blacklist_to_remove := order_id :: !blacklist_to_remove
  ) amended_blacklist;
  List.iter (Hashtbl.remove amended_blacklist) !blacklist_to_remove;
  let bl_removed = List.length !blacklist_to_remove in
  Mutex.unlock amended_blacklist_mutex;
  if bl_removed > 0 then
    Logging.debug_f ~section "Cleaned up %d stale amendment blacklist entries" bl_removed;

  (* Clean up stale processed_tids (>10min old) across all stores *)
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.tids_mutex;
    let tids_to_remove = ref [] in
    Hashtbl.iter (fun tid arrival_time ->
      if now -. arrival_time > 600.0 then tids_to_remove := tid :: !tids_to_remove
    ) store.processed_tids;
    List.iter (Hashtbl.remove store.processed_tids) !tids_to_remove;
    let tids_removed = List.length !tids_to_remove in
    Mutex.unlock store.tids_mutex;
    if tids_removed > 0 then
      Logging.debug_f ~section "Cleaned %d stale processed_tids for %s" tids_removed symbol
  ) all_symbols;

  (* Purge orphaned entries from order_to_symbol_queue.
     Terminal events (fill/cancel) remove order_ids from the Hashtbl via Hashtbl.remove
     but do NOT remove them from the Queue (OCaml Queue has no O(1) remove by value).
     Over time the queue accumulates one dead string entry per completed order — these
     are never drained because the eviction while-loop only fires when the Hashtbl
     exceeds its cap (which it doesn't, since terminals keep it small).
     Fix: during each cleanup cycle, rebuild the queue keeping only entries still
     present in the Hashtbl.  This is O(queue_length) but runs at most every 120s. *)
  Mutex.lock initialization_mutex;
  let original_queue_len = Queue.length order_to_symbol_queue in
  if original_queue_len > 0 then begin
    let temp = Queue.create () in
    Queue.iter (fun order_id ->
      if Hashtbl.mem order_to_symbol order_id then
        Queue.push order_id temp
    ) order_to_symbol_queue;
    Queue.clear order_to_symbol_queue;
    Queue.transfer temp order_to_symbol_queue;
    let removed = original_queue_len - Queue.length order_to_symbol_queue in
    if removed > 0 then
      Logging.debug_f ~section
        "Purged %d orphaned entries from order_to_symbol_queue (was %d, now %d)"
        removed original_queue_len (Queue.length order_to_symbol_queue)
  end;
  Mutex.unlock initialization_mutex

(** Clear all open orders across all stores - used on WebSocket reconnection
    to prevent stale phantom orders from blocking new order placement. *)
let clear_all_open_orders () =
  let all_symbols = get_all_symbols () in
  let total_removed = ref 0 in
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    let count = Hashtbl.length store.open_orders in
    total_removed := !total_removed + count;
    Hashtbl.clear store.open_orders;
    Mutex.unlock store.orders_mutex;
  ) all_symbols;
  (* Clear global index and eviction queue *)
  Mutex.lock initialization_mutex;
  Hashtbl.clear order_to_symbol;
  Queue.clear order_to_symbol_queue;
  Mutex.unlock initialization_mutex;
  if !total_removed > 0 then
    Logging.info_f ~section "Cleared %d stale open orders on reconnection" !total_removed
  else
    Logging.info ~section "No open orders to clear on reconnection"

let trigger_stale_order_cleanup ~reason () =
  Lwt.async (fun () ->
    Logging.debug_f ~section "Triggering stale order cleanup (reason=%s)" reason;
    cleanup_stale_orders ();
    Lwt.return_unit
  )

let[@inline always] get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer

let[@inline always] read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos

let[@inline always] has_execution_data symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.ready

let wait_for_execution_data symbols timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_execution_data symbols then
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
        | `Timeout -> Lwt.return (List.for_all has_execution_data symbols)
  in
  loop ()

(** Internal helper to update open orders and generate an event - adopts Kraken's "update_open_orders" pattern *)
let update_orders_internal ?user_ref store (event : execution_event) =
  let now = Unix.gettimeofday () in
  Mutex.lock store.orders_mutex;
  
  let is_terminal = match event.order_status with
    | FilledStatus | CanceledStatus | RejectedStatus | ExpiredStatus -> true
    | _ -> false
  in
  
  let existing_order = Hashtbl.find_opt store.open_orders event.order_id in

  (* Check if this non-terminal order was superseded by a cancel-replace amendment.
     If so, skip re-adding it — a late WS orderUpdates event would create a
     phantom duplicate that triggers the multi-buy cancel branch.
     Terminal events always process (to clean up tracking). *)
  let is_superseded = if is_terminal then false else begin
    Mutex.lock amended_blacklist_mutex;
    let result = Hashtbl.mem amended_blacklist event.order_id in
    Mutex.unlock amended_blacklist_mutex;
    result
  end in

  if is_superseded then begin
    Mutex.unlock store.orders_mutex;
    Logging.debug_f ~section "Skipping late WS event for superseded order %s [%s]" event.order_id event.symbol;
    (* Still write to ring buffer so exec event consumers see the event,
       but do NOT add to open_orders hashtable *)
    RingBuffer.write store.events_buffer event;
    notify_ready store
  end else begin

  if is_terminal then begin
    Hashtbl.remove store.open_orders event.order_id;
    (* Remove from global index *)
    Mutex.lock initialization_mutex;
    Hashtbl.remove order_to_symbol event.order_id;
    Mutex.unlock initialization_mutex;
  end else begin
    (* UserRef Recovery Logic:
       1. Use explicitly provided user_ref (from proactive inject_order)
       2. Use existing tracked user_ref
       3. Recover from cloid string (Hyperliquid encodes UserRef in cloid hex)
    *)
    let recovered_user_ref = match user_ref with
      | Some _ -> user_ref
      | None -> 
          (match existing_order with 
           | Some o -> o.order_userref 
           | None -> (match event.cl_ord_id with
               | Some clid ->
                   (try
                      let clean_clid = if String.starts_with ~prefix:"0x" clid then 
                        String.sub clid 2 (String.length clid - 2)
                      else clid in
                      
                      if String.length clean_clid >= 16 then
                        let last_part = String.sub clean_clid (String.length clean_clid - 16) 16 in
                        let uref = Int64.to_int (Int64.of_string ("0x" ^ last_part)) in
                        Logging.debug_f ~section "Recovered userref %d from cloid %s" uref clid;
                        Some uref
                      else
                        (try Some (int_of_string clid) with _ -> None)
                    with _ -> 
                      Logging.debug_f ~section "Failed to recover userref from cloid %s" clid;
                      None)
               | _ -> None))
    in
    
    let order : open_order = {
      order_id = event.order_id;
      symbol = event.symbol;
      side = event.side;
      order_qty = event.order_qty;
      cum_qty = event.cum_qty;
      remaining_qty = event.order_qty -. event.cum_qty;
      limit_price = event.limit_price;
      avg_price = event.avg_price;
      cum_cost = event.cum_cost;
      order_status = event.order_status;
      order_userref = recovered_user_ref;
      cl_ord_id = event.cl_ord_id;
      last_updated = now;
    } in
    
    Hashtbl.replace store.open_orders event.order_id order;
    (* Add to global index with FIFO eviction when over adaptive cap *)
    Mutex.lock initialization_mutex;
    add_to_order_to_symbol event.order_id event.symbol;
    Mutex.unlock initialization_mutex;
  end;
  Mutex.unlock store.orders_mutex;
  
  RingBuffer.write store.events_buffer event;
  notify_ready store

  end

let inject_order ~symbol ~order_id ~side ~qty ~price ?user_ref ?cl_ord_id () =
  let store = get_symbol_store symbol in
  let now = Unix.gettimeofday () in
  let event : execution_event = {
    order_id;
    symbol;
    exec_type = New;
    order_status = NewStatus;
    limit_price = Some price;
    side;
    order_qty = qty;
    cum_qty = 0.0;
    cum_cost = 0.0;
    avg_price = 0.0;
    timestamp = now;
    trade_id = None;
    last_qty = None;
    last_price = None;
    fee = None;
    cl_ord_id;
  } in
  
  update_orders_internal ?user_ref store event;
  Logging.debug_f ~section "Proactively injected open order: %s [%s] %s side %.8f limit_px=%.2f (cloid: %s, userref: %s)"
    order_id symbol (if side = Buy then "buy" else "sell") qty price
    (Option.value cl_ord_id ~default:"none")
    (match user_ref with Some u -> string_of_int u | None -> "none")

let find_registered_symbol coin =
  match Hyperliquid_instruments_feed.resolve_symbol coin with
  | Some symbol -> 
      Mutex.lock initialization_mutex;
      let res = if Hashtbl.mem stores symbol then Some symbol else None in
      Mutex.unlock initialization_mutex;
      res
  | None ->
      let result = ref None in
      Mutex.lock initialization_mutex;
      Hashtbl.iter (fun registered_symbol _ ->
        if registered_symbol = coin then result := Some registered_symbol
        else if String.starts_with ~prefix:(coin ^ "/") registered_symbol then result := Some registered_symbol
      ) stores;
      Mutex.unlock initialization_mutex;
      !result

(** Detect Hyperliquid-specific rejection status strings.
    HL uses statuses like "badAloPxRejected", "insufficientSpotBalanceRejected",
    "tickRejected", "perpMarginRejected", etc. instead of a generic "rejected". *)
let is_rejection_status s =
  let suffix = "Rejected" in
  let slen = String.length s and sfxlen = String.length suffix in
  slen > sfxlen && String.sub s (slen - sfxlen) sfxlen = suffix

let process_order_updates data_json =
  let open Yojson.Safe.Util in
  match data_json with
  | `List orders ->
      List.iter (fun order_update ->
        let order_obj = member "order" order_update in
        let coin = member "coin" order_obj |> to_string in
        let order_id = (match member "oid" order_obj with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
        
        let symbol_opt = 
          Mutex.lock initialization_mutex;
          let res = Hashtbl.find_opt order_to_symbol order_id in
          Mutex.unlock initialization_mutex;
          match res with
          | Some s -> Some s
          | None -> find_registered_symbol coin
        in
        
        match symbol_opt with
        | Some symbol ->
            let status = member "status" order_update |> to_string in
            let price = (match member "limitPx" order_obj with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0) in
            let qty = 
              match member "origSz" order_obj with 
              | `String s -> float_of_string s 
              | `Float f -> f 
              | `Int i -> float_of_int i 
              | _ -> (match member "sz" order_obj with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0)
            in
            let side = if (member "side" order_obj |> to_string) = "B" then Buy else Sell in
            let cl_ord_id = member "cloid" order_obj |> to_string_option in
            
            let store = get_symbol_store symbol in
            let now = Unix.gettimeofday () in
            
            let order_status = match status with
              | "open" -> NewStatus
              | "filled" -> FilledStatus
              | "canceled" -> CanceledStatus
              | "rejected" -> RejectedStatus
              | "marginCanceled" -> CanceledStatus
              | s when is_rejection_status s -> RejectedStatus
              | s -> UnknownStatus s
            in
            
            let exec_type = match status with
              | "open" -> New
              | "filled" -> Filled
              | "canceled" | "marginCanceled" -> Canceled
              | "rejected" -> Rejected
              | s when is_rejection_status s -> Rejected
              | _ -> Unknown status
            in
            
            let existing_order = 
              Mutex.lock store.orders_mutex;
              let o = Hashtbl.find_opt store.open_orders order_id in
              Mutex.unlock store.orders_mutex;
              o
            in
            
            let new_cum_qty = match status with
              | "filled" -> qty
              | _ -> (match existing_order with Some o -> o.cum_qty | None -> 0.0)
            in
            
            let new_cum_cost = match status with
              | "filled" -> qty *. price
              | _ -> (match existing_order with Some o -> o.cum_cost | None -> 0.0)
            in
            
            let new_avg_price = match status with
              | "filled" -> price
              | _ -> (match existing_order with Some o -> o.avg_price | None -> 0.0)
            in
            
            let event : execution_event = {
              order_id;
              symbol;
              exec_type;
              order_status;
              limit_price = Some price;
              side;
              order_qty = qty;
              cum_qty = new_cum_qty;
              cum_cost = new_cum_cost;
              avg_price = new_avg_price;
              timestamp = now;
              trade_id = None;
              last_qty = (if status = "filled" then Some qty else None);
              last_price = (if status = "filled" then Some price else None);
              fee = None;
              cl_ord_id = (match cl_ord_id with Some _ -> cl_ord_id | None -> (match existing_order with Some o -> o.cl_ord_id | None -> None));
            } in
            
            update_orders_internal store event;
            
            (match order_status with
             | FilledStatus | RejectedStatus ->
                 Logging.debug_f ~section "Order %s: %s [%s] (reason: %s)" (String.uppercase_ascii status) order_id symbol status
             | CanceledStatus ->
                 Logging.debug_f ~section "Order %s: %s [%s] (reason: %s)" (String.uppercase_ascii status) order_id symbol status
             | NewStatus ->
                 Logging.debug_f ~section "Order OPEN: %s [%s] %.8f @ %.2f" order_id symbol qty price
             | _ -> ());
        | None -> ()
      ) orders
  | _ -> ()

let process_user_events data_json =
  let open Yojson.Safe.Util in
  (* Handle fills *)
  let fills = try member "fills" data_json |> to_list with _ -> [] in
  List.iter (fun fill ->
    let coin = member "coin" fill |> to_string in
    let order_id = (match member "oid" fill with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
    
    let symbol_opt = 
      Mutex.lock initialization_mutex;
      let res = Hashtbl.find_opt order_to_symbol order_id in
      Mutex.unlock initialization_mutex;
      match res with
      | Some s -> Some s
      | None -> find_registered_symbol coin
    in
    
    match symbol_opt with
    | Some symbol ->
        let price = member "px" fill |> to_string |> float_of_string in
        let size = member "sz" fill |> to_string |> float_of_string in
        let side = if member "side" fill |> to_string = "B" then Buy else Sell in
        let tid = (match member "tid" fill with `Int i -> Int64.of_int i | `String s -> Int64.of_string s | _ -> 0L) in
        let fee = try member "fee" fill |> to_string |> float_of_string with _ -> 0.0 in
        let store = get_symbol_store symbol in
        
        (* Deduplicate trade fills *)
        let now = Unix.gettimeofday () in
        Mutex.lock store.tids_mutex;
        let already_processed = Hashtbl.mem store.processed_tids tid in
        if not already_processed then begin
          Hashtbl.replace store.processed_tids tid now;
          Queue.push tid store.processed_tids_queue;
          (* FIFO eviction when over cap *)
          while Hashtbl.length store.processed_tids > max_processed_tids do
            if Queue.is_empty store.processed_tids_queue then
              ignore (Hashtbl.length store.processed_tids) (* queue drifted, let timer cleanup handle *)
            else begin
              let oldest = Queue.pop store.processed_tids_queue in
              Hashtbl.remove store.processed_tids oldest
            end
          done
        end;
        Mutex.unlock store.tids_mutex;

        if not already_processed then begin
          (* Hold lock for entire read-compute-write cycle to prevent double-counted fills *)
          Mutex.lock store.orders_mutex;
          let (existing_order : open_order option) = Hashtbl.find_opt store.open_orders order_id in
          let cum_qty = match existing_order with Some o -> o.cum_qty +. size | None -> size in
          let order_qty = match existing_order with Some o -> o.order_qty | None -> size in
          let is_filled = cum_qty >= (order_qty -. 1e-6) in
          let status = if is_filled then FilledStatus else PartiallyFilledStatus in
          let limit_price = match existing_order with Some o -> o.limit_price | None -> Some price in
          let cl_ord_id = match existing_order with Some o -> o.cl_ord_id | None -> None in
          let cum_cost = match existing_order with Some o -> o.cum_cost +. (size *. price) | None -> size *. price in
          let avg_price = if cum_qty > 0.0 then cum_cost /. cum_qty else price in
          Mutex.unlock store.orders_mutex;
          
          let event : execution_event = {
            order_id; symbol; 
            exec_type = Trade;
            order_status = status; 
            limit_price; side;
            order_qty; cum_qty; 
            cum_cost; avg_price;
            timestamp = now; trade_id = Some tid;
            last_qty = Some size; last_price = Some price; fee = Some fee; cl_ord_id;
          } in
          
          update_orders_internal store event;
          
          if is_filled then
            Logging.info_f ~section "Order FILLED: %s [%s] %.8f @ %.2f (trade_id: %Ld)" order_id symbol size price tid
          else
            Logging.info_f ~section "Order PARTIALLY FILLED: %s [%s] %.8f @ %.2f (filled: %.8f/%.8f)" order_id symbol size price cum_qty order_qty
        end
    | None -> ()
  ) fills;

  (* Handle nonUserCancel *)
  let non_user_cancels = try member "nonUserCancel" data_json |> to_list with _ -> [] in
  List.iter (fun nuc ->
    let coin = member "coin" nuc |> to_string in
    let order_id = (match member "oid" nuc with `Int i -> string_of_int i | `String s -> s | _ -> "0") in
    
    let symbol_opt = 
      Mutex.lock initialization_mutex;
      let res = Hashtbl.find_opt order_to_symbol order_id in
      Mutex.unlock initialization_mutex;
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
            let event : execution_event = {
              order_id; symbol; 
              exec_type = Canceled;
              order_status = CanceledStatus;
              limit_price = current_order.limit_price; side = current_order.side;
              order_qty = current_order.order_qty; cum_qty = current_order.cum_qty;
              cum_cost = current_order.cum_cost; avg_price = current_order.avg_price;
              timestamp = now; trade_id = None; last_qty = None; last_price = None; fee = None;
              cl_ord_id = current_order.cl_ord_id;
            } in
            update_orders_internal store event;
            Logging.info_f ~section "Order NON-USER CANCEL: %s [%s]" order_id symbol
         | None -> ())
    | None -> ()
  ) non_user_cancels

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  try
    match channel with
    | Some "orderUpdates" ->
        let data = member "data" json in
        process_order_updates data
    | Some "userEvents"
    | Some "userFills" ->
        let data = member "data" json in
        process_user_events data
    | Some "webData2" ->
        (* webData2 parsing for openOrders and fills removed.
           We now rely entirely on targeted websocket feeds:
           orderUpdates, userFills, userEvents for real-time pushing,
           which replaces the need for this snapshot polling loop. *)
        ()
    | _ -> ()
  with exn -> 
    Logging.error_f ~section "Failed to process Hyperliquid executions data: %s" (Printexc.to_string exn)

(** Event-driven cleanup bus — fires on-demand (reconnect, manual trigger) and
    falls back to a 120s safety timer so stale data never accumulates indefinitely.
    Uses an Lwt_mvar as a one-shot signal channel; request_cleanup is idempotent. *)
let cleanup_mvar : unit Lwt_mvar.t = Lwt_mvar.create_empty ()

(** Signal the cleanup loop to run immediately (idempotent: skipped if already pending).
    Uses is_empty guard so no Lwt continuation is ever queued: the put only happens
    when the mvar is empty (previous signal consumed), making this truly O(1). *)
let request_cleanup () =
  (* is_empty is a pure synchronous check.  Only put when the mvar is empty so we
     never queue a blocking continuation inside Lwt.async — the original bug that
     caused one Lwt closure to pile up per trigger_stale_order_cleanup call while
     the cleanup task was sleeping (up to 120 s between drains). *)
  if Lwt_mvar.is_empty cleanup_mvar then
    Lwt.async (fun () -> Lwt_mvar.put cleanup_mvar ())

let periodic_tasks_started = Atomic.make false

let start_periodic_tasks () =
  if not (Atomic.exchange periodic_tasks_started true) then begin
    let rec run () =
      (* Block until an explicit signal OR 120s safety timeout *)
      Lwt.pick [
        (Lwt_mvar.take cleanup_mvar >|= fun () -> `Signal);
        (Lwt_unix.sleep 120.0 >|= fun () -> `Timeout);
      ] >>= fun reason ->
      Logging.debug_f ~section "Running HL executions cleanup (%s)"
        (match reason with `Signal -> "requested" | `Timeout -> "120s fallback");
      cleanup_stale_orders ();
      Lwt.async run;
      Lwt.return_unit
    in
    Lwt.async run
  end

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_market_data () in
    Lwt.catch (fun () ->
      Logging.info ~section "Starting Hyperliquid executions processor task";
      let%lwt () = Concurrency.Lwt_util.consume_stream process_market_data sub.stream in
      (* Stream ended normally (disconnect pushed None) — re-subscribe *)
      sub.close ();
      Logging.info ~section "Executions stream ended (disconnect), re-subscribing in 1s...";
      Lwt_unix.sleep 1.0 >>= fun () -> run ()
    ) (fun exn ->
      sub.close ();
      Logging.error_f ~section "Hyperliquid executions processor task crashed: %s. Restarting in 5s..." (Printexc.to_string exn);
      Lwt_unix.sleep 5.0 >>= fun () ->
      run ()
    )
  in
  Lwt.async run

let initialize symbols =
  start_periodic_tasks ();
  Logging.info ~section "Initializing Hyperliquid executions feed";
  List.iter (fun symbol ->
    let _ = get_symbol_store symbol in
    Logging.debug_f ~section "Created Hyperliquid executions buffer for %s" symbol
  ) symbols

let inject_open_orders data_json =
  let open Yojson.Safe.Util in
  try
    let orders = to_list data_json in
    let count = ref 0 in
    List.iter (fun order_obj ->
      try
        let coin = member "coin" order_obj |> to_string in
        let order_id = match member "oid" order_obj with `Int i -> string_of_int i | `String s -> s | _ -> "0" in
        let symbol_opt = find_registered_symbol coin in
        
        match symbol_opt with
        | Some symbol ->
            let price = match member "limitPx" order_obj with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0 in
            let qty = 
              match member "origSz" order_obj with 
              | `String s -> float_of_string s 
              | `Float f -> f 
              | `Int i -> float_of_int i 
              | _ -> (match member "sz" order_obj with `String s -> float_of_string s | `Float f -> f | `Int i -> float_of_int i | _ -> 0.0)
            in
            let side = if (member "side" order_obj |> to_string) = "B" then Buy else Sell in
            let cl_ord_id = member "cloid" order_obj |> to_string_option in
            let timestamp_ms = match member "timestamp" order_obj with `Int i -> float_of_int i | `Float f -> f | `String s -> float_of_string s | _ -> Unix.gettimeofday () *. 1000.0 in
            
            let store = get_symbol_store symbol in
            
            let event : execution_event = {
              order_id;
              symbol;
              exec_type = New;
              order_status = NewStatus;
              limit_price = Some price;
              side;
              order_qty = qty;
              cum_qty = 0.0;
              cum_cost = 0.0;
              avg_price = 0.0;
              timestamp = timestamp_ms /. 1000.0;
              trade_id = None;
              last_qty = None;
              last_price = None;
              fee = None;
              cl_ord_id;
            } in
            update_orders_internal store event;
            incr count;
            Logging.debug_f ~section "Injected startup open order: %s [%s] %s %.8f @ %.2f" order_id symbol (if side = Buy then "buy" else "sell") qty price
        | None -> ()
      with exn -> Logging.warn_f ~section "Failed to parse open order entry: %s" (Printexc.to_string exn)
    ) orders;
    Logging.info_f ~section "Injected %d initial open orders from snapshot" !count;
    (* Lock adaptive cap now that startup snapshot is fully consumed.
       Subsequent inserts will evict oldest entries when over cap. *)
    Mutex.lock initialization_mutex;
    mark_startup_complete ();
    Mutex.unlock initialization_mutex
  with exn ->
    Logging.error_f ~section "Failed to inject open orders: %s" (Printexc.to_string exn)