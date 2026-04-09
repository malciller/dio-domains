(** Kraken authenticated executions WebSocket feed. Tracks open orders and execution events per symbol via lock-free ring buffers for concurrent state synchronization. *)

open Lwt.Infix
open Concurrency

let section = "kraken_executions"
let ring_buffer_size = Kraken_common_types.default_ring_buffer_size_executions

let cleanup_handlers_started = Atomic.make false

open Kraken_common_types

(** Forces Conduit context evaluation with error handling. *)
let get_conduit_ctx = get_conduit_ctx

(** Execution event types from the Kraken WebSocket API. *)
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

let string_of_exec_type = function
  | PendingNew -> "pending_new"
  | New -> "new"
  | Trade -> "trade"
  | PartiallyFilled -> "partially_filled"
  | Filled -> "filled"
  | Canceled -> "canceled"
  | Expired -> "expired"
  | Amended -> "amended"
  | Restated -> "restated"
  | Rejected -> "rejected"
  | Unknown s -> s

let exec_type_of_string = function
  | "pending_new" -> PendingNew
  | "new" -> New
  | "trade" -> Trade
  | "partially_filled" -> PartiallyFilled
  | "filled" -> Filled
  | "canceled" -> Canceled
  | "expired" -> Expired
  | "amended" -> Amended
  | "restated" -> Restated
  | "rejected" -> Rejected
  | s -> Unknown s

(** Order side. *)
type side = Buy | Sell

let string_of_side = function
  | Buy -> "buy"
  | Sell -> "sell"

(* Defaults to Buy for unrecognized side strings. Unknown values are logged as warnings. *)
let side_of_string s =
  match s with
  | "buy" -> Buy
  | "sell" -> Sell
  | other ->
      Logging.warn_f ~section "Unknown order side %S, defaulting to Buy" other;
      Buy

(** Order status. *)
type order_status =
  | PendingNewStatus
  | NewStatus
  | PartiallyFilledStatus
  | FilledStatus
  | CanceledStatus
  | ExpiredStatus
  | RejectedStatus
  | UnknownStatus of string

let string_of_order_status = function
  | PendingNewStatus -> "pending_new"
  | NewStatus -> "new"
  | PartiallyFilledStatus -> "partially_filled"
  | FilledStatus -> "filled"
  | CanceledStatus -> "canceled"
  | ExpiredStatus -> "expired"
  | RejectedStatus -> "rejected"
  | UnknownStatus s -> s

let order_status_of_string = function
  | "pending_new" -> PendingNewStatus
  | "new" -> NewStatus
  | "partially_filled" -> PartiallyFilledStatus
  | "filled" -> FilledStatus
  | "canceled" -> CanceledStatus
  | "expired" -> ExpiredStatus
  | "rejected" -> RejectedStatus
  | s -> UnknownStatus s

(** Execution event representing an order state transition or fill. *)
type execution_event = {
  order_id: string;
  symbol: string;
  exec_type: exec_type;
  order_status: order_status;
  side: side;
  order_qty: float;
  cum_qty: float;
  cum_cost: float;
  avg_price: float;
  limit_price: float option;
  last_qty: float option;
  last_price: float option;
  fee: float option;
  trade_id: int64 option;
  order_userref: int option;
  cl_ord_id: string option;
  timestamp: float;
}

(** Event bus for publishing individual order updates. *)
module OrderUpdateEventBus = Event_bus.Make(struct
  type t = execution_event
end)

(** Singleton order update event bus. *)
let order_update_event_bus = OrderUpdateEventBus.create "order_update"

(** Open order record. *)
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
  order_userref: int option;  (* Optional numeric identifier for strategy-level filtering *)
  cl_ord_id: string option;   (* Client order ID for supplementary tracking *)
  last_updated: float;
}

(** Lock-free ring buffer for execution events. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Writes an execution event to the ring buffer. *)
let write_execution_event buffer event =
  RingBuffer.write buffer event;

(** Per-symbol execution store holding events, open orders, and readiness state. *)
type symbol_store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  last_event_time: float Atomic.t;
  orders_mutex: Mutex.t;
}

(** Global symbol-to-store mapping. *)
let symbol_stores : (string, symbol_store) Hashtbl.t = Hashtbl.create 64

(** Global order ID to symbol mapping with adaptive cap and FIFO eviction. Requires global_orders_mutex. *)
let order_to_symbol : (string, string) Hashtbl.t = Hashtbl.create 16
(** FIFO queue for O(1) oldest-entry eviction. *)
let order_to_symbol_queue : string Queue.t = Queue.create ()
(** Mutable cap; uncapped (max_int) during startup, locked after the initial snapshot. *)
let order_to_symbol_cap : int ref = ref max_int
let order_to_symbol_startup_done = Atomic.make false

(** Inserts an order ID to symbol mapping. Evicts the oldest entry via FIFO when exceeding cap. Caller must hold global_orders_mutex. *)
let add_to_order_to_symbol order_id symbol =
  if not (Hashtbl.mem order_to_symbol order_id) then
    Queue.push order_id order_to_symbol_queue;
  Hashtbl.replace order_to_symbol order_id symbol;
  if Atomic.get order_to_symbol_startup_done then
    while Hashtbl.length order_to_symbol > !order_to_symbol_cap do
      if Queue.is_empty order_to_symbol_queue then
        order_to_symbol_cap := Hashtbl.length order_to_symbol
      else begin
        let oldest = Queue.pop order_to_symbol_queue in
        Hashtbl.remove order_to_symbol oldest
      end
    done

(** Locks the adaptive cap after the startup snapshot. Cap = max(32, observed * 1.5 + 1). Executes once. *)
let lock_order_to_symbol_cap () =
  if not (Atomic.exchange order_to_symbol_startup_done true) then begin
    (* Caller (handle_snapshot) already holds global_orders_mutex. *)
    let observed = Hashtbl.length order_to_symbol in
    let cap = max 32 (observed + observed / 2 + 1) in
    order_to_symbol_cap := cap;
    Logging.info_f ~section
      "order_to_symbol adaptive cap locked at %d (observed %d entries at startup)" cap observed
  end

let global_orders_mutex = Mutex.create ()
(** Mutex for symbol_stores table initialization. *)
let initialization_mutex = Mutex.create ()

let ready_condition = Lwt_condition.create ()

(** Retrieves or lazily creates a per-symbol store. Wait-free on the hot path after initial creation. *)
let get_symbol_store symbol =
  match Hashtbl.find_opt symbol_stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      let store = 
        match Hashtbl.find_opt symbol_stores symbol with
        | Some s -> s
        | None ->
            let s = {
              events_buffer = RingBuffer.create ring_buffer_size;
              open_orders = Hashtbl.create 32;
              ready = Atomic.make false;
              last_event_time = Atomic.make 0.0;
              orders_mutex = Mutex.create ();
            } in
            Hashtbl.add symbol_stores symbol s;
            Logging.debug_f ~section "Created execution store for %s" symbol;
            s
      in
      Mutex.unlock initialization_mutex;
      store

(** Marks the store as ready and broadcasts to any waiting consumers. *)
let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try
      Lwt_condition.broadcast ready_condition ()
    with Invalid_argument _ ->
      (* Ignored: waiters may have timed out or been cancelled. *)
      ())
  end

let has_execution_data symbol =
  try
    let store = get_symbol_store symbol in
    Atomic.get store.ready
  with _ -> false

(** Blocks until execution data is available for all specified symbols or timeout elapses. *)
let wait_for_execution_data_lwt symbols timeout_seconds =
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

let wait_for_execution_data = wait_for_execution_data_lwt

(** Returns the list of open orders for a symbol. Acquires store.orders_mutex. *)
let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let orders = Hashtbl.fold (fun _id order acc -> order :: acc) store.open_orders [] in
  Mutex.unlock store.orders_mutex;
  orders

(** Zero-allocation fold over open orders for a symbol. Holds store.orders_mutex for the duration. *)
let[@inline always] fold_open_orders symbol ~init ~f =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let result = Hashtbl.fold (fun _id order acc -> f acc order) store.open_orders init in
  Mutex.unlock store.orders_mutex;
  result

(** Looks up a single open order by ID within a symbol store. *)
let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let order_opt = Hashtbl.find_opt store.open_orders order_id in
  Mutex.unlock store.orders_mutex;
  order_opt

(** Returns true if the given order ID exists in the symbol's open orders. *)
let[@inline always] has_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let exists = Hashtbl.mem store.open_orders order_id in
  Mutex.unlock store.orders_mutex;
  exists

(** Looks up an open order by ID across all symbols via the global order-to-symbol index. O(1) lookup; returns None if absent from cache. *)
let find_order_everywhere order_id =
  Mutex.lock global_orders_mutex;
  let symbol_opt = Hashtbl.find_opt order_to_symbol order_id in
  Mutex.unlock global_orders_mutex;
  match symbol_opt with
  | None -> None
  | Some symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      let order = Hashtbl.find_opt store.open_orders order_id in
      Mutex.unlock store.orders_mutex;
      order

(** Eagerly updates the cached limit price of an open order after a successful amend response, closing the stale-price window before the execution update arrives. No-op if the order is not tracked. *)
let update_open_order_price ~symbol ~order_id ~new_price =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  (match Hashtbl.find_opt store.open_orders order_id with
   | Some order ->
       let old_price = order.limit_price in
       let updated = { order with limit_price = Some new_price; last_updated = Unix.gettimeofday () } in
       Hashtbl.replace store.open_orders order_id updated;
       Logging.info_f ~section "EAGER_PRICE_UPDATE %s [%s]: %s -> %.8f (qty=%.8f remaining=%.8f)"
         order_id symbol
         (match old_price with Some p -> Printf.sprintf "%.8f" p | None -> "None")
         new_price order.order_qty order.remaining_qty
   | None ->
       Logging.info_f ~section "EAGER_PRICE_UPDATE %s [%s] not in cache, skipping (new_price=%.8f)"
         order_id symbol new_price);
  Mutex.unlock store.orders_mutex

(** Returns all symbols with initialized execution stores. *)
let get_all_symbols () =
  Mutex.lock global_orders_mutex;
  let symbols = ref [] in
  Hashtbl.iter (fun symbol _store -> symbols := symbol :: !symbols) symbol_stores;
  let result = !symbols in
  Mutex.unlock global_orders_mutex;
  result

(** Periodic maintenance that purges orphaned order_to_symbol_queue entries. Terminal events remove map entries but cannot efficiently remove queue entries; this rebuilds the queue retaining only entries still present in the map. *)
let cleanup_stale_orders () =
  Mutex.lock global_orders_mutex;
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
  Mutex.unlock global_orders_mutex

(** Idempotent Lwt_mvar cleanup signal. Prevents continuation stacking when triggers outpace consumption. *)
let cleanup_mvar : unit Lwt_mvar.t = Lwt_mvar.create_empty ()

let request_cleanup () =
  if Lwt_mvar.is_empty cleanup_mvar then
    Lwt.async (fun () -> Lwt_mvar.put cleanup_mvar ())

let periodic_tasks_started = Atomic.make false

let start_periodic_tasks () =
  if not (Atomic.exchange periodic_tasks_started true) then begin
    let rec run () =
      Lwt.pick [
        (Lwt_mvar.take cleanup_mvar >|= fun () -> `Signal);
        (Lwt_unix.sleep 120.0 >|= fun () -> `Timeout);
      ] >>= fun reason ->
      Logging.debug_f ~section "Running Kraken queue maintenance (%s)"
        (match reason with `Signal -> "requested" | `Timeout -> "120s fallback");
      cleanup_stale_orders ();
      Lwt.async run;
      Lwt.return_unit
    in
    Lwt.async run
  end

let trigger_stale_order_cleanup ~reason:_ () = request_cleanup ()


(** Returns the total count of open orders for a symbol. *)
let[@inline always] count_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let count = Hashtbl.length store.open_orders in
  Mutex.unlock store.orders_mutex;
  count

(** Returns (buy_count, sell_count) of open orders for a symbol. *)
let[@inline always] count_open_orders_by_side symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let buys = ref 0 in
  let sells = ref 0 in
  Hashtbl.iter (fun _id order ->
    match order.side with
    | Buy -> incr buys
    | Sell -> incr sells
  ) store.open_orders;
  Mutex.unlock store.orders_mutex;
  (!buys, !sells)

(** Reads execution events for a symbol since the given ring buffer position. *)
let[@inline always] read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos

(** Zero-allocation iteration over execution events since the given position. *)
let[@inline always] iter_execution_events symbol last_pos f =
  let store = get_symbol_store symbol in
  RingBuffer.iter_since store.events_buffer last_pos f

(** Returns the current ring buffer write position for tracking consumption. *)
let[@inline always] get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer

(** Reconciles the open orders table from an incoming execution event.
    Logging is deferred until after the mutex is released to avoid holding
    global_orders_mutex while contending on the logging output_mutex. *)
let update_open_orders store (event : execution_event) =
  (* Deferred log signal: captures what to log after mutex release. *)
  let log_action = ref `None in

  let is_terminal_status = match event.order_status with
    | FilledStatus | CanceledStatus | ExpiredStatus | RejectedStatus -> true
    | PendingNewStatus | NewStatus | PartiallyFilledStatus | UnknownStatus _ -> false
  in
  
  let raw_remaining_qty = event.order_qty -. event.cum_qty in
  let remaining_qty =
    if raw_remaining_qty <= 0.0 then 0.0 else raw_remaining_qty
  in
  let epsilon =
    let abs_order_qty = abs_float event.order_qty in
    if abs_order_qty = 0.0 then 1e-12 else abs_order_qty *. 1e-6
  in
  (* Guard: when order_qty is 0.0 (fallback default from a minimal WS event
     with missing quantity fields) and the status is non-terminal, this is NOT
     a real fill. Treating it as effectively filled removes valid orders from
     the open_orders table, breaking strategy reference price tracking. *)
  let is_effectively_filled =
    if event.order_qty = 0.0 && not is_terminal_status then false
    else remaining_qty <= epsilon
  in

  Mutex.lock store.orders_mutex;
  let was_tracked = Hashtbl.mem store.open_orders event.order_id in
  let needs_cleanup = ref false in
  (* Capture previous values for change detection logging. *)
  let prev_price_qty = if was_tracked then
    match Hashtbl.find_opt store.open_orders event.order_id with
    | Some prev -> Some (prev.limit_price, prev.order_qty, prev.remaining_qty)
    | None -> None
  else None in

  if is_terminal_status || is_effectively_filled then begin
    (* Terminal: remove from open orders if present. *)
    if was_tracked then Hashtbl.remove store.open_orders event.order_id
  end else begin
    (* Non-terminal: upsert into open orders.
       CRITICAL: Kraken's execution feed snapshot replays all open orders as
       exec_type=new/status=new but with MINIMAL data — limit_price is often
       absent (None) and qty fields may be zero. When we already have a cached
       entry with valid data, we must PRESERVE those fields rather than
       blindly overwriting them with empty/zero values. *)
    let merged_limit_price, merged_order_qty, merged_remaining_qty, merged_avg_price, merged_cum_cost =
      match Hashtbl.find_opt store.open_orders event.order_id with
      | Some prev ->
          (* Preserve cached price if incoming is None or zero —
             Kraken snapshot events arrive with limit_price = Some 0.0 *)
          let lp = match event.limit_price with
            | Some p when p > 1e-12 -> event.limit_price
            | _ -> prev.limit_price in
          (* Preserve cached order_qty if incoming is zero *)
          let oq = if event.order_qty > 1e-12 then event.order_qty else prev.order_qty in
          (* Preserve cached remaining_qty if incoming is zero but previous wasn't *)
          let rq = if remaining_qty > 1e-12 then remaining_qty
                   else if prev.remaining_qty > 1e-12 then prev.remaining_qty
                   else remaining_qty in
          (* Preserve avg_price and cum_cost similarly *)
          let ap = if event.avg_price > 1e-12 then event.avg_price else prev.avg_price in
          let cc = if event.cum_cost > 1e-12 then event.cum_cost else prev.cum_cost in
          (lp, oq, rq, ap, cc)
      | None ->
          (event.limit_price, event.order_qty, remaining_qty, event.avg_price, event.cum_cost)
    in
    let order = {
      order_id = event.order_id;
      symbol = event.symbol;
      side = event.side;
      order_qty = merged_order_qty;
      cum_qty = event.cum_qty;
      remaining_qty = merged_remaining_qty;
      limit_price = merged_limit_price;
      avg_price = merged_avg_price;
      cum_cost = merged_cum_cost;
      order_status = event.order_status;
      order_userref = event.order_userref;
      cl_ord_id = event.cl_ord_id;
      last_updated = event.timestamp;
    } in
    Hashtbl.replace store.open_orders event.order_id order;
    needs_cleanup := not was_tracked && Hashtbl.length store.open_orders > 1000;
  end;
  Mutex.unlock store.orders_mutex;

  (* Detect and log price/qty overwrites on existing orders. *)
  (match prev_price_qty with
   | Some (prev_lp, prev_oq, prev_rq) when not is_terminal_status && not is_effectively_filled ->
       (* Compare against the MERGED values that were actually stored,
          not the raw event values which may have been discarded by the merge. *)
       let stored_lp = match Hashtbl.find_opt store.open_orders event.order_id with
         | Some o -> o.limit_price | None -> event.limit_price in
       let stored_oq = match Hashtbl.find_opt store.open_orders event.order_id with
         | Some o -> o.order_qty | None -> event.order_qty in
       let stored_rq = match Hashtbl.find_opt store.open_orders event.order_id with
         | Some o -> o.remaining_qty | None -> remaining_qty in
       let price_changed = prev_lp <> stored_lp in
       let qty_changed = abs_float (prev_oq -. stored_oq) > 1e-12 in
       let rq_changed = abs_float (prev_rq -. stored_rq) > 1e-12 in
       if price_changed || qty_changed || rq_changed then
         Logging.info_f ~section
           "CACHE_OVERWRITE %s [%s] exec_type=%s status=%s: price %s->%s | qty %.8f->%.8f | remaining %.8f->%.8f"
           event.order_id event.symbol
           (string_of_exec_type event.exec_type)
           (string_of_order_status event.order_status)
           (match prev_lp with Some p -> Printf.sprintf "%.2f" p | None -> "None")
           (match stored_lp with Some p -> Printf.sprintf "%.2f" p | None -> "None")
           prev_oq stored_oq
           prev_rq stored_rq
       else
         (* Merge preserved existing data — snapshot replay was harmless *)
         Logging.debug_f ~section
           "CACHE_MERGE_NOOP %s [%s] exec_type=%s (snapshot replay, cached data preserved)"
           event.order_id event.symbol
           (string_of_exec_type event.exec_type)
   | _ -> ());

  if is_terminal_status || is_effectively_filled then begin
    if was_tracked then begin
      if is_terminal_status then begin
        Mutex.lock global_orders_mutex;
        Hashtbl.remove order_to_symbol event.order_id;
        Mutex.unlock global_orders_mutex;
      end;
      log_action := if is_terminal_status then `Removed_terminal else `Removed_zero_qty remaining_qty
    end else begin
      log_action := if is_terminal_status then `Terminal_untracked else `Filled_untracked
    end
  end else begin
    Mutex.lock global_orders_mutex;
    add_to_order_to_symbol event.order_id event.symbol;
    Mutex.unlock global_orders_mutex;
    log_action := `Upserted (was_tracked, !needs_cleanup, remaining_qty)
  end;

  (* Deferred logging: outside the critical section. *)
  (match !log_action with
   | `Removed_terminal ->
       Logging.debug_f ~section "Removed order: %s [%s] status=%s exec_type=%s"
         event.order_id event.symbol 
         (string_of_order_status event.order_status)
         (string_of_exec_type event.exec_type)
   | `Removed_zero_qty rq ->
       Logging.debug_f ~section "Removed order with zero remaining qty: %s [%s] status=%s exec_type=%s remaining=%.12f qty=%.12f"
         event.order_id event.symbol
         (string_of_order_status event.order_status)
         (string_of_exec_type event.exec_type)
         rq event.order_qty
   | `Terminal_untracked ->
       Logging.debug_f ~section "Terminal event for untracked order: %s [%s] status=%s"
         event.order_id event.symbol (string_of_order_status event.order_status)
   | `Filled_untracked ->
       Logging.debug_f ~section "Ignored effectively filled order for untracked id: %s [%s] status=%s"
         event.order_id event.symbol (string_of_order_status event.order_status)
   | `Upserted (was_present, needs_cleanup, remaining_qty) ->
       if needs_cleanup then
         trigger_stale_order_cleanup ~reason:"open_orders_exceeds_1000" ();
       if was_present then
         Logging.debug_f ~section "Updated open order: %s [%s] %.8f@%.2f (filled: %.8f/%.8f) status=%s"
           event.order_id event.symbol remaining_qty 
           (Option.value event.limit_price ~default:0.0) 
           event.cum_qty event.order_qty
           (string_of_order_status event.order_status)
       else
         Logging.debug_f ~section "Added new open order: %s [%s] %s side %.8f@%.2f status=%s"
           event.order_id event.symbol 
           (string_of_side event.side)
           event.order_qty
           (Option.value event.limit_price ~default:0.0)
           (string_of_order_status event.order_status);
       (* Log trade fills at info level for real-time monitoring. *)
       if event.exec_type = Trade then
         Logging.info_f ~section "Trade fill: %s [%s] qty=%.8f price=%.2f (total filled: %.8f/%.8f)"
           event.order_id event.symbol 
           (Option.value event.last_qty ~default:0.0)
           (Option.value event.last_price ~default:0.0)
           event.cum_qty event.order_qty
   | `None -> ())

(** Parses an optional float from JSON. Delegates to Kraken_common_types. *)
let parse_float_opt = Kraken_common_types.parse_float_opt

(** Parses an optional int64 from JSON. Delegates to Kraken_common_types. *)
let parse_int64_opt = Kraken_common_types.parse_int64_opt

(** Parses an optional int from JSON. Delegates to Kraken_common_types. *)
let parse_int_opt = Kraken_common_types.parse_int_opt

(** Parses an execution event from JSON. Handles both full and minimal (status-only) events by falling back to cached order data for missing fields. *)
let parse_execution_event json =
  try
    let open Yojson.Safe.Util in
    let order_id = member "order_id" json |> to_string in
    let exec_type_str = member "exec_type" json |> to_string in
    let order_status_str = member "order_status" json |> to_string in

    (* Symbol may be absent in minimal status updates (e.g., cancellations). *)
    let symbol_opt = member "symbol" json |> to_string_option in

    (* Resolve symbol from the global order-to-symbol index for minimal events. *)
    let symbol = match symbol_opt with
      | Some s -> Some s
      | None ->
          Mutex.lock global_orders_mutex;
          let s = Hashtbl.find_opt order_to_symbol order_id in
          Mutex.unlock global_orders_mutex;
          (match s with
           | Some sym -> Some sym
           | None ->
               (* Skip events with no symbol attribution, typical of pre-startup orders. *)
               Logging.debug_f ~section "Skipping event for unknown order %s (likely pre-startup order)" order_id;
               None)
    in

    (* Return None if symbol could not be resolved. *)
    match symbol with
    | None -> None
    | Some sym ->
        (* Fetch existing order to fill in missing fields from minimal events. *)
        let store = get_symbol_store sym in
        Mutex.lock store.orders_mutex;
        let existing_order = Hashtbl.find_opt store.open_orders order_id in
        Mutex.unlock store.orders_mutex;

        (* Extract fields from JSON with fallback to existing order data. *)
        let side_str =
          match member "side" json |> to_string_option with
          | Some s -> s
          | None ->
              (match existing_order with
               | Some order -> string_of_side order.side
               | None -> "buy")  (* Default to buy when no prior data exists. *)
        in

        let order_qty =
          match parse_float_opt json "order_qty" with
          | Some q -> q
          | None ->
              match parse_float_opt json "qty" with
              | Some q -> q
              | None ->
                  match parse_float_opt json "quantity" with
                  | Some q -> q
                  | None ->
                      (match existing_order with
                       | Some order -> order.order_qty
                       | None ->
                           Logging.info_f ~section
                             "FIELD FALLBACK order_qty=0.0 for %s [%s] exec_type=%s status=%s (no cached order)"
                             order_id (Option.value symbol_opt ~default:"?") exec_type_str order_status_str;
                           0.0)
        in

        let cum_qty =
          match parse_float_opt json "cum_qty" with
          | Some q -> q
          | None ->
              (match existing_order with
           | Some order -> order.cum_qty
           | None -> 0.0)
        in

        let cum_cost =
          match parse_float_opt json "cum_cost" with
          | Some c -> c
          | None ->
              (match existing_order with
               | Some order -> order.cum_cost
               | None -> 0.0)
        in

        let avg_price =
          match parse_float_opt json "avg_price" with
          | Some p -> p
          | None ->
              (match existing_order with
               | Some order -> order.avg_price
               | None -> 0.0)
        in

        let limit_price =
          match parse_float_opt json "limit_price" with
          | Some _ as p -> p
          | None ->
              match parse_float_opt json "price" with
              | Some _ as p -> p
              | None ->
                  (match existing_order with
                   | Some order -> order.limit_price
                   | None ->
                       Logging.info_f ~section
                         "FIELD FALLBACK limit_price=None for %s [%s] exec_type=%s status=%s (no cached order)"
                         order_id (Option.value symbol_opt ~default:"?") exec_type_str order_status_str;
                       None)
        in

        let last_qty = parse_float_opt json "last_qty" in
        let last_price = parse_float_opt json "last_price" in

        (* Extract fee from fee_usd_equiv or the first entry in the fees array. *)
        let fee =
          match parse_float_opt json "fee_usd_equiv" with
          | Some f -> Some f
          | None ->
              try
                let fees = member "fees" json |> to_list in
                match fees with
                | fee_obj :: _ ->
                    parse_float_opt fee_obj "qty"
                | [] -> None
              with _ -> None
        in

        let trade_id = parse_int64_opt json "trade_id" in

        let order_userref =
          match parse_int_opt json "order_userref" with
          | Some _ as u -> u
          | None ->
              (match existing_order with
               | Some order -> order.order_userref
               | None -> None)
        in

        let cl_ord_id =
          match member "cl_ord_id" json |> to_string_option with
          | Some _ as c -> c
          | None ->
              (match existing_order with
               | Some order -> order.cl_ord_id
               | None -> None)
        in

        (* Use current wall-clock time as timestamp, avoiding full RFC3339 parsing. *)
        let timestamp = Unix.gettimeofday () in

        (* Log when processing a minimal event (symbol resolved from cache). *)
        if symbol_opt = None then
          Logging.debug_f ~section "Processing minimal event for order %s [%s]: status=%s exec_type=%s"
            order_id sym order_status_str exec_type_str;

        Some {
          order_id;
          symbol = sym;
          exec_type = exec_type_of_string exec_type_str;
          order_status = order_status_of_string order_status_str;
          side = side_of_string side_str;
          order_qty;
          cum_qty;
          cum_cost;
          avg_price;
          limit_price;
          last_qty;
          last_price;
          fee;
          trade_id;
          order_userref;
          cl_ord_id;
          timestamp;
        }
  with exn ->
    Logging.warn_f ~section "Failed to parse execution event: %s | JSON: %s" 
      (Printexc.to_string exn)
      (Yojson.Safe.to_string json);
    None

(** Processes an execution snapshot: ingests events and reconciles stale open orders. *)
let handle_snapshot json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    Logging.debug_f ~section "Processing execution snapshot with %d items" (List.length data);
    
    (* Track order IDs present in this snapshot for reconciliation. *)
    let snapshot_order_ids = Hashtbl.create (List.length data) in
    
    List.iter (fun item ->
      match parse_execution_event item with
      | Some event ->
          let store = get_symbol_store event.symbol in
          write_execution_event store.events_buffer event;
          update_open_orders store event;
          Atomic.set store.last_event_time event.timestamp;
          notify_ready store;
          Concurrency.Exchange_wakeup.signal ~symbol:event.symbol;
          
          (* Mark order ID as active in snapshot *)
          Hashtbl.replace snapshot_order_ids event.order_id ();
          
          (* Update heartbeat *)
          on_heartbeat ()
      | None -> ()
    ) data;
    
    (* Reconcile: remove locally cached orders absent from the snapshot to prevent stale entries after reconnection. *)
    let stale_orders = ref [] in
    
    (* Scan all symbol stores since snapshot covers all subscribed symbols. *)
    let all_symbols = get_all_symbols () in
    List.iter (fun symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      
      (* Collect orders not present in the snapshot. *)
      Hashtbl.iter (fun order_id _ ->
        if not (Hashtbl.mem snapshot_order_ids order_id) then
          stale_orders := (symbol, order_id) :: !stale_orders
      ) store.open_orders;
      
      Mutex.unlock store.orders_mutex;
    ) all_symbols;
    
    (* Remove identified stale orders. *)
    let removed_count = List.length !stale_orders in
    if removed_count > 0 then begin
      Logging.info_f ~section "Reconciling open orders: removing %d stale orders not present in snapshot" removed_count;
      
      List.iter (fun (symbol, order_id) ->
        let store = get_symbol_store symbol in
        let was_present =
          Mutex.lock store.orders_mutex;
          let exists = Hashtbl.mem store.open_orders order_id in
          if exists then Hashtbl.remove store.open_orders order_id;
          Mutex.unlock store.orders_mutex;
          exists
        in
        if was_present then begin
          Mutex.lock global_orders_mutex;
          Hashtbl.remove order_to_symbol order_id;
          Mutex.unlock global_orders_mutex;
          Logging.debug_f ~section "Removed stale order during reconciliation: %s [%s]" order_id symbol
        end
      ) !stale_orders;
      
    end;
    
    (* Mark all initialized stores as ready regardless of whether they received snapshot events. *)
    List.iter (fun symbol ->
      let store = get_symbol_store symbol in
      notify_ready store;
    ) all_symbols;
    
    Logging.debug_f ~section "Execution snapshot processed and reconciled";
    (* Lock the adaptive order_to_symbol cap after startup snapshot ingestion. *)
    lock_order_to_symbol_cap ()
  with exn ->
    Logging.error_f ~section "Failed to process execution snapshot: %s"
      (Printexc.to_string exn)

(** Processes incremental execution update messages. *)
let handle_update json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json |> to_list in
    
    List.iter (fun item ->
      match parse_execution_event item with
      | Some event ->
          let store = get_symbol_store event.symbol in
          write_execution_event store.events_buffer event;
          update_open_orders store event;
          Atomic.set store.last_event_time event.timestamp;
          notify_ready store;
          Concurrency.Exchange_wakeup.signal ~symbol:event.symbol;

          (* Publish to order update event bus *)
          OrderUpdateEventBus.publish order_update_event_bus event;

          (* Update heartbeat *)
          on_heartbeat ()
      | None -> ()
    ) data
  with exn ->
    Logging.error_f ~section "Failed to process execution update: %s"
      (Printexc.to_string exn)

(** WebSocket message handler operating on pre-parsed JSON to avoid redundant serialization. *)
let handle_message_json json on_heartbeat =
  try
    let open Yojson.Safe.Util in
    let channel = member "channel" json |> to_string_option in
    let msg_type = member "type" json |> to_string_option in
    let method_type = member "method" json |> to_string_option in
    
    (match channel with
    | Some "heartbeat" -> ()
    | _ ->
        Logging.debug_f ~section "Executions message received: channel=%s, type=%s, method=%s"
          (Option.value channel ~default:"none")
          (Option.value msg_type ~default:"none")
          (Option.value method_type ~default:"none"));
    
    match channel, msg_type, method_type with
    | Some "executions", Some "snapshot", _ ->
        handle_snapshot json on_heartbeat
    | Some "executions", Some "update", _ ->
        handle_update json on_heartbeat
    | Some "heartbeat", _, _ ->
        on_heartbeat ()
    | _, _, Some "subscribe" ->
        let success = member "success" json |> to_bool_option in
        (match success with
        | Some true -> 
            Logging.info ~section "Subscribed to executions feed";
        | Some false -> 
            let error = member "error" json |> to_string_option in
            Logging.error_f ~section "Subscription failed: %s" 
              (Option.value error ~default:"Unknown error")
        | None -> ())
    | Some "status", _, _ ->
        Logging.debug ~section "Status message received"
    | _ ->
        ()
  with exn ->
    Logging.error_f ~section "Error handling message: %s" 
      (Printexc.to_string exn)

(** WebSocket message handler accepting raw string input. Parses JSON then delegates to handle_message_json. *)
let handle_message message on_heartbeat =
  Concurrency.Tick_event_bus.publish_tick ();
  try
    let json = Yojson.Safe.from_string message in
    handle_message_json json on_heartbeat
  with exn ->
    Logging.error_f ~section "Error parsing message: %s - %s" 
      (Printexc.to_string exn) message


(** Deprecated. Superseded by the unified connection hub. Retained for interface compatibility. *)
let start_message_handler _conn _token _on_failure _on_heartbeat =
  Lwt.return_unit

(** Subscribes to the Kraken executions channel on the authenticated WebSocket and starts the message consumption loop. *)
let connect_and_subscribe token ~on_failure:_ ~on_heartbeat ~on_connected =
  Logging.info ~section "Registering executions subscription on unified authenticated connection";
  
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "executions");
      ("token", `String token);
      ("snap_orders", `Bool true);
      ("snap_trades", `Bool true);
      ("order_status", `Bool true)
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

(** Creates a subscription to the order update event bus. Returns (stream, close_fn). *)
let subscribe_order_updates () =
  let subscription = OrderUpdateEventBus.subscribe order_update_event_bus in
  (subscription.stream, subscription.close)

(** Initializes execution stores for the given symbols and starts periodic maintenance. *)
let initialize symbols =
  start_periodic_tasks ();
  Logging.info_f ~section "Initializing executions feed for %d symbols" (List.length symbols);

  (* Pre-create symbol stores so hot-path lookups are wait-free. *)
  List.iter (fun symbol ->
    let _store = get_symbol_store symbol in
    Logging.debug_f ~section "Created lock-free execution store for %s" symbol
  ) symbols;

  Logging.info ~section "Execution stores initialized - now operating lock-free";
