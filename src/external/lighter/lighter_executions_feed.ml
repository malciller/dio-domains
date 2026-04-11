(** Lighter executions feed.
    Provides real-time order and trade event tracking via WebSocket subscriptions
    to [account_all_orders/{ACCOUNT_ID}]. Maintains per-symbol execution ring
    buffers, open order state, and event deduplication. Thread-safe access is
    enforced through per-resource mutexes and atomic flags. *)

let section = "lighter_executions_feed"

type side = Buy | Sell
type order_status =
  | PendingStatus
  | NewStatus
  | PartiallyFilledStatus
  | FilledStatus
  | CanceledStatus
  | ExpiredStatus
  | RejectedStatus
  | UnknownStatus of string

type open_order = {
  order_id: string;
  symbol: string;
  side: side;
  order_qty: float;
  cum_qty: float;
  remaining_qty: float;
  limit_price: float option;
  avg_price: float;
  order_status: order_status;
  order_userref: int option;
  cl_ord_id: string option;
  last_updated: float;
  order_expiry: float option;
}

type execution_event = {
  order_id: string;
  symbol: string;
  order_status: order_status;
  limit_price: float option;
  side: side;
  order_qty: float;
  cum_qty: float;
  avg_price: float;
  timestamp: float;
  is_amended: bool;
  cl_ord_id: string option;
}

(** Lock-free ring buffer used for execution event storage. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol execution state container with readiness signalling. *)
type store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  orders_mutex: Mutex.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Global order_id to symbol index for O(1) lookups. *)
let order_to_symbol : (string, string) Hashtbl.t = Hashtbl.create 64
let order_to_symbol_queue : string Queue.t = Queue.create ()
let order_to_symbol_cap = ref 256

(** Atomic flag set once after initial open orders snapshot completes. *)
let _startup_snapshot_done : bool Atomic.t = Atomic.make false
let set_startup_snapshot_done () =
  if not (Atomic.exchange _startup_snapshot_done true) then begin
    (* Mark all initialized per-symbol stores as ready so has_execution_data
       returns true even for symbols with zero open orders. *)
    Mutex.lock initialization_mutex;
    Hashtbl.iter (fun _symbol store ->
      if not (Atomic.get store.ready) then
        Atomic.set store.ready true
    ) stores;
    Mutex.unlock initialization_mutex;
    Logging.info ~section "Lighter open-order snapshot injected — domains may now activate";
    Concurrency.Exchange_wakeup.signal_all ()
  end
let is_startup_snapshot_done () = Atomic.get _startup_snapshot_done

(** Insert an order_id to symbol mapping with bounded FIFO eviction. *)
let add_to_order_to_symbol order_id symbol =
  if not (Hashtbl.mem order_to_symbol order_id) then
    Queue.push order_id order_to_symbol_queue;
  Hashtbl.replace order_to_symbol order_id symbol;
  while Hashtbl.length order_to_symbol > !order_to_symbol_cap do
    if Queue.is_empty order_to_symbol_queue then
      order_to_symbol_cap := Hashtbl.length order_to_symbol
    else begin
      let oldest = Queue.pop order_to_symbol_queue in
      Hashtbl.remove order_to_symbol oldest
    end
  done

(** Retrieve or lazily create a per-symbol store. Uses double-checked
    locking under initialization_mutex for thread-safe initialization. *)
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
              orders_mutex = Mutex.create ();
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

(** Core internal handler for order state transitions. *)
let update_orders_internal store (event : execution_event) =
  let now = Unix.gettimeofday () in
  Mutex.lock store.orders_mutex;

  let existing_opt = Hashtbl.find_opt store.open_orders event.order_id in
  let resolved_cl_ord_id =
    match event.cl_ord_id with
    | Some _ as c -> c
    | None ->
        (match existing_opt with
         | Some o -> o.cl_ord_id
         | None -> None)
  in

  let is_terminal = match event.order_status with
    | FilledStatus | CanceledStatus | RejectedStatus | ExpiredStatus -> true
    | _ -> false
  in

  if is_terminal then begin
    Hashtbl.remove store.open_orders event.order_id;
    Mutex.lock initialization_mutex;
    Hashtbl.remove order_to_symbol event.order_id;
    Mutex.unlock initialization_mutex;
  end else begin
    (* Preserve existing order_expiry if present; event updates don't carry expiry. *)
    let preserved_expiry = match existing_opt with
      | Some o -> o.order_expiry
      | None -> None
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
      order_status = event.order_status;
      order_userref = None;
      cl_ord_id = resolved_cl_ord_id;
      last_updated = now;
      order_expiry = preserved_expiry;
    } in
    Hashtbl.replace store.open_orders event.order_id order;
    Mutex.lock initialization_mutex;
    add_to_order_to_symbol event.order_id event.symbol;
    Mutex.unlock initialization_mutex;
  end;
  Mutex.unlock store.orders_mutex;

  RingBuffer.write store.events_buffer event;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol:event.symbol

(* --- Public query interface --- *)

let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let order = Hashtbl.find_opt store.open_orders order_id in
  Mutex.unlock store.orders_mutex;
  order

let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let orders = Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders [] in
  Mutex.unlock store.orders_mutex;
  orders

let[@inline always] fold_open_orders symbol ~init ~f =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let result = Hashtbl.fold (fun _id order acc -> f acc order) store.open_orders init in
  Mutex.unlock store.orders_mutex;
  result

let[@inline always] get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer

let[@inline always] read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos

let[@inline always] iter_execution_events symbol last_pos f =
  let store = get_symbol_store symbol in
  RingBuffer.iter_since store.events_buffer last_pos f

let[@inline always] has_execution_data symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.ready

(** Find an order across all symbol stores using the global index. *)
let find_order_everywhere order_id =
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

(** Clear all open orders across all symbol stores. Called on reconnection. *)
let clear_all_open_orders () =
  Mutex.lock initialization_mutex;
  let all_symbols = Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores [] in
  Mutex.unlock initialization_mutex;
  let total_removed = ref 0 in
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    let count = Hashtbl.length store.open_orders in
    total_removed := !total_removed + count;
    Hashtbl.clear store.open_orders;
    Mutex.unlock store.orders_mutex;
  ) all_symbols;
  Mutex.lock initialization_mutex;
  Hashtbl.clear order_to_symbol;
  Queue.clear order_to_symbol_queue;
  Mutex.unlock initialization_mutex;
  if !total_removed > 0 then
    Logging.debug_f ~section "Cleared %d stale open orders on reconnection" !total_removed

(** Proactively inject an order into the open-orders table.
    Used after place/amend to ensure immediate visibility. *)
let inject_order ~symbol ~order_id ~side ~qty ~price ?cl_ord_id () =
  let store = get_symbol_store symbol in
  let now = Unix.gettimeofday () in
  let event : execution_event = {
    order_id; symbol;
    order_status = NewStatus;
    limit_price = Some price;
    side;
    order_qty = qty;
    cum_qty = 0.0;
    avg_price = 0.0;
    timestamp = now;
    is_amended = false;
    cl_ord_id;
  } in
  update_orders_internal store event;
  Logging.debug_f ~section "Proactively injected open order: %s [%s] %s side %.8f limit_px=%.2f (cl_ord_id=%s)"
    order_id symbol (if side = Buy then "buy" else "sell") qty price
    (match cl_ord_id with Some c -> c | None -> "none")

(** Process order updates from Lighter WebSocket account stream.
    Lighter nests orders data under channel-specific keys, not "data". *)
let process_account_orders_update json =
  let open Yojson.Safe.Util in
  try
    let msg_type = (try member "type" json |> to_string with _ -> "unknown") in
    (* Log all account order messages to diagnose tracking issues *)
    let json_str = Yojson.Safe.to_string json in
    let truncated = if String.length json_str > 1500 then String.sub json_str 0 1500 ^ "..." else json_str in
    Logging.debug_f ~section "Account orders message (type=%s): %s" msg_type truncated;

    (* Lighter uses channel-specific data keys; try the most likely ones *)
    let orders_json =
      let try_key key =
        let v = member key json in
        if v <> `Null then Some v else None
      in
      match try_key "account_all_orders" with
      | Some v -> v
      | None -> match try_key "orders" with
        | Some v -> v
        | None -> match try_key "data" with
          | Some v -> v
          | None -> `Null
    in
    if orders_json = `Null then begin
      Logging.warn_f ~section "Account orders update has no recognizable data field (type=%s, keys=%s)"
        msg_type
        (try match json with `Assoc pairs -> String.concat ", " (List.map fst pairs) | _ -> "non-object" with _ -> "?");
      ()
    end else begin
      (* Orders can be an array or an object (keyed by order_id).
         Values in assoc may themselves be arrays of order objects. *)
      let orders = match orders_json with
        | `Assoc pairs ->
            List.concat_map (fun (_key, v) ->
              match v with
              | `Assoc _ -> [v]          (* single order object *)
              | `List items -> items     (* array of orders under one key *)
              | _ -> []                  (* skip non-object/non-array *)
            ) pairs
        | `List items -> items
        | _ ->
            Logging.debug_f ~section "Account orders data is neither array nor object: %s"
              (Yojson.Safe.to_string orders_json |> fun s ->
               if String.length s > 200 then String.sub s 0 200 ^ "..." else s);
            []
      in
    List.iter (fun order_json ->
      try
        match order_json with
        | `Assoc _ ->
        let order_index = member "order_index" order_json in
        let order_id = match order_index with
          | `String s -> s
          | `Int i -> string_of_int i
          | _ -> Lighter_types.parse_json_int64 order_index |> Int64.to_string
        in
        let market_index = Lighter_types.parse_json_int (member "market_index" order_json) in
        let symbol = match Lighter_instruments_feed.get_symbol ~market_index with
          | Some s -> s
          | None -> string_of_int market_index
        in

        (* Lighter sends status as int codes or as strings (e.g. "canceled").
           Do not use [parse_json_int] on string values: it maps bogus strings to 0,
           which is the "InProgress" code — so "canceled" was misread as Pending and
           the domain never ran [handle_order_cancelled]. *)
        let status_json = member "status" order_json in
        let map_int status_int =
          match Lighter_types.status_of_lighter_int status_int with
          | Dio_exchange.Exchange_intf.Types.Pending -> PendingStatus
          | Dio_exchange.Exchange_intf.Types.New -> NewStatus
          | Dio_exchange.Exchange_intf.Types.PartiallyFilled -> PartiallyFilledStatus
          | Dio_exchange.Exchange_intf.Types.Filled -> FilledStatus
          | Dio_exchange.Exchange_intf.Types.Canceled -> CanceledStatus
          | Dio_exchange.Exchange_intf.Types.Expired -> ExpiredStatus
          | Dio_exchange.Exchange_intf.Types.Rejected -> RejectedStatus
          | Dio_exchange.Exchange_intf.Types.Unknown s -> UnknownStatus s
        in
        let order_status =
          match status_json with
          | `Int _ | `Float _ ->
              map_int (Lighter_types.parse_json_int status_json)
          | `String s ->
              (match int_of_string_opt s with
               | Some n -> map_int n
               | None ->
                   match String.lowercase_ascii s with
                   | "open" | "new" | "active" -> NewStatus
                   | "partially_filled" | "partial" -> PartiallyFilledStatus
                   | "filled" -> FilledStatus
                   | "cancelled" | "canceled" -> CanceledStatus
                   | "expired" -> ExpiredStatus
                   | "rejected" -> RejectedStatus
                   | "pending" -> PendingStatus
                   | "" ->
                       let remaining = (try Lighter_types.parse_json_float (member "remaining_base_amount" order_json) with _ -> -1.0) in
                       let filled = (try Lighter_types.parse_json_float (member "filled_base_amount" order_json) with _ -> 0.0) in
                       if remaining = 0.0 && filled > 0.0 then FilledStatus
                       else if remaining > 0.0 then NewStatus
                       else UnknownStatus "no_status"
                   | low -> UnknownStatus low)
          | _ ->
              let status_str = (try to_string status_json with _ -> "") in
              match String.lowercase_ascii status_str with
              | "open" | "new" | "active" -> NewStatus
              | "partially_filled" | "partial" -> PartiallyFilledStatus
              | "filled" -> FilledStatus
              | "cancelled" | "canceled" -> CanceledStatus
              | "expired" -> ExpiredStatus
              | "rejected" -> RejectedStatus
              | "pending" -> PendingStatus
              | "" ->
                  let remaining = (try Lighter_types.parse_json_float (member "remaining_base_amount" order_json) with _ -> -1.0) in
                  let filled = (try Lighter_types.parse_json_float (member "filled_base_amount" order_json) with _ -> 0.0) in
                  if remaining = 0.0 && filled > 0.0 then FilledStatus
                  else if remaining > 0.0 then NewStatus
                  else UnknownStatus "no_status"
              | s -> UnknownStatus s
        in

        let is_ask = (try member "is_ask" order_json |> to_bool with _ -> false) in
        let side = if is_ask then Sell else Buy in
        let price = (try Lighter_types.parse_json_float (member "price" order_json) with _ -> 0.0) in
        let base_amount = (try Lighter_types.parse_json_float (member "initial_base_amount" order_json) with _ ->
          (try Lighter_types.parse_json_float (member "base_amount" order_json) with _ -> 0.0)) in
        let filled_base_amount = (try Lighter_types.parse_json_float (member "filled_base_amount" order_json) with _ -> 0.0) in
        let remaining = base_amount -. filled_base_amount in
        let avg_price = (try Lighter_types.parse_json_float (member "avg_fill_price" order_json) with _ -> price) in

        (* Parse order expiry from the API. Lighter uses "e" in the Go struct,
           but the REST/WS API may use "expiry" or "order_expiry". Try all.
           Lighter returns expiry as epoch milliseconds; convert to seconds
           to match Unix.gettimeofday(). *)
        let order_expiry =
          let try_field key =
            try
              let v = member key order_json in
              if v <> `Null then
                let raw = Lighter_types.parse_json_float v in
                (* Values > 1e12 are milliseconds; convert to seconds *)
                if raw > 1e12 then Some (raw /. 1000.0)
                else Some raw
              else None
            with _ -> None
          in
          match try_field "e" with
          | Some _ as v -> v
          | None -> match try_field "expiry" with
            | Some _ as v -> v
            | None -> try_field "order_expiry"
        in

        let store = get_symbol_store symbol in
        let is_amended = (try member "is_amended" order_json |> to_bool with _ -> false) in
        let now = Unix.gettimeofday () in

        (* When the exchange confirms an order, it assigns a real order_id
           (e.g., "577023702126945789") different from the client_order_index
           (e.g., "1") we used locally. Remove the stale local entry to
           prevent duplicates that confuse the strategy. *)
        let client_order_id = (try member "client_order_id" order_json |> to_string with _ -> "") in
        let cl_ord_id = if client_order_id <> "" then Some client_order_id else None in
        if client_order_id <> "" && client_order_id <> order_id then begin
          Mutex.lock store.orders_mutex;
          let had_stale = Hashtbl.mem store.open_orders client_order_id in
          Hashtbl.remove store.open_orders client_order_id;
          Mutex.unlock store.orders_mutex;
          if had_stale then begin
            Mutex.lock initialization_mutex;
            Hashtbl.remove order_to_symbol client_order_id;
            Mutex.unlock initialization_mutex;
            Logging.info_f ~section "Replaced local order %s with exchange order %s [%s]"
              client_order_id order_id symbol
          end
        end;

        let event : execution_event = {
          order_id; symbol;
          order_status; limit_price = Some price;
          side;
          order_qty = base_amount;
          cum_qty = filled_base_amount;
          avg_price;
          timestamp = now;
          is_amended;
          cl_ord_id;
        } in
        update_orders_internal store event;

        (* Set order_expiry on the open order entry if we parsed one.
           update_orders_internal won't have it because execution_event
           doesn't carry expiry — we set it directly on the stored order. *)
        let is_terminal_status = match order_status with
          | FilledStatus | CanceledStatus | RejectedStatus | ExpiredStatus -> true
          | _ -> false
        in
        (match order_expiry with
         | Some exp when not is_terminal_status ->
             Mutex.lock store.orders_mutex;
             (match Hashtbl.find_opt store.open_orders event.order_id with
              | Some o ->
                  Hashtbl.replace store.open_orders event.order_id { o with order_expiry = Some exp }
              | None -> ());
             Mutex.unlock store.orders_mutex
         | _ -> ());

        (match order_status with
         | FilledStatus ->
             Logging.info_f ~section "Order FILLED: %s [%s] %.8f @ %.2f" order_id symbol base_amount price
         | CanceledStatus ->
             Logging.info_f ~section "Order CANCELED: %s [%s] %.8f @ %.2f" order_id symbol base_amount price
         | NewStatus ->
             Logging.debug_f ~section "Order ACTIVE: %s [%s] %.8f @ %.2f remaining=%.8f" order_id symbol base_amount price remaining
         | PartiallyFilledStatus ->
             Logging.info_f ~section "Order PARTIAL: %s [%s] filled=%.8f/%.8f" order_id symbol filled_base_amount base_amount
         | _ -> ())
        | _ -> ()  (* skip non-object order entries *)
      with exn ->
        Logging.warn_f ~section "Failed to parse order entry: %s" (Printexc.to_string exn)
    ) orders
    end
  with exn ->
    Logging.error_f ~section "Failed to process account orders update: %s" (Printexc.to_string exn)

(** Periodic cleanup of stale orders (>24h). *)
let cleanup_stale_orders () =
  let now = Unix.gettimeofday () in
  let stale_threshold = 24.0 *. 3600.0 in
  Mutex.lock initialization_mutex;
  let all_symbols = Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores [] in
  Mutex.unlock initialization_mutex;
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    let stale = ref [] in
    Mutex.lock store.orders_mutex;
    Hashtbl.iter (fun order_id (order : open_order) ->
      if now -. order.last_updated > stale_threshold then
        stale := order_id :: !stale
    ) store.open_orders;
    List.iter (fun oid ->
      Hashtbl.remove store.open_orders oid;
      Mutex.lock initialization_mutex;
      Hashtbl.remove order_to_symbol oid;
      Mutex.unlock initialization_mutex;
    ) !stale;
    Mutex.unlock store.orders_mutex;
    if !stale <> [] then
      Logging.info_f ~section "Cleaned %d stale orders for %s" (List.length !stale) symbol
  ) all_symbols

(** Return all open orders with expiry data across all symbol stores.
    Used by the TIF renewal process to find orders approaching expiry. *)
let get_all_open_orders_with_expiry () =
  Mutex.lock initialization_mutex;
  let all_symbols = Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores [] in
  Mutex.unlock initialization_mutex;
  let result = ref [] in
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    Hashtbl.iter (fun _order_id (order : open_order) ->
      result := order :: !result
    ) store.open_orders;
    Mutex.unlock store.orders_mutex;
  ) all_symbols;
  !result

let initialize symbols =
  Logging.info_f ~section "Initializing Lighter executions feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = get_symbol_store symbol in
    Logging.debug_f ~section "Created Lighter execution store for %s" symbol
  ) symbols
