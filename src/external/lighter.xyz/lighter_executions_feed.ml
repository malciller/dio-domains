(** Modular engine component for processing Lighter execution events.
    Handles the ingestion and real-time synchronization of order updates
    and trade data by consuming the account WebSocket subscriptions.
    Each supported asset symbol is backed by an isolated locking scheme
    that guards a dedicated ring buffer and hash table to process inbound
    events with strict serialization and linearizability properties. *)



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

(** Dedicated lock-free ring buffer structure allocated per trading pair
    for ordered storage of real-time execution events. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Isolated state container for a specific trading pair. Maintains
    localized execution events, active order tracking structures, and
    an atomic readiness flag to block downstream components until
    market cache bootstrap is complete. Guarded by a dedicated mutex. *)
type store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  orders_mutex: Mutex.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Global indexing structure mapping unique order identifiers to their
    respective asset symbols. Incorporates dynamic capacity management
    and chronological eviction policies. Operations modifying this index
    must acquire the initialization lock. *)
let order_to_symbol : (string, string) Hashtbl.t = Hashtbl.create 64
(** Chronological tracking structure enforcing first-in-first-out eviction
    policies for the order-to-symbol global index. *)
let order_to_symbol_queue : string Queue.t = Queue.create ()
(** Threshold capacity variable governing the maximum index size. Initially
    uncapped to allow complete ingestion of startup payloads, subsequently
    constrained based on observed baseline system volume. *)
let order_to_symbol_cap : int ref = ref max_int
let order_to_symbol_startup_done = Atomic.make false


(** Synchronization primitive indicating successful completion of the
    initial active orders ingestion sequence. *)
let _startup_snapshot_done : bool Atomic.t = Atomic.make false
(** Freezes the adaptive capacity threshold following the completion of
    the initial execution snapshot. Computes a localized limit based on
    startup volume to constrain index memory allocation. Re-invocation
    is safely ignored. *)
let mark_startup_complete () =
  if not (Atomic.exchange order_to_symbol_startup_done true) then begin
    (* Precondition: initialization_mutex is held by the caller. *)
    let observed = Hashtbl.length order_to_symbol in
    let cap = max 32 (observed + observed / 2 + 1) in
    order_to_symbol_cap := cap;
    Logging.info_f ~section
      "order_to_symbol adaptive cap locked at %d (observed %d entries at startup)" cap observed
  end

let set_startup_snapshot_done () =
  if not (Atomic.exchange _startup_snapshot_done true) then begin
    (* Broadcast initialization completion to all active localized stores.
       Ensures downstream querying logic reports accurate readiness state
       irrespective of initial open order volume. *)
    Mutex.lock initialization_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
      Hashtbl.iter (fun _symbol store ->
        if not (Atomic.get store.ready) then
          Atomic.set store.ready true
      ) stores;
      mark_startup_complete ()
    );
    Logging.debug_f ~section "Lighter open-order snapshot injected. Core execution domains may now activate";
    Concurrency.Exchange_wakeup.signal_all ()
  end
let is_startup_snapshot_done () = Atomic.get _startup_snapshot_done

(** Injects an order identifier and its corresponding asset symbol into
    the global index. Executes eviction routines if the adaptive capacity
    threshold is eclipsed. Requires the caller to hold the initialization lock. *)
let add_to_order_to_symbol order_id symbol =
  if not (Hashtbl.mem order_to_symbol order_id) then
    Queue.push order_id order_to_symbol_queue;
  Hashtbl.replace order_to_symbol order_id symbol;
  (* Execute chronological eviction processing only after the primary
     startup sequence has finalized its operational capacity sizing. *)
  if Atomic.get order_to_symbol_startup_done then begin
    while Hashtbl.length order_to_symbol > !order_to_symbol_cap do
      if Queue.is_empty order_to_symbol_queue then
        (* Synchronize internal thresholds to the current state if
           length invariants have diverged from queue metrics. *)
        order_to_symbol_cap := Hashtbl.length order_to_symbol
      else begin
        let oldest = Queue.pop order_to_symbol_queue in
        Hashtbl.remove order_to_symbol oldest
      end
    done;
    (* Reconcile capacity boundaries against the queue to eliminate
       stale entries left by out-of-band removal requests. *)
    while Queue.length order_to_symbol_queue > !order_to_symbol_cap * 2 do
      if not (Queue.is_empty order_to_symbol_queue) then begin
        let oldest = Queue.pop order_to_symbol_queue in
        Hashtbl.remove order_to_symbol oldest
      end
    done
  end

(** Resolves the specific state container assigned to the provided symbol.
    Implements double-checked locking protocols to prevent data races
    during lazy cache allocation operations. *)
let get_symbol_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
        match Hashtbl.find_opt stores symbol with
        | Some store -> store
        | None ->
            let store = {
              events_buffer = RingBuffer.create 128;
              open_orders = Hashtbl.create 32;
              ready = Atomic.make (Atomic.get _startup_snapshot_done);
              orders_mutex = Mutex.create ();
            } in
            Hashtbl.add stores symbol store;
            store
      )

let notify_ready store =
  if not (Atomic.get store.ready) then Atomic.set store.ready true;
  (try Lwt_condition.broadcast ready_condition () with _ -> ())

(** Core deterministic transformation layer managing the lifecycle
    transitions of tracked orders within local storage partitions. *)
let update_orders_internal store (event : execution_event) =
  let now = Unix.gettimeofday () in
  Mutex.lock store.orders_mutex;

  Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
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
      Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
        Hashtbl.remove order_to_symbol event.order_id
      )
    end else begin
      (* Extract the expiration variable from historical tracking data
         as differential event payloads lack termination scheduling fields. *)
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
      Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
        add_to_order_to_symbol event.order_id event.symbol
      )
    end
  );

  RingBuffer.write store.events_buffer event;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol:event.symbol

(* --- Public query interface --- *)

let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
    Hashtbl.find_opt store.open_orders order_id
  )

let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
    Hashtbl.fold (fun _ o acc -> o :: acc) store.open_orders []
  )

let[@inline always] fold_open_orders symbol ~init ~f =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
    Hashtbl.fold (fun _id order acc -> f acc order) store.open_orders init
  )

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

(** Executes a secondary lookup targeting active index entities across
    all parallel stores by resolving the base symbol dependency first. *)
let find_order_everywhere order_id =
  Mutex.lock initialization_mutex;
  let symbol_opt = Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
    Hashtbl.find_opt order_to_symbol order_id
  ) in
  match symbol_opt with
  | Some symbol ->
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
        Hashtbl.find_opt store.open_orders order_id
      )
  | None -> None

(** Triggers a destructive reset operation affecting all parallel
    execution stores, erasing localized order tracking variables
    during catastrophic connectivity events. *)
let clear_all_open_orders () =
  Mutex.lock initialization_mutex;
  let all_symbols = Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
    Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores []
  ) in
  let total_removed = ref 0 in
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
      let count = Hashtbl.length store.open_orders in
      total_removed := !total_removed + count;
      Hashtbl.clear store.open_orders;
      Atomic.set store.ready false
    )
  ) all_symbols;
  Mutex.lock initialization_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
    Hashtbl.clear order_to_symbol;
    Queue.clear order_to_symbol_queue
  );
  Atomic.set _startup_snapshot_done false;
  if !total_removed > 0 then
    Logging.debug_f ~section "Cleared %d stale open orders on reconnection" !total_removed

(** Proactively injects deterministic order parameters into the local
    tracking state prior to receiving corresponding exchange validation.
    Mitigates latency impacts associated with synchronous verification. *)
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

(** Primary ingest controller for discrete order updates dispatched by
    the exchange via standard asynchronous broadcast mechanisms. *)
let process_account_orders_update json =
  let open Yojson.Safe.Util in
  try
    let msg_type = (try member "type" json |> to_string with _ -> "unknown") in
    (* Output serialized account message payloads directly for robust
       protocol debugging and anomaly detection tasks. *)
    if Logging.will_log Logging.DEBUG section then begin
      let json_str = Yojson.Safe.to_string json in
      let truncated = if String.length json_str > 1500 then String.sub json_str 0 1500 ^ "..." else json_str in
      Logging.debug_f ~section "Account orders message (type=%s): %s" msg_type truncated
    end;

    (* Interrogate the unstructured payload against known deterministic
       identifier keys implemented within the exchange serialization layer. *)
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
      (* Resolve unstructured arrays or nested key-value parameters into
         standardized extraction primitives for uniform processing logic. *)
      let orders = match orders_json with
        | `Assoc pairs ->
            List.concat_map (fun (_key, v) ->
              match v with
              | `Assoc _ -> [v]          (* Parse isolated object graph *)
              | `List items -> items     (* Parse standardized sequential array *)
              | _ -> []                  (* Prune unmatched unstructured variables *)
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

        (* Convert serialized identification codes to standard internal types.
           Prevents parsing layers from mapping unidentified alphanumeric
           strings to default zero integers which could incorrectly trigger
           pending status assignments and suppress execution workflows. *)
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

        let is_ask =
          let is_ask_val =
            try let v = member "is_ask" order_json in if v <> `Null then v else member "isAsk" order_json
            with _ -> `Null
          in
          match is_ask_val with
          | `Bool b -> b
          | `Int i -> i <> 0
          | `String s -> String.lowercase_ascii s = "true" || s = "1"
          | _ ->
              (try
                let side_str = member "side" order_json |> to_string |> String.lowercase_ascii in
                side_str = "sell" || side_str = "ask"
               with _ -> false)
        in
        let side = if is_ask then Sell else Buy in
        let price = (try Lighter_types.parse_json_float (member "price" order_json) with _ -> 0.0) in
        let base_amount = (try Lighter_types.parse_json_float (member "initial_base_amount" order_json) with _ ->
          (try Lighter_types.parse_json_float (member "base_amount" order_json) with _ -> 0.0)) in
        let filled_base_amount = (try Lighter_types.parse_json_float (member "filled_base_amount" order_json) with _ -> 0.0) in
        let remaining = base_amount -. filled_base_amount in
        let avg_price = (try Lighter_types.parse_json_float (member "avg_fill_price" order_json) with _ -> price) in

        (* Extract defined numerical expiration constants from the exchange.
           Supports multiple parameter names for backwards compatibility.
           Translates native millisecond definitions into standardized
           epoch variables aligned with internal state tracking logic. *)
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

        (* Execute deterministic substitution workflows upon detecting an
           assigned exchange identifier differing from local index keys.
           Prevents the execution system from diverging tracking variables
           and emitting redundant downstream reconciliation payloads. *)
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

        (* Interject the explicit expiration target onto the underlying
           recorded entity structure since standardized propagation events
           omit expiration state from dynamic differential updates. *)
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
             Logging.info_f ~section "Order FILLED: %s [%s] %.8f @ %.2f" order_id symbol base_amount price;
             (* Broadcast the reconciled execution parameters to the root event bus. *)
             let fill_value = filled_base_amount *. avg_price in
             let maker_fee_rate =
               match Dio_exchange.Exchange_intf.Registry.get "lighter" with
               | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
                   (match Ex.get_fees ~symbol with (Some f, _) -> f | _ -> 0.0)
               | None -> 0.0
             in
             let fee = fill_value *. maker_fee_rate in
             Concurrency.Fill_event_bus.publish_fill {
               venue = "lighter";
               symbol;
               side = (if is_ask then "sell" else "buy");
               amount = filled_base_amount;
               fill_price = avg_price;
               value = fill_value;
               fee;
               timestamp = Unix.gettimeofday ();
               order_id;
               trade_id = order_id;  (* Fallback resolution utilizing the base order key. *)
             }
         | CanceledStatus ->
             Logging.info_f ~section "Order CANCELED: %s [%s] %.8f @ %.2f" order_id symbol base_amount price
         | NewStatus ->
             Logging.debug_f ~section "Order ACTIVE: %s [%s] %.8f @ %.2f remaining=%.8f" order_id symbol base_amount price remaining
         | PartiallyFilledStatus ->
             Logging.info_f ~section "Order PARTIAL: %s [%s] filled=%.8f/%.8f" order_id symbol filled_base_amount base_amount
         | _ -> ())
        | _ -> ()  (* Iterate past invalid unmapped elements. *)
      with exn ->
        Logging.warn_f ~section "Failed to parse order entry: %s" (Printexc.to_string exn)
    ) orders
    end
  with exn ->
    Logging.error_f ~section "Failed to process account orders update: %s" (Printexc.to_string exn);
  (* Trigger the internal availability sequence upon interpreting specific
     state variables from dynamic execution channels. Resolves previous
     synchronization issues where internal modules encountered arbitrary
     delays while evaluating initialization primitives. Setting the toggle
     signals readiness across unblocked processing boundaries. *)
  set_startup_snapshot_done ()

(** Primary processing loop for executing state normalization against
    periodic synchronization payload data containing unified local mappings. *)
let handle_snapshot json =
  Logging.debug_f ~section "Processing Lighter execution snapshot...";
  process_account_orders_update json;

  let open Yojson.Safe.Util in
  let snapshot_order_ids = Hashtbl.create 32 in
  (try
    let orders_json =
      let try_key key = let v = member key json in if v <> `Null then Some v else None in
      match try_key "account_all_orders" with
      | Some v -> v
      | None -> match try_key "orders" with
        | Some v -> v
        | None -> match try_key "data" with Some v -> v | None -> `Null
    in
    let orders = match orders_json with
      | `Assoc pairs -> List.concat_map (fun (_,v) -> match v with `Assoc _ -> [v] | `List items -> items | _ -> []) pairs
      | `List items -> items
      | _ -> []
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
            Hashtbl.replace snapshot_order_ids order_id ()
        | _ -> ()
      with _ -> ()
    ) orders
  with exn -> Logging.warn_f ~section "Failed to extract IDs for reconcile: %s" (Printexc.to_string exn));

  (* Transmit cancellation payloads downstream by evaluating the delta
     between the provided execution data array and the internal cache. *)
  let stale_orders = ref [] in
  Mutex.lock initialization_mutex;
  let all_symbols = Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores [] in
  Mutex.unlock initialization_mutex;

  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
      Hashtbl.iter (fun order_id (cached_order: open_order) ->
        if not (Hashtbl.mem snapshot_order_ids order_id) then
          stale_orders := (symbol, store, cached_order) :: !stale_orders
      ) store.open_orders
    )
  ) all_symbols;

  List.iter (fun (symbol, store, (cached_order: open_order)) ->
    let event : execution_event = {
      order_id = cached_order.order_id;
      symbol;
      order_status = CanceledStatus;
      limit_price = cached_order.limit_price;
      side = cached_order.side;
      order_qty = cached_order.order_qty;
      cum_qty = cached_order.cum_qty;
      avg_price = cached_order.avg_price;
      timestamp = Unix.gettimeofday ();
      is_amended = false;
      cl_ord_id = cached_order.cl_ord_id;
    } in
    update_orders_internal store event;
    Logging.info_f ~section "Reconciled stale order: emitted CanceledStatus for %s [%s]" cached_order.order_id symbol
  ) !stale_orders;

  if !stale_orders <> [] then
    Logging.info_f ~section "Reconciled open orders: removed %d stale orders not present in snapshot" (List.length !stale_orders)

(** Asynchronous daemon executing cyclical cache reduction protocols.
    Eliminates tracking components assigned to orders experiencing extreme
    duration limits and terminates stale global mappings. *)
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
    Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
      Hashtbl.iter (fun order_id (order : open_order) ->
        if now -. order.last_updated > stale_threshold then
          stale := order_id :: !stale
      ) store.open_orders;
      List.iter (fun oid ->
        Hashtbl.remove store.open_orders oid;
        Mutex.lock initialization_mutex;
        Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
          Hashtbl.remove order_to_symbol oid
        )
      ) !stale
    );
    if !stale <> [] then
      Logging.info_f ~section "Cleaned %d stale orders for %s" (List.length !stale) symbol
  ) all_symbols;

  (* Eliminate non-functional identifiers inhabiting the global eviction queue.
     Queue operations implement constant-time algorithms which do not allow
     rapid unmapping functions when variables undergo cancellation events.
     Executing this deterministic purge logic synchronizes the chronological
     queue array with the true operational properties of the hash mappings. *)
  Mutex.lock initialization_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
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
    end
  )

let request_cleanup () =
  Lwt.async (fun () ->
    Lwt.catch
      (fun () ->
        Logging.debug_f ~section "Running Lighter executions cleanup (requested)";
        cleanup_stale_orders ();
        Lwt.return_unit)
      (fun exn ->
        Logging.error_f ~section "Failed immediate cleanup task: %s" (Printexc.to_string exn);
        Lwt.return_unit))

let periodic_tasks_started = Atomic.make false

let start_periodic_tasks () =
  if not (Atomic.exchange periodic_tasks_started true) then begin
    ignore (Concurrency.Lwt_util.run_periodic
      ~interval:120.0
      ~stop:(fun () -> false)
      (fun () ->
        Lwt.catch (fun () ->
          Logging.debug_f ~section "Running Lighter executions cleanup (120s fallback)";
          cleanup_stale_orders ();
          Lwt.return_unit
        ) (fun exn ->
          Logging.error_f ~section "Lighter executions cleanup task crashed: %s" (Printexc.to_string exn);
          Lwt.return_unit
        )
      ))
  end

(** Computes a unified collection of valid operational instances joined
    with explicit chronological constraints. Utilized by tracking daemons
    to process cancellation logic over impending constraints. *)
let get_all_open_orders_with_expiry () =
  Mutex.lock initialization_mutex;
  let all_symbols = Fun.protect ~finally:(fun () -> Mutex.unlock initialization_mutex) (fun () ->
    Hashtbl.fold (fun symbol _ acc -> symbol :: acc) stores []
  ) in
  let result = ref [] in
  List.iter (fun symbol ->
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock store.orders_mutex) (fun () ->
      Hashtbl.iter (fun _order_id (order : open_order) ->
        result := order :: !result
      ) store.open_orders
    )
  ) all_symbols;
  !result

let initialize symbols =
  start_periodic_tasks ();
  Logging.info_f ~section "Initializing Lighter executions feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = get_symbol_store symbol in
    Logging.debug_f ~section "Created Lighter execution store for %s" symbol
  ) symbols
