(** Provides order status and execution tracking through the ingestion of orderStatus, openOrder, and execDetails messages.
    Explicit market data subscriptions are not required. The Interactive Brokers gateway automatically streams these execution events when orders are placed or modified. This module also initializes the target state by requesting open order snapshots through reqOpenOrders and reqExecutions during the system startup phase. *)

let section = "ibkr_executions"

module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Defines the canonical structure for an execution event, which encapsulates data for either a partial fill, full execution, or an order status transition. *)
type execution_event = {
  order_id: string;
  symbol: string;
  side: string;           (** Indicates the order direction, strictly normalized to "BUY" or "SELL". *)
  status: Ibkr_types.tws_order_status;
  filled_qty: float;
  remaining_qty: float;
  avg_fill_price: float;
  last_fill_price: float;
  last_fill_qty: float;
  timestamp: float;
}

(** Represents the materialized state of an active order within the Interactive Brokers system, tracking filled quantities, remaining allocations, and limit pricing parameters. *)
type open_order = {
  oo_order_id: string;
  oo_symbol: string;
  oo_side: string;
  oo_qty: float;
  oo_filled_qty: float;
  oo_remaining_qty: float;
  oo_limit_price: float option;
  oo_status: Ibkr_types.tws_order_status;
  oo_last_updated: float;
}

(** Defines the execution storage structure partitioned by asset symbol. It encapsulates a ring buffer for discrete execution events, a hashtable for tracking active orders, an atomic readiness flag, and a mutex for concurrency control over the internal state mutations. *)
type symbol_store = {
  events_buffer: execution_event RingBuffer.t;
  open_orders: (string, open_order) Hashtbl.t;
  ready: bool Atomic.t;
  orders_mutex: Mutex.t;
}

let symbol_stores : (string, symbol_store) Hashtbl.t = Hashtbl.create 32
let initialization_mutex = Mutex.create ()
let ready_condition = Lwt_condition.create ()

(** A global mapping that correlates system order identifiers to their corresponding asset symbols, facilitating multi-symbol order tracking and execution routing. *)
let order_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 64
let global_mutex = Mutex.create ()

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
              events_buffer = RingBuffer.create Ibkr_types.default_ring_buffer_size_executions;
              open_orders = Hashtbl.create 16;
              ready = Atomic.make false;
              orders_mutex = Mutex.create ();
            } in
            Hashtbl.replace symbol_stores symbol s;
            s
      in
      Mutex.unlock initialization_mutex;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition ()
     with _ -> ())
  end

(** Injects a new mapping into the global order correlation index, binding a local order identifier to its specific asset symbol. This operation is synchronized via the global index mutex. *)
let register_order ~order_id ~symbol =
  Mutex.lock global_mutex;
  Hashtbl.replace order_to_symbol order_id symbol;
  Mutex.unlock global_mutex

(** Queries the global correlation index to retrieve the asset symbol associated with a given order identifier. Secures access to the shared index using the global mutex. *)
let resolve_symbol order_id =
  Mutex.lock global_mutex;
  let r = Hashtbl.find_opt order_to_symbol order_id in
  Mutex.unlock global_mutex;
  r

(** Purges an order identifier from the global correlation index. This lifecycle hook is executed exclusively when an order transitions to a terminal state, mitigating unbounded memory growth. *)
let unregister_order ~order_id =
  Mutex.lock global_mutex;
  Hashtbl.remove order_to_symbol order_id;
  Mutex.unlock global_mutex

(** Processes an incoming execution event to mutate the tracked open order state for a given symbol. It evaluates the terminality conditions and either evicts the completed order from the localized datastore or updates the filled and remaining quantities. *)
let update_open_orders symbol (event : execution_event) =
  let store = get_symbol_store symbol in
  let is_terminal = match event.status with
    | Ibkr_types.Filled | Cancelled | Inactive | ApiCancelled -> true
    | _ -> false
  in
  Mutex.lock store.orders_mutex;
  if is_terminal || event.remaining_qty <= 0.0 then begin
    Hashtbl.remove store.open_orders event.order_id;
    (* Purge the completed order allocation from the global tracking index to ensure stable memory utilization and prevent hash table collisions over extended runtimes. *)
    (try unregister_order ~order_id:(int_of_string event.order_id)
     with _ -> ())
  end else begin
    let limit_price = match Hashtbl.find_opt store.open_orders event.order_id with
      | Some existing -> existing.oo_limit_price
      | None -> None
    in
    Hashtbl.replace store.open_orders event.order_id {
      oo_order_id = event.order_id;
      oo_symbol = symbol;
      oo_side = event.side;
      oo_qty = event.filled_qty +. event.remaining_qty;
      oo_filled_qty = event.filled_qty;
      oo_remaining_qty = event.remaining_qty;
      oo_limit_price = limit_price;
      oo_status = event.status;
      oo_last_updated = event.timestamp;
    }
  end;
  Mutex.unlock store.orders_mutex;
  (* Write to ring buffer *)
  RingBuffer.write store.events_buffer event;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol

(** Decodes and processes incoming orderStatus payloads from the Interactive Brokers gateway. Extracts critical execution metrics including fill quantities, remaining sizes, and average execution prices. The sequence of expected fields includes version, order identifier, categorical status, filled size, remaining size, and execution pricing parameters. *)
let handle_order_status fields =
  let order_id, fields = Ibkr_codec.read_int fields in
  let status_str, fields = Ibkr_codec.read_string fields in
  let filled, fields = Ibkr_codec.read_float fields in
  let remaining, fields = Ibkr_codec.read_float fields in
  let avg_fill_price, fields = Ibkr_codec.read_float fields in
  let _perm_id, fields = Ibkr_codec.read_int fields in
  let _parent_id, fields = Ibkr_codec.read_int fields in
  let last_fill_price, fields = Ibkr_codec.read_float fields in
  let _client_id, fields = Ibkr_codec.read_int fields in
  let _why_held, _fields = Ibkr_codec.read_string fields in

  let status = Ibkr_types.parse_tws_order_status status_str in

  match resolve_symbol order_id with
  | Some symbol ->
      let event = {
        order_id = string_of_int order_id;
        symbol;
        side = "";  (* The orderStatus message omits the underlying side parameter. A temporary placeholder is utilized until the target side is retrieved from the cached open order state. *)
        status;
        filled_qty = filled;
        remaining_qty = remaining;
        avg_fill_price;
        last_fill_price;
        last_fill_qty = 0.0;  (* The discrete fill quantity is absent from the orderStatus payload and defaults to zero in this execution context. *)
        timestamp = Unix.gettimeofday ();
      } in
      (* Resolve the definitive order side by interrogating the cached state within the symbol specific localized store. *)
      let store = get_symbol_store symbol in
      Mutex.lock store.orders_mutex;
      let side = match Hashtbl.find_opt store.open_orders (string_of_int order_id) with
        | Some oo -> oo.oo_side
        | None -> "BUY"
      in
      Mutex.unlock store.orders_mutex;
      let event = { event with side } in
      update_open_orders symbol event;

      Logging.info_f ~section "Order %d [%s]: %s filled=%.2f remaining=%.2f avgPrice=%.4f"
        order_id symbol status_str filled remaining avg_fill_price;

      (* Dispatch the terminal execution payload to the centralized event bus, facilitating multi-consumer telemetry propagation and external notification mechanisms. *)
      (if status = Ibkr_types.Filled then begin
        let fill_value = filled *. avg_fill_price in
        let maker_fee_rate =
          match Dio_exchange.Exchange_intf.Registry.get "ibkr" with
          | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
              (match Ex.get_fees ~symbol with (Some f, _) -> f | _ -> 0.0)
          | None -> 0.0
        in
        let fee = fill_value *. maker_fee_rate in
        Concurrency.Fill_event_bus.publish_fill {
          venue = "ibkr";
          symbol;
          side = (if side = "BUY" then "buy" else "sell");
          amount = filled;
          fill_price = avg_fill_price;
          value = fill_value;
          fee;
          timestamp = Unix.gettimeofday ();
          order_id = string_of_int order_id;
          trade_id = string_of_int order_id;  (* The Interactive Brokers API lacks an explicit identifier for discrete trades. The canonical order identifier is appropriated for this purpose. *)
        }
      end)
  | None ->
      Logging.debug_f ~section "orderStatus for unknown order %d (status=%s)" order_id status_str

(** Ingests and evaluates incoming openOrder payloads. This deserialization path is critical for obtaining structural order properties, such as the limit price and execution side, which are intrinsically absent from standard orderStatus messages. *)
let handle_open_order fields =
  let order_id, fields = Ibkr_codec.read_int fields in
  (* Contract Parameter Deserialization Segment *)
  let con_id, fields = Ibkr_codec.read_int fields in
  let symbol, fields = Ibkr_codec.read_string fields in
  let _sec_type, fields = Ibkr_codec.read_string fields in
  let _last_trade_date, fields = Ibkr_codec.read_string fields in
  let _strike, fields = Ibkr_codec.read_float fields in
  let _right, fields = Ibkr_codec.read_string fields in
  let _multiplier, fields = Ibkr_codec.read_string fields in
  let _exchange, fields = Ibkr_codec.read_string fields in
  let _currency, fields = Ibkr_codec.read_string fields in
  let _local_symbol, fields = Ibkr_codec.read_string fields in
  let _trading_class, fields = Ibkr_codec.read_string fields in
  (* Order Parameter Deserialization Segment *)
  let action, fields = Ibkr_codec.read_string fields in
  let total_qty, fields = Ibkr_codec.read_float fields in
  let order_type, fields = Ibkr_codec.read_string fields in
  let lmt_price, fields = Ibkr_codec.read_float fields in
  let _aux_price, fields = Ibkr_codec.read_float fields in
  let _tif, fields = Ibkr_codec.read_string fields in
  let _oca_group, fields = Ibkr_codec.read_string fields in
  let _account, fields = Ibkr_codec.read_string fields in
  let _open_close, fields = Ibkr_codec.read_string fields in
  let _origin, fields = Ibkr_codec.read_int fields in
  let _order_ref, fields = Ibkr_codec.read_string fields in
  let _client_id, fields = Ibkr_codec.read_int fields in
  let _perm_id, fields = Ibkr_codec.read_int fields in
  let _outside_rth, fields = Ibkr_codec.read_int fields in
  let _hidden, fields = Ibkr_codec.read_int fields in
  let _discretionary_amt, fields = Ibkr_codec.read_float fields in
  let _good_after_time, fields = Ibkr_codec.read_string fields in
  let _deprecated, fields = Ibkr_codec.read_string fields in  (* Bypass deprecated parameter block to maintain payload alignment. *)
  let _fa_group, fields = Ibkr_codec.read_string fields in
  let _fa_method, fields = Ibkr_codec.read_string fields in
  let _fa_percentage, fields = Ibkr_codec.read_string fields in
  let _fa_profile, fields = Ibkr_codec.read_string fields in
  let _model_code, fields = Ibkr_codec.read_string fields in
  let _good_till_date, _fields = Ibkr_codec.read_string fields in
  (* Internal telemetry requirements are satiated. The function legitimately terminates deserialization early and skips the tail parameters of the openOrder payload. *)
  ignore con_id;

  (* Bind the retrieved order identifier to the localized symbol within the global tracking registry. *)
  register_order ~order_id ~symbol;

  let limit_price = if order_type = "LMT" then Some lmt_price else None in
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let existing = Hashtbl.find_opt store.open_orders (string_of_int order_id) in
  let oo = match existing with
    | Some oo ->
        { oo with
          oo_side = action;
          oo_qty = total_qty;
          oo_limit_price = limit_price;
          oo_last_updated = Unix.gettimeofday ();
        }
    | None ->
        { oo_order_id = string_of_int order_id;
          oo_symbol = symbol;
          oo_side = action;
          oo_qty = total_qty;
          oo_filled_qty = 0.0;
          oo_remaining_qty = total_qty;
          oo_limit_price = limit_price;
          oo_status = Ibkr_types.Submitted;
          oo_last_updated = Unix.gettimeofday ();
        }
  in
  Hashtbl.replace store.open_orders (string_of_int order_id) oo;
  Mutex.unlock store.orders_mutex;

  Logging.debug_f ~section "Open order: %d %s %s %.2f @ %s [%s]"
    order_id action symbol total_qty
    (match limit_price with Some p -> Printf.sprintf "%.4f" p | None -> "MKT")
    order_type

(** Decodes and records execDetails messages signifying a realized discrete trade execution. Incorporates realized transaction price and volume into the cached state. *)
let handle_exec_details fields =
  let _req_id, fields = Ibkr_codec.read_int fields in
  let order_id, fields = Ibkr_codec.read_int fields in
  (* Contract Parameter Deserialization Segment *)
  let _con_id, fields = Ibkr_codec.read_int fields in
  let symbol, fields = Ibkr_codec.read_string fields in
  let _sec_type, fields = Ibkr_codec.read_string fields in
  let _last_trade_date, fields = Ibkr_codec.read_string fields in
  let _strike, fields = Ibkr_codec.read_float fields in
  let _right, fields = Ibkr_codec.read_string fields in
  let _multiplier, fields = Ibkr_codec.read_string fields in
  let _exchange, fields = Ibkr_codec.read_string fields in
  let _currency, fields = Ibkr_codec.read_string fields in
  let _local_symbol, fields = Ibkr_codec.read_string fields in
  let _trading_class, fields = Ibkr_codec.read_string fields in
  (* Execution Parameter Deserialization Segment *)
  let _exec_id, fields = Ibkr_codec.read_string fields in
  let _time, fields = Ibkr_codec.read_string fields in
  let _acct_number, fields = Ibkr_codec.read_string fields in
  let _exchange_exec, fields = Ibkr_codec.read_string fields in
  let side, fields = Ibkr_codec.read_string fields in
  let shares, fields = Ibkr_codec.read_float fields in
  let price, fields = Ibkr_codec.read_float fields in
  let _perm_id, fields = Ibkr_codec.read_int fields in
  let _client_id, fields = Ibkr_codec.read_int fields in
  let _liquidation, fields = Ibkr_codec.read_int fields in
  let cum_qty, fields = Ibkr_codec.read_float fields in
  let avg_price, _fields = Ibkr_codec.read_float fields in

  register_order ~order_id ~symbol;

  let event = {
    order_id = string_of_int order_id;
    symbol;
    side = (if side = "BOT" then "BUY" else "SELL");
    status = Ibkr_types.Submitted;  (* The execDetails payload structurally lacks order status data. A submitted status is forced to facilitate further state machine evaluation. *)
    filled_qty = cum_qty;
    remaining_qty = 0.0;  (* Accurate remainder derivation is deferred to subsequent orderStatus event integrations. *)
    avg_fill_price = avg_price;
    last_fill_price = price;
    last_fill_qty = shares;
    timestamp = Unix.gettimeofday ();
  } in
  update_open_orders symbol event;

  Logging.info_f ~section "Execution: order %d [%s] %s %.2f shares @ %.4f (cum: %.2f @ %.4f)"
    order_id symbol (if side = "BOT" then "BUY" else "SELL")
    shares price cum_qty avg_price

(** Injects the module specific execution decoders into the centralized connection message dispatcher. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_order_status
    ~handler:handle_order_status;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_open_order
    ~handler:handle_open_order;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_execution_data
    ~handler:handle_exec_details

(** Broadcasts an explicit reqOpenOrders directive to the active TCP connection. Instigates a gateway response containing the comprehensive initial snapshot of all active system orders. *)
let request_open_orders conn =
  Logging.info ~section "Requesting open orders snapshot";
  Ibkr_connection.send conn [
    string_of_int Ibkr_types.msg_req_open_orders;
    "1";  (* version *)
  ]

(* ---- Public Mutators and Accessors ---- *)

let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let orders = Hashtbl.fold (fun _id order acc -> order :: acc) store.open_orders [] in
  Mutex.unlock store.orders_mutex;
  orders

let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let r = Hashtbl.find_opt store.open_orders order_id in
  Mutex.unlock store.orders_mutex;
  r

let[@inline always] fold_open_orders symbol ~init ~f =
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let result = Hashtbl.fold (fun _id order acc -> f acc order) store.open_orders init in
  Mutex.unlock store.orders_mutex;
  result

let[@inline always] read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos

let[@inline always] iter_execution_events symbol last_pos f =
  let store = get_symbol_store symbol in
  RingBuffer.iter_since store.events_buffer last_pos f

let[@inline always] get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer

let[@inline always] has_execution_data symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.ready

(** Iterates the shared registry and forcefully asserts the readiness constraint across all initialized symbol stores. This function is triggered by the openOrderEnd message, concluding the snapshot synchronization sequence. This logic averts initialization deadlocks initiated by the domain spawner waiting on inactive symbol executions. *)
let mark_ready_all () =
  Hashtbl.iter (fun symbol store ->
    if not (Atomic.get store.ready) then begin
      Logging.debug_f ~section "Marking executions feed ready for %s (snapshot complete)" symbol;
      notify_ready store
    end
  ) symbol_stores

(** Allocates memory structures and primes localized symbol stores preceding the initialization of dispatch handlers and active execution telemetry streams. *)
let initialize symbols =
  Logging.info_f ~section "Initializing executions feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = get_symbol_store symbol in ()
  ) symbols;
  register_handlers ();
  Logging.info ~section "Executions feed initialized"
