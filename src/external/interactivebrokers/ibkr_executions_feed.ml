(** Order status and execution tracking from orderStatus, openOrder, and
    execDetails messages; no explicit market data subscription is needed,
    TWS streams these automatically for orders placed by this client.
    On startup the module requests an open-order snapshot via
    reqOpenOrders (sent by the supervisor feeds) to prime state. *)

let section = "ibkr_executions"

module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Execution event: a fill (partial or full) or an order status transition. *)
type execution_event =
  { order_id : string
  ; symbol : string
  ; side : string (** Normalized to "BUY" or "SELL". *)
  ; status : Ibkr_types.tws_order_status
  ; filled_qty : float
  ; remaining_qty : float
  ; avg_fill_price : float
  ; last_fill_price : float
  ; last_fill_qty : float
  ; timestamp : float
  }

(** Live state of one open order at IBKR. *)
type open_order =
  { oo_order_id : string
  ; oo_symbol : string
  ; oo_side : string
  ; oo_qty : float
  ; oo_filled_qty : float
  ; oo_remaining_qty : float
  ; oo_limit_price : float option
  ; oo_status : Ibkr_types.tws_order_status
  ; oo_last_updated : float
  }

(** Per-symbol storage: event ring buffer, open-order table, atomic
    snapshot cache, readiness flag, and mutex. *)
type symbol_store =
  { events_buffer : execution_event RingBuffer.t
  ; open_orders : (string, open_order) Hashtbl.t
  ; open_orders_cache : open_order list Atomic.t
    (** Lock-free atomic snapshot cache of active open orders for the domain hotpath. *)
  ; ready : bool Atomic.t
  ; orders_mutex : Mutex.t
  }

let symbol_stores : (string, symbol_store) Hashtbl.t = Hashtbl.create 32
let initialization_mutex = Mutex.create ()
let ready_condition = Lwt_condition.create ()

(** Order id -> symbol map used to route execution messages. *)
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
        let s =
          { events_buffer =
              RingBuffer.create Ibkr_types.default_ring_buffer_size_executions
          ; open_orders = Hashtbl.create 16
          ; open_orders_cache = Atomic.make []
          ; ready = Atomic.make false
          ; orders_mutex = Mutex.create ()
          }
        in
        Hashtbl.replace symbol_stores symbol s;
        s
    in
    Mutex.unlock initialization_mutex;
    store
;;

(** Publishes an immutable snapshot of open_orders to the atomic cache.
    Must be called by writers under store.orders_mutex. *)
let[@inline] publish_open_orders_cache store =
  let snapshot = Hashtbl.fold (fun _id order acc -> order :: acc) store.open_orders [] in
  Atomic.set store.open_orders_cache snapshot
;;

let notify_ready store =
  if not (Atomic.get store.ready)
  then (
    Atomic.set store.ready true;
    try Lwt_condition.broadcast ready_condition () with
    | _ -> ())
;;

(** Maps [order_id] to [symbol] (mutex-guarded). *)
let register_order ~order_id ~symbol =
  Mutex.lock global_mutex;
  Hashtbl.replace order_to_symbol order_id symbol;
  Mutex.unlock global_mutex
;;

(** Symbol for [order_id], or [None]. *)
let resolve_symbol order_id =
  Mutex.lock global_mutex;
  let r = Hashtbl.find_opt order_to_symbol order_id in
  Mutex.unlock global_mutex;
  r
;;

(** Removes [order_id] from the map; call when the order reaches a
    terminal state so long-running processes do not leak entries. *)
let unregister_order ~order_id =
  Mutex.lock global_mutex;
  Hashtbl.remove order_to_symbol order_id;
  Mutex.unlock global_mutex
;;

(** Applies an execution event to the tracked open-order state: removes
    terminal or fully filled orders, otherwise updates fill quantities. *)
let update_open_orders symbol (event : execution_event) =
  let store = get_symbol_store symbol in
  let is_terminal =
    match event.status with
    | Ibkr_types.Filled | Cancelled | Inactive | ApiCancelled -> true
    | _ -> false
  in
  Mutex.lock store.orders_mutex;
  if is_terminal || event.remaining_qty <= 0.0
  then (
    Hashtbl.remove store.open_orders event.order_id;
    (* Drop the id from the global id->symbol index so long-running
       processes do not accumulate stale entries. *)
    try unregister_order ~order_id:(int_of_string event.order_id) with
    | _ -> ())
  else (
    let limit_price =
      match Hashtbl.find_opt store.open_orders event.order_id with
      | Some existing -> existing.oo_limit_price
      | None -> None
    in
    Hashtbl.replace
      store.open_orders
      event.order_id
      { oo_order_id = event.order_id
      ; oo_symbol = symbol
      ; oo_side = event.side
      ; oo_qty = event.filled_qty +. event.remaining_qty
      ; oo_filled_qty = event.filled_qty
      ; oo_remaining_qty = event.remaining_qty
      ; oo_limit_price = limit_price
      ; oo_status = event.status
      ; oo_last_updated = event.timestamp
      });
  publish_open_orders_cache store;
  Mutex.unlock store.orders_mutex;
  RingBuffer.write store.events_buffer event;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol
;;

(** orderStatus handler. Fields: orderId, status, filled, remaining,
    avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld. *)
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
    let event =
      { order_id = string_of_int order_id
      ; symbol
      ; side =
          ""
          (* orderStatus carries no side; filled from the cached open
           order below. *)
      ; status
      ; filled_qty = filled
      ; remaining_qty = remaining
      ; avg_fill_price
      ; last_fill_price
      ; last_fill_qty = 0.0 (* no per-fill quantity in orderStatus *)
      ; timestamp = Unix.gettimeofday ()
      }
    in
    (* Recover the side from the per-symbol open-order store. *)
    let store = get_symbol_store symbol in
    Mutex.lock store.orders_mutex;
    let side =
      match Hashtbl.find_opt store.open_orders (string_of_int order_id) with
      | Some oo -> oo.oo_side
      | None -> "BUY"
    in
    Mutex.unlock store.orders_mutex;
    let event = { event with side } in
    update_open_orders symbol event;
    Logging.info_f
      ~section
      "Order %d [%s]: %s filled=%.2f remaining=%.2f avgPrice=%.4f"
      order_id
      symbol
      status_str
      filled
      remaining
      avg_fill_price;
    (* Publish fills on the shared event bus. *)
    if status = Ibkr_types.Filled
    then (
      let fill_value = filled *. avg_fill_price in
      let maker_fee_rate =
        match Dio_exchange.Exchange_intf.Registry.get "ibkr" with
        | Some (module Ex : Dio_exchange.Exchange_intf.S) ->
          (match Ex.get_fees ~symbol with
           | Some f, _ -> f
           | _ -> 0.0)
        | None -> 0.0
      in
      let fee = fill_value *. maker_fee_rate in
      Concurrency.Fill_event_bus.publish_fill
        { venue = "ibkr"
        ; symbol
        ; side = (if side = "BUY" then "buy" else "sell")
        ; amount = filled
        ; fill_price = avg_fill_price
        ; value = fill_value
        ; fee
        ; timestamp = Unix.gettimeofday ()
        ; order_id = string_of_int order_id
        ; trade_id =
            string_of_int order_id
            (* No per-trade id in the API; the order id stands in. *)
        })
  | None ->
    Logging.debug_f
      ~section
      "orderStatus for unknown order %d (status=%s)"
      order_id
      status_str
;;

(** openOrder handler. Supplies fields orderStatus lacks (side, limit
    price, quantity); most trailing fields are ignored. *)
let handle_open_order fields =
  let order_id, fields = Ibkr_codec.read_int fields in
  (* Contract fields *)
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
  (* Order fields *)
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
  let _deprecated, fields = Ibkr_codec.read_string fields in
  (* Deprecated field, still read to preserve field alignment. *)
  let _fa_group, fields = Ibkr_codec.read_string fields in
  let _fa_method, fields = Ibkr_codec.read_string fields in
  let _fa_percentage, fields = Ibkr_codec.read_string fields in
  let _fa_profile, fields = Ibkr_codec.read_string fields in
  let _model_code, fields = Ibkr_codec.read_string fields in
  let _good_till_date, _fields = Ibkr_codec.read_string fields in
  (* Only a subset of openOrder is needed; stop decoding here. *)
  ignore con_id;
  register_order ~order_id ~symbol;
  let limit_price = if order_type = "LMT" then Some lmt_price else None in
  let store = get_symbol_store symbol in
  Mutex.lock store.orders_mutex;
  let existing = Hashtbl.find_opt store.open_orders (string_of_int order_id) in
  let oo =
    match existing with
    | Some oo ->
      { oo with
        oo_side = action
      ; oo_qty = total_qty
      ; oo_limit_price = limit_price
      ; oo_last_updated = Unix.gettimeofday ()
      }
    | None ->
      { oo_order_id = string_of_int order_id
      ; oo_symbol = symbol
      ; oo_side = action
      ; oo_qty = total_qty
      ; oo_filled_qty = 0.0
      ; oo_remaining_qty = total_qty
      ; oo_limit_price = limit_price
      ; oo_status = Ibkr_types.Submitted
      ; oo_last_updated = Unix.gettimeofday ()
      }
  in
  Hashtbl.replace store.open_orders (string_of_int order_id) oo;
  publish_open_orders_cache store;
  Mutex.unlock store.orders_mutex;
  Logging.debug_f
    ~section
    "Open order: %d %s %s %.2f @ %s [%s]"
    order_id
    action
    symbol
    total_qty
    (match limit_price with
     | Some p -> Printf.sprintf "%.4f" p
     | None -> "MKT")
    order_type
;;

(** execDetails handler: records a discrete fill. Fields follow the
    contract block, then execId, time, account, exchange, side, shares,
    price, permId, clientId, liquidation, cumQty, avgPrice. *)
let handle_exec_details fields =
  let _req_id, fields = Ibkr_codec.read_int fields in
  let _order_id, fields = Ibkr_codec.read_int fields in
  let contract_id, fields = Ibkr_codec.read_int fields in
  let symbol, fields = Ibkr_codec.read_string fields in
  let _sec_type, fields = Ibkr_codec.read_string fields in
  let _expiry, fields = Ibkr_codec.read_string fields in
  let _strike, fields = Ibkr_codec.read_float fields in
  let _right, fields = Ibkr_codec.read_string fields in
  let _multiplier, fields = Ibkr_codec.read_string fields in
  let _exchange, fields = Ibkr_codec.read_string fields in
  let _currency, fields = Ibkr_codec.read_string fields in
  let _local_symbol, fields = Ibkr_codec.read_string fields in
  let _trading_class, fields = Ibkr_codec.read_string fields in
  let exec_id, fields = Ibkr_codec.read_string fields in
  let time, fields = Ibkr_codec.read_string fields in
  let account, fields = Ibkr_codec.read_string fields in
  let exec_exchange, fields = Ibkr_codec.read_string fields in
  let side, fields = Ibkr_codec.read_string fields in
  let shares, fields = Ibkr_codec.read_float fields in
  let price, fields = Ibkr_codec.read_float fields in
  let perm_id, fields = Ibkr_codec.read_int fields in
  let client_id, fields = Ibkr_codec.read_int fields in
  let _liquidation, fields = Ibkr_codec.read_int fields in
  let cum_qty, fields = Ibkr_codec.read_float fields in
  let avg_price, _ = Ibkr_codec.read_float fields in
  Logging.info_f
    ~section
    "Execution details: %s %s %.2f @ %.2f (order_id=%d, exec_id=%s, time=%s, account=%s)"
    side
    symbol
    shares
    price
    perm_id
    exec_id
    time
    account;
  let event : execution_event =
    { order_id = string_of_int perm_id
    ; symbol
    ; side = (if side = "BOT" then "BUY" else "SELL")
    ; status = Ibkr_types.Filled
    ; filled_qty = cum_qty
    ; remaining_qty = 0.0
    ; avg_fill_price = avg_price
    ; last_fill_price = price
    ; last_fill_qty = shares
    ; timestamp = Unix.gettimeofday ()
    }
  in
  update_open_orders symbol event;
  let execution : Ibkr_types.execution =
    { exec_id
    ; exec_order_id = perm_id
    ; exec_symbol = symbol
    ; exec_side = side
    ; exec_qty = shares
    ; exec_price = price
    ; exec_time = time
    ; exec_account = account
    ; exec_exchange
    }
  in
  let _ = execution in
  let _ = client_id in
  let _ = contract_id in
  ()
;;

(** Registers the orderStatus/openOrder/execDetails handlers with the
    dispatcher. *)
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
;;

(** Requests an execution history snapshot from the gateway. Unused by
    the current startup sequence (open orders are requested instead);
    kept for manual replay of the day's fills. *)
let request_executions conn =
  Logging.info ~section "Requesting execution history snapshot";
  Ibkr_connection.send
    conn
    [ string_of_int Ibkr_types.msg_req_executions
    ; "1" (* version *)
    ; "1" (* reqId *)
    ; "" (* clientId *)
    ; "" (* acctCode *)
    ; "" (* time *)
    ; "" (* symbol *)
    ; "" (* secType *)
    ; "" (* exchange *)
    ; "" (* side *)
    ]
;;

(** Sends reqOpenOrders; TWS replies with a snapshot of active orders
    followed by openOrderEnd. *)
let request_open_orders conn =
  Logging.info ~section "Requesting open orders snapshot";
  Ibkr_connection.send
    conn
    [ string_of_int Ibkr_types.msg_req_open_orders; "1" (* version *) ]
;;

(* ---- Public Mutators and Accessors ---- *)

(** Return all symbols that have initialized execution stores. *)
let get_all_symbols () =
  Mutex.lock initialization_mutex;
  let symbols = Hashtbl.fold (fun symbol _ acc -> symbol :: acc) symbol_stores [] in
  Mutex.unlock initialization_mutex;
  symbols
;;

(** Lock-free read of all open orders for a symbol from the atomic cache. *)
let[@inline always] get_open_orders symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.open_orders_cache
;;

(** Lock-free read of a single open order by ID from the atomic cache. *)
let[@inline always] get_open_order symbol order_id =
  let store = get_symbol_store symbol in
  let orders = Atomic.get store.open_orders_cache in
  List.find_opt (fun (o : open_order) -> o.oo_order_id = order_id) orders
;;

(** Lock-free fold over open orders from the atomic cache. *)
let[@inline always] fold_open_orders symbol ~init ~f =
  let store = get_symbol_store symbol in
  let snapshot = Atomic.get store.open_orders_cache in
  List.fold_left (fun acc order -> f acc order) init snapshot
;;

let[@inline always] read_execution_events symbol last_pos =
  let store = get_symbol_store symbol in
  RingBuffer.read_since store.events_buffer last_pos
;;

let[@inline always] iter_execution_events symbol last_pos f =
  let store = get_symbol_store symbol in
  RingBuffer.iter_since store.events_buffer last_pos f
;;

let[@inline always] get_current_position symbol =
  let store = get_symbol_store symbol in
  RingBuffer.get_position store.events_buffer
;;

let[@inline always] get_current_position_fast symbol =
  let store = get_symbol_store symbol in
  fun () -> RingBuffer.get_position store.events_buffer
;;

let[@inline always] has_execution_data symbol =
  let store = get_symbol_store symbol in
  Atomic.get store.ready
;;

let[@inline always] has_execution_data_fast symbol =
  let store = get_symbol_store symbol in
  fun () -> Atomic.get store.ready
;;

(** Marks every initialized symbol store ready. Called on openOrderEnd
    after the startup snapshot, so waiters do not block on symbols that
    simply have no executions yet. *)
let mark_ready_all () =
  Hashtbl.iter
    (fun symbol store ->
       if not (Atomic.get store.ready)
       then (
         Logging.debug_f
           ~section
           "Marking executions feed ready for %s (snapshot complete)"
           symbol;
         notify_ready store))
    symbol_stores
;;

(** Pre-creates stores for [symbols] and registers handlers. *)
let initialize symbols =
  Logging.info_f
    ~section
    "Initializing executions feed for %d symbols"
    (List.length symbols);
  List.iter
    (fun symbol ->
       let _ = get_symbol_store symbol in
       ())
    symbols;
  register_handlers ();
  Logging.info ~section "Executions feed initialized"
;;
