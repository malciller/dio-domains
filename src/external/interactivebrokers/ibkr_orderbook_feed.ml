(** Level 2 order book feed: routes reqMktDepth updates into per-symbol
    sorted bid/ask arrays and publishes snapshots to lock-free ring
    buffers. Falls back to L1 tickPrice/tickSize for symbols without
    depth data. *)

let section = "ibkr_orderbook"

(** One price level. *)
type level =
  { price : float
  ; size : float
  }

(** Order book snapshot with timestamp. *)
type orderbook =
  { bids : level array
  ; asks : level array
  ; timestamp : float
  }

module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol state: ring buffer, readiness flag, mutable level arrays. *)
type store =
  { buffer : orderbook RingBuffer.t
  ; ready : bool Atomic.t
  ; mutable bids : level array
  ; mutable asks : level array
  ; depth : int
  }

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()

(** reqId -> symbol routing for market data responses. *)
let req_id_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 32

let next_req_id = Atomic.make 2000

(** Clears reqId mappings; called before reconnecting. *)
let clear_req_ids () = Hashtbl.clear req_id_to_symbol

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
    let depth = Ibkr_types.default_orderbook_depth in
    let store =
      { buffer = RingBuffer.create Ibkr_types.default_ring_buffer_size_orderbook
      ; ready = Atomic.make false
      ; bids = Array.make depth { price = 0.0; size = 0.0 }
      ; asks = Array.make depth { price = 0.0; size = 0.0 }
      ; depth
      }
    in
    Hashtbl.replace stores symbol store;
    store
;;

let store_opt symbol = Hashtbl.find_opt stores symbol

let notify_ready store =
  if not (Atomic.get store.ready)
  then (
    Atomic.set store.ready true;
    try Lwt_condition.broadcast ready_condition () with
    | _ -> ())
;;

(** Copies the current levels into a snapshot, writes it to the ring
    buffer, and signals readiness and exchange sleepers. *)
let flush_orderbook symbol store =
  let ob =
    { bids = Array.copy store.bids
    ; asks = Array.copy store.asks
    ; timestamp = Unix.gettimeofday ()
    }
  in
  RingBuffer.write store.buffer ob;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol
;;

(** updateMktDepth handler. Fields: version, reqId, position, operation,
    side, price, size. Applies insert/update/delete to the level array
    and flushes a snapshot. *)
let handle_market_depth fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let req_id, fields = Ibkr_codec.read_int fields in
  let position, fields = Ibkr_codec.read_int fields in
  let operation, fields = Ibkr_codec.read_int fields in
  let side, fields = Ibkr_codec.read_int fields in
  let price, fields = Ibkr_codec.read_float fields in
  let size, _fields = Ibkr_codec.read_float fields in
  match Hashtbl.find_opt req_id_to_symbol req_id with
  | None -> ()
  | Some symbol ->
    let store = ensure_store symbol in
    let arr = if side = 1 then store.bids else store.asks in
    if position >= 0 && position < store.depth
    then (
      (match operation with
       | 0 (* insert *) | 1 (* update *) -> arr.(position) <- { price; size }
       | 2 (* delete *) -> arr.(position) <- { price = 0.0; size = 0.0 }
       | _ -> ());
      flush_orderbook symbol store)
;;

(** tickPrice handler (L1 fallback). Updates the top-of-book quote and
    flushes. *)
let handle_tick_price fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let req_id, fields = Ibkr_codec.read_int fields in
  let tick_type, fields = Ibkr_codec.read_int fields in
  let price, fields = Ibkr_codec.read_float fields in
  let size, _fields = Ibkr_codec.read_float fields in
  Logging.debug_f
    ~section
    "TickPrice received (req_id=%d): tick_type=%d price=%f size=%f"
    req_id
    tick_type
    price
    size;
  match Hashtbl.find_opt req_id_to_symbol req_id with
  | None -> ()
  | Some symbol ->
    let store = ensure_store symbol in
    let updated = ref false in
    if tick_type = Ibkr_types.tick_bid || tick_type = Ibkr_types.tick_delayed_bid
    then (
      let curr_size = if size > 0.0 then size else store.bids.(0).size in
      store.bids.(0) <- { price; size = curr_size };
      updated := true)
    else if tick_type = Ibkr_types.tick_ask || tick_type = Ibkr_types.tick_delayed_ask
    then (
      let curr_size = if size > 0.0 then size else store.asks.(0).size in
      store.asks.(0) <- { price; size = curr_size };
      updated := true)
    else if
      tick_type = Ibkr_types.tick_last
      || tick_type = Ibkr_types.tick_delayed_last
      || tick_type = Ibkr_types.tick_close
      || tick_type = 75 (* delayed close *)
      || tick_type = 14
      (* open *) || tick_type = 76 (* delayed open *)
      || tick_type = 37 (* mark price *)
    then (
      (* Pre-market / delayed-frozen: seed last/close into both sides
         as a zero-spread approximation when quotes are missing. *)
      if store.bids.(0).price = 0.0 && price > 0.0
      then (
        store.bids.(0) <- { price; size = store.bids.(0).size };
        updated := true);
      if store.asks.(0).price = 0.0 && price > 0.0
      then (
        store.asks.(0) <- { price; size = store.asks.(0).size };
        updated := true))
    else
      Logging.debug_f
        ~section
        "Ignored tick_type=%d price=%f size=%f"
        tick_type
        price
        size;
    if !updated then flush_orderbook symbol store
;;

(** tickSize handler (L1 fallback). Updates the top-of-book size and
    flushes. *)
let handle_tick_size fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let req_id, fields = Ibkr_codec.read_int fields in
  let tick_type, fields = Ibkr_codec.read_int fields in
  let size, _fields = Ibkr_codec.read_float fields in
  Logging.debug_f
    ~section
    "TickSize received (req_id=%d): tick_type=%d size=%f"
    req_id
    tick_type
    size;
  match Hashtbl.find_opt req_id_to_symbol req_id with
  | None -> ()
  | Some symbol ->
    let store = ensure_store symbol in
    let updated = ref false in
    if
      tick_type = Ibkr_types.tick_bid_size || tick_type = Ibkr_types.tick_delayed_bid_size
    then (
      store.bids.(0) <- { price = store.bids.(0).price; size };
      updated := true)
    else if
      tick_type = Ibkr_types.tick_ask_size || tick_type = Ibkr_types.tick_delayed_ask_size
    then (
      store.asks.(0) <- { price = store.asks.(0).price; size };
      updated := true);
    if !updated then flush_orderbook symbol store
;;

(** One-off reqMktData snapshot to seed delayed/frozen close prices. *)
let request_snapshot conn ~contract =
  let symbol = contract.Ibkr_types.symbol in
  let _store = ensure_store symbol in
  let req_id = Atomic.fetch_and_add next_req_id 1 in
  Hashtbl.replace req_id_to_symbol req_id symbol;
  Logging.info_f ~section "Requesting L1 snapshot seed for %s (reqId=%d)" symbol req_id;
  let msg_fields =
    [ string_of_int Ibkr_types.msg_req_mkt_data
    ; "11"
    ; (* version *)
      string_of_int req_id
    ]
    @ Ibkr_codec.encode_contract_short contract
    @ [ "0"
      ; (* underComp *)
        "233,165"
      ; (* genericTickList *)
        "1"
      ; (* snapshot *)
        "0"
      ; (* regulatorySnapshot *)
        "" (* mktDataOptions *)
      ]
  in
  Ibkr_connection.send conn msg_fields
;;

(** Subscribes to market data for [contract]. STK/ETF via SMART has no
    L2 depth, so those get an L1 reqMktData ticker instead; other
    security types get reqMktDepth. *)
let subscribe conn ~contract =
  let symbol = contract.Ibkr_types.symbol in
  let _store = ensure_store symbol in
  if contract.sec_type = "STK"
  then (
    (* STK/ETF on SMART doesn't support L2 depth: fallback to L1 Top-Of-Book reqMktData *)
    let req_id = Atomic.fetch_and_add next_req_id 1 in
    Hashtbl.replace req_id_to_symbol req_id symbol;
    Logging.info_f
      ~section
      "Falling back to L1 ticker feed for %s (STK on %s L2 not supported)"
      symbol
      contract.exchange;
    let msg_fields =
      [ string_of_int Ibkr_types.msg_req_mkt_data
      ; "11"
      ; (* version *)
        string_of_int req_id
      ]
      @ Ibkr_codec.encode_contract_short contract
      @ [ "0"
        ; (* underComp *)
          "233,165"
        ; (* genericTickList *)
          "0"
        ; (* snapshot *)
          "0"
        ; (* regulatorySnapshot *)
          "" (* mktDataOptions *)
        ]
    in
    Ibkr_connection.send conn msg_fields)
  else (
    let req_id = Atomic.fetch_and_add next_req_id 1 in
    Hashtbl.replace req_id_to_symbol req_id symbol;
    Logging.info_f
      ~section
      "Subscribing to orderbook for %s (reqId=%d, depth=%d)"
      symbol
      req_id
      Ibkr_types.default_orderbook_depth;
    let msg_fields =
      [ string_of_int Ibkr_types.msg_req_mkt_depth
      ; "5"
      ; (* version *)
        string_of_int req_id
      ]
      @ Ibkr_codec.encode_contract_short contract
      @ [ string_of_int Ibkr_types.default_orderbook_depth
        ; "0"
        ; (* isSmartDepth *)
          "" (* mktDepthOptions *)
        ]
    in
    Ibkr_connection.send conn msg_fields)
;;

(** Registers the depth and tick handlers with the dispatcher. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_market_depth
    ~handler:handle_market_depth;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_market_depth_l2
    ~handler:handle_market_depth;
  (* L2 uses same structure *)
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_tick_price
    ~handler:handle_tick_price;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_tick_size
    ~handler:handle_tick_size
;;

(* ---- Public accessors ---- *)

let[@inline always] read_orderbook_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []
;;

let[@inline always] iter_orderbook_events symbol last_pos f =
  match store_opt symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos
;;

let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0
;;

let[@inline always] get_current_position_fast symbol =
  let store = ensure_store symbol in
  fun () -> RingBuffer.get_position store.buffer
;;

(** Pre-creates stores for [symbols] and registers handlers. *)
let initialize symbols =
  Logging.info_f
    ~section
    "Initializing orderbook feed for %d symbols"
    (List.length symbols);
  List.iter
    (fun symbol ->
       let _ = ensure_store symbol in
       ())
    symbols;
  register_handlers ();
  Logging.info ~section "Orderbook feed initialized"
;;
