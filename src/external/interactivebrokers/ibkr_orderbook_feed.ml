(** Level 2 order book feed implementation routing reqMktDepth responses into ring buffers.

    This module maintains sorted bid and ask array structures per symbol in memory.
    It synchronously processes operations (insert, update, delete) on price levels
    and writes point in time orderbook snapshots to lock free ring buffers. *)

let section = "ibkr_orderbook"

(** Represents a consolidated price level within the order book containing the quote price and available liquidity size. *)
type level = {
  price: float;
  size: float;
}

(** Complete point in time snapshot of the order book containing bidirectional depth and a timestamp for reconciliation. *)
type orderbook = {
  bids: level array;
  asks: level array;
  timestamp: float;
}

module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** In-memory state structure maintaining the active ring buffer, thread-safe readiness flags, and mutable level arrays for a specific symbol. *)
type store = {
  buffer: orderbook RingBuffer.t;
  ready: bool Atomic.t;
  mutable bids: level array;
  mutable asks: level array;
  depth: int;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()

(** Global mapping linking Interactive Brokers request IDs to their corresponding instrument symbols for response routing. *)
let req_id_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 32
let next_req_id = Atomic.make 2000

(** Clears stale request ID to symbol mappings. This function is invoked prior to socket reconnection to prevent orphaned state entries from accumulating across connection cycles. *)
let clear_req_ids () =
  Hashtbl.clear req_id_to_symbol

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let depth = Ibkr_types.default_orderbook_depth in
      let store = {
        buffer = RingBuffer.create Ibkr_types.default_ring_buffer_size_orderbook;
        ready = Atomic.make false;
        bids = Array.make depth { price = 0.0; size = 0.0 };
        asks = Array.make depth { price = 0.0; size = 0.0 };
        depth;
      } in
      Hashtbl.replace stores symbol store;
      store

let store_opt symbol = Hashtbl.find_opt stores symbol

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition ()
     with _ -> ())
  end

(** Materializes the current bid and ask arrays into an immutable snapshot and flushes it to the designated ring buffer. Triggers internal readiness conditions and broadcasts to exchange sleepers. *)
let flush_orderbook symbol store =
  let ob = {
    bids = Array.copy store.bids;
    asks = Array.copy store.asks;
    timestamp = Unix.gettimeofday ();
  } in
  RingBuffer.write store.buffer ob;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol

(** Processes incoming updateMktDepth gateway messages.
    Parses the binary string fields including version, request ID, absolute position, operation type, side, price, and size. Mutates the store arrays synchronously and triggers a ring buffer flush. *)
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
      if position >= 0 && position < store.depth then begin
        (match operation with
         | 0 (* insert *) | 1 (* update *) ->
             arr.(position) <- { price; size }
         | 2 (* delete *) ->
             arr.(position) <- { price = 0.0; size = 0.0 }
         | _ -> ());
        flush_orderbook symbol store
      end

(** Initiates a Level 2 order book subscription for the given symbol.
    For standard equity assets (STK/ETF) routed through SMART, deep market data is unsupported.
    In these unsupported cases, the module skips dispatching the gateway request and immediately marks the store as ready, as subsequent pipeline stages safely fall back to top of book ticker data. *)
let subscribe conn ~contract =
  let symbol = contract.Ibkr_types.symbol in
  let store = ensure_store symbol in
  if contract.sec_type = "STK" then begin
    (* STK/ETF on SMART doesn't support L2 depth: skip *)
    Logging.info_f ~section "Skipping L2 orderbook for %s (STK on %s not supported)"
      symbol contract.exchange;
    notify_ready store;
    Lwt.return_unit
  end else begin
    let req_id = Atomic.fetch_and_add next_req_id 1 in
    Hashtbl.replace req_id_to_symbol req_id symbol;
    Logging.info_f ~section "Subscribing to orderbook for %s (reqId=%d, depth=%d)"
      symbol req_id Ibkr_types.default_orderbook_depth;

    let msg_fields =
      [string_of_int Ibkr_types.msg_req_mkt_depth;
       "5";    (* version *)
       string_of_int req_id]
      @ Ibkr_codec.encode_contract_short contract
      @ [
        string_of_int Ibkr_types.default_orderbook_depth;
        "0";    (* isSmartDepth *)
        "";     (* mktDepthOptions *)
      ]
    in
    Ibkr_connection.send conn msg_fields
  end

(** Registers the incoming market depth message translation callbacks with the global socket dispatcher mapping. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_market_depth
    ~handler:handle_market_depth;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_market_depth_l2
    ~handler:handle_market_depth  (* L2 uses same structure *)

(* ---- Public accessors ---- *)

let[@inline always] read_orderbook_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

let[@inline always] iter_orderbook_events symbol last_pos f =
  match store_opt symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

(** Pre-allocates memory footprint for orderbook stores across all requested symbols and primes the dispatcher message callbacks. *)
let initialize symbols =
  Logging.info_f ~section "Initializing orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in ()
  ) symbols;
  register_handlers ();
  Logging.info ~section "Orderbook feed initialized"
