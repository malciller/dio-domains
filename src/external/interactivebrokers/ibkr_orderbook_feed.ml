(** L2 order book feed via reqMktDepth → ring buffers.

    Maintains sorted bid/ask arrays per symbol, writes orderbook
    snapshots to ring buffers on each update. *)

let section = "ibkr_orderbook"

(** Single price level in the order book. *)
type level = {
  price: float;
  size: float;
}

(** Orderbook snapshot. *)
type orderbook = {
  bids: level array;
  asks: level array;
  timestamp: float;
}

module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol order book state. *)
type store = {
  buffer: orderbook RingBuffer.t;
  ready: bool Atomic.t;
  mutable bids: level array;
  mutable asks: level array;
  depth: int;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()

(** reqId → symbol mapping. *)
let req_id_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 32
let next_req_id = Atomic.make 2000

(** Clear stale reqId→symbol mappings. Called before reconnection to
    prevent orphaned entries from prior connections accumulating. *)
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

(** Flush the current bid/ask arrays to the ring buffer. *)
let flush_orderbook symbol store =
  let ob = {
    bids = Array.copy store.bids;
    asks = Array.copy store.asks;
    timestamp = Unix.gettimeofday ();
  } in
  RingBuffer.write store.buffer ob;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol

(** Handle updateMktDepth message.
    Fields: version, reqId, position, operation, side, price, size *)
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

(** Subscribe to L2 order book for [symbol].
    For STK/ETF on SMART, deep market data is not supported —
    in that case we skip the subscription and mark ready immediately.
    The grid strategy only needs bid/ask from the ticker feed. *)
let subscribe conn ~contract =
  let symbol = contract.Ibkr_types.symbol in
  let store = ensure_store symbol in
  if contract.sec_type = "STK" then begin
    (* STK/ETF on SMART doesn't support L2 depth — skip *)
    Logging.info_f ~section "Skipping L2 orderbook for %s (STK on %s — not supported)"
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

(** Register handlers with the dispatcher. *)
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

(** Pre-allocate stores and register handlers. *)
let initialize symbols =
  Logging.info_f ~section "Initializing orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in ()
  ) symbols;
  register_handlers ();
  Logging.info ~section "Orderbook feed initialized"
