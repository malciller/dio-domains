(** Streaming ticker feed via reqMktData → ring buffers.

    Subscribes to bid/ask/last price updates for configured symbols.
    Writes ticker snapshots to per-symbol lock-free ring buffers.
    Same {buffer; ready} pattern as the Kraken ticker feed. *)

let section = "ibkr_ticker"

(** Ticker snapshot for a single symbol. *)
type ticker = {
  symbol: string;
  bid: float;
  ask: float;
  last: float;
  bid_size: float;
  ask_size: float;
  volume: float;
  timestamp: float;
}

module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol ticker store. *)
type store = {
  buffer: ticker RingBuffer.t;
  ready: bool Atomic.t;
  mutable bid: float;
  mutable ask: float;
  mutable last: float;
  mutable bid_size: float;
  mutable ask_size: float;
  mutable volume: float;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()

(** reqId → symbol mapping for tick dispatch. *)
let req_id_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 32
let next_req_id = Atomic.make 1000

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let store = {
        buffer = RingBuffer.create Ibkr_types.default_ring_buffer_size_ticker;
        ready = Atomic.make false;
        bid = 0.0; ask = 0.0; last = 0.0;
        bid_size = 0.0; ask_size = 0.0; volume = 0.0;
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

(** Write a ticker snapshot to the ring buffer. Called when any
    price/size field updates. *)
let flush_ticker symbol store =
  if store.bid > 0.0 && store.ask > 0.0 then begin
    let ticker = {
      symbol;
      bid = store.bid;
      ask = store.ask;
      last = store.last;
      bid_size = store.bid_size;
      ask_size = store.ask_size;
      volume = store.volume;
      timestamp = Unix.gettimeofday ();
    } in
    RingBuffer.write store.buffer ticker;
    notify_ready store;
    Concurrency.Exchange_wakeup.signal ~symbol
  end

(** Handle tickPrice message from the dispatcher. *)
let handle_tick_price fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let req_id, fields = Ibkr_codec.read_int fields in
  let tick_type, fields = Ibkr_codec.read_int fields in
  let price, fields = Ibkr_codec.read_float fields in
  let _size, fields = Ibkr_codec.read_int fields in  (* deprecated size field *)
  let _attribs, _fields = Ibkr_codec.read_int fields in
  match Hashtbl.find_opt req_id_to_symbol req_id with
  | None -> ()
  | Some symbol ->
      let store = ensure_store symbol in
      if tick_type = Ibkr_types.tick_bid || tick_type = Ibkr_types.tick_delayed_bid then begin
        store.bid <- price;
        flush_ticker symbol store
      end else if tick_type = Ibkr_types.tick_ask || tick_type = Ibkr_types.tick_delayed_ask then begin
        store.ask <- price;
        flush_ticker symbol store
      end else if tick_type = Ibkr_types.tick_last || tick_type = Ibkr_types.tick_delayed_last then begin
        store.last <- price;
        flush_ticker symbol store
      end

(** Handle tickSize message from the dispatcher. *)
let handle_tick_size fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let req_id, fields = Ibkr_codec.read_int fields in
  let tick_type, fields = Ibkr_codec.read_int fields in
  let size, _fields = Ibkr_codec.read_float fields in
  match Hashtbl.find_opt req_id_to_symbol req_id with
  | None -> ()
  | Some symbol ->
      let store = ensure_store symbol in
      if tick_type = Ibkr_types.tick_bid_size || tick_type = Ibkr_types.tick_delayed_bid_size then
        store.bid_size <- size
      else if tick_type = Ibkr_types.tick_ask_size || tick_type = Ibkr_types.tick_delayed_ask_size then
        store.ask_size <- size
      else if tick_type = Ibkr_types.tick_volume || tick_type = Ibkr_types.tick_delayed_volume then begin
        store.volume <- size
      end

(** Subscribe to market data for [symbol]. Requires a resolved contract. *)
let subscribe conn ~contract =
  let symbol = contract.Ibkr_types.symbol in
  let req_id = Atomic.fetch_and_add next_req_id 1 in
  let _ = ensure_store symbol in
  Hashtbl.replace req_id_to_symbol req_id symbol;
  Logging.info_f ~section "Subscribing to ticker for %s (reqId=%d)" symbol req_id;

  (* reqMktData: [msgId=1, version=11, reqId, contract...,
     comboLegs(BAG only), deltaNeutralContract(0=false),
     genericTickList, snapshot, regulatorySnapshot, mktDataOptions] *)
  let msg_fields =
    [string_of_int Ibkr_types.msg_req_mkt_data;
     "11";  (* version *)
     string_of_int req_id]
    @ Ibkr_codec.encode_contract_short contract
    @ [
      "0";    (* deltaNeutralContract = false *)
      "";     (* genericTickList *)
      "0";    (* snapshot *)
      "0";    (* regulatorySnapshot *)
      "";     (* mktDataOptions *)
    ]
  in
  Ibkr_connection.send conn msg_fields

(** Register tick handlers with the dispatcher. Call once at startup. *)
let register_handlers () =
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_tick_price
    ~handler:handle_tick_price;
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_tick_size
    ~handler:handle_tick_size;
  (* tickString and tickGeneric are informational — just log/ignore *)
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_tick_string
    ~handler:(fun _fields -> ());
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_tick_generic
    ~handler:(fun _fields -> ());
  (* marketDataType response — log the type switch *)
  Ibkr_dispatcher.register_handler
    ~msg_id:Ibkr_types.msg_in_market_data_type
    ~handler:(fun fields ->
      let _version, fields = Ibkr_codec.read_int fields in
      let req_id, fields = Ibkr_codec.read_int fields in
      let mkt_data_type, _fields = Ibkr_codec.read_int fields in
      let type_name = match mkt_data_type with
        | 1 -> "live" | 2 -> "frozen" | 3 -> "delayed" | 4 -> "delayed-frozen"
        | _ -> "unknown"
      in
      Logging.info_f ~section "Market data type for reqId=%d: %s (%d)"
        req_id type_name mkt_data_type)

(* ---- Public accessors ---- *)

let[@inline always] get_latest_ticker symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

let[@inline always] read_ticker_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

let[@inline always] iter_ticker_events symbol last_pos f =
  match store_opt symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let has_price_data symbol =
  match store_opt symbol with
  | Some store when Atomic.get store.ready -> Option.is_some (RingBuffer.read_latest store.buffer)
  | _ -> false

(** Pre-allocate stores for all symbols. *)
let initialize symbols =
  Logging.info_f ~section "Initializing ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created ticker buffer for %s" symbol
  ) symbols;
  register_handlers ();
  Logging.info ~section "Ticker feed initialized"
