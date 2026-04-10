(** Lighter ticker feed.
    Consumes [ticker/{MARKET_INDEX}] WebSocket channel updates, stores
    per-symbol BBO snapshots in lock-free ring buffers, and exposes
    low-latency read accessors for downstream pricing consumers. *)

let section = "lighter_ticker"

(** Canonical ticker record representing a single BBO snapshot for one symbol. *)
type ticker = {
  symbol: string;
  bid: float;
  bid_size: float;
  ask: float;
  ask_size: float;
  timestamp: float;
}

(** Lock-free ring buffer used for bounded, wait-free ticker storage. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol store pairing a ring buffer with an atomic readiness flag. *)
type store = {
  buffer: ticker RingBuffer.t;
  ready: bool Atomic.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Returns the store for [symbol], creating one if absent.
    Uses double-checked locking via [initialization_mutex] for thread safety. *)
let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      let store = match Hashtbl.find_opt stores symbol with
        | Some store -> store
        | None ->
            let store = {
              buffer = RingBuffer.create 100;
              ready = Atomic.make false;
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

(** Returns the most recent ticker snapshot for [symbol], or [None] if no data exists. *)
let[@inline always] get_latest_ticker symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

(** Returns all ticker events written after [last_pos] for [symbol]. *)
let[@inline always] read_ticker_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Iterates [f] over ticker events written after [last_pos] without allocation.
    Returns the new read position. *)
let[@inline always] iter_ticker_events symbol last_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

(** Returns the current write position for [symbol]'s ring buffer. *)
let[@inline always] get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let[@inline always] has_price_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false

let wait_for_price_data symbols timeout_seconds =
  let open Lwt.Infix in
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_price_data symbols then
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
        | `Timeout -> Lwt.return (List.for_all has_price_data symbols)
  in
  loop ()

(** Process a ticker update message from the Lighter WebSocket.
    Actual format: { "channel": "ticker:0", "ticker": { "s": "ETH", "b": { "price": "...", "size": "..." }, "a": { ... } } } *)
let process_ticker_update ~market_index json =
  let open Yojson.Safe.Util in
  let symbol = match Lighter_instruments_feed.get_symbol ~market_index with
    | Some s -> s
    | None ->
        Logging.warn_f ~section "Received ticker for unknown market_index %d" market_index;
        string_of_int market_index
  in
  try
    (* Lighter nests ticker data under "ticker", not "data" *)
    let ticker_data = member "ticker" json in
    if ticker_data = `Null then begin
      Logging.debug_f ~section "Ticker message for %s has null ticker field (type=%s)"
        symbol (try member "type" json |> to_string with _ -> "unknown");
      ()
    end else begin
      let bid_obj = member "b" ticker_data in
      let ask_obj = member "a" ticker_data in
      let bid_price = Lighter_types.parse_json_float (member "price" bid_obj) in
      let bid_size = Lighter_types.parse_json_float (member "size" bid_obj) in
      let ask_price = Lighter_types.parse_json_float (member "price" ask_obj) in
      let ask_size = Lighter_types.parse_json_float (member "size" ask_obj) in

      let store = ensure_store symbol in
      let changed = match RingBuffer.read_latest store.buffer with
        | Some t -> t.bid <> bid_price || t.ask <> ask_price || t.bid_size <> bid_size || t.ask_size <> ask_size
        | None -> true
      in
      if changed then begin
        let ticker = {
          symbol;
          bid = bid_price;
          bid_size;
          ask = ask_price;
          ask_size;
          timestamp = Unix.gettimeofday ();
        } in
        RingBuffer.write store.buffer ticker;
        notify_ready store;
        Concurrency.Exchange_wakeup.signal ~symbol;
        Logging.debug_f ~section "Ticker: %s bid=%.2f(%s) ask=%.2f(%s)"
          symbol bid_price (string_of_float bid_size) ask_price (string_of_float ask_size)
      end
    end
  with exn ->
    Logging.warn_f ~section "Failed to parse ticker update for market %d: %s (json=%s)"
      market_index (Printexc.to_string exn)
      (let s = Yojson.Safe.to_string json in
       if String.length s > 300 then String.sub s 0 300 ^ "..." else s)

let initialize symbols =
  Logging.info_f ~section "Initializing Lighter ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Lighter ticker buffer for %s" symbol
  ) symbols
