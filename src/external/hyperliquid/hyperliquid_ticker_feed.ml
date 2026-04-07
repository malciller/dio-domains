(** Hyperliquid ticker feed.
    Consumes allMids and l2Book WebSocket channels, stores per-symbol
    ticker snapshots in lock-free ring buffers, and exposes low-latency
    read accessors for downstream pricing consumers. *)



let section = "hyperliquid_ticker"

(** Canonical ticker record representing a single price snapshot for one symbol. *)
type ticker = {
  symbol: string;
  bid: float;
  ask: float;
  last: float;
  volume: float;
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

(** Returns the bid/ask midpoint for [symbol]. Inlined on the hot path. *)
let[@inline always] get_latest_price symbol =
  match get_latest_ticker symbol with
  | None -> None
  | Some ticker -> Some ((ticker.bid +. ticker.ask) /. 2.0)

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

let find_registered_symbol coin =
  (* Resolves a Hyperliquid coin identifier to a registered store symbol.
     Spot tokens (e.g. "@107") resolve via the instruments feed to their
     canonical pair (e.g. "HYPE/USDC"). Perp coins (e.g. "BTC") resolve to
     the bare name; if that is not in stores, the "/USDC" suffix is appended
     to match the config-style key (e.g. "BTC/USDC"). *)
  match Hyperliquid_instruments_feed.resolve_symbol coin with
  | Some symbol ->
      if Hashtbl.mem stores symbol then Some symbol
      else
        let usdc_symbol = symbol ^ "/USDC" in
        if Hashtbl.mem stores usdc_symbol then Some usdc_symbol
        else None
  | None -> None

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  match channel with
  | Some "allMids" ->
      let data = member "data" json in
      let mids_json = member "mids" data in
      let mids = (try to_assoc mids_json with _ -> 
        Logging.warn ~section "Failed to parse allMids as assoc list"; []) in
      
      if mids <> [] then begin
        Logging.debug_f ~section "Received allMids with %d coins" (List.length mids);
        
        List.iter (fun (coin, mid_json) ->
          match find_registered_symbol coin with
          | Some symbol ->
              (try
                let mid = to_string mid_json |> float_of_string in
                let ticker = {
                  symbol;
                  bid = mid;
                  ask = mid;
                  last = mid;
                  volume = 0.0;
                  timestamp = Unix.gettimeofday ();
                } in
                let store = ensure_store symbol in
                RingBuffer.write store.buffer ticker;
                notify_ready store;
                Concurrency.Exchange_wakeup.signal ~symbol;
                Logging.debug_f ~section "Ticker: %s mid=%.2f" symbol mid
              with exn ->
                Logging.warn_f ~section "Failed to parse mid for %s: %s" coin (Printexc.to_string exn))
          | None -> ()
        ) mids
      end
  | Some "l2Book" ->
      let data = member "data" json in
      let coin = member "coin" data |> to_string in
      (match find_registered_symbol coin with
      | Some symbol ->
          (try
            let levels = member "levels" data |> to_list in
            let bids = List.nth levels 0 |> to_list in
            let asks = List.nth levels 1 |> to_list in
            
            if bids <> [] && asks <> [] then begin
              let bid_px = List.hd bids |> member "px" |> to_string |> float_of_string in
              let ask_px = List.hd asks |> member "px" |> to_string |> float_of_string in
              let mid = (bid_px +. ask_px) /. 2.0 in
              
              let ticker = {
                symbol;
                bid = bid_px;
                ask = ask_px;
                last = mid;
                volume = 0.0;
                timestamp = Unix.gettimeofday ();
              } in
              
              let store = ensure_store symbol in
              RingBuffer.write store.buffer ticker;
              notify_ready store;
              Concurrency.Exchange_wakeup.signal ~symbol;
              Logging.debug_f ~section "Ticker: %s bid=%.2f ask=%.2f (from l2Book)" symbol bid_px ask_px
            end
          with exn ->
            Logging.warn_f ~section "Failed to parse l2Book for %s: %s" coin (Printexc.to_string exn))
      | None -> ())
  | _ -> ()

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_market_data () in
    Lwt.catch (fun () ->
      Logging.info ~section "Starting Hyperliquid ticker processor task";
      let%lwt () = Concurrency.Lwt_util.consume_stream process_market_data sub.stream in
      (* Stream ended (disconnect pushed None). Re-subscribe immediately;
         consume_stream blocks event-driven on the new stream until the
         WS reconnects and data flows. Sever Forward chain via Lwt.async. *)
      sub.close ();
      Logging.info ~section "Ticker stream ended (disconnect), re-subscribing...";
      Lwt.async run;
      Lwt.return_unit
    ) (fun exn ->
      sub.close ();
      Logging.error_f ~section "Hyperliquid ticker processor task crashed: %s. Re-subscribing..." (Printexc.to_string exn);
      Lwt.async run;
      Lwt.return_unit
    )
  in
  Lwt.async run

let initialize symbols =
  Logging.info_f ~section "Initializing Hyperliquid ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid ticker buffer for %s" symbol
  ) symbols