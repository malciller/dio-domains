(** Hyperliquid Ticker Feed *)



let section = "hyperliquid_ticker"

(** Ticker data *)
type ticker = {
  symbol: string;
  bid: float;
  ask: float;
  last: float;
  volume: float;
  timestamp: float;
}

(** Lock-free ring buffer for ticker data *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol ticker storage with readiness signalling *)
type store = {
  buffer: ticker RingBuffer.t;
  ready: bool Atomic.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Get or create store for a symbol - thread-safe with double-checked locking *)
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

(** Get latest ticker for a symbol *)
let[@inline always] get_latest_ticker symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

(** Get latest price (midpoint of bid/ask) - hot path, inlined *)
let[@inline always] get_latest_price symbol =
  match get_latest_ticker symbol with
  | None -> None
  | Some ticker -> Some ((ticker.bid +. ticker.ask) /. 2.0)

(** Read ticker events since last position *)
let[@inline always] read_ticker_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Get current write position *)
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
  (* Use instruments feed to resolve coin to canonical symbol.
     - Spot pairs: coin="@107" -> resolve -> "HYPE/USDC" -> found in stores ✓
     - Perps:      coin="BTC"  -> resolve -> "BTC"        -> NOT in stores (keyed "BTC/USDC")
                   so fall back to trying "BTC/USDC" to match config-style symbol. *)
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
        
        (* Track symbols that successfully wrote to buffer in this message to notify once *)
        let notified_symbols = Hashtbl.create 16 in
        
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
                Hashtbl.replace notified_symbols symbol store;
                Logging.debug_f ~section "Ticker: %s mid=%.2f" symbol mid
              with exn ->
                Logging.warn_f ~section "Failed to parse mid for %s: %s" coin (Printexc.to_string exn))
          | None -> ()
        ) mids;
        
        (* Notify readiness for all updated symbols (mirroring Kraken pattern) *)
        Hashtbl.iter (fun _ store -> notify_ready store) notified_symbols
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
      let%lwt () = Lwt_stream.iter process_market_data sub.stream in
      (* Stream ended normally (disconnect pushed None) — re-subscribe *)
      sub.close ();
      Logging.info ~section "Ticker stream ended (disconnect), re-subscribing in 1s...";
      Lwt.bind (Lwt_unix.sleep 1.0) (fun () -> run ())
    ) (fun exn ->
      sub.close ();
      Logging.error_f ~section "Hyperliquid ticker processor task crashed: %s. Restarting in 5s..." (Printexc.to_string exn);
      Lwt.bind (Lwt_unix.sleep 5.0) (fun () -> run ())
    )
  in
  Lwt.async run

let initialize symbols =
  Logging.info_f ~section "Initializing Hyperliquid ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid ticker buffer for %s" symbol
  ) symbols