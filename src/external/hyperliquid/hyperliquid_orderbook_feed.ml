(** Hyperliquid Orderbook Feed - WebSocket L2 depth subscription with ring buffer *)

open Lwt.Infix

let section = "hyperliquid_orderbook"

let ring_buffer_size = 64

type level = {
  price: float;
  size: float;
}

type orderbook = {
  symbol: string;
  bids: level array;
  asks: level array;
  timestamp: float;
}

(** Lock-free ring buffer for orderbook data *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol orderbook storage and readiness signalling *)
type store = {
  buffer: orderbook RingBuffer.t;
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
              buffer = RingBuffer.create ring_buffer_size;
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

let find_registered_symbol coin =
  (* Use instruments feed to resolve coin to canonical symbol.
     - Spot pairs: coin="@107" -> resolve -> "HYPE/USDC" -> found in stores as "HYPE/USDC" ✓
     - Perps:      coin="BTC"  -> resolve -> "BTC"        -> NOT in stores (keyed "BTC/USDC")
                   so fall back to trying "BTC/USDC" to match config-style symbol. *)
  match Hyperliquid_instruments_feed.resolve_symbol coin with
  | Some symbol ->
      if Hashtbl.mem stores symbol then Some symbol
      else
        (* Perp coins resolve to just the base name, but stores use BASE/USDC convention *)
        let usdc_symbol = symbol ^ "/USDC" in
        if Hashtbl.mem stores usdc_symbol then Some usdc_symbol
        else None
  | None -> None

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  match channel with
  | Some "l2Book" ->
      let data = member "data" json in
      let coin = member "coin" data |> to_string in
      (match find_registered_symbol coin with
      | Some symbol ->
          (try
            let levels = member "levels" data |> to_list in
            let raw_bids = List.nth levels 0 |> to_list in
            let raw_asks = List.nth levels 1 |> to_list in
            
            let parse_level l = {
              price = member "px" l |> to_string |> float_of_string;
              size = member "sz" l |> to_string |> float_of_string;
            } in
            
            let bids = List.map parse_level raw_bids |> Array.of_list in
            let asks = List.map parse_level raw_asks |> Array.of_list in
            
            let ob = {
              symbol;
              bids;
              asks;
              timestamp = Unix.gettimeofday ();
            } in
            
            let store = ensure_store symbol in
            RingBuffer.write store.buffer ob;
            notify_ready store;
            Logging.debug_f ~section "Orderbook update for %s: bids=%d asks=%d" 
              symbol (Array.length bids) (Array.length asks)
          with exn ->
            Logging.warn_f ~section "Failed to parse l2Book for %s: %s" coin (Printexc.to_string exn))
      | None -> ())
  | _ -> ()

let[@inline always] get_latest_orderbook symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

let[@inline always] get_best_bid_ask symbol =
  match get_latest_orderbook symbol with
  | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
      let bid = bids.(0) in
      let ask = asks.(0) in
      Some (bid.price, bid.size, ask.price, ask.size)
  | _ -> None

(** Read orderbook events since last position *)
let[@inline always] read_orderbook_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Get current write position *)
let[@inline always] get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let has_orderbook_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false

let wait_for_orderbook_data symbols timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_orderbook_data symbols then
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
        | `Timeout -> Lwt.return (List.for_all has_orderbook_data symbols)
  in
  loop ()

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_market_data () in
    Lwt.catch (fun () ->
      Logging.info ~section "Starting Hyperliquid orderbook processor task";
      Lwt_stream.iter process_market_data sub.stream
    ) (fun exn ->
      sub.close ();
      Logging.error_f ~section "Hyperliquid orderbook processor task crashed: %s. Restarting in 5s..." (Printexc.to_string exn);
      Lwt_unix.sleep 5.0 >>= fun () ->
      run ()
    )
  in
  Lwt.async run

let initialize symbols =
  Logging.info_f ~section "Initializing Hyperliquid orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid orderbook buffer for %s" symbol
  ) symbols
