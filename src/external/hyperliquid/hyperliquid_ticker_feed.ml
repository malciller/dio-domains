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

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let store = {
        buffer = RingBuffer.create 100;
        ready = Atomic.make false;
      } in
      Hashtbl.add stores symbol store;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

(** Get latest ticker for a symbol *)
let get_latest_ticker symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

(** Get latest price (midpoint of bid/ask) - hot path, inlined *)
let[@inline always] get_latest_price symbol =
  match get_latest_ticker symbol with
  | None -> None
  | Some ticker -> Some ((ticker.bid +. ticker.ask) /. 2.0)

(** Read ticker events since last position *)
let read_ticker_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Get current write position *)
let get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let find_registered_symbol coin =
  let result = ref None in
  Hashtbl.iter (fun registered_symbol _ ->
    if registered_symbol = coin || String.starts_with ~prefix:(coin ^ "/") registered_symbol then
      result := Some registered_symbol
  ) stores;
  !result

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
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt.async (fun () ->
    Logging.info ~section "Starting Hyperliquid ticker processor task";
    Lwt.catch (fun () ->
      Lwt_stream.iter process_market_data sub.stream
    ) (fun exn ->
      Logging.error_f ~section "Hyperliquid ticker processor task crashed: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  )

let initialize symbols =
  Logging.info_f ~section "Initializing Hyperliquid ticker feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid ticker buffer for %s" symbol
  ) symbols
