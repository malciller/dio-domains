(** Hyperliquid L2 orderbook feed. Subscribes to WebSocket depth updates and stores
    snapshots in per-symbol lock-free ring buffers. *)

open Lwt.Infix

let section = "hyperliquid_orderbook"

let ring_buffer_size = 16

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

(** Lock-free SPSC ring buffer for orderbook snapshots. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol store containing a ring buffer and an atomic readiness flag. *)
type store = {
  buffer: orderbook RingBuffer.t;
  ready: bool Atomic.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Returns the store for [symbol], creating one if absent. Uses double-checked
    locking via [initialization_mutex] for thread safety. *)
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
  (* Resolves a raw coin identifier to a registered store key.
     Spot pairs (e.g. "@107") resolve directly to their canonical symbol.
     Perp coins (e.g. "BTC") resolve to the base name; if not found in stores,
     falls back to the "BASE/USDC" convention used by configuration. *)
  match Hyperliquid_instruments_feed.resolve_symbol coin with
  | Some symbol ->
      if Hashtbl.mem stores symbol then Some symbol
      else
        (* Perp fallback: stores are keyed by BASE/USDC convention. *)
        let usdc_symbol = symbol ^ "/USDC" in
        if Hashtbl.mem stores usdc_symbol then Some usdc_symbol
        else None
  | None -> None

let string_match msg pos key =
  let key_len = String.length key in
  if pos + key_len > String.length msg then false
  else
    let rec loop i =
      if i = key_len then true
      else if msg.[pos + i] <> key.[i] then false
      else loop (i + 1)
    in loop 0

let get_coin msg =
  let coin_key = "\"coin\":\"" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> ""
    | Some idx ->
        if string_match msg idx coin_key then
          let start_idx = idx + 8 in
          match String.index_from_opt msg start_idx '"' with
          | Some end_idx -> String.sub msg start_idx (end_idx - start_idx)
          | None -> ""
        else search (idx + 1)
  in search 0

let parse_float_fast str start_idx end_idx =
  let len = end_idx - start_idx in
  if len = 0 then 0.0
  else
    let sign, start_idx = 
      if String.unsafe_get str start_idx = '-' then (-1.0, start_idx + 1)
      else (1.0, start_idx)
    in
    let rec loop idx acc dec frac_div =
      if idx = end_idx then acc /. frac_div
      else
        let c = String.unsafe_get str idx in
        if c = '.' then loop (idx + 1) acc true 1.0
        else if c >= '0' && c <= '9' then
          let d = float_of_int (Char.code c - 48) in
          if dec then loop (idx + 1) ((acc *. 10.0) +. d) true (frac_div *. 10.0)
          else loop (idx + 1) ((acc *. 10.0) +. d) false frac_div
        else loop (idx + 1) acc dec frac_div
    in
    sign *. loop start_idx 0.0 false 1.0

let rec parse_levels msg pos end_pos count acc =
  if count >= 1 || pos >= end_pos then List.rev acc
  else
    match String.index_from_opt msg pos '{' with
    | None -> List.rev acc
    | Some brace_idx ->
        if brace_idx >= end_pos then List.rev acc
        else
          let px_key = "\"px\":\"" in
          let sz_key = "\"sz\":\"" in
          let rec find_key key start_idx =
            match String.index_from_opt msg start_idx '"' with
            | None -> None
            | Some p ->
                if string_match msg p key then
                  let val_start = p + String.length key in
                  match String.index_from_opt msg val_start '"' with
                  | Some val_end -> Some (val_start, val_end)
                  | None -> None
                else find_key key (p + 1)
          in
          match find_key px_key brace_idx with
          | Some (px_start, px_end) ->
              (match find_key sz_key (px_end + 1) with
               | Some (sz_start, sz_end) ->
                    let px = parse_float_fast msg px_start px_end in
                    let sz = parse_float_fast msg sz_start sz_end in
                    parse_levels msg (sz_end + 1) end_pos (count + 1) ({price=px; size=sz} :: acc)
               | None -> List.rev acc)
          | None -> List.rev acc

let get_bids_asks msg =
  let levels_key = "\"levels\":[[" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> ([||], [||])
    | Some idx ->
        if string_match msg idx levels_key then
          let bids_start = idx + 11 in
          match String.index_from_opt msg bids_start ']' with
          | None -> ([||], [||])
          | Some bids_end ->
              let bids_list = parse_levels msg bids_start bids_end 0 [] in
              let asks_start = bids_end + 2 in
              if asks_start < String.length msg then
                match String.index_from_opt msg asks_start ']' with
                | None -> (Array.of_list bids_list, [||])
                | Some asks_end ->
                    let asks_list = parse_levels msg asks_start asks_end 0 [] in
                    (Array.of_list bids_list, Array.of_list asks_list)
              else (Array.of_list bids_list, [||])
        else search (idx + 1)
  in search 0

let process_raw_market_data msg =
  let now_ts = Unix.gettimeofday () in
  if String.starts_with ~prefix:"{\"channel\":\"l2Book\"," msg then begin
    let coin = get_coin msg in
    if coin <> "" then begin
      match find_registered_symbol coin with
      | Some symbol ->
          (try
            let bids, asks = get_bids_asks msg in
            let ob = {
              symbol;
              bids;
              asks;
              timestamp = now_ts;
            } in
            
            let store = ensure_store symbol in
            RingBuffer.write store.buffer ob;
            notify_ready store;
            Concurrency.Exchange_wakeup.signal ~symbol;
            Logging.debug_f ~section "Orderbook update for %s: bids=%d asks=%d" 
              symbol (Array.length bids) (Array.length asks)
          with exn ->
            Logging.warn_f ~section "Failed to parse l2Book for %s: %s" coin (Printexc.to_string exn))
      | None -> ()
    end
  end

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

let[@inline always] get_best_bid_ask_fast symbol =
  let store = ensure_store symbol in
  (fun () ->
     match RingBuffer.read_latest store.buffer with
     | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
         let bid = bids.(0) in
         let ask = asks.(0) in
         Some (bid.price, bid.size, ask.price, ask.size)
     | _ -> None
  )

(** Returns all orderbook snapshots written since [last_pos]. *)
let[@inline always] read_orderbook_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Iterates [f] over orderbook snapshots since [last_pos] without list allocation.
    Returns the new cursor position. *)
let[@inline always] iter_orderbook_events symbol last_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

(** Returns the current ring buffer write position for [symbol]. *)
let[@inline always] get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let[@inline always] get_current_position_fast symbol =
  let store = ensure_store symbol in
  (fun () -> RingBuffer.get_position store.buffer)

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
    let sub = Hyperliquid_ws.subscribe_raw_market_data () in
    Lwt.catch (fun () ->
      Logging.info ~section "Starting Hyperliquid orderbook processor task";
      let%lwt () = Concurrency.Lwt_util.consume_stream process_raw_market_data sub.stream in
      (* Stream ended (disconnect pushed None). Re-subscribe immediately;
         consume_stream blocks event-driven on the new stream until the
         WS reconnects and data flows. Sever Forward chain via Lwt.async. *)
      sub.close ();
      Logging.info ~section "Orderbook stream ended (disconnect), re-subscribing...";
      Lwt.async run;
      Lwt.return_unit
    ) (fun exn ->
      sub.close ();
      Logging.error_f ~section "Hyperliquid orderbook processor task crashed: %s. Re-subscribing..." (Printexc.to_string exn);
      Lwt.async run;
      Lwt.return_unit
    )
  in
  Lwt.async run

let initialize symbols =
  Logging.info_f ~section "Initializing Hyperliquid orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid orderbook buffer for %s" symbol
  ) symbols
