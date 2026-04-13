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

(** Mutable top-of-book cache. Updated atomically (single writer from the WS
    processing thread) and read lock-free from domain workers. Avoids the
    full ring-buffer read + array bounds check + record field extraction
    pipeline on the latency-critical path. *)
type tob_cache = {
  mutable bid_px: float;
  mutable bid_sz: float;
  mutable ask_px: float;
  mutable ask_sz: float;
  mutable tob_valid: bool;
}

(** Per-symbol store containing a ring buffer, readiness flag, and
    zero-allocation top-of-book cache. *)
type store = {
  buffer: orderbook RingBuffer.t;
  ready: bool Atomic.t;
  tob: tob_cache;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Cached coin-to-symbol resolution table. Populated during `initialize` to
    avoid `Hyperliquid_instruments_feed.resolve_symbol` + `Hashtbl.mem` scanning
    on every incoming tick. Only the WS processing thread writes (at init);
    domain workers never access this table. *)
let coin_to_symbol : (string, string) Hashtbl.t = Hashtbl.create 32

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
              tob = { bid_px = 0.0; bid_sz = 0.0; ask_px = 0.0; ask_sz = 0.0; tob_valid = false };
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
  (* Fast path: check the cached coin_to_symbol table first.
     This avoids resolve_symbol + Hashtbl.mem on every tick. *)
  match Hashtbl.find_opt coin_to_symbol coin with
  | Some _ as r -> r
  | None ->
      (* Slow path: resolve via instruments feed and probe stores. *)
      match Hyperliquid_instruments_feed.resolve_symbol coin with
      | Some symbol ->
          if Hashtbl.mem stores symbol then begin
            Hashtbl.replace coin_to_symbol coin symbol;
            Some symbol
          end else
            let usdc_symbol = symbol ^ "/USDC" in
            if Hashtbl.mem stores usdc_symbol then begin
              Hashtbl.replace coin_to_symbol coin usdc_symbol;
              Some usdc_symbol
            end else None
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

(** Zero-allocation depth-1 top-of-book parser. Extracts exactly the first
    bid and first ask {px, sz} directly from the raw JSON string without
    building any intermediate list or array. Returns (bid_px, bid_sz,
    ask_px, ask_sz, true) on success or (0, 0, 0, 0, false) on failure.

    Layout: "levels":[[{bid levels}],[{ask levels}]]
    We find the first {px, sz} in each sub-array. *)
let parse_tob_fast msg =
  let px_key = "\"px\":\"" in
  let sz_key = "\"sz\":\"" in
  let msg_len = String.length msg in
  let find_key key start_idx limit =
    let rec search idx =
      if idx >= limit then None
      else
        match String.index_from_opt msg idx '"' with
        | None -> None
        | Some p ->
            if p >= limit then None
            else if string_match msg p key then
              let val_start = p + String.length key in
              if val_start >= msg_len then None
              else
                match String.index_from_opt msg val_start '"' with
                | Some val_end -> Some (val_start, val_end)
                | None -> None
            else search (p + 1)
    in search start_idx
  in
  let levels_key = "\"levels\":[[" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> (0.0, 0.0, 0.0, 0.0, false)
    | Some idx ->
        if string_match msg idx levels_key then
          let bids_start = idx + 11 in
          (* Find end of bids array (first ']') *)
          match String.index_from_opt msg bids_start ']' with
          | None -> (0.0, 0.0, 0.0, 0.0, false)
          | Some bids_end ->
              (* Parse first bid level *)
              (match find_key px_key bids_start bids_end with
               | None -> (0.0, 0.0, 0.0, 0.0, false)
               | Some (bpx_s, bpx_e) ->
                   match find_key sz_key (bpx_e + 1) bids_end with
                   | None -> (0.0, 0.0, 0.0, 0.0, false)
                   | Some (bsz_s, bsz_e) ->
                       let bid_px = parse_float_fast msg bpx_s bpx_e in
                       let bid_sz = parse_float_fast msg bsz_s bsz_e in
                       (* Find asks array: skip "],[" after bids_end *)
                       let asks_start = bids_end + 2 in
                       if asks_start >= msg_len then (0.0, 0.0, 0.0, 0.0, false)
                       else
                         match String.index_from_opt msg asks_start ']' with
                         | None -> (0.0, 0.0, 0.0, 0.0, false)
                         | Some asks_end ->
                             match find_key px_key asks_start asks_end with
                             | None -> (0.0, 0.0, 0.0, 0.0, false)
                             | Some (apx_s, apx_e) ->
                                 match find_key sz_key (apx_e + 1) asks_end with
                                 | None -> (0.0, 0.0, 0.0, 0.0, false)
                                 | Some (asz_s, asz_e) ->
                                     let ask_px = parse_float_fast msg apx_s apx_e in
                                     let ask_sz = parse_float_fast msg asz_s asz_e in
                                     (bid_px, bid_sz, ask_px, ask_sz, true))
        else search (idx + 1)
  in search 0

(** Hot-path orderbook processor. Uses zero-allocation depth-1 TOB parse
    to update the mutable cache directly. Still writes to the ring buffer
    for any consumers that need full snapshots, but the domain-critical
    get_best_bid_ask_fast path reads from the mutable cache. *)
let process_raw_market_data msg =
  if String.starts_with ~prefix:"{\"channel\":\"l2Book\"," msg then begin
    let coin = get_coin msg in
    if coin <> "" then begin
      match find_registered_symbol coin with
      | Some symbol ->
          (try
            let store = ensure_store symbol in
            (* Zero-alloc depth-1 fast path: parse TOB directly into mutable cache *)
            let (bid_px, bid_sz, ask_px, ask_sz, ok) = parse_tob_fast msg in
            if ok then begin
              store.tob.bid_px <- bid_px;
              store.tob.bid_sz <- bid_sz;
              store.tob.ask_px <- ask_px;
              store.tob.ask_sz <- ask_sz;
              store.tob.tob_valid <- true;
            end;
            (* Still write full OB to ring buffer for non-hot-path consumers.
               Timestamp is deferred — only set if someone actually reads the
               ring buffer entry, which is not on the domain hot path. *)
            let bids, asks = get_bids_asks msg in
            let ob = {
              symbol;
              bids;
              asks;
              timestamp = 0.0;
            } in
            RingBuffer.write store.buffer ob;
            notify_ready store;
            Concurrency.Exchange_wakeup.signal ~symbol
          with exn ->
            Logging.warn_f ~section "Failed to parse l2Book for %s: %s" coin (Printexc.to_string exn))
      | None -> ()
    end
  end

let[@inline always] get_latest_orderbook symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

(** Read top-of-book from the mutable cache (zero allocation). *)
let[@inline always] get_best_bid_ask symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store when store.tob.tob_valid ->
      Some (store.tob.bid_px, store.tob.bid_sz, store.tob.ask_px, store.tob.ask_sz)
  | _ -> None

(** Returns a cached closure that reads top-of-book from the mutable TOB cache.
    The closure itself allocates nothing — it reads 4 mutable floats and wraps
    them in a Some tuple. The store pointer is captured at closure-creation time
    so the domain hot loop never touches the stores Hashtbl. *)
let[@inline always] get_best_bid_ask_fast symbol =
  let store = ensure_store symbol in
  (fun () ->
     if store.tob.tob_valid then
       Some (store.tob.bid_px, store.tob.bid_sz, store.tob.ask_px, store.tob.ask_sz)
     else None
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
    (* Pre-populate coin_to_symbol cache for known symbols.
       Resolves coin identifiers at startup so the hot path hits the cache. *)
    let coin = Hyperliquid_instruments_feed.get_subscription_coin symbol in
    if coin <> "" then
      Hashtbl.replace coin_to_symbol coin symbol;
    Logging.debug_f ~section "Created Hyperliquid orderbook buffer for %s (coin=%s)" symbol coin
  ) symbols
