[@@@ocaml.text
  " Hyperliquid L2 orderbook feed. Subscribes to WebSocket depth updates and stores\n\
  \    snapshots in per-symbol lock-free ring buffers. "]

open Lwt.Infix

let section = "hyperliquid_orderbook"
let ring_buffer_size = 64

type level =
  { price : float
  ; size : float
  }

type orderbook =
  { symbol : string
  ; bids : level array
  ; asks : level array
  ; timestamp : float
  }

module RingBuffer = Concurrency.Ring_buffer.RingBuffer
[@@ocaml.doc " Lock-free SPSC ring buffer for orderbook snapshots. "]

type tob_cache =
  { bid_px : float
  ; bid_sz : float
  ; ask_px : float
  ; ask_sz : float
  ; tob_valid : bool
  }
[@@ocaml.doc
  " Top-of-book cache. Single WS-thread writer publishes an immutable snapshot record with\n\
  \    one [Atomic.set]; domain workers read it with one [Atomic.get]. Avoids the \
   ring-buffer\n\
  \    read + bounds-check + field-extraction pipeline on the latency-critical path and\n\
  \    prevents torn reads under the OCaml 5 memory model. "]

type store =
  { buffer : string RingBuffer.t
  ; ready : bool Atomic.t
  ; tob : tob_cache Atomic.t
  }

let stores = Ds.Cow_table.create ~shard_count:32 ()
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

let coin_to_symbol : (string, string) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:32 ()
;;

let ensure_store symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store -> store
  | None ->
    Mutex.lock initialization_mutex;
    let store =
      match Ds.Cow_table.find_opt stores symbol with
      | Some store -> store
      | None ->
        let store =
          { buffer = RingBuffer.create ring_buffer_size
          ; ready = Atomic.make false
          ; tob =
              Atomic.make
                { bid_px = 0.0
                ; bid_sz = 0.0
                ; ask_px = 0.0
                ; ask_sz = 0.0
                ; tob_valid = false
                }
          }
        in
        Ds.Cow_table.set stores symbol store;
        store
    in
    Mutex.unlock initialization_mutex;
    store
[@@ocaml.doc
  " Returns the store for [symbol], creating one if absent. Uses double-checked locking\n\
  \    via [initialization_mutex] for thread safety. "]
;;

let notify_ready store =
  if not (Atomic.get store.ready)
  then (
    Atomic.set store.ready true;
    try Lwt_condition.broadcast ready_condition () with
    | _ -> ())
;;

let find_registered_symbol coin =
  match Ds.Cow_table.find_opt coin_to_symbol coin with
  | Some _ as r -> r
  | None ->
    (match Hyperliquid_instruments_feed.resolve_symbol coin with
     | Some symbol ->
       if Ds.Cow_table.mem stores symbol
       then (
         Ds.Cow_table.set coin_to_symbol coin symbol;
         Some symbol)
       else (
         let usdc_symbol = symbol ^ "/USDC" in
         if Ds.Cow_table.mem stores usdc_symbol
         then (
           Ds.Cow_table.set coin_to_symbol coin usdc_symbol;
           Some usdc_symbol)
         else None)
     | None -> None)
;;

let string_match msg pos key =
  let key_len = String.length key in
  if pos + key_len > String.length msg
  then false
  else (
    let rec loop i =
      if i = key_len
      then true
      else if msg.[pos + i] <> key.[i]
      then false
      else loop (i + 1)
    in
    loop 0)
;;

let get_coin msg =
  let coin_key = "\"coin\":\"" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> ""
    | Some idx ->
      if string_match msg idx coin_key
      then (
        let start_idx = idx + 8 in
        match String.index_from_opt msg start_idx '"' with
        | Some end_idx -> String.sub msg start_idx (end_idx - start_idx)
        | None -> "")
      else search (idx + 1)
  in
  search 0
;;

let parse_float_fast str start_idx end_idx =
  let len = end_idx - start_idx in
  if len = 0
  then 0.0
  else (
    let sign, start_idx =
      if String.unsafe_get str start_idx = '-'
      then -1.0, start_idx + 1
      else 1.0, start_idx
    in
    let rec loop idx acc dec frac_div =
      if idx = end_idx
      then acc /. frac_div
      else (
        let c = String.unsafe_get str idx in
        if c = '.'
        then loop (idx + 1) acc true 1.0
        else if c >= '0' && c <= '9'
        then (
          let d = float_of_int (Char.code c - 48) in
          if dec
          then loop (idx + 1) ((acc *. 10.0) +. d) true (frac_div *. 10.0)
          else loop (idx + 1) ((acc *. 10.0) +. d) false frac_div)
        else loop (idx + 1) acc dec frac_div)
    in
    sign *. loop start_idx 0.0 false 1.0)
;;

let rec parse_levels msg pos end_pos count acc =
  if count >= 10 || pos >= end_pos
  then List.rev acc
  else (
    match String.index_from_opt msg pos '{' with
    | None -> List.rev acc
    | Some brace_idx ->
      if brace_idx >= end_pos
      then List.rev acc
      else (
        let px_key = "\"px\":\"" in
        let sz_key = "\"sz\":\"" in
        let rec find_key key start_idx =
          match String.index_from_opt msg start_idx '"' with
          | None -> None
          | Some p ->
            if string_match msg p key
            then (
              let val_start = p + String.length key in
              match String.index_from_opt msg val_start '"' with
              | Some val_end -> Some (val_start, val_end)
              | None -> None)
            else find_key key (p + 1)
        in
        match find_key px_key brace_idx with
        | Some (px_start, px_end) ->
          (match find_key sz_key (px_end + 1) with
           | Some (sz_start, sz_end) ->
             let px = parse_float_fast msg px_start px_end in
             let sz = parse_float_fast msg sz_start sz_end in
             parse_levels
               msg
               (sz_end + 1)
               end_pos
               (count + 1)
               ({ price = px; size = sz } :: acc)
           | None -> List.rev acc)
        | None -> List.rev acc))
;;

let get_bids_asks msg =
  let levels_key = "\"levels\":[[" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> [||], [||]
    | Some idx ->
      if string_match msg idx levels_key
      then (
        let bids_start = idx + 11 in
        match String.index_from_opt msg bids_start ']' with
        | None -> [||], [||]
        | Some bids_end ->
          let bids_list = parse_levels msg bids_start bids_end 0 [] in
          let asks_start = bids_end + 2 in
          if asks_start < String.length msg
          then (
            match String.index_from_opt msg asks_start ']' with
            | None -> Array.of_list bids_list, [||]
            | Some asks_end ->
              let asks_list = parse_levels msg asks_start asks_end 0 [] in
              Array.of_list bids_list, Array.of_list asks_list)
          else Array.of_list bids_list, [||])
      else search (idx + 1)
  in
  search 0
;;

let parse_tob_fast msg =
  let px_key = "\"px\":\"" in
  let sz_key = "\"sz\":\"" in
  let msg_len = String.length msg in
  let find_key key start_idx limit =
    let rec search idx =
      if idx >= limit
      then None
      else (
        match String.index_from_opt msg idx '"' with
        | None -> None
        | Some p ->
          if p >= limit
          then None
          else if string_match msg p key
          then (
            let val_start = p + String.length key in
            if val_start >= msg_len
            then None
            else (
              match String.index_from_opt msg val_start '"' with
              | Some val_end -> Some (val_start, val_end)
              | None -> None))
          else search (p + 1))
    in
    search start_idx
  in
  let levels_key = "\"levels\":[[" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> 0.0, 0.0, 0.0, 0.0, false
    | Some idx ->
      if string_match msg idx levels_key
      then (
        let bids_start = idx + 11 in
        match String.index_from_opt msg bids_start ']' with
        | None -> 0.0, 0.0, 0.0, 0.0, false
        | Some bids_end ->
          (match find_key px_key bids_start bids_end with
           | None -> 0.0, 0.0, 0.0, 0.0, false
           | Some (bpx_s, bpx_e) ->
             (match find_key sz_key (bpx_e + 1) bids_end with
              | None -> 0.0, 0.0, 0.0, 0.0, false
              | Some (bsz_s, bsz_e) ->
                let bid_px = parse_float_fast msg bpx_s bpx_e in
                let bid_sz = parse_float_fast msg bsz_s bsz_e in
                let asks_start = bids_end + 2 in
                if asks_start >= msg_len
                then 0.0, 0.0, 0.0, 0.0, false
                else (
                  match String.index_from_opt msg asks_start ']' with
                  | None -> 0.0, 0.0, 0.0, 0.0, false
                  | Some asks_end ->
                    (match find_key px_key asks_start asks_end with
                     | None -> 0.0, 0.0, 0.0, 0.0, false
                     | Some (apx_s, apx_e) ->
                       (match find_key sz_key (apx_e + 1) asks_end with
                        | None -> 0.0, 0.0, 0.0, 0.0, false
                        | Some (asz_s, asz_e) ->
                          let ask_px = parse_float_fast msg apx_s apx_e in
                          let ask_sz = parse_float_fast msg asz_s asz_e in
                          bid_px, bid_sz, ask_px, ask_sz, true))))))
      else search (idx + 1)
  in
  search 0
[@@ocaml.doc
  " Zero-allocation depth-1 top-of-book parser: extracts the first bid and ask [{px, sz}]\n\
  \    from the raw JSON without intermediate collections. Returns\n\
  \    [(bid_px, bid_sz, ask_px, ask_sz, true)] on success, [(0, 0, 0, 0, false)] on \
   failure.\n\
  \    Layout: \"levels\":[[bids],[asks]]. "]
;;

let process_raw_market_data msg =
  if String.starts_with ~prefix:"{\"channel\":\"l2Book\"," msg
  then (
    let coin = get_coin msg in
    if coin <> ""
    then (
      match find_registered_symbol coin with
      | Some symbol ->
        (try
           let store = ensure_store symbol in
           let bid_px, bid_sz, ask_px, ask_sz, ok = parse_tob_fast msg in
           if ok
           then Atomic.set store.tob { bid_px; bid_sz; ask_px; ask_sz; tob_valid = true };
           RingBuffer.write store.buffer msg;
           notify_ready store;
           Concurrency.Exchange_wakeup.signal ~symbol;
           (* A perp l2Book is keyed by the bare coin, but the attached strategy domain is
              the spot pair. Wake that domain too so overlay trailing reacts to the perp
              top of book instead of waiting for a spot tick. *)
           if not (String.contains symbol '/')
           then (
             let spot = symbol ^ "/USDC" in
             match Ds.Cow_table.find_opt stores spot with
             | Some _ -> Concurrency.Exchange_wakeup.signal ~symbol:spot
             | None -> ())
         with
         | exn ->
           Logging.warn_f
             ~section
             "Failed to parse l2Book for %s: %s"
             coin
             (Printexc.to_string exn))
      | None -> ()))
[@@ocaml.doc
  " Hot-path orderbook processor. Uses zero-allocation depth-1 TOB parse to update the\n\
  \    mutable cache directly. Still writes to the ring buffer for any consumers that need\n\
  \    full snapshots, but the domain-critical get_best_bid_ask_fast path reads from the\n\
  \    mutable cache. "]
;;

let parse_orderbook symbol msg =
  let bids, asks = get_bids_asks msg in
  { symbol; bids; asks; timestamp = Unix.gettimeofday () }
[@@ocaml.doc
  " Parse a raw l2Book message into an [orderbook] on demand (the full book is parsed\n\
  \    lazily, only when a consumer reads the ring buffer). "]
[@@inline always]
;;

let get_latest_orderbook symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store ->
    (match RingBuffer.read_latest store.buffer with
     | Some msg -> Some (parse_orderbook symbol msg)
     | None -> None)
  | None -> None
[@@inline always]
;;

let get_best_bid_ask symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store ->
    let t = Atomic.get store.tob in
    if t.tob_valid then Some (t.bid_px, t.bid_sz, t.ask_px, t.ask_sz) else None
  | None -> None
[@@ocaml.doc
  " Read top-of-book from the atomic snapshot cache (zero allocation, lock-free, and free\n\
  \    of torn reads per L4). "]
[@@inline always]
;;

let get_best_bid_ask_fast symbol =
  let store = ensure_store symbol in
  fun () ->
    let t = Atomic.get store.tob in
    if t.tob_valid then Some (t.bid_px, t.bid_sz, t.ask_px, t.ask_sz) else None
[@@ocaml.doc
  " Returns a cached closure that reads top-of-book from the atomic TOB cache. The closure\n\
  \    itself allocates nothing; it reads one Atomic record and wraps it in a Some \
   tuple. The\n\
  \    store pointer is captured at closure-creation time so the domain hot loop never\n\
  \    touches the stores Hashtbl. "]
[@@inline always]
;;

let read_orderbook_events symbol last_pos =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store ->
    List.map (parse_orderbook symbol) (RingBuffer.read_since store.buffer last_pos)
  | None -> []
[@@ocaml.doc
  " Returns all orderbook snapshots written since [last_pos], parsing the raw messages\n\
  \    lazily on read. "]
[@@inline always]
;;

let iter_orderbook_events symbol last_pos f =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store ->
    ignore
      (RingBuffer.iter_since store.buffer last_pos (fun msg ->
         let ob = parse_orderbook symbol msg in
         f ob));
    RingBuffer.get_position store.buffer
  | None -> last_pos
[@@ocaml.doc
  " Iterates [f] over orderbook snapshots since [last_pos], parsing each raw message\n\
  \    lazily on read and without intermediate list allocation. Returns the new cursor\n\
  \    position. "]
[@@inline always]
;;

let get_current_position symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0
[@@ocaml.doc " Returns the current ring buffer write position for [symbol]. "]
[@@inline always]
;;

let get_current_position_fast symbol =
  let store = ensure_store symbol in
  fun () -> RingBuffer.get_position store.buffer
[@@inline always]
;;

let has_orderbook_data symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false
;;

let wait_for_orderbook_data symbols timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_orderbook_data symbols
    then Lwt.return_true
    else (
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0
      then Lwt.return_false
      else
        Lwt.pick
          [ (Lwt_condition.wait ready_condition >|= fun () -> `Again)
          ; (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
          ]
        >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_orderbook_data symbols))
  in
  loop ()
;;

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_raw_market_data () in
    Lwt.catch
      (fun () ->
        Logging.debug_f ~section "Starting Hyperliquid orderbook processor task";
        let __ppx_lwt_0 =
          Concurrency.Lwt_util.consume_stream process_raw_market_data sub.stream
        in
        Lwt.backtrace_bind
          "src/integrations/venues/hyperliquid/hyperliquid_orderbook_feed.ml"
          461
          (fun exn ->
            try Lwt.reraise exn with
            | exn -> exn)
          __ppx_lwt_0
          (fun () ->
            sub.close ();
            Logging.info ~section "Orderbook stream ended (disconnect), re-subscribing...";
            Lwt.async run;
            Lwt.return_unit))
      (fun exn ->
        sub.close ();
        Logging.error_f
          ~section
          "Hyperliquid orderbook processor task crashed: %s. Re-subscribing..."
          (Printexc.to_string exn);
        Lwt.async run;
        Lwt.return_unit)
  in
  Lwt.async run
;;

let initialize symbols =
  Logging.debug_f
    ~section
    "Initializing Hyperliquid orderbook feed for %d symbols"
    (List.length symbols);
  List.iter
    (fun symbol ->
      let _ = ensure_store symbol in
      (* Perp l2Book frames are keyed by the bare base coin; give them a store too, or
         [find_registered_symbol] drops the frame. *)
      (match String.split_on_char '/' symbol with
       | base :: _ when base <> symbol -> ignore (ensure_store base)
       | _ -> ());
      let coin = Hyperliquid_instruments_feed.get_subscription_coin symbol in
      if coin <> "" then Ds.Cow_table.set coin_to_symbol coin symbol;
      Logging.debug_f
        ~section
        "Created Hyperliquid orderbook buffer for %s (coin=%s)"
        symbol
        coin)
    symbols
;;
