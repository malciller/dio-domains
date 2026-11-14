[@@@ocaml.text
  " Hyperliquid L2 orderbook feed. Subscribes to WebSocket depth updates and stores\n\
  \    snapshots in per-symbol lock-free ring buffers. "]

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
  ; seq : int Atomic.t
  (** Book revision: bumped by every touch update, whether it arrived as an l2Book
      snapshot or a bbo frame. The hot path polls this to decide whether the market moved;
      the ring buffer position stays the replay cursor for full snapshots. *)
  }

let stores = Ds.Cow_table.create ~shard_count:32 ()
let initialization_mutex = Mutex.create ()

(** Coin -> every store key that carries that market's book. A spot pair maps to its own
    key; a perpetual maps to its bare coin, plus any configured pair that the venue has no
    spot listing for (controller symbols are spot-pair form even when the market they need
    is a perp). The table is built once in {!initialize} from instrument metadata, so a
    frame is routed by exact coin and never by guessing a base asset. *)
let coin_to_symbols : (string, string list) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:32 ()
;;

(** Register that [coin]'s book belongs in [symbol]'s store. Idempotent. Called from the
    Lwt thread only (startup and the dynamic-subscription scan), so the read-modify-write
    needs no lock. *)
let add_route ~coin ~symbol =
  let existing =
    match Ds.Cow_table.find_opt coin_to_symbols coin with
    | Some symbols -> symbols
    | None -> []
  in
  if not (List.mem symbol existing)
  then Ds.Cow_table.set coin_to_symbols coin (symbol :: existing)
;;

let find_registered_symbols coin =
  match Ds.Cow_table.find_opt coin_to_symbols coin with
  | Some symbols -> symbols
  | None -> []
;;

(* A coin with no configured market is a wiring bug, not a hot-path event. Warn once per
   coin so a misconfigured subscription cannot flood the log at feed rate. Frames that
   arrive before {!initialize} are dropped silently: the market set is not known yet. *)
let routes_ready = Atomic.make false
let unrouted_coins : (string, unit) Hashtbl.t = Hashtbl.create 16
let unrouted_mutex = Mutex.create ()

let warn_unrouted_coin coin =
  if Atomic.get routes_ready
  then (
    Mutex.lock unrouted_mutex;
    let first = not (Hashtbl.mem unrouted_coins coin) in
    if first then Hashtbl.replace unrouted_coins coin ();
    Mutex.unlock unrouted_mutex;
    if first
    then
      Logging.warn_f
        ~section
        "No configured Hyperliquid market for l2Book coin %s; dropping its frames"
        coin)
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
          ; seq = Atomic.make 0
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

(* Atomic flag only: book frames are decoded on the parse domain, which must not touch
   Lwt. The startup waiter polls. *)
let notify_ready store = Atomic.set store.ready true

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

(** Top-of-book parser for the [bbo] channel: the venue's only low-latency public touch
    feed ([l2Book] is throttled to one snapshot every few seconds). Layout:
    ["bbo":[{"px":..,"sz":..},{"px":..,"sz":..}]]. *)
let parse_bbo_fast msg =
  let key = "\"bbo\":[" in
  let klen = String.length key in
  let n = String.length msg in
  let rec find i =
    if i + klen > n
    then None
    else if string_match msg i key
    then Some (i + klen)
    else find (i + 1)
  in
  match find 0 with
  | None -> 0.0, 0.0, 0.0, 0.0, false
  | Some start ->
    (match String.index_from_opt msg start ']' with
     | None -> 0.0, 0.0, 0.0, 0.0, false
     | Some stop ->
       (match parse_levels msg start stop 0 [] with
        | { price = bid_px; size = bid_sz } :: { price = ask_px; size = ask_sz } :: _ ->
          bid_px, bid_sz, ask_px, ask_sz, true
        | _ -> 0.0, 0.0, 0.0, 0.0, false))
;;

(** Server event time of a book frame, from its millisecond ["time"] field. Scanned
    directly from the frame: no JSON DOM on the book hot path. [None] when the frame
    carries no timestamp. *)
let extract_event_time_s content =
  let key = "\"time\":" in
  let klen = String.length key in
  let n = String.length content in
  let matches_at i =
    let rec go k = k >= klen || (content.[i + k] = key.[k] && go (k + 1)) in
    i + klen <= n && go 0
  in
  let is_num_char c =
    (c >= '0' && c <= '9') || c = '.' || c = '-' || c = '+' || c = 'e' || c = 'E'
  in
  let rec find i =
    if i + klen > n
    then None
    else if matches_at i
    then (
      let j = ref (i + klen) in
      while !j < n && (content.[!j] = ' ' || content.[!j] = '\t') do
        incr j
      done;
      let start = !j in
      while !j < n && is_num_char content.[!j] do
        incr j
      done;
      if !j > start
      then (
        try
          let ms = float_of_string (String.sub content start (!j - start)) in
          if ms >= 1_000_000_000_000.0 && ms < 10_000_000_000_000.0
          then Some (ms /. 1000.0)
          else None
        with
        | _ -> None)
      else None)
    else find (i + 1)
  in
  find 0
;;

(** Record this frame's clock-corrected one-way transit. Runs on the parse domain and uses
    the receive time captured when the frame was submitted, so the measurement excludes
    parse-queue residency - the same point Kraken's orderbook feed records from. *)
let record_feed_latency msg =
  match extract_event_time_s msg with
  | None -> ()
  | Some event ->
    let recv =
      match Concurrency.Parse_worker.current_recv_wall () with
      | Some t -> t
      | None -> Unix.gettimeofday ()
    in
    Network_latency.record_feed_event_s "hyperliquid" ~event ~recv ()
;;

(** Apply one touch update to every store the coin is routed to. [ring] is set for l2Book
    snapshots, which are the replay log; bbo frames update the touch and revision only. *)
let apply_book_update ~coin ~msg ~bid_px ~bid_sz ~ask_px ~ask_sz ~ok ~ring =
  let symbols = find_registered_symbols coin in
  if symbols = []
  then warn_unrouted_coin coin
  else (
    record_feed_latency msg;
    List.iter
      (fun symbol ->
        try
          let store = ensure_store symbol in
          if ok
          then (
            Atomic.set store.tob { bid_px; bid_sz; ask_px; ask_sz; tob_valid = true };
            Atomic.incr store.seq);
          if ring then RingBuffer.write store.buffer msg;
          notify_ready store;
          Concurrency.Exchange_wakeup.signal ~symbol;
          (* A perp tick also drives the overlay trailing inside the spot-pair domain. The
             pair has its own market (and store) when the venue lists it; only the wake is
             shared, never the book. *)
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
            "Failed to apply book update for %s (coin %s): %s"
            symbol
            coin
            (Printexc.to_string exn))
      symbols)
;;

let process_raw_market_data msg =
  if String.starts_with ~prefix:"{\"channel\":\"l2Book\"," msg
  then (
    let coin = get_coin msg in
    if coin <> ""
    then (
      let bid_px, bid_sz, ask_px, ask_sz, ok = parse_tob_fast msg in
      apply_book_update ~coin ~msg ~bid_px ~bid_sz ~ask_px ~ask_sz ~ok ~ring:true))
  else if String.starts_with ~prefix:"{\"channel\":\"bbo\"," msg
  then (
    let coin = get_coin msg in
    if coin <> ""
    then (
      let bid_px, bid_sz, ask_px, ask_sz, ok = parse_bbo_fast msg in
      apply_book_update ~coin ~msg ~bid_px ~bid_sz ~ask_px ~ask_sz ~ok ~ring:false))
[@@ocaml.doc
  " Hot-path book processor. l2Book snapshots update the touch, bump the book revision,\n\
  \    and are appended to the ring for replay; bbo frames update the touch and revision\n\
  \    only. The domain-critical get_best_bid_ask_fast path reads the mutable cache, and\n\
  \    get_current_position_fast polls the revision, so a throttled depth feed no longer\n\
  \    stalls the decision loop. Writes every store the coin is routed to. "]
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

let get_best_bid_ask_into symbol (buf : float array) =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store ->
    let t = Atomic.get store.tob in
    if t.tob_valid
    then (
      buf.(0) <- t.bid_px;
      buf.(1) <- t.bid_sz;
      buf.(2) <- t.ask_px;
      buf.(3) <- t.ask_sz;
      true)
    else false
  | None -> false
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
  fun () -> Atomic.get store.seq
[@@ocaml.doc
  " Book revision for [symbol], not the ring cursor: it advances on every touch update,\n\
  \    including bbo frames that never touch the ring. The decision loop compares it to \
   its\n\
  \    last seen value; replay/display code uses {!get_current_position}. "]
[@@inline always]
;;

let has_orderbook_data symbol =
  match Ds.Cow_table.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false
;;

let wait_for_orderbook_data symbols timeout_seconds =
  let start_time = Unix.gettimeofday () in
  let rec wait_loop () =
    if List.for_all has_orderbook_data symbols
    then Lwt.return_true
    else if Unix.gettimeofday () -. start_time >= timeout_seconds
    then Lwt.return_false
    else
      let open Lwt.Infix in
      Lwt_unix.sleep 0.025 >>= wait_loop
  in
  wait_loop ()
;;

(** Coin a configured symbol's book arrives under. Exact market first; the only alias is a
    pair naming its own base perpetual when the venue does not list the pair as spot (the
    engine's controller symbols are pair-form even when the hedge market is a perp). *)
let route_coin symbol =
  match Hyperliquid_instruments_feed.lookup_market symbol with
  | Some (_, coin) -> Some coin
  | None ->
    (match String.index_opt symbol '/' with
     | Some i ->
       let base = String.sub symbol 0 i in
       (match Hyperliquid_instruments_feed.lookup_market base with
        | Some (Hyperliquid_instruments_feed.Perp, coin) -> Some coin
        | _ -> None)
     | None -> None)
;;

(** Register [symbol]'s store and route before any frame can arrive. Returns the coin the
    book arrives under, or [None] when the venue lists neither the exact market nor a base
    perpetual to alias. *)
let register_market symbol =
  match route_coin symbol with
  | None -> None
  | Some coin ->
    add_route ~coin ~symbol;
    Some coin
;;

let initialize symbols =
  Logging.debug_f
    ~section
    "Initializing Hyperliquid orderbook feed for %d symbols"
    (List.length symbols);
  List.iter
    (fun symbol ->
      match register_market symbol with
      | Some coin ->
        Logging.debug_f
          ~section
          "Created Hyperliquid orderbook buffer for %s (coin=%s)"
          symbol
          coin
      | None ->
        Logging.warn_f
          ~section
          "No Hyperliquid market for %s; it gets no book store and no subscription"
          symbol)
    symbols;
  Atomic.set routes_ready true
;;
