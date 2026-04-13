(**
   Kraken V2 WebSocket orderbook feed.
   Maintains per-symbol orderbook state via lock-free ring buffers.
   Validates orderbook integrity using CRC32 checksums per Kraken specification.
   Sequence tracking enforces strict ordering; gaps or rollbacks trigger resynchronization.
*)

open Lwt.Infix

let section = "kraken_orderbook"

let get_conduit_ctx = Kraken_common_types.get_conduit_ctx

let orderbook_depth = Kraken_common_types.default_orderbook_depth
let ring_buffer_size = Kraken_common_types.default_ring_buffer_size_orderbook

(** Atomic flag indicating whether cleanup handlers have been initialized. *)
let cleanup_handlers_started = Atomic.make false

(** CRC32 lookup table (IEEE 802.3 polynomial 0xEDB88320). *)
let crc32_table =
  Array.init 256 (fun n ->
    let c = ref (Int32.of_int n) in
    for _ = 0 to 7 do
      if Int32.logand !c 1l <> 0l then
        c := Int32.logxor (Int32.shift_right_logical !c 1) 0xEDB88320l
      else
        c := Int32.shift_right_logical !c 1
    done;
    !c)

let crc32_zlib s =
  let crc = ref 0xFFFFFFFFl in
  for i = 0 to String.length s - 1 do
    let byte = Char.code s.[i] in
    let idx = Int32.to_int (Int32.logand (Int32.logxor !crc (Int32.of_int byte)) 0xFFl) in
    crc := Int32.logxor crc32_table.(idx) (Int32.shift_right_logical !crc 8)
  done;
  Int32.logxor !crc 0xFFFFFFFFl

let update_crc crc char =
  let byte = Char.code char in
  let idx = Int32.to_int (Int32.logand (Int32.logxor crc (Int32.of_int byte)) 0xFFl) in
  Int32.logxor crc32_table.(idx) (Int32.shift_right_logical crc 8)

let add_normalized_to_crc crc s =
  let len = String.length s in
  let rec find_first i =
    if i >= len then crc
    else
      let c = s.[i] in
      if c = '0' || c = '.' then find_first (i + 1)
      else
        let rec feed c_state j =
          if j >= len then c_state
          else
            let c2 = s.[j] in
            if c2 = '.' then feed c_state (j + 1)
            else feed (update_crc c_state c2) (j + 1)
        in feed crc i
  in
  find_first 0

type level = {
  price: string;
  size: string;
  price_float: float;
  size_float: float;
  crc_price: string;
  crc_size: string;
}

type orderbook = {
  symbol: string;
  bids: level array;
  asks: level array;
  sequence: int64 option;
  checksum: int32 option; (** CRC32 checksum received from Kraken for validation. *)
  timestamp: float;
}

(** Return the first [n] elements of a list. Returns all elements if the list has fewer than [n]. *)
let take n lst =
  let rec take_aux acc n = function
    | [] -> List.rev acc
    | h :: t when n > 0 -> take_aux (h :: acc) (n - 1) t
    | _ -> List.rev acc
  in
  take_aux [] n lst

let to_decimal_str ?(trim_trailing=true) ?dec json =
  match json with
  | `String s -> s
  | `Float f ->
      let d = match dec with Some d -> d | None -> 12 in
      let s = Printf.sprintf "%.*f" d f in
      if not (String.contains s '.') || not trim_trailing then s
      else
        let parts = String.split_on_char '.' s in
        (match parts with
         | [whole; frac] ->
             if trim_trailing then
               (* Trim trailing zeros from fractional part. *)
               let len = String.length frac in
               let rec rtrim i =
                 if i <= 0 then ""
                 else if frac.[i-1] = '0' then rtrim (i-1)
                 else String.sub frac 0 i
               in
               let frac_clean = rtrim len in
               if String.length frac_clean = 0 then whole else whole ^ "." ^ frac_clean
             else whole ^ "." ^ frac
         | _ -> s)
  | `Int i -> string_of_int i
  | `Intlit s -> s
  | _ -> "0"

(** Parse price and quantity strings from JSON values for checksum input. *)
let parse_checksum_level price_json qty_json =
  let price_str = to_decimal_str ~trim_trailing:true price_json in
  let qty_str   = to_decimal_str ~trim_trailing:true qty_json in
  (price_str, qty_str)

(** Compute CRC32 checksum from raw JSON bid/ask arrays using the top 10 levels per side.
    Operates directly on JSON to preserve original string precision. *)
let calculate_checksum_from_json symbol bids_json asks_json : int32 =
  let parse_checksum_levels json =
    match json with
    | `List entries ->
        List.filter_map (fun entry ->
          match entry with
          | `Assoc fields ->
              (match List.assoc_opt "price" fields, List.assoc_opt "qty" fields with
               | Some price_json, Some qty_json ->
                   let price_str, qty_str = parse_checksum_level price_json qty_json in
                   Some (price_str, qty_str)
               | _ -> None)
          | `List [price_json; qty_json] -> Some (parse_checksum_level price_json qty_json)
          | `List [price_json; qty_json; _] -> Some (parse_checksum_level price_json qty_json)
          | _ -> None
        ) entries
    | _ -> []
  in

  let bids_levels = parse_checksum_levels bids_json in
  let asks_levels = parse_checksum_levels asks_json in

  (* Check if quantity string represents zero by scanning for any non-zero, non-decimal digit. *)
  let is_effectively_zero qty_str =
    let rec has_non_zero s i =
      if i >= String.length s then false
      else if s.[i] <> '0' && s.[i] <> '.' then true
      else has_non_zero s (i + 1)
    in
    not (has_non_zero qty_str 0)
  in

  (* Exclude levels with zero quantity. *)
  let valid_bids = List.filter (fun (_, qty_str) -> not (is_effectively_zero qty_str)) bids_levels in
  let valid_asks = List.filter (fun (_, qty_str) -> not (is_effectively_zero qty_str)) asks_levels in

  (* Sort bids descending by price, asks ascending by price. *)
  let sorted_bids = List.sort (fun (p1, _) (p2, _) ->
    Float.compare (float_of_string p2) (float_of_string p1)
  ) valid_bids in

  let sorted_asks = List.sort (fun (p1, _) (p2, _) ->
    Float.compare (float_of_string p1) (float_of_string p2)
  ) valid_asks in

  (* Select up to 10 levels per side. No padding per Kraken specification. *)
  let top_bids = take (min 10 (List.length sorted_bids)) sorted_bids in
  let top_asks = take (min 10 (List.length sorted_asks)) sorted_asks in

  Logging.debug_f ~section "Checksum input: symbol=%s bids=%d asks=%d" symbol (List.length bids_levels) (List.length asks_levels);
  Logging.debug_f ~section "Checksum levels used: bids=%d asks=%d" (List.length top_bids) (List.length top_asks);

  let crc = ref 0xFFFFFFFFl in
  List.iter (fun (price_str, qty_str) ->
    crc := add_normalized_to_crc !crc price_str;
    crc := add_normalized_to_crc !crc qty_str
  ) top_asks;
  List.iter (fun (price_str, qty_str) ->
    crc := add_normalized_to_crc !crc price_str;
    crc := add_normalized_to_crc !crc qty_str
  ) top_bids;
  
  let result = Int32.logxor !crc 0xFFFFFFFFl in

  Logging.debug_f ~section "Checksum CRC32: result=%ld (0x%08lx)"
    result result;

  result

(** Lock-free ring buffer for orderbook snapshots. Aliases the shared implementation. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

module PriceMap = Map.Make (struct
  type t = string
  let compare = String.compare
end)

(** Per-symbol mutable orderbook state, including bid/ask maps, ring buffer, and synchronization metadata. *)
type store = {
  buffer: orderbook RingBuffer.t;
  mutable bids: level PriceMap.t;
  mutable asks: level PriceMap.t;
  ready: bool Atomic.t;
  has_snapshot: bool Atomic.t;  (** True after an initial snapshot has been received. Updates are rejected until set. *)
  last_sequence: int64 option Atomic.t;  (** Last processed sequence number. Used for gap and rollback detection. *)
  mutable last_update: float;  (** Unix timestamp of the most recent data write. Used for staleness pruning. *)
}

type decimals = int * int  (** (pair_decimals, lot_decimals) precision tuple from AssetPairs API. *)

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let decimals_tbl : (string, decimals) Hashtbl.t = Hashtbl.create 16
let ready_condition = Lwt_condition.create ()

(** Retrieve price and quantity precision from the instruments feed cache. Returns None on failure. *)
let get_precision_from_instruments symbol =
  try

    Kraken_instruments_feed.get_precision_info symbol
  with _ -> None

let calculate_checksum symbol bids asks : int32 =
  (* Check if size string represents zero by scanning for any non-zero, non-decimal digit. *)
  let is_effectively_zero size =
    let rec has_non_zero s i =
      if i >= String.length s then false
      else if s.[i] <> '0' && s.[i] <> '.' then true
      else has_non_zero s (i + 1)
    in
    not (has_non_zero size 0)
  in

  (* Exclude levels with zero quantity. *)
  let valid_bids = Array.to_list bids |> List.filter (fun level -> not (is_effectively_zero level.size)) in
  let valid_asks = Array.to_list asks |> List.filter (fun level -> not (is_effectively_zero level.size)) in

  (* Sort bids descending by price, asks ascending by price. *)
  let sorted_bids = List.sort (fun l1 l2 ->
    Float.compare l2.price_float l1.price_float
  ) valid_bids in

  let sorted_asks = List.sort (fun l1 l2 ->
    Float.compare l1.price_float l2.price_float
  ) valid_asks in

  (* Select up to 10 levels per side. No padding per Kraken specification. *)
  let top_bids = take (min 10 (List.length sorted_bids)) sorted_bids in
  let top_asks = take (min 10 (List.length sorted_asks)) sorted_asks in

  Logging.debug_f ~section "Checksum input: symbol=%s bids=%d asks=%d" symbol (Array.length bids) (Array.length asks);
  Logging.debug_f ~section "Checksum levels used: bids=%d asks=%d" (List.length top_bids) (List.length top_asks);

  let crc = ref 0xFFFFFFFFl in
  List.iter (fun (level: level) ->
    crc := add_normalized_to_crc !crc level.crc_price;
    crc := add_normalized_to_crc !crc level.crc_size
  ) top_asks;
  List.iter (fun (level: level) ->
    crc := add_normalized_to_crc !crc level.crc_price;
    crc := add_normalized_to_crc !crc level.crc_size
  ) top_bids;

  let result = Int32.logxor !crc 0xFFFFFFFFl in

  Logging.debug_f ~section "Checksum CRC32: result=%ld (0x%08lx)"
    result result;

  result

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let store = {
        buffer = RingBuffer.create ring_buffer_size;
        bids = PriceMap.empty;
        asks = PriceMap.empty;
        ready = Atomic.make false;
        has_snapshot = Atomic.make false;
        last_sequence = Atomic.make None;
        last_update = Unix.time ();
      } in
        Hashtbl.add stores symbol store;
      store

let store_opt symbol = Hashtbl.find_opt stores symbol

let notify_ready ~symbol store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try
      Lwt_condition.broadcast ready_condition ()
    with _ ->
      (* Ignore all exceptions during broadcast - waiters may have been cancelled *)
      ())
  end;
  Concurrency.Exchange_wakeup.signal ~symbol

let is_effectively_zero size =
  (* Returns true if the string contains no non-zero, non-decimal, non-sign digits. *)
  let rec has_non_zero s i =
    if i >= String.length s then false
    else if s.[i] <> '0' && s.[i] <> '.' && s.[i] <> '-' then true
    else has_non_zero s (i + 1)
  in
  not (has_non_zero size 0)

let int64_of_json json =
  match json with
  | `Int i -> Some (Int64.of_int i)
  | `Intlit s -> (try Some (Int64.of_string s) with _ -> None)
  | `Float f -> (try Some (Int64.of_float f) with _ -> None)
  | `String s -> (try Some (Int64.of_string s) with _ -> None)
  | _ -> None

let int32_of_json json =
  match json with
  | `Int i -> Some (Int32.of_int i)
  | `Intlit s -> (try Some (Int32.of_string s) with _ -> None)
  | `Float f -> (try Some (Int32.of_float f) with _ -> None)
  | `String s -> (try Some (Int32.of_string s) with _ -> None)
  | _ -> None

let parse_level symbol price_json size_json =
  let pd, ld =
    match get_precision_from_instruments symbol with
    | Some (price_prec, qty_prec) -> (price_prec, qty_prec)
    | None ->
        (try Hashtbl.find decimals_tbl symbol
         with Not_found -> (8, 8))
  in

  let price_str = to_decimal_str ~dec:pd price_json in
  let qty_str = to_decimal_str ~dec:ld size_json in
  let price_float = try float_of_string price_str with _ -> 0.0 in
  let qty_float = try float_of_string qty_str with _ -> 0.0 in
  (* Skip CRC string formatting when checksum validation is disabled (depth < 10).
     Printf.sprintf "%.*f" allocates 2 strings per level per tick — eliminated here. *)
  let crc_price = if orderbook_depth >= 10 then Printf.sprintf "%.*f" pd price_float else "" in
  let crc_size = if orderbook_depth >= 10 then Printf.sprintf "%.*f" ld qty_float else "" in
  Some { price = price_str; size = qty_str; price_float; size_float = qty_float; crc_price; crc_size }

(** Parse a JSON array of price levels into (price_str, qty_str, price_float, qty_float) tuples.
    Supports both object format ({price, qty}) and legacy array format ([price, qty]). *)
let parse_levels symbol json =
  match json with
  | `List entries -> List.filter_map (fun entry ->
      match entry with
      (* Object format: {"price": ..., "qty": ...} *)
      | `Assoc fields ->
          (match List.assoc_opt "price" fields, List.assoc_opt "qty" fields with
           | Some price_json, Some qty_json -> parse_level symbol price_json qty_json
           | _ -> None)
      (* Array format: [price, qty] or [price, qty, timestamp] *)
      | `List [price_json; size_json] -> parse_level symbol price_json size_json
      | `List [price_json; size_json; _] -> parse_level symbol price_json size_json
      | _ -> None
    ) entries
  | _ -> []

(** Apply incremental level updates to a PriceMap. Zero-quantity levels are removed; non-zero levels are upserted. *)
let apply_levels map levels =
  List.fold_left (fun acc (lvl: level) ->
    if is_effectively_zero lvl.size then
      PriceMap.remove lvl.price acc
    else
      PriceMap.add lvl.price lvl acc
  ) map levels

(** Convert a PriceMap to a sorted level array truncated to [depth] entries.
    [sort_desc] controls descending (bids) vs ascending (asks) order. *)
let levels_to_array ?(sort_desc = false) map depth =
  let levels_list = PriceMap.fold (fun _ lvl acc -> lvl :: acc) map [] in

  let sorted_levels = List.sort (fun l1 l2 ->
    if sort_desc then Float.compare l2.price_float l1.price_float else Float.compare l1.price_float l2.price_float
  ) levels_list in

  let rec take n lst acc =
    if n = 0 then List.rev acc
    else match lst with
    | [] -> List.rev acc
    | level :: rest -> take (n - 1) rest (level :: acc)
  in

  Array.of_list (take depth sorted_levels [])

(** Rebuild a PriceMap containing only the top [max_levels] entries. Used to bound map size. *)
let rebuild_map_from_top_levels map sort_desc max_levels =
  let levels_array = levels_to_array ~sort_desc map max_levels in
  Array.fold_left (fun acc lvl ->
    PriceMap.add lvl.price lvl acc
  ) PriceMap.empty levels_array

(** Construct an [orderbook] record from the current store state and the raw JSON entry metadata. *)
let build_orderbook store symbol entry =
  let open Yojson.Safe.Util in
  let sequence =
    match int64_of_json (member "sequence" entry) with
    | Some seq -> Some seq
    | None -> None
  in
  (* Skip checksum JSON extraction when depth < 10: checksum validation is
     bypassed anyway, and parsing the JSON field allocates Int32 boxes. *)
  let checksum =
    if orderbook_depth >= 10 then
      let checksum_json = member "checksum" entry in
      match checksum_json with
      | `Int i -> Some (Int32.of_int i)
      | `Intlit s ->
          (try Some (Int32.of_string s) with _ -> None)
      | `Float f ->
          (try Some (Int32.of_float f) with _ -> None)
      | `String s ->
          (try Some (Int32.of_string s) with _ -> None)
      | _ -> None
    else None
  in
  (* Depth-1 fast path: construct singleton arrays directly from PriceMap
     tree traversal (O(log n)), avoiding the fold->sort->take->of_list pipeline
     in levels_to_array which allocates 4 intermediate structures. *)
  let bids =
    if orderbook_depth = 1 then
      match PriceMap.max_binding_opt store.bids with
      | Some (_, v) -> [|v|]
      | None -> [||]
    else levels_to_array ~sort_desc:true store.bids orderbook_depth
  in
  let asks =
    if orderbook_depth = 1 then
      match PriceMap.min_binding_opt store.asks with
      | Some (_, v) -> [|v|]
      | None -> [||]
    else levels_to_array ~sort_desc:false store.asks orderbook_depth
  in
  {
    symbol;
    bids;
    asks;
    sequence;
    checksum;
    timestamp = Unix.time ();
  }

(** Fetch price and lot decimal precision for the given symbols.
    Skips the AssetPairs REST call if all symbols already have precision data from the instruments feed. *)
let fetch_decimals symbols =
  let all_have_instruments_data =
    List.for_all (fun symbol ->
      match Kraken_instruments_feed.get_precision_info symbol with
      | Some _ -> true
      | None -> false
    ) symbols
  in

  if all_have_instruments_data then (
    Logging.debug_f ~section "All symbols have instruments precision data, skipping AssetPairs API call";
    Lwt.return ()
  ) else (
    Logging.debug_f ~section "Some symbols missing instruments precision data, fetching from AssetPairs API";
    let uri = Uri.of_string "https://api.kraken.com/0/public/AssetPairs" in
    Cohttp_lwt_unix.Client.get uri >>= fun (_resp, body) ->
    Cohttp_lwt.Body.to_string body >>= fun body_str ->
    try
      let json = Yojson.Safe.from_string body_str in
      let open Yojson.Safe.Util in
      match member "error" json with
      | `List [] ->
          let result = member "result" json in
          let pairs = to_assoc result in
          List.iter (fun (_pair_name, pair_json) ->
            let altname = to_string_option (member "altname" pair_json) in
            let wsname = to_string_option (member "wsname" pair_json) in
            let pd = to_int (member "pair_decimals" pair_json) in
            let ld = to_int (member "lot_decimals" pair_json) in
            List.iter (fun sym ->
              let norm = sym in
              let no_slash = String.concat "" (String.split_on_char '/' sym) in
              let is_ws_match = match wsname with
                | Some n -> n = norm || (n = "XBT/USD" && norm = "BTC/USD") || (n = "XETH/USD" && norm = "ETH/USD") || (n = "XXBTZUSD" && norm = "BTC/USD")
                | None -> false
              in
              let is_alt_match = match altname with
                | Some n -> n = no_slash || (n = "XBTUSD" && no_slash = "BTCUSD") || (n = "XETHUSD" && no_slash = "ETHUSD") || (n = "XXBTZUSD" && no_slash = "BTCUSD")
                | None -> false
              in
              if is_ws_match || is_alt_match then
                Hashtbl.add decimals_tbl sym (pd, ld)
            ) symbols
          ) pairs;
          Lwt.return ()
      | _ -> Lwt.fail_with "Error fetching asset pairs"
    with exn ->
      Logging.warn_f ~section "Failed to fetch decimals: %s" (Printexc.to_string exn);
      Lwt.return ()
  )

let notified_symbols_reusable : (string, store) Hashtbl.t = Hashtbl.create 16

(** Process a single orderbook WebSocket message. When [reset] is true, the message
    is treated as a snapshot (full state replacement). Otherwise it is an incremental update.
    Performs sequence validation, checksum verification, and ring buffer writes.
    Returns [Some ()] on successful parse, [None] on failure. *)
let process_orderbook_message ~reset json on_heartbeat =
  let open Yojson.Safe.Util in
  try
    let data = member "data" json |> to_list in
    Hashtbl.clear notified_symbols_reusable;
    let notified_symbols = notified_symbols_reusable in
    List.iter (fun entry ->
      try
        let symbol = member "symbol" entry |> to_string in
        let store = ensure_store symbol in

        if reset then begin
          (* Snapshot: clear existing state and reinitialize from this message. *)
          store.bids <- PriceMap.empty;
          store.asks <- PriceMap.empty;
          Atomic.set store.has_snapshot true;


          let sequence =
            match int64_of_json (member "sequence" entry) with
            | Some seq -> Some seq
            | None -> None
          in
          Atomic.set store.last_sequence sequence;
          Logging.debug_f ~section "Received snapshot for %s (sequence=%s), ready for updates"
            symbol (match sequence with Some s -> Int64.to_string s | None -> "none");

        end else begin
          (* Incremental update: discard if no snapshot has been received yet. *)
          if not (Atomic.get store.has_snapshot) then begin
            Logging.debug_f ~section "Ignoring update for %s: waiting for snapshot after reconnect"
              symbol;
            raise Exit  (* Skip processing this entry *)
          end;

          (* Validate monotonic sequence ordering. Rollbacks and gaps trigger full resync. *)
          let current_sequence =
            match int64_of_json (member "sequence" entry) with
            | Some seq -> Some seq
            | None -> None
          in
          let last_seq_opt = Atomic.get store.last_sequence in
          match current_sequence, last_seq_opt with
          | Some curr_seq, Some last_seq when Int64.compare curr_seq last_seq <= 0 ->
              Logging.warn_f ~section "Sequence rollback for %s: current=%Ld last=%Ld, marking out-of-sync"
                symbol curr_seq last_seq;
              store.bids <- PriceMap.empty;
              store.asks <- PriceMap.empty;
              RingBuffer.clear store.buffer;
              Atomic.set store.has_snapshot false;
              Atomic.set store.last_sequence None;
              raise Exit  (* Skip processing this entry *)
          | Some curr_seq, Some last_seq when Int64.compare curr_seq (Int64.add last_seq 1L) > 0 ->
              let gap = Int64.sub curr_seq last_seq in
              Logging.warn_f ~section "Sequence gap for %s: current=%Ld last=%Ld (gap=%Ld), marking out-of-sync"
                symbol curr_seq last_seq gap;
              store.bids <- PriceMap.empty;
              store.asks <- PriceMap.empty;
              RingBuffer.clear store.buffer;
              Atomic.set store.has_snapshot false;
              Atomic.set store.last_sequence None;
              raise Exit  (* Skip processing this entry *)
          | _ -> ()
        end;
        let bids_json = member "bids" entry in
        let asks_json = member "asks" entry in
        let bids = parse_levels symbol bids_json in
        let asks = parse_levels symbol asks_json in

        store.bids <- apply_levels store.bids bids;
        store.asks <- apply_levels store.asks asks;
        store.last_update <- Unix.time ();

        (* Truncate maps to configured bound, limiting memory usage.
           Depth-1 fast path: use PriceMap.max/min_binding_opt (O(log n),
           zero intermediate allocation) instead of the full
           fold->sort->take->of_list->fold rebuild pipeline. *)
        if orderbook_depth = 1 then begin
          store.bids <- (match PriceMap.max_binding_opt store.bids with
            | Some (k, v) -> PriceMap.singleton k v
            | None -> PriceMap.empty);
          store.asks <- (match PriceMap.min_binding_opt store.asks with
            | Some (k, v) -> PriceMap.singleton k v
            | None -> PriceMap.empty);
        end else begin
          store.bids <- rebuild_map_from_top_levels store.bids true orderbook_depth;
          store.asks <- rebuild_map_from_top_levels store.asks false orderbook_depth;
        end;

        let orderbook = build_orderbook store symbol entry in

        (* Compute and verify CRC32 from current state using top 10 levels per side. 
           If the configured depth is < 10, checksum validation is bypassed because 
           the stored map lacks the requisite levels to evaluate the CRC. *)
        let checksum_valid =
          if orderbook_depth >= 10 then begin
            let calculated_checksum = calculate_checksum symbol
              (levels_to_array ~sort_desc:true store.bids 10)
              (levels_to_array ~sort_desc:false store.asks 10) in
            match orderbook.checksum with
            | Some received_checksum ->
                if Int32.compare calculated_checksum received_checksum <> 0 then begin
                  Logging.warn_f ~section "Checksum mismatch for %s: received=%ld (0x%08lx) calculated=%ld (0x%08lx), marking out-of-sync"
                    symbol received_checksum received_checksum calculated_checksum calculated_checksum;

                  store.bids <- PriceMap.empty;
                  store.asks <- PriceMap.empty;
                  RingBuffer.clear store.buffer;
                  Atomic.set store.has_snapshot false;
                  Atomic.set store.last_sequence None;
                  false
                end else
                  true
            | None -> true
          end else
            true
        in

        if checksum_valid then begin
          RingBuffer.write store.buffer orderbook;
          Hashtbl.replace notified_symbols symbol store;


          let current_sequence =
            match int64_of_json (member "sequence" entry) with
            | Some seq -> Some seq
            | None -> None
          in
          Atomic.set store.last_sequence current_sequence;
        end;


        on_heartbeat ()
      with
      | Exit -> ()  (* Control flow: entry skipped due to missing snapshot or sequence error. *)
      | exn ->
        Logging.warn_f ~section "Failed to process orderbook entry: %s"
          (Printexc.to_string exn)
    ) data;

    (* Broadcast readiness and signal exchange wakeup for each symbol that received a valid write. *)
    Hashtbl.iter (fun symbol store -> notify_ready ~symbol store) notified_symbols;

    Some ()
  with exn ->
    Logging.warn_f ~section "Failed to parse orderbook message: %s"
      (Printexc.to_string exn);
    None

let[@inline always] get_latest_orderbook symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

let[@inline always] get_best_bid_ask symbol =
  match get_latest_orderbook symbol with
  | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
      let bid = bids.(0) in
      let ask = asks.(0) in
      Some (bid.price_float, bid.size_float, ask.price_float, ask.size_float)
  | _ -> None

let[@inline always] get_best_bid_ask_fast symbol =
  let store = ensure_store symbol in
  (fun () ->
     match RingBuffer.read_latest store.buffer with
     | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
         let bid = bids.(0) in
         let ask = asks.(0) in
         Some (bid.price_float, bid.size_float, ask.price_float, ask.size_float)
     | _ -> None
  )

(** Read all orderbook snapshots written since [last_pos]. Returns an empty list if the symbol is unknown. *)
let[@inline always] read_orderbook_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Iterate over orderbook snapshots since [last_pos] without allocating an intermediate list.
    Returns the new read position. *)
let[@inline always] iter_orderbook_events symbol last_pos f =
  match store_opt symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

(** Return the current ring buffer write position for the given symbol. Returns 0 if unknown. *)
let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let[@inline always] get_current_position_fast symbol =
  let store = ensure_store symbol in
  (fun () -> RingBuffer.get_position store.buffer)

let get_top_levels ?(depth = orderbook_depth) symbol =
  match get_latest_orderbook symbol with
  | None -> ([||], [||])
  | Some ob ->
      let trim arr =
        let count = min depth (Array.length arr) in
        Array.init count (fun idx -> arr.(idx))
      in
      (trim ob.bids, trim ob.asks)

let has_orderbook_data symbol =
  match store_opt symbol with
  | Some store when Atomic.get store.ready -> Option.is_some (RingBuffer.read_latest store.buffer)
  | _ -> false

(** Reset all per-symbol stores: clear bid/ask maps, replace ring buffers, and unset readiness flags.
    Called on reconnection to ensure no stale data persists. *)
let clear_all_stores () =
  Hashtbl.iter (fun symbol store ->
    Logging.debug_f ~section "Clearing orderbook store for %s" symbol;
    store.bids <- PriceMap.empty;
    store.asks <- PriceMap.empty;

    RingBuffer.clear store.buffer;
    Atomic.set store.ready false;
    Atomic.set store.has_snapshot false;
    Atomic.set store.last_sequence None;
    store.last_update <- Unix.time ()
  ) stores

(** Remove stores inactive for over 30 minutes and trim oversized price maps to [max_price_levels].
    Prevents unbounded memory growth from abandoned subscriptions or accumulated levels. *)
let prune_stale_data () =
  let now = Unix.gettimeofday () in
  let stale_threshold = 30.0 *. 60.0 in
  let max_price_levels = 100 in

  let stores_to_remove = ref [] in
  let trimmed_stores = ref [] in
  let total_stores_before = Hashtbl.length stores in

  Hashtbl.iter (fun symbol store ->
    let age = now -. store.last_update in


    if age > stale_threshold then begin
      stores_to_remove := symbol :: !stores_to_remove
    end else begin

      let bids_count = PriceMap.cardinal store.bids in
      let asks_count = PriceMap.cardinal store.asks in
      let trimmed = ref false in

      if bids_count > max_price_levels then begin
        store.bids <- rebuild_map_from_top_levels store.bids true max_price_levels;
        trimmed := true
      end;

      if asks_count > max_price_levels then begin
        store.asks <- rebuild_map_from_top_levels store.asks false max_price_levels;
        trimmed := true
      end;

      if !trimmed then
        trimmed_stores := symbol :: !trimmed_stores
    end
  ) stores;


  List.iter (fun symbol ->
    Hashtbl.remove stores symbol;
    Logging.debug_f ~section "Removed stale orderbook store for %s (age > 30min)" symbol
  ) !stores_to_remove;


  if !trimmed_stores <> [] then
    Logging.debug_f ~section "Trimmed price levels for %d active stores: %s"
      (List.length !trimmed_stores) (String.concat ", " !trimmed_stores);

  let stores_removed = List.length !stores_to_remove in
  let stores_trimmed = List.length !trimmed_stores in
  let total_stores_after = Hashtbl.length stores in

  if stores_removed > 0 || stores_trimmed > 0 then
    Logging.info_f ~section "Orderbook cleanup: removed %d stale stores, trimmed %d active stores (%d -> %d total stores)"
      stores_removed stores_trimmed total_stores_before total_stores_after

(** Asynchronously trigger orderbook pruning. [reason] is logged for diagnostics. *)
let trigger_orderbook_cleanup ~reason () =
  Lwt.async (fun () ->
    Logging.debug_f ~section "Triggering orderbook cleanup (reason=%s)" reason;
    prune_stale_data ();
    Lwt.return_unit
  )



let handle_message message on_heartbeat =
  Concurrency.Tick_event_bus.publish_tick ();
  try
    let json = Yojson.Safe.from_string message in
    let open Yojson.Safe.Util in
    let channel = member "channel" json |> to_string_option in
    let msg_type = member "type" json |> to_string_option in
    let method_type = member "method" json |> to_string_option in

    match channel, msg_type, method_type with
  | Some "book", Some "snapshot", _ ->
      ignore (process_orderbook_message ~reset:true json on_heartbeat)
  | Some "book", Some "update", _ ->
      ignore (process_orderbook_message ~reset:false json on_heartbeat)
    | Some "heartbeat", _, _ -> on_heartbeat ()
    | _, _, Some "subscribe" ->
        let result = member "result" json in
        let symbol = member "symbol" result |> to_string in
        Logging.debug_f ~section "Subscribed to %s orderbook feed" symbol
    | Some "status", _, _ ->
        Logging.debug_f ~section "Status message received"
    | _ ->
        Logging.debug_f ~section "Unhandled orderbook payload: %s" message
  with exn ->
    Logging.error_f ~section "Error handling orderbook message: %s - %s"
      (Printexc.to_string exn) message

let wait_for_orderbook_data_lwt symbols timeout_seconds =
  let start_time = Unix.gettimeofday () in
  let timeout_ref = ref false in
  let rec wait_loop () =
    if List.for_all has_orderbook_data symbols then
      Lwt.return_true
    else
      let elapsed = Unix.gettimeofday () -. start_time in
      if elapsed >= timeout_seconds then
        Lwt.return_false
      else if !timeout_ref then
        Lwt.return_false
      else
        (* Block on condition broadcast; treat cancellation as timeout. *)
        Lwt.catch (fun () ->
          Lwt_condition.wait ready_condition >>= fun () -> wait_loop ()
        ) (fun _ ->

          timeout_ref := true;
          Lwt.return_false
        )
  in
  (* Background timer enforces hard timeout via shared ref. *)
  Lwt.async (fun () ->
    Lwt_unix.sleep timeout_seconds >>= fun () ->
    timeout_ref := true;
    Lwt.return_unit
  );
  wait_loop ()

let wait_for_orderbook_data = wait_for_orderbook_data_lwt


(** Subscribe to orderbook channels and enter the asynchronous read loop.
    Invokes [on_failure] on connection loss; resolves the returned promise when the loop terminates. *)
let start_message_handler conn symbols on_failure on_heartbeat =
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "book");
      ("symbol", `List (List.map (fun s -> `String s) symbols));
      ("depth", `Int (max 10 orderbook_depth))
    ])
  ] in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ()) >>= fun () ->

  let stream = Lwt_stream.from (fun () ->
    Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= fun frame ->
      Lwt.return_some frame
    ) (function
      | End_of_file -> Lwt.return_none
      | exn -> Lwt.fail exn)
  ) in

  let process_frame = function
    | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
        Logging.warn ~section "Orderbook WebSocket closed by server";
        on_failure "Connection closed by server";
        Lwt.fail (Failure "Connection closed by server")
    | frame ->
        Lwt.catch
          (fun () ->
             handle_message frame.Websocket.Frame.content on_heartbeat;
             Lwt.return_unit)
          (fun exn ->
             Logging.error_f ~section "Error handling Kraken orderbook frame: %s" (Printexc.to_string exn);
             Lwt.return_unit)
  in

  let done_p =
    Lwt.catch
      (fun () -> Concurrency.Lwt_util.consume_stream (fun frame -> Lwt.async (fun () -> process_frame frame)) stream)
      (fun exn ->
         match exn with
         | Failure msg when msg = "Connection closed by server" ->
             Lwt.return_unit
         | _ ->
             Logging.error_f ~section "Orderbook WebSocket error during read: %s" (Printexc.to_string exn);
             on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
             Lwt.return_unit)
  in
let final_done_p =
    done_p >>= fun () ->
    (* Only handle EOF case if stream completes without error *)
    Logging.warn ~section "Orderbook WebSocket connection closed unexpectedly (End_of_file)";
    on_failure "Connection closed unexpectedly (End_of_file)";
    Lwt.return_unit
  in
  final_done_p

let active_conn = ref None

let subscribe_symbols symbols =
  fetch_decimals symbols >>= fun () ->
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    ()
  ) symbols;
  match !active_conn with
  | Some conn ->
      let subscribe_msg = `Assoc [
        ("method", `String "subscribe");
        ("params", `Assoc [
          ("channel", `String "book");
          ("symbol", `List (List.map (fun s -> `String s) symbols));
          ("depth", `Int (max 10 orderbook_depth))
        ])
      ] in
      let msg_str = Yojson.Safe.to_string subscribe_msg in
      Logging.debug_f ~section "Sending dynamic orderbook subscription for %d symbols" (List.length symbols);
      Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ())
  | None ->
      Logging.warn_f ~section "Cannot dynamically subscribe symbols, orderbook WS not connected";
      Lwt.return_unit

let connect_and_subscribe symbols ~on_failure ~on_heartbeat ~on_connected =
  let uri = Uri.of_string "wss://ws.kraken.com/v2" in

  Logging.debug_f ~section "Connecting to Kraken orderbook WebSocket...";
  Lwt_unix.getaddrinfo "ws.kraken.com" "443" [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let ip = match addresses with
    | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
        Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws.kraken.com"
  in
  let client = `TLS (`Hostname "ws.kraken.com", `IP ip, `Port 443) in
  let ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->

    active_conn := Some conn;
    Logging.debug_f ~section "Orderbook WebSocket established, subscribing...";

    on_connected ();
    start_message_handler conn symbols on_failure on_heartbeat >>= fun () ->
    Logging.debug_f ~section "Orderbook WebSocket connection closed";
    Lwt.return_unit

let initialize symbols =
  Logging.debug_f ~section "Initializing orderbook feed for %d symbols" (List.length symbols);
  fetch_decimals symbols >>= fun () ->
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created orderbook store for %s" symbol
  ) symbols;
  Logging.debug_f ~section "Orderbook feed stores initialized";


  Lwt.return_unit