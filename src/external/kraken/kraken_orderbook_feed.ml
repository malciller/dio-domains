(**
   Kraken V2 WebSocket orderbook feed.
   Maintains per-symbol orderbook state via lock-free ring buffers.
   Validates orderbook integrity using CRC32 checksums per Kraken specification.
   Sequence tracking enforces strict ordering; gaps or rollbacks trigger resynchronization.
*)

open Lwt.Infix

let section = "kraken_orderbook"

type state_t =
  { mutable active_conn : Websocket_lwt_unix.conn option
  ; mutex : Lwt_mutex.t
  }

let state = { active_conn = None; mutex = Lwt_mutex.create () }
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
      if Int32.logand !c 1l <> 0l
      then c := Int32.logxor (Int32.shift_right_logical !c 1) 0xEDB88320l
      else c := Int32.shift_right_logical !c 1
    done;
    !c)
;;

let crc32_zlib s =
  let crc = ref 0xFFFFFFFFl in
  for i = 0 to String.length s - 1 do
    let byte = Char.code s.[i] in
    let idx = Int32.to_int (Int32.logand (Int32.logxor !crc (Int32.of_int byte)) 0xFFl) in
    crc := Int32.logxor crc32_table.(idx) (Int32.shift_right_logical !crc 8)
  done;
  Int32.logxor !crc 0xFFFFFFFFl
;;

let update_crc crc char =
  let byte = Char.code char in
  let idx = Int32.to_int (Int32.logand (Int32.logxor crc (Int32.of_int byte)) 0xFFl) in
  Int32.logxor crc32_table.(idx) (Int32.shift_right_logical crc 8)
;;

let add_normalized_to_crc crc s =
  let len = String.length s in
  let rec find_first i =
    if i >= len
    then crc
    else (
      let c = s.[i] in
      if c = '0' || c = '.'
      then find_first (i + 1)
      else (
        let rec feed c_state j =
          if j >= len
          then c_state
          else (
            let c2 = s.[j] in
            if c2 = '.' then feed c_state (j + 1) else feed (update_crc c_state c2) (j + 1))
        in
        feed crc i))
  in
  find_first 0
;;

type level =
  { price : string (** Canonical fixed-decimals rendering; used as the map key. *)
  ; price_wire : string
    (** Price exactly as received on the wire. Kraken's CRC32 checksum is
        computed over wire strings (dots removed, leading zeros stripped,
        trailing zeros preserved), NOT over a re-formatted value - padding or
        rounding here changes the CRC and invalidates every check. *)
  ; size : string
  ; price_float : float
  ; size_float : float
  }

type orderbook =
  { symbol : string
  ; bids : level array
  ; asks : level array
  ; sequence : int64 option
  ; checksum : int32 option (** CRC32 checksum received from Kraken for validation. *)
  ; timestamp : float
  }

(** Returns the first [n] elements of a list. Returns all elements if the list has fewer than [n]. *)
let take n lst =
  let rec take_aux acc n = function
    | [] -> List.rev acc
    | h :: t when n > 0 -> take_aux (h :: acc) (n - 1) t
    | _ -> List.rev acc
  in
  take_aux [] n lst
;;

let to_decimal_str ?(trim_trailing = true) ?dec json =
  match json with
  | `String s -> s
  | `Float f ->
    let d =
      match dec with
      | Some d -> d
      | None -> 12
    in
    let s = Printf.sprintf "%.*f" d f in
    if (not (String.contains s '.')) || not trim_trailing
    then s
    else (
      let parts = String.split_on_char '.' s in
      match parts with
      | [ whole; frac ] ->
        if trim_trailing
        then (
          (* Trim trailing zeros from fractional part. *)
          let len = String.length frac in
          let rec rtrim i =
            if i <= 0
            then ""
            else if frac.[i - 1] = '0'
            then rtrim (i - 1)
            else String.sub frac 0 i
          in
          let frac_clean = rtrim len in
          if String.length frac_clean = 0 then whole else whole ^ "." ^ frac_clean)
        else whole ^ "." ^ frac
      | _ -> s)
  | `Int i -> string_of_int i
  | `Intlit s -> s
  | _ -> "0"
;;

(** Parses price and quantity strings from JSON values for checksum input. *)
let parse_checksum_level price_json qty_json =
  let price_str = to_decimal_str ~trim_trailing:true price_json in
  let qty_str = to_decimal_str ~trim_trailing:true qty_json in
  price_str, qty_str
;;

(** Compute CRC32 checksum from raw JSON bid/ask arrays using the top 10 levels per side.
    Operates directly on JSON to preserve original string precision. *)
let calculate_checksum_from_json symbol bids_json asks_json : int32 =
  let parse_checksum_levels json =
    match json with
    | `List entries ->
      List.filter_map
        (fun entry ->
           match entry with
           | `Assoc fields ->
             (match List.assoc_opt "price" fields, List.assoc_opt "qty" fields with
              | Some price_json, Some qty_json ->
                let price_str, qty_str = parse_checksum_level price_json qty_json in
                Some (price_str, qty_str)
              | _ -> None)
           | `List [ price_json; qty_json ] ->
             Some (parse_checksum_level price_json qty_json)
           | `List [ price_json; qty_json; _ ] ->
             Some (parse_checksum_level price_json qty_json)
           | _ -> None)
        entries
    | _ -> []
  in
  let bids_levels = parse_checksum_levels bids_json in
  let asks_levels = parse_checksum_levels asks_json in
  (* Check if quantity string represents zero by scanning for any non-zero, non-decimal digit. *)
  let is_effectively_zero qty_str =
    let rec has_non_zero s i =
      if i >= String.length s
      then false
      else if s.[i] <> '0' && s.[i] <> '.'
      then true
      else has_non_zero s (i + 1)
    in
    not (has_non_zero qty_str 0)
  in
  (* Exclude levels with zero quantity. *)
  let valid_bids =
    List.filter (fun (_, qty_str) -> not (is_effectively_zero qty_str)) bids_levels
  in
  let valid_asks =
    List.filter (fun (_, qty_str) -> not (is_effectively_zero qty_str)) asks_levels
  in
  (* Sort bids descending by price, asks ascending by price. *)
  let sorted_bids =
    List.sort
      (fun (p1, _) (p2, _) -> Float.compare (float_of_string p2) (float_of_string p1))
      valid_bids
  in
  let sorted_asks =
    List.sort
      (fun (p1, _) (p2, _) -> Float.compare (float_of_string p1) (float_of_string p2))
      valid_asks
  in
  (* Select up to 10 levels per side. No padding per Kraken specification. *)
  let top_bids = take (min 10 (List.length sorted_bids)) sorted_bids in
  let top_asks = take (min 10 (List.length sorted_asks)) sorted_asks in
  Logging.debug_f
    ~section
    "Checksum input: symbol=%s bids=%d asks=%d | levels used: bids=%d asks=%d"
    symbol
    (List.length bids_levels)
    (List.length asks_levels)
    (List.length top_bids)
    (List.length top_asks);
  let crc = ref 0xFFFFFFFFl in
  List.iter
    (fun (price_str, qty_str) ->
       crc := add_normalized_to_crc !crc price_str;
       crc := add_normalized_to_crc !crc qty_str)
    top_asks;
  List.iter
    (fun (price_str, qty_str) ->
       crc := add_normalized_to_crc !crc price_str;
       crc := add_normalized_to_crc !crc qty_str)
    top_bids;
  let result = Int32.logxor !crc 0xFFFFFFFFl in
  Logging.debug_f ~section "Checksum CRC32: result=%ld (0x%08lx)" result result;
  result
;;

(** Lock-free ring buffer for orderbook snapshots. Aliases the shared implementation. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

module PriceMap = Map.Make (struct
    type t = string

    let compare = String.compare
  end)

(** Per-symbol mutable orderbook state, including bid/ask maps, ring buffer, and synchronization metadata. *)
type store =
  { buffer : orderbook RingBuffer.t
  ; bids : (string, level) Hashtbl.t
  ; asks : (string, level) Hashtbl.t
  ; ready : bool Atomic.t
  ; has_snapshot : bool Atomic.t
    (** True after an initial snapshot has been received. Updates are rejected until set. *)
  ; last_sequence : int64 option Atomic.t
    (** Last processed sequence number. Used for gap and rollback detection. *)
  ; last_update_ns : int64 Atomic.t
    (** Mtime_clock monotonic nanoseconds of the most recent data write.
        Atomic because written by the parse domain and read by trading domains;
        monotonic so staleness is immune to wall-clock (NTP) steps. *)
  ; mutable checksum_tick : int
    (** Increments per book update; the per-tick checksum recompute (2 extra
        fold+sort+array passes) runs only every [checksum_every_n] ticks;
        see M2. *)
  }

(** (pair_decimals, lot_decimals) precision tuple from AssetPairs API. *)
type decimals = int * int

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let decimals_tbl : (string, decimals) Hashtbl.t = Hashtbl.create 16

(** Persistent registry of all subscribed symbols (configured + dynamic).
    Preserved across disconnects and resets so that dynamic subscriptions
    re-subscribe automatically on connection restart. *)
let all_subscribed_symbols : string list ref = ref []
let subscribed_mutex = Mutex.create ()

let get_all_subscribed_symbols () =
  Mutex.lock subscribed_mutex;
  let syms = !all_subscribed_symbols in
  Mutex.unlock subscribed_mutex;
  syms
;;

let add_subscribed_symbols symbols =
  Mutex.lock subscribed_mutex;
  let new_syms =
    List.filter (fun s -> not (List.mem s !all_subscribed_symbols)) symbols
  in
  all_subscribed_symbols := !all_subscribed_symbols @ new_syms;
  let total = !all_subscribed_symbols in
  Mutex.unlock subscribed_mutex;
  total
;;

(* P5: frame parsing/dispatch runs on the Parse_worker domain, so nothing in
   the dispatch path may touch Lwt primitives. The old [Lwt_condition]
   ready signal became an Atomic flag polled by the startup waiter, and the
   sequence-gap resubscribe trigger became a pending-queue drained by a
   small Lwt watcher on the main domain. *)
let resubscribe_symbol_ref : (string -> unit Lwt.t) option ref = ref None

(** Symbols awaiting a resubscribe after a sequence gap/rollback. Appended
    from the parse domain via CAS; drained by the Lwt watcher. *)
let pending_resubscribes : string list Atomic.t = Atomic.make []

let resubscribe_watcher_started = Atomic.make false

(* Checksum-triggered resubscribe cooldown state. Written only from the parse
   domain (single-threaded message processing), so no lock is required. *)
let resubscribe_cooldown : (string, float) Hashtbl.t = Hashtbl.create 16
let resubscribe_cooldown_s = Kraken_common_types.default_resubscribe_cooldown_s

let[@inline] request_resubscribe symbol =
  let rec loop () =
    let current = Atomic.get pending_resubscribes in
    if List.mem symbol current
    then ()
    else if Atomic.compare_and_set pending_resubscribes current (symbol :: current)
    then ()
    else loop ()
  in
  loop ()
;;

(** M2: recompute the book checksum at most once per this many updates per
    symbol. The checksum rebuild does 2 extra fold+sort+array passes per tick;
    every 10th update still validates constantly-changing books far more often
    than the exchange's drift window needs. *)
let checksum_every_n = 10

(** Retrieves price and quantity precision from the instruments feed cache. Returns None on failure. *)
let get_precision_from_instruments symbol =
  try Kraken_instruments_feed.get_precision_info symbol with
  | _ -> None
;;

let canonicalize_kraken_name s =
  let uppercase = String.uppercase_ascii s in
  let no_xbt =
    if String.length uppercase >= 4 && String.sub uppercase 0 4 = "XXBT"
    then "BTC" ^ String.sub uppercase 4 (String.length uppercase - 4)
    else if String.length uppercase >= 3 && String.sub uppercase 0 3 = "XBT"
    then "BTC" ^ String.sub uppercase 3 (String.length uppercase - 3)
    else if String.length uppercase >= 5 && String.sub uppercase 0 5 = "XXETH"
    then "ETH" ^ String.sub uppercase 5 (String.length uppercase - 5)
    else if String.length uppercase >= 4 && String.sub uppercase 0 4 = "XETH"
    then "ETH" ^ String.sub uppercase 4 (String.length uppercase - 4)
    else uppercase
  in
  if String.length no_xbt >= 4 && String.sub no_xbt (String.length no_xbt - 4) 4 = "ZUSD"
  then String.sub no_xbt 0 (String.length no_xbt - 4) ^ "USD"
  else if
    String.length no_xbt >= 4 && String.sub no_xbt (String.length no_xbt - 4) 4 = "ZEUR"
  then String.sub no_xbt 0 (String.length no_xbt - 4) ^ "EUR"
  else no_xbt
;;

let calculate_checksum symbol bids asks : int32 =
  let pd, _ld =
    match get_precision_from_instruments symbol with
    | Some (p, q) -> p, q
    | None ->
      (try Hashtbl.find decimals_tbl symbol with
       | Not_found -> 8, 8)
  in
  let crc = ref 0xFFFFFFFFl in
  (* Process top 10 asks (ascending price) *)
  let n_asks = min 10 (Array.length asks) in
  for i = 0 to n_asks - 1 do
    let lvl = asks.(i) in
    let s_p = to_decimal_str ~trim_trailing:false ~dec:pd (`String lvl.price_wire) in
    let s_q = to_decimal_str ~trim_trailing:false (`String lvl.size) in
    crc := add_normalized_to_crc !crc s_p;
    crc := add_normalized_to_crc !crc s_q
  done;
  (* Process top 10 bids (descending price) *)
  let n_bids = min 10 (Array.length bids) in
  for i = 0 to n_bids - 1 do
    let lvl = bids.(i) in
    let s_p = to_decimal_str ~trim_trailing:false ~dec:pd (`String lvl.price_wire) in
    let s_q = to_decimal_str ~trim_trailing:false (`String lvl.size) in
    crc := add_normalized_to_crc !crc s_p;
    crc := add_normalized_to_crc !crc s_q
  done;
  let result = Int32.logxor !crc 0xFFFFFFFFl in
  Logging.debug_f
    ~section
    "Checksum CRC32 for %s: result=%ld (0x%08lx)"
    symbol
    result
    result;
  result
;;

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
    let store =
      { buffer = RingBuffer.create ring_buffer_size
      ; bids = Hashtbl.create 1024
      ; asks = Hashtbl.create 1024
      ; ready = Atomic.make false
      ; has_snapshot = Atomic.make false
      ; last_sequence = Atomic.make None
      ; last_update_ns = Atomic.make (Mtime_clock.now_ns ())
      ; checksum_tick = 0
      }
    in
    Hashtbl.add stores symbol store;
    store
;;

let store_opt symbol = Hashtbl.find_opt stores symbol

let notify_ready ~symbol store =
  if not (Atomic.get store.ready)
  then
    (* P5: Atomic flag only - the startup waiter polls it, so this is safe
       from the Parse_worker domain (the old Lwt_condition.broadcast was
       not). *)
    Atomic.set store.ready true;
  Concurrency.Exchange_wakeup.signal ~symbol
;;

let is_effectively_zero size =
  (* Returns true if the string contains no non-zero, non-decimal, non-sign digits. *)
  let rec has_non_zero s i =
    if i >= String.length s
    then false
    else if s.[i] <> '0' && s.[i] <> '.' && s.[i] <> '-'
    then true
    else has_non_zero s (i + 1)
  in
  not (has_non_zero size 0)
;;

let int64_of_json json =
  match json with
  | `Int i -> Some (Int64.of_int i)
  | `Intlit s ->
    (try Some (Int64.of_string s) with
     | _ -> None)
  | `Float f ->
    (try Some (Int64.of_float f) with
     | _ -> None)
  | `String s ->
    (try Some (Int64.of_string s) with
     | _ -> None)
  | _ -> None
;;

let int32_of_json json =
  match json with
  | `Int i -> Some (Int32.of_int i)
  | `Intlit s ->
    (try Some (Int32.of_string s) with
     | _ -> None)
  | `Float f ->
    (try Some (Int32.of_float f) with
     | _ -> None)
  | `String s ->
    (try Some (Int32.of_string s) with
     | _ -> None)
  | _ -> None
;;

let parse_level symbol price_json size_json =
  let pd, ld =
    match get_precision_from_instruments symbol with
    | Some (price_prec, qty_prec) -> price_prec, qty_prec
    | None ->
      (try Hashtbl.find decimals_tbl symbol with
       | Not_found -> 8, 8)
  in
  (* NO trailing-zero trimming: the checksum input must be the exchange's
     fixed-decimal representation. The live v2 feed sends NUMBERS (not the
     documented strings), so numeric fields are re-rendered at full pair/lot
     precision - e.g. qty 5.1e-05 at lot_decimals=8 becomes "0.00005100",
     whose normalization ("5100") matches Kraken's server-side CRC input.
     Trimming would yield "51" and invalidate every checksum. String inputs
     pass through verbatim either way. *)
  let price_str_raw = to_decimal_str ~trim_trailing:false ~dec:pd price_json in
  let qty_str = to_decimal_str ~trim_trailing:false ~dec:ld size_json in
  let price_float =
    try float_of_string price_str_raw with
    | _ -> 0.0
  in
  let qty_float =
    try float_of_string qty_str with
    | _ -> 0.0
  in
  let price_str = Printf.sprintf "%.*f" pd price_float in
  Some
    { price = price_str
    ; price_wire = price_str_raw
    ; size = qty_str
    ; price_float
    ; size_float = qty_float
    }
;;

(** Parses JSON levels and applies them directly to the store's Hashtbl to avoid intermediate list allocations. *)
let parse_and_apply_levels symbol tbl json =
  match json with
  | `List entries ->
    List.iter
      (fun entry ->
         match entry with
         (* Object format: {"price": ..., "qty": ...} *)
         | `Assoc fields ->
           (match List.assoc_opt "price" fields, List.assoc_opt "qty" fields with
            | Some price_json, Some qty_json ->
              (match parse_level symbol price_json qty_json with
               | Some lvl ->
                 if is_effectively_zero lvl.size
                 then Hashtbl.remove tbl lvl.price
                 else Hashtbl.replace tbl lvl.price lvl
               | None -> ())
            | _ -> ())
         (* Array format: [price, qty] or [price, qty, timestamp] *)
         | `List [ price_json; size_json ] | `List [ price_json; size_json; _ ] ->
           (match parse_level symbol price_json size_json with
            | Some lvl ->
              if is_effectively_zero lvl.size
              then Hashtbl.remove tbl lvl.price
              else Hashtbl.replace tbl lvl.price lvl
            | None -> ())
         | _ -> ())
      entries
  | _ -> ()
;;

(** Converts a Hashtbl to a sorted level array truncated to [depth] entries.
    [sort_desc] controls descending (bids) vs ascending (asks) order. *)
let levels_to_array ?(sort_desc = false) tbl depth =
  let levels_list = Hashtbl.fold (fun _ lvl acc -> lvl :: acc) tbl [] in
  let sorted_levels =
    List.sort
      (fun l1 l2 ->
         if sort_desc
         then Float.compare l2.price_float l1.price_float
         else Float.compare l1.price_float l2.price_float)
      levels_list
  in
  let rec take n lst acc =
    if n = 0
    then List.rev acc
    else (
      match lst with
      | [] -> List.rev acc
      | level :: rest -> take (n - 1) rest (level :: acc))
  in
  Array.of_list (take depth sorted_levels [])
;;

(** Rebuilds a Hashtbl containing only the top [max_levels] entries. Used to bound map size. *)
let truncate_hashtbl tbl sort_desc max_levels =
  let levels_array = levels_to_array ~sort_desc tbl max_levels in
  Hashtbl.clear tbl;
  Array.iter (fun lvl -> Hashtbl.replace tbl lvl.price lvl) levels_array
;;

(** Constructs an [orderbook] record from the current store state and the raw JSON entry metadata. *)
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
    if orderbook_depth >= 10
    then (
      let checksum_json = member "checksum" entry in
      match checksum_json with
      | `Int i -> Some (Int32.of_int i)
      | `Intlit s ->
        (try Some (Int32.of_string s) with
         | _ -> None)
      | `Float f ->
        (try Some (Int32.of_float f) with
         | _ -> None)
      | `String s ->
        (try Some (Int32.of_string s) with
         | _ -> None)
      | _ -> None)
    else None
  in
  (* Constructs arrays directly from the Hashtbl by sorting. *)
  let bids = levels_to_array ~sort_desc:true store.bids orderbook_depth in
  let asks = levels_to_array ~sort_desc:false store.asks orderbook_depth in
  { symbol; bids; asks; sequence; checksum; timestamp = Unix.time () }
;;

(** Fetch price and lot decimal precision for the given symbols.
    Skips the AssetPairs REST call if all symbols already have precision data from the instruments feed. *)
let fetch_decimals symbols =
  let all_have_instruments_data =
    List.for_all
      (fun symbol ->
         match Kraken_instruments_feed.get_precision_info symbol with
         | Some _ -> true
         | None -> false)
      symbols
  in
  if all_have_instruments_data
  then (
    Logging.debug_f
      ~section
      "All symbols have instruments precision data, skipping AssetPairs API call";
    Lwt.return ())
  else (
    Logging.debug_f
      ~section
      "Some symbols missing instruments precision data, fetching from AssetPairs API";
    let uri = Uri.of_string "https://api.kraken.com/0/public/AssetPairs" in
    Cohttp_lwt_unix.Client.get uri
    >>= fun (_resp, body) ->
    Cohttp_lwt.Body.to_string body
    >>= fun body_str ->
    try
      let json = Yojson.Safe.from_string body_str in
      let open Yojson.Safe.Util in
      match member "error" json with
      | `List [] ->
        let result = member "result" json in
        let pairs = to_assoc result in
        List.iter
          (fun (_pair_name, pair_json) ->
             let altname = to_string_option (member "altname" pair_json) in
             let wsname = to_string_option (member "wsname" pair_json) in
             let pd = to_int (member "pair_decimals" pair_json) in
             let ld = to_int (member "lot_decimals" pair_json) in
             List.iter
               (fun sym ->
                  let norm = sym in
                  let no_slash = String.concat "" (String.split_on_char '/' sym) in
                  let is_ws_match =
                    match wsname with
                    | Some n -> canonicalize_kraken_name n = norm || n = norm
                    | None -> false
                  in
                  let is_alt_match =
                    match altname with
                    | Some n -> canonicalize_kraken_name n = no_slash || n = no_slash
                    | None -> false
                  in
                  if is_ws_match || is_alt_match then Hashtbl.add decimals_tbl sym (pd, ld))
               symbols)
          pairs;
        Lwt.return ()
      | _ -> Lwt.fail_with "Error fetching asset pairs"
    with
    | exn ->
      Logging.warn_f ~section "Failed to fetch decimals: %s" (Printexc.to_string exn);
      Lwt.return ())
;;

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
    List.iter
      (fun entry ->
         try
           let symbol = member "symbol" entry |> to_string in
           let store = ensure_store symbol in
           if reset
           then (
             (* Snapshot: clear existing state and reinitialize from this message. *)
             Hashtbl.clear store.bids;
             Hashtbl.clear store.asks;
             Atomic.set store.has_snapshot true;
             let sequence =
               match int64_of_json (member "sequence" entry) with
               | Some seq -> Some seq
               | None -> None
             in
             Atomic.set store.last_sequence sequence;
             Logging.debug_f
               ~section
               "Received snapshot for %s (sequence=%s), ready for updates"
               symbol
               (match sequence with
                | Some s -> Int64.to_string s
                | None -> "none"))
           else (
             (* Incremental update: discard if no snapshot has been received yet. *)
             if not (Atomic.get store.has_snapshot)
             then (
               Logging.debug_f
                 ~section
                 "Ignoring update for %s: waiting for snapshot after reconnect"
                 symbol;
               raise Exit (* Skip processing this entry *));
             (* Validate monotonic sequence ordering. Rollbacks and gaps trigger full resync. *)
             let current_sequence =
               match int64_of_json (member "sequence" entry) with
               | Some seq -> Some seq
               | None -> None
             in
             let last_seq_opt = Atomic.get store.last_sequence in
             match current_sequence, last_seq_opt with
             | Some curr_seq, Some last_seq when Int64.compare curr_seq last_seq <= 0 ->
               Logging.info_f
                 ~section
                 "Sequence rollback for %s: current=%Ld last=%Ld, marking out-of-sync"
                 symbol
                 curr_seq
                 last_seq;
               Hashtbl.clear store.bids;
               Hashtbl.clear store.asks;
               RingBuffer.clear store.buffer;
               Atomic.set store.has_snapshot false;
               Atomic.set store.last_sequence None;
               (* P5: domain-safe trigger - the Lwt watcher drains this. *)
               request_resubscribe symbol;
               raise Exit (* Skip processing this entry *)
             | Some curr_seq, Some last_seq
               when Int64.compare curr_seq (Int64.add last_seq 1L) > 0 ->
               let gap = Int64.sub curr_seq last_seq in
               Logging.info_f
                 ~section
                 "Sequence gap for %s: current=%Ld last=%Ld (gap=%Ld), marking \
                  out-of-sync"
                 symbol
                 curr_seq
                 last_seq
                 gap;
               Hashtbl.clear store.bids;
               Hashtbl.clear store.asks;
               RingBuffer.clear store.buffer;
               Atomic.set store.has_snapshot false;
               Atomic.set store.last_sequence None;
               (* P5: domain-safe trigger - the Lwt watcher drains this. *)
               request_resubscribe symbol;
               raise Exit (* Skip processing this entry *)
             | _ -> ());
           let bids_json = member "bids" entry in
           let asks_json = member "asks" entry in
           parse_and_apply_levels symbol store.bids bids_json;
           parse_and_apply_levels symbol store.asks asks_json;
           Atomic.set store.last_update_ns (Mtime_clock.now_ns ());
           (* Kraken v2 book contract: levels falling out of the subscribed
               scope NEVER receive a qty:0 removal. Retaining anything beyond
               [orderbook_depth] therefore accumulates stale ghosts that slide
               back into the computed top-10 during removal cascades and
               permanently desync checksum validation - verified live: 2x-depth
               retention drifted BTC/ADA within ~20 updates while strict
               depth truncation ran 14k+ validations with zero mismatches.
               Truncate to the subscribed depth after every message. *)
           if Hashtbl.length store.bids > orderbook_depth
           then truncate_hashtbl store.bids true orderbook_depth;
           if Hashtbl.length store.asks > orderbook_depth
           then truncate_hashtbl store.asks false orderbook_depth;
           let orderbook = build_orderbook store symbol entry in
           (* Compute and verify CRC32 from current state using top 10 levels per side.
            If the configured depth is < 10, checksum validation is bypassed because
            the stored map lacks the requisite levels to evaluate the CRC.
            M2: the checksum recompute (2 extra fold+sort+array passes) is throttled
            to every [checksum_every_n] updates per symbol; the book is still built
            and written per tick, only the redundant CRC pass is slowed down. *)
           store.checksum_tick <- store.checksum_tick + 1;
           let checksum_valid =
             if orderbook_depth >= 10 && store.checksum_tick mod checksum_every_n = 0
             then (
               let calculated_checksum =
                 calculate_checksum
                   symbol
                   (levels_to_array ~sort_desc:true store.bids 10)
                   (levels_to_array ~sort_desc:false store.asks 10)
               in
               match orderbook.checksum with
               | Some received_checksum ->
                 if Int32.compare calculated_checksum received_checksum <> 0
                 then (
                   (* Cooldown: a persistently failing validator must degrade
                       to periodic heals, not hot-loop unsub/resub (observed as
                       a storm when the checksum math itself was wrong). Only
                       touched from the parse domain - no locking needed. *)
                   let now = Unix.gettimeofday () in
                   let last =
                     match Hashtbl.find_opt resubscribe_cooldown symbol with
                     | Some t -> t
                     | None -> 0.0
                   in
                   Logging.warn_f
                     ~section
                     "Checksum mismatch for %s: received=%ld (0x%08lx) calculated=%ld \
                      (0x%08lx) - book desynced, requesting resubscribe"
                     symbol
                     received_checksum
                     received_checksum
                     calculated_checksum
                     calculated_checksum;
                   Hashtbl.clear store.bids;
                   Hashtbl.clear store.asks;
                   RingBuffer.clear store.buffer;
                   Atomic.set store.has_snapshot false;
                   Atomic.set store.last_sequence None;
                   if now -. last >= resubscribe_cooldown_s
                   then (
                     Hashtbl.replace resubscribe_cooldown symbol now;
                     request_resubscribe symbol)
                   else
                     Logging.debug_f
                       ~section
                       "Resubscribe for %s suppressed by cooldown (%.1fs)"
                       symbol
                       (resubscribe_cooldown_s -. (now -. last));
                   raise Exit)
                 else true
               | None -> true)
             else true
           in
           if checksum_valid
           then (
             RingBuffer.write store.buffer orderbook;
             Hashtbl.replace notified_symbols symbol store;
             let current_sequence =
               match int64_of_json (member "sequence" entry) with
               | Some seq -> Some seq
               | None -> None
             in
             Atomic.set store.last_sequence current_sequence);
           on_heartbeat ()
         with
         | Exit ->
           () (* Control flow: entry skipped due to missing snapshot or sequence error. *)
         | exn ->
           Logging.warn_f
             ~section
             "Failed to process orderbook entry: %s"
             (Printexc.to_string exn))
      data;
    (* Broadcast readiness and signal exchange wakeup for each symbol that received a valid write. *)
    Hashtbl.iter (fun symbol store -> notify_ready ~symbol store) notified_symbols;
    Some ()
  with
  | exn ->
    Logging.warn_f
      ~section
      "Failed to parse orderbook message: %s"
      (Printexc.to_string exn);
    None
;;

let[@inline always] get_latest_orderbook symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None
;;

(** Max age of a ring-buffer frame before it is considered stale and rejected.
    Without this guard a desynced/stalled feed serves its last frame forever,
    making both the dashboard and the sizing loop act on a frozen price. *)
let max_book_age = Kraken_common_types.default_max_book_age_s

let max_book_age_ns = Int64.of_float (max_book_age *. 1e9)

(* Monotonic clock: immune to wall-clock (NTP) steps. *)
let[@inline always] book_age_ns store =
  Int64.sub (Mtime_clock.now_ns ()) (Atomic.get store.last_update_ns)
;;

let[@inline always] fresh_frame store =
  match RingBuffer.read_latest store.buffer with
  | Some ({ bids; asks; _ } as ob)
    when Array.length bids > 0
         && Array.length asks > 0
         && Int64.compare (book_age_ns store) max_book_age_ns <= 0 -> Some ob
  | _ -> None
;;

let[@inline always] get_best_bid_ask symbol =
  match store_opt symbol with
  | Some store ->
    (match fresh_frame store with
     | Some { bids; asks; _ } ->
       let bid = bids.(0) in
       let ask = asks.(0) in
       Some (bid.price_float, bid.size_float, ask.price_float, ask.size_float)
     | None -> None)
  | None -> None
;;

let[@inline always] get_best_bid_ask_fast symbol =
  let store = ensure_store symbol in
  fun () ->
    match fresh_frame store with
    | Some { bids; asks; _ } ->
      let bid = bids.(0) in
      let ask = asks.(0) in
      Some (bid.price_float, bid.size_float, ask.price_float, ask.size_float)
    | None -> None
;;

(** Read all orderbook snapshots written since [last_pos]. Returns an empty list if the symbol is unknown. *)
let[@inline always] read_orderbook_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []
;;

(** Iterate over orderbook snapshots since [last_pos] without allocating an intermediate list.
    Returns the new read position. *)
let[@inline always] iter_orderbook_events symbol last_pos f =
  match store_opt symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos
;;

(** Returns the current ring buffer write position for the given symbol. Returns 0 if unknown. *)
let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0
;;

let[@inline always] get_current_position_fast symbol =
  let store = ensure_store symbol in
  fun () -> RingBuffer.get_position store.buffer
;;

let get_top_levels ?(depth = orderbook_depth) symbol =
  match get_latest_orderbook symbol with
  | None -> [||], [||]
  | Some ob ->
    let trim arr =
      let count = min depth (Array.length arr) in
      Array.init count (fun idx -> arr.(idx))
    in
    trim ob.bids, trim ob.asks
;;

let has_orderbook_data symbol =
  match store_opt symbol with
  | Some store when Atomic.get store.ready ->
    Option.is_some (RingBuffer.read_latest store.buffer)
  | _ -> false
;;

(** Resets all per-symbol stores: clears bid/ask maps, replaces ring buffers, and unsets readiness flags.
    Called on reconnection to ensure no stale data persists. *)
let clear_all_stores () =
  Hashtbl.iter
    (fun symbol store ->
       Logging.debug_f ~section "Clearing orderbook store for %s" symbol;
       Hashtbl.clear store.bids;
       Hashtbl.clear store.asks;
       RingBuffer.clear store.buffer;
       Atomic.set store.ready false;
       Atomic.set store.has_snapshot false;
       Atomic.set store.last_sequence None;
       Atomic.set store.last_update_ns (Mtime_clock.now_ns ()))
    stores
;;

(** Removes stores inactive for over 30 minutes and trims oversized price maps to [max_price_levels].
    Prevents unbounded memory growth from abandoned subscriptions or accumulated levels. *)
let prune_stale_data () =
  let now_ns = Mtime_clock.now_ns () in
  let stale_threshold_ns = Int64.of_float (30.0 *. 60.0 *. 1e9) in
  let max_price_levels = 100 in
  let stores_to_remove = ref [] in
  let trimmed_stores = ref [] in
  let total_stores_before = Hashtbl.length stores in
  Hashtbl.iter
    (fun symbol store ->
       let age = Int64.sub now_ns (Atomic.get store.last_update_ns) in
       if Int64.compare age stale_threshold_ns > 0
       then stores_to_remove := symbol :: !stores_to_remove
       else (
         let bids_count = Hashtbl.length store.bids in
         let asks_count = Hashtbl.length store.asks in
         let trimmed = ref false in
         if bids_count > max_price_levels
         then (
           truncate_hashtbl store.bids true max_price_levels;
           trimmed := true);
         if asks_count > max_price_levels
         then (
           truncate_hashtbl store.asks false max_price_levels;
           trimmed := true);
         if !trimmed then trimmed_stores := symbol :: !trimmed_stores))
    stores;
  List.iter
    (fun symbol ->
       Hashtbl.remove stores symbol;
       Logging.debug_f
         ~section
         "Removed stale orderbook store for %s (age > 30min)"
         symbol)
    !stores_to_remove;
  if !trimmed_stores <> []
  then
    Logging.debug_f
      ~section
      "Trimmed price levels for %d active stores: %s"
      (List.length !trimmed_stores)
      (String.concat ", " !trimmed_stores);
  let stores_removed = List.length !stores_to_remove in
  let stores_trimmed = List.length !trimmed_stores in
  let total_stores_after = Hashtbl.length stores in
  if stores_removed > 0 || stores_trimmed > 0
  then
    Logging.info_f
      ~section
      "Orderbook cleanup: removed %d stale stores, trimmed %d active stores (%d -> %d \
       total stores)"
      stores_removed
      stores_trimmed
      total_stores_before
      total_stores_after
;;

(** Asynchronously triggers orderbook pruning. [reason] is logged for diagnostics. *)
let trigger_orderbook_cleanup ~reason () =
  Lwt.async (fun () ->
    Logging.debug_f ~section "Triggering orderbook cleanup (reason=%s)" reason;
    prune_stale_data ();
    Lwt.return_unit)
;;

(** Timestamp of the last orderbook frame, for the ws_feed inter-message gap
    measurement (recorded only on book data, so heartbeats don't mask a
    stalled book feed). *)
let last_book_time = ref 0.0

(* P5: the per-connection heartbeat closure, published so the Parse_worker
   handler can invoke it from the parse domain (it is domain-safe: a mutex
   and a timestamp update). One orderbook connection exists at a time. *)
let current_on_heartbeat : (unit -> unit) option Atomic.t = Atomic.make None

(** P5: synchronous dispatch of an already-parsed frame. DOMAIN-SAFE: no
    Lwt primitives here - this runs on the Parse_worker domain. Sequence-gap
    resubscribes go through the pending queue; readiness through Atomics;
    logging/profiling/wakeups are all domain-safe. *)
let handle_dispatch json on_heartbeat =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  let msg_type = member "type" json |> to_string_option in
  let method_type = member "method" json |> to_string_option in
  match channel, msg_type, method_type with
  | Some "heartbeat", _, _ -> on_heartbeat ()
  | _, _, Some "heartbeat" -> on_heartbeat ()
  | Some "book", Some "snapshot", _ ->
    let now = Unix.gettimeofday () in
    if !last_book_time > 0.0
    then Network_latency.record_feed_s "kraken" (now -. !last_book_time);
    last_book_time := now;
    ignore (process_orderbook_message ~reset:true json on_heartbeat)
  | Some "book", Some "update", _ ->
    let now = Unix.gettimeofday () in
    if !last_book_time > 0.0
    then Network_latency.record_feed_s "kraken" (now -. !last_book_time);
    last_book_time := now;
    ignore (process_orderbook_message ~reset:false json on_heartbeat)
  | _, _, Some "subscribe" ->
    let success = member "success" json |> to_bool_option |> Option.value ~default:true in
    if not success
    then (
      let err_msg =
        member "error" json |> to_string_option |> Option.value ~default:"Unknown error"
      in
      Logging.error_f ~section "Kraken orderbook subscription failed: %s" err_msg)
    else (
      let result = member "result" json in
      let symbol =
        member "symbol" result |> to_string_option |> Option.value ~default:"unknown"
      in
      Logging.debug_f ~section "Subscribed to %s orderbook feed" symbol)
  | Some "status", _, _ -> Logging.debug_f ~section "Status message received"
  | _ ->
    Logging.info_f ~section "Unhandled orderbook payload: %s" (Yojson.Safe.to_string json)
;;

(** Parse-worker entry point: parse + dispatch on the parse domain. *)
let () =
  Concurrency.Parse_worker.register "kraken_ob" (fun message ->
    try
      let json = Yojson.Safe.from_string message in
      let on_heartbeat =
        match Atomic.get current_on_heartbeat with
        | Some f -> f
        | None -> fun () -> ()
      in
      handle_dispatch json on_heartbeat
    with
    | exn ->
      Logging.error_f
        ~section
        "Error parsing message: %s - %s"
        (Printexc.to_string exn)
        message)
;;

(** Synchronous path: parse + dispatch inline on the calling thread.
    Used as the overload fallback and by tests. *)
let handle_message message on_heartbeat =
  Concurrency.Tick_event_bus.publish_tick ();
  try
    let json = Yojson.Safe.from_string message in
    on_heartbeat ();
    handle_dispatch json on_heartbeat
  with
  | exn ->
    Logging.error_f
      ~section
      "Error parsing message: %s - %s"
      (Printexc.to_string exn)
      message
;;

(** P5: asynchronous path used by the WS read loop. Tick accounting and the
    heartbeat stay on the Lwt fiber; the JSON parse and dispatch move to the
    Parse_worker domain. Falls back to the synchronous path when the worker
    queue is full - Kraken book updates are deltas, so frames must never be
    dropped (a lost delta desyncs the book until the next snapshot). *)
let handle_message_async message on_heartbeat =
  Concurrency.Tick_event_bus.publish_tick ();
  Atomic.set current_on_heartbeat (Some on_heartbeat);
  if not (Concurrency.Parse_worker.submit "kraken_ob" message)
  then handle_message message on_heartbeat
;;

let wait_for_orderbook_data_lwt symbols timeout_seconds =
  let start_time = Unix.gettimeofday () in
  let rec wait_loop () =
    if List.for_all has_orderbook_data symbols
    then Lwt.return_true
    else (
      let elapsed = Unix.gettimeofday () -. start_time in
      if elapsed >= timeout_seconds
      then
        Lwt.return_false
        (* P5: poll the per-store ready flags instead of blocking on a
           condition variable - readiness is now published from the
           Parse_worker domain, which must not touch Lwt primitives. The
           25ms poll only runs during startup gating (bounded by the
           timeout), never on a hot path. *)
      else Lwt_unix.sleep 0.025 >>= fun () -> wait_loop ())
  in
  wait_loop ()
;;

let wait_for_orderbook_data = wait_for_orderbook_data_lwt

(** Subscribe to orderbook channels and enter the asynchronous read loop.
    Invokes [on_failure] on connection loss; resolves the returned promise when the loop terminates. *)
let start_message_handler conn symbols on_failure on_heartbeat =
  let subscribe_msg =
    `Assoc
      [ "method", `String "subscribe"
      ; ( "params"
        , `Assoc
            [ "channel", `String "book"
            ; "symbol", `List (List.map (fun s -> `String s) symbols)
            ; "depth", `Int (max 10 orderbook_depth)
            ] )
      ]
  in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ())
  >>= fun () ->
  let stream =
    Lwt_stream.from (fun () ->
      Lwt.catch
        (fun () -> Websocket_lwt_unix.read conn >>= fun frame -> Lwt.return_some frame)
        (function
          | End_of_file -> Lwt.return_none
          | exn -> Lwt.fail exn))
  in
  let process_frame = function
    | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
      Logging.warn ~section "Orderbook WebSocket closed by server";
      on_failure "Connection closed by server";
      Lwt.return_unit
    | frame ->
      (* P5: parse+dispatch run on the Parse_worker domain; this returns
         immediately (no awaiting), keeping the WS read loop tight. *)
      let () = handle_message_async frame.Websocket.Frame.content on_heartbeat in
      Lwt.return_unit
  in
  Lwt_mutex.with_lock state.mutex (fun () ->
    state.active_conn <- Some conn;
    Lwt.return_unit)
  >>= fun () ->
  let done_p =
    Lwt.catch
      (fun () -> Concurrency.Lwt_util.consume_stream_s process_frame stream)
      (fun exn ->
         match exn with
         | Failure msg when msg = "Connection closed by server" -> Lwt.return_unit
         | _ ->
           Logging.error_f
             ~section
             "Orderbook WebSocket error during read: %s"
             (Printexc.to_string exn);
           on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
           Lwt.return_unit)
  in
  let final_done_p =
    done_p
    >>= fun () ->
    Lwt_mutex.with_lock state.mutex (fun () ->
      state.active_conn <- None;
      Lwt.return_unit)
    >>= fun () ->
    Logging.warn
      ~section
      "Orderbook WebSocket connection closed unexpectedly (End_of_file)";
    on_failure "Connection closed unexpectedly (End_of_file)";
    Lwt.return_unit
  in
  final_done_p
;;

(* Per-symbol consecutive-failure counters for resubscribe backoff. Only
   touched from Lwt fibers on the main domain, so no mutex is required. *)
let resubscribe_attempts : (string, int) Hashtbl.t = Hashtbl.create 8

let resubscribe_backoff_delay_s attempt =
  let base = Kraken_common_types.default_resubscribe_backoff_base_s in
  let cap = Kraken_common_types.default_resubscribe_backoff_cap_s in
  let d = base *. (2. ** Float.of_int attempt) in
  (* +-25% jitter so concurrent symbol retries don't synchronize. *)
  let jitter = 0.75 +. Random.float 0.5 in
  Float.min (d *. jitter) cap
;;

let rec subscribe_symbols symbols =
  resubscribe_symbol_ref := Some (fun s -> resubscribe_symbol s);
  let _ = add_subscribed_symbols symbols in
  (* P5: drain sequence-gap resubscribe requests raised on the Parse_worker
     domain. The watcher runs on the Lwt main domain, where the Lwt-based
     [resubscribe_symbol] is safe. Started once; 50ms poll on a path that
     only fires when a feed degrades. *)
  if not (Atomic.get resubscribe_watcher_started)
  then (
    Atomic.set resubscribe_watcher_started true;
    let rec watcher () =
      let pending = Atomic.get pending_resubscribes in
      (match pending with
       | [] -> ()
       | _ ->
         if Atomic.compare_and_set pending_resubscribes pending []
         then
           List.iter
             (fun s ->
                (* [resubscribe_with_retry] owns failure handling: backoff,
                    jitter, bounded attempts. A dropped symbol here would stay
                    snapshot-less forever, discarding all deltas. *)
                Lwt.async (fun () -> resubscribe_with_retry s))
             pending
         else ());
      Lwt_unix.sleep 0.05 >>= fun () -> watcher ()
    in
    Lwt.async watcher);
  Kraken_instruments_feed.fetch_from_rest symbols
  >>= fun () ->
  fetch_decimals symbols
  >>= fun () ->
  List.iter
    (fun symbol ->
       let _ = ensure_store symbol in
       ())
    symbols;
  Lwt_mutex.with_lock state.mutex (fun () -> Lwt.return state.active_conn)
  >>= function
  | Some conn ->
    let subscribe_msg =
      `Assoc
        [ "method", `String "subscribe"
        ; ( "params"
          , `Assoc
              [ "channel", `String "book"
              ; "symbol", `List (List.map (fun s -> `String s) symbols)
              ; "depth", `Int (max 10 orderbook_depth)
              ] )
        ]
    in
    let msg_str = Yojson.Safe.to_string subscribe_msg in
    Logging.debug_f
      ~section
      "Sending dynamic orderbook subscription for %d symbols: %s"
      (List.length symbols)
      (String.concat ", " symbols);
    Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ())
  | None ->
    Logging.warn_f
      ~section
      "Registered %d symbols for dynamic subscription on next connect: %s"
      (List.length symbols)
      (String.concat ", " symbols);
    Lwt.return_unit

and resubscribe_symbol symbol =
  Lwt_mutex.with_lock state.mutex (fun () -> Lwt.return state.active_conn)
  >>= function
  | Some conn ->
    let unsub_msg =
      `Assoc
        [ "method", `String "unsubscribe"
        ; ( "params"
          , `Assoc
              [ "channel", `String "book"
              ; "symbol", `List [ `String symbol ]
              ; "depth", `Int (max 10 orderbook_depth)
              ] )
        ]
    in
    let msg_str = Yojson.Safe.to_string unsub_msg in
    Logging.info_f ~section "Unsubscribing %s before re-subscribing" symbol;
    Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ())
    >>= fun () -> Lwt_unix.sleep 0.1 >>= fun () -> subscribe_symbols [ symbol ]
  | None ->
    (* Gap fix: a missing connection is a failure, not a silent success.
       Raising routes into [resubscribe_with_retry]'s backoff loop; if the
       socket is truly dead the supervisor's reconnect path owns recovery
       (it clears all stores and resubscribes every symbol). *)
    failwith "orderbook WS not connected"

(** Drives one resubscribe to completion with exponential backoff + jitter,
    bounded by [default_max_resubscribe_attempts]. Success resets the counter;
    exhausting attempts logs an error and stops - recovery is then owned by
    the supervisor reconnect, which clears stores and resubscribes all
    symbols. *)
and resubscribe_with_retry symbol =
  let max_attempts = Kraken_common_types.default_max_resubscribe_attempts in
  let attempt =
    match Hashtbl.find_opt resubscribe_attempts symbol with
    | Some n -> n
    | None -> 0
  in
  if attempt >= max_attempts
  then (
    Hashtbl.remove resubscribe_attempts symbol;
    Logging.error_f
      ~section
      "Resubscribe for %s failed %d times - giving up until next sequence event or \
       reconnect"
      symbol
      attempt;
    Lwt.return_unit)
  else
    Lwt.catch
      (fun () ->
         resubscribe_symbol symbol
         >>= fun () ->
         Hashtbl.remove resubscribe_attempts symbol;
         Logging.info_f ~section "Resubscribe for %s completed" symbol;
         Lwt.return_unit)
      (fun exn ->
         let next = attempt + 1 in
         Hashtbl.replace resubscribe_attempts symbol next;
         let delay = resubscribe_backoff_delay_s attempt in
         Logging.warn_f
           ~section
           "Resubscribe for %s failed (%s) - attempt %d/%d, retrying in %.1fs"
           symbol
           (Printexc.to_string exn)
           next
           max_attempts
           delay;
         Lwt_unix.sleep delay >>= fun () -> resubscribe_with_retry symbol)
;;

let connect_and_subscribe symbols ~on_failure ~on_heartbeat ~on_connected =
  let all_syms = add_subscribed_symbols symbols in
  let uri = Uri.of_string "wss://ws.kraken.com/v2" in
  Logging.debug_f ~section "Connecting to Kraken orderbook WebSocket...";
  Lwt_unix.getaddrinfo "ws.kraken.com" "443" [ Unix.AI_FAMILY Unix.PF_INET ]
  >>= fun addresses ->
  let ip =
    match addresses with
    | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ -> Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws.kraken.com"
  in
  let client = `TLS (`Hostname "ws.kraken.com", `IP ip, `Port 443) in
  let ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx client uri
  >>= fun conn ->
  Lwt_mutex.with_lock state.mutex (fun () ->
    state.active_conn <- Some conn;
    Lwt.return_unit)
  >>= fun () ->
  Logging.debug_f ~section "Orderbook WebSocket established, subscribing...";
  on_connected ();
  start_message_handler conn all_syms on_failure on_heartbeat
  >>= fun () ->
  Logging.debug_f ~section "Orderbook WebSocket connection closed";
  Lwt.return_unit
;;

let initialize symbols =
  let _ = add_subscribed_symbols symbols in
  Logging.debug_f
    ~section
    "Initializing orderbook feed for %d symbols"
    (List.length symbols);
  fetch_decimals symbols
  >>= fun () ->
  List.iter
    (fun symbol ->
       let _ = ensure_store symbol in
       Logging.debug_f ~section "Created orderbook store for %s" symbol)
    symbols;
  Logging.debug_f ~section "Orderbook feed stores initialized";
  Lwt.return_unit
;;
