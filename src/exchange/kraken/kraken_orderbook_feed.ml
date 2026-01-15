(** Kraken Orderbook Feed - WebSocket v2 depth subscription with ring buffer *)
(* TODO: Extract duplicate utility functions (get_conduit_ctx) to common module *)

open Lwt.Infix
module Memory_events = Dio_memory_tracing.Memory_events

let section = "kraken_orderbook"

(* TODO: Duplicate function - also exists in kraken_trading_client.ml, should be moved to common utilities *)
(** Safely force Conduit context with error handling *)
let get_conduit_ctx () =
  try
    Lazy.force Conduit_lwt_unix.default_ctx
  with
  | CamlinternalLazy.Undefined ->
      Logging.error ~section "Conduit context was accessed before initialization - this should not happen";
      raise (Failure "Conduit context not initialized - ensure main.ml initializes it before domain spawning")
  | exn ->
      Logging.error_f ~section "Failed to get Conduit context: %s" (Printexc.to_string exn);
      raise exn

(* TODO: Magic numbers - orderbook_depth and ring_buffer_size should be configurable *)
let orderbook_depth = 25
let ring_buffer_size = 64

(** Global flag to track if cleanup handlers are running *)
let cleanup_handlers_started = Atomic.make false

(** Shared CRC32 implementation for checksum calculations *)
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

(** Shared helpers for checksum calculations *)
let remove_decimal s =
  let b = Buffer.create (String.length s) in
  String.iter (fun c -> if c <> '.' then Buffer.add_char b c) s;
  Buffer.contents b

let remove_leading_zeros s =
  let len = String.length s in
  let rec aux i =
    if i >= len then ""
    else if s.[i] = '0' then aux (i + 1)
    else String.sub s i (len - i)
  in
  let trimmed = aux 0 in
  if trimmed = "" then "0" else trimmed

(** Individual price level with both float and string representations *)
type level = {
  price: string;
  size: string;
  price_float: float;
  size_float: float;
}

type orderbook = {
  symbol: string;
  bids: level array;
  asks: level array;
  sequence: int64 option;
  checksum: int32 option; (* Kraken CRC32 checksum *)
  timestamp: float;
}

(** Take first n elements from list, or all elements if fewer than n *)
let take n lst =
  let rec take_aux acc n = function
    | [] -> List.rev acc
    | h :: t when n > 0 -> take_aux (h :: acc) (n - 1) t
    | _ -> List.rev acc
  in
  take_aux [] n lst

(** Decimal-aware string comparator for sorting prices (avoids float precision issues).
    Assumes format like "X.YYYY" or "0.YYYY"; compares numerically by aligning decimals. *)
let decimal_compare s1 s2 =
  let normalize s =
    let parts = String.split_on_char '.' s in
    match parts with
    | [whole] -> (whole, "0")
    | [whole; frac] -> (whole, frac)
    | _ -> ("0", "0")
  in
  let (w1, f1) = normalize s1 in
  let (w2, f2) = normalize s2 in
  let cmp_whole = compare (int_of_string w1) (int_of_string w2) in
  if cmp_whole <> 0 then cmp_whole else
  let pad_frac f = f ^ String.make (max 0 (15 - String.length f)) '0' in  (* Pad to 15 decimals for compare *)
  String.compare (pad_frac f1) (pad_frac f2)

let to_decimal_str ?(trim_trailing=true) ?dec json =
  match json with
  | `String s -> s  (* Rare, but handle if strings appear *)
  | `Float f ->
      let d = match dec with Some d -> d | None -> 12 in
      let s = Printf.sprintf "%.*f" d f in
      if not (String.contains s '.') || not trim_trailing then s
      else
        let parts = String.split_on_char '.' s in
        (match parts with
         | [whole; frac] ->
             if trim_trailing then
               (* Existing rtrim logic *)
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

(** Extract raw price/qty strings from JSON for checksum calculation *)
let parse_checksum_level price_json qty_json =
  let price_str = match price_json with
    | `String s -> s
    | `Float f -> 
        (* Use same formatting as to_decimal_str but without decimal lookup *)
        let s = Printf.sprintf "%.12f" f in
        if String.contains s '.' then
          let parts = String.split_on_char '.' s in
          match parts with
          | [whole; frac] ->
              let len = String.length frac in
              let rec rtrim i =
                if i <= 0 then ""
                else if frac.[i-1] = '0' then rtrim (i-1)
                else String.sub frac 0 i
              in
              let frac_clean = rtrim len in
              if String.length frac_clean = 0 then whole else whole ^ "." ^ frac_clean
          | _ -> s
        else s
    | `Int i -> string_of_int i
    | `Intlit s -> s
    | _ -> "0"
  in
  let qty_str = match qty_json with
    | `String s -> s
    | `Float f ->
        let s = Printf.sprintf "%.12f" f in
        if String.contains s '.' then
          let parts = String.split_on_char '.' s in
          match parts with
          | [whole; frac] ->
              let len = String.length frac in
              let rec rtrim i =
                if i <= 0 then ""
                else if frac.[i-1] = '0' then rtrim (i-1)
                else String.sub frac 0 i
              in
              let frac_clean = rtrim len in
              if String.length frac_clean = 0 then whole else whole ^ "." ^ frac_clean
          | _ -> s
        else s
    | `Int i -> string_of_int i
    | `Intlit s -> s
    | _ -> "0"
  in
  (price_str, qty_str)

(** Calculate CRC32 checksum per Kraken specification using top 10 levels *)
let calculate_checksum_from_json symbol bids_json asks_json : int32 =
  (* Parse levels directly from JSON for checksum calculation *)
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

  (* Helper: check if size is effectively zero using string comparison *)
  let is_effectively_zero qty_str =
    let rec has_non_zero s i =
      if i >= String.length s then false
      else if s.[i] <> '0' && s.[i] <> '.' then true
      else has_non_zero s (i + 1)
    in
    not (has_non_zero qty_str 0)
  in

  (* Filter out levels with zero quantities *)
  let valid_bids = List.filter (fun (_, qty_str) -> not (is_effectively_zero qty_str)) bids_levels in
  let valid_asks = List.filter (fun (_, qty_str) -> not (is_effectively_zero qty_str)) asks_levels in

  (* Sort bids descending (high to low), asks ascending (low to high) *)
  let sorted_bids = List.sort (fun (p1, _) (p2, _) ->
    let cmp = decimal_compare p2 p1 in
    cmp
  ) valid_bids in

  let sorted_asks = List.sort (fun (p1, _) (p2, _) ->
    let cmp = decimal_compare p1 p2 in
    cmp
  ) valid_asks in

  (* Take top min(10, available) - NO PADDING per Kraken spec *)
  let top_bids = take (min 10 (List.length sorted_bids)) sorted_bids in
  let top_asks = take (min 10 (List.length sorted_asks)) sorted_asks in

  Logging.debug_f ~section "Checksum input: symbol=%s bids=%d asks=%d" symbol (List.length bids_levels) (List.length asks_levels);
  Logging.debug_f ~section "Checksum levels used: bids=%d asks=%d" (List.length top_bids) (List.length top_asks);

  let format_price_level (price_str, qty_str) : string =
    let price_norm = remove_decimal price_str in
    let qty_norm = remove_decimal qty_str in

    let price_clean = remove_leading_zeros price_norm in
    let qty_clean = remove_leading_zeros qty_norm in

    price_clean ^ qty_clean
  in

  (* Generate asks string (sorted low to high) *)
  let asks_string = String.concat "" (List.map format_price_level top_asks) in

  (* Generate bids string (sorted high to low) *)
  let bids_string = String.concat "" (List.map format_price_level top_bids) in

  (* Concatenate asks + bids *)
  let combined_string = asks_string ^ bids_string in

  let result = crc32_zlib combined_string in

  Logging.debug_f ~section "Checksum CRC32: input_len=%d result=%ld (0x%08lx)"
    (String.length combined_string) result result;

  result

(** Lock-free ring buffer for orderbook data - shared implementation *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

module PriceMap = Map.Make (struct
  type t = string
  let compare = String.compare
end)

(** Per-symbol orderbook storage and readiness signalling *)
type store = {
  mutable buffer: orderbook RingBuffer.t;
  mutable bids: (string * string * float * float) PriceMap.t;
  mutable asks: (string * string * float * float) PriceMap.t;
  ready: bool Atomic.t;
  has_snapshot: bool Atomic.t;  (** Track if we have received a snapshot for this symbol *)
  last_sequence: int64 option Atomic.t;  (** Track the last sequence number for this symbol *)
  mutable last_update: float;  (** Track when this store was last updated *)
}

type decimals = int * int  (* pair_decimals, lot_decimals *)

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let decimals_tbl : (string, decimals) Hashtbl.t = Hashtbl.create 16
let ready_condition = Lwt_condition.create ()

(** Get precision info from instruments feed cache *)
let get_precision_from_instruments symbol =
  try
    (* Use the exported function from instruments feed *)
    Kraken_instruments_feed.get_precision_info symbol
  with _ -> None

let calculate_checksum symbol bids asks : int32 =
  let pd, ld =
    (* First try to get precision from instruments feed *)
    match get_precision_from_instruments symbol with
    | Some (price_prec, qty_prec) ->
        Logging.debug_f ~section "Using instruments feed precision for %s: price=%d qty=%d" symbol price_prec qty_prec;
        (price_prec, qty_prec)
    | None ->
        (* Fall back to decimals_tbl from AssetPairs API *)
        try Hashtbl.find decimals_tbl symbol
        with Not_found ->
          Logging.debug_f ~section "No precision found for %s, using defaults" symbol;
          (8, 8)  (* Fallback defaults *)
  in

  (* Helper: check if size is effectively zero using string comparison *)
  let is_effectively_zero size =
    (* Remove leading zeros and check if empty or all zeros *)
    let rec has_non_zero s i =
      if i >= String.length s then false
      else if s.[i] <> '0' && s.[i] <> '.' then true
      else has_non_zero s (i + 1)
    in
    not (has_non_zero size 0)
  in

  (* Filter out levels with zero quantities *)
  let valid_bids = Array.to_list bids |> List.filter (fun level -> not (is_effectively_zero level.size)) in
  let valid_asks = Array.to_list asks |> List.filter (fun level -> not (is_effectively_zero level.size)) in

  (* Sort bids descending (high to low), asks ascending (low to high) *)
  let sorted_bids = List.sort (fun l1 l2 ->
    decimal_compare l2.price l1.price
  ) valid_bids in

  let sorted_asks = List.sort (fun l1 l2 ->
    decimal_compare l1.price l2.price
  ) valid_asks in

  (* Take top min(10, available) - NO PADDING per Kraken spec *)
  let top_bids = take (min 10 (List.length sorted_bids)) sorted_bids in
  let top_asks = take (min 10 (List.length sorted_asks)) sorted_asks in

  Logging.debug_f ~section "Checksum input: symbol=%s bids=%d asks=%d" symbol (Array.length bids) (Array.length asks);
  Logging.debug_f ~section "Checksum levels used: bids=%d asks=%d" (List.length top_bids) (List.length top_asks);

  let format_price_level (level: level) : string =
    let full_price_str = Printf.sprintf "%.*f" pd level.price_float in
    let full_qty_str = Printf.sprintf "%.*f" ld level.size_float in
    let price_norm = remove_decimal full_price_str in
    let qty_norm = remove_decimal full_qty_str in

    let price_clean = remove_leading_zeros price_norm in
    let qty_clean = remove_leading_zeros qty_norm in

    price_clean ^ qty_clean
  in

  (* Generate asks string (sorted low to high) *)
  let asks_string = String.concat "" (List.map format_price_level top_asks) in

  (* Generate bids string (sorted high to low) *)
  let bids_string = String.concat "" (List.map format_price_level top_bids) in

  (* Concatenate asks + bids *)
  let combined_string = asks_string ^ bids_string in

  let result = crc32_zlib combined_string in

  Logging.debug_f ~section "Checksum CRC32: input_len=%d result=%ld (0x%08lx)"
    (String.length combined_string) result result;

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

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try
      Lwt_condition.broadcast ready_condition ()
    with _ ->
      (* Ignore all exceptions during broadcast - waiters may have been cancelled *)
      ())
  end

let is_effectively_zero size =
  (* Remove leading zeros and check if empty or all zeros *)
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
    (* First try to get precision from instruments feed *)
    match get_precision_from_instruments symbol with
    | Some (price_prec, qty_prec) ->
        Logging.debug_f ~section "Found instruments precision for %s: price=%d qty=%d" symbol price_prec qty_prec;
        (price_prec, qty_prec)
    | None ->
        (* Fall back to decimals_tbl from AssetPairs API *)
        try
          let decimals = Hashtbl.find decimals_tbl symbol in
          Logging.debug_f ~section "Found decimals_tbl for %s: %d, %d" symbol (fst decimals) (snd decimals);
          decimals
        with Not_found ->
          Logging.debug_f ~section "No decimals found for %s, using defaults" symbol;
          (8, 8)  (* Fallback defaults *)
  in

  let price_str = to_decimal_str ~dec:pd price_json in
  let qty_str = to_decimal_str ~dec:ld size_json in
  let price_float = try float_of_string price_str with _ -> 0.0 in
  let qty_float = try float_of_string qty_str with _ -> 0.0 in
  Some (price_str, qty_str, price_float, qty_float)

let parse_levels symbol json =
  match json with
  | `List entries -> List.filter_map (fun entry ->
      match entry with
      (* New format: objects with "price" and "qty" fields *)
      | `Assoc fields ->
          (match List.assoc_opt "price" fields, List.assoc_opt "qty" fields with
           | Some price_json, Some qty_json -> parse_level symbol price_json qty_json
           | _ -> None)
      (* Legacy format: arrays [price, qty] *)
      | `List [price_json; size_json] -> parse_level symbol price_json size_json
      | `List [price_json; size_json; _] -> parse_level symbol price_json size_json
      | _ -> None
    ) entries
  | _ -> []

let apply_levels map levels =
  List.fold_left (fun acc (price_str, size_str, price_float, size_float) ->
    if is_effectively_zero size_str then
      PriceMap.remove price_str acc
    else
      PriceMap.add price_str (price_str, size_str, price_float, size_float) acc
  ) map levels

let levels_to_array ?(sort_desc = false) map depth =
  let levels_list = PriceMap.fold (fun _ (price_str, size_str, price_float, size_float) acc ->
    { price = price_str; size = size_str; price_float; size_float } :: acc
  ) map [] in

  let sorted_levels = List.sort (fun l1 l2 ->
    let cmp = if sort_desc then decimal_compare l2.price l1.price else decimal_compare l1.price l2.price in
    cmp
  ) levels_list in

  let rec take n lst acc =
    if n = 0 then List.rev acc
    else match lst with
    | [] -> List.rev acc
    | level :: rest -> take (n - 1) rest (level :: acc)
  in

  Array.of_list (take depth sorted_levels [])

let rebuild_map_from_top_levels map sort_desc max_levels =
  let levels_array = levels_to_array ~sort_desc map max_levels in
  Array.fold_left (fun acc level ->
    PriceMap.add level.price (level.price, level.size, level.price_float, level.size_float) acc
  ) PriceMap.empty levels_array

let build_orderbook store symbol entry =
  let open Yojson.Safe.Util in
  let sequence =
    match int64_of_json (member "sequence" entry) with
    | Some seq -> Some seq
    | None -> None
  in
  let checksum =
    let checksum_json = member "checksum" entry in
    Logging.debug_f ~section "Parsing checksum JSON: %s" (Yojson.Safe.to_string checksum_json);
    match checksum_json with
    | `Int i ->
        Logging.debug_f ~section "Checksum parsed as Int: %d" i;
        Some (Int32.of_int i)
    | `Intlit s ->
        Logging.debug_f ~section "Checksum parsed as Intlit: %s" s;
        (try Some (Int32.of_string s) with exn ->
          Logging.error_f ~section "Failed to parse Intlit checksum %s: %s" s (Printexc.to_string exn);
          None)
    | `Float f ->
        Logging.debug_f ~section "Checksum parsed as Float: %f" f;
        (try Some (Int32.of_float f) with exn ->
          Logging.error_f ~section "Failed to parse Float checksum %f: %s" f (Printexc.to_string exn);
          None)
    | `String s ->
        Logging.debug_f ~section "Checksum parsed as String: %s" s;
        (try Some (Int32.of_string s) with exn ->
          Logging.error_f ~section "Failed to parse String checksum %s: %s" s (Printexc.to_string exn);
          None)
    | json ->
        Logging.error_f ~section "Unexpected checksum JSON type: %s" (Yojson.Safe.to_string json);
        None
  in
  {
    symbol;
    bids = levels_to_array ~sort_desc:true store.bids orderbook_depth;
    asks = levels_to_array ~sort_desc:false store.asks orderbook_depth;
    sequence;
    checksum;
    timestamp = Unix.time ();
  }

let fetch_decimals symbols =
  (* First check if we can get precision from instruments feed for all symbols *)
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
              if altname = Some sym || wsname = Some sym then
                Hashtbl.add decimals_tbl sym (pd, ld)
            ) symbols
          ) pairs;
          Lwt.return ()
      | _ -> Lwt.fail_with "Error fetching asset pairs"
    with exn ->
      Logging.warn_f ~section "Failed to fetch decimals: %s" (Printexc.to_string exn);
      Lwt.return ()
  )

let process_orderbook_message ~reset json on_heartbeat =
  let open Yojson.Safe.Util in
  try
    let data = member "data" json |> to_list in
    Logging.debug_f ~section "Processing orderbook message with %d entries (reset=%b)"
      (List.length data) reset;
    (* Track symbols that successfully wrote to buffer in this message *)
    let notified_symbols = Hashtbl.create 16 in
    List.iter (fun entry ->
      try
        let symbol = member "symbol" entry |> to_string in
        let store = ensure_store symbol in

        (* Handle snapshot vs update logic *)
        if reset then begin
          (* Snapshot: reset state and mark as having snapshot *)
          store.bids <- PriceMap.empty;
          store.asks <- PriceMap.empty;
          Atomic.set store.has_snapshot true;

          (* Extract sequence from snapshot entry *)
          let sequence =
            match int64_of_json (member "sequence" entry) with
            | Some seq -> Some seq
            | None -> None
          in
          Atomic.set store.last_sequence sequence;
          Logging.debug_f ~section "Received snapshot for %s (sequence=%s), ready for updates"
            symbol (match sequence with Some s -> Int64.to_string s | None -> "none");

        end else begin
          (* Update: only process if we have a snapshot *)
          if not (Atomic.get store.has_snapshot) then begin
            Logging.debug_f ~section "Ignoring update for %s: waiting for snapshot after reconnect"
              symbol;
            raise Exit  (* Skip processing this entry *)
          end;

          (* Validate sequence ordering for updates *)
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
              store.buffer <- RingBuffer.create ring_buffer_size;
              Atomic.set store.has_snapshot false;
              Atomic.set store.last_sequence None;
              raise Exit  (* Skip processing this entry *)
          | Some curr_seq, Some last_seq when Int64.compare curr_seq (Int64.add last_seq 1L) > 0 ->
              let gap = Int64.sub curr_seq last_seq in
              Logging.warn_f ~section "Sequence gap for %s: current=%Ld last=%Ld (gap=%Ld), marking out-of-sync"
                symbol curr_seq last_seq gap;
              store.bids <- PriceMap.empty;
              store.asks <- PriceMap.empty;
              store.buffer <- RingBuffer.create ring_buffer_size;
              Atomic.set store.has_snapshot false;
              Atomic.set store.last_sequence None;
              raise Exit  (* Skip processing this entry *)
          | _ -> ()  (* Sequence is valid or not present *)
        end;
        let bids_json = member "bids" entry in
        let asks_json = member "asks" entry in
        let bids = parse_levels symbol bids_json in
        let asks = parse_levels symbol asks_json in
        Logging.debug_f ~section "Parsed levels for %s: bids=%d asks=%d"
          symbol (List.length bids) (List.length asks);

        store.bids <- apply_levels store.bids bids;
        store.asks <- apply_levels store.asks asks;
        store.last_update <- Unix.time ();  (* Update timestamp on data change *)

        (* Truncate maps to top 25 levels to prevent bloat *)
        store.bids <- rebuild_map_from_top_levels store.bids true 25;
        store.asks <- rebuild_map_from_top_levels store.asks false 25;

        (* Calculate checksum from current orderbook state (top 10 levels) *)
        let calculated_checksum = calculate_checksum symbol
          (levels_to_array ~sort_desc:true store.bids 10)
          (levels_to_array ~sort_desc:false store.asks 10) in

        let orderbook = build_orderbook store symbol entry in
        Logging.debug_f ~section "Built orderbook for %s: bids=%d asks=%d checksum=%s"
          symbol (Array.length orderbook.bids) (Array.length orderbook.asks)
          (match orderbook.checksum with Some c -> Int32.to_string c | None -> "none");

        (* Verify checksum - only proceed if valid *)
        let checksum_valid =
          match orderbook.checksum with
          | Some received_checksum ->
              if Int32.compare calculated_checksum received_checksum <> 0 then begin
                Logging.warn_f ~section "Checksum mismatch for %s: received=%ld (0x%08lx) calculated=%ld (0x%08lx), marking out-of-sync"
                  symbol received_checksum received_checksum calculated_checksum calculated_checksum;
                (* Mark symbol out-of-sync on checksum mismatch *)
                store.bids <- PriceMap.empty;
                store.asks <- PriceMap.empty;
                store.buffer <- RingBuffer.create ring_buffer_size;
                Atomic.set store.has_snapshot false;
                Atomic.set store.last_sequence None;
                false  (* Don't write to buffer *)
              end else begin
                Logging.debug_f ~section "Checksum match for %s: %ld" symbol calculated_checksum;
                true   (* Write to buffer *)
              end
          | None -> true  (* No checksum to validate, proceed *)
        in

        if checksum_valid then begin
          RingBuffer.write store.buffer orderbook;
          (* Record that this symbol should be notified (only once per message) *)
          Hashtbl.replace notified_symbols symbol store;
          Logging.debug_f ~section "Orderbook written to ringbuffer: %s bids=%d asks=%d"
            symbol
            (Array.length orderbook.bids)
            (Array.length orderbook.asks);

          (* Update last sequence on successful write *)
          let current_sequence =
            match int64_of_json (member "sequence" entry) with
            | Some seq -> Some seq
            | None -> None
          in
          Atomic.set store.last_sequence current_sequence;
        end;

        (* Update connection heartbeat *)
        on_heartbeat ()
      with
      | Exit -> ()  (* Exit is used for control flow to skip entries - don't log *)
      | exn ->
        Logging.warn_f ~section "Failed to process orderbook entry: %s"
          (Printexc.to_string exn)
    ) data;

    (* Notify readiness for all symbols that successfully wrote to buffer (once per symbol per message) *)
    Hashtbl.iter (fun _ store -> notify_ready store) notified_symbols;

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
      Some (bid.price, bid.size, ask.price, ask.size)
  | _ -> None

(** Read orderbook events since last position - for domain consumers *)
let[@inline always] read_orderbook_events symbol last_pos =
  match store_opt symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Get current write position for tracking consumption *)
let[@inline always] get_current_position symbol =
  match store_opt symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

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

(** Clear all orderbook stores - used when reconnecting to ensure clean state *)
let clear_all_stores () =
  Hashtbl.iter (fun symbol store ->
    Logging.debug_f ~section "Clearing orderbook store for %s" symbol;
    store.bids <- PriceMap.empty;
    store.asks <- PriceMap.empty;
    (* Replace ring buffer with fresh empty one to clear any stale data *)
    store.buffer <- RingBuffer.create ring_buffer_size;
    Atomic.set store.ready false;
    Atomic.set store.has_snapshot false;
    Atomic.set store.last_sequence None;
    store.last_update <- Unix.time ()
  ) stores

(** Prune inactive stores and stale price levels to prevent memory growth *)
let prune_stale_data () =
  let now = Unix.gettimeofday () in
  let stale_threshold = 30.0 *. 60.0 in  (* 30 minutes *)
  let max_price_levels = 100 in  (* Max levels to keep per side *)

  let stores_to_remove = ref [] in
  let trimmed_stores = ref [] in
  let total_stores_before = Hashtbl.length stores in

  Hashtbl.iter (fun symbol store ->
    let age = now -. store.last_update in

    (* Check if store is stale *)
    if age > stale_threshold then begin
      stores_to_remove := symbol :: !stores_to_remove
    end else begin
      (* For active stores, check if price maps are too large *)
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

  (* Remove stale stores *)
  List.iter (fun symbol ->
    Hashtbl.remove stores symbol;
    Logging.debug_f ~section "Removed stale orderbook store for %s (age > 30min)" symbol
  ) !stores_to_remove;

  (* Log trimming actions *)
  if !trimmed_stores <> [] then
    Logging.debug_f ~section "Trimmed price levels for %d active stores: %s"
      (List.length !trimmed_stores) (String.concat ", " !trimmed_stores);

  let stores_removed = List.length !stores_to_remove in
  let stores_trimmed = List.length !trimmed_stores in
  let total_stores_after = Hashtbl.length stores in

  if stores_removed > 0 || stores_trimmed > 0 then
    Logging.info_f ~section "Orderbook cleanup: removed %d stale stores, trimmed %d active stores (%d -> %d total stores)"
      stores_removed stores_trimmed total_stores_before total_stores_after

(** Event-driven trigger for orderbook cleanup *)
let trigger_orderbook_cleanup ~reason () =
  Lwt.async (fun () ->
    Logging.debug_f ~section "Triggering orderbook cleanup (reason=%s)" reason;
    prune_stale_data ();
    Lwt.return_unit
  )

(** Start listening to memory events to initiate cleanups under pressure *)
let start_cleanup_handlers () =
  if Atomic.compare_and_set cleanup_handlers_started false true then begin
    let subscription = Memory_events.subscribe_memory_events () in
    Lwt.async (fun () ->
      let rec loop () =
        Lwt_stream.get subscription.stream >>= function
        | Some (Memory_events.MemoryPressure _) ->
            trigger_orderbook_cleanup ~reason:"memory_pressure" ();
            loop ()
        | Some (Memory_events.CleanupRequested | Memory_events.Heartbeat) ->
            loop ()

        | None ->
            subscription.close ();
            Logging.info ~section "Orderbook cleanup memory event stream closed";
            Lwt.return_unit
      in
      loop ()
    )
  end

let handle_message message on_heartbeat =
  Logging.debug_f ~section "Received orderbook WebSocket message (length=%d)"
    (String.length message);
  try
    let json = Yojson.Safe.from_string message in
    let open Yojson.Safe.Util in
    let channel = member "channel" json |> to_string_option in
    let msg_type = member "type" json |> to_string_option in
    let method_type = member "method" json |> to_string_option in
    Logging.debug_f ~section "Message parsed: channel=%s type=%s method=%s"
      (match channel with Some c -> c | None -> "none")
      (match msg_type with Some t -> t | None -> "none")
      (match method_type with Some m -> m | None -> "none");

    match channel, msg_type, method_type with
  | Some "book", Some "snapshot", _ ->
      ignore (process_orderbook_message ~reset:true json on_heartbeat);
      trigger_orderbook_cleanup ~reason:"orderbook_snapshot" ()
  | Some "book", Some "update", _ ->
      ignore (process_orderbook_message ~reset:false json on_heartbeat);
      trigger_orderbook_cleanup ~reason:"orderbook_update" ()
    | Some "heartbeat", _, _ -> on_heartbeat () (* Update connection heartbeat *)
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
        (* Wait for condition signal, but handle potential cancellation issues *)
        Lwt.catch (fun () ->
          Lwt_condition.wait ready_condition >>= fun () -> wait_loop ()
        ) (fun _ ->
          (* If condition wait fails (e.g., due to cancellation), treat as timeout *)
          timeout_ref := true;
          Lwt.return_false
        )
  in
  (* Start a background timeout that sets the flag *)
  Lwt.async (fun () ->
    Lwt_unix.sleep timeout_seconds >>= fun () ->
    timeout_ref := true;
    Lwt.return_unit
  );
  wait_loop ()

let wait_for_orderbook_data = wait_for_orderbook_data_lwt


(** Message handling loop - runs in background *)
let start_message_handler conn symbols on_failure on_heartbeat =
  let subscribe_msg = `Assoc [
    ("method", `String "subscribe");
    ("params", `Assoc [
      ("channel", `String "book");
      ("symbol", `List (List.map (fun s -> `String s) symbols));
      ("depth", `Int orderbook_depth)
    ])
  ] in
  let msg_str = Yojson.Safe.to_string subscribe_msg in
  Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg_str ()) >>= fun () ->

  let rec msg_loop () =
    Lwt.catch (fun () ->
      Websocket_lwt_unix.read conn >>= function
      | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
          Logging.warn ~section "Orderbook WebSocket closed by server";
          on_failure "Connection closed by server";
          Lwt.return_unit
      | frame ->
          handle_message frame.Websocket.Frame.content on_heartbeat;
          msg_loop ()
    ) (function
      | End_of_file ->
          Logging.warn ~section "Orderbook WebSocket connection closed unexpectedly (End_of_file)";
          (* Notify supervisor of connection failure *)
          on_failure "Connection closed unexpectedly (End_of_file)";
          Lwt.return_unit
      | exn ->
          Logging.error_f ~section "Orderbook WebSocket error during read: %s" (Printexc.to_string exn);
          (* Notify supervisor of connection failure *)
          on_failure (Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn));
          Lwt.return_unit
    )
  in
  msg_loop ()

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

    Logging.debug_f ~section "Orderbook WebSocket established, subscribing...";
    (* Call on_connected callback after successful connection and before starting message handler *)
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

  (* Enable event-driven cleanup *)
  start_cleanup_handlers ();
  trigger_orderbook_cleanup ~reason:"init" ();

  Lwt.return_unit