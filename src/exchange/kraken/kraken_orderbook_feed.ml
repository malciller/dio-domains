(** Kraken Orderbook Feed - WebSocket v2 depth subscription with ring buffer *)

open Lwt.Infix

let section = "kraken_orderbook"

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

let orderbook_depth = 25
let ring_buffer_size = 256

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

(** Lock-free ring buffer for orderbook data using atomics with memory tracing *)
module RingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    size: int;
    mutable tracking_record: Obj.t option;  (** Memory tracing record *)
  }

  let create size =
    let buffer = {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      size;
      tracking_record = None;
    } in
    (* Add memory tracking for the ring buffer structure *)
    let buffer_size = size * (Sys.word_size / 8) in  (* Estimate size based on array elements *)
    buffer.tracking_record <- Some (Obj.repr (Dio_memory_tracing.Memory_tracing.track_custom_structure
      "RingBuffer" buffer_size));
    buffer

  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos) (Some value);
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos

  let read_latest buffer =
    let pos = Atomic.get buffer.write_pos in
    let read_pos = if pos = 0 then buffer.size - 1 else pos - 1 in
    Atomic.get buffer.data.(read_pos)

  (** Read events from last known position - for domain consumers *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos then
      []
    else
      let rec collect acc pos =
        if pos = current_pos then
          List.rev acc
        else
          match Atomic.get buffer.data.(pos) with
          | Some event -> collect (event :: acc) ((pos + 1) mod buffer.size)
          | None -> collect acc ((pos + 1) mod buffer.size)
      in
      collect [] last_pos

  (** Clean up memory tracking when buffer is no longer needed *)
  let destroy buffer =
    match buffer.tracking_record with
    | Some record -> Dio_memory_tracing.Memory_tracing.untrack_custom_structure (Obj.obj record)
    | None -> ()
end

module PriceMap = Dio_memory_tracing.Memory_tracing.Tracked.Map.Make (struct
  type t = string
  let compare = String.compare
end)

(** Per-symbol orderbook storage and readiness signalling *)
type store = {
  mutable buffer: orderbook RingBuffer.t;
  mutable bids: (string * string * float * float) PriceMap.t;
  mutable asks: (string * string * float * float) PriceMap.t;
  mutable bids_size: int;  (** Track current size of bids map for efficient size checks *)
  mutable asks_size: int;  (** Track current size of asks map for efficient size checks *)
  ready: bool Atomic.t;
  has_snapshot: bool Atomic.t;  (** Track if we have received a snapshot for this symbol *)
  last_sequence: int64 option Atomic.t;  (** Track the last sequence number for this symbol *)
  mutable last_update: float;  (** Track when this store was last updated *)
  mutable cached_bids_array: level array option;  (** Cache sorted bids array to avoid recomputation *)
  mutable cached_asks_array: level array option;  (** Cache sorted asks array to avoid recomputation *)
  mutable cached_top10_bids: level array option;  (** Cache top 10 bids for checksum calculations *)
  mutable cached_top10_asks: level array option;  (** Cache top 10 asks for checksum calculations *)
  mutable cache_valid: bool;  (** Track if cached arrays are still valid *)
}

type decimals = int * int  (* pair_decimals, lot_decimals *)

let stores : (string, store) Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.t = Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.create 32
let decimals_tbl : (string, decimals) Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.t = Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.create 16
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
        match Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.find_opt decimals_tbl symbol with
        | Some result -> result
        | None ->
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
  match Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.find_opt stores symbol with
  | Some s -> s
  | None ->
      let s = {
        buffer = RingBuffer.create ring_buffer_size;
        bids = PriceMap.empty ();
        asks = PriceMap.empty ();
        bids_size = 0;
        asks_size = 0;
        ready = Atomic.make false;
        has_snapshot = Atomic.make false;
        last_sequence = Atomic.make None;
        last_update = Unix.time ();
        cached_bids_array = None;
        cached_asks_array = None;
        cached_top10_bids = None;
        cached_top10_asks = None;
        cache_valid = false;
      } in
      Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.add stores symbol s;
      s

let store_opt symbol =
  Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.find_opt stores symbol

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
        match Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.find_opt decimals_tbl symbol with
        | Some decimals ->
            Logging.debug_f ~section "Found decimals_tbl for %s: %d, %d" symbol (fst decimals) (snd decimals);
            decimals
        | None ->
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
  (* Build list of bindings to add/update and keys to remove *)
  let bindings_to_add = ref [] in
  let keys_to_remove = ref [] in

  List.iter (fun (price_str, size_str, price_float, size_float) ->
    if is_effectively_zero size_str then
      keys_to_remove := price_str :: !keys_to_remove
    else
      bindings_to_add := (price_str, (price_str, size_str, price_float, size_float)) :: !bindings_to_add
  ) levels;

  (* Get current bindings, filter out removals, add new ones *)
  let current_bindings = PriceMap.bindings map in
  let filtered_bindings = List.filter (fun (k, _) -> not (List.mem k !keys_to_remove)) current_bindings in
  let new_bindings = List.rev_append filtered_bindings !bindings_to_add in

  (* Create new map with all updates in one operation - no intermediate allocations *)
  PriceMap.of_list new_bindings

(** Apply levels and update size counter *)
let apply_levels_with_size_tracking store map levels is_bids =
  let new_map = apply_levels map levels in
  let size_change = (PriceMap.cardinal new_map) - (if is_bids then store.bids_size else store.asks_size) in
  if is_bids then
    store.bids_size <- store.bids_size + size_change
  else
    store.asks_size <- store.asks_size + size_change;
  new_map

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

  Dio_memory_tracing.Memory_tracing.Tracked.Array.of_list (take depth sorted_levels [])

(** Get cached sorted arrays for bids and asks, computing them if needed *)
let get_cached_arrays store depth =
  (* Only recompute if cache is invalid *)
  if not store.cache_valid then begin
    (* Helper to compute sorted array for a price map *)
    let compute_sorted_array price_map sort_desc target_depth =
      let levels_list = PriceMap.fold (fun _ (price_str, size_str, price_float, size_float) acc ->
        { price = price_str; size = size_str; price_float; size_float } :: acc
      ) price_map [] in

      let sorted_levels = List.sort (fun l1 l2 ->
        if sort_desc then decimal_compare l2.price l1.price else decimal_compare l1.price l2.price
      ) levels_list in

      let rec take n lst acc =
        if n = 0 then List.rev acc
        else match lst with
        | [] -> List.rev acc
        | level :: rest -> take (n - 1) rest (level :: acc)
      in

      (* Create regular array directly without tracked array intermediate *)
      let levels = take target_depth sorted_levels [] in
      Array.of_list levels
    in

    (* Compute arrays for requested depth *)
    let bids_array = compute_sorted_array store.bids true depth in
    let asks_array = compute_sorted_array store.asks false depth in
    store.cached_bids_array <- Some bids_array;
    store.cached_asks_array <- Some asks_array;

    (* Always compute top10 arrays for checksums *)
    let top10_bids = compute_sorted_array store.bids true 10 in
    let top10_asks = compute_sorted_array store.asks false 10 in
    store.cached_top10_bids <- Some top10_bids;
    store.cached_top10_asks <- Some top10_asks;

    store.cache_valid <- true;
  end;

  (* Return appropriate cached arrays *)
  if depth = 10 then begin
    (Option.get store.cached_top10_bids, Option.get store.cached_top10_asks)
  end else begin
    let bids_array = Option.get store.cached_bids_array in
    let asks_array = Option.get store.cached_asks_array in
    (* Trim if necessary, but avoid creating new arrays when possible *)
    if Array.length bids_array <= depth && Array.length asks_array <= depth then
      (bids_array, asks_array)
    else
      let bids_trimmed = if Array.length bids_array <= depth then bids_array
                         else Array.sub bids_array 0 depth in
      let asks_trimmed = if Array.length asks_array <= depth then asks_array
                         else Array.sub asks_array 0 depth in
      (bids_trimmed, asks_trimmed)
  end

(** Invalidate cache when maps are modified *)
let invalidate_cache store =
  store.cache_valid <- false;
  store.cached_bids_array <- None;
  store.cached_asks_array <- None;
  store.cached_top10_bids <- None;
  store.cached_top10_asks <- None

(** Rebuild map from top levels and return new size *)
let rebuild_map_from_top_levels map sort_desc max_levels current_size =
  (* Check if we need to rebuild at all - use provided size for efficiency *)
  if current_size <= max_levels then
    (* No need to rebuild - already within limits *)
    (map, current_size)
  else begin
    (* Avoid expensive Map -> List -> Array -> Map conversion chain *)
    (* Directly work with the map to keep only top max_levels entries *)
    let levels_list = PriceMap.fold (fun _ (price_str, size_str, price_float, size_float) acc ->
      { price = price_str; size = size_str; price_float; size_float } :: acc
    ) map [] in

    let sorted_levels = List.sort (fun l1 l2 ->
      let cmp = if sort_desc then decimal_compare l2.price l1.price else decimal_compare l1.price l2.price in
      cmp
    ) levels_list in

    (* Take only the top max_levels *)
    let top_levels = fst (List.fold_left (fun (acc, count) level ->
      if count >= max_levels then (acc, count)
      else (level :: acc, count + 1)
    ) ([], 0) sorted_levels) in

    (* Build bindings list first, then create new map in one operation *)
    let new_bindings = List.map (fun level ->
      (level.price, (level.price, level.size, level.price_float, level.size_float))
    ) top_levels in

    (* Create new map with all updates in one operation - no intermediate allocations *)
    let new_map = PriceMap.of_list new_bindings in

    (new_map, List.length top_levels)
  end

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
  (* Use cached arrays to avoid recomputation *)
  let bids_regular, asks_regular = get_cached_arrays store orderbook_depth in
  {
    symbol;
    bids = bids_regular;
    asks = asks_regular;
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
              if altname = Some sym || wsname = Some sym then begin
                Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.add decimals_tbl sym (pd, ld)
              end
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
  (* Track symbols that successfully wrote to buffer in this message *)
  let notified_symbols = Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.create 8 in
  try
    let data = member "data" json |> to_list in
    Logging.debug_f ~section "Processing orderbook message with %d entries (reset=%b)"
      (List.length data) reset;
    List.iter (fun entry ->
      try
        let symbol = member "symbol" entry |> to_string in
        let store = ensure_store symbol in

        (* Handle snapshot vs update logic *)
        if reset then begin
          (* Snapshot: reset state and mark as having snapshot *)
          (* Track replacements of old maps *)
          store.bids <- PriceMap.empty ~replace_old:(Some store.bids) ();
          store.asks <- PriceMap.empty ~replace_old:(Some store.asks) ();
          store.bids_size <- 0;
          store.asks_size <- 0;
          invalidate_cache store;
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
              (* Track replacements of old maps *)
              store.bids <- PriceMap.empty ~replace_old:(Some store.bids) ();
              store.asks <- PriceMap.empty ~replace_old:(Some store.asks) ();
              store.bids_size <- 0;
              store.asks_size <- 0;
              invalidate_cache store;
              RingBuffer.destroy store.buffer;
              store.buffer <- RingBuffer.create ring_buffer_size;
              Atomic.set store.has_snapshot false;
              Atomic.set store.last_sequence None;
              raise Exit  (* Skip processing this entry *)
          | Some curr_seq, Some last_seq when Int64.compare curr_seq (Int64.add last_seq 1L) > 0 ->
              let gap = Int64.sub curr_seq last_seq in
              Logging.warn_f ~section "Sequence gap for %s: current=%Ld last=%Ld (gap=%Ld), marking out-of-sync"
                symbol curr_seq last_seq gap;
              (* Track replacements of old maps *)
              store.bids <- PriceMap.empty ~replace_old:(Some store.bids) ();
              store.asks <- PriceMap.empty ~replace_old:(Some store.asks) ();
              store.bids_size <- 0;
              store.asks_size <- 0;
              invalidate_cache store;
              RingBuffer.destroy store.buffer;
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

        (* Apply levels and track map replacements *)
        let new_bids = apply_levels_with_size_tracking store store.bids bids true in
        let new_asks = apply_levels_with_size_tracking store store.asks asks false in
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.record_replacement
          store.bids.record.id
          new_bids.record.id;
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.record_replacement
          store.asks.record.id
          new_asks.record.id;
        store.bids <- new_bids;
        store.asks <- new_asks;
        store.last_update <- Unix.time ();  (* Update timestamp on data change *)

        (* Truncate maps to top 25 levels to prevent bloat - only if actually oversized *)
        let bids_map, bids_size = rebuild_map_from_top_levels store.bids true 25 store.bids_size in
        let asks_map, asks_size = rebuild_map_from_top_levels store.asks false 25 store.asks_size in
        (* Track map replacements *)
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.record_replacement
          store.bids.record.id
          bids_map.record.id;
        Dio_memory_tracing.Memory_tracing.Internal.Allocation_tracker.record_replacement
          store.asks.record.id
          asks_map.record.id;
        store.bids <- bids_map;
        store.asks <- asks_map;
        store.bids_size <- bids_size;
        store.asks_size <- asks_size;
        invalidate_cache store;

        (* Calculate checksum from current orderbook state (top 10 levels) *)
        let bids_top10, asks_top10 = get_cached_arrays store 10 in
        let calculated_checksum = calculate_checksum symbol bids_top10 asks_top10 in

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
                (* Track replacements of old maps *)
                store.bids <- PriceMap.empty ~replace_old:(Some store.bids) ();
                store.asks <- PriceMap.empty ~replace_old:(Some store.asks) ();
                store.bids_size <- 0;
                store.asks_size <- 0;
                invalidate_cache store;
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
          Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.replace notified_symbols symbol store;
          Logging.debug_f ~section "Orderbook written to ringbuffer: %s bids=%d asks=%d"
            symbol
            (Array.length orderbook.bids)
            (Array.length orderbook.asks);
          Telemetry.inc_counter (Telemetry.counter "orderbook_updates" ()) ();

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
    Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.iter (fun _ store -> notify_ready store) notified_symbols;
    Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.destroy notified_symbols;

    Some ()
  with exn ->
    (* Clean up hashtable in case of exception *)
    (try Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.destroy notified_symbols with _ -> ());
    Logging.warn_f ~section "Failed to parse orderbook message: %s"
      (Printexc.to_string exn);
    None

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
    | Some "book", Some "snapshot", _ -> ignore (process_orderbook_message ~reset:true json on_heartbeat)
    | Some "book", Some "update", _ -> ignore (process_orderbook_message ~reset:false json on_heartbeat)
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
  | Some store -> Atomic.get store.buffer.write_pos
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

(** Destroy all orderbook stores - used when reconnecting to ensure clean state *)
let destroy_all_stores () =
  (* Collect all symbols first - can't iterate and remove simultaneously *)
  let symbols_to_destroy =
    Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.fold
      (fun symbol _ acc -> symbol :: acc) stores [] in

  List.iter (fun symbol ->
    match Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.find_opt stores symbol with
    | Some store ->
        Logging.debug_f ~section "Destroying orderbook store for %s" symbol;
        (* Destroy ring buffer *)
        RingBuffer.destroy store.buffer;
        (* Destroy maps - note: PriceMap.empty with replace_old handles destruction *)
        ignore (PriceMap.empty ~replace_old:(Some store.bids) ());
        ignore (PriceMap.empty ~replace_old:(Some store.asks) ());
        (* Remove from hashtable - this completely destroys the store *)
        Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.remove stores symbol
    | None -> ()
  ) symbols_to_destroy

(** Prune inactive stores and stale price levels to prevent memory growth *)
let prune_stale_data () =
  let now = Unix.gettimeofday () in
  let stale_threshold = 30.0 *. 60.0 in  (* 30 minutes *)
  let max_price_levels = 100 in  (* Max levels to keep per side *)
  let max_stores = 50 in  (* Maximum number of active stores to prevent unbounded growth *)

  let stores_to_remove = ref [] in
  let trimmed_stores = ref [] in
  let total_stores_before = Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.length stores in

  (* Collect all stores with their last update times for LRU-style cleanup *)
  let store_activity = ref [] in
  Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.iter (fun symbol store ->
    store_activity := (symbol, store.last_update) :: !store_activity
  ) stores;

  (* Sort by last update time (oldest first) *)
  let sorted_stores = List.sort (fun (_, t1) (_, t2) -> compare t1 t2) !store_activity in

  (* Remove stores beyond our limit (keeping most recently active) *)
  if List.length sorted_stores > max_stores then begin
    let excess_count = List.length sorted_stores - max_stores in
    let rec take n lst acc =
      if n = 0 then List.rev acc
      else match lst with
      | [] -> List.rev acc
      | (symbol, _) :: rest -> take (n - 1) rest (symbol :: acc)
    in
    let stores_to_remove_lru = take excess_count sorted_stores [] in
    stores_to_remove := stores_to_remove_lru @ !stores_to_remove;
    Logging.debug_f ~section "Removing %d stores due to connection limit (%d max): %s"
      excess_count max_stores (String.concat ", " stores_to_remove_lru)
  end;

  Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.iter (fun symbol store ->
    let age = now -. store.last_update in

    (* Check if store is stale *)
    if age > stale_threshold then begin
      stores_to_remove := symbol :: !stores_to_remove
    end else begin
      (* For active stores, check if price maps are too large - use cached sizes for efficiency *)
      let trimmed = ref false in

      if store.bids_size > max_price_levels then begin
        let bids_map, bids_size = rebuild_map_from_top_levels store.bids true max_price_levels store.bids_size in
        store.bids <- bids_map;
        store.bids_size <- bids_size;
        invalidate_cache store;
        trimmed := true
      end;

      if store.asks_size > max_price_levels then begin
        let asks_map, asks_size = rebuild_map_from_top_levels store.asks false max_price_levels store.asks_size in
        store.asks <- asks_map;
        store.asks_size <- asks_size;
        invalidate_cache store;
        trimmed := true
      end;

      if !trimmed then
        trimmed_stores := symbol :: !trimmed_stores
    end
  ) stores;

  (* Remove stale and excess stores *)
  List.iter (fun symbol ->
    (match Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.find_opt stores symbol with
     | Some store -> RingBuffer.destroy store.buffer
     | None -> ());
    Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.remove stores symbol;
    Logging.debug_f ~section "Removed orderbook store for %s (stale or excess)" symbol
  ) !stores_to_remove;

  (* Log trimming actions *)
  if !trimmed_stores <> [] then
    Logging.debug_f ~section "Trimmed price levels for %d active stores: %s"
      (List.length !trimmed_stores) (String.concat ", " !trimmed_stores);

  let stores_removed = List.length !stores_to_remove in
  let stores_trimmed = List.length !trimmed_stores in
  let total_stores_after = Dio_memory_tracing.Memory_tracing.Tracked.SafeHashtbl.length stores in

  if stores_removed > 0 || stores_trimmed > 0 then
    Logging.info_f ~section "Orderbook cleanup: removed %d stores, trimmed %d active stores (%d -> %d total stores)"
      stores_removed stores_trimmed total_stores_before total_stores_after

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

  Logging.info_f ~section "Connecting to Kraken orderbook WebSocket...";
  Lwt_unix.getaddrinfo "ws.kraken.com" "443" [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let ip = match addresses with
    | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
        Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws.kraken.com"
  in
  let client = `TLS (`Hostname "ws.kraken.com", `IP ip, `Port 443) in
  let ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
  Logging.info_f ~section "Orderbook WebSocket established, subscribing...";
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

  (* Start periodic cleanup of stale data *)
  Lwt.async (fun () ->
    let rec cleanup_loop () =
      let%lwt () = Lwt_unix.sleep 120.0 in (* Clean up every 2 minutes *)
      prune_stale_data ();
      cleanup_loop ()
    in
    cleanup_loop ()
  );

  Lwt.return_unit