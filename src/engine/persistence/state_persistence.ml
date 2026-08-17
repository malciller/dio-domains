(**
   Persistent state management for strategy accumulation tracking.

   Serializes and deserializes per-symbol state fields to a JSON file
   so that values survive process and container restarts.

   Persisted fields per symbol:
   - reserved_base: base asset quantity accumulated via sell_mult.
   - accumulated_profit: realized PnL denominated in USDC.
   - last_fill_oid: order ID of the most recent fill that updated profit.
   - last_buy_fill_price: fill price of the most recent buy, used to
     compute profit on the subsequent sell fill.
   - last_sell_fill_price: fill price of the most recent sell.
   - last_buy_fill_qty: filled quantity of the most recent buy.
   - last_sell_fill_qty: filled quantity of the most recent sell.

   Storage path: /app/data/accumulated_state.json (production),
   ./data/accumulated_state.json (development).

   File format:
   { "SYMBOL": { "reserved_base": float, "accumulated_profit": float,
     "last_fill_oid": string, "last_buy_fill_price": float,
     "last_sell_fill_price": float, "last_buy_fill_qty": float,
     "last_sell_fill_qty": float }, ... }
*)

let section = "state_persistence"

(** Base directory for state files. Resolves to /app/data in Docker, ./data locally. *)
let state_dir = if Sys.file_exists "/app" then "/app/data" else "data"

let state_file () = Filename.concat state_dir "accumulated_state.json"

(** Mutex guarding all file I/O for thread safety across domains. *)
let file_mutex = Mutex.create ()

(** In-memory mirror of the state file, guarded by [file_mutex]. Parsed lazily
    on the first read/write and kept in sync on every save, so a save mutates
    the cached tree and re-serializes it instead of re-reading the disk and
    rebuilding the whole JSON tree (every symbol, every Alpaca sell level) on
    every write. The background persistence domain's allocations used to
    trigger major-GC stop-the-world pauses in the strategy domains. *)
let file_tree : Yojson.Basic.t option ref = ref None

(** Creates the state directory if it does not already exist. *)
let ensure_dir () =
  if not (Sys.file_exists state_dir)
  then (
    try Sys.mkdir state_dir 0o755 with
    | Sys_error msg ->
      Logging.warn_f ~section "Could not create state dir %s: %s" state_dir msg)
;;

(** Reads and parses the state file. Returns an empty assoc on missing or corrupt files.
    Must be called under [file_mutex]. Caches the parsed tree in [file_tree] so
    subsequent reads (and the saves built on top of it) skip the disk entirely. *)
let read_state_file () : Yojson.Basic.t =
  let path = state_file () in
  if Sys.file_exists path
  then (
    try
      let tree = Yojson.Basic.from_file path in
      file_tree := Some tree;
      tree
    with
    | Yojson.Json_error msg ->
      Logging.warn_f ~section "Corrupt state file %s: %s, starting fresh" path msg;
      (* Deliberately NOT cached: a transient error must not pin an empty tree,
         or the next save would clobber a good file with a partial one. *)
      `Assoc []
    | Sys_error msg ->
      Logging.warn_f ~section "Cannot read state file %s: %s" path msg;
      `Assoc [])
  else `Assoc []
;;

(** Extracts a float field from a symbol's JSON entry. Returns [default] if absent. *)
let get_float (json : Yojson.Basic.t) ~symbol ~field ~default =
  let open Yojson.Basic.Util in
  try json |> member symbol |> member field |> to_float with
  | _ -> default
;;

(** Extracts an optional float field from a symbol's JSON entry. Returns [None] if absent. *)
let get_float_opt (json : Yojson.Basic.t) ~symbol ~field =
  let open Yojson.Basic.Util in
  try Some (json |> member symbol |> member field |> to_float) with
  | _ -> None
;;

(** Extracts an optional string field from a symbol's JSON entry. Returns [None] if absent. *)
let get_string_opt (json : Yojson.Basic.t) ~symbol ~field =
  let open Yojson.Basic.Util in
  try Some (json |> member symbol |> member field |> to_string) with
  | _ -> None
;;

type symbol_state =
  { mutable reserved_base : float
  ; mutable accumulated_profit : float
  ; mutable last_fill_oid : string option
  ; mutable last_buy_fill_price : float option
  ; mutable last_sell_fill_price : float option
  ; mutable last_buy_fill_qty : float option
  ; mutable last_sell_fill_qty : float option
  ; mutable persisted_sell_levels : (float * float) list
  }

(** Global cache of state fields per symbol to ensure O(1) thread-safe reads and survive strategy restarts. *)
let cache : (string, symbol_state) Hashtbl.t = Hashtbl.create 16

let cache_mutex = Mutex.create ()

(** Unsafely populates the in-memory cache from the JSON state file. Must be called under both cache_mutex and file_mutex. *)
let populate_cache_from_file_unsafe () =
  let json = read_state_file () in
  let open Yojson.Basic.Util in
  let entries =
    try json |> to_assoc with
    | _ -> []
  in
  List.iter
    (fun (symbol, _) ->
       let reserved_base = get_float json ~symbol ~field:"reserved_base" ~default:0.0 in
       let accumulated_profit =
         get_float json ~symbol ~field:"accumulated_profit" ~default:0.0
       in
       let last_fill_oid = get_string_opt json ~symbol ~field:"last_fill_oid" in
       let last_buy_fill_price =
         get_float_opt json ~symbol ~field:"last_buy_fill_price"
       in
       let last_sell_fill_price =
         get_float_opt json ~symbol ~field:"last_sell_fill_price"
       in
       let last_buy_fill_qty = get_float_opt json ~symbol ~field:"last_buy_fill_qty" in
       let last_sell_fill_qty = get_float_opt json ~symbol ~field:"last_sell_fill_qty" in
       let persisted_sell_levels =
         try
           json
           |> member symbol
           |> member "sell_levels"
           |> to_list
           |> List.filter_map (fun item ->
             try
               let price, qty =
                 match item with
                 (* Compact form: [price, qty]. *)
                 | `List [ p; q ] -> to_float p, to_float q
                 (* Legacy form: {"price": p, "qty": q}. *)
                 | _ ->
                   item |> member "price" |> to_float, item |> member "qty" |> to_float
               in
               if (not (Float.is_nan price))
                  && price > 0.0
                  && (not (Float.is_nan qty))
                  && qty >= 1e-5
               then Some (price, qty)
               else None
             with
             | _ -> None)
         with
         | _ -> []
       in
       let state =
         { reserved_base
         ; accumulated_profit
         ; last_fill_oid
         ; last_buy_fill_price
         ; last_sell_fill_price
         ; last_buy_fill_qty
         ; last_sell_fill_qty
         ; persisted_sell_levels
         }
       in
       Hashtbl.replace cache symbol state)
    entries
;;

(** Ensures the symbol state exists in the in-memory cache, loading from disk on first access. *)
let ensure_symbol_in_cache ~symbol =
  Mutex.lock cache_mutex;
  let exists = Hashtbl.mem cache symbol in
  Mutex.unlock cache_mutex;
  if not exists
  then (
    Mutex.lock file_mutex;
    Mutex.lock cache_mutex;
    if not (Hashtbl.mem cache symbol)
    then (
      populate_cache_from_file_unsafe ();
      if not (Hashtbl.mem cache symbol)
      then (
        let default_state =
          { reserved_base = 0.0
          ; accumulated_profit = 0.0
          ; last_fill_oid = None
          ; last_buy_fill_price = None
          ; last_sell_fill_price = None
          ; last_buy_fill_qty = None
          ; last_sell_fill_qty = None
          ; persisted_sell_levels = []
          }
        in
        Hashtbl.replace cache symbol default_state));
    Mutex.unlock cache_mutex;
    Mutex.unlock file_mutex)
;;

(** Loads reserved_base for a symbol. Returns 0.0 if absent. Acquires cache_mutex. *)
let load_reserved_base ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.reserved_base in
  Mutex.unlock cache_mutex;
  if result > 0.0
  then Logging.debug_f ~section "Loaded reserved_base=%.8f for %s" result symbol;
  result
;;

(** Loads accumulated_profit for a symbol. Returns 0.0 if absent. Acquires cache_mutex. *)
let load_accumulated_profit ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.accumulated_profit in
  Mutex.unlock cache_mutex;
  if result > 0.0
  then Logging.debug_f ~section "Loaded accumulated_profit=%.6f for %s" result symbol;
  result
;;

(** Loads last_fill_oid for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_fill_oid ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_fill_oid in
  Mutex.unlock cache_mutex;
  (match result with
   | Some oid -> Logging.debug_f ~section "Loaded last_fill_oid=%s for %s" oid symbol
   | None -> ());
  result
;;

(** Loads last_buy_fill_price for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_buy_fill_price ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_buy_fill_price in
  Mutex.unlock cache_mutex;
  (match result with
   | Some price ->
     Logging.debug_f ~section "Loaded last_buy_fill_price=%.8f for %s" price symbol
   | None -> ());
  result
;;

(** Loads last_sell_fill_price for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_sell_fill_price ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_sell_fill_price in
  Mutex.unlock cache_mutex;
  (match result with
   | Some price ->
     Logging.debug_f ~section "Loaded last_sell_fill_price=%.8f for %s" price symbol
   | None -> ());
  result
;;

(** Loads last_buy_fill_qty for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_buy_fill_qty ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_buy_fill_qty in
  Mutex.unlock cache_mutex;
  (match result with
   | Some qty ->
     Logging.debug_f ~section "Loaded last_buy_fill_qty=%.8f for %s" qty symbol
   | None -> ());
  result
;;

(** Loads last_sell_fill_qty for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_sell_fill_qty ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_sell_fill_qty in
  Mutex.unlock cache_mutex;
  (match result with
   | Some qty ->
     Logging.debug_f ~section "Loaded last_sell_fill_qty=%.8f for %s" qty symbol
   | None -> ());
  result
;;

(** Loads persisted_sell_levels for a symbol. Returns [] if absent. Acquires cache_mutex. *)
let load_persisted_sell_levels ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result =
    List.sort (fun (p1, _) (p2, _) -> Float.compare p2 p1) state.persisted_sell_levels
  in
  Mutex.unlock cache_mutex;
  if result <> []
  then
    Logging.debug_f
      ~section
      "Loaded %d persisted_sell_levels for %s"
      (List.length result)
      symbol;
  result
;;

(** Builds the JSON entry (all persisted fields) for [state]. *)
let symbol_entry_of_state state =
  (* Construct required entry fields *)
  let base_fields =
    [ "reserved_base", `Float state.reserved_base
    ; "accumulated_profit", `Float state.accumulated_profit
    ]
  in
  (* Construct optional entry fields if present *)
  let oid_field =
    match state.last_fill_oid with
    | Some oid -> [ "last_fill_oid", `String oid ]
    | None -> []
  in
  let buy_price_field =
    match state.last_buy_fill_price with
    | Some price -> [ "last_buy_fill_price", `Float price ]
    | None -> []
  in
  let sell_price_field =
    match state.last_sell_fill_price with
    | Some price -> [ "last_sell_fill_price", `Float price ]
    | None -> []
  in
  let buy_qty_field =
    match state.last_buy_fill_qty with
    | Some qty -> [ "last_buy_fill_qty", `Float qty ]
    | None -> []
  in
  let sell_qty_field =
    match state.last_sell_fill_qty with
    | Some qty -> [ "last_sell_fill_qty", `Float qty ]
    | None -> []
  in
  let sell_levels_field =
    if state.persisted_sell_levels <> []
    then (
      (* Compact form: [ [price, qty], ... ] (the reader also accepts the
         legacy [ {"price": p, "qty": q}, ... ] form). Keeps the file small
         and the per-save serialization fast for large Alpaca sell grids -
         the verbose object form added two assoc objects + four strings per
         level on every save. *)
      let list_json =
        `List
          (List.map
             (fun (price, qty) -> `List [ `Float price; `Float qty ])
             state.persisted_sell_levels)
      in
      [ "sell_levels", list_json ])
    else []
  in
  `Assoc
    (base_fields
     @ oid_field
     @ buy_price_field
     @ sell_price_field
     @ buy_qty_field
     @ sell_qty_field
     @ sell_levels_field)
;;

(** Helper function that performs the actual read-modify-write cycle on disk under file_mutex. *)
let write_to_disk ~symbol ~state () =
  Mutex.lock file_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock file_mutex)
    (fun () ->
       try
         ensure_dir ();
         let tree =
           match !file_tree with
           | Some t -> t
           | None -> read_state_file ()
         in
         let open Yojson.Basic.Util in
         let entries =
           try tree |> to_assoc with
           | _ -> []
         in
         let new_entry = symbol_entry_of_state state in
         let updated = List.filter (fun (k, _) -> k <> symbol) entries in
         let final = `Assoc ((symbol, new_entry) :: updated) in
         (* Atomic write via temp file and rename *)
         let path = state_file () in
         let tmp = path ^ ".tmp" in
         let oc = open_out tmp in
         Fun.protect
           ~finally:(fun () -> close_out_noerr oc)
           (fun () ->
              output_string oc (Yojson.Basic.pretty_to_string final);
              output_char oc '\n');
         Sys.rename tmp path;
         (* Keep the in-memory mirror in sync: the next save serializes this
            tree directly without re-reading the disk. *)
         file_tree := Some final
       with
       | exn ->
         Logging.warn_f
           ~section
           "Failed to persist state for %s: %s"
           symbol
           (Printexc.to_string exn))
;;

(** Persists state for a symbol. Required fields: reserved_base, accumulated_profit.
    Optional fields are updated in the cache when provided, and the entire state is
    written to disk synchronously. *)
let save
      ~symbol
      ~reserved_base
      ~accumulated_profit
      ~last_fill_oid
      ~last_buy_fill_price
      ~last_sell_fill_price
      ?last_buy_fill_qty
      ?last_sell_fill_qty
      ?persisted_sell_levels
      ()
  =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  state.reserved_base <- reserved_base;
  state.accumulated_profit <- accumulated_profit;
  if last_fill_oid <> None then state.last_fill_oid <- last_fill_oid;
  state.last_buy_fill_price <- last_buy_fill_price;
  state.last_sell_fill_price <- last_sell_fill_price;
  Option.iter (fun qty -> state.last_buy_fill_qty <- qty) last_buy_fill_qty;
  Option.iter (fun qty -> state.last_sell_fill_qty <- qty) last_sell_fill_qty;
  Option.iter (fun levels -> state.persisted_sell_levels <- levels) persisted_sell_levels;
  let snapshot =
    { reserved_base = state.reserved_base
    ; accumulated_profit = state.accumulated_profit
    ; last_fill_oid = state.last_fill_oid
    ; last_buy_fill_price = state.last_buy_fill_price
    ; last_sell_fill_price = state.last_sell_fill_price
    ; last_buy_fill_qty = state.last_buy_fill_qty
    ; last_sell_fill_qty = state.last_sell_fill_qty
    ; persisted_sell_levels = state.persisted_sell_levels
    }
  in
  Mutex.unlock cache_mutex;
  write_to_disk ~symbol ~state:snapshot ()
;;

(** Pending async saves keyed by symbol. Only the LATEST snapshot per symbol
    matters (every snapshot carries the symbol's full state), so this is a
    table that [save_async] overwrites; the background worker drains the whole
    table and writes each symbol once. This collapses the redundant writes an
    Alpaca grid produces when [persistence_dirty] stays set cycle after cycle:
    a FIFO queue would otherwise enqueue one full-file rewrite per cycle per
    symbol, and the worker would grind through them all. *)
let save_queue : (string, symbol_state) Hashtbl.t = Hashtbl.create 16

let save_queue_mutex = Mutex.create ()
let save_cond = Condition.create ()

let rec background_worker () =
  Mutex.lock save_queue_mutex;
  while Hashtbl.length save_queue = 0 do
    Condition.wait save_cond save_queue_mutex
  done;
  (* Swap out the pending set while holding the lock; [save_async] keeps
     enqueueing into the now-empty table while this domain does the I/O. *)
  let pending = Hashtbl.copy save_queue in
  Hashtbl.reset save_queue;
  Mutex.unlock save_queue_mutex;
  Hashtbl.iter (fun symbol snapshot -> write_to_disk ~symbol ~state:snapshot ()) pending;
  background_worker ()
;;

let () = ignore (Domain.spawn background_worker)

let save_async
      ~symbol
      ~reserved_base
      ~accumulated_profit
      ~last_fill_oid
      ~last_buy_fill_price
      ~last_sell_fill_price
      ?last_buy_fill_qty
      ?last_sell_fill_qty
      ?persisted_sell_levels
      ()
  =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  state.reserved_base <- reserved_base;
  state.accumulated_profit <- accumulated_profit;
  if last_fill_oid <> None then state.last_fill_oid <- last_fill_oid;
  state.last_buy_fill_price <- last_buy_fill_price;
  state.last_sell_fill_price <- last_sell_fill_price;
  Option.iter (fun qty -> state.last_buy_fill_qty <- qty) last_buy_fill_qty;
  Option.iter (fun qty -> state.last_sell_fill_qty <- qty) last_sell_fill_qty;
  Option.iter (fun levels -> state.persisted_sell_levels <- levels) persisted_sell_levels;
  let snapshot =
    { reserved_base = state.reserved_base
    ; accumulated_profit = state.accumulated_profit
    ; last_fill_oid = state.last_fill_oid
    ; last_buy_fill_price = state.last_buy_fill_price
    ; last_sell_fill_price = state.last_sell_fill_price
    ; last_buy_fill_qty = state.last_buy_fill_qty
    ; last_sell_fill_qty = state.last_sell_fill_qty
    ; persisted_sell_levels = state.persisted_sell_levels
    }
  in
  Mutex.unlock cache_mutex;
  Mutex.lock save_queue_mutex;
  Hashtbl.replace save_queue symbol snapshot;
  Condition.signal save_cond;
  Mutex.unlock save_queue_mutex
;;
