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

   Storage path: /app/data/accumulated_state.json (production),
   ./data/accumulated_state.json (development).

   File format:
   { "SYMBOL": { "reserved_base": float, "accumulated_profit": float,
     "last_fill_oid": string, "last_buy_fill_price": float,
     "last_sell_fill_price": float }, ... }
*)

let section = "state_persistence"

(** Base directory for state files. Resolves to /app/data in Docker, ./data locally. *)
let state_dir =
  if Sys.file_exists "/app" then "/app/data"
  else "data"

let state_file () =
  Filename.concat state_dir "accumulated_state.json"

(** Mutex guarding all file I/O for thread safety across domains. *)
let file_mutex = Mutex.create ()

(** Creates the state directory if it does not already exist. *)
let ensure_dir () =
  if not (Sys.file_exists state_dir) then begin
    try Sys.mkdir state_dir 0o755
    with Sys_error msg ->
      Logging.warn_f ~section "Could not create state dir %s: %s" state_dir msg
  end

(** Reads and parses the state file. Returns an empty assoc on missing or corrupt files. *)
let read_state_file () : Yojson.Basic.t =
  let path = state_file () in
  if Sys.file_exists path then
    try Yojson.Basic.from_file path
    with
    | Yojson.Json_error msg ->
        Logging.warn_f ~section "Corrupt state file %s: %s, starting fresh" path msg;
        `Assoc []
    | Sys_error msg ->
        Logging.warn_f ~section "Cannot read state file %s: %s" path msg;
        `Assoc []
  else
    `Assoc []

(** Extracts a float field from a symbol's JSON entry. Returns [default] if absent. *)
let get_float (json : Yojson.Basic.t) ~symbol ~field ~default =
  let open Yojson.Basic.Util in
  try
    json |> member symbol |> member field |> to_float
  with _ -> default

(** Extracts an optional float field from a symbol's JSON entry. Returns [None] if absent. *)
let get_float_opt (json : Yojson.Basic.t) ~symbol ~field =
  let open Yojson.Basic.Util in
  try
    Some (json |> member symbol |> member field |> to_float)
  with _ -> None

(** Extracts an optional string field from a symbol's JSON entry. Returns [None] if absent. *)
let get_string_opt (json : Yojson.Basic.t) ~symbol ~field =
  let open Yojson.Basic.Util in
  try
    Some (json |> member symbol |> member field |> to_string)
  with _ -> None

type symbol_state = {
  mutable reserved_base: float;
  mutable accumulated_profit: float;
  mutable last_fill_oid: string option;
  mutable last_buy_fill_price: float option;
  mutable last_sell_fill_price: float option;
}

(** Global cache of state fields per symbol to ensure O(1) thread-safe reads and survive strategy restarts. *)
let cache : (string, symbol_state) Hashtbl.t = Hashtbl.create 16
let cache_mutex = Mutex.create ()

(** Unsafely populates the in-memory cache from the JSON state file. Must be called under both cache_mutex and file_mutex. *)
let populate_cache_from_file_unsafe () =
  let json = read_state_file () in
  let open Yojson.Basic.Util in
  let entries = try json |> to_assoc with _ -> [] in
  List.iter (fun (symbol, _) ->
    let reserved_base = get_float json ~symbol ~field:"reserved_base" ~default:0.0 in
    let accumulated_profit = get_float json ~symbol ~field:"accumulated_profit" ~default:0.0 in
    let last_fill_oid = get_string_opt json ~symbol ~field:"last_fill_oid" in
    let last_buy_fill_price = get_float_opt json ~symbol ~field:"last_buy_fill_price" in
    let last_sell_fill_price = get_float_opt json ~symbol ~field:"last_sell_fill_price" in
    let state = {
      reserved_base;
      accumulated_profit;
      last_fill_oid;
      last_buy_fill_price;
      last_sell_fill_price;
    } in
    Hashtbl.replace cache symbol state
  ) entries

(** Ensures the symbol state exists in the in-memory cache, loading from disk on first access. *)
let ensure_symbol_in_cache ~symbol =
  Mutex.lock cache_mutex;
  let exists = Hashtbl.mem cache symbol in
  Mutex.unlock cache_mutex;
  if not exists then begin
    Mutex.lock file_mutex;
    Mutex.lock cache_mutex;
    if not (Hashtbl.mem cache symbol) then begin
      populate_cache_from_file_unsafe ();
      if not (Hashtbl.mem cache symbol) then begin
        let default_state = {
          reserved_base = 0.0;
          accumulated_profit = 0.0;
          last_fill_oid = None;
          last_buy_fill_price = None;
          last_sell_fill_price = None;
        } in
        Hashtbl.replace cache symbol default_state
      end
    end;
    Mutex.unlock cache_mutex;
    Mutex.unlock file_mutex
  end

(** Loads reserved_base for a symbol. Returns 0.0 if absent. Acquires cache_mutex. *)
let load_reserved_base ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.reserved_base in
  Mutex.unlock cache_mutex;
  if result > 0.0 then
    Logging.info_f ~section "Loaded reserved_base=%.8f for %s" result symbol;
  result

(** Loads accumulated_profit for a symbol. Returns 0.0 if absent. Acquires cache_mutex. *)
let load_accumulated_profit ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.accumulated_profit in
  Mutex.unlock cache_mutex;
  if result > 0.0 then
    Logging.info_f ~section "Loaded accumulated_profit=%.6f for %s" result symbol;
  result

(** Loads last_fill_oid for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_fill_oid ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_fill_oid in
  Mutex.unlock cache_mutex;
  (match result with
   | Some oid -> Logging.info_f ~section "Loaded last_fill_oid=%s for %s" oid symbol
   | None -> ());
  result

(** Loads last_buy_fill_price for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_buy_fill_price ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_buy_fill_price in
  Mutex.unlock cache_mutex;
  (match result with
   | Some price -> Logging.info_f ~section "Loaded last_buy_fill_price=%.8f for %s" price symbol
   | None -> ());
  result

(** Loads last_sell_fill_price for a symbol. Returns [None] if absent. Acquires cache_mutex. *)
let load_last_sell_fill_price ~symbol =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  let result = state.last_sell_fill_price in
  Mutex.unlock cache_mutex;
  (match result with
   | Some price -> Logging.info_f ~section "Loaded last_sell_fill_price=%.8f for %s" price symbol
   | None -> ());
  result

(** Helper function that performs the actual read-modify-write cycle on disk under file_mutex. *)
let write_to_disk ~symbol ~state () =
  Mutex.lock file_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock file_mutex) (fun () ->
    try
      ensure_dir ();
      let existing = read_state_file () in
      let open Yojson.Basic.Util in
      let entries = try existing |> to_assoc with _ -> [] in
      (* Construct required entry fields *)
      let base_fields = [
        ("reserved_base", `Float state.reserved_base);
        ("accumulated_profit", `Float state.accumulated_profit);
      ] in
      (* Construct optional entry fields if present *)
      let oid_field = match state.last_fill_oid with
        | Some oid -> [("last_fill_oid", `String oid)]
        | None -> []
      in
      let buy_price_field = match state.last_buy_fill_price with
        | Some price -> [("last_buy_fill_price", `Float price)]
        | None -> []
      in
      let sell_price_field = match state.last_sell_fill_price with
        | Some price -> [("last_sell_fill_price", `Float price)]
        | None -> []
      in
      let new_entry = `Assoc (base_fields @ oid_field @ buy_price_field @ sell_price_field) in
      let updated = List.filter (fun (k, _) -> k <> symbol) entries in
      let final = `Assoc ((symbol, new_entry) :: updated) in

      (* Atomic write via temp file and rename *)
      let path = state_file () in
      let tmp = path ^ ".tmp" in
      let oc = open_out tmp in
      Fun.protect ~finally:(fun () -> close_out_noerr oc) (fun () ->
        output_string oc (Yojson.Basic.pretty_to_string final);
        output_char oc '\n'
      );
      Sys.rename tmp path
    with exn ->
      Logging.warn_f ~section "Failed to persist state for %s: %s" symbol (Printexc.to_string exn)
  )

(** Persists state for a symbol. Required fields: reserved_base, accumulated_profit.
    Optional fields are updated in the cache when provided, and the entire state is
    written to disk synchronously. *)
let save ~symbol ~reserved_base ~accumulated_profit ?last_fill_oid ?last_buy_fill_price ?last_sell_fill_price () =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  state.reserved_base <- reserved_base;
  state.accumulated_profit <- accumulated_profit;
  if last_fill_oid <> None then state.last_fill_oid <- last_fill_oid;
  if last_buy_fill_price <> None then state.last_buy_fill_price <- last_buy_fill_price;
  if last_sell_fill_price <> None then state.last_sell_fill_price <- last_sell_fill_price;
  let snapshot = {
    reserved_base = state.reserved_base;
    accumulated_profit = state.accumulated_profit;
    last_fill_oid = state.last_fill_oid;
    last_buy_fill_price = state.last_buy_fill_price;
    last_sell_fill_price = state.last_sell_fill_price;
  } in
  Mutex.unlock cache_mutex;
  write_to_disk ~symbol ~state:snapshot ()

let save_queue : (string * symbol_state) Queue.t = Queue.create ()
let save_queue_mutex = Mutex.create ()
let save_cond = Condition.create ()

let rec background_worker () =
  Mutex.lock save_queue_mutex;
  while Queue.is_empty save_queue do
    Condition.wait save_cond save_queue_mutex
  done;
  let (symbol, snapshot) = Queue.pop save_queue in
  Mutex.unlock save_queue_mutex;

  write_to_disk ~symbol ~state:snapshot ();

  background_worker ()

let () = ignore (Domain.spawn background_worker)

let save_async ~symbol ~reserved_base ~accumulated_profit ?last_fill_oid ?last_buy_fill_price ?last_sell_fill_price () =
  ensure_symbol_in_cache ~symbol;
  Mutex.lock cache_mutex;
  let state = Hashtbl.find cache symbol in
  state.reserved_base <- reserved_base;
  state.accumulated_profit <- accumulated_profit;
  if last_fill_oid <> None then state.last_fill_oid <- last_fill_oid;
  if last_buy_fill_price <> None then state.last_buy_fill_price <- last_buy_fill_price;
  if last_sell_fill_price <> None then state.last_sell_fill_price <- last_sell_fill_price;
  let snapshot = {
    reserved_base = state.reserved_base;
    accumulated_profit = state.accumulated_profit;
    last_fill_oid = state.last_fill_oid;
    last_buy_fill_price = state.last_buy_fill_price;
    last_sell_fill_price = state.last_sell_fill_price;
  } in
  Mutex.unlock cache_mutex;

  Mutex.lock save_queue_mutex;
  Queue.push (symbol, snapshot) save_queue;
  Condition.signal save_cond;
  Mutex.unlock save_queue_mutex
