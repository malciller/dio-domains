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

(** Loads reserved_base for a symbol. Returns 0.0 if absent. Acquires file_mutex. *)
let load_reserved_base ~symbol =
  Mutex.lock file_mutex;
  let result =
    try get_float (read_state_file ()) ~symbol ~field:"reserved_base" ~default:0.0
    with _ -> 0.0
  in
  Mutex.unlock file_mutex;
  if result > 0.0 then
    Logging.info_f ~section "Loaded reserved_base=%.8f for %s" result symbol;
  result

(** Loads accumulated_profit for a symbol. Returns 0.0 if absent. Acquires file_mutex. *)
let load_accumulated_profit ~symbol =
  Mutex.lock file_mutex;
  let result =
    try get_float (read_state_file ()) ~symbol ~field:"accumulated_profit" ~default:0.0
    with _ -> 0.0
  in
  Mutex.unlock file_mutex;
  if result > 0.0 then
    Logging.info_f ~section "Loaded accumulated_profit=%.6f for %s" result symbol;
  result

(** Loads last_fill_oid for a symbol. Returns [None] if absent. Acquires file_mutex. *)
let load_last_fill_oid ~symbol =
  Mutex.lock file_mutex;
  let result =
    try get_string_opt (read_state_file ()) ~symbol ~field:"last_fill_oid"
    with _ -> None
  in
  Mutex.unlock file_mutex;
  (match result with
   | Some oid -> Logging.info_f ~section "Loaded last_fill_oid=%s for %s" oid symbol
   | None -> ());
  result

(** Loads last_buy_fill_price for a symbol. Returns [None] if absent. Acquires file_mutex. *)
let load_last_buy_fill_price ~symbol =
  Mutex.lock file_mutex;
  let result =
    try get_float_opt (read_state_file ()) ~symbol ~field:"last_buy_fill_price"
    with _ -> None
  in
  Mutex.unlock file_mutex;
  (match result with
   | Some price -> Logging.info_f ~section "Loaded last_buy_fill_price=%.8f for %s" price symbol
   | None -> ());
  result

(** Loads last_sell_fill_price for a symbol. Returns [None] if absent. Acquires file_mutex. *)
let load_last_sell_fill_price ~symbol =
  Mutex.lock file_mutex;
  let result =
    try get_float_opt (read_state_file ()) ~symbol ~field:"last_sell_fill_price"
    with _ -> None
  in
  Mutex.unlock file_mutex;
  (match result with
   | Some price -> Logging.info_f ~section "Loaded last_sell_fill_price=%.8f for %s" price symbol
   | None -> ());
  result

(** Persists state for a symbol. Required fields: reserved_base, accumulated_profit.
    Optional fields (last_fill_oid, last_buy_fill_price, last_sell_fill_price) are
    preserved from disk when not explicitly provided. Performs a read-modify-write
    cycle under file_mutex with atomic rename to prevent partial writes. *)
let save ~symbol ~reserved_base ~accumulated_profit ?last_fill_oid ?last_buy_fill_price ?last_sell_fill_price () =
  Mutex.lock file_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock file_mutex) (fun () ->
    try
      ensure_dir ();
      let existing = read_state_file () in
      let open Yojson.Basic.Util in
      let entries = try existing |> to_assoc with _ -> [] in
      (* Construct required entry fields *)
      let base_fields = [
        ("reserved_base", `Float reserved_base);
        ("accumulated_profit", `Float accumulated_profit);
      ] in
      (* Retain existing last_fill_oid when not explicitly supplied *)
      let oid_field = match last_fill_oid with
        | Some oid -> [("last_fill_oid", `String oid)]
        | None ->
            (* Preserve value from disk if present *)
            let existing_oid = get_string_opt existing ~symbol ~field:"last_fill_oid" in
            (match existing_oid with
             | Some oid -> [("last_fill_oid", `String oid)]
             | None -> [])
      in
      (* Retain existing last_buy_fill_price when not explicitly supplied *)
      let buy_price_field = match last_buy_fill_price with
        | Some price -> [("last_buy_fill_price", `Float price)]
        | None ->
            let existing_price = get_float_opt existing ~symbol ~field:"last_buy_fill_price" in
            (match existing_price with
             | Some price -> [("last_buy_fill_price", `Float price)]
             | None -> [])
      in
      (* Retain existing last_sell_fill_price when not explicitly supplied *)
      let sell_price_field = match last_sell_fill_price with
        | Some price -> [("last_sell_fill_price", `Float price)]
        | None ->
            let existing_price = get_float_opt existing ~symbol ~field:"last_sell_fill_price" in
            (match existing_price with
             | Some price -> [("last_sell_fill_price", `Float price)]
             | None -> [])
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
      Sys.rename tmp path;
      Logging.debug_f ~section "Persisted state for %s: reserved_base=%.8f, accumulated_profit=%.6f, last_fill_oid=%s, last_buy_fill_price=%s"
        symbol reserved_base accumulated_profit
        (match last_fill_oid with Some o -> o | None -> "(unchanged)")
        (match last_buy_fill_price with Some p -> Printf.sprintf "%.8f" p | None -> "(unchanged)")
    with exn ->
      Logging.warn_f ~section "Failed to persist state for %s: %s" symbol (Printexc.to_string exn)
  )

type save_request = {
  symbol: string;
  reserved_base: float;
  accumulated_profit: float;
  last_fill_oid: string option;
  last_buy_fill_price: float option;
  last_sell_fill_price: float option;
}

let save_queue : save_request Queue.t = Queue.create ()
let save_queue_mutex = Mutex.create ()
let save_cond = Condition.create ()

let rec background_worker () =
  Mutex.lock save_queue_mutex;
  while Queue.is_empty save_queue do
    Condition.wait save_cond save_queue_mutex
  done;
  let req = Queue.pop save_queue in
  Mutex.unlock save_queue_mutex;

  save ~symbol:req.symbol
       ~reserved_base:req.reserved_base
       ~accumulated_profit:req.accumulated_profit
       ?last_fill_oid:req.last_fill_oid
       ?last_buy_fill_price:req.last_buy_fill_price
       ?last_sell_fill_price:req.last_sell_fill_price ();

  background_worker ()

let () = ignore (Domain.spawn background_worker)

let save_async ~symbol ~reserved_base ~accumulated_profit ?last_fill_oid ?last_buy_fill_price ?last_sell_fill_price () =
  Mutex.lock save_queue_mutex;
  Queue.push { symbol; reserved_base; accumulated_profit; last_fill_oid; last_buy_fill_price; last_sell_fill_price } save_queue;
  Condition.signal save_cond;
  Mutex.unlock save_queue_mutex
