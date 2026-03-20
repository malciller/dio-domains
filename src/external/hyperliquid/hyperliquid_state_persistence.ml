(**
   Persistent state for strategy accumulation tracking.

   Saves/loads reserved_base (base asset accumulated via sell_mult)
   and accumulated_profit (USDC realized PnL) to a JSON file so
   values survive Docker container restarts.

   File: /app/data/accumulated_state.json (or ./data/ in dev)
   Format: { "SYMBOL": { "reserved_base": float, "accumulated_profit": float }, ... }
*)

let section = "state_persistence"

(** State file path — /app/data in Docker, ./data locally *)
let state_dir =
  if Sys.file_exists "/app" then "/app/data"
  else "data"

let state_file () =
  Filename.concat state_dir "accumulated_state.json"

(** File-level mutex for multi-domain safety *)
let file_mutex = Mutex.create ()

(** Ensure the data directory exists *)
let ensure_dir () =
  if not (Sys.file_exists state_dir) then begin
    try Sys.mkdir state_dir 0o755
    with Sys_error msg ->
      Logging.warn_f ~section "Could not create state dir %s: %s" state_dir msg
  end

(** Read the entire JSON file, returning a Yojson assoc *)
let read_state_file () : Yojson.Basic.t =
  let path = state_file () in
  if Sys.file_exists path then
    try Yojson.Basic.from_file path
    with
    | Yojson.Json_error msg ->
        Logging.warn_f ~section "Corrupt state file %s: %s — starting fresh" path msg;
        `Assoc []
    | Sys_error msg ->
        Logging.warn_f ~section "Cannot read state file %s: %s" path msg;
        `Assoc []
  else
    `Assoc []

(** Extract a float field from a symbol's JSON entry *)
let get_float (json : Yojson.Basic.t) ~symbol ~field ~default =
  let open Yojson.Basic.Util in
  try
    json |> member symbol |> member field |> to_float
  with _ -> default

(** Load reserved_base for a symbol. Returns 0.0 if missing. *)
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

(** Load accumulated_profit for a symbol. Returns 0.0 if missing. *)
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

(** Save both reserved_base and accumulated_profit for a symbol.
    Reads existing file, updates the symbol's entry, writes atomically. *)
let save ~symbol ~reserved_base ~accumulated_profit =
  Mutex.lock file_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock file_mutex) (fun () ->
    try
      ensure_dir ();
      let existing = read_state_file () in
      let open Yojson.Basic.Util in
      let entries = try existing |> to_assoc with _ -> [] in
      (* Update this symbol's entry, preserve others *)
      let new_entry = `Assoc [
        ("reserved_base", `Float reserved_base);
        ("accumulated_profit", `Float accumulated_profit);
      ] in
      let updated = List.filter (fun (k, _) -> k <> symbol) entries in
      let final = `Assoc ((symbol, new_entry) :: updated) in

      (* Atomic write: temp file + rename *)
      let path = state_file () in
      let tmp = path ^ ".tmp" in
      let oc = open_out tmp in
      Fun.protect ~finally:(fun () -> close_out_noerr oc) (fun () ->
        output_string oc (Yojson.Basic.pretty_to_string final);
        output_char oc '\n'
      );
      Sys.rename tmp path;
      Logging.debug_f ~section "Persisted state for %s: reserved_base=%.8f, accumulated_profit=%.6f"
        symbol reserved_base accumulated_profit
    with exn ->
      Logging.warn_f ~section "Failed to persist state for %s: %s" symbol (Printexc.to_string exn)
  )
