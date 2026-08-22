(** Shared persistence infrastructure for the split persistence stores.

    One [t] instance owns ONE data file. It provides:
    - directory resolution (/app/data in Docker, ./data locally)
    - lazy disk read + in-memory JSON tree mirror (saves re-serialize the
      cached tree instead of re-reading the disk - the same pattern that
      keeps the background writer's allocations from triggering major-GC
      stop-the-world pauses in strategy domains)
    - per-store mutex, O(1) cached reads, latest-wins coalescing async saves
      drained by a dedicated background domain
    - hardened atomic writes: temp file -> flush -> fsync -> rename
    - corrupt-file backup (<name>.corrupt.<ts>) instead of silent discard

    No domain logic lives here: stores supply [parse]/[serialize] for their
    own value type and register a migration hook for legacy entries via
    [register_migrate_hook]; [migrate_if_legacy] detects the legacy flat
    symbol-keyed accumulated_state.json and fans each entry out to the
    registered hooks. *)

let section = "persistence_orchestrator"

(** Base directory for state files. Resolves to /app/data in Docker, ./data
    locally; DIO_DATA_DIR overrides (used by tests for hermetic fixtures).
    Computed per call so tests can redirect before touching a store. *)
let state_dir () =
  match Sys.getenv_opt "DIO_DATA_DIR" with
  | Some dir -> dir
  | None -> if Sys.file_exists "/app" then "/app/data" else "data"
;;

let ensure_dir () =
  let dir = state_dir () in
  if not (Sys.file_exists dir)
  then (
    try Sys.mkdir dir 0o755 with
    | Sys_error msg -> Logging.warn_f ~section "Could not create state dir %s: %s" dir msg)
;;

type 'a t =
  { filename : string
  ; parse : Yojson.Basic.t -> (string * 'a) list
  ; serialize : 'a -> Yojson.Basic.t
  ; file_mutex : Mutex.t
  ; mutable file_tree : Yojson.Basic.t option
  ; cache : (string, 'a) Hashtbl.t
  ; cache_mutex : Mutex.t
  ; save_queue : (string, 'a) Hashtbl.t
  ; save_queue_mutex : Mutex.t
  ; save_cond : Condition.t
  }

let file_path t = Filename.concat (state_dir ()) t.filename

(** Reads and parses the store's data file. Returns an empty assoc on missing
    or unreadable files; on CORRUPT JSON the bad file is backed up as
    <name>.corrupt.<ts> and an empty assoc is returned (the data is retained
    on disk for manual recovery, never silently discarded). Must be called
    under [t.file_mutex]. *)
let read_file_unsafe t : Yojson.Basic.t =
  let path = file_path t in
  if Sys.file_exists path
  then (
    try
      let tree = Yojson.Basic.from_file path in
      t.file_tree <- Some tree;
      tree
    with
    | Yojson.Json_error msg ->
      let backup = Printf.sprintf "%s.corrupt.%d" path (int_of_float (Unix.time ())) in
      Logging.warn_f
        ~section
        "Corrupt state file %s: %s; backing up to %s and starting fresh"
        path
        msg
        backup;
      (try Sys.rename path backup with
       | Sys_error rename_msg ->
         Logging.warn_f ~section "Could not back up corrupt file %s: %s" path rename_msg);
      `Assoc []
    | Sys_error msg ->
      Logging.warn_f ~section "Cannot read state file %s: %s" path msg;
      `Assoc [])
  else `Assoc []
;;

(** Atomic write: temp file -> flush -> fsync -> rename. Keeps the in-memory
    tree mirror in sync so the next save serializes directly without re-reading
    the disk. Must be called under [t.file_mutex]. *)
let write_file_unsafe t tree =
  ensure_dir ();
  let path = file_path t in
  let tmp = path ^ ".tmp" in
  let oc = open_out tmp in
  Fun.protect
    ~finally:(fun () -> close_out_noerr oc)
    (fun () ->
       output_string oc (Yojson.Basic.pretty_to_string tree);
       output_char oc '\n';
       flush oc;
       (* fsync the temp file before rename so a crash after rename cannot
          leave a truncated/empty target (power-loss durability). *)
       try Unix.fsync (Unix.descr_of_out_channel oc) with
       | _ -> ());
  Sys.rename tmp path;
  t.file_tree <- Some tree
;;

(** Read-modify-write of one key's entry under file_mutex. *)
let update_entry t key entry_json =
  Mutex.lock t.file_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock t.file_mutex)
    (fun () ->
       try
         let tree =
           match t.file_tree with
           | Some tree -> tree
           | None -> read_file_unsafe t
         in
         let open Yojson.Basic.Util in
         let entries =
           try tree |> to_assoc with
           | _ -> []
         in
         let updated = List.filter (fun (k, _) -> k <> key) entries in
         write_file_unsafe t (`Assoc ((key, entry_json) :: updated))
       with
       | exn ->
         Logging.warn_f
           ~section
           "Failed to persist %s entry %s: %s"
           t.filename
           key
           (Printexc.to_string exn))
;;

(** Ensures the whole file is parsed into the per-key cache (lazy first read). Must be called under both mutexes in the caller-chosen order. *)
let populate_cache_unsafe t =
  let tree = read_file_unsafe t in
  List.iter (fun (k, v) -> Hashtbl.replace t.cache k v) (t.parse tree)
;;

let load t ~key =
  Mutex.lock t.cache_mutex;
  match Hashtbl.find_opt t.cache key with
  | Some v ->
    Mutex.unlock t.cache_mutex;
    Some v
  | None ->
    Mutex.unlock t.cache_mutex;
    Mutex.lock t.file_mutex;
    Mutex.lock t.cache_mutex;
    let result =
      if Hashtbl.mem t.cache key
      then Hashtbl.find_opt t.cache key
      else (
        populate_cache_unsafe t;
        Hashtbl.find_opt t.cache key)
    in
    Mutex.unlock t.cache_mutex;
    Mutex.unlock t.file_mutex;
    result
;;

let put t ~key value =
  Mutex.lock t.cache_mutex;
  Hashtbl.replace t.cache key value;
  Mutex.unlock t.cache_mutex;
  update_entry t key (t.serialize value)
;;

(** Coalesced async save: latest-wins per key. Only the LATEST snapshot per
    key matters (each snapshot carries the full state), so the queue is a
    table that overwrites; the worker drains the whole table and writes each
    key once. This collapses redundant full-file rewrites when a hot loop
    marks the store dirty cycle after cycle. *)
let rec background_worker t () =
  Mutex.lock t.save_queue_mutex;
  while Hashtbl.length t.save_queue = 0 do
    Condition.wait t.save_cond t.save_queue_mutex
  done;
  let pending = Hashtbl.copy t.save_queue in
  Hashtbl.reset t.save_queue;
  Mutex.unlock t.save_queue_mutex;
  Mutex.lock t.cache_mutex;
  Hashtbl.iter (fun k v -> Hashtbl.replace t.cache k v) pending;
  Mutex.unlock t.cache_mutex;
  Hashtbl.iter (fun k v -> update_entry t k (t.serialize v)) pending;
  background_worker t ()
;;

let put_async t ~key value =
  (* Update the read cache immediately so a load after an async save observes
     the latest value even before the background writer flushes to disk. *)
  Mutex.lock t.cache_mutex;
  Hashtbl.replace t.cache key value;
  Mutex.unlock t.cache_mutex;
  Mutex.lock t.save_queue_mutex;
  Hashtbl.replace t.save_queue key value;
  Condition.signal t.save_cond;
  Mutex.unlock t.save_queue_mutex
;;

let create ~filename ~parse ~serialize =
  let t =
    { filename
    ; parse
    ; serialize
    ; file_mutex = Mutex.create ()
    ; file_tree = None
    ; cache = Hashtbl.create 16
    ; cache_mutex = Mutex.create ()
    ; save_queue = Hashtbl.create 16
    ; save_queue_mutex = Mutex.create ()
    ; save_cond = Condition.create ()
    }
  in
  ignore (Domain.spawn (background_worker t));
  t
;;

(** All keys currently known to the store (cache + file). *)
let keys t =
  Mutex.lock t.file_mutex;
  Mutex.lock t.cache_mutex;
  let from_file =
    match t.file_tree with
    | Some tree ->
      let open Yojson.Basic.Util in
      (try tree |> to_assoc |> List.map fst with
       | _ -> [])
    | None ->
      let tree = read_file_unsafe t in
      let open Yojson.Basic.Util in
      (try tree |> to_assoc |> List.map fst with
       | _ -> [])
  in
  let cached =
    Hashtbl.fold (fun k _ acc -> if List.mem k acc then acc else k :: acc) t.cache []
  in
  Mutex.unlock t.cache_mutex;
  Mutex.unlock t.file_mutex;
  List.rev_append cached from_file
;;

(** Configured strategies registered at startup (from config.json's trading
    entries): (strategy_name, symbol, venue, base_accumulation, sell_levels)
    tuples. Migration consults this to auto-map legacy symbol-keyed entries
    to full strategy keys when exactly one configured strategy matches the
    symbol. The opt-in flags let the strategy layer achieve STRICT opt-out
    semantics: when a subsystem is disabled for a symbol, hydration skips
    that store entirely (zero reads), not just zero writes. *)
let configured_strategies : (string * string * string * bool * bool) list ref = ref []

let register_configured_strategies entries = configured_strategies := entries

(** Returns the unique configured strategy matching [symbol], or None (zero or
    ambiguous matches). *)
let unique_configured_strategy_for_symbol symbol =
  let matches =
    List.filter (fun (_, sym, _, _, _) -> sym = symbol) !configured_strategies
  in
  match matches with
  | [ (strategy, _, venue, _, _) ] -> Some (strategy, venue)
  | _ -> None
;;

(** Returns the per-strategy persistence opt-in flags for [symbol] when
    exactly one configured strategy matches, else None. Flags default to the
    spec defaults (base_accumulation: true, sell_levels: false) when unknown. *)
let opt_in_flags_for_symbol symbol =
  let matches =
    List.filter (fun (_, sym, _, _, _) -> sym = symbol) !configured_strategies
  in
  match matches with
  | [ (_, _, _, base_accumulation, sell_levels) ] -> Some (base_accumulation, sell_levels)
  | [] -> Some (true, false)
  | _ ->
    Logging.warn_f
      ~section
      "Ambiguous configured strategies for symbol %s; using spec-default opt-in \
       (base_accumulation=true, sell_levels=false)"
      symbol;
    Some (true, false)
;;

let base_accumulation_opted_in symbol =
  match opt_in_flags_for_symbol symbol with
  | Some (b, _) -> b
  | None -> true
;;

let sell_levels_opted_in symbol =
  match opt_in_flags_for_symbol symbol with
  | Some (_, s) -> s
  | None -> false
;;

(* ------------------------------------------------------------------ *)
(* Legacy migration                                                    *)
(* ------------------------------------------------------------------ *)

(** Hooks registered by the concrete stores: each receives one legacy entry
    (symbol, raw JSON) and decides what to import from it and under which key. *)
let migrate_hooks : (string -> Yojson.Basic.t -> unit) list ref = ref []

let register_migrate_hook f = migrate_hooks := f :: !migrate_hooks
let legacy_file () = Filename.concat (state_dir ()) "accumulated_state.json"

(** Detects the legacy flat symbol-keyed accumulated_state.json, splits every
    entry into the new stores via registered hooks, and renames the original
    to accumulated_state.json.migrated.<ts> (retained until prod
    verification). Called once at startup, before any strategy hydrates. *)
let migrate_if_legacy () =
  let path = legacy_file () in
  if Sys.file_exists path
  then (
    let timestamp = int_of_float (Unix.time ()) in
    Logging.info_f ~section "Legacy %s detected; migrating" path;
    (try
       let tree = Yojson.Basic.from_file path in
       let open Yojson.Basic.Util in
       let entries =
         try tree |> to_assoc with
         | _ -> []
       in
       List.iter
         (fun (symbol, entry) -> List.iter (fun hook -> hook symbol entry) !migrate_hooks)
         entries
     with
     | exn ->
       Logging.warn_f
         ~section
         "Failed to migrate legacy %s: %s (file left in place)"
         path
         (Printexc.to_string exn));
    let renamed = Printf.sprintf "%s.migrated.%d" path timestamp in
    try Sys.rename path renamed with
    | Sys_error msg ->
      Logging.warn_f ~section "Could not rename migrated file %s: %s" path msg)
;;
