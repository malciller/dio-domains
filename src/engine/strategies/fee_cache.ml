(**
  Fee Cache

  TTL-based in-memory cache for maker and taker fees, keyed by
  (exchange, symbol). Supports LRU eviction at capacity, deduplication
  of concurrent fetches via in-flight promise tracking, and exponential
  backoff retry on transient errors. Cache misses return [None] without
  blocking the caller; background refresh is scheduled asynchronously.
*)

open Lwt.Infix

(** Kraken exchange API bindings. *)
open Kraken

let section = "fee_cache"

(** Per-symbol fee record with TTL expiry and LRU metadata. *)
type cache_entry = {
  maker_fee: float;
  taker_fee: float;
  timestamp: float;            (** Unix epoch at insertion. *)
  ttl_seconds: float;          (** Validity window in seconds. *)
  mutable last_access: float;  (** Unix epoch of most recent read; used for LRU eviction. *)
}

(** Upper bound on cache entries; triggers LRU eviction when reached. *)
let max_cache_size = 100

(** Primary cache table keyed by [(exchange, symbol)]. *)
let fee_cache : (string * string, cache_entry) Hashtbl.t = Hashtbl.create 32

(** Guards all reads and writes to [fee_cache]. *)
let cache_mutex = Mutex.create ()

(** Tracks pending fetch promises keyed by [(exchange, symbol)] to deduplicate concurrent requests. *)
let in_flight : (string * string, unit Lwt.t) Hashtbl.t = Hashtbl.create 32

(** Guards all reads and writes to [in_flight]. *)
let in_flight_mutex = Mutex.create ()

(** Default entry TTL in seconds (600s = 10 minutes). *)
let default_ttl = 600.0

(** Returns [true] if [entry] has not exceeded its TTL. *)
let is_valid entry =
  let now = Unix.time () in
  now -. entry.timestamp < entry.ttl_seconds

(** Removes the least-recently-accessed entry when cache is at [max_cache_size].
    Must be called while [cache_mutex] is held. *)
let evict_lru_if_needed () =
  if Hashtbl.length fee_cache >= max_cache_size then (
    let oldest_key = ref None in
    let oldest_time = ref Float.max_float in
    Hashtbl.iter (fun key entry ->
      if entry.last_access < !oldest_time then (
        oldest_time := entry.last_access;
        oldest_key := Some key
      )
    ) fee_cache;
    match !oldest_key with
    | Some key ->
        Hashtbl.remove fee_cache key;
        Logging.debug_f ~section "Evicted LRU fee cache entry (cache at capacity: %d)" max_cache_size
    | None -> ()
  )

(** Looks up the cached maker fee for [(exchange, symbol)].
    Returns [Some fee] on a valid hit, [None] on miss or expiry.
    Expired entries are removed eagerly. *)
let get_maker_fee ~exchange ~symbol =
  Mutex.lock cache_mutex;
  let result = match Hashtbl.find_opt fee_cache (exchange, symbol) with
    | Some entry when is_valid entry ->
        entry.last_access <- Unix.time ();
        Some entry.maker_fee
    | Some _ ->
        Hashtbl.remove fee_cache (exchange, symbol);
        None
    | None -> None
  in
  Mutex.unlock cache_mutex;
  result

(** Looks up the cached taker fee for [(exchange, symbol)].
    Returns [Some fee] on a valid hit, [None] on miss or expiry.
    Expired entries are removed eagerly. *)
let get_taker_fee ~exchange ~symbol =
  Mutex.lock cache_mutex;
  let result = match Hashtbl.find_opt fee_cache (exchange, symbol) with
    | Some entry when is_valid entry ->
        entry.last_access <- Unix.time ();
        Some entry.taker_fee
    | Some _ ->
        Hashtbl.remove fee_cache (exchange, symbol);
        None
    | None -> None
  in
  Mutex.unlock cache_mutex;
  result

(** Inserts or replaces the fee entry for [(exchange, symbol)].
    Triggers LRU eviction if the cache is at capacity before insertion. *)
let store_fees ~exchange ~symbol ~maker_fee ~taker_fee ~ttl_seconds =
  let now = Unix.time () in
  let entry = {
    maker_fee;
    taker_fee;
    timestamp = now;
    ttl_seconds;
    last_access = now;
  } in
  Mutex.lock cache_mutex;
  evict_lru_if_needed ();
  Hashtbl.replace fee_cache (exchange, symbol) entry;
  Mutex.unlock cache_mutex;
  Logging.debug_f ~section "Cached fees for %s/%s: maker=%.6f, taker=%.6f (TTL: %.0fs, cache size: %d/%d)" 
    exchange symbol maker_fee taker_fee ttl_seconds (Hashtbl.length fee_cache) max_cache_size

(** Asynchronously refreshes the fee for [symbol] on Kraken.
    No-op if a valid entry already exists. Deduplicates concurrent
    calls via the [in_flight] table. Uses linear backoff (0.5s increments,
    up to 3 attempts) on nonce errors. *)
let refresh_async symbol =
  Mutex.lock cache_mutex;
  let has_valid_entry =
    match Hashtbl.find_opt fee_cache ("kraken", symbol) with
    | Some entry when is_valid entry -> true
    | _ -> false
  in
  Mutex.unlock cache_mutex;

  if has_valid_entry then begin
    Logging.debug_f ~section "Fee for %s already cached and valid, skipping fetch" symbol;
    Lwt.return_unit
  end else begin
    Mutex.lock in_flight_mutex;
    let existing_promise = Hashtbl.find_opt in_flight ("kraken", symbol) in
    let promise = match existing_promise with
      | Some p ->
          Logging.debug_f ~section "Fee fetch for %s already in-flight, reusing promise" symbol;
          p
      | None ->
          let rec fetch_with_retry attempt =
            Lwt.catch (fun () ->
              Kraken_get_fee.get_fee_info symbol >>= fun fee_opt ->
              (match fee_opt with
               | Some { maker_fee = Some maker_fee; taker_fee = Some taker_fee; exchange; _ } ->
                   store_fees ~exchange ~symbol ~maker_fee ~taker_fee ~ttl_seconds:default_ttl;
                   Logging.info_f ~section "Fetched and cached fees for %s: maker=%.6f, taker=%.6f (attempt %d)" symbol maker_fee taker_fee attempt;
                   Lwt.return_unit
               | Some { maker_fee = Some maker_fee; taker_fee = None; exchange; _ } ->
                   (* Taker fee unavailable; fall back to maker fee for both. *)
                   store_fees ~exchange ~symbol ~maker_fee ~taker_fee:maker_fee ~ttl_seconds:default_ttl;
                   Logging.info_f ~section "Fetched and cached maker fee for %s: %.6f (taker=default to maker) (attempt %d)" symbol maker_fee attempt;
                   Lwt.return_unit
               | Some { exchange; _ } ->
                   Logging.debug_f ~section "No fee data available for %s/%s (attempt %d)" exchange symbol attempt;
                   Lwt.return_unit
               | None ->
                   Logging.debug_f ~section "Failed to fetch fee for %s (attempt %d)" symbol attempt;
                   Lwt.return_unit
              )
            ) (fun exn ->
              let error_msg = Printexc.to_string exn in
              let is_nonce_error = String.length error_msg >= 12 && String.sub error_msg 0 12 = "Invalid nonce" in
              if is_nonce_error && attempt < 3 then begin
                let backoff = float_of_int attempt *. 0.5 in
                Logging.warn_f ~section "Nonce error fetching fee for %s (attempt %d), retrying in %.1fs: %s"
                  symbol attempt backoff error_msg;
                Lwt_unix.sleep backoff >>= fun () ->
                fetch_with_retry (attempt + 1)
              end else begin
                Logging.warn_f ~section "Error fetching fee for %s (attempt %d): %s" symbol attempt error_msg;
                Lwt.return_unit
              end
            )
          in
          let p = fetch_with_retry 1 in
          Hashtbl.replace in_flight ("kraken", symbol) p;
          p
    in
    Mutex.unlock in_flight_mutex;

    (* Remove entry from [in_flight] on resolution or rejection. *)
    Lwt.on_any promise
      (fun () ->
         Mutex.lock in_flight_mutex;
         Hashtbl.remove in_flight ("kraken", symbol);
         Mutex.unlock in_flight_mutex)
      (fun _ ->
         Mutex.lock in_flight_mutex;
         Hashtbl.remove in_flight ("kraken", symbol);
         Mutex.unlock in_flight_mutex);

    promise
  end


(** Performs one-time startup initialization. Logs the configured TTL. *)
let init () =
  Logging.debug_f ~section "Fee cache initialized with TTL %.0f seconds" default_ttl

(** Removes all entries from the cache. Intended for test teardown. *)
let clear () =
  Mutex.lock cache_mutex;
  Hashtbl.clear fee_cache;
  Mutex.unlock cache_mutex


(** Returns [(total_entries, valid_entries)] for monitoring.
    Counts entries that have not yet exceeded their TTL as valid. *)
let stats () =
  Mutex.lock cache_mutex;
  let count = Hashtbl.length fee_cache in
  let valid_count = Hashtbl.fold (fun _ entry acc ->
    if is_valid entry then acc + 1 else acc
  ) fee_cache 0 in
  Mutex.unlock cache_mutex;
  (count, valid_count)
