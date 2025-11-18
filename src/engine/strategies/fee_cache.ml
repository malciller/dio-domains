(**
  Fee Cache - Caches maker fees per exchange/symbol with TTL and async refresh

  Provides dynamic fee data for strategies, falling back gracefully on cache misses.
  Uses async refresh to avoid blocking strategy execution paths.
*)

open Lwt.Infix

(** Access to Kraken modules *)
open Kraken

let section = "fee_cache"

(** Cache entry with timestamp, TTL, and LRU tracking *)
type cache_entry = {
  maker_fee: float;
  taker_fee: float;
  timestamp: float;
  ttl_seconds: float;
  mutable last_access: float;  (* Track last access for LRU eviction *)
}

(** Maximum cache size to prevent unbounded growth *)
let max_cache_size = 100

(** In-memory cache: (exchange, symbol) -> cache_entry *)
let fee_cache : (string * string, cache_entry) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Mutex.create ()

(** In-flight fetches: (exchange, symbol) -> unit promise *)
let in_flight : (string * string, unit Lwt.t) Hashtbl.t = Hashtbl.create 32
let in_flight_mutex = Mutex.create ()

(** Default TTL for cache entries (10 minutes) *)
let default_ttl = 600.0

(** Check if cache entry is still valid *)
let is_valid entry =
  let now = Unix.time () in
  now -. entry.timestamp < entry.ttl_seconds

(** Evict least recently used entry if cache is at capacity *)
let evict_lru_if_needed () =
  if Hashtbl.length fee_cache >= max_cache_size then (
    (* Find LRU entry *)
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

(** Get cached maker fee for exchange/symbol pair *)
let get_maker_fee ~exchange ~symbol =
  Mutex.lock cache_mutex;
  let result = match Hashtbl.find_opt fee_cache (exchange, symbol) with
    | Some entry when is_valid entry ->
        entry.last_access <- Unix.time ();  (* Update LRU timestamp *)
        Some entry.maker_fee
    | Some _ ->
        (* Entry exists but expired - remove it *)
        Hashtbl.remove fee_cache (exchange, symbol);
        None
    | None -> None
  in
  Mutex.unlock cache_mutex;
  result

(** Get cached taker fee for exchange/symbol pair *)
let get_taker_fee ~exchange ~symbol =
  Mutex.lock cache_mutex;
  let result = match Hashtbl.find_opt fee_cache (exchange, symbol) with
    | Some entry when is_valid entry ->
        entry.last_access <- Unix.time ();  (* Update LRU timestamp *)
        Some entry.taker_fee
    | Some _ ->
        (* Entry exists but expired - remove it *)
        Hashtbl.remove fee_cache (exchange, symbol);
        None
    | None -> None
  in
  Mutex.unlock cache_mutex;
  result

(** Store fees in cache with timestamp *)
let store_fees ~exchange ~symbol ~maker_fee ~taker_fee ~ttl_seconds =
  let now = Unix.time () in
  let entry = {
    maker_fee;
    taker_fee;
    timestamp = now;
    ttl_seconds;
    last_access = now;  (* Initialize last_access *)
  } in
  Mutex.lock cache_mutex;
  evict_lru_if_needed ();  (* Evict before adding if at capacity *)
  Hashtbl.replace fee_cache (exchange, symbol) entry;
  Mutex.unlock cache_mutex;
  Logging.debug_f ~section "Cached fees for %s/%s: maker=%.6f, taker=%.6f (TTL: %.0fs, cache size: %d/%d)" 
    exchange symbol maker_fee taker_fee ttl_seconds (Hashtbl.length fee_cache) max_cache_size

(** Async refresh of fee for a symbol - with in-flight guard and retry logic *)
let refresh_async symbol =
  (* Check if we have a valid cached entry first *)
  Mutex.lock cache_mutex;
  let has_valid_entry =
    match Hashtbl.find_opt fee_cache ("kraken", symbol) with
    | Some entry when is_valid entry -> true
    | _ -> false
  in
  Mutex.unlock cache_mutex;

  if has_valid_entry then begin
    Logging.debug_f ~section "Fee for %s already cached and valid, skipping fetch" symbol;
    Lwt.return_unit  (* Already have valid cached fee *)
  end else begin
    (* Check if fetch is already in-flight *)
    Mutex.lock in_flight_mutex;
    let existing_promise = Hashtbl.find_opt in_flight ("kraken", symbol) in
    let promise = match existing_promise with
      | Some p ->
          Logging.debug_f ~section "Fee fetch for %s already in-flight, reusing promise" symbol;
          p  (* Reuse existing in-flight fetch *)
      | None ->
          (* Start new fetch with retry logic *)
          let rec fetch_with_retry attempt =
            Lwt.catch (fun () ->
              Kraken_get_fee.get_fee_info symbol >>= fun fee_opt ->
              (match fee_opt with
               | Some { maker_fee = Some maker_fee; taker_fee = Some taker_fee; exchange; _ } ->
                   store_fees ~exchange ~symbol ~maker_fee ~taker_fee ~ttl_seconds:default_ttl;
                   Logging.info_f ~section "Fetched and cached fees for %s: maker=%.6f, taker=%.6f (attempt %d)" symbol maker_fee taker_fee attempt;
                   Lwt.return_unit
               | Some { maker_fee = Some maker_fee; taker_fee = None; exchange; _ } ->
                   (* Only maker fee available - use maker for both *)
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
              (* Check if this is a nonce error that might benefit from retry *)
              let is_nonce_error = String.length error_msg >= 12 && String.sub error_msg 0 12 = "Invalid nonce" in
              if is_nonce_error && attempt < 3 then begin
                let backoff = float_of_int attempt *. 0.5 in  (* 0.5s, 1.0s, 1.5s backoff *)
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

    (* Clean up in-flight table when promise completes *)
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


(** Initialize cache - called once at startup *)
let init () =
  Logging.debug_f ~section "Fee cache initialized with TTL %.0f seconds" default_ttl

(** Clear cache - for testing *)
let clear () =
  Mutex.lock cache_mutex;
  Hashtbl.clear fee_cache;
  Mutex.unlock cache_mutex


(** Get cache statistics for monitoring *)
let stats () =
  Mutex.lock cache_mutex;
  let count = Hashtbl.length fee_cache in
  let valid_count = Hashtbl.fold (fun _ entry acc ->
    if is_valid entry then acc + 1 else acc
  ) fee_cache 0 in
  Mutex.unlock cache_mutex;
  (count, valid_count)
