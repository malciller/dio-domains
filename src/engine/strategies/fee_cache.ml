(**
  Fee Cache

  TTL-based in-memory cache for maker and taker fees, keyed by
  (exchange, symbol). Uses lock-free Atomics for read paths to eliminate 
  hotpath contention across strategy domains.
*)

open Lwt.Infix

(** Kraken exchange API bindings. *)
open Kraken

let section = "fee_cache"

(** Per-symbol fee record with TTL expiry. *)
type cache_entry = {
  maker_fee: float;
  taker_fee: float;
  timestamp: float;            (** Unix epoch at insertion. *)
  ttl_seconds: float;          (** Validity window in seconds. *)
}

module StringMap = Map.Make(String)

(** Primary cache table keyed by "exchange|symbol". *)
let fee_cache = Atomic.make StringMap.empty

(** Tracks pending fetch promises keyed by "exchange|symbol" to deduplicate concurrent requests. *)
let in_flight : (string, unit Lwt.t) Hashtbl.t = Hashtbl.create 32

(** Guards all reads and writes to [in_flight]. *)
let in_flight_mutex = Mutex.create ()

(** Default entry TTL in seconds (600s = 10 minutes). *)
let default_ttl = 600.0

let make_key exchange symbol = exchange ^ "|" ^ symbol

(** Returns [true] if [entry] has not exceeded its TTL. *)
let is_valid entry =
  let now = Unix.time () in
  now -. entry.timestamp < entry.ttl_seconds

let evict_expired map =
  let now = Unix.time () in
  StringMap.filter (fun _ entry -> now -. entry.timestamp < entry.ttl_seconds) map

(** Looks up the cached maker fee for [(exchange, symbol)].
    Returns [Some fee] on a valid hit, [None] on miss or expiry. *)
let get_maker_fee ~exchange ~symbol =
  let key = make_key exchange symbol in
  let map = Atomic.get fee_cache in
  match StringMap.find_opt key map with
  | Some entry when is_valid entry -> Some entry.maker_fee
  | _ -> None

(** Looks up the cached taker fee for [(exchange, symbol)].
    Returns [Some fee] on a valid hit, [None] on miss or expiry. *)
let get_taker_fee ~exchange ~symbol =
  let key = make_key exchange symbol in
  let map = Atomic.get fee_cache in
  match StringMap.find_opt key map with
  | Some entry when is_valid entry -> Some entry.taker_fee
  | _ -> None

(** Inserts or replaces the fee entry for [(exchange, symbol)].
    Evicts expired entries opportunistically. *)
let store_fees ~exchange ~symbol ~maker_fee ~taker_fee ~ttl_seconds =
  let key = make_key exchange symbol in
  let entry = {
    maker_fee;
    taker_fee;
    timestamp = Unix.time ();
    ttl_seconds;
  } in
  let rec loop () =
    let map = Atomic.get fee_cache in
    let clean_map = evict_expired map in
    let new_map = StringMap.add key entry clean_map in
    if Atomic.compare_and_set fee_cache map new_map then ()
    else loop ()
  in loop ();
  Logging.debug_f ~section "Cached fees for %s: maker=%.6f, taker=%.6f (TTL: %.0fs, cache size: %d)" 
    key maker_fee taker_fee ttl_seconds (StringMap.cardinal (Atomic.get fee_cache))

(** Asynchronously refreshes the fee for [symbol] on Kraken.
    No-op if a valid entry already exists. Deduplicates concurrent
    calls via the [in_flight] table. Uses linear backoff (0.5s increments,
    up to 3 attempts) on nonce errors. *)
let refresh_async symbol =
  let key = make_key "kraken" symbol in
  let map = Atomic.get fee_cache in
  let has_valid_entry =
    match StringMap.find_opt key map with
    | Some entry when is_valid entry -> true
    | _ -> false
  in

  if has_valid_entry then begin
    Logging.debug_f ~section "Fee for %s already cached and valid, skipping fetch" symbol;
    Lwt.return_unit
  end else begin
    Mutex.lock in_flight_mutex;
    let existing_promise = Hashtbl.find_opt in_flight key in
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
          Hashtbl.replace in_flight key p;
          p
    in
    Mutex.unlock in_flight_mutex;

    (* Remove entry from [in_flight] on resolution or rejection. *)
    Lwt.on_any promise
      (fun () ->
         Mutex.lock in_flight_mutex;
         Hashtbl.remove in_flight key;
         Mutex.unlock in_flight_mutex)
      (fun _ ->
         Mutex.lock in_flight_mutex;
         Hashtbl.remove in_flight key;
         Mutex.unlock in_flight_mutex);

    promise
  end

(** Performs one-time startup initialization. Logs the configured TTL. *)
let init () =
  Logging.debug_f ~section "Fee cache initialized with TTL %.0f seconds" default_ttl

(** Removes all entries from the cache. Intended for test teardown. *)
let clear () =
  Atomic.set fee_cache StringMap.empty

(** Returns [(total_entries, valid_entries)] for monitoring.
    Counts entries that have not yet exceeded their TTL as valid. *)
let stats () =
  let map = Atomic.get fee_cache in
  let count = StringMap.cardinal map in
  let valid_count = StringMap.fold (fun _ entry acc ->
    if is_valid entry then acc + 1 else acc
  ) map 0 in
  (count, valid_count)
