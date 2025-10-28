(**
  Fee Cache - Caches maker fees per exchange/symbol with TTL and async refresh

  Provides dynamic fee data for strategies, falling back gracefully on cache misses.
  Uses async refresh to avoid blocking strategy execution paths.
*)

open Lwt.Infix

(** Access to Kraken modules *)
open Kraken

let section = "fee_cache"

(** Cache entry with timestamp and TTL *)
type cache_entry = {
  fee: float;
  timestamp: float;
  ttl_seconds: float;
}

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

(** Get cached maker fee for exchange/symbol pair *)
let get_maker_fee ~exchange ~symbol =
  Mutex.lock cache_mutex;
  let result = match Hashtbl.find_opt fee_cache (exchange, symbol) with
    | Some entry when is_valid entry -> Some entry.fee
    | Some _ ->
        (* Entry exists but expired - remove it *)
        Hashtbl.remove fee_cache (exchange, symbol);
        None
    | None -> None
  in
  Mutex.unlock cache_mutex;
  result

(** Store fee in cache with timestamp *)
let store_fee ~exchange ~symbol ~fee ~ttl_seconds =
  let entry = {
    fee;
    timestamp = Unix.time ();
    ttl_seconds;
  } in
  Mutex.lock cache_mutex;
  Hashtbl.replace fee_cache (exchange, symbol) entry;
  Mutex.unlock cache_mutex;
  Logging.debug_f ~section "Cached fee for %s/%s: %.6f (TTL: %.0fs)" exchange symbol fee ttl_seconds

(** Async refresh of fee for a symbol - with in-flight guard *)
let refresh_async symbol =
  (* Check if we have a valid cached entry first *)
  Mutex.lock cache_mutex;
  let has_valid_entry =
    match Hashtbl.find_opt fee_cache ("kraken", symbol) with
    | Some entry when is_valid entry -> true
    | _ -> false
  in
  Mutex.unlock cache_mutex;

  if has_valid_entry then
    Lwt.return_unit  (* Already have valid cached fee *)
  else begin
    (* Check if fetch is already in-flight *)
    Mutex.lock in_flight_mutex;
    let existing_promise = Hashtbl.find_opt in_flight ("kraken", symbol) in
    let promise = match existing_promise with
      | Some p -> p  (* Reuse existing in-flight fetch *)
      | None ->
          (* Start new fetch *)
          let p = Lwt.catch (fun () ->
            Kraken_get_fee.get_fee_info symbol >>= fun fee_opt ->
            (match fee_opt with
             | Some { maker_fee = Some maker_fee; exchange; _ } ->
                 store_fee ~exchange ~symbol ~fee:maker_fee ~ttl_seconds:default_ttl;
                 Logging.info_f ~section "Fetched and cached maker fee for %s: %.6f" symbol maker_fee;
                 Lwt.return_unit
             | Some { exchange; _ } ->
                 Logging.debug_f ~section "No maker fee available for %s/%s" exchange symbol;
                 Lwt.return_unit
             | None ->
                 Logging.debug_f ~section "Failed to fetch fee for %s" symbol;
                 Lwt.return_unit
            )
          ) (fun exn ->
            Logging.warn_f ~section "Error fetching fee for %s: %s" symbol (Printexc.to_string exn);
            Lwt.return_unit
          ) in
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

(** Refresh fee only if not already cached (for startup use) *)
let refresh_if_missing symbol =
  ignore (Lwt.async (fun () -> refresh_async symbol))

(** Initialize cache - called once at startup *)
let init () =
  Logging.debug_f ~section "Fee cache initialized with TTL %.0f seconds" default_ttl

(** Prime cache with initial symbols - optional pre-population *)
let prime symbols =
  List.iter (fun symbol ->
    ignore (Lwt.async (fun () -> refresh_async symbol));
    Logging.debug_f ~section "Priming fee cache for %s" symbol
  ) symbols

(** Get cache statistics for monitoring *)
let stats () =
  Mutex.lock cache_mutex;
  let count = Hashtbl.length fee_cache in
  let valid_count = Hashtbl.fold (fun _ entry acc ->
    if is_valid entry then acc + 1 else acc
  ) fee_cache 0 in
  Mutex.unlock cache_mutex;
  (count, valid_count)
