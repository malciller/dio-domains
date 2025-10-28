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

(** Async refresh of fee for a symbol *)
let refresh_async symbol =
  Lwt.async (fun () ->
    Lwt.catch (fun () ->
      Kraken_get_fee.get_fee_info symbol >>= fun fee_opt ->
      (match fee_opt with
       | Some { maker_fee = Some maker_fee; exchange; _ } ->
           store_fee ~exchange ~symbol ~fee:maker_fee ~ttl_seconds:default_ttl;
           Logging.debug_f ~section "Refreshed fee for %s: %.6f" symbol maker_fee;
           Lwt.return_unit
       | Some { exchange; _ } ->
           Logging.debug_f ~section "No maker fee available for %s/%s" exchange symbol;
           Lwt.return_unit
       | None ->
           Logging.debug_f ~section "Failed to fetch fee for %s" symbol;
           Lwt.return_unit
      )
    ) (fun exn ->
      Logging.warn_f ~section "Error refreshing fee for %s: %s" symbol (Printexc.to_string exn);
      Lwt.return_unit
    )
  )

(** Initialize cache - called once at startup *)
let init () =
  Logging.debug_f ~section "Fee cache initialized with TTL %.0f seconds" default_ttl

(** Prime cache with initial symbols - optional pre-population *)
let prime symbols =
  List.iter (fun symbol ->
    refresh_async symbol;
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
