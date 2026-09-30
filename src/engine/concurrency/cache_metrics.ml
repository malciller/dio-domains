(** Registry of cache health providers.

    The engine has a lot of caches spread across venues, the oracle, the strategy engine
    and the concurrency layer, and until now almost none of them were observable:
    [Fee_cache.stats] existed but had no caller, [Event_bus.get_subscriber_stats] was
    wired into a global registry that nothing ever read. Without hit rates and occupancy
    there is no way to tell a cache earning its keep from one quietly leaking, and no way
    to check that a bound is doing its job.

    Rather than have the dashboard import every venue — which would couple monitoring to
    the components it observes, and go stale the moment a cache moves — each cache
    registers a provider here and the dashboard reads the whole registry generically.

    Providers are called from whichever domain asks for the snapshot, so a provider must
    be cheap, non-blocking, and safe to call concurrently with the cache it describes.
    Nothing here may block or perform IO. *)

type metric =
  | Count of int (** An absolute count: entries, evictions, publishes. *)
  | Ratio of float (** 0.0-1.0, e.g. a hit rate. *)
  | Micros of int (** A duration in microseconds. *)

type sample =
  { name : string (** Stable identifier, e.g. ["kraken.order_to_symbol"]. *)
  ; metrics : (string * metric) list
  }

type provider =
  { pname : string
  ; sample : unit -> sample
  }

let providers : provider list Atomic.t = Atomic.make []
let provider_names : (string, unit) Hashtbl.t = Hashtbl.create 32
let section = "cache_metrics"

(** Registers a provider under [name].

    Registration is idempotent by name: registering a name that is already present is a
    no-op. Several call sites legitimately want to "make sure this is registered" without
    coordinating on who registers first — a module init and a dashboard snapshot, say —
    and a registry that grew a duplicate row on each call would report ambiguous numbers
    and, if a cache were rebuilt per connection, would leak a row per connection. *)
let register name sample =
  if not (Hashtbl.mem provider_names name)
  then (
    let p = { pname = name; sample } in
    let rec insert () =
      let current = Atomic.get providers in
      if Atomic.compare_and_set providers current (p :: current) then () else insert ()
    in
    Hashtbl.replace provider_names name ();
    insert ())
;;

(** Collects every registered provider, in stable name order.

    A provider that raises is reported as a sample carrying the error rather than aborting
    the whole snapshot: monitoring must not be able to take down the dashboard because one
    cache is in a bad state. *)
let snapshot () =
  let rows =
    List.map
      (fun p ->
        match p.sample () with
        | s -> s
        | exception exn ->
          { name = p.pname
          ; metrics =
              [ "error", Count 1
              ; "message", Count (Hashtbl.hash (Printexc.to_string exn))
              ]
          })
      (Atomic.get providers)
  in
  List.sort (fun a b -> String.compare a.name b.name) rows
;;

(** Number of registered providers. *)
let provider_count () = List.length (Atomic.get providers)

(** Convenience: a hit rate as a [Ratio], or 0.0 when nothing has been looked up yet. *)
let hit_ratio hits misses =
  if hits + misses = 0 then 0.0 else float hits /. float (hits + misses)
;;

let log_registry () =
  let rows = snapshot () in
  Logging.debug_f
    ~section
    "cache metrics registry: %d providers (%s)"
    (List.length rows)
    (String.concat ", " (List.map (fun s -> s.name) rows))
;;
