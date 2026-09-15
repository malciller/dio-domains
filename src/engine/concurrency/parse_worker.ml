(** Dedicated background domain for parsing high-rate feed frames off the Lwt scheduler
    thread.

    The main domain's Lwt event loop handles all venues' WebSocket traffic. Full-JSON
    parsing per frame (e.g. Kraken's v2 book channel) is allocation-heavy and adds latency
    jitter to every other fiber. This module moves such parsing onto one dedicated OCaml 5
    domain fed by a bounded queue.

    Contract:
    - Producers call [submit ~handler payload] from any thread/domain. Returns [false]
      when the queue is full; the caller MUST then handle the payload synchronously
      (inline fallback). Dropping is not an option for incremental feeds (Kraken book
      deltas): a lost delta desyncs the local book until the next snapshot, so the
      fallback preserves correctness under overload at the cost of the offload benefit.
    - Handlers run sequentially on the parse domain, preserving per-venue frame order.
      They MUST NOT touch Lwt primitives (promises, streams, [Lwt_condition], [Lwt_mvar]):
      those are single-domain. Mutexes, Condition variables, Atomics and Logging are
      domain-safe.
    - One handler per name; register before first use via [register].

    The queue is a stdlib [Queue] guarded by one mutex, not a lock-free structure: each
    producer's critical section is ~100ns against a ~10-50us parse, so a lock-free queue
    offers no measurable win. *)

(* OxCaml marks [Domain.spawn] as [do_not_spawn_domains]. This module spawns one bounded,
   config-gated parse domain, not an unbounded fan-out, so the GC concern the alert
   describes does not apply. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

type handler = string -> unit

module StringMap = Map.Make (String)

let handlers : handler StringMap.t Atomic.t = Atomic.make StringMap.empty

(* Per-handler frame count and cumulative parse time, single-writer (only the parse domain
   runs handlers), so the lock is uncontended except on snapshot. *)
let handler_stats : (string, int * float) Hashtbl.t = Hashtbl.create 8
let handler_stats_mutex = Mutex.create ()

(** Register [handler] under [name]. Call during module/startup init, before any frame can
    arrive. The handler is wrapped to record per-name frame count and cumulative wall time
    for diagnostics. *)
let register name handler =
  let wrapped payload =
    let t0 = Unix.gettimeofday () in
    handler payload;
    let dt = Unix.gettimeofday () -. t0 in
    Mutex.lock handler_stats_mutex;
    let n, total = Option.value (Hashtbl.find_opt handler_stats name) ~default:(0, 0.0) in
    Hashtbl.replace handler_stats name (n + 1, total +. dt);
    Mutex.unlock handler_stats_mutex
  in
  let rec loop () =
    let current = Atomic.get handlers in
    if Atomic.compare_and_set handlers current (StringMap.add name wrapped current)
    then ()
    else loop ()
  in
  loop ()
;;

(** Per-handler [name, frames, cumulative_seconds] snapshot (diagnostics). *)
let handler_stats_snapshot () =
  Mutex.lock handler_stats_mutex;
  let l = Hashtbl.fold (fun k (n, t) acc -> (k, n, t) :: acc) handler_stats [] in
  Mutex.unlock handler_stats_mutex;
  l
;;

(* Work queue: (handler name, raw frame). Bounded; overflow is reported via [submit] ->
   false, so no frame is dropped silently. *)
let queue_capacity = 65536
let queue_mutex = Mutex.create ()
let queue_condition = Condition.create ()
let queue : (string * string) Queue.t = Queue.create ()

let queue_length () =
  Mutex.lock queue_mutex;
  let n = Queue.length queue in
  Mutex.unlock queue_mutex;
  n
;;

(* Observability counters for diagnostics/dashboards. [submit_fallbacks] counts [submit]
   -> false (queue full), i.e. frames the caller had to parse inline on its own thread; a
   non-zero rate means the parse domain is saturated and hot paths are paying parse cost.
   [queue_high_water] is the peak depth seen. *)
let batches_drained = Atomic.make 0
let worker_started = Atomic.make false
let frames_submitted = Atomic.make 0
let frames_processed = Atomic.make 0
let submit_fallbacks = Atomic.make 0
let queue_high_water = Atomic.make 0
let max_batch = 64

let update_max cell v =
  let rec loop () =
    let cur = Atomic.get cell in
    if v <= cur then () else if Atomic.compare_and_set cell cur v then () else loop ()
  in
  loop ()
;;

(** Worker body: block until signalled, drain up to [max_batch] frames, repeat. Runs until
    process exit. *)
let worker_loop () =
  while true do
    (* Block until a producer signals pending work. The size re-check under the mutex
       makes this race-free against concurrent submissions. *)
    Mutex.lock queue_mutex;
    while Queue.length queue = 0 do
      Condition.wait queue_condition queue_mutex
    done;
    (* Move the batch out in O(1) so producers never wait on parsing. *)
    let batch = Queue.create () in
    let rec take n =
      if n >= max_batch || Queue.is_empty queue
      then ()
      else (
        Queue.push (Queue.pop queue) batch;
        take (n + 1))
    in
    take 0;
    Mutex.unlock queue_mutex;
    (* Execute handlers outside any lock. *)
    Queue.iter
      (fun (name, payload) ->
        match StringMap.find_opt name (Atomic.get handlers) with
        | Some handler ->
          (try handler payload with
           | exn ->
             Logging.error_f
               ~section:"parse_worker"
               "Handler '%s' failed: %s"
               name
               (Printexc.to_string exn))
        | None ->
          Logging.warn_f
            ~section:"parse_worker"
            "No handler registered for '%s'; frame dropped"
            name)
      batch;
    ignore (Atomic.fetch_and_add frames_processed (Queue.length batch));
    ignore (Atomic.fetch_and_add batches_drained 1)
  done
;;

(** Spawn the worker domain on first submit, lazily, so library load order does not
    determine GC-config exposure for domains spawned after [Config.apply_gc_config]. *)
let ensure_worker () =
  if Atomic.compare_and_set worker_started false true
  then ignore (Domain.spawn worker_loop)
;;

(** Submit a raw frame for asynchronous parsing.
    @return
      [true] if queued, [false] if the queue is full (caller must process synchronously). *)
let submit handler payload =
  ensure_worker ();
  ignore (Atomic.fetch_and_add frames_submitted 1);
  Mutex.lock queue_mutex;
  let depth = Queue.length queue in
  let ok =
    if depth >= queue_capacity
    then false
    else (
      Queue.push (handler, payload) queue;
      true)
  in
  if ok then Condition.signal queue_condition;
  Mutex.unlock queue_mutex;
  update_max queue_high_water (if ok then depth + 1 else depth);
  if not ok then ignore (Atomic.fetch_and_add submit_fallbacks 1);
  ok
;;

(** Number of batch drains completed (diagnostics). *)
let stats () = Atomic.get batches_drained

(** Frames submitted to the parse domain (diagnostics). *)
let frames_submitted () = Atomic.get frames_submitted

(** Frames the parse domain has run handlers for (diagnostics). *)
let frames_processed () = Atomic.get frames_processed

(** [submit] failures: frames the caller parsed inline because the queue was full. A
    non-zero rate means the parse domain is saturated and hot paths are paying parse cost
    (diagnostics). *)
let fallbacks () = Atomic.get submit_fallbacks

(** Peak queue depth seen (diagnostics). *)
let high_water () = Atomic.get queue_high_water

(* ---- Uniform venue routing ----

   Rather than each venue inventing a handler name ("kraken_ob", "kraken_exec"), a venue
   registers its [Exchange_intf.S.decode_frame] under [venue_handler_name venue] and its
   WS layer submits raw frames with [submit_frame]. Every venue then shares one offload
   path and one set of diagnostics, and a frame that arrives before the decoder is
   registered falls back inline instead of being dropped. *)

(** Parse-worker handler name under which [venue]'s frame decoder is registered. Kept in
    one place so producers and consumers cannot drift. *)
let venue_handler_name venue = "decode:" ^ venue

(** Register [decoder] as [venue]'s uniform frame decoder. Registration order between
    venues is irrelevant; a later registration for the same venue replaces the earlier
    one. *)
let register_venue_decoder ~venue decoder = register (venue_handler_name venue) decoder

(** Submit a raw [payload] for [venue] to the parse domain.
    @return
      [true] if queued; [false] if the queue is full OR no decoder has been registered for
      [venue] yet. Either way the caller MUST decode inline: a [false] never means the
      frame was handled. *)
let submit_frame venue payload =
  let name = venue_handler_name venue in
  if StringMap.mem name (Atomic.get handlers) then submit name payload else false
;;
