(** Background domains for parsing high-rate feed frames off the Lwt scheduler thread.

    The main domain's Lwt event loop handles all venues' WebSocket traffic. Full-JSON
    parsing per frame (e.g. Kraken's v2 book channel) is allocation-heavy and adds latency
    jitter to every other fiber. This module moves such parsing onto dedicated OCaml 5
    domains fed by bounded queues.

    Sharding: one queue/domain per venue (the suffix after [':'] or the prefix before
    ['_']). A venue's high-rate feed cannot delay another venue's book parse: Alpaca's SIP
    quotes never sit ahead of a Kraken delta in the same queue. At most one domain per
    active venue, spawned lazily on the first submission.

    Contract:
    - Producers call [submit ~handler payload] from any thread/domain. Returns [false]
      when the queue is full (or no handler is registered); the caller MUST then handle
      the payload synchronously (inline fallback). Dropping is not an option for
      incremental feeds (book deltas): a lost delta desyncs the local book until the next
      snapshot, so the fallback preserves correctness under overload at the cost of the
      offload benefit.
    - Handlers run sequentially per shard, preserving per-venue frame order. They MUST NOT
      touch Lwt primitives (promises, streams, [Lwt_condition], [Lwt_mvar]): those are
      single-domain. Mutexes, Condition variables, Atomics and Logging are domain-safe.
    - One handler per name; register before first use via [register].

    The queue is a stdlib [Queue] guarded by one mutex per shard, not a lock-free
    structure: each producer's critical section is ~100ns against a ~10-50us parse, so a
    lock-free queue offers no measurable win. *)

(* OxCaml marks [Domain.spawn] as [do_not_spawn_domains]. This module spawns a bounded set
   of per-venue parse domains (one per active venue), not an unbounded fan-out, so the GC
   concern the alert describes does not apply. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

type handler = string -> unit

module StringMap = Map.Make (String)

type shard =
  { queue : (string * string) Queue.t
  ; mutex : Mutex.t
  ; condition : Condition.t
  ; started : bool Atomic.t
  ; high_water : int Atomic.t
  }

let shards : (string, shard) Hashtbl.t = Hashtbl.create 8
let shards_mutex = Mutex.create ()

let shard_of_name name =
  match String.index_opt name ':' with
  | Some i -> String.sub name (i + 1) (String.length name - i - 1)
  | None ->
    (match String.index_opt name '_' with
     | Some i -> String.sub name 0 i
     | None -> name)
;;

let get_shard name =
  Mutex.lock shards_mutex;
  let shard =
    match Hashtbl.find_opt shards name with
    | Some shard -> shard
    | None ->
      let shard =
        { queue = Queue.create ()
        ; mutex = Mutex.create ()
        ; condition = Condition.create ()
        ; started = Atomic.make false
        ; high_water = Atomic.make 0
        }
      in
      Hashtbl.add shards name shard;
      shard
  in
  Mutex.unlock shards_mutex;
  shard
;;

(** Registered handler and the shard its frames are queued on. *)
let handlers : (shard * handler) StringMap.t Atomic.t = Atomic.make StringMap.empty

(* Per-handler frame count and cumulative parse time. Each shard's worker is the only
   writer for its own handlers; the mutex only contends across shards on snapshot. *)
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
  let shard = get_shard (shard_of_name name) in
  let rec loop () =
    let current = Atomic.get handlers in
    if Atomic.compare_and_set
         handlers
         current
         (StringMap.add name (shard, wrapped) current)
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

(* Per-shard work queue: (handler name, raw frame). Bounded; overflow is reported via
   [submit] -> false, so no frame is dropped silently. *)
let queue_capacity = 65536

(* Observability counters for diagnostics/dashboards. [submit_fallbacks] counts [submit]
   -> false (queue full or unregistered), i.e. frames the caller had to parse inline on
   its own thread; a non-zero rate means a parse domain is saturated and hot paths are
   paying parse cost. [queue_high_water] is the peak depth across all shards. *)
let batches_drained = Atomic.make 0
let frames_submitted = Atomic.make 0
let frames_processed = Atomic.make 0
let submit_fallbacks = Atomic.make 0
let max_batch = 64

let update_max cell v =
  let rec loop () =
    let cur = Atomic.get cell in
    if v <= cur then () else if Atomic.compare_and_set cell cur v then () else loop ()
  in
  loop ()
;;

(** Worker body for one shard: block until signalled, drain up to [max_batch] frames,
    repeat. Runs until process exit. *)
let worker_loop shard =
  while true do
    Mutex.lock shard.mutex;
    while Queue.is_empty shard.queue do
      Condition.wait shard.condition shard.mutex
    done;
    (* Move the batch out in O(1) so producers never wait on parsing. *)
    let batch = Queue.create () in
    let rec take n =
      if n >= max_batch || Queue.is_empty shard.queue
      then ()
      else (
        Queue.push (Queue.pop shard.queue) batch;
        take (n + 1))
    in
    take 0;
    Mutex.unlock shard.mutex;
    (* Execute handlers outside any lock. *)
    Queue.iter
      (fun (name, payload) ->
        match StringMap.find_opt name (Atomic.get handlers) with
        | Some (_, handler) ->
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

(** Spawn a shard's worker domain on first submit, lazily, so library load order does not
    determine GC-config exposure for domains spawned after [Config.apply_gc_config]. *)
let ensure_worker shard =
  if Atomic.compare_and_set shard.started false true
  then
    ignore
      (Domain.spawn (fun () ->
         Gc_config.apply ();
         (* Parsing is on the feed critical path. [DIO_PARSE_CPUS] pins each shard to a
            supplied P-core; unset leaves placement to the scheduler. *)
         Thread_affinity.pin_self_parse ();
         worker_loop shard))
;;

(** Submit a raw frame for asynchronous parsing.
    @return
      [true] if queued, [false] if the queue is full or [handler] is not registered (the
      caller must then process synchronously). *)
let submit handler payload =
  match StringMap.find_opt handler (Atomic.get handlers) with
  | None ->
    ignore (Atomic.fetch_and_add submit_fallbacks 1);
    false
  | Some (shard, _) ->
    ensure_worker shard;
    ignore (Atomic.fetch_and_add frames_submitted 1);
    Mutex.lock shard.mutex;
    let depth = Queue.length shard.queue in
    let ok =
      if depth >= queue_capacity
      then false
      else (
        Queue.push (handler, payload) shard.queue;
        true)
    in
    if ok then Condition.signal shard.condition;
    Mutex.unlock shard.mutex;
    update_max shard.high_water (if ok then depth + 1 else depth);
    if not ok then ignore (Atomic.fetch_and_add submit_fallbacks 1);
    ok
;;

(** Number of batch drains completed across shards (diagnostics). *)
let stats () = Atomic.get batches_drained

(** Frames submitted to the parse domains (diagnostics). *)
let frames_submitted () = Atomic.get frames_submitted

(** Frames the parse domains have run handlers for (diagnostics). *)
let frames_processed () = Atomic.get frames_processed

(** [submit] failures: frames the caller parsed inline because the queue was full or no
    handler was registered. A non-zero rate means a parse domain is saturated and hot
    paths are paying parse cost (diagnostics). *)
let fallbacks () = Atomic.get submit_fallbacks

(** Current queued frames across all shards (diagnostics). *)
let queue_length () =
  Mutex.lock shards_mutex;
  let all = Hashtbl.fold (fun _ shard acc -> shard :: acc) shards [] in
  Mutex.unlock shards_mutex;
  List.fold_left
    (fun total shard ->
      Mutex.lock shard.mutex;
      let n = Queue.length shard.queue in
      Mutex.unlock shard.mutex;
      total + n)
    0
    all
;;

(** Peak queue depth seen across all shards (diagnostics). *)
let high_water () =
  Mutex.lock shards_mutex;
  let all =
    Hashtbl.fold (fun _ shard acc -> Atomic.get shard.high_water :: acc) shards []
  in
  Mutex.unlock shards_mutex;
  List.fold_left max 0 all
;;

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

(** Submit a raw [payload] for [venue] to its parse domain.
    @return
      [true] if queued; [false] if the queue is full OR no decoder has been registered for
      [venue] yet. Either way the caller MUST decode inline: a [false] never means the
      frame was handled. *)
let submit_frame venue payload =
  let name = venue_handler_name venue in
  if StringMap.mem name (Atomic.get handlers) then submit name payload else false
;;
