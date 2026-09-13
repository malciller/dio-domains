(** Dedicated background domain for parsing high-rate feed frames off the Lwt
    scheduler thread.

    The main domain's Lwt event loop handles all venues' WebSocket traffic.
    Full-JSON parsing per frame (e.g. Kraken's v2 book channel) is
    allocation-heavy and adds latency jitter to every other fiber. This module
    moves such parsing onto one dedicated OCaml 5 domain fed by a bounded queue.

    Contract:
    - Producers call [submit ~handler payload] from any thread/domain. Returns
      [false] when the queue is full; the caller MUST then handle the payload
      synchronously (inline fallback). Dropping is not an option for incremental
      feeds (Kraken book deltas): a lost delta desyncs the local book until the
      next snapshot, so the fallback preserves correctness under overload at the
      cost of the offload benefit.
    - Handlers run sequentially on the parse domain, preserving per-venue frame
      order. They MUST NOT touch Lwt primitives (promises, streams,
      [Lwt_condition], [Lwt_mvar]): those are single-domain. Mutexes, Condition
      variables, Atomics and Logging are domain-safe.
    - One handler per name; register before first use via [register].

    The queue is a stdlib [Queue] guarded by one mutex, not a lock-free
    structure: each producer's critical section is ~100ns against a ~10-50us
    parse, so a lock-free queue offers no measurable win. *)

(* OxCaml marks [Domain.spawn] as [do_not_spawn_domains]. This module spawns one
   bounded, config-gated parse domain, not an unbounded fan-out, so the GC
   concern the alert describes does not apply. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

type handler = string -> unit

module StringMap = Map.Make (String)

let handlers : handler StringMap.t Atomic.t = Atomic.make StringMap.empty

(** Register [handler] under [name]. Call during module/startup init,
    before any frame can arrive. *)
let register name handler =
  let rec loop () =
    let current = Atomic.get handlers in
    if Atomic.compare_and_set handlers current (StringMap.add name handler current)
    then ()
    else loop ()
  in
  loop ()
;;

(* Work queue: (handler name, raw frame). Bounded; overflow is reported via
   [submit] -> false, so no frame is dropped silently. *)
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

(* Observability counters for diagnostics/dashboards. *)
let batches_drained = Atomic.make 0
let worker_started = Atomic.make false
let max_batch = 64

(** Worker body: block until signalled, drain up to [max_batch] frames, repeat.
    Runs until process exit. *)
let worker_loop () =
  while true do
    (* Block until a producer signals pending work. The size re-check under
       the mutex makes this race-free against concurrent submissions. *)
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
    ignore (Atomic.fetch_and_add batches_drained 1)
  done
;;

(** Spawn the worker domain on first submit, lazily, so library load order does
    not determine GC-config exposure for domains spawned after
    [Config.apply_gc_config]. *)
let ensure_worker () =
  if Atomic.compare_and_set worker_started false true
  then ignore (Domain.spawn worker_loop)
;;

(** Submit a raw frame for asynchronous parsing.
    @return [true] if queued, [false] if the queue is full (caller must process
    synchronously). *)
let submit handler payload =
  ensure_worker ();
  Mutex.lock queue_mutex;
  let ok =
    if Queue.length queue >= queue_capacity
    then false
    else (
      Queue.push (handler, payload) queue;
      true)
  in
  if ok then Condition.signal queue_condition;
  Mutex.unlock queue_mutex;
  ok
;;

(** Number of batch drains completed (diagnostics). *)
let stats () = Atomic.get batches_drained
