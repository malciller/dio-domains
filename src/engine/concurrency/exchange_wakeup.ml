(** Exchange Wakeup

    Per-symbol wakeup mechanism that allows domain workers to block until
    exchange data arrives for their assigned symbol, avoiding unnecessary
    wakeups across unrelated symbols.

    Exchange-specific modules call [signal ~symbol] AFTER writing new data
    for that symbol. Domain workers capture [get_generation ~symbol] at the
    top of a work cycle and call [wait_since ~symbol ~since] at the bottom:
    the call returns immediately if any signal arrived while the cycle ran,
    and otherwise parks until the next one. This closes the classic
    check-then-sleep lost-wakeup race where a signal landing between the
    last data read and the park was silently dropped (Condition.signal is
    not sticky), which previously stalled a quiet symbol's domain until an
    unrelated event happened to arrive.

    Protocol rules (both sides MUST follow them):
    - Producers: write data first, then [signal]. The signal bumps a
      monotonic per-symbol generation counter under the symbol mutex and
      fires the condition variable.
    - Consumers: capture the generation BEFORE reading producer state, do
      the work, then [wait_since]. A generation greater than the captured
      baseline proves data was written during the cycle, so the wait cannot
      sleep through it.

    [wait_since] spins briefly on the lock-free atomic counter before
    parking on the condition variable. Signals landing inside the spin
    window are absorbed without a futex wake/sleep round-trip, removing
    kernel scheduler latency from the common case.

    [signal_all] broadcasts to all waiting workers for cross-cutting events
    such as shutdown or snapshot completion.
*)

type symbol_sync =
  { mutex : Mutex.t
  ; condition : Condition.t
  ; generation : int Atomic.t (** Monotonic count of signals ever sent. *)
  }

(** Immutable map of known symbol sync records published through a single
    atomic cell. Reads ([get_sync] fast path) are lock-free and allocation-
    free; inserts CAS-replace the map. Entries are never removed, so a
    reader either sees a record or inserts it - no resize/torn-read hazard,
    unlike the previous raw Hashtbl read outside the registry mutex. *)
module SymbolMap = Map.Make (String)

let syncs : symbol_sync SymbolMap.t Atomic.t = Atomic.make SymbolMap.empty
let registry_mutex = Mutex.create ()

let[@inline] get_sync symbol =
  match SymbolMap.find_opt symbol (Atomic.get syncs) with
  | Some s -> s
  | None ->
    Mutex.lock registry_mutex;
    let s =
      (* Re-check under the lock so concurrent first-signallers share one
         record. Publish via CAS so readers never observe a stale map. *)
      let rec insert () =
        let current = Atomic.get syncs in
        match SymbolMap.find_opt symbol current with
        | Some s -> s
        | None ->
          let fresh =
            { mutex = Mutex.create ()
            ; condition = Condition.create ()
            ; generation = Atomic.make 0
            }
          in
          if Atomic.compare_and_set syncs current (SymbolMap.add symbol fresh current)
          then fresh
          else insert ()
      in
      insert ()
    in
    Mutex.unlock registry_mutex;
    s
;;

(** Lock-free read of the symbol's current generation. Consumers use this
    as the baseline argument to [wait_since]. *)
let[@inline] get_generation ~symbol = Atomic.get (get_sync symbol).generation

(** Signals the condition variable for [symbol], waking the domain worker
    blocked on that symbol. The generation bump happens under the symbol
    mutex so it can never interleave with a waiter's predicate re-check
    (the standard condition-variable discipline that makes the signal
    impossible to lose). Callers must have finished writing the data the
    signal advertises BEFORE calling this. *)
let signal ~symbol =
  let sync = get_sync symbol in
  Mutex.lock sync.mutex;
  Atomic.set sync.generation (Atomic.get sync.generation + 1);
  Condition.signal sync.condition;
  Mutex.unlock sync.mutex
;;

(** Signals all per-symbol condition variables. Used for events that require
    waking every waiting worker, such as shutdown or snapshot completion.
    Acquires per-symbol mutexes sequentially, guaranteeing no cross-symbol
    contention. *)
let signal_all () =
  let all_syncs = SymbolMap.fold (fun _ sync acc -> sync :: acc) (Atomic.get syncs) [] in
  List.iter
    (fun sync ->
       Mutex.lock sync.mutex;
       Atomic.set sync.generation (Atomic.get sync.generation + 1);
       Condition.signal sync.condition;
       Mutex.unlock sync.mutex)
    all_syncs
;;

(* Spin iterations before parking. Tuned to roughly one microsecond on
   server-class hardware: long enough to absorb a producer racing ahead of
   the waiter, short enough that an idle park costs negligible CPU. *)
let default_spin_iterations = 400

(** Blocks until the symbol's generation exceeds [since], i.e. until any
    producer signals this symbol after the caller captured its baseline.
    Returns immediately (without taking any lock) when a signal already
    arrived between the baseline capture and this call - the property that
    makes the surrounding produce/consume cycle race-free.

    Phase 1 spins on the atomic generation counter; phase 2 takes the
    symbol mutex and parks on the condition variable with a predicate loop
    (spurious wakeups and signals fired before the mutex was acquired are
    both handled by re-checking the generation under the lock). *)
let wait_since ~symbol ~since =
  let sync = get_sync symbol in
  let rec spin i =
    if Atomic.get sync.generation <> since
    then ()
    else if i <= 0
    then (
      (* Park phase: acquire the symbol mutex BEFORE the predicate loop -
         [Condition.wait] requires the caller to hold it. Violating that is
         UB: glibc happens to tolerate it, macOS raises EPERM on the
         eventual unlock (caught by the concurrency regression tests). *)
      Mutex.lock sync.mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock sync.mutex)
        (fun () ->
           while Atomic.get sync.generation = since do
             Condition.wait sync.condition sync.mutex
           done))
    else spin (i - 1)
  in
  spin default_spin_iterations
;;
