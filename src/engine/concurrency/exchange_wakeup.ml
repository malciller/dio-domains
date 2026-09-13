(** Per-symbol wakeup mechanism.

    Domain workers block until exchange data arrives for their assigned symbol,
    avoiding cross-symbol wakeups.

    Protocol (both sides MUST follow):
    - Producers: write data, then [signal ~symbol]. The signal increments a
      monotonic per-symbol generation counter under the symbol mutex and fires
      the condition variable.
    - Consumers: capture [get_generation ~symbol] BEFORE reading producer state,
      do the work, then call [wait_since ~symbol ~since]. A generation greater
      than the baseline proves data was written during the cycle, so the wait
      cannot sleep through it. This closes the check-then-sleep lost-wakeup
      race (Condition.signal is not sticky).

    [wait_since] spins briefly on the atomic counter before parking on the
    condition variable; signals inside the spin window are absorbed without a
    futex wake/sleep round-trip.

    [signal_all] broadcasts to all waiting workers for cross-cutting events
    such as shutdown or snapshot completion. *)

type symbol_sync =
  { mutex : Mutex.t
  ; condition : Condition.t
  ; generation : int Atomic.t (** Monotonic count of signals ever sent. *)
  }

(** Immutable map of symbol sync records in one atomic cell. The [get_sync] fast
    path is lock-free and allocation-free; inserts CAS-replace the map. Entries
    are never removed: a reader either sees a record or inserts one, so no
    resize or torn read is possible. *)
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
         record; publish via CAS so readers never see a stale map. *)
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

type sync_handle = symbol_sync

let get_sync_handle = get_sync
let[@inline] get_generation_fast (sync : sync_handle) = Atomic.get sync.generation

(** Lock-free read of the symbol's current generation; baseline for [wait_since]. *)
let[@inline] get_generation ~symbol = get_generation_fast (get_sync symbol)

(** Signal [symbol]'s condition variable, waking its blocked worker. The
    generation bump occurs under the symbol mutex, so it cannot interleave with
    a waiter's predicate re-check. Callers MUST finish writing the signalled
    data before calling. *)
let signal ~symbol =
  let sync = get_sync symbol in
  Mutex.lock sync.mutex;
  Atomic.set sync.generation (Atomic.get sync.generation + 1);
  Condition.signal sync.condition;
  Mutex.unlock sync.mutex
;;

(** Signal every per-symbol condition variable for cross-cutting events such as
    shutdown or snapshot completion. Acquires per-symbol mutexes sequentially. *)
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

(* Spin iterations before parking. Each iteration is one atomic read; the spin
   is a latency optimization only. A shorter spin costs at most one futex
   wake/sleep round-trip in the rare race case. *)
let default_spin_iterations = 100

(** Fast-path [wait_since] over a pre-resolved [sync_handle]; avoids map
    traversal on the hot domain cycle. *)
let wait_since_fast (sync : sync_handle) ~since =
  let rec spin i =
    if Atomic.get sync.generation <> since
    then ()
    else if i <= 0
    then (
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

(** Block until the symbol's generation exceeds [since]. Returns immediately
    without locking when a signal arrived after the baseline capture.
    Phase 1 spins on the atomic generation; phase 2 parks on the condition
    variable under the symbol mutex with a predicate loop, which handles
    spurious wakeups and signals fired before the mutex was acquired. *)
let wait_since ~symbol ~since =
  let sync = get_sync symbol in
  wait_since_fast sync ~since
;;
