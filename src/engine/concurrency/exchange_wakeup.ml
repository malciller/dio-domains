(** Per-symbol wakeup mechanism.

    Domain workers block until exchange data arrives for their assigned symbol, avoiding
    cross-symbol wakeups.

    Protocol (both sides MUST follow):
    - Producers: write data, then [signal ~symbol]. The signal increments a monotonic
      per-symbol generation counter under the symbol mutex and fires the condition
      variable.
    - Consumers: capture [get_generation ~symbol] BEFORE reading producer state, do the
      work, then call [wait_since ~symbol ~since]. A generation greater than the baseline
      proves data was written during the cycle, so the wait cannot sleep through it. This
      closes the check-then-sleep lost-wakeup race (Condition.signal is not sticky).

    [wait_since] spins briefly on the atomic counter before parking on the condition
    variable; signals inside the spin window are absorbed without a futex wake/sleep
    round-trip.

    [signal_all] broadcasts to all waiting workers for cross-cutting events such as
    shutdown or snapshot completion. *)

type symbol_sync =
  { mutex : Mutex.t
  ; condition : Condition.t
  ; generation : int Atomic.t (** Monotonic count of signals ever sent. *)
  ; signal_ns : int Atomic.t
      (** Monotonic time of the last signal, written by the producer. The consumer reads it
          on resume, so [now - signal_ns] is the wake cost (futex round-trip plus
          scheduling) that no stage span covers. 0 before the first signal. *)
  }

(** Immutable map of symbol sync records in one atomic cell. The [get_sync] fast path is
    lock-free and allocation-free; inserts CAS-replace the map. Entries are never removed:
    a reader either sees a record or inserts one, so no resize or torn read is possible. *)
module SymbolMap = Map.Make (String)

let syncs : symbol_sync SymbolMap.t Atomic.t = Atomic.make SymbolMap.empty
let registry_mutex = Mutex.create ()

let[@inline] get_sync symbol =
  match SymbolMap.find_opt symbol (Atomic.get syncs) with
  | Some s -> s
  | None ->
    Mutex.lock registry_mutex;
    let s =
      (* Re-check under the lock so concurrent first-signallers share one record; publish
         via CAS so readers never see a stale map. *)
      let rec insert () =
        let current = Atomic.get syncs in
        match SymbolMap.find_opt symbol current with
        | Some s -> s
        | None ->
          let fresh =
            { mutex = Mutex.create ()
            ; condition = Condition.create ()
            ; generation = Atomic.make 0
            ; signal_ns = Atomic.make 0
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

(** Wake aliases: signalling one key also wakes the listed keys. A strategy that trades a
    venue's perp runs inside the domain of the instrument it hedges (a Kraken spot pair,
    say), but the events that must move it come from the perp book on another venue. The
    owner registers the alias once; the feed then wakes the trading domain without knowing
    anything about it.

    The map is immutable and published through one atomic cell, so the signal path stays
    lock-free in the common case (no alias) and allocation-free in every case. *)
let aliases : string list SymbolMap.t Atomic.t = Atomic.make SymbolMap.empty

let add_alias ~symbol ~target =
  if not (String.equal symbol target)
  then (
    let rec loop () =
      let current = Atomic.get aliases in
      let existing =
        match SymbolMap.find_opt symbol current with
        | Some l -> l
        | None -> []
      in
      if List.mem target existing
      then ()
      else (
        let next =
          SymbolMap.add symbol (target :: existing) current
        in
        if not (Atomic.compare_and_set aliases current next) then loop ())
    in
    loop ())
;;

let[@inline] aliases_of symbol = SymbolMap.find_opt symbol (Atomic.get aliases)

let bump (sync : symbol_sync) =
  Mutex.lock sync.mutex;
  Atomic.set sync.generation (Atomic.get sync.generation + 1);
  (* Timestamp inside the critical section, so a consumer that sees the new generation
     always sees a timestamp at least as new. One non-allocating clock read per frame. *)
  Atomic.set sync.signal_ns (Monotonic_clock.now_ns ());
  Condition.signal sync.condition;
  Mutex.unlock sync.mutex
;;

(** Monotonic time of [sync]'s last signal. Paired with the time the consumer resumes, this
    is the wake latency: the part of tick-to-decision that happens between the producer
    publishing and the domain running again, which no measured cycle span covers. *)
let[@inline] last_signal_ns_fast (sync : sync_handle) = Atomic.get sync.signal_ns

(** Signal [symbol]'s condition variable, waking its blocked worker, and any domain aliased
    to it. The generation bump occurs under the symbol mutex, so it cannot interleave with a
    waiter's predicate re-check. Callers MUST finish writing the signalled data before
    calling. *)
let signal ~symbol =
  bump (get_sync symbol);
  match aliases_of symbol with
  | None -> ()
  | Some targets -> List.iter (fun target -> bump (get_sync target)) targets
;;

(** Signal every per-symbol condition variable for cross-cutting events such as shutdown
    or snapshot completion. Acquires per-symbol mutexes sequentially. *)
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

(* Spin iterations before parking. Each iteration is one atomic read; the spin is a
   latency optimization only. A shorter spin costs at most one futex wake/sleep round-trip
   in the rare race case. *)
let default_spin_iterations = 100

(** Spin budget before parking, in atomic-read iterations (~10ns each). Tunable so the
    trade-off between a futex round-trip and a busy core can be measured on the host
    without a rebuild; 0 parks immediately. *)
let spin_iterations = Atomic.make default_spin_iterations

let spin_budget () = Atomic.get spin_iterations

(** [DIO_BUSY_SPIN=1] keeps the domain spinning on its core instead of parking on the
    condition variable. A signal always bumps the generation, so shutdown still wakes a
    busy-spinning domain. Read after dotenv by [configure]. *)
let spin_config : bool Atomic.t = Atomic.make false

let busy_spin_enabled () = Atomic.get spin_config

(** True when a spinning domain on every trading CPU would only ever contend with
    itself: the CPUs are in the kernel's isolated set, so the scheduler will not place other
    runnable threads there. *)
let spin_safe () =
  (* The allocator hands out P-cores plus their SMT siblings, so checking only the
     primaries leaves a spinner on a core the scheduler still fills. *)
  match Thread_affinity.trading_allocation_order () with
  | [] -> false
  | cpus ->
    List.for_all
      (fun cpu -> Thread_affinity.is_isolated cpu)
      cpus
;;

(** Read [DIO_BUSY_SPIN]. Must run after [Logging.load_dotenv]. [auto] enables the spin only
    when the trading CPUs are isolated, so turning it on cannot silently recreate the
    contention that made it slower than parking. *)
let configure () =
  let busy =
    match Sys.getenv_opt "DIO_BUSY_SPIN" with
    | Some s ->
      (match String.lowercase_ascii (String.trim s) with
       | "1" | "true" | "on" | "yes" -> true
       | "auto" -> spin_safe ()
       | _ -> false)
    | None -> false
  in
  if busy && not (spin_safe ())
  then
    Logging.warn
      ~section:"exchange_wakeup"
      "DIO_BUSY_SPIN requested but the trading cpus are not isolated; parking instead";
  (* [auto] refuses to spin without isolation; an explicit opt-in is honoured, since a
     cpuset-based isolation is not visible in /sys/devices/system/cpu/isolated. *)
  Atomic.set spin_config busy;
  (match Sys.getenv_opt "DIO_SPIN_ITERS" with
   | Some s ->
     (try Atomic.set spin_iterations (max 0 (int_of_string (String.trim s))) with
      | _ -> ())
   | None -> ())
;;

(** Fast-path [wait_since] over a pre-resolved [sync_handle]; avoids map traversal on the
    hot domain cycle. *)
let wait_since_fast (sync : sync_handle) ~since =
  if Atomic.get spin_config
  then
    (* Dedicated-core mode: never park. [signal]/[signal_all] bump the generation, so a
       stop request still breaks the loop. *)
    while Atomic.get sync.generation = since do
      Domain.cpu_relax ()
    done
  else (
    let budget = spin_budget () in
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
    spin budget)
;;

(** Block until the symbol's generation exceeds [since]. Returns immediately without
    locking when a signal arrived after the baseline capture. Phase 1 spins on the atomic
    generation; phase 2 parks on the condition variable under the symbol mutex with a
    predicate loop, which handles spurious wakeups and signals fired before the mutex was
    acquired. *)
let wait_since ~symbol ~since =
  let sync = get_sync symbol in
  wait_since_fast sync ~since
;;
