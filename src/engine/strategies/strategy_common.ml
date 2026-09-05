(** Common types and infrastructure shared across all trading strategies.
    Provides unified order representation, in-flight deduplication caches,
    a ring buffer for order queuing, and the strategy module signature. *)

open Lwt.Infix
module StringMap = Map.Make (String)

(** Per-symbol trading parameters parsed from config.json. *)
type trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float * float
    (** (min, max) grid interval percentages; resolved to equal bounds when a scalar is provided *)
  ; sell_mult : string
  ; min_usd_balance : string option
  ; max_exposure : string option
  ; strategy : string
  ; maker_fee : float option
  ; taker_fee : float option
  ; testnet : bool
  ; hedge : bool
  ; accumulation_buffer : float * float
    (** (min, max) quote profit buffer; interpolated at runtime via Fear and Greed index *)
  ; data_feed : string option
  ; base_accumulation : bool
    (** Per-strategy opt-in to base-accumulation persistence (default true
        when absent from config.json; disabled means zero I/O). *)
  ; sell_levels : bool
    (** Per-strategy opt-in to pending-sell-level persistence (default true
        when absent from config.json). *)
  }

(** Integer userref tags for per-strategy order grouping on the exchange. *)
let strategy_userref_grid = 1 (* grid strategy *)

let strategy_userref_mm = 2 (* market maker strategy *)

(** Returns true if [order_userref] matches the given [strategy_userref]. *)
let is_strategy_order strategy_userref order_userref = strategy_userref = order_userref

(** Order side: buy or sell. *)
type order_side =
  | Buy
  | Sell

let string_of_order_side = function
  | Buy -> "buy"
  | Sell -> "sell"
;;

(** Operation type for a strategy order. *)
type operation_type =
  | Place (* submit a new order *)
  | Amend (* modify an existing order *)
  | Cancel (* cancel an existing order *)

let string_of_operation_type = function
  | Place -> "place"
  | Amend -> "amend"
  | Cancel -> "cancel"
;;

(** Discriminant for the strategy that produced an order. *)
type strategy_id =
  | Ladder
  | MM
  | Hedger

let string_of_strategy_id = function
  | Ladder -> "Ladder"
  | MM -> "MM"
  | Hedger -> "Hedger"
;;

(** Unified order record emitted by all strategies. *)
type strategy_order =
  { operation : operation_type (* place, amend, or cancel *)
  ; order_id : string option (* target order ID for amend/cancel; None for place *)
  ; symbol : string
  ; side : order_side
  ; order_type : string (* e.g. "limit", "market" *)
  ; qty : float
  ; price : float option
  ; time_in_force : string (* e.g. "GTC", "IOC", "FOK" *)
  ; post_only : bool
  ; userref : int option (* exchange-level strategy tag *)
  ; strategy : strategy_id (* originating strategy variant *)
  ; exchange : string (* target exchange *)
  ; duplicate_key : string (* composite key for deduplication *)
  }

(** Build a composite deduplication key from order parameters.
    Uses string concatenation to avoid intermediate buffer allocations. *)
let generate_duplicate_key symbol side quantity limit_price =
  let q_str = string_of_float quantity in
  match limit_price with
  | Some p -> symbol ^ "|" ^ side ^ "|" ^ q_str ^ "|" ^ string_of_float p
  | None -> symbol ^ "|" ^ side ^ "|" ^ q_str ^ "|market"
;;

(** Per-symbol strategy order-action counters.

    Incremented each time a strategy successfully pushes an order action
    (place/amend/cancel) to a ring buffer; snapshot+reset once per latency
    window by the domain worker so the dashboard's STRAT/S column reports
    ACTUAL strategy executions per second, not raw strategy-invocation
    cycles, which for a fast feed are far higher than the real number of
    order actions the strategy takes. *)
module Order_actions = struct
  let counters : (string, int Atomic.t) Hashtbl.t = Hashtbl.create 16
  let mutex = Mutex.create ()

  let incr symbol =
    Mutex.lock mutex;
    let c =
      match Hashtbl.find_opt counters symbol with
      | Some c -> c
      | None ->
        let c = Atomic.make 0 in
        Hashtbl.add counters symbol c;
        c
    in
    Atomic.incr c;
    Mutex.unlock mutex
  ;;

  (** Return and zero the count for [symbol] since the last reset. *)
  let snapshot_and_reset symbol =
    Mutex.lock mutex;
    let n =
      match Hashtbl.find_opt counters symbol with
      | Some c ->
        Hashtbl.remove counters symbol;
        Atomic.get c
      | None -> 0
    in
    Mutex.unlock mutex;
    n
  ;;
end

(** In-flight order cache for deduplication of pending place/cancel requests.

    Sharded (HFT_AUDIT.md H4): the registry used to be ONE Hashtbl behind ONE
    global mutex shared by every symbol and every domain, the single most
    cross-cutting lock in the engine. Now the table and its mutex are split
    across [num_shards] independent shards keyed by hash of the duplicate key
    (which embeds the symbol), so independent symbols/domains no longer
    serialize on one lock; the common case locks only the key's own shard. *)
module InFlightOrders = struct
  let num_shards = 64

  let registries : (string, float) Hashtbl.t array =
    Array.init num_shards (fun _ -> Hashtbl.create 128)
  ;;

  let shard_mutexes : Mutex.t array = Array.init num_shards (fun _ -> Mutex.create ())
  let shard_of key = Hashtbl.hash key land (num_shards - 1)

  (** Atomically insert [duplicate_key] if absent. Returns true on insertion,
      false if the key was already present. *)
  let add_in_flight_order duplicate_key =
    let now = Unix.gettimeofday () in
    let shard = shard_of duplicate_key in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry duplicate_key in
    if not exists then Hashtbl.replace registry duplicate_key now;
    Mutex.unlock mutex;
    not exists
  ;;

  (** Remove [duplicate_key] from the cache. Returns true if it was present. *)
  let remove_in_flight_order duplicate_key =
    let shard = shard_of duplicate_key in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry duplicate_key in
    if exists then Hashtbl.remove registry duplicate_key;
    Mutex.unlock mutex;
    exists
  ;;

  (** Check if [duplicate_key] is present in the cache. *)
  let is_in_flight duplicate_key =
    let shard = shard_of duplicate_key in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry duplicate_key in
    Mutex.unlock mutex;
    exists
  ;;

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    let total = ref 0 in
    for s = 0 to num_shards - 1 do
      let mutex = shard_mutexes.(s) in
      Mutex.lock mutex;
      total := !total + Hashtbl.length registries.(s);
      Mutex.unlock mutex
    done;
    !total
  ;;

  let last_cleanup = Atomic.make 0.0

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age = 60.0) () =
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_cleanup in
    if now -. last > 1.0 && Atomic.compare_and_set last_cleanup last now
    then (
      let removed = ref 0 in
      for s = 0 to num_shards - 1 do
        let mutex = shard_mutexes.(s) in
        Mutex.lock mutex;
        let registry = registries.(s) in
        let initial_size = Hashtbl.length registry in
        Hashtbl.filter_map_inplace
          (fun _ timestamp ->
             if now -. timestamp <= max_age then Some timestamp else None)
          registry;
        removed := !removed + (initial_size - Hashtbl.length registry);
        Mutex.unlock mutex
      done;
      0, !removed)
    else 0, 0
  ;;

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
    let drift, trimmed = cleanup () in
    Some (Some drift, Some trimmed)
  ;;
end

(** In-flight amendment lifecycle registry: deduplication of pending amend
    requests PLUS the exchange's mid-amend order-replacement events.

    Exchanges implement amendments differently - Kraken modifies the order in
    place (same id), Hyperliquid and Alpaca replace it under the hood (the old
    order is cancelled and a new id is created). A replacement therefore emits
    a cancel event for the OLD id, either while the amend request is pending or
    shortly after it completes (event ordering on the wire). This registry
    gives every exchange the same lifecycle so strategies react uniformly:

    - [Pending] while the request is in flight: a cancel event for the old id
      is the amend's side effect, not a real cancellation.
    - [Replaced new_id] once the exchange confirms (old_id <> new_id): the
      entry is retained for the cleanup window so a LATE cancel event for the
      old id is still recognized as the amend's side effect and cannot reset
      the replacement order's tracking.
    - Same-id amends (Kraken) do not retain an entry: events for that id are
      always real.
    - [Failed]/[Skipped] are terminal: the entry is dropped so a follow-up
      cancel event is handled as a real one (the failure path already
      reconciled tracking). *)
module InFlightAmendments = struct
  type phase =
    | Pending
    | Replaced of string (* the replacement (new) order id *)
    | Failed of string (* terminal: reason; entry dropped *)
    | Skipped (* terminal: suppressed no-op; entry dropped *)

  type entry =
    { mutable phase : phase
    ; mutable last : float
    }

  (* Sharded like InFlightOrders (HFT_AUDIT.md H4): no single global mutex
     across all symbols/domains. Keyed by hash of the order id. *)
  let num_shards = 64

  let registries : (string, entry) Hashtbl.t array =
    Array.init num_shards (fun _ -> Hashtbl.create 128)
  ;;

  let shard_mutexes : Mutex.t array = Array.init num_shards (fun _ -> Mutex.create ())
  let shard_of key = Hashtbl.hash key land (num_shards - 1)

  (** Atomically insert [order_id] as [Pending] if absent. Returns true on
      insertion, false if the id is already tracked (any phase - a replaced
      id stays tracked for the cleanup window, so re-amending it is a
      duplicate). *)
  let add_in_flight_amendment order_id =
    let now = Unix.gettimeofday () in
    let shard = shard_of order_id in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    if not exists then Hashtbl.replace registry order_id { phase = Pending; last = now };
    Mutex.unlock mutex;
    not exists
  ;;

  (** Read the phase of [order_id], or [None] when untracked. *)
  let phase_of order_id =
    let shard = shard_of order_id in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let phase =
      match Hashtbl.find_opt registry order_id with
      | Some (entry : entry) -> Some entry.phase
      | None -> None
    in
    Mutex.unlock mutex;
    phase
  ;;

  (** Returns true if [order_id] has a pending (unanswered) amendment. *)
  let is_in_flight order_id = phase_of order_id = Some Pending

  (** Returns true if [order_id] was just replaced by an amendment and its
      old id is still in the recognition window. *)
  let is_superseded order_id =
    match phase_of order_id with
    | Some (Replaced _) -> true
    | _ -> false
  ;;

  (** True while the exchange may still deliver events for [order_id] as a
      side effect of an amendment (request pending, or replacement just
      completed): a cancel event in this state must not reset tracking. *)
  let is_amend_lifecycle_active order_id =
    match phase_of order_id with
    | Some (Pending | Replaced _) -> true
    | _ -> false
  ;;

  (** Remove [order_id] from the registry (terminal phases, cleanup).
      Returns true if it was present. *)
  let remove_in_flight_amendment order_id =
    let shard = shard_of order_id in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    if exists then Hashtbl.remove registry order_id;
    Mutex.unlock mutex;
    exists
  ;;

  (** The exchange confirmed the amendment. A replace (old_id <> new_id)
      keeps the old id registered as [Replaced] for the cleanup window so a
      late cancel event for it is recognized as the amend's side effect; a
      same-id amend (Kraken) drops the entry - its events are always real. *)
  let note_amendment_succeeded ~old_id ~new_id =
    if old_id = new_id
    then ignore (remove_in_flight_amendment old_id)
    else (
      let now = Unix.gettimeofday () in
      let shard = shard_of old_id in
      let registry = registries.(shard) in
      let mutex = shard_mutexes.(shard) in
      Mutex.lock mutex;
      (match Hashtbl.find_opt registry old_id with
       | Some (entry : entry) ->
         entry.phase <- Replaced new_id;
         entry.last <- now
       | None -> Hashtbl.replace registry old_id { phase = Replaced new_id; last = now });
      Mutex.unlock mutex)
  ;;

  (** The exchange rejected the amendment: terminal. The entry is dropped so
      a follow-up cancel event for the old id is handled as a real one (the
      amend-failure path has already reconciled tracking). *)
  let note_amendment_failed ~old_id ~reason:_ = ignore (remove_in_flight_amendment old_id)

  (** The amendment was suppressed as a no-op: terminal, entry dropped. *)
  let note_amendment_skipped ~old_id = ignore (remove_in_flight_amendment old_id)

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    let total = ref 0 in
    for s = 0 to num_shards - 1 do
      let mutex = shard_mutexes.(s) in
      Mutex.lock mutex;
      total := !total + Hashtbl.length registries.(s);
      Mutex.unlock mutex
    done;
    !total
  ;;

  let last_cleanup = Atomic.make 0.0

  (** Reap only TERMINAL leftovers older than [max_age] seconds - the
      [Replaced] recognition window is bounded by this cleanup. [Pending]
      entries are owned by an in-flight REST request and are always resolved
      by exactly one guaranteed terminal event (Amended/Amendment_skipped/
      Amendment_failed) or a recognized cancel; aging one out here would
      drop the amend-recognition window while the exchange still owns the
      request, so a mid-flight cancel event would reset tracking as if it
      were real. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age = 60.0) () =
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_cleanup in
    if now -. last > 1.0 && Atomic.compare_and_set last_cleanup last now
    then (
      let removed = ref 0 in
      for s = 0 to num_shards - 1 do
        let mutex = shard_mutexes.(s) in
        Mutex.lock mutex;
        let registry = registries.(s) in
        let initial_size = Hashtbl.length registry in
        Hashtbl.filter_map_inplace
          (fun _ (entry : entry) ->
             match entry.phase with
             | Pending -> Some entry
             | Replaced _ | Failed _ | Skipped ->
               if now -. entry.last <= max_age then Some entry else None)
          registry;
        removed := !removed + (initial_size - Hashtbl.length registry);
        Mutex.unlock mutex
      done;
      0, !removed)
    else 0, 0
  ;;

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
    let drift, trimmed = cleanup () in
    Some (Some drift, Some trimmed)
  ;;
end

(** Fixed-size, zero-allocation MPSC ring buffer.
    Replaces the Michael-Scott queue to eliminate node allocations on the hot path.
    Uses padding to prevent false sharing between producer and consumer domains. *)
module LockFreeQueue = struct
  type 'a t =
    { array : 'a option Atomic.t array
    ; size : int
    ; mask : int
    ; head : int Atomic.t
    ; _pad1 : int64
    ; _pad2 : int64
    ; _pad3 : int64
    ; _pad4 : int64
    ; _pad5 : int64
    ; _pad6 : int64
    ; _pad7 : int64
    ; tail : int Atomic.t
    }

  let create ?(size = 65536) () =
    (* Ensure size is a power of 2 for fast masking *)
    let rec next_power_of_2 v p = if p >= v then p else next_power_of_2 v (p * 2) in
    let size = next_power_of_2 size 1 in
    let mask = size - 1 in
    let array = Array.init size (fun _ -> Atomic.make None) in
    { array
    ; size
    ; mask
    ; head = Atomic.make 0
    ; _pad1 = 0L
    ; _pad2 = 0L
    ; _pad3 = 0L
    ; _pad4 = 0L
    ; _pad5 = 0L
    ; _pad6 = 0L
    ; _pad7 = 0L
    ; tail = Atomic.make 0
    }
  ;;

  (** Concurrent enqueue for multiple producers. Returns None if full. *)
  let write q v =
    let rec loop () =
      let t = Atomic.get q.tail in
      let h = Atomic.get q.head in
      if t - h >= q.size
      then None (* Queue is full *)
      else if Atomic.compare_and_set q.tail t (t + 1)
      then (
        Atomic.set q.array.(t land q.mask) (Some v);
        Some ())
      else loop ()
    in
    loop ()
  ;;

  (** Single consumer dequeue. Non-blocking: returns None if the slot is
      empty, even if the tail index has been advanced by a producer.
      This prevents the consumer from spinning if a producer is preempted. *)
  let read q =
    let h = Atomic.get q.head in
    if h = Atomic.get q.tail
    then None (* Queue is empty *)
    else (
      let slot = q.array.(h land q.mask) in
      match Atomic.get slot with
      | None -> None (* Producer claimed slot but hasn't written yet; don't spin *)
      | Some v ->
        Atomic.set slot None;
        Atomic.set q.head (h + 1);
        Some v)
  ;;

  (** O(1) size tracking. Safe for hot loops. *)
  let size q =
    let count = Atomic.get q.tail - Atomic.get q.head in
    if count < 0 then 0 else count
  ;;

  let read_batch q max_items =
    let rec read_n acc n =
      if n >= max_items
      then List.rev acc
      else (
        match read q with
        | Some item -> read_n (item :: acc) (n + 1)
        | None -> List.rev acc)
    in
    read_n [] 0
  ;;
end

(** Domain-safe order signal channel.
    Domain workers call [broadcast ()] to notify the supervisor's Lwt event loop
    that new orders are available. Implemented via a Unix self-pipe so that a
    single-byte write from any domain wakes the Lwt scheduler without touching
    Lwt internals (Lwt_condition is NOT safe from non-Lwt domains).

    The [pending] atomic flag coalesces multiple rapid broadcasts into a single
    pipe write to avoid saturating the pipe buffer under high order throughput. *)
module OrderSignal = struct
  let read_fd, write_fd =
    let r, w = Unix.pipe ~cloexec:true () in
    Unix.set_nonblock r;
    Unix.set_nonblock w;
    r, w
  ;;

  (** Lwt wrapper for the read end of the self-pipe. Created once at module
      init to avoid per-wait allocation. *)
  let lwt_read_fd = Lwt_unix.of_unix_file_descr ~blocking:false ~set_flags:false read_fd

  (** Atomic flag to coalesce multiple broadcasts into one pipe write. *)
  let pending = Atomic.make false

  (** Signal from any domain that new orders are available.
      Safe to call from OCaml 5 domain workers, no Lwt internals are touched. *)
  let broadcast () =
    if not (Atomic.exchange pending true)
    then (
      (* First signaller since last drain; write a single byte to wake Lwt. *)
      try ignore (Unix.write write_fd (Bytes.make 1 '\x00') 0 1) with
      | Unix.Unix_error (Unix.EAGAIN, _, _) ->
        () (* pipe buffer full; Lwt side will drain *)
      | Unix.Unix_error (Unix.EWOULDBLOCK, _, _) -> ()
      | _ -> ())
  ;;

  (** Block in the Lwt event loop until the pipe becomes readable (a broadcast
      arrived). Drains the pipe and clears the pending flag before returning. *)
  let wait () =
    Lwt_unix.wait_read lwt_read_fd
    >>= fun () ->
    (* Drain all accumulated bytes from the pipe. *)
    let buf = Bytes.create 64 in
    (try
       while true do
         let n = Unix.read read_fd buf 0 64 in
         if n = 0 then raise Exit
       done
     with
     | _ -> ());
    Atomic.set pending false;
    Lwt.return_unit
  ;;
end

(** Module signature that all strategy implementations must satisfy. *)
module type S = sig
  type config

  val execute
    :  config
    -> float option
    -> (float * float * float * float) option
    -> float option
    -> float option
    -> int
    -> int
    -> (string * float * float * string * int option) list
    -> int
    -> unit

  val get_pending_orders : int -> strategy_order list

  val handle_order_acknowledged
    :  now:float
    -> string
    -> string
    -> order_side
    -> float
    -> unit

  val handle_order_rejected : now:float -> string -> order_side -> float -> unit

  val handle_order_cancelled
    :  now:float
    -> string
    -> string
    -> order_side
    -> string option
    -> unit

  val handle_order_filled
    :  now:float
    -> string
    -> string
    -> order_side
    -> fill_price:float
    -> fill_qty:float
    -> string option
    -> unit

  val handle_order_amended
    :  now:float
    -> string
    -> string
    -> string
    -> order_side
    -> float
    -> unit

  val handle_order_amendment_skipped
    :  now:float
    -> string
    -> string
    -> order_side
    -> float
    -> unit

  val handle_order_amendment_failed
    :  now:float
    -> string
    -> string
    -> order_side
    -> string
    -> unit

  val handle_order_failed : now:float -> string -> order_side -> string -> unit
  val cleanup_pending_cancellation : string -> string -> unit
  val cleanup_strategy_state : string -> unit
  val init : unit -> unit
end
