(** Common types and infrastructure shared across all trading strategies. Provides unified
    order representation, in-flight deduplication caches, a ring buffer for order queuing,
    and the strategy module signature. *)

open Lwt.Infix
module StringMap = Map.Make (String)

(** Per-symbol trading parameters parsed from config.json. *)
type trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float * float
  (** (min, max) grid interval percentages; resolved to equal bounds when a scalar is
      provided *)
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
  (** Per-strategy opt-in to base-accumulation persistence (default true when absent from
      config.json; disabled means zero I/O). *)
  ; sell_levels : bool
  (** Per-strategy opt-in to pending-sell-level persistence (default true when absent from
      config.json). *)
  ; cpu_priority : int
  (** Core-assignment priority: trading domains are pinned to trading CPUs in descending
      [cpu_priority] order, so a higher value claims a P-core before a lower one. Ties
      keep config order. Default 0. *)
  }

(** Integer userref tags for per-strategy order grouping on the exchange. *)
let strategy_userref_grid = 1 (* grid strategy *)

let strategy_userref_mm = 2 (* market maker strategy *)
let strategy_userref_carry = 3 (* carry overlay perp orders *)

(** Decode the strategy tag embedded in a Hyperliquid-style client order id. The tag is
    the top byte of the trailing 64 bits. Returns [None] for non-hex ids or a zero tag. *)
let userref_of_cloid (clid : string) : int option =
  let clean =
    if String.starts_with ~prefix:"0x" clid
    then String.sub clid 2 (String.length clid - 2)
    else clid
  in
  if String.length clean < 16
  then None
  else (
    let last = String.sub clean (String.length clean - 16) 16 in
    try
      let v = Int64.of_string ("0x" ^ last) in
      let tag = Int64.shift_right_logical v 56 in
      if Int64.compare tag 0L > 0 then Some (Int64.to_int tag) else None
    with
    | _ -> None)
;;

(** Returns true if [order_userref] matches the given [strategy_userref]. *)
let is_strategy_order strategy_userref order_userref = strategy_userref = order_userref

(** True for grid-owned userrefs. Untagged and zero-tag orders count as grid: Alpaca and
    IBKR omit the userref, and an amend can drop it. Carry and MM orders are tagged, so
    excluding them keeps the books separate. *)
let is_grid_order = function
  | Some r -> r = strategy_userref_grid || r = 0
  | None -> true
;;

(** Ownership label for serialized orders. Unknown non-zero tags map to ["other"] rather
    than being silently treated as grid. *)
let order_owner = function
  | Some r when r = strategy_userref_grid || r = 0 -> "grid"
  | Some r when r = strategy_userref_carry -> "carry"
  | Some r when r = strategy_userref_mm -> "mm"
  | Some _ -> "other"
  | None -> "grid"
;;

(** Instrument class for an overlay order fill. The execution event carries only the order
    id, so the placement side registers the class here, the supervisor binds the venue
    order id on ack, and the domain loop looks it up on the fill. *)
type carry_market =
  | Carry_perp
  | Carry_spot

type carry_order_meta =
  { com_market : carry_market
  ; com_maker : bool
  ; com_kind : string
  ; com_key : string
  (** The full per-instrument in-flight key ([overlay_inflight_key]). The logical
      [com_kind] alone is shared by every symbol, so tracked-order lookups must match on
      this key. *)
  ; com_at : float
  (** Wall-clock registration time. A venue amend replaces the order id; if the old id's
      fill/cancel event is lost, its entry would otherwise suppress every later replace
      for this kind forever. Tracking is bounded by age for that reason. *)
  }

let carry_registry_mutex = Mutex.create ()
let carry_pending : (string, carry_order_meta) Hashtbl.t = Hashtbl.create 16
let carry_orders : (string, carry_order_meta) Hashtbl.t = Hashtbl.create 64
let carry_order_queue : string Queue.t = Queue.create ()

(** True only for the execution statuses that end an order's life. [New] and
    [PartiallyFilled] are live: the id -> kind mapping must survive so [order_replace]
    recognizes its own resting order instead of placing a duplicate every tick. *)
let carry_order_terminal (status : Dio_exchange.Exchange_intf.Types.order_status) =
  let open Dio_exchange.Exchange_intf.Types in
  match status with
  | Filled | Canceled | Rejected | Expired -> true
  | Pending | New | PartiallyFilled | Unknown _ -> false
;;

let register_pending_carry_order ~dedup_key ~market ~maker ~kind =
  Mutex.lock carry_registry_mutex;
  Hashtbl.replace
    carry_pending
    dedup_key
    { com_market = market
    ; com_maker = maker
    ; com_kind = kind
    ; com_key = dedup_key
    ; com_at = Unix.gettimeofday ()
    };
  Mutex.unlock carry_registry_mutex
;;

(** True when an acked carry order for [key] is tracked (awaiting fill or cancel). Mirrors
    the grid's [has.tracked.buy]: suppresses duplicates while the venue's open-orders scan
    catches up.

    No age bound: the mapping is created on ack and retired only by a terminal event (or
    an amend's old-id retirement). A resting take-profit can sit for minutes. A lost
    terminal event is surfaced by the in-flight stale sweep, not by re-placing. *)
let has_tracked_carry_order ~key () =
  Mutex.lock carry_registry_mutex;
  let found =
    Hashtbl.fold
      (fun _ meta acc -> acc || String.equal meta.com_key key)
      carry_orders
      false
  in
  Mutex.unlock carry_registry_mutex;
  found
;;

let trim_carry_orders_locked () =
  while Hashtbl.length carry_orders > 2048 && not (Queue.is_empty carry_order_queue) do
    let oid = Queue.pop carry_order_queue in
    Hashtbl.remove carry_orders oid
  done
;;

let bind_carry_order ~dedup_key ~order_id =
  if not (String.equal dedup_key "")
  then (
    Mutex.lock carry_registry_mutex;
    (match Hashtbl.find_opt carry_pending dedup_key with
     | Some meta ->
       Hashtbl.remove carry_pending dedup_key;
       Hashtbl.replace carry_orders order_id meta;
       Queue.push order_id carry_order_queue;
       trim_carry_orders_locked ()
     | None -> ());
    Mutex.unlock carry_registry_mutex)
;;

(** Register a replacement (amend) order id directly from the amend ack. The old id is
    retired by the caller: a venue amend produces a new id, and unless the old mapping is
    removed it keeps suppressing replacements for the same kind. *)
let register_carry_order_id ~order_id ~market ~maker ~kind ~key =
  Mutex.lock carry_registry_mutex;
  Hashtbl.replace
    carry_orders
    order_id
    { com_market = market
    ; com_maker = maker
    ; com_kind = kind
    ; com_key = key
    ; com_at = Unix.gettimeofday ()
    };
  Queue.push order_id carry_order_queue;
  trim_carry_orders_locked ();
  Mutex.unlock carry_registry_mutex
;;

let retire_pending_carry_order ~dedup_key =
  if not (String.equal dedup_key "")
  then (
    Mutex.lock carry_registry_mutex;
    Hashtbl.remove carry_pending dedup_key;
    Mutex.unlock carry_registry_mutex)
;;

let lookup_carry_order ~order_id =
  Mutex.lock carry_registry_mutex;
  let meta = Hashtbl.find_opt carry_orders order_id in
  Mutex.unlock carry_registry_mutex;
  meta
;;

let retire_carry_order ~order_id =
  Mutex.lock carry_registry_mutex;
  Hashtbl.remove carry_orders order_id;
  Mutex.unlock carry_registry_mutex
;;

(** Retire a carry order and return its in-flight key when tracked. The cancel ack
    releases the placement latch: the terminal event can race the ack or be dropped.
    [None] when the id was untracked. *)
let retire_carry_order_key ~order_id =
  Mutex.lock carry_registry_mutex;
  let meta = Hashtbl.find_opt carry_orders order_id in
  Hashtbl.remove carry_orders order_id;
  Mutex.unlock carry_registry_mutex;
  match meta with
  | Some m -> Some m.com_key
  | None -> None
;;

(** Order side: buy or sell. *)
type order_side =
  | Buy
  | Sell

let string_of_order_side = function
  | Buy -> "buy"
  | Sell -> "sell"
;;

(** Instrument class carried by every order from construction to fill. Aliased to the
    exchange layer's type so there is exactly one definition in the process. *)
type market = Dio_exchange.Exchange_intf.Types.market =
  | Spot
  | Perp

let string_of_market = function
  | Spot -> "spot"
  | Perp -> "perp"
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
  | Carry

let string_of_strategy_id = function
  | Ladder -> "jacobs_ladder"
  | MM -> "market_maker"
  | Hedger -> "Hedger"
  | Carry -> "carry_overlay"
;;

(** Unified order record emitted by all strategies. *)
type strategy_order =
  { operation : operation_type (* place, amend, or cancel *)
  ; order_id : string option (* target order ID for amend/cancel; None for place *)
  ; symbol : string
  ; market : market
  ; side : order_side
  ; order_type : string (* e.g. "limit", "market" *)
  ; qty : float
  ; price : float option
  ; time_in_force : string (* e.g. "GTC", "IOC", "FOK" *)
  ; post_only : bool
  ; reduce_only : bool
  ; userref : int option (* exchange-level strategy tag *)
  ; strategy : strategy_id (* originating strategy variant *)
  ; exchange : string (* target exchange *)
  ; duplicate_key : string (* composite key for deduplication *)
  }

(** Build a composite deduplication key from order parameters. Uses string concatenation
    to avoid intermediate buffer allocations. *)
let generate_duplicate_key symbol side quantity limit_price =
  let q_str = string_of_float quantity in
  match limit_price with
  | Some p -> symbol ^ "|" ^ side ^ "|" ^ q_str ^ "|" ^ string_of_float p
  | None -> symbol ^ "|" ^ side ^ "|" ^ q_str ^ "|market"
;;

(** In-flight dedupe key for an overlay placement.

    The logical kind (["carry:entry"], ["carry:recycle:close|ro"], ...) stays the value
    the carry registry uses to match a resting order. The in-flight registry needs a key
    that is unique per venue and instrument: a bare ["carry:entry"] is shared by every
    symbol, so a placement that is still in flight - or whose key was never released -
    suppresses the placement for every other symbol on every venue.

    Shared by placement, replacement, and restart adoption so all three derive the exact
    same key. *)
let overlay_inflight_key ~dedup_key ~side ~qty ~price ~exchange ~instrument ~reduce_only =
  let base =
    if String.equal dedup_key ""
    then generate_duplicate_key instrument side qty price
    else dedup_key
  in
  base ^ "|" ^ exchange ^ "|" ^ instrument ^ if reduce_only then "|ro" else ""
;;

(** Per-symbol strategy order-action counters.

    Incremented on each successful push of a place/amend/cancel to a ring buffer. The
    domain worker snapshots and resets once per latency window; the dashboard STRAT/S
    column reports actual order actions per second, not strategy-invocation cycles. *)
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

    Sharded across [num_shards] independent tables and mutexes keyed by hash of the
    duplicate key (which embeds the symbol). Independent symbols and domains do not
    serialize on one global lock; the common case locks only the key's own shard. *)
module InFlightOrders = struct
  let num_shards = 64

  let registries : (string, float) Hashtbl.t array =
    Array.init num_shards (fun _ -> Hashtbl.create 128)
  ;;

  let shard_mutexes : Mutex.t array = Array.init num_shards (fun _ -> Mutex.create ())
  let shard_of key = Hashtbl.hash key land (num_shards - 1)

  (** Atomically insert [duplicate_key] if absent. Returns true on insertion, false if the
      key was already present. *)
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

  (** Entries held longer than [age] seconds. A healthy placement releases its key when
      the venue ack lands (well under a second), so a key held for seconds is stuck and is
      silently suppressing retries. Returns [(count, oldest held)] where the oldest is
      [(key, age_seconds)]. Used by the periodic sweep so a leaked key is reported once
      per sweep instead of staying invisible until the 60s TTL evicts it. *)
  let stale ?(age = 30.0) () =
    let now = Unix.gettimeofday () in
    let count = ref 0 in
    let oldest = ref None in
    for s = 0 to num_shards - 1 do
      let mutex = shard_mutexes.(s) in
      Mutex.lock mutex;
      Hashtbl.iter
        (fun key ts ->
          let held = now -. ts in
          if held > age
          then (
            incr count;
            match !oldest with
            | Some (_, best) when best >= held -> ()
            | _ -> oldest := Some (key, held)))
        registries.(s);
      Mutex.unlock mutex
    done;
    !count, !oldest
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
  let get_cleanup_fn () () =
    let drift, trimmed = cleanup () in
    Some (Some drift, Some trimmed)
  ;;
end

(** In-flight amendment lifecycle registry: deduplication of pending amend requests plus
    the exchange's mid-amend order-replacement events.

    Amendment semantics differ by exchange: Kraken modifies the order in place (same id);
    Hyperliquid/Alpaca cancel the old order and create a new id. A replacement emits a
    cancel event for the old id, either while the amend is pending or shortly after
    completion. This registry maps all exchanges to one lifecycle:

    - [Pending]: amend request in flight; a cancel for the old id is the amend's side
      effect, not a real cancellation.
    - [Replaced new_id]: exchange confirmed a replace (old_id <> new_id); the entry is
      retained through the cleanup window so a late cancel for the old id cannot reset the
      replacement order's tracking.
    - Same-id amends (Kraken) retain no entry: events for that id are real.
    - [Failed]/[Skipped]: terminal; the entry is dropped so a follow-up cancel is handled
      as real (the failure path reconciled tracking). *)
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

  (* Sharded like InFlightOrders: keyed by hash of the order id. *)
  let num_shards = 64

  let registries : (string, entry) Hashtbl.t array =
    Array.init num_shards (fun _ -> Hashtbl.create 128)
  ;;

  let shard_mutexes : Mutex.t array = Array.init num_shards (fun _ -> Mutex.create ())
  let shard_of key = Hashtbl.hash key land (num_shards - 1)

  (** Atomically insert [order_id] as [Pending] if absent. Returns true on insertion,
      false if the id is already tracked (any phase - a replaced id stays tracked for the
      cleanup window, so re-amending it is a duplicate). *)
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

  (** Returns true if [order_id] was just replaced by an amendment and its old id is still
      in the recognition window. *)
  let is_superseded order_id =
    match phase_of order_id with
    | Some (Replaced _) -> true
    | _ -> false
  ;;

  (** True while the exchange may still deliver events for [order_id] as a side effect of
      an amendment (request pending, or replacement just completed): a cancel event in
      this state must not reset tracking. *)
  let is_amend_lifecycle_active order_id =
    match phase_of order_id with
    | Some (Pending | Replaced _) -> true
    | _ -> false
  ;;

  (** True when [order_id] is the replacement (new) id produced by an amendment whose old
      id is still in the recognition window. A cancel+replace venue (Hyperliquid/Alpaca)
      returns a new order id and the feed transiently lists the old and new id together;
      order management that acts on "more than one buy" must treat this new id as part of
      the amendment, not as a duplicate to cancel. The replacement id has no registry
      entry of its own (the old id holds the [Replaced] entry), so
      [is_amend_lifecycle_active] alone does not cover it. *)
  let is_replacement_target order_id =
    let rec scan s =
      if s >= num_shards
      then false
      else (
        let registry = registries.(s) in
        let mutex = shard_mutexes.(s) in
        Mutex.lock mutex;
        let found =
          Hashtbl.fold
            (fun _ (entry : entry) acc ->
              acc
              ||
              match entry.phase with
              | Replaced new_id -> String.equal new_id order_id
              | _ -> false)
            registry
            false
        in
        Mutex.unlock mutex;
        found || scan (s + 1))
    in
    scan 0
  ;;

  (** Remove [order_id] from the registry (terminal phases, cleanup). Returns true if it
      was present. *)
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

  (** The exchange confirmed the amendment. A replace (old_id <> new_id) keeps the old id
      registered as [Replaced] for the cleanup window so a late cancel event for it is
      recognized as the amend's side effect; a same-id amend (Kraken) drops the entry -
      its events are always real. *)
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

  (** The exchange rejected the amendment: terminal. The entry is dropped so a follow-up
      cancel event for the old id is handled as a real one (the amend-failure path has
      already reconciled tracking). *)
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

  (** Evict only terminal entries older than [max_age] seconds; this bounds the [Replaced]
      recognition window. [Pending] entries are owned by an in-flight REST request and are
      always resolved by exactly one terminal event
      (Amended/Amendment_skipped/Amendment_failed) or a recognized cancel. Evicting a
      [Pending] entry would drop the amend-recognition window while the exchange still
      owns the request, so a mid-flight cancel would reset tracking as if real. Returns
      [(0, removed_count)]. *)
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
  let get_cleanup_fn () () =
    let drift, trimmed = cleanup () in
    Some (Some drift, Some trimmed)
  ;;
end

(** Fixed-size, zero-allocation MPSC ring buffer. Cache-line padding separates producer
    and consumer indices to prevent false sharing. *)
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

  (** Single consumer dequeue. Non-blocking: returns None if the slot is empty, even if
      the tail index has been advanced by a producer. This prevents the consumer from
      spinning if a producer is preempted. *)
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

(** Domain-safe order signal channel. Domain workers call [broadcast ()] to notify the
    supervisor's Lwt event loop that new orders are available. Implemented via a Unix
    self-pipe: a single-byte write from any domain wakes the Lwt scheduler without
    touching Lwt internals (Lwt_condition is not safe from non-Lwt domains).

    [pending] coalesces rapid broadcasts into one pipe write to avoid saturating the pipe
    buffer under high order throughput. *)
module OrderSignal = struct
  let read_fd, write_fd =
    let r, w = Unix.pipe ~cloexec:true () in
    Unix.set_nonblock r;
    Unix.set_nonblock w;
    r, w
  ;;

  (** Lwt wrapper for the read end of the self-pipe. Created once at module init to avoid
      per-wait allocation. *)
  let lwt_read_fd = Lwt_unix.of_unix_file_descr ~blocking:false ~set_flags:false read_fd

  (** Atomic flag to coalesce multiple broadcasts into one pipe write. *)
  let pending = Atomic.make false

  (** Signal from any domain that new orders are available. Safe to call from OCaml 5
      domain workers, no Lwt internals are touched. *)
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

  (** Block in the Lwt event loop until the pipe becomes readable (a broadcast arrived).
      Drains the pipe and clears the pending flag before returning. *)
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
