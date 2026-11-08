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

(** Bounded id -> meta index. The keyspace is every venue order id ever registered, which
    is unbounded because an amend replaces the id. [Fifo_index] tracks admission
    separately from map membership, so retiring an id out of band cannot make a later
    re-insert double-queue and evict a live row; its FIFO queue still needs a periodic
    [trim_carry_orders] because removals leave superseded queue records behind. *)
let carry_orders : carry_order_meta Concurrency.Fifo_index.t =
  Concurrency.Fifo_index.create
    ~label:"strategy.carry_orders"
    ~cap:Concurrency.Cache_limits.carry_order_index_cap
    ()
;;

let carry_order_capacity = Concurrency.Fifo_index.cap carry_orders

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

(** Age after which a mapping whose order the venue's open-order scan does not list stops
    suppressing placement. A resting order is matched by that scan, so this never limits a
    take-profit's life; it only releases mappings whose terminal event was lost. Matches
    the grid's ghost-buy grace. *)
let carry_tracked_ack_grace_s = 15.0

(** True when a carry order acked within [max_age] for [key] is tracked. Suppresses
    duplicates while the venue's open-orders scan catches up; only call after a scan
    failed to find the order. *)
let has_tracked_carry_order ?(max_age = carry_tracked_ack_grace_s) ~key () =
  let now = Unix.gettimeofday () in
  Mutex.lock carry_registry_mutex;
  (* Early exit: the fold visits every live carry order, and the common case is a hit. The
     working set is small, so an index would cost more to keep consistent than the scan it
     replaces; stopping at the first match removes the tail. *)
  let found =
    try
      Concurrency.Fifo_index.fold
        (fun _ meta () ->
          if String.equal meta.com_key key && now -. meta.com_at <= max_age
          then raise_notrace Exit)
        carry_orders
        ();
      false
    with
    | Exit -> true
  in
  Mutex.unlock carry_registry_mutex;
  found
;;

(** Drop admission records whose row was retired out of band. [Fifo_index.evict] only pops
    while the map is over its cap, so without this sweep a long-lived process that retires
    most orders leaves one queue record per order ever seen. Called from the engine's
    periodic in-flight cleanup; returns the number dropped. *)
let trim_carry_orders () =
  Mutex.lock carry_registry_mutex;
  let dropped = Concurrency.Fifo_index.trim_queue carry_orders in
  Mutex.unlock carry_registry_mutex;
  dropped
;;

let set_carry_order_locked order_id meta =
  Concurrency.Fifo_index.set carry_orders ~key:order_id ~value:meta
;;

let bind_carry_order ~dedup_key ~order_id =
  if not (String.equal dedup_key "")
  then (
    Mutex.lock carry_registry_mutex;
    (match Hashtbl.find_opt carry_pending dedup_key with
     | Some meta ->
       Hashtbl.remove carry_pending dedup_key;
       set_carry_order_locked order_id meta
     | None -> ());
    Mutex.unlock carry_registry_mutex)
;;

(** Register a replacement (amend) order id directly from the amend ack. The old id is
    retired by the caller: a venue amend produces a new id, and unless the old mapping is
    removed it keeps suppressing replacements for the same kind. *)
let register_carry_order_id ~order_id ~market ~maker ~kind ~key =
  Mutex.lock carry_registry_mutex;
  set_carry_order_locked
    order_id
    { com_market = market
    ; com_maker = maker
    ; com_kind = kind
    ; com_key = key
    ; com_at = Unix.gettimeofday ()
    };
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
  let meta = Concurrency.Fifo_index.find_opt carry_orders order_id in
  Mutex.unlock carry_registry_mutex;
  meta
;;

let retire_carry_order ~order_id =
  Mutex.lock carry_registry_mutex;
  Concurrency.Fifo_index.remove carry_orders order_id;
  Mutex.unlock carry_registry_mutex
;;

(** Retire a carry order and return its in-flight key when tracked. The cancel ack
    releases the placement latch: the terminal event can race the ack or be dropped.
    [None] when the id was untracked. *)
let retire_carry_order_key ~order_id =
  Mutex.lock carry_registry_mutex;
  let meta = Concurrency.Fifo_index.find_opt carry_orders order_id in
  Concurrency.Fifo_index.remove carry_orders order_id;
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
  | Carry -> "inventory_hedge"
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

(** Room a reduce-only order has left on [side] in the live position. A reduce-only order
    may only shrink an existing position: a stale strategy input, a duplicated close, or a
    position that vanished between the snapshot and the send must never flip the book into
    the opposite side. Clamped to the position, and zero when the position is flat. An
    unknown position passes through - the venue's own reduce-only flag is the fallback
    when no snapshot has landed yet. *)
let reduce_only_room ~(position : float option) ~(side : order_side) ~(qty : float)
  : float
  =
  match position with
  | Some p when Float.is_finite p ->
    let room =
      match side with
      | Sell -> Float.max 0.0 p
      | Buy -> Float.max 0.0 (-.p)
    in
    Float.min (Float.max 0.0 qty) room
  | _ -> Float.max 0.0 qty
;;

(** Whether an untagged resting order belongs to this logical kind. When the venue reports
    reduce-only it must agree: otherwise a non-reduce-only sell could be amended as a
    take-profit and later fill as a short. Venues that hide the flag fall back to the
    exact target price. *)
let untagged_order_is_own_kind
  ~(price_hits_target : bool)
  ~(adopt_by_flags : bool)
  ~(want_reduce_only : bool)
  ~(venue_reduce_only : bool option)
  =
  match venue_reduce_only with
  | Some reported -> reported = want_reduce_only && (price_hits_target || adopt_by_flags)
  | None -> price_hits_target
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

(** Venue-failure backoff for overlay placements. [order_replace] runs on every book tick,
    so a rejection (429, insufficient margin, unresolved amend id) would otherwise
    re-issue on the next tick and amplify the failure. Keyed by the full in-flight key so
    a failing kind/symbol backs off independently. *)
module Placement_backoff = struct
  let table : (string, float) Hashtbl.t = Hashtbl.create 32
  let mutex = Mutex.create ()
  let default_s = 5.0
  let rate_limited_s = 15.0
  let unresolved_amend_s = 3.0

  let contains s sub =
    let n = String.length s
    and m = String.length sub in
    let rec go i = i + m <= n && (String.equal (String.sub s i m) sub || go (i + 1)) in
    m = 0 || go 0
  ;;

  let is_rate_limited err = contains err "429" || contains err "Too Many Requests"

  let note ?(hold = default_s) ~key () =
    Mutex.lock mutex;
    Hashtbl.replace table key (Unix.gettimeofday () +. hold);
    Mutex.unlock mutex
  ;;

  let note_failure ?(err = "") ~key () =
    note ~hold:(if is_rate_limited err then rate_limited_s else default_s) ~key ()
  ;;

  (** A Hyperliquid amend ack did not resolve the replacement id: the venue order is live
      under an id the open-orders cache may not know yet. Hold the key briefly instead of
      placing a duplicate beside it. *)
  let note_unresolved_amend ~key () = note ~hold:unresolved_amend_s ~key ()

  let clear ~key =
    Mutex.lock mutex;
    Hashtbl.remove table key;
    Mutex.unlock mutex
  ;;

  let remaining ~key () =
    let now = Unix.gettimeofday () in
    Mutex.lock mutex;
    let left =
      match Hashtbl.find_opt table key with
      | Some until when until > now -> until -. now
      | Some _ ->
        Hashtbl.remove table key;
        0.0
      | None -> 0.0
    in
    Mutex.unlock mutex;
    left
  ;;

  (** Drop expired holds. [remaining] prunes only the key it is asked about, so a symbol
      that stops being replaced after a rejection would otherwise retain its entry for the
      life of the process. Keys are bounded by configuration; this sweep keeps the table
      at the live backoff set rather than everything that ever failed. *)
  let cleanup () =
    let now = Unix.gettimeofday () in
    Mutex.lock mutex;
    let before = Hashtbl.length table in
    Hashtbl.filter_map_inplace
      (fun _ until -> if until > now then Some until else None)
      table;
    let removed = before - Hashtbl.length table in
    Mutex.unlock mutex;
    removed
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

  (** A [Pending] entry normally lives for one REST round-trip (seconds). Ten minutes is
      far beyond the longest request timeout, so an entry older than this cannot still
      have a request in flight: its response was lost or its callback never ran. Dropping
      it bounds the only phase that is otherwise never evicted; a terminal event that
      arrives later still reconciles tracking. *)
  let pending_max_age_s = 600.0

  let registries : (string, entry) Hashtbl.t array =
    Array.init num_shards (fun _ -> Hashtbl.create 128)
  ;;

  let shard_mutexes : Mutex.t array = Array.init num_shards (fun _ -> Mutex.create ())
  let shard_of key = Hashtbl.hash key land (num_shards - 1)

  (* Reverse index: replacement (new) id -> old id. [is_replacement_target] runs on the
     order-management path for every extra buy candidate, and the old implementation
     locked and folded all 64 shards to answer it. This map is keyed by the new id, which
     is the only thing the caller knows, so it cannot be sharded by the old id; it is one
     small table behind its own lock.

     Lock order is always shard mutex -> replacement mutex, never the reverse, so the two
     never deadlock. *)
  let replacement_index : (string, string) Hashtbl.t = Hashtbl.create 256
  let replacement_mutex = Mutex.create ()

  let register_replacement ~old_id ~new_id =
    Mutex.lock replacement_mutex;
    Hashtbl.replace replacement_index new_id old_id;
    Mutex.unlock replacement_mutex
  ;;

  let forget_replacement new_id =
    Mutex.lock replacement_mutex;
    Hashtbl.remove replacement_index new_id;
    Mutex.unlock replacement_mutex
  ;;

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
    Mutex.lock replacement_mutex;
    let found = Hashtbl.mem replacement_index order_id in
    Mutex.unlock replacement_mutex;
    found
  ;;

  (** Remove [order_id] from the registry (terminal phases, cleanup). Returns true if it
      was present. *)
  let remove_in_flight_amendment order_id =
    let shard = shard_of order_id in
    let registry = registries.(shard) in
    let mutex = shard_mutexes.(shard) in
    Mutex.lock mutex;
    let phase =
      match Hashtbl.find_opt registry order_id with
      | Some (entry : entry) ->
        Hashtbl.remove registry order_id;
        Some entry.phase
      | None -> None
    in
    Mutex.unlock mutex;
    (match phase with
     | Some (Replaced new_id) -> forget_replacement new_id
     | _ -> ());
    Option.is_some phase
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
      let previous =
        match Hashtbl.find_opt registry old_id with
        | Some (entry : entry) ->
          let previous =
            match entry.phase with
            | Replaced previous -> Some previous
            | _ -> None
          in
          entry.phase <- Replaced new_id;
          entry.last <- now;
          previous
        | None ->
          Hashtbl.replace registry old_id { phase = Replaced new_id; last = now };
          None
      in
      Mutex.unlock mutex;
      (match previous with
       | Some previous -> forget_replacement previous
       | None -> ());
      register_replacement ~old_id ~new_id)
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
      let stale_pending = ref 0 in
      for s = 0 to num_shards - 1 do
        let mutex = shard_mutexes.(s) in
        Mutex.lock mutex;
        let registry = registries.(s) in
        let stale = ref [] in
        Hashtbl.iter
          (fun key (entry : entry) ->
            let keep =
              match entry.phase with
              | Pending ->
                if now -. entry.last <= pending_max_age_s
                then true
                else (
                  incr stale_pending;
                  false)
              | Replaced _ | Failed _ | Skipped -> now -. entry.last <= max_age
            in
            if not keep then stale := (key, entry.phase) :: !stale)
          registry;
        List.iter
          (fun (key, phase) ->
            Hashtbl.remove registry key;
            incr removed;
            match phase with
            | Replaced new_id -> forget_replacement new_id
            | _ -> ())
          !stale;
        Mutex.unlock mutex
      done;
      if !stale_pending > 0
      then
        Logging.warn_f
          ~section:"strategy_common"
          "evicted %d unanswered amendment(s) older than %.0fs (lost terminal event); \
           the amend registry stays bounded"
          !stale_pending
          pending_max_age_s;
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
    and consumer indices to prevent false sharing.

    Slot protocol (this is what makes [broadcast] safe): a producer advances [tail], then
    stores the payload, then publishes the slot by setting [state] to {!published}. The
    consumer only reads a slot whose [state] is {!published}, so it can never observe a
    torn or half-written payload. Producers must therefore call
    [OrderSignal.broadcast] *after* [write] returns [Some] — the wakeup is the statement
    "a published slot is waiting", and reversing that order would reintroduce the
    lost-wakeup window. *)
module LockFreeQueue = struct
  (* 0 = empty, 2 = published. There is no persistent 1 state: the producer claims the
     slot via the [tail] CAS, so [state] only needs to distinguish published from not. *)
  let state_empty = 0
  let state_published = 2

  type 'a slot =
    { mutable payload : 'a
    ; state : int Atomic.t
    }

  type 'a t =
    { array : 'a slot array
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
    let array =
      Array.init size (fun _ ->
        { payload = Obj.magic 0; state = Atomic.make state_empty })
    in
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

  (** Concurrent enqueue for multiple producers. Returns None if full. Zero allocation:
      the payload is stored directly, no option wrapper is allocated per push. *)
  let write q v =
    let rec loop () =
      let t = Atomic.get q.tail in
      let h = Atomic.get q.head in
      if t - h >= q.size
      then None (* Queue is full *)
      else if Atomic.compare_and_set q.tail t (t + 1)
      then (
        let slot = q.array.(t land q.mask) in
        slot.payload <- v;
        Atomic.set slot.state state_published;
        Some ())
      else loop ()
    in
    loop ()
  ;;

  (** Single consumer dequeue. Non-blocking: returns None if the slot is not yet
      published, even if the tail index has been advanced by a producer. This prevents the
      consumer from spinning if a producer is preempted. *)
  let read q =
    let h = Atomic.get q.head in
    if h = Atomic.get q.tail
    then None (* Queue is empty *)
    else (
      let slot = q.array.(h land q.mask) in
      if Atomic.get slot.state <> state_published
      then None (* Producer claimed the slot but hasn't published it; don't spin *)
      else (
        let v = slot.payload in
        (* Drop the reference so a consumed payload is collectable. Safe because [state]
           gates every read of [payload]. *)
        slot.payload <- Obj.magic 0;
        Atomic.set slot.state state_empty;
        Atomic.set q.head (h + 1);
        Some v))
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

  (** Single-byte wake payload, allocated once. [broadcast] runs on every trading domain
      so it must not allocate. *)
  let wake_byte = Bytes.make 1 '\x00'

  let write_wake_byte () =
    try ignore (Unix.write write_fd wake_byte 0 1) with
    | Unix.Unix_error (Unix.EAGAIN, _, _) ->
      () (* pipe buffer full; Lwt side will drain *)
    | Unix.Unix_error (Unix.EWOULDBLOCK, _, _) -> ()
    | _ -> ()
  ;;

  (* Producer/consumer protocol. A producer publishes its queue slot BEFORE calling
     [broadcast] (see [LockFreeQueue.write] call sites), so the only obligation here is:
     if the queue may be non-empty, a pipe byte is pending or [pending] is false.

     [wait] clears [pending] BEFORE draining, not after. Draining first is a lost-wakeup
     bug: a producer that calls [broadcast] in the window between the drain and the clear
     observes [pending = true], skips its own write, and then the consumer clears the flag
     and parks forever on an order that was already enqueued. Clearing first means any
     producer in that window sees [false], writes its byte, and either the drain below
     consumes it or the next [wait_read] returns immediately. *)
  let broadcast () = if not (Atomic.exchange pending true) then write_wake_byte ()

  (** Block in the Lwt event loop until the pipe becomes readable (a broadcast arrived). *)
  let wait () =
    Lwt_unix.wait_read lwt_read_fd
    >>= fun () ->
    Atomic.set pending false;
    let buf = Bytes.create 64 in
    (try
       while true do
         let n = Unix.read read_fd buf 0 64 in
         if n = 0 then raise Exit
       done
     with
     | _ -> ());
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
