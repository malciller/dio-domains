(** Bounded string-keyed index with FIFO-by-insertion eviction.

    Backs the per-venue "order id -> (symbol, side)" maps in the execution feeds. Those
    keys are open-order identifiers: the map is written on every order event and read on
    every fill, and its keyspace is bounded only by how many orders the venue ever
    returns, so it must be explicitly capped.

    **Not an LRU, deliberately.** The value attached to an order id is needed only while
    that order is live, and the population is genuinely working set. LRU would rank by
    recency of *lookup*, which is the wrong signal twice over: the fill handler looks up
    the same order id every time it reconciles, so a repeatedly-touched old id would be
    pinned near the head of the eviction order and push out a live order; and lookups do
    not change whether an id is still needed. Insertion age is the signal that actually
    tracks liveness, and FIFO is both cheaper and more predictable. The same argument
    applies to the fill-dedup sets elsewhere in the engine, which is why they are
    time-windowed rather than recency-ranked.

    **The tricky part is out-of-band removal.** Callers delete terminal order ids straight
    from the map (a fill or cancel retires the order), which leaves the insertion queue
    holding keys that are no longer in the map. The hand-rolled versions of this in the
    three execution feeds each got a different aspect of that wrong:

    - Membership was inferred as "is the key in the map?", which is wrong precisely
      because the map and the queue can disagree. Re-inserting an id that had been removed
      from the map but was still queued pushed it a *second* time; that duplicate was
      popped later and deleted the live re-inserted row, evicting an order that was still
      open.
    - A "cap/queue divergence" guard responded by raising the cap to the current map size.
      That turns a transient inconsistency into a permanent leak, because the new, larger
      cap never comes back down.

    This module therefore tracks queue membership explicitly, in [seqs], and stamps each
    queue entry with the sequence number it was admitted under. [remove] clears the
    membership so a later re-insert is admitted as a *new* entry at the back of the queue;
    the stale entry is recognised by its outdated sequence number on pop and skipped
    rather than deleting the live row. All of that is O(1) per operation.

    **Bounding policy.** The cap is finite from the very first insert. It previously
    started at [max_int] and only became finite once [lock_cap] ran after the startup
    snapshot, so a snapshot that errored, was skipped, or raced left the index unbounded
    for the life of the process on a key that grows without limit. [lock_cap] retunes the
    cap once from the observed startup volume, floored and clamped, and is an adjustment
    rather than the thing that makes the bound exist.

    Not thread-safe by itself: the owning feed holds its index mutex across these calls.
    That is deliberate — the feeds mutate several related structures under one lock and a
    per-operation lock here would not compose with them. *)

let default_cap = 65_536
let min_cap = 32

type 'value t =
  { map : (string, 'value) Hashtbl.t
  (** id -> value; the only structure read by callers. *)
  ; seqs : (string, int) Hashtbl.t (** id -> sequence number of its live queue entry. *)
  ; order : (string * int) Queue.t (** FIFO of [(id, seq)], oldest first. *)
  ; label : string (** Feed name, for log messages. *)
  ; mutable cap : int
  ; mutable next_seq : int
  ; mutable evictions : int
  ; mutable stale_skips : int
  ; mutable trims : int
  }

let create ~label ?(cap = default_cap) () =
  { map = Hashtbl.create 64
  ; seqs = Hashtbl.create 64
  ; order = Queue.create ()
  ; label
  ; cap = max min_cap cap
  ; next_seq = 0
  ; evictions = 0
  ; stale_skips = 0
  ; trims = 0
  }
;;

let length t = Hashtbl.length t.map
let cap t = t.cap
let find_opt t key = Hashtbl.find_opt t.map key
let mem t key = Hashtbl.mem t.map key

type stats =
  { entries : int
  ; capacity : int (** Current cap, in [min_cap]..[default_cap]. *)
  ; queued : int (** Live admission records, i.e. [Hashtbl.length seqs]. *)
  ; pending_queue : int (** Entries in the FIFO, including superseded ones. *)
  ; evictions : int
  ; stale_skips : int (** Superseded queue entries correctly not applied. *)
  ; trims : int
  }

let stats t =
  { entries = Hashtbl.length t.map
  ; capacity = t.cap
  ; queued = Hashtbl.length t.seqs
  ; pending_queue = Queue.length t.order
  ; evictions = t.evictions
  ; stale_skips = t.stale_skips
  ; trims = t.trims
  }
;;

(** Drops queue entries that no longer correspond to a live admission, i.e. ids that were
    removed out of band. Order is preserved. O(queue length); call from the feed's
    periodic maintenance pass, not from the hot path. Returns the number dropped. *)
let trim_queue t =
  let before = Queue.length t.order in
  let kept = Queue.create () in
  Queue.transfer t.order kept;
  let survivors = Queue.create () in
  while not (Queue.is_empty kept) do
    let key, seq = Queue.pop kept in
    match Hashtbl.find_opt t.seqs key with
    | Some s when s = seq -> Queue.push (key, seq) survivors
    | _ -> t.stale_skips <- t.stale_skips + 1
  done;
  Queue.transfer survivors t.order;
  let dropped = before - Queue.length t.order in
  if dropped > 0
  then (
    t.trims <- t.trims + 1;
    Logging.debug_f
      ~section:(Printf.sprintf "%s_index" t.label)
      "trimmed %d superseded queue entries (%d -> %d, %d entries, cap %d)"
      dropped
      before
      (Queue.length t.order)
      (Hashtbl.length t.map)
      t.cap);
  dropped
;;

(** Evicts oldest-first until the map is within its cap. A popped entry is only applied if
    its sequence number is still the current one for that id; otherwise it has been
    superseded by a re-insert and is skipped. *)
let rec evict t =
  if Hashtbl.length t.map > t.cap
  then
    if Queue.is_empty t.order
    then (
      (* The queue is exhausted but the map is still over cap, so admission records and
         map rows disagree in a way [trim_queue] cannot explain. Rebuild from [seqs] and
         say so, rather than growing the cap to fit and leaking. *)
      let live = Hashtbl.length t.map in
      Hashtbl.iter (fun key seq -> Queue.push (key, seq) t.order) t.seqs;
      Logging.warn_f
        ~section:(Printf.sprintf "%s_index" t.label)
        "eviction queue exhausted with %d entries over cap %d; rebuilt from %d admission \
         records (eviction order is no longer strictly by age)"
        live
        t.cap
        (Hashtbl.length t.seqs);
      if Queue.is_empty t.order then t.cap <- Hashtbl.length t.map)
    else (
      let key, seq = Queue.pop t.order in
      (match Hashtbl.find_opt t.seqs key with
       | Some s when s = seq ->
         Hashtbl.remove t.map key;
         Hashtbl.remove t.seqs key;
         t.evictions <- t.evictions + 1
       | _ -> t.stale_skips <- t.stale_skips + 1);
      evict t)
;;

(** Binds [key] to [value], admitting it to the eviction queue at the back if it is not
    already queued, then enforcing the cap. *)
let set t ~key ~value =
  (match Hashtbl.find_opt t.seqs key with
   | Some _ -> ()
   | None ->
     let seq = t.next_seq in
     t.next_seq <- seq + 1;
     Hashtbl.replace t.seqs key seq;
     Queue.push (key, seq) t.order);
  Hashtbl.replace t.map key value;
  evict t
;;

(** Retires [key]. O(1).

    The admission record is dropped as well so that a later re-insert of the same order id
    is treated as new work and admitted at the back of the queue, where it belongs. The
    superseded queue entry is skipped on pop. *)
let remove t key =
  Hashtbl.remove t.map key;
  Hashtbl.remove t.seqs key
;;

(** Empties the index. Used on reconnect, where every cached id is stale. *)
let clear t =
  Hashtbl.clear t.map;
  Hashtbl.clear t.seqs;
  Queue.clear t.order
;;

(** Registers this index with [Cache_metrics] under [name], so its occupancy and eviction
    counters reach the dashboard. Call once, after the index is created.

    Occupancy against capacity is the number worth watching: a steady-state [entries] that
    has crept up to [capacity] means the cap is doing real work and is probably too tight
    for the venue's peak, while [stale_skips] climbing without [evictions] climbing means
    the eviction queue is being rebuilt. *)
let publish_metrics t ~name =
  let open Cache_metrics in
  Cache_metrics.register name (fun () ->
    let s = stats t in
    { name
    ; metrics =
        [ "entries", Count s.entries
        ; "capacity", Count s.capacity
        ; "queued", Count s.queued
        ; "pending_queue", Count s.pending_queue
        ; "evictions", Count s.evictions
        ; "stale_skips", Count s.stale_skips
        ; "trims", Count s.trims
        ]
    })
;;

(** Retunes the cap from the volume observed once the startup snapshot has been ingested:
    [observed * 1.5 + 1], floored at [floor] and clamped to [[min_cap], [default_cap]].

    This adjusts a bound that already exists; it is not what makes the index bounded. *)
let lock_cap t ~observed ~floor =
  let tuned = max floor (observed + (observed / 2) + 1) in
  let clamped = max min_cap (min default_cap tuned) in
  let previous = t.cap in
  t.cap <- clamped;
  (* Tightening below the current size evicts straight away rather than leaving the index
     over its new cap until the next insert. *)
  evict t;
  Logging.debug_f
    ~section:(Printf.sprintf "%s_index" t.label)
    "cap tuned %d -> %d (observed %d entries at startup, floor %d, %d entries now)"
    previous
    clamped
    observed
    floor
    (Hashtbl.length t.map)
;;
