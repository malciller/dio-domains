(** Copy-on-write sharded hash map, safe for concurrent multi-domain reads and writes.

    A plain [Hashtbl] shared across OCaml 5 domains cannot be read while another domain
    writes it. A resize mutates the bucket array in place, so a concurrent reader can
    observe a torn bucket pointer and spin or fault. Locking every read repairs that but
    puts a shared lock back on the hot path, which defeats the purpose of the caches that
    use these tables.

    This structure splits the keyspace across a fixed power-of-two array of shards. Each
    shard is an immutable open-addressed map published through its own [Atomic]:

    - A read is one array index, one [Atomic.get] and one array probe — lock-free,
      allocation-free, and safe against a concurrent writer, because a published shard is
      never mutated again. ([find_opt] boxes the value into a fresh option per hit;
      [find_default] destructures in place and allocates nothing.)
    - A write copies only the shard it lands in and CAS-replaces that one slot, so the
      copy cost is O(size of one shard) rather than O(size of the whole map). The obvious
      alternative — [Hashtbl.copy] of the entire table per insert — is quadratic in the
      number of keys and is what this type exists to avoid.

    The outer array is fixed at [create] and never resized, so there is no second resize
    to race on either.

    **Why shards are hand-rolled rather than [Hashtbl]s.** A shard of plain [Hashtbl]
    would hash the key twice per lookup: once here to pick the shard, then again inside
    that shard's [Hashtbl]. On the instrument read path — the hottest lookup in the
    engine, hit on every strategy decision and every order encode — that measured 77ns
    against 33ns for a bare unsynchronized [Hashtbl], a tax of roughly 18% on a 250ns
    strategy cycle, to buy safety that the unlocked [Hashtbl] did not actually have.
    Deriving both the shard index and the slot index from one [Hashtbl.hash] brings it to
    36ns, within 7% of the racy baseline and ~15x faster than the mutex-guarded
    alternative. See [test/engine/perf/cow_read.ml].

    Sizing: pass a shard count around 2-8x the expected live key count, rounded up to a
    power of two. Too few makes every write copy a large shard; too many wastes an array
    of atomics and hurts cache locality. [create] rounds up automatically, so a
    non-power-of-two argument is fine.

    Contention: [set], [add_if_absent] and [remove] retry on CAS failure without a mutex,
    so a hot shard under many concurrent writers copies more than once and can livelock.
    That is acceptable for the write rates here (config load, instrument warm-up, periodic
    refresh). If a caller already holds a mutex, [set] still works — the CAS simply
    succeeds on the first attempt. *)

let default_shard_count = 64

(** Smallest power of two >= [n], so shard selection can mask instead of divide. *)
let round_up_pow2 n =
  let n = max 1 n in
  let rec go p = if p >= n || p > max_int / 2 then p else go (p * 2) in
  go 1
;;

(* --- shard: open-addressed map keyed by a precomputed hash ---------------------- *)

(* Occupancy target before growing. Linear probing degrades quickly past ~70%, and growth
   is a copy of the whole shard, so 50% keeps the expected probe count near 1.5. *)
let load_factor_denominator = 2
let initial_slot_count = 8

type ('key, 'value) shard =
  { mutable slots : ('key * 'value) option array
  ; mutable mask : int (** Array length - 1; lengths are powers of two. *)
  ; shift : int
  (** Low bits of the hash consumed by shard selection. Must be constant across all shards
      of one table, since it decides the slot index. *)
  ; mutable count : int
  }

(** Slot to probe first. The shard index consumes the low [shift] bits of the hash, so the
    slot index must come from higher bits — reusing the low ones would confine every key
    in a shard to the same few slots. *)
let[@inline] slot_start s h = (h lsr s.shift) land s.mask

let shard_create shift =
  { slots = Array.make initial_slot_count None
  ; mask = initial_slot_count - 1
  ; shift
  ; count = 0
  }
;;

(** Copy of [s] sharing nothing mutable with it: the result is private to the caller until
    it is published with [Atomic.compare_and_set]. *)
let shard_copy s =
  { slots = Array.copy s.slots; mask = s.mask; shift = s.shift; count = s.count }
;;

(** Returns the slot cell holding [k], or [None]. Returning the cell lets [find_default]
    destructure it instead of re-boxing. *)
let shard_find s h k =
  let slots = s.slots in
  let mask = s.mask in
  let rec probe i =
    match slots.(i) with
    | None -> None
    | Some (k', _) as cell -> if k' = k then cell else probe ((i + 1) land mask)
  in
  probe (slot_start s h)
;;

(* Doubling the slot array and rehashing. Amortised O(1) per insert, and only on the
   writer's private copy, so it never races a reader. *)
let shard_grow s =
  let old = s.slots in
  let new_len = Array.length old * 2 in
  let fresh =
    { slots = Array.make new_len None; mask = new_len - 1; shift = s.shift; count = 0 }
  in
  Array.iter
    (function
      | None -> ()
      | Some (k, v) ->
        let h = Hashtbl.hash k in
        let slots = fresh.slots in
        let mask = fresh.mask in
        let rec probe i =
          match slots.(i) with
          | Some _ -> probe ((i + 1) land mask)
          | None ->
            slots.(i) <- Some (k, v);
            fresh.count <- fresh.count + 1
        in
        probe (slot_start fresh h))
    old;
  s.slots <- fresh.slots;
  s.mask <- fresh.mask;
  s.count <- fresh.count
;;

(** Inserts or replaces [k -> v] in a shard the caller owns exclusively — a fresh copy, or
    a shard being rebuilt during a grow — and grows the slot array if the load factor
    calls for it. Nothing here races a reader: the shard is not reachable from the table
    until the caller publishes it. *)
let shard_write s h k v =
  let slots = s.slots in
  let mask = s.mask in
  let rec probe i =
    match slots.(i) with
    | Some (k', _) when k' = k -> slots.(i) <- Some (k, v)
    | Some _ -> probe ((i + 1) land mask)
    | None ->
      slots.(i) <- Some (k, v);
      s.count <- s.count + 1;
      if s.count * load_factor_denominator > Array.length slots then shard_grow s
  in
  probe (slot_start s h)
;;

(* --- table --------------------------------------------------------------------- *)

type ('key, 'value) t =
  { shards : ('key, 'value) shard Atomic.t array
  ; mask : int (** Shard count - 1; shard counts are powers of two. *)
  ; shift : int (** log2 shard count. *)
  }

let create ?(shard_count = default_shard_count) () =
  let n = round_up_pow2 shard_count in
  let shift =
    let rec go n acc = if n <= 1 then acc else go (n / 2) (acc + 1) in
    go n 0
  in
  { shards = Array.init n (fun _ -> Atomic.make (shard_create shift))
  ; mask = n - 1
  ; shift
  }
;;

let shard_count t = Array.length t.shards
let[@inline] shard_index t h = h land t.mask

let find_opt t k =
  let h = Hashtbl.hash k in
  match shard_find (Atomic.get t.shards.(shard_index t h)) h k with
  | Some (_, v) -> Some v
  | None -> None
;;

(** [find_default t k ~default], allocation-free. Same result as [find_opt] with an
    [Option.get ~default], for read paths that already have a fallback in hand. *)
let find_default t k ~default =
  let h = Hashtbl.hash k in
  match shard_find (Atomic.get t.shards.(shard_index t h)) h k with
  | Some (_, v) -> v
  | None -> default
;;

let find t k =
  let h = Hashtbl.hash k in
  match shard_find (Atomic.get t.shards.(shard_index t h)) h k with
  | Some (_, v) -> v
  | None -> raise Not_found
;;

let mem t k = find_opt t k <> None

(** Publishes [k -> v], replacing any existing binding. Lock-free; retries if another
    domain republished the same shard first. A no-op write (the key is already bound to
    the physically identical value) skips the copy entirely. *)
let set t k v =
  let h = Hashtbl.hash k in
  let i = shard_index t h in
  let rec loop () =
    let old = Atomic.get t.shards.(i) in
    match shard_find old h k with
    | Some (_, v') when v' == v -> ()
    | _ ->
      (* Copy only this shard, mutate the copy privately, then publish it. *)
      let next = shard_copy old in
      shard_write next h k v;
      if not (Atomic.compare_and_set t.shards.(i) old next) then loop ()
  in
  loop ()
;;

(** Binds [k -> v] only if [k] is absent. Returns [true] if this call installed the
    binding, [false] if [k] was already present. *)
let add_if_absent t k v =
  let h = Hashtbl.hash k in
  let i = shard_index t h in
  let rec loop () =
    let old = Atomic.get t.shards.(i) in
    match shard_find old h k with
    | Some _ -> false
    | None ->
      let next = shard_copy old in
      shard_write next h k v;
      if Atomic.compare_and_set t.shards.(i) old next then true else loop ()
  in
  loop ()
;;

(** [true] when [h] lies in the cyclic half-open range (i, j] — the slots a key homed at
    [h] traverses on its way to [j]. An entry whose home is in that range is already
    reachable without the hole at [i] and must stay put; otherwise its only route to [j]
    runs through the hole, so pulling it back preserves reachability. *)
let[@inline] cyclically_between i j h =
  if i <= j then h > i && h <= j else h <= j || h > i
;;

(** Drops [k] if present. Lock-free.

    Backward-shift deletion (Knuth 6.4R), not a bare hole. [None] is the probe sequence's
    end marker, so a hole punched in front of a colliding entry orphans it: [find_opt]
    reports absent for a binding still in the shard, and a later [set] installs a second
    copy, double-counting [count]. Instead each following entry that depends on the hole
    moves back into it and the hole walks forward, until it reaches an empty slot.
    The 50% load cap guarantees one exists, so no cluster-length bound is needed. *)
let remove t k =
  let h = Hashtbl.hash k in
  let i = shard_index t h in
  let rec loop () =
    let old = Atomic.get t.shards.(i) in
    match shard_find old h k with
    | None -> ()
    | Some _ ->
      let next = shard_copy old in
      let slots = next.slots in
      let mask = next.mask in
      let shift = next.shift in
      let rec unlink i =
        match slots.(i) with
        | Some (k', _) when k' = k ->
          slots.(i) <- None;
          next.count <- next.count - 1;
          (* Where the empty slot currently is: starts at the slot just vacated and walks
             forward as entries are pulled back over it. *)
          let hole = ref i in
          let rec pull j =
            match slots.(j) with
            | None -> () (* Cluster ends: the hole is in its final position. *)
            | Some (k', v') ->
              let home = (Hashtbl.hash k' lsr shift) land mask in
              if cyclically_between !hole j home
              then pull ((j + 1) land mask)
              else (
                (* [k'] probed past the hole, so [j] is only reachable while the hole is
                   there. Move it back and continue from the hole it leaves. *)
                slots.(!hole) <- Some (k', v');
                slots.(j) <- None;
                hole := j;
                pull ((j + 1) land mask))
          in
          pull ((i + 1) land mask)
        | Some _ -> unlink ((i + 1) land mask)
        | None -> ()
      in
      unlink (slot_start next h);
      if not (Atomic.compare_and_set t.shards.(i) old next) then loop ()
  in
  loop ()
;;

let length t =
  let total = ref 0 in
  Array.iter (fun s -> total := !total + (Atomic.get s).count) t.shards;
  !total
;;

let is_empty t = length t = 0

(** Empties the table. Not atomic across shards, so a concurrent reader may observe a mix
    of pre- and post-clear shards. Only for teardown and test paths. *)
let clear t = Array.iter (fun s -> Atomic.set s (shard_create t.shift)) t.shards

(** Folds over a snapshot of the bindings. The fold sees each shard as of the moment it
    was read, so it is not a globally consistent view under concurrent writes. *)
let fold f t ~init =
  let acc = ref init in
  Array.iter
    (fun s ->
      let sh = Atomic.get s in
      Array.iter
        (function
          | Some (k, v) -> acc := f k v !acc
          | None -> ())
        sh.slots)
    t.shards;
  !acc
;;

let iter f t = fold (fun k v () -> f k v) t ~init:()
let bindings t = fold (fun k v acc -> (k, v) :: acc) t ~init:[]
