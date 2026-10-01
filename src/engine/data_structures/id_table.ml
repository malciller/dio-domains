(** Copy-on-write paged array for integer keys, safe for concurrent multi-domain reads and
    writes.

    Venue-assigned ids — IBKR order ids and req ids, exchange sequence numbers — come from
    a monotonic counter, so the key is already an index. Hashing it is wasted work, and
    open addressing over a near-dense keyspace degenerates into a long probe chain whose
    deletion rules are easy to get wrong: [Cow_table] punching a hole on delete orphaned
    every key that had probed past it, silently reporting them absent while they were
    still in the shard.

    Here the id selects a page and an offset, so there is no chain:

    - A read is two array indexes and two [Atomic.get]s. No hashing, no probing, no
      allocation, and safe against a concurrent writer because a published page is never
      mutated again.
    - A write copies only the page it lands in, so cost is independent of how many ids are
      live.
    - [remove] is a single store of [None]. Nothing to repair, so deletion cannot orphan a
      neighbour.

    Pages are allocated lazily and the directory grows by copying pointers, so memory
    tracks the span of ids in use rather than the largest id: sparse 10^9-scale order ids
    cost a page each.

    Negative keys are not representable — lookups return [None], writes are ignored and
    counted in [dropped_negative].

    Integer keys only. Use [Cow_table] for anything else. *)

let default_page_shift = 8
let page_mask_for shift = (1 lsl shift) - 1

type 'v t =
  { dir : 'v option array Atomic.t array Atomic.t
  (** Page directory. Fixed length once published; growth replaces the whole directory,
      copying the page pointers but never a page. *)
  ; page_shift : int
  (** Bits of an id consumed for its in-page offset. 256 entries per page: one indirection
      per read, 2KB copied per write. *)
  ; page_mask : int
  ; count : int Atomic.t
  ; dropped_negative : int Atomic.t
  }

(* One shared empty page for every slot, so an unwritten table costs one array. *)
let create ?(page_shift = default_page_shift) () =
  let empty = Atomic.make (Array.make (1 lsl page_shift) None) in
  { dir = Atomic.make [| empty |]
  ; page_shift
  ; page_mask = page_mask_for page_shift
  ; count = Atomic.make 0
  ; dropped_negative = Atomic.make 0
  }
;;

(** Grows [dir] so page [p] is addressable, doubling until it fits. Existing slots are
    shared; each new slot gets its own empty page — a page is an [Atomic] holding a
    replaceable array, so two slots pointing at one would alias and a write to id 300
    would clobber id 44. *)
let grown_to ~page_size dir p =
  let old_len = Array.length dir in
  if p < old_len
  then dir
  else (
    let new_len = ref (max 1 (old_len * 2)) in
    while !new_len <= p do
      new_len := !new_len * 2
    done;
    (* [Array.init], not [Array.make]: [make] evaluates its element once and fills every
       slot with that value. *)
    let next = Array.init !new_len (fun _ -> Atomic.make (Array.make page_size None)) in
    Array.blit dir 0 next 0 old_len;
    next)
;;

let[@inline] page_index t id = id lsr t.page_shift
let[@inline] page_offset t id = id land t.page_mask

let find_opt t id =
  if id < 0
  then None
  else (
    let dir = Atomic.get t.dir in
    let p = page_index t id in
    if p >= Array.length dir then None else (Atomic.get dir.(p)).(page_offset t id))
;;

(** [find_default t id ~default], allocation-free. Prefer this where a miss already has a
    value in hand — the routing and instrument read paths. *)
let find_default t id ~default =
  if id < 0
  then default
  else (
    let dir = Atomic.get t.dir in
    let p = page_index t id in
    if p >= Array.length dir
    then default
    else (
      match (Atomic.get dir.(p)).(page_offset t id) with
      | Some v -> v
      | None -> default))
;;

let find t id =
  match find_opt t id with
  | Some v -> v
  | None -> raise Not_found
;;

let mem t id = find_opt t id <> None

(** Publishes [id -> v], replacing any existing binding. Lock-free.

    Republishing the physically identical value skips the copy, so an id re-marked dirty
    every cycle with unchanged state costs one read. *)
let set t id v =
  if id < 0
  then Atomic.incr t.dropped_negative
  else (
    let p = page_index t id in
    let off = page_offset t id in
    let rec loop () =
      let dir = Atomic.get t.dir in
      if p >= Array.length dir
      then (
        (* Install a bigger directory, then loop either way: losing the CAS means another
           domain installed one that already has room for [p]. *)
        ignore
          (Atomic.compare_and_set
             t.dir
             dir
             (grown_to ~page_size:(1 lsl t.page_shift) dir p));
        loop ())
      else (
        let page = dir.(p) in
        let entries = Atomic.get page in
        match entries.(off) with
        | Some v' when v' == v -> ()
        | old ->
          let next = Array.copy entries in
          next.(off) <- Some v;
          (* Count only once the CAS has won, or a retry that re-reads an empty slot
             double-counts. Parens are required: without them the [else] binds to the
             inner [if] and a lost CAS returns unit instead of retrying, dropping the
             write. *)
          if Atomic.compare_and_set page entries next
          then (if old = None then Atomic.incr t.count)
          else loop ())
    in
    loop ())
;;

(** Binds [id -> v] only if absent. [true] if this call installed the binding, [false] if
    [id] was already present. *)
let add_if_absent t id v =
  if id < 0
  then false
  else (
    let p = page_index t id in
    let off = page_offset t id in
    let rec loop () =
      let dir = Atomic.get t.dir in
      if p >= Array.length dir
      then (
        (* Same rule as [set]: a lost growth CAS still has to install the id, or this
           reports it absent. *)
        ignore
          (Atomic.compare_and_set
             t.dir
             dir
             (grown_to ~page_size:(1 lsl t.page_shift) dir p));
        loop ())
      else (
        let page = dir.(p) in
        let entries = Atomic.get page in
        match entries.(off) with
        | Some _ -> false
        | None ->
          let next = Array.copy entries in
          next.(off) <- Some v;
          (* Same rule as [set]: the count moves only for a transition this call actually
             publishes. *)
          if Atomic.compare_and_set page entries next
          then (
            Atomic.incr t.count;
            true)
          else loop ())
    in
    loop ())
;;

(** Drops [id] if present. Lock-free.

    One store into a slot addressed directly by the id — no probe chain to repair, so this
    cannot orphan a neighbour. The CAS retries because a lost race would otherwise skip
    the removal and leak the binding until the terminal event, and the caller has already
    reported success. *)
let remove t id =
  if id >= 0
  then (
    let p = page_index t id in
    let off = page_offset t id in
    let dir = Atomic.get t.dir in
    if p >= Array.length dir
    then ()
    else (
      let page = dir.(p) in
      let rec on_page () =
        let entries = Atomic.get page in
        match entries.(off) with
        | None -> ()
        | Some _ ->
          let next = Array.copy entries in
          next.(off) <- None;
          if Atomic.compare_and_set page entries next
          then Atomic.decr t.count
          else on_page ()
      in
      on_page ()))
;;

(** Live binding count. Exact for a quiesced table, since the count only moves alongside a
    CAS that published the transition. Under concurrent set/remove on the same ids two
    domains can interleave so both count against a state that no longer exists; that
    approximation is fine for a metric and a leak check, and matches [Cow_table.length]. *)
let length t = Atomic.get t.count

let is_empty t = length t = 0

(** Publishes an empty directory. Not atomic against in-flight writers — teardown and test
    paths only, same contract as [Cow_table.clear]. *)
let clear t =
  let empty = Atomic.make (Array.make (1 lsl t.page_shift) None) in
  Atomic.set t.dir [| empty |];
  Atomic.set t.count 0
;;

(** Ids whose negative value [set] ignored. Non-zero means a venue sent an id that violates
    the protocol. *)
let dropped_negative t = Atomic.get t.dropped_negative

(** Folds over the live bindings as [f id v acc]. Not a consistent snapshot under
    concurrent writes. An index loop rather than [Array.iteri] because the id comes from the
    offset, which [Array.iteri]'s element argument would make redundant. *)
let fold f t ~init =
  let acc = ref init in
  let dir = Atomic.get t.dir in
  Array.iteri
    (fun p page ->
      let entries = Atomic.get page in
      let base = p lsl t.page_shift in
      for off = 0 to Array.length entries - 1 do
        match entries.(off) with
        | Some v -> acc := f (base + off) v !acc
        | None -> ()
      done)
    dir;
  !acc
;;

let iter f t = fold (fun _ v () -> f v) t ~init:()
let bindings t = fold (fun k v acc -> (k, v) :: acc) t ~init:[]
