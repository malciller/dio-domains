(** Bounded ring buffer with FIFO eviction semantics.

    Provides a fixed-capacity circular buffer backed by atomic variables.
    Designed for single-writer, multi-reader market data feeds (ticker,
    orderbook, executions) where bounded memory usage is required.
    Overwrites the oldest entry when the buffer is full.

    Positions are ABSOLUTE and monotonic: [write_pos] counts total writes
    and never wraps. Consumer cursors returned by [get_position] /
    [iter_since] / [read_since] stay valid for the lifetime of the buffer,
    including across laps and [clear].

    Lap policy: a reader starting behind [write_pos - size] resumes at the
    oldest surviving entry; evicted entries are skipped deterministically.
    During iteration, a slot whose stored sequence number does not match
    the expected position was overwritten mid-iteration (the reader is being
    lapped live) and is skipped rather than mis-delivered.

    Cross-domain safety (seqlock discipline): the writer publishes each slot
    under a sentinel protocol - [seq] is set to a reserved "writing" value
    before the payload is stored and only set to the slot's absolute position
    once the payload is complete. Readers validate [seq] both BEFORE and
    AFTER reading the payload; a payload is delivered only when both checks
    match. Because [seq] can never equal a valid position while the payload
    is mid-update, a reader can neither observe a torn payload nor a newer
    payload mis-delivered at an older cursor, and the [Obj.magic 0] empty
    marker can never escape [clear].

    Performance: Slots are pre-allocated mutable records, making [write]
    completely allocation-free (zero minor heap allocation). Power-of-two
    capacities bypass integer division with bitwise masking.
*)

(** Fixed-capacity circular buffer parameterized over element type. *)
module RingBuffer = struct
  type 'a slot =
    { mutable value : 'a
    ; seq : int Atomic.t
      (** Absolute sequence number of the write that produced this slot.
          Reserved sentinels: -1 = empty/cleared, -2 = writer mid-update. *)
    }

  type 'a t =
    { slots : 'a slot array
    ; write_pos : int Atomic.t (** Total writes ever performed; never resets. *)
    ; size : int
    ; mask : int (** Non-negative mask if size is a power of 2, else -1 *)
    }

  (** [create size] allocates a ring buffer with [size] slots.
      @raise Invalid_argument if [size <= 0]. *)
  let create size =
    if size <= 0 then invalid_arg "RingBuffer.create: size must be positive";
    let is_pow2 = size land (size - 1) = 0 in
    let mask = if is_pow2 then size - 1 else -1 in
    let slots =
      Array.init size (fun _ -> { value = Obj.magic 0; seq = Atomic.make (-1) })
    in
    { slots; write_pos = Atomic.make 0; size; mask }
  ;;

  let[@inline always] index_of buffer pos =
    if buffer.mask >= 0 then pos land buffer.mask else pos mod buffer.size
  ;;

  (** [write buffer value] stores [value] at the current write position
      and advances the (absolute) index. Single-writer only; concurrent
      writers require external synchronization.
      Zero allocation on the hot path. The sentinel protocol guarantees a
      concurrent reader can never observe a half-written slot: [seq] equals
      a valid position only while [value] holds the complete payload. *)
  let[@inline always] write buffer value =
    let pos = Atomic.get buffer.write_pos in
    let idx = index_of buffer pos in
    let slot = buffer.slots.(idx) in
    Atomic.set slot.seq (-2);
    (* Lock out readers before touching the payload. *)
    slot.value <- value;
    Atomic.set slot.seq pos;
    (* Publish: valid positions only ever name complete payloads. *)
    Atomic.set buffer.write_pos (pos + 1)
  ;;

  (** [read_slot buffer pos] reads the slot for absolute position [pos]
      under seqlock discipline. Returns [Some value] only when [seq] matched
      [pos] both before and after the payload read - the value is then the
      complete event written at [pos], not a mid-update or lapped payload.
      Returns [None] when the slot is empty/cleared or is being overwritten
      live (the reader is being lapped); the entry is skipped, per the lap
      policy, rather than mis-delivered. *)
  let[@inline] read_slot buffer pos =
    let idx = index_of buffer pos in
    let slot = buffer.slots.(idx) in
    if Atomic.get slot.seq = pos
    then (
      let value = slot.value in
      if Atomic.get slot.seq = pos then Some value else None)
    else None
  ;;

  (** [read_latest buffer] returns the most recently written element,
      or [None] if the buffer is empty. Walks back over at most [size]
      slots so a writer racing the reader cannot produce a missed read. *)
  let read_latest buffer =
    let current = Atomic.get buffer.write_pos in
    if current = 0
    then None
    else (
      let oldest = max 0 (current - buffer.size) in
      let rec go pos =
        if pos < oldest
        then None
        else (
          match read_slot buffer pos with
          | Some value -> Some value
          | None -> go (pos - 1))
      in
      go (current - 1))
  ;;

  (** Clamp a consumer cursor to the oldest surviving entry. Entries below
      the clamp were evicted by the writer lapping the consumer. *)
  let[@inline always] clamp_start buffer last_pos current =
    let oldest = current - buffer.size in
    if last_pos < oldest then oldest else if last_pos < 0 then 0 else last_pos
  ;;

  (** [read_since buffer last_pos] collects all elements written since
      [last_pos] up to the current write position. Returns an empty list
      if no new elements exist. *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos >= current_pos
    then []
    else (
      let start = clamp_start buffer last_pos current_pos in
      let rec collect acc pos =
        if pos >= current_pos
        then List.rev acc
        else (
          let matched =
            match read_slot buffer pos with
            | Some value -> [ value ]
            | None -> []
          in
          collect (List.rev_append matched acc) (pos + 1))
      in
      collect [] start)
  ;;

  (** [iter_since buffer last_pos f] applies [f] to each element from
      [last_pos] to the current write position without allocating a list.
      Returns the new ABSOLUTE position for the caller to track
      consumption. Inlined to reduce closure overhead on the hot path. *)
  let[@inline always] iter_since buffer last_pos f =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos >= current_pos
    then current_pos
    else (
      let pos = ref (clamp_start buffer last_pos current_pos) in
      while !pos < current_pos do
        (match read_slot buffer !pos with
         | Some value -> f value
         | None -> ());
        incr pos
      done;
      current_pos)
  ;;

  (** [read_all buffer] returns every currently-stored element ordered by
      write sequence (oldest first). Safe against concurrent writers:
      entries carry their sequence numbers and are only delivered after a
      double validation, so the result is always a consistent ordering of
      complete entries even if the writer advances during the scan. *)
  let read_all buffer =
    let current = Atomic.get buffer.write_pos in
    let oldest = max 0 (current - buffer.size) in
    let acc = ref [] in
    Array.iter
      (fun slot ->
         let p = Atomic.get slot.seq in
         if p >= oldest && p < current
         then (
           let value = slot.value in
           (* Re-validate: the slot may have been rewritten mid-read. *)
           if Atomic.get slot.seq = p then acc := (p, value) :: !acc))
      buffer.slots;
    List.sort (fun (a, _) (b, _) -> compare a b) !acc |> List.map snd
  ;;

  (** [get_position buffer] returns the current ABSOLUTE write index.
      Consumers use this value as the [last_pos] argument to [read_since]
      or [iter_since]. Unlike the previous modulo-based positions, the
      returned cursor remains comparable across laps and clears. *)
  let[@inline always] get_position buffer = Atomic.get buffer.write_pos

  (** [clear buffer] drops all stored entries. The write position is NOT
      reset: consumer cursors stay valid (entries written after the clear
      get higher positions than anything before it), so a reader that held
      a pre-clear cursor observes "no new data" until the next write
      instead of stalling or re-reading stale slots. Not safe to call
      concurrently with writers. Safe against concurrent readers: [seq] is
      invalidated FIRST, so no reader can validate-then-read a slot whose
      payload is about to be clobbered, and the [Obj.magic 0] marker can
      never be delivered (readers double-validate). *)
  let clear buffer =
    Array.iter
      (fun slot ->
         Atomic.set slot.seq (-1);
         slot.value <- Obj.magic 0)
      buffer.slots
  ;;

  (** [capacity buffer] returns the fixed slot count of the buffer. *)
  let[@inline always] capacity buffer = buffer.size
end
