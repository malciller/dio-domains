(** Bounded ring buffer with FIFO eviction semantics.

    Provides a fixed-capacity circular buffer backed by atomic variables.
    Designed for single-writer, multi-reader market data feeds (ticker,
    orderbook, executions) where bounded memory usage is required.
    Overwrites the oldest entry when the buffer is full.

    Positions are ABSOLUTE and monotonic: [write_pos] counts total writes
    and never wraps. Consumer cursors returned by [get_position] /
    [iter_since] / [read_since] stay valid for the lifetime of the buffer,
    including across laps and [clear]. This fixes the previous design where
    positions were taken modulo capacity: a slow consumer whose producer
    lapped it could observe aliased positions (empty-looking buffers with
    dropped events) and re-read overwritten slots as new events.

    Lap policy: a reader starting behind [write_pos - size] resumes at the
    oldest surviving entry; evicted entries are skipped deterministically.
    During iteration, a slot whose stored sequence number does not match
    the expected position was overwritten mid-iteration (the reader is being
    lapped live) and is skipped rather than mis-delivered.
*)

(** Fixed-capacity circular buffer parameterized over element type. *)
module RingBuffer = struct
  type 'a t =
    { data : ('a * int) option Atomic.t array
      (** Each slot carries the absolute sequence number of the write that
          produced it, so readers can distinguish intact entries from slots
          overwritten by a racing writer. *)
    ; write_pos : int Atomic.t (** Total writes ever performed; never resets. *)
    ; size : int
    }

  (** [create size] allocates a ring buffer with [size] slots.
      @raise Invalid_argument if [size <= 0]. *)
  let create size =
    if size <= 0 then invalid_arg "RingBuffer.create: size must be positive";
    { data = Array.init size (fun _ -> Atomic.make None)
    ; write_pos = Atomic.make 0
    ; size
    }
  ;;

  (** [write buffer value] stores [value] at the current write position
      and advances the (absolute) index. Single-writer only; concurrent
      writers require external synchronization. *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos mod buffer.size) (Some (value, pos));
    Atomic.set buffer.write_pos (pos + 1)
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
          match Atomic.get buffer.data.(pos mod buffer.size) with
          | Some (v, p) when p = pos -> Some v
          | _ -> go (pos - 1))
      in
      go (current - 1))
  ;;

  (** Clamp a consumer cursor to the oldest surviving entry. Entries below
      the clamp were evicted by the writer lapping the consumer. *)
  let[@inline] clamp_start buffer last_pos current =
    let oldest = current - buffer.size in
    if last_pos < oldest then oldest else if last_pos < 0 then 0 else last_pos
  ;;

  (** [read_since buffer last_pos] collects all elements written since
      [last_pos] up to the current write position. Returns an empty list
      if no new elements exist. Allocates an intermediate list. *)
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
            match Atomic.get buffer.data.(pos mod buffer.size) with
            | Some (event, p) when p = pos -> [ event ]
            | _ -> []
          in
          collect (List.rev_append matched acc) (pos + 1))
      in
      collect [] start)
  ;;

  (** [iter_since buffer last_pos f] applies [f] to each element from
      [last_pos] to the current write position without allocating a list.
      Returns the new ABSOLUTE position for the caller to track
      consumption. Inlined to reduce closure overhead on the hot path. *)
  let[@inline] iter_since buffer last_pos f =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos >= current_pos
    then current_pos
    else (
      let pos = ref (clamp_start buffer last_pos current_pos) in
      while !pos < current_pos do
        (match Atomic.get buffer.data.(!pos mod buffer.size) with
         | Some (event, p) when p = !pos -> f event
         | _ -> ());
        incr pos
      done;
      current_pos)
  ;;

  (** [read_all buffer] returns every currently-stored element ordered by
      write sequence (oldest first). Safe against concurrent writers:
      entries carry their sequence numbers, so the result is always a
      consistent ordering even if the writer advances during the scan. *)
  let read_all buffer =
    let acc = ref [] in
    Array.iter
      (fun slot ->
         match Atomic.get slot with
         | Some (v, p) -> acc := (p, v) :: !acc
         | None -> ())
      buffer.data;
    List.sort (fun (a, _) (b, _) -> compare a b) !acc |> List.map snd
  ;;

  (** [get_position buffer] returns the current ABSOLUTE write index.
      Consumers use this value as the [last_pos] argument to [read_since]
      or [iter_since]. Unlike the previous modulo-based positions, the
      returned cursor remains comparable across laps and clears. *)
  let get_position buffer = Atomic.get buffer.write_pos

  (** [clear buffer] drops all stored entries. The write position is NOT
      reset: consumer cursors stay valid (entries written after the clear
      get higher positions than anything before it), so a reader that held
      a pre-clear cursor observes "no new data" until the next write
      instead of stalling or re-reading stale slots. Not safe to call
      concurrently with writers. *)
  let clear buffer = Array.iter (fun atomic -> Atomic.set atomic None) buffer.data

  (** [capacity buffer] returns the fixed slot count of the buffer. *)
  let capacity buffer = buffer.size
end
