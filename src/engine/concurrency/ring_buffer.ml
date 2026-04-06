(** Bounded ring buffer with FIFO eviction semantics.

    Provides a fixed-capacity circular buffer backed by atomic variables.
    Designed for single-writer, multi-reader market data feeds (ticker,
    orderbook, executions) where bounded memory usage is required.
    Overwrites the oldest entry when the buffer is full.
*)

(** Fixed-capacity circular buffer parameterized over element type. *)
module RingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    size: int;
  }

  (** [create size] allocates a ring buffer with [size] slots.
      @raise Invalid_argument if [size <= 0]. *)
  let create size =
    if size <= 0 then
      invalid_arg "RingBuffer.create: size must be positive";
    {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      size;
    }

  (** [write buffer value] stores [value] at the current write position
      and advances the index modulo capacity. Single-writer only;
      concurrent writers require external synchronization. *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos) (Some value);
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos

  (** [read_latest buffer] returns the most recently written element,
      or [None] if the buffer is empty. Wait-free for readers. *)
  let read_latest buffer =
    let pos = Atomic.get buffer.write_pos in
    let read_pos = if pos = 0 then buffer.size - 1 else pos - 1 in
    Atomic.get buffer.data.(read_pos)

  (** [read_since buffer last_pos] collects all elements written since
      [last_pos] up to the current write position. Returns an empty list
      if no new elements exist. Allocates an intermediate list. *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos then
      []
    else
      let rec collect acc pos =
        if pos = current_pos then
          List.rev acc
        else
          match Atomic.get buffer.data.(pos) with
          | Some event -> collect (event :: acc) ((pos + 1) mod buffer.size)
          | None -> collect acc ((pos + 1) mod buffer.size)
      in
      collect [] last_pos

  (** [iter_since buffer last_pos f] applies [f] to each element from
      [last_pos] to the current write position without allocating a list.
      Returns the new position for the caller to track consumption.
      Inlined to reduce closure overhead on the hot path. *)
  let[@inline] iter_since buffer last_pos f =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos then
      current_pos
    else begin
      let pos = ref last_pos in
      while !pos <> current_pos do
        (match Atomic.get buffer.data.(!pos) with
         | Some event -> f event
         | None -> ());
        pos := (!pos + 1) mod buffer.size
      done;
      current_pos
    end

  (** [read_all buffer] returns every non-empty slot as a list,
      scanning indices 0 through [size - 1] in order. *)
  let read_all buffer =
    let rec collect_all acc i =
      if i >= buffer.size then
        List.rev acc
      else
        match Atomic.get buffer.data.(i) with
        | Some event -> collect_all (event :: acc) (i + 1)
        | None -> collect_all acc (i + 1)
    in
    collect_all [] 0

  (** [get_position buffer] returns the current write index.
      Consumers use this value as the [last_pos] argument to
      [read_since] or [iter_since]. *)
  let get_position buffer =
    Atomic.get buffer.write_pos

  (** [clear buffer] resets all slots to [None] and the write
      position to 0. Not safe to call concurrently with writers. *)
  let clear buffer =
    Array.iter (fun atomic -> Atomic.set atomic None) buffer.data;
    Atomic.set buffer.write_pos 0

  (** [capacity buffer] returns the fixed slot count of the buffer. *)
  let capacity buffer = buffer.size
end
