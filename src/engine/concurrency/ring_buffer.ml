(** Bounded Ring Buffer - Generic lock-free ring buffer with FIFO eviction

    Used for market data feeds (ticker, orderbook, executions) to prevent 
    unbounded memory growth. Supports thread-safe operations with atomic variables.
*)

(** Generic bounded ring buffer with FIFO eviction *)
module RingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    size: int;
  }

  (** Create a new ring buffer with the specified capacity *)
  let create size =
    if size <= 0 then
      invalid_arg "RingBuffer.create: size must be positive";
    {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      size;
    }

  (** Lock-free write - single writer, overwrites oldest entry when full *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos) (Some value);
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos

  (** Wait-free read - returns most recent entry *)
  let read_latest buffer =
    let pos = Atomic.get buffer.write_pos in
    let read_pos = if pos = 0 then buffer.size - 1 else pos - 1 in
    Atomic.get buffer.data.(read_pos)

  (** Read events from last known position - for domain consumers *)
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

  (** Read all events currently in the buffer *)
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

  (** Get current write position for tracking consumption *)
  let get_position buffer =
    Atomic.get buffer.write_pos

  (** Clear all entries in the buffer *)
  let clear buffer =
    Array.iter (fun atomic -> Atomic.set atomic None) buffer.data;
    Atomic.set buffer.write_pos 0

  (** Get capacity of the buffer *)
  let capacity buffer = buffer.size
end
