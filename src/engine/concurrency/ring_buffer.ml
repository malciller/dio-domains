(** Bounded Ring Buffer - Generic lock-free ring buffer with FIFO eviction

    Used for Layer 1 caches to prevent unbounded memory growth.
    Supports thread-safe operations with atomic variables.
*)

(** Generic bounded ring buffer with FIFO eviction *)
module RingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    size: int;
    count: int Atomic.t;  (* Track number of valid entries *)
  }

  (** Create a new ring buffer with the specified capacity *)
  let create size =
    if size <= 0 then
      invalid_arg "RingBuffer.create: size must be positive";
    {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      size;
      count = Atomic.make 0;
    }

  (** Lock-free write - overwrites oldest entry when full (FIFO eviction) *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    Atomic.set buffer.data.(pos) (Some value);
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos;
    let current_count = Atomic.get buffer.count in
    if current_count < buffer.size then
      ignore (Atomic.compare_and_set buffer.count current_count (current_count + 1))
    (* When full, we overwrite oldest but count stays at size *)

  (** Get current number of valid entries *)
  let length buffer = Atomic.get buffer.count

  (** Get capacity of the buffer *)
  let capacity buffer = buffer.size

  (** Read all current entries in the buffer (for snapshot rebuilding) *)
  let read_all buffer =
    let write_pos = Atomic.get buffer.write_pos in
    let count = Atomic.get buffer.count in
    if count = 0 then
      []
    else
      let rec collect acc remaining pos =
        if remaining <= 0 then
          List.rev acc
        else
          match Atomic.get buffer.data.(pos) with
          | Some value -> collect (value :: acc) (remaining - 1) ((pos + 1) mod buffer.size)
          | None -> collect acc (remaining - 1) ((pos + 1) mod buffer.size)
      in
      (* Start reading from the oldest entry *)
      let start_pos = if count < buffer.size then 0 else write_pos in
      collect [] count start_pos

  (** Read latest entry (most recent) *)
  let read_latest buffer =
    if Atomic.get buffer.count = 0 then
      None
    else
      let write_pos = Atomic.get buffer.write_pos in
      let read_pos = if write_pos = 0 then buffer.size - 1 else write_pos - 1 in
      Atomic.get buffer.data.(read_pos)

  (** Read entries from a specific position onwards (for incremental updates) *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos || Atomic.get buffer.count = 0 then
      []
    else
      let rec collect acc pos =
        if pos = current_pos then
          List.rev acc
        else
          match Atomic.get buffer.data.(pos) with
          | Some value -> collect (value :: acc) ((pos + 1) mod buffer.size)
          | None -> collect acc ((pos + 1) mod buffer.size)
      in
      collect [] last_pos

  (** Clear all entries in the buffer *)
  let clear buffer =
    Array.iter (fun atomic -> Atomic.set atomic None) buffer.data;
    Atomic.set buffer.write_pos 0;
    Atomic.set buffer.count 0

  (** Check if buffer is empty *)
  let is_empty buffer = Atomic.get buffer.count = 0

  (** Check if buffer is at full capacity *)
  let is_full buffer = Atomic.get buffer.count >= buffer.size
end
