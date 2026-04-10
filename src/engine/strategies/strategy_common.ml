(** Common types and infrastructure shared across all trading strategies.
    Provides unified order representation, in-flight deduplication caches,
    a ring buffer for order queuing, and the strategy module signature. *)

open Lwt.Infix

(** Integer userref tags for per-strategy order grouping on the exchange. *)
let strategy_userref_grid = 1  (* grid strategy *)
let strategy_userref_mm = 2    (* market maker strategy *)

(** Returns true if [order_userref] matches the given [strategy_userref]. *)
let is_strategy_order strategy_userref order_userref =
  strategy_userref = order_userref

(** Order side: buy or sell. *)
type order_side = Buy | Sell

let string_of_order_side = function
  | Buy -> "buy"
  | Sell -> "sell"

(** Operation type for a strategy order. *)
type operation_type =
  | Place  (* submit a new order *)
  | Amend  (* modify an existing order *)
  | Cancel (* cancel an existing order *)

let string_of_operation_type = function
  | Place -> "place"
  | Amend -> "amend"
  | Cancel -> "cancel"

(** Discriminant for the strategy that produced an order. *)
type strategy_id = Grid | MM | Hedger

let string_of_strategy_id = function
  | Grid -> "Grid" | MM -> "MM" | Hedger -> "Hedger"

(** Unified order record emitted by all strategies. *)
type strategy_order = {
  operation: operation_type;  (* place, amend, or cancel *)
  order_id: string option;    (* target order ID for amend/cancel; None for place *)
  symbol: string;
  side: order_side;
  order_type: string;  (* e.g. "limit", "market" *)
  qty: float;
  price: float option;
  time_in_force: string;  (* e.g. "GTC", "IOC", "FOK" *)
  post_only: bool;
  userref: int option;  (* exchange-level strategy tag *)
  strategy: strategy_id; (* originating strategy variant *)
  exchange: string;     (* target exchange *)
  duplicate_key: string; (* composite key for deduplication *)
}

(** Build a composite deduplication key from order parameters.
    Uses a single [Buffer] pass to avoid intermediate string allocations
    that nested [Printf.sprintf] calls would incur. *)
let generate_duplicate_key symbol side quantity limit_price =
  let buf = Buffer.create 48 in
  Buffer.add_string buf symbol;
  Buffer.add_char   buf '|';
  Buffer.add_string buf side;
  Buffer.add_char   buf '|';
  Buffer.add_string buf (string_of_float quantity);
  Buffer.add_char   buf '|';
  (match limit_price with
   | Some p -> Buffer.add_string buf (string_of_float p)
   | None   -> Buffer.add_string buf "market");
  Buffer.contents buf

(** In-flight order cache for deduplication of pending place/cancel requests. *)
module InFlightOrders = struct
  (* duplicate_key -> insertion timestamp *)
  let registry : (string, float) Hashtbl.t = Hashtbl.create 16
  let mutex = Mutex.create ()

  (** Atomically insert [duplicate_key] if absent. Returns true on insertion,
      false if the key was already present. *)
  let add_in_flight_order duplicate_key =
    Mutex.lock mutex;
    match Hashtbl.mem registry duplicate_key with
    | true ->
        Mutex.unlock mutex;
        false (* already tracked *)
    | false ->
        Hashtbl.add registry duplicate_key (Unix.gettimeofday ());
        Mutex.unlock mutex;
        true (* newly inserted *)

  (** Remove [duplicate_key] from the cache. Returns true if it was present. *)
  let remove_in_flight_order duplicate_key =
    Mutex.lock mutex;
    let existed = Hashtbl.mem registry duplicate_key in
    if existed then Hashtbl.remove registry duplicate_key;
    Mutex.unlock mutex;
    existed

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    Mutex.lock mutex;
    let size = Hashtbl.length registry in
    Mutex.unlock mutex;
    size

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age=60.0) () =
    Mutex.lock mutex;
    let now = Unix.gettimeofday () in
    let removed = ref 0 in

    (* In-place scan; safe under mutex *)
    Hashtbl.filter_map_inplace (fun _key timestamp ->
      if now -. timestamp > max_age then begin
        incr removed;
        None
      end else
        Some timestamp
    ) registry;

    Mutex.unlock mutex;
    (0, !removed)

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** In-flight amendment cache for deduplication of pending amend requests. *)
module InFlightAmendments = struct
  (* order_id -> insertion timestamp *)
  let registry : (string, float) Hashtbl.t = Hashtbl.create 16
  let mutex = Mutex.create ()

  (** Atomically insert [order_id] if absent. Returns true on insertion,
      false if already tracked. *)
  let add_in_flight_amendment order_id =
    Mutex.lock mutex;
    match Hashtbl.mem registry order_id with
    | true ->
        Mutex.unlock mutex;
        false (* already tracked *)
    | false ->
        Hashtbl.add registry order_id (Unix.gettimeofday ());
        Mutex.unlock mutex;
        true (* newly inserted *)

  (** Returns true if [order_id] has a pending amendment. *)
  let is_in_flight order_id =
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    Mutex.unlock mutex;
    exists

  (** Remove [order_id] from the cache. Returns true if it was present. *)
  let remove_in_flight_amendment order_id =
    Mutex.lock mutex;
    let existed = Hashtbl.mem registry order_id in
    if existed then Hashtbl.remove registry order_id;
    Mutex.unlock mutex;
    existed

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    Mutex.lock mutex;
    let size = Hashtbl.length registry in
    Mutex.unlock mutex;
    size

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age=60.0) () =
    Mutex.lock mutex;
    let now = Unix.gettimeofday () in
    let removed = ref 0 in

    (* In-place scan; safe under mutex *)
    Hashtbl.filter_map_inplace (fun _key timestamp ->
      if now -. timestamp > max_age then begin
        incr removed;
        None
      end else
        Some timestamp
    ) registry;

    Mutex.unlock mutex;
    (0, !removed)

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** Fixed-capacity ring buffer for queuing strategy orders.
    Not thread-safe; callers must hold [order_buffer_mutex]. *)
module OrderRingBuffer = struct
  type 'a t = {
    data: 'a option array;
    mutable write_pos: int;  (* next write index *)
    mutable read_pos: int;   (* next read index *)
    size: int;               (* capacity of the backing array *)
  }

  let create size =
    {
      data = Array.make size None;
      write_pos = 0;
      read_pos = 0;
      size;
    }

  (** Enqueue [value]. Returns [Some ()] on success, [None] if the buffer is full. *)
  let write buffer value =
    let pos = buffer.write_pos in
    let next_pos = (pos + 1) mod buffer.size in
    if next_pos = buffer.read_pos then
      None  (* buffer full *)
    else begin
      buffer.data.(pos) <- Some value;
      buffer.write_pos <- next_pos;
      Some ()
    end

  (** Dequeue the next item. Returns [None] if the buffer is empty. *)
  let read buffer =
    if buffer.read_pos = buffer.write_pos then
      None  (* empty *)
    else
      match buffer.data.(buffer.read_pos) with
      | Some value ->
          buffer.data.(buffer.read_pos) <- None;
          buffer.read_pos <- (buffer.read_pos + 1) mod buffer.size;
          Some value
      | None -> None  (* invariant violation: slot should not be None *)

  (** Return the number of items currently queued. *)
  let size buffer =
    if buffer.write_pos >= buffer.read_pos then
      buffer.write_pos - buffer.read_pos
    else
      buffer.size - buffer.read_pos + buffer.write_pos

  (** Dequeue up to [max_items] entries, returned in FIFO order. *)
  let read_batch buffer max_items =
    let rec read_n acc n =
      if n >= max_items then List.rev acc
      else
        match read buffer with
        | Some item -> read_n (item :: acc) (n + 1)
        | None -> List.rev acc
    in
    read_n [] 0
end

(** Domain-safe order signal channel.
    Domain workers call [broadcast ()] to notify the supervisor's Lwt event loop
    that new orders are available. Implemented via a Unix self-pipe so that a
    single-byte write from any domain wakes the Lwt scheduler without touching
    Lwt internals (Lwt_condition is NOT safe from non-Lwt domains).

    The [pending] atomic flag coalesces multiple rapid broadcasts into a single
    pipe write to avoid saturating the pipe buffer under high order throughput. *)
module OrderSignal = struct
  let read_fd, write_fd =
    let (r, w) = Unix.pipe ~cloexec:true () in
    Unix.set_nonblock r;
    Unix.set_nonblock w;
    (r, w)

  (** Lwt wrapper for the read end of the self-pipe. Created once at module
      init to avoid per-wait allocation. *)
  let lwt_read_fd = Lwt_unix.of_unix_file_descr ~blocking:false ~set_flags:false read_fd

  (** Atomic flag to coalesce multiple broadcasts into one pipe write. *)
  let pending = Atomic.make false

  (** Signal from any domain that new orders are available.
      Safe to call from OCaml 5 domain workers — no Lwt internals are touched. *)
  let broadcast () =
    if not (Atomic.exchange pending true) then
      (* First signaller since last drain; write a single byte to wake Lwt. *)
      (try ignore (Unix.write write_fd (Bytes.make 1 '\x00') 0 1) with
       | Unix.Unix_error (Unix.EAGAIN, _, _) -> ()  (* pipe buffer full; Lwt side will drain *)
       | Unix.Unix_error (Unix.EWOULDBLOCK, _, _) -> ()
       | _ -> ())

  (** Block in the Lwt event loop until the pipe becomes readable (a broadcast
      arrived). Drains the pipe and clears the pending flag before returning. *)
  let wait () =
    Lwt_unix.wait_read lwt_read_fd >>= fun () ->
    (* Drain all accumulated bytes from the pipe. *)
    let buf = Bytes.create 64 in
    (try while true do
       let n = Unix.read read_fd buf 0 64 in
       if n = 0 then raise Exit
     done with _ -> ());
    Atomic.set pending false;
    Lwt.return_unit
end

(** Module signature that all strategy implementations must satisfy. *)
module type S = sig
  type config
  val execute : 
    config -> float option -> (float * float * float * float) option -> 
    float option -> float option -> int -> int -> 
    (string * float * float * string * int option) list -> int -> unit
    
  val get_pending_orders : int -> strategy_order list
  val handle_order_acknowledged : string -> string -> order_side -> float -> unit
  val handle_order_rejected : string -> order_side -> float -> unit
  val handle_order_cancelled : string -> string -> order_side -> unit
  val handle_order_filled : string -> string -> order_side -> fill_price:float -> unit
  val handle_order_amended : string -> string -> string -> order_side -> float -> unit
  val handle_order_amendment_skipped : string -> string -> order_side -> float -> unit
  val handle_order_amendment_failed : string -> string -> order_side -> string -> unit
  val handle_order_failed : string -> order_side -> string -> unit
  val cleanup_pending_cancellation : string -> string -> unit
  val cleanup_strategy_state : string -> unit
  val init : unit -> unit
end

