(** Common types and infrastructure shared across all trading strategies.
    Provides unified order representation, in-flight deduplication caches,
    a ring buffer for order queuing, and the strategy module signature. *)

open Lwt.Infix

module StringMap = Map.Make(String)

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
    Uses string concatenation to avoid intermediate buffer allocations. *)
let generate_duplicate_key symbol side quantity limit_price =
  let q_str = string_of_float quantity in
  match limit_price with
  | Some p -> symbol ^ "|" ^ side ^ "|" ^ q_str ^ "|" ^ string_of_float p
  | None   -> symbol ^ "|" ^ side ^ "|" ^ q_str ^ "|market"

(** In-flight order cache for deduplication of pending place/cancel requests. *)
module InFlightOrders = struct
  let registry : (string, float) Hashtbl.t = Hashtbl.create 1024
  let mutex = Mutex.create ()

  (** Atomically insert [duplicate_key] if absent. Returns true on insertion,
      false if the key was already present. *)
  let add_in_flight_order duplicate_key =
    let now = Unix.gettimeofday () in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry duplicate_key in
    if not exists then Hashtbl.replace registry duplicate_key now;
    Mutex.unlock mutex;
    not exists

  (** Remove [duplicate_key] from the cache. Returns true if it was present. *)
  let remove_in_flight_order duplicate_key =
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry duplicate_key in
    if exists then Hashtbl.remove registry duplicate_key;
    Mutex.unlock mutex;
    exists

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    Mutex.lock mutex;
    let size = Hashtbl.length registry in
    Mutex.unlock mutex;
    size

  let last_cleanup = Atomic.make 0.0

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age=60.0) () =
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_cleanup in
    if now -. last > 1.0 && Atomic.compare_and_set last_cleanup last now then begin
      Mutex.lock mutex;
      let initial_size = Hashtbl.length registry in
      Hashtbl.filter_map_inplace (fun _ timestamp ->
        if now -. timestamp <= max_age then Some timestamp else None
      ) registry;
      let final_size = Hashtbl.length registry in
      Mutex.unlock mutex;
      (0, initial_size - final_size)
    end else
      (0, 0)

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** In-flight amendment cache for deduplication of pending amend requests. *)
module InFlightAmendments = struct
  let registry : (string, float) Hashtbl.t = Hashtbl.create 1024
  let mutex = Mutex.create ()

  (** Atomically insert [order_id] if absent. Returns true on insertion,
      false if already tracked. *)
  let add_in_flight_amendment order_id =
    let now = Unix.gettimeofday () in
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    if not exists then Hashtbl.replace registry order_id now;
    Mutex.unlock mutex;
    not exists

  (** Returns true if [order_id] has a pending amendment. *)
  let is_in_flight order_id =
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    Mutex.unlock mutex;
    exists

  (** Remove [order_id] from the cache. Returns true if it was present. *)
  let remove_in_flight_amendment order_id =
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    if exists then Hashtbl.remove registry order_id;
    Mutex.unlock mutex;
    exists

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    Mutex.lock mutex;
    let size = Hashtbl.length registry in
    Mutex.unlock mutex;
    size

  let last_cleanup = Atomic.make 0.0

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age=60.0) () =
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_cleanup in
    if now -. last > 1.0 && Atomic.compare_and_set last_cleanup last now then begin
      Mutex.lock mutex;
      let initial_size = Hashtbl.length registry in
      Hashtbl.filter_map_inplace (fun _ timestamp ->
        if now -. timestamp <= max_age then Some timestamp else None
      ) registry;
      let final_size = Hashtbl.length registry in
      Mutex.unlock mutex;
      (0, initial_size - final_size)
    end else
      (0, 0)

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** Fixed-size, zero-allocation MPSC ring buffer.
    Replaces the Michael-Scott queue to eliminate node allocations on the hot path.
    Uses padding to prevent false sharing between producer and consumer domains. *)
module LockFreeQueue = struct
  type 'a t = {
    array: 'a option Atomic.t array;
    size: int;
    mask: int;
    head: int Atomic.t;
    _pad1: int64; _pad2: int64; _pad3: int64; _pad4: int64;
    _pad5: int64; _pad6: int64; _pad7: int64;
    tail: int Atomic.t;
  }

  let create ?(size=65536) () =
    (* Ensure size is a power of 2 for fast masking *)
    let rec next_power_of_2 v p = if p >= v then p else next_power_of_2 v (p * 2) in
    let size = next_power_of_2 size 1 in
    let mask = size - 1 in
    let array = Array.init size (fun _ -> Atomic.make None) in
    {
      array;
      size;
      mask;
      head = Atomic.make 0;
      _pad1 = 0L; _pad2 = 0L; _pad3 = 0L; _pad4 = 0L;
      _pad5 = 0L; _pad6 = 0L; _pad7 = 0L;
      tail = Atomic.make 0;
    }

  (** Concurrent enqueue for multiple producers. Returns None if full. *)
  let write q v =
    let rec loop () =
      let t = Atomic.get q.tail in
      let h = Atomic.get q.head in
      if t - h >= q.size then None (* Queue is full *)
      else if Atomic.compare_and_set q.tail t (t + 1) then begin
        Atomic.set q.array.(t land q.mask) (Some v);
        Some ()
      end else
        loop ()
    in loop ()

  (** Single consumer dequeue. *)
  let read q =
    let h = Atomic.get q.head in
    if h = Atomic.get q.tail then None (* Queue is empty *)
    else begin
      let slot = q.array.(h land q.mask) in
      (* Spin if the producer has claimed the index but not yet written the value *)
      let rec spin () =
        match Atomic.get slot with
        | None -> Domain.cpu_relax (); spin ()
        | Some v ->
            Atomic.set slot None;
            Atomic.set q.head (h + 1);
            Some v
      in spin ()
    end

  (** O(1) size tracking. Safe for hot loops. *)
  let size q =
    let count = Atomic.get q.tail - Atomic.get q.head in
    if count < 0 then 0 else count

  let read_batch q max_items =
    let rec read_n acc n =
      if n >= max_items then List.rev acc
      else match read q with
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
  val handle_order_acknowledged : now:float -> string -> string -> order_side -> float -> unit
  val handle_order_rejected : now:float -> string -> order_side -> float -> unit
  val handle_order_cancelled : now:float -> string -> string -> order_side -> string option -> unit
  val handle_order_filled : now:float -> string -> string -> order_side -> fill_price:float -> string option -> unit
  val handle_order_amended : now:float -> string -> string -> string -> order_side -> float -> unit
  val handle_order_amendment_skipped : now:float -> string -> string -> order_side -> float -> unit
  val handle_order_amendment_failed : now:float -> string -> string -> order_side -> string -> unit
  val handle_order_failed : now:float -> string -> order_side -> string -> unit
  val cleanup_pending_cancellation : string -> string -> unit
  val cleanup_strategy_state : string -> unit
  val init : unit -> unit
end

