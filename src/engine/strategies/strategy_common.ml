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
  let registry = Atomic.make StringMap.empty

  (** Atomically insert [duplicate_key] if absent. Returns true on insertion,
      false if the key was already present. *)
  let add_in_flight_order duplicate_key =
    let now = Unix.gettimeofday () in
    let rec loop () =
      let map = Atomic.get registry in
      if StringMap.mem duplicate_key map then
        false
      else
        let new_map = StringMap.add duplicate_key now map in
        if Atomic.compare_and_set registry map new_map then
          true
        else loop ()
    in loop ()

  (** Remove [duplicate_key] from the cache. Returns true if it was present. *)
  let rec remove_in_flight_order duplicate_key =
    let map = Atomic.get registry in
    if not (StringMap.mem duplicate_key map) then
      false
    else
      let new_map = StringMap.remove duplicate_key map in
      if Atomic.compare_and_set registry map new_map then
        true
      else remove_in_flight_order duplicate_key

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    StringMap.cardinal (Atomic.get registry)

  let last_cleanup = Atomic.make 0.0

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age=60.0) () =
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_cleanup in
    if now -. last > 1.0 && Atomic.compare_and_set last_cleanup last now then begin
      let rec modify_map () =
        let map = Atomic.get registry in
        let new_map = StringMap.filter (fun _key timestamp -> now -. timestamp <= max_age) map in
        let trimmed = StringMap.cardinal map - StringMap.cardinal new_map in
        if Atomic.compare_and_set registry map new_map then
          (0, trimmed)
        else
          modify_map ()
      in
      modify_map ()
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
  let registry = Atomic.make StringMap.empty

  (** Atomically insert [order_id] if absent. Returns true on insertion,
      false if already tracked. *)
  let add_in_flight_amendment order_id =
    let now = Unix.gettimeofday () in
    let rec loop () =
      let map = Atomic.get registry in
      if StringMap.mem order_id map then
        false
      else
        let new_map = StringMap.add order_id now map in
        if Atomic.compare_and_set registry map new_map then
          true
        else loop ()
    in loop ()

  (** Returns true if [order_id] has a pending amendment. *)
  let is_in_flight order_id =
    StringMap.mem order_id (Atomic.get registry)

  (** Remove [order_id] from the cache. Returns true if it was present. *)
  let rec remove_in_flight_amendment order_id =
    let map = Atomic.get registry in
    if not (StringMap.mem order_id map) then
      false
    else
      let new_map = StringMap.remove order_id map in
      if Atomic.compare_and_set registry map new_map then
        true
      else remove_in_flight_amendment order_id

  (** Return the number of entries currently tracked. *)
  let get_registry_size () =
    StringMap.cardinal (Atomic.get registry)

  let last_cleanup = Atomic.make 0.0

  (** Evict entries older than [max_age] seconds. Returns [(0, removed_count)]. *)
  let cleanup ?(max_age=60.0) () =
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_cleanup in
    if now -. last > 1.0 && Atomic.compare_and_set last_cleanup last now then begin
      let rec modify_map () =
        let map = Atomic.get registry in
        let new_map = StringMap.filter (fun _key timestamp -> now -. timestamp <= max_age) map in
        let trimmed = StringMap.cardinal map - StringMap.cardinal new_map in
        if Atomic.compare_and_set registry map new_map then
          (0, trimmed)
        else
          modify_map ()
      in
      modify_map ()
    end else
      (0, 0)

  (** Return a closure compatible with the event registry cleanup interface. *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** Unbounded lock-free workflow queue using Michael-Scott Queue architecture.
    Completely replaces OrderRingBuffer, eliminating cross-domain pipeline mutex contention. *)
module LockFreeQueue = struct
  type 'a node =
    | Nil
    | Node of 'a option * 'a node Atomic.t

  type 'a t = {
    head: 'a node Atomic.t;
    tail: 'a node Atomic.t;
  }

  let create () =
    let dummy = Node (None, Atomic.make Nil) in
    { head = Atomic.make dummy; tail = Atomic.make dummy }

  (** Concurrent enqueue mapping to former write semantics. *)
  let write q v =
    let new_node = Node (Some v, Atomic.make Nil) in
    let rec loop () =
      let tail_node = Atomic.get q.tail in
      match tail_node with
      | Nil -> assert false
      | Node (_, next) ->
          let next_node = Atomic.get next in
          if tail_node == Atomic.get q.tail then begin
            match next_node with
            | Nil ->
                if Atomic.compare_and_set next Nil new_node then begin
                  ignore (Atomic.compare_and_set q.tail tail_node new_node);
                  Some ()
                end else
                  loop ()
            | Node _ ->
                ignore (Atomic.compare_and_set q.tail tail_node next_node);
                loop ()
          end else
            loop ()
    in loop ()

  (** Concurrent dequeue mapping to former read semantics. *)
  let read q =
    let rec loop () =
      let head_node = Atomic.get q.head in
      let tail_node = Atomic.get q.tail in
      match head_node with
      | Nil -> assert false
      | Node (_, next) ->
          let next_node = Atomic.get next in
          if head_node == Atomic.get q.head then begin
            if head_node == tail_node then begin
              match next_node with
              | Nil -> None
              | Node _ ->
                  ignore (Atomic.compare_and_set q.tail tail_node next_node);
                  loop ()
            end else begin
              match next_node with
              | Nil -> assert false
              | Node (v_opt, _) ->
                  if Atomic.compare_and_set q.head head_node next_node then
                    v_opt
                  else
                    loop ()
            end
          end else
            loop ()
    in loop ()

  (** Costly size counting proxy - don't use on hot loops. *)
  let size q =
    let rec count n node =
      match node with
      | Nil -> n
      | Node (_, next) -> count (n + 1) (Atomic.get next)
    in
    match Atomic.get q.head with
    | Nil -> 0
    | Node (_, next) -> count 0 (Atomic.get next)

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

