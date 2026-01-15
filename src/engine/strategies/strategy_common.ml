(**
  Common types shared between strategies (suicide_grid, market_maker, etc.)
  
  This module provides unified types for strategy orders to enable consistent
  order processing in the supervisor regardless of which strategy generates them.
*)

(** Strategy userref values - simple integers to tag orders by strategy
    These are non-unique identifiers that group all orders from a strategy together *)
let strategy_userref_grid = 1  (* Grid strategy *)
let strategy_userref_mm = 2    (* Market maker strategy *)

(** Check if a userref matches a specific strategy *)
let is_strategy_order strategy_userref order_userref =
  strategy_userref = order_userref

(** Order side - buy or sell *)
type order_side = Buy | Sell

let string_of_order_side = function
  | Buy -> "buy"
  | Sell -> "sell"

(** Operation type - what to do with an order *)
type operation_type =
  | Place  (* Place a new order *)
  | Amend  (* Amend an existing order *)
  | Cancel (* Cancel an existing order *)

let string_of_operation_type = function
  | Place -> "place"
  | Amend -> "amend"
  | Cancel -> "cancel"

(** Unified strategy order type used by all strategies *)
type strategy_order = {
  operation: operation_type;  (* What to do with this order *)
  order_id: string option;    (* For amend/cancel operations - the target order ID *)
  symbol: string;
  side: order_side;
  order_type: string;  (* "limit", "market", etc. *)
  qty: float;
  price: float option;
  time_in_force: string;  (* "GTC", "IOC", "FOK" *)
  post_only: bool;
  userref: int option;  (* Strategy identifier: 1=GRID, 2=MM, 3=ARB *)
  strategy: string;     (* Strategy name for order processing *)
  exchange: string;     (* Exchange to execute order on *)
  duplicate_key: string; (* Unique key for duplicate detection *)
}

(** Generate a hash key for duplicate order detection *)
let generate_duplicate_key symbol side quantity limit_price =
  let limit_price_str = match limit_price with
    | Some p -> Printf.sprintf "%.8f" p
    | None -> "market"
  in
  Printf.sprintf "%s|%s|%.8f|%s" symbol side quantity limit_price_str

(** In-flight order cache to prevent duplicate orders *)
module InFlightOrders = struct
  (* Map duplicate_key -> timestamp *)
  let registry : (string, float) Hashtbl.t = Hashtbl.create 1024
  let mutex = Mutex.create ()

  (** Check if an order is already in-flight and add it if not *)
  let add_in_flight_order duplicate_key =
    Mutex.lock mutex;
    match Hashtbl.mem registry duplicate_key with
    | true ->
        Mutex.unlock mutex;
        false (* Already existed *)
    | false ->
        Hashtbl.add registry duplicate_key (Unix.gettimeofday ());
        Mutex.unlock mutex;
        true (* Added successfully *)

  (** Remove an order from the in-flight cache *)
  let remove_in_flight_order duplicate_key =
    Mutex.lock mutex;
    let existed = Hashtbl.mem registry duplicate_key in
    if existed then Hashtbl.remove registry duplicate_key;
    Mutex.unlock mutex;
    existed

  (** Get the current size of the in-flight orders registry *)
  let get_registry_size () =
    Mutex.lock mutex;
    let size = Hashtbl.length registry in
    Mutex.unlock mutex;
    size

  (** Cleanup stale entries *)
  let cleanup ?(max_age=60.0) () =
    Mutex.lock mutex;
    let now = Unix.gettimeofday () in
    let removed = ref 0 in

    (* Scan and remove stale entries directly - safe under mutex protection *)
    Hashtbl.filter_map_inplace (fun _key timestamp ->
      if now -. timestamp > max_age then begin
        incr removed;
        None  (* Remove this entry *)
      end else
        Some timestamp  (* Keep this entry *)
    ) registry;

    Mutex.unlock mutex;
    (0, !removed) (* drift, trimmed *)

  (** Return cleanup function for event registry integration *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** In-flight amendment cache to prevent duplicate amendments *)
module InFlightAmendments = struct
  (* Map order_id -> timestamp *)
  let registry : (string, float) Hashtbl.t = Hashtbl.create 1024
  let mutex = Mutex.create ()

  (** Check if an amendment is already in-flight and add it if not *)
  let add_in_flight_amendment order_id =
    Mutex.lock mutex;
    match Hashtbl.mem registry order_id with
    | true ->
        Mutex.unlock mutex;
        false (* Already existed *)
    | false ->
        Hashtbl.add registry order_id (Unix.gettimeofday ());
        Mutex.unlock mutex;
        true (* Added successfully *)

  (** Check if an amendment is currently in-flight *)
  let is_in_flight order_id =
    Mutex.lock mutex;
    let exists = Hashtbl.mem registry order_id in
    Mutex.unlock mutex;
    exists

  (** Remove an amendment from the in-flight cache *)
  let remove_in_flight_amendment order_id =
    Mutex.lock mutex;
    let existed = Hashtbl.mem registry order_id in
    if existed then Hashtbl.remove registry order_id;
    Mutex.unlock mutex;
    existed

  (** Get the current size of the in-flight amendments registry *)
  let get_registry_size () =
    Mutex.lock mutex;
    let size = Hashtbl.length registry in
    Mutex.unlock mutex;
    size

  (** Cleanup stale entries *)
  let cleanup ?(max_age=60.0) () =
    Mutex.lock mutex;
    let now = Unix.gettimeofday () in
    let removed = ref 0 in

    (* Scan and remove stale entries directly - safe under mutex protection *)
    Hashtbl.filter_map_inplace (fun _key timestamp ->
      if now -. timestamp > max_age then begin
        incr removed;
        None  (* Remove this entry *)
      end else
        Some timestamp  (* Keep this entry *)
    ) registry;

    Mutex.unlock mutex;
    (0, !removed) (* drift, trimmed *)

  (** Return cleanup function for event registry integration *)
  let get_cleanup_fn () =
    fun () ->
      let (drift, trimmed) = cleanup () in
      Some (Some drift, Some trimmed)
end

(** Lock-free ring buffer for strategy orders *)
module OrderRingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t array;
    write_pos: int Atomic.t;
    read_pos: int Atomic.t;
    size: int;
  }

  let create size =
    {
      data = Array.init size (fun _ -> Atomic.make None);
      write_pos = Atomic.make 0;
      read_pos = Atomic.make 0;
      size;
    }

  (** Lock-free write - producer side *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    let read_pos = Atomic.get buffer.read_pos in
    let next_pos = (pos + 1) mod buffer.size in

    (* Check if buffer is full *)
    if next_pos = read_pos then
      None  (* Buffer full, drop the order *)
    else begin
      Atomic.set buffer.data.(pos) (Some value);
      Atomic.set buffer.write_pos next_pos;
      Some ()
    end

  (** Lock-free read - consumer side *)
  let read buffer =
    let read_pos = Atomic.get buffer.read_pos in
    let write_pos = Atomic.get buffer.write_pos in

    if read_pos = write_pos then
      None  (* Buffer empty *)
    else
      match Atomic.get buffer.data.(read_pos) with
      | Some value ->
          Atomic.set buffer.data.(read_pos) None;
          let next_read_pos = (read_pos + 1) mod buffer.size in
          Atomic.set buffer.read_pos next_read_pos;
          Some value
      | None -> None  (* Shouldn't happen in normal operation *)

  (** Get current buffer usage *)
  let size buffer =
    let read_pos = Atomic.get buffer.read_pos in
    let write_pos = Atomic.get buffer.write_pos in
    if write_pos >= read_pos then
      write_pos - read_pos
    else
      buffer.size - read_pos + write_pos

  (** Batch read multiple items *)
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

