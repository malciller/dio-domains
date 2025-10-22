(**
  Common types shared between strategies (suicide_grid, market_maker, etc.)
  
  This module provides unified types for strategy orders to enable consistent
  order processing in the supervisor regardless of which strategy generates them.
*)

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
  userref: int option;  (* For tracking strategy orders *)
  strategy: string;     (* Strategy name for order processing *)
}

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

