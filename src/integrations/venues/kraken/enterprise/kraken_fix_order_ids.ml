(** ClOrdID allocation and the order-userref side table.

    FIX identifies orders by [ClOrdID] (tag [11]) and has no equivalent of Kraken's
    [order_userref], which the engine uses to attribute an order to the strategy that
    placed it. The userref is therefore recorded locally when the order is sent and
    stamped back onto every event derived from the execution reports.

    ClOrdID format: ever-increasing microseconds since the epoch, as Kraken's Spot FIX
    examples prescribe (max 18 characters, monotonic). An atomic bump keeps them strictly
    increasing across the strategy domains even when two orders are placed in the same
    microsecond. *)

let section = "kraken_fix_order_ids"
let counter : int64 Atomic.t = Atomic.make 0L

(** Next ClOrdID. Strictly increasing, 16 digits. *)
let next () =
  let now = Int64.of_float (Unix.gettimeofday () *. 1_000_000.0) in
  let rec bump () =
    let prev = Atomic.get counter in
    let candidate = if Int64.compare now prev > 0 then now else Int64.add prev 1L in
    if Atomic.compare_and_set counter prev candidate then candidate else bump ()
  in
  Int64.to_string (bump ())
;;

(** Ceiling on tracked ClOrdIDs. Only working orders need the mapping; this many covers a
    deep grid with room to spare, and the oldest entry is evicted past it. *)
let capacity = 8192

let userrefs : (string, int option) Hashtbl.t = Hashtbl.create 256
let insertion_order : string Queue.t = Queue.create ()
let lock = Mutex.create ()

(** Remembers a ClOrdID we used, with the strategy attribution when the caller supplied
    one. Presence in the table is also how the cancel/amend paths tell an order this FIX
    session placed (cancellable over FIX) from one placed by another session, which only
    the WebSocket path can reach. *)
let note ?userref ~cl_ord_id () =
  Mutex.lock lock;
  if not (Hashtbl.mem userrefs cl_ord_id)
  then (
    Queue.push cl_ord_id insertion_order;
    Hashtbl.replace userrefs cl_ord_id userref;
    if Queue.length insertion_order > capacity
    then (
      let evicted = Queue.pop insertion_order in
      Hashtbl.remove userrefs evicted));
  Mutex.unlock lock
;;

(** True when this FIX session placed [cl_ord_id], i.e. when an [OrderCancelRequest] or
    [OrderCancelReplaceRequest] on the session can reach it. *)
let placed_here cl_ord_id =
  Mutex.lock lock;
  let found = Hashtbl.mem userrefs cl_ord_id in
  Mutex.unlock lock;
  found
;;

(** Strategy attribution for [cl_ord_id], when the order was placed here and the caller
    supplied a userref. *)
let userref_of_cl_ord_id cl_ord_id =
  Mutex.lock lock;
  let found = Hashtbl.find_opt userrefs cl_ord_id |> Option.join in
  Mutex.unlock lock;
  found
;;

(** Drops attribution once an order reaches a terminal state. *)
let forget cl_ord_id =
  Mutex.lock lock;
  Hashtbl.remove userrefs cl_ord_id;
  Mutex.unlock lock
;;
