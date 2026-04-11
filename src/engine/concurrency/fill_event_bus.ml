(** Centralized fill event bus for cross-venue order fill notifications.

    All exchange execution feeds publish fill events to a single shared
    ring buffer when an order reaches Filled status. Downstream consumers
    (e.g., Discord notifier) read from this buffer using position-based
    iteration, identical to per-exchange execution ring buffers.

    Concurrency model:
    - Writers: exchange execution feed threads (one per venue).
      Access serialized by [write_mutex] since RingBuffer is single-writer.
    - Readers: Lwt fibers in the main domain, woken by [fill_condition].

    The ring buffer is bounded (capacity 256); the oldest fill is silently
    evicted when full. This is acceptable because the Discord notifier
    drains on each wakeup and rarely falls 256 fills behind. *)

module RingBuffer = Ring_buffer.RingBuffer

(** Fill event record published on each complete order fill. *)
type fill_event = {
  venue: string;       (** Exchange name (e.g., "kraken", "lighter", "hyperliquid"). *)
  symbol: string;      (** Trading pair (e.g., "BTC/USD"). *)
  side: string;        (** "buy" or "sell". *)
  amount: float;       (** Filled quantity. *)
  fill_price: float;   (** Average fill price. *)
  value: float;        (** Gross value: amount * fill_price. *)
  fee: float;          (** Estimated fee: value * maker_fee. *)
  timestamp: float;    (** Unix timestamp of the fill. *)
}

(** Global fill event ring buffer. Capacity 256 provides ample headroom
    for bursty grid strategies while bounding memory usage. *)
let buffer : fill_event RingBuffer.t = RingBuffer.create 256

(** Mutex serializing writes from multiple exchange feed threads. *)
let write_mutex = Mutex.create ()

(** Lwt condition signaled after each write to wake the consumer fiber. *)
let fill_condition : unit Lwt_condition.t = Lwt_condition.create ()

(** Publish a fill event to the centralized buffer.
    Thread-safe: acquires [write_mutex] for the ring buffer write,
    then broadcasts [fill_condition] to wake Lwt consumers. *)
let publish_fill (event : fill_event) =
  Mutex.lock write_mutex;
  RingBuffer.write buffer event;
  Mutex.unlock write_mutex;
  (* Broadcast is safe to call from any thread — Lwt_condition internally
     handles cross-domain signaling via the Lwt scheduler. *)
  (try Lwt_condition.broadcast fill_condition () with _ -> ())

(** Return the current write position. Consumers use this as their
    starting cursor for [iter_since]. *)
let get_position () =
  RingBuffer.get_position buffer

(** Iterate over fill events from [last_pos] to the current write position
    without allocating an intermediate list. Returns the new read position. *)
let iter_since last_pos f =
  RingBuffer.iter_since buffer last_pos f

(** Block until [fill_condition] is broadcast. Returns immediately if
    a fill was published between the caller's last read and this call. *)
let wait_for_fill () =
  Lwt_condition.wait fill_condition
