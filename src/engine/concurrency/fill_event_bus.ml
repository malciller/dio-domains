(** Centralized fill event bus for cross-venue order fill notifications.

    All exchange execution feeds publish fill events to a single shared
    ring buffer when an order reaches Filled status. Downstream consumers
    (e.g., Discord notifier) read from this buffer using position-based
    iteration, identical to per-exchange execution ring buffers.

    Concurrency model:
    - Writers: exchange execution feed handlers - including code running
      on the Parse_worker domain (P5) - serialized by [write_mutex] since
      RingBuffer is single-writer.
    - Readers: Lwt fibers in the main domain, polling [generation].

    Domain safety: publishing uses ONLY Mutex + Atomic primitives, so fills
    may be published from any domain. Signalling is a monotonic generation
    counter rather than an [Lwt_condition]: Lwt structures are single-
    domain, so the previous broadcast was unsafe from non-Lwt contexts.
    The consumer ([wait_for_fill]) polls the counter; its sole caller is
    the Discord notifier, which is latency-insensitive. *)

module RingBuffer = Ring_buffer.RingBuffer

(** Fill event record published on each complete order fill. *)
type fill_event =
  { venue : string (** Exchange name (e.g., "kraken", "lighter", "hyperliquid"). *)
  ; symbol : string (** Trading pair (e.g., "BTC/USD"). *)
  ; side : string (** "buy" or "sell". *)
  ; amount : float (** Filled quantity. *)
  ; fill_price : float (** Average fill price. *)
  ; value : float (** Gross value: amount * fill_price. *)
  ; fee : float (** Estimated fee: value * maker_fee. *)
  ; timestamp : float (** Unix timestamp of the fill. *)
  ; order_id : string (** Exchange order ID for deduplication. *)
  ; trade_id : string (** Exchange trade/execution ID for deduplication. *)
  }

(** Global fill event ring buffer. R3: raised from 256 - a burst of fills
    (volatile market, mass take-profit triggers) could lap the Discord
    consumer within one drain. 1024 slots x small records is ~200KB. *)
let buffer : fill_event RingBuffer.t = RingBuffer.create 1024

(** Mutex serializing writes from multiple domains. *)
let write_mutex = Mutex.create ()

(** Monotonic fill counter, bumped under [write_mutex] after each new
    event. Replaces the former Lwt_condition broadcast. *)
let generation = Atomic.make 0

(** Bounded deduplication set for published fills.
    Prevents WebSocket reconnect replays from re-publishing fills that
    were already sent to Discord. Key: (order_id, trade_id). *)
let dedup_cap = 512

let dedup_set : (string * string, unit) Hashtbl.t = Hashtbl.create dedup_cap
let dedup_queue : (string * string) Queue.t = Queue.create ()

(** Publish a fill event to the centralized buffer.
    Domain-safe: acquires [write_mutex] for the dedup + ring buffer write,
    then bumps [generation] to release polling consumers.
    Silently drops duplicate fills based on (order_id, trade_id). *)
let publish_fill (event : fill_event) =
  let key = event.order_id, event.trade_id in
  Mutex.lock write_mutex;
  if Hashtbl.mem dedup_set key
  then Mutex.unlock write_mutex
  (* Duplicate fill; already published, skip silently *)
  else (
    Hashtbl.replace dedup_set key ();
    Queue.push key dedup_queue;
    (* Evict oldest entries when cap is exceeded *)
    while Hashtbl.length dedup_set > dedup_cap do
      if Queue.is_empty dedup_queue
      then ignore (Hashtbl.length dedup_set)
      else (
        let oldest = Queue.pop dedup_queue in
        Hashtbl.remove dedup_set oldest)
    done;
    RingBuffer.write buffer event;
    Atomic.set generation (Atomic.get generation + 1);
    Mutex.unlock write_mutex)
;;

(** Return the current write position. Consumers use this as their
    starting cursor for [iter_since]. *)
let get_position () = RingBuffer.get_position buffer

(** Iterate over fill events from [last_pos] to the current write position
    without allocating an intermediate list. Returns the new read position. *)
let iter_since last_pos f = RingBuffer.iter_since buffer last_pos f

(** Resolve once a fill newer than the caller's snapshot has been
    published. Polls the generation counter at 50ms: the sole consumer is
    the Discord notifier, so event-driven wakeup buys nothing and the poll
    keeps publishers domain-safe. Returns immediately if a fill was
    published since the caller captured its position. *)
let wait_for_fill ?(poll_interval = 0.05) () =
  let g = Atomic.get generation in
  let rec loop () =
    if Atomic.get generation <> g
    then Lwt.return_unit
    else Lwt.bind (Lwt_unix.sleep poll_interval) loop
  in
  loop ()
;;

(** Read and sort the entire buffer returning the most recent fills first. *)
let get_recent_fills () =
  let fills = RingBuffer.read_all buffer in
  List.sort (fun a b -> compare b.timestamp a.timestamp) fills
;;
