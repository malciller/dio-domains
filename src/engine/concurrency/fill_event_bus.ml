(** Centralized fill event bus for cross-venue order fill notifications.

    All exchange execution feeds publish a fill event to one shared ring buffer when an
    order reaches Filled status. Consumers read it by position-based iteration, as with
    per-exchange execution ring buffers.

    Concurrency:
    - Writers: exchange execution feed handlers, including code on the Parse_worker
      domain, serialized by [write_mutex] (RingBuffer is single-writer).
    - Readers: Lwt fibers on the main domain, polling [generation].

    Publishing uses only Mutex and Atomic primitives, so fills may be published from any
    domain. Signalling uses a monotonic generation counter rather than an [Lwt_condition],
    which is single-domain. [wait_for_fill] polls the counter; its sole caller, the
    Discord notifier, is latency-insensitive. *)

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

(** Global fill event ring buffer. 1024 slots, sized so a burst or mass take-profit
    exceeds one drain without blocking; ~200 KB. *)
let buffer : fill_event RingBuffer.t = RingBuffer.create 1024

(** Mutex serializing writes from multiple domains. *)
let write_mutex = Mutex.create ()

(** Monotonic fill counter, incremented under [write_mutex] after each new event. *)
let generation = Atomic.make 0

(** Bounded deduplication set for published fills, keyed by [(order_id, trade_id)];
    prevents WebSocket reconnect replays from re-publishing. *)
let dedup_cap = 512

let dedup_set : (string * string, unit) Hashtbl.t = Hashtbl.create dedup_cap
let dedup_queue : (string * string) Queue.t = Queue.create ()

(** Publish a fill event to the shared buffer. Domain-safe: acquires [write_mutex] for the
    dedup set and ring-buffer write, then increments [generation]. Duplicate
    [(order_id, trade_id)] fills are dropped silently. *)
let publish_fill (event : fill_event) =
  let key = event.order_id, event.trade_id in
  Mutex.lock write_mutex;
  if Hashtbl.mem dedup_set key
  then Mutex.unlock write_mutex (* Duplicate fill; already published, skip silently *)
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

(** Current write position; consumers use it as the starting cursor for [iter_since]. *)
let get_position () = RingBuffer.get_position buffer

(** Iterate fill events from [last_pos] to the current write position without allocating a
    list. Returns the new read position. *)
let iter_since last_pos f = RingBuffer.iter_since buffer last_pos f

(** Resolve once a fill newer than the caller's snapshot is published. Polls the
    generation counter every [poll_interval] seconds (default 0.05). Polling is used
    because the sole consumer, the Discord notifier, is latency-insensitive and polling
    keeps publishers domain-safe. Returns immediately if a fill was published after the
    snapshot.

    The polling tail is spawned via [Lwt.async] rather than chaining the next sleep with
    [Lwt.bind], which would accumulate one Lwt [Forward] node per tick while idle.
    [waiter] is a cancellable [Lwt.task]; a caller cancel is observed at the next tick. *)
let wait_for_fill ?(poll_interval = 0.05) () =
  let g = Atomic.get generation in
  if Atomic.get generation <> g
  then Lwt.return_unit
  else (
    let waiter, wakener = Lwt.task () in
    let rec poll () =
      Lwt.bind (Lwt_unix.sleep poll_interval) (fun () ->
        if Atomic.get generation <> g
        then (
          if Lwt.is_sleeping waiter then Lwt.wakeup_later wakener ();
          Lwt.return_unit)
        else if Lwt.is_sleeping waiter
        then (
          Lwt.async poll;
          Lwt.return_unit)
        else Lwt.return_unit)
    in
    Lwt.async poll;
    waiter)
;;

(** Read the entire buffer, most recent fills first. *)
let get_recent_fills () =
  let fills = RingBuffer.read_all buffer in
  List.sort (fun a b -> compare b.timestamp a.timestamp) fills
;;
