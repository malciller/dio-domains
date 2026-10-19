(** Lock-free doorbell from venue feeds to the capital oracle.

    Producers run on venue parse/WS threads; they must not execute oracle logic, take a
    lock, allocate, or copy a payload. Each producer therefore does exactly two things:
    bump a monotonic generation and ring one coalesced doorbell. The oracle owns the
    payload: it drains the venue's existing lock-free execution ring and reads the live
    balance / open-order stores. This keeps the market-data path untouched and makes the
    control path event-driven without polling.

    There is no timer here. A generation is only a "state moved" proof; the oracle
    consumes the actual state from the stores when it wakes. *)

let exec_generation = Atomic.make 0
let balance_generation = Atomic.make 0
let orders_generation = Atomic.make 0

(** The oracle installs its wake function once at startup. The indirection keeps this
    library below the engine in the dependency graph. *)
let wake_fn : (unit -> unit) Atomic.t = Atomic.make (fun () -> ())

let set_wake f = Atomic.set wake_fn f
let clear_wake () = Atomic.set wake_fn (fun () -> ())

(** Ring the coalesced doorbell. The oracle's [request_pass] increments its own counter
    and writes at most one byte to the self-pipe, so a burst of events collapses into one
    wake-up. *)
let[@inline] ring () = (Atomic.get wake_fn) ()

(** New execution event(s) are available in the venue's per-symbol ring. The oracle drains
    them with its own cursor; no payload crosses this boundary. *)
let notify_exec () =
  Atomic.incr exec_generation;
  ring ()
;;

(** The venue's tradeable balance figure moved. The oracle re-reads the live store. *)
let notify_balance () =
  Atomic.incr balance_generation;
  ring ()
;;

(** The venue's open-order set changed (a resting buy appeared, filled, or was cancelled).
    This is the claim edge: allocation sizes against resting buys, so the change is a
    capital decision input even without a fill/terminal event. *)
let notify_orders () =
  Atomic.incr orders_generation;
  ring ()
;;

let exec_gen () = Atomic.get exec_generation
let balance_gen () = Atomic.get balance_generation
let orders_gen () = Atomic.get orders_generation
