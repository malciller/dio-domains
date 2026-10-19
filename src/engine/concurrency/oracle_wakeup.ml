(** Lock-free doorbell from venue feeds to the capital oracle.

    Producers run on venue parse/WS threads; they must not execute oracle logic, take a
    lock, allocate, or copy a payload. Each producer only rings one coalesced doorbell.
    The oracle owns the payload: it drains the venue's existing lock-free execution ring
    and reads the live balance / open-order stores when it wakes. *)

(** Installed once by the oracle at startup. The indirection keeps this library below the
    engine in the dependency graph. *)
let wake_fn : (unit -> unit) Atomic.t = Atomic.make (fun () -> ())

let set_wake f = Atomic.set wake_fn f
let clear_wake () = Atomic.set wake_fn (fun () -> ())

(** Ring the coalesced doorbell. [request_pass] writes at most one byte to the self-pipe,
    so a burst of events collapses into one wake-up. *)
let[@inline] ring () = (Atomic.get wake_fn) ()

(** New execution event(s) are in the venue's per-symbol ring. *)
let notify_exec () = ring ()

(** The venue's tradeable balance figure moved. *)
let notify_balance () = ring ()

(** The venue's open-order set changed: a resting buy appeared, filled, or was cancelled.
    Allocation sizes against resting buys, so this is a capital input even without a fill
    or terminal event. *)
let notify_orders () = ring ()
