(** Exchange Wakeup
    
    A shared Mutex + Condition variable that allows domain workers to block
    efficiently waiting for new exchange data, rather than busy-spinning.

    Exchange-specific modules (e.g. Hyperliquid_ws) call [signal] on every
    incoming frame.  Domain workers in Domain_spawner call [wait] to block
    until real data arrives.
*)

let mutex = Mutex.create ()
let condition = Condition.create ()

(** Signal all waiting domain workers that new exchange data has arrived. *)
let signal () =
  Mutex.lock mutex;
  Condition.broadcast condition;
  Mutex.unlock mutex

(** Block the calling thread until [signal] is called.
    Purely event-driven: returns only when a real exchange event fires. *)
let wait () =
  Mutex.lock mutex;
  Condition.wait condition mutex;
  Mutex.unlock mutex
