(** Exchange Wakeup

    Per-symbol mutex and condition variable mechanism that allows domain workers
    to block until exchange data arrives for their assigned symbol, avoiding
    unnecessary wakeups across unrelated symbols.

    Exchange-specific modules call [signal ~symbol] to wake the domain worker
    responsible for that symbol. Domain workers call [wait ~symbol] to block
    until data for their symbol is available.

    [signal_all] broadcasts to all waiting workers for cross-cutting events
    such as shutdown or snapshot completion.
*)

(** Global mutex protecting [conditions] and synchronizing all condition
    variable operations in this module. *)
let mutex = Mutex.create ()

(** Hash table mapping symbol strings to their dedicated condition variables.
    Entries are created lazily on first access per symbol. *)
let conditions : (string, Condition.t) Hashtbl.t = Hashtbl.create 16

(** Global condition variable used for broadcast events that must wake all
    waiting workers regardless of symbol assignment. *)
let global_condition = Condition.create ()

(** Returns the condition variable for [symbol], creating one if it does not
    exist. Caller must hold [mutex]. *)
let[@inline] get_condition_locked symbol =
  match Hashtbl.find_opt conditions symbol with
  | Some c -> c
  | None ->
      let c = Condition.create () in
      Hashtbl.add conditions symbol c;
      c

(** Signals the condition variable for [symbol], waking the domain worker
    blocked on that symbol. Acquires and releases [mutex] internally. *)
let[@warning "-32"] signal ~symbol =
  Mutex.lock mutex;
  let cond = get_condition_locked symbol in
  Condition.signal cond;
  Mutex.unlock mutex

(** Signals all per-symbol condition variables and broadcasts on the global
    condition variable. Used for events that require waking every waiting
    worker, such as shutdown or snapshot completion. *)
let signal_all () =
  Mutex.lock mutex;
  Hashtbl.iter (fun _ cond -> Condition.signal cond) conditions;
  Condition.broadcast global_condition;
  Mutex.unlock mutex

(** Blocks the calling domain worker until the condition variable for [symbol]
    is signaled or a global broadcast occurs. Acquires [mutex], waits on the
    per-symbol condition, then releases [mutex] on return. *)
let wait ~symbol =
  Mutex.lock mutex;
  let cond = get_condition_locked symbol in
  Condition.wait cond mutex;
  Mutex.unlock mutex
