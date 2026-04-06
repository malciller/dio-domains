(** Exchange Wakeup
    
    Per-symbol Mutex + Condition variables that allow domain workers to block
    efficiently waiting for new exchange data for THEIR symbol, rather than
    waking on every event across all symbols (thundering herd).

    Exchange-specific modules call [signal ~symbol] to wake only the domain
    worker responsible for that symbol.  Domain workers call [wait ~symbol]
    to block until data for their specific symbol arrives.

    A global [signal_all] is retained for cross-cutting events like shutdown
    or startup snapshot completion.
*)

let mutex = Mutex.create ()

(** Per-symbol condition variables *)
let conditions : (string, Condition.t) Hashtbl.t = Hashtbl.create 16

(** Global condition for broadcast-to-all events (shutdown, etc.) *)
let global_condition = Condition.create ()

(** Get or create a per-symbol condition variable. Must be called under [mutex]. *)
let[@inline] get_condition_locked symbol =
  match Hashtbl.find_opt conditions symbol with
  | Some c -> c
  | None ->
      let c = Condition.create () in
      Hashtbl.add conditions symbol c;
      c

(** Signal the domain worker for a specific symbol. *)
let[@warning "-32"] signal ~symbol =
  Mutex.lock mutex;
  let cond = get_condition_locked symbol in
  Condition.signal cond;
  Mutex.unlock mutex

(** Signal ALL waiting domain workers (e.g. shutdown, snapshot done). *)
let signal_all () =
  Mutex.lock mutex;
  Hashtbl.iter (fun _ cond -> Condition.signal cond) conditions;
  Condition.broadcast global_condition;
  Mutex.unlock mutex

(** Block the calling thread until data for [symbol] arrives,
    or a global broadcast fires. *)
let wait ~symbol =
  Mutex.lock mutex;
  let cond = get_condition_locked symbol in
  Condition.wait cond mutex;
  Mutex.unlock mutex


