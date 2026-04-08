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

type symbol_sync = {
  mutex: Mutex.t;
  condition: Condition.t;
}

(** Mutex protecting ONLY the creation of new symbol_sync records.
    Not held during wait/signal. *)
let registry_mutex = Mutex.create ()

(** Hash table mapping symbol strings to their dedicated condition variables and mutexes.
    Entries are created lazily on first access per symbol. *)
let syncs : (string, symbol_sync) Hashtbl.t = Hashtbl.create 16

(** Returns the sync record for [symbol], creating one if it does not
    exist. Thread-safe lazily initialized. *)
let[@inline] get_sync symbol =
  match Hashtbl.find_opt syncs symbol with
  | Some s -> s
  | None ->
      Mutex.lock registry_mutex;
      let s = match Hashtbl.find_opt syncs symbol with
        | Some s -> s
        | None ->
            let s = { mutex = Mutex.create (); condition = Condition.create () } in
            Hashtbl.add syncs symbol s;
            s
      in
      Mutex.unlock registry_mutex;
      s

(** Signals the condition variable for [symbol], waking the domain worker
    blocked on that symbol. Acquires and releases ONLY the per-symbol mutex. *)
let[@warning "-32"] signal ~symbol =
  let sync = get_sync symbol in
  Mutex.lock sync.mutex;
  Condition.signal sync.condition;
  Mutex.unlock sync.mutex

(** Signals all per-symbol condition variables. Used for events that require 
    waking every waiting worker, such as shutdown or snapshot completion. 
    Acquires per-symbol mutexes sequentially, guaranteeing no cross-symbol contention. *)
let signal_all () =
  (* Snapshot syncs without locking registry_mutex is fine since Hashtbl is
     append-only for our usage but let's be safe. *)
  Mutex.lock registry_mutex;
  let all_syncs = Hashtbl.fold (fun _ sync acc -> sync :: acc) syncs [] in
  Mutex.unlock registry_mutex;
  List.iter (fun sync ->
    Mutex.lock sync.mutex;
    Condition.signal sync.condition;
    Mutex.unlock sync.mutex
  ) all_syncs

(** Blocks the calling domain worker until the condition variable for [symbol]
    is signaled. Acquires the per-symbol mutex, waits on the
    per-symbol condition, then releases the per-symbol mutex on return. *)
let wait ~symbol =
  let sync = get_sync symbol in
  Mutex.lock sync.mutex;
  Condition.wait sync.condition sync.mutex;
  Mutex.unlock sync.mutex
