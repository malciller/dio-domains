(** Exchange Wakeup

    Per-symbol mutex and condition variable mechanism that allows domain workers
    to block until exchange data arrives for their assigned symbol, avoiding
    unnecessary wakeups across unrelated symbols.

    Exchange-specific modules call [signal ~symbol] to wake the domain worker
    responsible for that symbol. Domain workers call [wait ~symbol] to block
    until data for their symbol is available.

    [signal_all] broadcasts to all waiting workers for cross-cutting events
    such as shutdown or snapshot completion.

    Uses per-symbol mutex+condition pairs to eliminate cross-symbol contention
    on the hot path. The global mutex is only held briefly during lazy
    initialization of new per-symbol entries.
*)

(** Per-symbol synchronization pair: dedicated mutex and condition variable. *)
type symbol_sync = {
  mutex: Mutex.t;
  condition: Condition.t;
}

(** Hash table mapping symbol strings to their dedicated sync pairs.
    Protected by [init_mutex] during lazy creation only. *)
let symbol_syncs : (string, symbol_sync) Hashtbl.t = Hashtbl.create 16

(** Mutex protecting [symbol_syncs] table initialization.
    Not held during signal/wait operations. *)
let init_mutex = Mutex.create ()

(** Returns the sync pair for [symbol], creating one if it does not exist.
    Double-checked lock: fast path is lock-free after initial creation. *)
let[@inline] get_sync symbol =
  match Hashtbl.find_opt symbol_syncs symbol with
  | Some s -> s
  | None ->
      Mutex.lock init_mutex;
      let s = match Hashtbl.find_opt symbol_syncs symbol with
        | Some s -> s
        | None ->
            let s = { mutex = Mutex.create (); condition = Condition.create () } in
            Hashtbl.add symbol_syncs symbol s;
            s
      in
      Mutex.unlock init_mutex;
      s

(** Signals the condition variable for [symbol], waking the domain worker
    blocked on that symbol. Only contends with the single domain assigned
    to this symbol. *)
let[@warning "-32"] signal ~symbol =
  let sync = get_sync symbol in
  Mutex.lock sync.mutex;
  Condition.signal sync.condition;
  Mutex.unlock sync.mutex

(** Signals all per-symbol condition variables. Used for events that require
    waking every waiting worker, such as shutdown or snapshot completion.
    Briefly acquires init_mutex to snapshot the sync table, then signals
    each pair independently. *)
let signal_all () =
  Mutex.lock init_mutex;
  let syncs = Hashtbl.fold (fun _ sync acc -> sync :: acc) symbol_syncs [] in
  Mutex.unlock init_mutex;
  List.iter (fun sync ->
    Mutex.lock sync.mutex;
    Condition.signal sync.condition;
    Mutex.unlock sync.mutex
  ) syncs

(** Blocks the calling domain worker until the condition variable for [symbol]
    is signaled. Only contends with the signal path for this specific symbol,
    eliminating cross-symbol wakeup interference. *)
let wait ~symbol =
  let sync = get_sync symbol in
  Mutex.lock sync.mutex;
  Condition.wait sync.condition sync.mutex;
  Mutex.unlock sync.mutex
