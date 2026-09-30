(** Builds a {!Strategy_trace.t} from observations recorded during a run.

    The domain loop (once instrumented) calls [record_*] as it emits order intents,
    finishes a cycle with [end_cycle], and reads the accumulated trace with [finish]. Kept
    dependency-free so it can run on the hot path when enabled. *)

type t =
  { mutable cycles : Strategy_trace.cycle list (* reversed *)
  ; mutable obs : Strategy_trace.obs list (* reversed within the current cycle *)
  ; mutable index : int
  }

let create () = { cycles = []; obs = []; index = 0 }
let record t o = t.obs <- o :: t.obs
let record_order_intent t oi = record t (Strategy_trace.Order_intent oi)
let record_emitted t e = record t (Strategy_trace.Emitted e)
let record_state t entries = record t (Strategy_trace.State entries)
let record_event t (e : Strategy_trace.event_obs) = record t (Strategy_trace.Event e)
let record_persistence t key value = record t (Strategy_trace.Persistence (key, value))

(** Per-symbol active recorders for hot-path emission hooks (e.g. the shared order
    buffer). Empty when tracing is off, so the hook is a single hashtable lookup. The
    order buffer is shared across domains, so routing is by the emitted order's symbol. *)
(* Registration happens from a trading Domain while other Domains are already live, and
   [is_active] / [record_event_if_active] are consulted per emitted order and per lifecycle
   event. Written under a mutex, read without one; a Cow_table closes that. *)
let active_by_symbol : (string, t) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:8 ()
;;

let active_mutex = Mutex.create ()

let register symbol t =
  Mutex.lock active_mutex;
  Ds.Cow_table.set active_by_symbol symbol t;
  Mutex.unlock active_mutex
;;

let record_emitted_if_active e =
  match Ds.Cow_table.find_opt active_by_symbol e.Strategy_trace.em_symbol with
  | Some t -> record_emitted t e
  | None -> ()
;;

(** True when a recorder is registered for [symbol]. Callers on the dispatch path use this
    to avoid building the observation record at all when tracing is off (the common case). *)
let is_active symbol = Ds.Cow_table.mem active_by_symbol symbol

(** Record an order-lifecycle event against the recorder registered for [symbol]. *)
let record_event_if_active symbol (e : Strategy_trace.event_obs) =
  match Ds.Cow_table.find_opt active_by_symbol symbol with
  | Some t -> record_event t e
  | None -> ()
;;

let end_cycle t =
  t.cycles <- { Strategy_trace.c_index = t.index; c_obs = List.rev t.obs } :: t.cycles;
  t.obs <- [];
  t.index <- t.index + 1
;;

(** Ends the in-progress cycle if it has observations, then returns the trace. *)
let finish t =
  if t.obs <> [] then end_cycle t;
  List.rev t.cycles
;;

(** Non-mutating view of the trace so far (the current cycle is included without ending
    it). Safe to call mid-run for periodic persistence. *)
let snapshot t =
  let cycles =
    if t.obs = []
    then t.cycles
    else { Strategy_trace.c_index = t.index; c_obs = List.rev t.obs } :: t.cycles
  in
  List.rev cycles
;;
