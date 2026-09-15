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
let record_persistence t key value = record t (Strategy_trace.Persistence (key, value))

(** Per-symbol active recorders for hot-path emission hooks (e.g. the shared order
    buffer). Empty when tracing is off, so the hook is a single hashtable lookup. The
    order buffer is shared across domains, so routing is by the emitted order's symbol. *)
let active_by_symbol : (string, t) Hashtbl.t = Hashtbl.create 8

let active_mutex = Mutex.create ()

let register symbol t =
  Mutex.lock active_mutex;
  Hashtbl.replace active_by_symbol symbol t;
  Mutex.unlock active_mutex
;;

let record_emitted_if_active e =
  match Hashtbl.find_opt active_by_symbol e.Strategy_trace.em_symbol with
  | Some t -> record_emitted t e
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
