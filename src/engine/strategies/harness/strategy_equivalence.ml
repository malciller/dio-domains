(** Differential comparison of runs' observable traces.

    A run is a function that records observations into a {!Strategy_event_recorder.t};
    [capture] materializes its trace, and [compare_traces] reports the first divergence
    against a reference trace. *)

type result =
  | Equiv
  | Divergence of string

let compare_traces ~(reference : Strategy_trace.t) ~(candidate : Strategy_trace.t)
  : result
  =
  match Strategy_trace.compare reference candidate with
  | None -> Equiv
  | Some msg -> Divergence msg
;;

let format = function
  | Equiv -> "equivalent"
  | Divergence m -> "divergence: " ^ m
;;

let is_equiv = function
  | Equiv -> true
  | Divergence _ -> false
;;

(** Capture the trace produced by running [f]. *)
let capture (f : Strategy_event_recorder.t -> unit) : Strategy_trace.t =
  let r = Strategy_event_recorder.create () in
  f r;
  Strategy_event_recorder.finish r
;;

(** Run [f] and diff its trace against a reference trace. *)
let check ~reference (f : Strategy_event_recorder.t -> unit) : result =
  compare_traces ~reference ~candidate:(capture f)
;;

(** A per-cycle replay input: the market/account facts fed to the strategy that cycle. *)
type input =
  { ri_price : float
  ; ri_now : float
  ; ri_kind : string
  ; ri_fields : (string * Strategy_expr.value) list
  }

(** Replay [inputs] deterministically through a strategy under test.

    [setup] builds a fresh per-run state (e.g. a compiled runtime); [step] advances it one
    cycle per input and records the cycle's observations (calling
    [Strategy_event_recorder.end_cycle]). Returns the resulting trace, which
    [compare_traces] diffs against a reference. This is the mechanism that makes cross-run
    comparison valid: both runs see the identical input stream from an identical starting
    state. *)
let replay
  : type s.
    inputs:input list
    -> setup:(unit -> s)
    -> step:(s -> input -> Strategy_event_recorder.t -> unit)
    -> Strategy_trace.t
  =
  fun ~inputs ~setup ~step ->
  let r = Strategy_event_recorder.create () in
  let st = setup () in
  List.iter (fun i -> step st i r) inputs;
  Strategy_event_recorder.finish r
;;

(** Run [candidate_setup]/[candidate_step] over [inputs] and diff against a reference
    trace produced from the same [inputs]. *)
let check_replay
  ~(reference : Strategy_trace.t)
  ~(inputs : input list)
  ~(setup : unit -> 's)
  ~(step : 's -> input -> Strategy_event_recorder.t -> unit)
  : result
  =
  compare_traces ~reference ~candidate:(replay ~inputs ~setup ~step)
;;
