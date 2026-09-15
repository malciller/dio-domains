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
