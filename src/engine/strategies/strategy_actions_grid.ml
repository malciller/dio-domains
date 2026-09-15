(** Grid decision action handlers (milestone 3).

    Maps strategy-file actions to the reference grid functions, faithful by construction:
    the handler calls the same function the reference calls. This is the seam into which
    the remaining grid actions are ported one by one, checked off against the
    reference-action mapping table (design §8.2). *)

open Strategy_expr

let float_arg args key =
  match List.assoc_opt key args with
  | Some (V_float f) -> f
  | Some (V_int i) -> float_of_int i
  | Some (V_string s) ->
    (try float_of_string s with
     | _ -> nan)
  | _ -> nan
;;

(** The buy-leg reference price: the last bid, else the ask
    ([Jacobs_ladder_execution.compute_buy_ref_price]). *)
let compute_buy_ref_price args =
  [ ( "price"
    , V_float
        (Jacobs_ladder.compute_buy_ref_price
           ~bid_price:(float_arg args "bid")
           ~ask_price:(float_arg args "ask")) )
  ]
;;

let run (_ : Strategy_runtime.t) name args =
  match name with
  | "compute_buy_ref_price" -> compute_buy_ref_price args
  | _ -> []
;;

let handler : Strategy_runtime.handler = { run }
