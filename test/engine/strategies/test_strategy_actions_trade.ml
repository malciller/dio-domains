open Dio_strategies
module V = Strategy_expr

module Stub = struct
  type ctx = string list ref

  let log c s = c := s :: !c

  let read_book c =
    log c "read_book";
    [ "bid", V.V_float 1.0; "ask", V.V_float 2.0 ]
  ;;

  let read_capacity c ~asset =
    log c ("cap:" ^ asset);
    [ "buying_power", V.V_float 10.0 ]
  ;;

  let read_open_orders c =
    log c "open";
    []
  ;;

  let place c ~side ~qty ~price ~post_only ~dedup_key =
    log c (Printf.sprintf "place:%s:%.4g:%.4g:%b:%s" side qty price post_only dedup_key);
    [ "token", V.V_string "t1" ]
  ;;

  let amend c ~token ~price ~qty ~dedup_key =
    log c (Printf.sprintf "amend:%s:%.4g:%.4g:%s" token price qty dedup_key);
    []
  ;;

  let cancel c ~token ~dedup_key =
    log c ("cancel:" ^ token ^ ":" ^ dedup_key);
    []
  ;;

  let cancel_all c ~side ~dedup_key =
    log c ("cancel_all:" ^ side ^ ":" ^ dedup_key);
    []
  ;;

  let track_buy c ~token ~price = log c (Printf.sprintf "tb:%s:%.4g" token price)
  let track_sell c ~token ~price = log c (Printf.sprintf "ts:%s:%.4g" token price)
  let set_cooldown c ~name ~seconds = log c (Printf.sprintf "cd:%s:%.4g" name seconds)
  let update_reserved_base c ~qty = log c (Printf.sprintf "resv:%.4g" qty)
  let accumulate c ~qty ~profit = log c (Printf.sprintf "acc:%.4g:%.4g" qty profit)
  let set_time c s = log c ("time:" ^ s)
  let notify_oracle c m = log c ("oracle:" ^ m)
  let gate_balance _ = true
  let gate_capital_halted _ = false
  let is_ghost _ ~token = String.equal token "g"
  let reconcile_position c = log c "recon_pos"
  let reconcile_persisted_sell_levels c = log c "recon_lvl"
  let compute_sell_price _ ~base ~mult = base *. mult

  let compute_amend_price _ ~ref ~lo:_ ~hi =
    match hi with
    | Some h -> min ref h
    | None -> ref
  ;;

  let compute_grid_price _ ~ref ~lo:_ ~hi:_ ~side:_ ~snap:_ = ref
  let compute_buy_ref_price _ ~bid ~ask:_ = bid
  let owed_sell_price _ ~bid:_ ~ask ~capital_exhausted:_ = ask
end

module H = Strategy_actions_trade.Make (Stub)

let mk_rt () =
  match
    Strategy_file.parse_string
      {|{"name":"t","version":1,"triggers":["book_update"],"steps":[]}|}
  with
  | Ok f -> Strategy_runtime.create f
  | Error e -> failwith e
;;

let run ctx name args = (H.handler ctx).Strategy_runtime.run (mk_rt ()) name args

let test_place_buy () =
  let ctx = ref [] in
  let out =
    run
      ctx
      "place_buy"
      [ "qty", V.V_float 0.5; "price", V.V_float 100.0; "dedup_key", V.V_string "k" ]
  in
  Alcotest.(check (option string))
    "returns token"
    (Some "t1")
    (match List.assoc_opt "token" out with
     | Some (V.V_string s) -> Some s
     | _ -> None);
  Alcotest.(check (list string))
    "logged place"
    [ "place:buy:0.5:100:true:k" ]
    (List.rev !ctx)
;;

let test_gates () =
  let ctx = ref [] in
  Alcotest.(check (option bool))
    "balance gate"
    (Some true)
    (match List.assoc_opt "ok" (run ctx "gate_balance" []) with
     | Some (V.V_bool b) -> Some b
     | _ -> None);
  Alcotest.(check (option bool))
    "capital gate"
    (Some false)
    (match List.assoc_opt "halted" (run ctx "gate_capital_halted" []) with
     | Some (V.V_bool b) -> Some b
     | _ -> None);
  Alcotest.(check (option bool))
    "is_ghost"
    (Some true)
    (match List.assoc_opt "ghost" (run ctx "is_ghost" [ "token", V.V_string "g" ]) with
     | Some (V.V_bool b) -> Some b
     | _ -> None)
;;

let test_unknown () =
  Alcotest.(check int) "unknown action no-op" 0 (List.length (run (ref []) "nope" []))
;;

let test_pure_math () =
  let ctx = ref [] in
  Alcotest.(check (option (float 1e-9)))
    "compute_sell_price"
    (Some 110.0)
    (match
       List.assoc_opt
         "price"
         (run ctx "compute_sell_price" [ "base", V.V_float 100.0; "mult", V.V_float 1.1 ])
     with
     | Some (V.V_float f) -> Some f
     | _ -> None);
  Alcotest.(check (option (float 1e-9)))
    "owed_sell_price"
    (Some 2.0)
    (match
       List.assoc_opt
         "price"
         (run ctx "owed_sell_price" [ "bid", V.V_float 1.0; "ask", V.V_float 2.0 ])
     with
     | Some (V.V_float f) -> Some f
     | _ -> None)
;;

let () =
  Alcotest.run
    "strategy_actions_trade"
    [ ( "dispatch"
      , [ Alcotest.test_case "place_buy" `Quick test_place_buy
        ; Alcotest.test_case "gates" `Quick test_gates
        ; Alcotest.test_case "unknown" `Quick test_unknown
        ; Alcotest.test_case "pure math" `Quick test_pure_math
        ] )
    ]
;;
