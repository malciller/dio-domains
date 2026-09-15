open Dio_strategies
module V = Strategy_expr

let stub_ctx submit_log =
  Strategy_engine.create
    ~symbol:"BTC/USD"
    ~exchange:"kraken"
    ~account:(Account_state.create ())
    ~now:(fun () -> 100.0)
    ~tob:(fun () -> Some (99.0, 1.0, 101.0, 1.0))
    ~capacity:(fun ~asset:_ -> [ "buying_power", V.V_float 500.0 ])
    ~iter_open_orders:(fun f -> f "s1" 105.0 1.0 "sell" None)
    ~submit_place:(fun ~side ~qty ~price ~post_only ~dedup_key ->
      submit_log
      := Printf.sprintf "place:%s:%.4g:%.4g:%b:%s" side qty price post_only dedup_key
         :: !submit_log;
      Some "tok1")
    ~submit_amend:(fun ~token ~price ~qty ~dedup_key ->
      submit_log
      := Printf.sprintf "amend:%s:%.4g:%.4g:%s" token price qty dedup_key :: !submit_log)
    ~submit_cancel:(fun ~token ~dedup_key ->
      submit_log := Printf.sprintf "cancel:%s:%s" token dedup_key :: !submit_log)
    ~submit_cancel_all:(fun ~side ~dedup_key ->
      submit_log := Printf.sprintf "cancel_all:%s:%s" side dedup_key :: !submit_log)
    ~gate_balance:(fun () -> true)
    ~gate_capital_halted:(fun () -> false)
    ~is_ghost:(fun token -> String.equal token "g")
    ~reconcile_position:(fun () -> ())
    ~reconcile_persisted:(fun () -> ())
    ~price:
      { Strategy_engine.pm_sell = (fun ~base ~mult -> base *. mult)
      ; pm_amend =
          (fun ~ref ~lo:_ ~hi ->
            match hi with
            | Some h -> min ref h
            | None -> ref)
      ; pm_grid = (fun ~ref ~lo:_ ~hi:_ ~side:_ ~snap:_ -> ref)
      ; pm_buy_ref = (fun ~bid ~ask:_ -> bid)
      ; pm_owed_sell = (fun ~bid:_ ~ask ~capital_exhausted:_ -> ask)
      }
;;

let mk_rt () =
  match
    Strategy_file.parse_string
      {|{"name":"t","version":1,"triggers":["book_update"],"steps":[]}|}
  with
  | Ok f -> Strategy_runtime.create f
  | Error e -> failwith e
;;

let run ctx name args =
  (Strategy_engine.handler ctx).Strategy_runtime.run (mk_rt ()) name args
;;

let test_read_book () =
  let log = ref [] in
  let out = run (stub_ctx log) "read_book" [] in
  Alcotest.(check (option (float 1e-9)))
    "mid"
    (Some 100.0)
    (match List.assoc_opt "mid" out with
     | Some (V.V_float f) -> Some f
     | _ -> None)
;;

let test_place () =
  let log = ref [] in
  let out =
    run
      (stub_ctx log)
      "place_buy"
      [ "qty", V.V_float 0.5; "price", V.V_float 99.0; "dedup_key", V.V_string "k" ]
  in
  Alcotest.(check (option string))
    "token"
    (Some "tok1")
    (match List.assoc_opt "token" out with
     | Some (V.V_string s) -> Some s
     | _ -> None);
  Alcotest.(check (list string)) "submitted" [ "place:buy:0.5:99:true:k" ] (List.rev !log)
;;

let test_open_orders () =
  let log = ref [] in
  let out = run (stub_ctx log) "read_open_orders" [] in
  Alcotest.(check (option int))
    "sells"
    (Some 1)
    (match List.assoc_opt "open_sells" out with
     | Some (V.V_int i) -> Some i
     | _ -> None)
;;

let test_ghost () =
  let log = ref [] in
  Alcotest.(check (option bool))
    "ghost"
    (Some true)
    (match
       List.assoc_opt "ghost" (run (stub_ctx log) "is_ghost" [ "token", V.V_string "g" ])
     with
     | Some (V.V_bool b) -> Some b
     | _ -> None)
;;

let () =
  Alcotest.run
    "strategy_engine"
    [ ( "ctx"
      , [ Alcotest.test_case "read_book" `Quick test_read_book
        ; Alcotest.test_case "place" `Quick test_place
        ; Alcotest.test_case "open orders" `Quick test_open_orders
        ; Alcotest.test_case "ghost" `Quick test_ghost
        ] )
    ]
;;
