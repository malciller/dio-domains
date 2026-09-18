open Dio_strategies

(* Exercises the shipped [dip_scalper.strategy] end to end: it must parse, validate,
   round-trip through the compiled JSON, and produce the intended decisions when driven
   through the runtime. *)

let register () = Strategy_actions_builtin.register_all ()

let load () =
  match Strategy_loader.parse_file ~path:"dip_scalper.strategy" with
  | Error e -> Alcotest.failf "parse dip_scalper.strategy: %s" e
  | Ok f -> f
;;

let names (calls : Strategy_runtime.action_call list) =
  List.map (fun (c : Strategy_runtime.action_call) -> c.ac_action) calls
;;

let seed_bull_facts rt =
  let b k v = Strategy_runtime.set_platform rt k (Strategy_expr.V_bool v) in
  b "price_nan" false;
  b "check_stale_balance" false;
  b "asset_balance_nan" false;
  b "quote_balance_nan" false;
  b "maker_fee_set" true;
  b "fee_refresh_due" false;
  b "oracle_halted" false;
  b "has_pending_buy" false;
  b "has_tracked_buy" false;
  b "buy_capital_low" false;
  b "buy_crossing" false;
  b "buy_quote_nan" false;
  b "buy_balance_ok" true
;;

let rec_handler log =
  { Strategy_runtime.run =
      (fun _t name args ->
        log := (name, args) :: !log;
        [])
  }
;;

let test_shape () =
  register ();
  let f = load () in
  Alcotest.(check string) "name" "dip_scalper" f.name;
  Alcotest.(check int) "version" 1 f.version;
  Alcotest.(check int) "state declarations" 4 (List.length f.state);
  Alcotest.(check int) "tunables" 3 (List.length f.params);
  Alcotest.(check (list string))
    "triggers"
    [ "fill"; "order_lifecycle"; "book_update"; "balance_update" ]
    f.triggers
;;

let test_validates () =
  register ();
  let f = load () in
  let errors = Strategy_compile.errors (Strategy_compile.validate f) in
  List.iter
    (fun (d : Strategy_compile.diagnostic) ->
      Alcotest.failf "unexpected diagnostic at %s: %s" d.where d.msg)
    errors;
  Alcotest.(check int) "no errors" 0 (List.length errors)
;;

let test_compile_roundtrip () =
  register ();
  let f = load () in
  let rendered = Yojson.Basic.to_string (Strategy_file.to_json f) in
  match Strategy_file.parse_string rendered with
  | Error e -> Alcotest.failf "compiled JSON does not reparse: %s" e
  | Ok g ->
    Alcotest.(check string)
      "round-trip stable"
      rendered
      (Yojson.Basic.to_string (Strategy_file.to_json g))
;;

let test_enter_on_book () =
  register ();
  let log = ref [] in
  let rt = Strategy_runtime.create ~handlers:(rec_handler log) (load ()) in
  seed_bull_facts rt;
  let calls =
    Strategy_runtime.run_cycle
      rt
      ~price:100.0
      ~now:0.0
      ~event:(Strategy_runtime.make_event "book_update" [])
  in
  Alcotest.(check (list string))
    "book_update decision sequence"
    [ "init_venue_state"
    ; "resolve_book"
    ; "early_facts"
    ; "set_gate"
    ; "expire_amend_cooldowns"
    ; "evict_ghost_orders"
    ; "scan_open_orders"
    ; "cycle_facts"
    ; "buy_gate"
    ; "set_gate"
    ; "place_buy"
    ; "track_buy"
    ]
    (names calls);
  let place_buy =
    List.find
      (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "place_buy")
      calls
  in
  match List.assoc_opt "price" place_buy.ac_args with
  | Some (Strategy_expr.V_float p) ->
    Alcotest.(check (float 0.0001)) "0.4% below reference" 99.6 p
  | _ -> Alcotest.fail "place_buy price not evaluated"
;;

let test_sell_on_buy_fill () =
  register ();
  let log = ref [] in
  let rt = Strategy_runtime.create ~handlers:(rec_handler log) (load ()) in
  seed_bull_facts rt;
  let fill =
    Strategy_runtime.make_event
      "filled"
      [ "side", Strategy_expr.V_string "buy"
      ; "price", Strategy_expr.V_float 100.0
      ; "qty", Strategy_expr.V_float 2.0
      ]
  in
  let calls = Strategy_runtime.run_cycle rt ~price:100.0 ~now:1.0 ~event:fill in
  Alcotest.(check (list string))
    "buy fill rests the exit sell"
    [ "place_sell"; "accumulate"; "set_gate" ]
    (names calls);
  let place_sell =
    List.find
      (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "place_sell")
      calls
  in
  (match List.assoc_opt "price" place_sell.ac_args with
   | Some (Strategy_expr.V_float p) ->
     Alcotest.(check (float 0.0001)) "0.6% above fill" 100.6 p
   | _ -> Alcotest.fail "place_sell price not evaluated");
  match Strategy_runtime.get_state rt "last_fill" with
  | Strategy_expr.V_float f ->
    Alcotest.(check (float 0.0001)) "last fill recorded" 100.0 f
  | _ -> Alcotest.fail "last_fill not recorded"
;;

let test_sell_fill_notifies () =
  register ();
  let log = ref [] in
  let rt = Strategy_runtime.create ~handlers:(rec_handler log) (load ()) in
  seed_bull_facts rt;
  let fill =
    Strategy_runtime.make_event
      "filled"
      [ "side", Strategy_expr.V_string "sell"
      ; "price", Strategy_expr.V_float 101.0
      ; "qty", Strategy_expr.V_float 2.0
      ]
  in
  let calls = Strategy_runtime.run_cycle rt ~price:101.0 ~now:2.0 ~event:fill in
  Alcotest.(check (list string))
    "sell fill closes the round trip"
    [ "notify_oracle" ]
    (names calls)
;;

let test_balance_update () =
  register ();
  let log = ref [] in
  let rt = Strategy_runtime.create ~handlers:(rec_handler log) (load ()) in
  let ev =
    Strategy_runtime.make_event
      "balance_update"
      [ "asset", Strategy_expr.V_string "SOL"; "available", Strategy_expr.V_float 12.5 ]
  in
  ignore (Strategy_runtime.run_cycle rt ~price:100.0 ~now:3.0 ~event:ev);
  match Strategy_runtime.get_state rt "last_balance" with
  | Strategy_expr.V_float b -> Alcotest.(check (float 0.0001)) "balance noted" 12.5 b
  | _ -> Alcotest.fail "last_balance not recorded"
;;

let () =
  Alcotest.run
    "dip_scalper"
    [ ( "file"
      , [ Alcotest.test_case "shape" `Quick test_shape
        ; Alcotest.test_case "validates clean" `Quick test_validates
        ; Alcotest.test_case "compiled JSON round-trips" `Quick test_compile_roundtrip
        ] )
    ; ( "runtime"
      , [ Alcotest.test_case "book_update arms an entry" `Quick test_enter_on_book
        ; Alcotest.test_case "buy fill rests the exit" `Quick test_sell_on_buy_fill
        ; Alcotest.test_case
            "sell fill closes the round trip"
            `Quick
            test_sell_fill_notifies
        ; Alcotest.test_case "balance update noted" `Quick test_balance_update
        ] )
    ]
;;
