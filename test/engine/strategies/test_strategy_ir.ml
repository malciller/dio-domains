let kind_of scope ~key =
  match scope, key with
  | Dio_strategies.Strategy_expr.Platform, "ask" -> Dio_strategies.Strategy_ir.K_float
  | Dio_strategies.Strategy_expr.Platform, "flag" -> Dio_strategies.Strategy_ir.K_bool
  | _ -> Dio_strategies.Strategy_ir.K_float
;;

let parse s =
  match Dio_strategies.Strategy_expr.parse s with
  | Ok e -> e
  | Error m -> Alcotest.failf "parse: %s" m
;;

let make_env layout ~ask ~flag ~price =
  let env =
    { Dio_strategies.Strategy_ir.floats = Array.make (max 1 layout.Dio_strategies.Strategy_ir.float_count) 0.0
    ; ints = Array.make (max 1 layout.Dio_strategies.Strategy_ir.int_count) 0
    ; bools = Array.make (max 1 layout.Dio_strategies.Strategy_ir.bool_count) false
    ; strings = Array.make (max 1 layout.Dio_strategies.Strategy_ir.string_count) ""
    ; price
    ; now = 0.0
    ; event_kind = ""
    ; event_side = ""
    }
  in
  (match Hashtbl.find_opt layout.Dio_strategies.Strategy_ir.kinds
           (Dio_strategies.Strategy_expr.Platform, "ask") with
   | Some (Dio_strategies.Strategy_ir.K_float, i) -> env.Dio_strategies.Strategy_ir.floats.(i) <- ask
   | _ -> ());
  (match Hashtbl.find_opt layout.Dio_strategies.Strategy_ir.kinds
           (Dio_strategies.Strategy_expr.Platform, "flag") with
   | Some (Dio_strategies.Strategy_ir.K_bool, i) -> env.Dio_strategies.Strategy_ir.bools.(i) <- flag
   | _ -> ());
  env
;;

let test_bool () =
  let layout = Dio_strategies.Strategy_ir.create_layout () in
  let e = parse "$platform.ask > 100.0 and $platform.flag" in
  let f = Dio_strategies.Strategy_ir.compile_bool layout ~kind_of e in
  let env = make_env layout ~ask:101.0 ~flag:true ~price:0.0 in
  Alcotest.(check bool) "true" true (f env);
  let env = make_env layout ~ask:99.0 ~flag:true ~price:0.0 in
  Alcotest.(check bool) "false on ask" false (f env)
;;

let test_float () =
  let layout = Dio_strategies.Strategy_ir.create_layout () in
  let e = parse "$price * 2.0 + $platform.ask" in
  let f = Dio_strategies.Strategy_ir.compile_float layout ~kind_of e in
  let env = make_env layout ~ask:10.0 ~flag:false ~price:5.0 in
  Alcotest.(check (float 0.0001)) "20" 20.0 (f env)
;;

let test_zero_alloc () =
  let layout = Dio_strategies.Strategy_ir.create_layout () in
  let e = parse "$platform.ask > 100.0 and $platform.flag" in
  let f = Dio_strategies.Strategy_ir.compile_bool layout ~kind_of e in
  let env = make_env layout ~ask:101.0 ~flag:true ~price:0.0 in
  ignore (f env);
  let before = Monotonic_clock.minor_words () in
  for _ = 1 to 100_000 do
    ignore (f env)
  done;
  let after = Monotonic_clock.minor_words () in
  Alcotest.(check int) "compiled guard allocates zero words" 0 (after - before)
;;


let test_guard () =
  let layout = Dio_strategies.Strategy_ir.create_layout () in
  let g =
    Dio_strategies.Strategy_file.G_all
      [ Dio_strategies.Strategy_file.G_event "book_update"
      ; Dio_strategies.Strategy_file.G_expr "$platform.flag"
      ]
  in
  let f = Dio_strategies.Strategy_ir.compile_guard layout ~kind_of g in
  let env = make_env layout ~ask:0.0 ~flag:true ~price:0.0 in
  env.Dio_strategies.Strategy_ir.event_kind <- "book_update";
  Alcotest.(check bool) "event and flag" true (f env);
  env.Dio_strategies.Strategy_ir.event_kind <- "filled";
  Alcotest.(check bool) "wrong event" false (f env)
;;

let () =
  Alcotest.run
    "strategy_ir"
    [ ( "expr"
      , [ Alcotest.test_case "bool guard" `Quick test_bool
        ; Alcotest.test_case "float expr" `Quick test_float
        ; Alcotest.test_case "zero allocation" `Quick test_zero_alloc
        ; Alcotest.test_case "typed guard" `Quick test_guard
        ] )
    ]
;;
