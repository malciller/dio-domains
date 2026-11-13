module S = Dio_strategies.Strategy_expr
module IR = Dio_strategies.Strategy_ir
module P = Dio_strategies.Strategy_program
module F = Dio_strategies.Strategy_file
module Script = Dio_strategies.Strategy_script

let load_file () =
  match Script.parse_file "short_overlay.strategy" with
  | Error m -> Alcotest.failf "parse: %s" m
  | Ok f -> f
;;

let compile_file f =
  match P.compile f with
  | Error m -> Alcotest.failf "compile: %s" m
  | Ok p -> p
;;

let load () = compile_file (load_file ())
;;

let env_of_program p =
  let l = p.P.layout in
  { IR.floats = Array.make (max 1 l.IR.float_count) 0.0
  ; ints = Array.make (max 1 l.IR.int_count) 0
  ; bools = Array.make (max 1 l.IR.bool_count) false
  ; strings = Array.make (max 1 l.IR.string_count) ""
  ; price = 100.0
  ; now = 10.0
  ; event_kind = ""
  ; event_side = ""
  }
;;

let slot p scope key =
  match Hashtbl.find_opt p.P.layout.IR.kinds (scope, key) with
  | Some x -> x
  | None -> Alcotest.failf "unassigned slot %s" key
;;

let set_float p env key v =
  match slot p S.State key with
  | IR.K_float, i -> env.IR.floats.(i) <- v
  | _ -> Alcotest.failf "state %s is not float" key
;;

let set_bool p env key v =
  match slot p S.State key with
  | IR.K_bool, i -> env.IR.bools.(i) <- v
  | _ -> Alcotest.failf "state %s is not bool" key
;;

let get_float p env key =
  match slot p S.State key with
  | IR.K_float, i -> env.IR.floats.(i)
  | _ -> Alcotest.failf "state %s is not float" key
;;

let set_platform_float p env key v =
  match slot p S.Platform key with
  | IR.K_float, i -> env.IR.floats.(i) <- v
  | _ -> Alcotest.failf "platform %s is not float" key
;;

let set_platform_bool p env key v =
  match slot p S.Platform key with
  | IR.K_bool, i -> env.IR.bools.(i) <- v
  | _ -> Alcotest.failf "platform %s is not bool" key
;;

let set_event_float p env key v =
  match slot p S.Event key with
  | IR.K_float, i -> env.IR.floats.(i) <- v
  | _ -> Alcotest.failf "event %s is not float" key
;;

let set_event_int p env key v =
  match slot p S.Event key with
  | IR.K_int, i -> env.IR.ints.(i) <- v
  | _ -> Alcotest.failf "event %s is not int" key
;;

let seed_params p env =
  let l = p.P.layout in
  Hashtbl.iter
    (fun (scope, _) key ->
      match scope with
      | S.Param ->
        (match Hashtbl.find_opt l.IR.kinds (scope, key) with
         | Some (IR.K_float, i) ->
           let v =
             match key with
             | "short_pct" -> 25.0
             | "stress_rise" -> 80.0
             | "buffer" -> 10.0
             | "short_tp_roe" -> 2.0
             | "recycle_base_share" -> 0.25
             | "max_inventory_pct" -> 100.0
             | _ -> 0.0
           in
           env.IR.floats.(i) <- v
         | _ -> ())
      | _ -> ())
    l.IR.keys
;;

let seed_state p env =
  set_float p env "short_base_target" 0.0;
  set_bool p env "flow_initialized" false;
  set_bool p env "wait_for_buy" false;
  set_bool p env "entry_pending" false;
  set_bool p env "recycle_pending" false;
  set_float p env "short_entry" 0.0;
  set_float p env "short_entry_qty" 0.0;
  set_float p env "pending_gain" 0.0
;;

let test_fill_and_entry () =
  let p = load () in
  let env = env_of_program p in
  let sink = P.create_sink () in
  seed_params p env;
  seed_state p env;
  (* A spot buy of 100 at 100 creates a 25% flow target. *)
  env.IR.event_kind <- "filled";
  env.IR.event_side <- "buy";
  set_event_float p env "is_perp" 0.0;
  set_event_float p env "is_buy" 1.0;
  set_event_float p env "qty" 100.0;
  set_event_float p env "price" 100.0;
  set_event_float p env "fee" 0.0;
  set_event_int p env "seq" 1;
  set_event_float p env "is_replay" 0.0;
  set_event_float p env "timestamp" 1.0;
  P.run p env sink;
  Alcotest.(check (float 0.0001)) "target" 25.0 (get_float p env "short_base_target");
  (* The book cycle sizes and places the entry. *)
  env.IR.event_kind <- "book_update";
  env.IR.price <- 100.0;
  env.IR.now <- 10.0;
  set_platform_bool p env "perp_valid" true;
  set_platform_bool p env "funding_valid" true;
  set_platform_float p env "ask" 100.0;
  set_platform_float p env "bid" 99.0;
  set_platform_float p env "mark" 100.0;
  set_platform_float p env "min_notional" 10.0;
  set_platform_float p env "accumulated_balance" 100.0;
  set_platform_float p env "perp_account_value" 1000.0;
  set_platform_float p env "perp_withdrawable" 1000.0;
  set_platform_float p env "perp_max_leverage" 10.0;
  set_platform_float p env "perp_maintenance_frac" 0.1;
  set_platform_float p env "perp_leverage" 10.0;
  set_platform_float p env "perp_qty" 0.0;
  set_platform_float p env "perp_entry" 0.0;
  set_platform_float p env "perp_margin" 0.0;
  set_platform_float p env "parent_order_qty" 0.0;
  set_platform_bool p env "parent_unfunded" false;
  set_platform_float p env "reserve_share" 0.0;
  set_platform_float p env "now" 10.0;
  P.run p env sink;
  let sells =
    Array.to_list (Array.sub sink.P.orders 0 sink.P.order_count)
    |> List.filter (fun (o : P.order_intent) -> String.equal o.side "sell" && o.qty > 0.0)
  in
  (match sells with
   | [] -> Alcotest.fail "no sell entry emitted"
   | o :: _ ->
     Alcotest.(check (float 0.0001)) "entry at ask" 100.0 o.price;
     Alcotest.(check bool) "entry qty positive" true (o.qty > 0.0))
;;

let seed_book p env =
  env.IR.event_kind <- "book_update";
  env.IR.price <- 100.0;
  env.IR.now <- 10.0;
  set_platform_bool p env "perp_valid" true;
  set_platform_bool p env "funding_valid" true;
  set_platform_float p env "ask" 100.0;
  set_platform_float p env "bid" 99.0;
  set_platform_float p env "mark" 100.0;
  set_platform_float p env "min_notional" 10.0;
  set_platform_float p env "accumulated_balance" 100.0;
  set_platform_float p env "perp_account_value" 1000.0;
  set_platform_float p env "perp_withdrawable" 1000.0;
  set_platform_float p env "perp_max_leverage" 10.0;
  set_platform_float p env "perp_maintenance_frac" 0.1;
  set_platform_float p env "perp_leverage" 10.0;
  set_platform_float p env "perp_qty" 0.0;
  set_platform_float p env "perp_entry" 0.0;
  set_platform_float p env "perp_margin" 0.0;
  set_platform_float p env "parent_order_qty" 0.0;
  set_platform_bool p env "parent_unfunded" false;
  set_platform_float p env "reserve_share" 0.0;
  set_platform_float p env "now" 10.0
;;

let test_zero_alloc () =
  let p = load () in
  let env = env_of_program p in
  let sink = P.create_sink () in
  seed_params p env;
  seed_state p env;
  seed_book p env;
  P.run p env sink;
  let before = Monotonic_clock.minor_words () in
  for _ = 1 to 1000 do
    P.run p env sink
  done;
  let after = Monotonic_clock.minor_words () in
  Alcotest.(check int) "compiled run allocates zero words" 0 (after - before)
;;

(* The cutover fills the env from pre-resolved slots: same values, no per-slot hashing. *)
let test_resolved_fill_matches () =
  let file = load_file () in
  let p = compile_file file in
  let rt = Dio_strategies.Strategy_runtime.create file in
  List.iter
    (fun (k, v) -> Dio_strategies.Strategy_runtime.set_platform rt k (S.V_float v))
    [ "mark", 123.5; "bid", 123.0; "ask", 124.0; "reserve_share", 12.0; "now", 99.0 ];
  Dio_strategies.Strategy_runtime.set_platform rt "perp_valid" (S.V_bool true);
  Dio_strategies.Strategy_runtime.set_state rt "short_base_target" (S.V_float 3.0);
  let e1 = env_of_program p in
  let e2 = env_of_program p in
  let price = 123.5 in
  let now = 99.0 in
  let fields = [ "side", S.V_string "buy"; "qty", S.V_float 1.0 ] in
  P.fill_env p e1 rt ~price ~now ~event_kind:"book_update" ~event_fields:fields;
  P.fill_env_resolved
    (P.resolve p)
    rt
    e2
    ~price
    ~now
    ~event_kind:"book_update"
    ~event_fields:fields;
  Alcotest.(check bool) "same floats" true (e1.IR.floats = e2.IR.floats);
  Alcotest.(check bool) "same bools" true (e1.IR.bools = e2.IR.bools);
  Alcotest.(check bool) "same strings" true (e1.IR.strings = e2.IR.strings);
  Alcotest.(check bool) "same ints" true (e1.IR.ints = e2.IR.ints)
;;

(* State written by the compiled program reaches the runtime by pre-resolved slot. *)
let test_resolved_state_sync () =
  let file = load_file () in
  let p = compile_file file in
  let rt = Dio_strategies.Strategy_runtime.create file in
  let resolved = P.resolve p in
  let env = env_of_program p in
  set_float p env "short_base_target" 7.25;
  Array.iter
    (fun (rs : P.resolved_slot) ->
      if rs.P.rs_scope = S.State
      then
        Dio_strategies.Strategy_runtime.set_state_slot
          rt
          rs.P.rs_slot
          (match rs.P.rs_kind with
           | IR.K_float -> S.V_float env.IR.floats.(rs.P.rs_env)
           | IR.K_int -> S.V_int env.IR.ints.(rs.P.rs_env)
           | IR.K_bool -> S.V_bool env.IR.bools.(rs.P.rs_env)
           | IR.K_string -> S.V_string env.IR.strings.(rs.P.rs_env)))
    resolved;
  match Dio_strategies.Strategy_runtime.get_state rt "short_base_target" with
  | S.V_float f -> Alcotest.(check (float 0.0001)) "state synced" 7.25 f
  | _ -> Alcotest.fail "state not synced"
;;

(* A step's [let] binding is substituted into the guard and actions that read it. *)
let test_let_substitution () =
  let text =
    "strategy let_test\nversion 1\n\nremembers:\n  fired: bool\n\n\
     when book.updates:\n\n  fire:\n    let armed = $platform.mark > 0.0\n\
     \x20   if armed and not $state.fired:\n      set.gate(name: \"fired\", value: \"true\")\n"
  in
  match Script.parse_string text with
  | Error m -> Alcotest.failf "parse: %s" m
  | Ok f ->
    let p =
      match P.compile f with
      | Error m -> Alcotest.failf "compile: %s" m
      | Ok p -> p
    in
    let env = env_of_program p in
    let sink = P.create_sink () in
    set_bool p env "fired" false;
    set_platform_float p env "mark" 100.0;
    env.IR.event_kind <- "book_update";
    P.run p env sink;
    Alcotest.(check bool) "let guard fired" true (match slot p S.State "fired" with
      | IR.K_bool, i -> env.IR.bools.(i)
      | _ -> Alcotest.fail "fired is not bool")
;;

(* The fact refresh is the per-event path's only remaining allocation source. With the
   compiled env as the sink it must allocate nothing: the dense slot tables replaced the
   Hashtbl lookups, whose [Some] boxed a value per fact written. *)
let test_facts_zero_alloc () =
  let file = load_file () in
  let p = compile_file file in
  let env = env_of_program p in
  let rt = Dio_strategies.Strategy_runtime.create file in
  let io = P.env_facts_io p env rt in
  let ctx = Dio_strategies.Strategy_cycle_engine.create () in
  ctx.cg_symbol <- "TEST/USDC";
  ctx.cg_now <- 10.0;
  ctx.cg_asset
  <- Some
       { Dio_strategies.Strategy_state.exchange = "nonexistent"
       ; symbol = "TEST/USDC"
       ; qty = "0.1"
       ; grid_interval = 0.01
       ; sell_mult = "0.999"
       ; strategy = "test"
       ; maker_fee = Some 0.0
       ; taker_fee = Some 0.0
       ; accumulation_buffer = 0.0
       ; base_accumulation = false
       ; sell_levels_persistence = false
       };
  (* Warm the per-symbol caches, which resolve once. *)
  Dio_strategies.Strategy_cycle_engine.market_facts_io io ctx;
  let before = Monotonic_clock.minor_words () in
  for _ = 1 to 1000 do
    Dio_strategies.Strategy_cycle_engine.market_facts_io io ctx
  done;
  let after = Monotonic_clock.minor_words () in
  Alcotest.(check int) "fact refresh allocates zero words" 0 (after - before)
;;

(* The grid strategy uses engine phases and step assignments; it must compile and replay
   its phases in order. *)
let test_grid_compiles () =
  match Script.parse_file "jacobs_ladder.strategy" with
  | Error m -> Alcotest.failf "parse: %s" m
  | Ok f ->
    let p =
      match P.compile f with
      | Error m -> Alcotest.failf "compile: %s" m
      | Ok p -> p
    in
    Alcotest.(check bool) "steps compiled" true (Array.length p.P.steps > 0);
    let env = env_of_program p in
    env.IR.event_kind <- "book_update";
    let sink = P.create_sink () in
    P.run p env sink;
    let phases = ref 0 in
    for i = 0 to sink.P.cmd_count - 1 do
      match sink.P.cmds.(i) with
      | P.Cm_phase id ->
        incr phases;
        Alcotest.(check bool)
          "phase id in range"
          true
          (id >= 0 && id < Array.length P.engine_phases)
      | _ -> ()
    done;
    Alcotest.(check bool) "engine phases replayed" true (!phases > 0)
;;


(* The buy-side latch is the last gate before a paused grid: it must never fire while the
   plan says the balance covers the buy. This pins the compiled guard for the two-sided
   case (balance sufficient) and the failing case (balance short). *)
(* The buy-side latch is the last gate before a paused grid. The compiled guards are
   correct; what they cannot see is that the plan phase republishes [buy_balance_ok] later
   in the same pass, so the latch action itself must re-check the fresh plan (see
   [Strategy_cycle_engine.buy_place_latch_capital_low]). These cases pin the guard so a
   future change cannot make it unconditional. *)
let test_buy_latch_guard () =
  let f =
    match Script.parse_file "jacobs_ladder.strategy" with
    | Error m -> Alcotest.failf "parse: %s" m
    | Ok f -> f
  in
  let p = compile_file f in
  let latch = ref (-1) in
  Array.iteri
    (fun i n -> if n = "buy_place_latch_capital_low" then latch := i)
    P.engine_phases;
  Alcotest.(check bool) "phase registered" true (!latch >= 0);
  let step_with phase =
    let steps =
      Array.to_list p.P.steps
      |> List.filter (fun (st : P.step) ->
        Array.exists
          (function P.Engine_phase i -> String.equal P.engine_phases.(i) phase | _ -> false)
          st.then_actions)
    in
    Alcotest.(check int) ("one " ^ phase ^ " step") 1 (List.length steps);
    match (List.hd steps).P.guard with
    | Some g -> g
    | None -> Alcotest.fail (phase ^ " step has no guard")
  in
  let guard = step_with "buy_place_latch_capital_low" in
  let send_guard = step_with "buy_place_send" in
  let env = env_of_program p in
  env.IR.event_kind <- "book_update";
  set_bool p env "cycle_ok" true;
  set_bool p env "buy_active" true;
  set_bool p env "buy_pending" true;
  set_bool p env "buy_should_cancel" false;
  set_bool p env "buy_count_zero" true;
  set_platform_bool p env "buy_balance_ok" false;
  set_platform_bool p env "quote_balance_stale" false;
  set_platform_bool p env "buy_capital_low" false;
  set_platform_bool p env "buy_crossing" false;
  set_platform_bool p env "buy_cooldown" false;
  set_platform_bool p env "buy_inflight" false;
  Alcotest.(check bool) "latch fires when the balance is short" true (guard env);
  Alcotest.(check bool) "send is suppressed when the balance is short" false (send_guard env);
  set_platform_bool p env "buy_balance_ok" true;
  Alcotest.(check bool)
    "latch must not fire when the balance covers the buy"
    false
    (guard env);
  Alcotest.(check bool) "send fires when the balance covers the buy" true (send_guard env)
;;

let () =
  Alcotest.run
    "strategy_program"
    [ ( "compile"
      , [ Alcotest.test_case "fill then entry" `Quick test_fill_and_entry
        ; Alcotest.test_case "zero allocation" `Quick test_zero_alloc
        ; Alcotest.test_case "resolved fill" `Quick test_resolved_fill_matches
        ; Alcotest.test_case "resolved state sync" `Quick test_resolved_state_sync
        ; Alcotest.test_case "let substitution" `Quick test_let_substitution
        ; Alcotest.test_case "grid compiles" `Quick test_grid_compiles
        ; Alcotest.test_case "facts zero allocation" `Quick test_facts_zero_alloc
        ; Alcotest.test_case "buy latch guard" `Quick test_buy_latch_guard
        ] )
    ]
;;
