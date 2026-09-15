open Dio_strategies

let () = Strategy_actions_builtin.register_all ()
let vf f = Strategy_expr.V_float f
let vs s = Strategy_expr.V_string s

let strategy_json =
  {|{
  "name": "rt",
  "version": 1,
  "triggers": ["book_update", "fill"],
  "params": {
    "qty": { "type": "decimal_str", "default": "2" },
    "mult": { "type": "float", "default": "1.5" }
  },
  "state": {
    "tracked_buy": { "type": "buy_intent?", "persist": false },
    "reserved": { "type": "float", "persist": false }
  },
  "steps": [
    {
      "id": "buy",
      "when": { "event": "book_update",
                "all": [ { "is_none": "$state.tracked_buy" },
                         { "not": { "pending": "buy" } } ] },
      "then": [
        { "action": "compute_grid_price",
          "args": { "ref": "$price", "lo": "$params.mult_f",
                    "hi": "$params.mult_f", "side": "below" },
          "bind": { "px": "$out.price" } },
        { "action": "place_buy",
          "args": { "qty": "$params.qty_dec", "price": "$local.px",
                    "dedup_key": "buy:initial" },
          "bind": { "tok": "$out.token" } },
        { "action": "track_buy",
          "args": { "token": "$local.tok", "price": "$local.px" } }
      ],
      "stop": true
    },
    {
      "id": "sell",
      "when": { "event": "fill", "side": "buy" },
      "then": [
        { "action": "compute_sell_price",
          "args": { "base": "$event.fill_price", "mult": "$params.mult_f" },
          "bind": { "spx": "$out.price" } },
        { "action": "place_sell",
          "args": { "qty": "$event.fill_qty", "price": "$local.spx",
                    "dedup_key": "sell:$event.fill_order_id" } },
        { "action": "accumulate", "args": { "qty": "$event.fill_qty" } }
      ]
    }
  ]
}|}
;;

let parse () =
  match Strategy_file.parse_string strategy_json with
  | Error e -> Alcotest.failf "parse: %s" e
  | Ok f -> f
;;

let env_with_price p =
  { Strategy_expr.price = (fun () -> Ok (vf p))
  ; event = (fun _ -> Error "no event")
  ; state = (fun _ -> Ok Strategy_expr.V_none)
  ; param = (fun _ -> Error "no param")
  ; local = (fun _ -> Error "no local")
  ; signal = (fun _ -> Error "no signal")
  ; now = (fun () -> Ok (vf 1000.0))
  ; platform = (fun _ -> Error "no platform")
  }
;;

let test_eval_arith () =
  let env = env_with_price 10.0 in
  match Strategy_expr.eval_arg env "$price * 2.0 + 1.0" with
  | Ok (Strategy_expr.V_float f) -> Alcotest.(check (float 0.0001)) "arith" 21.0 f
  | Ok v -> Alcotest.failf "unexpected value %s" (Strategy_expr.string_of_value v)
  | Error e -> Alcotest.fail e
;;

let test_eval_compare () =
  let env = env_with_price 10.0 in
  match Strategy_expr.eval_arg env "$price < 20.0 and $price >= 10.0" with
  | Ok (Strategy_expr.V_bool b) -> Alcotest.(check bool) "cmp" true b
  | Ok v -> Alcotest.failf "unexpected value %s" (Strategy_expr.string_of_value v)
  | Error e -> Alcotest.fail e
;;

let test_template () =
  let env =
    { (env_with_price 10.0) with
      Strategy_expr.event =
        (fun f ->
          match f with
          | "fill_order_id" -> Ok (vs "o1")
          | _ -> Error "no")
    }
  in
  match Strategy_expr.eval_arg env "sell:$event.fill_order_id" with
  | Ok (Strategy_expr.V_string s) -> Alcotest.(check string) "template" "sell:o1" s
  | Ok v -> Alcotest.failf "unexpected value %s" (Strategy_expr.string_of_value v)
  | Error e -> Alcotest.fail e
;;

let make_handler log =
  { Strategy_runtime.run =
      (fun t name args ->
        log := (name, args) :: !log;
        match name with
        | "compute_grid_price" -> [ "price", vf 99.0 ]
        | "compute_sell_price" -> [ "price", vf 150.0 ]
        | "place_buy" -> [ "token", vs "tok-1" ]
        | "track_buy" ->
          Strategy_runtime.set_state t "tracked_buy" (Strategy_expr.V_bool true);
          []
        | "accumulate" ->
          Strategy_runtime.set_state t "reserved" (vf 2.0);
          []
        | _ -> [])
  }
;;

let action_names calls =
  List.map (fun (c : Strategy_runtime.action_call) -> c.ac_action) calls
;;

let test_runtime_buy_then_no_repeat () =
  let f = parse () in
  let log = ref [] in
  let rt = Strategy_runtime.create ~handlers:(make_handler log) f in
  let c1 =
    Strategy_runtime.run_cycle
      rt
      ~price:100.0
      ~now:0.0
      ~event:(Strategy_runtime.make_event "book_update" [])
  in
  Alcotest.(check (list string))
    "cycle 1 actions"
    [ "compute_grid_price"; "place_buy"; "track_buy" ]
    (action_names c1);
  let c2 =
    Strategy_runtime.run_cycle
      rt
      ~price:100.0
      ~now:1.0
      ~event:(Strategy_runtime.make_event "book_update" [])
  in
  Alcotest.(check (list string)) "cycle 2 actions" [] (action_names c2)
;;

let test_runtime_fill () =
  let f = parse () in
  let log = ref [] in
  let rt = Strategy_runtime.create ~handlers:(make_handler log) f in
  let fill =
    Strategy_runtime.make_event
      "fill"
      [ "side", vs "buy"
      ; "fill_price", vf 100.0
      ; "fill_qty", vf 2.0
      ; "fill_order_id", vs "o1"
      ]
  in
  let calls = Strategy_runtime.run_cycle rt ~price:100.0 ~now:0.0 ~event:fill in
  Alcotest.(check (list string))
    "fill actions"
    [ "compute_sell_price"; "place_sell"; "accumulate" ]
    (action_names calls);
  let place_sell =
    List.find
      (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "place_sell")
      calls
  in
  (match List.assoc_opt "dedup_key" place_sell.ac_args with
   | Some (Strategy_expr.V_string s) ->
     Alcotest.(check string) "dedup template" "sell:o1" s
   | Some v -> Alcotest.failf "dedup wrong type %s" (Strategy_expr.string_of_value v)
   | None -> Alcotest.fail "missing dedup_key");
  (* qty 2 accumulated; reserved set by handler *)
  match Strategy_runtime.get_state rt "reserved" with
  | Some (Strategy_expr.V_float r) -> Alcotest.(check (float 0.0001)) "reserved" 2.0 r
  | _ -> Alcotest.fail "reserved not set"
;;

let test_grid_buy_ref_price () =
  let json =
    {|{"name":"p","version":1,"triggers":["book_update"],"steps":[
       {"id":"s","then":[
         {"action":"compute_buy_ref_price","args":{"bid":"100.0","ask":"101.0"},"bind":{"px":"$out.price"}},
         {"action":"echo","args":{"price":"$local.px"}}
       ]}]}|}
  in
  match Strategy_file.parse_string json with
  | Error e -> Alcotest.fail e
  | Ok f ->
    let rt = Strategy_runtime.create ~handlers:Strategy_actions_grid.handler f in
    let calls =
      Strategy_runtime.run_cycle
        rt
        ~price:100.0
        ~now:0.0
        ~event:(Strategy_runtime.make_event "book_update" [])
    in
    let echo =
      List.find
        (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "echo")
        calls
    in
    (match List.assoc_opt "price" echo.ac_args with
     | Some (Strategy_expr.V_float p) ->
       Alcotest.(check (float 0.0001)) "buy ref price = bid" 100.0 p
     | _ -> Alcotest.fail "no price")
;;

let test_grid_owed_sell_price () =
  let json =
    {|{"name":"p","version":1,"triggers":["book_update"],"steps":[
       {"id":"s","then":[
         {"action":"owed_sell_price","args":{"bid":"100.0","ask":"100.5"},"bind":{"px":"$out.price"}},
         {"action":"echo","args":{"price":"$local.px"}}
       ]}]}|}
  in
  match Strategy_file.parse_string json with
  | Error e -> Alcotest.fail e
  | Ok f ->
    let rt = Strategy_runtime.create ~handlers:Strategy_actions_grid.handler f in
    Strategy_runtime.set_state rt "last_buy_fill_price" (Strategy_expr.V_float 100.0);
    Strategy_runtime.set_state
      rt
      "resuming_after_balance_flag"
      (Strategy_expr.V_bool false);
    Strategy_runtime.set_platform rt "grid_interval" (Strategy_expr.V_float 1.0);
    Strategy_runtime.set_caps
      rt
      { Strategy_runtime.round_price = (fun x -> x)
      ; exchange = "hyperliquid"
      ; remaintain_expired_sells = false
      };
    let calls =
      Strategy_runtime.run_cycle
        rt
        ~price:100.0
        ~now:0.0
        ~event:(Strategy_runtime.make_event "book_update" [])
    in
    let echo =
      List.find
        (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "echo")
        calls
    in
    (match List.assoc_opt "price" echo.ac_args with
     | Some (Strategy_expr.V_float p) ->
       Alcotest.(check (float 0.0001)) "owed sell price" 101.0 p
     | _ -> Alcotest.fail "no price")
;;

let test_grid_available_base () =
  let json =
    {|{"name":"p","version":1,"triggers":["book_update"],"steps":[
       {"id":"s","then":[
         {"action":"available_base","args":{"venue_authoritative":false,"asset_balance_nan":false,"venue_available":"100.0","ledger_balance":"50.0","unreflected_credit":"1.0","reserved_base":"5.0","committed_sell":"2.0","unnetted_hold":"0.5"},"bind":{"av":"$out.available"}},
         {"action":"echo","args":{"available":"$local.av"}}
       ]}]}|}
  in
  match Strategy_file.parse_string json with
  | Error e -> Alcotest.fail e
  | Ok f ->
    let rt = Strategy_runtime.create ~handlers:Strategy_actions_grid.handler f in
    let calls =
      Strategy_runtime.run_cycle
        rt
        ~price:100.0
        ~now:0.0
        ~event:(Strategy_runtime.make_event "book_update" [])
    in
    let echo =
      List.find
        (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "echo")
        calls
    in
    (match List.assoc_opt "available" echo.ac_args with
     | Some (Strategy_expr.V_float a) ->
       Alcotest.(check (float 0.0001)) "available base (ledger)" 43.0 a
     | _ -> Alcotest.fail "no available")
;;

let test_grid_grid_price () =
  let json =
    {|{"name":"p","version":1,"triggers":["book_update"],"steps":[
       {"id":"s","then":[
         {"action":"grid_price","args":{"current":"100.0","grid_interval_pct":"1.0","is_above":false},"bind":{"px":"$out.price"}},
         {"action":"echo","args":{"price":"$local.px"}}
       ]}]}|}
  in
  match Strategy_file.parse_string json with
  | Error e -> Alcotest.fail e
  | Ok f ->
    let rt = Strategy_runtime.create ~handlers:Strategy_actions_grid.handler f in
    let calls =
      Strategy_runtime.run_cycle
        rt
        ~price:100.0
        ~now:0.0
        ~event:(Strategy_runtime.make_event "book_update" [])
    in
    let echo =
      List.find
        (fun (c : Strategy_runtime.action_call) -> String.equal c.ac_action "echo")
        calls
    in
    (match List.assoc_opt "price" echo.ac_args with
     | Some (Strategy_expr.V_float p) ->
       Alcotest.(check (float 0.0001)) "grid price (below)" 99.0 p
     | _ -> Alcotest.fail "no price")
;;

module Stub_engine = struct
  type ctx = int ref

  let prepare _ = true
  let cleanup _ = ()
  let sync c = incr c
  let refresh_fee _ = ()
  let guard _ = true
  let buy_gate _ = true
  let buy_facts _ = false, 0, false
  let buy_cancel _ = ()
  let buy_place _ = ()
  let buy_amend _ = ()
  let sell_prepare _ = ()
  let sell_place _ = ()
  let sell_finalize _ = ()
end

module Stub_grid = Strategy_actions_grid.Make (Stub_engine)

let test_fine_dispatch () =
  let ctx = ref 0 in
  let handler = Stub_grid.handler ctx in
  let json =
    {|{"name":"p","version":1,"triggers":["book_update"],"steps":[
       {"id":"s","then":[{"action":"grid_sync","args":{}}]}]}|}
  in
  match Strategy_file.parse_string json with
  | Error e -> Alcotest.fail e
  | Ok f ->
    let rt = Strategy_runtime.create ~handlers:handler f in
    ignore
      (Strategy_runtime.run_cycle
         rt
         ~price:1.0
         ~now:0.0
         ~event:(Strategy_runtime.make_event "book_update" []));
    Alcotest.(check int) "grid_sync invoked" 1 !ctx
;;

let () =
  Alcotest.run
    "strategy_runtime"
    [ ( "expr"
      , [ Alcotest.test_case "arithmetic" `Quick test_eval_arith
        ; Alcotest.test_case "comparison/bool" `Quick test_eval_compare
        ; Alcotest.test_case "template" `Quick test_template
        ] )
    ; ( "runtime"
      , [ Alcotest.test_case "buy then no repeat" `Quick test_runtime_buy_then_no_repeat
        ; Alcotest.test_case "sell on fill" `Quick test_runtime_fill
        ; Alcotest.test_case "grid buy ref price handler" `Quick test_grid_buy_ref_price
        ; Alcotest.test_case
            "grid owed sell price handler"
            `Quick
            test_grid_owed_sell_price
        ; Alcotest.test_case "grid available base handler" `Quick test_grid_available_base
        ; Alcotest.test_case "grid grid price handler" `Quick test_grid_grid_price
        ; Alcotest.test_case "fine grid dispatch" `Quick test_fine_dispatch
        ] )
    ]
;;
