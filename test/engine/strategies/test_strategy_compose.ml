open Dio_strategies

let valid_json =
  {|{
  "name": "mini",
  "version": 1,
  "triggers": ["book_update", "fill"],
  "params": { "qty": { "type": "decimal_str", "default": "1" } },
  "state": {
    "tracked_buy": { "type": "buy_intent?", "persist": true },
    "last_amend_at": { "type": "float?", "persist": false }
  },
  "steps": [
    {
      "id": "place_initial_buy",
      "when": { "event": "book_update",
                "all": [ { "is_none": "$state.tracked_buy" },
                         { "not": { "pending": "buy" } } ] },
      "then": [
        { "action": "compute_grid_price",
          "args": { "ref": "$price", "lo": "$params.qty_f",
                    "hi": "$params.qty_f", "side": "below" },
          "bind": { "buy_px": "$out.price" } },
        { "action": "place_buy",
          "args": { "qty": "$params.qty_dec", "price": "$local.buy_px",
                    "post_only": true, "dedup_key": "buy:initial" },
          "bind": { "tok": "$out.token" } }
      ],
      "stop": true
    },
    {
      "id": "sell_on_fill",
      "when": { "event": "fill", "side": "buy" },
      "then": [
        { "action": "place_sell",
          "args": { "qty": "$event.fill_qty", "price": "$event.fill_price",
                    "dedup_key": "sell:$event.fill_order_id" } }
      ]
    }
  ]
}|}
;;

let contains sub s =
  let sl = String.length s
  and fl = String.length sub in
  let rec loop i =
    i + fl <= sl && (String.equal (String.sub s i fl) sub || loop (i + 1))
  in
  loop 0
;;

let register () = Strategy_actions_builtin.register_all ()

let errors_of_json s =
  register ();
  match Strategy_file.parse_string s with
  | Error e -> Alcotest.failf "unexpected parse error: %s" e
  | Ok f -> Strategy_compile.errors (Strategy_compile.validate f)
;;

let test_parse_valid () =
  register ();
  match Strategy_file.parse_string valid_json with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    Alcotest.(check string) "name" "mini" f.name;
    Alcotest.(check int) "version" 1 f.version;
    Alcotest.(check int) "triggers" 2 (List.length f.triggers);
    Alcotest.(check int) "params" 1 (List.length f.params);
    Alcotest.(check int) "state" 2 (List.length f.state);
    Alcotest.(check int) "steps" 2 (List.length f.steps)
;;

let test_validate_clean () =
  let errors = errors_of_json valid_json in
  List.iter
    (fun (d : Strategy_compile.diagnostic) -> Alcotest.failf "unexpected: %s" d.msg)
    errors;
  Alcotest.(check int) "no errors" 0 (List.length errors)
;;

let test_bad_json () =
  register ();
  match Strategy_file.parse_string "{ not json" with
  | Ok _ -> Alcotest.fail "expected parse error"
  | Error _ -> ()
;;

let test_unknown_action () =
  let json =
    String.concat
      ""
      [ {|{"name":"x","version":1,"triggers":["book_update"],"steps":[{"id":"s","then":[|}
      ; {|{"action":"no_such_action","args":{}}|}
      ; {|]}]}|}
      ]
  in
  let errors = errors_of_json json in
  Alcotest.(check bool)
    "reports unknown action"
    true
    (List.exists
       (fun (d : Strategy_compile.diagnostic) -> contains "unknown action" d.msg)
       errors)
;;

let test_unknown_state_ref () =
  let json =
    {|{"name":"x","version":1,"triggers":["book_update"],
       "steps":[{"id":"s","when":{"is_none":"$state.nope"},"then":[]}]}|}
  in
  let errors = errors_of_json json in
  Alcotest.(check bool)
    "reports unknown state"
    true
    (List.exists
       (fun (d : Strategy_compile.diagnostic) -> contains "unknown state" d.msg)
       errors)
;;

let test_unknown_trigger () =
  let json =
    {|{"name":"x","version":1,"triggers":["moon_phase"],
       "steps":[{"id":"s","then":[]}]}|}
  in
  let errors = errors_of_json json in
  Alcotest.(check bool)
    "reports unknown trigger"
    true
    (List.exists
       (fun (d : Strategy_compile.diagnostic) -> contains "unknown trigger" d.msg)
       errors)
;;

let test_missing_dedup_key () =
  let json =
    {|{"name":"x","version":1,"triggers":["book_update"],
       "steps":[{"id":"s","then":[
         {"action":"place_buy","args":{"qty":"1","price":"100"}}]}]}|}
  in
  let errors = errors_of_json json in
  Alcotest.(check bool)
    "reports missing dedup_key"
    true
    (List.exists
       (fun (d : Strategy_compile.diagnostic) -> contains "dedup_key" d.msg)
       errors)
;;

let test_unknown_event_field () =
  let json =
    {|{"name":"x","version":1,"triggers":["fill"],
       "steps":[{"id":"s","when":{"event":"fill"},
                 "then":[{"action":"place_sell",
                          "args":{"qty":"$event.bogus","price":"$event.fill_price",
                                  "dedup_key":"d"}}]}]}|}
  in
  let errors = errors_of_json json in
  Alcotest.(check bool)
    "reports unknown event field"
    true
    (List.exists
       (fun (d : Strategy_compile.diagnostic) -> contains "unknown event field" d.msg)
       errors)
;;

let () =
  Alcotest.run
    "strategy_compose"
    [ ( "parse"
      , [ Alcotest.test_case "valid file" `Quick test_parse_valid
        ; Alcotest.test_case "bad json" `Quick test_bad_json
        ] )
    ; ( "validate"
      , [ Alcotest.test_case "clean file" `Quick test_validate_clean
        ; Alcotest.test_case "unknown action" `Quick test_unknown_action
        ; Alcotest.test_case "unknown state ref" `Quick test_unknown_state_ref
        ; Alcotest.test_case "unknown trigger" `Quick test_unknown_trigger
        ; Alcotest.test_case "missing dedup key" `Quick test_missing_dedup_key
        ; Alcotest.test_case "unknown event field" `Quick test_unknown_event_field
        ] )
    ]
;;
