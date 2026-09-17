open Dio_strategies

let register () = Strategy_actions_builtin.register_all ()

module Str = struct
  let contains hay needle =
    let hl = String.length hay
    and nl = String.length needle in
    let rec go i = i + nl <= hl && (String.sub hay i nl = needle || go (i + 1)) in
    go 0
  ;;
end

let mini_str =
  {|
-- Mini grid, written as instructions
strategy mini
version 1

remembers:
  cycle.ok: bool
  tracked.buy: buy.intent? persist
  last.amend.at: float?

tunable:
  qty: decimal_str = "1"
  grid.interval: range = [0.16, 0.16]
  mode: enum ["spot", "ladder"] = "spot"

when any.order.event:
  lifecycle:
    apply.order.event
    stop

when book.updates:

  prepare:
    resolve.book
    early.facts
    cycle.ok = not $platform.price.nan

  place.buy:
    if cycle.ok and is.none(tracked.buy) and not pending("buy"):
      compute.grid.price(ref: $price, lo: $params.grid.interval.lo,
                         hi: $params.grid.interval.hi, side: "below",
                         bind(buy.px: price))
      place.buy(qty: $params.qty.dec, price: $local.buy.px,
                post_only: true, dedup_key: "buy:initial")

  sweep:
    if cycle.ok and capacity(quote.gte: $price * $params.qty.f):
      sell.excess.sweep.phase
|}
;;

let test_parse_mini () =
  register ();
  match Strategy_script.parse_string mini_str with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    Alcotest.(check string) "name" "mini" f.name;
    Alcotest.(check int) "version" 1 f.version;
    Alcotest.(check int) "state count" 3 (List.length f.state);
    Alcotest.(check int) "params count" 3 (List.length f.params);
    (* 9 lifecycle + 3 book_update *)
    Alcotest.(check int) "steps" 12 (List.length f.steps);
    Alcotest.(check (list string))
      "triggers"
      [ "fill"; "order_lifecycle"; "book_update" ]
      f.triggers
;;

let test_roundtrip_json () =
  register ();
  match Strategy_script.parse_string mini_str with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    let s = Yojson.Basic.to_string (Strategy_file.to_json f) in
    (match Strategy_file.parse_string s with
     | Error e -> Alcotest.failf "round-trip parse error: %s (%s)" e s
     | Ok g ->
       let s2 = Yojson.Basic.to_string (Strategy_file.to_json g) in
       Alcotest.(check string) "round-trip stable" s s2)
;;

let test_validate_clean () =
  register ();
  match Strategy_script.parse_string mini_str with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    let errors = Strategy_compile.errors (Strategy_compile.validate f) in
    List.iter
      (fun (d : Strategy_compile.diagnostic) ->
        Alcotest.failf "unexpected diagnostic: %s" d.msg)
      errors;
    Alcotest.(check int) "no errors" 0 (List.length errors)
;;

let test_event_expansion () =
  register ();
  let s =
    {|
strategy ev
version 1
when any.order.event:
  lifecycle:
    apply.order.event
    stop
|}
  in
  match Strategy_script.parse_string s with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    Alcotest.(check int) "nine steps" 9 (List.length f.steps);
    let ids = List.map (fun (s : Strategy_file.step) -> s.st_id) f.steps in
    Alcotest.(check (list string))
      "ids"
      [ "lifecycle_filled"
      ; "lifecycle_cancelled"
      ; "lifecycle_acknowledged"
      ; "lifecycle_amended"
      ; "lifecycle_failed"
      ; "lifecycle_rejected"
      ; "lifecycle_amendment_skipped"
      ; "lifecycle_amendment_failed"
      ; "lifecycle_cancel_cleanup"
      ]
      ids
;;

let test_bare_gate_assignment () =
  register ();
  let s =
    {|
strategy g
version 1
remembers:
  armed: bool
when book.updates:
  set.it:
    armed = not $platform.price.nan and $platform.check.stale.balance
|}
  in
  match Strategy_script.parse_string s with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    let step = List.hd f.steps in
    (match step.st_then with
     | [ { a_name; a_args; _ } ] ->
       Alcotest.(check string) "action" "set_gate" a_name;
       Alcotest.(check string)
         "value"
         "not $platform.price_nan and $platform.check_stale_balance"
         (match List.assoc "value" a_args with
          | `String s -> s
          | _ -> "<non-string>")
     | _ -> Alcotest.fail "expected one set_gate action")
;;

let test_if_otherwise () =
  register ();
  let s =
    {|
strategy branch
version 1
when book.updates:
  branch:
    if $platform.price.nan:
      mark.stale.cycle
    otherwise:
      cycle.facts
|}
  in
  match Strategy_script.parse_string s with
  | Error e -> Alcotest.failf "parse error: %s" e
  | Ok f ->
    let step = List.hd f.steps in
    let names l = List.map (fun (a : Strategy_file.action) -> a.a_name) l in
    Alcotest.(check (list string)) "then" [ "mark_stale_cycle" ] (names step.st_then);
    Alcotest.(check (list string)) "else" [ "cycle_facts" ] (names step.st_else)
;;

let test_error_positional () =
  register ();
  let s =
    {|
strategy err
version 1
when book.updates:
  bad:
    place.buy("1", "100")
|}
  in
  match Strategy_script.parse_string s with
  | Error e -> Alcotest.(check bool) "positional rejected" true (Str.contains e "named")
  | Ok _ -> Alcotest.fail "expected positional-arg rejection"
;;

let test_error_bad_state () =
  register ();
  let s = {|
strategy err
version 1
remembers:
  foo: bogus
|} in
  match Strategy_script.parse_string s with
  | Error _ -> ()
  | Ok _ -> Alcotest.fail "expected unknown-state-type error"
;;

let test_error_unknown_name () =
  register ();
  let s =
    {|
strategy err
version 1
remembers:
  armed: bool
when book.updates:
  s:
    if mystery.thing:
      stop
|}
  in
  match Strategy_script.parse_string s with
  | Error e ->
    Alcotest.(check bool) "reports unknown name" true (Str.contains e "mystery")
  | Ok _ -> Alcotest.fail "expected unknown-name error"
;;

(* ── jacobs_ladder: the shipped strategy file must parse, validate, and stay stable ── *)

let jl_path = "jacobs_ladder.strategy"

(** Frozen step ids in file order. Locks the shape of the shipped strategy: a regression
    in label expansion or ordering fails here. *)
let expected_jl_ids =
  [ "ev_filled"
  ; "ev_cancelled"
  ; "ev_acknowledged"
  ; "ev_amended"
  ; "ev_failed"
  ; "ev_rejected"
  ; "ev_amendment_skipped"
  ; "ev_amendment_failed"
  ; "ev_cancel_cleanup"
  ; "prepare"
  ; "skip_nan_price"
  ; "cleanup"
  ; "sync"
  ; "fee"
  ; "mark_stale"
  ; "buy_gate"
  ; "buy_cancel"
  ; "buy_place_plan"
  ; "buy_place_send"
  ; "buy_place_send_insufficient"
  ; "buy_place_latch"
  ; "buy_place_warn"
  ; "buy_amend_has_sell"
  ; "buy_amend_with_sell"
  ; "buy_amend_no_sell"
  ; "sell_prepare"
  ; "sell_place_should"
  ; "sell_place"
  ; "sell_finalize_latch"
  ; "sell_excess_sweep"
  ; "sell_finalize_end"
  ]
;;

let test_jacobs_ladder_shape () =
  register ();
  match Strategy_loader.parse_file ~path:jl_path with
  | Error e -> Alcotest.failf "parse %s: %s" jl_path e
  | Ok f ->
    let errors = Strategy_compile.errors (Strategy_compile.validate f) in
    Alcotest.(check int) "validates" 0 (List.length errors);
    Alcotest.(check (list string))
      "step ids"
      expected_jl_ids
      (List.map (fun (s : Strategy_file.step) -> s.st_id) f.steps);
    (* The JSON form is the compiled artifact; it must round-trip losslessly. *)
    let s = Yojson.Basic.to_string (Strategy_file.to_json f) in
    (match Strategy_file.parse_string s with
     | Error e -> Alcotest.failf "round-trip parse error: %s" e
     | Ok g ->
       let s2 = Yojson.Basic.to_string (Strategy_file.to_json g) in
       Alcotest.(check string) "round-trip stable" s s2)
;;

let () =
  Alcotest.run
    "strategy_script"
    [ ( "parse"
      , [ Alcotest.test_case "mini script" `Quick test_parse_mini
        ; Alcotest.test_case "event expansion" `Quick test_event_expansion
        ; Alcotest.test_case "bare gate assignment" `Quick test_bare_gate_assignment
        ; Alcotest.test_case "if/otherwise" `Quick test_if_otherwise
        ; Alcotest.test_case "round-trip json" `Quick test_roundtrip_json
        ] )
    ; "validate", [ Alcotest.test_case "clean file" `Quick test_validate_clean ]
    ; ( "errors"
      , [ Alcotest.test_case "positional rejected" `Quick test_error_positional
        ; Alcotest.test_case "unknown state type" `Quick test_error_bad_state
        ; Alcotest.test_case "unknown name" `Quick test_error_unknown_name
        ] )
    ; ( "jacobs_ladder"
      , [ Alcotest.test_case "shipped strategy shape" `Quick test_jacobs_ladder_shape ] )
    ]
;;
