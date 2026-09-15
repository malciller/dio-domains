(* Tests for the Alpaca orderbook TOB semantics:
   - The top-of-book comes ONLY from the WebSocket quote stream ("q" messages) - the
     single data source, matching every other exchange in this codebase. No REST snapshot
     polling, no ticker fallback.
   - Trade prints ("t" messages) are recorded for analytics but NEVER publish a bid/ask -
     no single-price seed before a quote, no crossing-side nudge, no stale-quote fallback.
     A fabricated bid = ask = last trade showed raw print volatility that is not a real
     market. *)

let () = Random.self_init ()

let check_tob_eps eps expected got =
  let to_nested = function
    | Some (a, b, c, d) -> Some ((a, b), (c, d))
    | None -> None
  in
  let f = Alcotest.float eps in
  let p2 = Alcotest.pair f f in
  let p4 = Alcotest.pair p2 p2 in
  Alcotest.check (Alcotest.option p4) "top-of-book" (to_nested expected) (to_nested got)
;;

let check_tob = check_tob_eps 0.0
let handle raw = Alpaca.Orderbook.handle_message_str raw

let quote_msg symbol bid ask ts =
  Printf.sprintf
    "{\"T\":\"q\",\"S\":\"%s\",\"bp\":%.2f,\"bs\":10.0,\"ap\":%.2f,\"as\":10.0,\"t\":\"%s\"}"
    symbol
    bid
    ask
    ts
;;

let trade_msg symbol price ts =
  Printf.sprintf
    "{\"T\":\"t\",\"S\":\"%s\",\"p\":%.2f,\"s\":5.0,\"t\":\"%s\"}"
    symbol
    price
    ts
;;

let check_book symbol expected =
  let got = Alpaca.Orderbook.get_best_bid_ask symbol in
  check_tob expected got
;;

(* ---- Trade prints never publish TOB ------------------------------------- *)

let test_trade_before_any_quote_publishes_nothing () =
  (* No quote has arrived: a trade must not seed a single price, so the store has no valid
     TOB until a real quote lands. *)
  let sym = "T_NOSEED" in
  handle (trade_msg sym 138.5 "2026-01-02T15:00:01Z");
  check_book sym None
;;

let test_trade_after_quote_never_moves_book () =
  let sym = "T_NOMOVE" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* A print above the ask must not move the book: one print is not a quote. *)
  handle (trade_msg sym 142.0 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* A print below the bid: no movement. *)
  handle (trade_msg sym 138.0 "2026-01-02T15:00:02Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* A print inside the spread: no movement. *)
  handle (trade_msg sym 140.5 "2026-01-02T15:00:03Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* Out-of-order print from a previous session: no movement. *)
  handle (trade_msg sym 130.0 "2026-01-02T14:59:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

let test_trade_after_stale_quote_never_publishes_fallback () =
  (* With no fresh quote for a long stretch, trade prints must never fabricate bid = ask =
     last trade; the book holds the last real quote until the REST snapshot poll refreshes
     it. A trade newer than the quote must not change the published TOB. *)
  let sym = "T_NOSTALE" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  handle (trade_msg sym 130.0 "2026-01-02T18:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

(* ---- Real quotes publish TOB -------------------------------------------- *)

let test_quote_publishes_bid_ask () =
  let sym = "Q_OK" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

let test_later_quote_updates_book () =
  let sym = "Q_MOVE" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  handle (quote_msg sym 140.5 141.5 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.5, 10.0, 141.5, 10.0))
;;

let test_one_sided_quote_merges_previous_side () =
  (* A "q" message with only one side fills in the other from the previous real quote (WS
     frames can carry a single-side update). *)
  let sym = "Q_ONESIDE" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  handle (quote_msg sym 140.1 0.0 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.1, 10.0, 141.0, 10.0))
;;

(* ---- Json_scan port: parsing-edge equivalence ---------------------------- *)

let test_batch_array_processes_all_events () =
  (* A single frame can carry an array of events; every element must be applied in order
     (the last quote wins). *)
  let sym = "J_ARRAY" in
  handle
    (Printf.sprintf
       "[{\"T\":\"q\",\"S\":\"%s\",\"bp\":140.0,\"bs\":10.0,\"ap\":141.0,\"as\":10.0,\"t\":\"2026-01-02T15:00:00Z\"},{\"T\":\"q\",\"S\":\"%s\",\"bp\":142.0,\"bs\":12.0,\"ap\":143.0,\"as\":12.0,\"t\":\"2026-01-02T15:00:01Z\"}]"
       sym
       sym);
  check_book sym (Some (142.0, 12.0, 143.0, 12.0))
;;

let test_exponent_and_string_encoded_numbers () =
  (* Kraken/Alpaca feeds emit sizes as numbers (incl. 2.5e-3) and a few fields as strings;
     both shapes must decode to the same floats. *)
  let sym = "J_NUM" in
  handle
    (Printf.sprintf
       "{\"T\":\"q\",\"S\":\"%s\",\"bp\":\"140.50\",\"bs\":2.5e-3,\"ap\":141.25,\"as\":\"1e2\",\"t\":\"2026-01-02T15:00:00Z\"}"
       sym);
  check_tob_eps
    1e-9
    (Some (140.5, 0.0025, 141.25, 100.0))
    (Alpaca.Orderbook.get_best_bid_ask sym)
;;

let test_reordered_and_nested_fields () =
  (* Fields out of canonical order, plus a nested object that reuses the same key names
     ("t" and "T" appear inside "meta"). Field lookup must skip nested containers and
     match only top-level keys. *)
  let sym = "J_ORDER" in
  handle
    (Printf.sprintf
       "{\"as\":10.0,\"ap\":141.0,\"meta\":{\"t\":\"2026-01-01T00:00:00Z\",\"T\":\"x\"},\"bs\":10.0,\"bp\":140.0,\"S\":\"%s\",\"t\":\"2026-01-02T15:00:00Z\",\"T\":\"q\"}"
       sym);
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

let () =
  Alcotest.run
    "alpaca_orderbook"
    [ ( "trade_never_publishes_tob"
      , [ Alcotest.test_case
            "trade before any quote publishes nothing"
            `Quick
            test_trade_before_any_quote_publishes_nothing
        ; Alcotest.test_case
            "trade after quote never moves the book"
            `Quick
            test_trade_after_quote_never_moves_book
        ; Alcotest.test_case
            "trade after stale quote never publishes a fallback"
            `Quick
            test_trade_after_stale_quote_never_publishes_fallback
        ] )
    ; ( "quote_publishes_tob"
      , [ Alcotest.test_case "quote publishes bid/ask" `Quick test_quote_publishes_bid_ask
        ; Alcotest.test_case
            "later quote updates the book"
            `Quick
            test_later_quote_updates_book
        ; Alcotest.test_case
            "one-sided quote merges the previous side"
            `Quick
            test_one_sided_quote_merges_previous_side
        ] )
    ; ( "json_scan_port"
      , [ Alcotest.test_case
            "batch array applies every event in order"
            `Quick
            test_batch_array_processes_all_events
        ; Alcotest.test_case
            "exponent and string-encoded numbers decode"
            `Quick
            test_exponent_and_string_encoded_numbers
        ; Alcotest.test_case
            "reordered and nested fields resolve to top-level keys"
            `Quick
            test_reordered_and_nested_fields
        ] )
    ]
;;
