(* Tests for the Alpaca orderbook TOB semantics:
   - The top-of-book comes ONLY from the WebSocket quote stream ("q"
     messages) - the single data source, matching every other exchange in
     this codebase. No REST snapshot polling, no ticker fallback.
   - Trade prints ("t" messages) are recorded for analytics but NEVER publish
     a bid/ask - no single-price seed before a quote, no crossing-side nudge,
     no stale-quote fallback. A fabricated bid = ask = last trade showed raw
     print volatility that is not a real market. *)

let () = Random.self_init ()

let check_tob expected got =
  let to_nested = function
    | Some (a, b, c, d) -> Some ((a, b), (c, d))
    | None -> None
  in
  let f = Alcotest.float 0.0 in
  let p2 = Alcotest.pair f f in
  let p4 = Alcotest.pair p2 p2 in
  Alcotest.check (Alcotest.option p4) "top-of-book" (to_nested expected) (to_nested got)
;;

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
  (* No real quote has ever arrived: the trade must NOT seed a single price.
     The store simply has no valid TOB until a real quote (WS or REST) lands. *)
  let sym = "T_NOSEED" in
  handle (trade_msg sym 138.5 "2026-01-02T15:00:01Z");
  check_book sym None
;;

let test_trade_after_quote_never_moves_book () =
  let sym = "T_NOMOVE" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* A print ABOVE the ask (would have "lifted the ask" before): the book must
     not move - a single print is not a two-sided quote. *)
  handle (trade_msg sym 142.0 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* A print BELOW the bid: still no movement. *)
  handle (trade_msg sym 138.0 "2026-01-02T15:00:02Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* A print inside the spread: no movement. *)
  handle (trade_msg sym 140.5 "2026-01-02T15:00:03Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* An out-of-order print from a previous session: no movement. *)
  handle (trade_msg sym 130.0 "2026-01-02T14:59:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

let test_trade_after_stale_quote_never_publishes_fallback () =
  (* Even with no fresh quote for a long stretch, trade prints must never
     fabricate a bid = ask = last trade. The book holds the last real quote
     while the REST snapshot poll refreshes it. We prove the invariant
     directly: a trade with a NEWER event time than the quote cannot change
     the published TOB. *)
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
  (* A "q" message with only one side fills in the other from the previous
     real quote (WS frames can carry a single-side update). *)
  let sym = "Q_ONESIDE" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  handle (quote_msg sym 140.1 0.0 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.1, 10.0, 141.0, 10.0))
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
    ]
;;
