(* Tests for the Alpaca orderbook TOB semantics:
   - [update_tob_from_trade]: a trade print is evidence of price, never a
     two-sided quote - it must not fabricate a bid/ask from a fill. It only
     nudges a single crossed side, is ignored when out-of-order relative to
     the current quote, and only serves as a labeled single-price fallback
     when no real quote has arrived for [stale_quote_seconds].
   - WS "q"/"t" handler integration: real quotes win, crossing trades nudge a
     single side, older prints never move a fresher quote. *)

let () = Random.self_init ()

(* ---- update_tob_from_trade (pure) -------------------------------------- *)

let book = Some (140.0, 10.0, 141.0, 10.0)

(** Alcotest 1.9 has [pair]/[triple] but no [quad]: compare a 4-tuple option
    through a nested pair. *)
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

let test_no_book_seeds_single_price () =
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:false
      ~trade_newer:true
      None
      ~price:140.5
      ~size:4.0
  in
  check_tob (Some (140.5, 4.0, 140.5, 4.0)) r
;;

let test_print_inside_spread_leaves_book_unchanged () =
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:false
      ~trade_newer:true
      book
      ~price:140.5
      ~size:4.0
  in
  check_tob None r
;;

let test_print_at_ask_leaves_book_unchanged () =
  (* A fill AT the ask confirms the quote; it must not churn the book. *)
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:false
      ~trade_newer:true
      book
      ~price:141.0
      ~size:4.0
  in
  check_tob None r
;;

let test_print_above_ask_lifts_ask_only () =
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:false
      ~trade_newer:true
      book
      ~price:142.0
      ~size:5.0
  in
  check_tob (Some (140.0, 10.0, 142.0, 5.0)) r
;;

let test_print_below_bid_drops_bid_only () =
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:false
      ~trade_newer:true
      book
      ~price:139.0
      ~size:5.0
  in
  check_tob (Some (139.0, 5.0, 141.0, 10.0)) r
;;

let test_stale_quote_uses_single_price_fallback () =
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:true
      ~trade_newer:true
      book
      ~price:138.0
      ~size:6.0
  in
  check_tob (Some (138.0, 6.0, 138.0, 6.0)) r
;;

let test_out_of_order_print_never_moves_fresher_quote () =
  (* A stale print from a previous session (older event time than the current
     quote) must never move the book - this was the pre-market -> regular
     oscillation trigger. *)
  let r =
    Alpaca.Orderbook.update_tob_from_trade
      ~quote_stale:false
      ~trade_newer:false
      book
      ~price:130.0
      ~size:6.0
  in
  check_tob None r
;;

(* ---- WS "q"/"t" handler integration ------------------------------------ *)

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

let test_quote_then_crossing_trade_lifts_ask_only () =
  let sym = "TQ_A" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* Newer print above the ask: lifts the ask, bid preserved. *)
  handle (trade_msg sym 142.0 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.0, 10.0, 142.0, 5.0))
;;

let test_quote_then_older_trade_leaves_book_unchanged () =
  let sym = "TQ_B" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  (* Print from BEFORE the quote's event time: ignored entirely. *)
  handle (trade_msg sym 135.0 "2026-01-02T14:59:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

let test_quote_then_inside_spread_trade_leaves_book_unchanged () =
  let sym = "TQ_C" in
  handle (quote_msg sym 140.0 141.0 "2026-01-02T15:00:00Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0));
  handle (trade_msg sym 140.5 "2026-01-02T15:00:01Z");
  check_book sym (Some (140.0, 10.0, 141.0, 10.0))
;;

let test_trade_before_any_quote_seeds_single_price () =
  let sym = "TQ_D" in
  handle (trade_msg sym 138.5 "2026-01-02T15:00:01Z");
  (* No quote has ever arrived for this symbol (quote_wall_ts = 0): the trade
     seeds a single price until the quote feed catches up. *)
  check_book sym (Some (138.5, 5.0, 138.5, 5.0))
;;

let () =
  Alcotest.run
    "alpaca_orderbook"
    [ ( "update_tob_from_trade"
      , [ Alcotest.test_case
            "no book seeds single price"
            `Quick
            test_no_book_seeds_single_price
        ; Alcotest.test_case
            "print inside spread leaves book unchanged"
            `Quick
            test_print_inside_spread_leaves_book_unchanged
        ; Alcotest.test_case
            "print at ask leaves book unchanged"
            `Quick
            test_print_at_ask_leaves_book_unchanged
        ; Alcotest.test_case
            "print above ask lifts ask only"
            `Quick
            test_print_above_ask_lifts_ask_only
        ; Alcotest.test_case
            "print below bid drops bid only"
            `Quick
            test_print_below_bid_drops_bid_only
        ; Alcotest.test_case
            "stale quote uses single-price fallback"
            `Quick
            test_stale_quote_uses_single_price_fallback
        ; Alcotest.test_case
            "out-of-order print never moves fresher quote"
            `Quick
            test_out_of_order_print_never_moves_fresher_quote
        ] )
    ; ( "ws_handlers"
      , [ Alcotest.test_case
            "quote then crossing trade lifts ask only"
            `Quick
            test_quote_then_crossing_trade_lifts_ask_only
        ; Alcotest.test_case
            "quote then older trade leaves book unchanged"
            `Quick
            test_quote_then_older_trade_leaves_book_unchanged
        ; Alcotest.test_case
            "quote then inside-spread trade leaves book unchanged"
            `Quick
            test_quote_then_inside_spread_trade_leaves_book_unchanged
        ; Alcotest.test_case
            "trade before any quote seeds single price"
            `Quick
            test_trade_before_any_quote_seeds_single_price
        ] )
    ]
;;
