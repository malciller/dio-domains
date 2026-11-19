(* Alpaca oracle adapter tests: bar and calendar parsing. Pure functions only (no
   network). *)

let near a b = Alcotest.(check (float 1e-9)) "approx" a b

let test_parse_bars () =
  let json =
    Yojson.Safe.from_string
      {|{"bars":[
           {"t":"2024-01-02T05:00:00Z","o":100.0,"h":101.0,"l":99.0,"c":100.5,"v":1000},
           {"t":"2024-01-03T05:00:00Z","o":100.5,"h":103.0,"l":100.0,"c":102.5,"v":1200}
         ]}|}
  in
  let bars = Alpaca.Alpaca_oracle.parse_bars json in
  Alcotest.(check int) "two bars" 2 (List.length bars);
  let first = List.hd bars in
  Alcotest.(check string) "date truncated to YYYY-MM-DD" "2024-01-02" first.date;
  near 100.5 first.close;
  near 1000.0 first.volume
;;

let test_parse_bars_empty () =
  let json = Yojson.Safe.from_string {|{"bars":[]}|} in
  Alcotest.(check int) "empty bars" 0 (List.length (Alpaca.Alpaca_oracle.parse_bars json));
  (* A missing "bars" key also yields no bars, never a crash. *)
  Alcotest.(check int)
    "no bars key"
    0
    (List.length (Alpaca.Alpaca_oracle.parse_bars (`Assoc [])))
;;

let test_parse_calendar () =
  let json =
    Yojson.Safe.from_string
      {|[{"date":"2024-01-02"},{"date":"2024-01-03"},{"date":"2024-01-02"}]|}
  in
  let dates = Alpaca.Alpaca_oracle.parse_calendar json in
  Alcotest.(check (list string))
    "dates sorted + deduped"
    [ "2024-01-02"; "2024-01-03" ]
    dates
;;

let test_aggregate_daily () =
  let bar date o h l c v =
    { Dio_exchange.Exchange_intf.Types.date
    ; open_ = o
    ; high = h
    ; low = l
    ; close = c
    ; volume = v
    }
  in
  (* Two sessions of hourly bars, deliberately unsorted within a session. *)
  let intraday =
    [ bar "2026-11-18" 100.0 101.0 99.5 100.5 10.0
    ; bar "2026-11-18" 100.5 103.0 100.0 102.5 20.0
    ; bar "2026-11-18" 102.5 102.8 98.0 99.0 5.0
    ; bar "2026-11-19" 99.0 100.0 98.5 99.5 7.0
    ]
  in
  let daily = Alpaca.Alpaca_oracle.aggregate_daily intraday in
  Alcotest.(check int) "one bar per session" 2 (List.length daily);
  let d1 = List.nth daily 0 in
  Alcotest.(check string) "ascending sessions" "2026-11-18" d1.date;
  near 100.0 d1.open_;
  near 103.0 d1.high;
  near 98.0 d1.low;
  near 99.0 d1.close;
  near 35.0 d1.volume;
  let d2 = List.nth daily 1 in
  Alcotest.(check string) "second session" "2026-11-19" d2.date;
  near 99.5 d2.close
;;

(* Cold start: a listing with no historical bars on its feed is modeled from the session
   bar accumulated out of the live stream. *)
let test_live_session_bar () =
  let open Alpaca.Orderbook in
  let now = Unix.time () in
  let s1 = observe_session None ~price:100.0 ~volume:5.0 ~at:now in
  let s = Option.get s1 in
  near 100.0 s.open_px;
  near 100.0 s.high_px;
  near 100.0 s.low_px;
  near 100.0 s.last_px;
  near 5.0 s.volume;
  (* High/low absorb the range; close tracks the last price; volume accumulates. *)
  let s2 = observe_session s1 ~price:104.0 ~volume:2.0 ~at:now in
  let s = Option.get s2 in
  near 100.0 s.open_px;
  near 104.0 s.high_px;
  near 104.0 s.last_px;
  near 7.0 s.volume;
  let s3 = observe_session s2 ~price:99.0 ~volume:1.0 ~at:now in
  let s = Option.get s3 in
  near 100.0 s.open_px;
  near 104.0 s.high_px;
  near 99.0 s.low_px;
  near 99.0 s.last_px;
  near 8.0 s.volume;
  (* A non-positive price is not an observation. *)
  let s4 = observe_session s3 ~price:0.0 ~volume:1.0 ~at:now in
  near 99.0 (Option.get s4).last_px;
  near 8.0 (Option.get s4).volume;
  (* A prior session rolls the accumulator over instead of widening today's range. *)
  let s5 = observe_session s4 ~price:50.0 ~volume:1.0 ~at:(now -. (2.0 *. 86_400.0)) in
  let s = Option.get s5 in
  near 50.0 s.open_px;
  near 50.0 s.high_px;
  near 50.0 s.low_px
;;

let test_venue_contract () =
  let open Alpaca.Alpaca_oracle in
  Alcotest.(check string) "default quote USD" "USD" default_quote;
  Alcotest.(check (float 1e-9))
    "dollar notional floor of $1 (fractional minimum order value)"
    1.0
    (min_notional ~symbol:"QQQ")
;;

let () =
  Alcotest.run
    "alpaca_oracle"
    [ ( "parsers"
      , [ Alcotest.test_case "parse bars" `Quick test_parse_bars
        ; Alcotest.test_case "empty bars tolerated" `Quick test_parse_bars_empty
        ; Alcotest.test_case "parse calendar" `Quick test_parse_calendar
        ; Alcotest.test_case
            "intraday folds into daily sessions"
            `Quick
            test_aggregate_daily
        ] )
    ; ( "venue contract"
      , [ Alcotest.test_case "default_quote / min_notional" `Quick test_venue_contract ] )
    ; ( "live session"
      , [ Alcotest.test_case "tape folds into a session bar" `Quick test_live_session_bar
        ] )
    ]
;;
