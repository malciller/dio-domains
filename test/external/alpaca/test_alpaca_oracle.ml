(* Alpaca oracle adapter tests: bar and calendar parsing. Pure functions
   only (no network). *)

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
        ] )
    ; ( "venue contract"
      , [ Alcotest.test_case "default_quote / min_notional" `Quick test_venue_contract ] )
    ]
;;
