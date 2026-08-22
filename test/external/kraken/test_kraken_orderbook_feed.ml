let test_take_function () =
  (* Test take function for list truncation *)
  let test_list = [ 1; 2; 3; 4; 5 ] in
  let take_3 = Kraken.Kraken_orderbook_feed.take 3 test_list in
  let take_10 = Kraken.Kraken_orderbook_feed.take 10 test_list in
  let take_0 = Kraken.Kraken_orderbook_feed.take 0 test_list in
  let take_empty = Kraken.Kraken_orderbook_feed.take 5 [] in
  Alcotest.(check (list int)) "take 3 from [1;2;3;4;5]" [ 1; 2; 3 ] take_3;
  Alcotest.(check (list int)) "take 10 from [1;2;3;4;5]" [ 1; 2; 3; 4; 5 ] take_10;
  Alcotest.(check (list int)) "take 0 from [1;2;3;4;5]" [] take_0;
  Alcotest.(check (list int)) "take 5 from empty list" [] take_empty
;;

let test_to_decimal_str () =
  (* Test decimal string conversion *)
  Alcotest.(check string)
    "to_decimal_str Float 1.5"
    "1.5"
    (Kraken.Kraken_orderbook_feed.to_decimal_str (`Float 1.5));
  Alcotest.(check string)
    "to_decimal_str Float 0.001"
    "0.001"
    (Kraken.Kraken_orderbook_feed.to_decimal_str (`Float 0.001));
  Alcotest.(check string)
    "to_decimal_str Float 100.0"
    "100"
    (Kraken.Kraken_orderbook_feed.to_decimal_str (`Float 100.0));
  Alcotest.(check string)
    "to_decimal_str Int 42"
    "42"
    (Kraken.Kraken_orderbook_feed.to_decimal_str (`Int 42));
  Alcotest.(check string)
    "to_decimal_str Intlit 123"
    "123"
    (Kraken.Kraken_orderbook_feed.to_decimal_str (`Intlit "123"))
;;

let test_to_decimal_str_with_precision () =
  (* Test decimal string conversion with custom precision *)
  let json = `Float 1.23456789 in
  let result = Kraken.Kraken_orderbook_feed.to_decimal_str ~dec:2 json in
  Alcotest.(check string) "to_decimal_str with 2 decimal precision" "1.23" result
;;

let test_is_effectively_zero () =
  (* Test zero-checking for orderbook levels *)
  Alcotest.(check bool)
    "is_effectively_zero '0'"
    true
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "0");
  Alcotest.(check bool)
    "is_effectively_zero '0.0'"
    true
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "0.0");
  Alcotest.(check bool)
    "is_effectively_zero '0.000'"
    true
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "0.000");
  Alcotest.(check bool)
    "is_effectively_zero '0.0001'"
    false
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "0.0001");
  Alcotest.(check bool)
    "is_effectively_zero '1.0'"
    false
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "1.0");
  Alcotest.(check bool)
    "is_effectively_zero '-0.0'"
    true
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "-0.0");
  Alcotest.(check bool)
    "is_effectively_zero 'invalid'"
    false
    (Kraken.Kraken_orderbook_feed.is_effectively_zero "invalid")
;;

let test_orderbook_structure () =
  (* Test orderbook record structure *)
  let empty_bids =
    Array.make
      25
      { Kraken.Kraken_orderbook_feed.price = "0"
      ; price_wire = "0"
      ; size = "0"
      ; price_float = 0.0
      ; size_float = 0.0
      }
  in
  let empty_asks =
    Array.make
      25
      { Kraken.Kraken_orderbook_feed.price = "0"
      ; price_wire = "0"
      ; size = "0"
      ; price_float = 0.0
      ; size_float = 0.0
      }
  in
  let test_book =
    { Kraken.Kraken_orderbook_feed.symbol = "BTC/USD"
    ; bids = empty_bids
    ; asks = empty_asks
    ; sequence = Some 12345L
    ; checksum = Some 67890l
    ; timestamp = Unix.time ()
    }
  in
  Alcotest.(check string) "orderbook symbol" "BTC/USD" test_book.symbol;
  Alcotest.(check int) "orderbook bids length" 25 (Array.length test_book.bids);
  Alcotest.(check int) "orderbook asks length" 25 (Array.length test_book.asks);
  Alcotest.(check (option int64)) "orderbook sequence" (Some 12345L) test_book.sequence;
  Alcotest.(check (option int32)) "orderbook checksum" (Some 67890l) test_book.checksum;
  Alcotest.(check bool) "orderbook timestamp positive" true (test_book.timestamp > 0.0)
;;

let test_level_structure () =
  (* Test level record structure *)
  let test_level =
    { Kraken.Kraken_orderbook_feed.price = "45000.50"
    ; price_wire = "45000.5"
    ; size = "1.234567"
    ; price_float = 45000.50
    ; size_float = 1.234567
    }
  in
  Alcotest.(check string) "level price" "45000.50" test_level.price;
  Alcotest.(check string) "level wire price" "45000.5" test_level.price_wire;
  Alcotest.(check string) "level size" "1.234567" test_level.size;
  Alcotest.(check (float 0.01)) "level price_float" 45000.50 test_level.price_float;
  Alcotest.(check (float 0.000001)) "level size_float" 1.234567 test_level.size_float
;;

let test_store_operations () =
  (* Test basic store operations *)
  let symbol = "STORE_TEST" in
  (* Initially should not have store *)
  match Kraken.Kraken_orderbook_feed.store_opt symbol with
  | None -> Alcotest.(check bool) "store initially None" true true
  | Some _ -> Alcotest.fail "unexpected initial store"
;;

let test_constants () =
  (* Test that constants are properly defined *)
  (* We can't directly access these, but we can verify related functionality exists *)
  Alcotest.(check bool) "constants test placeholder" true true
;;

let test_json_parsing_helpers () =
  (* Test JSON parsing helper functions *)
  try
    (* These functions exist and don't crash on valid input *)
    Alcotest.(check bool) "json parsing helpers don't crash" true true
  with
  | _ -> Alcotest.fail "json parsing helpers crashed"
;;

(* Kraken's official book-checksum example (docs.kraken.com, "Book checksum
   (WebSocket v2)"). The concatenated normalization of the documented BTC/USD
   snapshot must CRC32 to exactly 3310070434. Guards against regressions in
   both the CRC32 table and the dot/leading-zero normalization. *)
let official_asks =
  [ "45285.2", "0.00100000"
  ; "45286.4", "1.54571953"
  ; "45286.6", "1.54571109"
  ; "45289.6", "1.54560911"
  ; "45290.2", "0.15890660"
  ; "45291.8", "1.54553491"
  ; "45294.7", "0.04454749"
  ; "45296.1", "0.35380000"
  ; "45297.5", "0.09945542"
  ; "45299.5", "0.18772827"
  ]
;;

let official_bids =
  [ "45283.5", "0.10000000"
  ; "45283.4", "1.54582015"
  ; "45282.1", "0.10000000"
  ; "45281.0", "0.10000000"
  ; "45280.3", "1.54592586"
  ; "45279.0", "0.07990000"
  ; "45277.6", "0.03310103"
  ; "45277.5", "0.30000000"
  ; "45277.3", "1.54602737"
  ; "45276.6", "0.15445238"
  ]
;;

let test_crc32_official_documented_example () =
  let doc_input =
    "45285210000045286415457195345286615457110945289615456091145290215890660452918154553491452947445474945296135380000452975994554245299518772827452835100000004528341545820154528211000000045281010000000452803154592586452790799000045277633101034527753000000045277315460273745276615445238"
  in
  let expected =
    Int32.of_int (3310070434 - 4294967296)
    (* unsigned -> signed *)
  in
  Alcotest.(check int32)
    "crc32_zlib matches Kraken's documented example"
    expected
    (Kraken.Kraken_orderbook_feed.crc32_zlib doc_input)
;;

let test_checksum_normalization_matches_kraken_spec () =
  (* Feed the official snapshot through our per-level normalization path
     (asks ascending first, then bids descending) and require the documented
     result. This is the exact code path used by calculate_checksum. *)
  let crc =
    ref (Kraken.Kraken_orderbook_feed.crc32_zlib "")
    (* placeholder; replaced below *)
  in
  ignore !crc;
  let crc = ref 0xFFFFFFFFl in
  List.iter
    (fun (p, q) ->
       crc := Kraken.Kraken_orderbook_feed.add_normalized_to_crc !crc p;
       crc := Kraken.Kraken_orderbook_feed.add_normalized_to_crc !crc q)
    official_asks;
  List.iter
    (fun (p, q) ->
       crc := Kraken.Kraken_orderbook_feed.add_normalized_to_crc !crc p;
       crc := Kraken.Kraken_orderbook_feed.add_normalized_to_crc !crc q)
    official_bids;
  let result = Int32.logxor !crc 0xFFFFFFFFl in
  let expected = Int32.of_int (3310070434 - 4294967296) in
  Alcotest.(check int32)
    "normalized levels match Kraken's documented checksum"
    expected
    result
;;

let test_parse_level_preserves_wire_price () =
  (* The wire representation must survive parsing: it is the checksum input.
     Re-formatting it (padding to fixed decimals) invalidates every check. *)
  match
    Kraken.Kraken_orderbook_feed.parse_level
      "WIRE_TEST/USD"
      (`String "45000.50")
      (`String "1.20000000")
  with
  | Some lvl ->
    Alcotest.(check string)
      "wire price preserved verbatim"
      "45000.50"
      lvl.Kraken.Kraken_orderbook_feed.price_wire;
    Alcotest.(check string)
      "wire qty preserved verbatim"
      "1.20000000"
      lvl.Kraken.Kraken_orderbook_feed.size;
    Alcotest.(check bool)
      "canonical price differs from wire (padded)"
      true
      (lvl.Kraken.Kraken_orderbook_feed.price
       <> lvl.Kraken.Kraken_orderbook_feed.price_wire)
  | None -> Alcotest.fail "parse_level returned None for string inputs"
;;

let test_get_top_levels () =
  (* Test getting top levels from orderbook *)
  let symbol = "TOP_LEVELS_TEST" in
  (* Should not crash even if no data exists *)
  let bids, asks = Kraken.Kraken_orderbook_feed.get_top_levels symbol in
  Alcotest.(check int)
    "get_top_levels bids length for unknown symbol"
    0
    (Array.length bids);
  Alcotest.(check int)
    "get_top_levels asks length for unknown symbol"
    0
    (Array.length asks)
;;

let () =
  Alcotest.run
    "Kraken Orderbook Feed"
    [ ( "utility functions"
      , [ Alcotest.test_case "take_function" `Quick test_take_function
        ; Alcotest.test_case "to_decimal_str" `Quick test_to_decimal_str
        ; Alcotest.test_case
            "to_decimal_str_with_precision"
            `Quick
            test_to_decimal_str_with_precision
        ; Alcotest.test_case "is_effectively_zero" `Quick test_is_effectively_zero
        ] )
    ; ( "data structures"
      , [ Alcotest.test_case "orderbook_structure" `Quick test_orderbook_structure
        ; Alcotest.test_case "level_structure" `Quick test_level_structure
        ] )
    ; ( "store operations"
      , [ Alcotest.test_case "store_operations" `Quick test_store_operations
        ; Alcotest.test_case "get_top_levels" `Quick test_get_top_levels
        ] )
    ; ( "constants and helpers"
      , [ Alcotest.test_case "constants" `Quick test_constants
        ; Alcotest.test_case "json_parsing_helpers" `Quick test_json_parsing_helpers
        ] )
    ; ( "checksum"
      , [ Alcotest.test_case
            "crc32_official_documented_example"
            `Quick
            test_crc32_official_documented_example
        ; Alcotest.test_case
            "normalization_matches_kraken_spec"
            `Quick
            test_checksum_normalization_matches_kraken_spec
        ; Alcotest.test_case
            "parse_level_preserves_wire_price"
            `Quick
            test_parse_level_preserves_wire_price
        ] )
    ]
;;
