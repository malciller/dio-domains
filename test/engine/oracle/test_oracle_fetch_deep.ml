(* Oracle_fetch deep-history merge tests: the Yahoo deep series (from the
   [dio.yahoo] client) is prepended to the venue series, the venue wins on
   any overlap, and nothing is synthesized.

   The merge lives in [Oracle_fetch] (it builds [Oracle_types.series], the
   oracle's own domain type); the Yahoo client itself is tested in
   test/external/yahoo/test_yahoo.ml. *)

open Dio_oracle

let bar ~date ~close =
  { Oracle_types.date; open_ = close; high = close; low = close; close; volume = 0.0 }
;;

let test_merge_series () =
  let venue =
    { Oracle_types.symbol = "ETH/USD"
    ; calendar_kind = Oracle_types.Crypto
    ; bars =
        [| bar ~date:"2024-08-21" ~close:2600.0; bar ~date:"2024-08-22" ~close:2650.0 |]
    ; gaps = []
    }
  in
  (* Deep bars strictly before the venue start are prepended; an overlapping
     date stays with the venue. *)
  let deep =
    { Oracle_types.symbol = "ETH-USD"
    ; calendar_kind = Oracle_types.Crypto
    ; bars =
        [| bar ~date:"2024-08-19" ~close:2550.0
         ; bar ~date:"2024-08-20" ~close:2570.0
         ; bar ~date:"2024-08-21" ~close:9999.0
        |]
    ; gaps = []
    }
  in
  let merged, added = Oracle_fetch.merge_series ~venue ~deep in
  Alcotest.(check int) "two deep bars added" 2 added;
  Alcotest.(check int) "merged length" 4 (Array.length merged.bars);
  (* Venue bar wins on the overlap date. *)
  let overlap =
    Array.to_list merged.bars
    |> List.find (fun (b : Oracle_types.bar) -> b.date = "2024-08-21")
  in
  Alcotest.(check (float 1e-9)) "venue wins overlap" 2600.0 overlap.close;
  (* No deep bars before the venue start -> unchanged. *)
  let empty_deep = { deep with bars = [||] } in
  let merged2, added2 = Oracle_fetch.merge_series ~venue ~deep:empty_deep in
  Alcotest.(check int) "no deep bars" 0 added2;
  Alcotest.(check int) "unchanged" 2 (Array.length merged2.bars);
  (* A DESCENDING venue series (some venue feeds return newest-first) must
     merge the same way: the venue start is its minimum date. *)
  let desc_venue =
    { venue with bars = Array.of_list (Array.to_list venue.bars |> List.rev) }
  in
  let merged3, added3 = Oracle_fetch.merge_series ~venue:desc_venue ~deep in
  Alcotest.(check int) "descending venue adds same deep bars" 2 added3;
  Alcotest.(check int) "descending venue merged length" 4 (Array.length merged3.bars);
  let overlap3 =
    Array.to_list merged3.bars
    |> List.find (fun (b : Oracle_types.bar) -> b.date = "2024-08-21")
  in
  Alcotest.(check (float 1e-9)) "descending venue wins overlap" 2600.0 overlap3.close
;;

let () =
  Alcotest.run
    "oracle_fetch_deep"
    [ ( "merge"
      , [ Alcotest.test_case "deep prepend, venue wins overlap" `Quick test_merge_series ]
      )
    ]
;;
