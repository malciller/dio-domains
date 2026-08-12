(* Tests for the Hyperliquid oracle fetch's spot/perpetual resolution logic.
   These exercise the pure functions only (no network). *)

let meta_fixture () =
  (* A spotMeta slice: token index table plus one canonical pair (PURR/USDC)
     and two wrapped pairs exposed as "@N" aliases (BTC spot = UBTC/USDC,
     "@142"; HFUN/USDC, "@1"). *)
  {|{
     "tokens": [
       {"name":"USDC","index":0,"szDecimals":8},
       {"name":"PURR","index":1,"szDecimals":8},
       {"name":"HFUN","index":2,"szDecimals":8},
       {"name":"UBTC","index":197,"szDecimals":8}
     ],
     "universe": [
       {"name":"PURR/USDC","tokens":[1,0],"index":0,"isCanonical":true},
       {"name":"@1","tokens":[2,0],"index":1,"isCanonical":false},
       {"name":"@142","tokens":[197,0],"index":142,"isCanonical":false}
     ]
   }|}
;;

let pairs_fixture () =
  let json = Yojson.Safe.from_string (meta_fixture ()) in
  Dio_oracle.Oracle_fetch_hyperliquid.spot_meta_pairs_of_json json
;;

let test_spot_meta_pairs () =
  let pairs = pairs_fixture () in
  (* The canonical pair maps to its own name; wrapped pairs map to the "@N"
     alias that candleSnapshot accepts (the universe "name" field). *)
  Alcotest.(check (list (pair string string)))
    "feed symbol -> candle coin"
    [ "PURR/USDC", "PURR/USDC"; "HFUN/USDC", "@1"; "BTC/USDC", "@142" ]
    pairs
;;

let test_coin_of_symbol_perp () =
  let pairs = pairs_fixture () in
  (* Bare coin names are perpetuals: the coin is used as-is. *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC" in
  Alcotest.(check (option string)) "bare coin is a perp" (Some "BTC") c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs " sol " in
  Alcotest.(check (option string)) "bare coin trimmed/upper" (Some "SOL") c
;;

let test_coin_of_symbol_spot () =
  let pairs = pairs_fixture () in
  (* Named spot pair resolves to its exact candle coin. *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "PURR/USDC" in
  Alcotest.(check (option string)) "named spot pair" (Some "PURR/USDC") c;
  (* "USD" quote is normalized to the Hyperliquid spot quote "USDC". *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "PURR/USD" in
  Alcotest.(check (option string)) "USD quote normalized" (Some "PURR/USDC") c;
  (* Wrapped majors resolve through the feed-style key to the "@N" alias, so
     spot history is used instead of leaving the asset inactive. *)
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC/USDC" in
  Alcotest.(check (option string)) "wrapped major -> @N" (Some "@142") c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "BTC/USD" in
  Alcotest.(check (option string)) "wrapped major USD quote" (Some "@142") c
;;

let test_coin_of_symbol_no_spot_pair () =
  let pairs = pairs_fixture () in
  (* Symbols that are not a Hyperliquid spot pair have no spot history: they
     resolve to None, never to a perpetual proxy. The raw wrapped token name
     ("UBTC/USDC") is not the feed-style key either - configs use the
     canonical base ("BTC/USDC"), same as the instruments feed. *)
  let cases = [ "XRP/USDC"; "LINK/USD"; "UBTC/USDC"; "BTC/USDT" ] in
  List.iter
    (fun symbol ->
       let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs symbol in
       Alcotest.(check (option string))
         (Printf.sprintf "%s is not a spot pair" symbol)
         None
         c)
    cases
;;

let test_coin_of_symbol_edge_cases () =
  let pairs = pairs_fixture () in
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "" in
  Alcotest.(check (option string)) "empty symbol" None c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "purr/usdc" in
  Alcotest.(check (option string)) "lowercase named pair" (Some "PURR/USDC") c;
  let c = Dio_oracle.Oracle_fetch_hyperliquid.coin_of_symbol ~pairs "btc/usdc" in
  Alcotest.(check (option string)) "lowercase wrapped major" (Some "@142") c
;;

let () =
  Alcotest.run
    "oracle_fetch_hyperliquid"
    [ ( "spot resolution"
      , [ Alcotest.test_case
            "feed symbol mapping from spotMeta"
            `Quick
            test_spot_meta_pairs
        ; Alcotest.test_case "perp coins pass through" `Quick test_coin_of_symbol_perp
        ; Alcotest.test_case "spot pairs resolve" `Quick test_coin_of_symbol_spot
        ; Alcotest.test_case
            "non-spot-pair symbols return None"
            `Quick
            test_coin_of_symbol_no_spot_pair
        ; Alcotest.test_case "edge cases" `Quick test_coin_of_symbol_edge_cases
        ] )
    ]
;;
