(* Tests for Alpaca config mode selection (paper vs live) and market hours. *)

let restore_defaults () =
  Alpaca.Module.Config.set_testnet true;
  Alpaca.Module.Config.set_data_feed "iex"
;;

let test_rest_paper_url () =
  Alpaca.Module.Config.set_testnet true;
  let url = Alpaca.Types.Config.rest_base_url () in
  Alcotest.(check string) "paper rest url" "https://paper-api.alpaca.markets" url;
  restore_defaults ()
;;

let test_rest_live_url () =
  Alpaca.Module.Config.set_testnet false;
  let url = Alpaca.Types.Config.rest_base_url () in
  Alcotest.(check string) "live rest url" "https://api.alpaca.markets" url;
  restore_defaults ()
;;

let test_trading_ws_paper_url () =
  Alpaca.Module.Config.set_testnet true;
  let url = Alpaca.Types.Config.trading_ws_url () in
  Alcotest.(check string)
    "paper trading ws url"
    "wss://paper-api.alpaca.markets/stream"
    url;
  restore_defaults ()
;;

let test_trading_ws_live_url () =
  Alpaca.Module.Config.set_testnet false;
  let url = Alpaca.Types.Config.trading_ws_url () in
  Alcotest.(check string) "live trading ws url" "wss://api.alpaca.markets/stream" url;
  restore_defaults ()
;;

let test_set_data_feed_iex () =
  Alpaca.Module.Config.set_data_feed "iex";
  Alcotest.(check string) "iex feed" "iex" !Alpaca.Types.Config.data_feed;
  restore_defaults ()
;;

let test_set_data_feed_sip () =
  Alpaca.Module.Config.set_data_feed "sip";
  Alcotest.(check string) "sip feed" "sip" !Alpaca.Types.Config.data_feed;
  restore_defaults ()
;;

let test_set_data_feed_garbage () =
  Alpaca.Module.Config.set_data_feed "bogus";
  Alcotest.(check string)
    "garbage feed normalizes to iex"
    "iex"
    !Alpaca.Types.Config.data_feed;
  restore_defaults ()
;;

let test_set_testnet_flips_is_paper () =
  Alpaca.Module.Config.set_testnet false;
  Alcotest.(check bool) "is_paper false for live" false !Alpaca.Types.Config.is_paper;
  Alpaca.Module.Config.set_testnet true;
  Alcotest.(check bool) "is_paper true for paper" true !Alpaca.Types.Config.is_paper;
  restore_defaults ()
;;

let test_set_testnet_flips_paper_mode () =
  Alpaca.Module.Config.set_testnet false;
  Alcotest.(check bool) "paper_mode false for live" false !Alpaca.Market_hours.paper_mode;
  Alpaca.Module.Config.set_testnet true;
  Alcotest.(check bool) "paper_mode true for paper" true !Alpaca.Market_hours.paper_mode;
  restore_defaults ()
;;

let test_paper_market_open_24_7 () =
  Alpaca.Module.Config.set_testnet true;
  Alcotest.(check bool)
    "paper market always open"
    true
    (Alpaca.Market_hours.is_market_open ());
  restore_defaults ()
;;

let test_paper_seconds_until_next_open_zero () =
  Alpaca.Module.Config.set_testnet true;
  Alcotest.(check (float 0.001))
    "paper never closes"
    0.0
    (Alpaca.Market_hours.seconds_until_next_open ());
  restore_defaults ()
;;

let () =
  Alcotest.run
    "alpaca"
    [ ( "rest urls"
      , [ Alcotest.test_case "paper" `Quick test_rest_paper_url
        ; Alcotest.test_case "live" `Quick test_rest_live_url
        ] )
    ; ( "trading ws urls"
      , [ Alcotest.test_case "paper" `Quick test_trading_ws_paper_url
        ; Alcotest.test_case "live" `Quick test_trading_ws_live_url
        ] )
    ; ( "data feed"
      , [ Alcotest.test_case "iex" `Quick test_set_data_feed_iex
        ; Alcotest.test_case "sip" `Quick test_set_data_feed_sip
        ; Alcotest.test_case "garbage normalizes to iex" `Quick test_set_data_feed_garbage
        ] )
    ; ( "set_testnet"
      , [ Alcotest.test_case "flips is_paper" `Quick test_set_testnet_flips_is_paper
        ; Alcotest.test_case "flips paper_mode" `Quick test_set_testnet_flips_paper_mode
        ] )
    ; ( "market hours"
      , [ Alcotest.test_case "paper mode always open" `Quick test_paper_market_open_24_7
        ; Alcotest.test_case
            "paper mode never waits for open"
            `Quick
            test_paper_seconds_until_next_open_zero
        ] )
    ]
;;
