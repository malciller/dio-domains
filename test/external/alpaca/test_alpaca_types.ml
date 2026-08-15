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

(* ── 24/5 schedule predicate (display-side session window) ────────────────── *)

(** The 24/5 schedule (Sun 8:00 PM ET - Fri 8:00 PM ET) must hold regardless
    of paper mode: a paper account accepts orders 24/7, but the dashboard's
    paused status has to follow the equity market's session window. *)
let test_schedule_boundaries () =
  let open Alpaca.Market_hours in
  let check_at desc wday hour expected =
    Alcotest.(check bool) desc expected (is_schedule_open_at wday hour)
  in
  (* Sunday: closed before 8 PM ET, open from 8 PM ET. *)
  check_at "Sun 6pm closed" 0 18 false;
  check_at "Sun 7pm closed" 0 19 false;
  check_at "Sun 8pm open" 0 20 true;
  check_at "Sun 11pm open" 0 23 true;
  (* Mon-Thu: open all day. *)
  check_at "Mon 2am open" 1 2 true;
  check_at "Mon 10am open" 1 10 true;
  check_at "Tue 5pm open" 2 17 true;
  check_at "Wed 3pm open" 3 15 true;
  check_at "Thu 1pm open" 4 13 true;
  (* Friday: open before 8 PM ET, closed at/after 8 PM ET. *)
  check_at "Fri 7pm open" 5 19 true;
  check_at "Fri 8pm closed" 5 20 false;
  check_at "Fri 11pm closed" 5 23 false;
  (* Saturday: closed all day. *)
  check_at "Sat midnight closed" 6 0 false;
  check_at "Sat noon closed" 6 12 false;
  check_at "Sat 11pm closed" 6 23 false
;;

let test_schedule_matches_live_market_open () =
  (* In live mode the operational gate and the session window agree; the two
     only diverge in paper mode (where the gate is forced open 24/7). *)
  Alpaca.Module.Config.set_testnet false;
  Alcotest.(check bool)
    "live market open follows schedule"
    (Alpaca.Market_hours.is_market_open ())
    (Alpaca.Market_hours.is_schedule_open ());
  restore_defaults ()
;;

(* ── effective_tif_and_extended (session-aware TIF) ──────────────────────── *)

let tif
      ?(crypto = false)
      ?(fractional = false)
      ?(order_type = "limit")
      ?(time_in_force = Some "GTC")
      ?(in_extended = false)
      ?(use_extended = true)
      ()
  =
  Alpaca.Rest.effective_tif_and_extended
    ~is_crypto:crypto
    ~is_fractional:fractional
    ~order_type
    ~time_in_force
    ~in_extended_session:in_extended
    ~use_extended
;;

let test_regular_session_gtc () =
  let tif_str, ext = tif () in
  Alcotest.(check string) "regular GTC stays gtc" "gtc" tif_str;
  Alcotest.(check bool) "regular GTC no extended flag" false ext
;;

let test_regular_session_ioc () =
  let tif_str, ext = tif ~time_in_force:(Some "IOC") () in
  Alcotest.(check string) "regular IOC stays ioc" "ioc" tif_str;
  Alcotest.(check bool) "regular IOC no extended flag" false ext
;;

let test_regular_session_fok () =
  let tif_str, ext = tif ~time_in_force:(Some "FOK") () in
  Alcotest.(check string) "regular FOK stays fok" "fok" tif_str;
  Alcotest.(check bool) "regular FOK no extended flag" false ext
;;

let test_regular_session_day () =
  let tif_str, ext = tif ~time_in_force:(Some "DAY") () in
  Alcotest.(check string) "regular DAY stays day" "day" tif_str;
  Alcotest.(check bool) "regular DAY no extended flag" false ext
;;

let test_regular_session_default_fractional () =
  let tif_str, ext = tif ~fractional:true ~time_in_force:None () in
  Alcotest.(check string) "regular fractional default day" "day" tif_str;
  Alcotest.(check bool) "regular fractional no extended flag" false ext
;;

let test_regular_session_fractional_gtc () =
  let tif_str, ext = tif ~fractional:true ~time_in_force:(Some "GTC") () in
  Alcotest.(check string) "regular fractional GTC forced to day" "day" tif_str;
  Alcotest.(check bool) "regular fractional GTC no extended flag" false ext
;;

let test_regular_session_fractional_ioc () =
  let tif_str, ext = tif ~fractional:true ~time_in_force:(Some "IOC") () in
  Alcotest.(check string) "regular fractional IOC forced to day" "day" tif_str;
  Alcotest.(check bool) "regular fractional IOC no extended flag" false ext
;;

let test_regular_session_default_whole () =
  let tif_str, ext = tif ~time_in_force:None () in
  Alcotest.(check string) "regular whole default gtc" "gtc" tif_str;
  Alcotest.(check bool) "regular whole no extended flag" false ext
;;

let test_extended_session_gtc_downgraded () =
  let tif_str, ext = tif ~in_extended:true () in
  Alcotest.(check string) "extended GTC downgraded to day" "day" tif_str;
  Alcotest.(check bool) "extended GTC marked extended" true ext
;;

let test_extended_session_ioc_downgraded () =
  let tif_str, ext = tif ~in_extended:true ~time_in_force:(Some "IOC") () in
  Alcotest.(check string) "extended IOC downgraded to day" "day" tif_str;
  Alcotest.(check bool) "extended IOC marked extended" true ext
;;

let test_extended_session_fok_downgraded () =
  let tif_str, ext = tif ~in_extended:true ~time_in_force:(Some "FOK") () in
  Alcotest.(check string) "extended FOK downgraded to day" "day" tif_str;
  Alcotest.(check bool) "extended FOK marked extended" true ext
;;

let test_extended_session_day () =
  let tif_str, ext = tif ~in_extended:true ~time_in_force:(Some "DAY") () in
  Alcotest.(check string) "extended DAY stays day" "day" tif_str;
  Alcotest.(check bool) "extended DAY marked extended" true ext
;;

let test_extended_session_market_order_not_marked () =
  let tif_str, ext = tif ~in_extended:true ~order_type:"market" () in
  Alcotest.(check string) "extended market keeps gtc" "gtc" tif_str;
  Alcotest.(check bool) "extended market not marked" false ext
;;

let test_extended_session_disabled () =
  let tif_str, ext = tif ~in_extended:true ~use_extended:false () in
  Alcotest.(check string) "extended trading disabled keeps gtc" "gtc" tif_str;
  Alcotest.(check bool) "extended trading disabled no flag" false ext
;;

let test_crypto_never_extended () =
  let tif_str, ext = tif ~crypto:true ~in_extended:true () in
  Alcotest.(check string) "crypto keeps gtc" "gtc" tif_str;
  Alcotest.(check bool) "crypto not marked extended" false ext
;;

let test_crypto_default () =
  let tif_str, _ext = tif ~crypto:true ~fractional:true ~time_in_force:None () in
  Alcotest.(check string) "crypto default gtc" "gtc" tif_str
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
    ; ( "24/5 schedule"
      , [ Alcotest.test_case
            "schedule boundaries (Sun 8pm - Fri 8pm)"
            `Quick
            test_schedule_boundaries
        ; Alcotest.test_case
            "live market open follows schedule"
            `Quick
            test_schedule_matches_live_market_open
        ] )
    ; ( "session TIF"
      , [ Alcotest.test_case "regular GTC" `Quick test_regular_session_gtc
        ; Alcotest.test_case "regular IOC" `Quick test_regular_session_ioc
        ; Alcotest.test_case "regular FOK" `Quick test_regular_session_fok
        ; Alcotest.test_case "regular DAY" `Quick test_regular_session_day
        ; Alcotest.test_case
            "regular default fractional"
            `Quick
            test_regular_session_default_fractional
        ; Alcotest.test_case
            "regular fractional GTC"
            `Quick
            test_regular_session_fractional_gtc
        ; Alcotest.test_case
            "regular fractional IOC"
            `Quick
            test_regular_session_fractional_ioc
        ; Alcotest.test_case
            "regular default whole"
            `Quick
            test_regular_session_default_whole
        ; Alcotest.test_case
            "extended GTC downgraded"
            `Quick
            test_extended_session_gtc_downgraded
        ; Alcotest.test_case
            "extended IOC downgraded"
            `Quick
            test_extended_session_ioc_downgraded
        ; Alcotest.test_case
            "extended FOK downgraded"
            `Quick
            test_extended_session_fok_downgraded
        ; Alcotest.test_case "extended DAY" `Quick test_extended_session_day
        ; Alcotest.test_case
            "extended market order"
            `Quick
            test_extended_session_market_order_not_marked
        ; Alcotest.test_case "extended disabled" `Quick test_extended_session_disabled
        ; Alcotest.test_case "crypto never extended" `Quick test_crypto_never_extended
        ; Alcotest.test_case "crypto default" `Quick test_crypto_default
        ] )
    ]
;;
