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

(* ── market hours: the 24/5 calendar closes over the weekend in every mode ── *)

(* Fixed UTC instants whose US/Eastern conversion is stable (August 2026,
   EDT = UTC-4): Fri Aug 21, Sat Aug 22, Sun Aug 23, Wed Aug 26. The clock
   seam makes the session boundaries deterministic regardless of when the
   suite runs. *)
let fri_1959_et = 1787356740.0
let fri_2001_et = 1787356860.0
let sat_noon_et = 1787414400.0
let sun_1959_et = 1787529540.0
let sun_2001_et = 1787529660.0
let wed_0300_et = 1787727600.0

let with_frozen_clock t f =
  Alpaca.Module.Config.set_testnet true;
  Alpaca.Market_hours.now_override := Some t;
  Fun.protect f ~finally:(fun () ->
    Alpaca.Market_hours.now_override := None;
    restore_defaults ())
;;

let test_weekend_closed_in_paper_mode () =
  with_frozen_clock sat_noon_et (fun () ->
    Alcotest.(check bool)
      "Saturday noon ET closed"
      false
      (Alpaca.Market_hours.is_market_open ());
    let secs = Alpaca.Market_hours.seconds_until_next_open () in
    (* Reopens Sunday 8:00 PM ET - 32 hours after Saturday noon ET. *)
    Alcotest.(check (float 120.0))
      "weekend reopen is Sunday 8 PM ET (~32h)"
      (32.0 *. 3600.0)
      secs)
;;

let test_friday_close_boundary () =
  with_frozen_clock fri_1959_et (fun () ->
    Alcotest.(check bool)
      "Friday 19:59 ET still open"
      true
      (Alpaca.Market_hours.is_market_open ()));
  with_frozen_clock fri_2001_et (fun () ->
    Alcotest.(check bool)
      "Friday 20:01 ET closed for the weekend"
      false
      (Alpaca.Market_hours.is_market_open ()))
;;

let test_sunday_reopen_boundary () =
  with_frozen_clock sun_1959_et (fun () ->
    Alcotest.(check bool)
      "Sunday 19:59 ET still closed"
      false
      (Alpaca.Market_hours.is_market_open ());
    let secs = Alpaca.Market_hours.seconds_until_next_open () in
    Alcotest.(check (float 30.0)) "reopens at Sunday 8:00 PM ET" 60.0 secs);
  with_frozen_clock sun_2001_et (fun () ->
    Alcotest.(check bool)
      "Sunday 20:01 ET open (overnight)"
      true
      (Alpaca.Market_hours.is_market_open ());
    Alcotest.(check (float 0.0))
      "no reopen wait while open"
      0.0
      (Alpaca.Market_hours.seconds_until_next_open ()))
;;

let test_weekday_overnight_open () =
  with_frozen_clock wed_0300_et (fun () ->
    Alcotest.(check bool)
      "Wednesday 03:00 ET open (24/5 overnight)"
      true
      (Alpaca.Market_hours.is_market_open ()))
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
      , [ Alcotest.test_case "flips is_paper" `Quick test_set_testnet_flips_is_paper ] )
    ; ( "market hours"
      , [ Alcotest.test_case
            "weekend closed in paper mode"
            `Quick
            test_weekend_closed_in_paper_mode
        ; Alcotest.test_case "Friday close boundary" `Quick test_friday_close_boundary
        ; Alcotest.test_case "Sunday reopen boundary" `Quick test_sunday_reopen_boundary
        ; Alcotest.test_case "weekday overnight open" `Quick test_weekday_overnight_open
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
