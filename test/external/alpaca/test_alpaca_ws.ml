(* Tests for Alpaca WebSocket feed/ping wiring:
   - send_ping returns false when no connection is established (both feeds)
   - venue profiler wiring: ping/feed recordings land under the "alpaca"
     venue so the dashboard NETWORK page can render them *)

let test_data_feed_send_ping_disconnected () =
  let result = Lwt_main.run (Alpaca.Orderbook.send_ping ~req_id:1 ~timeout_ms:100) in
  Alcotest.(check bool) "data feed ping not connected" false result
;;

let test_trading_feed_send_ping_disconnected () =
  let result = Lwt_main.run (Alpaca.Executions.send_ping ~req_id:2 ~timeout_ms:100) in
  Alcotest.(check bool) "trading feed ping not connected" false result
;;

(** Publishes the current live window, then returns the published ws_ping /
    ws_feed snapshots for the "alpaca" venue, or [None] when absent. *)
let published_venue_snapshot label =
  Network_latency.publish_all ();
  let snaps = Network_latency.snapshots "alpaca" in
  match List.assoc_opt label snaps with
  | Some (Some snap) -> Some snap
  | _ -> None
;;

let test_ping_records_under_alpaca_venue () =
  Network_latency.record_ping_s "alpaca" 0.05;
  match published_venue_snapshot "ws_ping" with
  | Some snap ->
    Alcotest.(check int) "ping window has one sample" 1 snap.Latency_profiler.samples
  | None -> Alcotest.fail "ws_ping snapshot missing for alpaca venue"
;;

let test_feed_records_under_alpaca_venue () =
  Network_latency.record_feed_s "alpaca" 0.02;
  match published_venue_snapshot "ws_feed" with
  | Some snap ->
    Alcotest.(check int) "feed window has one sample" 1 snap.Latency_profiler.samples
  | None -> Alcotest.fail "ws_feed snapshot missing for alpaca venue"
;;

let test_venue_labels_present () =
  let labels =
    Network_latency.snapshots "alpaca" |> List.map fst |> List.sort_uniq String.compare
  in
  let expected = [ "rest_request"; "signer"; "ws_feed"; "ws_ping" ] in
  Alcotest.(check (list string)) "all four NETWORK labels" expected labels
;;

let () =
  Alcotest.run
    "alpaca_ws"
    [ ( "ping"
      , [ Alcotest.test_case
            "data feed ping without connection returns false"
            `Quick
            test_data_feed_send_ping_disconnected
        ; Alcotest.test_case
            "trading feed ping without connection returns false"
            `Quick
            test_trading_feed_send_ping_disconnected
        ; Alcotest.test_case
            "ping latency records under alpaca venue"
            `Quick
            test_ping_records_under_alpaca_venue
        ] )
    ; ( "feed"
      , [ Alcotest.test_case
            "feed latency records under alpaca venue"
            `Quick
            test_feed_records_under_alpaca_venue
        ; Alcotest.test_case
            "all four NETWORK labels present"
            `Quick
            test_venue_labels_present
        ] )
    ]
;;
