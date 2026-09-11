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

(* ── Events API (SSE) ingestion ────────────────────────────────────────── *)

let contains haystack needle =
  let n = String.length needle in
  let h = String.length haystack in
  let rec go i =
    i + n <= h && (String.equal (String.sub haystack i n) needle || go (i + 1))
  in
  n = 0 || go 0
;;

(** A top-level TradeUpdateEventV2 fill (no legacy stream/data wrapper) must
    land in the execution store, and its ULID must become the resume cursor so
    a reconnect issues since_id instead of replaying from scratch. *)
let test_v2_fill_ingested_and_cursor_set () =
  let symbol = "EVT_FILL/USD" in
  let json =
    Yojson.Safe.from_string
      {|{
        "event": "fill",
        "event_id": "01G112NTT0XAXKDZK3AABK68TH",
        "at": "2025-01-14T16:05:51.872012Z",
        "execution_id": "ccf7d1dc-78e1-4eb5-92c6-5c86b2bcca8f",
        "price": "0.07",
        "qty": "1",
        "position_qty": "1",
        "order": {
          "id": "edada91a-8b55-4916-a153-8c7a9817e708",
          "client_order_id": "12345",
          "symbol": "EVT_FILL/USD",
          "side": "buy",
          "status": "filled",
          "qty": "1",
          "filled_qty": "1",
          "limit_price": "0.05",
          "filled_avg_price": "0.07"
        }
      }|}
  in
  let before = Alpaca.Executions.get_current_position symbol in
  Alpaca.Executions.handle_trade_update json;
  let after = Alpaca.Executions.get_current_position symbol in
  Alcotest.(check int) "fill appended to the exec store" (before + 1) after;
  let uri = Uri.to_string (Alpaca.Executions.events_uri ()) in
  Alcotest.(check bool)
    "reconnect resumes from the last event id"
    true
    (contains uri "since_id=01G112NTT0XAXKDZK3AABK68TH")
;;

(** A trade_bust reverses a prior execution and is not modeled by the fill
    ledger: it must be surfaced but must NOT be appended as a fill. *)
let test_v2_bust_not_ingested () =
  let symbol = "EVT_BUST/USD" in
  let json =
    Yojson.Safe.from_string
      {|{
        "event": "trade_bust",
        "event_id": "01G112NTT0XAXKDZK3AABK68TJ",
        "previous_execution_id": "ccf7d1dc-78e1-4eb5-92c6-5c86b2bcca8f",
        "qty": "1",
        "order": {
          "id": "bust-order",
          "symbol": "EVT_BUST/USD",
          "side": "buy",
          "status": "filled",
          "qty": "1",
          "filled_qty": "1"
        }
      }|}
  in
  Alpaca.Executions.handle_trade_update json;
  Alcotest.(check int)
    "bust is not appended as a fill"
    0
    (Alpaca.Executions.get_current_position symbol)
;;

(** The SSE frame reader must split a multi-frame body into events and count
    comment heartbeats, independent of the HTTP transport. *)
let test_sse_stream_parses_frames () =
  let symbol = "EVT_SSE/USD" in
  let body =
    Cohttp_lwt.Body.of_string
      (": connected\n\n"
       ^ "data: \
          {\"event\":\"new\",\"event_id\":\"01A\",\"order\":{\"id\":\"o1\",\"symbol\":\"EVT_SSE/USD\",\"side\":\"buy\",\"status\":\"new\",\"qty\":\"1\",\"filled_qty\":\"0\",\"limit_price\":\"10\"}}\n\n"
       ^ ": heartbeat\n\n"
       ^ "data: \
          {\"event\":\"fill\",\"event_id\":\"01B\",\"price\":\"10.5\",\"order\":{\"id\":\"o1\",\"symbol\":\"EVT_SSE/USD\",\"side\":\"buy\",\"status\":\"filled\",\"qty\":\"1\",\"filled_qty\":\"1\",\"limit_price\":\"10\"}}\n\n"
      )
  in
  let heartbeats = ref 0 in
  Lwt_main.run
    (Alpaca.Executions.consume_event_stream
       ~on_heartbeat:(fun () -> incr heartbeats)
       body);
  Alcotest.(check int)
    "both frames appended to the exec store"
    2
    (Alpaca.Executions.get_current_position symbol);
  Alcotest.(check bool) "heartbeat lines observed" true (!heartbeats > 0)
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
    ; ( "events"
      , [ Alcotest.test_case
            "v2 fill ingested and resume cursor set"
            `Quick
            test_v2_fill_ingested_and_cursor_set
        ; Alcotest.test_case
            "v2 trade_bust not ingested as a fill"
            `Quick
            test_v2_bust_not_ingested
        ; Alcotest.test_case
            "SSE body splits into frames and counts heartbeats"
            `Quick
            test_sse_stream_parses_frames
        ] )
    ]
;;
