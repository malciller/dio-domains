(* Holdings pause-state tests: the dashboard's paused status must reflect the
   capital oracle's INACTIVE verdict (the oracle-paused state), not just the
   grid's internal capital-low flag - an asset the oracle says cannot fund
   its first buy is paused even when the grid state is quiet. *)

let strategy_json ?(oracle = `Null) ?(capital_low = false) ?(market_closed = false) () =
  `Assoc
    [ "exchange", `String "hyperliquid"
    ; ( "strategy"
      , `Assoc
          [ "type", `String "Ladder"
          ; "capital_low", `Bool capital_low
          ; "market_is_closed", `Bool market_closed
          ] )
    ; "oracle", oracle
    ]
;;

let oracle_json active = `Assoc [ "active", `Bool active; "reason", `String "test" ]

let test_oracle_inactive () =
  (* No oracle decision yet (before the first pass): not oracle-paused. *)
  Alcotest.(check bool)
    "no decision -> not oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive (strategy_json ()))
    false;
  (* Oracle says ACTIVE -> not paused. *)
  Alcotest.(check bool)
    "active decision -> not oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive (strategy_json ~oracle:(oracle_json true) ()))
    false;
  (* Oracle says INACTIVE -> paused. *)
  Alcotest.(check bool)
    "inactive decision -> oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive (strategy_json ~oracle:(oracle_json false) ()))
    true;
  (* A balance (non-strategy) entry has no oracle field at all. *)
  Alcotest.(check bool)
    "balance entry -> not oracle-paused"
    (Dashboard_ui.Holdings.oracle_inactive
       (`Assoc [ "asset", `String "X"; "balance", `Float 1.0 ]))
    false
;;

let test_strategy_paused () =
  (* Paused = oracle INACTIVE, or the grid's capital-low flag, or the market
     closed. *)
  Alcotest.(check bool)
    "quiet active grid -> running"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ()))
    false;
  Alcotest.(check bool)
    "capital-low grid -> paused"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~capital_low:true ()))
    true;
  Alcotest.(check bool)
    "oracle-INACTIVE grid -> paused (the fix)"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~oracle:(oracle_json false) ()))
    true;
  Alcotest.(check bool)
    "market closed -> paused"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~market_closed:true ()))
    true;
  Alcotest.(check bool)
    "oracle-ACTIVE grid -> running"
    (Dashboard_ui.Holdings.strategy_paused (strategy_json ~oracle:(oracle_json true) ()))
    false
;;

let test_latency_format () =
  (* Sub-microsecond latencies must render at nanosecond level. *)
  Alcotest.(check string) "500ns" "500ns" (Dashboard_ui.Theme.format_latency_us 0.5);
  Alcotest.(check string) "123ns" "123ns" (Dashboard_ui.Theme.format_latency_us 0.1234);
  Alcotest.(check string) "1ns" "1ns" (Dashboard_ui.Theme.format_latency_us 0.001);
  (* Microsecond and above keep their existing scales. *)
  Alcotest.(check string) "2us" "2µs" (Dashboard_ui.Theme.format_latency_us 1.5);
  Alcotest.(check string) "50us" "50µs" (Dashboard_ui.Theme.format_latency_us 50.0);
  Alcotest.(check string) "2.5ms" "2.5ms" (Dashboard_ui.Theme.format_latency_us 2500.0);
  Alcotest.(check string) "2.0s" "2.0s" (Dashboard_ui.Theme.format_latency_us 2_000_000.0);
  Alcotest.(check string) "zero" "0µs" (Dashboard_ui.Theme.format_latency_us 0.0);
  (* The sub-microsecond predicate drives the dark-green ns styling: only
     nonzero values below 1us count (zero reads as idle, not ns). *)
  Alcotest.(check bool) "500ns is sub-us" true (Dashboard_ui.Theme.is_sub_us 0.5);
  Alcotest.(check bool) "0.999us is sub-us" true (Dashboard_ui.Theme.is_sub_us 0.999);
  Alcotest.(check bool) "1us is not sub-us" false (Dashboard_ui.Theme.is_sub_us 1.0);
  Alcotest.(check bool) "2.5us is not sub-us" false (Dashboard_ui.Theme.is_sub_us 2.5);
  Alcotest.(check bool) "zero is not sub-us" false (Dashboard_ui.Theme.is_sub_us 0.0)
;;

(** One latency window fixture (fresh relative to the snapshot timestamp). *)
let latency_snapshot () =
  `Assoc
    [ "name", `String "test"
    ; "p50", `Float 1.0
    ; "p90", `Float 3.0
    ; "p99", `Float 5.0
    ; "p999", `Float 9.0
    ; "samples", `Int 10
    ; "sub_us_samples", `Int 0
    ; "overflow", `Int 0
    ; "executions", `Int 10
    ; "executions_per_sec", `Float 2.0
    ; "last_exec_time", `Float 999.0
    ; "window_start", `Float 900.0
    ; "window_end", `Float 999.0
    ; "max_cause", `Null
    ]
;;

let latency_json () =
  let snap = latency_snapshot () in
  let domain_lats =
    [ "oracle", snap
    ; "orderbook", snap
    ; "strategy", snap
    ; "execution", snap
    ; "cycle", snap
    ]
  in
  `Assoc
    [ "timestamp", `Float 1000.0
    ; ( "strategies"
      , `Assoc
          [ "BTC/USDC", `Assoc [ "exchange", `String "hyperliquid" ]
          ; "ETH/USDC", `Assoc [ "exchange", `String "hyperliquid" ]
          ] )
    ; ( "latencies"
      , `Assoc [ "BTC/USDC", `Assoc domain_lats; "ETH/USDC", `Assoc domain_lats ] )
    ]
;;

let test_latency_pages () =
  let open Dashboard_ui in
  (* Default view is the CORE page; the section has room for the future
     network metrics without widening the table. *)
  Alcotest.(check int) "starts on CORE page" 0 (Latencies.current_page_index ());
  Alcotest.(check int) "two latency pages" 2 (Latencies.page_count ());
  (* CORE merges the pipeline stages AND the full cycle span. *)
  Alcotest.(check (list string))
    "CORE columns"
    [ "oracle"; "orderbook"; "strategy"; "execution"; "cycle" ]
    (Latencies.page_metrics 0);
  let net = Latencies.page_metrics 1 in
  Alcotest.(check bool)
    "NETWORK carries network labels"
    (List.mem "ws_ping" net
     && List.mem "ws_feed" net
     && List.mem "rest_request" net
     && List.mem "signer" net)
    true;
  (* Every page's trend header must fit the trend column. A longer label
     would silently overrun the fixed-width column and shift every border
     to its right out of alignment (regression: "(ORACLE P99)" is 12 chars
     in an 11-wide column). *)
  for i = 0 to Latencies.page_count () - 1 do
    Alcotest.(check bool)
      (Printf.sprintf "page %d trend label fits the trend column" i)
      (String.length (Latencies.page_trend_label i) <= Latencies.trend_col_w)
      true
  done;
  (* Navigation wraps in both directions. *)
  Latencies.next_page ();
  Alcotest.(check int) "next -> NETWORK" 1 (Latencies.current_page_index ());
  Latencies.next_page ();
  Alcotest.(check int) "next wraps to CORE" 0 (Latencies.current_page_index ());
  Latencies.prev_page ();
  Alcotest.(check int) "prev wraps to NETWORK" 1 (Latencies.current_page_index ());
  Latencies.set_page 0
;;

let test_latency_page_render () =
  let open Dashboard_ui in
  let json = latency_json () in
  (* Rows stay visible across pages (freshness is checked across all pages),
     so the not-yet-instrumented NETWORK page still renders cleanly instead
     of blanking out. *)
  Latencies.set_page 0;
  Alcotest.(check bool)
    "CORE page renders"
    (Notty.I.height (Latencies.render_latencies 180 json) > 0)
    true;
  Alcotest.(check bool)
    "CORE page renders compact"
    (Notty.I.height (Latencies.render_latencies 100 json) > 0)
    true;
  Latencies.set_page 1;
  Alcotest.(check bool)
    "NETWORK page renders"
    (Notty.I.height (Latencies.render_latencies 180 json) > 0)
    true;
  Latencies.set_page 0;
  (* A snapshot with no latency map renders nothing. *)
  Alcotest.(check bool)
    "no latencies renders empty"
    (Notty.I.height (Latencies.render_latencies 180 (`Assoc [ "timestamp", `Float 1.0 ]))
     = 0)
    true
;;

let test_network_latency () =
  (* Recording network measurements and publishing windows produces the
     NETWORK-page labels (ws_ping / ws_feed / rest_request / signer) with
     the recorded distributions. *)
  Network_latency.record_ping_s "kraken" 0.005;
  Network_latency.record_ping_s "kraken" 0.009;
  Network_latency.record_feed_s "kraken" 0.05;
  Network_latency.record_rest_s "kraken" 0.12;
  Network_latency.record_signer "hyperliquid" (Mtime.Span.of_uint64_ns 1_500L);
  Network_latency.publish_all ();
  let snaps = Network_latency.snapshots "kraken" in
  (match List.assoc_opt "ws_ping" snaps with
   | Some (Some snap) ->
     Alcotest.(check int) "ping window has both samples" 2 snap.samples;
     Alcotest.(check bool) "ping p50 in measured range" (snap.p50 > 4000.0) true
   | _ -> Alcotest.fail "missing ws_ping snapshot");
  (match List.assoc_opt "ws_feed" snaps with
   | Some (Some snap) -> Alcotest.(check int) "feed recorded" 1 snap.samples
   | _ -> Alcotest.fail "missing ws_feed snapshot");
  (match List.assoc_opt "rest_request" snaps with
   | Some (Some snap) -> Alcotest.(check int) "rest recorded" 1 snap.samples
   | _ -> Alcotest.fail "missing rest_request snapshot");
  (match List.assoc_opt "signer" snaps with
   | Some snap -> Alcotest.(check bool) "signer label present" (Option.is_some snap) true
   | None -> Alcotest.fail "missing signer label");
  (* A venue with no measurements has no windows at all. *)
  Alcotest.(check bool)
    "unmeasured venue has no snapshots"
    (Network_latency.snapshots "ibkr" = [])
    true
;;

(** A single ws_ping window: fresh (with samples) or idle (no samples). *)
let ping_win ~fresh =
  if fresh
  then
    `Assoc
      [ "name", `String "t"
      ; "p50", `Float 5_000.0
      ; "p90", `Float 7_000.0
      ; "p99", `Float 9_000.0
      ; "p999", `Float 15_000.0
      ; "samples", `Int 10
      ; "sub_us_samples", `Int 0
      ; "overflow", `Int 0
      ; "executions", `Int 10
      ; "executions_per_sec", `Float 2.0
      ; "last_exec_time", `Float 999.0
      ; "window_start", `Float 900.0
      ; "window_end", `Float 999.0
      ; "max_cause", `Null
      ]
  else
    `Assoc
      [ "name", `String "t"
      ; "p50", `Float 0.0
      ; "p90", `Float 0.0
      ; "p99", `Float 0.0
      ; "p999", `Float 0.0
      ; "samples", `Int 0
      ; "sub_us_samples", `Int 0
      ; "overflow", `Int 0
      ; "executions", `Int 0
      ; "executions_per_sec", `Float 0.0
      ; "last_exec_time", `Float 0.0
      ; "window_start", `Float 900.0
      ; "window_end", `Float 999.0
      ; "max_cause", `Null
      ]
;;

let ping_json symbol ~fresh =
  `Assoc
    [ "timestamp", `Float 1000.0
    ; "strategies", `Assoc [ symbol, `Assoc [ "exchange", `String "hyperliquid" ] ]
    ; "latencies", `Assoc [ symbol, `Assoc [ "ws_ping", ping_win ~fresh ] ]
    ]
;;

let render_to_text img =
  let buf = Buffer.create 65536 in
  Notty.Render.to_buffer buf Notty.Cap.ansi (0, 0) (180, 20) img;
  Buffer.contents buf
;;

let contains_sub str sub =
  let len_s = String.length str in
  let len_sub = String.length sub in
  if len_sub = 0
  then true
  else if len_s < len_sub
  then false
  else (
    let found = ref false in
    let i = ref 0 in
    while !i <= len_s - len_sub && not !found do
      if String.sub str !i len_sub = sub then found := true else incr i
    done;
    !found)
;;

let test_latency_persistence () =
  let open Dashboard_ui in
  Latencies.set_page 1;
  (* Fresh window populates the persistence cache. *)
  ignore (Latencies.render_latencies 180 (ping_json "AAA/USDC" ~fresh:true));
  (* An idle window for the same symbol keeps the last measured values on
     screen instead of reverting to "idle" (short-lived metrics like ping
     stay visible between windows). *)
  let persisted =
    render_to_text (Latencies.render_latencies 180 (ping_json "AAA/USDC" ~fresh:false))
  in
  Alcotest.(check bool)
    "idle window persists the last value"
    (contains_sub persisted "5.0ms")
    true;
  Alcotest.(check bool)
    "persisted window no longer shows idle"
    (not (contains_sub persisted "idle"))
    true;
  (* A symbol that was never measured still shows idle. *)
  let never =
    render_to_text (Latencies.render_latencies 180 (ping_json "BBB/USDC" ~fresh:false))
  in
  Alcotest.(check bool)
    "unmeasured symbol still shows idle"
    (contains_sub never "idle")
    true;
  Latencies.set_page 0
;;

let () =
  Alcotest.run
    "dashboard_holdings"
    [ ( "pause state"
      , [ Alcotest.test_case
            "oracle verdict drives the paused state"
            `Quick
            test_oracle_inactive
        ; Alcotest.test_case
            "paused = oracle OR capital-low OR market closed"
            `Quick
            test_strategy_paused
        ] )
    ; ( "latency formatting"
      , [ Alcotest.test_case
            "sub-microsecond latencies display at ns level"
            `Quick
            test_latency_format
        ] )
    ; ( "latency pages"
      , [ Alcotest.test_case "page navigation wraps both ways" `Quick test_latency_pages
        ; Alcotest.test_case "every page renders rows" `Quick test_latency_page_render
        ] )
    ; ( "network latency"
      , [ Alcotest.test_case
            "network windows publish under the NETWORK-page labels"
            `Quick
            test_network_latency
        ] )
    ; ( "latency persistence"
      , [ Alcotest.test_case
            "idle windows keep the last measured value"
            `Quick
            test_latency_persistence
        ] )
    ]
;;
