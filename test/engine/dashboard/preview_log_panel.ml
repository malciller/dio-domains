(* Dev aid: render the ENGINE LOGS panel headlessly from a synthetic snapshot, so the
   panel can be eyeballed without a live engine: dune exec
   test/engine/dashboard/preview_flow_panel.exe *)

let entry seq level section message =
  `Assoc
    [ "seq", `Int seq
    ; "ts", `Float (12.0 +. (float_of_int seq *. 0.001))
    ; "level", `String level
    ; "section", `String section
    ; "message", `String message
    ]
;;

let () =
  let json =
    `Assoc
      [ "timestamp", `Float 100000.0
      ; "uptime_s", `Float 3600.0
      ; ( "logs"
        , `List
            [ entry 1 "DEBUG" "cache" "snapshot cache warm (entry 3/8)"
            ; entry
                2
                "INFO"
                "BTC/USDC"
                "↯ TICK  BTC/USDC · mid 67235.50 · +0.42bp ▲·book bid 67234.1×1.20 · ask \
                 67236.9×0.80·strat grid · qty 0.038772"
            ; entry
                3
                "INFO"
                "hyperliquid_ws"
                "feed connected (wss://api.hyperliquid.xyz/ws)"
            ; entry 4 "INFO" "oracle" "capital oracle pass: 8 assets analyzed in 42.1ms"
            ; entry
                5
                "WARN"
                "kraken_get_fee"
                "fee cache miss for ETH/USD; using default maker fee"
            ; entry 6 "INFO" "BTC/USDC" "▸ SEND  BUY 0.038772 @ 67230.5"
            ; entry 7 "INFO" "strategy" "grid materialized for BTC/USDC (gi 0.8000%)"
            ; entry
                8
                "ERROR"
                "order_executor"
                "place rejected for SOL/USDC: insufficient margin"
            ; entry
                9
                "INFO"
                "BTC/USDC"
                "▲ FILL  BTC/USDC 0.038772 @ 67231.0 filled (+1.8bp)"
            ; entry 10 "CRITICAL" "main" "SIGTERM received; beginning graceful shutdown"
            ; entry 11 "INFO" "pulse" "8 domains · 41.2k cycles/s"
            ; entry 12 "INFO" "dashboard_server" "Dashboard client connected (active: 1)"
            ] )
      ]
  in
  let snap = Dashboard_ui.Snapshot.of_json json in
  let w = 120 in
  let img = Dashboard_ui.Log_feed.render_logs w snap snap.Dashboard_ui.Snapshot.logs in
  let buf = Buffer.create 65536 in
  Notty.Render.to_buffer buf Notty.Cap.ansi (0, 0) (w, Notty.I.height img) img;
  print_string (Buffer.contents buf);
  print_newline ();
  (* The latency panel's title, which must also advertise the LOGS selector. *)
  let ti = Dashboard_ui.Latencies.render_latency_title w snap in
  let buf2 = Buffer.create 4096 in
  Notty.Render.to_buffer buf2 Notty.Cap.ansi (0, 0) (w, Notty.I.height ti) ti;
  print_string (Buffer.contents buf2);
  print_newline ();
  (* Alert popup preview. *)
  let alerts =
    [ { Dashboard_ui.Alerts.a_level = "CRITICAL"
      ; a_section = "main"
      ; a_message = "SIGTERM received; beginning graceful shutdown"
      ; a_count = 1
      }
    ; { Dashboard_ui.Alerts.a_level = "ERROR"
      ; a_section = "order_executor"
      ; a_message = "place rejected for SOL/USDC: insufficient margin on the venue"
      ; a_count = 3
      }
    ; { Dashboard_ui.Alerts.a_level = "WARN"
      ; a_section = "kraken_get_fee"
      ; a_message = "fee cache miss for ETH/USD; using default maker fee"
      ; a_count = 7
      }
    ; { Dashboard_ui.Alerts.a_level = "ERROR"
      ; a_section = "alpaca_rest"
      ; a_message =
          "Cancel order failed HTTP 422 for db72db3e-74e0-4bf2-8edd-28d0d2b82e5b: \
           {\"code\":42210000,\"message\":\"order pending cancel\"}"
      ; a_count = 1
      }
    ]
  in
  let mi = Dashboard_ui.Alerts.render_modal ~target_w:w ~target_h:24 alerts in
  let buf3 = Buffer.create 65536 in
  Notty.Render.to_buffer buf3 Notty.Cap.ansi (0, 0) (w, Notty.I.height mi) mi;
  print_string (Buffer.contents buf3);
  print_newline ()
;;
