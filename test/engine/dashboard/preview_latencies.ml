(* Dev aid: render the ENGINE LATENCY panel headlessly from synthetic windows, so the
   trend column can be eyeballed without a live engine: dune exec
   test/engine/dashboard/preview_latencies.exe *)

let labels =
  [ "orderbook"
  ; "execution"
  ; "prep"
  ; "strategy"
  ; "cycle"
  ; "oracle"
  ; "wake"
  ; "gate"
  ; "native_res"
  ; "native_bridge"
  ; "native_run"
  ; "native_actions"
  ; "ws_feed"
  ; "ws_ping"
  ; "rest_request"
  ; "signer"
  ; "e2e"
  ]
;;

let metric p99 =
  `Assoc
    [ "p50", `Float (p99 /. 2.0)
    ; "p90", `Float p99
    ; "p99", `Float p99
    ; "p999", `Float (p99 *. 1.5)
    ; "samples", `Int 1200
    ; "union_samples", `Int 1200
    ; "sub_us_samples", `Int 0
    ; "overflow", `Int 0
    ; "executions", `Int 1200
    ; "executions_per_sec", `Float 2.0
    ; "last_exec_time", `Float 999.0
    ; "window_start", `Float 0.0
    ; "window_end", `Float 1000.0
    ; "max_cause", `Null
    ]
;;

let json_at t p99 =
  `Assoc
    [ "timestamp", `Float t
    ; ( "strategies"
      , `Assoc
          [ ( "BTC/USDC"
            , `Assoc [ "exchange", `String "hyperliquid"; "strategy", `Assoc [] ] )
          ] )
    ; ( "latencies"
      , `Assoc [ "BTC/USDC", `Assoc (List.map (fun l -> l, metric p99) labels) ] )
    ]
;;

let () =
  let w =
    match Array.length Sys.argv with
    | 2 -> int_of_string Sys.argv.(1)
    | _ -> 140
  in
  (* Feed a ramp so the sparkline has a visible trend, then plot it. *)
  for i = 0 to 19 do
    let snap =
      Dashboard_ui.Snapshot.of_json (json_at (1000.0 +. float i) (2.0 +. float i))
    in
    Dashboard_ui.Latencies.ingest snap
  done;
  let snap = Dashboard_ui.Snapshot.of_json (json_at 1020.0 21.0) in
  for page = 0 to Dashboard_ui.Latencies.page_count () - 1 do
    Dashboard_ui.Latencies.set_page page;
    let img = Dashboard_ui.Latencies.render_latencies w snap in
    let buf = Buffer.create 65536 in
    Notty.Render.to_buffer buf Notty.Cap.ansi (0, 0) (w, Notty.I.height img) img;
    print_string (Buffer.contents buf);
    print_newline ();
    print_newline ()
  done
;;
