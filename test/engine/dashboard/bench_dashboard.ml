(* Dashboard render benchmark.

   Renders a representative snapshot through the main view and the asset
   detail view, reporting parse and per-frame render costs. Run manually:

     opam exec -- dune exec test/engine/dashboard/bench_dashboard.exe

   Optional argument: number of strategies (default 8). *)

open Dashboard_ui

let base_asset i = Printf.sprintf "AST%02d" i

let book_json ~mid =
  let side f =
    `List
      (List.init 10 (fun k ->
         let p = mid *. (1.0 +. (f *. float (k + 1) *. 0.0005)) in
         `Assoc [ "price", `Float p; "qty", `Float (float (10 - k) *. 1.5) ]))
  in
  [ "bids", side (-1.0); "asks", side 1.0 ]
;;

let strategy_json i =
  let base = base_asset i in
  let mid = 100.0 +. float i in
  let sells =
    `List
      (List.init 5 (fun k ->
         `Assoc
           [ "id", `String (Printf.sprintf "s%d-%d" i k)
           ; "price", `Float (mid *. (1.0 +. (float (k + 1) *. 0.01)))
           ; "qty", `Float (float (k + 1))
           ]))
  in
  let oracle =
    `Assoc
      [ "active", `Bool (i mod 3 <> 0)
      ; "reason", `String "bench"
      ; "buy_qty", `Float 2.5
      ; "max_drawdown_pct", `Float 0.12
      ; "grid_interval", `Float 0.4
      ; "d_surv", `Float 0.8
      ; "exhaustion_price", `Float (mid *. 0.7)
      ]
  in
  let market =
    `Assoc
      ([ "bid", `Float (mid *. 0.999)
       ; "ask", `Float (mid *. 1.001)
       ; "base_asset", `String base
       ; "base_balance", `Float 12.0
       ; "staked_balance", `Float 1.0
       ; "quote_balance", `Float 5000.0
       ; ( "buy_orders"
         , `List
             [ `Assoc
                 [ "id", `String "b"; "price", `Float (mid *. 0.99); "qty", `Float 3.0 ]
             ] )
       ; "sell_orders", sells
       ]
       @ book_json ~mid)
  in
  ( Printf.sprintf "%s/USDC" base
  , `Assoc
      [ "exchange", `String "hyperliquid"
      ; ( "strategy"
        , `Assoc
            [ "type", `String "Ladder"
            ; "buy_price", `Float (mid *. 0.99)
            ; "buy_qty", `Float 3.0
            ; "sell_orders", sells
            ; "sell_count", `Int 5
            ; "capital_low", `Bool false
            ; "last_buy_fill", `Float (mid *. 0.995)
            ; "last_sell_fill", `Float (mid *. 1.01)
            ] )
      ; "market", market
      ; "oracle", oracle
      ; "grid_interval_lo", `Float 0.4
      ] )
;;

let balance_json i =
  let a = base_asset (100 + i) in
  `Assoc
    [ "exchange", `String "kraken"
    ; "asset", `String a
    ; "symbol", `String (a ^ "/USD")
    ; "balance", `Float (float (i + 1))
    ; "bid", `Float 10.0
    ; "ask", `Float 10.1
    ; "sell_orders", `List []
    ]
;;

let metric_json =
  `Assoc
    [ "p50", `Float 120.0
    ; "p90", `Float 300.0
    ; "p99", `Float 800.0
    ; "p999", `Float 1500.0
    ; "samples", `Int 40
    ; "executions", `Int 12
    ; "executions_per_sec", `Float 2.4
    ; "window_end", `Float 1000.0
    ]
;;

let snapshot_json n =
  `Assoc
    [ "timestamp", `Float 1000.0
    ; "uptime_s", `Float 12345.0
    ; "fear_and_greed", `Float 55.0
    ; ( "memory"
      , `Assoc
          [ "heap_mb", `Int 120
          ; "live_kb", `Int 80000
          ; "free_kb", `Int 40000
          ; "space_overhead", `Int 40
          ; "gc_major", `Int 12
          ; "gc_minor", `Int 900
          ; "compactions", `Int 1
          ; "fragments", `Int 3
          ] )
    ; "strategies", `Assoc (List.init n strategy_json)
    ; "all_balances", `List (List.init 6 balance_json)
    ; ( "recent_fills"
      , `List
          (List.init 50 (fun i ->
             `Assoc
               [ "venue", `String "hyperliquid"
               ; "symbol", `String "AST00/USDC"
               ; "side", `String (if i mod 2 = 0 then "buy" else "sell")
               ; "amount", `Float 1.5
               ; "fill_price", `Float 100.5
               ; "timestamp", `Float (1000.0 -. float i)
               ])) )
    ; ( "latencies"
      , `Assoc
          (List.init n (fun i ->
             ( Printf.sprintf "%s/USDC" (base_asset i)
             , `Assoc
                 [ "oracle", metric_json
                 ; "orderbook", metric_json
                 ; "strategy", metric_json
                 ; "execution", metric_json
                 ; "cycle", metric_json
                 ] ))) )
    ; "oracle_latency", `Assoc [ "pass", metric_json ]
    ]
;;

let render_main w s =
  let buf = Buffer.create 65536 in
  let img =
    Notty.I.vcat
      [ Kpi_cards.render_kpi_cards w s
      ; Ticker_feed.render_ticker w s
      ; Holdings.render_strategies ~selected_index:(Some 0) w s
      ; Recent_fills_feed.render_fills w s
      ; Memory.render_memory w s
      ; Latencies.render_latencies w s
      ; Footer.render_footer w s
      ]
    |> Notty.I.hsnap ~align:`Left w
  in
  Notty.Render.to_buffer buf Notty.Cap.ansi (0, 0) (w, Notty.I.height img) img;
  Buffer.length buf
;;

let render_detail w h s =
  let buf = Buffer.create 65536 in
  let img = Asset_graph.render_asset_detail w h "" s |> Notty.I.hsnap ~align:`Left w in
  Notty.Render.to_buffer buf Notty.Cap.ansi (0, 0) (w, Notty.I.height img) img;
  Buffer.length buf
;;

let time_ms n f =
  let t0 = Unix.gettimeofday () in
  for _ = 1 to n do
    ignore (f ())
  done;
  let dt = (Unix.gettimeofday () -. t0) *. 1000.0 in
  dt /. float n
;;

let () =
  let n = if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 8 in
  let w = 220
  and h = 60 in
  let json = snapshot_json n in
  let parse_ms = time_ms 100 (fun () -> Snapshot.of_json json) in
  let s = Snapshot.of_json json in
  let main_ms = time_ms 50 (fun () -> render_main w s) in
  let detail_ms = time_ms 50 (fun () -> render_detail w h s) in
  Printf.printf
    "dashboard render (%d strategies, %dx%d): parse %.2fms  main %.2fms (%d bytes)  \
     detail %.2fms (%d bytes)\n\
     %!"
    n
    w
    h
    parse_ms
    main_ms
    (render_main w s)
    detail_ms
    (render_detail w h s)
;;
