let () =
  Logging.init ();
  Logging.set_colors true;
  Logging.set_output stdout;
  Flow_log.configure
    { Flow_log.enabled = true
    ; min_interval_s = 0.0
    ; heartbeat_s = 0.0
    ; pulse_s = 0.0
    ; price_ticks = true
    };
  Flow_log.tick
    ~section:"BTC/USDC"
    ~headline:"BTC/USDC · mid 67235.50 · +0.42bp ▲"
    ~fields:
      [ "book", "bid 67234.1×1.20 · ask 67236.9×0.80 · spr 0.42bp · imb 0.60"
      ; "strat", "grid · qty 0.038772 · gi 0.8000%"
      ];
  Flow_log.tick
    ~section:"ETH/USDC"
    ~headline:"ETH/USDC · mid 3284.117 · -1.07bp ▼"
    ~fields:
      [ "book", "bid 3284.05×14.2 · ask 3284.18×9.7 · spr 0.40bp · imb 0.59"
      ; "strat", "grid · qty 0.914 · gi 0.6200%"
      ];
  Flow_log.tick
    ~section:"SOL/USDC"
    ~headline:"SOL/USDC · mid 151.8820 · +2.31bp ▲"
    ~fields:
      [ "book", "bid 151.874×820 · ask 151.890×640 · spr 1.05bp · imb 0.56"
      ; "strat", "grid · qty 18.4 · gi 1.1000%"
      ];
  Flow_log.event
    ~section:"BTC/USDC"
    ~category:Flow_log.Signal
    ~headline:"⟨BTC/USDC⟩ α=+2.3σ · α-weighted edge 1.8bp"
    ~fields:
      [ ( "book"
        , "bid 67234.1×1.20 · ask 67236.9×0.80 · mid 67235.50 · spr 0.42bp · imb 0.60" )
      ; "calc", "edge 1.8bp · vol 0.31 · range 1.4% · fng 63"
      ; "oracle", "ACTIVE · qty 0.038772 · gi 0.8000% · D_surv 83.6%"
      ; "gate", "exec ARMED · market OPEN · oracle-gate OPEN · buys clear"
      ; "send", "▸ BUY 0.038772 @ 67230.5  post-only GTC"
      ; "lat", "ob 12µs · prep 8µs · strat 34µs · total 256µs"
      ];
  Flow_log.event
    ~section:"BTC/USDC"
    ~category:Flow_log.Send
    ~headline:"BUY 0.038772 @ 67230.5"
    ~fields:
      [ "venue", "hyperliquid · tif GTC · post-only"
      ; "qty", "0.038772"
      ; "px", "67230.5"
      ; "id", "new"
      ; "strat", "grid"
      ];
  Flow_log.event
    ~section:"BTC/USDC"
    ~category:Flow_log.Amend
    ~headline:"BUY 0.038772 -> 67248.9"
    ~fields:
      [ "venue", "hyperliquid · tif GTC · post-only"
      ; "qty", "0.038772"
      ; "px", "67248.9"
      ; "id", "0x9f31…a2"
      ; "strat", "grid"
      ];
  Flow_log.event
    ~section:"BTC/USDC"
    ~category:Flow_log.Cancel
    ~headline:"BUY cancel 0x9f31…a2"
    ~fields:[ "venue", "hyperliquid · tif GTC · post-only"; "id", "0x9f31…a2" ];
  Flow_log.event
    ~section:"BTC/USDC"
    ~category:Flow_log.Fill
    ~headline:"⟨BTC/USDC⟩ 0.038772 @ 67231.0 filled (+1.8bp)"
    ~fields:
      [ "pnl", "realized +$0.07 · inventory +0.0388 BTC"
      ; "react", "sell placed 0.038772 @ 67274.6 (+1 rung)"
      ];
  Flow_log.pulse
    ~headline:"8 domains · 41.2k cycles/s"
    ~fields:
      [ "rate", "41.2k cyc/s · 3 exec/window · pool 8"
      ; "tail", "p99 88µs · max 340µs · stw 2µs"
      ];
  Thread.delay 0.3
;;
