(* Oracle_venues - resolves each asset's venue metadata (price tick, lot
   size) from the real exchange, mirroring the live supervisor's
   instrument-feed initialization so the capital survival model uses the
   correct increments per asset per venue.

   The oracle CLI is an out-of-process binary: the venue libraries are linked
   through dio.oracle, but under OCaml's unit-level dead-code elimination the
   top-level `Exchange.Registry.register` side effects in the venue modules only
   run when the modules are referenced. The references below force
   registration (the same pattern as bin/main.ml), so Suicide_grid_config
   resolves increments through the real venue modules instead of warning about
   an empty registry.

   In online mode [init] then populates the instrument caches so per-asset
   lookups return real ticks/lots:
   - Kraken: GET /0/public/AssetPairs for the requested symbols (per-symbol
     tick sizes, e.g. ADA/USD 0.0001).
   - Hyperliquid: POST /info {type:meta} / {type:spotMeta} for per-symbol
     szDecimals (lot sizes, e.g. BTC 0.00001); the price tick is a venue
     constant (0.01).
   - Alpaca: static tick (0.01) / fractional lots - nothing to fetch.

   Offline mode (--from-csv / --from-json) skips every network call; the
   caches stay empty and Suicide_grid_config logs a missing-metadata warning
   rather than silently defaulting. *)

open Lwt.Infix

let section = "oracle_venues"

(* Force venue module initialization so the Exchange.Registry.register
   side effects run at load time. *)
let () = ignore Kraken.Kraken_module.Kraken_impl.name
let () = ignore Hyperliquid.Module.Hyperliquid_impl.name
let () = ignore Alpaca.Module.Alpaca_impl.name

let init ?(offline = false) (tasks : Oracle_tasks.task list) : unit Lwt.t =
  if offline || tasks = []
  then Lwt.return_unit
  else (
    let kraken_symbols =
      List.filter_map
        (fun (t : Oracle_tasks.task) ->
           if t.exchange = "kraken" then Some t.symbol else None)
        tasks
    in
    let hyperliquid_symbols =
      List.filter_map
        (fun (t : Oracle_tasks.task) ->
           if t.exchange = "hyperliquid" then Some t.symbol else None)
        tasks
    in
    (* The Hyperliquid instrument cache is shared with the live engine's
       WebSocket feed (which uses the trading environment), so the REST
       metadata must be fetched from the SAME environment. Mainnet and
       testnet spot pair indices differ (e.g. HYPE/USDC: 107 mainnet vs
       1035 testnet); fetching mainnet meta while trading testnet poisons
       the asset-id cache and routes orders to the wrong spot pair. *)
    let hyperliquid_testnet =
      List.find_map
        (fun (t : Oracle_tasks.task) ->
           if t.exchange = "hyperliquid" then Some t.config.testnet else None)
        tasks
      |> Option.value ~default:true
    in
    Lwt.catch
      (fun () ->
         let init_kraken =
           if kraken_symbols = []
           then Lwt.return_unit
           else Kraken.Kraken_instruments_feed.initialize_symbols kraken_symbols
         in
         init_kraken
         >>= fun () ->
         if hyperliquid_symbols = []
         then Lwt.return_unit
         else
           Hyperliquid.Instruments_feed.fetch_meta_from_rest
             ~testnet:hyperliquid_testnet
             ())
      (fun exn ->
         Logging.warn_f
           ~section
           "venue instrument metadata init failed (%s); increments will fall back with a \
            warning"
           (Printexc.to_string exn);
         Lwt.return_unit))
;;
