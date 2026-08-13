(* Oracle_venues - resolves each asset's venue metadata (price tick, lot
   size) from the real exchange, mirroring the live supervisor's
   instrument-feed initialization so the capital survival model uses the
   correct increments per asset per venue.

   The oracle CLI is an out-of-process binary: the venue libraries are linked
   through dio.oracle, but under OCaml's unit-level dead-code elimination the
   top-level registry-register side effects in the venue modules only run
   when the modules are referenced. The references below force registration
   of BOTH the live-trading modules (Exchange_intf.Registry) and the oracle
   data-venue adapters (Exchange_intf.Oracle.Registry) - the same pattern as
   bin/main.ml - so Suicide_grid_config resolves increments through the real
   venue modules instead of warning about an empty registry, and the oracle
   data layer dispatches through Exchange_intf.Oracle.Registry.

   In online mode [init] then populates the instrument caches so per-asset
   lookups return real ticks/lots. It is fully registry-driven: each venue's
   adapter implements init_instruments itself, so a new venue only needs to
   implement Exchange_intf.Oracle.S, register it (and be force-referenced
   here) to participate:
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

module Exchange = Dio_exchange.Exchange_intf

(* Force venue module initialization so the top-level registry-register side
   effects (Exchange_intf.Registry AND Exchange_intf.Oracle.Registry) run at
   load time. *)
let () = ignore Kraken.Kraken_module.Kraken_impl.name
let () = ignore Hyperliquid.Module.Hyperliquid_impl.name
let () = ignore Alpaca.Module.Alpaca_impl.name

let init ?(offline = false) (tasks : Oracle_tasks.task list) : unit Lwt.t =
  if offline || tasks = []
  then Lwt.return_unit
  else (
    (* Group the requested symbols per venue (preserving each venue's
       testnet flag from its first task). *)
    let by_venue : (string, bool * string list) Hashtbl.t = Hashtbl.create 8 in
    List.iter
      (fun (t : Oracle_tasks.task) ->
         let testnet, syms =
           match Hashtbl.find_opt by_venue t.exchange with
           | Some (tn, s) -> tn, t.symbol :: s
           | None -> t.config.testnet, [ t.symbol ]
         in
         Hashtbl.replace by_venue t.exchange (testnet, syms))
      tasks;
    Lwt.catch
      (fun () ->
         (* Sequential per venue, in task order (a failed venue does not
            stop the others; the catch below absorbs it). *)
         Hashtbl.fold
           (fun exchange (testnet, syms) acc ->
              acc
              >>= fun () ->
              match Exchange.Oracle.Registry.get exchange with
              | Some (module V) -> V.init_instruments ~testnet ~symbols:syms
              | None ->
                Logging.warn_f
                  ~section
                  "no oracle data-venue registered for '%s'; skipping instrument \
                   metadata init (increments will fall back with a warning)"
                  exchange;
                Lwt.return_unit)
           by_venue
           Lwt.return_unit)
      (fun exn ->
         Logging.warn_f
           ~section
           "venue instrument metadata init failed (%s); increments will fall back with a \
            warning"
           (Printexc.to_string exn);
         Lwt.return_unit))
;;
