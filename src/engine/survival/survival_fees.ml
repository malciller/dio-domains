(* Survival_fees - resolves real maker/taker fees per trading venue and holds
   them per asset, mirroring the live supervisor's fee enrichment.

   The grid path replay and inverse sizing depend on the maker fee. Instead of
   a hardcoded flat rate, the CLI resolves each asset's fee from its exchange
   (Kraken TradeVolume for the exact account tier, Hyperliquid /info userFees,
   Alpaca is commission-free). Fees are cached per (exchange, symbol) for the
   process lifetime and also stored in the shared Dio_strategies.Fee_cache, so
   a fee fetched here is held per asset like the supervisor does.

   A maker fee explicitly set in config.json ("maker_fee") or passed via
   --fee always wins; fetching only happens when neither is present. *)

open Lwt.Infix

let section = "survival_fees"

let fallback_maker_fee = function
  | "kraken" -> 0.0016
  | "hyperliquid" -> 0.0002
  | "alpaca" -> 0.0
  | _ -> 0.0016
;;

let fallback_taker_fee = function
  | "kraken" -> 0.0026
  | "hyperliquid" -> 0.0005
  | "alpaca" -> 0.0
  | _ -> 0.0026
;;

(** Per-process cache of resolved (maker, taker) fees per (exchange, symbol). *)
let fee_cache : (string * string, float * float) Hashtbl.t = Hashtbl.create 16

(** Fetch (maker, taker) from the real exchange for one asset. *)
let fetch_fees ~(exchange : string) ~(symbol : string) ~(testnet : bool)
  : (float * float) Lwt.t
  =
  match exchange with
  | "kraken" ->
    Kraken.Kraken_get_fee.get_fee_info symbol
    >|= fun info ->
    (match info with
     | Some f ->
       ( Option.value
           f.Kraken.Kraken_get_fee.maker_fee
           ~default:(fallback_maker_fee exchange)
       , Option.value
           f.Kraken.Kraken_get_fee.taker_fee
           ~default:(fallback_taker_fee exchange) )
     | None -> fallback_maker_fee exchange, fallback_taker_fee exchange)
  | "hyperliquid" ->
    Hyperliquid.Get_fee.get_fee_info ~testnet ()
    >|= fun info ->
    let is_spot = String.contains symbol '/' in
    (match info with
     | Some f ->
       let maker =
         if is_spot
         then Option.value f.Hyperliquid.Get_fee.spot_maker_fee ~default:0.0
         else Option.value f.Hyperliquid.Get_fee.maker_fee ~default:0.0002
       in
       let taker =
         if is_spot
         then Option.value f.Hyperliquid.Get_fee.spot_taker_fee ~default:0.001
         else Option.value f.Hyperliquid.Get_fee.taker_fee ~default:0.0005
       in
       maker, taker
     | None -> (if is_spot then 0.0 else 0.0002), if is_spot then 0.001 else 0.0005)
  | "alpaca" -> Lwt.return (0.0, 0.0)
  | exchange ->
    Logging.warn_f
      ~section
      "no live fee endpoint for exchange '%s'; using venue default maker %.4f%%"
      exchange
      (fallback_maker_fee exchange *. 100.0);
    Lwt.return (fallback_maker_fee exchange, fallback_taker_fee exchange)
;;

(** Load .env (KRAKEN/HYPERLIQUID/ALPACA credentials) into the process env, if
    present. Idempotent enough for CLI use. *)
let load_dotenv () =
  try Dotenv.export ~path:".env" () with
  | _ -> ()
;;

(** Resolve (maker, taker) for an asset, cached per (exchange, symbol). Falls
    back to venue defaults when the exchange fee endpoint is unreachable. *)
let resolved_fees ~(exchange : string) ~(symbol : string) ~(testnet : bool)
  : (float * float) Lwt.t
  =
  match Hashtbl.find_opt fee_cache (exchange, symbol) with
  | Some fees -> Lwt.return fees
  | None ->
    load_dotenv ();
    Lwt.catch
      (fun () -> fetch_fees ~exchange ~symbol ~testnet)
      (fun exn ->
         Logging.warn_f
           ~section
           "fee fetch for %s/%s failed (%s); using venue default maker %.4f%%"
           exchange
           symbol
           (Printexc.to_string exn)
           (fallback_maker_fee exchange *. 100.0);
         Lwt.return (fallback_maker_fee exchange, fallback_taker_fee exchange))
    >|= fun fees ->
    Hashtbl.replace fee_cache (exchange, symbol) fees;
    fees
;;

(** Enrich a trading_config with the real exchange maker/taker fee, holding the
    result per asset on the config itself and in the shared Fee_cache. Honors an
    explicit config.json "maker_fee"/"taker_fee". In offline mode no network is
    used and the venue default is applied with a warning. *)
let enrich (tc : Dio_strategies.Strategy_common.trading_config) ~(offline : bool)
  : Dio_strategies.Strategy_common.trading_config Lwt.t
  =
  match tc.maker_fee, tc.taker_fee with
  | Some maker, _ ->
    let taker = Option.value tc.taker_fee ~default:maker in
    Dio_strategies.Fee_cache.store_fees
      ~exchange:tc.exchange
      ~symbol:tc.symbol
      ~maker_fee:maker
      ~taker_fee:taker
      ~ttl_seconds:600.0;
    Lwt.return { tc with taker_fee = Some taker }
  | None, _ when offline ->
    let maker = fallback_maker_fee tc.exchange in
    let taker = fallback_taker_fee tc.exchange in
    Logging.warn_f
      ~section
      "offline mode: not fetching live fees for %s/%s; using venue default maker %.4f%% \
       (pass --fee to override)"
      tc.exchange
      tc.symbol
      (maker *. 100.0);
    Dio_strategies.Fee_cache.store_fees
      ~exchange:tc.exchange
      ~symbol:tc.symbol
      ~maker_fee:maker
      ~taker_fee:taker
      ~ttl_seconds:600.0;
    Lwt.return { tc with maker_fee = Some maker; taker_fee = Some taker }
  | None, _ ->
    resolved_fees ~exchange:tc.exchange ~symbol:tc.symbol ~testnet:tc.testnet
    >|= fun (maker, taker) ->
    Dio_strategies.Fee_cache.store_fees
      ~exchange:tc.exchange
      ~symbol:tc.symbol
      ~maker_fee:maker
      ~taker_fee:taker
      ~ttl_seconds:600.0;
    { tc with maker_fee = Some maker; taker_fee = Some taker }
;;
