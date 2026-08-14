(* Oracle_fees - resolves real maker/taker fees per trading venue and holds
   them per asset, mirroring the live supervisor's fee enrichment.

   The grid path replay and inverse sizing depend on the maker fee. Instead of
   a hardcoded flat rate, each asset's fee is resolved from its venue's oracle
   adapter ([Exchange_intf.Oracle.S.fetch_fees], dispatched through
   [Exchange_intf.Oracle.Registry]) - a new venue is plug-and-play here too.
   Fees are cached per (exchange, symbol) for the process lifetime and also
   stored in the shared Dio_strategies.Fee_cache, so a fee fetched here is
   held per asset like the supervisor does. [default_fees] is the venue's own
   offline / failed-fetch fallback.

   A maker fee explicitly set in config.json ("maker_fee") or passed via
   --fee always wins; fetching only happens when neither is present. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "oracle_fees"

(* Last-resort generic fee, used ONLY when no oracle adapter is registered
   for the venue (pure/offline/test contexts where the venue libraries are
   not linked). Registered venues use their own
   [Exchange_intf.Oracle.S.default_fees] instead - venue-specific fee data
   lives in the venue's adapter, never here. *)
let fallback_maker_fee = 0.0016
let fallback_taker_fee = 0.0026

(** Per-process cache of resolved (maker, taker) fees per (exchange, symbol). *)
let fee_cache : (string * string, float * float) Hashtbl.t = Hashtbl.create 16

(** Venue default (maker, taker) for [exchange]/[symbol]: the registered
    adapter's [default_fees] when available, else the generic fallback. *)
let venue_default_fees (exchange : string) (symbol : string) : float * float =
  match Exchange.Oracle.Registry.get (String.lowercase_ascii exchange) with
  | Some (module V) -> V.default_fees ~symbol
  | None -> fallback_maker_fee, fallback_taker_fee
;;

(** Fetch (maker, taker) from the real exchange for one asset, through the
    venue's oracle adapter. *)
let fetch_fees ~(exchange : string) ~(symbol : string) ~(testnet : bool)
  : (float * float) Lwt.t
  =
  match Exchange.Oracle.Registry.get (String.lowercase_ascii exchange) with
  | Some (module V) -> V.fetch_fees ~testnet ~symbol
  | None ->
    Logging.warn_f
      ~section
      "no live fee endpoint for exchange '%s'; using venue default maker %.4f%%"
      exchange
      (fallback_maker_fee *. 100.0);
    Lwt.return (fallback_maker_fee, fallback_taker_fee)
;;

(** Load .env (KRAKEN/HYPERLIQUID/ALPACA credentials) into the process env, if
    present. Idempotent enough for CLI use. *)
let load_dotenv () =
  try Dotenv.export ~path:".env" () with
  | _ -> ()
;;

(** Resolve (maker, taker) for an asset, cached per (exchange, symbol). Falls
    back to the venue's [default_fees] when the exchange fee endpoint is
    unreachable. *)
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
           (fst (venue_default_fees exchange symbol) *. 100.0);
         Lwt.return (venue_default_fees exchange symbol))
    >|= fun fees ->
    Hashtbl.replace fee_cache (exchange, symbol) fees;
    fees
;;

(** Enrich a trading_config with the real exchange maker/taker fee, holding the
    result per asset on the config itself and in the shared Fee_cache. Honors an
    explicit config.json "maker_fee"/"taker_fee". In offline mode no network is
    used and the venue's [default_fees] is applied with a warning. *)
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
    let maker, taker = venue_default_fees tc.exchange tc.symbol in
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
