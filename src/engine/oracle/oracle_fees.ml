(* Oracle_fees - resolve maker/taker fees per venue and hold them per asset.

   The grid replay and inverse sizing depend on the maker fee, resolved from the venue's
   oracle adapter ([Exchange_intf.Oracle.S.fetch_fees] via
   [Exchange_intf.Oracle.Registry]) rather than a hardcoded rate. Fees are cached per
   (exchange, symbol) for the process lifetime and stored in [Dio_strategies.Fee_cache].
   The venue's [default_fees] is the offline/failed-fetch fallback. An explicit
   config.json "maker_fee" (or --fee) always wins; fetching happens only when neither is
   set. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "oracle_fees"

(* Last-resort generic fee, used only when no oracle adapter is registered for the venue
   (pure/offline/test contexts). Registered venues use their own
   [Exchange_intf.Oracle.S.default_fees]. *)
let fallback_maker_fee = 0.0016
let fallback_taker_fee = 0.0026

(** Per-process cache of resolved (maker, taker) fees per (exchange, symbol). *)
let fee_cache : (string * string, float * float) Hashtbl.t = Hashtbl.create 16

(** Venue default (maker, taker) for [exchange]/[symbol]: the registered adapter's
    [default_fees] when available, else the generic fallback. *)
let venue_default_fees (exchange : string) (symbol : string) : float * float =
  match Exchange.Oracle.Registry.get (String.lowercase_ascii exchange) with
  | Some (module V) -> V.default_fees ~symbol
  | None -> fallback_maker_fee, fallback_taker_fee
;;

(** Fetch (maker, taker) from the real exchange for one asset, through the venue's oracle
    adapter. *)
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

(** Load .env (KRAKEN/HYPERLIQUID/ALPACA credentials) into the process env if present. *)
let load_dotenv () =
  try Logging.load_dotenv ~path:".env" () with
  | _ -> ()
;;

(** Resolve (maker, taker) for an asset, cached per (exchange, symbol). Falls back to the
    venue's [default_fees] when the exchange fee endpoint is unreachable. *)
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

(** Enrich a trading_config with the exchange maker/taker fee, stored on the config and in
    the shared Fee_cache. An explicit config.json "maker_fee"/"taker_fee" wins. Offline
    mode uses the venue [default_fees] with a warning and no network. *)
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
