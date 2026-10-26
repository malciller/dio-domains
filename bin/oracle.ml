(* dio-oracle.exe -- configuration-tuning entrypoint.

   Runs the oracle decision pipeline offline against configured assets and prints the
   decision surface: active / grid_interval / buy_qty / sell_qty plus internal
   diagnostics. Pools are synthetic, sized with --quote per asset (and --base for sell
   sizing), so tuning is reproducible and live balances are never touched.

   History uses the same all-time merged pipeline as the live runtime (venue bars
   + Yahoo deep history). With --cache-only, only disk-cached history is used. *)

let usage =
  {|dio-oracle [options]

Sizing uses the REAL per-venue account balance by default (the quote pool is the
venue's actual available quote, shared across the venue's assets exactly as the live
runtime allocates it). --cache-only only affects history, not balances.

Options:
  --symbol SYM    Analyze only SYM (default: every trading entry).
  --quote FLOAT   Force a synthetic quote pool per asset (disables live balances).
  --base FLOAT    Force a synthetic base balance for sell sizing.
  --synthetic     Force synthetic pools (default quote 10000, base 0).
  --live          Use live venue balances (default).
  --cache-only    Use only disk-cached history; never touch the network.
  --json          Emit machine-readable JSON instead of a table.
  --help          This text.
|}
;;

type args =
  { mutable symbol : string
  ; mutable quote : float
  ; mutable base : float
  ; mutable cache_only : bool
  ; mutable json : bool
  ; mutable live : bool
  }

let parse_args () =
  let a =
    { symbol = ""
    ; quote = 10_000.0
    ; base = 0.0
    ; cache_only = false
    ; json = false
    ; live = true
    }
  in
  let rec go = function
    | [] -> ()
    | "--help" :: _ | "-h" :: _ ->
      print_endline usage;
      exit 0
    | "--symbol" :: v :: r ->
      a.symbol <- v;
      go r
    | "--quote" :: v :: r ->
      a.quote <- Float.of_string v;
      a.live <- false;
      go r
    | "--base" :: v :: r ->
      a.base <- Float.of_string v;
      a.live <- false;
      go r
    | "--synthetic" :: r ->
      a.live <- false;
      go r
    | "--live" :: r ->
      a.live <- true;
      go r
    | "--cache-only" :: r ->
      a.cache_only <- true;
      go r
    | "--json" :: r ->
      a.json <- true;
      go r
    | bad :: _ ->
      Printf.eprintf "unknown argument: %s\n%s\n" bad usage;
      exit 2
  in
  go (Array.to_list Sys.argv |> List.tl);
  a
;;

(** Quote currency of a symbol: "BTC/USDC" -> "USDC"; a bare ticker (Alpaca) -> "USD". *)
let quote_currency_of_symbol symbol =
  match String.rindex_opt symbol '/' with
  | Some i -> String.sub symbol (i + 1) (String.length symbol - i - 1)
  | None -> "USD"
;;

(** Base asset of a symbol: "BTC/USDC" -> "BTC"; a bare ticker (Alpaca) -> the ticker. *)
let base_asset_of_symbol symbol =
  match String.rindex_opt symbol '/' with
  | Some i -> String.sub symbol 0 i
  | None -> symbol
;;

(** Per-asset effective knobs: the engine config's "oracle" section with its assets
    overrides keyed by symbol (same resolution as the live runtime). *)
let knobs_for ~(config : Dio_engine.Config.config option) ~(symbol : string)
  : float * float * float
  =
  match config with
  | None ->
    let d = Dio_oracle.Oracle_runtime.default_config () in
    d.target_survival, d.min_active_dsurv, d.qty_cap_mult
  | Some c ->
    let d =
      match c.oracle with
      | Some o -> o
      | None -> Dio_oracle.Oracle_runtime.default_config ()
    in
    let ov =
      match c.oracle with
      | Some o ->
        Option.map snd (List.find_opt (fun (s, _) -> String.equal s symbol) o.assets)
      | None -> None
    in
    let get get_ov default = Option.value (Option.bind ov get_ov) ~default in
    ( get (fun o -> o.target_survival) d.target_survival
    , get (fun o -> o.min_active_dsurv) d.min_active_dsurv
    , get (fun o -> o.qty_cap_mult) d.qty_cap_mult )
;;

type row =
  { exchange : string
  ; symbol : string
  ; current : float (** Live price used for the replay (0.0 when unusable). *)
  ; quote_used : float (** Quote pool the decision was sized against. *)
  ; base_used : float (** Base balance available for sell sizing. *)
  ; maker : float (** Maker fee used by the survival walk. *)
  ; priority : int (** Config presentation order; venue pool allocation order. *)
  ; outcome : Dio_oracle.Oracle_pipeline.outcome option
  ; error : string
  }

let analyze_one
  ~(args : args)
  ~(config : Dio_engine.Config.config option)
  ~(available_quote : float)
  ~(available_asset : float)
  ~(priority : int)
  (tc : Dio_strategies.Strategy_common.trading_config)
  : row Lwt.t
  =
  let open Lwt.Infix in
  let offline = args.cache_only in
  Lwt.catch
    (fun () ->
      Dio_oracle.Oracle_pipeline.history_of
        ~offline
        ~exchange:tc.exchange
        ~symbol:tc.symbol
      >>= fun series ->
      let current =
        match Dio_oracle.Oracle_pipeline.current_price_of_series series with
        | Some c when c > 0.0 -> c
        | _ -> 0.0
      in
      if current <= 0.0
      then
        Lwt.return
          { exchange = tc.exchange
          ; symbol = tc.symbol
          ; current
          ; quote_used = available_quote
          ; base_used = available_asset
          ; maker = 0.0
          ; priority
          ; outcome = None
          ; error = "no usable close in history"
          }
      else (
        let target_survival, min_active_dsurv, qty_cap_mult =
          knobs_for ~config ~symbol:tc.symbol
        in
        let qty_raw =
          try Float.of_string tc.qty with
          | _ -> 0.0
        in
        let bounds =
          { Dio_oracle.Oracle_core.qty = Float.max 1e-12 qty_raw
          ; qty_cap_mult = Float.max 1.0 qty_cap_mult
          ; gi_min = fst tc.grid_interval
          ; gi_max = snd tc.grid_interval
          }
        in
        let maker =
          match tc.maker_fee with
          | Some f -> f
          | None -> fst (Dio_oracle.Oracle_fees.venue_default_fees tc.exchange tc.symbol)
        in
        let fees =
          { Dio_oracle.Oracle_core.maker_fee = maker; fee_in_base_buy = false }
        in
        let inputs : Dio_oracle.Oracle_pipeline.inputs =
          { exchange = tc.exchange
          ; symbol = tc.symbol
          ; bars = series.bars
          ; current_price = current
          ; available_quote
          ; sell_qty = available_asset
          ; bounds
          ; target_survival
          ; min_active_dsurv
          ; fees
          ; has_resting_buy = false
          }
        in
        match Dio_oracle.Oracle_pipeline.decide ~inputs with
        | None ->
          Lwt.return
            { exchange = tc.exchange
            ; symbol = tc.symbol
            ; current
            ; quote_used = available_quote
            ; base_used = available_asset
            ; maker
            ; priority
            ; outcome = None
            ; error = "no references"
            }
        | Some o ->
          Lwt.return
            { exchange = tc.exchange
            ; symbol = tc.symbol
            ; current
            ; quote_used = available_quote
            ; base_used = available_asset
            ; maker
            ; priority
            ; outcome = Some o
            ; error = ""
            }))
    (fun exn ->
      Lwt.return
        { exchange = tc.exchange
        ; symbol = tc.symbol
        ; current = 0.0
        ; quote_used = available_quote
        ; base_used = available_asset
        ; maker = 0.0
        ; priority
        ; outcome = None
        ; error = Printexc.to_string exn
        })
;;

let string_of_outcome (r : row) (o : Dio_oracle.Oracle_pipeline.outcome) =
  let exhaust =
    Dio_oracle.Oracle_core.exhaustion_price_of
      ~current:r.current
      ~funded_floor:o.runway.funded_floor
      ~d_surv:o.resolution.d_surv
  in
  Printf.sprintf
    "%-18s %-11s d_surv %.3f exhaust %12.6g | quote %12.6g floor %12.6g ath %12.6g atl \
     %12.6g mdd %5.1f%%"
    (Dio_oracle.Oracle_runtime.string_of_regime o.runway.regime)
    (Dio_oracle.Oracle_runtime.string_of_branch o.resolution.branch)
    o.resolution.d_surv
    exhaust
    r.quote_used
    o.runway.floor_price
    o.refs.ath
    o.refs.atl
    (o.refs.max_drawdown_pct *. 100.0)
;;

let print_table rows args =
  Printf.printf
    "%s\n\n"
    (if args.live
     then "pools: REAL per-venue balances (quote shared across the venue's assets)"
     else
       Printf.sprintf
         "pools: SYNTHETIC quote %.2f base %.6g (per asset)"
         args.quote
         args.base);
  Printf.printf
    " %-14s %-14s %-7s %10s %12s %12s  %s\n"
    "EXCHANGE"
    "SYMBOL"
    "ACTIVE"
    "GI%"
    "BUY_QTY"
    "SELL_QTY"
    "DIAGNOSTICS";
  List.iter
    (fun (r : row) ->
      match r.outcome with
      | None ->
        Printf.printf
          " %-14s %-14s %-7s %10s %12s %12s  ERROR: %s\n"
          r.exchange
          r.symbol
          "-"
          "-"
          "-"
          "-"
          r.error
      | Some o ->
        let d = o.decision in
        Printf.printf
          " %-14s %-14s %-7s %10.4f %12.6g %12.6g  %s\n"
          r.exchange
          r.symbol
          (if d.active then "yes" else "no")
          d.grid_interval
          d.buy_qty
          r.base_used
          (string_of_outcome r o))
    rows
;;

let print_json rows args =
  let rows_json =
    List.map
      (fun (r : row) ->
        let common =
          [ "exchange", `String r.exchange
          ; "symbol", `String r.symbol
          ; "live_balances", `Bool args.live
          ]
        in
        match r.outcome with
        | None -> `Assoc (common @ [ "error", `String r.error ])
        | Some o ->
          let d = o.decision in
          `Assoc
            (common
             @ [ "active", `Bool d.active
               ; "grid_interval", `Float d.grid_interval
               ; "buy_qty", `Float d.buy_qty
               ; "sell_qty", `Float r.base_used
               ; "quote_used", `Float r.quote_used
               ; "d_surv", `Float o.resolution.d_surv
               ; ( "exhaustion_price"
                 , `Float
                     (Dio_oracle.Oracle_core.exhaustion_price_of
                        ~current:r.current
                        ~funded_floor:o.runway.funded_floor
                        ~d_surv:o.resolution.d_surv) )
               ; ( "regime"
                 , `String (Dio_oracle.Oracle_runtime.string_of_regime o.runway.regime) )
               ; ( "branch"
                 , `String
                     (Dio_oracle.Oracle_runtime.string_of_branch o.resolution.branch) )
               ; "floor_price", `Float o.runway.floor_price
               ; "ath", `Float o.refs.ath
               ; "atl", `Float o.refs.atl
               ; "max_drawdown_pct", `Float o.refs.max_drawdown_pct
               ]))
      rows
  in
  print_endline (Yojson.Basic.to_string (`List rows_json))
;;

(** Mirror the live runtime's shared venue drawdown simulation. The CLI hands every asset
    on a venue the venue's full quote pool (as the runtime does for each asset's own
    decision); this then walks the venue's active assets in config order, allocating one
    rung at a time until the venue's real quote is exhausted, and overrides each active
    row's [d_surv] with the shared-capital result. *)
let apply_venue_shared_sim (rows : row list) : row list =
  let groups : (string, row list) Hashtbl.t = Hashtbl.create 8 in
  List.iter
    (fun (r : row) ->
      let key = String.lowercase_ascii r.exchange in
      let prior = Option.value (Hashtbl.find_opt groups key) ~default:[] in
      Hashtbl.replace groups key (r :: prior))
    rows;
  let override : (string, float) Hashtbl.t = Hashtbl.create 8 in
  Hashtbl.iter
    (fun _key group_rows ->
      let group_rows = List.rev group_rows in
      let total_quote =
        match group_rows with
        | r :: _ -> r.quote_used
        | [] -> 0.0
      in
      let sims =
        List.filter_map
          (fun (r : row) ->
            match r.outcome with
            | Some o when o.decision.active ->
              Some
                { Dio_oracle.Oracle_pools.id = Printf.sprintf "%s|%s" r.exchange r.symbol
                ; priority = r.priority
                ; current = r.current
                ; grid_interval = o.decision.grid_interval
                ; buy_qty = o.decision.buy_qty
                ; maker_fee = r.maker
                }
            | _ -> None)
          group_rows
      in
      List.iter
        (fun (id, (o : Dio_oracle.Oracle_pools.sim_outcome)) ->
          Hashtbl.replace override id o.d_surv)
        (Dio_oracle.Oracle_pools.simulate_drawdown_survival ~total_quote sims))
    groups;
  List.map
    (fun (r : row) ->
      let id = Printf.sprintf "%s|%s" r.exchange r.symbol in
      match r.outcome, Hashtbl.find_opt override id with
      | Some o, Some d when o.decision.active ->
        { r with outcome = Some { o with resolution = { o.resolution with d_surv = d } } }
      | _ -> r)
    rows
;;

(* Force the venue adapter modules to link: each registers its oracle adapter into
   Exchange_intf.ln as a side effect, and the fetch pipeline dispatches purely through
   that registry. *)
let () = ignore Kraken.Kraken_module.Kraken_impl.name
let () = ignore Hyperliquid.Module.Hyperliquid_impl.name
let () = ignore Lighter.Module.Lighter_impl.name
let () = ignore Alpaca.Module.Alpaca_impl.name

let () =
  Logging.init ();
  let args = parse_args () in
  let offline = args.cache_only in
  Lwt_main.run
    (let open Lwt.Infix in
     Lwt.return (Dio_oracle.Oracle_fees.load_dotenv ())
     >>= fun () ->
     let config = Dio_engine.Config.read_config () in
     let grid_trading =
       List.filter
         (fun (tc : Dio_strategies.Strategy_common.trading_config) -> tc.strategy <> "")
         config.trading
     in
     let tcs =
       if args.symbol = ""
       then grid_trading
       else
         List.filter
           (fun (tc : Dio_strategies.Strategy_common.trading_config) ->
             String.lowercase_ascii tc.symbol = String.lowercase_ascii args.symbol)
           grid_trading
     in
     if tcs = []
     then Lwt.fail_with (Printf.sprintf "no trading entries match '%s'" args.symbol)
     else
       (* Warm real fees where the venue adapter provides them and the network is
          available; explicit config fees always win and stay untouched. *)
       (if offline
        then Lwt.return ()
        else
          Lwt_list.iter_s
            (fun (tc : Dio_strategies.Strategy_common.trading_config) ->
              if Option.is_none tc.maker_fee
              then
                Lwt.catch
                  (fun () ->
                    Dio_oracle.Oracle_fees.resolved_fees
                      ~exchange:tc.exchange
                      ~symbol:tc.symbol
                      ~testnet:tc.testnet
                    >|= fun _ -> ())
                  (fun _ -> Lwt.return ())
              else Lwt.return ())
            tcs)
       >>= fun () ->
       (* Resolve the quote/base pool each asset is sized against: the venue's REAL
          account balance by default (shared across the venue's assets), or a synthetic
          pool when --synthetic / --quote / --base was given. *)
       let bal_cache
         : (string * bool, Dio_oracle.Oracle_balances.snapshot option) Hashtbl.t
         =
         Hashtbl.create 8
       in
       Lwt_list.mapi_s
         (fun i (tc : Dio_strategies.Strategy_common.trading_config) ->
           if not args.live
           then Lwt.return (i, tc, args.quote, args.base)
           else (
             let key = String.lowercase_ascii tc.exchange, tc.testnet in
             let got =
               match Hashtbl.find_opt bal_cache key with
               | Some cached -> Lwt.return cached
               | None ->
                 Lwt.catch
                   (fun () ->
                     Dio_oracle.Oracle_balances.fetch_account_live
                       ~exchange:tc.exchange
                       ~testnet:tc.testnet
                       ()
                     >|= function
                     | Ok snapshot -> Some snapshot
                     | Error e ->
                       Printf.eprintf
                         "oracle: balance fetch failed for %s: %s\n"
                         tc.exchange
                         e;
                       None)
                   (fun exn ->
                     Printf.eprintf
                       "oracle: balance fetch exception for %s: %s\n"
                       tc.exchange
                       (Printexc.to_string exn);
                     Lwt.return None)
                 >|= fun res ->
                 Hashtbl.replace bal_cache key res;
                 res
             in
             got
             >|= fun snap ->
             match snap with
             | Some snapshot ->
               let quote =
                 Dio_oracle.Oracle_balances.available_quote
                   snapshot
                   ~quote:(quote_currency_of_symbol tc.symbol)
               in
               let base =
                 Dio_oracle.Oracle_balances.available_asset
                   snapshot
                   ~asset:(base_asset_of_symbol tc.symbol)
               in
               if quote > 0.0
               then i, tc, quote, base
               else (
                 Printf.eprintf
                   "oracle: %s has no available %s; sizing against 0 (pass --synthetic \
                    to fabricate a pool)\n"
                   tc.symbol
                   (quote_currency_of_symbol tc.symbol);
                 i, tc, 0.0, base)
             | None ->
               Printf.eprintf
                 "oracle: no live balance for %s; sizing against 0 (pass --synthetic to \
                  fabricate a pool)\n"
                 tc.exchange;
               i, tc, 0.0, 0.0))
         tcs
       >>= fun sized ->
       Lwt_list.map_s
         (fun (i, (tc : Dio_strategies.Strategy_common.trading_config), quote, base) ->
           analyze_one
             ~args
             ~config:(Some config)
             ~available_quote:quote
             ~available_asset:base
             ~priority:i
             tc)
         sized
       >>= fun rows ->
       let rows = apply_venue_shared_sim rows in
       if args.json then print_json rows args else print_table rows args;
       Lwt.return ())
;;
