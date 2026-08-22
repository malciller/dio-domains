(* dio-oracle.exe - the configuration-tuning entrypoint.

   Runs the exact oracle decision pipeline offline against the configured
   assets and prints the decision surface: the four contract values
   (active / grid_interval / buy_qty / sell_qty) plus internal diagnostics.
   It never touches live balances: pools are synthetic, sized with --quote
   per asset (and --base for sell sizing), so tuning is reproducible.

   History comes from the same all-time merged pipeline as the live runtime
   (venue bars + Yahoo deep history). With --cache-only no network is used:
   only what the disk cache already holds. *)

let usage =
  {|dio-oracle [options]

Options:
  --symbol SYM    Analyze only SYM (default: every trading entry).
  --quote FLOAT   Synthetic quote pool per asset (default 10000.0).
  --base FLOAT    Synthetic base balance for sell sizing (default 0.0).
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
  }

let parse_args () =
  let a =
    { symbol = ""; quote = 10_000.0; base = 0.0; cache_only = false; json = false }
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
      go r
    | "--base" :: v :: r ->
      a.base <- Float.of_string v;
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

(** Per-asset effective knobs: the engine config's "oracle" section with its
    assets overrides keyed by symbol (same resolution as the live runtime). *)
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
  ; outcome : Dio_oracle.Oracle_pipeline.outcome option
  ; error : string
  }

let analyze_one
      ~(args : args)
      ~(config : Dio_engine.Config.config option)
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
           ; available_quote = args.quote
           ; sell_qty = 0.0
           ; bounds
           ; target_survival
           ; min_active_dsurv
           ; fees
           }
         in
         match Dio_oracle.Oracle_pipeline.decide ~inputs with
         | None ->
           Lwt.return
             { exchange = tc.exchange
             ; symbol = tc.symbol
             ; outcome = None
             ; error = "no references"
             }
         | Some o ->
           Lwt.return
             { exchange = tc.exchange; symbol = tc.symbol; outcome = Some o; error = "" }))
    (fun exn ->
       Lwt.return
         { exchange = tc.exchange
         ; symbol = tc.symbol
         ; outcome = None
         ; error = Printexc.to_string exn
         })
;;

let string_of_outcome (o : Dio_oracle.Oracle_pipeline.outcome) =
  Printf.sprintf
    "%-18s %-11s d_surv %.3f | floor %12.6g ath %12.6g atl %12.6g mdd %5.1f%%"
    (Dio_oracle.Oracle_runtime.string_of_regime o.runway.regime)
    (Dio_oracle.Oracle_runtime.string_of_branch o.resolution.branch)
    o.resolution.d_surv
    o.runway.floor_price
    o.refs.ath
    o.refs.atl
    (o.refs.max_drawdown_pct *. 100.0)
;;

let sell_qty_of_args (args : args) =
  Dio_oracle.Oracle_pools.sell_qty_of
    ~base_balance:args.base
    ~reserved_base:0.0
    ~resting_sell_base:0.0
;;

let print_table rows args =
  Printf.printf
    "synthetic pools: quote %.2f base %.6g (per asset)\n\n"
    args.quote
    args.base;
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
           (sell_qty_of_args args)
           (string_of_outcome o))
    rows
;;

let print_json rows args =
  let rows_json =
    List.map
      (fun (r : row) ->
         let common = [ "exchange", `String r.exchange; "symbol", `String r.symbol ] in
         match r.outcome with
         | None -> `Assoc (common @ [ "error", `String r.error ])
         | Some o ->
           let d = o.decision in
           `Assoc
             (common
              @ [ "active", `Bool d.active
                ; "grid_interval", `Float d.grid_interval
                ; "buy_qty", `Float d.buy_qty
                ; "sell_qty", `Float (sell_qty_of_args args)
                ; "d_surv", `Float o.resolution.d_surv
                ; "regime", `String (Dio_oracle.Oracle_runtime.string_of_regime o.runway.regime)
                ; "branch", `String (Dio_oracle.Oracle_runtime.string_of_branch o.resolution.branch)
                ; "floor_price", `Float o.runway.floor_price
                ; "ath", `Float o.refs.ath
                ; "atl", `Float o.refs.atl
                ; "max_drawdown_pct", `Float o.refs.max_drawdown_pct
                ]))
      rows
  in
  print_endline (Yojson.Basic.to_string (`List rows_json))
;;

(* Force the venue adapter modules to link: each registers its oracle
   adapter into Exchange_intf.ln as a side effect, and the fetch pipeline
   dispatches purely through that registry. *)
let () = ignore Kraken.Kraken_module.Kraken_impl.name
let () = ignore Hyperliquid.Module.Hyperliquid_impl.name
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
     let tcs =
       if args.symbol = ""
       then config.trading
       else
         List.filter
           (fun (tc : Dio_strategies.Strategy_common.trading_config) ->
              String.lowercase_ascii tc.symbol = String.lowercase_ascii args.symbol)
           config.trading
     in
     if tcs = []
     then Lwt.fail_with (Printf.sprintf "no trading entries match '%s'" args.symbol)
     else
       (* Warm real fees where the venue adapter provides them and we may use
          the network; explicit config fees always win and stay untouched. *)
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
       Lwt_list.map_s (analyze_one ~args ~config:(Some config)) tcs
       >>= fun rows ->
       if args.json then print_json rows args else print_table rows args;
       Lwt.return ())
;;
