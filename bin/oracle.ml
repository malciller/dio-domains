(* DIO Capital Oracle Report CLI.

   dune exec dio-oracle -- BTC/USD --exchange kraken
   dune exec dio-oracle -- AAPL --exchange alpaca --capital 5000 --target-survival 0.99
   dune exec dio-oracle

   Values default from the config.json trading config (qty, grid_interval,
   sell_mult, maker_fee, accumulation_buffer, data_feed); every value is
   overridable from the CLI. With no SYMBOL, every asset in config.json's
   "trading" list is analyzed on its own configured exchange. Offline mode via
   --from-csv / --from-json loads a single asset series from a file instead of
   the network and still requires a SYMBOL. Portfolio mode is enabled with
   --portfolio or any topology/allocation option. *)

open Lwt.Infix
open Dio_oracle

(* The strategy model this CLI runs: the grid ladder (the only model today).
   The deployment engine and the generic sizing inversions are instantiated
   over it, so the whole oracle pipeline is strategy-parameterized. *)
module G = Oracle_strategy.Grid
module D = Oracle_deploy.Engine (G)
module S = Oracle_replay.Sizing (G)

let pct f = Printf.sprintf "%6.1f%%" (f *. 100.0)

let string_contains haystack needle =
  let hl = String.length haystack in
  let nl = String.length needle in
  let rec go i = i + nl <= hl && (String.sub haystack i nl = needle || go (i + 1)) in
  nl > 0 && go 0
;;

let float_list_of_string s =
  String.split_on_char ',' s |> List.map String.trim |> List.map float_of_string
;;

let int_list_of_string s =
  String.split_on_char ',' s |> List.map String.trim |> List.map int_of_string
;;

let today_iso () =
  let tm = Unix.localtime (Unix.time ()) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

type args =
  { symbol : string
  ; exchange : string
  ; exchange_explicit : bool
  ; capital : float option
  ; portfolio : bool
  ; topology : string option
  ; total_capital : float option
  ; split : string
  ; allocations : string list
  ; transfer_specs : string list
  ; positions_file : string option
  ; save_positions : string option
  ; qty : float option
  ; grid_interval : float option
  ; fee : float option
  ; sell_mult : float option
  ; accumulation_buffer : float option
  ; price_increment : float option
  ; qty_increment : float option
  ; qty_min : float option
  ; min_notional : float option
  ; data_feed : string option
  ; start_price : float option
  ; start_date : string option
  ; end_date : string option
  ; horizons : int list option
  ; thresholds : float list option
  ; percentiles : float list option
  ; gap_tolerance : int option
  ; vol_window : int option
  ; class_name : string option
  ; members : string list option
  ; kappa : int
  ; kappa_explicit : bool
  ; target_survival : float
  ; fng : float option
  ; fng_weight : float
  ; min_active_dsurv : float
  ; qty_cap_mult : float
  ; no_deep_history : bool
  ; weight_by_sessions : bool
  ; json : bool
  ; from_csv : string option
  ; from_json : string option
  ; max_capital : float option
  }

let usage = "Usage: dio-oracle [SYMBOL] [options]"

let parse_args () =
  let symbol = ref "" in
  let exchange = ref "kraken" in
  let exchange_explicit = ref false in
  let capital = ref None in
  let portfolio = ref false in
  let topology = ref None in
  let total_capital = ref None in
  let split = ref "equal" in
  let allocations = ref [] in
  let transfer_specs = ref [] in
  let positions_file = ref None in
  let save_positions = ref None in
  let qty = ref None in
  let grid_interval = ref None in
  let fee = ref None in
  let sell_mult = ref None in
  let accumulation_buffer = ref None in
  let price_increment = ref None in
  let qty_increment = ref None in
  let qty_min = ref None in
  let min_notional = ref None in
  let data_feed = ref "" in
  let start_price = ref None in
  let start_date = ref "" in
  let end_date = ref "" in
  let horizons = ref None in
  let thresholds = ref None in
  let percentiles = ref None in
  let gap_tolerance = ref None in
  let vol_window = ref None in
  let class_name = ref "" in
  let members = ref None in
  let kappa = ref 200 in
  let kappa_explicit = ref false in
  let target_survival = ref 0.99 in
  let fng = ref None in
  let fng_weight = ref 0.5 in
  let min_active_dsurv = ref 0.0 in
  let qty_cap_mult = ref 1.0 in
  let no_deep_history = ref false in
  let weight_by_sessions = ref true in
  let json = ref false in
  let from_csv = ref "" in
  let from_json = ref "" in
  let max_capital = ref None in
  let speclist =
    Arg.align
      [ ( "--exchange"
        , Arg.Symbol
            ( [ "kraken"; "alpaca"; "hyperliquid" ]
            , fun s ->
                exchange := s;
                exchange_explicit := true )
        , " kraken|alpaca|hyperliquid (default kraken; sets the calendar kind)" )
      ; ( "--capital"
        , Arg.Float (fun f -> capital := Some f)
        , " venue-locked pool per venue account (override live balance; the engine \
           allocates it down the config priority order)" )
      ; ( "--portfolio"
        , Arg.Set portfolio
        , " run all requested assets as one shared portfolio (legacy mode)" )
      ; ( "--topology"
        , Arg.String
            (fun path ->
              topology := Some path;
              portfolio := true)
        , " portfolio topology JSON file" )
      ; ( "--total-capital"
        , Arg.Float (fun f -> total_capital := Some f)
        , " total quote capital split equally per venue account (the engine allocates \
           each venue pool down the config priority order)" )
      ; ( "--split"
        , Arg.Symbol
            ( [ "equal"; "explicit" ]
            , fun value ->
                split := value;
                portfolio := true )
        , " portfolio fund split: equal or explicit allocations" )
      ; ( "--allocation"
        , Arg.String
            (fun value ->
              allocations := value :: !allocations;
              portfolio := true)
        , " repeat VENUE/SYMBOL=AMOUNT to fund a venue pool (summed per venue)" )
      ; ( "--transfer"
        , Arg.String
            (fun value ->
              transfer_specs := value :: !transfer_specs;
              portfolio := true)
        , " repeat SESSION:FROM->TO=AMOUNT for a manual portfolio transfer" )
      ; ( "--positions-file"
        , Arg.String
            (fun path ->
              positions_file := Some path;
              portfolio := true)
        , " load saved portfolio pool/base state" )
      ; ( "--save-positions"
        , Arg.String
            (fun path ->
              save_positions := Some path;
              portfolio := true)
        , " save final portfolio pool/base state" )
      ; ( "--qty"
        , Arg.Float (fun f -> qty := Some f)
        , " order size (default from config.json)" )
      ; ( "--gi"
        , Arg.Float (fun f -> grid_interval := Some f)
        , " grid interval %% (default max of config.json grid_interval)" )
      ; ( "--fee"
        , Arg.Float (fun f -> fee := Some f)
        , " maker fee (default live exchange fee per asset; else config.json maker_fee)" )
      ; ( "--sell-mult"
        , Arg.Float (fun f -> sell_mult := Some f)
        , " sell multiplier (default from config.json)" )
      ; ( "--accumulation-buffer"
        , Arg.Float (fun f -> accumulation_buffer := Some f)
        , " quote accumulation buffer (default from config.json)" )
      ; ( "--price-increment"
        , Arg.Float (fun f -> price_increment := Some f)
        , " price tick (default exchange registry / 0.01)" )
      ; ( "--qty-increment"
        , Arg.Float (fun f -> qty_increment := Some f)
        , " lot size (default exchange registry / 0.01)" )
      ; ( "--qty-min"
        , Arg.Float (fun f -> qty_min := Some f)
        , " venue minimum order quantity (default exchange registry / 0.0)" )
      ; ( "--min-notional"
        , Arg.Float (fun f -> min_notional := Some f)
        , " venue minimum order notional in quote; hyperliquid defaults to 10.0 (USDC \
           spot floor), others 0.0" )
      ; ( "--data-feed"
        , Arg.Set_string data_feed
        , " alpaca feed iex|sip (default from config.json)" )
      ; ( "--start-price"
        , Arg.Float (fun f -> start_price := Some f)
        , " grid start price (default last close)" )
      ; ( "--start"
        , Arg.Set_string start_date
        , " start date YYYY-MM-DD for alpaca (default 2010-01-01)" )
      ; ( "--end"
        , Arg.Set_string end_date
        , " end date YYYY-MM-DD for alpaca (default today)" )
      ; ( "--horizons"
        , Arg.String (fun s -> horizons := Some (int_list_of_string s))
        , " comma horizon list in sessions (default 30,90,180,365 crypto / 21,63,126,252 \
           equity)" )
      ; ( "--thresholds"
        , Arg.String (fun s -> thresholds := Some (float_list_of_string s))
        , " comma drawdown thresholds in %% (default 5,10,20,30,40,50)" )
      ; ( "--percentiles"
        , Arg.String (fun s -> percentiles := Some (float_list_of_string s))
        , " comma percentiles (default 50,75,90,95,99)" )
      ; ( "--gap-tolerance"
        , Arg.Int (fun i -> gap_tolerance := Some i)
        , " max allowed missing sessions before refusing to analyze (default 5)" )
      ; ( "--vol-window"
        , Arg.Int (fun i -> vol_window := Some i)
        , " rolling vol window for warmup (default 60)" )
      ; ( "--class"
        , Arg.Set_string class_name
        , " risk class name (default from config.json asset_class)" )
      ; ( "--members"
        , Arg.String
            (fun s ->
              members := Some (String.split_on_char ',' s |> List.map String.trim))
        , " comma member symbols for the class pool" )
      ; ( "--kappa"
        , Arg.Int
            (fun i ->
              kappa := i;
              kappa_explicit := true)
        , " class weight in the blend (default 200; a config.json per-class kappa wins \
           unless this is passed)" )
      ; ( "--target-survival"
        , Arg.Float (fun f -> target_survival := f)
        , " target blended survival for sizing (default 0.99)" )
      ; ( "--fng"
        , Arg.Float (fun f -> fng := Some f)
        , " Fear & Greed override (0-100); default fetches the live index (crypto only; \
           equities use the survival model alone)" )
      ; ( "--fng-weight"
        , Arg.Float (fun f -> fng_weight := f)
        , " weight of the F&G side in the gi blend (default 0.5; the survival side \
           carries the rest)" )
      ; ( "--min-active-dsurv"
        , Arg.Float (fun f -> min_active_dsurv := f)
        , " assets whose replayed D_surv stays below this are recommended inactive and \
           their capital passes down (default 0.0 = fundable means active)" )
      ; ( "--qty-cap-mult"
        , Arg.Float (fun f -> qty_cap_mult := f)
        , " deployment ceiling as a multiple of the config qty (default 1.0: each \
           asset's qty is capped at its config qty so surplus capital passes down the \
           priority order; 0.0 = uncapped, the priority asset deploys the whole pool)" )
      ; ( "--no-deep-history"
        , Arg.Set no_deep_history
        , " disable the Yahoo deep-history extension (venue-feed bars only; the venue \
           API caps some feeds at ~2 years)" )
      ; ( "--no-weight-by-sessions"
        , Arg.Unit (fun () -> weight_by_sessions := false)
        , " equal-weight each class member instead of by session count" )
      ; ( "--max-capital"
        , Arg.Float (fun f -> max_capital := Some f)
        , " upper bound of the capital binary search (default 1e9)" )
      ; "--json", Arg.Set json, " emit JSON report"
      ; ( "--from-csv"
        , Arg.Set_string from_csv
        , " load the asset series from a CSV file (offline)" )
      ; ( "--from-json"
        , Arg.Set_string from_json
        , " load the asset series from a JSON file (offline)" )
      ]
  in
  Arg.parse speclist (fun s -> symbol := s) usage;
  { symbol = !symbol
  ; exchange = !exchange
  ; exchange_explicit = !exchange_explicit
  ; capital = !capital
  ; portfolio = !portfolio
  ; topology = !topology
  ; total_capital = !total_capital
  ; split = !split
  ; allocations = List.rev !allocations
  ; transfer_specs = List.rev !transfer_specs
  ; positions_file = !positions_file
  ; save_positions = !save_positions
  ; qty = !qty
  ; grid_interval = !grid_interval
  ; fee = !fee
  ; sell_mult = !sell_mult
  ; accumulation_buffer = !accumulation_buffer
  ; price_increment = !price_increment
  ; qty_increment = !qty_increment
  ; qty_min = !qty_min
  ; min_notional = !min_notional
  ; data_feed = (if !data_feed = "" then None else Some !data_feed)
  ; start_price = !start_price
  ; start_date = (if !start_date = "" then None else Some !start_date)
  ; end_date = (if !end_date = "" then None else Some !end_date)
  ; horizons = !horizons
  ; thresholds = !thresholds
  ; percentiles = !percentiles
  ; gap_tolerance = !gap_tolerance
  ; vol_window = !vol_window
  ; class_name = (if !class_name = "" then None else Some !class_name)
  ; members = !members
  ; kappa = !kappa
  ; kappa_explicit = !kappa_explicit
  ; target_survival = !target_survival
  ; fng = !fng
  ; fng_weight = !fng_weight
  ; min_active_dsurv = !min_active_dsurv
  ; qty_cap_mult = !qty_cap_mult
  ; no_deep_history = !no_deep_history
  ; weight_by_sessions = !weight_by_sessions
  ; json = !json
  ; from_csv = (if !from_csv = "" then None else Some !from_csv)
  ; from_json = (if !from_json = "" then None else Some !from_json)
  ; max_capital = !max_capital
  }
;;

(** Per-run cache of fetched series, shared across assets and class members so
    e.g. ETH/USD is only downloaded once. *)
let fetch_cache : (string * string, Oracle_types.series) Hashtbl.t = Hashtbl.create 32

(** Fetch one symbol's daily series over the network (cached per run). *)
let fetch_series_for (a : args) ~(exchange : string) (symbol : string)
  : Oracle_types.series Lwt.t
  =
  match Hashtbl.find_opt fetch_cache (exchange, symbol) with
  | Some s -> Lwt.return s
  | None ->
    let fetch =
      match exchange with
      | "kraken" ->
        Oracle_fetch_kraken.fetch_ohlc ~symbol ()
        >|= Oracle_fetch_kraken.series_of_bars ~symbol
      | "hyperliquid" ->
        Oracle_fetch_hyperliquid.fetch_candles ~symbol ()
        >|= Oracle_fetch_hyperliquid.series_of_bars ~symbol
      | "alpaca" ->
        let start = Option.value a.start_date ~default:"2010-01-01" in
        let end_date = Option.value a.end_date ~default:(today_iso ()) in
        let feed = Option.value a.data_feed ~default:"iex" in
        Oracle_fetch_alpaca.fetch_bars ~feed ~symbol ~start_date:start ~end_date ()
        >|= Oracle_fetch_alpaca.series_of_bars ~symbol
      | _ -> invalid_arg "unknown exchange"
    in
    fetch
    >|= fun series ->
    Hashtbl.add fetch_cache (a.exchange, symbol) series;
    series
;;

let fetch_series (a : args) (symbol : string) : Oracle_types.series Lwt.t =
  fetch_series_for a ~exchange:a.exchange symbol
;;

(** Extend a venue series backward with the Yahoo deep history for the same
    underlying asset (venue bars win on overlap; nothing is synthesized).
    Returns the deepened series and the number of deep bars added. *)
let deepen_series (a : args) ~(exchange : string) (series : Oracle_types.series)
  : (Oracle_types.series * int) Lwt.t
  =
  let venue_bars = series.bars in
  if
    a.no_deep_history
    || Array.length venue_bars = 0
    || Option.is_some a.from_csv
    || Option.is_some a.from_json
  then Lwt.return (series, 0)
  else (
    match Oracle_fetch_yahoo.symbol_of ~exchange series.symbol with
    | None -> Lwt.return (series, 0)
    | Some yahoo_symbol ->
      let venue_first = venue_bars.(0).Oracle_types.date in
      let end_date = Oracle_calendar.add_days venue_first (-1) in
      Oracle_fetch_yahoo.fetch_daily
        ~start_date:"2015-01-01"
        ~symbol:yahoo_symbol
        ~end_date
        ()
      >|= fun deep_bars ->
      let deep = Oracle_fetch_yahoo.series_of_bars ~symbol:yahoo_symbol deep_bars in
      Oracle_fetch_yahoo.merge_series ~venue:series ~deep)
;;

(** Load the class member pool: explicit --members when online, config.json
    "classes" members for the resolved class when online, otherwise the asset
    alone (offline mode). Members are deepened like the asset itself. *)
let load_members
      (a : args)
      (classes : (string * Dio_engine.Config.class_pool) list)
      ~(class_name : string)
      (asset : Oracle_types.series)
  : Oracle_types.series list Lwt.t
  =
  let syms =
    match a.members with
    | Some ms when ms <> [] -> ms
    | _ ->
      (match List.assoc_opt class_name classes with
       | Some pool when pool.members <> [] -> pool.members
       | _ -> [])
  in
  if syms = []
  then (
    Printf.eprintf
      "oracle: no class members known for '%s' (add a \"classes\" entry in config.json \
       or pass --members); using asset alone\n"
      class_name;
    Lwt.return [ asset ])
  else (
    let rec go = function
      | [] -> Lwt.return []
      | s :: rest ->
        fetch_series a s
        >>= fun series ->
        go rest
        >>= fun acc ->
        if Array.length series.bars = 0
        then Lwt.return acc
        else
          deepen_series a ~exchange:a.exchange series
          >>= fun (series, _) -> Lwt.return (series :: acc)
    in
    go syms
    >>= fun members -> if members = [] then Lwt.return [ asset ] else Lwt.return members)
;;

(** Per-run Fear & Greed: an explicit --fng wins; otherwise fetch the live
    index (blocking, cached, fallback 50). Offline runs get None (the caller
    decides per calendar kind whether F&G applies at all). *)
let resolve_fng (a : args) =
  match a.fng with
  | Some f -> Some f
  | None ->
    if Option.is_some a.from_csv || Option.is_some a.from_json
    then None
    else (
      try Some (Cmc.Fear_and_greed.fetch_and_cache_sync ()) with
      | _ -> None)
;;

(** Venue account identity: venue + quote + testnet (capital is locked per
    account; all of the account's assets draw from one pool). *)
let account_of_task (task : Oracle_tasks.task) =
  Oracle_topology.key
    ~venue:task.exchange
    ~symbol:task.symbol
    ~testnet:task.config.testnet
    ()
;;

let same_account
      (left : Oracle_topology.instrument_key)
      (right : Oracle_topology.instrument_key)
  =
  left.venue = right.venue && left.testnet = right.testnet && left.quote = right.quote
;;

let account_id (account : Oracle_topology.instrument_key) =
  Printf.sprintf
    "%s/%s%s"
    account.venue
    account.quote
    (if account.testnet then "@testnet" else "")
;;

(** Tasks grouped by venue account, preserving config.json order (priority
    order within each account). *)
let group_by_account (tasks : Oracle_tasks.task list) =
  let accounts = ref [] in
  List.iter
    (fun task ->
       let account = account_of_task task in
       if not (List.exists (fun a -> same_account a account) !accounts)
       then accounts := account :: !accounts)
    tasks;
  let accounts = List.rev !accounts in
  List.map
    (fun account ->
       ( account
       , List.filter (fun task -> same_account account (account_of_task task)) tasks ))
    accounts
;;

(** Venue-locked pools per account, mirroring the portfolio capital rules:
    an explicit --capital is the pool for every account; --total-capital
    splits equally per account; offline defaults to 1000 per account; online
    with neither fetches each account's live available quote balance. *)
let venue_pools (a : args) ~(offline : bool) (tasks : Oracle_tasks.task list)
  : (Oracle_topology.instrument_key * float) list
  =
  let accounts = List.map fst (group_by_account tasks) in
  let pool_for (account : Oracle_topology.instrument_key) =
    match a.capital with
    | Some capital -> capital
    | None ->
      (match a.total_capital with
       | Some total ->
         if accounts = [] then 0.0 else total /. float_of_int (List.length accounts)
       | None when offline -> 1000.0
       | None ->
         let task =
           match
             List.find_opt
               (fun (task : Oracle_tasks.task) ->
                  same_account account (account_of_task task))
               tasks
           with
           | Some task -> task
           | None -> failwith "oracle: no task on venue account"
         in
         (match Lwt_main.run (Oracle_balances.fetch_task task) with
          | Ok snapshot ->
            let balance = Oracle_balances.available_quote snapshot ~quote:account.quote in
            if balance < 0.0 then 0.0 else balance
          | Error error ->
            failwith
              (Printf.sprintf
                 "oracle: balance fetch failed for %s: %s"
                 (account_id account)
                 error)))
  in
  List.map (fun account -> account, pool_for account) accounts
;;

(** Replay capital for the supporting analysis: the deployment's deployed
    capital (the ladder's consumption through the governing drawdown). *)

type coverage_row =
  Oracle_types.horizon
  * Oracle_types.historical_path_coverage
  * Oracle_types.sizing_result
  * Oracle_types.sizing_result
  * Oracle_types.sizing_result
(* horizon, path coverage, static min capital, max qty, empirical min
     capital *)

let horizon_label_of (c_ : Oracle_types.historical_path_coverage) =
  c_.Oracle_types.horizon.Oracle_types.label
;;

(** The engine's headline block for one asset: the recommended qty/gi, the
    deployed capital, the reserve passed down, and the verification coverage. *)
let deployment_block
      (a : args)
      ~(account : Oracle_topology.instrument_key)
      ~(venue_pool : float)
      ~(deployment : Oracle_types.asset_deployment)
      ~(index : int)
      ~(n_tasks : int)
      ~(gi_lo : float)
      ~(gi_hi : float)
      (b : Buffer.t)
  =
  let line fmt =
    Printf.ksprintf
      (fun s ->
         Buffer.add_string b s;
         Buffer.add_char b '\n')
      fmt
  in
  let d = deployment in
  if index = 1
  then (
    line "=== DIO Capital Oracle ===";
    line
      "Venue: %s   Pool: %.2f %s%s"
      (account_id account)
      venue_pool
      account.quote
      (match d.Oracle_types.parameter_components.Oracle_types.fng with
       | Some fng ->
         Printf.sprintf
           "   F&G: %.0f (gi weight %.2f)"
           fng
           d.parameter_components.fng_weight
       | None -> "   (equity: oracle model only)");
    line "");
  line "  [%d/%d] %s" index n_tasks a.symbol;
  if not d.active
  then (
    line "    INACTIVE: %s" d.reason;
    line "    capital passes to the next asset");
  if d.active
  then (
    let pc = d.parameter_components in
    let gi_desc =
      match pc.fng_parameter, pc.fng with
      | Some fng_parameter, Some fng ->
        Printf.sprintf
          "fng %.0f -> %.2f%% | surv %.2f%% | w %.2f"
          fng
          fng_parameter
          pc.survival_parameter
          pc.fng_weight
      | _ -> Printf.sprintf "surv %.2f%%" pc.survival_parameter
    in
    line "    ACTIVE   qty %.4f   gi %.2f%%   (%s)" d.qty d.parameter gi_desc;
    line
      "    governing %s   d_gov %s   D_surv %s (replay)   min quote DD %s"
      d.governing_horizon
      (pct d.d_gov)
      (pct d.d_surv)
      (pct d.min_quote_drawdown);
    let reserve =
      if d.remainder +. 1e-9 > 0.0
      then Printf.sprintf "   reserve %.2f (passes to the next asset)" d.remainder
      else ""
    in
    line "    deployed %.2f / share %.2f%s" d.deployed d.pool_share reserve;
    let cov =
      List.map
        (fun (c : Oracle_types.deployment_coverage) ->
           Printf.sprintf "%s %s" c.horizon_label (pct c.blended_coverage))
        d.coverage
      |> String.concat "  "
    in
    line "    coverage: %s" cov;
    line
      "    config: \"qty\": \"%s\"   \"grid_interval\": [%.2f, %.2f]"
      (if d.qty = 0.0 then "0.0" else Printf.sprintf "%.6g" d.qty)
      gi_lo
      gi_hi);
  List.iter (fun (w : string) -> line "    warning: %s" w) d.warnings
;;

(** The gi tuning surface for one asset: each candidate interval's deployment
    and whether it clears the target on the replayed path. *)
let tuning_block (a : args) ~(deployment : Oracle_types.asset_deployment) (b : Buffer.t) =
  let line fmt =
    Printf.ksprintf
      (fun s ->
         Buffer.add_string b s;
         Buffer.add_char b '\n')
      fmt
  in
  let d = deployment in
  line "  Tuning surface (target blended survival %.1f%%):" (a.target_survival *. 100.0);
  line
    "    %-7s %11s %12s %11s %11s %8s %9s"
    "gi"
    "qty"
    "deployed"
    "D_surv(s)"
    "D_surv(r)"
    "cov"
    "profit";
  List.iter
    (fun (r : Oracle_types.deployment_row) ->
       let cov =
         List.fold_left
           (fun acc (c : Oracle_types.deployment_coverage) ->
              Float.min acc c.blended_coverage)
           1.0
           r.coverage
       in
       line
         "    %-7s %11.5f %12.2f %11s %11s %8s %9.5f"
         (Printf.sprintf "%.2f%%" r.parameter)
         r.qty
         r.deployed
         (pct r.d_surv_static)
         (pct r.d_surv_replay)
         (if r.coverage = [] then "-" else pct cov)
         r.profit_proxy)
    d.tuning_rows;
  line ""
;;

(** Human-readable report: the deployment headline (engine decision) followed
    by the supporting per-asset analysis (MFD tables, replay, inverse
    sizing). [replay_out]/[coverages] are the supporting replay data at the
    deployment's sizing; absent when the asset is inactive. *)
let report_text
      (a : args)
      ~(account : Oracle_topology.instrument_key)
      ~(venue_pool : float)
      ~(deployment : Oracle_types.asset_deployment)
      ~(index : int)
      ~(n_tasks : int)
      ~(gi_lo : float)
      ~(gi_hi : float)
      ~(deep_bars : int)
      (grid : Dio_strategies.Grid_core.config)
      (members : Oracle_types.series list)
      (class_name : string)
      (r : Oracle.result)
      (replay_out : G.outcome option)
      (coverages : coverage_row list)
  =
  let b = Buffer.create 4096 in
  let line fmt =
    Printf.ksprintf
      (fun s ->
         Buffer.add_string b s;
         Buffer.add_char b '\n')
      fmt
  in
  deployment_block a ~account ~venue_pool ~deployment ~index ~n_tasks ~gi_lo ~gi_hi b;
  if deployment.Oracle_types.active then tuning_block a ~deployment b;
  line "Supporting analysis (asset-level):";
  line
    "Symbol: %s   Exchange: %s   Class: %s (%d members, kappa %d)"
    a.symbol
    a.exchange
    class_name
    (List.length members)
    a.kappa;
  line
    "Bars: %d  (%s .. %s)   max gap: %d (tolerance %d)%s"
    r.n_sessions
    r.first_date
    r.last_date
    r.max_gap
    (Option.value a.gap_tolerance ~default:5)
    (if deep_bars > 0
     then
       Printf.sprintf
         "   (of which %d deep-history bars from Yahoo for %s)"
         deep_bars
         a.symbol
     else "");
  line "";
  line "MFD percentile tables (%s of max drawdown at each percentile):" "%";
  line
    "  columns: asset = linear-interp percentile of the asset's own MFD windows; class = \
     weighted pooled percentile of the class members; blended = inverted z-blend CDF";
  line
    "  sample sizes: n = asset overlapping starts; asset_eff = the asset's own \
     non-overlapping windows; class_eff = the pooled class effective sample size";
  let class_tbls =
    match r.class_estimates with
    | c_ :: _ -> c_.Oracle_types.percentile_tables
    | [] -> []
  in
  let blended_tbls =
    List.map (fun bt -> bt.Oracle_types.table) r.blended_percentile_tables
  in
  let n = List.length r.percentile_tables in
  for i = 0 to n - 1 do
    let at = List.nth r.percentile_tables i in
    let ct = if i < List.length class_tbls then List.nth class_tbls i else at in
    let bt = if i < List.length blended_tbls then List.nth blended_tbls i else at in
    let rows3 =
      List.map2 (fun cr (ar, br) -> ar, cr, br) ct.rows (List.combine at.rows bt.rows)
    in
    let row =
      List.map
        (fun ( (ar : Oracle_types.percentile_row)
             , (cr : Oracle_types.percentile_row)
             , (br : Oracle_types.percentile_row) ) ->
           Printf.sprintf
             "P%g %s/%s/%s"
             ar.percentile
             (pct ar.mfd)
             (pct cr.mfd)
             (pct br.mfd))
        rows3
      |> String.concat "   "
    in
    line
      "  %-7s n=%d asset_eff=%d class_eff=%d   %s"
      at.horizon_label
      at.n_starts
      at.n_eff
      ct.n_eff
      row
  done;
  (match replay_out with
   | None -> ()
   | Some replay_out ->
     line "";
     line
       "Grid replay (qty %.4f  gi %.2f%%  capital %.2f  fee %.3f%%):"
       grid.qty
       grid.grid_interval_pct
       grid.start_quote
       (grid.maker_fee *. 100.0);
     line "  gates: qty_min %.4f  min_notional %.2f quote" grid.qty_min grid.min_notional;
     line
       "  D_surv = %s (%s)"
       (pct replay_out.d_surv)
       (if replay_out.exhausted then "capital low hit" else "never exhausted");
     (match replay_out.halt_cause with
      | None -> ()
      | Some `Capital ->
        line
          "  halt cause: quote capital exhausted - the grid could no longer fund the \
           (dynamically sized) next buy order"
      | Some `Not_placeable ->
        line
          "  halt cause: parameter sizing - the required buy quantity stays below \
           qty_min even after dynamic up-sizing; more capital cannot fix this");
     line
       "  fills: %d buy / %d sell   min quote drawdown %s"
       replay_out.buy_fills
       replay_out.sell_fills
       (pct replay_out.min_quote_drawdown);
     line "";
     line "Historical path coverage at D_surv:";
     line "  %-7s %10s %10s %10s" "horizon" "asset" "class" "blended";
     List.iter
       (fun ((_h, c_, _cap, _q, _emp) : coverage_row) ->
          line
            "  %-7s %10s %10s %10s"
            (horizon_label_of c_)
            (pct c_.asset_coverage)
            (pct c_.class_coverage)
            (pct c_.blended_coverage))
       coverages;
     line "";
     line "Inverse sizing (target blended survival %.1f%%):" (a.target_survival *. 100.0);
     line
       "  %-7s %14s %8s %8s   %10s %8s"
       "horizon"
       "min-capital"
       "cov"
       "d_surv"
       "max-qty"
       "cov";
     List.iter
       (fun ((_h, c_, cap, q, _emp) : coverage_row) ->
          let cov_str reachable v =
            let s = pct v in
            if reachable then s else s ^ " *"
          in
          line
            "  %-7s %14.2f %8s %8s   %10.4f %8s"
            (horizon_label_of c_)
            cap.value
            (cov_str cap.reachable cap.coverage)
            (pct cap.d_surv)
            q.value
            (cov_str q.reachable q.coverage))
       coverages;
     line "";
     line
       "Empirical min capital (advisory, from actual path replay; the static sizing \
        above is the safe recommendation):";
     line "  %-7s %14s %8s %8s %10s" "horizon" "empirical" "cov" "d_surv" "buffer x";
     List.iter
       (fun ((_h, c_, cap, _q, emp) : coverage_row) ->
          let cov_str reachable v =
            let s = pct v in
            if reachable then s else s ^ " *"
          in
          let buffer =
            if emp.reachable && emp.value > 0.0 then cap.value /. emp.value else 0.0
          in
          line
            "  %-7s %14.2f %8s %8s %10.2f"
            (horizon_label_of c_)
            emp.value
            (cov_str emp.reachable emp.coverage)
            (pct emp.d_surv)
            buffer)
       coverages;
     let any_unreachable =
       List.exists
         (fun ((_h, _c_, cap, q, emp) : coverage_row) ->
            (not cap.reachable) || (not q.reachable) || not emp.reachable)
         coverages
     in
     if any_unreachable
     then
       line
         "  * target not reached: no parameter within the search bounds clears it (the \
          required capital exceeds --max-capital, or the required qty is below \
          --qty-increment)");
  Buffer.contents b
;;

(** JSON report. *)
let report_json
      (a : args)
      ~(account : Oracle_topology.instrument_key)
      ~(venue_pool : float)
      ~(deployment : Oracle_types.asset_deployment)
      ~(gi_lo : float)
      ~(gi_hi : float)
      ~(deep_bars : int)
      (grid : Dio_strategies.Grid_core.config)
      (asset : Oracle_types.series)
      (members : Oracle_types.series list)
      (class_name : string)
      (r : Oracle.result)
      (replay_out : G.outcome option)
      (coverages : coverage_row list)
  : Yojson.Safe.t
  =
  let gap_j (g : Oracle_types.gap) =
    `Assoc
      [ "after", `String g.after
      ; "before", `String g.before
      ; "missing_days", `Int g.missing_days
      ]
  in
  let percentile_rows (t : Oracle_types.percentile_table) =
    `Assoc
      (("n_starts", `Int t.n_starts)
       :: ("n_eff", `Int t.n_eff)
       :: List.map
            (fun (row : Oracle_types.percentile_row) ->
               Printf.sprintf "p%g" row.percentile, `Float row.mfd)
            t.rows)
  in
  let asset_tbls = List.map percentile_rows r.percentile_tables in
  let sizing_j ((_h, c_, cap, q, emp) : coverage_row) =
    `Assoc
      [ "horizon", `String (horizon_label_of c_)
      ; "asset_coverage", `Float c_.asset_coverage
      ; "class_coverage", `Float c_.class_coverage
      ; "blended_coverage", `Float c_.blended_coverage
      ; "min_capital", `Float cap.value
      ; "min_capital_d_surv", `Float cap.d_surv
      ; "min_capital_coverage", `Float cap.coverage
      ; "min_capital_reachable", `Bool cap.reachable
      ; "max_qty", `Float q.value
      ; "max_qty_coverage", `Float q.coverage
      ; "max_qty_reachable", `Bool q.reachable
      ; "empirical_min_capital", `Float emp.value
      ; "empirical_min_capital_d_surv", `Float emp.d_surv
      ; "empirical_min_capital_coverage", `Float emp.coverage
      ; "empirical_min_capital_reachable", `Bool emp.reachable
      ; ( "empirical_capital_buffer_ratio"
        , `Float
            (if emp.reachable && emp.value > 0.0 then cap.value /. emp.value else 0.0) )
      ]
  in
  let coverage_j (c : Oracle_types.deployment_coverage) =
    c.horizon_label, `Float c.blended_coverage
  in
  let row_j (r : Oracle_types.deployment_row) =
    `Assoc
      [ "gi", `Float r.parameter
      ; "qty", `Float r.qty
      ; "deployed", `Float r.deployed
      ; "d_surv_static", `Float r.d_surv_static
      ; "d_surv_replay", `Float r.d_surv_replay
      ; "min_quote_drawdown", `Float r.min_quote_drawdown
      ; "passed", `Bool r.passed
      ; "profit_proxy", `Float r.profit_proxy
      ; "coverage", `Assoc (List.map coverage_j r.coverage)
      ]
  in
  let d = deployment in
  let pc = d.parameter_components in
  let deployment_j =
    `Assoc
      [ "active", `Bool d.active
      ; "reason", `String d.reason
      ; "pool_share", `Float d.pool_share
      ; "deployed", `Float d.deployed
      ; "remainder", `Float d.remainder
      ; "governing_horizon", `String d.governing_horizon
      ; "d_gov", `Float d.d_gov
      ; "qty", `Float d.qty
      ; "gi", `Float d.parameter
      ; "fng", Option.fold ~none:`Null ~some:(fun f -> `Float f) pc.fng
      ; "gi_fng", Option.fold ~none:`Null ~some:(fun g -> `Float g) pc.fng_parameter
      ; "gi_survival", `Float pc.survival_parameter
      ; "gi_resolved", `Float pc.resolved_parameter
      ; "fng_weight", `Float pc.fng_weight
      ; "d_surv", `Float d.d_surv
      ; "min_quote_drawdown", `Float d.min_quote_drawdown
      ; "coverage", `Assoc (List.map coverage_j d.coverage)
      ; "warnings", `List (List.map (fun w -> `String w) d.warnings)
      ; "tuning", `List (List.map row_j d.tuning_rows)
      ; ( "config"
        , `Assoc
            [ "qty", `String (if d.qty = 0.0 then "0.0" else Printf.sprintf "%.6g" d.qty)
            ; "grid_interval", `List [ `Float gi_lo; `Float gi_hi ]
            ] )
      ]
  in
  let replay_j =
    match replay_out with
    | None -> `Null
    | Some replay_out ->
      `Assoc
        [ "d_surv", `Float replay_out.d_surv
        ; "exhausted", `Bool replay_out.exhausted
        ; "grid_halted", `Bool (replay_out.halt_cause <> None)
        ; ( "halt_cause"
          , match replay_out.halt_cause with
            | None -> `Null
            | Some `Capital -> `String "capital"
            | Some `Not_placeable -> `String "not_placeable" )
        ; "min_quote_drawdown", `Float replay_out.min_quote_drawdown
        ; "buy_fills", `Int replay_out.buy_fills
        ; "sell_fills", `Int replay_out.sell_fills
        ]
  in
  `Assoc
    [ "symbol", `String a.symbol
    ; "exchange", `String a.exchange
    ; "venue", `String (account_id account)
    ; "venue_pool", `Float venue_pool
    ; "deployment", deployment_j
    ; "n_bars", `Int r.n_sessions
    ; ( "history"
      , `Assoc
          [ "deep_bars", `Int deep_bars
          ; "deep_source", `String "yahoo"
          ; "venue_bars", `Int (max 0 (r.n_sessions - deep_bars))
          ] )
    ; "first_date", `String r.first_date
    ; "last_date", `String r.last_date
    ; "max_gap", `Int r.max_gap
    ; "gaps", `List (List.map gap_j r.gaps)
    ; ( "class"
      , `Assoc
          [ "name", `String class_name
          ; "kappa", `Int a.kappa
          ; "member_count", `Int (List.length members)
          ] )
    ; ( "percentiles"
      , `Assoc
          (List.map2
             (fun (t : Oracle_types.percentile_table) j -> t.horizon_label, j)
             r.percentile_tables
             asset_tbls) )
    ; ( "grid"
      , `Assoc
          [ "qty", `Float grid.qty
          ; "sell_mult", `Float grid.sell_mult
          ; "grid_interval_pct", `Float grid.grid_interval_pct
          ; "maker_fee", `Float grid.maker_fee
          ; "accumulation_buffer", `Float grid.accumulation_buffer
          ; "price_increment", `Float grid.price_increment
          ; "qty_increment", `Float grid.qty_increment
          ; "qty_min", `Float grid.qty_min
          ; "min_notional", `Float grid.min_notional
          ; "start_price", `Float grid.start_price
          ; "start_quote", `Float grid.start_quote
          ] )
    ; "replay", replay_j
    ; "sizing", `List (List.map sizing_j coverages)
    ; ( "last_close"
      , `Float
          (if Array.length asset.bars = 0
           then 0.0
           else asset.bars.(Array.length asset.bars - 1).close) )
    ]
;;

(** Run the full per-asset pipeline: the deployment engine's decision against
    the venue pool share (threading the remaining pool back to the caller),
    then the supporting analysis at the deployment's sizing. In text mode
    prints the report and returns [None]; in JSON mode returns the report
    value without printing. Returns the pool remaining after this asset. *)
let run_one
      (a : args)
      (classes : (string * Dio_engine.Config.class_pool) list)
      (task : Oracle_tasks.task)
      ~(pool : float)
      ~(fng : float option)
      ~(fng_weight : float)
      ~(min_active_dsurv : float)
      ~(index : int)
      ~(n_tasks : int)
  : Yojson.Safe.t option * float
  =
  let a =
    { a with symbol = task.Oracle_tasks.symbol; exchange = task.Oracle_tasks.exchange }
  in
  let account = account_of_task task in
  let calendar_kind = Oracle_tasks.calendar_kind_of_exchange a.exchange in
  let offline = Option.is_some a.from_csv || Option.is_some a.from_json in
  let tc = task.Oracle_tasks.config in
  let class_name =
    match a.class_name, tc.asset_class with
    | Some cl, _ -> cl
    | None, Some cl -> cl
    | None, None ->
      failwith
        (Printf.sprintf
           "oracle: no risk class for %s/%s: set \"asset_class\" in its config.json \
            trading entry or pass --class"
           task.Oracle_tasks.exchange
           task.Oracle_tasks.symbol)
  in
  (* Effective kappa: explicit --kappa > per-class kappa in config.json >
     the 200 default. *)
  let kappa =
    if a.kappa_explicit
    then a.kappa
    else (
      match List.assoc_opt class_name classes with
      | Some pool -> Option.value pool.kappa ~default:200
      | None -> 200)
  in
  let a = { a with kappa } in
  let tc = Lwt_main.run (Oracle_fees.enrich tc ~offline) in
  let asset =
    match a.from_csv, a.from_json with
    | Some path, _ -> Oracle_loader.load_csv_file ~symbol:a.symbol ~calendar_kind ~path
    | _, Some path -> Oracle_loader.load_json_file ~symbol:a.symbol ~calendar_kind ~path
    | None, None -> Lwt_main.run (fetch_series a a.symbol)
  in
  let asset, deep_bars = Lwt_main.run (deepen_series a ~exchange:a.exchange asset) in
  let members =
    if offline then [ asset ] else Lwt_main.run (load_members a classes ~class_name asset)
  in
  let start_price =
    Option.value
      a.start_price
      ~default:
        (if Array.length asset.bars = 0
         then 0.0
         else asset.bars.(Array.length asset.bars - 1).close)
  in
  (* The config's gi range drives the deployment (the engine picks within it);
     an explicit --gi fixes the range to that single value. *)
  let gi_lo, gi_hi =
    match a.grid_interval with
    | Some g -> g, g
    | None -> tc.grid_interval
  in
  let grid_interval_pct = gi_hi in
  let grid =
    Grid_adapter.of_trading_config tc ~start_price ~start_quote:0.0 ~grid_interval_pct
  in
  let grid =
    { grid with
      qty = Option.value a.qty ~default:grid.qty
    ; sell_mult = Option.value a.sell_mult ~default:grid.sell_mult
    ; maker_fee = Option.value a.fee ~default:grid.maker_fee
    ; accumulation_buffer =
        Option.value a.accumulation_buffer ~default:grid.accumulation_buffer
    ; price_increment = Option.value a.price_increment ~default:grid.price_increment
    ; qty_increment = Option.value a.qty_increment ~default:grid.qty_increment
    ; qty_min = Option.value a.qty_min ~default:grid.qty_min
    ; min_notional = Option.value a.min_notional ~default:grid.min_notional
    }
  in
  let equity_sessions =
    if a.exchange = "alpaca"
    then (
      let start = Option.value a.start_date ~default:"2010-01-01" in
      let end_date = Option.value a.end_date ~default:(today_iso ()) in
      try
        let dates =
          Lwt_main.run (Oracle_fetch_alpaca.fetch_calendar ~start_date:start ~end_date ())
        in
        if dates = []
        then Some Oracle_sessions.business_weekday
        else Some (Oracle_fetch_alpaca.model_of_calendar_dates dates)
      with
      | exn ->
        Printf.eprintf
          "oracle: calendar fetch failed (%s); using business weekdays\n"
          (Printexc.to_string exn);
        Some Oracle_sessions.business_weekday)
    else None
  in
  let horizons =
    match a.horizons with
    | Some ns ->
      List.map
        (fun n ->
           { Oracle_types.label = Oracle_types.horizon_label calendar_kind n
           ; sessions = n
           ; calendar_days = Oracle_types.calendar_days_of_sessions calendar_kind n
           })
        ns
    | None -> (Oracle.default_config ~calendar_kind).horizons
  in
  let thresholds_pct = Option.value a.thresholds ~default:Oracle.default_thresholds_pct in
  let percentiles = Option.value a.percentiles ~default:Oracle.default_percentiles in
  let vol_window = Option.value a.vol_window ~default:60 in
  let gap_tolerance = Option.value a.gap_tolerance ~default:5 in
  (* Adapt the warmup to the history and the longest horizon, and drop the
     horizons the history cannot support, so a short asset analyzes on what
     fits instead of hard-failing (each horizon needs warmup + horizon + 2
     bars; the vol window must shrink when the history is short). *)
  let n_bars = Array.length asset.bars in
  let max_h =
    List.fold_left (fun acc (h : Oracle_types.horizon) -> max acc h.sessions) 0 horizons
  in
  (* The blend needs a real trailing-vol window: with fewer than two returns
     the std is identically zero and every start gets filtered as "flat". *)
  let warmup = max 2 (min vol_window (n_bars - max_h - 2)) in
  let horizons =
    List.filter
      (fun (h : Oracle_types.horizon) -> n_bars >= warmup + h.sessions + 2)
      horizons
  in
  if horizons = []
  then
    failwith
      (Printf.sprintf
         "history too short for the requested horizon/warmup (each horizon needs warmup \
          + horizon + 2 bars; pass --vol-window and --horizons to fit the available \
          data, or remove the asset from config.json trading): %d bars available"
         n_bars);
  let cfg =
    { Oracle.horizons
    ; thresholds_pct
    ; percentiles
    ; vol_window = warmup
    ; gap_tolerance
    ; classes = [ { Oracle.name = class_name; kappa = a.kappa; members } ]
    ; equity_sessions
    ; weight_by_sessions = a.weight_by_sessions
    }
  in
  let r = Oracle.analyze asset cfg in
  let model h =
    Oracle_replay.blend_model_of
      ~horizon:h
      ~asset
      ~class_members:members
      ~kappa:a.kappa
      ~warmup
      ()
  in
  let models = List.map model horizons in
  (* The engine's decision: qty, gi, active, deployed capital against this
     asset's share of the venue pool. *)
  let deployment =
    D.deploy_asset
      ~asset
      ~cfg:grid
      ~lo:gi_lo
      ~hi:gi_hi
      ~models
      ~target_survival:a.target_survival
      ~pool
      ~fng
      ~fng_weight
      ~min_active_dsurv
      ~use_fng:(calendar_kind = Oracle_types.Crypto)
      ~param_steps:10
      ~scan_points:24
      ~qty_cap_mult:a.qty_cap_mult
  in
  if not deployment.Oracle_types.active
  then
    if a.json
    then
      ( Some
          (report_json
             a
             ~account
             ~venue_pool:pool
             ~deployment
             ~gi_lo
             ~gi_hi
             ~deep_bars
             grid
             asset
             members
             class_name
             r
             None
             [])
      , deployment.remainder )
    else (
      print_endline
        (report_text
           a
           ~account
           ~venue_pool:pool
           ~deployment
           ~index
           ~n_tasks
           ~gi_lo
           ~gi_hi
           ~deep_bars
           grid
           members
           class_name
           r
           None
           []);
      None, deployment.remainder)
  else (
    (* Supporting analysis at the deployment's sizing. *)
    let grid =
      { grid with
        qty = deployment.qty
      ; grid_interval_pct = deployment.parameter
      ; start_quote = deployment.deployed
      }
    in
    let replay_out = G.replay grid asset in
    let coverages =
      List.map
        (fun h ->
           let m = model h in
           let c_ = Oracle_replay.historical_path_coverage m ~d_surv:replay_out.d_surv in
           let capital_res =
             S.find_min_capital
               ~grid
               ~model:m
               ~target_survival:a.target_survival
               ?hi:a.max_capital
               ()
           in
           let qty_res = S.max_qty ~grid ~model:m ~target_survival:a.target_survival () in
           let empirical_res =
             S.empirical_min_capital
               ~grid
               ~model:m
               ~target_survival:a.target_survival
               ?hi:a.max_capital
               ()
           in
           h, c_, capital_res, qty_res, empirical_res)
        horizons
    in
    if a.json
    then
      ( Some
          (report_json
             a
             ~account
             ~venue_pool:pool
             ~deployment
             ~gi_lo
             ~gi_hi
             ~deep_bars
             grid
             asset
             members
             class_name
             r
             (Some replay_out)
             coverages)
      , deployment.remainder )
    else (
      print_endline
        (report_text
           a
           ~account
           ~venue_pool:pool
           ~deployment
           ~index
           ~n_tasks
           ~gi_lo
           ~gi_hi
           ~deep_bars
           grid
           members
           class_name
           r
           (Some replay_out)
           coverages);
      None, deployment.remainder))
;;

type portfolio_node =
  { spec : Oracle_topology.position_spec
  ; task : Oracle_tasks.task
  ; series : Oracle_types.series
  ; initial_base : float
  }

let task_for_key (tasks : Oracle_tasks.task list) (key : Oracle_topology.instrument_key) =
  match
    List.find_opt
      (fun (task : Oracle_tasks.task) ->
         String.lowercase_ascii task.exchange = key.venue
         && String.lowercase_ascii task.symbol = String.lowercase_ascii key.symbol
         && task.config.testnet = key.testnet)
      tasks
  with
  | Some task -> task
  | None ->
    let config = Oracle_tasks.default_trading_config key.venue key.symbol in
    { Oracle_tasks.symbol = key.symbol
    ; exchange = key.venue
    ; config = { config with testnet = key.testnet }
    }
;;

let portfolio_definition (a : args) (tasks : Oracle_tasks.task list) =
  let definition : Oracle_topology.definition =
    match a.topology with
    | None -> Oracle_topology.definition_of_tasks tasks
    | Some path ->
      (match Oracle_topology.load path with
       | Ok definition -> definition
       | Error error -> failwith ("oracle: " ^ error))
  in
  let add_allocation (definition : Oracle_topology.definition) value =
    match Oracle_topology.parse_allocation value with
    | Error error -> failwith ("oracle: " ^ error)
    | Ok allocation ->
      let positions =
        if
          List.exists
            (fun (position : Oracle_topology.position_spec) ->
               Oracle_topology.equal_key position.key allocation.key)
            definition.positions
        then
          List.map
            (fun (position : Oracle_topology.position_spec) ->
               if Oracle_topology.equal_key position.key allocation.key
               then allocation
               else position)
            definition.positions
        else allocation :: definition.positions
      in
      { definition with positions }
  in
  let definition = List.fold_left add_allocation definition a.allocations in
  let add_transfer transfers value =
    match Oracle_topology.parse_transfer value with
    | Ok transfer -> transfer :: transfers
    | Error error -> failwith ("oracle: " ^ error)
  in
  let definition =
    { definition with
      transfers = List.fold_left add_transfer definition.transfers a.transfer_specs
    }
  in
  match Oracle_topology.validate definition with
  | Ok () -> definition
  | Error errors -> failwith ("oracle: invalid topology: " ^ String.concat "; " errors)
;;

let loaded_portfolio_state (a : args) =
  match a.positions_file with
  | None -> []
  | Some path ->
    (match Oracle_portfolio_state.load path with
     | Ok positions -> positions
     | Error error -> failwith ("oracle: " ^ error))
;;

let apply_saved_allocations
      (definition : Oracle_topology.definition)
      (saved : Oracle_portfolio_state.position list)
  =
  let positions =
    List.map
      (fun (position : Oracle_topology.position_spec) ->
         if position.capital <> None
         then position
         else (
           match
             List.find_opt
               (fun (saved_position : Oracle_portfolio_state.position) ->
                  Oracle_topology.equal_key saved_position.key position.key)
               saved
           with
           | Some saved_position -> { position with capital = Some saved_position.pool }
           | None -> position))
      definition.positions
  in
  { definition with positions }
;;

let saved_base_for key saved =
  match
    List.find_opt
      (fun (position : Oracle_portfolio_state.position) ->
         Oracle_topology.equal_key position.key key)
      saved
  with
  | Some position -> position.base
  | None -> 0.0
;;

let portfolio_series (a : args) (key : Oracle_topology.instrument_key) ~(offline : bool) =
  if offline
  then (
    match a.from_csv, a.from_json with
    | Some path, _ ->
      Oracle_loader.load_csv_file
        ~symbol:key.symbol
        ~calendar_kind:(Oracle_tasks.calendar_kind_of_exchange key.venue)
        ~path
    | _, Some path ->
      Oracle_loader.load_json_file
        ~symbol:key.symbol
        ~calendar_kind:(Oracle_tasks.calendar_kind_of_exchange key.venue)
        ~path
    | None, None ->
      failwith "oracle: offline portfolio mode requires --from-csv or --from-json")
  else Lwt_main.run (fetch_series_for a ~exchange:key.venue key.symbol)
;;

(* Capital assignment for the venue-pooled portfolio model.

   The model's top level is the venue: each venue account (venue + quote +
   testnet) owns one pool, and all of that venue's assets draw from it. The
   per-asset share below is only accounting; the venue pool is the sum of its
   assets' shares and is what the replay actually spends.

   A venue pool resolves to:
   - the sum of its explicit --allocation / topology capitals when any asset on
     the venue is explicit,
   - --total-capital split equally per venue (explicit venues keep their sums),
   - --capital per venue,
   - 1000.0 per venue offline,
   - the venue's fetched available quote balance online.

   Unspecified assets on a venue split the venue pool equally among
   themselves. *)
let portfolio_capitals (a : args) ~(offline : bool) (nodes : portfolio_node list)
  : (portfolio_node * float) list
  =
  let same_account
        (left : Oracle_topology.instrument_key)
        (right : Oracle_topology.instrument_key)
    =
    left.venue = right.venue && left.testnet = right.testnet && left.quote = right.quote
  in
  let accounts =
    List.fold_left
      (fun acc (node : portfolio_node) ->
         if List.exists (fun key -> same_account key node.spec.key) acc
         then acc
         else node.spec.key :: acc)
      []
      nodes
  in
  let explicit_total =
    List.fold_left
      (fun total (node : portfolio_node) ->
         total +. Option.value node.spec.capital ~default:0.0)
      0.0
      nodes
  in
  let explicit_on account =
    List.fold_left
      (fun total (node : portfolio_node) ->
         if same_account account node.spec.key
         then total +. Option.value node.spec.capital ~default:0.0
         else total)
      0.0
      nodes
  in
  let has_explicit account =
    List.exists
      (fun (node : portfolio_node) ->
         node.spec.capital <> None && same_account account node.spec.key)
      nodes
  in
  let unspecified_on account =
    List.filter
      (fun (node : portfolio_node) ->
         node.spec.capital = None && same_account account node.spec.key)
      nodes
  in
  if
    a.split = "explicit"
    && List.exists (fun (node : portfolio_node) -> node.spec.capital = None) nodes
  then failwith "oracle: --split explicit requires a capital for every position";
  (match a.total_capital with
   | Some total when total < 0.0 || not (Float.is_finite total) ->
     failwith "oracle: --total-capital must be finite and non-negative"
   | Some total when total +. 1e-9 < explicit_total ->
     failwith "oracle: explicit topology allocations exceed --total-capital"
   | _ -> ());
  (match a.capital with
   | Some capital when capital < 0.0 || not (Float.is_finite capital) ->
     failwith "oracle: --capital must be finite and non-negative"
   | _ -> ());
  let unspecified_accounts =
    List.filter (fun account -> not (has_explicit account)) accounts
  in
  let venue_pool (account : Oracle_topology.instrument_key) =
    if has_explicit account
    then explicit_on account
    else (
      match a.total_capital with
      | Some total ->
        if unspecified_accounts = []
        then 0.0
        else (total -. explicit_total) /. float_of_int (List.length unspecified_accounts)
      | None ->
        (match a.capital with
         | Some capital -> capital
         | None when offline -> 1000.0
         | None ->
           let task =
             match
               List.find_opt
                 (fun (node : portfolio_node) -> same_account account node.spec.key)
                 nodes
             with
             | Some node -> node.task
             | None -> failwith "oracle: no position on venue account"
           in
           (match Lwt_main.run (Oracle_balances.fetch_task task) with
            | Ok snapshot -> Oracle_balances.available_quote snapshot ~quote:account.quote
            | Error error -> failwith ("oracle: balance fetch failed: " ^ error))))
  in
  let capitals =
    List.map
      (fun (node : portfolio_node) ->
         let account = node.spec.key in
         let pool = venue_pool account in
         let share =
           if node.spec.capital <> None
           then Option.value node.spec.capital ~default:0.0
           else (
             let unspecified = unspecified_on account in
             let remaining = pool -. explicit_on account in
             if remaining < 0.0 || unspecified = []
             then 0.0
             else remaining /. float_of_int (List.length unspecified))
         in
         if share < 0.0
         then
           failwith
             ("oracle: insufficient "
              ^ account.quote
              ^ " balance for portfolio venue "
              ^ account.venue);
         node, share)
      nodes
  in
  capitals
;;

let portfolio_grid (a : args) (node : portfolio_node) capital ~(offline : bool) =
  let tc = Lwt_main.run (Oracle_fees.enrich node.task.config ~offline) in
  let start_price =
    Option.value
      a.start_price
      ~default:
        (if Array.length node.series.bars = 0
         then 0.0
         else node.series.bars.(Array.length node.series.bars - 1).close)
  in
  let grid_interval_pct = Option.value a.grid_interval ~default:(snd tc.grid_interval) in
  let grid =
    Grid_adapter.of_trading_config tc ~start_price ~start_quote:capital ~grid_interval_pct
  in
  { grid with
    qty = Option.value a.qty ~default:grid.qty
  ; sell_mult = Option.value a.sell_mult ~default:grid.sell_mult
  ; maker_fee = Option.value a.fee ~default:grid.maker_fee
  ; accumulation_buffer =
      Option.value a.accumulation_buffer ~default:grid.accumulation_buffer
  ; price_increment = Option.value a.price_increment ~default:grid.price_increment
  ; qty_increment = Option.value a.qty_increment ~default:grid.qty_increment
  ; qty_min = Option.value a.qty_min ~default:grid.qty_min
  ; min_notional = Option.value a.min_notional ~default:grid.min_notional
  }
;;

let venue_id (v : Oracle_portfolio.venue_outcome) =
  Printf.sprintf "%s/%s%s" v.venue v.quote (if v.testnet then "@testnet" else "")
;;

let venue_of_node (key : Oracle_topology.instrument_key) =
  Printf.sprintf "%s/%s%s" key.venue key.quote (if key.testnet then "@testnet" else "")
;;

let report_portfolio_text
      (definition : Oracle_topology.definition)
      (nodes : (portfolio_node * float) list)
      (result : Oracle_portfolio.result)
  =
  let b = Buffer.create 2048 in
  let line fmt =
    Printf.ksprintf
      (fun value ->
         Buffer.add_string b value;
         Buffer.add_char b '\n')
      fmt
  in
  line "=== DIO Capital Oracle Portfolio ===";
  line
    "Sessions: %d   exhausted: %b   first exhausted session: %s"
    result.n_sessions
    result.exhausted
    (match result.first_exhausted_session with
     | Some session -> string_of_int session
     | None -> "none");
  line
    "Venues: %d   Positions: %d   Transfers: %d"
    (List.length result.venues)
    (List.length nodes)
    (List.length definition.transfers);
  List.iter
    (fun (venue : Oracle_portfolio.venue_outcome) ->
       line
         "  %s  pool %.2f -> %.2f   min DD %s   D_surv %s   low %b   fills %d/%d   base \
          %.6f   assets: %s"
         (venue_id venue)
         venue.initial_pool
         venue.final_pool
         (pct venue.pool_min_drawdown)
         (pct venue.d_surv)
         venue.capital_low
         venue.buy_fills
         venue.sell_fills
         venue.final_base
         (String.concat ", " venue.assets);
       List.iter
         (fun ((node, capital) : portfolio_node * float) ->
            if venue_of_node node.spec.key = venue_id venue
            then (
              let outcome =
                List.find
                  (fun (value : Oracle_portfolio.position_outcome) ->
                     value.venue = node.spec.key.venue
                     && value.asset = node.spec.key.symbol)
                  result.positions
              in
              line
                "      %s  share %.2f  fills %d/%d  base %.6f"
                (Oracle_topology.key_id node.spec.key)
                capital
                outcome.buy_fills
                outcome.sell_fills
                outcome.final_base))
         nodes)
    result.venues;
  List.iter
    (fun (transfer : Oracle_topology.transfer_spec) ->
       line
         "  transfer session %d: %s -> %s  %.2f"
         transfer.session
         (Oracle_topology.key_id transfer.from_key)
         (Oracle_topology.key_id transfer.to_key)
         transfer.amount)
    definition.transfers;
  Buffer.contents b
;;

let report_portfolio_json
      (definition : Oracle_topology.definition)
      (nodes : (portfolio_node * float) list)
      (result : Oracle_portfolio.result)
  : Yojson.Safe.t
  =
  let venue_json (venue : Oracle_portfolio.venue_outcome) =
    `Assoc
      [ "venue", `String venue.venue
      ; "quote", `String venue.quote
      ; "testnet", `Bool venue.testnet
      ; "assets", `List (List.map (fun asset -> `String asset) venue.assets)
      ; "initial_pool", `Float venue.initial_pool
      ; "final_pool", `Float venue.final_pool
      ; "pool_min_drawdown", `Float venue.pool_min_drawdown
      ; "d_surv", `Float venue.d_surv
      ; "capital_low", `Bool venue.capital_low
      ; ( "first_exhausted_drawdown"
        , Option.fold
            ~none:`Null
            ~some:(fun value -> `Float value)
            venue.first_exhausted_drawdown )
      ; ( "first_exhausted_session"
        , Option.fold
            ~none:`Null
            ~some:(fun value -> `Int value)
            venue.first_exhausted_session )
      ; "buy_fills", `Int venue.buy_fills
      ; "sell_fills", `Int venue.sell_fills
      ; "final_base", `Float venue.final_base
      ]
  in
  let outcome_json ((node, capital) : portfolio_node * float) =
    let outcome =
      List.find
        (fun (value : Oracle_portfolio.position_outcome) ->
           value.venue = node.spec.key.venue && value.asset = node.spec.key.symbol)
        result.positions
    in
    `Assoc
      [ "venue", `String node.spec.key.venue
      ; "symbol", `String node.spec.key.symbol
      ; "base", `String node.spec.key.base
      ; "quote", `String node.spec.key.quote
      ; "testnet", `Bool node.spec.key.testnet
      ; "share", `Float capital
      ; "buy_fills", `Int outcome.buy_fills
      ; "sell_fills", `Int outcome.sell_fills
      ; "final_base", `Float outcome.final_base
      ]
  in
  `Assoc
    [ "mode", `String "portfolio"
    ; "n_sessions", `Int result.n_sessions
    ; "exhausted", `Bool result.exhausted
    ; ( "first_exhausted_session"
      , Option.fold
          ~none:`Null
          ~some:(fun value -> `Int value)
          result.first_exhausted_session )
    ; "topology", Oracle_topology.to_json definition
    ; "venues", `List (List.map venue_json result.venues)
    ; "positions", `List (List.map outcome_json nodes)
    ; "transfers", `List (List.map Oracle_topology.transfer_json definition.transfers)
    ]
;;

let run_portfolio (a : args) (tasks : Oracle_tasks.task list) : Yojson.Safe.t option =
  let offline = Option.is_some a.from_csv || Option.is_some a.from_json in
  let saved = loaded_portfolio_state a in
  let definition =
    portfolio_definition a tasks |> fun value -> apply_saved_allocations value saved
  in
  if definition.positions = [] then failwith "oracle: portfolio topology has no positions";
  if offline && List.length definition.positions > 1
  then failwith "oracle: offline portfolio mode supports one historical input file";
  let nodes =
    List.map
      (fun (spec : Oracle_topology.position_spec) ->
         let task = task_for_key tasks spec.key in
         if not (Oracle_tasks.known_exchange task.exchange)
         then failwith ("oracle: unsupported portfolio exchange " ^ task.exchange);
         { spec
         ; task
         ; series = portfolio_series a spec.key ~offline
         ; initial_base = saved_base_for spec.key saved
         })
      definition.positions
  in
  let node_tasks = List.map (fun (node : portfolio_node) -> node.task) nodes in
  if not offline then Lwt_main.run (Oracle_venues.init node_tasks);
  let capitals = portfolio_capitals a ~offline nodes in
  let capital_by_key key =
    match
      List.find_opt
        (fun ((node, _) : portfolio_node * float) ->
           Oracle_topology.equal_key node.spec.key key)
        capitals
    with
    | Some (_, capital) -> capital
    | None -> failwith ("oracle: no capital allocation for " ^ Oracle_topology.key_id key)
  in
  let timeline =
    Oracle_topology.timeline_of_series
      (List.map (fun (node : portfolio_node) -> node.series) nodes)
  in
  let positions =
    List.map
      (fun (node : portfolio_node) ->
         let capital = capital_by_key node.spec.key in
         { Oracle_portfolio.venue = node.spec.key.venue
         ; asset = node.spec.key.symbol
         ; quote = node.spec.key.quote
         ; testnet = node.spec.key.testnet
         ; pool = capital
         ; initial_base = node.initial_base
         ; bars = Oracle_topology.align_series timeline node.series
         ; subgrids = [ portfolio_grid a node capital ~offline ]
         })
      nodes
  in
  let transfers = List.map Oracle_topology.to_portfolio_transfer definition.transfers in
  let result = Oracle_portfolio.simulate_aligned ~timeline ~positions ~transfers () in
  let venue_of_key (key : Oracle_topology.instrument_key) =
    List.find_opt
      (fun (venue : Oracle_portfolio.venue_outcome) ->
         venue.venue = key.venue && venue.quote = key.quote && venue.testnet = key.testnet)
      result.venues
  in
  (match a.save_positions with
   | None -> ()
   | Some path ->
     let saved_positions =
       List.map
         (fun (node : portfolio_node) ->
            let outcome =
              List.find
                (fun (value : Oracle_portfolio.position_outcome) ->
                   value.venue = node.spec.key.venue && value.asset = node.spec.key.symbol)
                result.positions
            in
            let share =
              match venue_of_key node.spec.key with
              | Some venue when venue.assets <> [] ->
                venue.final_pool /. float_of_int (List.length venue.assets)
              | _ -> 0.0
            in
            { Oracle_portfolio_state.key = node.spec.key
            ; pool = share
            ; base = outcome.final_base
            })
         nodes
     in
     (try Oracle_portfolio_state.save path saved_positions with
      | exn ->
        failwith
          (Printf.sprintf "oracle: cannot save positions: %s" (Printexc.to_string exn))));
  if a.json
  then Some (report_portfolio_json definition capitals result)
  else (
    print_endline (report_portfolio_text definition capitals result);
    None)
;;

let main () =
  let a = parse_args () in
  let config = Dio_engine.Config.read_config () in
  let offline = Option.is_some a.from_csv || Option.is_some a.from_json in
  let tasks, unsupported =
    if a.portfolio && offline && a.symbol = ""
    then
      (* Portfolio mode with a topology file never needs a positional SYMBOL:
         positions come from the topology, and offline runs skip venue init.
         Resolve the config trading list as the task pool. *)
      Oracle_tasks.resolve_tasks
        ~symbol:""
        ~exchange:a.exchange
        ~exchange_explicit:a.exchange_explicit
        ~trading:config.trading
        ~offline:false
    else
      Oracle_tasks.resolve_tasks
        ~symbol:a.symbol
        ~exchange:a.exchange
        ~exchange_explicit:a.exchange_explicit
        ~trading:config.trading
        ~offline
  in
  (* Populate venue instrument metadata (ticks/lots) before any grid replay so
     increments resolve from the real exchange, not the 0.01 fallback. *)
  if not offline then Lwt_main.run (Oracle_venues.init tasks);
  let warnings =
    List.map
      (fun (symbol, exchange) ->
         Printf.sprintf
           "unsupported exchange in config.json for capital oracle modeling: %s"
           (if exchange = "" then symbol else Printf.sprintf "%s (%s)" symbol exchange))
      unsupported
  in
  if tasks = [] && warnings = [] && not a.portfolio
  then (
    Printf.eprintf
      "oracle: no SYMBOL given and config.json has no runnable trading assets (see --help)\n";
    exit 1)
  else if a.portfolio
  then (
    if not a.json then List.iter print_endline warnings;
    let report = run_portfolio a tasks in
    match report, a.json with
    | Some report, true ->
      let report =
        if warnings = []
        then report
        else
          `Assoc
            (("warnings", `List (List.map (fun w -> `String w) warnings))
             ::
             (match report with
              | `Assoc fields -> fields
              | other -> [ "portfolio", other ]))
      in
      print_endline (Yojson.Safe.to_string report)
    | _ -> ())
  else (
    let fng = resolve_fng a in
    let pools = venue_pools a ~offline tasks in
    let pool_of account =
      match List.find_opt (fun (acc, _) -> same_account acc account) pools with
      | Some (_, p) -> p
      | None -> 0.0
    in
    let failures = ref 0 in
    let reports = ref [] in
    if not a.json then List.iter print_endline warnings;
    let grouped = group_by_account tasks in
    List.iter
      (fun (account, account_tasks) ->
         let pool = ref (pool_of account) in
         let n_tasks = List.length account_tasks in
         List.iteri
           (fun i task ->
              let pool_before = !pool in
              try
                let report, remaining =
                  run_one
                    a
                    config.classes
                    task
                    ~pool:pool_before
                    ~fng
                    ~fng_weight:a.fng_weight
                    ~min_active_dsurv:a.min_active_dsurv
                    ~index:(i + 1)
                    ~n_tasks
                in
                pool := remaining;
                match report with
                | Some r -> reports := r :: !reports
                | None -> ()
              with
              | exn ->
                incr failures;
                let msg = Printexc.to_string exn in
                if
                  string_contains msg "empty distribution"
                  || string_contains msg "history too short"
                then
                  Printf.eprintf
                    "oracle: '%s' (%s) skipped: history too short for the requested \
                     horizon/warmup (each horizon needs warmup + horizon + 2 bars; pass \
                     --vol-window and --horizons to fit the available data, or remove \
                     the asset from config.json trading); capital stays in the venue \
                     pool: %s\n"
                    task.Oracle_tasks.symbol
                    task.Oracle_tasks.exchange
                    msg
                else
                  Printf.eprintf
                    "oracle: '%s' (%s) failed: %s\n"
                    task.Oracle_tasks.symbol
                    task.Oracle_tasks.exchange
                    msg
              (* On failure the capital stays in the venue pool and passes
                    to the next asset. *))
           account_tasks;
         if (not a.json) && account_tasks <> []
         then
           print_endline
             (Printf.sprintf "  Surplus: %.2f %s (idle reserve)" !pool account.quote))
      grouped;
    let warn_j = `List (List.map (fun w -> `String w) warnings) in
    let add_warnings (j : Yojson.Safe.t) =
      if warnings = []
      then j
      else
        `Assoc
          (("warnings", warn_j)
           ::
           (match j with
            | `Assoc l -> l
            | other -> [ "assets", other ]))
    in
    let reports = List.rev !reports in
    (match reports, a.json with
     | [ r ], true -> print_endline (Yojson.Safe.to_string (add_warnings r))
     | _ :: _ :: _, true ->
       print_endline (Yojson.Safe.to_string (add_warnings (`List reports)))
     | [], true when warnings <> [] ->
       print_endline (Yojson.Safe.to_string (`Assoc [ "warnings", warn_j ]))
     | _ -> ());
    if !failures > 0 then exit 1)
;;

let () = main ()
