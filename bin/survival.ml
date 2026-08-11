(* DIO Capital Survival Report CLI.

   dune exec dio-survival -- BTC/USD --exchange kraken
   dune exec dio-survival -- AAPL --exchange alpaca --capital 5000 --target-survival 0.99
   dune exec dio-survival

   Values default from the config.json trading config (qty, grid_interval,
   sell_mult, maker_fee, accumulation_buffer, data_feed); every value is
   overridable from the CLI. With no SYMBOL, every asset in config.json's
   "trading" list is analyzed on its own configured exchange. Offline mode via
   --from-csv / --from-json loads a single asset series from a file instead of
   the network and still requires a SYMBOL. *)

open Lwt.Infix
open Dio_survival

let pct f = Printf.sprintf "%6.1f%%" (f *. 100.0)

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
  ; qty : float option
  ; grid_interval : float option
  ; fee : float option
  ; sell_mult : float option
  ; accumulation_buffer : float option
  ; price_increment : float option
  ; qty_increment : float option
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
  ; target_survival : float
  ; weight_by_sessions : bool
  ; json : bool
  ; from_csv : string option
  ; from_json : string option
  ; max_capital : float option
  }

let usage = "Usage: dio-survival [SYMBOL] [options]"

let parse_args () =
  let symbol = ref "" in
  let exchange = ref "kraken" in
  let exchange_explicit = ref false in
  let capital = ref None in
  let qty = ref None in
  let grid_interval = ref None in
  let fee = ref None in
  let sell_mult = ref None in
  let accumulation_buffer = ref None in
  let price_increment = ref None in
  let qty_increment = ref None in
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
  let kappa = ref 2 in
  let target_survival = ref 0.99 in
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
        , " quote capital for grid replay (default 1000.0)" )
      ; ( "--qty"
        , Arg.Float (fun f -> qty := Some f)
        , " order size (default from config.json)" )
      ; ( "--gi"
        , Arg.Float (fun f -> grid_interval := Some f)
        , " grid interval %% (default max of config.json grid_interval)" )
      ; ( "--fee"
        , Arg.Float (fun f -> fee := Some f)
        , " maker fee (default from config.json)" )
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
        , " risk class name (default from symbol registry)" )
      ; ( "--members"
        , Arg.String
            (fun s ->
              members := Some (String.split_on_char ',' s |> List.map String.trim))
        , " comma member symbols for the class pool" )
      ; "--kappa", Arg.Int (fun i -> kappa := i), " class weight in the blend (default 2)"
      ; ( "--target-survival"
        , Arg.Float (fun f -> target_survival := f)
        , " target blended survival for inverse sizing (default 0.99)" )
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
  ; qty = !qty
  ; grid_interval = !grid_interval
  ; fee = !fee
  ; sell_mult = !sell_mult
  ; accumulation_buffer = !accumulation_buffer
  ; price_increment = !price_increment
  ; qty_increment = !qty_increment
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
  ; target_survival = !target_survival
  ; weight_by_sessions = !weight_by_sessions
  ; json = !json
  ; from_csv = (if !from_csv = "" then None else Some !from_csv)
  ; from_json = (if !from_json = "" then None else Some !from_json)
  ; max_capital = !max_capital
  }
;;

(** Per-run cache of fetched series, shared across assets and class members so
    e.g. ETH/USD is only downloaded once. *)
let fetch_cache : (string * string, Survival_types.series) Hashtbl.t = Hashtbl.create 32

(** Fetch one symbol's daily series over the network (cached per run). *)
let fetch_series (a : args) (symbol : string) : Survival_types.series Lwt.t =
  match Hashtbl.find_opt fetch_cache (a.exchange, symbol) with
  | Some s -> Lwt.return s
  | None ->
    let fetch =
      match a.exchange with
      | "kraken" ->
        Survival_fetch_kraken.fetch_ohlc ~symbol ()
        >|= Survival_fetch_kraken.series_of_bars ~symbol
      | "hyperliquid" ->
        Survival_fetch_hyperliquid.fetch_candles ~symbol ()
        >|= Survival_fetch_hyperliquid.series_of_bars ~symbol
      | "alpaca" ->
        let start = Option.value a.start_date ~default:"2010-01-01" in
        let end_date = Option.value a.end_date ~default:(today_iso ()) in
        let feed = Option.value a.data_feed ~default:"iex" in
        Survival_fetch_alpaca.fetch_bars ~feed ~symbol ~start_date:start ~end_date ()
        >|= Survival_fetch_alpaca.series_of_bars ~symbol
      | _ -> invalid_arg "unknown exchange"
    in
    fetch
    >|= fun series ->
    Hashtbl.add fetch_cache (a.exchange, symbol) series;
    series
;;

(** Load the class member pool: explicit --members when online, registry members
    online, otherwise the asset alone (offline mode). *)
let load_members (a : args) (asset : Survival_types.series)
  : Survival_types.series list Lwt.t
  =
  let class_name =
    Option.value
      a.class_name
      ~default:(Survival_classes.Registry.class_of_symbol a.symbol)
  in
  let syms =
    match a.members with
    | Some ms when ms <> [] -> ms
    | _ -> Survival_classes.Registry.member_symbols class_name
  in
  if syms = []
  then (
    Printf.eprintf
      "survival: no class members known for '%s'; using asset alone\n"
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
        if Array.length series.bars = 0 then Lwt.return acc else Lwt.return (series :: acc)
    in
    go syms
    >>= fun members -> if members = [] then Lwt.return [ asset ] else Lwt.return members)
;;

type coverage_row =
  Survival_types.horizon
  * Survival_types.historical_path_coverage
  * Survival_types.sizing_result
  * Survival_types.sizing_result

let horizon_label_of (c_ : Survival_types.historical_path_coverage) =
  c_.Survival_types.horizon.Survival_types.label
;;

(** Human-readable report. *)
let report_text
      (a : args)
      (grid : Dio_strategies.Grid_core.config)
      (members : Survival_types.series list)
      (class_name : string)
      (r : Survival.result)
      (replay_out : Survival_replay.outcome)
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
  line "=== DIO Capital Survival Report ===";
  line
    "Symbol: %s   Exchange: %s   Class: %s (%d members, kappa %d)"
    a.symbol
    a.exchange
    class_name
    (List.length members)
    a.kappa;
  line
    "Bars: %d  (%s .. %s)   max gap: %d (tolerance %d)"
    r.n_sessions
    r.first_date
    r.last_date
    r.max_gap
    (Option.value a.gap_tolerance ~default:5);
  line "";
  line "MFD percentile tables (%s of max drawdown at each percentile):" "%";
  let class_tbls =
    match r.class_estimates with
    | c_ :: _ -> c_.Survival_types.percentile_tables
    | [] -> []
  in
  let blended_tbls =
    List.map (fun bt -> bt.Survival_types.table) r.blended_percentile_tables
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
        (fun ( (ar : Survival_types.percentile_row)
             , (cr : Survival_types.percentile_row)
             , (br : Survival_types.percentile_row) ) ->
           Printf.sprintf
             "P%g %s/%s/%s"
             ar.percentile
             (pct ar.mfd)
             (pct cr.mfd)
             (pct br.mfd))
        rows3
      |> String.concat "   "
    in
    line "  %-7s n=%d   %s" at.horizon_label at.n_starts row
  done;
  line "";
  line
    "Grid replay (qty %.4f  gi %.2f%%  capital %.2f  fee %.3f%%):"
    grid.qty
    grid.grid_interval_pct
    grid.start_quote
    (grid.maker_fee *. 100.0);
  line
    "  D_surv = %s (%s)"
    (pct replay_out.d_surv)
    (if replay_out.exhausted then "capital low hit" else "never exhausted");
  line
    "  fills: %d buy / %d sell   min quote drawdown %s"
    replay_out.buy_fills
    replay_out.sell_fills
    (pct replay_out.min_quote_drawdown);
  line "";
  line "Historical path coverage at D_surv:";
  line "  %-7s %10s %10s %10s" "horizon" "asset" "class" "blended";
  List.iter
    (fun ((_h, c_, _cap, _q) : coverage_row) ->
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
    (fun ((_h, c_, cap, q) : coverage_row) ->
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
  let any_unreachable =
    List.exists
      (fun ((_h, _c_, cap, q) : coverage_row) -> (not cap.reachable) || not q.reachable)
      coverages
  in
  if any_unreachable
  then
    line
      "  * target not reached: no parameter within the search bounds clears it (the \
       required capital exceeds --max-capital, or the required qty is below \
       --qty-increment)";
  Buffer.contents b
;;

(** JSON report. *)
let report_json
      (a : args)
      (grid : Dio_strategies.Grid_core.config)
      (asset : Survival_types.series)
      (members : Survival_types.series list)
      (class_name : string)
      (r : Survival.result)
      (replay_out : Survival_replay.outcome)
      (coverages : coverage_row list)
  : Yojson.Safe.t
  =
  let gap_j (g : Survival_types.gap) =
    `Assoc
      [ "after", `String g.after
      ; "before", `String g.before
      ; "missing_days", `Int g.missing_days
      ]
  in
  let percentile_rows (t : Survival_types.percentile_table) =
    `Assoc
      (List.map
         (fun (row : Survival_types.percentile_row) ->
            Printf.sprintf "p%g" row.percentile, `Float row.mfd)
         t.rows)
  in
  let asset_tbls = List.map percentile_rows r.percentile_tables in
  let sizing_j ((_h, c_, cap, q) : coverage_row) =
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
      ]
  in
  `Assoc
    [ "symbol", `String a.symbol
    ; "exchange", `String a.exchange
    ; "n_bars", `Int r.n_sessions
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
             (fun t j -> t.Survival_types.horizon_label, j)
             r.percentile_tables
             asset_tbls) )
    ; ( "grid"
      , `Assoc
          [ "qty", `Float grid.qty
          ; "sell_mult", `Float grid.sell_mult
          ; "grid_interval_pct", `Float grid.grid_interval_pct
          ; "maker_fee", `Float grid.maker_fee
          ; "accumulation_buffer", `Float grid.accumulation_buffer
          ; "start_price", `Float grid.start_price
          ; "start_quote", `Float grid.start_quote
          ] )
    ; ( "replay"
      , `Assoc
          [ "d_surv", `Float replay_out.d_surv
          ; "exhausted", `Bool replay_out.exhausted
          ; "min_quote_drawdown", `Float replay_out.min_quote_drawdown
          ; "buy_fills", `Int replay_out.buy_fills
          ; "sell_fills", `Int replay_out.sell_fills
          ] )
    ; "sizing", `List (List.map sizing_j coverages)
    ; ( "last_close"
      , `Float
          (if Array.length asset.bars = 0
           then 0.0
           else asset.bars.(Array.length asset.bars - 1).close) )
    ]
;;

(** Run the full per-asset pipeline. In text mode prints the report and returns
    [None]; in JSON mode returns the report value without printing. *)
let run_one (a : args) (task : Survival_tasks.task) : Yojson.Safe.t option =
  let a =
    { a with
      symbol = task.Survival_tasks.symbol
    ; exchange = task.Survival_tasks.exchange
    }
  in
  let calendar_kind = Survival_tasks.calendar_kind_of_exchange a.exchange in
  let tc = task.Survival_tasks.config in
  let offline = Option.is_some a.from_csv || Option.is_some a.from_json in
  let asset =
    match a.from_csv, a.from_json with
    | Some path, _ -> Survival_loader.load_csv_file ~symbol:a.symbol ~calendar_kind ~path
    | _, Some path -> Survival_loader.load_json_file ~symbol:a.symbol ~calendar_kind ~path
    | None, None -> Lwt_main.run (fetch_series a a.symbol)
  in
  let members = if offline then [ asset ] else Lwt_main.run (load_members a asset) in
  let start_price =
    Option.value
      a.start_price
      ~default:
        (if Array.length asset.bars = 0
         then 0.0
         else asset.bars.(Array.length asset.bars - 1).close)
  in
  let grid_interval_pct = Option.value a.grid_interval ~default:(snd tc.grid_interval) in
  let grid =
    Grid_adapter.of_trading_config
      tc
      ~start_price
      ~start_quote:(Option.value a.capital ~default:1000.0)
      ~grid_interval_pct
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
    }
  in
  let class_name =
    Option.value
      a.class_name
      ~default:(Survival_classes.Registry.class_of_symbol a.symbol)
  in
  let equity_sessions =
    if a.exchange = "alpaca"
    then (
      let start = Option.value a.start_date ~default:"2010-01-01" in
      let end_date = Option.value a.end_date ~default:(today_iso ()) in
      try
        let dates =
          Lwt_main.run
            (Survival_fetch_alpaca.fetch_calendar ~start_date:start ~end_date ())
        in
        if dates = []
        then Some Survival_sessions.business_weekday
        else Some (Survival_fetch_alpaca.model_of_calendar_dates dates)
      with
      | exn ->
        Printf.eprintf
          "survival: calendar fetch failed (%s); using business weekdays\n"
          (Printexc.to_string exn);
        Some Survival_sessions.business_weekday)
    else None
  in
  let horizons =
    match a.horizons with
    | Some ns ->
      List.map
        (fun n ->
           { Survival_types.label = Survival_types.horizon_label calendar_kind n
           ; sessions = n
           ; calendar_days = Survival_types.calendar_days_of_sessions calendar_kind n
           })
        ns
    | None -> (Survival.default_config ~calendar_kind).horizons
  in
  let thresholds_pct =
    Option.value a.thresholds ~default:Survival.default_thresholds_pct
  in
  let percentiles = Option.value a.percentiles ~default:Survival.default_percentiles in
  let vol_window = Option.value a.vol_window ~default:60 in
  let gap_tolerance = Option.value a.gap_tolerance ~default:5 in
  let cfg =
    { Survival.horizons
    ; thresholds_pct
    ; percentiles
    ; vol_window
    ; gap_tolerance
    ; classes = [ { Survival.name = class_name; kappa = a.kappa; members } ]
    ; equity_sessions
    ; weight_by_sessions = a.weight_by_sessions
    }
  in
  let r = Survival.analyze asset cfg in
  let model h =
    Survival_replay.blend_model_of
      ~horizon:h
      ~asset
      ~class_members:members
      ~kappa:a.kappa
      ~warmup:vol_window
      ()
  in
  let replay_out = Survival_replay.replay_series grid asset in
  let coverages =
    List.map
      (fun h ->
         let m = model h in
         let c_ = Survival_replay.historical_path_coverage m ~d_surv:replay_out.d_surv in
         let capital_res =
           Survival_replay.find_min_capital
             ~grid
             ~model:m
             ~target_survival:a.target_survival
             ?hi:a.max_capital
             ()
         in
         let qty_res =
           Survival_replay.max_qty ~grid ~model:m ~target_survival:a.target_survival ()
         in
         h, c_, capital_res, qty_res)
      horizons
  in
  if a.json
  then Some (report_json a grid asset members class_name r replay_out coverages)
  else (
    print_endline (report_text a grid members class_name r replay_out coverages);
    None)
;;

let main () =
  let a = parse_args () in
  let config = Dio_engine.Config.read_config () in
  let offline = Option.is_some a.from_csv || Option.is_some a.from_json in
  let tasks, unsupported =
    Survival_tasks.resolve_tasks
      ~symbol:a.symbol
      ~exchange:a.exchange
      ~exchange_explicit:a.exchange_explicit
      ~trading:config.trading
      ~offline
  in
  let warnings =
    List.map
      (fun (symbol, exchange) ->
         Printf.sprintf
           "unsupported exchange in config.json for capital survival modeling: %s"
           (if exchange = "" then symbol else Printf.sprintf "%s (%s)" symbol exchange))
      unsupported
  in
  if tasks = [] && warnings = []
  then (
    Printf.eprintf
      "survival: no SYMBOL given and config.json has no runnable trading assets (see \
       --help)\n";
    exit 1)
  else (
    let multiple = List.length tasks > 1 in
    let one (task : Survival_tasks.task) =
      try run_one a task with
      | exn when multiple ->
        Printf.eprintf
          "survival: '%s' (%s) failed: %s\n"
          task.Survival_tasks.symbol
          task.Survival_tasks.exchange
          (Printexc.to_string exn);
        None
    in
    if not a.json then List.iter print_endline warnings;
    let reports = List.filter_map one tasks in
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
    match reports, a.json with
    | [ r ], true -> print_endline (Yojson.Safe.to_string (add_warnings r))
    | _ :: _ :: _, true ->
      print_endline (Yojson.Safe.to_string (add_warnings (`List reports)))
    | [], true when warnings <> [] ->
      print_endline (Yojson.Safe.to_string (`Assoc [ "warnings", warn_j ]))
    | _ -> ())
;;

let () = main ()
