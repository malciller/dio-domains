(* Oracle_runtime - the live-engine realtime capital oracle.

   The CLI (bin/oracle.ml) analyzes the config's trading assets on demand and
   prints a report. This module runs the same pipeline continuously inside the
   trading engine: each pass resolves the trading assets, fetches their
   histories, runs the deployment engine per venue account (threading the pool
   down the config priority order), and publishes one decision per asset to a
   lock-free snapshot. Trading domains read their asset's decision every cycle
   (an Atomic.get of an immutable list) and adopt the qty / grid_interval /
   active flags; an inactive asset stops placing new orders and its capital
   passes to the next asset in the account's priority order.

   Failures are last-known-good: an asset whose analysis fails this pass keeps
   its previous decision, an account whose balance cannot be fetched is skipped
   entirely, and an entirely failed pass publishes nothing. The runtime never
   crashes the engine. *)

open Lwt.Infix
module G = Oracle_strategy.Grid
module D = Oracle_deploy.Engine (G)

let section = "oracle_runtime"

(** One asset's live trading decision, published for the domain workers. *)
type decision =
  { exchange : string
  ; symbol : string
  ; active : bool (** Run the strategy (place new orders). *)
  ; reason : string (** Why the asset is inactive, when it is. *)
  ; qty : float (** Recommended order size. *)
  ; grid_interval : float (** Recommended grid interval in %. *)
  ; d_surv : float (** Replayed D_surv at the recommended sizing. *)
  ; d_gov : float (** Governing drawdown across the reachable horizons. *)
  ; governing_horizon : string (** The binding horizon. *)
  ; deployed : float (** Capital the ladder consumes at the recommended sizing. *)
  ; pool_share : float (** Capital this asset drew from the venue pool. *)
  ; remainder : float (** Pool share passed to the next asset. *)
  ; warnings : string list
  ; updated_at : float (** Unix time of the pass that produced this decision. *)
  }

(** Risk-class member pool mirroring config.json's "classes" (same field
    names as Dio_engine.Config.class_pool, kept local so dio.oracle does not
    depend on dio.engine). *)
type class_pool =
  { members : string list
  ; kappa : int option
  }

(** Runtime knobs. Defaults mirror the CLI's defaults so the engine behaves
    exactly like `dio-oracle` unless configured. *)
type runtime_config =
  { target_survival : float (** Blended survival target for sizing (default 0.99). *)
  ; fng_weight : float (** Weight of the F&G side in the gi blend (default 0.5). *)
  ; min_active_dsurv : float
    (** Assets whose replayed D_surv stays below this are recommended inactive
        and their capital passes down (default 0.0 = fundable means active). *)
  ; qty_cap_mult : float
    (** Deployment ceiling as a multiple of the config qty (default 1.0: each
        asset's qty is capped at its config qty so surplus capital passes down
        the priority order; 0.0 = uncapped, the priority asset deploys the
        whole pool). *)
  ; no_deep_history : bool (** Disable the Yahoo deep-history extension. *)
  ; weight_by_sessions : bool (** Weight class members by session count. *)
  ; refresh_seconds : float (** Cadence between analysis passes (default 300). *)
  ; horizons : int list option (** Session horizons (default per calendar kind). *)
  ; max_capital : float option (** Upper bound of the sizing binary search. *)
  }

let default_config () =
  { target_survival = 0.99
  ; fng_weight = 0.5
  ; min_active_dsurv = 0.0
  ; qty_cap_mult = 1.0
  ; no_deep_history = false
  ; weight_by_sessions = true
  ; refresh_seconds = 300.0
  ; horizons = None
  ; max_capital = None
  }
;;

(** The published snapshot: an immutable list swapped atomically. Domain
    workers call [decision_for] every cycle; the write side only ever replaces
    the whole list in one Atomic.set, so readers never observe a torn state. *)
let decisions_ref : decision list Atomic.t = Atomic.make []

let decisions () = Atomic.get decisions_ref

(** Domain-safe lookup: exchange and symbol are lower-cased for the match so
    config spelling (BTC/USD vs btc/usd) never matters. *)
let decision_for ~(exchange : string) ~(symbol : string) : decision option =
  let exchange = String.lowercase_ascii exchange in
  let symbol = String.lowercase_ascii symbol in
  List.find_opt
    (fun (d : decision) ->
       String.lowercase_ascii d.exchange = exchange
       && String.lowercase_ascii d.symbol = symbol)
    (Atomic.get decisions_ref)
;;

(** Merge [fresh] decisions over the current snapshot: assets absent from
    [fresh] (analysis failed, account balance unavailable, or the asset was
    removed) keep their previous decision, so a partial or failed pass never
    halts trading on stale knowledge. Assets in [fresh] replace their old
    decision. *)
let publish (fresh : decision list) =
  let key (d : decision) =
    String.lowercase_ascii d.exchange ^ "/" ^ String.lowercase_ascii d.symbol
  in
  let fresh_keys = List.map key fresh in
  let kept =
    List.filter
      (fun (d : decision) -> not (List.mem (key d) fresh_keys))
      (Atomic.get decisions_ref)
  in
  Atomic.set decisions_ref (kept @ fresh)
;;

(** Pass counters for observability (logged, never read on a hot path). *)
let pass_count : int Atomic.t = Atomic.make 0

let last_pass_at : float Atomic.t = Atomic.make 0.0
let last_pass_ok : bool Atomic.t = Atomic.make true
let shutdown_requested = Atomic.make false

(* ------------------------------------------------------------------ *)
(* Per-pass pipeline (ported from bin/oracle.ml's run_one core).      *)
(* ------------------------------------------------------------------ *)

let today_iso () =
  let tm = Unix.localtime (Unix.time ()) in
  Printf.sprintf
    "%04d-%02d-%02d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
;;

(** Per-pass cache of fetched series, shared across assets and class members
    so e.g. ETH/USD is only downloaded once per pass. *)
let fetch_cache : (string * string, Oracle_types.series) Hashtbl.t = Hashtbl.create 32

let fetch_series_for
      (tc : Dio_strategies.Strategy_common.trading_config)
      (symbol : string)
  : Oracle_types.series Lwt.t
  =
  let exchange = tc.exchange in
  match Hashtbl.find_opt fetch_cache (exchange, symbol) with
  | Some series -> Lwt.return series
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
        let feed = Option.value tc.data_feed ~default:"iex" in
        Oracle_fetch_alpaca.fetch_bars
          ~feed
          ~symbol
          ~start_date:"2010-01-01"
          ~end_date:(today_iso ())
          ()
        >|= Oracle_fetch_alpaca.series_of_bars ~symbol
      | _ -> invalid_arg ("oracle_runtime: unknown exchange " ^ exchange)
    in
    fetch
    >|= fun series ->
    Hashtbl.replace fetch_cache (exchange, symbol) series;
    series
;;

(** Extend a venue series backward with the Yahoo deep history for the same
    underlying asset (venue bars win on overlap; nothing is synthesized).
    Returns the deepened series and the number of deep bars added. *)
let deepen_series
      (rc : runtime_config)
      ~(exchange : string)
      (series : Oracle_types.series)
  : (Oracle_types.series * int) Lwt.t
  =
  let venue_bars = series.bars in
  if rc.no_deep_history || Array.length venue_bars = 0
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

(** Load the class member pool for [class_name] from the runtime's class
    pools (config.json "classes"), fetched on the asset's own exchange and
    deepened like the asset itself. Falls back to the asset alone when no
    pool is known. *)
let load_members
      (rc : runtime_config)
      (classes : (string * class_pool) list)
      (tc : Dio_strategies.Strategy_common.trading_config)
      ~(class_name : string)
      (asset : Oracle_types.series)
  : Oracle_types.series list Lwt.t
  =
  let syms =
    match List.assoc_opt class_name classes with
    | Some pool when pool.members <> [] -> pool.members
    | _ -> []
  in
  if syms = []
  then (
    Logging.debug_f
      ~section
      "no class members known for '%s'; analyzing %s alone"
      class_name
      asset.symbol;
    Lwt.return [ asset ])
  else (
    let rec go = function
      | [] -> Lwt.return []
      | symbol :: rest ->
        fetch_series_for tc symbol
        >>= fun series ->
        go rest
        >>= fun acc ->
        if Array.length series.bars = 0
        then Lwt.return acc
        else
          deepen_series rc ~exchange:tc.exchange series
          >>= fun (series, _) -> Lwt.return (series :: acc)
    in
    go syms
    >>= fun members -> if members = [] then Lwt.return [ asset ] else Lwt.return members)
;;

(** Resolved Fear & Greed: the live index (blocking, cached, fallback None). *)
let resolve_fng () : float option =
  try Some (Cmc.Fear_and_greed.fetch_and_cache_sync ()) with
  | _ -> None
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
    (fun (task : Oracle_tasks.task) ->
       let account = account_of_task task in
       if not (List.exists (fun a -> same_account a account) !accounts)
       then accounts := account :: !accounts)
    tasks;
  let accounts = List.rev !accounts in
  List.map
    (fun account ->
       ( account
       , List.filter
           (fun (task : Oracle_tasks.task) -> same_account account (account_of_task task))
           tasks ))
    accounts
;;

(** Venue-locked pool per account: the account's live available quote balance.
    [None] means the balance could not be fetched this pass - the caller skips
    the whole account rather than publish a decision based on a wrong pool. *)
let venue_pools (tasks : Oracle_tasks.task list)
  : (Oracle_topology.instrument_key * float option) list Lwt.t
  =
  let accounts = List.map fst (group_by_account tasks) in
  let pool_for (account : Oracle_topology.instrument_key) : float option Lwt.t =
    match
      List.find_opt
        (fun (task : Oracle_tasks.task) -> same_account account (account_of_task task))
        tasks
    with
    | None -> Lwt.return_none
    | Some task ->
      Lwt.catch
        (fun () ->
           Oracle_balances.fetch_task task
           >|= function
           | Error error ->
             Logging.warn_f
               ~section
               "balance fetch failed for %s: %s; keeping last-known-good decisions for \
                this account"
               (account_id account)
               error;
             None
           | Ok snapshot ->
             Some
               (Float.max
                  0.0
                  (Oracle_balances.available_quote snapshot ~quote:account.quote)))
        (fun exn ->
           Logging.warn_f
             ~section
             "balance fetch failed for %s (%s); keeping last-known-good decisions for \
              this account"
             (account_id account)
             (Printexc.to_string exn);
           Lwt.return_none)
  in
  Lwt_list.map_s pool_for accounts >|= fun pools -> List.combine accounts pools
;;

(** The deployment decision for one asset against its venue pool share. This
    is the CLI's run_one core without the report tables: series fetch +
    deepen + members + analysis + deployment engine. [index]/[n_tasks] are
    only for logging. *)
let decide_asset
      (rc : runtime_config)
      (classes : (string * class_pool) list)
      (task : Oracle_tasks.task)
      ~(pool : float)
      ~(fng : float option)
      ~(index : int)
      ~(n_tasks : int)
  : decision Lwt.t
  =
  let exchange = task.Oracle_tasks.exchange in
  let calendar_kind = Oracle_tasks.calendar_kind_of_exchange exchange in
  let tc = task.Oracle_tasks.config in
  let class_name =
    match tc.asset_class with
    | Some name -> name
    | None -> "default"
  in
  let kappa =
    match List.assoc_opt class_name classes with
    | Some pool -> Option.value pool.kappa ~default:200
    | None -> 200
  in
  Oracle_fees.enrich tc ~offline:false
  >>= fun tc ->
  fetch_series_for tc task.Oracle_tasks.symbol
  >>= fun asset ->
  deepen_series rc ~exchange asset
  >>= fun (asset, _deep_bars) ->
  load_members rc classes tc ~class_name asset
  >>= fun members ->
  let start_price =
    if Array.length asset.bars = 0
    then 0.0
    else asset.bars.(Array.length asset.bars - 1).Oracle_types.close
  in
  let gi_lo, gi_hi = tc.grid_interval in
  let grid =
    Grid_adapter.of_trading_config
      tc
      ~start_price
      ~start_quote:0.0
      ~grid_interval_pct:gi_hi
  in
  let equity_sessions_promise =
    if exchange = "alpaca"
    then
      Lwt.catch
        (fun () ->
           Oracle_fetch_alpaca.fetch_calendar
             ~start_date:"2010-01-01"
             ~end_date:(today_iso ())
             ()
           >|= fun dates ->
           if dates = []
           then Some Oracle_sessions.business_weekday
           else Some (Oracle_fetch_alpaca.model_of_calendar_dates dates))
        (fun exn ->
           Logging.warn_f
             ~section
             "calendar fetch failed for %s (%s); using business weekdays"
             task.Oracle_tasks.symbol
             (Printexc.to_string exn);
           Lwt.return (Some Oracle_sessions.business_weekday))
    else Lwt.return_none
  in
  equity_sessions_promise
  >>= fun equity_sessions ->
  let horizons =
    match rc.horizons with
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
  let vol_window = 60 in
  let gap_tolerance = 5 in
  (* Adapt the warmup to the history and the longest horizon, and drop the
     horizons the history cannot support, so a short asset analyzes on what
     fits instead of hard-failing. *)
  let n_bars = Array.length asset.bars in
  let max_h =
    List.fold_left (fun acc (h : Oracle_types.horizon) -> max acc h.sessions) 0 horizons
  in
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
         "history too short for the requested horizon/warmup (%d bars available)"
         n_bars);
  let cfg =
    { Oracle.horizons
    ; thresholds_pct = Oracle.default_thresholds_pct
    ; percentiles = Oracle.default_percentiles
    ; vol_window = warmup
    ; gap_tolerance
    ; classes = [ { Oracle.name = class_name; kappa; members } ]
    ; equity_sessions
    ; weight_by_sessions = rc.weight_by_sessions
    }
  in
  let _r = Oracle.analyze asset cfg in
  let model h =
    Oracle_replay.blend_model_of
      ~horizon:h
      ~asset
      ~class_members:members
      ~kappa
      ~warmup
      ()
  in
  let models = List.map model horizons in
  let deployment =
    D.deploy_asset
      ~asset
      ~cfg:grid
      ~lo:gi_lo
      ~hi:gi_hi
      ~models
      ~target_survival:rc.target_survival
      ~pool
      ~fng
      ~fng_weight:rc.fng_weight
      ~min_active_dsurv:rc.min_active_dsurv
      ~use_fng:(calendar_kind = Oracle_types.Crypto)
      ~param_steps:10
      ~scan_points:24
      ~qty_cap_mult:rc.qty_cap_mult
  in
  Logging.info_f
    ~section
    "[%d/%d] %s/%s %s qty %.6g gi %.2f%% deployed %.2f / share %.2f remainder %.2f"
    index
    n_tasks
    exchange
    task.Oracle_tasks.symbol
    (if deployment.Oracle_types.active then "ACTIVE" else "INACTIVE")
    deployment.Oracle_types.qty
    deployment.Oracle_types.parameter
    deployment.Oracle_types.deployed
    deployment.Oracle_types.pool_share
    deployment.Oracle_types.remainder;
  List.iter
    (fun w -> Logging.warn_f ~section "%s/%s: %s" exchange task.Oracle_tasks.symbol w)
    deployment.Oracle_types.warnings;
  Lwt.return
    { exchange
    ; symbol = task.Oracle_tasks.symbol
    ; active = deployment.Oracle_types.active
    ; reason = deployment.Oracle_types.reason
    ; qty = deployment.Oracle_types.qty
    ; grid_interval = deployment.Oracle_types.parameter
    ; d_surv = deployment.Oracle_types.d_surv
    ; d_gov = deployment.Oracle_types.d_gov
    ; governing_horizon = deployment.Oracle_types.governing_horizon
    ; deployed = deployment.Oracle_types.deployed
    ; pool_share = deployment.Oracle_types.pool_share
    ; remainder = deployment.Oracle_types.remainder
    ; warnings = deployment.Oracle_types.warnings
    ; updated_at = Unix.gettimeofday ()
    }
;;

(** Run one full analysis pass over the trading assets and publish the
    decisions. Tolerates every failure mode (fetch, analysis, balance); a
    failed asset or account keeps its last-known-good decision. *)
let run_pass
      ?(config = default_config ())
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * class_pool) list)
      ()
  : unit Lwt.t
  =
  let tasks, unsupported =
    Oracle_tasks.resolve_tasks
      ~symbol:""
      ~exchange:"kraken"
      ~exchange_explicit:false
      ~trading
      ~offline:false
  in
  List.iter
    (fun (symbol, exchange) ->
       Logging.warn_f
         ~section
         "unsupported exchange in config.json for capital-oracle modeling: %s (%s); \
          asset keeps its current engine behavior"
         symbol
         (if exchange = "" then "unknown" else exchange))
    unsupported;
  if tasks = []
  then (
    Logging.debug ~section "no runnable assets for the capital oracle this pass";
    Lwt.return_unit)
  else (
    Hashtbl.reset fetch_cache;
    let fng = resolve_fng () in
    let grouped = group_by_account tasks in
    let decisions = ref [] in
    let rec thread_pool pool = function
      | [] -> Lwt.return pool
      | task :: rest ->
        Lwt.catch
          (fun () ->
             decide_asset
               config
               classes
               task
               ~pool
               ~fng
               ~index:(List.length rest + 1)
               ~n_tasks:(List.length tasks)
             >|= fun decision ->
             decisions := decision :: !decisions;
             decision.remainder)
          (fun exn ->
             Logging.warn_f
               ~section
               "capital-oracle analysis failed for %s/%s (%s); keeping last-known-good \
                decision, capital stays in the venue pool"
               task.Oracle_tasks.exchange
               task.Oracle_tasks.symbol
               (Printexc.to_string exn);
             Lwt.return pool)
        >>= fun remaining -> thread_pool remaining rest
    in
    venue_pools tasks
    >>= fun pools ->
    let account_pool account =
      match List.assoc_opt account pools with
      | Some (Some pool) -> Some pool
      | _ -> None
    in
    Lwt_list.iter_s
      (fun (account, account_tasks) ->
         match account_pool account with
         | None -> Lwt.return_unit
         | Some pool ->
           thread_pool pool account_tasks
           >|= fun surplus ->
           Logging.debug_f
             ~section
             "venue %s: pool %.2f, surplus %.2f (idle reserve)"
             (account_id account)
             pool
             surplus)
      grouped
    >>= fun () ->
    publish !decisions;
    Atomic.incr pass_count;
    Atomic.set last_pass_at (Unix.gettimeofday ());
    Atomic.set last_pass_ok true;
    Logging.info_f
      ~section
      "capital-oracle pass complete: %d decision(s) published (pass #%d)"
      (List.length !decisions)
      (Atomic.get pass_count);
    Lwt.return_unit)
;;

(** Start the live runtime: initialize venue metadata once, run the first
    pass immediately, then refresh on the configured cadence. Runs on the Lwt
    scheduler ([Lwt.async]); the engine's domains pick up each published
    snapshot on their next cycle. [on_publish] (optional) is invoked after
    each pass with the full snapshot - the engine uses it to wake domains so
    a new decision applies immediately instead of on the next market event. *)
let start
      ?(config = default_config ())
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * class_pool) list)
      ?(on_publish : decision list -> unit = fun _ -> ())
      ()
  =
  Random.self_init ();
  Atomic.set shutdown_requested false;
  let tasks, _ =
    Oracle_tasks.resolve_tasks
      ~symbol:""
      ~exchange:"kraken"
      ~exchange_explicit:false
      ~trading
      ~offline:false
  in
  let rec loop () =
    if Atomic.get shutdown_requested
    then Lwt.return_unit
    else
      Lwt.catch
        (fun () -> run_pass ~config ~trading ~classes ())
        (fun exn ->
           Atomic.set last_pass_ok false;
           Logging.error_f
             ~section
             "capital-oracle pass failed (%s); keeping last-known-good decisions"
             (Printexc.to_string exn);
           Lwt.return_unit)
      >>= fun () ->
      on_publish (decisions ());
      Lwt_unix.sleep (config.refresh_seconds +. Random.float 15.0) >>= loop
  in
  (* Venue instrument metadata is initialized once here (idempotent; the
     supervisor keeps its own). *)
  Lwt.async (fun () ->
    Lwt.catch
      (fun () -> Oracle_venues.init tasks)
      (fun exn ->
         Logging.warn_f
           ~section
           "venue instrument metadata init failed (%s); increments fall back"
           (Printexc.to_string exn);
         Lwt.return_unit)
    >>= fun () -> loop ());
  ()
;;

let shutdown () = Atomic.set shutdown_requested true
