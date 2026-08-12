(* Oracle_runtime - the live-engine realtime capital oracle.

   The CLI (bin/oracle.ml) analyzes the config's trading assets on demand and
   prints a report. This module runs the same pipeline continuously inside the
   trading engine: each pass resolves the trading assets, fetches their
   histories, runs the deployment engine per venue account, and publishes one
   decision per asset to a lock-free snapshot. Trading domains read their
   asset's decision every cycle (an Atomic.get of an immutable list) and adopt
   the qty / grid_interval / active flags; an inactive asset stops placing new
   orders and its capital passes to the next asset in the account's priority
   order.

   Allocation is two-phase and joint across each account's assets. Phase A
   analyzes every asset first, computing each one's sizing reservation: the
   funding needed to reach its governing drawdown at the tightest config grid
   interval (minimum ladder cost at the sizing floor, plus the first-buy cost).
   Phase B then sizes sequentially, highest priority first, against a budget
   that is the remaining pool minus a reserve for the lower-priority assets'
   minimum drawdown funding - so a priority asset only grows after the rest of
   the account can still fill their drawdowns at minimum qty, and an asset is
   disabled only when even its first buy cannot be funded. With enough capital
   every active asset reaches its full drawdown runway; only under a genuine
   capital shortage does the lowest-priority asset go inactive.

   Deployment is target-driven: every asset is sized to meet the target
   survival and nothing more. A normal (authoritative-history) asset grows its
   order qty until the ladder's runway through the governing drawdown consumes
   the budget it is handed, or the replayed path can no longer clear the
   target survival (the sizing is then shrunk back to the largest surviving
   qty and the excess passes to the next asset). An immature (fallback) asset
   is pinned at the sizing floor - the observed drawdown is fully funded at
   the floor and a larger qty would deploy more capital for zero additional
   survival - so its deployment is exactly its reservation and the rest of the
   pool passes down the priority order. The qty only ever respects the venue
   floor (>= qty_min), the survival constraint and the optional qty_cap_mult
   ceiling (config-only concentration limit; the runtime default 0.0 =
   uncapped); the config qty remains the template/fallback, not a ceiling.

   A fully deployed account shows a ~zero available-quote pool, so every asset
   publishes inactive ("cannot fund the first buy") while it awaits sell fills
   to restore capital. This is a normal state, never a failure: when a fill
   returns quote to the account, the next pass re-sizes the assets in config
   priority order and the domains resume on the published decision. The runtime
   polls at the fast [poll_seconds] cadence while no asset is active so a
   capital return is recognized quickly instead of waiting for the full
   refresh cadence.

   Passes are also event-driven: the engine calls [request_pass] (one lock-free
   Atomic increment, microsecond-scale, from the domain worker loop) on every
   fill and canceled/rejected/expired order, and the runtime wakes within ~50ms
   instead of sleeping out the cadence - so a decision that may change with the
   parameters (pool, sizing, survival) is recomputed and re-published as soon
   as the market state that could change it moves, without busy-waiting.
   Bursts of events coalesce into a single pass through [min_trigger_gap].

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
  ; d_cover : float
    (** The sizing drawdown: the largest ACTUAL peak-to-valley drawdown of
        the asset's history (the fall from the current price the grid must
        fund). No ATH anchoring - a 1000x run-up never reads as a phantom
        99.9% drawdown. *)
  ; governing_horizon : string (** The binding horizon. *)
  ; deployed : float (** Capital the ladder consumes at the recommended sizing. *)
  ; pool_share : float (** Capital this asset drew from the venue pool. *)
  ; remainder : float (** Pool share passed to the next asset. *)
  ; range : Oracle_types.range_stats option
    (** Per-asset historical price-range reference (ATH / low / position).
        Display context only. *)
  ; p2v : Oracle_types.p2v_stats option
    (** The largest actual peak-to-valley drawdown event (peak -> valley,
        dates): where [d_cover] comes from. *)
  ; parameter_components : Oracle_types.parameter_components
    (** How the resolved grid interval was composed: the F&G side (crypto
        only), the survival-constrained parameter, the per-asset range side
        and the weights they carried in the blend. Consumers adopt
        [resolved_parameter] (== [grid_interval]) and never recompute a
        competing F&G-only value over the config range - the blend is the
        single source of truth for the grid interval while the oracle holds
        a decision. *)
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
  ; range_weight : float
    (** Weight of the per-asset historical range side in the gi blend
        (default 0.25): near the ATH the potential fall is the whole
        historical span, so spacing widens (preserve runway); near the lows
        the remaining downside is bounded, so spacing tightens (aggressive
        accumulation zone). The survival side carries the remainder, and the
        blend is never tighter than the survival-constrained parameter. *)
  ; min_active_dsurv : float
    (** Assets whose replayed D_surv stays below this are recommended inactive
        and their capital passes down (default 0.0 = fundable means active). *)
  ; qty_cap_mult : float
    (** Deployment ceiling as a multiple of the config qty (default 0.0 =
        uncapped: each asset grows its qty to deploy the whole pool share it
        is handed, bounded only by the survival replay; a value > 0 caps each
        asset's deployment at [config qty * mult] so surplus capital passes
        down the priority order). *)
  ; no_deep_history : bool (** Disable the Yahoo deep-history extension. *)
  ; weight_by_sessions : bool (** Weight class members by session count. *)
  ; refresh_seconds : float (** Cadence between analysis passes (default 300). *)
  ; poll_seconds : float
    (** Fast cadence used while no asset is active (default 30): a fully
        deployed account with a ~zero pool polls at this rate so capital that
        becomes available on sell fills is recognized (and trading resumed in
        config priority order) quickly. *)
  ; horizons : int list option (** Session horizons (default per calendar kind). *)
  ; max_capital : float option (** Upper bound of the sizing binary search. *)
  ; startup_wait_seconds : float
    (** How long a trading domain withholds grid strategy execution while it
        waits for the runtime's first published decision for its asset before
        falling back to config/F&G sizing (default 60). The runtime's first
        pass fetches histories and can take several seconds; the wait keeps
        the strategy from placing/amending orders with un-blessed default
        sizing and then bouncing to the oracle values when the first decision
        lands. *)
  ; assets : (string * asset_overrides) list
    (** Per-asset override layer (config.json "oracle" -> "assets", keyed by
        the trading-config symbol, case-insensitive): present keys replace
        the global knobs for that asset's sizing/blend/history only. Capital
        pooling stays venue-level. A "venue/symbol" key wins over "symbol". *)
  }

(** Partial per-asset knobs (all optional): merged onto the global
    [runtime_config] by [resolve_for]. Only sizing/blend/history knobs are
    overridable; cadence and wait machinery ([refresh_seconds], [poll_seconds],
    [max_capital], [startup_wait_seconds]) stays global. *)
and asset_overrides =
  { target_survival : float option
  ; fng_weight : float option
  ; range_weight : float option
  ; min_active_dsurv : float option
  ; qty_cap_mult : float option
  ; no_deep_history : bool option
  ; weight_by_sessions : bool option
  ; horizons : int list option
  }

let default_config () =
  { target_survival = 0.99
  ; fng_weight = 0.5
  ; range_weight = 0.25
  ; min_active_dsurv = 0.0
  ; qty_cap_mult = 0.0
  ; no_deep_history = false
  ; weight_by_sessions = true
  ; refresh_seconds = 300.0
  ; poll_seconds = 30.0
  ; horizons = None
  ; max_capital = None
  ; startup_wait_seconds = 60.0
  ; assets = []
  }
;;

(** Human-readable list of the present knobs in a per-asset override, for the
    startup summary. Only present (Some) knobs are listed; the all-None record
    yields []. *)
let override_fields (o : asset_overrides) : string list =
  let fields = ref [] in
  let push : 'a. 'a option -> ('a -> string) -> unit =
    fun opt fmt ->
    match opt with
    | Some v -> fields := fmt v :: !fields
    | None -> ()
  in
  push o.target_survival (Printf.sprintf "target_survival %.2f");
  push o.fng_weight (Printf.sprintf "fng_weight %.2f");
  push o.range_weight (Printf.sprintf "range_weight %.2f");
  push o.min_active_dsurv (Printf.sprintf "min_active_dsurv %.2f");
  push o.qty_cap_mult (Printf.sprintf "qty_cap_mult %.2f");
  push o.no_deep_history (Printf.sprintf "deep_history %b");
  push o.weight_by_sessions (Printf.sprintf "weight_by_sessions %b");
  push o.horizons (fun hs ->
    Printf.sprintf "horizons [%s]" (String.concat "," (List.map string_of_int hs)));
  List.rev !fields
;;

(** Resolve the effective knobs for one asset: the global config with the
    asset's overrides merged in (present keys only). Keys match the
    trading-config symbol case-insensitively; a "venue/symbol" key ("venue"
    from [exchange]) wins over the bare symbol. Unknown keys never match and
    fall through to the global config. *)
let resolve_for (config : runtime_config) ~(exchange : string) (symbol : string) =
  let sym = String.lowercase_ascii symbol in
  let keyed = String.lowercase_ascii (exchange ^ "/" ^ symbol) in
  let find key =
    List.find_map
      (fun (k, o) -> if String.lowercase_ascii k = key then Some o else None)
      config.assets
  in
  let merge (o : asset_overrides) =
    { target_survival = Option.value o.target_survival ~default:config.target_survival
    ; fng_weight = Option.value o.fng_weight ~default:config.fng_weight
    ; range_weight = Option.value o.range_weight ~default:config.range_weight
    ; min_active_dsurv = Option.value o.min_active_dsurv ~default:config.min_active_dsurv
    ; qty_cap_mult = Option.value o.qty_cap_mult ~default:config.qty_cap_mult
    ; no_deep_history = Option.value o.no_deep_history ~default:config.no_deep_history
    ; weight_by_sessions =
        Option.value o.weight_by_sessions ~default:config.weight_by_sessions
    ; horizons =
        (match o.horizons with
         | Some h -> Some h
         | None -> config.horizons)
    ; max_capital = config.max_capital
    ; refresh_seconds = config.refresh_seconds
    ; poll_seconds = config.poll_seconds
    ; startup_wait_seconds = config.startup_wait_seconds
    ; assets = config.assets
    }
  in
  match find keyed with
  | Some o -> merge o
  | None ->
    (match find sym with
     | Some o -> merge o
     | None -> config)
;;

(* Per-asset cache of the last deployment-warning set from [size_asset].
   The oracle runs a pass every refresh and re-derives the same warnings
   (under-funded, parameter clamped, coverage gap) each time; log them at
   warn only when they first appear or change, otherwise keep them at debug
   so the log stays readable. *)
let last_deployment_warnings : (string, string list) Hashtbl.t = Hashtbl.create 32

(* Per-asset cache of the last detail-block text (drawdown event, blend
   composition) so repeated detail is logged at debug, not every pass. *)
let last_detail_lines : (string, string) Hashtbl.t = Hashtbl.create 32

(** The sleep until the next pass. While every published decision is inactive
    (e.g. a fully deployed account with a ~zero available-quote pool, awaiting
    sell fills to restore capital) the runtime polls at the fast [poll_seconds]
    cadence so a capital return is recognized quickly; otherwise it keeps the
    normal [refresh_seconds] cadence. An empty snapshot (nothing has published
    yet) keeps the normal cadence. *)
let next_sleep ~(config : runtime_config) ~(decisions : decision list) =
  let any_active = List.exists (fun (d : decision) -> d.active) decisions in
  if decisions <> [] && not any_active
  then config.poll_seconds
  else config.refresh_seconds
;;

(** Refresh/poll cadence with a small random jitter so passes from multiple
    engine instances (or accounts) do not pile up on the same clock tick. *)
let jittered base = base +. Random.float (Float.min 15.0 (base /. 2.0))

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

(** Completed pass attempts, successes and failures alike: incremented in the
    runtime loop right before [on_publish] wakes the domains, so by the time a
    domain wakes on the publish signal this counter already reflects the
    attempt that just finished. Trading domains open their startup gate once
    the FIRST attempt is done and no decision exists for their asset
    (analysis failed, or the runtime could not complete a pass at all):
    last-known-good is empty at fresh startup, so waiting longer only delays
    the config/F&G fallback the engine intends. *)
let pass_attempts : int Atomic.t = Atomic.make 0

let first_pass_attempt_done () = Atomic.get pass_attempts >= 1

(** Whether the runtime models this asset at all: only assets on the
    known exchanges (kraken, hyperliquid, alpaca) are analyzed and get a
    published decision. Pure name check (same predicate Oracle_tasks uses to
    build its task list), so it is safe to call from a domain before the
    runtime's first pass -- or even if the runtime never started. Assets this
    returns false for are not gated at domain startup and keep trading on
    config/F&G sizing from the first cycle. *)
let tracks_asset ~(exchange : string) ~(symbol : string) : bool =
  ignore symbol;
  Oracle_tasks.known_exchange exchange
;;

let last_pass_at : float Atomic.t = Atomic.make 0.0
let last_pass_ok : bool Atomic.t = Atomic.make true
let shutdown_requested = Atomic.make false

(** Event-driven trigger: the engine calls [request_pass] (a single Atomic
    increment - lock-free, no allocation, no scheduler handoff) whenever a
    fill or a canceled/rejected/expired order could change an asset's pool or
    sizing, and the loop honors it at the next [wait_until] slice instead of
    sleeping out the full cadence. The generation counter makes each wake
    one-shot: [wait_until] captures the value before sleeping, so only a NEW
    [request_pass] (another increment) wakes the next wait. *)
let pass_requested : int Atomic.t = Atomic.make 0

let request_pass () = Atomic.incr pass_requested

(** Minimum gap between event-triggered passes (seconds): a burst of events
    (e.g. the engine ingesting the startup execution snapshot) coalesces into
    one pass instead of one pass per event. *)
let min_trigger_gap = 2.0

(** Sleep until [deadline], waking early when a [request_pass] arrives at
    least [min_trigger_gap] after the last pass. Checked in 50ms slices: a
    trigger is honored within ~50ms of [request_pass] (well inside a
    market-event latency budget) while a plain sleep still holds the exact
    cadence. *)
let rec wait_until ~(deadline : float) ~(generation : int) () =
  if
    Atomic.get pass_requested <> generation
    && Unix.gettimeofday () >= Atomic.get last_pass_at +. min_trigger_gap
  then Lwt.return_unit
  else (
    let now = Unix.gettimeofday () in
    if now >= deadline
    then Lwt.return_unit
    else
      Lwt_unix.sleep (Float.min 0.05 (deadline -. now))
      >>= fun () -> wait_until ~deadline ~generation ())
;;

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

(** Resolved Fear & Greed: the live index (blocking, cached, fallback None).
    [None] means no live reading was ever fetched (missing key, timeout, HTTP
    error): the F&G cache holds genuinely fetched values only, so a failed
    fetch is distinguishable from a neutral reading and the deployment blends
    pure survival instead of a fabricated sentiment value. *)
let resolve_fng () : float option =
  try
    let _ = Cmc.Fear_and_greed.fetch_and_cache_sync () in
    Cmc.Fear_and_greed.get_cached ()
  with
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
             let pool =
               Float.max
                 0.0
                 (Oracle_balances.available_quote snapshot ~quote:account.quote)
             in
             let lines =
               snapshot.balances
               |> List.filter (fun (b : Oracle_balances.balance) ->
                 b.available > 0.0 || b.total > 0.0)
               |> List.map (fun (b : Oracle_balances.balance) ->
                 if Float.abs (b.available -. b.total) < 1e-9
                 then Printf.sprintf "%s %.6g" b.asset b.available
                 else Printf.sprintf "%s %.6g/%.6g" b.asset b.available b.total)
             in
             Logging.info_f
               ~section
               "venue %s balance: %s -> pool $%.2f"
               (account_id account)
               (String.concat " · " lines)
               pool;
             Some pool)
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

(** One asset's sizing reservation: the funding it needs to reach its
    governing drawdown at the tightest config grid interval - the conservative
    (worst-case) end of its range. Lower-priority assets reserve this minimum
    before a higher-priority asset grows, so the joint budget guarantees every
    active asset can fund its drawdown while there is enough capital, and an
    asset is only disabled when even its first buy cannot be funded. *)
type reservation =
  { q_min : float (** sizing floor (venue lot / config qty) *)
  ; d_gov : float (** governing drawdown on the sizing basis *)
  ; d_cover : float (** Actual peak-to-valley sizing drawdown (the runway) *)
  ; governing_horizon : string
  ; fallback : bool (** basis is the raw deepest-observed fallback *)
  ; n_fills : int (** ladder fills through [d_cover] at the tightest interval *)
  ; min_cost : float (** ladder cost at [q_min] through [d_cover] at gi_lo *)
  ; first_buy : float (** cost of the first fill at [q_min] at gi_lo *)
  }

(** One asset's per-pass analysis: everything sizing needs against any pool.
    This is the CLI's run_one core without the report tables: series fetch +
    deepen + members + analysis + model build. [index]/[n_tasks] are only for
    logging. *)
type analysis =
  { exchange : string
  ; symbol : string
  ; asset : Oracle_types.series
  ; calendar_kind : Oracle_types.calendar_kind
  ; grid : G.config
  ; lo : float
  ; hi : float
  ; models : Oracle_replay.blend_model list
  ; reservation : reservation
  ; rc : runtime_config
    (** Effective knobs of the analyzed asset (global + per-asset overrides
        resolved by [resolve_for]) - sizing consumes these, not the global
        config. *)
  }

let analyze_asset
      (rc : runtime_config)
      (classes : (string * class_pool) list)
      (task : Oracle_tasks.task)
      ~(index : int)
      ~(n_tasks : int)
  : analysis Lwt.t
  =
  let exchange = task.Oracle_tasks.exchange in
  let calendar_kind = Oracle_tasks.calendar_kind_of_exchange exchange in
  let tc = task.Oracle_tasks.config in
  (* The asset's own analysis runs on its resolved knobs (global + overrides);
     class-member series stay on the global config - they are shared pool
     inputs feeding the kappa blend, not per-asset decisions. *)
  let global_rc = rc in
  let rc = resolve_for global_rc ~exchange task.Oracle_tasks.symbol in
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
  >>= fun (asset, deep_bars) ->
  load_members global_rc classes tc ~class_name asset
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
  (* Analysis inputs of this pass, so the history/member/horizon basis each
     decision was computed on is traceable in the engine log (debug: it
     repeats every pass unchanged). *)
  Logging.debug_f
    ~section
    "[%d/%d] %s/%s: history %d bars (+%d deep), %d class member(s) [%s], warmup %d, \
     horizons [%s]"
    index
    n_tasks
    exchange
    task.Oracle_tasks.symbol
    n_bars
    deep_bars
    (List.length members)
    (String.concat "," (List.map (fun (m : Oracle_types.series) -> m.symbol) members))
    warmup
    (String.concat "," (List.map (fun (h : Oracle_types.horizon) -> h.label) horizons));
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
  let reservation =
    match Oracle_deploy.governing_basis ~models ~target_survival:rc.target_survival with
    | None ->
      { q_min = D.sizing_floor ~cfg:grid
      ; d_gov = 0.0
      ; d_cover = 0.0
      ; governing_horizon = ""
      ; fallback = false
      ; n_fills = 0
      ; min_cost = 0.0
      ; first_buy = 0.0
      }
    | Some (d_gov, governing_horizon, fallback) ->
      let q_min = D.sizing_floor ~cfg:grid in
      (* The reservation funds the asset's sizing drawdown - the ATH-scaled
         remaining drop to the expected floor for mature assets, the raw
         largest ACTUAL peak-to-valley drawdown for immature fallback assets
         (see Oracle_math.sizing_reference_of). No ATH-to-ATL anchoring: a
         1000x run-up only registers the falls that actually took place. *)
      let d_cover =
        match Oracle_math.sizing_reference_of ~fallback asset with
        | Some r -> r.d_cover
        | None -> d_gov
      in
      let grid_lo = G.set_parameter grid gi_lo in
      let n_fills = G.fills_for_drawdown grid_lo ~d:d_cover in
      let min_cost = G.cost_at grid_lo ~qty:q_min ~n_fills in
      let first_buy = G.cost_at grid_lo ~qty:q_min ~n_fills:1 in
      { q_min; d_gov; d_cover; governing_horizon; fallback; n_fills; min_cost; first_buy }
  in
  (* The reservation (allocation machinery detail) - debug: it repeats every
     pass unchanged. *)
  Logging.debug_f
    ~section
    "[%d/%d] %s/%s reservation: min order %.6g, first buy $%.2f; the %.1f%% worst \
     drawdown needs $%.2f at the tightest grid %.2f%% (%s)"
    index
    n_tasks
    exchange
    task.Oracle_tasks.symbol
    reservation.q_min
    reservation.first_buy
    (reservation.d_cover *. 100.0)
    reservation.min_cost
    gi_lo
    (if reservation.fallback then "raw/fallback history" else "blend target");
  Lwt.return
    { exchange
    ; symbol = task.Oracle_tasks.symbol
    ; asset
    ; calendar_kind
    ; grid
    ; lo = gi_lo
    ; hi = gi_hi
    ; models
    ; reservation
    ; rc
    }
;;

(** Size one analyzed asset against a budget: the pool share handed to it by
    the runtime's allocation (its own budget after reserving the lower-
    priority assets' minimum drawdown funding). Runs the deployment engine and
    logs the decision. [index]/[n_tasks] are only for logging; [venue_pool]
    is the account's total capital (for the log context). *)
let size_asset
      (rc : runtime_config)
      (analysis : analysis)
      ~(pool : float)
      ~(venue_pool : float)
      ~(fng : float option)
      ~(index : int)
      ~(n_tasks : int)
  : decision Lwt.t
  =
  let { exchange; symbol; asset; calendar_kind; grid; lo; hi; models; _ } = analysis in
  let deployment =
    D.deploy_asset
      ~asset
      ~cfg:grid
      ~lo
      ~hi
      ~models
      ~target_survival:rc.target_survival
      ~pool
      ~fng
      ~fng_weight:rc.fng_weight
      ~range_weight:rc.range_weight
      ~min_active_dsurv:rc.min_active_dsurv
      ~use_fng:(calendar_kind = Oracle_types.Crypto)
      ~param_steps:10
      ~scan_points:24
      ~qty_cap_mult:rc.qty_cap_mult
  in
  (* ===== Per-asset decision log =====
     INFO: one scannable line per asset, every pass - the heartbeat of the
     decision ("what is it doing, with how much, against which drawdown, is
     it funded"). DEBUG / on-change: the detail block (drawdown event prices
     and dates, model horizon, F&G blend composition) - repeated detail is
     noise, so it is logged once and then only when it changes. *)
  let base_of symbol =
    match String.split_on_char '/' symbol with
    | b :: _ -> b
    | [] -> symbol
  in
  let horizon_lbl =
    if deployment.Oracle_types.governing_horizon = ""
    then "-"
    else deployment.Oracle_types.governing_horizon
  in
  let key = Printf.sprintf "%s/%s" exchange symbol in
  let p2v_lbl =
    match deployment.Oracle_types.p2v, deployment.Oracle_types.sizing with
    | Some _, Some r when r.Oracle_types.outlier ->
      (* No recovered anchor: the measured floor overshoot funds the asset. *)
      Printf.sprintf
        "drop %.1f%% (floor overshoot)"
        (deployment.Oracle_types.d_cover *. 100.0)
    | Some _, Some r when r.Oracle_types.at_floor ->
      (* Living at/below the expected floor: the remainder is exhausted, the
         measured floor overshoot funds the asset. *)
      Printf.sprintf
        "drop %.1f%% (floor overshoot)"
        (deployment.Oracle_types.d_cover *. 100.0)
    | Some p, Some r when r.Oracle_types.d_cover +. 1e-9 < p.Oracle_types.max_drawdown ->
      (* Partway down from the ATH: only the remaining fall to the expected
         floor is funded (the worst-ever drop is context). *)
      Printf.sprintf
        "drop %.1f%% (worst %.1f%% %s→%s)"
        (deployment.Oracle_types.d_cover *. 100.0)
        (p.Oracle_types.max_drawdown *. 100.0)
        p.Oracle_types.peak_date
        p.Oracle_types.valley_date
    | Some p, _ ->
      (* At/near the ATH (or fallback raw sizing): the full event drawdown. *)
      Printf.sprintf
        "drop %.1f%% (%s→%s)"
        (deployment.Oracle_types.d_cover *. 100.0)
        p.Oracle_types.peak_date
        p.Oracle_types.valley_date
    | None, _ -> Printf.sprintf "drop %.1f%%" (deployment.Oracle_types.d_cover *. 100.0)
  in
  let health =
    if not deployment.Oracle_types.active
    then "inactive"
    else if deployment.Oracle_types.row.passed
    then "funded"
    else "UNDER-FUNDED"
  in
  if deployment.Oracle_types.active
  then
    Logging.info_f
      ~section
      "[%d/%d] %s/%s ACTIVE — buy %.6g %s every %.2f%% | capital $%.2f of $%.2f | drop \
       %s | survives %.1f%% | %s"
      index
      n_tasks
      exchange
      symbol
      deployment.Oracle_types.qty
      (base_of symbol)
      deployment.Oracle_types.parameter
      deployment.Oracle_types.pool_share
      venue_pool
      p2v_lbl
      (deployment.Oracle_types.d_surv *. 100.0)
      health
  else
    Logging.info_f
      ~section
      "[%d/%d] %s/%s INACTIVE — %s | capital $%.2f passes down"
      index
      n_tasks
      exchange
      symbol
      deployment.Oracle_types.reason
      deployment.Oracle_types.remainder;
  (* The detail block: event prices/dates, model horizon, and how the
     resolved gi/qty were weighted (the F&G blend for crypto; pure oracle for
     equities). *)
  let detail = Buffer.create 256 in
  let add fmt =
    Printf.ksprintf
      (fun s ->
         Buffer.add_string detail s;
         Buffer.add_char detail '\n')
      fmt
  in
  if deployment.Oracle_types.active
  then (
    (match deployment.Oracle_types.p2v with
     | Some p ->
       add
         "      worst drop %.1f%% (peak $%.2f on %s → valley $%.2f on %s) · model %.1f%% \
          @ %s"
         (p.Oracle_types.max_drawdown *. 100.0)
         p.Oracle_types.peak
         p.Oracle_types.peak_date
         p.Oracle_types.valley
         p.Oracle_types.valley_date
         (deployment.Oracle_types.d_gov *. 100.0)
         horizon_lbl
     | None ->
       add
         "      worst drop %.1f%% (no actual drawdown in the history) · model %.1f%% @ %s"
         (deployment.Oracle_types.d_cover *. 100.0)
         (deployment.Oracle_types.d_gov *. 100.0)
         horizon_lbl);
    (* The funding reference: where the funded drawdown comes from (the
       ATH-scaled remainder to the expected floor, or the measured floor
       overshoot when the remainder is exhausted / no recovered anchor). *)
    (match deployment.Oracle_types.sizing with
     | Some r when r.Oracle_types.outlier ->
       add
         "      funding: deepest drawdown not recovered — floor overshoot %.1f%%%s"
         (deployment.Oracle_types.d_cover *. 100.0)
         (if Option.is_none r.Oracle_types.overshoot_p90
          then " (no floor-break history: 15% fallback)"
          else " (90th pct)")
     | Some r when r.Oracle_types.at_floor ->
       (match r.Oracle_types.floor_ref with
        | Some floor_ref ->
          add
            "      funding: at/below floor $%.2f — floor overshoot %.1f%%%s"
            floor_ref
            (deployment.Oracle_types.d_cover *. 100.0)
            (if Option.is_none r.Oracle_types.overshoot_p90
             then " (no floor-break history: 15% fallback)"
             else " (90th pct)")
        | None -> ())
     | Some r ->
       (match r.Oracle_types.floor_ref with
        | Some floor_ref ->
          add
            "      funding: drop %.1f%% to floor $%.2f (ATH $%.2f − %.1f%% worst)"
            (deployment.Oracle_types.d_cover *. 100.0)
            floor_ref
            (match deployment.Oracle_types.range with
             | Some rg -> rg.Oracle_types.ath
             | None -> r.Oracle_types.floor_ref |> Option.value ~default:0.0)
            (match deployment.Oracle_types.p2v with
             | Some p -> p.Oracle_types.max_drawdown *. 100.0
             | None -> 0.0)
        | None -> ())
     | None -> ());
    let pc = deployment.Oracle_types.parameter_components in
    match pc.Oracle_types.fng_parameter, pc.Oracle_types.fng with
    | Some fp, Some fng ->
      let w_survival =
        Float.max 0.0 (1.0 -. pc.Oracle_types.fng_weight -. pc.Oracle_types.range_weight)
      in
      let sides, raw_blend =
        match pc.Oracle_types.range_parameter with
        | Some rp ->
          let total =
            pc.Oracle_types.fng_weight +. w_survival +. pc.Oracle_types.range_weight
          in
          ( Printf.sprintf
              "fng %.2f -> %.4f%% (w %.2f) | range %.4f%% (w %.2f) | survival %.4f%% (w \
               %.2f)"
              fng
              fp
              pc.Oracle_types.fng_weight
              rp
              pc.Oracle_types.range_weight
              pc.Oracle_types.survival_parameter
              w_survival
          , ((pc.Oracle_types.fng_weight *. fp)
             +. (w_survival *. pc.Oracle_types.survival_parameter)
             +. (pc.Oracle_types.range_weight *. rp))
            /. total )
        | None ->
          let total = pc.Oracle_types.fng_weight +. w_survival in
          ( Printf.sprintf
              "fng %.2f -> %.4f%% (w %.2f) | survival %.4f%% (w %.2f)"
              fng
              fp
              pc.Oracle_types.fng_weight
              pc.Oracle_types.survival_parameter
              w_survival
          , ((pc.Oracle_types.fng_weight *. fp)
             +. (w_survival *. pc.Oracle_types.survival_parameter))
            /. total )
      in
      let clamp_note =
        if raw_blend +. 1e-9 < pc.Oracle_types.survival_parameter
        then
          Printf.sprintf
            " (blend %.4f%% clamped: survival binds, F&G/range contribute 0)"
            raw_blend
        else ""
      in
      add
        "      gi blend: %s -> resolved %.4f%%%s"
        sides
        pc.Oracle_types.resolved_parameter
        clamp_note;
      (* The qty channel of the same blend: fear up-sizes toward the
         survival-max, greed pulls back toward the floor. When the survival-max
         is the floor itself (under-funded pool) there is no headroom and the
         F&G qty contribution is zero by construction - logged so it is visible
         rather than silent. *)
      let q_min = D.sizing_floor ~cfg:grid in
      let k = 1.0 -. (Float.max 0.0 (Float.min 100.0 fng) /. 100.0) in
      let headroom = deployment.Oracle_types.qty -. q_min > 1e-12 in
      add
        "      qty blend: floor %.6g -> resolved %.6g (fng %.2f, k %.2f%s)"
        q_min
        deployment.Oracle_types.qty
        fng
        k
        (if headroom then "" else "; no headroom: F&G qty contribution 0")
    | None, Some _ ->
      (* Equity (pure oracle): no sentiment blend on gi or qty - the
         survival-constrained values are adopted. *)
      add
        "      sizing: pure oracle — gi %.4f%% (survival-constrained) · qty %.6g \
         (survival-max)"
        pc.Oracle_types.survival_parameter
        deployment.Oracle_types.qty
    | None, None ->
      add
        "      sizing: no live Fear & Greed — model only (gi %.4f%%)"
        pc.Oracle_types.survival_parameter
    | Some _, None -> ());
  let detail_str = Buffer.contents detail in
  let detail_changed =
    match Hashtbl.find_opt last_detail_lines key with
    | Some prev -> prev <> detail_str
    | None -> true
  in
  Hashtbl.replace last_detail_lines key detail_str;
  if detail_str <> ""
  then
    if detail_changed
    then Logging.info ~section detail_str
    else Logging.debug ~section detail_str;
  (* ATH/ATL context (order-independent, display only). *)
  (match deployment.Oracle_types.range with
   | Some r ->
     Logging.debug_f
       ~section
       "[%d/%d] %s/%s range context: ATH %.2f, low %.2f, price %.2f, span %.1f%%"
       index
       n_tasks
       exchange
       symbol
       r.Oracle_types.ath
       r.Oracle_types.all_time_low
       r.Oracle_types.price
       (r.Oracle_types.range_span *. 100.0)
   | None -> ());
  let warnings = deployment.Oracle_types.warnings in
  let warn_key = Printf.sprintf "%s/%s" exchange symbol in
  let warnings_changed =
    match Hashtbl.find_opt last_deployment_warnings warn_key with
    | Some prev -> prev <> warnings
    | None -> true
  in
  Hashtbl.replace last_deployment_warnings warn_key warnings;
  List.iter
    (fun w ->
       if warnings_changed
       then Logging.warn_f ~section "%s/%s: %s" exchange symbol w
       else Logging.debug_f ~section "%s/%s: %s (unchanged)" exchange symbol w)
    warnings;
  Lwt.return
    ({ exchange
     ; symbol
     ; active = deployment.Oracle_types.active
     ; reason = deployment.Oracle_types.reason
     ; qty = deployment.Oracle_types.qty
     ; grid_interval = deployment.Oracle_types.parameter
     ; d_surv = deployment.Oracle_types.d_surv
     ; d_gov = deployment.Oracle_types.d_gov
     ; d_cover = deployment.Oracle_types.d_cover
     ; governing_horizon = deployment.Oracle_types.governing_horizon
     ; deployed = deployment.Oracle_types.deployed
     ; pool_share = deployment.Oracle_types.pool_share
     ; remainder = deployment.Oracle_types.remainder
     ; range = deployment.Oracle_types.range
     ; p2v = deployment.Oracle_types.p2v
     ; parameter_components = deployment.Oracle_types.parameter_components
     ; warnings = deployment.Oracle_types.warnings
     ; updated_at = Unix.gettimeofday ()
     }
     : decision)
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
    Logging.info ~section "no runnable assets for the capital oracle this pass";
    Lwt.return_unit)
  else (
    Hashtbl.reset fetch_cache;
    let fng = resolve_fng () in
    let grouped = group_by_account tasks in
    Logging.info_f
      ~section
      "capital-oracle pass #%d starting: %d asset(s) in %d account(s)%s"
      (Atomic.get pass_count + 1)
      (List.length tasks)
      (List.length grouped)
      (match fng with
       | Some f -> Printf.sprintf ", f&g %.2f" f
       | None -> ", f&g unavailable");
    let decisions = ref [] in
    (* Phase A: analyze every asset in priority order FIRST, so each asset's
       sizing reservation (minimum drawdown funding + first-buy cost) is known
       before any capital is handed out. *)
    let rec analyze_all acc = function
      | [] -> Lwt.return (List.rev acc)
      | task :: rest ->
        Lwt.catch
          (fun () ->
             analyze_asset
               config
               classes
               task
               ~index:(List.length rest + 1)
               ~n_tasks:(List.length tasks)
             >|= fun analysis -> analysis :: acc)
          (fun exn ->
             Logging.warn_f
               ~section
               "capital-oracle analysis failed for %s/%s (%s); keeping last-known-good \
                decision, capital stays in the venue pool"
               task.Oracle_tasks.exchange
               task.Oracle_tasks.symbol
               (Printexc.to_string exn);
             Lwt.return acc)
        >>= fun acc' -> analyze_all acc' rest
    in
    (* Phase B: size sequentially, highest priority first. Each asset's budget
       is the remaining pool minus a reserve for the lower-priority assets'
       minimum drawdown funding, so a priority asset only grows after the rest
       of the account can still fill their drawdowns at minimum qty - and an
       asset is disabled only when even its first buy cannot be funded. *)
    let rec allocate ~venue_pool pool = function
      | [] -> Lwt.return pool
      | analysis :: rest ->
        let reserve =
          Float.min
            (List.fold_left
               (fun acc (a : analysis) -> acc +. a.reservation.min_cost)
               0.0
               rest)
            (Float.max 0.0 (pool -. analysis.reservation.first_buy))
        in
        let budget = pool -. reserve in
        Lwt.catch
          (fun () ->
             size_asset
               analysis.rc
               analysis
               ~pool:budget
               ~venue_pool
               ~fng
               ~index:(List.length rest + 1)
               ~n_tasks:(List.length tasks)
             >|= fun decision ->
             decisions := decision :: !decisions;
             decision.remainder +. reserve)
          (fun exn ->
             Logging.warn_f
               ~section
               "capital-oracle sizing failed for %s/%s (%s); keeping last-known-good \
                decision, capital stays in the venue pool"
               analysis.exchange
               analysis.symbol
               (Printexc.to_string exn);
             Lwt.return pool)
        >>= fun next -> allocate ~venue_pool next rest
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
           analyze_all [] account_tasks
           >>= fun analyses ->
           allocate ~venue_pool:pool pool analyses
           >|= fun surplus ->
           Logging.info_f
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
  (* Startup summary: the effective runtime knobs, the class pools feeding the
     kappa blend, and the assets/accounts the oracle will track, so what the
     live engine's oracle is doing is visible from the first log lines. *)
  Logging.info_f
    ~section
    "runtime knobs: target_survival %.2f, fng_weight %.2f, range_weight %.2f, \
     min_active_dsurv %.2f, qty_cap_mult %.2f, weight_by_sessions %b, deep_history %s, \
     refresh %.0fs, poll %.0fs, horizons [%s]"
    config.target_survival
    config.fng_weight
    config.range_weight
    config.min_active_dsurv
    config.qty_cap_mult
    config.weight_by_sessions
    (if config.no_deep_history then "off" else "on")
    config.refresh_seconds
    config.poll_seconds
    (match config.horizons with
     | Some ns -> String.concat "," (List.map string_of_int ns)
     | None -> "default");
  (* Per-asset override map: resolved at every pass for each task. A key that
     matches no tracked symbol (checked against "symbol" and "venue/symbol")
     is a config typo - surface it once at startup. *)
  let known_key key =
    List.exists
      (fun (task : Oracle_tasks.task) ->
         let sym = task.Oracle_tasks.symbol in
         let keyed = task.Oracle_tasks.exchange ^ "/" ^ sym in
         String.lowercase_ascii key = String.lowercase_ascii sym
         || String.lowercase_ascii key = String.lowercase_ascii keyed)
      tasks
  in
  if config.assets <> []
  then
    List.iter
      (fun ((key, o) : string * asset_overrides) ->
         let fields = override_fields o in
         if not (known_key key)
         then
           Logging.warn_f
             ~section
             "asset override '%s' matches no tracked symbol (trading config); ignored - \
              fix the key or remove it"
             key;
         Logging.info_f
           ~section
           "asset overrides %s: %s"
           key
           (if fields = [] then "(empty)" else String.concat ", " fields))
      config.assets;
  List.iter
    (fun ((name, pool) : string * class_pool) ->
       Logging.info_f
         ~section
         "class '%s': members [%s], kappa %s"
         name
         (String.concat "," pool.members)
         (match pool.kappa with
          | Some k -> string_of_int k
          | None -> "default"))
    classes;
  List.iter
    (fun (task : Oracle_tasks.task) ->
       Logging.info_f
         ~section
         "tracking %s/%s (account %s, class %s)"
         task.Oracle_tasks.exchange
         task.Oracle_tasks.symbol
         (account_id (account_of_task task))
         (match task.Oracle_tasks.config.asset_class with
          | Some c -> c
          | None -> "default"))
    tasks;
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
      (* Mark the attempt finished BEFORE waking the domains: a domain that
         wakes on this publish signal must already see it in its gate check. *)
      Atomic.incr pass_attempts;
      on_publish (decisions ());
      (* The next pass runs at the cadence deadline, or early when the engine
         requests one ([request_pass] on fills / canceled-rejected-expired
         events): the wait captures the current generation, so only a NEW
         request wakes it, and honors the request at the next 50ms slice
         (coalescing bursts through [min_trigger_gap]). *)
      let generation = Atomic.get pass_requested in
      let deadline =
        Unix.gettimeofday () +. jittered (next_sleep ~config ~decisions:(decisions ()))
      in
      wait_until ~deadline ~generation () >>= loop
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
