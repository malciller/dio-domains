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
   Phase B then sizes strictly in config (priority) order: each asset deploys
   against the ENTIRE remaining pool - the priority asset is sized fully
   before the next asset draws anything - and whatever a deployment does not
   consume passes down the priority order. Nothing is reserved for
   lower-priority assets, so a capital shortage never starves the priority
   asset to its first buy and never lets the least-prioritized asset absorb
   the pool; only a fully funded account idles surplus. Capital pooling is
   venue-locked: every asset of an account draws from the one account pool,
   and the pass-down guarantees pool resources are never double-counted (the
   sizing budget of each asset is the pool MINUS everything the higher
   priority assets consumed).

   Deployment is survival-driven over the FULL config ranges (the sizing
   rules): the grid interval is the most aggressive (tightest) value in the
   config range [gi_min, gi_max] that reaches 100% replay survival at the
   minimum order size - "first get 100% survivability at the most aggressive
   grid_interval(min,max)" - and the order qty then grows, only while 100%
   survival holds, to deploy the pool: the largest qty in
   [qty_min, qty_min * qty_cap_mult] that still survives the whole replayed
   history (qty_cap_mult is the cap, not a rule - the qty only grows to
   deploy residual capital behind 100% survivability, and a value <= 0 means
   the qty never grows). When NO grid interval reaches 100% survival, the
   deployment stretches: the minimum qty at the grid interval MAXIMUM - the
   widest spacing the config allows stretches the capital's survival as far
   as possible (the order size is never increased in this mode). An asset
   whose pool cannot fund even its first buy at the minimum order size is
   inactive and its whole share passes down the priority order - so with the
   stretch sizing a priority asset consumes only what its minimum-order
   ladder needs and the next asset still funds its own first order. An
   under-funded ACTIVE grid that has a committed resting buy is the one
   exception: its first buy is already funded (the cost is locked in the
   account balance, which is why the pool reads low), so it keeps running on
   that committed capital and draws NOTHING new from the available pool -
   its whole share passes down, letting a lower-priority asset fund its own
   first order instead of being starved by an under-funded priority grid
   that cannot use the capital anyway. A fully deployed account shows a
   ~zero available-quote pool, so every asset publishes inactive ("cannot
   fund the first buy") while it awaits sell fills to restore capital. This
   is a normal state, never a failure: when a fill returns quote to the
   account, the next pass re-sizes the assets in config priority order and
   the domains resume on the published decision. The runtime polls at the
   fast [poll_seconds] cadence while no asset is active so a capital return
   is recognized quickly instead of waiting for the full refresh cadence.

   PRIORITY RECLAMATION: when a higher-priority asset fills its last buy and
   the available pool can no longer fund a replacement while a lower-priority
   asset still holds committed buy capital, the account is under-deployed in
   priority order. The runtime computes a reclamation plan per account every
   pass (see Oracle_reclaim): it selects the FEWEST lower-priority resting
   buys - lowest priority first - whose committed capital closes the funding
   gap, and publishes those assets INACTIVE-with-reclaim (reclaim_capital) so
   their domains cancel the resting buy(s). The released capital returns to
   the account pool, the cancel event triggers the next pass, and the
   higher-priority asset re-sizes ACTIVE and resumes. A lower-priority asset
   is never reclaimed unless the deallocation actually funds a higher-priority
   first buy - otherwise it stays active on its committed capital.

   SELLS ARE NEVER CAPITAL-GATED: a grid whose buy placement is halted (pool
   exhausted, or oracle INACTIVE) still places the sell for a just-filled buy
   - the sell is the account's capital recovery path (it needs no quote, only
   inventory), so inventory is never left unreclaimable.

   Passes are fully event-driven: the engine calls [request_pass] (a lock-free
   Atomic increment plus an [Lwt_condition] broadcast, microsecond-scale, from
   the domain worker loop) on every fill and canceled/rejected/expired order,
   and the loops wake immediately on the condition - no polling slices, no
   minimum-trigger gap. A fill/cancel also enqueues its pool delta
   ([notify_fill] / [notify_order_cancel]), which the pass applies to the
   account pool IN PROCESS - the decision path never waits on a network
   balance refresh; the background refresher reconciles the authoritative
   value afterwards. Bursts of events coalesce naturally: one drain consumes
   the whole queue per pass, and the account-sizing memo cache turns an
   unchanged account into a microsecond re-publish.

   Failures are last-known-good: an asset whose analysis fails this pass keeps
   its previous decision, an account whose balance cannot be fetched is skipped
   entirely, and an entirely failed pass publishes nothing. The runtime never
   crashes the engine. *)

open Lwt.Infix
module G = Oracle_strategy.Grid
module D = Oracle_deploy.Engine (G)
module Exchange = Dio_exchange.Exchange_intf

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
  ; reclaim_capital : bool
    (** Priority reclamation: the domain must cancel this asset's resting
        buy(s) so the committed capital returns to the account pool for a
        higher-priority asset (see the reclamation pass). *)
  ; reclaim_target : string
    (** The higher-priority asset the reclaimed capital funds ("" when not
        reclaiming). *)
  ; range : Oracle_types.range_stats option
    (** Per-asset historical price-range reference (ATH / low / position).
        Display context only. *)
  ; p2v : Oracle_types.p2v_stats option
    (** The largest actual peak-to-valley drawdown event (peak -> valley,
        dates): where [d_cover] comes from. *)
  ; parameter_components : Oracle_types.parameter_components
    (** How the resolved grid interval was chosen: the survival-driven value
        (the tightest config parameter with 100% replay survival at the
        minimum order size, or the grid maximum in stretch mode). Consumers
        adopt [resolved_parameter] (== [grid_interval]) and never recompute a
        competing F&G-only value over the config range - the sizing is the
        single source of truth for the grid interval while the oracle holds
        a decision. The F&G/range weights are carried for the record only;
        they are inert in the sizing. *)
  ; gi_reason : string (** Why the grid interval is what it is (observability). *)
  ; qty_reason : string (** Why the order qty is what it is (observability). *)
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
  { target_survival : float
    (** Blended survival target for the governing-basis selection and the
        warnings (default 0.99). The SIZING itself targets 100% replay
        survival over the full config range; [target_survival] decides which
        horizons reach the target (the governing drawdown) and when the
        history is flagged immature. *)
  ; fng_weight : float
    (** Kept for config compatibility; INERT in sizing - the grid interval
        and qty are survival-driven over the config ranges (no sentiment
        blend). *)
  ; range_weight : float
    (** Kept for config compatibility; INERT in sizing (see [fng_weight]). *)
  ; min_active_dsurv : float
    (** Assets whose replayed D_surv stays below this are recommended inactive
        and their capital passes down (default 0.0 = fundable means active). *)
  ; qty_cap_mult : float
    (** The qty ceiling as a multiple of the config qty: the order qty never
        grows beyond [config qty * mult], and it only grows at all - to
        deploy residual capital - while 100% replay survival holds (the cap
        is a ceiling, not a rule; default 0.0 = the qty never grows beyond
        the minimum). *)
  ; no_deep_history : bool (** Disable the Yahoo deep-history extension. *)
  ; weight_by_sessions : bool (** Weight class members by session count. *)
  ; refresh_seconds : float
    (** Cadence of full history refresh (default 300): new bars / class
        members / deep history. Also the pass loop's safety-net deadline (the
        decision path is event-driven; this only bounds conditions that emit
        no event). *)
  ; poll_seconds : float
    (** Cadence of the background refresher's authoritative balance
        reconciliation (default 30). The decision path is event-driven and
        never waits on this - fills/cancels apply their pool deltas locally at
        event time ([notify_fill] / [notify_order_cancel]); this timer keeps
        the materialized venue pools honest against drift and missed events. *)
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

(** The safety-net sleep until the next pass. The decision path is
    event-driven - fills, order events, and refresher publishes wake it via
    [wake_condition] - so this cadence is only a bound for conditions that
    emit no event (e.g. drift in the live top-of-book anchor the sizing
    re-reads at decision time). It is always the refresh cadence: the fast
    poll cadence previously used while nothing was active existed to
    recognize capital returns quickly, which the local pool deltas now do at
    event time. *)
let next_sleep ~(config : runtime_config) ~(decisions : decision list) =
  ignore decisions;
  config.refresh_seconds
;;

(** Refresh/poll cadence with a small random jitter so passes from multiple
    engine instances (or accounts) do not pile up on the same clock tick. *)
let jittered base = base +. Random.float (Float.min 15.0 (base /. 2.0))

(** The published snapshot: an immutable list swapped atomically. Domain
    workers call [decision_for] every cycle; the write side only ever replaces
    the whole list in one Atomic.set, so readers never observe a torn state. *)
let decisions_ref : decision list Atomic.t = Atomic.make []

(* H5: copy-on-write keyed lookup. The single writer (publish) builds a fresh
   Hashtbl with pre-lowercased "exchange/symbol" keys and swaps the reference;
   readers do one [Atomic.get] + Hashtbl.find, with no per-cycle lowercasing
   and no O(N) scan. The table is never mutated after publication, so
   concurrent
   readers are safe under the OCaml 5 memory model. *)
let decisions_map : (string, decision) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 16)
;;

let key_of (d : decision) =
  String.lowercase_ascii d.exchange ^ "/" ^ String.lowercase_ascii d.symbol
;;

(** Pass publication generation: bumped once per [publish], so trading
    domains keyed on it re-read their decision the moment a new pass
    publishes. The refresher's [refresh_generation] only changes on
    background-refresh cycles; keying the domains' decision cache on that
    would leave a decision that changed on a PASS invisible until the next
    refresh cycle - exactly the stall that breaks priority reclamation: the
    INACTIVE-with-reclaim decision (and the later re-activation) must reach
    the domain promptly or the cancelled buy is never released and the
    priority asset never resumes. *)
let publish_generation : int Atomic.t = Atomic.make 0

let get_publish_generation () = Atomic.get publish_generation

(** Symbols whose decision changed on the most recent [publish] (appeared,
    changed value, or changed reclaim state). Consumed by the engine's
    per-symbol domain wakeups so an unrelated asset's domain never wakes on a
    pass that did not touch it. Written by [publish], read by [run_loop] on
    the same Lwt scheduler thread - no locking needed. *)
let last_changed_symbols : string list ref = ref []

(** The whole current decision snapshot: the atomically-swapped immutable
    list, used by [on_publish] and the loop's cadence/reclaim logic. Readers
    always see a consistent list, never a torn mid-swap state. *)
let decisions () = Atomic.get decisions_ref

(** Domain-safe lookup: exchange and symbol are lower-cased for the match so
    config spelling (BTC/USD vs btc/usd) never matters. O(1) hashtable hit on
    the per-cycle hot path (no list scan, no per-candidate lowercasing). *)
let decision_for ~(exchange : string) ~(symbol : string) : decision option =
  let key = String.lowercase_ascii exchange ^ "/" ^ String.lowercase_ascii symbol in
  Hashtbl.find_opt (Atomic.get decisions_map) key
;;

(** Merge [fresh] decisions over the current snapshot: assets absent from
    [fresh] (analysis failed, account balance unavailable, or the asset was
    removed) keep their previous decision, so a partial or failed pass never
    halts trading on stale knowledge. Assets in [fresh] replace their old
    decision. *)
let publish (fresh : decision list) =
  let prev = Atomic.get decisions_ref in
  let fresh_keys = List.map key_of fresh in
  let kept =
    List.filter (fun (d : decision) -> not (List.mem (key_of d) fresh_keys)) prev
  in
  Atomic.set decisions_ref (kept @ fresh);
  (* Rebuild the keyed map copy-on-write for the next readers. *)
  let map = Hashtbl.create (List.length (kept @ fresh)) in
  List.iter (fun (d : decision) -> Hashtbl.replace map (key_of d) d) (kept @ fresh);
  Atomic.set decisions_map map;
  (* Domains keyed on [get_publish_generation] re-read their decision on the
     next cycle: a new pass (a new reclaim, a re-activation) is adopted
     immediately, not at the next background-refresh cycle. *)
  Atomic.incr publish_generation;
  (* Changed-only wakeup set: symbols whose decision appeared, changed, or was
     removed this pass. The engine signals only those domains (per-symbol),
     so an unrelated asset's domain never wakes on a pass that did not touch
     it. *)
  let changed =
    List.filter_map
      (fun (d : decision) ->
         let key = key_of d in
         let changed =
           match List.find_opt (fun (p : decision) -> key_of p = key) prev with
           | None -> true
           | Some p ->
             p.active <> d.active
             || p.qty <> d.qty
             || p.grid_interval <> d.grid_interval
             || p.reclaim_capital <> d.reclaim_capital
             || p.reason <> d.reason
         in
         if changed then Some d.symbol else None)
      (kept @ fresh)
  in
  last_changed_symbols := changed
;;

(** Pass counters for observability (logged, never read on a hot path). *)
let pass_count : int Atomic.t = Atomic.make 0

(* ------------------------------------------------------------------ *)
(* Oracle engine latency metrics.                                     *)
(* ------------------------------------------------------------------ *)

(** Engine-global latency profilers for one analysis pass, windowed per pass
    (published at the end of every pass). The dashboard reads the published
    snapshots through [profiler_snapshots]. All recording happens on the Lwt
    scheduler thread (passes are sequential in one fiber), so the live
    histograms need no extra locking; readers touch only the atomically
    published snapshot of the completed window. *)
type engine_profilers =
  { prof_pass : Latency_profiler.t
    (** Whole [run_pass]: cadence-to-cadence oracle work. *)
  ; prof_balance : Latency_profiler.t
    (** Venue pool / balance resolution (Fear & Greed + live store or REST
        balance phase). *)
  ; prof_fetch : Latency_profiler.t
    (** Per-asset history fetch + analysis phase (Phase A). *)
  ; prof_sizing : Latency_profiler.t (** Sizing / deployment phase (Phase B). *)
  }

let engine_profs =
  { prof_pass =
      Latency_profiler.create ~bucket_us:1_000 ~max_latency_us:60_000_000 "oracle:pass"
  ; prof_balance =
      Latency_profiler.create ~bucket_us:1_000 ~max_latency_us:60_000_000 "oracle:balance"
  ; prof_fetch =
      Latency_profiler.create ~bucket_us:1_000 ~max_latency_us:60_000_000 "oracle:fetch"
  ; prof_sizing =
      Latency_profiler.create ~bucket_us:100 ~max_latency_us:10_000_000 "oracle:sizing"
  }
;;

(** Per-asset latency profilers: one histogram per tracked asset covering its
    whole per-pass pipeline (fetch + analysis + sizing). Keyed by the trading
    config symbol so the dashboard can match domain rows; windows are
    published per pass, like the engine-global profilers. *)
let asset_profiler_cache : (string, Latency_profiler.t) Hashtbl.t = Hashtbl.create 16

let asset_profiler_of symbol =
  match Hashtbl.find_opt asset_profiler_cache symbol with
  | Some prof -> prof
  | None ->
    let prof =
      Latency_profiler.create
        ~bucket_us:1_000
        ~max_latency_us:60_000_000
        (symbol ^ ":oracle")
    in
    Hashtbl.replace asset_profiler_cache symbol prof;
    prof
;;

(** Record one span into an asset's per-asset profiler. *)
let record_asset_latency symbol span cause =
  Latency_profiler.record_with_cause (asset_profiler_of symbol) span cause
;;

(** Engine-global oracle latency snapshots (most recently completed windows),
    for the dashboard. *)
let profiler_snapshots () =
  [ "pass", Latency_profiler.published_snapshot engine_profs.prof_pass
  ; "balance", Latency_profiler.published_snapshot engine_profs.prof_balance
  ; "fetch", Latency_profiler.published_snapshot engine_profs.prof_fetch
  ; "sizing", Latency_profiler.published_snapshot engine_profs.prof_sizing
  ]
;;

(** Per-asset oracle latency snapshots: (symbol, snapshot option) list, for
    the dashboard's per-domain latency rows. *)
let asset_profiler_snapshots () =
  Hashtbl.fold
    (fun symbol prof acc -> (symbol, Latency_profiler.published_snapshot prof) :: acc)
    asset_profiler_cache
    []
;;

(* ------------------------------------------------------------------ *)
(* Materialized oracle state + background refresh (microsecond        *)
(* decision path).                                                     *)
(*                                                                     *)
(* The decision path (run_pass) never touches the network. A          *)
(* background refresher owns every REST call (balances, history       *)
(* deltas, deep history, class members, the Alpaca calendar) and      *)
(* publishes an immutable materialized world state atomically.        *)
(* Analyses are memoized on the physical identity of their inputs     *)
(* (the refresher reuses bar arrays when nothing changed, so a        *)
(* physical compare is the fast path), and account sizing is          *)
(* memoized on (analysis ids, pool bucket, strategy state). A pass    *)
(* with nothing changed is a fingerprint compare + publish:           *)
(* microseconds.                                                       *)
(* ------------------------------------------------------------------ *)

(** One asset's materialized history: everything the analysis of that
    asset reads, minus live in-process values (the top-of-book anchor is
    re-read at sizing time). All arrays are immutable after publication. *)
type asset_material =
  { am_exchange : string
  ; am_symbol : string
  ; am_tc : Dio_strategies.Strategy_common.trading_config
    (** Fee-enriched trading config (enrichment is network-once, done by
        the refresher). *)
  ; am_bars : Oracle_types.bar array
    (** Merged venue + deep history (raw bars, sorted). *)
  ; am_deep_bars : int
  ; am_members : Oracle_types.series list
    (** Class member series (the kappa blend inputs). *)
  ; am_calendar : Oracle_sessions.model option (** Equity session model (Alpaca only). *)
  ; am_calendar_fp : string
    (** Fingerprint of the calendar dates the model was built from, so a
        rebuilt model object with identical dates is not a change. *)
  }

(** The materialized world state the decision path consumes. Swapped
    atomically as a whole by the refresher; the pass reads the current
    record and never blocks on network. Hashtbls inside are private to
    their record: filled before publication, immutable after. *)
type materialized =
  { m_assets : asset_material list
  ; m_balances : (string, (float * Oracle_balances.snapshot) option) Hashtbl.t
    (** Keyed by [account_id]: the account's available-quote pool and its
        balance snapshot, or None when the venue reported nothing. *)
  ; m_fng : float option
  ; m_epoch : int
    (** Refresh generation this record was published at (==
        [refresh_generation]). *)
  ; m_last_history_at : float
    (** Wall clock of the last history phase (full histories are re-fetched
        at the refresh cadence; balance-only cycles keep the arrays). *)
  }

let materialized_ref : materialized option Atomic.t = Atomic.make None
let materialized () = Atomic.get materialized_ref

(** The background refresher fiber of the current [run_loop] generation:
    cancelled when the loop ends (shutdown) or restarts (supervisor
    auto-restart), so two refreshers can never run at once. *)
let refresh_fiber : unit Lwt.t option ref = ref None

(** Refresh generations: bumped once per published cycle. The pass loop
    wakes early when a cycle CHANGED analysis-relevant inputs
    ([refresh_history_changed]); balance-only cycles never wake the pass
    (the pass's own cadence and the bounded wait on fill events cover
    balance freshness). *)
let refresh_generation : int Atomic.t = Atomic.make 0

(** Monotonic generation counter bumped after every published pass. Domain
    workers cache their resolved decision and only re-look-up when this value
    changes (H5 "changed-only" adoption). *)
let get_refresh_generation () = Atomic.get refresh_generation

let refresh_history_changed : bool Atomic.t = Atomic.make false

(** Per-symbol fetch failure backoff (seconds since epoch): a failed
    history fetch (timeout, venue error) is retried in the background at
    [poll] cadence, but not before [retry_after] - a sick upstream cannot
    make the refresher spin. Grows exponentially on repeated failures. *)
let retry_after : (string, float) Hashtbl.t = Hashtbl.create 32

let retry_backoff : (string, float) Hashtbl.t = Hashtbl.create 32

(** Daily Alpaca calendar cache: keyed by exchange, holds (fetched_date,
    model, fingerprint). Refetched at most once per day. *)
let calendar_cache : (string, string * Oracle_sessions.model * string) Hashtbl.t =
  Hashtbl.create 4
;;

(* --- Analysis memoization (types/caches live after the [analysis]      *)
(*     record definition below, which they reference)                    *)

(** Physical-or-structural equality of two bar arrays: the refresher keeps
    the same array object while nothing changed (fast `==`); otherwise an
    exact full compare (microsecond-class for a few thousand bars). Exact,
    not sampled: a venue correction anywhere in the history must invalidate
    the memoized analysis. *)
let same_bars (a : Oracle_types.bar array) (b : Oracle_types.bar array) =
  a == b
  ||
  let n = Array.length a in
  n = Array.length b
  &&
  let rec go i =
    if i >= n
    then true
    else (
      let ba = a.(i) in
      let bb = b.(i) in
      ba.date = bb.date
      && ba.open_ = bb.open_
      && ba.high = bb.high
      && ba.low = bb.low
      && ba.close = bb.close
      && ba.volume = bb.volume
      && go (i + 1))
  in
  go 0
;;

let same_series (a : Oracle_types.series) (b : Oracle_types.series) =
  a == b || (a.symbol = b.symbol && same_bars a.bars b.bars)
;;

let same_members (a : Oracle_types.series list) (b : Oracle_types.series list) =
  a == b || (List.length a = List.length b && List.for_all2 same_series a b)
;;

let same_fng (a : float option) (b : float option) =
  match a, b with
  | None, None -> true
  | Some x, Some y -> Float.abs (x -. y) < 1e-6
  | _ -> false
;;

(** Whether a new materialized record changed any analysis-relevant input
    versus the previous one: wakes the pass loop early. Balance-only
    cycles return false. *)
let history_changed (prev : materialized option) (cur : materialized) =
  match prev with
  | None -> true
  | Some prev ->
    (not (same_fng prev.m_fng cur.m_fng))
    || List.length prev.m_assets <> List.length cur.m_assets
    || List.exists
         (fun (a : asset_material) ->
            match
              List.find_opt
                (fun (b : asset_material) ->
                   b.am_exchange = a.am_exchange && b.am_symbol = a.am_symbol)
                prev.m_assets
            with
            | None -> true
            | Some b ->
              (not (same_bars a.am_bars b.am_bars))
              || a.am_deep_bars <> b.am_deep_bars
              || (not (same_members a.am_members b.am_members))
              || a.am_calendar_fp <> b.am_calendar_fp)
         cur.m_assets
;;

(** Completed REAL pass attempts: incremented in the runtime loop right
    before [on_publish] wakes the domains, so by the time a domain wakes on
    the publish signal this counter already reflects the attempt that just
    finished. Trading domains open their startup gate once the FIRST attempt
    is done and no decision exists for their asset (analysis failed, or the
    runtime could not complete a pass at all): last-known-good is empty at
    fresh startup, so waiting longer only delays the config/F&G fallback the
    engine intends.

    A pass only counts as an attempt when it actually reached the decision
    phase - i.e. a materialized state existed to decide on. The cold-start
    pass ("first background refresh still in progress") publishes nothing:
    counting it would open the domains' F&G-only fallback gate while the
    oracle's first real decisions are still seconds away, letting capital-
    unaware F&G sizing place orders the oracle's first pass immediately
    declares INACTIVE (rejected by the exchanges for insufficient funds). *)
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

let shutdown_requested = Atomic.make false

(** Event-driven trigger: the engine calls [request_pass] (a single Atomic
    increment plus a broadcast of [wake_condition] - lock-free, no syscall)
    whenever a fill or a canceled/rejected/expired order could change an
    asset's pool or sizing, and the loops wake immediately on the condition
    instead of sleeping out the cadence or polling in slices. The generation
    counter makes each wake one-shot: the wait captures the value before
    sleeping, so only a NEW [request_pass] (another increment + broadcast)
    wakes the next wait. *)
let pass_requested : int Atomic.t = Atomic.make 0

(** Cross-domain wake channel. [request_pass] is called from OCaml domain
    workers (each trading domain runs on its own OCaml 5 domain), so the wait
    must be a real wakeup, not a polled flag. Broadcasting an [Lwt_condition]
    from a foreign domain is safe - the same pattern [fill_event_bus] already
    relies on - and the register-then-recheck idiom in [wait_until] closes the
    lost-wakeup race. *)
let wake_condition : unit Lwt_condition.t = Lwt_condition.create ()

let request_pass () =
  Atomic.incr pass_requested;
  Lwt_condition.broadcast wake_condition ()
;;

(** Whether a [request_pass] arrived after the captured generation. *)
let pass_requested_changed generation = Atomic.get pass_requested <> generation

(** Whether the refresher published a cycle that changed analysis-relevant
    inputs ([refresh_history_changed]): an f&g move, new bars, a member or
    calendar change. Balance-only cycles never wake the pass. *)
let refresh_inputs_changed refresh_gen =
  Atomic.get refresh_generation <> refresh_gen && Atomic.get refresh_history_changed
;;

(** Sleep until [deadline], waking immediately on a new [request_pass] or a
    refresher cycle that changed analysis inputs. The only timer is the
    cadence deadline; events wake the wait directly via [wake_condition]
    (microsecond-class), not by polling in 50ms slices. Register-then-recheck
    closes the lost-wakeup race: an event that lands between the predicate
    check and the wait registration is caught by the self-broadcast, since
    the waiter is already registered by then. *)
let rec wait_until ~(deadline : float) ~(generation : int) ~(refresh_gen : int) () =
  if pass_requested_changed generation || refresh_inputs_changed refresh_gen
  then Lwt.return_unit
  else (
    let now = Unix.gettimeofday () in
    if now >= deadline
    then Lwt.return_unit
    else (
      let w = Lwt_condition.wait wake_condition in
      if pass_requested_changed generation || refresh_inputs_changed refresh_gen
      then Lwt_condition.broadcast wake_condition ();
      Lwt.pick
        [ (w >|= fun () -> ())
        ; (Lwt_unix.sleep (max 0.0 (deadline -. now)) >|= fun () -> ())
        ]
      >>= fun () -> wait_until ~deadline ~generation ~refresh_gen ()))
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

(** Fetch one symbol's daily series through the shared registry-driven
    pipeline (Oracle_fetch: venue adapter dispatch, disk cache, clean-series
    normalization). Thin wrapper keeping the runtime's local signature. *)
let fetch_series_for
      (tc : Dio_strategies.Strategy_common.trading_config)
      (symbol : string)
  : Oracle_types.series Lwt.t
  =
  Oracle_fetch.fetch_series_for ~exchange:tc.exchange ~symbol ?feed:tc.data_feed ()
;;

(** Extend a venue series backward with the Yahoo deep history for the same
    underlying asset (venue bars win on overlap; nothing is synthesized).
    Delegates to the shared pipeline (Oracle_fetch). Returns the deepened
    series and the number of deep bars added. *)
let deepen_series (rc : runtime_config) (series : Oracle_types.series)
  : (Oracle_types.series * int) Lwt.t
  =
  Oracle_fetch.deepen_series ~no_deep_history:rc.no_deep_history series
;;

(** Top-of-book anchor for the grid's order type: the live websocket-fed bid
    for the buy ladder the sizing seeds. The grid's first order is a resting
    BUY, and the live strategy prices buys off the bid (compute_buy_ref_price
    is bid-first and buy targets are capped at the bid), so the replay anchor
    uses the bid - never a mid - with the ask backing a missing bid and the
    history close as the last resort. A live price that diverges from the
    history close by more than the sanity band (stale feed, wrong symbol)
    falls back to the close. *)
let live_buy_anchor ~(exchange : string) ~(symbol : string) ~(fallback : float) =
  let band = 0.30 in
  let sane price =
    fallback > 0.0 && price > 0.0 && Float.abs (price -. fallback) /. fallback <= band
  in
  match Exchange.Registry.get exchange with
  | None -> fallback
  | Some (module Ex) ->
    (match Ex.get_top_of_book ~symbol with
     | Some (bid, _, _, _) when sane bid -> bid
     | Some (_, _, ask, _) when sane ask -> ask
     | _ -> fallback)
;;

(** Load the class member pool through the shared pipeline (Oracle_fetch:
    members gather their history purely from Yahoo unless they ARE the
    active asset on this exchange). Thin wrapper keeping the runtime's local
    signature; [class_member_source] itself now lives in Oracle_fetch. *)
let load_members
      (rc : runtime_config)
      (classes : (string * class_pool) list)
      (tc : Dio_strategies.Strategy_common.trading_config)
      ~(class_name : string)
      (asset : Oracle_types.series)
  : Oracle_types.series list Lwt.t
  =
  let pool_members =
    match List.assoc_opt class_name classes with
    | Some pool when pool.members <> [] -> pool.members
    | _ -> []
  in
  Oracle_fetch.load_members
    ~no_deep_history:rc.no_deep_history
    ~offline:false
    ~exchange:tc.exchange
    pool_members
    ~class_name
    asset
    ~fetch_series:(fun symbol ->
      Oracle_fetch.fetch_series_for ~exchange:tc.exchange ~symbol ?feed:tc.data_feed ())
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

(** Account key of a trading asset (venue/quote@testnet), matching
    [account_of_task] so an event from a trading domain lands on the same
    account the runtime sizes. *)
let account_key_of ~(exchange : string) ~(symbol : string) ~(testnet : bool) =
  account_id (Oracle_topology.key ~venue:exchange ~symbol ~testnet ())
;;

(* ------------------------------------------------------------------ *)
(* Pool-delta event channel (network-independent decision path).      *)
(*                                                                     *)
(* A fill/cancel on a trading domain changes the venue pool. The       *)
(* domain notifies the runtime with the quote delta and the pass       *)
(* applies it to the account pool IN PROCESS - the decision path never *)
(* waits on a network balance refresh. The background refresher        *)
(* reconciles the authoritative venue value afterwards and its         *)
(* published record supersedes the deltas (see [resolve_account_pool]).*)
(* ------------------------------------------------------------------ *)

(** One pool-delta event enqueued by a trading domain: the account whose pool
    moved and the quote delta (a buy fill consumes value+fee, a sell fill
    returns value-fee, a canceled buy returns its remaining committed value).
    [baseline] is the refresh generation the delta is applied against; the
    pass applies a delta only when its baseline equals the materialized
    record's epoch, so a fresher authoritative pool (fetched after the event)
    that already includes it is never double-counted. *)
type pool_event =
  { account : string
  ; delta : float
  ; baseline : int
  ; cause : string
  }

(** Cross-domain writers (one per trading domain), single scheduler-thread
    consumer (the pass loop) - so the queue needs only an OCaml mutex, not an
    Lwt mutex. *)
let event_mutex = Mutex.create ()

(** Bounded queue cap: under a pathological fill flood faster than the
    scheduler can drain, older deltas are dropped rather than growing
    unboundedly - the next authoritative refresh reconciles them. *)
let event_queue_cap = 16_384

let event_queue : pool_event Queue.t = Queue.create ()

(** Scheduler-thread-owned pending pool deltas, keyed by account_id: the
    events drained since the last pass, applied to the materialized pool by
    [resolve_account_pool]. Cleared at the start of each pass (deltas the
    previous pass consumed have a stale baseline and are filtered anyway);
    never touched by domain threads. *)
let pending_pool_adjustments : (string, (float * int) list) Hashtbl.t = Hashtbl.create 8

let enqueue_pool_event account delta cause =
  Mutex.lock event_mutex;
  if Queue.length event_queue >= event_queue_cap
  then
    Logging.debug_f
      ~section
      "pool-event queue full (%d); dropping delta for %s (%s) - the next balance refresh \
       reconciles"
      event_queue_cap
      account
      cause
  else
    Queue.push
      { account; delta; baseline = Atomic.get refresh_generation; cause }
      event_queue;
  Mutex.unlock event_mutex
;;

(** Drain the queued trading events into [pending_pool_adjustments]. Called
    once per pass on the scheduler thread. *)
let drain_pool_events () =
  Hashtbl.clear pending_pool_adjustments;
  let events = ref [] in
  Mutex.lock event_mutex;
  (try
     while true do
       events := Queue.pop event_queue :: !events
     done
   with
   | Queue.Empty -> ());
  Mutex.unlock event_mutex;
  List.iter
    (fun (e : pool_event) ->
       let cur =
         Option.value (Hashtbl.find_opt pending_pool_adjustments e.account) ~default:[]
       in
       Hashtbl.replace pending_pool_adjustments e.account ((e.delta, e.baseline) :: cur))
    !events
;;

(** A fill changed the venue pool: enqueue the quote delta (a buy consumes
    value+fee, a sell returns value-fee) and wake the runtime. The decision
    path applies it to the materialized pool immediately - never waiting on a
    network balance refresh; the background refresher reconciles the
    authoritative value afterwards. [fee] is the estimated absolute fee in
    quote terms. *)
let notify_fill
      ~(exchange : string)
      ~(symbol : string)
      ~(testnet : bool)
      ~(side : Exchange.Types.order_side)
      ~(filled_qty : float)
      ~(avg_price : float)
      ~(fee : float)
  =
  let value = filled_qty *. avg_price in
  let delta =
    match side with
    | Exchange.Types.Buy -> -.(value +. Float.max 0.0 fee)
    | Exchange.Types.Sell -> value -. Float.max 0.0 fee
  in
  enqueue_pool_event (account_key_of ~exchange ~symbol ~testnet) delta "fill";
  request_pass ()
;;

(** A canceled/rejected/expired order released committed capital: enqueue the
    returned quote (a canceled BUY returns its remaining committed value -
    remaining_qty x limit price) and wake the runtime, so the account's pool
    recovers at event time instead of at the next balance refresh. A canceled
    sell releases base inventory, not quote, so it never touches the pool. *)
let notify_order_cancel
      ~(exchange : string)
      ~(symbol : string)
      ~(testnet : bool)
      ~(side : Exchange.Types.order_side)
      ~(value : float)
  =
  if side = Exchange.Types.Buy
  then
    enqueue_pool_event
      (account_key_of ~exchange ~symbol ~testnet)
      (Float.max 0.0 value)
      "order_cancel";
  request_pass ()
;;

(** Decision-time account pool: the in-process single source of truth when the
    venue has a live balance store (the websocket-fed store is updated by the
    venue's own fill/balance feed - no network, no waiting), otherwise the
    materialized REST pool plus the local pool deltas from trading events
    ([pending_pool_adjustments]). Deltas are applied only when their baseline
    refresh generation matches the materialized record being consumed - a
    fresher authoritative pool (fetched after the event) already includes the
    event and is never double-counted. The decision path never blocks on a
    network balance fetch. *)
let resolve_account_pool ~(m : materialized) (account : Oracle_topology.instrument_key)
  : (float * Oracle_balances.snapshot) option
  =
  match
    Oracle_balances.snapshot_of_live_store
      ~exchange:account.venue
      ~testnet:account.testnet
      ()
  with
  | Some snap ->
    let pool =
      Float.max 0.0 (Oracle_balances.available_quote snap ~quote:account.quote)
    in
    Some (pool, snap)
  | None ->
    (match Hashtbl.find_opt m.m_balances (account_id account) with
     | None | Some None -> None
     | Some (Some (base, snap)) ->
       let pending =
         match Hashtbl.find_opt pending_pool_adjustments (account_id account) with
         | None -> 0.0
         | Some deltas ->
           List.fold_left
             (fun acc (d, baseline) -> if baseline = m.m_epoch then acc +. d else acc)
             0.0
             deltas
       in
       Some (Float.max 0.0 (base +. pending), snap))
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

(** Venue-locked pool per account: the account's live available quote balance
    together with the balance snapshot (the sizing seeds its replay grid from
    the account's held base). [None] means the balance could not be fetched
    this pass - the caller skips the whole account rather than publish a
    decision based on a wrong pool. *)
let venue_pools ~(prev : (string * float) list) (tasks : Oracle_tasks.task list)
  : (Oracle_topology.instrument_key * (float * Oracle_balances.snapshot) option) list
      Lwt.t
  =
  let accounts = List.map fst (group_by_account tasks) in
  let pool_for (account : Oracle_topology.instrument_key)
    : (float * Oracle_balances.snapshot) option Lwt.t
    =
    match
      List.find_opt
        (fun (task : Oracle_tasks.task) -> same_account account (account_of_task task))
        tasks
    with
    | None -> Lwt.return_none
    | Some task ->
      Lwt.catch
        (fun () ->
           (* Live-engine path first: the websocket-fed balance store (already
              in-process, no standalone HTTP round-trip) when it has data;
              REST one-shot fallback otherwise (CLI parity, and the
              authoritative path for Hyperliquid spot-only pools). *)
           Oracle_balances.fetch_task_live task
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
             let source =
               match snapshot.Oracle_balances.balances with
               | { Oracle_balances.wallet_type = "live"; _ } :: _ -> "live store"
               | _ -> "rest"
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
             (* The refresher polls balances at the fast cadence; the venue
                line is INFO when the pool moved meaningfully (>=1%) or is
                new, DEBUG otherwise - a quiet market does not flood the log
                ten times per pass. *)
             let loud =
               match List.assoc_opt (account_id account) prev with
               | None -> true
               | Some prev_pool ->
                 Float.abs (pool -. prev_pool) /. Float.max 1e-9 (Float.abs prev_pool)
                 >= 0.01
             in
             let msg =
               Printf.sprintf
                 "venue %s balance (%s) pool $%.2f: %s"
                 (account_id account)
                 source
                 pool
                 (String.concat " · " lines)
             in
             if loud then Logging.info ~section msg else Logging.debug ~section msg;
             Some (pool, snapshot))
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
    (worst-case) end of its range. Informational (logged per pass): the
    allocation itself is strict priority order, each asset sizing against the
    entire remaining pool. *)
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
  ; tc : Dio_strategies.Strategy_common.trading_config
    (** The fee-enriched trading config the grid was built from: sizing
        rebuilds the grid with the live anchor at decision time, so a
        memoized analysis never pins a stale price. *)
  ; id : int
    (** Monotone identity: memoized analyses keep their id, recomputed ones
        get a fresh id - account-level sizing memoization fingerprints on
        these. *)
  }

(* --- Analysis memoization -------------------------------------------- *)

(** Everything one asset's analysis depends on, in a fingerprint-able
    form. The cache returns the SAME [analysis] record for an equal input
    (physical identity of the arrays is the fast path), so downstream
    identity compares (account sizing memoization) see "unchanged". *)
type analysis_input =
  { ai_bars : Oracle_types.bar array
  ; ai_deep_bars : int
  ; ai_members : Oracle_types.series list
  ; ai_calendar : Oracle_sessions.model option
  ; ai_fng : float option
  ; ai_kappa : int
  ; ai_warmup : int
  ; ai_horizons : string
  ; ai_rc : string (** Resolved per-asset knobs, rendered deterministically. *)
  ; ai_tc : string (** Fee-enriched trading-config fields that reach the grid/sizing. *)
  ; ai_kind : string
  }

let analysis_cache : (string, analysis_input * analysis) Hashtbl.t = Hashtbl.create 16
let next_analysis_id = ref 0

let same_input (a : analysis_input) (b : analysis_input) =
  a == b
  || (same_bars a.ai_bars b.ai_bars
      && a.ai_deep_bars = b.ai_deep_bars
      && same_members a.ai_members b.ai_members
      && a.ai_calendar == b.ai_calendar
      && same_fng a.ai_fng b.ai_fng
      && a.ai_kappa = b.ai_kappa
      && a.ai_warmup = b.ai_warmup
      && a.ai_horizons = b.ai_horizons
      && a.ai_rc = b.ai_rc
      && a.ai_tc = b.ai_tc
      && a.ai_kind = b.ai_kind)
;;

(* --- Account sizing memoization -------------------------------------- *)

(** Account-level sizing fingerprint: sizing is sequential within an
    account (pass-down), so it is memoized per account, not per asset.
    The pool is bucketed (a <0.5% move cannot change the quantized qty/gi
    verdict), the strategy state (accumulated profit / reserved base /
    venue base) is exact so a fill of any size invalidates the cache, and
    the analysis ids cover history/fng/knob changes. *)
type account_fp =
  { af_analyses : string list
    (** Per-asset analysis ids in priority order ("none" = missing). *)
  ; af_pool : float
  ; af_fng : float option
  ; af_state : string
    (** Strategy-state tokens per asset (accumulated profit, reserved
        base, venue base). *)
  }

(** Cached per account: (fingerprint, decisions, first-buy gate token). The
    token is [account_gate_token] evaluated with the SIZING-TIME pool and the
    sizing-time first-buy costs; the reuse path re-evaluates it against the
    current pool and LIVE costs (the anchor price is deliberately not part of
    the fingerprint), so a gate that flipped in either dimension forces a
    fresh sizing instead of reusing a stale ACTIVE/INACTIVE verdict. *)
let size_cache : (string, account_fp * decision list * bool list) Hashtbl.t =
  Hashtbl.create 8
;;

let account_fp_eq (a : account_fp) (b : account_fp) =
  a.af_analyses = b.af_analyses
  && a.af_state = b.af_state
  && same_fng a.af_fng b.af_fng
  && (a.af_pool = b.af_pool
      || Float.abs (a.af_pool -. b.af_pool) /. Float.max 1e-9 (Float.abs b.af_pool)
         < 0.005)
;;

(** Analyze one asset against its MATERIALIZED history (no network: the
    refresher fetched and merged the venue series, the deep history, the
    class members and the calendar; fees were enriched there too).
    Memoized: when every analysis input is unchanged (physical compare of
    the bar arrays - the refresher reuses array objects while nothing
    changed - plus members, calendar, f&g, knobs), the SAME [analysis]
    record is returned in microseconds and the heavy replay/blend is not
    re-run. Returns ([analysis], [true]) on a memoization hit.

    The live top-of-book anchor is deliberately NOT part of the
    fingerprint: sizing rebuilds the grid from the fresh anchor at decision
    time, so a memoized analysis never pins a stale price. *)
let analyze_asset
      (rc : runtime_config)
      (classes : (string * class_pool) list)
      (task : Oracle_tasks.task)
      ~(index : int)
      ~(n_tasks : int)
      ~(am : asset_material)
      ~(fng : float option)
  : (analysis * bool) Lwt.t
  =
  let exchange = task.Oracle_tasks.exchange in
  let calendar_kind = Oracle_tasks.calendar_kind_of_exchange exchange in
  let tc = am.am_tc in
  let asset =
    { Oracle_types.symbol = task.Oracle_tasks.symbol
    ; calendar_kind
    ; bars = am.am_bars
    ; gaps = []
    }
  in
  let members = am.am_members in
  let equity_sessions = am.am_calendar in
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
  (* The asset's own analysis runs on its resolved knobs (global + overrides);
     class-member series stay on the global config - they are shared pool
     inputs feeding the kappa blend, not per-asset decisions. *)
  let rc = resolve_for rc ~exchange task.Oracle_tasks.symbol in
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
  let rc_key =
    Printf.sprintf
      "ts%.4g|fngw%.4g|rw%.4g|mad%.4g|qcm%.4g|ndh%b|wbs%b|h[%s]"
      rc.target_survival
      rc.fng_weight
      rc.range_weight
      rc.min_active_dsurv
      rc.qty_cap_mult
      rc.no_deep_history
      rc.weight_by_sessions
      (match rc.horizons with
       | Some ns -> String.concat "," (List.map string_of_int ns)
       | None -> "")
  in
  let tc_key =
    Printf.sprintf
      "%s/%s|gi%.4g-%.4g|qty%s|sm%s|mf%.6g|tf%.6g|ab%.6g|df%s|cls%s"
      tc.exchange
      tc.symbol
      (fst tc.grid_interval)
      (snd tc.grid_interval)
      tc.qty
      tc.sell_mult
      (Option.value tc.maker_fee ~default:0.001)
      (Option.value tc.taker_fee ~default:0.001)
      (fst tc.accumulation_buffer)
      (Option.value tc.data_feed ~default:"")
      (Option.value tc.asset_class ~default:"")
  in
  let ai =
    { ai_bars = am.am_bars
    ; ai_deep_bars = am.am_deep_bars
    ; ai_members = members
    ; ai_calendar = equity_sessions
    ; ai_fng = fng
    ; ai_kappa = kappa
    ; ai_warmup = warmup
    ; ai_horizons =
        String.concat "," (List.map (fun (h : Oracle_types.horizon) -> h.label) horizons)
    ; ai_rc = rc_key
    ; ai_tc = tc_key
    ; ai_kind =
        (match calendar_kind with
         | Oracle_types.Crypto -> "crypto"
         | Equity -> "equity")
    }
  in
  let key = exchange ^ "/" ^ task.Oracle_tasks.symbol in
  match Hashtbl.find_opt analysis_cache key with
  | Some (prev, analysis) when same_input prev ai ->
    Logging.debug_f
      ~section
      "[%d/%d] %s/%s: analysis reused (inputs unchanged: %d bars, %d member(s))"
      index
      n_tasks
      exchange
      task.Oracle_tasks.symbol
      n_bars
      (List.length members);
    Lwt.return (analysis, true)
  | _ ->
    (* The grid ladder for the reservation is anchored at the live
       top-of-book bid (the websocket orderbook feed, not a standalone HTTP
       call) when the engine holds one, with the history close as fallback.
       The mid is never used: the sizing models the buy leg at the price a
       buy actually references. The reservation is informational; the
       decision-time sizing rebuilds this grid from the fresh anchor. *)
    let last_close =
      if n_bars = 0 then 0.0 else asset.bars.(n_bars - 1).Oracle_types.close
    in
    let start_price =
      live_buy_anchor ~exchange ~symbol:task.Oracle_tasks.symbol ~fallback:last_close
    in
    let live_anchor =
      start_price > 0.0
      && last_close > 0.0
      && Float.abs (start_price -. last_close) /. last_close > 1e-6
    in
    Logging.debug_f
      ~section
      "[%d/%d] %s/%s start price %.6g (%s, history close %.6g)"
      index
      n_tasks
      exchange
      task.Oracle_tasks.symbol
      start_price
      (if live_anchor
       then "live top-of-book bid"
       else if start_price > 0.0
       then "history close"
       else "unavailable")
      last_close;
    let gi_lo, gi_hi = tc.grid_interval in
    let grid =
      Grid_adapter.of_trading_config
        tc
        ~start_price
        ~start_quote:0.0
        ~grid_interval_pct:gi_hi
    in
    (* Analysis inputs of this pass, so the history/member/horizon basis each
       decision was computed on is traceable in the engine log (debug: it
       repeats only when the inputs actually change). *)
    Logging.debug_f
      ~section
      "[%d/%d] %s/%s: history %d bars (+%d deep), %d class member(s) [%s], warmup %d, \
       horizons [%s]"
      index
      n_tasks
      exchange
      task.Oracle_tasks.symbol
      n_bars
      am.am_deep_bars
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
        { q_min
        ; d_gov
        ; d_cover
        ; governing_horizon
        ; fallback
        ; n_fills
        ; min_cost
        ; first_buy
        }
    in
    (* The reservation (allocation machinery detail) - debug: it repeats only
       on a recompute. *)
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
    let analysis =
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
      ; tc
      ; id =
          (incr next_analysis_id;
           !next_analysis_id)
      }
    in
    Hashtbl.replace analysis_cache key (ai, analysis);
    Lwt.return (analysis, false)
;;

(** Size one analyzed asset against a budget: the entire remaining venue
    pool, handed down in strict priority order (the priority asset is sized
    fully before the next asset draws anything). Runs the deployment engine
    (survival-driven over the full gi/qty ranges - see Oracle_deploy) and
    logs the decision. [index]/[n_tasks] are only for logging;
    [venue_pool] is the account's total capital (for the log context). *)

(** Capital locked by one asset's resting buy orders: the sum of
    remaining_qty x limit price over its open buys. This is what a
    cancellation returns to the account's available pool - the basis of the
    priority-reclamation plan. Same snapshot-based fold (M12/H6) as
    [has_committed_buy], so the store mutex is held only for the snapshot
    walk. *)
let committed_buy_value ~(exchange : string) ~(symbol : string) : float =
  match Exchange.Registry.get exchange with
  | None -> 0.0
  | Some (module Ex) ->
    Ex.fold_open_orders ~symbol ~init:0.0 ~f:(fun acc (o : Exchange.Types.open_order) ->
      if o.side = Exchange.Types.Buy && o.remaining_qty > 0.0
      then
        acc
        +. (o.remaining_qty
            *.
            match o.limit_price with
            | Some p -> p
            | None -> 0.0)
      else acc)
;;

let size_asset
      (rc : runtime_config)
      (analysis : analysis)
      ~(pool : float)
      ~(venue_pool : float)
      ~(snapshot : Oracle_balances.snapshot option)
      ~(fng : float option)
      ~(index : int)
      ~(n_tasks : int)
  : decision Lwt.t
  =
  let { exchange; symbol; asset; calendar_kind; lo; hi; models; tc; _ } = analysis in
  (* The ladder is anchored at the LIVE top-of-book bid at decision time -
     never a price pinned by a memoized analysis. The grid's first order is
     a resting buy and buys price off the bid (compute_buy_ref_price is
     bid-first), so the sizing models the buy leg at the price a buy
     actually references; the mid is never used. *)
  let last_close =
    if Array.length asset.bars = 0
    then 0.0
    else asset.bars.(Array.length asset.bars - 1).Oracle_types.close
  in
  let start_price = live_buy_anchor ~exchange ~symbol ~fallback:last_close in
  let grid =
    Grid_adapter.of_trading_config tc ~start_price ~start_quote:0.0 ~grid_interval_pct:hi
  in
  (* The sizing replay starts from the account's ACTUAL grid state: the base
     it holds (available on the venue), the base locked in resting sells and
     the accumulated profit buffer (persisted + live - the strategy state
     loads accumulated_state.json on first access) - so the survival verdict
     answers "can THIS grid, as it runs, survive?" instead of "can a
     hypothetical fresh grid?". The accumulation-sell gate (Hyperliquid)
     needs the profit buffer; without the seed the replay can never sell and
     systematically understates D_surv. *)
  let seed =
    match snapshot with
    | None -> None
    | Some snapshot ->
      (* Concurrency note: st.reserved_base / st.accumulated_profit are read
         without the strategy mutex. Safe by the same argument as the
         dashboard encoder (see dashboard_state.ml): word-sized immutable
         boxed floats swapped wholesale, reader sees old-or-new, and the
         oracle pass tolerates a snapshot a moment stale (the next pass
         re-derives from fresh balances). Only the symbol's domain thread
         writes these fields since the lifecycle event queues landed. *)
      let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
      let base_asset =
        match String.split_on_char '/' symbol with
        | b :: _ -> b
        | [] -> symbol
      in
      let base = Oracle_balances.available_asset snapshot ~asset:base_asset in
      Some
        { Dio_strategies.Grid_core_types.initial_base = base
        ; initial_reserved_base = st.reserved_base
        ; initial_accumulated_profit = st.accumulated_profit
        }
  in
  (* A committed resting buy (already funded and resting on the exchange -
     its cost is locked in the account balance, which is why the available
     pool reads low) keeps the grid alive: the asset is never "cannot fund
     the first buy" while it has one. The grid's own capital gates pause it
     when the pool cannot extend another rung. M12: uses the snapshot-based
     fold (H6) instead of get_open_orders' list build, so the store mutex is
     held only for the snapshot walk, never across the callback. *)
  let has_committed_buy =
    match Exchange.Registry.get exchange with
    | None -> false
    | Some (module Ex) ->
      Ex.fold_open_orders
        ~symbol
        ~init:false
        ~f:(fun acc (o : Exchange.Types.open_order) ->
          acc || (o.side = Exchange.Types.Buy && o.remaining_qty > 0.0))
  in
  let deployment =
    D.deploy_asset
      ~seed
      ~has_committed_buy
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
  (* The per-asset report: header + detail as ONE atomic message so the
     decision lines never interleave with other domains' logs. Logged at INFO
     when the report changed since the last pass, DEBUG otherwise (a still
     market stays quiet between the per-pass latency summaries). *)
  let header =
    if deployment.Oracle_types.active
    then
      Printf.sprintf
        "[%d/%d] %s/%s ACTIVE buy %.6g %s @ %.2f%% | cap $%.2f/$%.2f | %s | surv %.1f%% \
         | %s"
        index
        n_tasks
        exchange
        symbol
        deployment.Oracle_types.qty
        (base_of symbol)
        deployment.Oracle_types.parameter
        (* The capital this deployment actually consumes - the sizing no
           longer models the priority asset against the whole venue pool;
           what it does not consume passes down the priority order. *)
        deployment.Oracle_types.deployed
        venue_pool
        p2v_lbl
        (deployment.Oracle_types.d_surv *. 100.0)
        health
    else
      Printf.sprintf
        "[%d/%d] %s/%s INACTIVE — %s | $%.2f passes down"
        index
        n_tasks
        exchange
        symbol
        deployment.Oracle_types.reason
        deployment.Oracle_types.remainder
  in
  (* The detail block: event prices/dates, model horizon, and the sizing
     reasons behind the resolved gi/qty (survival-driven over the config
     ranges; the F&G/range weights are inert in the sizing). *)
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
         "worst drop %.1f%% (peak $%.2f on %s → valley $%.2f on %s) · model %.1f%% @ %s"
         (p.Oracle_types.max_drawdown *. 100.0)
         p.Oracle_types.peak
         p.Oracle_types.peak_date
         p.Oracle_types.valley
         p.Oracle_types.valley_date
         (deployment.Oracle_types.d_gov *. 100.0)
         horizon_lbl
     | None ->
       add
         "worst drop %.1f%% (no actual drawdown in the history) · model %.1f%% @ %s"
         (deployment.Oracle_types.d_cover *. 100.0)
         (deployment.Oracle_types.d_gov *. 100.0)
         horizon_lbl);
    (* The funding reference: where the funded drawdown comes from (the
       ATH-scaled remainder to the expected floor, or the measured floor
       overshoot when the remainder is exhausted / no recovered anchor). *)
    (match deployment.Oracle_types.sizing with
     | Some r when r.Oracle_types.outlier ->
       add
         "funding: deepest drawdown not recovered — floor overshoot %.1f%%%s"
         (deployment.Oracle_types.d_cover *. 100.0)
         (if Option.is_none r.Oracle_types.overshoot_p90
          then " (no floor-break history: 15% fallback)"
          else " (90th pct)")
     | Some r when r.Oracle_types.at_floor ->
       (match r.Oracle_types.floor_ref with
        | Some floor_ref ->
          add
            "funding: at/below floor $%.2f — floor overshoot %.1f%%%s"
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
            "funding: drop %.1f%% to floor $%.2f (ATH $%.2f − %.1f%% worst)"
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
    (* The sizing: how the gi and qty were chosen. The sizing is
       survival-driven over the full config ranges - the gi is the tightest
       value reaching 100% replay survival at the minimum order size (or the
       grid maximum in stretch mode when 100% is unreachable), and the qty is
       the minimum (stretch) or the largest value keeping 100% survival,
       capped at qty * qty_cap_mult. The reasons are carried by the
       deployment itself. *)
    add
      "sizing: gi %.4f%% (%s) · qty %.6g (%s)"
      deployment.Oracle_types.parameter
      deployment.Oracle_types.gi_reason
      deployment.Oracle_types.qty
      deployment.Oracle_types.qty_reason;
    (* The accumulated-state seed: what the sizing replay started from (held
       base, base locked in resting sells, the accumulated profit buffer) -
       the grid is modeled as it actually runs, not as a fresh grid. *)
    (match seed with
     | Some
         { Dio_strategies.Grid_core_types.initial_base = b
         ; initial_reserved_base = r
         ; initial_accumulated_profit = p
         }
       when b > 0.0 || r > 0.0 || p > 0.0 ->
       add "seeded: base %.6g, reserved %.6g, accumulated profit %.6g" b r p
     | _ -> ());
    if has_committed_buy
    then add "committed buy resting: the grid keeps running on committed capital");
  let detail_str = Buffer.contents detail in
  let report =
    if detail_str = "" then header else header ^ "\n" ^ String.trim detail_str
  in
  let report_changed =
    match Hashtbl.find_opt last_detail_lines key with
    | Some prev -> prev <> report
    | None -> true
  in
  Hashtbl.replace last_detail_lines key report;
  if report_changed then Logging.info ~section report else Logging.debug ~section report;
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
     ; reclaim_capital = false
     ; reclaim_target = ""
     ; range = deployment.Oracle_types.range
     ; p2v = deployment.Oracle_types.p2v
     ; parameter_components = deployment.Oracle_types.parameter_components
     ; gi_reason = deployment.Oracle_types.gi_reason
     ; qty_reason = deployment.Oracle_types.qty_reason
     ; warnings = deployment.Oracle_types.warnings
     ; updated_at = Unix.gettimeofday ()
     }
     : decision)
;;

(* ------------------------------------------------------------------ *)
(* Priority reclamation.                                               *)
(* ------------------------------------------------------------------ *)

(** Case-sensitive substring search (no dependency on the strategy module's
    helper). *)
let contains_substring s fragment =
  let sl = String.length s
  and fl = String.length fragment in
  let rec go i = i + fl <= sl && (String.sub s i fl = fragment || go (i + 1)) in
  go 0
;;

(** One asset's capital-gate inputs for an account, in account priority
    order. Built once per account per pass and shared by the first-buy-gate
    cache check (Oracle_reclaim) and the reclamation plan. *)
type gate_input =
  { g_exchange : string
  ; g_symbol : string
  ; g_first_buy : float
    (** Cost of the first buy at the minimum order size, from the SAME
        live-anchored grid the sizing rebuilds at decision time - the
        memoized analysis grid can pin a stale anchor price, which would
        mis-size the gate and the reclaim gap after a price move. *)
  ; g_committed : float
    (** Capital locked by this asset's resting buys (what a cancel returns
        to the pool). *)
  ; g_has_committed_buy : bool
    (** A committed resting buy exempts the asset from the first-buy gate
        (its first buy is already funded). *)
  ; g_pool : float
    (** The pass-down budget the asset was sized against (its decision's
        [pool_share]) - the pool remaining after higher-priority assets
        consumed their share. *)
  ; g_capital_blocked : bool
    (** INACTIVE for capital reasons only (first-buy gate failed) - a valid
        reclaim target. *)
  }

(** Build the capital-gate inputs for one account from its tasks, analyses
    and the account's decisions (the same decisions the reclaim plan patches
    and the gate check compares). Assets whose analysis is missing this pass
    are dropped - they are neither targets nor candidates. *)
let account_gate_inputs
      (account_tasks : Oracle_tasks.task list)
      (analyses : analysis list)
      ~(decisions : decision list)
  : gate_input list
  =
  let decision_of (exchange : string) (symbol : string) =
    List.find_opt
      (fun (d : decision) -> d.exchange = exchange && d.symbol = symbol)
      decisions
  in
  List.filter_map
    (fun (task : Oracle_tasks.task) ->
       let exchange = task.Oracle_tasks.exchange in
       let symbol = task.Oracle_tasks.symbol in
       match
         List.find_opt
           (fun (a : analysis) -> a.exchange = exchange && a.symbol = symbol)
           analyses
       with
       | None -> None
       | Some a ->
         let last_close =
           if Array.length a.asset.Oracle_types.bars = 0
           then 0.0
           else
             a.asset.Oracle_types.bars.(Array.length a.asset.Oracle_types.bars - 1)
               .Oracle_types.close
         in
         let start_price = live_buy_anchor ~exchange ~symbol ~fallback:last_close in
         let grid =
           Grid_adapter.of_trading_config
             a.tc
             ~start_price
             ~start_quote:0.0
             ~grid_interval_pct:a.hi
         in
         let q_min = D.sizing_floor ~cfg:grid in
         let first_buy = G.cost_at (G.set_parameter grid a.hi) ~qty:q_min ~n_fills:1 in
         let committed = committed_buy_value ~exchange ~symbol in
         let has_committed_buy =
           match Exchange.Registry.get exchange with
           | None -> false
           | Some (module Ex) ->
             Ex.fold_open_orders
               ~symbol
               ~init:false
               ~f:(fun acc (o : Exchange.Types.open_order) ->
                 acc || (o.side = Exchange.Types.Buy && o.remaining_qty > 0.0))
         in
         let d = decision_of exchange symbol in
         (* A valid reclaim target is INACTIVE for capital reasons ONLY: an
            asset that would stay inactive once funded (no usable history,
            D_surv below the active floor) must not have lower-priority
            ladders canceled for nothing. *)
         let capital_blocked =
           match d with
           | Some d ->
             (not d.active) && contains_substring d.reason "cannot fund the first buy"
           | None -> false
         in
         Some
           { g_exchange = exchange
           ; g_symbol = symbol
           ; g_first_buy = first_buy
           ; g_committed = committed
           ; g_has_committed_buy = has_committed_buy
           ; g_pool =
               (match d with
                | Some d -> d.pool_share
                | None -> 0.0)
           ; g_capital_blocked = capital_blocked
           })
    account_tasks
;;

(** The first-buy gate outcome for each of an account's assets under a pool:
    folds the assets in priority order, each consuming its sized [deployed]
    share (the pass-down), and reports whether each asset's first buy is
    fundable from its remaining budget. A committed resting buy exempts the
    asset. The result is compared against the token STORED at sizing time to
    detect any gate flip since then - in either dimension: a pool move inside
    the 0.5% fingerprint bucket (a small capital return crossing an asset's
    first-buy cost) or a first-buy COST move (the live anchor price fell so
    the parked asset is now fundable). A flip must bypass the cache or the
    asset keeps a stale ACTIVE/INACTIVE decision - the "capital returned but
    the strategy did not resume" stall. *)
let account_gate_token
      ~(pool : float)
      (gate_inputs : gate_input list)
      ~(deployed_of : string -> float)
  : bool list
  =
  let rec go budget inputs acc =
    match inputs with
    | [] -> List.rev acc
    | gi :: rest ->
      let fundable = gi.g_has_committed_buy || budget +. 1e-9 >= gi.g_first_buy in
      go (budget -. deployed_of gi.g_symbol) rest (fundable :: acc)
  in
  go pool gate_inputs []
;;

(** The per-account reclamation plan from the account's gate inputs: which
    lower-priority resting buys to cancel so a higher-priority, capital-
    blocked asset that cannot fund its first buy from its pass-down budget
    can resume (see Oracle_reclaim for the selection rules - fewest
    cancellations, then lowest priority). Computed FRESH every pass on the
    current decisions and applied on top of them, so a changed plan takes
    effect without busting the account sizing cache. Returns
    [(exchange, symbol, target)] reclaim entries, keyed by exchange+symbol so
    duplicate symbols across accounts can never cross-patch. *)
let reclaim_plan (gate_inputs : gate_input list) : (string * string * string) list =
  let inputs =
    List.map
      (fun (gi : gate_input) ->
         { Oracle_reclaim.symbol = gi.g_symbol
         ; first_buy_cost = gi.g_first_buy
         ; committed_value = gi.g_committed
         ; pool = gi.g_pool
         ; capital_blocked = gi.g_capital_blocked
         })
      gate_inputs
  in
  (* The pure planner enumerates exactly while the candidate set stays under
     20 and falls back to a greedy cover above it (never a silent no-op).
     Surface the fallback once per pass for an account that large so the
     degraded "fewest cancellations" guarantee is visible in the log. *)
  let n_candidates =
    List.length (List.filter (fun (gi : gate_input) -> gi.g_committed > 0.0) gate_inputs)
  in
  if n_candidates >= 20
  then
    Logging.debug_f
      ~section
      "reclamation for an account with %d committed candidates: exact \
       fewest-cancellation selection skipped (greedy cover used)"
      n_candidates;
  let exchange_of symbol =
    match List.find_opt (fun (gi : gate_input) -> gi.g_symbol = symbol) gate_inputs with
    | Some gi -> gi.g_exchange
    | None -> ""
  in
  List.map (fun (s, t) -> exchange_of s, s, t) (Oracle_reclaim.plan inputs)
;;

(** Patch an account's decisions with the reclamation plan: a reclaimed asset
    is published INACTIVE-with-reclaim so its domain cancels its resting buy
    and the capital returns to the pool for the higher-priority target.
    Matched on exchange+symbol so a plan for one account can never patch
    another account's decision for the same symbol. [remainder] is left
    untouched - both the committed-running and the inactive deployments
    already pass the whole pool down, so the sequential pass-down is
    unchanged. Idempotent and applied fresh each pass (on top of the cached,
    unpatched decisions). *)
let apply_reclaim ~(plan : (string * string * string) list) (ds : decision list)
  : decision list
  =
  let plan_of (d : decision) =
    List.find_opt (fun (e, s, _) -> e = d.exchange && s = d.symbol) plan
  in
  List.map
    (fun (d : decision) ->
       match plan_of d with
       | None -> d
       | Some (_, _, target) ->
         { d with
           active = false
         ; reason = Printf.sprintf "capital reallocated to %s (higher priority)" target
         ; reclaim_capital = true
         ; reclaim_target = target
         ; deployed = 0.0
         })
    ds
;;

(* ------------------------------------------------------------------ *)
(* Background refresher (all oracle network lives here).               *)
(* ------------------------------------------------------------------ *)

(** Resolve one asset's materialized history: fee enrichment (network
    once per asset per process), the venue series (disk-cache delta), the
    Yahoo deep extension, and the class member series. Runs entirely in
    the background refresher - the decision path never calls this. A
    failure (timeout, venue error) is retried at the poll cadence with
    exponential backoff; the asset keeps its last-known-good history in
    the materialized record meanwhile. *)
let refresh_asset
      (config : runtime_config)
      (classes : (string * class_pool) list)
      (task : Oracle_tasks.task)
  : asset_material option Lwt.t
  =
  let exchange = task.Oracle_tasks.exchange in
  let symbol = task.Oracle_tasks.symbol in
  let key = exchange ^ "/" ^ symbol in
  if Unix.gettimeofday () < Option.value (Hashtbl.find_opt retry_after key) ~default:0.0
  then Lwt.return None
  else
    Lwt.catch
      (fun () ->
         let tc = task.Oracle_tasks.config in
         Oracle_fees.enrich tc ~offline:false
         >>= fun tc ->
         fetch_series_for tc symbol
         >>= fun series ->
         deepen_series config series
         >>= fun (series, deep_bars) ->
         let class_name =
           match tc.asset_class with
           | Some name -> name
           | None -> "default"
         in
         load_members config classes tc ~class_name series
         >>= fun members ->
         (* Equity-only session calendar (per the registry's calendar_kind):
             refetched at most once per day; the model object is reused
             while the fetched dates are identical, so the analysis
             fingerprint stays stable. Crypto venues get (None, "none"). *)
         (let today = today_iso () in
          match Hashtbl.find_opt calendar_cache exchange with
          | Some (fetched, model, fp) when fetched = today -> Lwt.return (Some model, fp)
          | _ ->
            Oracle_fetch.fetch_calendar_model
              ~exchange
              ~start_date:"2010-01-01"
              ~end_date:today
              ()
            >|= (function
             | Some (model, fp) ->
               Hashtbl.replace calendar_cache exchange (today, model, fp);
               Some model, fp
             | None -> None, "none"))
         >>= fun (calendar, calendar_fp) ->
         Hashtbl.remove retry_after key;
         Hashtbl.remove retry_backoff key;
         Lwt.return
           (Some
              { am_exchange = exchange
              ; am_symbol = symbol
              ; am_tc = tc
              ; am_bars = series.Oracle_types.bars
              ; am_deep_bars = deep_bars
              ; am_members = members
              ; am_calendar = calendar
              ; am_calendar_fp = calendar_fp
              }))
      (fun exn ->
         Logging.warn_f
           ~section
           "background history refresh failed for %s/%s (%s); keeping last-known-good \
            history, retrying with backoff"
           exchange
           symbol
           (Printexc.to_string exn);
         let backoff =
           let prev = Option.value (Hashtbl.find_opt retry_backoff key) ~default:0.0 in
           let next = if prev = 0.0 then 30.0 else Float.min 300.0 (prev *. 2.0) in
           Hashtbl.replace retry_backoff key next;
           next
         in
         Hashtbl.replace retry_after key (Unix.gettimeofday () +. backoff);
         Lwt.return None)
;;

(** One refresh cycle: fresh balances (all accounts, in parallel), full
    history refresh when due (refresh cadence; cold start always), and the
    fear & greed index - published as a new materialized record. Balance
    fetches run every cycle (poll cadence, and on fill events); history
    fetches run at the refresh cadence and keep the previous cycle's
    arrays otherwise (stable analysis fingerprints). *)
let refresh_cycle
      ~(config : runtime_config)
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * class_pool) list)
  : unit Lwt.t
  =
  let prev = Atomic.get materialized_ref in
  let now = Unix.gettimeofday () in
  let history_due =
    match prev with
    | None -> true
    | Some p -> now >= p.m_last_history_at +. config.refresh_seconds
  in
  let tasks, _ =
    Oracle_tasks.resolve_tasks
      ~symbol:""
      ~exchange:"kraken"
      ~exchange_explicit:false
      ~trading
      ~offline:false
  in
  Oracle_fetch.clear_cache ();
  Oracle_balances.clear_cache ();
  (* Balances: the refresher's per-cycle network, parallel per account. *)
  let prev_pools =
    match prev with
    | None -> []
    | Some p ->
      Hashtbl.fold
        (fun key v acc ->
           match v with
           | Some (pool, _) -> (key, pool) :: acc
           | None -> acc)
        p.m_balances
        []
  in
  let balance_start = Mtime_clock.now_ns () in
  venue_pools ~prev:prev_pools tasks
  >>= fun pools ->
  let balance_ms =
    Int64.to_float (Int64.sub (Mtime_clock.now_ns ()) balance_start) /. 1e6
  in
  Latency_profiler.record
    engine_profs.prof_balance
    (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) balance_start));
  (* Histories (on the refresh cadence; cold start always). *)
  let t_fetch_all = Mtime_clock.now_ns () in
  (if history_due
   then
     Lwt_list.map_p (fun task -> refresh_asset config classes task) tasks
     >|= List.filter_map Fun.id
   else (
     let assets =
       match prev with
       | None -> []
       | Some p -> p.m_assets
     in
     Lwt.return assets))
  >>= fun assets ->
  let history_ms =
    Int64.to_float (Int64.sub (Mtime_clock.now_ns ()) t_fetch_all) /. 1e6
  in
  Latency_profiler.record
    engine_profs.prof_fetch
    (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) t_fetch_all));
  let balances = Hashtbl.create 8 in
  List.iter
    (fun (account, pool) -> Hashtbl.replace balances (account_id account) pool)
    pools;
  let m =
    { m_assets = assets
    ; m_balances = balances
    ; m_fng = resolve_fng ()
    ; m_epoch = Atomic.get refresh_generation + 1
    ; m_last_history_at =
        (if history_due
         then now
         else (
           match prev with
           | Some p -> p.m_last_history_at
           | None -> now))
    }
  in
  let changed = history_changed prev m in
  Atomic.set materialized_ref (Some m);
  Atomic.incr refresh_generation;
  if changed then Atomic.set refresh_history_changed true;
  (* Wake the pass loop: it re-checks [refresh_inputs_changed] and only runs a
     pass when a cycle changed analysis-relevant inputs (f&g move, new bars,
     member/calendar change); balance-only cycles fall back to sleep. *)
  Lwt_condition.broadcast wake_condition ();
  (* Publish the balance/fetch latency windows for the dashboard (the pass
     publishes its own windows; the two never reset each other's
     profilers). *)
  let _ = Latency_profiler.snapshot_and_reset engine_profs.prof_balance in
  let _ = Latency_profiler.snapshot_and_reset engine_profs.prof_fetch in
  if history_due
  then
    Logging.info_f
      ~section
      "refresh cycle complete: %d asset(s) history refreshed in %.1fs · balances %.1fms \
       · epoch %d"
      (List.length assets)
      (history_ms /. 1000.0)
      balance_ms
      m.m_epoch
  else
    Logging.debug_f
      ~section
      "refresh cycle complete: %d asset(s), history kept · balances %.1fms · epoch %d"
      (List.length assets)
      balance_ms
      m.m_epoch;
  Lwt.return_unit
;;

(** Sleep until [deadline], waking immediately when a [request_pass] arrives
    (a fill or order event: balances should refresh promptly so the next pass
    sizes against fresh pools). Same event-driven wait as [wait_until] - the
    only timer is the cadence deadline. *)
let rec wait_until_refresh ~(deadline : float) ~(generation : int) () =
  if pass_requested_changed generation
  then Lwt.return_unit
  else (
    let now = Unix.gettimeofday () in
    if now >= deadline
    then Lwt.return_unit
    else (
      let w = Lwt_condition.wait wake_condition in
      if pass_requested_changed generation then Lwt_condition.broadcast wake_condition ();
      Lwt.pick
        [ (w >|= fun () -> ())
        ; (Lwt_unix.sleep (max 0.0 (deadline -. now)) >|= fun () -> ())
        ]
      >>= fun () -> wait_until_refresh ~deadline ~generation ()))
;;

(** The background refresher loop: cycles at the [poll_seconds] balance-
    reconciliation cadence (waking immediately on fill/order events),
    publishing the materialized state each cycle. Runs as its own fiber
    alongside the pass loop; both stop on shutdown. *)
let rec refresh_loop
          ~(config : runtime_config)
          ~(trading : Dio_strategies.Strategy_common.trading_config list)
          ~(classes : (string * class_pool) list)
          ()
  : unit Lwt.t
  =
  if Atomic.get shutdown_requested
  then Lwt.return_unit
  else
    Lwt.catch
      (fun () -> refresh_cycle ~config ~trading ~classes)
      (fun exn ->
         Logging.error_f
           ~section
           "background refresh cycle failed (%s); keeping last-known-good materialized \
            state"
           (Printexc.to_string exn);
         Lwt.return_unit)
    >>= fun () ->
    let generation = Atomic.get pass_requested in
    let deadline = Unix.gettimeofday () +. jittered config.poll_seconds in
    wait_until_refresh ~deadline ~generation ()
    >>= fun () -> refresh_loop ~config ~trading ~classes ()
;;

(** Run one decision pass over the materialized oracle state and publish
    the decisions. NO network: balances, histories, members and the
    calendar were materialized by the background refresher; analyses are
    memoized on their inputs (microseconds when unchanged) and account
    sizing is memoized on (analysis ids, pool bucket, strategy state).
    Tolerates every failure mode; a failed asset or account keeps its
    last-known-good decision.

    Returns [true] when the pass actually reached the decision phase (a
    materialized state existed to decide on), [false] when it could not (no
    runnable assets, or the cold-start refresher has not published the first
    materialized state yet). The runtime loop counts only [true] passes as
    "attempts" for the domains' startup gate: a pass that published nothing
    must not race the F&G-only fallback ahead of the oracle's first real
    decisions. *)
let run_pass
      ?(config = default_config ())
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * class_pool) list)
      ()
  : bool Lwt.t
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
    Lwt.return false)
  else (
    (* Engine latency window for this pass: the decision path only - the
       refresher records and publishes its own balance/fetch windows. *)
    let pass_start = Mtime_clock.now_ns () in
    let touched_assets = ref [] in
    let span_from t = Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) t) in
    let materialized = Atomic.get materialized_ref in
    match materialized with
    | None ->
      (* Cold start: the refresher is still fetching the first histories in
         the background; nothing to decide on yet. Domains keep their
         startup gate (they wait for the first decision) exactly as they do
         while the first pass fetched inline - and this pass does NOT count
         as an attempt, so the F&G-only fallback cannot race the first real
         decisions. *)
      Logging.info
        ~section
        "capital-oracle pass: first background refresh still in progress; no decisions \
         yet";
      Latency_profiler.record engine_profs.prof_pass (span_from pass_start);
      let _ = Latency_profiler.snapshot_and_reset engine_profs.prof_pass in
      Lwt.return false
    | Some m ->
      (* Drain the trading-domain pool events enqueued since the last pass and
         apply them to the materialized pools at account resolution time
         ([resolve_account_pool]) - the decision path never waits on a network
         balance refresh. *)
      drain_pool_events ();
      let fng = m.m_fng in
      let grouped = group_by_account tasks in
      Logging.debug_f
        ~section
        "capital-oracle pass #%d starting: %d asset(s) in %d account(s)%s"
        (Atomic.get pass_count + 1)
        (List.length tasks)
        (List.length grouped)
        (match fng with
         | Some f -> Printf.sprintf ", f&g %.2f" f
         | None -> ", f&g unavailable");
      let decisions = ref [] in
      (* Analysis work counts for the per-pass latency summary. *)
      let n_recomputed = ref 0 in
      let n_cached = ref 0 in
      (* Phase A: analyze every asset in parallel. The analysis reads only
         the materialized history (memoized: unchanged inputs return the
         same record in microseconds); the only wall-clock cost is the
         replay/blend recompute for assets whose inputs actually changed. *)
      let analysis_timeout = 60.0 in
      let analyze_one (task : Oracle_tasks.task) ~(index : int)
        : (Oracle_tasks.task * analysis option) Lwt.t
        =
        Lwt.catch
          (fun () ->
             let t_asset = Mtime_clock.now_ns () in
             let key = task.Oracle_tasks.exchange ^ "/" ^ task.Oracle_tasks.symbol in
             match
               List.find_opt
                 (fun (am : asset_material) ->
                    am.am_exchange = task.Oracle_tasks.exchange
                    && am.am_symbol = task.Oracle_tasks.symbol)
                 m.m_assets
             with
             | None ->
               Logging.debug_f
                 ~section
                 "no materialized history yet for %s; keeping last-known-good decision"
                 key;
               Lwt.return None
             | Some am ->
               Lwt.pick
                 [ (analyze_asset
                      config
                      classes
                      task
                      ~index
                      ~n_tasks:(List.length tasks)
                      ~am
                      ~fng
                    >|= fun (analysis, reused) ->
                    record_asset_latency
                      task.Oracle_tasks.symbol
                      (Mtime.Span.of_uint64_ns
                         (Int64.sub (Mtime_clock.now_ns ()) t_asset))
                      (fun () -> if reused then "cache-hit" else "analyze");
                    touched_assets := task.Oracle_tasks.symbol :: !touched_assets;
                    if reused then incr n_cached else incr n_recomputed;
                    `Ok (Some analysis))
                 ; (Lwt_unix.sleep analysis_timeout
                    >|= fun () ->
                    `Timeout
                      (Printf.sprintf "analysis timed out after %.0fs" analysis_timeout))
                 ]
               >|= (function
                | `Ok result -> result
                | `Timeout why ->
                  Logging.warn_f
                    ~section
                    "capital-oracle analysis timed out for %s (%s); keeping \
                     last-known-good decision, capital stays in the venue pool"
                    key
                    why;
                  None))
          (fun exn ->
             Logging.warn_f
               ~section
               "capital-oracle analysis failed for %s (%s); keeping last-known-good \
                decision, capital stays in the venue pool"
               (task.Oracle_tasks.exchange ^ "/" ^ task.Oracle_tasks.symbol)
               (Printexc.to_string exn);
             Lwt.return None)
        >|= fun result -> task, result
      in
      Lwt_list.map_p
        (fun (index, task) -> analyze_one task ~index)
        (List.mapi (fun i task -> i + 1, task) tasks)
      >>= fun task_analyses ->
      let task_analyses =
        List.filter_map
          (fun (task, result) -> Option.map (fun analysis -> task, analysis) result)
          task_analyses
      in
      let analyses_for account =
        List.filter_map
          (fun (task, analysis) ->
             if same_account account (account_of_task task) then Some analysis else None)
          task_analyses
      in
      (* Phase B: size sequentially per account, highest priority first,
         each asset against the ENTIRE remaining pool. Memoized per account
         on (analysis ids, pool bucket, strategy state): an unchanged
         account re-publishes its previous decisions instead of re-running
         the deployment engine. *)
      let state_token
            (task : Oracle_tasks.task)
            (snapshot : Oracle_balances.snapshot option)
        =
        let symbol = task.Oracle_tasks.symbol in
        let st = Dio_strategies.Suicide_grid.get_strategy_state symbol in
        let base =
          match snapshot with
          | None -> 0.0
          | Some snapshot ->
            let base_asset =
              match String.split_on_char '/' symbol with
              | b :: _ -> b
              | [] -> symbol
            in
            Oracle_balances.available_asset snapshot ~asset:base_asset
        in
        Printf.sprintf
          "%s:%.6g:%.6g:%.6g"
          symbol
          st.accumulated_profit
          st.reserved_base
          base
      in
      let rec size_all
                ~(venue_pool : float)
                ~(snapshot : Oracle_balances.snapshot option)
                (pool : float)
        = function
        | [] -> Lwt.return pool
        | analysis :: rest ->
          Lwt.catch
            (fun () ->
               let t_asset = Mtime_clock.now_ns () in
               size_asset
                 analysis.rc
                 analysis
                 ~pool
                 ~venue_pool
                 ~snapshot
                 ~fng
                 ~index:(List.length rest + 1)
                 ~n_tasks:(List.length tasks)
               >|= fun decision ->
               record_asset_latency
                 analysis.symbol
                 (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) t_asset))
                 (fun () -> "sizing");
               decisions := decision :: !decisions;
               decision.remainder)
            (fun exn ->
               Logging.warn_f
                 ~section
                 "capital-oracle sizing failed for %s/%s (%s); keeping last-known-good \
                  decision, capital stays in the venue pool"
                 analysis.exchange
                 analysis.symbol
                 (Printexc.to_string exn);
               Lwt.return pool)
          >>= fun next -> size_all ~venue_pool ~snapshot next rest
      in
      let reused_accounts = ref 0 in
      let rec take n l =
        if n <= 0
        then []
        else (
          match l with
          | [] -> []
          | x :: rest -> x :: take (n - 1) rest)
      in
      let rec drop n l =
        if n <= 0
        then l
        else (
          match l with
          | [] -> []
          | _ :: rest -> drop (n - 1) rest)
      in
      Lwt_list.iter_s
        (fun (account, account_tasks) ->
           match resolve_account_pool ~m account with
           | None ->
             (* No pool source for this account (no live store, no materialized
                balance, and no baseline to apply deltas to): keep its
                last-known-good decisions (publish merges). *)
             Lwt.return_unit
           | Some (pool, snapshot) ->
             let analyses = analyses_for account in
             let fp =
               { af_analyses =
                   List.map
                     (fun task ->
                        match
                          List.find_opt
                            (fun (a : analysis) ->
                               a.exchange = task.Oracle_tasks.exchange
                               && a.symbol = task.Oracle_tasks.symbol)
                            analyses
                        with
                        | Some a -> string_of_int a.id
                        | None -> "none")
                     account_tasks
               ; af_pool = pool
               ; af_fng = fng
               ; af_state =
                   String.concat
                     "|"
                     (List.map
                        (fun task -> state_token task (Some snapshot))
                        account_tasks)
               }
             in
             let t_size = Mtime_clock.now_ns () in
             (* Priority reclamation: which lower-priority resting buys to
                cancel so an unfundable higher-priority asset can resume.
                Computed fresh every pass (cheap) and applied on top of the
                memoized decisions below, so a changed plan takes effect on
                cache hits too (the plan is deliberately NOT part of the
                sizing fingerprint - constant buy-amends must not bust it).
                The plan is computed from the account's gate inputs on the
                decisions it will patch (see below). *)
             let log_reclaim reclaim =
               if reclaim <> []
               then
                 Logging.warn_f
                   ~section
                   "capital reclamation for %s: canceling %s to fund higher-priority \
                    assets (pool $%.2f)"
                   (account_id account)
                   (String.concat
                      ", "
                      (List.map (fun (_, s, t) -> Printf.sprintf "%s for %s" s t) reclaim))
                   pool
             in
             (* Cache reuse is gate-aware: reuse the cached decisions only
                 when no asset's first-buy gate has flipped since they were
                 sized. The stored token was computed at sizing time (sizing
                 pool + sizing costs); the fresh token uses the current pool
                 and LIVE first-buy costs (account_gate_inputs re-anchors on
                 top-of-book, which is deliberately NOT in the fingerprint).
                 A difference in either dimension - a small capital return
                 crossing an asset's first-buy cost inside the 0.5% pool
                 bucket, or an anchor-price drop making a parked priority
                 asset fundable - forces a fresh sizing; reusing the stale
                 INACTIVE decision would leave the strategy paused on
                 available capital, which is exactly the "capital returned
                 but the strategy did not resume in priority order" stall. *)
             let reuse_entry =
               match Hashtbl.find_opt size_cache (account_id account) with
               | Some (prev_fp, prev_decisions, sized_gate) when account_fp_eq prev_fp fp
                 ->
                 let deployed_of =
                   let m = Hashtbl.create 8 in
                   List.iter
                     (fun (d : decision) -> Hashtbl.replace m d.symbol d.deployed)
                     prev_decisions;
                   fun s -> Option.value (Hashtbl.find_opt m s) ~default:0.0
                 in
                 let gate_inputs =
                   account_gate_inputs account_tasks analyses ~decisions:prev_decisions
                 in
                 let same_gate =
                   account_gate_token ~pool gate_inputs ~deployed_of = sized_gate
                 in
                 Some (prev_fp, prev_decisions, gate_inputs, same_gate)
               | _ -> None
             in
             (match reuse_entry with
              | Some (_, prev_decisions, gate_inputs, true) ->
                incr reused_accounts;
                List.iter
                  (fun (d : decision) ->
                     record_asset_latency
                       d.symbol
                       (Mtime.Span.of_uint64_ns
                          (Int64.sub (Mtime_clock.now_ns ()) t_size))
                       (fun () -> "sizing-reuse"))
                  prev_decisions;
                Latency_profiler.record engine_profs.prof_sizing (span_from t_size);
                let reclaim = reclaim_plan gate_inputs in
                log_reclaim reclaim;
                let patched = apply_reclaim ~plan:reclaim prev_decisions in
                decisions := List.rev_append patched !decisions;
                (match List.rev patched with
                 | last :: _ ->
                   Logging.debug_f
                     ~section
                     "capital-oracle reuse: account %s sizing unchanged (pool $%.2f) - \
                      %d decision(s) kept (surplus $%.2f)"
                     (account_id account)
                     pool
                     (List.length prev_decisions)
                     last.remainder
                 | [] -> ());
                Lwt.return_unit
              | _ ->
                (* No cache entry, the fingerprint moved, or a first-buy gate
                    flipped since the cached sizing (pool or cost side):
                    re-size this account fresh. *)
                let before = List.length !decisions in
                size_all ~venue_pool:pool ~snapshot:(Some snapshot) pool analyses
                >|= fun surplus ->
                let n = List.length !decisions - before in
                let account_decisions = take n !decisions |> List.rev in
                (* The account's gate inputs on its own fresh decisions: they
                     feed the reclaim plan and are stamped (as a gate token)
                     into the cache for later flip detection. *)
                let gate_inputs_sized =
                  account_gate_inputs account_tasks analyses ~decisions:account_decisions
                in
                (* Stamp the sizing-time gate token (this pool, these first-
                      buy costs) so later passes can detect a flip in either
                      dimension and bypass the cache. *)
                let sized_gate =
                  let m = Hashtbl.create 8 in
                  List.iter
                    (fun (d : decision) -> Hashtbl.replace m d.symbol d.deployed)
                    account_decisions;
                  account_gate_token ~pool gate_inputs_sized ~deployed_of:(fun s ->
                    Option.value (Hashtbl.find_opt m s) ~default:0.0)
                in
                Hashtbl.replace
                  size_cache
                  (account_id account)
                  (fp, account_decisions, sized_gate);
                (* Apply the reclaim plan: the account's decisions just came off
                     the head of [!decisions]; swap them for the patched ones. *)
                let reclaim = reclaim_plan gate_inputs_sized in
                log_reclaim reclaim;
                let patched = apply_reclaim ~plan:reclaim account_decisions in
                decisions := List.rev_append patched (drop n !decisions);
                Latency_profiler.record engine_profs.prof_sizing (span_from t_size);
                Logging.debug_f
                  ~section
                  "venue %s: pool %.2f, surplus %.2f (idle reserve)"
                  (account_id account)
                  pool
                  surplus))
        grouped
      >>= fun () ->
      publish !decisions;
      Atomic.incr pass_count;
      (* The refresher records balance/fetch windows; the pass publishes its
         own windows (pass + sizing) and the per-asset windows. *)
      let pass_span = span_from pass_start in
      Latency_profiler.record engine_profs.prof_pass pass_span;
      let _ = Latency_profiler.snapshot_and_reset engine_profs.prof_pass in
      let _ = Latency_profiler.snapshot_and_reset engine_profs.prof_sizing in
      List.iter
        (fun symbol ->
           ignore (Latency_profiler.snapshot_and_reset (asset_profiler_of symbol)))
        !touched_assets;
      Atomic.set refresh_history_changed false;
      (* One scannable latency summary per pass: the whole pass time plus the
         phase breakdown (balance/fetch are the refresher's most recent cycle;
         sizing and the per-asset analysis are this pass) so performance work
         has a single line to track. *)
      let n_decisions = List.length !decisions in
      let n_active =
        List.fold_left
          (fun acc (d : decision) -> if d.active then acc + 1 else acc)
          0
          !decisions
      in
      let ms_of snap =
        match snap with
        | None -> "—"
        | Some (s : Latency_profiler.snapshot) -> Printf.sprintf "%.1fms" (s.p50 /. 1000.0)
      in
      let worst_asset =
        List.fold_left
          (fun acc symbol ->
             match Latency_profiler.published_snapshot (asset_profiler_of symbol) with
             | Some (s : Latency_profiler.snapshot) when s.samples > 0 && s.p99 > 0.0 ->
               (match acc with
                | None -> Some (symbol, s)
                | Some (_, best) when s.p99 > best.p99 -> Some (symbol, s)
                | _ -> acc)
             | _ -> acc)
          None
          !touched_assets
      in
      Logging.info_f
        ~section
        "pass #%d: %d decisions (%d active) %d account(s) %.1fs · %s · bal %s fetch %s \
         sizing %s · %d recomputed + %d cached%s%s"
        (Atomic.get pass_count)
        n_decisions
        n_active
        (List.length grouped)
        (Int64.to_float (Mtime.Span.to_uint64_ns pass_span) /. 1e9)
        (match fng with
         | Some f -> Printf.sprintf "f&g %.1f" f
         | None -> "f&g n/a")
        (ms_of (Latency_profiler.published_snapshot engine_profs.prof_balance))
        (ms_of (Latency_profiler.published_snapshot engine_profs.prof_fetch))
        (ms_of (Latency_profiler.published_snapshot engine_profs.prof_sizing))
        !n_recomputed
        !n_cached
        (match worst_asset with
         | Some (symbol, (s : Latency_profiler.snapshot)) ->
           Printf.sprintf " · slowest %s p99 %.1fms" symbol (s.p99 /. 1000.0)
         | None -> "")
        (if !reused_accounts > 0
         then Printf.sprintf " · %d account(s) reused" !reused_accounts
         else "");
      Lwt.return true)
;;

(** Run the live oracle loop: initialize venue metadata once, run the first
    pass immediately, then refresh on the configured cadence. Runs on the Lwt
    scheduler; the engine's domains pick up each published snapshot on their
    next cycle. [on_publish] (optional) is invoked after each pass with the
    symbols whose decision changed this pass and the full snapshot - the
    engine uses it to wake those domains so a new decision applies immediately
    instead of on the next market event.

    Resolves when [shutdown] is requested or the loop ends. The supervisor
    runs this as the oracle's supervised connect_fn so the runtime is managed
    like every other supervised module (registered, heartbeated, restarted). *)
let run_loop
      ?(config = default_config ())
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * class_pool) list)
      ?(on_publish : string list -> decision list -> unit = fun _ _ -> ())
      ()
  : unit Lwt.t
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
  (* Priority reclamation is self-driving: a pass that publishes reclaim
     decisions means the domains are canceling resting buys right now, which
     releases capital back to the account pool. Re-run immediately (an event
     trigger wakes both loops, past the one-shot wait) so the released capital
     is recognized within moments of the cancel - the pool delta from the
     cancel's [notify_order_cancel] re-sizes without a network wait. The
     reclaim signature bounds the loop: it self-drives only when the reclaim
     set CHANGED, so a reclaim that stalls (cancel stuck, no event) waits for
     the domain's own [request_pass] / the poll cadence instead of spinning. *)
  let last_reclaim_sig = ref "" in
  let reclaim_signature () =
    decisions ()
    |> List.filter (fun (d : decision) -> d.reclaim_capital)
    |> List.map (fun (d : decision) -> d.symbol ^ ">" ^ d.reclaim_target)
    |> List.sort String.compare
    |> String.concat ","
  in
  let rec loop () =
    if Atomic.get shutdown_requested
    then Lwt.return_unit
    else
      Lwt.catch
        (fun () -> run_pass ~config ~trading ~classes ())
        (fun exn ->
           Logging.error_f
             ~section
             "capital-oracle pass failed (%s); keeping last-known-good decisions"
             (Printexc.to_string exn);
           Lwt.return false)
      >>= fun attempted ->
      (* Mark the attempt finished BEFORE waking the domains: a domain that
         wakes on this publish signal must already see it in its gate check.
         Only a pass that actually reached the decision phase counts (see
         run_pass): the cold-start pass publishes nothing, so counting it
         would open the domains' F&G-only fallback gate while the oracle's
         first real decisions are still seconds away. *)
      if attempted then Atomic.incr pass_attempts;
      on_publish !last_changed_symbols (decisions ());
      (* The next pass runs at the cadence deadline, or early when the engine
         requests one ([request_pass] on fills / canceled-rejected-expired
         events) or when the background refresher materialized a change
         ([refresh_history_changed]): the wait captures both generations, so
         only NEW events wake it (each wait is one-shot; bursts coalesce
         through the single queue drain per pass and the account sizing
         memo cache). *)
      let reclaim_now = reclaim_signature () in
      let reclaim_cycle = reclaim_now <> "" && reclaim_now <> !last_reclaim_sig in
      last_reclaim_sig := reclaim_now;
      (* A reclaim pass self-drives: fire the event trigger so both loops wake
         immediately (the wait below captures the post-trigger generation, so
         this wake is one-shot) and the refresher publishes fresh balances for
         the follow-up pass. *)
      if reclaim_cycle then request_pass () else ();
      let generation = Atomic.get pass_requested in
      let refresh_gen = Atomic.get refresh_generation in
      let deadline =
        Unix.gettimeofday () +. jittered (next_sleep ~config ~decisions:(decisions ()))
      in
      wait_until ~deadline ~generation ~refresh_gen () >>= fun () -> loop ()
  in
  (* Venue instrument metadata is initialized once here (idempotent; the
     supervisor keeps its own). *)
  Lwt.catch
    (fun () -> Oracle_venues.init tasks)
    (fun exn ->
       Logging.warn_f
         ~section
         "venue instrument metadata init failed (%s); increments fall back"
         (Printexc.to_string exn);
       Lwt.return_unit)
  >>= fun () ->
  (* The background refresher owns every network call (balances, histories,
     members, calendar). The pass loop and the refresher run as parallel
     fibers and both stop on shutdown; the loop above is the decision
     path, which never waits on network beyond the bounded fill-wait. A
     supervisor auto-restart cancels any previous-generation refresher
     first, so exactly one refresher runs per runtime generation. *)
  Option.iter Lwt.cancel !refresh_fiber;
  (* Lwt promises run eagerly: build the refresher promise directly (its
      loop already swallows exceptions) and keep it for cancellation. *)
  refresh_fiber
  := Some
       (Lwt.catch
          (fun () -> refresh_loop ~config ~trading ~classes ())
          (fun _ -> Lwt.return_unit));
  loop ()
  >>= fun () ->
  Option.iter Lwt.cancel !refresh_fiber;
  Lwt.return_unit
;;

(** Start the runtime detached on the Lwt scheduler (fire-and-forget; the
    loop's failures are logged and never crash the engine). The supervisor
    path (Supervisor.start_oracle) uses [run_loop] directly so it owns the
    lifecycle: registration, heartbeat monitoring, and auto-restart. *)
let start
      ?(config = default_config ())
      ~(trading : Dio_strategies.Strategy_common.trading_config list)
      ~(classes : (string * class_pool) list)
      ?(on_publish : string list -> decision list -> unit = fun _ _ -> ())
      ()
  =
  Lwt.async (fun () ->
    Lwt.catch
      (fun () -> run_loop ~config ~trading ~classes ~on_publish ())
      (fun exn ->
         Logging.error_f
           ~section
           "capital-oracle loop ended unexpectedly (%s); keeping last-known-good \
            decisions"
           (Printexc.to_string exn);
         Lwt.return_unit));
  ()
;;

(** Whether the runtime has been told to stop (the supervisor sets this on
    engine shutdown via [shutdown]). *)
let is_stopped () = Atomic.get shutdown_requested

let shutdown () = Atomic.set shutdown_requested true
