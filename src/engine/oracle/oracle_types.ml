(* DIO Capital Oracle Engine - shared types.

   Naming (per the review): F_h(d) = P(MFD_h <= d) is a CDF -> "coverage";
   S_h(d) = 1 - F_h(d) is the survival function. *)

type calendar_kind =
  | Crypto (* 24/7: a session is a calendar day; missing days are gaps. *)
  | Equity
(* Market sessions only; calendar equivalence derived from session counts. *)

type bar =
  { date : string
  ; open_ : float
  ; high : float
  ; low : float
  ; close : float
  ; volume : float
  }

(** A run of missing sessions. [after]/[before] are the ISO dates bounding the
    gap; [missing_days] is the number of expected sessions skipped (calendar
    days for crypto). *)
type gap =
  { after : string
  ; before : string
  ; missing_days : int
  }

type series =
  { symbol : string
  ; calendar_kind : calendar_kind
  ; bars : bar array
  ; gaps : gap list
  }

(** Risk archetype. Membership and class estimation are data-pipeline concerns
    (Phase 2); the type is defined now so the blend is typed end-to-end. *)
type archetype =
  | Crypto_core
  | Crypto_alt
  | Equity_etf
  | Equity_momentum

let archetype_of_string = function
  | "crypto_core" | "Crypto_Core" -> Crypto_core
  | "crypto_alt" | "Crypto_Alt" -> Crypto_alt
  | "equity_etf" | "Equity_Etf" -> Equity_etf
  | "equity_momentum" | "Equity_Momentum" -> Equity_momentum
  | s -> invalid_arg ("Oracle.archetype_of_string: " ^ s)
;;

let string_of_archetype = function
  | Crypto_core -> "crypto_core"
  | Crypto_alt -> "crypto_alt"
  | Equity_etf -> "equity_etf"
  | Equity_momentum -> "equity_momentum"
;;

(** One point on a survival surface: coverage = F_h(d), survival = 1 - F_h(d). *)
type surface_row =
  { drawdown_pct : float
  ; coverage : float
  ; survival : float
  }

type survival_surface =
  { horizon_label : string
  ; calendar_days : int
  ; n_starts : int
    (** Number of valid per-start MFD windows over all (overlapping) starts.
        Display-only: the kappa blend weights by the window count on the
        model's sampling basis instead (see Oracle_replay.blend_index). *)
  ; rows : surface_row list
  }

type percentile_row =
  { percentile : float
  ; mfd : float
  }

type percentile_table =
  { horizon_label : string
  ; calendar_days : int
  ; n_starts : int
    (** Number of valid per-start MFD windows over all (overlapping) starts.
        Overlapping windows are autocorrelated, so this overstates the
        information content for tail percentiles. *)
  ; n_eff : int
    (** Effective sample size: number of non-overlapping windows (stride =
        horizon sessions) the percentile rows are actually estimated from.
        A 365-session horizon on ~1600 sessions yields ~4 independent
        windows; P99 is then the worst of those, which is the honest answer. *)
  ; rows : percentile_row list
  }

(** Estimated class curves over the pooled member history (Phase 2). Each
    surface/table shares the pooled per-start sample set for its horizon. *)
type class_estimate =
  { class_name : string
  ; kappa : int
  ; member_count : int
  ; surfaces : survival_surface list
  ; percentile_tables : percentile_table list
  }

(** Asset curve blended toward one class: F_blend(d) = (n_a*F_a + kappa*F_c)/
    (n_a + kappa), S_blend = 1 - F_blend. *)
type blended_surface =
  { class_name : string
  ; surface : survival_surface
  }

type blended_percentile_table =
  { class_name : string
  ; table : percentile_table
  }

type horizon =
  { label : string
  ; sessions : int
  ; calendar_days : int
  }

let horizon_label kind n =
  match kind with
  | Crypto -> Printf.sprintf "%dd" n
  | Equity -> Printf.sprintf "%ds" n
;;

(** Calendar-day equivalent of [sessions] for a calendar kind. For crypto a
    session is a day; for equity a session is one market day (approx. 1/252 of
    a year, i.e. the classic 252-session year). *)
let calendar_days_of_sessions kind sessions =
  match kind with
  | Crypto -> sessions
  | Equity -> int_of_float (Float.round (float_of_int sessions *. (365.0 /. 252.0)))
;;

(** Per-horizon survival headline: how deep a drawdown the grid survives on the
    asset's own history vs. blended toward its risk class. *)
type historical_path_coverage =
  { horizon : horizon
  ; asset_coverage : float
    (** F_asset_h(D_surv): share of the asset's own starts that survived a
        drawdown at least as deep as the grid's. *)
  ; class_coverage : float
    (** Pooled class F_h(D_surv), volatility-translated: the class z-CDF
        evaluated at each asset start's own regime d / (sigma_s * sqrt h),
        averaged over the asset's starts. *)
  ; blended_coverage : float (** (n_a*F_asset + kappa*F_class)/(n_a + kappa). *)
  }

(** Result of inverse sizing: the capital / qty that just clears a target
    blended survival probability on the historical path. [reachable] is false
    when no parameter within the search bounds clears the target (e.g. the
    target sits inside the gap between the deepest exhausting coverage and the
    never-exhausted coverage of 1.0, or the user's [max_capital] bound is too
    low). *)
type sizing_result =
  { parameter : string
  ; value : float
  ; d_surv : float
  ; coverage : float
  ; reachable : bool
  }

(** Per-asset historical price-range reference, computed from the (deepened)
    series: the all-time high, the all-time low and where the current price
    sits within that span. This is the "potential price range" of the asset -
    the reference the sizing and spacing use to know how deep a fall from the
    current level can plausibly go and how much of it has already happened. *)
type range_stats =
  { ath : float (** Highest high over the whole history. *)
  ; all_time_low : float (** Lowest low over the whole history. *)
  ; price : float (** Last close. *)
  ; d_from_ath : float
    (** Drawdown already realized from the peak: (ath - price) / ath. 0 at
        the ATH, [range_span] at the all-time low. *)
  ; d_to_low : float
    (** Remaining straight-line fall to the historical low:
        (price - low) / price. 0 at the low, bounded by [range_span]. *)
  ; range_span : float
    (** Widest drawdown the history has actually spanned: (ath - low) / ath. *)
  }

(* ---- Deployment sizing (the engine's core output) ---- *)

(** Blended coverage of one horizon at the deployment's D_surv. *)
type deployment_coverage =
  { horizon_label : string
  ; blended_coverage : float
  }

(** One row of the parameter tuning surface: the deployment the model produces
    at a candidate strategy parameter (grid: grid interval), and whether it
    clears the target survival on the actual replayed path. *)
type deployment_row =
  { parameter : float (** Candidate strategy parameter (grid: grid interval in %). *)
  ; qty : float
    (** Deployed quantity (lot-rounded, >= qty_min, down-sized by the
        verification loop when the replayed path cannot clear the target). *)
  ; deployed : float
    (** Quote capital the ladder consumes through the governing drawdown at
        this sizing (capped at the pool share). *)
  ; d_surv_static : float
    (** Design runway: the static drawdown the sizing funds (the governing
        drawdown target, modulo fill-count rounding). *)
  ; d_surv_replay : float (** Actual runway from the strategy path replay at the pool. *)
  ; min_quote_drawdown : float
    (** Worst realized dip of the quote balance over the replayed path. *)
  ; coverage : deployment_coverage list
    (** Per-horizon blended coverage at the replayed D_surv. *)
  ; passed : bool (** Every horizon clears the target survival on the replayed path. *)
  ; profit_proxy : float
    (** Net profit of one strategy cycle per unit of deployed capital
        (advisory tuning metric). *)
  }

(** The two inputs that decide the resolved strategy parameter. Crypto blends
    the Fear & Greed signal against the oracle's capital-constrained
    tightness; equities use the oracle side alone. The per-asset historical
    range position (ATH reference) joins the blend as a third side: near the
    top of the range the potential fall is the whole span, so spacing widens
    (preserve runway); near the lows the remaining downside is bounded, so
    spacing tightens - an aggressive accumulator that works with the F&G
    contrarian convention. *)
type parameter_components =
  { fng : float option (** Resolved Fear & Greed value (None for equities). *)
  ; fng_parameter : float option
    (** lo + (hi-lo)*fng/100: the contrarian signal (fear tightens the
        parameter, for the grid densifying levels and accumulating base at
        depressed prices). *)
  ; survival_parameter : float
    (** Tightest parameter in the config range whose deployment clears the
        target survival on the replayed path: the density the available
        capital and the asset's historical volatility can actually afford. *)
  ; resolved_parameter : float
    (** Final parameter: the weighted blend of the fng / survival / range
        sides, clamped to the range and never tighter than
        [survival_parameter] (the runway wins over sentiment and range
        aggression alike). *)
  ; fng_weight : float (** The weight the F&G side carried in the blend (0.5 default). *)
  ; range_parameter : float option
    (** lo + (1 - position) * (hi - lo) with position = d_from_ath /
        range_span in [0, 1]: near the ATH (position 0) spacing widens toward
        hi, near the all-time low (position 1) it tightens toward lo. *)
  ; range_weight : float
    (** The weight the range side carried in the blend (0.25 default). *)
  }

(** The engine's decision for one asset: the position size and strategy
    parameter that deploy the pool share while reserving enough runway for the
    target drawdown, and whether the asset should stay active at all. *)
type asset_deployment =
  { active : bool (** The model's recommendation: fund and run this asset. *)
  ; reason : string (** Why the asset is inactive, when it is. *)
  ; pool_share : float
    (** Capital this asset drew from the venue pool (config-order priority). *)
  ; deployed : float
    (** Capital the ladder actually consumes at the recommended sizing
        (<= pool_share; the under-funded case consumes the whole share). *)
  ; remainder : float (** pool_share - deployed: capital passed to the next asset. *)
  ; governing_horizon : string
    (** The horizon whose target drawdown is the deepest (the binding one). *)
  ; d_gov : float (** Deepest drawdown with F_blend(d) >= target across the horizons. *)
  ; parameter_components : parameter_components
  ; qty : float
  ; parameter : float (** Resolved strategy parameter (grid: grid interval in %). *)
  ; d_surv : float (** Replayed D_surv at the recommended sizing. *)
  ; min_quote_drawdown : float
  ; range : range_stats option
    (** Per-asset historical price-range reference (ATH / low / position).
        None only when the series is empty. *)
  ; coverage : deployment_coverage list
  ; warnings : string list
  ; tuning_rows : deployment_row list
    (** The parameter scan surface (one row per candidate parameter). *)
  ; row : deployment_row (** The resolved row (the recommendation). *)
  }

(** One venue account's allocation result. *)
type venue_deployment =
  { venue : string
  ; quote : string
  ; testnet : bool
  ; pool : float (** Venue-locked quote capital available to the account. *)
  ; surplus : float (** Pool left after the priority-ordered allocation (idle reserve). *)
  ; assets : asset_deployment list (** In config priority order. *)
  }
