(* DIO Capital Survival Engine - shared types.

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
  | Large_cap_stable
  | Large_cap_volatile
  | Equity_index
  | Equity_volatile

let archetype_of_string = function
  | "large_cap_stable" | "Large_Cap_Stable" -> Large_cap_stable
  | "large_cap_volatile" | "Large_Cap_Volatile" -> Large_cap_volatile
  | "equity_index" | "Equity_Index" -> Equity_index
  | "equity_volatile" | "Equity_Volatile" -> Equity_volatile
  | s -> invalid_arg ("Survival.archetype_of_string: " ^ s)
;;

let string_of_archetype = function
  | Large_cap_stable -> "large_cap_stable"
  | Large_cap_volatile -> "large_cap_volatile"
  | Equity_index -> "equity_index"
  | Equity_volatile -> "equity_volatile"
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
    (** Number of valid per-start MFD windows pooled for this horizon. This is
        the [n_asset] weight in the kappa blend. *)
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
  ; class_coverage : float (** Pooled class F_h(D_surv). *)
  ; blended_coverage : float (** (n_asset*F_asset + kappa*F_class)/(n_asset + kappa). *)
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
