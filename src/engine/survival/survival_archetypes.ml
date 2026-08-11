(* Survival archetypes - risk classes for the kappa blend.

   Provides the data model and default class definitions. Class survival curves
   are estimated from pooled member history by Survival_classes; members here
   come from the Survival_classes.Registry symbol -> class map (Phase 2). Kappa
   defaults: 252 sessions for equity classes, 365 for crypto (one per-session
   "prior weight" per full year of the class's own history). *)

open Survival_types

type risk_class =
  { name : string
  ; kappa : int
  ; archetypes : archetype list
  ; members : string list
  }

let default_kappa = function
  | Crypto -> 365
  | Equity -> 252
;;

let default_classes ~(calendar_kind : calendar_kind) =
  let k = default_kappa calendar_kind in
  let members name = Survival_classes.Registry.member_symbols name in
  match calendar_kind with
  | Crypto ->
    [ { name = "large_cap_stable"
      ; kappa = k
      ; archetypes = [ Large_cap_stable ]
      ; members = members "large_cap_stable"
      }
    ; { name = "large_cap_volatile"
      ; kappa = k
      ; archetypes = [ Large_cap_volatile ]
      ; members = members "large_cap_volatile"
      }
    ]
  | Equity ->
    [ { name = "equity_index"
      ; kappa = k
      ; archetypes = [ Equity_index ]
      ; members = members "equity_index"
      }
    ; { name = "equity_volatile"
      ; kappa = k
      ; archetypes = [ Equity_volatile ]
      ; members = members "equity_volatile"
      }
    ]
;;
