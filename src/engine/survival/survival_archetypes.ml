(* Survival archetypes - risk-class data model.

   Provides the data model and default class definitions. Class survival curves
   are estimated from pooled member history by Survival_classes; members come
   from the config.json "classes" map (see Survival_tasks / bin/survival.ml),
   never from hardcoded symbol lists. Kappa defaults: 252 sessions for equity
   classes, 365 for crypto (one per-session "prior weight" per full year of the
   class's own history). *)

open Survival_types

type risk_class =
  { name : string
  ; kappa : int
  ; archetypes : archetype list
  }

let default_kappa = function
  | Crypto -> 365
  | Equity -> 252
;;

let default_classes ~(calendar_kind : calendar_kind) =
  let k = default_kappa calendar_kind in
  match calendar_kind with
  | Crypto ->
    [ { name = "large_cap_stable"; kappa = k; archetypes = [ Large_cap_stable ] }
    ; { name = "large_cap_volatile"; kappa = k; archetypes = [ Large_cap_volatile ] }
    ]
  | Equity ->
    [ { name = "equity_index"; kappa = k; archetypes = [ Equity_index ] }
    ; { name = "equity_volatile"; kappa = k; archetypes = [ Equity_volatile ] }
    ]
;;
