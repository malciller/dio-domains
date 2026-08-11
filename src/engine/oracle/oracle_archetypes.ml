(* Oracle archetypes - risk-class data model.

   Provides the data model and default class definitions. Class survival curves
   are estimated from pooled member history by Oracle_classes; members come
   from the config.json "classes" map (see Oracle_tasks / bin/oracle.ml),
   never from hardcoded symbol lists. Kappa defaults: 252 sessions for equity
   classes, 365 for crypto (one per-session "prior weight" per full year of the
   class's own history). *)

open Oracle_types

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
    [ { name = "crypto_core"; kappa = k; archetypes = [ Crypto_core ] }
    ; { name = "crypto_alt"; kappa = k; archetypes = [ Crypto_alt ] }
    ]
  | Equity ->
    [ { name = "equity_etf"; kappa = k; archetypes = [ Equity_etf ] }
    ; { name = "equity_momentum"; kappa = k; archetypes = [ Equity_momentum ] }
    ]
;;
