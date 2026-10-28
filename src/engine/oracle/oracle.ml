(* DIO Capital Oracle - facade.

   Pipeline: merged price history (venue bars + Yahoo deep history, backwards-only) ->
   references (max drawdown / high / low over a trailing lookback window, all time when it
   is 0) -> runway math (floor price, regime, aggressiveness) -> survival replay +
   parameter search -> four-field decision record per asset.

   Oracle_fetch all-time merged per-asset series, disk-cached. Oracle_core references,
   runway, d_surv, search, decision record. Oracle_pipeline one pure pass: history in,
   decision out. Oracle_pools per-venue allocation, priority walk, cancel cascade.
   Oracle_runtime event-driven engine wiring the modules above. *)

module Core = Oracle_core
module Pipeline = Oracle_pipeline
module Pools = Oracle_pools
