(* DIO Capital Oracle - facade.

   The oracle is one pipeline: all-time merged price history (venue bars +
   Yahoo deep history, backwards-only) -> references (max drawdown / ATH /
   ATL) -> runway math (floor price, regime, aggressiveness) -> survival
   replay + parameter search -> a four-field decision record per asset.
   Modules:

   - Oracle_fetch     all-time merged series per asset (disk-cached)
   - Oracle_core      references, runway, d_surv, search, decision record
   - Oracle_pipeline  one pure pass: history in, decision out
   - Oracle_pools     per-venue allocation, priority walk, cancel cascade
   - Oracle_runtime   event-driven engine wiring the above live

   Strategy path replay lives in Grid_core (dio.strategies); the runtime
   consumes it through Oracle_pipeline only. *)

module Core = Oracle_core
module Pipeline = Oracle_pipeline
module Pools = Oracle_pools
