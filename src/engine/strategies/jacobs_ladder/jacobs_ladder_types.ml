(* Deprecated shim: the core state types and registry now live in {!Strategy_state}.
   Retained so existing references keep resolving during the generic-action refactor;
   delete once no module references [Jacobs_ladder_types]. *)

include Strategy_state
