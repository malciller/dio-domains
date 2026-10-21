(** Hyperliquid account abstraction mode.

    In [unifiedAccount] and [portfolioMargin], USDC-margined perps are collateralised by
    the spot USDC balance, and the per-DEX [clearinghouseState.marginSummary] is not
    meaningful (HL docs: "unified account and portfolio margin show all balances and holds
    in the spot clearinghouse state. Individual perp dex user states are not meaningful").
    In standard/disabled mode spot and perp balances are separate, so the per-DEX summary
    is the right source.

    The mode is read once from the venue's [userAbstraction] info endpoint at startup.
    Until it is known we keep the per-DEX summary, which is correct for standard accounts
    and only briefly wrong for unified ones. *)

let section = "hyperliquid_account_mode"

type t =
  | Unknown
  | Standard
  | Unified
  | Portfolio

let current = Atomic.make Unknown
let get () = Atomic.get current

let to_string = function
  | Unknown -> "unknown"
  | Standard -> "standard"
  | Unified -> "unifiedAccount"
  | Portfolio -> "portfolioMargin"
;;

let of_string = function
  | "unifiedAccount" -> Unified
  | "portfolioMargin" -> Portfolio
  | "disabled" | "default" | "dexAbstraction" -> Standard
  | _ -> Unknown
;;

let set mode =
  let previous = Atomic.exchange current mode in
  if previous <> mode
  then (
    Logging.info_f ~section "account abstraction: %s" (to_string mode);
    match mode with
    | Portfolio ->
      Logging.warn_f
        ~section
        "portfolio margin detected; perp margin equity is approximated from spot USDC \
         only (other eligible collateral is ignored, so the ratio is conservative)"
    | Unknown ->
      Logging.debug ~section "account abstraction unknown; using per-DEX margin"
    | Standard | Unified -> ())
;;

(** True when spot USDC is the perp collateral source. For [Portfolio] this is a
    deliberately conservative approximation: other eligible spot collateral is ignored. *)
let uses_spot_collateral mode =
  match mode with
  | Unified | Portfolio -> true
  | Unknown | Standard -> false
;;

let uses_spot_collateral_now () = uses_spot_collateral (get ())
