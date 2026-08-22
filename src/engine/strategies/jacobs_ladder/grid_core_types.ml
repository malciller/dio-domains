(* Grid_core - pure DIO Grid state machine types.

   Oracle-grade re-implementation of the live Jacobs_ladder semantics so the
   survival engine answers "can this grid survive?" against the exact behaviour
   of Jacobs_ladder_execution, without the venue/network stack. Contract tests
   pin the level/rounding helpers to Jacobs_ladder_config. *)

type exchange_model =
  | Kraken
  | Hyperliquid
  | Lighter
  | Ibkr
  | Alpaca

(** Per-bar OHLC. Low/high drive fills; close is used for session reporting. *)
type bar =
  { high : float
  ; low : float
  ; close : float
  }

(** Intra-bar event ordering. Pessimistic (default): buys consume before a sell
    replenishes. Optimistic: sell first. *)
type ordering =
  | Buy_first
  | Sell_first

type side =
  [ `Buy
  | `Sell
  ]

type fill =
  { side : side
  ; price : float
  ; qty : float
  ; quote_delta : float
  }

(** Why the grid's first capital-low event fired.
    [`Capital]: the quote balance could not fund the (dynamically up-sized)
    buy cost - true capital exhaustion.
    [`Not_placeable]: the required quantity still fails the venue's qty_min
    gate even after dynamic up-sizing - a parameter/venue-sizing halt that
    more capital cannot fix. *)
type halt_cause =
  [ `Capital
  | `Not_placeable
  ]

(** Starting state for a grid that models an already-running strategy: the
    account's current base holdings, base locked in resting sells, and the
    accumulated profit buffer (the sell gate for accumulation venues). All
    zeros = a fresh grid. The oracle seeds its sizing replays from the live
    account/accumulated state so the survival verdict answers "can THIS grid,
    as it actually runs, survive?" instead of "can a hypothetical fresh grid?" *)
type seed =
  { initial_base : float
  ; initial_reserved_base : float
  ; initial_accumulated_profit : float
  }

let empty_seed =
  { initial_base = 0.0; initial_reserved_base = 0.0; initial_accumulated_profit = 0.0 }
;;
