(* Grid_core - pure DIO Grid state machine types.

   Survival-grade re-implementation of the live Suicide_grid semantics so the
   survival engine answers "can this grid survive?" against the exact behaviour
   of Suicide_grid_execution, without the venue/network stack. Contract tests
   pin the level/rounding helpers to Suicide_grid_config. *)

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
