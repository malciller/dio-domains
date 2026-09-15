(** Per-venue capability descriptors.

    Encodes what a venue actually guarantees: whether balances net open-order holds,
    whether the venue exposes an immediately-sellable figure, whether ack timing is
    observable, and the integrity flags the grid previously carried as a per-symbol matrix
    (`jacobs_ladder_config.ml`). The central accounting module (milestone 2) reads these
    descriptors instead of strategy-local flags, so every strategy and venue inherits the
    same guarantees. *)

module Exchange = Dio_exchange.Exchange_intf

type t =
  { venue : string
  ; time_in_force : string
  ; track_pending_sells : bool
  ; use_accumulation_sells : bool
  ; sell_failure_sets_asset_low : bool
  ; use_reserved_base_guard : bool
  ; use_unnetted_sell_hold : bool
  ; balance_nets_open_order_holds : bool
  ; hold_netted_from_venue_state : bool
  ; asset_low_requires_balance_change : bool
  ; merge_preserved_sells : bool
  ; check_stale_balance : bool
  ; remaintain_expired_sells : bool
  }

let kraken =
  { venue = "kraken"
  ; time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = false
  ; asset_low_requires_balance_change = true
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

let hyperliquid =
  { venue = "hyperliquid"
  ; time_in_force = "Alo"
  ; track_pending_sells = false
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = true
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = false
  ; remaintain_expired_sells = false
  }
;;

let ibkr =
  { venue = "ibkr"
  ; time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = false
  ; hold_netted_from_venue_state = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

let lighter =
  { venue = "lighter"
  ; time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = false
  ; hold_netted_from_venue_state = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = false
  ; remaintain_expired_sells = false
  }
;;

let alpaca =
  { venue = "alpaca"
  ; time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = true
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = true
  }
;;

let for_exchange (exchange : string) : t =
  match Exchange.Types.exchange_of_string exchange with
  | Hyperliquid -> hyperliquid
  | Lighter -> lighter
  | Ibkr -> ibkr
  | Alpaca -> alpaca
  | Kraken | Custom _ -> kraken
;;
