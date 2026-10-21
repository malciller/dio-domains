(** Fixed slots for every fact and gate the cycle engine publishes.

    The strategy file addresses these as [$platform.<name>] / [$state.<name>]; each such
    reference is interned into the same table at parse time, so the file and the engine
    resolve to the same slot. Resolving the engine side once here — at module
    initialisation, before any domain spawns — turns every per-cycle fact publication into
    an array store instead of a string hash lookup, restoring the fixed fact/gate plumbing
    the engine had before the strategy-file indirection.

    Adding a fact: define it here and publish via [Strategy_runtime.set_platform_slot].
    The string form of the name must match the [$platform.*] reference used in the file. *)

let platform = Strategy_expr.intern_key

(* early_facts: published before the order-feed scan. *)
let price_nan = platform "price_nan"
let check_stale_balance = platform "check_stale_balance"
let asset_balance_nan = platform "asset_balance_nan"
let quote_balance_nan = platform "quote_balance_nan"
let maker_fee_set = platform "maker_fee_set"
let fee_refresh_due = platform "fee_refresh_due"

(* cycle_facts: published after the scan. *)
let oracle_halted = platform "oracle_halted"
let tif_recovery_pending = platform "tif_recovery_pending"
let tif_recovery_since = platform "tif_recovery_since"
let has_pending_buy = platform "has_pending_buy"
let has_tracked_buy = platform "has_tracked_buy"
let inflight_cancel_buy = platform "inflight_cancel_buy"
let inflight_amend_buy = platform "inflight_amend_buy"
let open_buy_count = platform "open_buy_count"
let has_recent_amend_buy = platform "has_recent_amend_buy"

(* buy_place_plan. *)
let buy_price = platform "buy_price"
let buy_qty = platform "buy_qty"
let buy_quote_needed = platform "buy_quote_needed"
let buy_available = platform "buy_available"
let buy_balance_ok = platform "buy_balance_ok"
let buy_capital_low = platform "buy_capital_low"
let buy_crossing = platform "buy_crossing"
let buy_quote_nan = platform "buy_quote_nan"
let buy_cooldown = platform "buy_cooldown"
let buy_inflight = platform "buy_inflight"

(* sell_finalize_facts. *)
let remaintain_expired_sells = platform "remaintain_expired_sells"
let sell_missing_empty = platform "sell_missing_empty"
let just_filled_buy = platform "just_filled_buy"
let resuming_after_balance = platform "resuming_after_balance"
let buy_attempted = platform "buy_attempted"
let sell_pushed = platform "sell_pushed"
let has_active_sell = platform "has_active_sell"
let balance_fresh = platform "balance_fresh"

(* market_facts: generic execution context (venue-agnostic). *)
let symbol = platform "symbol"
let base_coin = platform "base_coin"
let now = platform "now"
let bid = platform "bid"
let ask = platform "ask"
let spot_bid = platform "spot_bid"
let spot_ask = platform "spot_ask"
let mark = platform "mark"
let oracle = platform "oracle"
let funding_apr = platform "funding_apr"
let premium = platform "premium"
let funding_valid = platform "funding_valid"
let perp_valid = platform "perp_valid"
let perp_qty = platform "perp_qty"
let perp_entry = platform "perp_entry"
let perp_margin = platform "perp_margin"
let perp_leverage = platform "perp_leverage"
let perp_liq = platform "perp_liq"
let perp_account_value = platform "perp_account_value"
let perp_maintenance = platform "perp_maintenance"
let margin_ratio = platform "margin_ratio"
let accumulated_balance = platform "accumulated_balance"
let spot_base_total = platform "spot_base_total"
let usdc_free = platform "usdc_free"
let carry_reserved = platform "carry_reserved"
let min_notional = platform "min_notional"
let min_liq_reserve = platform "min_liq_reserve"

(* Carry overlay actual-fill facts. Published from the durable carry state, never derived
   from the trigger mark. *)
let carry_close_price = platform "carry_close_price"
let carry_close_qty = platform "carry_close_qty"
let carry_close_kind = platform "carry_close_kind"
let carry_close_is_recycle = platform "carry_close_is_recycle"
let carry_close_realized = platform "carry_close_realized"
let carry_close_realized_total = platform "carry_close_realized_total"
let carry_close_seq = platform "carry_close_seq"
let carry_recycle_realized_total = platform "carry_recycle_realized_total"
let carry_recycle_seq = platform "carry_recycle_seq"
let carry_spot_price = platform "carry_spot_price"
let carry_spot_qty = platform "carry_spot_qty"
let carry_spot_seq = platform "carry_spot_seq"

(* Latest carry perp entry fill, published by the domain loop on the fill event so the
   take-profit can be sized before the next position snapshot. [carry_entry_fill_at] is
   the freshness gate; a perp buy (the close) clears all three. *)
let carry_entry_fill_price = platform "carry_entry_fill_price"
let carry_entry_fill_qty = platform "carry_entry_fill_qty"
let carry_entry_fill_at = platform "carry_entry_fill_at"

(* Engine-control flags and branch facts published by the handler. *)
let engine_continue = platform "engine:continue"
let engine_buy_active = platform "engine:buy_active"
let amend_has_sell = platform "amend_has_sell"
let sell_place_should = platform "sell_place_should"
