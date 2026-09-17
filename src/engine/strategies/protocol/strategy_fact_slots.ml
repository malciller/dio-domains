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

(* Engine-control flags and branch facts published by the handler. *)
let engine_continue = platform "engine:continue"
let engine_buy_active = platform "engine:buy_active"
let amend_has_sell = platform "amend_has_sell"
let sell_place_should = platform "sell_place_should"
