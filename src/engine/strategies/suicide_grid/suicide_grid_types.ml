(* Suicide Grid - Core Types & State Registry *)

let section = "suicide_grid"

open Strategy_common
module Exchange = Dio_exchange.Exchange_intf

(** Returns the first [n] elements of a list. Tail-recursive. *)
let take n lst =
  let rec aux acc n = function
    | [] -> List.rev acc
    | _ when n <= 0 -> List.rev acc
    | x :: xs -> aux (x :: acc) (n - 1) xs
  in
  aux [] n lst
;;

(** Case-sensitive substring search. Returns true if [s] contains [fragment]. *)
let contains_fragment s fragment =
  let sl = String.length s
  and fl = String.length fragment in
  let rec loop i = i + fl <= sl && (String.sub s i fl = fragment || loop (i + 1)) in
  loop 0
;;

(** Exchange-specific behavioral configuration. *)
type exchange_config =
  { time_in_force : string (** TIF value for limit orders ("Alo" or "GTC") *)
  ; track_pending_sells : bool (** Add sell orders to pending_orders tracking *)
  ; use_accumulation_sells : bool (** Enable profit-gated accumulation sell path *)
  ; sell_uses_mult : bool (** true: sell qty = qty * sell_mult; false: 1:1 sells *)
  ; sell_failure_sets_asset_low : bool
    (** Set asset_low on sell insufficient-balance rejection *)
  ; use_reserved_base_guard : bool
    (** Check reserved_base + locked_in_sells before selling *)
  ; asset_low_requires_balance_change : bool
    (** true: clear asset_low only on balance increase *)
  ; merge_preserved_sells : bool
    (** Merge recently_injected_sells into open_sell_orders *)
  ; check_stale_balance : bool
    (** Block strategy execution when balance data is missing *)
  ; remaintain_expired_sells : bool
    (** Re-submit missing sell grid levels (Alpaca GTC maintenance) *)
  }

(** Per-asset trading configuration. *)
type trading_config =
  { exchange : string
  ; symbol : string
  ; qty : string
  ; grid_interval : float
  ; sell_mult : string
  ; strategy : string
  ; maker_fee : float option
  ; taker_fee : float option
  ; accumulation_buffer : float
  }

(** Mutable per-symbol strategy state. *)
type strategy_state =
  { mutable last_buy_order_price : float option
  ; mutable last_buy_order_id : string option
  ; mutable open_sell_orders : (string * float * float) list (* (order_id, price, qty) *)
  ; mutable persisted_sell_levels : (float * float) list
    (* (target_price, qty) stack for Alpaca GTC *)
  ; mutable recently_injected_sells : (string * float * float) list
    (* (order_id, price, timestamp) *)
  ; mutable pending_orders : (string * order_side * float * float) list
    (* (order_id, side, price, timestamp) *)
  ; mutable last_cycle : int
  ; mutable last_order_time : float (* Unix timestamp of most recent order submission *)
  ; mutable inflight_cancel_buy : bool
    (* true while buy cancel is pending confirmation via order channel *)
  ; mutable inflight_amend_buy : bool
    (* true while buy amend is in-flight; gates duplicate-buy cancel during inject/remove window *)
  ; mutable amend_cooldowns : (string, float) Hashtbl.t
    (* order_id -> expiry Unix timestamp *)
  ; mutable last_cleanup_time : float
  ; mutable inflight_buy : bool (* true while buy Place is pending ack or reject *)
  ; mutable inflight_sell : bool (* true while sell Place is pending ack or reject *)
  ; mutable evicted_orders : (string, float) Hashtbl.t
    (* order_id -> expiry_ts; blocks rebuilt orders *)
  ; mutable asset_low : bool
    (* set when asset balance is insufficient for next sell; pauses sell and buy *)
  ; mutable capital_low : bool
    (* set when quote balance is insufficient for next buy; pauses strategy *)
  ; mutable capital_low_logged : bool (* suppresses repeated capital-low log warnings *)
  ; mutable capital_low_at_balance : float
    (* quote_bal snapshot when capital_low was set; clears only on balance increase; -1.0 = unstamped *)
  ; mutable last_buy_attempted_insufficient : bool
    (* true for the one cycle where a buy was placed despite a KNOWN local
       balance shortfall (balance snapshot stale): the resulting exchange
       rejection is foreordained, so it must not latch capital_low again -
       the fresh balance on the next store update governs. Cleared when a buy
       is placed against sufficient balance or the order acks/fills. *)
  ; mutable resuming_after_balance_flag : bool
    (* true for one cycle after asset_low/capital_low clears; re-gates new sells on accumulation_buffer *)
  ; mutable just_filled_buy : bool
    (* true when a buy order has filled; bypasses recovery gate on sell placement *)
  ; mutable force_buy_reanchor : bool
    (* true when the sizing source (capital oracle) published a changed
       qty/grid_interval: the buy-trailing leg then re-anchors the resting
       buy to the new sizing target even when that means amending it DOWN
       (normal trailing only moves the buy up, so a widening grid interval
       would otherwise leave the book running at the old, tighter spacing).
       Set by the domain worker on sizing change, cleared by the strategy
       once the buy sits at the target. *)
  ; mutable reserved_quote : float
    (* quote amount reserved by current open buy for this symbol *)
  ; mutable accumulated_profit : float
    (* realized PnL from buy/sell cycles; gates accumulation sell placement *)
  ; mutable reserved_base : float
    (* base asset accumulated via sell_mult; excluded from sellable balance *)
  ; mutable last_buy_fill_price : float option
    (* fill price of most recent buy; cost basis for sell profit calc *)
  ; mutable last_sell_fill_price : float option
    (* fill price of most recent sell; cost basis for consecutive sells *)
  ; mutable last_buy_fill_qty : float option
    (* filled qty of most recent buy; sizes follow-up sells *)
  ; mutable last_sell_fill_qty : float option
    (* filled qty of most recent sell; sizes consecutive sells *)
  ; mutable grid_qty : float (* cached config qty; used by fill handler for profit calc *)
  ; mutable cached_sell_mult : float
    (* cached parsed sell_mult; avoids float_of_string per cycle *)
  ; mutable cached_ecfg : exchange_config
    (* cached exchange_config; avoids string comparison per cycle *)
  ; mutable maker_fee : float
    (* cached maker fee rate; used by fill handler for profit calc *)
  ; mutable exchange_id : string (* cached exchange name; used for persistence routing *)
  ; mutable startup_replay : bool
    (* true during startup fill replay; suppresses profit calculation *)
  ; mutable last_fill_oid : string option
    (* OID of last profit-credited fill; replay resumption point *)
  ; mutable highest_startup_oid : string option
    (* highest fill OID observed during startup; bootstraps new strategies *)
  ; mutable anticipated_base_credit : float
    (* base qty from buy fills not yet reflected in balance feed *)
  ; mutable last_seen_asset_balance : float
    (* previous asset_bal value; used to detect balance feed updates *)
  ; mutable persistence_dirty : bool
    (* true when accumulation state changed; flushed by caller outside hotloop *)
  ; mutable last_cycle_orders_hash : int (* tracks exchange state for 0-alloc diffing *)
  ; mutable last_cycle_buy_count : int
  ; mutable duplicate_key_buy : string
  ; mutable duplicate_key_sell : string
  ; mutable cached_round_price : float -> float
  ; mutable cached_price_increment : float
  ; mutable cached_qty_increment : float
  ; mutable cached_qty_min : float
  ; mutable exchange_reserved_atomic : float Atomic.t option
  ; processed_fills : (string, unit) Hashtbl.t
  ; processed_fills_queue : string Queue.t
  ; mutex : Mutex.t (* per-symbol mutex; prevents concurrent strategy execution *)
  }

(** Default exchange configs referenced during state creation before full config load *)
let default_kraken_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = false
  ; sell_uses_mult = true
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; asset_low_requires_balance_change = true
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

(** Global registry of per-symbol strategy states. *)
let strategy_states = Atomic.make Strategy_common.StringMap.empty

(** Retrieves or lazily initializes the strategy state for [asset_symbol]. *)
let rec get_strategy_state asset_symbol =
  let map = Atomic.get strategy_states in
  match Strategy_common.StringMap.find_opt asset_symbol map with
  | Some state -> state
  | None ->
    let persisted_reserved_base =
      Dio_persistence.State_persistence.load_reserved_base ~symbol:asset_symbol
    in
    let persisted_accumulated_profit =
      Dio_persistence.State_persistence.load_accumulated_profit ~symbol:asset_symbol
    in
    let persisted_last_fill_oid =
      Dio_persistence.State_persistence.load_last_fill_oid ~symbol:asset_symbol
    in
    let persisted_last_buy_fill_price =
      Dio_persistence.State_persistence.load_last_buy_fill_price ~symbol:asset_symbol
    in
    let persisted_last_sell_fill_price =
      Dio_persistence.State_persistence.load_last_sell_fill_price ~symbol:asset_symbol
    in
    let persisted_last_buy_fill_qty =
      Dio_persistence.State_persistence.load_last_buy_fill_qty ~symbol:asset_symbol
    in
    let persisted_last_sell_fill_qty =
      Dio_persistence.State_persistence.load_last_sell_fill_qty ~symbol:asset_symbol
    in
    let persisted_sell_levels =
      Dio_persistence.State_persistence.load_persisted_sell_levels ~symbol:asset_symbol
    in
    let new_state =
      { last_buy_order_price = None
      ; last_buy_order_id = None
      ; open_sell_orders = []
      ; persisted_sell_levels
      ; recently_injected_sells = []
      ; pending_orders = []
      ; last_cycle = 0
      ; last_order_time = 0.0
      ; inflight_cancel_buy = false
      ; inflight_amend_buy = false
      ; amend_cooldowns = Hashtbl.create 16
      ; last_cleanup_time = 0.0
      ; inflight_buy = false
      ; inflight_sell = false
      ; evicted_orders = Hashtbl.create 16
      ; asset_low = false
      ; capital_low = false
      ; capital_low_logged = false
      ; capital_low_at_balance = 0.0
      ; last_buy_attempted_insufficient = false
      ; resuming_after_balance_flag = false
      ; just_filled_buy = false
      ; force_buy_reanchor = false
      ; reserved_quote = 0.0
      ; accumulated_profit = persisted_accumulated_profit
      ; reserved_base = persisted_reserved_base
      ; last_buy_fill_price = persisted_last_buy_fill_price
      ; last_sell_fill_price = persisted_last_sell_fill_price
      ; last_buy_fill_qty = persisted_last_buy_fill_qty
      ; last_sell_fill_qty = persisted_last_sell_fill_qty
      ; grid_qty = 0.0
      ; cached_sell_mult = 1.0
      ; cached_ecfg = default_kraken_config
      ; maker_fee = 0.0
      ; exchange_id = ""
      ; startup_replay = true
      ; last_fill_oid = persisted_last_fill_oid
      ; highest_startup_oid = None
      ; anticipated_base_credit = 0.0
      ; last_seen_asset_balance = 0.0
      ; persistence_dirty = false
      ; last_cycle_orders_hash = 0
      ; last_cycle_buy_count = 0
      ; duplicate_key_buy = Printf.sprintf "%s|buy|grid" asset_symbol
      ; duplicate_key_sell = Printf.sprintf "%s|sell|grid" asset_symbol
      ; cached_round_price = Float.round
      ; cached_price_increment = 0.01
      ; cached_qty_increment = 0.01
      ; cached_qty_min = 1.0
      ; exchange_reserved_atomic = None
      ; processed_fills = Hashtbl.create 1024
      ; processed_fills_queue = Queue.create ()
      ; mutex = Mutex.create ()
      }
    in
    if
      Atomic.compare_and_set
        strategy_states
        map
        (Strategy_common.StringMap.add asset_symbol new_state map)
    then new_state
    else get_strategy_state asset_symbol
;;
