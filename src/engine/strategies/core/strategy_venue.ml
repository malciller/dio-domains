(* Venue configuration, precision helpers and grid pricing (strategy-agnostic). *)

open Strategy_state

(* Kraken matches HL: 1:1 sells, retention skimmed into reserved_base on the buy fill. *)
let kraken_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_uses_mult = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; enforce_venue_min_qty = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = false
  ; evict_unlisted_sells = false
  ; asset_low_requires_balance_change = true
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

let hyperliquid_config =
  { time_in_force = "Alo"
  ; track_pending_sells = false
  ; use_accumulation_sells = true
  ; sell_uses_mult = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; enforce_venue_min_qty = false
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = true
  ; evict_unlisted_sells = true
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = false
  ; remaintain_expired_sells = false
  }
;;

let ibkr_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_uses_mult = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; enforce_venue_min_qty = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = false
  ; hold_netted_from_venue_state = false
  ; evict_unlisted_sells = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = false
  }
;;

let lighter_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = true
  ; sell_uses_mult = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; enforce_venue_min_qty = true
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = false
  ; hold_netted_from_venue_state = false
  ; evict_unlisted_sells = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = false
  ; remaintain_expired_sells = false
  }
;;

let alpaca_config =
  { time_in_force = "GTC"
  ; track_pending_sells = true
  ; use_accumulation_sells = false
  ; sell_uses_mult = false
  ; sell_failure_sets_asset_low = true
  ; use_reserved_base_guard = true
  ; enforce_venue_min_qty = false
  ; use_unnetted_sell_hold = true
  ; balance_nets_open_order_holds = true
  ; hold_netted_from_venue_state = true
  ; evict_unlisted_sells = false
  ; asset_low_requires_balance_change = false
  ; merge_preserved_sells = true
  ; check_stale_balance = true
  ; remaintain_expired_sells = true
  }
;;

let get_exchange_config exchange =
  match Exchange.Types.exchange_of_string exchange with
  | Hyperliquid -> hyperliquid_config
  | Lighter -> lighter_config
  | Ibkr -> ibkr_config
  | Alpaca -> alpaca_config
  | Kraken | Custom _ -> kraken_config
;;

let[@inline always] hl_like_spot_fee_exchange id =
  match Exchange.Types.exchange_of_string id with
  | Hyperliquid | Lighter -> true
  | Kraken | Ibkr | Alpaca | Custom _ -> false
;;

let ibkr_commission ~qty ~price =
  let per_share_rate = 0.0035 in
  let raw = qty *. per_share_rate in
  let min_fee = 0.35 in
  let max_fee = 0.01 *. qty *. price in
  Float.max min_fee (Float.min raw max_fee)
;;

(** Quote fee paid on the buy leg for [qty] at [price]. Mirrors the sell-fill fee split:
    IBKR pays its per-order commission, HL-like spot charges the fee in base (zero quote;
    the base haircut already covers it), everything else pays [maker_fee] on the notional. *)
let buy_leg_fee ~exchange ~maker_fee ~qty ~price =
  if qty <= 0.0 || price <= 0.0
  then 0.0
  else if String.equal exchange "ibkr"
  then ibkr_commission ~qty ~price
  else if hl_like_spot_fee_exchange exchange
  then 0.0
  else price *. qty *. maker_fee
;;

(** Fee-rate resolution: an explicit config value wins, then the live [Fee_cache] entry
    (populated by the supervisor's venue fetch), then the caller's last known value. It
    never substitutes a literal 0.0, so a transient cache miss cannot silently make fills
    free. *)
let resolve_fee_rate ~configured ~cached ~current =
  match configured with
  | Some f when Float.is_finite f -> f
  | _ ->
    (match cached with
     | Some f when Float.is_finite f -> f
     | _ -> current)
;;

(* Venue dispatch memo tables. Both were plain Hashtbls read and written with no lock from
   every trading Domain - [get_exchange_module] alone is called twice per strategy cycle
   (venue_available_base and venue_gross_base), so a resize racing a lookup was reachable
   on the hot path. Now Cow_tables: lock-free reads, and a miss copies one shard rather
   than the whole map. Keyspace is the four registered venues, so the bounds are fixed. *)
let _exchange_module_cache : (string, (module Exchange.S)) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:4 ()
;;

let get_exchange_module exchange =
  match Ds.Cow_table.find_opt _exchange_module_cache exchange with
  | Some m -> Some m
  | None ->
    (match Exchange.Registry.get exchange with
     | Some m ->
       Ds.Cow_table.set _exchange_module_cache exchange m;
       Some m
     | None -> None)
;;

let _round_price_fn_cache : (string, float -> float) Ds.Cow_table.t =
  Ds.Cow_table.create ~shard_count:8 ()
;;

let get_round_price_fn symbol exchange =
  let key = symbol ^ "|" ^ exchange in
  match Ds.Cow_table.find_opt _round_price_fn_cache key with
  | Some f -> f
  | None ->
    let f =
      match get_exchange_module exchange with
      | Some (module Ex : Exchange.S) -> fun p -> Ex.round_price ~symbol ~price:p
      | None -> Float.round
    in
    Ds.Cow_table.set _round_price_fn_cache key f;
    f
;;

let get_price_increment symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) ->
    (match Ex.get_price_increment ~symbol with
     | Some inc -> inc
     | None ->
       Logging.warn_f
         ~section
         "No price increment info for %s/%s (venue metadata missing); using default 0.01"
         exchange
         symbol;
       0.01)
  | None ->
    Logging.warn_f
      ~section
      "No price increment info for %s/%s (exchange module not registered); using default \
       0.01"
      exchange
      symbol;
    0.01
;;

let get_qty_increment_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) ->
    (match Ex.get_qty_increment ~symbol with
     | Some inc -> inc
     | None ->
       Logging.warn_f
         ~section
         "No qty increment info for %s/%s (venue metadata missing); using default 0.01"
         exchange
         symbol;
       0.01)
  | None ->
    Logging.warn_f
      ~section
      "No qty increment info for %s/%s (exchange module not registered); using default \
       0.01"
      exchange
      symbol;
    0.01
;;

(** Floor [value] down to the lot whose reciprocal step is [inv] (i.e. a step of
    [1/.inv]). A relative epsilon is added before the floor so binary representation error
    cannot drop an on-lot quantity a full step: [0.0003 *. 1e5 = 29.999999999999996]
    floors to [0.00029] without it, but to [0.0003] with it. *)
let floor_to_inv value inv =
  if inv <= 0.0 || not (Float.is_finite inv)
  then value
  else (
    let scaled = value *. inv in
    floor (scaled +. (1e-9 *. Float.max 1.0 (Float.abs scaled))) /. inv)
;;

(** Ceil [value] up to the lot whose reciprocal step is [inv] (i.e. a step of [1/.inv]).
    The same relative epsilon is used as [floor_to_inv], subtracted before the ceiling, so
    binary representation error cannot bump an on-lot quantity a full step. *)
let ceil_to_inv value inv =
  if inv <= 0.0 || not (Float.is_finite inv)
  then value
  else (
    let scaled = value *. inv in
    ceil (scaled -. (1e-9 *. Float.max 1.0 (Float.abs scaled))) /. inv)
;;

let round_qty qty symbol exchange =
  floor_to_inv qty (1.0 /. get_qty_increment_val symbol exchange)
;;

(** Ceil [qty] up to the venue lot for [symbol]. Overlay orders use this before the
    notional floor check: Hyperliquid rounds size down to the lot, so a quantity sized at
    exactly the $10 floor can land below it after rounding. SOL is the sharp case (spot
    lot 0.001, perp lot 0.01): $10 / 108 = 0.0925 SOL rounds down to 0.09 = $9.73, which
    the venue rejects. Rounding the requested size up keeps the placed notional at or
    above the floor. *)
let round_qty_up qty symbol exchange =
  ceil_to_inv qty (1.0 /. get_qty_increment_val symbol exchange)
;;

(** Minimum accepted order QUANTITY for [symbol] in base-asset units, from the live venue
    module (e.g. 0.0005 BTC on Hyperliquid spot; 0.0 = unknown). Venue floor every order
    must clear; independent of the grid's [qty]. *)
let get_qty_min_val symbol exchange =
  match get_exchange_module exchange with
  | Some (module Ex : Exchange.S) -> Option.value (Ex.get_qty_min ~symbol) ~default:0.0
  | None -> 0.0
;;

(** Default minimum order notional for [symbol] in quote terms (0.0 = unconstrained), from
    the venue's oracle adapter ([Exchange_intf.Oracle.S.min_notional]): Hyperliquid 10
    USDC floor for both spot and perps, Alpaca $1 fractional minimum, others 0.0.
    Unregistered venues are unconstrained. Same resolution as the oracle's [Grid_adapter],
    so the live grid and replay agree on the floor. *)
let get_min_notional_val symbol exchange =
  match Exchange.Oracle.Registry.get exchange with
  | Some (module V) -> V.min_notional ~symbol
  | None -> 0.0
;;

(** Quote-currency amount added to the venue minimum by a configured [qty] of the form
    ["n + min"] or ["min + n"] with [n] > 0, e.g. ["1 + min"] is the minimum plus 1 USDC
    of notional at the order's price. [None] when [qty] is not that form. A malformed
    amount warns and falls back to 0.0 (the bare minimum) rather than a size that cannot
    fill. *)
let qty_min_plus_quote (qty : string) : float option =
  let s = String.lowercase_ascii (String.trim qty) in
  let n = String.length s in
  let k = 3 in
  let parse_amount amount =
    match float_of_string_opt (String.trim amount) with
    | Some q when Float.is_finite q && q > 0.0 -> Some q
    | _ ->
      Logging.warn_f
        ~section
        "Invalid qty '%s' (want 'n + min' or 'min + n' with n > 0); using the venue \
         minimum"
        qty;
      Some 0.0
  in
  if n <= k
  then None
  else if String.equal (String.sub s (n - k) k) "min"
  then (
    let prefix = String.trim (String.sub s 0 (n - k)) in
    let p = String.length prefix in
    if p = 0 || not (Char.equal prefix.[p - 1] '+')
    then None
    else parse_amount (String.sub prefix 0 (p - 1)))
  else if String.equal (String.sub s 0 k) "min"
  then (
    let rest = String.trim (String.sub s k (n - k)) in
    let p = String.length rest in
    if p = 0 || not (Char.equal rest.[0] '+')
    then None
    else parse_amount (String.sub rest 1 (p - 1)))
  else None
;;

(** Multiplier over the venue minimum named by a configured [qty], or [None] when [qty] is
    a plain number. ["min"] is 1.0; ["2.5(min)"] is 2.5. A malformed multiplier warns and
    falls back to 1.0 (the minimum, still placeable) rather than a size that cannot fill. *)
let qty_min_multiplier (qty : string) : float option =
  let s = String.lowercase_ascii (String.trim qty) in
  if String.equal s "min"
  then Some 1.0
  else (
    let suffix = "(min)" in
    let n = String.length s in
    let k = String.length suffix in
    if n > k && String.equal (String.sub s (n - k) k) suffix
    then (
      let prefix = String.trim (String.sub s 0 (n - k)) in
      match float_of_string_opt prefix with
      | Some m when Float.is_finite m && m >= 1.0 -> Some m
      | _ ->
        Logging.warn_f
          ~section
          "Invalid qty multiplier '%s' (want n(min) with n >= 1); using the venue minimum"
          qty;
        Some 1.0)
    else None)
;;

(** Minimum placeable base quantity from resolved venue metadata: the base-quantity floor
    or the quote-notional floor at [price], whichever is larger. Venues round size DOWN to
    the lot, so the notional leg is ceiled (Hyperliquid SOL: 0.0925 -> 0.09). Pure, so the
    hot path passes cached metadata. *)
let min_order_base ~qty_min ~qty_increment ~min_notional ~price =
  if (not (price > 0.0)) || not (Float.is_finite price)
  then qty_min
  else if min_notional > 0.0 && qty_increment > 0.0
  then Float.max qty_min (ceil_to_inv (min_notional /. price) (1.0 /. qty_increment))
  else qty_min
;;

(** [mult] times the minimum placeable base quantity for [symbol] on [exchange] at
    [price], resolved from the live venue modules. The product is ceiled to the lot, which
    the venue would otherwise round back down below the requested multiple.
    Price-dependent, so it is resolved at spawn and on each oracle publish, never per
    market tick. *)
let venue_min_qty ~mult ~exchange ~symbol ~price =
  let increment = get_qty_increment_val symbol exchange in
  let base =
    min_order_base
      ~qty_min:(get_qty_min_val symbol exchange)
      ~qty_increment:increment
      ~min_notional:(get_min_notional_val symbol exchange)
      ~price
  in
  if mult <= 1.0 then base else ceil_to_inv (mult *. base) (1.0 /. increment)
;;

(** [base] plus [extra_quote] worth of the quote currency at [price], in base units and
    ceiled to the lot. The minimum itself already clears its own floor, so only the added
    notional can fall between lots. *)
let add_quote_base ~extra_quote ~base ~qty_increment ~price =
  if extra_quote <= 0.0 || (not (price > 0.0)) || not (Float.is_finite price)
  then base
  else ceil_to_inv (base +. (extra_quote /. price)) (1.0 /. qty_increment)
;;

(** The venue minimum placeable base quantity for [symbol] plus [extra_quote] of quote at
    [price] (["n + min"] / ["min + n"]). Price-dependent, so resolved at spawn and on each
    oracle publish. *)
let venue_min_qty_plus_quote ~extra_quote ~exchange ~symbol ~price =
  let increment = get_qty_increment_val symbol exchange in
  let base =
    min_order_base
      ~qty_min:(get_qty_min_val symbol exchange)
      ~qty_increment:increment
      ~min_notional:(get_min_notional_val symbol exchange)
      ~price
  in
  add_quote_base ~extra_quote ~base ~qty_increment:increment ~price
;;

(** Venue tradeable figure for [symbol]'s quote currency, or [nan]. Diagnostic: the grid
    uses the domain figure (already net of overlay reservations); the raw figure lets
    capital-low logs show the overlay claim. *)
let venue_quote_balance ~exchange ~symbol =
  match String.index_opt symbol '/' with
  | None -> nan
  | Some i ->
    let quote = String.sub symbol (i + 1) (String.length symbol - i - 1) in
    (match get_exchange_module exchange with
     | Some (module Ex : Exchange.S) ->
       (try Ex.get_tradeable_balance ~asset:quote with
        | _ -> nan)
     | None -> nan)
;;

let venue_lot_qty grid_qty exchange state =
  match exchange with
  | "ibkr" ->
    if grid_qty <= 0.0
    then 0.0
    else (
      let q = floor_to_inv grid_qty (1.0 /. state.cached_qty_increment) in
      if q > 0.0 then q else state.cached_venue_min_qty)
  | "lighter" ->
    if grid_qty <= 0.0
    then 0.0
    else (
      let q = floor_to_inv grid_qty (1.0 /. state.cached_qty_increment) in
      if q > 0.0 then q else state.cached_venue_min_qty)
  | _ -> grid_qty
;;

(** Base quantity for a new order resting at [price]: the grid's configured size, floored
    up to [n] times the venue minimum when [strategy.qty] is ["n(min)"], or to the venue
    minimum plus [n] quote when it is ["n + min"] or ["min + n"]. Evaluated at the order's
    own price, so a move between oracle passes cannot place a size the venue rejects. *)
let order_qty ~price ~exchange state =
  let qty = venue_lot_qty state.grid_qty exchange state in
  let mult = state.qty_min_mult in
  let extra_quote = state.qty_min_plus_quote in
  if mult <= 0.0 && extra_quote <= 0.0
  then qty
  else (
    let base =
      min_order_base
        ~qty_min:state.cached_venue_min_qty
        ~qty_increment:state.cached_qty_increment
        ~min_notional:state.cached_venue_min_notional
        ~price
    in
    let floor =
      if extra_quote > 0.0
      then
        add_quote_base ~extra_quote ~base ~qty_increment:state.cached_qty_increment ~price
      else if mult > 1.0
      then ceil_to_inv (mult *. base) (1.0 /. state.cached_qty_increment)
      else base
    in
    Float.max qty floor)
;;

(** Venues whose order feed does not echo the strategy userref. On these, a resting
    order's identity must come from its venue-side flags (market, side, reduce-only). *)
let drops_userref ~exchange =
  String.equal (String.lowercase_ascii (String.trim exchange)) "lighter"
;;

let parse_config_float config value_name default exchange symbol =
  try float_of_string config with
  | Failure _ ->
    Logging.warn_f
      ~section
      "Invalid %s value '%s' for %s/%s, using default %.4f"
      value_name
      config
      exchange
      symbol
      default;
    default
;;

(** Resolve a configured [qty] string to base units. ["min"] and ["n(min)"] ask for the
    venue's minimum placeable size times [n]; ["n + min"] and ["min + n"] ask for the
    minimum plus [n] of quote currency at [price]; anything else is the configured number.
    An unparsable value resolves to 0.0, which withholds buying rather than trading a
    guessed size. *)
let resolve_config_qty (qty : string) ~exchange ~symbol ~price =
  match qty_min_multiplier qty with
  | Some mult -> venue_min_qty ~mult ~exchange ~symbol ~price
  | None ->
    (match qty_min_plus_quote qty with
     | Some extra_quote -> venue_min_qty_plus_quote ~extra_quote ~exchange ~symbol ~price
     | None -> parse_config_float qty "qty" 0.0 exchange symbol)
;;

let get_min_move_threshold price_increment =
  (* Exchange minimum price move: one tick ([price_increment], e.g. $0.01 on
     Alpaca/Hyperliquid). An amendment one tick away is a valid resting price, so the
     trailing leg re-anchors on every valid price step and the buy tracks price action
     without a deadband. *)
  price_increment
;;

(** Pure grid price: [current] moved by [grid_interval_pct] percent (up when [is_above]),
    snapped by [round_price]. Shared by the grid and the config-driven interpreter. *)
let grid_price ~round_price ~current ~grid_interval_pct ~is_above =
  let interval = current *. (grid_interval_pct /. 100.0) in
  round_price (if is_above then current +. interval else current -. interval)
;;

let calculate_grid_price current_price grid_interval_pct is_above state =
  grid_price
    ~round_price:state.cached_round_price
    ~current:current_price
    ~grid_interval_pct
    ~is_above
;;
