(* Grid_core - pure, survival-grade DIO Grid buy/sell state machine.

   Mirrors the live grid (Suicide_grid_execution):
   - buy level      = ref * (1 - gi/100), sell level = last_buy_fill * (1 + gi/100)
   - trailing buy   = min(ref*(1-gi/100), sell - 2*gi/100*ref)   [exact_target rule]
   - buy gate       = quote >= q * price * (1 + fee)
   - sell qty per venue: Kraken qty*sell_mult; HL/Lighter/IBKR accumulation
     sells once accumulated_profit >= rounding_diff*sell_price + buffer;
     Alpaca 1:1.
   - at most one sell per bar; buys may ladder down as far as the bar low and
     quote allow (worst-case intraday fills).
   - capital_low (quote can't fund the next buy) pauses buying and clears when
     quote recovers; the FIRST occurrence is the survival event.

   Known simplifications (documented in the plan):
   - ref price = the triggering fill price (no live bid/ask at bar resolution),
     so the ladder is exactly geometric: B_{n+1} = round(B_n * (1 - gi/100)).
   - the daily model fills one ladder step per crossing; multi-step same-bar
     fills are modeled conservatively (buy loop), never more than quote allows.
   - capital_low clears on quote recovery (live requires a balance increase);
     the recovery gate on sells is approximated by the profit/buffer check. *)

open Grid_core_types

type config =
  { qty : float
  ; sell_mult : float
  ; grid_interval_pct : float
  ; maker_fee : float
  ; accumulation_buffer : float
  ; price_increment : float
  ; qty_increment : float
  ; exchange_model : exchange_model
  ; start_price : float
  ; start_quote : float
  }

type state =
  { mutable quote : float
  ; mutable base : float
  ; mutable accumulated_profit : float
  ; mutable reserved_base : float
  ; mutable resting_buy : float option
  ; mutable resting_sell : float option
  ; mutable last_buy_fill_price : float option
  ; mutable capital_low : bool
  ; mutable ever_capital_low : bool
  ; mutable first_capital_low_buy : float option
  ; mutable buy_fills : int
  ; mutable sell_fills : int
  }

type result =
  { fills : fill list
  ; quote_by_session : float array
  ; min_quote : float
  ; min_quote_drawdown : float
  ; first_capital_low_drawdown : float option
  ; first_capital_low_session : int option
  ; exhausted : bool
  ; final_quote : float
  ; final_base : float
  ; buy_fills : int
  ; sell_fills : int
  }

let exchange_model_of_string = function
  | "kraken" | "Kraken" -> Kraken
  | "hyperliquid" | "Hyperliquid" | "hl" -> Hyperliquid
  | "lighter" | "Lighter" -> Lighter
  | "ibkr" | "Ibkr" | "IBKR" -> Ibkr
  | "alpaca" | "Alpaca" -> Alpaca
  | s -> invalid_arg ("Grid_core.exchange_model_of_string: " ^ s)
;;

(* Per-venue flags mirroring Suicide_grid_config.{kraken,hyperliquid,ibkr,
   lighter,alpaca}_config. Order: use_accumulation_sells, sell_uses_mult. *)
let venue_flags : exchange_model -> bool * bool = function
  | Kraken -> false, true
  | Hyperliquid -> true, false
  | Lighter -> true, false
  | Ibkr -> true, false
  | Alpaca -> false, false
;;

let use_accumulation_sells cfg =
  let acc, _ = venue_flags cfg.exchange_model in
  acc
;;

let sell_uses_mult cfg =
  let _, m = venue_flags cfg.exchange_model in
  m
;;

(* Rounding: prices to nearest tick (matches exchange round_to_incr), lots
   floored (matches Suicide_grid_config.round_qty). *)
let round_price cfg p =
  let inv = 1.0 /. cfg.price_increment in
  Float.round (p *. inv) /. inv
;;

let round_lot cfg q =
  let inv = 1.0 /. cfg.qty_increment in
  Float.floor ((q *. inv) +. 1e-9) /. inv
;;

(* Level helpers. These must agree with Suicide_grid_config.calculate_grid_price
   when that function is used with an identity rounding state (contract test). *)
let buy_level cfg ~ref = round_price cfg (ref *. (1.0 -. (cfg.grid_interval_pct /. 100.0)))

let sell_level cfg ~ref =
  round_price cfg (ref *. (1.0 +. (cfg.grid_interval_pct /. 100.0)))
;;

(** Trailing buy target with a resting sell: the exact_target rule from
    Suicide_grid_execution (double_grid_interval = bid * 2*gi/100). *)
let trail_buy_level cfg ~bid ~sell =
  let grid_buy = buy_level cfg ~ref:bid in
  let exact =
    round_price cfg (sell -. (bid *. (2.0 *. cfg.grid_interval_pct /. 100.0)))
  in
  Float.min grid_buy exact
;;

(** Minimum price move required before trailing an amendment. Mirrors
    Suicide_grid_config.get_min_move_threshold. *)
let min_move_threshold cfg price =
  Float.max
    (cfg.price_increment *. 10.0)
    (price *. (cfg.grid_interval_pct *. 0.05 /. 100.0))
;;

let buy_fill_cost cfg price = price *. cfg.qty *. (1.0 +. cfg.maker_fee)
let can_place_buy cfg ~state ~price = state.quote >= buy_fill_cost cfg price

let create cfg =
  let b = buy_level cfg ~ref:cfg.start_price in
  { quote = cfg.start_quote
  ; base = 0.0
  ; accumulated_profit = 0.0
  ; reserved_base = 0.0
  ; resting_buy = Some b
  ; resting_sell = None
  ; last_buy_fill_price = None
  ; capital_low = false
  ; ever_capital_low = false
  ; first_capital_low_buy = None
  ; buy_fills = 0
  ; sell_fills = 0
  }
;;

(** Sell quantity per venue. Returns (qty, required_profit); required_profit > 0
    signals an accumulation (reduced) sell gated on accumulated_profit. *)
let compute_sell_qty cfg ~state ~sell_price =
  let accumulation rounded =
    let rounding_diff = cfg.qty -. rounded in
    let required = (rounding_diff *. sell_price) +. cfg.accumulation_buffer in
    if state.accumulated_profit >= required && rounded > 0.0
    then rounded, required
    else cfg.qty, 0.0
  in
  if sell_uses_mult cfg && not (use_accumulation_sells cfg)
  then
    (* Kraken: sell qty = qty * sell_mult, residual base retained. *)
    round_lot cfg (cfg.qty *. cfg.sell_mult), 0.0
  else if use_accumulation_sells cfg
  then (
    match cfg.exchange_model with
    | Ibkr ->
      (* Whole-share accumulation: sell qty-1 once profit clears the buffer. *)
      accumulation (Float.max 0.0 (cfg.qty -. 1.0))
    | _ ->
      (* Hyperliquid / Lighter. *)
      accumulation (round_lot cfg (cfg.qty *. cfg.sell_mult)))
  else
    (* Alpaca: 1:1. *)
    cfg.qty, 0.0
;;

(** Advances the state machine through one bar. Returns fills in chronological
    order for the bar. *)
let on_bar cfg ~state ~bar ~ordering =
  let fills = ref [] in
  let add f = fills := f :: !fills in
  (* Clear capital_low once quote recovers enough for the resting buy. *)
  if state.capital_low
  then (
    match state.resting_buy with
    | Some b when state.quote >= buy_fill_cost cfg b -> state.capital_low <- false
    | _ -> ());
  let on_buy_fill b =
    state.buy_fills <- state.buy_fills + 1;
    state.accumulated_profit <- state.accumulated_profit -. (b *. cfg.qty *. cfg.maker_fee);
    match cfg.exchange_model with
    | Alpaca ->
      let inc = cfg.qty -. (cfg.sell_mult *. cfg.qty) in
      if inc > 0.0 then state.reserved_base <- state.reserved_base +. inc
    | _ -> ()
  in
  let process_buy () =
    let rec loop () =
      match state.resting_buy with
      | None -> ()
      | Some b ->
        if bar.low <= b && b > 0.0
        then
          if can_place_buy cfg ~state ~price:b
          then (
            let cost = buy_fill_cost cfg b in
            state.quote <- state.quote -. cost;
            state.base <- state.base +. cfg.qty;
            state.last_buy_fill_price <- Some b;
            state.resting_buy <- None;
            let s = sell_level cfg ~ref:b in
            state.resting_sell <- Some s;
            let nb = trail_buy_level cfg ~bid:b ~sell:s in
            state.resting_buy <- Some nb;
            on_buy_fill b;
            add { side = `Buy; price = b; qty = cfg.qty; quote_delta = -.cost };
            (* Only continue the ladder while the level strictly descends,
               otherwise a degenerate bar (price -> 0) would loop forever. *)
            if nb < b then loop ())
          else (
            state.capital_low <- true;
            if not state.ever_capital_low
            then (
              state.ever_capital_low <- true;
              state.first_capital_low_buy <- Some b))
        else ()
    in
    loop ()
  in
  let process_sell () =
    match state.resting_sell with
    | Some s when bar.high >= s ->
      let q_s, required = compute_sell_qty cfg ~state ~sell_price:s in
      let available = Float.max 0.0 (state.base -. state.reserved_base) in
      let q_s = Float.min q_s available in
      if q_s > 0.0
      then (
        state.resting_sell <- None;
        let gross = s *. q_s in
        let fee = gross *. cfg.maker_fee in
        let proceeds = gross -. fee in
        state.quote <- state.quote +. proceeds;
        state.base <- Float.max 0.0 (state.base -. q_s);
        state.sell_fills <- state.sell_fills + 1;
        if required > 0.0
        then (
          state.accumulated_profit <- state.accumulated_profit -. required;
          let inc = cfg.qty -. q_s in
          if inc > 0.0 then state.reserved_base <- state.reserved_base +. inc)
        else (
          match state.last_buy_fill_price with
          | Some bp ->
            let profit = ((s -. bp) *. q_s) -. fee in
            if profit > 0.0
            then state.accumulated_profit <- state.accumulated_profit +. profit
          | None -> ());
        add { side = `Sell; price = s; qty = q_s; quote_delta = proceeds })
    | _ -> ()
  in
  (match ordering with
   | Sell_first ->
     process_sell ();
     process_buy ()
   | Buy_first ->
     process_buy ();
     process_sell ());
  List.rev !fills
;;

(** Replays the grid over a bar series. `min_quote` is tracked after every fill
    (intra-bar trough), so ordering is observable. *)
let replay cfg ~bars ~ordering =
  let state = create cfg in
  let n = Array.length bars in
  let quote_series = Array.make n 0.0 in
  let fills = ref [] in
  let min_quote = ref cfg.start_quote in
  let first_cl_session = ref None in
  Array.iteri
    (fun i bar ->
       let start_q = state.quote in
       let fs = on_bar cfg ~state ~bar ~ordering in
       fills := List.rev_append fs !fills;
       let q = ref start_q in
       List.iter
         (fun f ->
            q := !q +. f.quote_delta;
            min_quote := Float.min !min_quote !q)
         fs;
       quote_series.(i) <- state.quote;
       min_quote := Float.min !min_quote state.quote;
       if state.ever_capital_low && !first_cl_session = None
       then first_cl_session := Some i)
    bars;
  let first_cl_dd =
    Option.map (fun b -> 1.0 -. (b /. cfg.start_price)) state.first_capital_low_buy
  in
  { fills = List.rev !fills
  ; quote_by_session = quote_series
  ; min_quote = !min_quote
  ; min_quote_drawdown = 1.0 -. (!min_quote /. cfg.start_quote)
  ; first_capital_low_drawdown = first_cl_dd
  ; first_capital_low_session = !first_cl_session
  ; exhausted = state.ever_capital_low
  ; final_quote = state.quote
  ; final_base = state.base
  ; buy_fills = state.buy_fills
  ; sell_fills = state.sell_fills
  }
;;
