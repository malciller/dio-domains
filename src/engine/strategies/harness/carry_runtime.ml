(** Carry-overlay durable runtime glue.

    Keeps the strategy's remembered fields in [carry_state.json] and classifies actual
    overlay fills for the profit recycle. This module is the only place that knows both
    sides (the strategy runtime and the persistence store); the domain loop calls it at
    boot, on every fill, and after every overlay cycle. *)

module Store = Dio_persistence.Carry_state_store
module Rt = Strategy_runtime
module Expr = Strategy_expr
module Common = Strategy_common

let section = "carry_runtime"

let base_coin_of_symbol symbol =
  match String.split_on_char '/' symbol with
  | b :: _ -> b
  | [] -> symbol
;;

let load ~exchange ~symbol = Store.load ~exchange ~base_coin:(base_coin_of_symbol symbol)

let getf rt key default =
  match Rt.get_state rt key with
  | Expr.V_float f -> f
  | _ -> default
;;

let getp rt key default =
  match Rt.get_platform rt key with
  | Expr.V_float f -> f
  | _ -> default
;;

let getb rt key default =
  match Rt.get_state rt key with
  | Expr.V_bool b -> b
  | _ -> default
;;

let geti rt key default =
  match Rt.get_state rt key with
  | Expr.V_int i -> i
  (* Ladder arithmetic (add_index + 1) evaluates through the float path, so an incremented
     counter is stored as a float. *)
  | Expr.V_float f -> int_of_float f
  | _ -> default
;;

let setf rt key v = Rt.set_state rt key (Expr.V_float v)
let setb rt key v = Rt.set_state rt key (Expr.V_bool v)
let seti rt key v = Rt.set_state rt key (Expr.V_int v)

(** Restore the remembered state before the first overlay cycle. *)
let seed rt (st : Store.t) =
  setf rt "entry" st.entry;
  setf rt "anchor" st.anchor;
  setf rt "ladder_ratio" st.ladder_ratio;
  seti rt "add_index" st.add_index;
  setf rt "next_add" st.next_add;
  setb rt "add_armed" st.add_armed;
  setb rt "risk_paused" st.risk_paused;
  setb rt "recycle_pending" st.recycle_pending;
  setb rt "pending_close" st.pending_close;
  setb rt "close_fill_seen" st.close_fill_seen;
  setf rt "pending_gain" st.pending_gain;
  setf rt "pending_base_quote" st.pending_base_quote;
  setf rt "pending_base" st.pending_base;
  setf rt "close_entry" st.close_entry;
  setf rt "close_qty" st.close_qty;
  setf rt "close_started_at" st.close_started_at;
  seti rt "close_seq_seen" st.close_seq_seen;
  setf rt "close_realized_seen" st.close_realized_seen;
  seti rt "recycle_seq_seen" st.recycle_seq_seen;
  setf rt "recycle_realized_seen" st.recycle_realized_seen;
  seti rt "spot_seq_seen" st.spot_seq_seen;
  setf rt "last_recycle_base" st.last_recycle_base
;;

(** Snapshot the remembered state. The ladder state is always persisted; [entry] is
    persisted while a short is open (the venue average is re-synced when flat). Fill
    fields and sequence cursors are owned by the runtime and carried through. *)
let snapshot rt (st : Store.t) ~now =
  let short_qty = getf rt "short_qty" 0.0 in
  let trailing = short_qty > 0.0001 in
  let next =
    { st with
      entry = (if trailing then getf rt "entry" st.entry else st.entry)
    ; anchor = getf rt "anchor" st.anchor
    ; ladder_ratio = getf rt "ladder_ratio" st.ladder_ratio
    ; add_index = geti rt "add_index" st.add_index
    ; next_add = getf rt "next_add" st.next_add
    ; add_armed = getb rt "add_armed" st.add_armed
    ; risk_paused = getb rt "risk_paused" st.risk_paused
    ; recycle_pending = getb rt "recycle_pending" st.recycle_pending
    ; pending_close = getb rt "pending_close" st.pending_close
    ; close_fill_seen = getb rt "close_fill_seen" st.close_fill_seen
    ; pending_gain = getf rt "pending_gain" st.pending_gain
    ; pending_base_quote = getf rt "pending_base_quote" st.pending_base_quote
    ; pending_base = getf rt "pending_base" st.pending_base
    ; close_entry = getf rt "close_entry" st.close_entry
    ; close_qty = getf rt "close_qty" st.close_qty
    ; close_started_at = getf rt "close_started_at" st.close_started_at
    ; close_seq_seen = geti rt "close_seq_seen" st.close_seq_seen
    ; close_realized_seen = getf rt "close_realized_seen" st.close_realized_seen
    ; recycle_seq_seen = geti rt "recycle_seq_seen" st.recycle_seq_seen
    ; recycle_realized_seen = getf rt "recycle_realized_seen" st.recycle_realized_seen
    ; spot_seq_seen = geti rt "spot_seq_seen" st.spot_seq_seen
    ; last_recycle_base = getf rt "last_recycle_base" st.last_recycle_base
    ; last_leverage = getp rt "perp_leverage" st.last_leverage
    ; full_balance_ref = st.full_balance_ref
    }
  in
  if next = st then st else { next with updated_at = now }
;;

let save_if_changed ~exchange ~symbol rt (r : Store.t ref) ~now =
  let next = snapshot rt !r ~now in
  if next <> !r
  then (
    r := next;
    Store.save_async ~exchange ~base_coin:(base_coin_of_symbol symbol) next)
;;

(** Realized PnL for a buy that closes a short, net of the order's fee class. *)
let realized_close ~entry ~price ~qty ~fee_rate =
  if entry > 0.0 then (qty *. (entry -. price)) -. (price *. qty *. fee_rate) else 0.0
;;

(** Record one actual overlay fill. A perp buy is a short close: its realized PnL is
    measured against the entry captured when the close was initiated ([close_entry]) and
    netted of the fee class the order was registered with. *)
let record_fill
  ~exchange
  ~symbol
  ~(market : Common.carry_market)
  ~maker
  ~kind
  ~(side : Common.order_side)
  ~price
  ~qty
  ~now
  ~maker_fee
  ~taker_fee
  (r : Store.t ref)
  =
  let st = !r in
  let fee_rate = if maker then maker_fee else taker_fee in
  let fee = price *. qty *. fee_rate in
  let next =
    match market, side with
    | Common.Carry_perp, Common.Buy ->
      let entry =
        if st.pending_close && st.close_entry > 0.0 then st.close_entry else st.entry
      in
      let realized = realized_close ~entry ~price ~qty ~fee_rate in
      Store.record_close_fill st ~price ~qty ~realized ~fee ~kind ~ts:now
    | Common.Carry_spot, Common.Buy ->
      Store.record_spot_fill st ~price ~qty ~realized:0.0 ~fee ~kind ~ts:now
    | _ -> st
  in
  if next <> st
  then (
    r := next;
    Store.save_async ~exchange ~base_coin:(base_coin_of_symbol symbol) next)
;;

(** Reset the ladder to the configured start. Used when the basis mode changes so the old
    ratio is never applied to the new basis before the position snapshot arrives. *)
let reset_ladder ~start_ratio rt =
  setf rt "anchor" 0.0;
  setf rt "ladder_ratio" start_ratio;
  seti rt "add_index" 0;
  seti rt "ladder_rungs" 0;
  setf rt "next_add" 0.0;
  setb rt "add_armed" false;
  setf rt "add_carry" 0.0;
  setf rt "add_rung_qty" 0.0;
  setf rt "add_target" 0.0;
  setf rt "add_arm_qty" 0.0
;;

(** Seed a ladder reconstructed from the live short (see
    [Domain_spawner.reconstruct_ladder]). Unlike [init_ladder], the anchor and rung index
    come from the position, so bands the price crossed while the process was down stay
    ahead of [add_index] and are placed by the normal arming path. *)
let restore_ladder
  ~anchor
  ~add_index
  ~rung_qty
  ~next_add
  ~first
  ~growth
  ~rungs
  ~ratio
  ~start_ratio
  rt
  =
  setf rt "anchor" anchor;
  setf rt "ladder_ratio" ratio;
  setf rt "ladder_start" start_ratio;
  setf rt "ladder_first" first;
  setf rt "ladder_growth" growth;
  seti rt "ladder_rungs" rungs;
  seti rt "add_index" add_index;
  setf rt "next_add" next_add;
  setf rt "add_carry" 0.0;
  setf rt "add_rung_qty" rung_qty;
  setf rt "add_target" 0.0;
  setf rt "add_arm_qty" 0.0;
  setb rt "add_armed" false
;;

(** Seed the ladder from the live short: the current size is the ladder start, so the
    caller derives the remaining rungs from it. The mark anchors the bands, not the
    historical entry. *)
let init_ladder
  ~max_ratio
  ~add_first
  ~add_growth
  ~num_rungs
  ~add_threshold
  ~leverage
  ~mark
  ~entry
  ~accumulated
  ~short_qty
  rt
  =
  if short_qty > 0.0001 && accumulated > 0.0 && entry > 0.0
  then (
    let ratio = Float.min max_ratio (Float.max 0.0 (short_qty /. accumulated)) in
    let anchor = if mark > 0.0 then mark else entry in
    let next_add =
      if num_rungs > 0 && leverage > 0.0
      then anchor *. (1.0 +. (add_threshold /. leverage))
      else 0.0
    in
    setf rt "anchor" anchor;
    setf rt "ladder_ratio" ratio;
    setf rt "ladder_start" ratio;
    setf rt "ladder_first" add_first;
    setf rt "ladder_growth" add_growth;
    seti rt "ladder_rungs" num_rungs;
    seti rt "add_index" 0;
    setf rt "next_add" next_add;
    setf rt "add_carry" 0.0;
    setf rt "add_rung_qty" (add_first *. accumulated);
    setf rt "add_target" 0.0;
    setf rt "add_arm_qty" 0.0;
    setb rt "add_armed" false)
;;
