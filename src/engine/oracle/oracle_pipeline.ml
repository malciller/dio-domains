(* Oracle pipeline: one asset, one pass.

   Wires the all-time merged price history into the pure core:
   references -> runway -> parameter search -> decision record. This is the
   ONLY path from market data to a [decision]; everything downstream
   (pools, execution) consumes the four-field record as-is. *)

(** Inputs for one decision pass on one asset on one venue. *)
type inputs =
  { exchange : string
  ; symbol : string
  ; bars : Oracle_types.bar array (** All-time merged history. *)
  ; current_price : float (** Latest trade/quote price. *)
  ; available_quote : float (** Quote pool share allocated to this asset. *)
  ; sell_qty : float
    (** Base sell size from the venue pool accounting (base pool minus
        reserved_base minus resting-sell base). *)
  ; bounds : Oracle_core.bounds
  ; target_survival : float
  ; min_active_dsurv : float
  ; fees : Oracle_core.fee_model
  }

(** Everything a pass learned, for logging/telemetry. *)
type outcome =
  { refs : Oracle_core.references
  ; runway : Oracle_core.runway
  ; resolution : Oracle_core.resolution
  ; decision : Oracle_core.decision
  }

(** Current price of a series: the last bar's close. *)
let current_price_of_series (s : Oracle_types.series) : float option =
  let n = Array.length s.bars in
  if n = 0 then None else Some s.bars.(n - 1).close
;;

(** All-time merged history for one asset: the venue's registry-backed
    daily series delta-fetched against the disk cache, extended BACKWARDS
    with Yahoo deep history (venue bars win on overlap; nothing is ever
    synthesized). No gap tolerance, no minimum length: what the sources
    provide is what the references see. *)
let history_of ~(offline : bool) ~(exchange : string) ~(symbol : string)
  : Oracle_types.series Lwt.t
  =
  let open Lwt.Infix in
  Oracle_fetch.fetch_series_for ~offline ~exchange ~symbol ()
  >>= fun venue ->
  if offline
  then Lwt.return venue
  else Oracle_fetch.deepen_series ~no_deep_history:false ~offline:false venue >|= fst
;;

(** One pure pass: history in, decision out. [None] when there is no usable
    history (empty or all-invalid bars) - no decision can be made. *)
let decide ~(inputs : inputs) : outcome option =
  match Oracle_core.references_of ~bars:inputs.bars with
  | None -> None
  | Some refs ->
    let runway =
      Oracle_core.runway_of
        ~current:inputs.current_price
        ~refs
        ~target_survival:inputs.target_survival
    in
    let resolution =
      Oracle_core.resolve
        ~regime:runway.regime
        ~current:inputs.current_price
        ~funded_floor:runway.funded_floor
        ~aggressiveness:runway.aggressiveness
        ~bounds:inputs.bounds
        ~quote:inputs.available_quote
        ~fees:inputs.fees
        ~target_survival:inputs.target_survival
        ()
    in
    let decision =
      Oracle_core.decision_of
        ~resolution
        ~sell_qty:inputs.sell_qty
        ~available_quote:inputs.available_quote
        ~current:inputs.current_price
        ~min_active_dsurv:inputs.min_active_dsurv
    in
    Some { refs; runway; resolution; decision }
;;
