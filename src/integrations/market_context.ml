(** Venue-agnostic market context shared by feeds and strategy code.

    Feeds publish funding/basis, perp position, and account-margin snapshots here.
    Strategies read them without depending on a venue package. Keys are
    [exchange ^ "|" ^ coin], so a multi-venue process cannot mix contexts.

    Reads and writes are copy-on-write: the published table is immutable, so readers never
    take a lock and never observe a half-written snapshot. Updates are low-frequency (one
    per asset-context frame / user-state frame), so copying a small table is cheaper than
    a mutex on the strategy path. *)

type funding = Exchange_intf.Market_context.funding =
  { funding : float (** hourly funding rate, e.g. 0.0000125 *)
  ; premium : float (** perp premium vs oracle, e.g. -0.0002083 *)
  ; mark : float
  ; oracle : float
  ; updated_at : float (** unix seconds *)
  }

type position = Exchange_intf.Market_context.position =
  { coin : string
  ; qty : float (** signed base quantity; negative is short *)
  ; entry : float
  ; position_value : float
  ; unrealized_pnl : float
  ; margin_used : float
  ; liquidation : float option
  ; leverage : float option
  ; updated_at : float
  }

type margin = Exchange_intf.Market_context.margin =
  { account_value : float
  ; margin_used : float
  ; maintenance_margin : float
  ; withdrawable : float
  ; updated_at : float
  }

let key exchange coin = exchange ^ "|" ^ coin
let funding_table : (string, funding) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 16)

let position_table : (string, position) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 16)
;;

let margin_table : (string, margin) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 8)
let positions_tick : (string, float) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 8)

(** Copy-on-write update. Retries if another writer published between the copy and the
    compare-and-set. *)
let update table f =
  let rec loop () =
    let old = Atomic.get table in
    let copy = Hashtbl.copy old in
    f copy;
    if Atomic.compare_and_set table old copy then () else loop ()
  in
  loop ()
;;

let update_funding ~exchange ~coin ctx =
  update funding_table (fun t -> Hashtbl.replace t (key exchange coin) ctx)
;;

let get_funding ~exchange ~coin =
  Hashtbl.find_opt (Atomic.get funding_table) (key exchange coin)
;;

let update_position ~exchange p =
  update position_table (fun t -> Hashtbl.replace t (key exchange p.coin) p)
;;

(** Replaces the venue's whole position set. Dropping absent coins is what turns a closed
    position into a zero position instead of a stale one. *)
let replace_positions ~exchange ~updated_at positions =
  update position_table (fun t ->
    Hashtbl.iter
      (fun k _ ->
        match String.index_opt k '|' with
        | Some i when String.sub k 0 i = exchange -> Hashtbl.remove t k
        | _ -> ())
      t;
    List.iter (fun p -> Hashtbl.replace t (key exchange p.coin) p) positions);
  update positions_tick (fun t -> Hashtbl.replace t exchange updated_at)
;;

let clear_position ~exchange ~coin =
  update position_table (fun t -> Hashtbl.remove t (key exchange coin))
;;

let get_position ~exchange ~coin =
  Hashtbl.find_opt (Atomic.get position_table) (key exchange coin)
;;

(** Marks a full position snapshot as received without changing the stored positions. Used
    when the venue's frame omits [assetPositions] on a flat account. *)
let mark_positions_updated ~exchange ~updated_at =
  update positions_tick (fun t -> Hashtbl.replace t exchange updated_at)
;;

(** Unix time of the last full position snapshot for [exchange], or [None] before the
    first one. Callers use this to tell "flat" apart from "never received a snapshot". *)
let get_positions_updated_at ~exchange =
  match Hashtbl.find_opt (Atomic.get positions_tick) exchange with
  | Some t when t > 0.0 -> Some t
  | _ -> None
;;

let update_margin ~exchange m =
  update margin_table (fun t -> Hashtbl.replace t exchange m)
;;

let get_margin ~exchange = Hashtbl.find_opt (Atomic.get margin_table) exchange

(** Drops every snapshot for [exchange]. Used by tests and by a venue's full resync. *)
let clear ~exchange =
  let prefix = exchange ^ "|" in
  let plen = String.length prefix in
  let is_exchange k = String.length k >= plen && String.sub k 0 plen = prefix in
  update funding_table (fun t ->
    let keys =
      Hashtbl.fold (fun k _ acc -> if is_exchange k then k :: acc else acc) t []
    in
    List.iter (Hashtbl.remove t) keys);
  update position_table (fun t ->
    let keys =
      Hashtbl.fold (fun k _ acc -> if is_exchange k then k :: acc else acc) t []
    in
    List.iter (Hashtbl.remove t) keys);
  update positions_tick (fun t -> Hashtbl.remove t exchange);
  update margin_table (fun t -> Hashtbl.remove t exchange)
;;

(** Equity / maintenance margin, or [None] when the snapshot is missing or has no
    maintenance requirement. *)
let get_margin_ratio ~exchange =
  match get_margin ~exchange with
  | Some m when m.maintenance_margin > 0.0 ->
    Some (m.account_value /. m.maintenance_margin)
  | _ -> None
;;

let annualized_funding ~exchange ~coin =
  match get_funding ~exchange ~coin with
  | Some f -> Some (f.funding *. 24.0 *. 365.0)
  | None -> None
;;
