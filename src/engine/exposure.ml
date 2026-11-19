(** Event-driven valuation for [max_exposure]. Trading domains read one immutable
    snapshot; the shared maintenance domain recomputes it on balance/fill notifications,
    never on a timer. Denominators are risk references, not execution prices. *)

let section = "exposure"

module Exchange = Dio_exchange.Exchange_intf

type snapshot =
  { generation : int
  ; computed_at : float
  ; portfolio_value : float
  ; venue_values : float array
  ; asset_quantities : float array
  (** Total base held across the primary leg and its configured inventory legs. *)
  ; venue_asset_quantities : float array (** Primary leg only, for the venue cap. *)
  }

let empty_snapshot =
  { generation = 0
  ; computed_at = 0.0
  ; portfolio_value = 0.0
  ; venue_values = [||]
  ; asset_quantities = [||]
  ; venue_asset_quantities = [||]
  }
;;

let snapshot_atomic = Atomic.make empty_snapshot
let generation = Atomic.make 0
let dirty = Atomic.make true
let started = Atomic.make false

(* Venue indices are fixed once, on the main domain before trading domains spawn. A domain
   caches its index at spawn; the worker publishes values in the same order. *)
let venue_mutex = Mutex.create ()
let venue_table : string array option ref = ref None
let venue_index_cache : (string, int) Hashtbl.t = Hashtbl.create 8

(** One capped strategy's holdings: primary venue leg first, then the overlay inventory
    legs attached to it. Registered once at domain spawn; the maintenance worker reads the
    array lock-free. *)
type asset_spec = { as_legs : (string * string) array }

let asset_specs : asset_spec array Atomic.t = Atomic.make [||]
let asset_mutex = Mutex.create ()
let asset_index_cache : (string, int) Hashtbl.t = Hashtbl.create 8

let ensure_venues () =
  match !venue_table with
  | Some names -> names
  | None ->
    Mutex.lock venue_mutex;
    let names =
      match !venue_table with
      | Some names -> names
      | None ->
        let names =
          Exchange.Registry.get_all_names () |> List.sort String.compare |> Array.of_list
        in
        venue_table := Some names;
        names
    in
    Mutex.unlock venue_mutex;
    names
;;

(** Stable index of the venue in the published array, or -1 when the venue is not
    registered. *)
let venue_index ~exchange =
  let key = String.lowercase_ascii (String.trim exchange) in
  match Hashtbl.find_opt venue_index_cache key with
  | Some idx -> idx
  | None ->
    let names = ensure_venues () in
    let rec find i =
      if i >= Array.length names
      then -1
      else if String.equal names.(i) key
      then i
      else find (i + 1)
    in
    let idx = find 0 in
    Hashtbl.replace venue_index_cache key idx;
    idx
;;

let quote_assets = [ "USD"; "USDC"; "USDT"; "ZUSD"; "USDE" ]
let is_quote_asset asset = List.mem (String.uppercase_ascii asset) quote_assets
let exchange_kind exchange = Exchange.Types.exchange_of_string exchange

let quote_of_exchange exchange =
  match exchange_kind exchange with
  | Hyperliquid | Lighter -> "USDC"
  | Kraken | Ibkr | Alpaca | Custom _ -> "USD"
;;

let is_equity_exchange exchange =
  match exchange_kind exchange with
  | Alpaca | Ibkr -> true
  | _ -> false
;;

(** Alpaca mirrors its cash into both USD and USDC; Kraken can expose ZUSD beside USD.
    Count only one carry leg for those venues. *)
let duplicate_quote exchange asset =
  match exchange_kind exchange, String.uppercase_ascii asset with
  | Alpaca, "USDC" -> true
  | Kraken, "ZUSD" -> true
  | _ -> false
;;

let symbol_of_balance exchange asset =
  if is_equity_exchange exchange then asset else asset ^ "/" ^ quote_of_exchange exchange
;;

let mid_of_book = function
  | Some (bid, _, ask, _) when bid > 0.0 && ask > 0.0 -> Some ((bid +. ask) /. 2.0)
  | Some (bid, _, _, _) when bid > 0.0 -> Some bid
  | Some (_, _, ask, _) when ask > 0.0 -> Some ask
  | _ -> None
;;

let price_on exchange symbol =
  match Exchange.Registry.get exchange with
  | None -> None
  | Some (module Ex) -> mid_of_book (Ex.get_top_of_book ~symbol)
;;

(** Last-resort valuation for an asset whose own venue has no quote yet. The supervisor
    subscribes non-configured holdings precisely so portfolio valuation can use another
    venue's book. *)
let price_of ~exchange ~asset =
  let direct = symbol_of_balance exchange asset in
  match price_on exchange direct with
  | Some price -> Some price
  | None ->
    let venue_mark =
      match exchange_kind exchange with
      | Alpaca -> Alpaca.Balances.get_position_price asset
      | _ -> None
    in
    (match venue_mark with
     | Some price when price > 0.0 -> Some price
     | _ ->
       let rec scan = function
         | [] -> None
         | other :: rest when String.equal other exchange -> scan rest
         | other :: rest ->
           let symbol = symbol_of_balance other asset in
           (match price_on other symbol with
            | Some price when price > 0.0 -> Some price
            | _ -> scan rest)
       in
       scan (Array.to_list (ensure_venues ())))
;;

let venue_value exchange =
  match Exchange.Registry.get exchange with
  | None -> 0.0
  | Some (module Ex) ->
    let total = ref 0.0 in
    List.iter
      (fun (asset, qty) ->
        if qty <> 0.0 && Float.is_finite qty
        then
          if is_quote_asset asset
          then (if not (duplicate_quote exchange asset) then total := !total +. qty)
          else (
            match price_of ~exchange ~asset with
            | Some price when price > 0.0 && Float.is_finite price ->
              total := !total +. (qty *. price)
            | _ -> ()))
      (Ex.get_all_balances ());
    !total
;;

let leg_qty ~exchange ~symbol =
  match Exchange.Registry.get exchange with
  | None -> 0.0
  | Some (module Ex) ->
    let qty = Ex.get_total_balance ~asset:(Config.base_coin_of_symbol symbol) in
    if Float.is_finite qty then qty else 0.0
;;

let asset_quantities (spec : asset_spec) =
  let total, venue =
    Array.fold_left
      (fun (total, venue) (exchange, symbol) ->
        let qty = leg_qty ~exchange ~symbol in
        total +. qty, if Float.is_nan venue then qty else venue)
      (0.0, Float.nan)
      spec.as_legs
  in
  total, if Float.is_nan venue then 0.0 else venue
;;

(** Rebuild and publish the immutable snapshot. Only the maintenance worker calls this. *)
let compute () =
  let names = ensure_venues () in
  let values = Array.map venue_value names in
  let portfolio_value = Array.fold_left (fun acc value -> acc +. value) 0.0 values in
  let specs = Atomic.get asset_specs in
  let totals = Array.make (Array.length specs) 0.0 in
  let venue_totals = Array.make (Array.length specs) 0.0 in
  Array.iteri
    (fun idx spec ->
      let total, venue = asset_quantities spec in
      totals.(idx) <- total;
      venue_totals.(idx) <- venue)
    specs;
  let generation = Atomic.fetch_and_add generation 1 + 1 in
  Atomic.set
    snapshot_atomic
    { generation
    ; computed_at = Unix.gettimeofday ()
    ; portfolio_value
    ; venue_values = values
    ; asset_quantities = totals
    ; venue_asset_quantities = venue_totals
    }
;;

(** Worker body. A false [dirty] is the common case and costs one atomic read. *)
let run () =
  if Atomic.get dirty
  then (
    Atomic.set dirty false;
    try compute () with
    | exn ->
      Atomic.set dirty true;
      Logging.error_f ~section "valuation pass failed: %s" (Printexc.to_string exn))
;;

(** Pull one fresh valuation on the next maintenance pass. Safe from any domain. *)
let notify () =
  Atomic.set dirty true;
  Aux_services.signal_maintenance ()
;;

(** Start the shared service and publish an initial snapshot before any trading domain is
    spawned. Subsequent passes run on the maintenance domain. *)
let start () =
  if Atomic.compare_and_set started false true
  then (
    (try compute () with
     | exn ->
       Logging.error_f ~section "initial valuation failed: %s" (Printexc.to_string exn));
    Aux_services.register_maintenance ~name:"exposure" ~run)
;;

(** Register a capped strategy's holding legs once at spawn. The primary leg must be
    first; [Config.inventory_legs_for] guarantees that order. Returns the snapshot slot. *)
let register_asset ~exchange ~symbol ~legs =
  start ();
  let key = String.lowercase_ascii (String.trim exchange) ^ "|" ^ symbol in
  Mutex.lock asset_mutex;
  let idx, added =
    match Hashtbl.find_opt asset_index_cache key with
    | Some idx -> idx, false
    | None ->
      let specs = Atomic.get asset_specs in
      let idx = Array.length specs in
      let legs =
        match legs with
        | [] -> [ exchange, symbol ]
        | _ -> legs
      in
      Atomic.set asset_specs (Array.append specs [| { as_legs = Array.of_list legs } |]);
      Hashtbl.replace asset_index_cache key idx;
      idx, true
  in
  Mutex.unlock asset_mutex;
  if added then notify ();
  idx
;;

let current () = Atomic.get snapshot_atomic
let enabled () = Atomic.get started

(** The short overlay's fallback trigger. The three causes stay separate inputs (so logs
    can name them) but produce one outcome. *)
let parent_unfunded ~oracle_active ~capital_low ~exposure_high =
  (not oracle_active) || capital_low || exposure_high
;;

let pct_ratio value denominator =
  if denominator > 0.0 && Float.is_finite denominator && Float.is_finite value
  then value /. denominator *. 100.0
  else Float.nan
;;

(** Hot-path decision: one bool. A zero/unknown denominator fails open. The portfolio
    numerator includes configured inventory legs; the venue numerator is the primary leg. *)
let breached
  ~venue_position_value
  ~portfolio_position_value
  ~portfolio_value
  ~venue_value
  ~(limits : Dio_strategies.Strategy_common.exposure_limit option)
  =
  match limits with
  | None -> false
  | Some limits ->
    let portfolio_hit =
      match limits.portfolio_pct with
      | Some pct when portfolio_value > 0.0 ->
        portfolio_position_value /. portfolio_value > pct /. 100.0
      | _ -> false
    in
    let venue_hit =
      match limits.venue_pct with
      | Some pct when venue_value > 0.0 ->
        venue_position_value /. venue_value > pct /. 100.0
      | _ -> false
    in
    portfolio_hit || venue_hit
;;

(** Percentages for transition logs only; not on the steady-state path. *)
let ratios ~venue_position_value ~portfolio_position_value ~portfolio_value ~venue_value =
  ( pct_ratio portfolio_position_value portfolio_value
  , pct_ratio venue_position_value venue_value )
;;
