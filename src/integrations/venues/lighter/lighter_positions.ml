(** Lighter perp position and margin snapshots.

    Positions arrive on [account_all/{ACCOUNT_ID}] and account equity on
    [user_stats/{ACCOUNT_ID}], both already subscribed by [Lighter_ws]. The REST
    [/api/v1/account] snapshot seeds the same store at startup. Published snapshots live
    in [Dio_exchange.Market_context] so strategy code reads them without depending on this
    package. *)

module Exchange = Dio_exchange.Exchange_intf

let exchange = "lighter"
let section = "lighter_positions"

type margin_inputs =
  { account_value : float
  ; margin_used : float
  ; withdrawable : float
  ; updated_at : float
  }

let last_margin_inputs : margin_inputs option Atomic.t = Atomic.make None
let last_positions : Exchange.Market_context.position list Atomic.t = Atomic.make []

let float_of_json json =
  try Lighter_types.parse_json_float json with
  | _ -> 0.0
;;

let float_opt = function
  | `Null -> None
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `Intlit s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | `String s ->
    (try Some (float_of_string (String.trim s)) with
     | _ -> None)
  | _ -> None
;;

let account_node json =
  let open Yojson.Safe.Util in
  match member "account_all" json with
  | (`Assoc _ | `List _) as v -> v
  | _ ->
    (match member "data" json with
     | (`Assoc _ | `List _) as v -> v
     | _ -> json)
;;

(** [Some nodes] when the frame carries an authoritative position set, [None] when the
    field is absent so a partial frame cannot clear live positions. *)
let position_nodes account =
  let open Yojson.Safe.Util in
  match member "positions" account with
  | `Assoc fields -> Some (List.map snd fields)
  | `List items -> Some items
  | _ -> None
;;

(** Frame node truncated to 200 bytes, for logs. *)
let json_snippet json =
  let s = Yojson.Safe.to_string json in
  if String.length s <= 200 then s else String.sub s 0 200 ^ "..."
;;

(** [market_id -> coin] seen on an earlier frame. Position frames can omit [symbol], and a
    node that resolves to no coin is dropped. Copy-on-write: parsed frames and the REST
    resync both write it. *)
let market_coins : (int, string) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 16)

let remember_market_coin market_id coin =
  let rec loop () =
    let current = Atomic.get market_coins in
    match Hashtbl.find_opt current market_id with
    | Some c when String.equal c coin -> ()
    | _ ->
      let copy = Hashtbl.copy current in
      Hashtbl.replace copy market_id coin;
      if not (Atomic.compare_and_set market_coins current copy) then loop ()
  in
  loop ()
;;

let market_coin market_id = Hashtbl.find_opt (Atomic.get market_coins) market_id

(** Last non-flat position per coin: [(signed qty, entry)]. Kept after a position flattens
    so a close fill prices off the venue's entry basis even when its position frame lands
    first. Copy-on-write: readers never see a mutated table. *)
let last_basis : (string, float * float) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 16)
;;

let remember_basis (p : Exchange.Market_context.position) =
  let qty = p.Exchange.Market_context.qty in
  let entry = p.Exchange.Market_context.entry in
  if entry > 0.0 && Float.abs qty > 1e-12
  then (
    let rec loop () =
      let current = Atomic.get last_basis in
      let copy = Hashtbl.copy current in
      Hashtbl.replace copy p.Exchange.Market_context.coin (qty, entry);
      if not (Atomic.compare_and_set last_basis current copy) then loop ()
    in
    loop ())
;;

(** Realized PnL of closing [amount] of [coin] at [price] against the venue's last
    reported entry basis. [None] when no position of the closing side was ever seen, so an
    unknown basis leaves the field off the fill instead of reporting zero. *)
let close_pnl ~coin ~price ~amount ~closing =
  match Hashtbl.find_opt (Atomic.get last_basis) coin with
  | Some (qty, entry) when entry > 0.0 ->
    if String.equal closing "long" && qty > 0.0
    then Some ((price -. entry) *. amount)
    else if String.equal closing "short" && qty < 0.0
    then Some ((entry -. price) *. amount)
    else None
  | _ -> None
;;

let parse_position ~now json =
  let open Yojson.Safe.Util in
  let market_id =
    try member "market_id" json |> to_int with
    | _ -> -1
  in
  let symbol =
    let raw =
      try member "symbol" json |> to_string with
      | _ -> ""
    in
    if raw <> ""
    then raw
    else if market_id >= 0
    then
      Option.value
        (Lighter_instruments_feed.get_symbol ~market_index:market_id)
        ~default:""
    else ""
  in
  let coin =
    let resolved = Lighter_types.base_coin symbol in
    if resolved <> ""
    then (
      if market_id >= 0 then remember_market_coin market_id resolved;
      resolved)
    else if market_id >= 0
    then Option.value (market_coin market_id) ~default:""
    else ""
  in
  let raw_qty = float_of_json (member "position" json) in
  let sign =
    try member "sign" json |> to_int with
    | _ -> 0
  in
  let signed_qty =
    if sign < 0 || (sign = 0 && raw_qty < 0.0)
    then -.Float.abs raw_qty
    else Float.abs raw_qty
  in
  let liquidation =
    match float_opt (member "liquidation_price" json) with
    | Some p when p > 0.0 -> Some p
    | _ -> None
  in
  let leverage =
    match float_opt (member "initial_margin_fraction" json) with
    | Some f when f > 0.0 && f < 1.0 -> Some (1.0 /. f)
    | _ -> None
  in
  { Exchange.Market_context.coin
  ; qty = signed_qty
  ; entry = float_of_json (member "avg_entry_price" json)
  ; position_value = float_of_json (member "position_value" json)
  ; unrealized_pnl = float_of_json (member "unrealized_pnl" json)
  ; margin_used = float_of_json (member "allocated_margin" json)
  ; liquidation
  ; leverage
  ; updated_at = now
  }
;;

let maintenance_of_positions (positions : Exchange.Market_context.position list) =
  List.fold_left
    (fun total p ->
      match
        Lighter_instruments_feed.get_maintenance_margin_fraction
          p.Exchange.Market_context.coin
      with
      | Some bps when bps > 0.0 ->
        total +. (Float.abs p.Exchange.Market_context.position_value *. bps /. 10_000.0)
      | _ -> total)
    0.0
    positions
;;

let publish_margin ?maintenance inputs =
  let maintenance_margin =
    match maintenance with
    | Some m -> m
    | None -> maintenance_of_positions (Atomic.get last_positions)
  in
  Dio_exchange.Market_context.update_margin
    ~exchange
    { Exchange.Market_context.account_value = inputs.account_value
    ; margin_used = inputs.margin_used
    ; maintenance_margin = Float.max 0.0 maintenance_margin
    ; withdrawable = inputs.withdrawable
    ; updated_at = inputs.updated_at
    }
;;

let apply_positions ~now nodes =
  let live =
    List.filter_map
      (fun json ->
        try
          let p = parse_position ~now json in
          if Float.abs p.Exchange.Market_context.qty <= 1e-12
          then None
          else if p.Exchange.Market_context.coin = ""
          then (
            Logging.warn_f
              ~section
              "Dropping position with no resolvable market: %s"
              (json_snippet json);
            None)
          else Some p
        with
        | exn ->
          Logging.warn_f
            ~section
            "Dropping unparseable position node: %s"
            (Printexc.to_string exn);
          None)
      nodes
  in
  let previous = Atomic.get last_positions in
  List.iter
    (fun (old : Exchange.Market_context.position) ->
      if not
           (List.exists
              (fun p -> String.equal p.Exchange.Market_context.coin old.coin)
              live)
      then
        Logging.info_f
          ~section
          "Position cleared: %s qty=%.8g"
          old.Exchange.Market_context.coin
          old.Exchange.Market_context.qty)
    previous;
  Atomic.set last_positions live;
  List.iter remember_basis live;
  Dio_exchange.Market_context.replace_positions ~exchange ~updated_at:now live;
  List.iter
    (fun p -> Concurrency.Exchange_wakeup.signal ~symbol:p.Exchange.Market_context.coin)
    live;
  match Atomic.get last_margin_inputs with
  | Some inputs -> publish_margin inputs
  | None -> ()
;;

(** Applies an [account_all] snapshot. Returns [true] when a position set was present. *)
let process_account_all json =
  let account = account_node json in
  match position_nodes account with
  | None -> false
  | Some nodes ->
    apply_positions ~now:(Unix.gettimeofday ()) nodes;
    true
;;

let user_stats_node json =
  let open Yojson.Safe.Util in
  match member "stats" json with
  | `Assoc _ as stats -> stats
  | _ ->
    (match member "user_stats" json with
     | `Assoc _ as user_stats ->
       (match member "stats" user_stats with
        | `Assoc _ as stats -> stats
        | _ -> user_stats)
     | _ ->
       (match member "data" json with
        | `Assoc _ as data ->
          (match member "stats" data with
           | `Assoc _ as stats -> stats
           | _ -> data)
        | _ -> `Null))
;;

(** Applies an [user_stats] snapshot. Margin used is initial margin in use; Lighter does
    not publish maintenance there, so it is derived from the live perp positions and each
    market's maintenance fraction. *)
let process_user_stats json =
  let open Yojson.Safe.Util in
  match user_stats_node json with
  | `Null -> false
  | stats ->
    let field key = float_opt (member key stats) in
    let has_margin_snapshot =
      List.exists
        (fun key -> Option.is_some (field key))
        [ "collateral"; "available_balance"; "portfolio_value" ]
    in
    if not has_margin_snapshot
    then false
    else (
      let collateral = Option.value (field "collateral") ~default:0.0 in
      let available = Option.value (field "available_balance") ~default:0.0 in
      let portfolio = field "portfolio_value" in
      let now = Unix.gettimeofday () in
      let inputs =
        { account_value = Option.value portfolio ~default:collateral
        ; margin_used = Float.max 0.0 (collateral -. available)
        ; withdrawable = available
        ; updated_at = now
        }
      in
      Atomic.set last_margin_inputs (Some inputs);
      publish_margin inputs;
      true)
;;

(** Applies the REST [/api/v1/account] snapshot used at startup. The top-level maintenance
    requirement is authoritative, so it is published as-is instead of being reconstructed
    from per-market fractions. *)
let process_rest_account json =
  let open Yojson.Safe.Util in
  let account =
    match member "accounts" json with
    | `List (first :: _) -> first
    | _ -> json
  in
  let now = Unix.gettimeofday () in
  (match position_nodes account with
   | Some nodes -> apply_positions ~now nodes
   | None -> ());
  let get key = float_opt (member key account) in
  let collateral = Option.value (get "collateral") ~default:0.0 in
  let available = Option.value (get "available_balance") ~default:0.0 in
  let has_margin_snapshot =
    List.exists
      (fun key -> Option.is_some (get key))
      [ "total_asset_value"; "cross_initial_margin_requirement"; "collateral" ]
  in
  if not has_margin_snapshot
  then ()
  else (
    let account_value =
      match get "total_asset_value" with
      | Some v -> v
      | None -> collateral
    in
    let margin_used =
      match get "cross_initial_margin_requirement" with
      | Some v when v > 0.0 -> v
      | _ -> Float.max 0.0 (collateral -. available)
    in
    let inputs =
      { account_value; margin_used; withdrawable = available; updated_at = now }
    in
    Atomic.set last_margin_inputs (Some inputs);
    publish_margin ?maintenance:(get "cross_maintenance_margin_requirement") inputs)
;;

let reset () =
  Atomic.set last_margin_inputs None;
  Atomic.set last_positions []
;;
