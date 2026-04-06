(**
   Kraken instrument metadata feed.

   Maintains a local cache of per-pair trading parameters (tick sizes, quantity
   constraints, cost minimums, trading status) sourced from the Kraken REST
   API ([/0/public/AssetPairs]).  A WebSocket-based feed path is stubbed but
   not yet implemented; [initialize_symbols] populates the cache via REST.

   Consumers (e.g. [Kraken_module]) query the cache synchronously through
   [get_price_increment], [get_qty_increment], [get_qty_min], etc.
*)

open Lwt.Infix

let section = "kraken_instrument"

(** Trading status of a Kraken pair as reported by the API. *)
type pair_status = 
  | Online
  | CancelOnly
  | Delisted
  | LimitOnly
  | Maintenance
  | PostOnly
  | ReduceOnly
  | WorkInProgress
  | Unknown

(** Parse a Kraken API status string into [pair_status]. *)
let status_of_string = function
  | "online" -> Online | "cancel_only" -> CancelOnly | "delisted" -> Delisted
  | "limit_only" -> LimitOnly | "maintenance" -> Maintenance | "post_only" -> PostOnly
  | "reduce_only" -> ReduceOnly | "work_in_progress" -> WorkInProgress | _ -> Unknown

(** Serialize [pair_status] to its Kraken API string form. *)
let status_to_string = function
  | Online -> "online" | CancelOnly -> "cancel_only" | Delisted -> "delisted"
  | LimitOnly -> "limit_only" | Maintenance -> "maintenance" | PostOnly -> "post_only"
  | ReduceOnly -> "reduce_only" | WorkInProgress -> "work_in_progress" | Unknown -> "unknown"

(** Returns [true] only when status is [Online]. *)
let is_tradeable = function Online -> true | _ -> false

(** Cached instrument metadata for a single trading pair. *)
type pair_info = {
  symbol: string;           (** Canonical pair symbol (e.g. "BTC/USD"). *)
  base: string;             (** Base asset identifier. *)
  quote: string;            (** Quote asset identifier. *)
  status: pair_status;      (** Current trading status. *)
  qty_precision: int;       (** Decimal places for order quantities. *)
  qty_increment: float;     (** Minimum quantity step size. *)
  qty_min: float;           (** Minimum allowed order quantity. *)
  price_precision: int;     (** Decimal places for prices. *)
  price_increment: float;   (** Minimum price tick size. *)
  cost_precision: int;      (** Decimal places for cost values. *)
  cost_min: float;          (** Minimum notional order cost. *)
  marginable: bool;         (** Whether margin trading is available. *)
  has_index: bool;          (** Whether an index price exists. *)
  last_updated: float;      (** Unix timestamp of last cache write. *)
}

(** In-memory symbol-to-[pair_info] cache. Protected by [cache_mutex]. *)
let pair_cache : (string, pair_info) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Lwt_mutex.create ()

(** Parse a single pair JSON object (WebSocket schema) into [pair_info].
    Returns [None] if any required field is missing or malformed. *)
let parse_pair_info json : pair_info option =
  try
    let open Yojson.Safe.Util in
    Some { symbol = json |> member "symbol" |> to_string;
           base = json |> member "base" |> to_string;
           quote = json |> member "quote" |> to_string;
           status = json |> member "status" |> to_string |> status_of_string;
           qty_precision = json |> member "qty_precision" |> to_int;
           qty_increment = json |> member "qty_increment" |> to_float;
           qty_min = json |> member "qty_min" |> to_float;
           price_precision = json |> member "price_precision" |> to_int;
           price_increment = json |> member "price_increment" |> to_float;
           cost_precision = json |> member "cost_precision" |> to_int;
           cost_min = json |> member "cost_min" |> to_float;
           marginable = json |> member "marginable" |> to_bool;
           has_index = json |> member "has_index" |> to_bool;
           last_updated = Unix.time () }
  with _ -> None

(** Insert or replace [info] in [pair_cache] under [cache_mutex].
    Logs a warning on status transitions and a debug line on first insertion. *)
let update_pair_info info =
  Lwt_mutex.with_lock cache_mutex (fun () ->
    let prev_status = Hashtbl.find_opt pair_cache info.symbol |> Option.map (fun p -> p.status) in
    Hashtbl.replace pair_cache info.symbol info;
    (match prev_status with
    | Some prev when prev <> info.status ->
        Logging.warn_f ~section "Pair %s status changed: %s -> %s" info.symbol (status_to_string prev) (status_to_string info.status)
    | None ->
        Logging.debug_f ~section "Pair %s initialized: status=%s, qty_min=%.8f, price_inc=%.8f"
          info.symbol (status_to_string info.status) info.qty_min info.price_increment
    | _ -> ());
    Lwt.return_unit)

(** Look up [symbol] in the pair cache. Acquires [cache_mutex]. *)
let get_pair_info symbol : pair_info option Lwt.t =
  Lwt_mutex.with_lock cache_mutex (fun () -> Lwt.return (Hashtbl.find_opt pair_cache symbol))

(** Returns [true] if [symbol] exists in the cache with [Online] status. *)
let is_pair_tradeable symbol : bool Lwt.t =
  get_pair_info symbol >|= function None -> false | Some info -> is_tradeable info.status

(** Process a full instrument data snapshot (WebSocket schema).
    Extracts the ["pairs"] array from the ["data"] envelope and updates
    the cache for each successfully parsed pair. *)
let process_instrument_data json =
  try
    let open Yojson.Safe.Util in
    let data = member "data" json in
    let pairs = member "pairs" data |> to_list in
    
    Lwt_list.iter_s (fun pair_json ->
      match parse_pair_info pair_json with
      | Some info -> update_pair_info info
      | None -> Lwt.return_unit
    ) pairs
  with exn ->
    Logging.error_f ~section "Failed to process instrument data: %s" (Printexc.to_string exn);
    Lwt.return_unit

(** Placeholder for a future WebSocket-based instrument feed subscription.
    Currently a no-op; the cache is populated via [fetch_from_rest] instead. *)
let connect_and_subscribe () : unit Lwt.t =
  Logging.info ~section "WebSocket instrument stream not yet implemented; using REST API fallback";
  Lwt.return_unit

(** Fetch pair metadata from [GET /0/public/AssetPairs] and populate the
    cache for each symbol in [symbols].

    Symbol matching accounts for Kraken naming conventions: the function
    normalises the requested symbol to uppercase, strips slashes, and
    compares against both the [wsname] and [altname] fields in the API
    response, including legacy aliases (e.g. XBT/USD for BTC/USD). *)
let fetch_from_rest symbols =
  Lwt.catch (fun () ->
    let open Cohttp_lwt_unix in
    let url = Uri.of_string "https://api.kraken.com/0/public/AssetPairs" in
    Client.get url >>= fun (_resp, body) ->
    Cohttp_lwt.Body.to_string body >>= fun body_str ->
    Logging.debug_f ~section "Fetched AssetPairs data (%d bytes)" (String.length body_str);
    
    let open Yojson.Safe.Util in
    let json = Yojson.Safe.from_string body_str in
    let result = member "result" json in
    
    Lwt_list.iter_s (fun symbol ->
      let norm = String.uppercase_ascii symbol in
      let no_slash = String.map (fun c -> if c = '/' then '\000' else c) norm |> String.split_on_char '\000' |> String.concat "" in
      let pairs_assoc = to_assoc result in
      let matches_pair (_, pair_json) =
        try
          let altname = member "altname" pair_json |> to_string_option in
          let wsname = member "wsname" pair_json |> to_string_option in
          let ws_match = function Some n -> n = norm || (n = "XBT/USD" && norm = "BTC/USD") || (n = "XETH/USD" && norm = "ETH/USD") | None -> false in
          let alt_match = function Some n -> n = no_slash || (n = "XBTUSD" && no_slash = "BTCUSD") || (n = "XETHUSD" && no_slash = "ETHUSD") | None -> false in
          ws_match wsname || alt_match altname
        with _ -> false
      in
      let found = List.find_opt matches_pair pairs_assoc in
      
      match found with
      | None -> Logging.info_f ~section "No REST data found for %s, will use default instrument data" symbol; Lwt.return_unit
      | Some (pair_name, pair_json) ->
          Logging.debug_f ~section "Found pair data for %s: pair_name=%s" symbol pair_name;
          let safe_get fn def field = try fn (member field pair_json) with _ -> def in
          let qty_prec = safe_get to_int 8 "lot_decimals" in
          let qty_inc = 10.0 ** (-.float_of_int qty_prec) in
          let qty_min = safe_get (fun m -> to_string m |> float_of_string) 0.0001 "ordermin" in
          let price_inc = safe_get (fun m -> to_string m |> float_of_string) 0.01 "tick_size" in
          let cost_min = safe_get (fun m -> to_string m |> float_of_string) 0.5 "costmin" in

          (* Derive price precision: prefer explicit JSON fields, fall back
             to counting decimal digits in tick_size. *)
          let price_prec =

            (try member "pair_decimals" pair_json |> to_int with _ ->
            (try member "decimals" pair_json |> to_int with _ ->
              (* Infer precision from tick_size by counting decimal places. *)
              let rec count_decimals v count =
                if count > 12 then count  (* Cap at 12 to avoid float rounding drift. *)
                else
                  let scaled = v *. 10.0 ** float_of_int count in
                  if Float.abs (scaled -. Float.round scaled) < 1e-9 then count
                  else count_decimals v (count + 1)
              in
              count_decimals price_inc 0)) in

          let pair_info = {
            symbol; base = safe_get to_string "" "base"; quote = safe_get to_string "" "quote";
            status = (try status_of_string (member "status" pair_json |> to_string) with _ -> Online);
            qty_precision = qty_prec; qty_increment = qty_inc; qty_min;
            price_precision = price_prec; price_increment = price_inc;
            cost_precision = safe_get to_int 5 "cost_decimals"; cost_min;
            marginable = safe_get (fun m -> to_int m > 0) false "margin_call";
            has_index = true; last_updated = Unix.time () } in

          Logging.info_f ~section "Instrument data for %s: status=%s, qty_precision=%d, qty_increment=%.8f, qty_min=%.8f, price_precision=%d, price_increment=%.8f, cost_precision=%d, cost_min=%.2f, marginable=%b"
            symbol (status_to_string pair_info.status) pair_info.qty_precision pair_info.qty_increment pair_info.qty_min
            pair_info.price_precision pair_info.price_increment pair_info.cost_precision pair_info.cost_min pair_info.marginable;

          update_pair_info pair_info
    ) symbols
  ) (fun exn ->
    Logging.error_f ~section "Failed to fetch instrument data from REST: %s" (Printexc.to_string exn);
    Lwt.return_unit
  )

(** Round [value] to the nearest multiple of [increment]. *)
let round_to_increment value increment = Float.round (value /. increment) *. increment

(** Round [qty] to the nearest valid quantity step for [info]. *)
let round_quantity info qty = round_to_increment qty info.qty_increment

(** Round [price] to the nearest valid tick for [info]. *)
let round_price info price = round_to_increment price info.price_increment

(** Validate [qty] and [price] against the constraints in [info].
    Rounds both values to their respective increments, then checks
    minimum quantity, minimum cost (qty * price), and tradeable status.
    Returns [Ok (rounded_qty, rounded_price, cost)] or [Error msg]. *)
let validate_order info ~qty ~price =
  let rqty = round_quantity info qty in
  let rprice = round_price info price in
  let cost = rqty *. rprice in
  if rqty < info.qty_min then Error (Printf.sprintf "Quantity %.8f below minimum %.8f" rqty info.qty_min)
  else if cost < info.cost_min then Error (Printf.sprintf "Order cost %.5f below minimum %.5f" cost info.cost_min)
  else if not (is_tradeable info.status) then Error (Printf.sprintf "Pair %s not tradeable (status: %s)" info.symbol (status_to_string info.status))
  else Ok (rqty, rprice, cost)

(** Populate the instrument cache for [symbols] via the REST API.
    Intended to be called once at application startup before trading begins. *)
let initialize_symbols symbols : unit Lwt.t =
  Logging.debug_f ~section "Initializing instrument data for %d symbols" (List.length symbols);
  fetch_from_rest symbols

(** Duplicate binding of [get_pair_info]; retained for link compatibility. *)
let get_pair_info symbol : (pair_info option) Lwt.t =
  Lwt_mutex.with_lock cache_mutex (fun () -> Lwt.return (Hashtbl.find_opt pair_cache symbol))

(** Return [(price_precision, qty_precision)] for [symbol], or [None].
    Synchronous; reads [pair_cache] without acquiring [cache_mutex]. *)
let get_precision_info symbol : (int * int) option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> (info.price_precision, info.qty_precision))
  with _ -> None

(** Return the minimum price tick size for [symbol], or [None].
    Synchronous; reads [pair_cache] without acquiring [cache_mutex]. *)
let get_price_increment symbol : float option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> info.price_increment)
  with _ -> None

(** Return price precision for [symbol]. Raises [Failure] if not cached. *)
let get_price_precision_exn symbol : int =
  match get_precision_info symbol with
  | Some (price_prec, _) -> price_prec
  | None -> failwith (Printf.sprintf "No price precision found for symbol %s" symbol)

(** Return the minimum order quantity for [symbol], or [None].
    Synchronous; reads [pair_cache] without acquiring [cache_mutex]. *)
let get_qty_min symbol : float option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> info.qty_min)
  with _ -> None

(** Return the minimum quantity step size for [symbol], or [None].
    Synchronous; reads [pair_cache] without acquiring [cache_mutex]. *)
let get_qty_increment symbol : float option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> info.qty_increment)
  with _ -> None

