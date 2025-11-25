(** Kraken Instrument Data - WebSocket v2 subscription for pair status and parameters *)

open Lwt.Infix

let section = "kraken_instrument"

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

let status_of_string = function
  | "online" -> Online | "cancel_only" -> CancelOnly | "delisted" -> Delisted
  | "limit_only" -> LimitOnly | "maintenance" -> Maintenance | "post_only" -> PostOnly
  | "reduce_only" -> ReduceOnly | "work_in_progress" -> WorkInProgress | _ -> Unknown

let status_to_string = function
  | Online -> "online" | CancelOnly -> "cancel_only" | Delisted -> "delisted"
  | LimitOnly -> "limit_only" | Maintenance -> "maintenance" | PostOnly -> "post_only"
  | ReduceOnly -> "reduce_only" | WorkInProgress -> "work_in_progress" | Unknown -> "unknown"

let is_tradeable = function Online -> true | _ -> false

type pair_info = {
  symbol: string;
  base: string;
  quote: string;
  status: pair_status;
  qty_precision: int;
  qty_increment: float;
  qty_min: float;
  price_precision: int;
  price_increment: float;
  cost_precision: int;
  cost_min: float;
  marginable: bool;
  has_index: bool;
  last_updated: float;
}

(* In-memory cache of pair information *)
let pair_cache : (string, pair_info) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Lwt_mutex.create ()

(** Parse pair info from JSON *)
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

(** Update cache with pair info *)
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

(** Get pair info from cache *)
let get_pair_info symbol : pair_info option Lwt.t =
  Lwt_mutex.with_lock cache_mutex (fun () -> Lwt.return (Hashtbl.find_opt pair_cache symbol))

(** Check if a pair is currently tradeable *)
let is_pair_tradeable symbol : bool Lwt.t =
  get_pair_info symbol >|= function None -> false | Some info -> is_tradeable info.status

(** Process instrument snapshot or update *)
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

(** WebSocket connection management - to be implemented *)
let connect_and_subscribe () : unit Lwt.t =
  Logging.info ~section "WebSocket instrument stream not yet implemented; using REST API fallback";
  Lwt.return_unit

(** Fetch instrument data from Kraken REST API *)
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

          (* Determine price precision without hardcoded symbol fallbacks *)
          let price_prec =
            (* Prefer explicit precision fields when available *)
            (try member "pair_decimals" pair_json |> to_int with _ ->
            (try member "decimals" pair_json |> to_int with _ ->
              (* Derive precision from tick_size if decimals are not provided *)
              let rec count_decimals v count =
                if count > 12 then count (* cap to avoid infinite loops due to float inaccuracy *)
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

(** Round a value to the nearest increment *)
let round_to_increment value increment = Float.round (value /. increment) *. increment

(** Round quantity to valid increment *)
let round_quantity info qty = round_to_increment qty info.qty_increment

(** Round price to valid increment *)  
let round_price info price = round_to_increment price info.price_increment

(** Validate if an order meets all requirements *)
let validate_order info ~qty ~price =
  let rqty = round_quantity info qty in
  let rprice = round_price info price in
  let cost = rqty *. rprice in
  if rqty < info.qty_min then Error (Printf.sprintf "Quantity %.8f below minimum %.8f" rqty info.qty_min)
  else if cost < info.cost_min then Error (Printf.sprintf "Order cost %.5f below minimum %.5f" cost info.cost_min)
  else if not (is_tradeable info.status) then Error (Printf.sprintf "Pair %s not tradeable (status: %s)" info.symbol (status_to_string info.status))
  else Ok (rqty, rprice, cost)

(** Initialize instrument data for a list of symbols using REST fallback *)
let initialize_symbols symbols : unit Lwt.t =
  Logging.debug_f ~section "Initializing instrument data for %d symbols" (List.length symbols);
  fetch_from_rest symbols

let get_pair_info symbol : (pair_info option) Lwt.t =
  Lwt_mutex.with_lock cache_mutex (fun () -> Lwt.return (Hashtbl.find_opt pair_cache symbol))

(** Get precision info for a symbol - synchronous version for internal use *)
let get_precision_info symbol : (int * int) option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> (info.price_precision, info.qty_precision))
  with _ -> None

(** Get price increment for a symbol - synchronous version for internal use *)
let get_price_increment symbol : float option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> info.price_increment)
  with _ -> None

let get_price_precision_exn symbol : int =
  match get_precision_info symbol with
  | Some (price_prec, _) -> price_prec
  | None -> failwith (Printf.sprintf "No price precision found for symbol %s" symbol)

(** Get minimum quantity for a symbol - synchronous version for internal use *)
let get_qty_min symbol : float option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> info.qty_min)
  with _ -> None

(** Get quantity increment for a symbol - synchronous version for internal use *)
let get_qty_increment symbol : float option =
  try
    Hashtbl.find_opt pair_cache symbol |> Option.map (fun info -> info.qty_increment)
  with _ -> None

