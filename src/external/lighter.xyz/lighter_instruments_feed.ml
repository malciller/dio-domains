(** Lighter instruments feed.
    Fetches market metadata from [GET /api/v1/orderBookDetails] at startup,
    builds a bidirectional symbol↔market_index map, and validates that all
    user-configured symbols exist on Lighter. Provides price/quantity
    precision helpers for downstream consumers. *)

let section = "lighter_instruments_feed"

let pair_cache : (string, Lighter_types.market_info) Hashtbl.t = Hashtbl.create 32
let index_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Mutex.create ()

let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let wait_until_ready () =
  if Atomic.get is_ready then Lwt.return_unit
  else Lwt_condition.wait ready_condition

let notify_ready () =
  Atomic.set is_ready true;
  Lwt_condition.broadcast ready_condition ()

(** Fetch and parse market metadata from [/api/v1/orderBookDetails].
    Populates [pair_cache] and [index_to_symbol] maps.
    Validates that all [required_symbols] exist; fails loudly if not. *)
let fetch_and_initialize ~base_url ~required_symbols =
  let url = base_url ^ "/api/v1/orderBookDetails" in
  Logging.info_f ~section "Fetching Lighter instruments from %s..." url;
  let fetch =
    Lwt.catch (fun () ->
      let uri = Uri.of_string url in
      let%lwt (_resp, body) = Cohttp_lwt_unix.Client.get uri in
      let%lwt body_str = Cohttp_lwt.Body.to_string body in
      let json = Yojson.Safe.from_string body_str in
      let open Yojson.Safe.Util in

      let markets = member "order_book_details" json |> to_list in
      Mutex.lock cache_mutex;
      Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
        List.iter (fun market ->
          try
            let market_index = member "market_id" market |> to_int in
            let raw_symbol = member "symbol" market |> to_string in
            (* Lighter returns bare symbols for perps (e.g. "ETH") and dash-separated
               for spot (e.g. "ETH-USDC"). Keep perp symbols as-is to avoid collision
               with spot ETH/USDC which we register separately at index 2048. *)
            let symbol = raw_symbol in
            let supported_size_decimals = member "supported_size_decimals" market |> to_int in
            let supported_price_decimals = member "supported_price_decimals" market |> to_int in
            let min_base_amount = Lighter_types.parse_json_float (member "min_base_amount" market) in
            let min_quote_amount = Lighter_types.parse_json_float (member "min_quote_amount" market) in
            let taker_fee = (try Lighter_types.parse_json_float (member "taker_fee" market) with _ -> 0.0) in
            let maker_fee = (try Lighter_types.parse_json_float (member "maker_fee" market) with _ -> 0.0) in
            let info : Lighter_types.market_info = {
              market_index;
              symbol;
              supported_size_decimals;
              supported_price_decimals;
              min_base_amount;
              min_quote_amount;
              taker_fee;
              maker_fee;
            } in
            Hashtbl.replace pair_cache symbol info;
            Hashtbl.replace index_to_symbol market_index symbol;
            Logging.debug_f ~section "Registered market: %s (raw=%s, index=%d, price_dec=%d, size_dec=%d)"
              symbol raw_symbol market_index supported_price_decimals supported_size_decimals
          with exn ->
            Logging.warn_f ~section "Failed to parse market entry: %s" (Printexc.to_string exn)
        ) markets
      );
      (* Spot markets are not listed in orderBookDetails but use indices >= 2048.
         Register known spot markets explicitly. Use perp market's precision as baseline. *)
      let spot_markets = [
        ("ETH/USDC", 2048, "ETH");  (* spot ETH-USDC *)
      ] in
      List.iter (fun (spot_symbol, spot_index, perp_symbol) ->
        let perp_info = Hashtbl.find_opt pair_cache perp_symbol in
        let (size_dec, price_dec, min_base, min_quote) = match perp_info with
          | Some pi -> (pi.supported_size_decimals, pi.supported_price_decimals,
                        pi.min_base_amount, pi.min_quote_amount)
          | None -> (4, 2, 0.005, 10.0)  (* fallback defaults *)
        in
        let info : Lighter_types.market_info = {
          market_index = spot_index;
          symbol = spot_symbol;
          supported_size_decimals = size_dec;
          supported_price_decimals = price_dec;
          min_base_amount = min_base;
          min_quote_amount = min_quote;
          taker_fee = 0.0;
          maker_fee = 0.0;
        } in
        Hashtbl.replace pair_cache spot_symbol info;
        Hashtbl.replace index_to_symbol spot_index spot_symbol;
        Logging.info_f ~section "Registered spot market: %s (index=%d, copied precision from %s perp)"
          spot_symbol spot_index perp_symbol
      ) spot_markets;

      (* Validate all required symbols exist *)
      let missing = List.filter (fun sym ->
        Mutex.lock cache_mutex;
        Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
          not (Hashtbl.mem pair_cache sym)
        )
      ) required_symbols in

      if missing <> [] then begin
        let missing_str = String.concat ", " missing in
        Logging.error_f ~section "FATAL: Required symbols not found on Lighter: [%s]" missing_str;
        Logging.error_f ~section "Available symbols: [%s]"
          (Mutex.lock cache_mutex;
           let syms = Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
             Hashtbl.fold (fun sym _ acc -> sym :: acc) pair_cache []
           ) in
           String.concat ", " syms);
        failwith (Printf.sprintf "Lighter: required symbols not found: %s" missing_str)
      end;

      Logging.info_f ~section "Initialized Lighter instruments feed with %d markets (%d required symbols validated)"
        (List.length markets) (List.length required_symbols);

      notify_ready ();
      Lwt.return_unit
    ) (fun exn ->
      Logging.error_f ~section "Failed to fetch orderBookDetails: %s" (Printexc.to_string exn);
      notify_ready ();
      Lwt.return_unit
    )
  in
  let timeout =
    Lwt_unix.sleep 10.0 |> Lwt.map (fun () ->
      Logging.error_f ~section "Instruments fetch timed out after 10s";
      notify_ready ()
    )
  in
  Lwt.pick [fetch; timeout]

(** Lookup instrument info by symbol. *)
let lookup_info symbol =
  Mutex.lock cache_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
    Hashtbl.find_opt pair_cache symbol
  )

(** Resolve a symbol string to its Lighter market_index. *)
let get_market_index ~symbol =
  match lookup_info symbol with
  | Some info -> Some info.market_index
  | None -> None

(** Resolve a market_index back to a symbol string. *)
let get_symbol ~market_index =
  Mutex.lock cache_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
    Hashtbl.find_opt index_to_symbol market_index
  )

(** Returns the minimum price increment for [symbol]. *)
let get_price_increment symbol =
  match lookup_info symbol with
  | Some info -> Some (Lighter_types.price_increment_of_decimals info.supported_price_decimals)
  | None -> None

(** Returns the minimum quantity increment for [symbol]. *)
let get_qty_increment symbol =
  match lookup_info symbol with
  | Some info -> Some (Lighter_types.qty_increment_of_decimals info.supported_size_decimals)
  | None -> None

(** Returns the minimum order quantity for [symbol]. *)
let get_qty_min symbol =
  match lookup_info symbol with
  | Some info -> Some info.min_base_amount
  | None -> None

(** Round price to the exchange's valid precision for [symbol]. *)
let round_price_for_symbol symbol price =
  match lookup_info symbol with
  | Some info -> Lighter_types.round_price ~price_decimals:info.supported_price_decimals price
  | None -> price

(** Round quantity down to the exchange's valid precision for [symbol]. *)
let round_qty_for_symbol symbol qty =
  match lookup_info symbol with
  | Some info -> Lighter_types.round_qty ~size_decimals:info.supported_size_decimals qty
  | None -> qty

(** Initialize with mock data for non-live scenarios. *)
let initialize symbols =
  Mutex.lock cache_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
    List.iteri (fun idx symbol ->
      let info : Lighter_types.market_info = {
        market_index = idx;
        symbol;
        supported_size_decimals = 4;
        supported_price_decimals = 2;
        min_base_amount = 0.001;
        min_quote_amount = 1.0;
        taker_fee = 0.0;
        maker_fee = 0.0;
      } in
      Hashtbl.replace pair_cache symbol info;
      Hashtbl.replace index_to_symbol idx symbol
    ) symbols
  );
  Logging.info_f ~section "Initialized Lighter instruments feed with %d mock symbols" (List.length symbols)
