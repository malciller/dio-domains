(** Lighter instruments feed.
    Fetches market metadata from the [GET /api/v1/orderBookDetails] REST endpoint at startup,
    builds a bidirectional mapping between textual symbols and numeric market indices, and validates that all
    user configured symbols exist on the Lighter exchange. This module provides price and quantity
    precision rounding helpers required for formatting outbound order payloads. *)

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

(** Fetch and parse market metadata from the [/api/v1/orderBookDetails] REST endpoint.
    Populates the internal [pair_cache] mapping strings to market info, and [index_to_symbol] mapping integer IDs to string symbols.
    Validates that all symbols in the [required_symbols] list exist; throws an exception to halt startup if validation fails. *)
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
            (* Lighter returns bare symbols for perpetual futures (for example "ETH") and dash separated
               symbols for spot markets (for example "ETH-USDC"). Keep perpetual futures symbols in their raw format to avoid collision
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
      (* Discover spot markets dynamically utilizing the explorer API.
         Spot markets are omitted from the orderBookDetails endpoint which is exclusive to perpetual futures, but
         appear in the explorer's /api/markets endpoint with indices greater than or equal to 2048.
         Register any market absent in the primary pair_cache, deriving precision parameters
         from the corresponding perpetual contract (for example ETH/USDC derives from ETH, LIT/USDC derives from LIT). *)
      let%lwt () = Lwt.catch (fun () ->
        let markets_url = "https://explorer.elliot.ai/api/markets" in
        Logging.info_f ~section "Fetching spot market indices from %s" markets_url;
        let%lwt (_resp, body) = Cohttp_lwt_unix.Client.get (Uri.of_string markets_url) in
        let%lwt body_str = Cohttp_lwt.Body.to_string body in
        let markets_json = Yojson.Safe.from_string body_str in
        let all_markets = match markets_json with
          | `List items -> items
          | _ -> []
        in
        Mutex.lock cache_mutex;
        Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
          List.iter (fun entry ->
            try
              let sym = member "symbol" entry |> to_string in
              let mi = member "market_index" entry |> to_int in
              (* Restrict registration to markets not already populated in the primary pair_cache, isolating spot markets. *)
              if not (Hashtbl.mem pair_cache sym) then begin
                (* Derive the base perpetual symbol from the composite spot symbol: "ETH/USDC" yields "ETH", "LIT/USDC" yields "LIT". *)
                let perp_symbol = match String.split_on_char '/' sym with
                  | base :: _ -> base
                  | _ -> sym
                in
                let perp_info = Hashtbl.find_opt pair_cache perp_symbol in
                let (size_dec, price_dec, min_base, min_quote) = match perp_info with
                  | Some pi -> (pi.supported_size_decimals, pi.supported_price_decimals,
                                pi.min_base_amount, pi.min_quote_amount)
                  | None -> (4, 2, 0.005, 10.0)
                in
                let info : Lighter_types.market_info = {
                  market_index = mi;
                  symbol = sym;
                  supported_size_decimals = size_dec;
                  supported_price_decimals = price_dec;
                  min_base_amount = min_base;
                  min_quote_amount = min_quote;
                  taker_fee = 0.0;
                  maker_fee = 0.0;
                } in
                Hashtbl.replace pair_cache sym info;
                Hashtbl.replace index_to_symbol mi sym;
                Logging.info_f ~section "Registered spot market: %s (index=%d, precision from %s perp)"
                  sym mi perp_symbol
              end else begin
                (* Ensure the index_to_symbol mapping is populated for markets discovered earlier in the execution flow. *)
                if not (Hashtbl.mem index_to_symbol mi) then
                  Hashtbl.replace index_to_symbol mi sym
              end
            with _ -> ()
          ) all_markets
        );
        Logging.info_f ~section "Spot market discovery complete: %d total markets in /api/markets"
          (List.length all_markets);
        Lwt.return_unit
      ) (fun exn ->
        Logging.warn_f ~section "Failed to fetch spot markets from explorer: %s (spot symbols may be unavailable)"
          (Printexc.to_string exn);
        Lwt.return_unit
      ) in

      (* Validate that all symbols specified in the required list exist in the populated cache. *)
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

(** Retrieve instrument metadata via a thread safe lookup utilizing the textual symbol. *)
let lookup_info symbol =
  Mutex.lock cache_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
    Hashtbl.find_opt pair_cache symbol
  )

(** Resolve a textual symbol string to its underlying Lighter exchange integer market_index. *)
let get_market_index ~symbol =
  match lookup_info symbol with
  | Some info -> Some info.market_index
  | None -> None

(** Resolve an integer exchange market_index back to its textual symbol string via a thread safe lookup. *)
let get_symbol ~market_index =
  Mutex.lock cache_mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock cache_mutex) (fun () ->
    Hashtbl.find_opt index_to_symbol market_index
  )

(** Retrieve the minimum allowable price increment for the specified [symbol] to satisfy exchange tick size constraints. *)
let get_price_increment symbol =
  match lookup_info symbol with
  | Some info -> Some (Lighter_types.price_increment_of_decimals info.supported_price_decimals)
  | None -> None

(** Retrieve the minimum allowable quantity increment for the specified [symbol] to satisfy exchange lot size constraints. *)
let get_qty_increment symbol =
  match lookup_info symbol with
  | Some info -> Some (Lighter_types.qty_increment_of_decimals info.supported_size_decimals)
  | None -> None

(** Retrieve the minimum permissible order quantity for the specified [symbol]. *)
let get_qty_min symbol =
  match lookup_info symbol with
  | Some info -> Some info.min_base_amount
  | None -> None

(** Apply rounding arithmetic to truncate a given price to the exchange's valid precision decimal places for the specified [symbol]. *)
let round_price_for_symbol symbol price =
  match lookup_info symbol with
  | Some info -> Lighter_types.round_price ~price_decimals:info.supported_price_decimals price
  | None -> price

(** Apply rounding arithmetic to truncate a given quantity down to the exchange's valid precision decimal places for the specified [symbol]. *)
let round_qty_for_symbol symbol qty =
  match lookup_info symbol with
  | Some info -> Lighter_types.round_qty ~size_decimals:info.supported_size_decimals qty
  | None -> qty

(** Initialize the instrument cache with static mock data for testing environments or non live execution scenarios. *)
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
