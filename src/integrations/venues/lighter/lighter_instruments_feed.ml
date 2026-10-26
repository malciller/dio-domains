[@@@ocaml.text
  " Lighter instruments feed. Fetches market metadata from the\n\
  \    [GET /api/v1/orderBookDetails] REST endpoint at startup, builds a bidirectional\n\
  \    mapping between textual symbols and numeric market indices, and validates that all\n\
  \    user configured symbols exist on the Lighter exchange. Also provides price and\n\
  \    quantity precision rounding helpers for outbound order payloads. "]

let section = "lighter_instruments_feed"
let pair_cache : (string, Lighter_types.market_info) Hashtbl.t = Hashtbl.create 32
let index_to_symbol : (int, string) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Mutex.create ()

(** Published immutable copies for lock-free readers. Writes stay under [cache_mutex];
    each publish replaces the whole table, so a reader never sees a partially updated
    map. *)
let pair_snapshot : (string, Lighter_types.market_info) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 32)
;;

let index_snapshot : (int, string) Hashtbl.t Atomic.t =
  Atomic.make (Hashtbl.create 32)
;;

let publish () =
  Atomic.set pair_snapshot (Hashtbl.copy pair_cache);
  Atomic.set index_snapshot (Hashtbl.copy index_to_symbol)
;;
let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let wait_until_ready () =
  if Atomic.get is_ready then Lwt.return_unit else Lwt_condition.wait ready_condition
;;

let notify_ready () =
  Atomic.set is_ready true;
  Lwt_condition.broadcast ready_condition ()
;;

let fetch_and_initialize ~base_url ~required_symbols =
  let url = base_url ^ "/api/v1/orderBookDetails" in
  Logging.info_f ~section "Fetching Lighter instruments from %s..." url;
  let fetch =
    Lwt.catch
      (fun () ->
        let uri = Uri.of_string url in
        let __ppx_lwt_0 = Cohttp_lwt_unix.Client.get uri in
        Lwt.backtrace_bind
          "src/integrations/venues/lighter/lighter_instruments_feed.ml"
          37
          (fun exn ->
            try Lwt.reraise exn with
            | exn -> exn)
          __ppx_lwt_0
          (fun (_resp, body) ->
            let __ppx_lwt_0 = Cohttp_lwt.Body.to_string body in
            Lwt.backtrace_bind
              "src/integrations/venues/lighter/lighter_instruments_feed.ml"
              38
              (fun exn ->
                try Lwt.reraise exn with
                | exn -> exn)
              __ppx_lwt_0
              (fun body_str ->
                let json = Yojson.Safe.from_string body_str in
                let open Yojson.Safe.Util in
                let markets = member "order_book_details" json |> to_list in
                Mutex.lock cache_mutex;
                Fun.protect
                  ~finally:(fun () -> Mutex.unlock cache_mutex)
                  (fun () ->
                    List.iter
                      (fun market ->
                        try
                          let market_index = member "market_id" market |> to_int in
                          let raw_symbol = member "symbol" market |> to_string in
                          let symbol = raw_symbol in
                          let supported_size_decimals =
                            member "supported_size_decimals" market |> to_int
                          in
                          let supported_price_decimals =
                            member "supported_price_decimals" market |> to_int
                          in
                          let min_base_amount =
                            Lighter_types.parse_json_float
                              (member "min_base_amount" market)
                          in
                          let min_quote_amount =
                            Lighter_types.parse_json_float
                              (member "min_quote_amount" market)
                          in
                          let taker_fee =
                            try
                              Lighter_types.parse_json_float (member "taker_fee" market)
                            with
                            | _ -> 0.0
                          in
                          let maker_fee =
                            try
                              Lighter_types.parse_json_float (member "maker_fee" market)
                            with
                            | _ -> 0.0
                          in
                          let maintenance_margin_fraction =
                            try
                              Lighter_types.parse_json_float
                                (member "maintenance_margin_fraction" market)
                            with
                            | _ -> 0.0
                          in
                          let min_initial_margin_fraction =
                            try
                              Lighter_types.parse_json_float
                                (member "min_initial_margin_fraction" market)
                            with
                            | _ -> 0.0
                          in
                          let info : Lighter_types.market_info =
                            { market_index
                            ; symbol
                            ; supported_size_decimals
                            ; supported_price_decimals
                            ; min_base_amount
                            ; min_quote_amount
                            ; maintenance_margin_fraction
                            ; min_initial_margin_fraction
                            ; taker_fee
                            ; maker_fee
                            }
                          in
                          Hashtbl.replace pair_cache symbol info;
                          Hashtbl.replace index_to_symbol market_index symbol;
                          Logging.debug_f
                            ~section
                            "Registered market: %s (raw=%s, index=%d, price_dec=%d, \
                             size_dec=%d)"
                            symbol
                            raw_symbol
                            market_index
                            supported_price_decimals
                            supported_size_decimals
                        with
                        | exn ->
                          Logging.warn_f
                            ~section
                            "Failed to parse market entry: %s"
                            (Printexc.to_string exn))
                      markets);
                publish ();
                let __ppx_lwt_0 =
                  Lwt.catch
                    (fun () ->
                      let markets_url = "https://explorer.elliot.ai/api/markets" in
                      Logging.info_f
                        ~section
                        "Fetching spot market indices from %s"
                        markets_url;
                      let __ppx_lwt_0 =
                        Cohttp_lwt_unix.Client.get (Uri.of_string markets_url)
                      in
                      Lwt.backtrace_bind
                        "src/integrations/venues/lighter/lighter_instruments_feed.ml"
                        113
                        (fun exn ->
                          try Lwt.reraise exn with
                          | exn -> exn)
                        __ppx_lwt_0
                        (fun (_resp, body) ->
                          let __ppx_lwt_0 = Cohttp_lwt.Body.to_string body in
                          Lwt.backtrace_bind
                            "src/integrations/venues/lighter/lighter_instruments_feed.ml"
                            116
                            (fun exn ->
                              try Lwt.reraise exn with
                              | exn -> exn)
                            __ppx_lwt_0
                            (fun body_str ->
                              let markets_json = Yojson.Safe.from_string body_str in
                              let all_markets =
                                match markets_json with
                                | `List items -> items
                                | _ -> []
                              in
                              Mutex.lock cache_mutex;
                              Fun.protect
                                ~finally:(fun () -> Mutex.unlock cache_mutex)
                                (fun () ->
                                  List.iter
                                    (fun entry ->
                                      try
                                        let sym = member "symbol" entry |> to_string in
                                        let mi = member "market_index" entry |> to_int in
                                        if not (Hashtbl.mem pair_cache sym)
                                        then (
                                          let perp_symbol =
                                            match String.split_on_char '/' sym with
                                            | base :: _ -> base
                                            | _ -> sym
                                          in
                                          let perp_info =
                                            Hashtbl.find_opt pair_cache perp_symbol
                                          in
                                          let ( size_dec
                                              , price_dec
                                              , min_base
                                              , min_quote
                                              , mmf
                                              , min_imf )
                                            =
                                            match perp_info with
                                            | Some pi ->
                                              ( pi.supported_size_decimals
                                              , pi.supported_price_decimals
                                              , pi.min_base_amount
                                              , pi.min_quote_amount
                                              , pi.maintenance_margin_fraction
                                              , pi.min_initial_margin_fraction )
                                            | None -> 4, 2, 0.005, 10.0, 0.0, 0.0
                                          in
                                          let info : Lighter_types.market_info =
                                            { market_index = mi
                                            ; symbol = sym
                                            ; supported_size_decimals = size_dec
                                            ; supported_price_decimals = price_dec
                                            ; min_base_amount = min_base
                                            ; min_quote_amount = min_quote
                                            ; maintenance_margin_fraction = mmf
                                            ; min_initial_margin_fraction = min_imf
                                            ; taker_fee = 0.0
                                            ; maker_fee = 0.0
                                            }
                                          in
                                          Hashtbl.replace pair_cache sym info;
                                          Hashtbl.replace index_to_symbol mi sym;
                                          Logging.info_f
                                            ~section
                                            "Registered spot market: %s (index=%d, \
                                             precision from %s perp)"
                                            sym
                                            mi
                                            perp_symbol)
                                        else if not (Hashtbl.mem index_to_symbol mi)
                                        then Hashtbl.replace index_to_symbol mi sym
                                      with
                                      | _ -> ())
                                    all_markets);
                              publish ();
                              Logging.info_f
                                ~section
                                "Spot market discovery complete: %d total markets in \
                                 /api/markets"
                                (List.length all_markets);
                              Lwt.return_unit)))
                    (fun exn ->
                      Logging.warn_f
                        ~section
                        "Failed to fetch spot markets from explorer: %s (spot symbols \
                         may be unavailable)"
                        (Printexc.to_string exn);
                      Lwt.return_unit)
                in
                Lwt.backtrace_bind
                  "src/integrations/venues/lighter/lighter_instruments_feed.ml"
                  108
                  (fun exn ->
                    try Lwt.reraise exn with
                    | exn -> exn)
                  __ppx_lwt_0
                  (fun () ->
                    let missing =
                      List.filter
                        (fun sym ->
                          Mutex.lock cache_mutex;
                          Fun.protect
                            ~finally:(fun () -> Mutex.unlock cache_mutex)
                            (fun () -> not (Hashtbl.mem pair_cache sym)))
                        required_symbols
                    in
                    if missing <> []
                    then (
                      let missing_str = String.concat ", " missing in
                      Logging.error_f
                        ~section
                        "FATAL: Required symbols not found on Lighter: [%s]"
                        missing_str;
                      Logging.error_f
                        ~section
                        "Available symbols: [%s]"
                        (Mutex.lock cache_mutex;
                         let syms =
                           Fun.protect
                             ~finally:(fun () -> Mutex.unlock cache_mutex)
                             (fun () ->
                               Hashtbl.fold (fun sym _ acc -> sym :: acc) pair_cache [])
                         in
                         String.concat ", " syms);
                      failwith
                        (Printf.sprintf
                           "Lighter: required symbols not found: %s"
                           missing_str));
                    Logging.info_f
                      ~section
                      "Initialized Lighter instruments feed with %d markets (%d required \
                       symbols validated)"
                      (List.length markets)
                      (List.length required_symbols);
                    notify_ready ();
                    Lwt.return_unit))))
      (fun exn ->
        Logging.error_f
          ~section
          "Failed to fetch orderBookDetails: %s"
          (Printexc.to_string exn);
        notify_ready ();
        Lwt.return_unit)
  in
  let timeout =
    Lwt_unix.sleep 10.0
    |> Lwt.map (fun () ->
      Logging.error_f ~section "Instruments fetch timed out after 10s";
      notify_ready ())
  in
  Lwt.pick [ fetch; timeout ]
[@@ocaml.doc
  " Fetch and parse market metadata from the [/api/v1/orderBookDetails] REST endpoint.\n\
  \    Populates the internal [pair_cache] mapping strings to market info, and\n\
  \    [index_to_symbol] mapping integer IDs to string symbols. Validates that all \
   symbols in\n\
  \    the [required_symbols] list exist; throws an exception to halt startup if \
   validation\n\
  \    fails. "]
;;

let lookup_info symbol = Hashtbl.find_opt (Atomic.get pair_snapshot) symbol
[@@ocaml.doc " Lock-free instrument metadata lookup by symbol. "]
;;

let get_market_index ~symbol =
  match lookup_info symbol with
  | Some info -> Some info.market_index
  | None -> None
[@@ocaml.doc " Symbol to Lighter market index. "]
;;

let get_symbol ~market_index = Hashtbl.find_opt (Atomic.get index_snapshot) market_index
[@@ocaml.doc " Lock-free market index to symbol. "]
;;

let get_price_increment symbol =
  match lookup_info symbol with
  | Some info ->
    Some (Lighter_types.price_increment_of_decimals info.supported_price_decimals)
  | None -> None
[@@ocaml.doc " Tick size for [symbol], derived from price decimals. "]
;;

let get_qty_increment symbol =
  match lookup_info symbol with
  | Some info ->
    Some (Lighter_types.qty_increment_of_decimals info.supported_size_decimals)
  | None -> None
[@@ocaml.doc " Lot size for [symbol], derived from size decimals. "]
;;

let get_qty_min symbol =
  match lookup_info symbol with
  | Some info -> Some info.min_base_amount
  | None -> None
[@@ocaml.doc " Minimum order quantity for [symbol]. "]
;;

let get_maintenance_margin_fraction symbol =
  match lookup_info symbol with
  | Some info -> Some info.maintenance_margin_fraction
  | None -> None
[@@ocaml.doc " Maintenance margin fraction in basis points for [symbol] (120 = 1.2%). "]
;;

let round_price_for_symbol symbol price =
  match lookup_info symbol with
  | Some info ->
    Lighter_types.round_price ~price_decimals:info.supported_price_decimals price
  | None -> price
[@@ocaml.doc " Round price to tick precision for [symbol]. "]
;;

let round_qty_for_symbol symbol qty =
  match lookup_info symbol with
  | Some info -> Lighter_types.round_qty ~size_decimals:info.supported_size_decimals qty
  | None -> qty
[@@ocaml.doc " Round qty down to lot precision for [symbol]. "]
;;

let initialize
  ?(maintenance_margin_fraction = 0.0)
  ?(min_initial_margin_fraction = 0.0)
  symbols
  =
  Mutex.lock cache_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock cache_mutex)
    (fun () ->
      List.iteri
        (fun idx symbol ->
          let info : Lighter_types.market_info =
            { market_index = idx
            ; symbol
            ; supported_size_decimals = 4
            ; supported_price_decimals = 2
            ; min_base_amount = 0.001
            ; min_quote_amount = 1.0
            ; maintenance_margin_fraction
            ; min_initial_margin_fraction
            ; taker_fee = 0.0
            ; maker_fee = 0.0
            }
          in
          Hashtbl.replace pair_cache symbol info;
          Hashtbl.replace index_to_symbol idx symbol)
        symbols);
  publish ();
  Logging.info_f
    ~section
    "Initialized Lighter instruments feed with %d mock symbols"
    (List.length symbols)
[@@ocaml.doc " Populate the cache with mock data for tests or non-live runs. "]
;;
