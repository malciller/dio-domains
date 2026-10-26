(** Lighter data-venue adapter for the capital oracle; implements
    [Exchange_intf.Oracle.S].

    Sources: daily candles from [GET /api/v1/candles] paginated forward in day-windows;
    per-market fees and minimum notional from [orderBookDetails]; account balances from
    [GET /api/v1/account]. All HTTP calls are timeout-bounded. Mainnet-only, matching the
    live adapter. *)

open Lwt.Infix
module Exchange = Dio_exchange.Exchange_intf

let section = "oracle_lighter"
let default_timeout = 10.0
let ms_per_day = 86_400_000L
let window_days = 500
let max_windows = 20
let default_start_ms = 1_704_067_200_000L (* 2024-01-01 *)

let get_json path =
  Lwt.catch
    (fun () ->
      Lwt_unix.with_timeout default_timeout (fun () ->
        Cohttp_lwt_unix.Client.get (Uri.of_string (Lighter_endpoints.api_base_url ^ path)))
      >>= fun (response, body) ->
      Cohttp_lwt.Body.to_string body
      >|= fun body_str ->
      let status = Cohttp.Response.status response |> Cohttp.Code.code_of_status in
      if status < 200 || status >= 300
      then Error (Printf.sprintf "HTTP %d: %s" status body_str)
      else (
        try Ok (Yojson.Safe.from_string body_str) with
        | exn ->
          Error (Printf.sprintf "invalid JSON response: %s" (Printexc.to_string exn))))
    (fun exn -> Lwt.return (Error (Printexc.to_string exn)))
;;

let ms_of_iso (date : string) : int64 =
  let y, m, d = Exchange.Types.iso_ymd date in
  Int64.mul (Int64.of_int (Exchange.Types.days_from_civil y m d)) ms_per_day
;;

let iso_of_ms (t : int64) =
  let days = Int64.to_int (Int64.div t ms_per_day) in
  let y, m, d = Exchange.Types.civil_from_days days in
  Printf.sprintf "%04d-%02d-%02d" y m d
;;

let float_of_json = function
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

let int_of_json = function
  | `Int i -> Some i
  | `Intlit s ->
    (try Some (int_of_string s) with
     | _ -> None)
  | `Float f -> Some (int_of_float f)
  | `String s ->
    (try Some (int_of_string (String.trim s)) with
     | _ -> None)
  | _ -> None
;;

let parse_candle (j : Yojson.Safe.t) : Exchange.Types.bar option =
  let open Yojson.Safe.Util in
  try
    let num key =
      match float_of_json (member key j) with
      | Some f -> f
      | None -> raise Exit
    in
    let t =
      match int_of_json (member "t" j) with
      | Some t -> t
      | None -> raise Exit
    in
    Some
      { date = iso_of_ms (Int64.of_int t)
      ; open_ = num "o"
      ; high = num "h"
      ; low = num "l"
      ; close = num "c"
      ; volume = Option.value (float_of_json (member "v" j)) ~default:0.0
      }
  with
  | _ -> None
;;

(** Parses a [Candles] envelope ([{"code":200,"r":"1d","c":[...]}]). *)
let parse_candles (json : Yojson.Safe.t) : Exchange.Types.bar list =
  let open Yojson.Safe.Util in
  let rows =
    match member "c" json with
    | `List rows -> rows
    | _ ->
      (match json with
       | `List rows -> rows
       | _ -> [])
  in
  List.filter_map parse_candle rows
;;

(** Sorts and de-duplicates fetched windows by date. Source normalization stays central in
    the oracle. *)
let windows_to_series (windows : Exchange.Types.bar list list) : Exchange.Types.bar list =
  let sorted =
    List.concat windows
    |> List.sort (fun (a : Exchange.Types.bar) b -> String.compare a.date b.date)
  in
  let rec dedup = function
    | (a : Exchange.Types.bar) :: ((b : Exchange.Types.bar) :: _ as rest)
      when a.date = b.date -> dedup rest
    | (a : Exchange.Types.bar) :: rest -> a :: dedup rest
    | [] -> []
  in
  dedup sorted
;;

type market_meta =
  { market_id : int
  ; maker_fee : float
  ; taker_fee : float
  ; min_quote_amount : float
  }

let market_meta : (string, market_meta) Hashtbl.t = Hashtbl.create 128
let market_meta_mutex = Mutex.create ()
let market_meta_loaded = Atomic.make false

let cached_market_meta symbol =
  Mutex.lock market_meta_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock market_meta_mutex)
    (fun () -> Hashtbl.find_opt market_meta symbol)
;;

let store_market_meta symbol meta =
  Mutex.lock market_meta_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock market_meta_mutex)
    (fun () -> Hashtbl.replace market_meta symbol meta)
;;

let load_market_meta () =
  if Atomic.get market_meta_loaded
  then Lwt.return (Ok ())
  else
    get_json "/api/v1/orderBookDetails"
    >|= function
    | Error error -> Error error
    | Ok json ->
      let open Yojson.Safe.Util in
      let perps =
        match member "order_book_details" json with
        | `List entries -> entries
        | _ -> []
      in
      let spots =
        match member "spot_order_book_details" json with
        | `List entries -> entries
        | _ -> []
      in
      let entries = perps @ spots in
      (match entries with
       | _ :: _ ->
         List.iter
           (fun entry ->
             match to_string_option (member "symbol" entry) with
             | Some symbol ->
               (match int_of_json (member "market_id" entry) with
                | Some market_id ->
                  store_market_meta
                    symbol
                    { market_id
                    ; maker_fee =
                        Option.value
                          (float_of_json (member "maker_fee" entry))
                          ~default:0.0
                    ; taker_fee =
                        Option.value
                          (float_of_json (member "taker_fee" entry))
                          ~default:0.0
                    ; min_quote_amount =
                        Option.value
                          (float_of_json (member "min_quote_amount" entry))
                          ~default:0.0
                    }
                | None -> ())
             | None -> ())
           entries;
         Atomic.set market_meta_loaded true
       | [] -> ());
      Ok ()
;;

let resolve_market_index symbol =
  match Lighter_instruments_feed.get_market_index ~symbol with
  | Some market_id -> Lwt.return (Ok market_id)
  | None ->
    (match cached_market_meta symbol with
     | Some meta -> Lwt.return (Ok meta.market_id)
     | None ->
       load_market_meta ()
       >|= fun loaded ->
       (match loaded with
        | Error error -> Error error
        | Ok () ->
          (match cached_market_meta symbol with
           | Some meta -> Ok meta.market_id
           | None -> Error (Printf.sprintf "unknown Lighter market %s" symbol))))
;;

let fetch_page ~market_id ~start_ms ~end_ms =
  let path =
    Printf.sprintf
      "/api/v1/candles?market_id=%d&resolution=1d&start_timestamp=%Ld&end_timestamp=%Ld&count_back=500"
      market_id
      start_ms
      end_ms
  in
  get_json path
  >|= function
  | Error error -> Error error
  | Ok json ->
    let open Yojson.Safe.Util in
    (match member "code" json with
     | `Int 200 | `String "200" | `Null -> Ok (parse_candles json)
     | _ ->
       Error (Printf.sprintf "Lighter candles rejected: %s" (Yojson.Safe.to_string json)))
;;

let calendar_kind = Exchange.Types.Crypto
let fetch_calendar ~start_date:_ ~end_date:_ : string list Lwt.t = Lwt.return []

let clip_from from bars =
  match from with
  | None -> bars
  | Some from -> List.filter (fun (bar : Exchange.Types.bar) -> bar.date >= from) bars
;;

let fetch_bars ?feed:_ ?end_date:_ ~from ~symbol () : Exchange.Types.bar list Lwt.t =
  resolve_market_index symbol
  >>= function
  | Error error ->
    Logging.warn_f ~section "Lighter candle lookup failed for %s (%s)" symbol error;
    Lwt.return []
  | Ok market_id ->
    let start_ms = Option.fold ~none:default_start_ms ~some:ms_of_iso from in
    let now_ms = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
    let span = Int64.mul ms_per_day (Int64.of_int window_days) in
    let finish acc = clip_from from (windows_to_series acc) in
    let rec go from_ms acc windows =
      if windows = 0
      then Lwt.return (finish acc)
      else (
        let to_ms = Int64.min (Int64.add from_ms span) now_ms in
        fetch_page ~market_id ~start_ms:from_ms ~end_ms:to_ms
        >>= function
        | Error error ->
          Logging.warn_f
            ~section
            "Lighter candle page failed for %s (%s), returning %d window(s) so far"
            symbol
            error
            (List.length acc);
          Lwt.return (finish acc)
        | Ok bars ->
          if to_ms >= now_ms
          then Lwt.return (finish (bars :: acc))
          else go to_ms (bars :: acc) (windows - 1))
    in
    go start_ms [] max_windows
;;

let default_fees ~symbol:_ : float * float = 0.0, 0.0

let fetch_fees ~testnet:_ ~symbol : (float * float) Lwt.t =
  match Lighter_instruments_feed.lookup_info symbol with
  | Some info -> Lwt.return (info.maker_fee, info.taker_fee)
  | None ->
    (match cached_market_meta symbol with
     | Some meta -> Lwt.return (meta.maker_fee, meta.taker_fee)
     | None ->
       load_market_meta ()
       >|= fun _ ->
       (match cached_market_meta symbol with
        | Some meta -> meta.maker_fee, meta.taker_fee
        | None -> default_fees ~symbol))
;;

let account_index () =
  match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
  | Some s ->
    (try Some (int_of_string s) with
     | _ -> None)
  | None -> None
;;

let normalize_asset symbol =
  let symbol = String.uppercase_ascii (String.trim symbol) in
  if symbol = "USDC" then "USDC" else symbol
;;

let parse_account_balances (json : Yojson.Safe.t)
  : ((string * float * float) list, string) result
  =
  let open Yojson.Safe.Util in
  match member "accounts" json with
  | `List (account :: _) ->
    let opt key = float_of_json (member key account) in
    let assets =
      match member "assets" account with
      | `List entries -> entries
      | _ -> []
    in
    let parsed =
      List.filter_map
        (fun asset ->
          match to_string_option (member "symbol" asset) with
          | None -> None
          | Some symbol ->
            let balance =
              Option.value (float_of_json (member "balance" asset)) ~default:0.0
            in
            let locked =
              Option.value (float_of_json (member "locked_balance" asset)) ~default:0.0
            in
            let margin =
              Option.value (float_of_json (member "margin_balance" asset)) ~default:0.0
            in
            let total = balance +. margin in
            let available = Float.max 0.0 (balance -. locked) +. margin in
            if total = 0.0 && available = 0.0
            then None
            else Some (normalize_asset symbol, available, total))
        assets
    in
    if List.exists (fun (asset, _, _) -> asset = "USDC") parsed
    then Ok parsed
    else (
      let collateral = Option.value (opt "collateral") ~default:0.0 in
      let available_balance = Option.value (opt "available_balance") ~default:0.0 in
      let usdc_available = Float.max available_balance 0.0 in
      let usdc_total =
        match opt "total_asset_value" with
        | Some value -> Some value
        | None ->
          let value = Float.max collateral usdc_available in
          if value > 0.0 then Some value else None
      in
      match usdc_total with
      | Some total -> Ok (("USDC", usdc_available, total) :: parsed)
      | None -> Ok parsed)
  | _ -> Error "Lighter account response has no accounts list"
;;

let fetch_balances ~testnet:_ : ((string * float * float) list, string) result Lwt.t =
  match account_index () with
  | None -> Lwt.return (Error "LIGHTER_ACCOUNT_INDEX is not set")
  | Some index ->
    get_json (Printf.sprintf "/api/v1/account?by=index&value=%d" index)
    >|= (function
     | Error error -> Error error
     | Ok json -> parse_account_balances json)
;;

(** The live store holds total collateral, not the hold-netted available view; the oracle
    uses REST [fetch_balances] so live and modeled capital agree. *)
let live_balances () : (string * float * float) list option = None

let default_quote = "USDC"

let min_notional ~symbol =
  match Lighter_instruments_feed.lookup_info symbol with
  | Some info when info.min_quote_amount > 0.0 -> info.min_quote_amount
  | _ ->
    (match cached_market_meta symbol with
     | Some meta when meta.min_quote_amount > 0.0 -> meta.min_quote_amount
     | _ -> 10.0)
;;

let init_instruments ~testnet:_ ~symbols : unit Lwt.t =
  Lwt.catch
    (fun () ->
      Lighter_instruments_feed.fetch_and_initialize
        ~base_url:Lighter_endpoints.api_base_url
        ~required_symbols:symbols)
    (fun exn ->
      Logging.warn_f
        ~section
        "Lighter instrument initialization failed (%s)"
        (Printexc.to_string exn);
      Lwt.return_unit)
;;

let name = "lighter"
(* Registration happens in [Lighter_module] (a module cannot register itself: the wrapped
   self-path would dangle). *)
