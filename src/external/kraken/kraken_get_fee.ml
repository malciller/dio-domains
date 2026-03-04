open Lwt.Infix
open Cohttp_lwt_unix

let section = "kraken_get_fee"
let endpoint = "https://api.kraken.com"

type fee_info = {
  exchange: string;
  maker_fee: float option;
  taker_fee: float option;
}

(** Normalize symbol - Kraken accepts symbols as-is like "BTC/USD" or "BTCUSD" *)
let symbol_to_kraken_pair = String.(fun s -> uppercase_ascii (trim s))

(** Load API credentials from .env file *)
let get_api_credentials_from_env () : (string * string) Lwt.t =
  Lwt.catch (fun () -> Dotenv.export ~path:".env" (); Lwt.return_unit) (fun _ -> Lwt.return_unit) >>= fun () ->
  let get_env var = match Sys.getenv_opt var with
    | Some v -> Lwt.return v | None -> Lwt.fail_with (Printf.sprintf "Missing environment variable: %s" var) in
  Lwt.both (get_env "KRAKEN_API_KEY") (get_env "KRAKEN_API_SECRET")

(* TODO: Function is too long and complex - consider breaking into smaller functions *)
(** Get actual account fee schedule from TradeVolume endpoint *)
let get_fee_info symbol : fee_info option Lwt.t =
  Lwt.catch (fun () ->
    get_api_credentials_from_env () >>= fun (api_key, api_secret) ->
    let path = "/0/private/TradeVolume" in
    let nonce = Kraken_common_types.nonce () in
    let kraken_pair = symbol_to_kraken_pair symbol in
    Logging.debug_f ~section "Requesting fees for %s (Kraken pair: %s)" symbol kraken_pair;
    let encoded_body = Uri.encoded_of_query [("nonce", [nonce]); ("pair", [kraken_pair])] in
    let signature = Kraken_common_types.sign ~secret:api_secret ~path ~body:encoded_body ~nonce in
    let headers = Cohttp.Header.of_list [("API-Key", api_key); ("API-Sign", signature); ("Content-Type", "application/x-www-form-urlencoded")] in
    Client.post ~headers ~body:(Cohttp_lwt.Body.of_string encoded_body) 
      (Uri.of_string (endpoint ^ path)) >>= fun (resp, body) ->
      
    Cohttp_lwt.Body.to_string body >>= fun body_str ->
    let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
    if status <> 200 then
      (Logging.error_f ~section "TradeVolume HTTP %d: %s" status body_str; Lwt.return_none)
    else begin
      let open Yojson.Safe.Util in
      let json = Yojson.Safe.from_string body_str in
      match member "error" json with
      | `List (_ :: _ as errs) ->
          Logging.error_f ~section "TradeVolume API error: %s" (errs |> filter_string |> String.concat "; ");
          Lwt.return_none
      | _ ->
          let result = member "result" json in
          let pair_keys = try to_assoc (member "fees" result) |> List.map fst with _ -> [] in
          let pair_key_opt = List.find_opt (fun k -> k <> "fee" && k <> "volume") pair_keys in
          (match pair_key_opt with
          | None -> Logging.warn_f ~section "No pair data found in TradeVolume response for %s" symbol; Lwt.return_none
          | Some pair_key ->
              let safe_fee obj = try Some (member pair_key obj |> member "fee" |> to_string |> float_of_string) with _ -> None in
              let taker = safe_fee (member "fees" result) in
              let maker = match safe_fee (member "fees_maker" result) with None -> taker | some -> some in
              (match taker, maker with
              | Some t, Some m ->
                  Logging.debug_f ~section "Retrieved account fees for %s (pair=%s): maker=%.4f%%, taker=%.4f%%" symbol pair_key m t;
                  Lwt.return (Some { exchange = "kraken"; maker_fee = Some (m /. 100.0); taker_fee = Some (t /. 100.0) })
              | _ -> Logging.warn_f ~section "Failed to parse fee values for %s (pair: %s)" symbol pair_key; Lwt.return_none))
    end
  ) (fun exn ->
    Logging.error_f ~section "Failed to fetch account fees for %s: %s" 
      symbol (Printexc.to_string exn);
    Lwt.return_none
  )

let get_fees_for_symbols symbols =
  Lwt_list.map_s (fun symbol -> get_fee_info symbol >|= fun fee_opt -> (symbol, fee_opt)) symbols
