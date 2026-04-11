(* Hyperliquid_get_fee -- per-user fee rate retrieval for perpetual and spot markets.
   Queries the Hyperliquid /info endpoint with the configured wallet address,
   parses account-specific and schedule-level rates, and falls back to
   hardcoded tier maximums when neither is available. *)

let section = "hyperliquid_get_fee"

type fee_info = {
  exchange: string;
  maker_fee: float option;
  taker_fee: float option;
  spot_maker_fee: float option;
  spot_taker_fee: float option;
}

(** Reads the HYPERLIQUID_WALLET_ADDRESS environment variable.
    Fails with an Lwt exception if the variable is unset. *)
let get_wallet_address_from_env () : string Lwt.t =
  match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" with
  | Some v -> Lwt.return v 
  | None -> Lwt.fail_with "Missing environment variable: HYPERLIQUID_WALLET_ADDRESS"

let parse_fee_info body_str =
  let open Yojson.Safe.Util in
  try
    let json = Yojson.Safe.from_string body_str in
    (* Perp fee rates. The API returns decimal string values (e.g. "0.0001"). *)
    let maker = member "userAddRate" json |> to_string_option |> Option.map float_of_string in
    let taker = member "userCrossRate" json |> to_string_option |> Option.map float_of_string in
    
    (* Spot fee resolution order:
       1. userSpotAddRate / userSpotCrossRate (account-specific, includes referral and staking discounts).
       2. feeSchedule.spotAdd / feeSchedule.spotCross (base schedule rates).
       3. Hardcoded defaults: 0.0004 maker, 0.0007 taker (highest Hyperliquid spot tier). *)
    let spot_maker =
      let user_rate = member "userSpotAddRate" json |> to_string_option |> Option.map float_of_string in
      match user_rate with
      | Some _ -> user_rate
      | None ->
          (try
             let schedule = member "feeSchedule" json in
             match member "spotAdd" schedule |> to_string_option |> Option.map float_of_string with
             | Some _ as r -> r
             | None -> Some 0.0004
           with _ -> Some 0.0004)
    in
    let spot_taker =
      let user_rate = member "userSpotCrossRate" json |> to_string_option |> Option.map float_of_string in
      match user_rate with
      | Some _ -> user_rate
      | None ->
          (try
             let schedule = member "feeSchedule" json in
             match member "spotCross" schedule |> to_string_option |> Option.map float_of_string with
             | Some _ as r -> r
             | None -> Some 0.0007
           with _ -> Some 0.0007)
    in
    
    Logging.debug_f ~section "Retrieved Hyperliquid fees: perp_maker=%s, perp_taker=%s, spot_maker=%s, spot_taker=%s" 
      (Option.map (fun f -> Printf.sprintf "%.4f%%" (f *. 100.)) maker |> Option.value ~default:"None")
      (Option.map (fun f -> Printf.sprintf "%.4f%%" (f *. 100.)) taker |> Option.value ~default:"None")
      (Option.map (fun f -> Printf.sprintf "%.4f%%" (f *. 100.)) spot_maker |> Option.value ~default:"None")
      (Option.map (fun f -> Printf.sprintf "%.4f%%" (f *. 100.)) spot_taker |> Option.value ~default:"None");
      
    Some {
      exchange = "hyperliquid";
      maker_fee = maker;
      taker_fee = taker;
      spot_maker_fee = spot_maker;
      spot_taker_fee = spot_taker;
    }
  with exn ->
    Logging.error_f ~section "Failed to parse Hyperliquid fee response: %s (JSON: %s)" (Printexc.to_string exn) body_str;
    None

(** Fetches per-user fee rates from the Hyperliquid /info endpoint via POST.
    Selects mainnet or testnet base URL based on [~testnet].
    Applies a 5-second timeout. Returns [None] on HTTP error, timeout, or parse failure. *)
let get_fee_info ~testnet () : fee_info option Lwt.t =
  let open Lwt.Infix in
  let base_url = if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz" in
  Lwt.catch (fun () ->
    get_wallet_address_from_env () >>= fun wallet ->
    let url = Uri.of_string (base_url ^ "/info") in
    let body = Cohttp_lwt.Body.of_string (Printf.sprintf "{\"type\":\"userFees\",\"user\":\"%s\"}" wallet) in
    let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
    
    Logging.info_f ~section "Fetching Hyperliquid fees for wallet %s..." wallet;
    
    (* 5-second timeout guard against unresponsive upstream *)
    Lwt_unix.with_timeout 5.0 (fun () ->
      Cohttp_lwt_unix.Client.post ~headers ~body url >>= fun (resp, resp_body) ->
      if Cohttp.Response.status resp |> Cohttp.Code.code_of_status |> fun c -> c >= 200 && c < 300 then
        Cohttp_lwt.Body.to_string resp_body >|= fun body_str ->
        Logging.debug_f ~section "Raw fee response: %s" body_str;
        parse_fee_info body_str
      else
        Cohttp_lwt.Body.to_string resp_body >>= fun body_str ->
        Logging.error_f ~section "Hyperliquid fee request failed with status %d: %s" 
          (Cohttp.Response.status resp |> Cohttp.Code.code_of_status) body_str;
        Lwt.return None
    )
  ) (fun exn ->
    Logging.error_f ~section "Error fetching Hyperliquid fees: %s" (Printexc.to_string exn);
    Lwt.return None
  )