(**
   Kraken WebSocket authentication token generator.

   Provisions ephemeral tokens for authenticated Kraken WebSocket channels
   (e.g., ownTrades, openOrders) via the REST endpoint
   [/0/private/GetWebSocketsToken]. Performs HMAC-SHA512 request signing
   using credentials loaded from the .env file. Tokens are short-lived and
   must be refreshed periodically by the caller.
*)

open Lwt.Infix
open Cohttp_lwt_unix
open Yojson.Safe
open Logging

let endpoint = "https://api.kraken.com"
let section = "kraken_generate_auth_token"

(** Loads .env into the process environment if present, then reads
    [KRAKEN_API_KEY] and [KRAKEN_API_SECRET]. Returns both values as a
    pair. Raises [Failure] if either variable is missing or empty. *)
let get_api_credentials_from_env () : (string * string) Lwt.t =
  Lwt.catch (fun () -> Dotenv.export ~path:".env" (); Lwt.return_unit) (fun _ -> Lwt.return_unit) >>= fun () ->
  let get_env var =
    match Sys.getenv_opt var with
    | Some v when v <> "" -> Lwt.return v
    | _ -> Lwt.fail_with (Printf.sprintf "Missing environment variable: %s" var)
  in
  Lwt.both (get_env "KRAKEN_API_KEY") (get_env "KRAKEN_API_SECRET")

(** Performs a signed POST to [/0/private/GetWebSocketsToken] and
    returns the ephemeral token string from the JSON response.
    Raises [Failure] on HTTP-level errors, non-empty API error arrays,
    or unexpected response structure. Uses [Kraken_common_types.sign]
    for HMAC-SHA512 nonce-based request authentication. *)
let get_token () : string Lwt.t =
  get_api_credentials_from_env () >>= fun (api_key, api_secret) ->
  let path = "/0/private/GetWebSocketsToken" in
  let nonce = Kraken_common_types.nonce () in
  let body_str = "nonce=" ^ nonce in
  let signature = Kraken_common_types.sign ~secret:api_secret ~path ~body:body_str ~nonce in
  let headers = Cohttp.Header.of_list [
    ("API-Key", api_key);
    ("API-Sign", signature);
    ("Content-Type", "application/x-www-form-urlencoded");
  ] in
  Client.post ~headers ~body:(Cohttp_lwt.Body.of_string body_str) 
    (Uri.of_string (endpoint ^ path)) >>= fun (resp, body) ->
  
  Cohttp_lwt.Body.to_string body >>= fun body_str ->
  let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
  if status <> 200 then begin
    error_f ~section "HTTP %d: %s" status body_str;
    Lwt.fail_with (Printf.sprintf "Kraken API HTTP error %d" status)
  end else begin
    let json = from_string body_str in
    match Util.(member "error" json, member "result" json |> member "token") with
    | `List (_ :: _ as errs), _ ->
        let msg = errs |> List.map to_string |> String.concat "; " in
        error_f ~section "API error: %s" msg;
        Lwt.fail_with ("Kraken API error: " ^ msg)
    | (`Null | `List []), `String token -> Lwt.return token
    | _ ->
        error_f ~section "Unexpected response format";
        Lwt.fail_with "Invalid Kraken API response"
  end