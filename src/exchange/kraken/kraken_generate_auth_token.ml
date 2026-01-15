(** Kraken WebSocket Authentication Token Generator *)

open Lwt.Infix
open Cohttp_lwt_unix
open Yojson.Safe
open Logging

let endpoint = "https://api.kraken.com"
let section = "kraken_generate_auth_token"

(** Load .env file and retrieve API credentials *)
let get_api_credentials_from_env () : (string * string) Lwt.t =
  Lwt.catch (fun () -> Dotenv.export ~path:".env" (); Lwt.return_unit) (fun _ -> Lwt.return_unit) >>= fun () ->
  let get_env var =
    match Sys.getenv_opt var with
    | Some v when v <> "" -> Lwt.return v
    | _ -> Lwt.fail_with (Printf.sprintf "Missing environment variable: %s" var)
  in
  Lwt.both (get_env "KRAKEN_API_KEY") (get_env "KRAKEN_API_SECRET")

(** Generate WebSocket authentication token *)
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