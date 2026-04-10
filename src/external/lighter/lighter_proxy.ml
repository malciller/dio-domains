(** Lighter proxy configuration.
    Routes Lighter traffic through a Cloudflare Worker + Durable Object
    reverse proxy to bypass CloudFront geo-restrictions.
    
    Setup:
      1. Deploy the proxy (from repo root):
           cd proxy/cloudflare && npx wrangler deploy
      
      2. Set the environment variable:
           export LIGHTER_PROXY_URL=https://lighter-proxy.<subdomain>.workers.dev
    
    When LIGHTER_PROXY_URL is set, all Lighter traffic (HTTP and WebSocket)
    is routed through the proxy. The proxy runs on Cloudflare's edge
    pinned to Western Europe, making requests appear to originate from there.
    
    When LIGHTER_PROXY_URL is unset, connections go directly to Lighter.
    WebSocket uses readonly=true to bypass geo-restriction for read-only data. *)

let section = "lighter_proxy"

(** Read a value from the .env file if it exists. *)
let read_dotenv key =
  try
    let ic = open_in ".env" in
    let result = ref None in
    (try while true do
      let line = input_line ic in
      let line = String.trim line in
      if String.length line > 0 && line.[0] <> '#' then begin
        match String.split_on_char '=' line with
        | k :: rest when String.trim k = key ->
            let v = String.concat "=" rest |> String.trim in
            result := Some v
        | _ -> ()
      end
    done with End_of_file -> ());
    close_in ic;
    !result
  with Sys_error _ -> None

(** Resolve an env variable: check process environment first, then .env file. *)
let env_or_dotenv key =
  match Sys.getenv_opt key |> Option.map String.trim with
  | Some s when s <> "" -> Some s
  | _ -> read_dotenv key

(** The real Lighter API hostname. *)
let direct_hostname = "mainnet.zklighter.elliot.ai"

(** The real Lighter API base URL for REST calls. *)
let direct_base_url = "https://mainnet.zklighter.elliot.ai"

(** Parsed proxy URL from LIGHTER_PROXY_URL (env var or .env file). *)
let proxy_url : string option =
  match env_or_dotenv "LIGHTER_PROXY_URL" with
  | Some s when s <> "" ->
      (* Strip trailing slash *)
      let url = if String.length s > 0 && s.[String.length s - 1] = '/'
        then String.sub s 0 (String.length s - 1)
        else s in
      Logging.info_f ~section "Lighter proxy configured: %s" url;
      Some url
  | _ ->
      Logging.info_f ~section "No LIGHTER_PROXY_URL configured, connecting directly (readonly mode)";
      None

(** Whether a proxy is configured. *)
let is_proxied () = Option.is_some proxy_url

(** Returns the base URL for REST API calls.
    Proxied: https://your-proxy.deno.dev
    Direct:  https://mainnet.zklighter.elliot.ai *)
let api_base_url () =
  match proxy_url with
  | Some url -> url
  | None -> direct_base_url

(** Returns the hostname and port to connect to for WebSocket.
    Proxied: (your-proxy.deno.dev, 443)
    Direct:  (mainnet.zklighter.elliot.ai, 443) *)
let ws_connect_target () =
  match proxy_url with
  | Some url ->
      let uri = Uri.of_string url in
      let host = Uri.host uri |> Option.value ~default:direct_hostname in
      let port = Uri.port uri |> Option.value ~default:443 in
      (host, port)
  | None -> (direct_hostname, 443)

(** Returns the full WebSocket URL.
    Proxied: wss://your-proxy.deno.dev/stream (full read/write)
    Direct:  wss://mainnet.zklighter.elliot.ai/stream?readonly=true *)
let ws_url () =
  match proxy_url with
  | Some url ->
      let uri = Uri.of_string url in
      let host = Uri.host uri |> Option.value ~default:direct_hostname in
      Printf.sprintf "wss://%s/stream" host
  | None ->
      Printf.sprintf "wss://%s/stream?readonly=true" direct_hostname
