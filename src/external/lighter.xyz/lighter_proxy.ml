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

let consecutive_proxy_failures = Atomic.make 0

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

(** Parsed proxy URLs from LIGHTER_PROXY_URL (comma-separated list). *)
let proxy_urls : string list =
  match env_or_dotenv "LIGHTER_PROXY_URL" with
  | Some s when s <> "" ->
      String.split_on_char ',' s
      |> List.map String.trim
      |> List.filter (fun url -> url <> "")
      |> List.map (fun s ->
           if String.length s > 0 && s.[String.length s - 1] = '/'
           then String.sub s 0 (String.length s - 1)
           else s)
      |> fun urls ->
          if List.length urls > 0 then
            Logging.info_f ~section "Lighter proxy configured with %d pool(s): %s"
              (List.length urls) (String.concat ", " urls);
          urls
  | _ ->
      Logging.info_f ~section "No LIGHTER_PROXY_URL configured, connecting directly (readonly mode)";
      []

let current_proxy_index = Atomic.make 0

(** Rotates the active proxy to the next instance in the comma-separated list.
    Called automatically on connection failure to support multi-account failover. *)
let rotate_proxy () =
  let len = List.length proxy_urls in
  if len > 1 then begin
    let current = Atomic.get current_proxy_index in
    let next = (current + 1) mod len in
    Atomic.set current_proxy_index next;
    Logging.info_f ~section "Rotating to fallback Lighter proxy: %s" (List.nth proxy_urls next)
  end;
  Atomic.incr consecutive_proxy_failures

let reset_proxy_failures () =
  Atomic.set consecutive_proxy_failures 0

let has_more_proxies () =
  let len = List.length proxy_urls in
  if len <= 1 then false
  else Atomic.get consecutive_proxy_failures < len - 1

(** Returns the currently active proxy URL (if any). *)
let proxy_url () =
  let len = List.length proxy_urls in
  if len = 0 then None
  else Some (List.nth proxy_urls (Atomic.get current_proxy_index))

(** Whether a proxy is configured. *)
let is_proxied () = List.length proxy_urls > 0

(** Returns the base URL for REST API calls.
    Proxied: https://your-proxy.deno.dev
    Direct:  https://mainnet.zklighter.elliot.ai *)
let api_base_url () =
  match proxy_url () with
  | Some url -> url
  | None -> direct_base_url

(** Returns the hostname and port to connect to for the private WebSocket.
    Proxied: (your-proxy.workers.dev, 443)
    Direct:  (mainnet.zklighter.elliot.ai, 443) *)
let private_ws_connect_target () =
  match proxy_url () with
  | Some url ->
      let uri = Uri.of_string url in
      let host = Uri.host uri |> Option.value ~default:direct_hostname in
      let port = Uri.port uri |> Option.value ~default:443 in
      (host, port)
  | None -> (direct_hostname, 443)

(** Returns the hostname and port for the public WebSocket.
    Always direct: (mainnet.zklighter.elliot.ai, 443) *)
let public_ws_connect_target () =
  (direct_hostname, 443)

(** Returns the full WebSocket URL for public data.
    Always direct to Lighter: wss://mainnet.zklighter.elliot.ai/stream?readonly=true *)
let public_ws_url () =
  Printf.sprintf "wss://%s/stream?readonly=true" direct_hostname

(** Returns the full WebSocket URL for private data.
    Proxied: wss://your-proxy.workers.dev/stream
    Direct:  wss://mainnet.zklighter.elliot.ai/stream *)
let private_ws_url () =
  match proxy_url () with
  | Some url ->
      let uri = Uri.of_string url in
      let host = Uri.host uri |> Option.value ~default:direct_hostname in
      Printf.sprintf "wss://%s/stream?sessionId=dio-private" host
  | None ->
      Printf.sprintf "wss://%s/stream" direct_hostname
