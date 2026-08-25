(** Proxy routing for Lighter API and WebSocket traffic through Cloudflare
    Workers, bypassing geographic access restrictions at the exchange ingress.

    Deployment:
      cd proxy/cloudflare && npx wrangler deploy
      export LIGHTER_PROXY_URL=https://lighter-proxy.<subdomain>.workers.dev

    With [LIGHTER_PROXY_URL] set (comma separated pool), all traffic goes
    through workers running in permitted jurisdictions. Without it, traffic
    connects directly; in direct mode the public WebSocket appends
    [readonly=true] to its query params to get past the geo block. *)

let section = "lighter_proxy"
let consecutive_proxy_failures = Atomic.make 0

(** Parses KEY=VALUE pairs from [.env]; fallback when the process environment
    lacks a variable. *)
let read_dotenv key =
  try
    let ic = open_in ".env" in
    let result = ref None in
    (try
       while true do
         let line = input_line ic in
         let line = String.trim line in
         if String.length line > 0 && line.[0] <> '#'
         then (
           match String.split_on_char '=' line with
           | k :: rest when String.trim k = key ->
             let v = String.concat "=" rest |> String.trim in
             result := Some v
           | _ -> ())
       done
     with
     | End_of_file -> ());
    close_in ic;
    !result
  with
  | Sys_error _ -> None
;;

(** Returns the environment variable if set, else the [.env] value. *)
let env_or_dotenv key =
  match Sys.getenv_opt key |> Option.map String.trim with
  | Some s when s <> "" -> Some s
  | _ -> read_dotenv key
;;

(** Lighter mainnet hostname. *)
let direct_hostname = "mainnet.zklighter.elliot.ai"

(** Lighter mainnet REST base URL. *)
let direct_base_url = "https://mainnet.zklighter.elliot.ai"

(** Proxy pool from [LIGHTER_PROXY_URL]: comma separated, trailing slashes
    stripped. Empty when unset. *)
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
  | _ -> []
;;

let _init_logged = ref false

let _do_log () =
  if not !_init_logged
  then (
    _init_logged := true;
    if List.length proxy_urls > 0
    then
      Logging.info_f
        ~section
        "Lighter proxy configured with %d pool(s): %s"
        (List.length proxy_urls)
        (String.concat ", " proxy_urls)
    else
      Logging.info_f
        ~section
        "No LIGHTER_PROXY_URL configured, connecting directly (readonly mode)")
;;

let current_proxy_index = Atomic.make 0

(** Advances the proxy pool index round-robin and increments the failure
    counter; called by the WS layer after connection failures so another
    account's worker can take over. *)
let rotate_proxy () =
  let len = List.length proxy_urls in
  if len > 1
  then (
    let current = Atomic.get current_proxy_index in
    let next = (current + 1) mod len in
    Atomic.set current_proxy_index next;
    Logging.info_f
      ~section
      "Rotating to fallback Lighter proxy: %s"
      (List.nth proxy_urls next));
  Atomic.incr consecutive_proxy_failures
;;

let reset_proxy_failures () = Atomic.set consecutive_proxy_failures 0

let has_more_proxies () =
  let len = List.length proxy_urls in
  if len <= 1 then false else Atomic.get consecutive_proxy_failures < len - 1
;;

(** Current proxy URL, or None in direct mode. *)
let proxy_url () =
  _do_log ();
  let len = List.length proxy_urls in
  if len = 0 then None else Some (List.nth proxy_urls (Atomic.get current_proxy_index))
;;

(** True when a proxy pool is configured. *)
let is_proxied () = List.length proxy_urls > 0

(** REST base URL: active proxy, else the direct mainnet endpoint. *)
let api_base_url () =
  match proxy_url () with
  | Some url -> url
  | None -> direct_base_url
;;

(** (host, port) for the private/authenticated WS: from the current proxy,
    else the direct mainnet endpoint on 443. *)
let private_ws_connect_target () =
  match proxy_url () with
  | Some url ->
    let uri = Uri.of_string url in
    let host = Uri.host uri |> Option.value ~default:direct_hostname in
    let port = Uri.port uri |> Option.value ~default:443 in
    host, port
  | None -> direct_hostname, 443
;;

(** (host, port) for the public WS: always direct, never proxied. *)
let public_ws_connect_target () = direct_hostname, 443

(** Public market data WS URL: direct, with [readonly=true] so the geo block
    does not apply. *)
let public_ws_url () = Printf.sprintf "wss://%s/stream?readonly=true" direct_hostname

(** Authenticated WS URL: proxied host (with [sessionId=dio-private]) when a
    proxy is configured, else direct. *)
let private_ws_url () =
  match proxy_url () with
  | Some url ->
    let uri = Uri.of_string url in
    let host = Uri.host uri |> Option.value ~default:direct_hostname in
    Printf.sprintf "wss://%s/stream?sessionId=dio-private" host
  | None -> Printf.sprintf "wss://%s/stream" direct_hostname
;;
