(** Modular networking component managing proxy routing for the Lighter exchange integration.
    Handles the redirection of API requests and WebSocket streams through a distributed
    edge network architecture to circumvent geographic access controls applied at the ingress layer.
    
    Deployment Protocol:
      1. Initialize the edge infrastructure routing namespace from the repository root:
           cd proxy/cloudflare && npx wrangler deploy
      
      2. Inject the routing pool definitions into the runtime environment:
           export LIGHTER_PROXY_URL=https://lighter-proxy.<subdomain>.workers.dev
    
    If the LIGHTER_PROXY_URL parameter is populated, the routing engine enforces 
    proxy utilization for all bidirectional traffic channels. The target infrastructure 
    operates on localized persistent execution environments bound to valid jurisdictions.
    
    If left undefined, the module defaults to standard direct connectivity mechanisms.
    In direct mode, WebSocket connections are synthesized with explicit read-only 
    flags modifying the query parameters to bypass geographic ingress blocks. *)

let section = "lighter_proxy"

let consecutive_proxy_failures = Atomic.make 0

(** Synchronous operating system read sequence parsing key-value pairs from localized
    environment definition files, circumventing the primary process configuration. *)
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

(** Resolves targeted execution parameters by sequentially interrogating the
    process environment space before falling back to localized file parsing logic. *)
let env_or_dotenv key =
  match Sys.getenv_opt key |> Option.map String.trim with
  | Some s when s <> "" -> Some s
  | _ -> read_dotenv key

(** Primary destination hostname mapped to the exchange's core execution network. *)
let direct_hostname = "mainnet.zklighter.elliot.ai"

(** Primary REST interface URL mapped to the exchange's core execution network. *)
let direct_base_url = "https://mainnet.zklighter.elliot.ai"

(** Instantiated pool of proxy endpoints extracted from the targeted environment
    variable and tokenized utilizing delimiter processing logic. *)
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

(** Executes a circular state progression against the instantiated proxy pool index.
    Triggered automatically by the connection manager upon intercepting localized
    network failures to guarantee multi-account operational continuity sequences. *)
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

(** Resolves the current target connection URI mapped to the active execution state
    of the proxy index sequence. Yields no value during direct connectivity modes. *)
let proxy_url () =
  let len = List.length proxy_urls in
  if len = 0 then None
  else Some (List.nth proxy_urls (Atomic.get current_proxy_index))

(** Evaluates the initialization state of the proxy configuration pool to determine
    if localized routing overrides are active within the current execution environment. *)
let is_proxied () = List.length proxy_urls > 0

(** Computes the definitive REST API endpoint resolution sequence.
    Proxied paths dynamically map to specified edge environments.
    Direct paths default to the core deterministic exchange endpoint. *)
let api_base_url () =
  match proxy_url () with
  | Some url -> url
  | None -> direct_base_url

(** Computes the localized socket resolution parameters for authenticated streams.
    Extracts explicit hostname patterns and standard encrypted port mappings from the
    active proxy configuration or defaults to the primary exchange coordinates. *)
let private_ws_connect_target () =
  match proxy_url () with
  | Some url ->
      let uri = Uri.of_string url in
      let host = Uri.host uri |> Option.value ~default:direct_hostname in
      let port = Uri.port uri |> Option.value ~default:443 in
      (host, port)
  | None -> (direct_hostname, 443)

(** Computes the localized socket resolution parameters for unauthenticated pipelines.
    Always maintains a direct routing topology bypassing the proxy architecture. *)
let public_ws_connect_target () =
  (direct_hostname, 443)

(** Synthesizes the finalized WebSocket pipeline URI for aggregated market data.
    Enforces direct connectivity using specialized read-only query parameters
    to ensure geographic routing restrictions are correctly averted. *)
let public_ws_url () =
  Printf.sprintf "wss://%s/stream?readonly=true" direct_hostname

(** Synthesizes the finalized WebSocket pipeline URI for authenticated session states.
    Automatically transitions between direct and proxied routing topologies based
    on the initialized configuration parameters extracted at runtime. *)
let private_ws_url () =
  match proxy_url () with
  | Some url ->
      let uri = Uri.of_string url in
      let host = Uri.host uri |> Option.value ~default:direct_hostname in
      Printf.sprintf "wss://%s/stream?sessionId=dio-private" host
  | None ->
      Printf.sprintf "wss://%s/stream" direct_hostname
