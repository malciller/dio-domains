(** Direct endpoints for Lighter mainnet. *)

let hostname = "mainnet.zklighter.elliot.ai"
let api_base_url = "https://" ^ hostname
let ws_connect_target () = hostname, 443
let public_ws_url () = Printf.sprintf "wss://%s/stream?readonly=true" hostname

(** Dial target for the authenticated socket only: Lighter's ingress geo-blocks the
    private WebSocket from US addresses while public data and REST answer normally, so
    this one socket is pinned through the non-US egress relay (/etc/hosts on prod-node-1)
    instead of dragging the public feed and REST calls along with it.

    Deliberately outside DNS: an unpinned host fails to resolve rather than silently
    falling back to a blocked direct connection. TLS still validates [hostname] — see
    [Lighter_ws.connect_one]. *)
let private_ws_hostname = "lighter-private.zklighter.internal"

let private_ws_connect_target () = private_ws_hostname, 443
let private_ws_url () = Printf.sprintf "wss://%s/stream" hostname

(** The same relay carries signed writes: [POST /api/v1/sendTx] started geo-blocking
    independently of the socket, while reads still answer directly.
    [Lighter_actions.send_tx] dials [private_rest_connect_host] but keeps the URL, TLS SNI
    and [Host] header on [hostname]. *)
let private_rest_connect_host () = private_ws_hostname
