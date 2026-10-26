(** Direct endpoints for Lighter mainnet. *)

let hostname = "mainnet.zklighter.elliot.ai"
let api_base_url = "https://" ^ hostname
let ws_connect_target () = hostname, 443
let public_ws_url () = Printf.sprintf "wss://%s/stream?readonly=true" hostname
let private_ws_url () = Printf.sprintf "wss://%s/stream" hostname
