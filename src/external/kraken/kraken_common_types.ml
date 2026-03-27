(** Common Kraken API types, constants, and utilities *)

let section = "kraken_common"


let nonce_ms_multiplier = 1000.0

(* ---- Feed constants ---- *)

(** Default orderbook depth (number of price levels to maintain per side) *)
let default_orderbook_depth = 25

(** Default ring buffer size for the orderbook feed *)
let default_ring_buffer_size_orderbook = 16

(** Default ring buffer size for the executions feed *)
let default_ring_buffer_size_executions = 32

(** Maximum number of non-configured (dynamic) assets to track in the balances feed *)
let default_dynamic_assets_cap = 50

(** Age threshold (seconds) above which a balance is considered stale *)
let default_balance_staleness_threshold_s = 300.0

(** Common quote currencies always tracked by the balances feed
    regardless of which trading pairs are configured *)
let default_configured_currencies = ["USD"; "EUR"; "USDT"; "USDC"]


(** Safely force Conduit context with error handling *)
let get_conduit_ctx () =
  try
    Lazy.force Conduit_lwt_unix.default_ctx
  with
  | CamlinternalLazy.Undefined ->
      Logging.error ~section "Conduit context was accessed before initialization - this should not happen";
      raise (Failure "Conduit context not initialized - ensure main.ml initializes it before domain spawning")
  | exn ->
      Logging.error_f ~section "Failed to get Conduit context: %s" (Printexc.to_string exn);
      raise exn



let nonce () : string =
  Unix.gettimeofday () *. nonce_ms_multiplier |> Int64.of_float |> Int64.to_string

(** Normalize base64/base64url secret with whitespace removal and padding *)
let normalize_base64_secret (secret : string) : string =
  let trimmed = String.trim secret in
  if trimmed = "" then failwith "Kraken API secret is empty";
  let sanitized = String.to_seq trimmed
    |> Seq.filter (fun c -> not (List.mem c [' '; '\n'; '\r'; '\t']))
    |> Seq.map (function '-' -> '+' | '_' -> '/' | c -> c)
    |> String.of_seq in
  let base64_block_size = 4 in
  let padding = (base64_block_size - (String.length sanitized mod base64_block_size)) mod base64_block_size in
  if padding = 0 then sanitized else sanitized ^ String.make padding '='

(** Decode a base64-encoded Kraken API secret.
    Raises [Failure] if the secret is malformed - this is a startup-time invariant. *)
let decode_secret_base64 (secret : string) : string =
  match Base64.decode (normalize_base64_secret secret) with
  | Ok decoded -> decoded
  | Error (`Msg msg) -> failwith (Printf.sprintf "Invalid Kraken API secret (base64 decode failed): %s" msg)

(** Sign a Kraken API request using HMAC-SHA512.
    Raises [Failure] on base64 encoding failure - indicates a runtime invariant violation. *)
let sign ~secret ~path ~body ~nonce =
  let sha256 = Digestif.SHA256.(digest_string (nonce ^ body) |> to_raw_string) in
  let hmac = Digestif.SHA512.(hmac_string ~key:(decode_secret_base64 secret) (path ^ sha256) |> to_raw_string) in
  match Base64.encode ~pad:true hmac with
  | Ok encoded -> encoded
  | Error (`Msg msg) -> failwith ("Base64 encoding failed: " ^ msg)



(* ---- Shared JSON field parsers ---- *)
(** These helpers accept the full JSON object and a field name so callers
    avoid repeating the same match/member boilerplate. *)

let parse_float_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Float f -> Some f
    | `Int i -> Some (float_of_int i)
    | `String s -> (try Some (float_of_string s) with _ -> None)
    | `Intlit s -> (try Some (float_of_string s) with _ -> None)
    | _ -> None
  with _ -> None

let parse_int64_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Int i -> Some (Int64.of_int i)
    | `Intlit s -> (try Some (Int64.of_string s) with _ -> None)
    | `String s -> (try Some (Int64.of_string s) with _ -> None)
    | _ -> None
  with _ -> None

let parse_int_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Int i -> Some i
    | `Intlit s -> (try Some (int_of_string s) with _ -> None)
    | `String s -> (try Some (int_of_string s) with _ -> None)
    | _ -> None
  with _ -> None

(* ---- WebSocket response types ---- *)

(** Common response structure for Kraken WebSocket API *)
type ws_response = {
  method_: string;
  success: bool;
  req_id: int option;
  time_in: string;
  time_out: string;
  result: Yojson.Safe.t option;
  error: string option;
  warnings: string list option;
}

(** Add order response *)
type add_order_result = {
  order_id: string;
  cl_ord_id: string option;
  order_userref: int option;
}

type add_order_response = ws_response

(** Amend order response *)
type amend_order_result = {
  amend_id: string;
  order_id: string;
  cl_ord_id: string option;
}

type amend_order_response = ws_response

(** Cancel order response *)
type cancel_order_result = {
  order_id: string;
  cl_ord_id: string option;
}

type cancel_order_response = ws_response
