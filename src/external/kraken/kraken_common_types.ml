(** Shared types, constants, and utility functions for the Kraken API integration.
    Provides cryptographic signing, nonce generation, base64 secret handling,
    JSON field parsers, and default feed configuration values used across all
    Kraken subsystems. *)

let section = "kraken_common"


(** Multiplier to convert Unix epoch seconds (float) to millisecond precision for nonce generation. *)
let nonce_ms_multiplier = 1000.0

(* Feed configuration defaults *)

(** Number of price levels maintained per side (bid/ask) in the orderbook. *)
let default_orderbook_depth = 10

(** Ring buffer capacity (slot count) for the orderbook feed channel. *)
let default_ring_buffer_size_orderbook = 16

(** Ring buffer capacity (slot count) for the executions feed channel. *)
let default_ring_buffer_size_executions = 32

(** Upper bound on the number of dynamically discovered assets tracked by the balances feed. *)
let default_dynamic_assets_cap = 50

(** Maximum age (seconds) before a cached balance entry is considered stale. *)
let default_balance_staleness_threshold_s = 300.0

(** Quote currencies permanently tracked by the balances feed regardless of configured trading pairs. *)
let default_configured_currencies = ["USD"; "EUR"; "USDT"; "USDC"]


(** Forces evaluation of the lazy Conduit TLS context.
    Raises [Failure] if the context has not been initialized prior to domain spawning. *)
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



(** Generates a monotonically increasing nonce string from the current Unix time in milliseconds.
    Used as a replay-prevention token in authenticated Kraken API requests. *)
let nonce () : string =
  Unix.gettimeofday () *. nonce_ms_multiplier |> Int64.of_float |> Int64.to_string

(** Normalizes a base64 or base64url encoded secret string.
    Strips whitespace, converts base64url characters (- and _) to standard base64 (+ and /),
    and appends padding as required. Raises [Failure] if the input is empty. *)
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

(** Decodes a base64-encoded Kraken API secret into raw bytes.
    Applies normalization before decoding. Raises [Failure] on malformed input,
    enforcing a startup-time invariant that the configured secret is valid. *)
let decode_secret_base64 (secret : string) : string =
  match Base64.decode (normalize_base64_secret secret) with
  | Ok decoded -> decoded
  | Error (`Msg msg) -> failwith (Printf.sprintf "Invalid Kraken API secret (base64 decode failed): %s" msg)

(** Computes the HMAC-SHA512 signature for an authenticated Kraken REST request.
    Procedure: SHA256(nonce || body), then HMAC-SHA512(decoded_secret, path || sha256_digest).
    Returns the base64-encoded signature. Raises [Failure] if base64 encoding of the
    result fails, indicating a runtime invariant violation. *)
let sign ~secret ~path ~body ~nonce =
  let sha256 = Digestif.SHA256.(digest_string (nonce ^ body) |> to_raw_string) in
  let hmac = Digestif.SHA512.(hmac_string ~key:(decode_secret_base64 secret) (path ^ sha256) |> to_raw_string) in
  match Base64.encode ~pad:true hmac with
  | Ok encoded -> encoded
  | Error (`Msg msg) -> failwith ("Base64 encoding failed: " ^ msg)



(* Shared JSON field parsers *)
(** Polymorphic JSON field extractors. Each function accepts a [Yojson.Safe.t] object
    and a field name, returning [Some value] on successful coercion or [None] on
    type mismatch or missing field. Suppresses all exceptions. *)

(** Extracts a float from field [field]. Coerces from [`Float], [`Int], [`String], or [`Intlit]. *)
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

(** Extracts an [int64] from field [field]. Coerces from [`Int], [`Intlit], or [`String]. *)
let parse_int64_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Int i -> Some (Int64.of_int i)
    | `Intlit s -> (try Some (Int64.of_string s) with _ -> None)
    | `String s -> (try Some (Int64.of_string s) with _ -> None)
    | _ -> None
  with _ -> None

(** Extracts an [int] from field [field]. Coerces from [`Int], [`Intlit], or [`String]. *)
let parse_int_opt json field =
  try
    let open Yojson.Safe.Util in
    match member field json with
    | `Int i -> Some i
    | `Intlit s -> (try Some (int_of_string s) with _ -> None)
    | `String s -> (try Some (int_of_string s) with _ -> None)
    | _ -> None
  with _ -> None

(* WebSocket response types *)

(** Envelope type for Kraken WebSocket v2 API responses.
    Carries method name, success flag, optional request correlation ID,
    server-side timestamps, and optional result/error/warning payloads. *)
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

(** Result payload returned by the add_order WebSocket method.
    Contains the exchange-assigned order ID and optional client-supplied identifiers. *)
type add_order_result = {
  order_id: string;
  cl_ord_id: string option;
  order_userref: int option;
}

(** Type alias for the add_order response envelope. *)
type add_order_response = ws_response

(** Result payload returned by the amend_order WebSocket method.
    Contains the amendment ID, target order ID, and optional client order ID. *)
type amend_order_result = {
  amend_id: string;
  order_id: string;
  cl_ord_id: string option;
}

(** Type alias for the amend_order response envelope. *)
type amend_order_response = ws_response

(** Result payload returned by the cancel_order WebSocket method.
    Contains the cancelled order ID and optional client order ID. *)
type cancel_order_result = {
  order_id: string;
  cl_ord_id: string option;
}

(** Type alias for the cancel_order response envelope. *)
type cancel_order_response = ws_response
