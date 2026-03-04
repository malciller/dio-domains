(** Common Kraken API types and utilities *)
(* TODO: Magic number - 1000.0 should be defined as a constant for milliseconds conversion *)

let nonce () : string =
  Unix.gettimeofday () *. 1000.0 |> Int64.of_float |> Int64.to_string

(** Normalize base64/base64url secret with whitespace removal and padding *)
let normalize_base64_secret (secret : string) : string =
  let trimmed = String.trim secret in
  if trimmed = "" then failwith "Kraken API secret is empty";
  let sanitized = String.to_seq trimmed 
    |> Seq.filter (fun c -> not (List.mem c [' '; '\n'; '\r'; '\t']))
    |> Seq.map (function '-' -> '+' | '_' -> '/' | c -> c)
    |> String.of_seq in
  (* TODO: Magic number - 4 is the base64 block size, should be defined as a constant *)
  let padding = (4 - (String.length sanitized mod 4)) mod 4 in
  if padding = 0 then sanitized else sanitized ^ String.make padding '='

(* TODO: Harsh error handling - failwith terminates program, consider returning Result type instead *)
let decode_secret_base64 (secret : string) : string =
  match Base64.decode (normalize_base64_secret secret) with
  | Ok decoded -> decoded
  | Error (`Msg msg) -> failwith (Printf.sprintf "Invalid Kraken API secret (base64 decode failed): %s" msg)

(* TODO: Harsh error handling - failwith terminates program, consider returning Result type instead *)
(** Sign a Kraken API request using HMAC-SHA512 *)
let sign ~secret ~path ~body ~nonce =
  let sha256 = Digestif.SHA256.(digest_string (nonce ^ body) |> to_raw_string) in
  let hmac = Digestif.SHA512.(hmac_string ~key:(decode_secret_base64 secret) (path ^ sha256) |> to_raw_string) in
  match Base64.encode ~pad:true hmac with
  | Ok encoded -> encoded
  | Error (`Msg msg) -> failwith ("Base64 encoding failed: " ^ msg)

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
