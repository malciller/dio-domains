(** Lighter signer FFI binding.
    Binds to the precompiled lighter_signer shared library (.dylib/.so)
    using OCaml ctypes for native-speed EdDSA/BabyJubJub/Poseidon signing.
    Thread safety: all FFI calls are serialized through [signer_mutex]
    since the Go shared library's internal state is not thread-safe. *)

let section = "lighter_signer"

open Lwt.Infix

(* --- Dynamic library loading (deferred until first use) --- *)

let lib_path =
  match Sys.getenv_opt "LIGHTER_SIGNER_LIB_PATH" with
  | Some p -> p
  | None ->
      (* Auto-detect platform-appropriate default.
         Sys.os_type is "Unix" on both macOS and Linux; distinguish via /proc. *)
      let os =
        if Sys.file_exists "/proc" then "linux"
        else "darwin"
      in
      let arch =
        let ic = Unix.open_process_in "uname -m" in
        let machine = try String.trim (input_line ic) with End_of_file -> "arm64" in
        ignore (Unix.close_process_in ic);
        match machine with
        | "x86_64" | "amd64" -> "amd64"
        | _ -> "arm64"
      in
      Printf.sprintf "./lighter-signer-%s-%s" os arch

let lib = lazy (
  try
    let filename =
      if Sys.file_exists (lib_path ^ ".dylib") then lib_path ^ ".dylib"
      else if Sys.file_exists (lib_path ^ ".so") then lib_path ^ ".so"
      else if Sys.file_exists lib_path then lib_path
      else begin
        Logging.warn_f ~section "Signer library not found at %s, will try dlopen with bare path" lib_path;
        lib_path
      end
    in
    Logging.info_f ~section "Loading lighter signer shared library from: %s (dlopen RTLD_NOW)..." filename;
    let handle = Dl.dlopen ~filename ~flags:[Dl.RTLD_NOW] in
    Logging.info_f ~section "Lighter signer library loaded successfully";
    handle
  with exn ->
    Logging.error_f ~section "FATAL: Failed to load lighter signer library from %s: %s" lib_path (Printexc.to_string exn);
    raise exn
)

let get_lib () = Lazy.force lib

(* --- FFI function bindings (deferred) --- *)

open Ctypes
open Foreign

(* --- C struct types matching the Go shared library --- *)

(** StrOrErr { str *char; err *char } — returned by CreateAuthToken *)
type str_or_err
let str_or_err : str_or_err structure typ = structure "StrOrErr"
let str_or_err_str = field str_or_err "str" string_opt
let str_or_err_err = field str_or_err "err" string_opt
let () = seal str_or_err

(** SignedTxResponse { txType uint8; txInfo *char; txHash *char; messageToSign *char; err *char } *)
type signed_tx_response
let signed_tx_response : signed_tx_response structure typ = structure "SignedTxResponse"
let _stx_tx_type = field signed_tx_response "txType" uint8_t
let stx_tx_info  = field signed_tx_response "txInfo" string_opt
let _stx_tx_hash = field signed_tx_response "txHash" string_opt
let _stx_message = field signed_tx_response "messageToSign" string_opt
let stx_err      = field signed_tx_response "err" string_opt
let () = seal signed_tx_response

(** Helper: extract the txInfo string from a SignedTxResponse, or raise on error. *)
let extract_signed_tx (resp : signed_tx_response structure) : string =
  match getf resp stx_err with
  | Some err when err <> "" -> failwith (Printf.sprintf "Signer FFI error: %s" err)
  | _ ->
      match getf resp stx_tx_info with
      | Some info -> info
      | None -> ""

(* Lighter mainnet chain ID.
   Python SDK: chain_id = 304 if ("mainnet" in url or "api" in url) else 300
   Our proxy forwards to mainnet.zklighter.elliot.ai, so chain_id = 304. *)
let chain_id = ref 304

(** CreateClient(url, privateKey, chainId, apiKeyIndex, accountIndex) -> error *char
    Note: accountIndex is C.longlong (int64) in Go. *)
let create_client =
  lazy (
    let ffi_fn = foreign "CreateClient" ~from:(get_lib ())
      (string @-> string @-> int @-> int @-> int64_t @-> returning string_opt) in
    fun url private_key ~chain_id:cid ~api_key_index ~account_index ->
      match ffi_fn url private_key cid api_key_index (Int64.of_int account_index) with
      | Some s -> s | None -> ""
  )

(** CheckClient(apiKeyIndex, accountIndex) -> error *char
    accountIndex is C.longlong (int64) in Go. *)
let check_client =
  lazy (
    let ffi_fn = foreign "CheckClient" ~from:(get_lib ())
      (int @-> int64_t @-> returning string_opt) in
    fun api_key_index account_index ->
      match ffi_fn api_key_index (Int64.of_int account_index) with
      | Some s -> s | None -> ""
  )

(** CreateAuthToken(deadline, apiKeyIndex, accountIndex) -> StrOrErr
    accountIndex is C.longlong (int64) in Go. *)
let create_auth_token_ffi =
  lazy (
    let ffi_fn = foreign "CreateAuthToken" ~from:(get_lib ())
      (int64_t @-> int @-> int64_t @-> returning str_or_err) in
    fun deadline api_key_index account_index ->
      let resp = ffi_fn deadline api_key_index (Int64.of_int account_index) in
      match getf resp str_or_err_err with
      | Some err when err <> "" -> failwith (Printf.sprintf "CreateAuthToken error: %s" err)
      | _ ->
          match getf resp str_or_err_str with
          | Some s -> s
          | None -> ""
  )

(** SignCreateOrder — full Go signature with all 17 parameters.
    We pass 0 for integrator fields and skipNonce=0 since we manage nonces ourselves. *)
let sign_create_order_ffi =
  lazy (
    let ffi_fn = foreign "SignCreateOrder" ~from:(get_lib ())
      (int @-> int64_t @-> int64_t @-> int @-> int @->
       int @-> int @-> int @-> int @-> int64_t @->
       int64_t @-> int @-> int @->
       uint8_t @-> int64_t @->
       int @-> int64_t @-> returning signed_tx_response) in
    fun ~market_index ~client_order_index ~base_amount ~price
        ~is_ask ~order_type ~tif ~reduce_only ~trigger_price ~order_expiry
        ~nonce ~api_key_index ~account_index ->
      let resp = ffi_fn
        market_index client_order_index base_amount price
        is_ask order_type tif reduce_only trigger_price order_expiry
        0L 0 0  (* integrator: accountIndex, takerFee, makerFee *)
        (Unsigned.UInt8.of_int 0) (Int64.of_int nonce)  (* skipNonce=0, nonce *)
        api_key_index (Int64.of_int account_index) in
      extract_signed_tx resp
  )

(** SignCancelOrder(marketIndex, orderIndex, skipNonce, nonce, apiKeyIndex, accountIndex) *)
let sign_cancel_order_ffi =
  lazy (
    let ffi_fn = foreign "SignCancelOrder" ~from:(get_lib ())
      (int @-> int64_t @->
       uint8_t @-> int64_t @->
       int @-> int64_t @-> returning signed_tx_response) in
    fun ~market_index ~order_index ~nonce ~api_key_index ~account_index ->
      let resp = ffi_fn
        market_index order_index
        (Unsigned.UInt8.of_int 0) (Int64.of_int nonce)
        api_key_index (Int64.of_int account_index) in
      extract_signed_tx resp
  )

(** SignModifyOrder(marketIndex, orderIndex, baseAmount, price, triggerPrice,
    integratorAccountIndex, integratorTakerFee, integratorMakerFee,
    skipNonce, nonce, apiKeyIndex, accountIndex) *)
let sign_modify_order_ffi =
  lazy (
    let ffi_fn = foreign "SignModifyOrder" ~from:(get_lib ())
      (int @-> int64_t @-> int64_t @-> int64_t @-> int64_t @->
       int64_t @-> int @-> int @->
       uint8_t @-> int64_t @->
       int @-> int64_t @-> returning signed_tx_response) in
    fun ~market_index ~order_index ~new_base_amount ~new_price
        ~nonce ~api_key_index ~account_index ->
      let resp = ffi_fn
        market_index order_index new_base_amount new_price
        0L  (* triggerPrice *)
        0L 0 0  (* integrator: accountIndex, takerFee, makerFee *)
        (Unsigned.UInt8.of_int 0) (Int64.of_int nonce)
        api_key_index (Int64.of_int account_index) in
      extract_signed_tx resp
  )

(** SignCancelAllOrders(timeInForce, time, skipNonce, nonce, apiKeyIndex, accountIndex) *)
let sign_cancel_all_orders_ffi =
  lazy (
    let ffi_fn = foreign "SignCancelAllOrders" ~from:(get_lib ())
      (int @-> int64_t @->
       uint8_t @-> int64_t @->
       int @-> int64_t @-> returning signed_tx_response) in
    fun ~tif ~time ~nonce ~api_key_index ~account_index ->
      let resp = ffi_fn
        tif time
        (Unsigned.UInt8.of_int 0) (Int64.of_int nonce)
        api_key_index (Int64.of_int account_index) in
      extract_signed_tx resp
  )

(* --- Thread safety --- *)

let signer_mutex = Mutex.create ()

let with_signer_lock f =
  Mutex.lock signer_mutex;
  let result = try f () with exn -> Mutex.unlock signer_mutex; raise exn in
  Mutex.unlock signer_mutex;
  result

(* --- Nonce management --- *)

let nonce_counter = Atomic.make 0

let get_and_increment_nonce () =
  Atomic.fetch_and_add nonce_counter 1

let set_nonce n =
  Atomic.set nonce_counter n

(** Fetch the next nonce from the REST API and initialize the counter. *)
let initialize_nonce ~base_url ~api_key_index ~account_index =
  let url = Printf.sprintf "%s/api/v1/nextNonce?account_index=%d&api_key_index=%d"
    base_url account_index api_key_index in
  Logging.info_f ~section "Fetching initial nonce from %s..." url;
  let fetch =
    Lwt.catch (fun () ->
      let uri = Uri.of_string url in
      let%lwt (_resp, body) = Cohttp_lwt_unix.Client.get uri in
      let%lwt body_str = Cohttp_lwt.Body.to_string body in
      let json = Yojson.Safe.from_string body_str in
      let nonce = Yojson.Safe.Util.(member "nonce" json |> to_int) in
      set_nonce nonce;
      Logging.info_f ~section "Initialized nonce to %d for account %d, api_key %d" nonce account_index api_key_index;
      Lwt.return_unit
    ) (fun exn ->
      Logging.error_f ~section "Failed to fetch initial nonce: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  in
  let timeout =
    Lwt_unix.sleep 10.0 >>= fun () ->
    Logging.error_f ~section "Nonce fetch timed out after 10s";
    Lwt.return_unit
  in
  Lwt.pick [fetch; timeout]

(* --- Configuration --- *)

let api_key_index = ref 0
let account_index = ref 0

(** Read accessors for nonce recovery. Used by lighter_actions to call
    initialize_nonce when the exchange returns "invalid nonce". *)
let get_api_key_index () = !api_key_index
let get_account_index () = !account_index

(** Initialize the signer client. Must be called before any signing operations. *)
let initialize ~base_url ~private_key ~key_index ~acct_index =
  api_key_index := key_index;
  account_index := acct_index;
  Logging.info_f ~section "Calling CreateClient FFI (api_key_index=%d, account_index=%d, chain_id=%d)..." key_index acct_index !chain_id;
  let err = with_signer_lock (fun () ->
    (Lazy.force create_client) base_url private_key
      ~chain_id:!chain_id ~api_key_index:key_index ~account_index:acct_index
  ) in
  Logging.info_f ~section "CreateClient FFI returned";
  if err <> "" then begin
    Logging.error_f ~section "CreateClient failed: %s" err;
    Error err
  end else begin
    Logging.info_f ~section "Signer client initialized for api_key_index=%d account_index=%d" key_index acct_index;
    (* Verify the client setup *)
    Logging.info_f ~section "Calling CheckClient FFI...";
    let check_err = with_signer_lock (fun () ->
      (Lazy.force check_client) key_index acct_index
    ) in
    Logging.info_f ~section "CheckClient FFI returned";
    if check_err <> "" then begin
      Logging.warn_f ~section "CheckClient warning: %s" check_err;
      Ok ()
    end else
      Ok ()
  end

(* --- Auth token management --- *)

let cached_auth_token : string option ref = ref None
let auth_token_expiry = ref 0.0

(** Generate a fresh auth token valid for ~7 hours.
    CreateAuthToken now returns the token string directly (not JSON). *)
let refresh_auth_token () =
  let deadline = Int64.of_float (Unix.gettimeofday () +. (7.0 *. 3600.0)) in
  try
    let token = with_signer_lock (fun () ->
      (Lazy.force create_auth_token_ffi) deadline !api_key_index !account_index
    ) in
    if token = "" then begin
      Logging.error_f ~section "CreateAuthToken returned empty token";
      ""
    end else begin
      cached_auth_token := Some token;
      auth_token_expiry := Unix.gettimeofday () +. (6.5 *. 3600.0);
      Logging.info_f ~section "Auth token refreshed, valid until %.0f" !auth_token_expiry;
      token
    end
  with exn ->
    Logging.error_f ~section "Failed to create auth token: %s" (Printexc.to_string exn);
    ""

let get_auth_token () =
  match !cached_auth_token with
  | Some token when Unix.gettimeofday () < !auth_token_expiry -> token
  | _ -> refresh_auth_token ()

(* --- Signing operations --- *)

let sign_create_order
    ~market_index ~client_order_index ~base_amount ~price
    ~is_ask ~order_type ~tif ~reduce_only ~expiry =
  let nonce = get_and_increment_nonce () in
  let result = with_signer_lock (fun () ->
    (Lazy.force sign_create_order_ffi)
      ~market_index ~client_order_index ~base_amount ~price:(Int64.to_int price)
      ~is_ask:(if is_ask then 1 else 0)
      ~order_type ~tif
      ~reduce_only:(if reduce_only then 1 else 0)
      ~trigger_price:0 ~order_expiry:expiry
      ~nonce ~api_key_index:!api_key_index ~account_index:!account_index
  ) in
  Logging.debug_f ~section "SignCreateOrder: market=%d client_oid=%Ld amount=%Ld price=%Ld ask=%b nonce=%d"
    market_index client_order_index base_amount price is_ask nonce;
  result

let sign_cancel_order ~market_index ~order_index =
  let nonce = get_and_increment_nonce () in
  let result = with_signer_lock (fun () ->
    (Lazy.force sign_cancel_order_ffi)
      ~market_index ~order_index
      ~nonce ~api_key_index:!api_key_index ~account_index:!account_index
  ) in
  Logging.debug_f ~section "SignCancelOrder: market=%d order=%Ld nonce=%d" market_index order_index nonce;
  result

let sign_modify_order ~market_index ~order_index ~new_base_amount ~new_price =
  let nonce = get_and_increment_nonce () in
  let result = with_signer_lock (fun () ->
    (Lazy.force sign_modify_order_ffi)
      ~market_index ~order_index ~new_base_amount ~new_price
      ~nonce ~api_key_index:!api_key_index ~account_index:!account_index
  ) in
  Logging.debug_f ~section "SignModifyOrder: market=%d order=%Ld amount=%Ld price=%Ld nonce=%d"
    market_index order_index new_base_amount new_price nonce;
  result

let sign_cancel_all_orders ~market_index =
  let nonce = get_and_increment_nonce () in
  let time = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
  let result = with_signer_lock (fun () ->
    (Lazy.force sign_cancel_all_orders_ffi)
      ~tif:0 ~time
      ~nonce ~api_key_index:!api_key_index ~account_index:!account_index
  ) in
  Logging.debug_f ~section "SignCancelAllOrders: market=%d nonce=%d" market_index nonce;
  result
