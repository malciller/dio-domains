(** Provides Foreign Function Interface bindings to the precompiled Lighter signer shared library.
    Integrates OCaml with the underlying native library using ctypes to enable highly performant EdDSA, BabyJubJub, and Poseidon cryptographic signing operations.
    Concurrency considerations dictate that all Foreign Function Interface calls must be strictly serialized utilizing the internal Mutex, as the underlying Go shared library does not guarantee thread safety for its internal state structures. *)

let section = "lighter_signer"

open Lwt.Infix

(* Dynamic Library Initialization Module *)

let lib_path =
  match Sys.getenv_opt "LIGHTER_SIGNER_LIB_PATH" with
  | Some p -> p
  | None ->
      (* Implement platform detection logic to identify the operating system executing the process.
         The system architecture returns Unix for both macOS and Linux environments, necessitating a filesystem check against the proc directory to disambiguate the platform. *)
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

(* Foreign Function Interface Bindings Declaration *)

open Ctypes
open Foreign

(* Foreign Function Interface Structure Definitions corresponding to the underlying Go library types *)

(** Structure representing either a string pointer or an error pointer returned by the authentication token creation process. *)
type str_or_err
let str_or_err : str_or_err structure typ = structure "StrOrErr"
let str_or_err_str = field str_or_err "str" (ptr_opt char)
let str_or_err_err = field str_or_err "err" (ptr_opt char)
let () = seal str_or_err

(** Structure representing the signed transaction response containing transaction type, transaction information payload, transaction hash, the raw message designated for signing, and an optional error pointer. *)
type signed_tx_response
let signed_tx_response : signed_tx_response structure typ = structure "SignedTxResponse"
let _stx_tx_type = field signed_tx_response "txType" uint8_t
let stx_tx_info  = field signed_tx_response "txInfo" (ptr_opt char)
let _stx_tx_hash = field signed_tx_response "txHash" (ptr_opt char)
let _stx_message = field signed_tx_response "messageToSign" (ptr_opt char)
let stx_err      = field signed_tx_response "err" (ptr_opt char)
let () = seal signed_tx_response

(* Memory Allocation and Cleanup Management for Foreign Function Interface Strings *)
let go_free_ffi = lazy (
  foreign "Free" ~from:(get_lib ()) (ptr void @-> returning void)
)

let safe_free p_opt =
  match p_opt with
  | None -> ()
  | Some p -> (Lazy.force go_free_ffi) (to_voidp p)

let read_c_string_and_free p_opt =
  match p_opt with
  | None -> ""
  | Some p ->
      let rec loop i =
        let c = !@(p +@ i) in
        if c = '\000' then i
        else loop (i + 1)
      in
      let len = loop 0 in
      let bytes = Bytes.create len in
      for i = 0 to len - 1 do
        Bytes.set bytes i !@(p +@ i)
      done;
      let s = Bytes.to_string bytes in
      safe_free p_opt;
      s

(** Extracts the transaction information string payload from a signed transaction response structure and subsequently deallocates all associated unmanaged memory pointers to prevent memory leaks. *)
let extract_signed_tx (resp : signed_tx_response structure) : string =
  let err_ptr = getf resp stx_err in
  let info_ptr = getf resp stx_tx_info in
  let hash_ptr = getf resp _stx_tx_hash in
  let msg_ptr = getf resp _stx_message in
  let err_str = read_c_string_and_free err_ptr in
  let info_str = read_c_string_and_free info_ptr in
  safe_free hash_ptr;
  safe_free msg_ptr;
  if err_str <> "" then failwith (Printf.sprintf "Signer FFI error: %s" err_str)
  else info_str

(* Defines the cryptographic chain identifier for the Lighter mainnet environment.
   The integration logic routes traffic through a specific proxy directed to the mainnet endpoint, thereby setting the chain identifier deterministically to 304 for all corresponding operations. *)
let chain_id = ref 304

(** Initializes a new client instance within the Foreign Function Interface utilizing the provided connection uniform resource locator, private cryptographic key, chain identifier, application programming interface key index, and account index. The account index is represented as a 64 bit integer corresponding to the underlying Go long long type. Returns an optional error string pointer. *)
let create_client =
  lazy (
    let ffi_fn = foreign "CreateClient" ~from:(get_lib ())
      (string @-> string @-> int @-> int @-> int64_t @-> returning (ptr_opt char)) in
    fun url private_key ~chain_id:cid ~api_key_index ~account_index ->
      let err_ptr = ffi_fn url private_key cid api_key_index (Int64.of_int account_index) in
      read_c_string_and_free err_ptr
  )

(** Validates the operational status of an existing client instance within the Foreign Function Interface utilizing the application programming interface key index and the 64 bit integer account index. Returns an optional error string pointer indicating the status of the client initialization. *)
let check_client =
  lazy (
    let ffi_fn = foreign "CheckClient" ~from:(get_lib ())
      (int @-> int64_t @-> returning (ptr_opt char)) in
    fun api_key_index account_index ->
      let err_ptr = ffi_fn api_key_index (Int64.of_int account_index) in
      read_c_string_and_free err_ptr
  )

(** Generates a cryptographic authentication token via the Foreign Function Interface bound by the specified deadline timestamp, using the application programming interface key index and the 64 bit integer account index. Returns a structure encapsulating either the generated token string pointer or an error string pointer. *)
let create_auth_token_ffi =
  lazy (
    let ffi_fn = foreign "CreateAuthToken" ~from:(get_lib ())
      (int64_t @-> int @-> int64_t @-> returning str_or_err) in
    fun deadline api_key_index account_index ->
      let resp = ffi_fn deadline api_key_index (Int64.of_int account_index) in
      let err_str = read_c_string_and_free (getf resp str_or_err_err) in
      let str_str = read_c_string_and_free (getf resp str_or_err_str) in
      if err_str <> "" then failwith (Printf.sprintf "CreateAuthToken error: %s" err_str)
      else str_str
  )

(** Computes the digital signature for an order creation request utilizing the exhaustive set of 17 parameters mandated by the underlying Go library implementation. Integrator specific fields and the skip nonce parameter are configured to deterministic null values, delegating the responsibility of continuous nonce synchronization to the internal OCaml state management. *)
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

(** Computes the digital signature for an order cancellation request bounded by the target market index, specific order identifier, cryptographic nonce value, application programming interface key index, and account identifier within the Foreign Function Interface. *)
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

(** Computes the digital signature for an order modification transaction, authorizing adjustments to the base fractional amount and price parameters for a specific order identifier within the targeted market index. Handles parameter serialization required for the Foreign Function Interface invocation, including default assignments for integrator specific variables and manual nonce provisioning. *)
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

(** Computes the digital signature for a comprehensive cancellation command targeting all active orders within a specific market, utilizing the current system timestamp and local cryptographic nonce to satisfy the signing requirements of the Foreign Function Interface. *)
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

(* Concurrency Management and Mutual Exclusion Controls *)

let signer_mutex = Mutex.create ()

let with_signer_lock f =
  Mutex.lock signer_mutex;
  let result = try f () with exn -> Mutex.unlock signer_mutex; raise exn in
  Mutex.unlock signer_mutex;
  result

(* Cryptographic Nonce Synchronization and Trajectory Management *)

let nonce_counter = Atomic.make 0

let get_and_increment_nonce () =
  Atomic.fetch_and_add nonce_counter 1

let set_nonce n =
  Atomic.set nonce_counter n

(** Establishes synchrony with the remote state by fetching the requisite cryptographic nonce via a representational state transfer request and subsequently updating the local atomic counter to mirror the server expectations. *)
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

(* Local Operational State and Configuration Management *)

let api_key_index = ref 0
let account_index = ref 0

(** Provides read access to the locally cached application programming interface key and account identifiers. Facilitates external recovery procedures allowing upstream action modules to trigger resynchronization workflows when remote endpoints indicate state divergence regarding the expected sequence nonce. *)
let get_api_key_index () = !api_key_index
let get_account_index () = !account_index

(** Orchestrates the initialization sequence for the underlying cryptographic signing client instance. Establish operational parameters including endpoint destinations, private keys, and deterministic indices before allowing any cryptographic signing operations to traverse the Foreign Function Interface boundaries. *)
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

(* Authentication Token Lifecycle and Expiration Management *)

let cached_auth_token : string option ref = ref None
let auth_token_expiry = ref 0.0

(** Provisions a new cryptographic authentication token configured with an extended validity threshold. Invokes the native Foreign Function Interface to construct the underlying token material and asserts the successful retrieval of a raw token string payload absent any serialized data structure encapsulation requirements. *)
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

(* Cryptographic Transaction Signing Operational Implementations *)

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
