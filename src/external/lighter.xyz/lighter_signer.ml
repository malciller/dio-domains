(** ctypes FFI bindings to the precompiled Lighter signer shared library
    (EdDSA/BabyJubJub/Poseidon signing implemented in Go). All FFI calls are
    serialized through [signer_mutex] because the Go library is not thread
    safe. *)

let section = "lighter_signer"

open Lwt.Infix

(* Shared library loading *)

let lib_path =
  match Sys.getenv_opt "LIGHTER_SIGNER_LIB_PATH" with
  | Some p -> p
  | None ->
    (* uname reports "Unix" for both macOS and Linux; detect Linux via /proc. *)
    let os = if Sys.file_exists "/proc" then "linux" else "darwin" in
    let arch =
      let ic = Unix.open_process_in "uname -m" in
      let machine =
        try String.trim (input_line ic) with
        | End_of_file -> "arm64"
      in
      ignore (Unix.close_process_in ic);
      match machine with
      | "x86_64" | "amd64" -> "amd64"
      | _ -> "arm64"
    in
    Printf.sprintf "./lighter-signer-%s-%s" os arch
;;

let lib =
  lazy
    (try
       let filename =
         if Sys.file_exists (lib_path ^ ".dylib")
         then lib_path ^ ".dylib"
         else if Sys.file_exists (lib_path ^ ".so")
         then lib_path ^ ".so"
         else if Sys.file_exists lib_path
         then lib_path
         else (
           Logging.warn_f
             ~section
             "Signer library not found at %s, will try dlopen with bare path"
             lib_path;
           lib_path)
       in
       Logging.info_f
         ~section
         "Loading lighter signer shared library from: %s (dlopen RTLD_NOW)..."
         filename;
       let handle = Dl.dlopen ~filename ~flags:[ Dl.RTLD_NOW ] in
       Logging.info_f ~section "Lighter signer library loaded successfully";
       handle
     with
     | exn ->
       Logging.error_f
         ~section
         "FATAL: Failed to load lighter signer library from %s: %s"
         lib_path
         (Printexc.to_string exn);
       raise exn)
;;

let get_lib () = Lazy.force lib

(* FFI bindings *)

open Ctypes
open Foreign

(* Structures matching the Go library's return types *)

(** Go result carrying either a payload string or an error string. *)
type str_or_err

let str_or_err : str_or_err structure typ = structure "StrOrErr"
let str_or_err_str = field str_or_err "str" (ptr_opt char)
let str_or_err_err = field str_or_err "err" (ptr_opt char)
let () = seal str_or_err

(** Signed tx response: tx type, tx info payload, tx hash, raw message to sign,
    optional error. *)
type signed_tx_response

let signed_tx_response : signed_tx_response structure typ = structure "SignedTxResponse"
let _stx_tx_type = field signed_tx_response "txType" uint8_t
let stx_tx_info = field signed_tx_response "txInfo" (ptr_opt char)
let _stx_tx_hash = field signed_tx_response "txHash" (ptr_opt char)
let _stx_message = field signed_tx_response "messageToSign" (ptr_opt char)
let stx_err = field signed_tx_response "err" (ptr_opt char)
let () = seal signed_tx_response

(* C string memory management *)
let go_free_ffi = lazy (foreign "Free" ~from:(get_lib ()) (ptr void @-> returning void))

let safe_free p_opt =
  match p_opt with
  | None -> ()
  | Some p -> (Lazy.force go_free_ffi) (to_voidp p)
;;

let read_c_string_and_free p_opt =
  match p_opt with
  | None -> ""
  | Some p ->
    let rec loop i =
      let c = !@(p +@ i) in
      if c = '\000' then i else loop (i + 1)
    in
    let len = loop 0 in
    let bytes = Bytes.create len in
    for i = 0 to len - 1 do
      Bytes.set bytes i !@(p +@ i)
    done;
    let s = Bytes.to_string bytes in
    safe_free p_opt;
    s
;;

(** Reads the [tx_info] string out of a signed tx response, frees every
    returned C string, and raises if the signer reported an error. *)
let extract_signed_tx (resp : signed_tx_response structure) : string =
  let err_ptr = getf resp stx_err in
  let info_ptr = getf resp stx_tx_info in
  let hash_ptr = getf resp _stx_tx_hash in
  let msg_ptr = getf resp _stx_message in
  let err_str = read_c_string_and_free err_ptr in
  let info_str = read_c_string_and_free info_ptr in
  safe_free hash_ptr;
  safe_free msg_ptr;
  if err_str <> ""
  then failwith (Printf.sprintf "Signer FFI error: %s" err_str)
  else info_str
;;

(* Lighter mainnet chain id; fixed because all traffic targets mainnet. *)
let chain_id = ref 304

(** CreateClient FFI: builds the Go signer client from URL, private key, chain
    id, api key index, and account index (Go int64). Returns an error string
    pointer or NULL. *)
let create_client =
  lazy
    (let ffi_fn =
       foreign
         "CreateClient"
         ~from:(get_lib ())
         (string @-> string @-> int @-> int @-> int64_t @-> returning (ptr_opt char))
     in
     fun url private_key ~chain_id:cid ~api_key_index ~account_index ->
       let err_ptr =
         ffi_fn url private_key cid api_key_index (Int64.of_int account_index)
       in
       read_c_string_and_free err_ptr)
;;

(** CheckClient FFI: verifies the initialized client for the given api
    key/account indices; returns an error string or NULL. *)
let check_client =
  lazy
    (let ffi_fn =
       foreign
         "CheckClient"
         ~from:(get_lib ())
         (int @-> int64_t @-> returning (ptr_opt char))
     in
     fun api_key_index account_index ->
       let err_ptr = ffi_fn api_key_index (Int64.of_int account_index) in
       read_c_string_and_free err_ptr)
;;

(** CreateAuthToken FFI: mints an auth token valid until the given deadline;
    returns a str/err structure. *)
let create_auth_token_ffi =
  lazy
    (let ffi_fn =
       foreign
         "CreateAuthToken"
         ~from:(get_lib ())
         (int64_t @-> int @-> int64_t @-> returning str_or_err)
     in
     fun deadline api_key_index account_index ->
       let resp = ffi_fn deadline api_key_index (Int64.of_int account_index) in
       let err_str = read_c_string_and_free (getf resp str_or_err_err) in
       let str_str = read_c_string_and_free (getf resp str_or_err_str) in
       if err_str <> ""
       then failwith (Printf.sprintf "CreateAuthToken error: %s" err_str)
       else str_str)
;;

(** SignCreateOrder FFI (17 args per the Go ABI). Integrator fields
    (accountIndex/takerFee/makerFee) pass zero and skipNonce=0; nonce supply
    and sync are handled on the OCaml side. *)
let sign_create_order_ffi =
  lazy
    (let ffi_fn =
       foreign
         "SignCreateOrder"
         ~from:(get_lib ())
         (int
          @-> int64_t
          @-> int64_t
          @-> int
          @-> int
          @-> int
          @-> int
          @-> int
          @-> int
          @-> int64_t
          @-> int64_t
          @-> int
          @-> int
          @-> uint8_t
          @-> int64_t
          @-> int
          @-> int64_t
          @-> returning signed_tx_response)
     in
     fun ~market_index
       ~client_order_index
       ~base_amount
       ~price
       ~is_ask
       ~order_type
       ~tif
       ~reduce_only
       ~trigger_price
       ~order_expiry
       ~nonce
       ~api_key_index
       ~account_index ->
       let resp =
         ffi_fn
           market_index
           client_order_index
           base_amount
           price
           is_ask
           order_type
           tif
           reduce_only
           trigger_price
           order_expiry
           0L
           0
           0 (* integrator: accountIndex, takerFee, makerFee *)
           (Unsigned.UInt8.of_int 0)
           (Int64.of_int nonce) (* skipNonce=0, nonce *)
           api_key_index
           (Int64.of_int account_index)
       in
       extract_signed_tx resp)
;;

(** SignCancelOrder FFI: signs a cancellation for [market_index]/[order_index]. *)
let sign_cancel_order_ffi =
  lazy
    (let ffi_fn =
       foreign
         "SignCancelOrder"
         ~from:(get_lib ())
         (int
          @-> int64_t
          @-> uint8_t
          @-> int64_t
          @-> int
          @-> int64_t
          @-> returning signed_tx_response)
     in
     fun ~market_index ~order_index ~nonce ~api_key_index ~account_index ->
       let resp =
         ffi_fn
           market_index
           order_index
           (Unsigned.UInt8.of_int 0)
           (Int64.of_int nonce)
           api_key_index
           (Int64.of_int account_index)
       in
       extract_signed_tx resp)
;;

(** SignModifyOrder FFI: changes qty/price of an existing order. Zero-fills
    trigger price and integrator fields; nonce is passed explicitly. *)
let sign_modify_order_ffi =
  lazy
    (let ffi_fn =
       foreign
         "SignModifyOrder"
         ~from:(get_lib ())
         (int
          @-> int64_t
          @-> int64_t
          @-> int64_t
          @-> int64_t
          @-> int64_t
          @-> int
          @-> int
          @-> uint8_t
          @-> int64_t
          @-> int
          @-> int64_t
          @-> returning signed_tx_response)
     in
     fun ~market_index
       ~order_index
       ~new_base_amount
       ~new_price
       ~nonce
       ~api_key_index
       ~account_index ->
       let resp =
         ffi_fn
           market_index
           order_index
           new_base_amount
           new_price
           0L (* triggerPrice *)
           0L
           0
           0 (* integrator: accountIndex, takerFee, makerFee *)
           (Unsigned.UInt8.of_int 0)
           (Int64.of_int nonce)
           api_key_index
           (Int64.of_int account_index)
       in
       extract_signed_tx resp)
;;

(** SignCancelAllOrders FFI: signs a cancel-all using the supplied time and
    nonce. *)
let sign_cancel_all_orders_ffi =
  lazy
    (let ffi_fn =
       foreign
         "SignCancelAllOrders"
         ~from:(get_lib ())
         (int
          @-> int64_t
          @-> uint8_t
          @-> int64_t
          @-> int
          @-> int64_t
          @-> returning signed_tx_response)
     in
     fun ~tif ~time ~nonce ~api_key_index ~account_index ->
       let resp =
         ffi_fn
           tif
           time
           (Unsigned.UInt8.of_int 0)
           (Int64.of_int nonce)
           api_key_index
           (Int64.of_int account_index)
       in
       extract_signed_tx resp)
;;

(* Concurrency: single mutex around all FFI calls *)

let signer_mutex = Mutex.create ()

let with_signer_lock f =
  Mutex.lock signer_mutex;
  let result =
    try f () with
    | exn ->
      Mutex.unlock signer_mutex;
      raise exn
  in
  Mutex.unlock signer_mutex;
  result
;;

(* Nonce tracking *)

let nonce_counter = Atomic.make 0
let get_and_increment_nonce () = Atomic.fetch_and_add nonce_counter 1
let set_nonce n = Atomic.set nonce_counter n

(** Fetches the next nonce from [/api/v1/nextNonce] and resets the local atomic
    counter to match the exchange. Races a 10s timeout. *)
let initialize_nonce ~base_url ~api_key_index ~account_index =
  let url =
    Printf.sprintf
      "%s/api/v1/nextNonce?account_index=%d&api_key_index=%d"
      base_url
      account_index
      api_key_index
  in
  Logging.info_f ~section "Fetching initial nonce from %s..." url;
  let fetch =
    Lwt.catch
      (fun () ->
         let uri = Uri.of_string url in
         let%lwt _resp, body = Cohttp_lwt_unix.Client.get uri in
         let%lwt body_str = Cohttp_lwt.Body.to_string body in
         let json = Yojson.Safe.from_string body_str in
         let nonce = Yojson.Safe.Util.(member "nonce" json |> to_int) in
         set_nonce nonce;
         Logging.info_f
           ~section
           "Initialized nonce to %d for account %d, api_key %d"
           nonce
           account_index
           api_key_index;
         Lwt.return_unit)
      (fun exn ->
         Logging.error_f
           ~section
           "Failed to fetch initial nonce: %s"
           (Printexc.to_string exn);
         Lwt.return_unit)
  in
  let timeout =
    Lwt_unix.sleep 10.0
    >>= fun () ->
    Logging.error_f ~section "Nonce fetch timed out after 10s";
    Lwt.return_unit
  in
  Lwt.pick [ fetch; timeout ]
;;

(* Local signer state *)

let api_key_index = ref 0
let account_index = ref 0

(** Accessors for the cached api key/account indices, e.g. used by the nonce
    resync path in [lighter_actions.ml]. *)
let get_api_key_index () = !api_key_index

let get_account_index () = !account_index

(** Creates the Go signer client ([CreateClient]) with the given credentials
    and verifies it with [CheckClient]. Must run before any signing call. *)
let initialize ~base_url ~private_key ~key_index ~acct_index =
  api_key_index := key_index;
  account_index := acct_index;
  Logging.info_f
    ~section
    "Calling CreateClient FFI (api_key_index=%d, account_index=%d, chain_id=%d)..."
    key_index
    acct_index
    !chain_id;
  let err =
    with_signer_lock (fun () ->
      (Lazy.force create_client)
        base_url
        private_key
        ~chain_id:!chain_id
        ~api_key_index:key_index
        ~account_index:acct_index)
  in
  Logging.info_f ~section "CreateClient FFI returned";
  if err <> ""
  then (
    Logging.error_f ~section "CreateClient failed: %s" err;
    Error err)
  else (
    Logging.info_f
      ~section
      "Signer client initialized for api_key_index=%d account_index=%d"
      key_index
      acct_index;
    Logging.info_f ~section "Calling CheckClient FFI...";
    let check_err =
      with_signer_lock (fun () -> (Lazy.force check_client) key_index acct_index)
    in
    Logging.info_f ~section "CheckClient FFI returned";
    if check_err <> ""
    then (
      Logging.warn_f ~section "CheckClient warning: %s" check_err;
      Ok ())
    else Ok ())
;;

(* Auth token caching *)

let cached_auth_token : string option ref = ref None
let auth_token_expiry = ref 0.0

(** Mints an auth token with a 7h deadline and caches it for 6.5h. Returns ""
    on failure. *)
let refresh_auth_token () =
  let deadline = Int64.of_float (Unix.gettimeofday () +. (7.0 *. 3600.0)) in
  try
    let token =
      with_signer_lock (fun () ->
        (Lazy.force create_auth_token_ffi) deadline !api_key_index !account_index)
    in
    if token = ""
    then (
      Logging.error_f ~section "CreateAuthToken returned empty token";
      "")
    else (
      cached_auth_token := Some token;
      auth_token_expiry := Unix.gettimeofday () +. (6.5 *. 3600.0);
      Logging.info_f ~section "Auth token refreshed, valid until %.0f" !auth_token_expiry;
      token)
  with
  | exn ->
    Logging.error_f ~section "Failed to create auth token: %s" (Printexc.to_string exn);
    ""
;;

let get_auth_token () =
  match !cached_auth_token with
  | Some token when Unix.gettimeofday () < !auth_token_expiry -> token
  | _ -> refresh_auth_token ()
;;

(* Signing entry points *)

(** Times a signing operation and records it in the "lighter" signer profiler:
    local FFI work (hash + ECDSA over the nonce) shown on the dashboard's
    NETWORK page. *)
let time_signer f =
  let start_ns = Mtime_clock.now_ns () in
  let r = f () in
  Network_latency.record_signer
    "lighter"
    (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) start_ns));
  r
;;

let sign_create_order
      ~market_index
      ~client_order_index
      ~base_amount
      ~price
      ~is_ask
      ~order_type
      ~tif
      ~reduce_only
      ~expiry
  =
  let nonce = get_and_increment_nonce () in
  let result =
    time_signer (fun () ->
      with_signer_lock (fun () ->
        (Lazy.force sign_create_order_ffi)
          ~market_index
          ~client_order_index
          ~base_amount
          ~price:(Int64.to_int price)
          ~is_ask:(if is_ask then 1 else 0)
          ~order_type
          ~tif
          ~reduce_only:(if reduce_only then 1 else 0)
          ~trigger_price:0
          ~order_expiry:expiry
          ~nonce
          ~api_key_index:!api_key_index
          ~account_index:!account_index))
  in
  Logging.debug_f
    ~section
    "SignCreateOrder: market=%d client_oid=%Ld amount=%Ld price=%Ld ask=%b nonce=%d"
    market_index
    client_order_index
    base_amount
    price
    is_ask
    nonce;
  result
;;

let sign_cancel_order ~market_index ~order_index =
  let nonce = get_and_increment_nonce () in
  let result =
    time_signer (fun () ->
      with_signer_lock (fun () ->
        (Lazy.force sign_cancel_order_ffi)
          ~market_index
          ~order_index
          ~nonce
          ~api_key_index:!api_key_index
          ~account_index:!account_index))
  in
  Logging.debug_f
    ~section
    "SignCancelOrder: market=%d order=%Ld nonce=%d"
    market_index
    order_index
    nonce;
  result
;;

let sign_modify_order ~market_index ~order_index ~new_base_amount ~new_price =
  let nonce = get_and_increment_nonce () in
  let result =
    time_signer (fun () ->
      with_signer_lock (fun () ->
        (Lazy.force sign_modify_order_ffi)
          ~market_index
          ~order_index
          ~new_base_amount
          ~new_price
          ~nonce
          ~api_key_index:!api_key_index
          ~account_index:!account_index))
  in
  Logging.debug_f
    ~section
    "SignModifyOrder: market=%d order=%Ld amount=%Ld price=%Ld nonce=%d"
    market_index
    order_index
    new_base_amount
    new_price
    nonce;
  result
;;

let sign_cancel_all_orders ~market_index =
  let nonce = get_and_increment_nonce () in
  let time = Int64.of_float (Unix.gettimeofday () *. 1000.0) in
  let result =
    time_signer (fun () ->
      with_signer_lock (fun () ->
        (Lazy.force sign_cancel_all_orders_ffi)
          ~tif:0
          ~time
          ~nonce
          ~api_key_index:!api_key_index
          ~account_index:!account_index))
  in
  Logging.debug_f ~section "SignCancelAllOrders: market=%d nonce=%d" market_index nonce;
  result
;;
