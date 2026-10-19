(** FFI bindings to the precompiled Lighter signer shared library
    (EdDSA/BabyJubJub/Poseidon signing implemented in Go). All FFI calls are serialized
    through [signer_mutex] because the Go library is not thread safe.

    The OCaml-facing wrapper is in-tree; the Go shared library remains the audited native
    signer. *)

let section = "lighter_signer"

open Lwt.Infix

let lib_path =
  match Sys.getenv_opt "LIGHTER_SIGNER_LIB_PATH" with
  | Some p -> p
  | None ->
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

let library =
  lazy
    (let filename =
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
     Logging.info_f ~section "Loading lighter signer shared library from: %s..." filename;
     Lighter_ffi.load filename;
     Logging.info_f ~section "Lighter signer library loaded successfully")
;;

let ensure_loaded () = Lazy.force library
let chain_id = ref 304

let create_client =
  lazy
    (fun url private_key ~chain_id:cid ~api_key_index ~account_index ->
      ensure_loaded ();
      Lighter_ffi.create_client
        url
        private_key
        cid
        api_key_index
        (Int64.of_int account_index))
;;

let check_client =
  lazy
    (fun api_key_index account_index ->
      ensure_loaded ();
      Lighter_ffi.check_client api_key_index (Int64.of_int account_index))
;;

let create_auth_token_ffi =
  lazy
    (fun deadline api_key_index account_index ->
      ensure_loaded ();
      Lighter_ffi.create_auth_token deadline api_key_index (Int64.of_int account_index))
;;

let sign_create_order_ffi =
  lazy
    (fun ~market_index
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
      ensure_loaded ();
      Lighter_ffi.sign_create_order
        market_index
        client_order_index
        base_amount
        (Int64.to_int price)
        is_ask
        order_type
        tif
        reduce_only
        trigger_price
        order_expiry
        (Int64.of_int nonce)
        api_key_index
        (Int64.of_int account_index))
;;

let sign_cancel_order_ffi =
  lazy
    (fun ~market_index ~order_index ~nonce ~api_key_index ~account_index ->
      ensure_loaded ();
      Lighter_ffi.sign_cancel_order
        market_index
        order_index
        (Int64.of_int nonce)
        api_key_index
        (Int64.of_int account_index))
;;

let sign_modify_order_ffi =
  lazy
    (fun ~market_index
      ~order_index
      ~new_base_amount
      ~new_price
      ~nonce
      ~api_key_index
      ~account_index ->
      ensure_loaded ();
      Lighter_ffi.sign_modify_order
        market_index
        order_index
        new_base_amount
        new_price
        (Int64.of_int nonce)
        api_key_index
        (Int64.of_int account_index))
;;

let sign_cancel_all_orders_ffi =
  lazy
    (fun ~tif ~time ~nonce ~api_key_index ~account_index ->
      ensure_loaded ();
      Lighter_ffi.sign_cancel_all_orders
        tif
        time
        (Int64.of_int nonce)
        api_key_index
        (Int64.of_int account_index))
;;

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

let nonce_counter = Atomic.make 0
let get_and_increment_nonce () = Atomic.fetch_and_add nonce_counter 1
let set_nonce n = Atomic.set nonce_counter n

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
        let __ppx_lwt_0 = Cohttp_lwt_unix.Client.get uri in
        Lwt.backtrace_bind
          "src/external/lighter.xyz/lighter_signer.ml"
          395
          (fun exn ->
            try Lwt.reraise exn with
            | exn -> exn)
          __ppx_lwt_0
          (fun (_resp, body) ->
            let __ppx_lwt_0 = Cohttp_lwt.Body.to_string body in
            Lwt.backtrace_bind
              "src/external/lighter.xyz/lighter_signer.ml"
              396
              (fun exn ->
                try Lwt.reraise exn with
                | exn -> exn)
              __ppx_lwt_0
              (fun body_str ->
                let json = Yojson.Safe.from_string body_str in
                let nonce =
                  let open Yojson.Safe.Util in
                  member "nonce" json |> to_int
                in
                set_nonce nonce;
                Logging.info_f
                  ~section
                  "Initialized nonce to %d for account %d, api_key %d"
                  nonce
                  account_index
                  api_key_index;
                Lwt.return_unit)))
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
[@@ocaml.doc
  " Fetches the next nonce from [/api/v1/nextNonce] and resets the local atomic counter to\n\
  \    match the exchange. Races a 10s timeout. "]
;;

let api_key_index = ref 0
let account_index = ref 0

let get_api_key_index () = !api_key_index
[@@ocaml.doc
  " Accessors for the cached api key/account indices, e.g. used by the nonce resync path\n\
  \    in [lighter_actions.ml]. "]
;;

let get_account_index () = !account_index

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
[@@ocaml.doc
  " Creates the Go signer client ([CreateClient]) with the given credentials and verifies\n\
  \    it with [CheckClient]. Must run before any signing call. "]
;;

type auth_token =
  { token : string
  ; expiry : float
  }

let cached_auth_token : auth_token option Atomic.t = Atomic.make None

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
      let expiry = Unix.gettimeofday () +. (6.5 *. 3600.0) in
      Atomic.set cached_auth_token (Some { token; expiry });
      Logging.info_f ~section "Auth token refreshed, valid until %.0f" expiry;
      token)
  with
  | exn ->
    Logging.error_f ~section "Failed to create auth token: %s" (Printexc.to_string exn);
    ""
[@@ocaml.doc
  " Mints an auth token with a 7h deadline and caches it for 6.5h. Returns \"\" on \
   failure. "]
;;

let get_auth_token () =
  match Atomic.get cached_auth_token with
  | Some t when Unix.gettimeofday () < t.expiry -> t.token
  | _ -> refresh_auth_token ()
;;

let time_signer f =
  let start_ns = Mtime_clock.now_ns () in
  let r = f () in
  Network_latency.record_signer
    "lighter"
    (Mtime.Span.of_uint64_ns (Int64.sub (Mtime_clock.now_ns ()) start_ns));
  r
[@@ocaml.doc
  " Times a signing operation and records it in the \"lighter\" signer profiler: local FFI\n\
  \    work (hash + ECDSA over the nonce) shown on the dashboard's NETWORK page. "]
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
          ~price
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
