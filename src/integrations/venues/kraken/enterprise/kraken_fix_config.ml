(** FIX session configuration, read once from the environment.

    Everything here is account provisioning data handed out by Kraken during onboarding,
    so it lives beside the other venue credentials in [.env] rather than in [config.json]:
    the tier is a property of the account, not of a strategy.

    Variables, all read once at startup:

    - [KRAKEN_ACCOUNT_TIER] - [standard] (WebSocket) or [enterprise] (FIX).
    - [KRAKEN_FIX_API_KEY] - FIX key (Kraken Pro, key type "FIX").
    - [KRAKEN_FIX_API_SECRET] - base64 secret that signs the logon password.
    - [KRAKEN_FIX_SENDER_COMP_ID] - our CompID, assigned at onboarding.
    - [KRAKEN_FIX_TARGET_COMP_ID] - [KRAKEN-TRD] by default.
    - [KRAKEN_FIX_HOST] - [colo-london.vip-fix.kraken.com] by default.
    - [KRAKEN_FIX_PORT] - trading port, [4001] by default.
    - [KRAKEN_FIX_HEARTBEAT_S] - negotiated HeartBtInt, [30] by default.
    - [KRAKEN_FIX_CANCEL_ON_DISCONNECT] - [1] cancels this session's orders when it drops;
      [0] (the default) leaves them working, matching the WebSocket workflow.
    - [KRAKEN_FIX_UAT] - [1] switches host and port to the UAT environment.
    - [KRAKEN_FIX_REQUEST_TIMEOUT_MS] - order acknowledgement timeout, [10000] by default. *)

let section = "kraken_fix_config"

type t =
  { host : string
  ; port : int
  ; sender_comp_id : string
  ; target_comp_id : string
  ; api_key : string
  ; api_secret : string
  ; heartbeat_s : int
  ; cancel_on_disconnect : bool
  (** [true] sends [8674=0]: every order placed on the session dies with it. [false]
      (default) sends [8674=1], matching the WebSocket adapter, where a dropped connection
      leaves resting orders working. *)
  ; connect_timeout_s : float
  ; logon_timeout_s : float
  ; request_timeout_ms : int
  }

let production_host = "colo-london.vip-fix.kraken.com"
let uat_host = "colo-london.vip-fix.uat.kraken.com"
let trading_port = 4001
let uat_trading_port = 4001
let env var = Sys.getenv_opt var |> Option.map String.trim

let env_or var ~default =
  match env var with
  | Some v when v <> "" -> v
  | _ -> default
;;

let env_int var ~default =
  match env var with
  | Some v ->
    (try int_of_string v with
     | _ -> default)
  | None -> default
;;

let env_bool var ~default =
  match env var |> Option.map String.lowercase_ascii with
  | Some ("1" | "true" | "yes" | "y" | "on") -> true
  | Some ("0" | "false" | "no" | "n" | "off") -> false
  | Some other ->
    Logging.warn_f ~section "%s=%S is not a boolean; using %b" var other default;
    default
  | None -> default
;;

(** Reads the session configuration. Returns [None] when the credentials a logon needs are
    absent: the tier resolver already refuses [enterprise] in that case, so this is a
    second, independent guard rather than the primary check. *)
let load () : t option =
  match
    env "KRAKEN_FIX_API_KEY", env "KRAKEN_FIX_API_SECRET", env "KRAKEN_FIX_SENDER_COMP_ID"
  with
  | Some api_key, Some api_secret, Some sender_comp_id
    when api_key <> "" && api_secret <> "" && sender_comp_id <> "" ->
    let uat = env_bool "KRAKEN_FIX_UAT" ~default:false in
    Some
      { host =
          env_or "KRAKEN_FIX_HOST" ~default:(if uat then uat_host else production_host)
      ; port =
          env_int
            "KRAKEN_FIX_PORT"
            ~default:(if uat then uat_trading_port else trading_port)
      ; sender_comp_id
      ; target_comp_id = env_or "KRAKEN_FIX_TARGET_COMP_ID" ~default:"KRAKEN-TRD"
      ; api_key
      ; api_secret
      ; heartbeat_s = max 5 (env_int "KRAKEN_FIX_HEARTBEAT_S" ~default:30)
      ; cancel_on_disconnect = env_bool "KRAKEN_FIX_CANCEL_ON_DISCONNECT" ~default:false
      ; connect_timeout_s = 20.0
      ; logon_timeout_s = 15.0
      ; request_timeout_ms = env_int "KRAKEN_FIX_REQUEST_TIMEOUT_MS" ~default:10_000
      }
  | _ ->
    Logging.error
      ~section
      "KRAKEN_FIX_API_KEY, KRAKEN_FIX_API_SECRET and KRAKEN_FIX_SENDER_COMP_ID are all \
       required for a FIX session";
    None
;;
