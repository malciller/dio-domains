(** Order-entry transport selection for the Kraken account.

    The dimension adapter ([Kraken_module]) and the execution feed must not care whether
    orders travel over the v2 WebSocket or over FIX: they write and read the same unified
    types, and the execution cache stays the single source of truth. This module is the
    only place that knows both transports exist. Each transport registers itself at load
    time; {!active} returns the one the account tier selected.

    Why a registry rather than a direct call: the FIX implementation lives in its own
    library ([dio.kraken_fix]) so the spot WebSocket adapter carries no FIX code. The
    dependency therefore runs one way (FIX knows the spot adapter's cache and types, the
    spot adapter knows only this record), and linking [dio.kraken_fix] is what makes the
    FIX transport available.

    A tier of [enterprise] with no FIX implementation linked, or no FIX credentials
    configured, falls back to the WebSocket transport with an error in the log: an
    unreachable order path would silently stop trading. *)

let section = "kraken_order_transport"

(** Where execution reports and open-order state come from. The execution feed subscribes
    to exactly one source, so a fill is never applied twice. *)
type execution_source =
  | Execution_from_websocket
  | Execution_from_fix

(** Everything the engine needs from a Kraken order-entry transport. [token] is the
    WebSocket account token the engine threads through the order path; FIX sessions
    authenticate at logon instead and ignore it. *)
type t =
  { name : string
  ; execution_source : execution_source
  ; place_order :
      token:string
      -> order_type:string
      -> side:string
      -> order_qty:float
      -> symbol:string
      -> ?limit_price:float
      -> ?time_in_force:string
      -> ?post_only:bool
      -> ?margin:bool
      -> ?reduce_only:bool
      -> ?order_userref:int
      -> ?cl_ord_id:string
      -> ?trigger_price:float
      -> ?trigger_price_type:string
      -> ?display_qty:float
      -> ?fee_preference:string
      -> ?validate:bool
      -> ?retry_config:Error_handling.retry_config
      -> unit
      -> (Kraken_common_types.add_order_result, string) result Lwt.t
  ; amend_order :
      token:string
      -> order_id:string
      -> ?cl_ord_id:string
      -> ?order_qty:float
      -> ?limit_price:float
      -> ?limit_price_type:string
      -> ?post_only:bool
      -> ?trigger_price:float
      -> ?trigger_price_type:string
      -> ?display_qty:float
      -> ?deadline:string
      -> ?validate:bool
      -> ?symbol:string
      -> ?retry_config:Error_handling.retry_config
      -> unit
      -> (Kraken_common_types.amend_order_result, string) result Lwt.t
  ; cancel_orders :
      token:string
      -> ?order_ids:string list
      -> ?cl_ord_ids:string list
      -> ?order_userrefs:int list
      -> ?retry_config:Error_handling.retry_config
      -> unit
      -> (Kraken_common_types.cancel_order_result list, string) result Lwt.t
  ; is_connected : unit -> bool
  ; connect_and_monitor :
      token:string
      -> on_failure:(string -> unit)
      -> on_connected:(unit -> unit)
      -> unit Lwt.t
  ; send_ping : req_id:int -> timeout_ms:int -> Kraken_common_types.ws_response Lwt.t
  }

let websocket_transport : t option Atomic.t = Atomic.make None
let fix_transport : t option Atomic.t = Atomic.make None
let publish cell transport = Atomic.set cell (Some transport)

(** Registers the v2 WebSocket transport. Called from [Kraken_module] at load time. *)
let register_websocket transport = publish websocket_transport transport

(** Registers the FIX transport. Called from [Kraken_fix] at load time. *)
let register_fix transport = publish fix_transport transport

(** The transport the account tier selects. [standard] accounts always get the WebSocket;
    [enterprise] accounts need both the FIX implementation linked and a registered
    transport, otherwise they are downgraded loudly. *)
let active () =
  let standard () =
    match Atomic.get websocket_transport with
    | Some t -> t
    | None ->
      failwith
        "No Kraken order-entry transport registered: dio.kraken's Kraken_module must be \
         loaded before orders are placed"
  in
  if not (Kraken_account_tier.is_fix ())
  then standard ()
  else (
    match Atomic.get fix_transport with
    | Some t -> t
    | None ->
      Logging.error
        ~section
        "KRAKEN_ACCOUNT_TIER=enterprise but no FIX transport is linked (add \
         dio.kraken_fix to the executable); using the WebSocket transport";
      standard ())
;;

(** The WebSocket transport regardless of tier. FIX sessions use it for the account
    channels they do not carry (balances), and the health probe uses it to witness that
    the WebSocket itself is alive. *)
let websocket () =
  match Atomic.get websocket_transport with
  | Some t -> Some t
  | None -> None
;;

(** True when execution reports arrive over FIX, so the WebSocket executions channel must
    not be subscribed (one fill must not be applied twice). *)
let execution_from_fix () =
  match Atomic.get fix_transport with
  | Some t when Kraken_account_tier.is_fix () -> t.execution_source = Execution_from_fix
  | _ -> false
;;
