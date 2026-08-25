(** TWS API message dispatcher.

    Routes inbound messages to handlers registered by integer message id,
    and correlates multi-message responses by request id (contract
    details, executions). *)

let section = "ibkr_dispatcher"

(** Message handler: receives the fields after the message id. *)
type handler = string list -> unit

(** ReqId-correlated handler: data callback, end callback, and the
    condition signaled when the sequence terminates. *)
type req_handler =
  { on_data : string list -> unit
  ; on_end : unit -> unit
  ; condition : unit Lwt_condition.t
  }

(** Handlers keyed by message id; feeds register at startup. *)
let handlers : (int, handler) Hashtbl.t = Hashtbl.create 32

(** ReqId-correlated handlers, tracked until the end marker arrives. *)
let req_handlers : (int, req_handler) Hashtbl.t = Hashtbl.create 32

(** Active connection, set by [initialize] and cleared by [reset]. *)
let connection : Ibkr_connection.t option ref = ref None

(** Registers a handler for [msg_id], replacing any existing one. *)
let register_handler ~msg_id ~handler:h = Hashtbl.replace handlers msg_id h

(** Registers a reqId-correlated handler and returns the condition that
    is signaled when the response sequence ends. *)
let register_req_handler ~req_id ~on_data ~on_end =
  let condition = Lwt_condition.create () in
  Hashtbl.replace req_handlers req_id { on_data; on_end; condition };
  condition
;;

(** Removes the reqId-correlated handler; call after completion to avoid
    leaking entries. *)
let remove_req_handler ~req_id = Hashtbl.remove req_handlers req_id

(** Callback fired when the initial open-order snapshot ends. Set via
    this reference (rather than a module dependency) so the executions
    feed can finalize state without a cyclic dependency on the
    dispatcher. *)
let on_open_orders_end : (unit -> unit) option ref = ref None

(** Clears all handlers and connection state. Called before connecting
    so stale registrations do not survive a reconnect. *)
let reset () =
  Hashtbl.clear handlers;
  Hashtbl.clear req_handlers;
  connection := None;
  on_open_orders_end := None;
  Logging.info ~section "Dispatcher state reset (handlers cleared)"
;;

(** Stores the connection handle. *)
let set_connection conn = connection := Some conn

(** Active connection; fails if not yet initialized. *)
let get_connection () =
  match !connection with
  | Some conn -> conn
  | None -> failwith "IBKR dispatcher: connection not initialized"
;;

(** Routes one message: first by message id in [handlers], otherwise by
    treating the leading field as a reqId into [req_handlers]. *)
let dispatch ~msg_id ~fields =
  Logging.debug_f ~section "<<< msg_id=%d fields=%d" msg_id (List.length fields);
  match Hashtbl.find_opt handlers msg_id with
  | Some handler ->
    (try handler fields with
     | exn ->
       Logging.error_f
         ~section
         "Handler error for msg_id=%d: %s"
         msg_id
         (Printexc.to_string exn))
  | None ->
    (* Fall back to reqId correlation on the leading field. *)
    (match fields with
     | req_id_str :: _rest ->
       let req_id =
         try int_of_string req_id_str with
         | _ -> -1
       in
       (match Hashtbl.find_opt req_handlers req_id with
        | Some rh ->
          (try rh.on_data fields with
           | exn ->
             Logging.error_f
               ~section
               "ReqHandler error for reqId=%d msg_id=%d: %s"
               req_id
               msg_id
               (Printexc.to_string exn))
        | None ->
          Logging.debug_f
            ~section
            "Unhandled msg_id=%d (fields: %d)"
            msg_id
            (List.length fields))
     | [] -> Logging.debug_f ~section "Unhandled msg_id=%d (no fields)" msg_id)
;;

(** nextValidOrderIds handler: stores the server-supplied starting order
    id on the connection; later placements increment it. *)
let handle_next_valid_id fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let order_id, _fields = Ibkr_codec.read_int fields in
  let conn = get_connection () in
  conn.next_order_id <- order_id;
  Logging.info_f ~section "Next valid order ID: %d" order_id
;;

(** managedAccounts handler: caches the first listed account id. *)
let handle_managed_accounts fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let accounts, _fields = Ibkr_codec.read_string fields in
  let conn = get_connection () in
  (* Account ids are comma separated; take the first. *)
  let account =
    match String.split_on_char ',' accounts with
    | a :: _ -> String.trim a
    | [] -> accounts
  in
  conn.account_id <- account;
  Logging.info_f ~section "Managed account: %s" account
;;

(** error/errMsg handler. Classifies by code for logging; for errors
    tied to a pending reqId, signals its condition so waiters fail fast.
    Fields: version, id, code, message, advancedOrderReject. *)
let handle_error fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let id, fields = Ibkr_codec.read_int fields in
  let code, fields = Ibkr_codec.read_int fields in
  let message, fields = Ibkr_codec.read_string fields in
  let _adv_order_reject, _fields = Ibkr_codec.read_string fields in
  (* id = -1: system-wide info, not bound to an order/request. *)
  if id = -1
  then Logging.info_f ~section "Gateway info [%d]: %s" code message
  else if code >= 2100 && code <= 2110
  then
    (* Connectivity status notices; not fatal. *)
    Logging.info_f ~section "Gateway status [%d] id=%d: %s" code id message
  else if code = 10089 || code = 10167 || code = 10168
  then
    (* Expected in paper trading when live market data is unavailable. *)
    Logging.info_f ~section "Market data info [%d] id=%d: %s" code id message
  else (
    if code = 200
    then Logging.error_f ~section "Contract error [%d] id=%d: %s" code id message
    else if code >= 100 && code < 200
    then Logging.error_f ~section "Order error [%d] id=%d: %s" code id message
    else Logging.warn_f ~section "Gateway error [%d] id=%d: %s" code id message;
    (* Signal any request blocked on this id so it fails promptly. *)
    match Hashtbl.find_opt req_handlers id with
    | Some rh ->
      Lwt_condition.signal rh.condition ();
      Hashtbl.remove req_handlers id
    | None -> ())
;;

(** Ends a reqId-correlated sequence: runs the end callback, signals the
    condition, and removes the handler. [req_id_index] locates the reqId
    within the marker payload. *)
let handle_end_marker ~req_id_index fields =
  match List.nth_opt fields req_id_index with
  | Some req_id_str ->
    let req_id =
      try int_of_string req_id_str with
      | _ -> -1
    in
    (match Hashtbl.find_opt req_handlers req_id with
     | Some rh ->
       rh.on_end ();
       Lwt_condition.signal rh.condition ();
       Hashtbl.remove req_handlers req_id
     | None -> ())
  | None -> ()
;;

(** Registers connection-lifecycle handlers. Called once from
    [initialize]. *)
let register_core_handlers () =
  register_handler ~msg_id:Ibkr_types.msg_in_next_valid_id ~handler:handle_next_valid_id;
  register_handler
    ~msg_id:Ibkr_types.msg_in_managed_accounts
    ~handler:handle_managed_accounts;
  register_handler ~msg_id:Ibkr_types.msg_in_error ~handler:handle_error;
  (* ContractDetails routes to the reqId table. For server versions
     >= 164 there is no version field: fields after the message id are
     reqId, symbol, secType, ... *)
  register_handler ~msg_id:Ibkr_types.msg_in_contract_data ~handler:(fun fields ->
    match fields with
    | req_id_str :: _ ->
      let req_id =
        try int_of_string req_id_str with
        | _ -> -1
      in
      (match Hashtbl.find_opt req_handlers req_id with
       | Some rh ->
         (try rh.on_data fields with
          | exn ->
            Logging.error_f
              ~section
              "ContractDetails handler error for reqId=%d: %s"
              req_id
              (Printexc.to_string exn))
       | None ->
         Logging.debug_f ~section "ContractDetails for unregistered reqId=%d" req_id)
    | [] -> Logging.warn ~section "ContractDetails with no fields");
  (* End markers for reqId-correlated responses *)
  register_handler ~msg_id:Ibkr_types.msg_in_contract_data_end ~handler:(fun fields ->
    (* Marker payload: version, reqId. *)
    let _version, fields = Ibkr_codec.read_int fields in
    handle_end_marker ~req_id_index:0 fields);
  register_handler ~msg_id:Ibkr_types.msg_in_open_order_end ~handler:(fun _fields ->
    Logging.debug ~section "Open orders end";
    (* Invoked via the [on_open_orders_end] reference so the executions
       feed can finalize the snapshot without a module cycle. *)
    match !on_open_orders_end with
    | Some f -> f ()
    | None -> ());
  register_handler ~msg_id:Ibkr_types.msg_in_execution_data_end ~handler:(fun fields ->
    let _version, fields = Ibkr_codec.read_int fields in
    handle_end_marker ~req_id_index:0 fields);
  register_handler ~msg_id:Ibkr_types.msg_in_account_download_end ~handler:(fun _fields ->
    Logging.debug ~section "Account download end");
  register_handler ~msg_id:Ibkr_types.msg_in_position_end ~handler:(fun _fields ->
    Logging.debug ~section "Position data end")
;;

(** Hooks that re-register feed handlers after [reset]; set by the
    supervisor to avoid module cycles. *)
let on_initialize_hooks : (unit -> unit) list ref = ref []

(** Stores the connection, registers core handlers, and runs the
    initialization hooks to restore feed handlers cleared by [reset]. *)
let initialize conn =
  set_connection conn;
  register_core_handlers ();
  List.iter (fun f -> f ()) !on_initialize_hooks;
  Logging.info ~section "Dispatcher initialized with core handlers"
;;
