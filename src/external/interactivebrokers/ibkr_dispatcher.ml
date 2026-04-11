(** TWS API message dispatcher.

    Routes inbound messages from IB Gateway to the appropriate handler
    module based on message ID. Event-driven — each incoming message
    type triggers a registered callback.

    Also provides request/response correlation for messages that use
    reqId-based pairing (e.g., reqContractDetails → contractDetails). *)

let section = "ibkr_dispatcher"

(** Callback type for message handlers. Receives the remaining fields
    after the message ID has been consumed. *)
type handler = string list -> unit

(** Callback signature for reqId-correlated responses. *)
type req_handler = {
  on_data: string list -> unit;
  on_end: unit -> unit;
  condition: unit Lwt_condition.t;
}

(** Per-message-ID handler table. Mutable to allow feeds to register
    themselves during startup. *)
let handlers : (int, handler) Hashtbl.t = Hashtbl.create 32

(** Per-reqId correlation table for request/response patterns
    (contractDetails, execDetails, etc.). *)
let req_handlers : (int, req_handler) Hashtbl.t = Hashtbl.create 32

(** Mutable reference to the connection, set during initialization. *)
let connection : Ibkr_connection.t option ref = ref None

(** Register a handler for a specific inbound message ID. *)
let register_handler ~msg_id ~handler:h =
  Hashtbl.replace handlers msg_id h

(** Register a reqId-correlated handler. Returns the [Lwt_condition.t]
    that will be signalled when the "end" message arrives. *)
let register_req_handler ~req_id ~on_data ~on_end =
  let condition = Lwt_condition.create () in
  Hashtbl.replace req_handlers req_id { on_data; on_end; condition };
  condition

(** Remove a reqId-correlated handler after completion. *)
let remove_req_handler ~req_id =
  Hashtbl.remove req_handlers req_id

(** Callback invoked when the gateway sends openOrderEnd, indicating the
    initial open-orders snapshot is complete. Set externally (e.g., by the
    supervisor) to avoid a dependency cycle with ibkr_executions_feed. *)
let on_open_orders_end : (unit -> unit) option ref = ref None

(** Reset all dispatcher state. Called before reconnection to prevent
    stale handler entries and leaked closures from accumulating. *)
let reset () =
  Hashtbl.clear handlers;
  Hashtbl.clear req_handlers;
  connection := None;
  on_open_orders_end := None;
  Logging.info ~section "Dispatcher state reset (handlers cleared)"

(** Set the active connection reference. *)
let set_connection conn =
  connection := Some conn

(** Get the active connection, raising if not set. *)
let get_connection () =
  match !connection with
  | Some conn -> conn
  | None -> failwith "IBKR dispatcher: connection not initialized"

(** Master dispatch function. Called by the connection reader loop for
    every inbound message. Routes by message ID to registered handlers
    or reqId-correlated handlers. *)
let dispatch ~msg_id ~fields =
  Logging.debug_f ~section "<<< msg_id=%d fields=%d" msg_id (List.length fields);
  (* First, try the static handler table *)
  match Hashtbl.find_opt handlers msg_id with
  | Some handler ->
      (try handler fields
       with exn ->
         Logging.error_f ~section "Handler error for msg_id=%d: %s"
           msg_id (Printexc.to_string exn))
  | None ->
      (* For messages that carry a reqId as the first field,
         check the req_handlers table *)
      (match fields with
       | req_id_str :: _rest ->
           let req_id = try int_of_string req_id_str with _ -> -1 in
           (match Hashtbl.find_opt req_handlers req_id with
            | Some rh ->
                (try rh.on_data fields
                 with exn ->
                   Logging.error_f ~section "ReqHandler error for reqId=%d msg_id=%d: %s"
                     req_id msg_id (Printexc.to_string exn))
            | None ->
                Logging.debug_f ~section "Unhandled msg_id=%d (fields: %d)"
                  msg_id (List.length fields))
       | [] ->
           Logging.debug_f ~section "Unhandled msg_id=%d (no fields)" msg_id)

(** Handle the nextValidId message. Updates the connection's order ID counter. *)
let handle_next_valid_id fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let order_id, _fields = Ibkr_codec.read_int fields in
  let conn = get_connection () in
  conn.next_order_id <- order_id;
  Logging.info_f ~section "Next valid order ID: %d" order_id

(** Handle the managedAccounts message. Stores the account ID. *)
let handle_managed_accounts fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let accounts, _fields = Ibkr_codec.read_string fields in
  let conn = get_connection () in
  (* Take the first account if multiple are comma-separated *)
  let account = match String.split_on_char ',' accounts with
    | a :: _ -> String.trim a
    | [] -> accounts
  in
  conn.account_id <- account;
  Logging.info_f ~section "Managed account: %s" account

(** Handle error messages from the gateway. *)
let handle_error fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let id, fields = Ibkr_codec.read_int fields in
  let code, fields = Ibkr_codec.read_int fields in
  let message, fields = Ibkr_codec.read_string fields in
  let _adv_order_reject, _fields = Ibkr_codec.read_string fields in
  (* id=-1 means informational messages (e.g., "market data farm connected") *)
  if id = -1 then
    Logging.info_f ~section "Gateway info [%d]: %s" code message
  else if code >= 2100 && code <= 2110 then
    (* Connectivity status messages — informational *)
    Logging.info_f ~section "Gateway status [%d] id=%d: %s" code id message
  else if code = 10089 || code = 10167 || code = 10168 then
    (* Market data subscription/mode notifications — expected during
       paper trading when live data is not subscribed. *)
    Logging.info_f ~section "Market data info [%d] id=%d: %s" code id message
  else begin
    (* Log the error *)
    if code = 200 then
      Logging.error_f ~section "Contract error [%d] id=%d: %s" code id message
    else if code >= 100 && code < 200 then
      Logging.error_f ~section "Order error [%d] id=%d: %s" code id message
    else
      Logging.warn_f ~section "Gateway error [%d] id=%d: %s" code id message;
    (* If there's a req_handler waiting, signal it so it fails fast *)
    match Hashtbl.find_opt req_handlers id with
    | Some rh ->
        Lwt_condition.signal rh.condition ();
        Hashtbl.remove req_handlers id
    | None -> ()
  end

(** Handle "end" markers for reqId-correlated messages. *)
let handle_end_marker ~req_id_index fields =
  match List.nth_opt fields req_id_index with
  | Some req_id_str ->
      let req_id = try int_of_string req_id_str with _ -> -1 in
      (match Hashtbl.find_opt req_handlers req_id with
       | Some rh ->
           rh.on_end ();
           Lwt_condition.signal rh.condition ();
           Hashtbl.remove req_handlers req_id
       | None -> ())
  | None -> ()

(** Register all core handlers. Called once during initialization. *)
let register_core_handlers () =
  register_handler
    ~msg_id:Ibkr_types.msg_in_next_valid_id
    ~handler:handle_next_valid_id;

  register_handler
    ~msg_id:Ibkr_types.msg_in_managed_accounts
    ~handler:handle_managed_accounts;

  register_handler
    ~msg_id:Ibkr_types.msg_in_error
    ~handler:handle_error;

  (* contractDetails: route to reqId-correlated handler.
     For serverVersion >= 164 (ours is 176), there is NO version field.
     Fields after msgId are: [reqId, symbol, secType, ...] *)
  register_handler
    ~msg_id:Ibkr_types.msg_in_contract_data
    ~handler:(fun fields ->
      (* No version field for serverVersion >= 164 *)
      match fields with
      | req_id_str :: _ ->
          let req_id = try int_of_string req_id_str with _ -> -1 in
          (match Hashtbl.find_opt req_handlers req_id with
           | Some rh ->
               (try rh.on_data fields
                with exn ->
                  Logging.error_f ~section "ContractDetails handler error for reqId=%d: %s"
                    req_id (Printexc.to_string exn))
           | None ->
               Logging.debug_f ~section "ContractDetails for unregistered reqId=%d" req_id)
      | [] ->
          Logging.warn ~section "ContractDetails with no fields");

  (* End markers for reqId-correlated responses *)
  register_handler
    ~msg_id:Ibkr_types.msg_in_contract_data_end
    ~handler:(fun fields ->
      (* contractDetailsEnd: version, reqId *)
      let _version, fields = Ibkr_codec.read_int fields in
      handle_end_marker ~req_id_index:0 fields);

  register_handler
    ~msg_id:Ibkr_types.msg_in_open_order_end
    ~handler:(fun _fields ->
      Logging.debug ~section "Open orders end";
      (* Invoke the on_open_orders_end callback if registered.
         Used by supervisor to mark execution stores as ready
         after the initial snapshot completes. Callback ref avoids
         a direct dependency on ibkr_executions_feed. *)
      (match !on_open_orders_end with
       | Some f -> f ()
       | None -> ()));

  register_handler
    ~msg_id:Ibkr_types.msg_in_execution_data_end
    ~handler:(fun fields ->
      let _version, fields = Ibkr_codec.read_int fields in
      handle_end_marker ~req_id_index:0 fields);

  register_handler
    ~msg_id:Ibkr_types.msg_in_account_download_end
    ~handler:(fun _fields ->
      Logging.debug ~section "Account download end");

  register_handler
    ~msg_id:Ibkr_types.msg_in_position_end
    ~handler:(fun _fields ->
      Logging.debug ~section "Position data end")

(** Callbacks to re-register feed handlers after reset().
    Set externally by the supervisor to avoid dependency cycles. *)
let on_initialize_hooks : (unit -> unit) list ref = ref []

(** Initialize the dispatcher with a connection and register core handlers.
    Also invokes on_initialize_hooks to re-register feed handlers that
    get wiped by reset() on every connect/reconnect. *)
let initialize conn =
  set_connection conn;
  register_core_handlers ();
  (* Re-register feed handlers via hooks — avoids dependency cycles *)
  List.iter (fun f -> f ()) !on_initialize_hooks;
  Logging.info ~section "Dispatcher initialized with core handlers"
