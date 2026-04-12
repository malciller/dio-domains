(** TWS API message dispatcher.

    This module is responsible for routing inbound messages from the Interactive Brokers Gateway to the correct handler module based on the integer message identifier. The architecture operates on an event-driven paradigm where each distinct incoming message type triggers a specific, previously registered callback function.

    Additionally, the module implements request and response correlation for asynchronous data streams that utilize identifier-based pairing, such as correlating contract details requests with their corresponding responses. *)

let section = "ibkr_dispatcher"

(** Defines the callback signature for standard message handlers. The function receives the list of string fields remaining in the message array after the initial message identifier has been consumed by the dispatcher. *)
type handler = string list -> unit

(** Defines the record structure for handlers that process responses correlated by a request identifier. This includes a data callback, a completion callback, and an Lwt condition variable utilized for synchronizing the asynchronous completion state. *)
type req_handler = {
  on_data: string list -> unit;
  on_end: unit -> unit;
  condition: unit Lwt_condition.t;
}

(** A hash table indexing registered handler functions by their integer message identifier. The table is mutable to enable disparate feed modules to dynamically register their specific parsing routines during application startup. *)
let handlers : (int, handler) Hashtbl.t = Hashtbl.create 32

(** A hash table indexing correlated request handlers by their integer request identifier. This tracks multi-message response sequences such as contract details or execution details until the termination marker is received. *)
let req_handlers : (int, req_handler) Hashtbl.t = Hashtbl.create 32

(** A mutable option reference maintaining the active socket connection state. This reference is populated during the initialization phase and cleared upon reset operations. *)
let connection : Ibkr_connection.t option ref = ref None

(** Registers a specific parsing and processing function for a given incoming message identifier. This overwrites any existing handler registered for the same identifier. *)
let register_handler ~msg_id ~handler:h =
  Hashtbl.replace handlers msg_id h

(** Registers a handler sequence parameterized by a request identifier to track multi-part asynchronous responses. The function returns an Lwt condition variable that will be broadcast when the terminal message of the sequence is processed. *)
let register_req_handler ~req_id ~on_data ~on_end =
  let condition = Lwt_condition.create () in
  Hashtbl.replace req_handlers req_id { on_data; on_end; condition };
  condition

(** Removes the handler associated with the specified request identifier from the correlation table. This cleanup is mandated to prevent memory leaks after an asynchronous multi-part response completes. *)
let remove_req_handler ~req_id =
  Hashtbl.remove req_handlers req_id

(** An option reference containing a callback function invoked by the dispatcher when the Interactive Brokers Gateway transmits the terminal marker for the initial open orders snapshot. The callback is registered externally by the execution supervisor to prevent circular architectural dependencies with the execution feed module. *)
let on_open_orders_end : (unit -> unit) option ref = ref None

(** Restores the dispatcher state to its initial blank parameters. This method executes prior to connection establishment sequences to actively purge stale handler references, clear correlation tables, and preclude the aggregation of orphaned closure frames in memory. *)
let reset () =
  Hashtbl.clear handlers;
  Hashtbl.clear req_handlers;
  connection := None;
  on_open_orders_end := None;
  Logging.info ~section "Dispatcher state reset (handlers cleared)"

(** Mutates the internal option reference to maintain a handle to the provided active socket connection. *)
let set_connection conn =
  connection := Some conn

(** Retrieves the current active connection object. A runtime exception is triggered if the caller attempts to evaluate the connection reference while the connection is unitialized. *)
let get_connection () =
  match !connection with
  | Some conn -> conn
  | None -> failwith "IBKR dispatcher: connection not initialized"

(** The root coordination mechanism invoked iteratively by the underlying socket reader utility for every deserialized incoming datagram. The logic partitions execution paths based on the integer message identifier towards either the statically registered handler table or the dynamically typed request correlation table. *)
let dispatch ~msg_id ~fields =
  Logging.debug_f ~section "<<< msg_id=%d fields=%d" msg_id (List.length fields);
  (* Initially attempt to resolve the routing address within the static message identifier callback structure. *)
  match Hashtbl.find_opt handlers msg_id with
  | Some handler ->
      (try handler fields
       with exn ->
         Logging.error_f ~section "Handler error for msg_id=%d: %s"
           msg_id (Printexc.to_string exn))
  | None ->
      (* Secondary fallback processes the initial deserialized array field as a numeric request identifier to interface with the asymmetric continuation dictionary. *)
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

(** Processes the structural layout of the next valid identifier message packet. This procedure increments the sequence tracking counter on the connection object to coordinate future order placement sequences. *)
let handle_next_valid_id fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let order_id, _fields = Ibkr_codec.read_int fields in
  let conn = get_connection () in
  conn.next_order_id <- order_id;
  Logging.info_f ~section "Next valid order ID: %d" order_id

(** Processes the payload of the managed accounts response from the venue. The handler extracts the default account tag string for subsequent use in order configurations. *)
let handle_managed_accounts fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let accounts, _fields = Ibkr_codec.read_string fields in
  let conn = get_connection () in
  (* Safely extract the primary account designation string assuming the venue emits a comma separated structure of identifiers. *)
  let account = match String.split_on_char ',' accounts with
    | a :: _ -> String.trim a
    | [] -> accounts
  in
  conn.account_id <- account;
  Logging.info_f ~section "Managed account: %s" account

(** Evaluates system notifications, gateway faults, and error payloads delivered by the venue. The handler inspects the error classification integers and either emits the corresponding logging telemetry or signals the correlating request identifiers to force early termination of asynchronous loops. *)
let handle_error fields =
  let _version, fields = Ibkr_codec.read_int fields in
  let id, fields = Ibkr_codec.read_int fields in
  let code, fields = Ibkr_codec.read_int fields in
  let message, fields = Ibkr_codec.read_string fields in
  let _adv_order_reject, _fields = Ibkr_codec.read_string fields in
  (* Identifier -1 denotes non-fatal informational broadcast messages from the system. *)
  if id = -1 then
    Logging.info_f ~section "Gateway info [%d]: %s" code message
  else if code >= 2100 && code <= 2110 then
    (* Connectivity status messages are informational and do not indicate a fatal connection condition. *)
    Logging.info_f ~section "Gateway status [%d] id=%d: %s" code id message
  else if code = 10089 || code = 10167 || code = 10168 then
    (* Market data subscription and operation mode notifications are expected conditions during simulated trading loops when realtime quote streaming is deactivated. *)
    Logging.info_f ~section "Market data info [%d] id=%d: %s" code id message
  else begin
    (* Evaluate bounds boundaries to serialize exact telemetry payload types. *)
    if code = 200 then
      Logging.error_f ~section "Contract error [%d] id=%d: %s" code id message
    else if code >= 100 && code < 200 then
      Logging.error_f ~section "Order error [%d] id=%d: %s" code id message
    else
      Logging.warn_f ~section "Gateway error [%d] id=%d: %s" code id message;
    (* Resolve synchronous blockers tied to request configurations to prompt swift failure exceptions and unlock system dependencies. *)
    match Hashtbl.find_opt req_handlers id with
    | Some rh ->
        Lwt_condition.signal rh.condition ();
        Hashtbl.remove req_handlers id
    | None -> ()
  end

(** Terminates a request correlation sequence by evaluating the end marker payload. The handler invokes completion routines, broadcasts the synchronization condition, and explicitly clears the reference from the correlation database. *)
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

(** Populates the static dispatcher database with fundamental handlers crucial to connection maintenance and session establishment. This function is restricted to executing precisely once during the initialization block. *)
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

  (* The contract details payloads must route to the request identifier correlated callback table. It is important to note that for server versions greater than or equal to 164 there is no distinct version field present in the network datagram. The remaining fields subsequent to the message identifier follow the structure of request identifier, trailing symbol characters, and security types. *)
  register_handler
    ~msg_id:Ibkr_types.msg_in_contract_data
    ~handler:(fun fields ->
      (* No version field validation process required for recent server versions. *)
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
      (* The structure of the terminal marker datagram follows the index format of version sequentially followed by the numeric request identifier. *)
      let _version, fields = Ibkr_codec.read_int fields in
      handle_end_marker ~req_id_index:0 fields);

  register_handler
    ~msg_id:Ibkr_types.msg_in_open_order_end
    ~handler:(fun _fields ->
      Logging.debug ~section "Open orders end";
      (* Invoke the callback mechanism dynamically injected into the local reference if functionally active. This paradigm empowers the execution supervisor to correctly demarcate execution structures into ready states strictly following the termination of the starting snapshots. Utilizing a referenced callback mitigates a cyclical module dependency problem between the execution stream and the static dispatcher file. *)
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

(** A mutable list of initialization closures intended to restore the dynamic handler registry subsequent to a systematic reset operation. These functions are seeded externally by the operational supervisor to bypass module cycle restrictions. *)
let on_initialize_hooks : (unit -> unit) list ref = ref []

(** Finalizes the boot parameters of the dispatcher by persisting a handle to the connection and launching the static core handlers. It systematically executes the external initialization closures which are necessary to recover state mappings dynamically allocated by data feeds that were sanitized by the reset procedure. *)
let initialize conn =
  set_connection conn;
  register_core_handlers ();
  (* Re-register dynamic external feed handlers through initialization hooks to bypass restrictive module definition cycles during runtime. *)
  List.iter (fun f -> f ()) !on_initialize_hooks;
  Logging.info ~section "Dispatcher initialized with core handlers"
