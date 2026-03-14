(** Hyperliquid Unified WebSocket Client
    Handles both market data subscriptions and authenticated L1 actions
*)

open Lwt.Infix
open Concurrency

let section = "hyperliquid_ws"

(** Safely force Conduit context *)
let get_conduit_ctx () =
  try
    Lazy.force Conduit_lwt_unix.default_ctx
  with
  | CamlinternalLazy.Undefined ->
      Logging.error ~section "Conduit context was accessed before initialization";
      raise (Failure "Conduit context not initialized")
  | exn ->
      Logging.error_f ~section "Failed to get Conduit context: %s" (Printexc.to_string exn);
      raise exn

let shutdown_requested = Atomic.make false
let signal_shutdown () = Atomic.set shutdown_requested true

module Response_table = Hashtbl.Make (struct
  type t = int
  let equal = Int.equal
  let hash = Hashtbl.hash
end)

module Heartbeat_bus = Event_bus.Make (struct type t = unit end)
module Connection_bus = Event_bus.Make (struct type t = [ `Connected | `Disconnected of string ] end)

type connection_with_generation = {
  conn: Websocket_lwt_unix.conn;
  generation: int;
}

module Market_data_bus = Event_bus.Make (struct type t = Yojson.Safe.t end)
let market_data_bus = Market_data_bus.create "hyperliquid_market_data"
let subscribe_market_data () = Market_data_bus.subscribe market_data_bus

type state = {
  mutex: Lwt_mutex.t;
  mutable conn: connection_with_generation option;
  mutable reader: unit Lwt.t option;
  mutable connecting: bool;
  mutable connection_generation: int;
  responses: (Yojson.Safe.t Lwt.u * float) Response_table.t;  (* wakener * timestamp *)
  mutable on_failure: (string -> unit) option;
  connected: bool Atomic.t;
  mutable subscriptions: Yojson.Safe.t list;
  mutable req_id_seq: int;
}

let state = {
  mutex = Lwt_mutex.create ();
  conn = None;
  reader = None;
  connecting = false;
  connection_generation = 0;
  responses = Response_table.create 32;
  on_failure = None;
  connected = Atomic.make false;
  subscriptions = [];
  req_id_seq = 1;
}



let heartbeat_bus = Heartbeat_bus.create "hyperliquid_ws_heartbeat"
let connection_bus = Connection_bus.create "hyperliquid_ws_connection"

let heartbeat_stream () = Heartbeat_bus.subscribe heartbeat_bus
let connection_stream () = Connection_bus.subscribe connection_bus

let notify_heartbeat () = Heartbeat_bus.publish heartbeat_bus ()
let notify_connection status = Connection_bus.publish connection_bus status

let next_req_id () =
  Lwt_mutex.with_lock state.mutex (fun () ->
    let id = state.req_id_seq in
    state.req_id_seq <- id + 1;
    Lwt.return id
  )

let resolve_response id json =
  Lwt_mutex.with_lock state.mutex (fun () ->
    try
      let wakener, _timestamp = Response_table.find state.responses id in
      Response_table.remove state.responses id;
      Lwt.return (Some wakener)
    with Not_found ->
      Lwt.return None
  ) >>= function
  | Some wakener ->
      (try Lwt.wakeup_later wakener json with _ -> ());
      Lwt.return_unit
  | None ->
      Lwt.return_unit

let handle_frame frame =
  Concurrency.Tick_event_bus.publish_tick ();
  notify_heartbeat ();
  
  try
    let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
    let channel = Yojson.Safe.Util.(member "channel" json |> to_string_option) in
    
    (* Responses to 'post' method specify channel="post" and include the id *)
    if channel = Some "post" || channel = Some "error" then begin
      let data = Yojson.Safe.Util.member "data" json in
      let maybe_id = match Yojson.Safe.Util.member "id" data with
        | `Int i -> Some i
        | _ -> None
      in
      match maybe_id with
      | Some id -> 
          Lwt.async (fun () -> resolve_response id json);
          Lwt.return_unit
      | None -> 
          if channel = Some "error" then
            Logging.error_f ~section "Hyperliquid WS Error: %s" frame.Websocket.Frame.content;
          Lwt.return_unit
    end else if channel = Some "pong" then begin
       Lwt.return_unit
    end else begin
      (* General Market Data routing *)
      Market_data_bus.publish market_data_bus json;
      Lwt.return_unit
    end
  with exn ->
    Logging.error_f ~section "Failed to handle frame: %s (Frame: %s)" (Printexc.to_string exn) frame.Websocket.Frame.content;
    Lwt.return_unit

let reset_state conn ~notify_failure reason =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | Some current when current.conn == conn ->
        let new_generation = state.connection_generation + 1 in
        let wakers = Response_table.fold (fun _ (wak, _) acc -> wak :: acc) state.responses [] in
        let reader = state.reader in
        Response_table.clear state.responses;
        state.conn <- None;
        state.reader <- None;
        state.connecting <- false;
        state.connection_generation <- new_generation;
        Atomic.set state.connected false;
        let failure_cb = if notify_failure then state.on_failure else None in
        Lwt.return (wakers, reader, true, failure_cb)
    | _ ->
        state.connecting <- false;
        Lwt.return ([], None, false, None)
  ) >>= fun (wakers, reader_task, should_reset, failure_cb) ->
  if not should_reset then Lwt.return_unit
  else begin
    (match reader_task with
     | Some reader -> Lwt.cancel reader
     | None -> ());
    Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->
    List.iter (fun wak ->
      try Lwt.wakeup_later_exn wak (Failure reason) with _ -> ()
    ) wakers;
    notify_connection (`Disconnected reason);
    (match failure_cb with Some f -> f reason | None -> ());
    Lwt.return_unit
  end

let rec reader_loop conn generation =
  if Atomic.get shutdown_requested then Lwt.return_unit
  else
    Lwt.catch
      (fun () ->
        Websocket_lwt_unix.read conn >>= function
        | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
            reset_state conn ~notify_failure:true "Connection closed by server"
        | frame ->
            handle_frame frame >>= fun () ->
            reader_loop conn generation
      )
      (function
        | End_of_file -> reset_state conn ~notify_failure:true "Connection closed unexpectedly (End_of_file)"
        | exn -> reset_state conn ~notify_failure:true (Printf.sprintf "WebSocket read error: %s" (Printexc.to_string exn))
      )

let start_reader conn generation =
  let task =
    Lwt.catch
      (fun () -> reader_loop conn generation)
      (fun exn -> reset_state conn ~notify_failure:true (Printexc.to_string exn))
  in
  Lwt.async (fun () -> task);
  task

let connect () : Websocket_lwt_unix.conn Lwt.t =
  let uri = Uri.of_string "wss://api.hyperliquid.xyz/ws" in
  Logging.info ~section "Connecting to Hyperliquid WebSocket...";
  Lwt_unix.getaddrinfo "api.hyperliquid.xyz" "443" [ Unix.AI_FAMILY Unix.PF_INET ] >>= fun addresses ->
  let ip =
    match addresses with
    | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ -> Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve api.hyperliquid.xyz"
  in
  let client = `TLS (`Hostname "api.hyperliquid.xyz", `IP ip, `Port 443) in
  let ctx = get_conduit_ctx () in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
  Logging.info ~section "Hyperliquid WebSocket connection established";
  Lwt.return conn

let ensure_connection ?on_failure ?on_connected () =
  Lwt_mutex.with_lock state.mutex (fun () ->
    (match on_failure with Some f -> state.on_failure <- Some f | None -> ());
    if state.conn <> None then begin
      (match on_connected with Some f -> f () | None -> ());
      Lwt.return `Already
    end else if state.connecting then begin
      Lwt.return `Wait
    end else begin
      state.connecting <- true;
      Lwt.return `Connect
    end
  ) >>= function
  | `Already -> Lwt.return_unit
  | `Wait ->
      let rec wait_for_connection () =
        Lwt_unix.sleep 0.1 >>= fun () ->
        Lwt_mutex.with_lock state.mutex (fun () ->
          if state.conn <> None then begin
            (match on_connected with Some f -> f () | None -> ());
            Lwt.return true
          end else if state.connecting then
            Lwt.return false
          else Lwt.return true
        ) >>= fun done_ ->
        if done_ then Lwt.return_unit else wait_for_connection ()
      in
      wait_for_connection ()
  | `Connect ->
      Lwt.catch
        (fun () ->
           connect () >>= fun conn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             let generation = state.connection_generation in
             state.connecting <- false;
             state.conn <- Some { conn; generation };
             state.reader <- None;
             Atomic.set state.connected true;
             Lwt.return (Some (conn, generation)))
        )
        (fun exn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connecting <- false;
             Lwt.return_unit) >>= fun () ->
           Lwt.fail exn
        ) >>= function
      | None -> Lwt.return_unit
      | Some (conn, generation) ->
          let reader = start_reader conn generation in
          Lwt_mutex.with_lock state.mutex (fun () ->
            state.reader <- Some reader;
            Lwt.return_unit) >>= fun () ->
          notify_connection `Connected;
          Lwt_mutex.with_lock state.mutex (fun () -> Lwt.return state.subscriptions) >>= fun subs ->
          Lwt_list.iter_s (fun sub ->
            let content = Yojson.Safe.to_string sub in
            Websocket_lwt_unix.write conn (Websocket.Frame.create ~content ())
          ) (List.rev subs) >>= fun () ->
          (match on_connected with Some f -> f () | None -> ());
          reader >>= fun () ->
          Lwt.return_unit

let subscribe message =
  Lwt_mutex.with_lock state.mutex (fun () ->
    if not (List.mem message state.subscriptions) then
      state.subscriptions <- message :: state.subscriptions;
    match state.conn with
    | Some conn_with_gen ->
        let content = Yojson.Safe.to_string message in
        Websocket_lwt_unix.write conn_with_gen.conn (Websocket.Frame.create ~content ())
    | None -> Lwt.return_unit
  )

let send_post_request payload ~timeout_ms =
  next_req_id () >>= fun id ->
  let message = `Assoc [
    ("method", `String "post");
    ("id", `Int id);
    ("request", payload)
  ] in
  let message_str = Yojson.Safe.to_string message in
  
  if Atomic.get shutdown_requested then
    Lwt.fail_with "Shutdown"
  else
    Lwt_mutex.with_lock state.mutex (fun () ->
      match state.conn with
      | None -> Lwt.return (Error (Failure "Hyperliquid WS not connected"))
      | Some conn ->
          let waiter, wakener = Lwt.wait () in
          Response_table.add state.responses id (wakener, Unix.time ());
          Lwt.catch
            (fun () ->
               Websocket_lwt_unix.write conn.conn (Websocket.Frame.create ~content:message_str ()) >|= fun () ->
               Ok waiter)
            (fun exn ->
               Response_table.remove state.responses id;
               Lwt.return (Error exn))
    ) >>= function
    | Error exn -> Lwt.fail exn
    | Ok waiter ->
        let timeout = float_of_int timeout_ms /. 1000.0 in
        Lwt.pick [
          (waiter >|= fun resp -> `Response resp);
          (Lwt_unix.sleep timeout >|= fun () -> `Timeout)
        ] >>= function
        | `Response r -> Lwt.return r
        | `Timeout ->
            Lwt_mutex.with_lock state.mutex (fun () ->
              let still_pending = Response_table.mem state.responses id in
              if still_pending then Response_table.remove state.responses id;
              Lwt.return still_pending
            ) >>= fun was_pending ->
            if was_pending then begin
              Lwt.cancel waiter;
              Lwt.fail_with "Timeout waiting for response"
            end else Lwt.return `Null (* Race condition fallback *)

let is_connected () = Atomic.get state.connected
let init () = ensure_connection ()
let connect_and_monitor ~on_failure ~on_connected = ensure_connection ~on_failure ~on_connected ()

let close () =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | None -> Lwt.return None
    | Some conn ->
        state.conn <- None;
        Atomic.set state.connected false;
        let pending = Response_table.fold (fun _ (wak, _) acc -> wak :: acc) state.responses [] in
        Response_table.clear state.responses;
        Lwt.return (Some (conn.conn, pending))
  ) >>= function
  | None -> Lwt.return_unit
  | Some (conn, pending) ->
      List.iter (fun wak -> Lwt.wakeup_later_exn wak (Failure "Client requested close")) pending;
      notify_connection (`Disconnected "client_closed");
      Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit)
