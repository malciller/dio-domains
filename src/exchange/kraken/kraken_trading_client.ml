(** Kraken Trading WebSocket Client

    This module provides a WebSocket client for sending authenticated trading requests
    (place orders, amend orders, cancel orders) to Kraken's WebSocket API v2.
*)

open Lwt.Infix
open Concurrency

let section = "kraken_trading_client"

let string_contains str substr =
  let str_len = String.length str and substr_len = String.length substr in
  if substr_len > str_len then false
  else
    let rec loop i =
      if i + substr_len > str_len then false
      else if String.sub str i substr_len = substr then true
      else loop (i + 1)
    in
    loop 0

module Response_table = Hashtbl.Make (struct
  type t = int
  let equal = Int.equal
  let hash = Hashtbl.hash
end)

module Heartbeat_bus = Event_bus.Make (struct
  type t = unit
end)

module Connection_bus = Event_bus.Make (struct
  type t = [ `Connected | `Disconnected of string ]
end)

type state = {
  mutex: Lwt_mutex.t;
  mutable conn: Websocket_lwt_unix.conn option;
  mutable reader: unit Lwt.t option;
  mutable connecting: bool;
  responses: Kraken_common_types.ws_response Lwt.u Response_table.t;
  mutable on_failure: (string -> unit) option;
  connected: bool Atomic.t;
}

let state = {
  mutex = Lwt_mutex.create ();
  conn = None;
  reader = None;
  connecting = false;
  responses = Response_table.create 32;
  on_failure = None;
  connected = Atomic.make false;
}

let heartbeat_bus = Heartbeat_bus.create "kraken_trading_heartbeat"
let connection_bus = Connection_bus.create "kraken_trading_connection"

let heartbeat_stream () = Heartbeat_bus.subscribe heartbeat_bus
let connection_stream () = Connection_bus.subscribe connection_bus

let notify_heartbeat () = Heartbeat_bus.publish heartbeat_bus ()
let notify_connection status = Connection_bus.publish connection_bus status

let rec json_to_string_precise ?field_name symbol json =
  match json with
  | `Null -> "null"
  | `Bool b -> if b then "true" else "false"
  | `Int i -> string_of_int i
  | `Float f ->
      let precision =
        match symbol with
        | Some sym ->
            (match Kraken_instruments_feed.get_precision_info sym with
             | Some (price_precision, qty_precision) ->
                 (match field_name with
                  | Some name when string_contains name "price" -> price_precision
                  | Some name when string_contains name "qty" -> qty_precision
                  | Some name when string_contains name "order_qty" -> qty_precision
                  | Some name when string_contains name "limit_price" -> price_precision
                  | Some name when string_contains name "trigger_price" -> price_precision
                  | Some name when string_contains name "display_qty" -> qty_precision
                  | _ -> qty_precision)
             | None -> 8)
        | None -> 8
      in
      Printf.sprintf "%.*f" precision f
  | `String s ->
      "\""
      ^ (String.concat "" (List.map (function
          | '"' -> "\\\""
          | '\\' -> "\\\\"
          | '\b' -> "\\b"
          | '\012' -> "\\f"
          | '\n' -> "\\n"
          | '\r' -> "\\r"
          | '\t' -> "\\t"
          | c -> String.make 1 c
        ) (List.init (String.length s) (String.get s))))
      ^ "\""
  | `List items ->
      "[" ^ String.concat "," (List.map (json_to_string_precise ?field_name symbol) items) ^ "]"
  | `Assoc pairs ->
      "{" ^ String.concat "," (List.map (fun (k, v) ->
        "\"" ^ k ^ "\":" ^ json_to_string_precise ~field_name:k symbol v) pairs) ^ "}"
  | `Intlit s -> s

let parse_ws_response json : Kraken_common_types.ws_response =
  let method_ = Yojson.Safe.Util.(member "method" json |> to_string) in
  if String.equal method_ "pong" then begin
    let req_id = Yojson.Safe.Util.(member "req_id" json |> to_int_option) in
    let time_in = Yojson.Safe.Util.(member "time_in" json |> to_string) in
    let time_out = Yojson.Safe.Util.(member "time_out" json |> to_string) in
    { method_; success = true; req_id; time_in; time_out; result = None; error = None; warnings = None }
  end else begin
    let success = Yojson.Safe.Util.(member "success" json |> to_bool) in
    let req_id = Yojson.Safe.Util.(member "req_id" json |> to_int_option) in
    let time_in = Yojson.Safe.Util.(member "time_in" json |> to_string) in
    let time_out = Yojson.Safe.Util.(member "time_out" json |> to_string) in
    let result = Yojson.Safe.Util.(member "result" json |> Option.some) in
    let error = Yojson.Safe.Util.(member "error" json |> to_string_option) in
    let warnings =
      match Yojson.Safe.Util.member "warnings" json with
      | `List lst -> Some (List.map Yojson.Safe.Util.to_string lst)
      | _ -> None
    in
    { method_; success; req_id; time_in; time_out; result; error; warnings }
  end

let resolve_response req_id response =
  Lwt_mutex.with_lock state.mutex (fun () ->
    let wakener_opt =
      try
        let wak = Response_table.find state.responses req_id in
        Response_table.remove state.responses req_id;
        Some wak
      with Not_found -> None
    in
    Lwt.return wakener_opt
  ) >>= function
  | Some wak ->
      Lwt.return (Lwt.wakeup_later wak response)
  | None ->
      Logging.debug ~section (Printf.sprintf "No waiter found for req_id=%d" req_id);
      Lwt.return_unit

let fail_all_pending reason =
  let pending =
    Lwt_mutex.with_lock state.mutex (fun () ->
      let wakers = Response_table.fold (fun _ wak acc -> wak :: acc) state.responses [] in
      Response_table.clear state.responses;
      Lwt.return wakers
    )
  in
  pending >|= List.iter (fun wak -> Lwt.wakeup_later_exn wak (Failure reason))

let reset_state conn ~notify_failure reason =
  Lwt_mutex.with_lock state.mutex (fun () ->
    let (pending, should_reset, failure_cb) =
      match state.conn with
      | Some current when current == conn ->
          let wakers = Response_table.fold (fun _ wak acc -> wak :: acc) state.responses [] in
          Response_table.clear state.responses;
          state.conn <- None;
          state.reader <- None;
          state.connecting <- false;
          Atomic.set state.connected false;
          let failure_cb = if notify_failure then state.on_failure else None in
          (wakers, true, failure_cb)
      | _ ->
          state.connecting <- false;
          ([], false, None)
    in
    Lwt.return (pending, should_reset, failure_cb)
  ) >>= fun (pending, should_reset, failure_cb) ->
  if not should_reset then Lwt.return_unit
  else
    Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit) >>= fun () ->
    List.iter (fun wak -> Lwt.wakeup_later_exn wak (Failure reason)) pending;
    notify_connection (`Disconnected reason);
    (match failure_cb with Some f -> f reason | None -> ());
    Lwt.return_unit

let handle_json json =
  let open Yojson.Safe.Util in
  let method_opt = member "method" json |> to_string_option in
  match method_opt with
  | Some method_name ->
      let response = parse_ws_response json in
      (match response.req_id with
       | Some req_id -> resolve_response req_id response
       | None ->
          Logging.debug ~section
            (Printf.sprintf "Unhandled trading response without req_id (method=%s)" method_name);
          Lwt.return_unit)
  | None ->
      (match member "channel" json |> to_string_option with
       | Some "heartbeat" -> Lwt.return_unit
       | _ ->
          Logging.debug ~section
            (Printf.sprintf "Unhandled trading message: %s" (Yojson.Safe.to_string json));
          Lwt.return_unit)

let handle_frame frame =
  notify_heartbeat ();
  Lwt.catch
    (fun () ->
       let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
       handle_json json)
    (fun exn ->
       Logging.error ~section
         (Printf.sprintf "Failed to handle trading frame: %s" (Printexc.to_string exn));
       Lwt.return_unit)

let rec reader_loop conn =
  Websocket_lwt_unix.read conn >>= function
  | { Websocket.Frame.opcode = Websocket.Frame.Opcode.Close; _ } ->
      Logging.warn ~section "Trading WebSocket closed by server";
      reset_state conn ~notify_failure:true "Connection closed by server"
  | frame ->
      handle_frame frame >>= fun () -> reader_loop conn

let start_reader conn =
  let task =
    Lwt.catch
      (fun () -> reader_loop conn)
      (fun exn ->
         let reason = Printf.sprintf "WebSocket error: %s" (Printexc.to_string exn) in
         Logging.error ~section reason;
         reset_state conn ~notify_failure:true reason)
  in
  Lwt.async (fun () -> task);
  task

let connect _token : Websocket_lwt_unix.conn Lwt.t =
  let uri = Uri.of_string "wss://ws-auth.kraken.com/v2" in
  Logging.info ~section "Connecting to Kraken authenticated WebSocket for trading...";
  Lwt_unix.getaddrinfo "ws-auth.kraken.com" "443" [ Unix.AI_FAMILY Unix.PF_INET ] >>= fun addresses ->
  let ip =
    match addresses with
    | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ -> Ipaddr_unix.of_inet_addr addr
    | _ -> failwith "Failed to resolve ws-auth.kraken.com"
  in
  let client = `TLS (`Hostname "ws-auth.kraken.com", `IP ip, `Port 443) in
  let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
  Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
  Logging.info ~section "Trading WebSocket connection established";
  Lwt.return conn

let ensure_connection ?on_failure token =
  Lwt_mutex.with_lock state.mutex (fun () ->
    (match on_failure with Some f -> state.on_failure <- Some f | None -> ());
    if state.conn <> None || state.connecting then Lwt.return `Already
    else begin
      state.connecting <- true;
      Lwt.return `Connect
    end
  ) >>= function
  | `Already -> Lwt.return_unit
  | `Connect ->
      Lwt.catch
        (fun () ->
           connect token >>= fun conn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connecting <- false;
             state.conn <- Some conn;
             state.reader <- None;
             Atomic.set state.connected true;
             Lwt.return (Some conn))
        )
        (fun exn ->
           Lwt_mutex.with_lock state.mutex (fun () ->
             state.connecting <- false;
             Lwt.return_unit) >>= fun () ->
           Lwt.fail exn
        ) >>= function
      | None -> Lwt.return_unit
      | Some conn ->
          let reader = start_reader conn in
          Lwt_mutex.with_lock state.mutex (fun () ->
            state.reader <- Some reader;
            Lwt.return_unit) >>= fun () ->
          notify_connection `Connected;
          Lwt.return_unit

let send_message ~message_str ~req_id ~timeout_ms =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | None -> Lwt.return (Error (Failure "Trading WebSocket not connected"))
    | Some conn ->
        let waiter, wakener = Lwt.wait () in
        Response_table.replace state.responses req_id wakener;
        Lwt.catch
          (fun () ->
             Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:message_str ()) >|= fun () ->
             Ok waiter)
          (fun exn ->
             Response_table.remove state.responses req_id;
             Lwt.return (Error exn))
  ) >>= function
  | Error exn -> Lwt.fail exn
  | Ok waiter ->
      let timeout = float_of_int timeout_ms /. 1000.0 in
      Lwt.pick [
        (waiter >|= fun response -> `Response response);
        (Lwt_unix.sleep timeout >|= fun () -> `Timeout)
      ] >>= function
      | `Response response -> Lwt.return response
      | `Timeout ->
          Lwt_mutex.with_lock state.mutex (fun () ->
            Response_table.remove state.responses req_id;
            Lwt.return_unit) >>= fun () ->
          Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)

let get_connection () : Websocket_lwt_unix.conn Lwt.t =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | Some conn -> Lwt.return (Ok conn)
    | None -> Lwt.return (Error "Trading WebSocket not connected. Call init() first."))
  >>= function
  | Ok conn -> Lwt.return conn
  | Error msg -> Lwt.fail_with msg

let send_ping ~req_id ~timeout_ms : Kraken_common_types.ws_response Lwt.t =
  get_connection () >>= fun _ ->
  let message = `Assoc [ ("method", `String "ping"); ("req_id", `Int req_id) ] in
  let message_str = json_to_string_precise None message in
  send_message ~message_str ~req_id ~timeout_ms

let send_request ~symbol ~method_ ~params ~req_id ~timeout_ms : Kraken_common_types.ws_response Lwt.t =
  let start_time = Telemetry.start_timer () in
  let message = `Assoc [
      ("method", `String method_);
      ("params", params);
      ("req_id", `Int req_id)
    ] in
  let message_str = json_to_string_precise symbol message in
  Logging.debug ~section (Printf.sprintf "Sending WebSocket message: %s" message_str);
  send_message ~message_str ~req_id ~timeout_ms >>= fun response ->
  let _ = Telemetry.record_duration Telemetry.Common.api_duration start_time in
  Telemetry.inc_counter Telemetry.Common.api_requests ();
  Lwt.return response

let init token : unit Lwt.t = ensure_connection token

let connect_and_monitor token ~on_failure = ensure_connection ~on_failure token

let is_connected () = Atomic.get state.connected

let close () : unit Lwt.t =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match state.conn with
    | None -> Lwt.return None
    | Some conn ->
        state.conn <- None;
        state.reader <- None;
        state.connecting <- false;
        Atomic.set state.connected false;
        let pending = Response_table.fold (fun _ wak acc -> wak :: acc) state.responses [] in
        Response_table.clear state.responses;
        Lwt.return (Some (conn, pending))
  ) >>= function
  | None -> Lwt.return_unit
  | Some (conn, pending) ->
      List.iter (fun wak -> Lwt.wakeup_later_exn wak (Failure "Client requested close")) pending;
      notify_connection (`Disconnected "client_closed");
      Lwt.catch (fun () -> Websocket_lwt_unix.close_transport conn) (fun _ -> Lwt.return_unit)

let heartbeat_stream = heartbeat_stream
let connection_stream = connection_stream