(** Hyperliquid WebSocket client - market data subscription hub *)

open Lwt.Infix

let section = "hyperliquid_ws"

(** Subscription handle returned to consumers *)
type subscription = {
  stream: Yojson.Safe.t Lwt_stream.t;
  close: unit -> unit;
}

let is_connected_ref = Atomic.make false
let is_connected () = Atomic.get is_connected_ref

let active_connection = ref None
let connection_mutex = Lwt_mutex.create ()

(** Global subscriber list *)
let pushers : (Yojson.Safe.t option -> unit) list ref = ref []
let pushers_mutex = Mutex.create ()

module Response_table = Hashtbl.Make (struct
  type t = int
  let equal = Int.equal
  let hash = Hashtbl.hash
end)

let responses : (Yojson.Safe.t Lwt.u * float) Response_table.t = Response_table.create 32
let responses_mutex = Lwt_mutex.create ()

(** Broadcast to all subscribers *)
let broadcast_message json =
  Mutex.lock pushers_mutex;
  let ps = !pushers in
  Mutex.unlock pushers_mutex;
  if ps = [] then Logging.warn ~section "No subscribers for WebSocket message!";
  List.iter (fun push -> push (Some json)) ps

(** Subscribe to all incoming market data messages. *)
let subscribe_market_data () =
  let (stream, push) = Lwt_stream.create () in
  Mutex.lock pushers_mutex;
  pushers := push :: !pushers;
  Mutex.unlock pushers_mutex;
  let close () =
    Mutex.lock pushers_mutex;
    pushers := List.filter (fun p -> p != push) !pushers;
    Mutex.unlock pushers_mutex
  in
  { stream; close }

(** Send a subscription message to the active connection *)
let subscribe json =
  Lwt_mutex.with_lock connection_mutex (fun () ->
    match !active_connection with
    | Some conn ->
        let msg = Yojson.Safe.to_string json in
        Logging.info_f ~section "Sending subscription: %s" msg;
        Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:msg ())
    | None ->
        Logging.warn ~section "Cannot subscribe: WebSocket not connected";
        Lwt.return_unit
  )

(** Send initial subscriptions to Hyperliquid *)
let subscribe_to_feeds ~symbols ~wallet =
  let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "allMids")])]) in
  
  let%lwt () = 
    if wallet <> "" then
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "webData2"); ("user", `String wallet)])]) in
      let%lwt () = subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "userEvents"); ("user", `String wallet)])]) in
      subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "orderUpdates"); ("user", `String wallet)])])
    else Lwt.return_unit
  in
  
  Lwt_list.iter_s (fun symbol ->
    (* Use instruments feed to get correct coin identifier:
       perps use base name (e.g. "HYPE"), spot pairs use "@N" format *)
    let coin = Hyperliquid_instruments_feed.get_subscription_coin symbol in
    Logging.info_f ~section "Subscribing to l2Book for %s (coin=%s)" symbol coin;
    subscribe (`Assoc [("method", `String "subscribe"); ("subscription", `Assoc [("type", `String "l2Book"); ("coin", `String coin)])])
  ) symbols

let handle_frame ~on_heartbeat (frame : Websocket.Frame.t) =
  match frame.Websocket.Frame.opcode with
  | Websocket.Frame.Opcode.Text ->
      Concurrency.Tick_event_bus.publish_tick ();
      on_heartbeat ();
      (try
        let json = Yojson.Safe.from_string frame.Websocket.Frame.content in
        let channel = 
          let open Yojson.Safe.Util in
          try member "channel" json |> to_string with _ -> ""
        in
        
        (* post responses are logged in hyperliquid_module as "Hyperliquid order raw response" *)
        (* pong is noisy, but we want to see post for now for debugging timeouts *)
        let is_noisy = List.mem channel ["pong"] in

        if not is_noisy then begin
          let log_msg =
            if String.length frame.Websocket.Frame.content > 1000 then
              String.sub frame.Websocket.Frame.content 0 1000 ^ "... [TRUNCATED]"
            else frame.Websocket.Frame.content
          in
          if channel = "webData2" then
            Logging.debug_f ~section "webData2 received: %s" log_msg
          else
            Logging.debug_f ~section "Raw WS message: %s" log_msg
        end;
        
        (* Check if it's a response to a request *)
        let is_response = 
          let id_opt = 
            let rec find_id node =
              match node with
              | `Assoc pairs ->
                  (match List.assoc_opt "id" pairs with
                   | Some (`Int id) -> Some id
                   | Some (`Intlit s) -> (try Some (int_of_string s) with _ -> None)
                   | Some (`String s) -> (try Some (int_of_string s) with _ -> None)
                   | Some (`Float f) -> Some (int_of_float f)
                   | _ -> 
                       (* If not at this level, check inside "data" or "response" *)
                       (match List.assoc_opt "data" pairs with
                        | Some next_node -> find_id next_node
                        | None -> 
                            (match List.assoc_opt "response" pairs with
                             | Some next_node -> find_id next_node
                             | None -> None)))
              | _ -> None
            in
            find_id json
          in
          match id_opt with
          | Some id ->
              (* It's a response with an ID *)
              Lwt.async (fun () ->
                Lwt_mutex.with_lock responses_mutex (fun () ->
                  try
                    let (wakener, _) = Response_table.find responses id in
                    Response_table.remove responses id;
                    Lwt.wakeup_later wakener json;
                    Lwt.return_unit
                  with Not_found -> 
                    if channel = "post" then
                      Logging.info_f ~section "Received 'post' response for unknown req_id %d: %s" id (Yojson.Safe.to_string json);
                    Lwt.return_unit
                )
              );
              true
          | None -> 
              if channel = "post" then
                Logging.warn_f ~section "Received 'post' message without identifiable ID: %s" (Yojson.Safe.to_string json);
              false
        in
        
        if not is_response then begin
          (* Debug log for data messages occasionally *)
          if not is_noisy then
            if channel = "webData2" then
              Logging.info_f ~section "Broadcasting webData2 data message"
            else
              Logging.debug_f ~section "Broadcasting data message: channel=%s" channel;
          broadcast_message json
        end
      with exn ->
        Logging.warn_f ~section "Failed to parse WS message: %s" (Printexc.to_string exn));
      Lwt.return_unit
  | Websocket.Frame.Opcode.Close ->
      Logging.info ~section "WebSocket connection closed by server";
      Atomic.set is_connected_ref false;
      Lwt.return_unit
  | _ -> Lwt.return_unit

(** Connect to Hyperliquid WebSocket and broadcast messages to all subscribers. *)
let connect_and_monitor ~on_failure ~on_connected ~on_heartbeat ~testnet =
  let base_url = if testnet then "api.hyperliquid-testnet.xyz" else "api.hyperliquid.xyz" in
  let url = Printf.sprintf "wss://%s/ws" base_url in
  let hostname = base_url in
  let port = 443 in
  Logging.info_f ~section "Connecting to Hyperliquid WebSocket: %s" url;
  let uri = Uri.of_string url in
  Lwt.catch (fun () ->
    Lwt_unix.getaddrinfo hostname (string_of_int port) [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
    let ip = match addresses with
      | {Unix.ai_addr = Unix.ADDR_INET (addr, _); _} :: _ ->
          Ipaddr_unix.of_inet_addr addr
      | _ -> failwith (Printf.sprintf "Failed to resolve %s" hostname)
    in
    let client = `TLS (`Hostname hostname, `IP ip, `Port port) in
    let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Websocket_lwt_unix.connect ~ctx client uri >>= fun conn ->
    Lwt_mutex.with_lock connection_mutex (fun () ->
      active_connection := Some conn;
      Atomic.set is_connected_ref true;
      Lwt.return_unit
    ) >>= fun () ->
    on_connected ();
    
    (* Start heartbeat task *)
    let rec heartbeat () =
      if Atomic.get is_connected_ref then
        Lwt.catch (fun () ->
          let ping_msg = `Assoc [("method", `String "ping")] in
          subscribe ping_msg >>= fun () ->
          Lwt_unix.sleep 30.0 >>= heartbeat
        ) (fun _ -> Lwt.return_unit)
      else Lwt.return_unit
    in
    Lwt.async heartbeat;

    let rec loop () =
      Websocket_lwt_unix.read conn >>= fun frame ->
      handle_frame ~on_heartbeat frame >>= fun () ->
      if Atomic.get is_connected_ref then loop () else Lwt.return_unit
    in
    Lwt.catch loop (fun exn ->
      Lwt_mutex.with_lock connection_mutex (fun () ->
        active_connection := None;
        Atomic.set is_connected_ref false;
        Lwt.return_unit
      ) >>= fun () ->
      Lwt.fail exn
    )
  ) (fun exn ->
    let error_msg = Printexc.to_string exn in
    Logging.error_f ~section "WebSocket connection error: %s" error_msg;
    Atomic.set is_connected_ref false;
    on_failure error_msg;
    Lwt.return_unit
  )

(** Send a request and wait for a response using the given req_id. *)
let send_request ~json ~req_id ~timeout_ms =
  let waiter, wakener = Lwt.wait () in
  Lwt_mutex.with_lock responses_mutex (fun () ->
    Response_table.add responses req_id (wakener, Unix.time ());
    Lwt.return_unit
  ) >>= fun () ->
  subscribe json >>= fun () ->
  
  let timeout = float_of_int timeout_ms /. 1000.0 in
  Lwt.pick [
    waiter;
    (Lwt_unix.sleep timeout >>= fun () ->
      Lwt_mutex.with_lock responses_mutex (fun () ->
        if Response_table.mem responses req_id then begin
          Response_table.remove responses req_id;
          Lwt.return `Timeout
        end else Lwt.return `Resolved
      ) >>= function
      | `Timeout -> Lwt.fail_with (Printf.sprintf "Timeout waiting for response to req_id %d" req_id)
      | `Resolved -> waiter
    )
  ]