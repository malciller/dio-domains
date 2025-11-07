
(** Connection Supervisor - Monitors and manages WebSocket connections *)

open Lwt.Infix

let section = "supervisor"

(** Open Strategy_common for shared order types *)
open Dio_strategies.Strategy_common

(** Connection state *)
type connection_state =
  | Disconnected
  | Connecting
  | Connected
  | Failed of string

(** Circuit breaker state *)
type circuit_breaker_state =
  | Closed  (* Normal operation *)
  | Open    (* Failing too much, temporarily disabled *)
  | HalfOpen  (* Testing if service recovered *)

(** Supervised connection *)
type supervised_connection = {
  name: string;
  mutable state: connection_state;
  mutable last_connected: float option;
  mutable last_disconnected: float option;
  mutable last_connecting: float option;  (* Timestamp when entered Connecting state *)
  mutable last_data_received: float option;  (* For heartbeat monitoring *)
  mutable last_ping_sent: float option;  (* For ping/pong monitoring *)
  ping_failures: int Atomic.t;  (* Consecutive ping failures *)
  mutable reconnect_attempts: int;
  mutable total_connections: int;
  mutable circuit_breaker: circuit_breaker_state;
  mutable circuit_breaker_failures: int;  (* Consecutive failures *)
  mutable circuit_breaker_last_failure: float option;
  mutable connect_fn: (unit -> unit Lwt.t) option;  (* Optional - None for monitoring-only connections *)
  mutex: Mutex.t;
}

(** Registry of supervised connections *)
let connections : (string, supervised_connection) Hashtbl.t = Hashtbl.create 16
let registry_mutex = Mutex.create ()

(** Shutdown flag for graceful termination *)
let shutdown_requested = Atomic.make false

(** Global authentication token for reuse across modules *)
module Token_store = struct
  (* Single writer (supervisor init) with atomic snapshots to avoid races *)
  let token : string option Atomic.t = Atomic.make None

  let set value = Atomic.set token value

  let get () = Atomic.get token
end

(** Generate unique ping request ID - start high to avoid conflicts with trading req_ids *)
let next_ping_req_id =
  let counter = ref 1000000 in
  fun () -> incr counter; !counter


(** Register a new supervised connection with auto-restart capability *)
let register ~name ~connect_fn =
  Mutex.lock registry_mutex;
  let conn = {
    name;
    state = Disconnected;
    last_connected = None;
    last_disconnected = Some (Unix.time ());  (* Set to now to prevent immediate auto-restart *)
    last_connecting = None;
    last_data_received = None;
    last_ping_sent = None;
    ping_failures = Atomic.make 0;
    reconnect_attempts = 0;
    total_connections = 0;
    circuit_breaker = Closed;
    circuit_breaker_failures = 0;
    circuit_breaker_last_failure = None;
    connect_fn = connect_fn;
    mutex = Mutex.create ();
  } in
  Hashtbl.replace connections name conn;
  Mutex.unlock registry_mutex;
  Logging.info_f ~section "Registered supervised connection: %s" name;
  conn

(** Register an existing connection for monitoring only (no auto-restart) *)
let register_for_monitoring ~name =
  Mutex.lock registry_mutex;
  let conn = {
    name;
    state = Connected;  (* Assume it's connected since we're just monitoring *)
    last_connected = Some (Unix.time ());
    last_disconnected = None;
    last_connecting = None;
    last_data_received = Some (Unix.time ());  (* Assume data was recently received *)
    last_ping_sent = None;
    ping_failures = Atomic.make 0;
    reconnect_attempts = 0;
    total_connections = 1;
    circuit_breaker = Closed;
    circuit_breaker_failures = 0;
    circuit_breaker_last_failure = None;
    connect_fn = None;  (* No restart capability for monitoring-only *)
    mutex = Mutex.create ();
  } in
  Hashtbl.replace connections name conn;
  Mutex.unlock registry_mutex;
  Logging.info_f ~section "Registered connection for monitoring: %s" name;
  conn

(** Update connection state *)
let set_state conn new_state =
  Mutex.lock conn.mutex;
  let old_state = conn.state in
  conn.state <- new_state;
  
  (match new_state with
  | Connected ->
      conn.last_connected <- Some (Unix.time ());
      conn.last_connecting <- None;  (* Clear connecting timestamp *)
      conn.last_data_received <- Some (Unix.time ());  (* Reset data heartbeat on connect *)
      conn.last_ping_sent <- None;  (* Reset ping tracking on connect *)
      Atomic.set conn.ping_failures 0;  (* Reset ping failures on connect *)
      conn.reconnect_attempts <- 0;
      conn.total_connections <- conn.total_connections + 1;
      Telemetry.set_gauge (Telemetry.gauge "connection_state" ~labels:[("name", conn.name)] ()) (1.0);
      Logging.info_f ~section "[%s] Connection established (total: %d)"
        conn.name conn.total_connections
  | Disconnected ->
      conn.last_disconnected <- Some (Unix.time ());
      conn.last_connecting <- None;  (* Clear connecting timestamp *)
      Telemetry.set_gauge (Telemetry.gauge "connection_state" ~labels:[("name", conn.name)] ()) (0.0);
      Logging.warn_f ~section "[%s] Connection lost" conn.name
  | Connecting ->
      conn.last_connecting <- Some (Unix.time ());  (* Record when we started connecting *)
      Logging.info_f ~section "[%s] Attempting connection (attempt #%d)"
        conn.name (conn.reconnect_attempts + 1)
  | Failed reason ->
      conn.last_disconnected <- Some (Unix.time ());
      conn.last_connecting <- None;  (* Clear connecting timestamp *)
      conn.reconnect_attempts <- conn.reconnect_attempts + 1;
      Telemetry.set_gauge (Telemetry.gauge "connection_state" ~labels:[("name", conn.name)] ()) (-1.0);
      Telemetry.inc_counter (Telemetry.counter "connection_failures" ~labels:[("name", conn.name)] ()) ();
      Logging.error_f ~section "[%s] Connection failed: %s (attempt #%d)"
        conn.name reason conn.reconnect_attempts);
  
  Mutex.unlock conn.mutex;
  
  (* Log state transitions *)
  if old_state <> new_state then
    Logging.debug_f ~section "[%s] State transition: %s -> %s" 
      conn.name 
      (match old_state with
       | Disconnected -> "Disconnected"
       | Connecting -> "Connecting"
       | Connected -> "Connected"
       | Failed _ -> "Failed")
      (match new_state with
       | Disconnected -> "Disconnected"
       | Connecting -> "Connecting"
       | Connected -> "Connected"
       | Failed _ -> "Failed")

(** Get connection state *)
let get_state conn =
  Mutex.lock conn.mutex;
  let state = conn.state in
  Mutex.unlock conn.mutex;
  state

(** Get connection uptime in seconds *)
let get_uptime conn =
  Mutex.lock conn.mutex;
  let uptime = match conn.last_connected, conn.state with
    | Some t, Connected -> Some (Unix.time () -. t)
    | _ -> None
  in
  Mutex.unlock conn.mutex;
  uptime

(** Set connection function (for updating after registration) *)
let set_connect_fn conn connect_fn =
  Mutex.lock conn.mutex;
  conn.connect_fn <- connect_fn;
  Mutex.unlock conn.mutex

(** Update last data received timestamp (for heartbeat monitoring) *)
let update_data_heartbeat conn =
  Mutex.lock conn.mutex;
  conn.last_data_received <- Some (Unix.time ());
  Mutex.unlock conn.mutex



(** Check if circuit breaker allows connection attempt *)
let circuit_breaker_allows_connection conn =
  Mutex.lock conn.mutex;
  let current_time = Unix.time () in
  let allowed = match conn.circuit_breaker with
    | Closed -> true
    | Open ->
        begin match conn.circuit_breaker_last_failure with
        | Some failure_time when current_time -. failure_time > 300.0 ->  (* 5 minutes timeout *)
            conn.circuit_breaker <- HalfOpen;
            Logging.info_f ~section "[%s] Circuit breaker HALF-OPEN (testing recovery)" conn.name;
            Telemetry.inc_counter (Telemetry.counter "circuit_breaker_half_open" ~labels:[("name", conn.name)] ()) ();
            true
        | _ -> false
        end
    | HalfOpen -> true  (* Allow one attempt in half-open state *)
  in
  Mutex.unlock conn.mutex;
  allowed

(** Update circuit breaker state based on connection result *)
let update_circuit_breaker conn success =
  Mutex.lock conn.mutex;
  if success then begin
    (* Success - reset circuit breaker *)
    conn.circuit_breaker <- Closed;
    conn.circuit_breaker_failures <- 0;
    conn.circuit_breaker_last_failure <- None;
  end else begin
    (* Failure - increment counter and potentially open circuit *)
    conn.circuit_breaker_failures <- conn.circuit_breaker_failures + 1;
    conn.circuit_breaker_last_failure <- Some (Unix.time ());

    if conn.circuit_breaker_failures >= 5 then begin  (* Open after 5 consecutive failures *)
      conn.circuit_breaker <- Open;
      Logging.warn_f ~section "[%s] Circuit breaker OPEN after %d consecutive failures" conn.name conn.circuit_breaker_failures;
      Telemetry.inc_counter (Telemetry.counter "circuit_breaker_tripped" ~labels:[("name", conn.name)] ()) ();
    end
  end;
  Mutex.unlock conn.mutex

(** Start a supervised connection - schedules it in the Lwt event loop *)
let start_async conn =
  match conn.connect_fn with
  | None ->
      Logging.warn_f ~section "[%s] Cannot start connection - no connect function provided (monitoring only)" conn.name
  | Some connect_fn ->
      (* Check if connection is already attempting to connect *)
      Mutex.lock conn.mutex;
      let current_state = conn.state in
      Mutex.unlock conn.mutex;

      if current_state = Connecting then begin
        Logging.debug_f ~section "[%s] Connection already connecting, skipping duplicate start" conn.name;
      end else begin
        (* Check circuit breaker before attempting connection *)
        if not (circuit_breaker_allows_connection conn) then begin
          Logging.warn_f ~section "[%s] Circuit breaker blocks connection attempt" conn.name;
          set_state conn (Failed "Circuit breaker open");
        end else begin
          (* Set to Connecting immediately to prevent duplicate restarts *)
          set_state conn Connecting;
          Mutex.lock conn.mutex;
          conn.reconnect_attempts <- conn.reconnect_attempts + 1;
          let attempt_num = conn.reconnect_attempts in
          Mutex.unlock conn.mutex;

          Logging.info_f ~section "[%s] Starting supervised connection (attempt #%d)"
            conn.name attempt_num;

          let open Lwt.Infix in
          Lwt.async (fun () ->
            (* The connection function now handles its own state management *)
            (* Just start the connection and let it manage success/failure *)
            Lwt.catch (fun () ->
              connect_fn () >>= fun () ->
              (* Connection function completed - this shouldn't happen for WebSocket connections *)
              Logging.warn_f ~section "[%s] Connection function completed unexpectedly" conn.name;
              Lwt.return_unit
            ) (fun exn ->
              let error_msg = Printexc.to_string exn in
              Logging.error_f ~section "[%s] Unexpected error in connection function: %s" conn.name error_msg;
              (* Ensure state is set to Failed on any exception during connection setup *)
              set_state conn (Failed error_msg);
              Lwt.return_unit
            )
          )
        end
      end

(** Restart a connection *)
let restart conn =
  Logging.info_f ~section "[%s] Manually restarting connection" conn.name;
  set_state conn Disconnected;
  (* Reset reconnect attempts for manual restart *)
  Mutex.lock conn.mutex;
  conn.reconnect_attempts <- 0;
  Mutex.unlock conn.mutex;
  start_async conn

(** Monitor all connections and report status *)
let monitor_loop () =
  let cycle_count = ref 0 in
  while true do
    try
      Thread.delay 2.0;  (* Check every 2 seconds for faster reconnection detection *)
      incr cycle_count;

      Mutex.lock registry_mutex;
      let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
      Mutex.unlock registry_mutex;
    
    (* Check each connection and trigger auto-restart if needed *)
    List.iter (fun conn ->
      (* Get all state information atomically *)
      Mutex.lock conn.mutex;
      let state = conn.state in
      let attempts = conn.reconnect_attempts in
      let last_disconnected = conn.last_disconnected in
      let last_connecting = conn.last_connecting in
      let has_connect_fn = Option.is_some conn.connect_fn in
      Mutex.unlock conn.mutex;

      (* Health monitoring and backup restart logic *)
      match state, has_connect_fn with
      | Failed reason, true ->
          (* Check current state atomically to avoid race conditions *)
          Mutex.lock conn.mutex;
          let current_state = conn.state in
          Mutex.unlock conn.mutex;

          if current_state <> Connecting then begin
            (* Calculate backoff delay: 2s, 4s, 6s, ... max 30s *)
            let delay = min 30.0 (2.0 *. Float.of_int attempts) in

            (* Check if enough time has passed since last disconnect *)
            let should_reconnect =
              match last_disconnected with
              | Some t -> Unix.time () -. t >= delay
              | None -> true
            in

            if should_reconnect then begin
              Logging.info_f ~section "[%s] Backup auto-reconnecting after %.1fs backoff (reason: %s)..." conn.name delay reason;
              start_async conn
            end
          end
      | Disconnected, true ->
          (* Connection is disconnected but not failed - might be intentional shutdown *)
          (* Only restart if it's been disconnected for more than 60 seconds *)
          let should_reconnect =
            match last_disconnected with
            | Some t -> Unix.time () -. t >= 60.0
            | None -> false
          in

          if should_reconnect then begin
            Logging.warn_f ~section "[%s] Connection disconnected for >60s, restarting..." conn.name;
            start_async conn
          end
      | Connecting, _ ->
          (* Already connecting, check for stuck connections *)
          let stuck_time = match last_connecting with
            | Some t -> Unix.time () -. t
            | None -> 0.0  (* Shouldn't happen if state logic is correct, but defensive *)
          in
          if stuck_time > 120.0 then begin  (* Stuck for more than 2 minutes *)
            Logging.error_f ~section "[%s] Connection stuck in 'Connecting' state for %.0fs, restarting..." conn.name stuck_time;
            (* Atomically set state and restart to avoid race conditions *)
            Mutex.lock conn.mutex;
            let current_state = conn.state in
            Mutex.unlock conn.mutex;

            if current_state = Connecting then begin
              set_state conn Disconnected;
              start_async conn
            end
          end
      | Connected, _ ->
          (* Connected and healthy - implement heartbeat monitoring for all connections *)
          let current_time = Unix.time () in

          (* For trading connections, use active ping/pong monitoring *)
          if String.equal conn.name "kraken_trading_ws" then begin
            let should_ping =
              match conn.last_ping_sent with
              | None -> true  (* Never pinged before *)
              | Some last_ping -> current_time -. last_ping >= 15.0  (* Ping every 15 seconds to stay under 30s timeout *)
            in

            if should_ping then begin
              (* Send ping asynchronously *)
              conn.last_ping_sent <- Some current_time;
              Lwt.async (fun () ->
                let req_id = next_ping_req_id () in
                Lwt.catch
                  (fun () ->
                    Kraken.Kraken_trading_client.send_ping ~req_id ~timeout_ms:5000 >>= fun response ->
                    if response.success then begin
                      Logging.debug_f ~section "[%s] Ping successful (req_id: %d)" conn.name req_id;
                      Atomic.set conn.ping_failures 0;  (* Reset failure count on success *)
                      update_data_heartbeat conn;  (* Update heartbeat since we got a response *)
                      Lwt.return_unit
                    end else begin
                      Logging.warn_f ~section "[%s] Ping failed: %s" conn.name
                        (match response.error with Some e -> e | None -> "unknown error");
                      Atomic.incr conn.ping_failures;
                      (* Note: State transition will be handled by main monitor loop *)
                      Lwt.return_unit
                    end
                  )
                  (fun exn ->
                    Logging.warn_f ~section "[%s] Ping exception: %s" conn.name (Printexc.to_string exn);
                    Atomic.incr conn.ping_failures;
                    (* Note: State transition will be handled by main monitor loop *)
                    Lwt.return_unit
                  )
              )
            end;

            (* Check for ping failures - done here to avoid mutex deadlock in async callback *)
            let ping_failures = Atomic.get conn.ping_failures in
            if ping_failures >= 3 then begin
              Logging.error_f ~section "[%s] Ping failed %d times, marking connection as failed"
                conn.name ping_failures;
              set_state conn (Failed "ping timeout");
            end
          end else begin
            (* For non-trading connections, monitor data heartbeat *)
            match conn.last_data_received with
            | Some last_data when current_time -. last_data > 60.0 ->  (* No data for 60 seconds *)
                Logging.warn_f ~section "[%s] No data received for %.0fs, marking connection as failed"
                  conn.name (current_time -. last_data);
                set_state conn (Failed "data timeout")
            | _ -> ()
          end
      | _ -> ()
    ) conn_list;
    
    (* Telemetry updates are handled by supervisor_cache event-driven updates *)
    with exn ->
      Logging.error_f ~section "Exception in monitor loop: %s" (Printexc.to_string exn);
      Logging.error_f ~section "Monitor loop continuing after exception..."
  done

(** Initialize all websocket feeds and connections *)
let initialize_feeds () : ((Dio_engine.Config.trading_config list * string) Lwt.t) =
  Logging.info ~section "Initializing websocket feeds...";

  (* Get trading configurations *)
  let config = Dio_engine.Config.read_config () in
  let configs = config.trading in
  Logging.info_f ~section "Loaded %d trading configuration(s)" (List.length configs);

  (* Extract Kraken symbols for websocket connections *)
  let kraken_symbols = configs
                      |> List.filter (fun cfg -> cfg.Dio_engine.Config.exchange = "kraken")
                      |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in

  Logging.info_f ~section "Connecting to %d Kraken websockets..." (List.length kraken_symbols);

  (* Start initialization sequence as a promise chain *)

  (* Initialize data stores for websocket feeds *)
  Logging.info ~section "Step 1: Initializing ticker feed stores...";
  Kraken.Kraken_ticker_feed.initialize kraken_symbols;

  Logging.info ~section "Step 2: Initializing instruments feed stores...";
  let%lwt () = Kraken.Kraken_instruments_feed.initialize_symbols kraken_symbols in

  Logging.info ~section "Step 3: Initializing orderbook feed stores...";
  let%lwt () = Kraken.Kraken_orderbook_feed.initialize kraken_symbols in

  Logging.info ~section "Step 4: Getting authentication token...";
  let%lwt auth_token = Kraken.Kraken_generate_auth_token.get_token () in
  Logging.info ~section "Authentication token obtained";

  (* Store token globally for reuse by order executor *)
  Token_store.set (Some auth_token);

  Logging.info ~section "Step 5: Initializing balances feed stores...";
  (* Extract all unique assets from trading symbols *)
  let all_assets = configs
                  |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol)
                  |> List.map (fun symbol -> String.split_on_char '/' symbol |> List.hd)  (* Get base asset *)
                  |> List.sort_uniq String.compare
                  |> fun assets -> "USD" :: assets in  (* Add USD as quote currency *)
  let () = try
    Kraken.Kraken_balances_feed.initialize all_assets;
    Logging.info ~section "Balances feed stores initialized";
  with exn ->
    Logging.error_f ~section "Failed to initialize balances feed stores: %s" (Printexc.to_string exn)
  in

  Logging.info ~section "Step 6: Initializing executions feed stores...";
  Kraken.Kraken_executions_feed.initialize kraken_symbols;

  (* Register and start supervised websocket connections *)
  Logging.info ~section "Step 7: Starting supervised websocket connections...";

  (* Ticker feed *)
  let ticker_conn = register ~name:"kraken_ticker_ws" ~connect_fn:None in
  let ticker_connect_fn () =
    (* Wrap the connection in try-catch for proper error handling *)
    Lwt.catch (fun () ->
      let on_failure reason = set_state ticker_conn (Failed reason) in
      let on_heartbeat () = update_data_heartbeat ticker_conn in
      let on_connected () = set_state ticker_conn Connected in
      Kraken.Kraken_ticker_feed.connect_and_subscribe kraken_symbols ~on_failure ~on_heartbeat ~on_connected >>= fun () ->
      (* Connection function completed - this shouldn't happen for WebSocket connections *)
      Lwt.return_unit
    ) (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section "[%s] Connection failed during establishment: %s" ticker_conn.name error_msg;
      set_state ticker_conn (Failed error_msg);
      Lwt.return_unit
    )
  in
  set_connect_fn ticker_conn (Some ticker_connect_fn);
  start_async ticker_conn;

  (* Orderbook feed *)
  let orderbook_conn = register ~name:"kraken_orderbook_ws" ~connect_fn:None in
  let orderbook_connect_fn () =
    (* Clear orderbook stores before reconnecting to ensure clean state *)
    Kraken.Kraken_orderbook_feed.clear_all_stores ();
    (* Wrap the connection in try-catch for proper error handling *)
    Lwt.catch (fun () ->
      let on_failure reason = set_state orderbook_conn (Failed reason) in
      let on_heartbeat () = update_data_heartbeat orderbook_conn in
      let on_connected () = set_state orderbook_conn Connected in
      Kraken.Kraken_orderbook_feed.connect_and_subscribe kraken_symbols ~on_failure ~on_heartbeat ~on_connected >>= fun () ->
      (* Connection function completed - this shouldn't happen for WebSocket connections *)
      Lwt.return_unit
    ) (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section "[%s] Connection failed during establishment: %s" orderbook_conn.name error_msg;
      set_state orderbook_conn (Failed error_msg);
      Lwt.return_unit
    )
  in
  set_connect_fn orderbook_conn (Some orderbook_connect_fn);
  start_async orderbook_conn;

  (* Balances feed *)
  let balances_conn = register ~name:"kraken_balances_ws" ~connect_fn:None in
  let balances_connect_fn () =
    (* Wrap the connection in try-catch for proper error handling *)
    Lwt.catch (fun () ->
      let on_failure reason = set_state balances_conn (Failed reason) in
      let on_heartbeat () = update_data_heartbeat balances_conn in
      let on_connected () = set_state balances_conn Connected in
      Kraken.Kraken_balances_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected >>= fun () ->
      (* Connection function completed - this shouldn't happen for WebSocket connections *)
      Lwt.return_unit
    ) (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section "[%s] Connection failed during establishment: %s" balances_conn.name error_msg;
      set_state balances_conn (Failed error_msg);
      Lwt.return_unit
    )
  in
  set_connect_fn balances_conn (Some balances_connect_fn);
  start_async balances_conn;

  (* Executions feed *)
  let executions_conn = register ~name:"kraken_executions_ws" ~connect_fn:None in
  let executions_connect_fn () =
    (* Wrap the connection in try-catch for proper error handling *)
    Lwt.catch (fun () ->
      let on_failure reason = set_state executions_conn (Failed reason) in
      let on_heartbeat () = update_data_heartbeat executions_conn in
      let on_connected () = set_state executions_conn Connected in
      Kraken.Kraken_executions_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected >>= fun () ->
      (* Connection function completed - this shouldn't happen for WebSocket connections *)
      Lwt.return_unit
    ) (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section "[%s] Connection failed during establishment: %s" executions_conn.name error_msg;
      set_state executions_conn (Failed error_msg);
      Lwt.return_unit
    )
  in
  set_connect_fn executions_conn (Some executions_connect_fn);
  start_async executions_conn;

  (* Trading client for order operations *)
  let trading_conn = register ~name:"kraken_trading_ws" ~connect_fn:None in
  let trading_connect_fn () =
    (* Wrap the connection in try-catch for proper error handling *)
    Lwt.catch (fun () ->
      let on_failure reason =
        set_state trading_conn (Failed reason);
        (* Immediately trigger reconnection attempt to avoid waiting for monitor loop *)
        Lwt.async (fun () ->
          Lwt.catch (fun () ->
            Lwt_unix.sleep 0.1 >>= fun () ->  (* Small delay to prevent tight loops *)
            start_async trading_conn;
            Lwt.return_unit
          ) (fun exn ->
            Logging.warn_f ~section "[%s] Exception during emergency reconnection: %s" trading_conn.name (Printexc.to_string exn);
            Lwt.return_unit
          )
        )
      in
      let on_connected () = set_state trading_conn Connected in
      Kraken.Kraken_trading_client.connect_and_monitor auth_token ~on_failure ~on_connected >>= fun () ->
      (* Connection function completed - this shouldn't happen for WebSocket connections *)
      Lwt.return_unit
    ) (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section "[%s] Connection failed during establishment: %s" trading_conn.name error_msg;
      set_state trading_conn (Failed error_msg);
      Lwt.return_unit
    )
  in
  set_connect_fn trading_conn (Some trading_connect_fn);
  start_async trading_conn;

  (* Wait for executions data FIRST to avoid race condition *)
  Logging.info ~section "Waiting for executions feed to be ready...";
  let%lwt executions_ready = Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 in
  if not executions_ready then
    Logging.debug ~section "Timeout waiting for executions data, continuing anyway..."
  else
    Logging.info ~section "✓ Executions feed ready";

  (* Wait for initial data from all websocket feeds *)
  Logging.info ~section "Waiting for initial market data from all feeds...";

  (* Wait for ticker data *)
  let%lwt ticker_ready = Kraken.Kraken_ticker_feed.wait_for_price_data kraken_symbols 10.0 in
  if not ticker_ready then
    Logging.warn ~section "Timeout waiting for ticker data, continuing anyway..."
  else
    Logging.info ~section "✓ Ticker feed ready";

  (* Wait for orderbook data *)
  let%lwt orderbook_ready = Kraken.Kraken_orderbook_feed.wait_for_orderbook_data kraken_symbols 10.0 in
  if not orderbook_ready then
    Logging.warn ~section "Timeout waiting for orderbook data, continuing anyway..."
  else
    Logging.info ~section "✓ Orderbook feed ready";

  (* Wait for executions data again *)
  let%lwt executions_ready = Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 in
  if not executions_ready then
    Logging.debug ~section "Timeout waiting for executions data, continuing anyway..."
  else
    Logging.info ~section "✓ Executions feed ready";

  (* Wait for balance data *)
  let%lwt balances_ready = Kraken.Kraken_balances_feed.wait_for_balance_data all_assets 10.0 in
  if not balances_ready then
    Logging.warn ~section "Timeout waiting for balance data, continuing anyway..."
  else
    Logging.info ~section "✓ Balances feed ready";

  Logging.info ~section "All feeds initialized with market data!";

  (* Fetch fees for all assets *)
  Logging.info ~section "Step 8: Fetching trading fees for all assets...";

  (* Convert fee fetching to promise-based operations *)
  let%lwt configs_with_fees = Lwt_list.map_s (fun asset ->
    try
      if asset.Dio_engine.Config.exchange = "kraken" then begin
        Logging.debug_f ~section "Fetching fees for %s..." asset.Dio_engine.Config.symbol;
        let%lwt fee_info_opt = Kraken.Kraken_get_fee.get_fee_info asset.Dio_engine.Config.symbol in
        let%lwt result = match fee_info_opt with
        | Some fee_info ->
            Logging.debug_f ~section "Retrieved fees for %s: maker=%.4f%% taker=%.4f%%"
              asset.Dio_engine.Config.symbol
              (Option.value fee_info.Kraken.Kraken_get_fee.maker_fee ~default:0. *. 100.)
              (Option.value fee_info.Kraken.Kraken_get_fee.taker_fee ~default:0. *. 100.);
            (* Store fees in Fee_cache so UI can access them *)
            (match fee_info.Kraken.Kraken_get_fee.maker_fee, fee_info.Kraken.Kraken_get_fee.taker_fee with
             | Some maker, Some taker ->
                 Dio_strategies.Fee_cache.store_fees 
                   ~exchange:asset.Dio_engine.Config.exchange 
                   ~symbol:asset.Dio_engine.Config.symbol 
                   ~maker_fee:maker 
                   ~taker_fee:taker 
                   ~ttl_seconds:600.0
             | Some maker, None ->
                 (* Use maker for both if taker not available *)
                 Dio_strategies.Fee_cache.store_fees 
                   ~exchange:asset.Dio_engine.Config.exchange 
                   ~symbol:asset.Dio_engine.Config.symbol 
                   ~maker_fee:maker 
                   ~taker_fee:maker 
                   ~ttl_seconds:600.0
             | _ -> ());
            Lwt.return { asset with
              Dio_engine.Config.maker_fee = fee_info.Kraken.Kraken_get_fee.maker_fee;
              Dio_engine.Config.taker_fee = fee_info.Kraken.Kraken_get_fee.taker_fee }
        | None ->
            Logging.warn_f ~section "Failed to fetch fees for %s, using defaults" asset.Dio_engine.Config.symbol;
            (* Store default fees in cache *)
            Dio_strategies.Fee_cache.store_fees 
              ~exchange:asset.Dio_engine.Config.exchange 
              ~symbol:asset.Dio_engine.Config.symbol 
              ~maker_fee:0.0025 
              ~taker_fee:0.0040 
              ~ttl_seconds:600.0;
            Lwt.return { asset with
              Dio_engine.Config.maker_fee = Some 0.0016;  (* 0.16% maker fee default *)
              Dio_engine.Config.taker_fee = Some 0.0026 } (* 0.26% taker fee default *) in
        (* Add small delay between requests to ensure unique nonces *)
        let%lwt () = Lwt_unix.sleep 0.05 in  (* 50ms delay *)
        Lwt.return result
      end else begin
        Logging.warn_f ~section "Fee fetching not implemented for exchange: %s, using defaults" asset.Dio_engine.Config.exchange;
        (* Store default fees in cache for unsupported exchanges *)
        Dio_strategies.Fee_cache.store_fees 
          ~exchange:asset.Dio_engine.Config.exchange 
          ~symbol:asset.Dio_engine.Config.symbol 
          ~maker_fee:0.0016 
          ~taker_fee:0.0026 
          ~ttl_seconds:600.0;
        (* Provide default fee values for unsupported exchanges *)
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some 0.0016;  (* 0.16% maker fee default *)
          Dio_engine.Config.taker_fee = Some 0.0026 } (* 0.26% taker fee default *)
      end
    with exn ->
      Logging.warn_f ~section "Exception during fee fetching for %s: %s, using defaults"
        asset.Dio_engine.Config.symbol (Printexc.to_string exn);
      (* Store default fees in cache when fetching fails *)
      Dio_strategies.Fee_cache.store_fees 
        ~exchange:asset.Dio_engine.Config.exchange 
        ~symbol:asset.Dio_engine.Config.symbol 
        ~maker_fee:0.0016 
        ~taker_fee:0.0026 
        ~ttl_seconds:600.0;
      (* Provide default fee values when fetching fails to ensure strategies have fee data *)
      Lwt.return { asset with
        Dio_engine.Config.maker_fee = Some 0.0016;  (* 0.16% maker fee default *)
        Dio_engine.Config.taker_fee = Some 0.0026 } (* 0.26% taker fee default *)
  ) configs in

  Lwt.return (configs_with_fees, auth_token)

(** Order processing loop - consumes orders from strategy ring buffers *)
let order_processing_loop () =
  let section = "order_processor" in
  let cycle_count = ref 0 in
  let orders_placed = ref 0 in
  let order_mutex = Mutex.create () in

  while not (Atomic.get shutdown_requested) do
    Thread.delay 1.0;  (* Process orders every second *)
    incr cycle_count;

    (* Check if trading WebSocket is connected *)
    if not (Kraken.Kraken_trading_client.is_connected ()) then begin
      (* Check if reconnection is in progress to avoid false alerts *)
      let is_reconnecting =
        try
          let trading_conn = Hashtbl.find connections "kraken_trading_ws" in
          get_state trading_conn = Connecting
        with Not_found -> false
      in
      if not is_reconnecting then
        Logging.warn ~section "Trading WebSocket not connected, skipping order processing";
      (* Clear any pending orders to avoid stale amendments *)
      ignore (Dio_strategies.Suicide_grid.Strategy.get_pending_orders 1000);
      ignore (Dio_strategies.Market_maker.Strategy.get_pending_orders 1000)
    end else begin
    try
      (* Get pending orders from suicide grid strategy *)
      let pending_grid_orders = Dio_strategies.Suicide_grid.Strategy.get_pending_orders 100 in

      (* Get pending orders from market maker strategy *)
      let pending_mm_orders = Dio_strategies.Market_maker.Strategy.get_pending_orders 100 in

      (* Process each pending order - serialize to avoid mutex deadlocks *)

      (* Process suicide grid orders *)
      List.iter (fun order ->
        Mutex.lock order_mutex;
        try
          (* Get authentication token *)
          let auth_token = match Token_store.get () with
            | Some token -> token
            | None ->
              Logging.warn ~section "No auth token available for order operations";
              raise (Failure "No auth token")
          in

          (* Handle different operation types *)
          (match order.operation with
           | Place ->
               (* Convert strategy order to order executor format *)
               let order_request = {
                 Dio_engine.Order_executor.order_type = order.order_type;
                 side = (match order.side with
                        | Buy -> "buy"
                        | Sell -> "sell");
                 quantity = order.qty;
                 symbol = order.symbol;
                 limit_price = order.price;
                 time_in_force = Some order.time_in_force;
                 post_only = Some order.post_only;
                 margin = None;
                 reduce_only = None;
                 order_userref = order.userref;
                 cl_ord_id = None;
                 trigger_price = None;
                 trigger_price_type = None;
                 display_qty = None;
                 fee_preference = None;
                 duplicate_key = Dio_engine.Order_executor.Test.generate_duplicate_key order.symbol (match order.side with Buy -> "buy" | Sell -> "sell") order.qty order.price;
               } in

               (* Place the order using Lwt async but within mutex protection *)
               Lwt.async (fun () ->
                 Lwt.catch (fun () ->
                   let _order_start_time = Telemetry.start_timer () in
                   Dio_engine.Order_executor.place_order ~token:auth_token order_request >>= function
                   | result ->
                       begin match result with
                   | Ok result ->
                       incr orders_placed;
                       Logging.info_f ~section "✓ Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
                         (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                         (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                         result.order_id;
                       Telemetry.inc_counter Telemetry.Common.orders_placed ();

                       (* Notify strategy that order was acknowledged *)
                       (match order.price with
                        | Some price ->
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                                   order.symbol result.order_id
                                   (match order.side with
                                    | Buy -> Buy
                                    | Sell -> Sell)
                                   price
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                                   order.symbol result.order_id
                                   (match order.side with
                                    | Buy -> Buy
                                    | Sell -> Sell)
                                   price
                             | _ ->
                                 Logging.warn_f ~section "Unknown strategy '%s' for order acknowledgment: %s" order.strategy result.order_id
                            )
                        | None ->
                            Logging.warn_f ~section "Order acknowledged but no price available for strategy update: %s" result.order_id
                       );
                       Lwt.return_unit
                   | Error err ->
                       Logging.error_f ~section "✗ Order placement failed: %s %s %.8f @ %s - %s"
                         (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                         (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                         err;
                       Telemetry.inc_counter Telemetry.Common.orders_failed ();

                       (* Notify strategy that order was rejected *)
                       (match order.price with
                        | Some price ->
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                                   order.symbol order.side price
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.handle_order_rejected
                                   order.symbol order.side price
                             | _ ->
                                 Logging.warn_f ~section "Unknown strategy '%s' for order rejection: %s" order.strategy err
                            )
                        | None ->
                            Logging.warn_f ~section "Order rejected but no price available for strategy update: %s" err
                       );
                       Lwt.return_unit
                       end
                 ) (fun exn ->
                   Logging.error_f ~section "✗ Exception placing order %s %s: %s"
                     (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                   Telemetry.inc_counter Telemetry.Common.orders_failed ();

                   (* For exceptions, also notify strategy that order placement failed *)
                   (match order.price with
                    | Some price ->
                        (match order.strategy with
                         | "Grid" ->
                             Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                               order.symbol order.side price
                         | "MM" ->
                             Dio_strategies.Market_maker.Strategy.handle_order_rejected
                               order.symbol order.side price
                         | _ ->
                             Logging.warn_f ~section "Unknown strategy '%s' for order exception: %s" order.strategy (Printexc.to_string exn)
                        )
                    | None ->
                        Logging.warn_f ~section "Order exception but no price available for strategy update: %s" (Printexc.to_string exn)
                   );
                   Lwt.return_unit
                 )
               )

           | Amend ->
               (* Handle amend operation *)
               (match order.order_id with
                | Some target_order_id ->
                    let amend_request = {
                      Dio_engine.Order_executor.order_id = target_order_id;
                      cl_ord_id = None;
                      new_quantity = Some order.qty;
                      new_limit_price = order.price;
                      limit_price_type = None;
                      post_only = Some order.post_only;
                      new_trigger_price = None;
                      trigger_price_type = None;
                      new_display_qty = None;
                      deadline = None;
                      symbol = Some order.symbol;
                    } in

                    Lwt.async (fun () ->
                      Lwt.catch (fun () ->
                        Dio_engine.Order_executor.amend_order ~token:auth_token amend_request >>= function
                        | Ok result ->
                            incr orders_placed;
                            Logging.info_f ~section "✓ Order amended successfully: %s %s %.8f @ %s (Amend ID: %s)"
                              (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                              (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                              result.amend_id;
                            Telemetry.inc_counter (Telemetry.counter "orders_amended" ()) ();

                            (* Notify strategy that amendment was acknowledged *)
                            (match order.price with
                             | Some price ->
                                 (match order.strategy with
                                  | "Grid" ->
                                      Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                                        order.symbol result.order_id order.side price
                                  | "MM" ->
                                      Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                                        order.symbol result.order_id order.side price
                                  | _ ->
                                      Logging.warn_f ~section "Unknown strategy '%s' for amendment acknowledgment: %s" order.strategy result.order_id
                                 )
                             | None ->
                                 Logging.warn_f ~section "Amendment acknowledged but no price available for strategy update: %s" result.order_id
                            );
                            Lwt.return_unit
                        | Error err ->
                            Logging.error_f ~section "✗ Order amendment failed: %s %s %.8f @ %s - %s"
                              (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                              (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                              err;
                            Telemetry.inc_counter Telemetry.Common.orders_failed ();

                            (* Notify strategy that amendment was rejected *)
                            (match order.price with
                             | Some price ->
                                 (match order.strategy with
                                  | "Grid" ->
                                      Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                                        order.symbol order.side price
                                  | "MM" ->
                                      Dio_strategies.Market_maker.Strategy.handle_order_rejected
                                        order.symbol order.side price
                                  | _ ->
                                      Logging.warn_f ~section "Unknown strategy '%s' for amendment rejection: %s" order.strategy err
                                 )
                             | None ->
                                 Logging.warn_f ~section "Amendment rejected but no price available for strategy update: %s" err
                            );
                            Lwt.return_unit
                      ) (fun exn ->
                        Logging.error_f ~section "✗ Exception amending order %s %s: %s"
                          (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol (Printexc.to_string exn);
                        Telemetry.inc_counter Telemetry.Common.orders_failed ();

                        (* For exceptions, also notify strategy that amendment failed *)
                        (match order.price with
                         | Some price ->
                             (match order.strategy with
                              | "Grid" ->
                                  Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                                    order.symbol order.side price
                              | "MM" ->
                                  Dio_strategies.Market_maker.Strategy.handle_order_rejected
                                    order.symbol order.side price
                              | _ ->
                                  Logging.warn_f ~section "Unknown strategy '%s' for amendment exception: %s" order.strategy (Printexc.to_string exn)
                             )
                         | None ->
                             Logging.warn_f ~section "Amendment exception but no price available for strategy update: %s" (Printexc.to_string exn)
                        );
                        Lwt.return_unit
                      )
                    )
                | None ->
                    Logging.error_f ~section "Amendment request missing target order ID for %s %s"
                      (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol;
                    Telemetry.inc_counter Telemetry.Common.orders_failed ()
               )

           | Cancel ->
               (* Handle cancel operation *)
               (match order.order_id with
                | Some target_order_id ->
                    Lwt.async (fun () ->
                      Lwt.catch (fun () ->
                        Dio_engine.Order_executor.cancel_order ~token:auth_token ~order_id:target_order_id >>= function
                        | Ok results ->
                            let count = List.length results in
                            orders_placed := !orders_placed + count;
                            Logging.info_f ~section "✓ Cancelled %d order(s) successfully: %s" count target_order_id;
                            Telemetry.inc_counter (Telemetry.counter "orders_cancelled" ()) ~value:count ();

                            (* Clean up pending cancellation tracking *)
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | _ -> ());

                            (* For cancellations, we don't notify the strategy about acknowledgements *)
                            Lwt.return_unit
                        | Error err ->
                            Logging.error_f ~section "✗ Order cancellation failed: %s - %s" target_order_id err;
                            Telemetry.inc_counter Telemetry.Common.orders_failed ();

                            (* Clean up pending cancellation tracking on failure *)
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | _ -> ());

                            Lwt.return_unit
                      ) (fun exn ->
                        Logging.error_f ~section "✗ Exception cancelling order %s: %s" target_order_id (Printexc.to_string exn);
                        Telemetry.inc_counter Telemetry.Common.orders_failed ();

                        (* Clean up pending cancellation tracking on exception *)
                        (match order.strategy with
                         | "Grid" ->
                             Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                         | "MM" ->
                             Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                         | _ -> ());

                        Lwt.return_unit
                      )
                    )
                | None ->
                    Logging.error_f ~section "Cancel request missing target order ID for %s" order.symbol;
                    Telemetry.inc_counter Telemetry.Common.orders_failed ()
               )
          );
          Mutex.unlock order_mutex
        with exn ->
          Mutex.unlock order_mutex;
          Logging.error_f ~section "Error processing order %s %s: %s"
            (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
      ) pending_grid_orders;

      (* Process market maker orders *)
      List.iter (fun order ->
        Mutex.lock order_mutex;
        try
          (* Get authentication token *)
          let auth_token = match Token_store.get () with
            | Some token -> token
            | None ->
              Logging.warn ~section "No auth token available for order operations";
              raise (Failure "No auth token")
          in

          (* Handle different operation types *)
          (match order.operation with
           | Place ->
               (* Convert strategy order to order executor format *)
               let order_request = {
                 Dio_engine.Order_executor.order_type = order.order_type;
                 side = (match order.side with
                        | Buy -> "buy"
                        | Sell -> "sell");
                 quantity = order.qty;
                 symbol = order.symbol;
                 limit_price = order.price;
                 time_in_force = Some order.time_in_force;
                 post_only = Some order.post_only;
                 margin = None;
                 reduce_only = None;
                 order_userref = order.userref;
                 cl_ord_id = None;
                 trigger_price = None;
                 trigger_price_type = None;
                 display_qty = None;
                 fee_preference = None;
                 duplicate_key = Dio_engine.Order_executor.Test.generate_duplicate_key order.symbol (match order.side with Buy -> "buy" | Sell -> "sell") order.qty order.price;
               } in

               (* Place the order using Lwt async but within mutex protection *)
               Lwt.async (fun () ->
                 Lwt.catch (fun () ->
                   let _order_start_time = Telemetry.start_timer () in
                   Dio_engine.Order_executor.place_order ~token:auth_token order_request >>= function
                   | result ->
                       begin match result with
                   | Ok result ->
                       incr orders_placed;
                       Logging.info_f ~section "✓ Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
                         (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                         (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                         result.order_id;
                       Telemetry.inc_counter Telemetry.Common.orders_placed ();

                       (* Notify strategy that order was acknowledged *)
                       (match order.price with
                        | Some price ->
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                                   order.symbol result.order_id
                                   (match order.side with
                                    | Buy -> Buy
                                    | Sell -> Sell)
                                   price
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                                   order.symbol result.order_id
                                   (match order.side with
                                    | Buy -> Buy
                                    | Sell -> Sell)
                                   price
                             | _ ->
                                 Logging.warn_f ~section "Unknown strategy '%s' for order acknowledgment: %s" order.strategy result.order_id
                            )
                        | None ->
                            Logging.warn_f ~section "Order acknowledged but no price available for strategy update: %s" result.order_id
                       );
                       Lwt.return_unit
                   | Error err ->
                       Logging.error_f ~section "✗ Order placement failed: %s %s %.8f @ %s - %s"
                         (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                         (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                         err;
                       Telemetry.inc_counter Telemetry.Common.orders_failed ();

                       (* Notify strategy that order was rejected *)
                       (match order.price with
                        | Some price ->
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                                   order.symbol order.side price
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.handle_order_rejected
                                   order.symbol order.side price
                             | _ ->
                                 Logging.warn_f ~section "Unknown strategy '%s' for order rejection: %s" order.strategy err
                            )
                        | None ->
                            Logging.warn_f ~section "Order rejected but no price available for strategy update: %s" err
                       );
                       Lwt.return_unit
                       end
                 ) (fun exn ->
                   Logging.error_f ~section "✗ Exception placing order %s %s: %s"
                     (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                   Telemetry.inc_counter Telemetry.Common.orders_failed ();

                   (* For exceptions, also notify strategy that order placement failed *)
                   (match order.price with
                    | Some price ->
                        (match order.strategy with
                         | "Grid" ->
                             Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                               order.symbol order.side price
                         | "MM" ->
                             Dio_strategies.Market_maker.Strategy.handle_order_rejected
                               order.symbol order.side price
                         | _ ->
                             Logging.warn_f ~section "Unknown strategy '%s' for order exception: %s" order.strategy (Printexc.to_string exn)
                        )
                    | None ->
                        Logging.warn_f ~section "Order exception but no price available for strategy update: %s" (Printexc.to_string exn)
                   );
                   Lwt.return_unit
                 )
               )

           | Amend ->
               (* Handle amend operation *)
               (match order.order_id with
                | Some target_order_id ->
                    let amend_request = {
                      Dio_engine.Order_executor.order_id = target_order_id;
                      cl_ord_id = None;
                      new_quantity = Some order.qty;
                      new_limit_price = order.price;
                      limit_price_type = None;
                      post_only = Some order.post_only;
                      new_trigger_price = None;
                      trigger_price_type = None;
                      new_display_qty = None;
                      deadline = None;
                      symbol = Some order.symbol;
                    } in

                    Lwt.async (fun () ->
                      Lwt.catch (fun () ->
                        Dio_engine.Order_executor.amend_order ~token:auth_token amend_request >>= function
                        | Ok result ->
                            incr orders_placed;
                            Logging.info_f ~section "✓ Order amended successfully: %s %s %.8f @ %s (Amend ID: %s)"
                              (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                              (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                              result.amend_id;
                            Telemetry.inc_counter (Telemetry.counter "orders_amended" ()) ();

                            (* Notify strategy that amendment was acknowledged *)
                            (match order.price with
                             | Some price ->
                                 (match order.strategy with
                                  | "Grid" ->
                                      Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                                        order.symbol result.order_id order.side price
                                  | "MM" ->
                                      Dio_strategies.Market_maker.Strategy.handle_order_acknowledged
                                        order.symbol result.order_id order.side price
                                  | _ ->
                                      Logging.warn_f ~section "Unknown strategy '%s' for amendment acknowledgment: %s" order.strategy result.order_id
                                 )
                             | None ->
                                 Logging.warn_f ~section "Amendment acknowledged but no price available for strategy update: %s" result.order_id
                            );
                            Lwt.return_unit
                        | Error err ->
                            Logging.error_f ~section "✗ Order amendment failed: %s %s %.8f @ %s - %s"
                              (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol order.qty
                              (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                              err;
                            Telemetry.inc_counter Telemetry.Common.orders_failed ();

                            (* Notify strategy that amendment was rejected *)
                            (match order.price with
                             | Some price ->
                                 (match order.strategy with
                                  | "Grid" ->
                                      Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                                        order.symbol order.side price
                                  | "MM" ->
                                      Dio_strategies.Market_maker.Strategy.handle_order_rejected
                                        order.symbol order.side price
                                  | _ ->
                                      Logging.warn_f ~section "Unknown strategy '%s' for amendment rejection: %s" order.strategy err
                                 )
                             | None ->
                                 Logging.warn_f ~section "Amendment rejected but no price available for strategy update: %s" err
                            );
                            Lwt.return_unit
                      ) (fun exn ->
                        Logging.error_f ~section "✗ Exception amending order %s %s: %s"
                          (match order.side with
                          | Buy -> "buy"
                          | Sell -> "sell") order.symbol (Printexc.to_string exn);
                        Telemetry.inc_counter Telemetry.Common.orders_failed ();

                        (* For exceptions, also notify strategy that amendment failed *)
                        (match order.price with
                         | Some price ->
                             (match order.strategy with
                              | "Grid" ->
                                  Dio_strategies.Suicide_grid.Strategy.handle_order_rejected
                                    order.symbol order.side price
                              | "MM" ->
                                  Dio_strategies.Market_maker.Strategy.handle_order_rejected
                                    order.symbol order.side price
                              | _ ->
                                  Logging.warn_f ~section "Unknown strategy '%s' for amendment exception: %s" order.strategy (Printexc.to_string exn)
                             )
                         | None ->
                             Logging.warn_f ~section "Amendment exception but no price available for strategy update: %s" (Printexc.to_string exn)
                        );
                        Lwt.return_unit
                      )
                    )
                | None ->
                    Logging.error_f ~section "Amendment request missing target order ID for %s %s"
                      (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol;
                    Telemetry.inc_counter Telemetry.Common.orders_failed ()
               )

           | Cancel ->
               (* Handle cancel operation *)
               (match order.order_id with
                | Some target_order_id ->
                    Lwt.async (fun () ->
                      Lwt.catch (fun () ->
                        Dio_engine.Order_executor.cancel_order ~token:auth_token ~order_id:target_order_id >>= function
                        | Ok results ->
                            let count = List.length results in
                            orders_placed := !orders_placed + count;
                            Logging.info_f ~section "✓ Cancelled %d order(s) successfully: %s" count target_order_id;
                            Telemetry.inc_counter (Telemetry.counter "orders_cancelled" ()) ~value:count ();

                            (* Clean up pending cancellation tracking *)
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | _ -> ());

                            (* For cancellations, we don't notify the strategy about acknowledgements *)
                            Lwt.return_unit
                        | Error err ->
                            Logging.error_f ~section "✗ Order cancellation failed: %s - %s" target_order_id err;
                            Telemetry.inc_counter Telemetry.Common.orders_failed ();

                            (* Clean up pending cancellation tracking on failure *)
                            (match order.strategy with
                             | "Grid" ->
                                 Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | "MM" ->
                                 Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                             | _ -> ());

                            Lwt.return_unit
                      ) (fun exn ->
                        Logging.error_f ~section "✗ Exception cancelling order %s: %s" target_order_id (Printexc.to_string exn);
                        Telemetry.inc_counter Telemetry.Common.orders_failed ();

                        (* Clean up pending cancellation tracking on exception *)
                        (match order.strategy with
                         | "Grid" ->
                             Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                         | "MM" ->
                             Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                         | _ -> ());

                        Lwt.return_unit
                      )
                    )
                | None ->
                    Logging.error_f ~section "Cancel request missing target order ID for %s" order.symbol;
                    Telemetry.inc_counter Telemetry.Common.orders_failed ()
               )
          );
          Mutex.unlock order_mutex
        with exn ->
          Mutex.unlock order_mutex;
          Logging.error_f ~section "Error processing order %s %s: %s"
            (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
      ) pending_mm_orders;

      (* Log progress every 100 cycles (~100 seconds) *)
      if !cycle_count mod 100 = 0 then
        Logging.info_f ~section "Order processing: %d orders placed, %d grid + %d mm pending in current batch"
          !orders_placed (List.length pending_grid_orders) (List.length pending_mm_orders)

    with exn ->
      Logging.error_f ~section "Exception in order processing loop: %s" (Printexc.to_string exn);
    end;

  done

(** Start the supervisor monitoring thread and initialize all feeds *)
let start_monitoring () =
  Logging.info ~section "Starting connection supervisor";

  (* Start the monitoring thread first *)
  let _monitor_thread = Thread.create monitor_loop () in

  (* Start the order processing thread *)
  let _order_thread = Thread.create order_processing_loop () in

  (* Initialize all feeds and get configs with fees and token *)
  (* Use Lwt_main.run here to handle the promise from initialize_feeds *)
  let (configs_with_fees, _auth_token) = Lwt_main.run (initialize_feeds ()) in

  configs_with_fees

(** Start the order executor with the authentication token *)
let start_order_executor () : unit Lwt.t =

  (* Get the token from the global storage *)
  let _auth_token = match Token_store.get () with
    | Some token -> token
    | None ->
        Logging.warn ~section "No stored auth token found, generating new one";
        let token = Lwt_main.run (Kraken.Kraken_generate_auth_token.get_token ()) in
        Token_store.set (Some token);
        token
  in

  Dio_engine.Order_executor.init

(** Get connection by name *)
let get_connection name =
  Mutex.lock registry_mutex;
  let conn = Hashtbl.find_opt connections name in
  Mutex.unlock registry_mutex;
  match conn with
  | Some c -> c
  | None -> failwith (Printf.sprintf "Connection '%s' not found" name)

(** Get all connections *)
let get_all_connections () =
  Mutex.lock registry_mutex;
  let conns = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  conns

(** Stop order processing loop *)
let stop_order_processing () =
  Atomic.set shutdown_requested true;
  Logging.info ~section "Order processing loop shutdown requested"

(** Stop all connections *)
let stop_all () =
  stop_order_processing ();
  Thread.delay 0.5;  (* Give order processing thread time to finish current iteration *)
  Logging.warn ~section "Stopping all supervised connections";
  Mutex.lock registry_mutex;
  Hashtbl.iter (fun _name conn ->
    set_state conn Disconnected
  ) connections;
  Mutex.unlock registry_mutex

