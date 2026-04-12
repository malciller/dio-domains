
(** Connection supervisor. Monitors WebSocket connection lifecycle, performs
    health checks via ping/pong and data heartbeats, and triggers automatic
    reconnection with circuit breaker and backoff logic. Also owns the
    centralized order processing loop that drains strategy ring buffers. *)

open Lwt.Infix

let section = "supervisor"

(* Shared order types (side, operation, strategy_order). *)
open Dio_strategies.Strategy_common


(* Canonical connection types and global registry from Supervisor_types. *)
open Supervisor_types


(* Atomic shutdown flag and condition variable for graceful termination. *)
let shutdown_requested = Atomic.make false
let shutdown_mutex = Mutex.create ()
let shutdown_cond = Condition.create ()

(* Timestamp of the last Supervisor_cache.force_update call.
   Rate-limits cache refreshes to prevent allocation bursts during
   rapid reconnect cycles. *)
let last_supervisor_cache_update = ref 0.0

(** Polls the shutdown flag in 100ms increments and returns early if set. *)
let interruptible_sleep seconds =
  if Atomic.get shutdown_requested then ()
  else begin
    Mutex.lock shutdown_mutex;
    if Atomic.get shutdown_requested then
      Mutex.unlock shutdown_mutex
    else begin
      Mutex.unlock shutdown_mutex;
      let rec sleep_loop remaining =
        if remaining <= 0.0 || Atomic.get shutdown_requested then ()
        else begin
          let sleep_time = min remaining 0.1 in
          Thread.delay sleep_time;
          sleep_loop (remaining -. sleep_time)
        end
      in
      sleep_loop seconds
    end
  end

(** Global authentication token store. Single-writer (supervisor init),
    lock-free reads via Atomic snapshot. *)
module Token_store = struct
  (* Atomic ref: single writer at init, concurrent lock-free readers. *)
  let token : string option Atomic.t = Atomic.make None

  let set value = Atomic.set token value

  let get () = Atomic.get token
end

(** Generates monotonically increasing ping request IDs starting at 1000001
    to avoid collisions with trading request IDs. *)
let next_ping_req_id =
  let counter = ref 1000000 in
  fun () -> incr counter; !counter


(** Registers a new supervised connection in the global registry.
    Initializes all state fields and stores the optional connect_fn
    for automatic reconnection. *)
let register ~name ~connect_fn =
  Mutex.lock registry_mutex;
  let conn = {
    name;
    state = Disconnected;
    last_connected = None;
    last_disconnected = Some (Unix.time ());  (* Seed with current time to suppress immediate auto-restart *)
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

(** Registers an existing connection for health monitoring only.
    No connect_fn is provided, so automatic reconnection is disabled. *)
let register_for_monitoring ~name =
  Mutex.lock registry_mutex;
  let conn = {
    name;
    state = Connected;  (* Assumed already connected since this is monitor-only *)
    last_connected = Some (Unix.time ());
    last_disconnected = None;
    last_connecting = None;
    last_data_received = Some (Unix.time ());  (* Seed heartbeat timestamp *)
    last_ping_sent = None;
    ping_failures = Atomic.make 0;
    reconnect_attempts = 0;
    total_connections = 1;
    circuit_breaker = Closed;
    circuit_breaker_failures = 0;
    circuit_breaker_last_failure = None;
    connect_fn = None;  (* No connect_fn: reconnection disabled *)
    mutex = Mutex.create ();
  } in
  Hashtbl.replace connections name conn;
  Mutex.unlock registry_mutex;
  Logging.info_f ~section "Registered connection for monitoring: %s" name;
  conn

(** Transitions connection to [new_state], updating timestamps and counters
    under the per-connection mutex. Propagates changes to the supervisor
    cache at most once per second. *)
let set_state conn new_state =
  Mutex.lock conn.mutex;
  let old_state = conn.state in
  conn.state <- new_state;

  
  (match new_state with
  | Connected ->
      conn.last_connected <- Some (Unix.time ());
      conn.last_connecting <- None;
      conn.last_data_received <- Some (Unix.time ());
      conn.last_ping_sent <- None;
      Atomic.set conn.ping_failures 0;
      conn.reconnect_attempts <- 0;
      conn.total_connections <- conn.total_connections + 1;
      Logging.info_f ~section "[%s] Connection established (total: %d)"
          conn.name conn.total_connections
  | Disconnected ->
      conn.last_disconnected <- Some (Unix.time ());
      conn.last_connecting <- None;
      Logging.warn_f ~section "[%s] Connection lost" conn.name
  | Connecting ->
      conn.last_connecting <- Some (Unix.time ());
      Logging.info_f ~section "[%s] Attempting connection (attempt #%d)"
          conn.name (conn.reconnect_attempts + 1)
  | Failed reason ->
      conn.last_disconnected <- Some (Unix.time ());
      conn.last_connecting <- None;
      conn.reconnect_attempts <- conn.reconnect_attempts + 1;
      Logging.error_f ~section "[%s] Connection failed: %s (attempt #%d)"
          conn.name reason conn.reconnect_attempts);
  
  Mutex.unlock conn.mutex;
  
  (* Log state transitions and propagate to cache *)
  if old_state <> new_state then begin

       
    (* Rate-limit cache updates to at most once per second.
       The dashboard state_broadcaster picks up interim deltas
       at its next 500ms tick regardless. *)
    let now = Unix.gettimeofday () in
    let last = !last_supervisor_cache_update in
    if now -. last >= 1.0 then begin
      last_supervisor_cache_update := now;
      Supervisor_cache.force_update ()
    end
  end

(** Returns the current connection state under mutex. *)
let get_state conn =
  Mutex.lock conn.mutex;
  let state = conn.state in
  Mutex.unlock conn.mutex;
  state

(** Returns elapsed seconds since the connection entered Connected state,
    or None if currently disconnected. *)
let get_uptime conn =
  Mutex.lock conn.mutex;
  let uptime = match conn.last_connected, conn.state with
    | Some t, Connected -> Some (Unix.time () -. t)
    | _ -> None
  in
  Mutex.unlock conn.mutex;
  uptime

(** Replaces the stored connect_fn (used for deferred registration). *)
let set_connect_fn conn connect_fn =
  Mutex.lock conn.mutex;
  conn.connect_fn <- connect_fn;
  Mutex.unlock conn.mutex

(** Records current time as the last data heartbeat for this connection. *)
let update_data_heartbeat conn =
  Mutex.lock conn.mutex;
  conn.last_data_received <- Some (Unix.time ());
  Mutex.unlock conn.mutex



(** Checks whether the circuit breaker permits a connection attempt.
    Caller must hold conn.mutex. Transitions Open to HalfOpen after
    a 300s (5 min) cooldown. *)
let circuit_breaker_allows_connection_unlocked conn =
  let current_time = Unix.time () in
  match conn.circuit_breaker with
  | Closed -> true
  | Open ->
      begin match conn.circuit_breaker_last_failure with
      | Some failure_time when current_time -. failure_time > 300.0 ->  (* 5 min cooldown *)
          conn.circuit_breaker <- HalfOpen;
          Logging.info_f ~section "[%s] Circuit breaker HALF-OPEN (testing recovery)" conn.name;
          true
      | _ -> false
      end
  | HalfOpen -> true  (* Permit one probe attempt *)

(** Thread-safe wrapper around [circuit_breaker_allows_connection_unlocked]. *)
let circuit_breaker_allows_connection conn =
  Mutex.lock conn.mutex;
  let allowed = circuit_breaker_allows_connection_unlocked conn in
  Mutex.unlock conn.mutex;
  allowed

(** Updates circuit breaker state. On success, resets to Closed.
    On failure, increments the counter and opens the circuit
    after 5 consecutive failures. *)
let update_circuit_breaker conn success =
  Mutex.lock conn.mutex;
  if success then begin
    (* Reset circuit breaker on success *)
    conn.circuit_breaker <- Closed;
    conn.circuit_breaker_failures <- 0;
    conn.circuit_breaker_last_failure <- None;
  end else begin
    (* Increment failure counter; open circuit at threshold *)
    conn.circuit_breaker_failures <- conn.circuit_breaker_failures + 1;
    conn.circuit_breaker_last_failure <- Some (Unix.time ());

    if conn.circuit_breaker_failures >= 5 then begin  (* Threshold: 5 consecutive failures *)
      conn.circuit_breaker <- Open;
      Logging.warn_f ~section "[%s] Circuit breaker OPEN after %d consecutive failures" conn.name conn.circuit_breaker_failures;
    end
  end;
  Mutex.unlock conn.mutex

(** Schedules connect_fn in the Lwt event loop if the circuit breaker
    permits and the connection is not already in Connecting state.
    Transitions state to Connecting under mutex before launching. *)
let start_async conn =
  match conn.connect_fn with
  | None ->
      Logging.warn_f ~section "[%s] Cannot start connection - no connect function provided (monitoring only)" conn.name
  | Some connect_fn ->
      Mutex.lock conn.mutex;
      let should_start, attempt_num_opt = match conn.state with
        | Connecting -> 
            Mutex.unlock conn.mutex;
            false, None
        | _ ->
            if not (circuit_breaker_allows_connection_unlocked conn) then begin
              conn.state <- Failed "Circuit breaker open";
              conn.last_disconnected <- Some (Unix.time ());
              conn.last_connecting <- None;
              conn.reconnect_attempts <- conn.reconnect_attempts + 1;
              let attempt_num = conn.reconnect_attempts in
              Mutex.unlock conn.mutex;
              
              Logging.warn_f ~section "[%s] Circuit breaker blocks connection attempt" conn.name;
              Logging.error_f ~section "[%s] Connection failed: Circuit breaker open (attempt #%d)" conn.name attempt_num;

              false, None
            end else begin
              conn.state <- Connecting;
              conn.last_connecting <- Some (Unix.time ());
              conn.reconnect_attempts <- conn.reconnect_attempts + 1;
              let attempt_num = conn.reconnect_attempts in
              Mutex.unlock conn.mutex;
              

              Logging.info_f ~section "[%s] Attempting connection (attempt #%d)" conn.name attempt_num;

              
              let now = Unix.gettimeofday () in
              let last = !last_supervisor_cache_update in
              if now -. last >= 1.0 then begin
                last_supervisor_cache_update := now;
                Supervisor_cache.force_update ()
              end;
              Logging.info_f ~section "[%s] Starting supervised connection (attempt #%d)" conn.name attempt_num;
              true, Some attempt_num
            end
      in

      if should_start then begin
        let attempt_num = Option.get attempt_num_opt in
        let open Lwt.Infix in
        Lwt.async (fun () ->
          (* connect_fn manages its own Connected/Failed transitions *)
          Lwt.catch (fun () ->
            connect_fn () >>= fun () ->
            (* WebSocket connect_fn should block indefinitely; early return is abnormal *)
            Mutex.lock conn.mutex;
            let already_failed = match conn.state with Failed _ -> true | _ -> false in
            let latest_attempt = conn.reconnect_attempts in
            Mutex.unlock conn.mutex;
            if not already_failed && latest_attempt = attempt_num then begin
              Logging.warn_f ~section "[%s] Connection function completed unexpectedly" conn.name;
              set_state conn (Failed "connection completed unexpectedly")
            end;
            Lwt.return_unit
          ) (fun exn ->
            let error_msg = Printexc.to_string exn in
            Mutex.lock conn.mutex;
            let latest_attempt = conn.reconnect_attempts in
            Mutex.unlock conn.mutex;
            if latest_attempt = attempt_num then begin
              Logging.error_f ~section "[%s] Unexpected error in connection function: %s" conn.name error_msg;
              (* Transition to Failed on unhandled exception *)
              set_state conn (Failed error_msg)
            end else begin
              Logging.debug_f ~section "[%s] Muting error from superseded connection attempt: %s" conn.name error_msg
            end;
            Lwt.return_unit
          )
        )
      end

(** Forces a reconnect by resetting state to Disconnected and
    clearing the reconnect counter before calling [start_async]. *)
let restart conn =
  Logging.info_f ~section "[%s] Manually restarting connection" conn.name;
  set_state conn Disconnected;
  (* Reset backoff counter for manual restart *)
  Mutex.lock conn.mutex;
  conn.reconnect_attempts <- 0;
  Mutex.unlock conn.mutex;
  start_async conn

(** Tick-driven health monitor. Subscribes to the tick event bus and checks
    all registered connections at most once per second. Implements:
    - Linear backoff reconnection for Failed connections (2s..30s)
    - Stale disconnect detection (60s idle in Disconnected state)
    - Stuck-connecting timeout (120s)
    - Active ping/pong liveness for authenticated WebSockets
    - Passive data heartbeat timeout for market data feeds *)
let monitor_loop () =
  let cycle_count = ref 0 in
  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else begin
      let%lwt () = Lwt_unix.sleep 1.0 in
      if Atomic.get shutdown_requested then Lwt.return_unit else begin
        let current_time = Unix.time () in
        try
          incr cycle_count;

                Mutex.lock registry_mutex;
                let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
                Mutex.unlock registry_mutex;
              
                (* Iterate connections and apply health checks *)
                List.iter (fun conn ->
                  if Atomic.get shutdown_requested then () else
                  (* Snapshot state fields under mutex *)
                  Mutex.lock conn.mutex;
                  let state = conn.state in
                  let attempts = conn.reconnect_attempts in
                  let last_disconnected = conn.last_disconnected in
                  let last_connecting = conn.last_connecting in
                  let has_connect_fn = Option.is_some conn.connect_fn in
                  Mutex.unlock conn.mutex;

                  (* Health check and backup reconnection logic *)
                  match state, has_connect_fn with
                  | Failed reason, true ->
                      (* Re-read state under lock to prevent TOCTOU race *)
                      Mutex.lock conn.mutex;
                      let current_state = conn.state in
                      Mutex.unlock conn.mutex;

                      if current_state <> Connecting then begin
                        (* IBKR market hours gate: skip reconnection attempts
                           outside US equity extended hours (4 AM – 8 PM ET).
                           The connect_fn itself will sleep until the next
                           open window, so there's nothing for the monitor to do. *)
                        if String.equal conn.name "ibkr_gateway"
                           && not (Ibkr.Market_hours.is_market_open ()) then
                          ()  (* Market closed — suppress reconnection *)
                        else begin
                          (* Exponential backoff: 0s, 2s, 4s, 8s, ... capped at 30s (300s for IBKR and Lighter) *)
                          let max_delay = 
                            if String.equal conn.name "ibkr_gateway" || String.equal conn.name "lighter_ws" then 300.0 
                            else 30.0 
                          in
                          let delay = 
                            if attempts <= 1 then 0.0
                            else min max_delay (2.0 ** Float.of_int (attempts - 1))
                          in

                          (* Only reconnect after backoff elapses *)
                          let should_reconnect =
                            match last_disconnected with
                            | Some t -> current_time -. t >= delay
                            | None -> true
                          in

                          if should_reconnect then begin
                            Logging.info_f ~section "[%s] Backup auto-reconnecting after %.1fs backoff (reason: %s)..." conn.name delay reason;
                            start_async conn
                          end
                        end
                      end
                  | Disconnected, true ->
                      (* Disconnected without failure may be intentional.
                         Only restart after 60s idle to avoid interfering with
                         graceful shutdown or manual disconnect. *)
                      let should_reconnect =
                        match last_disconnected with
                        | Some t -> current_time -. t >= 60.0
                        | None -> false
                      in

                      if should_reconnect then begin
                        Logging.warn_f ~section "[%s] Connection disconnected for >60s, restarting..." conn.name;
                        start_async conn
                      end
                  | Connecting, _ ->
                      (* Detect stuck Connecting state *)
                      let stuck_time = match last_connecting with
                        | Some t -> current_time -. t
                        | None -> 0.0  (* Defensive fallback *)
                      in
                      if stuck_time > 120.0 then begin  (* 2 min timeout *)
                        Logging.error_f ~section "[%s] Connection stuck in 'Connecting' state for %.0fs, restarting..." conn.name stuck_time;
                        (* Re-check under mutex before forcing restart *)
                        Mutex.lock conn.mutex;
                        let current_state = conn.state in
                        Mutex.unlock conn.mutex;

                        if current_state = Connecting then begin
                          set_state conn Disconnected;
                          start_async conn
                        end
                      end
                  | Connected, _ ->
                      (* Active ping/pong liveness for authenticated connections *)
                      if String.equal conn.name "kraken_auth_ws" || String.equal conn.name "hyperliquid_ws" || String.equal conn.name "lighter_ws" then begin
                        let should_ping =
                          match conn.last_ping_sent with
                          | None -> true  (* First ping *)
                          | Some last_ping -> current_time -. last_ping >= 15.0  (* 15s interval, under 30s server timeout *)
                        in

                        if should_ping then begin
                          (* Dispatch ping asynchronously *)
                          conn.last_ping_sent <- Some current_time;
                          Lwt.async (fun () ->
                            let req_id = next_ping_req_id () in
                            if String.equal conn.name "kraken_auth_ws" then
                              Lwt.catch
                                (fun () ->
                                  Kraken.Kraken_trading_client.send_ping ~req_id ~timeout_ms:5000 >>= fun response ->
                                  if response.success then begin

                                    Atomic.set conn.ping_failures 0;
                                    update_data_heartbeat conn;
                                    Lwt.return_unit
                                  end else begin
                                    Logging.warn_f ~section "[%s] Ping failed: %s" conn.name
                                      (match response.error with Some e -> e | None -> "unknown error");
                                    Atomic.incr conn.ping_failures;
                                    Lwt.return_unit
                                  end
                                )
                                (fun exn ->
                                  Logging.warn_f ~section "[%s] Ping exception: %s" conn.name (Printexc.to_string exn);
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit
                                )
                            else if String.equal conn.name "hyperliquid_ws" then
                              Lwt.catch
                                (fun () ->
                                  Hyperliquid.Ws.send_ping ~req_id ~timeout_ms:5000 >>= fun success ->
                                  if success then begin

                                    Atomic.set conn.ping_failures 0;
                                    update_data_heartbeat conn;
                                    Lwt.return_unit
                                  end else begin
                                    Logging.warn_f ~section "[%s] Ping failed (req_id: %d)" conn.name req_id;
                                    Atomic.incr conn.ping_failures;
                                    Lwt.return_unit
                                  end
                                )
                                (fun exn ->
                                  Logging.warn_f ~section "[%s] Ping exception: %s" conn.name (Printexc.to_string exn);
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit
                                )
                            else if String.equal conn.name "lighter_ws" then
                              Lwt.catch
                                (fun () ->
                                  Lighter.Ws.send_ping ~req_id ~timeout_ms:5000 >>= fun success ->
                                  if success then begin

                                    Atomic.set conn.ping_failures 0;
                                    update_data_heartbeat conn;
                                    Lwt.return_unit
                                  end else begin
                                    Logging.warn_f ~section "[%s] Ping failed (req_id: %d)" conn.name req_id;
                                    Atomic.incr conn.ping_failures;
                                    Lwt.return_unit
                                  end
                                )
                                (fun exn ->
                                  Logging.warn_f ~section "[%s] Ping exception: %s" conn.name (Printexc.to_string exn);
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit
                                )
                            else Lwt.return_unit
                          )
                        end;

                        (* Check ping failures outside async to avoid mutex deadlock *)
                        let ping_failures = Atomic.get conn.ping_failures in
                        if ping_failures >= 3 then begin
                          Logging.error_f ~section "[%s] Ping failed %d times, marking connection as failed"
                            conn.name ping_failures;
                          set_state conn (Failed "ping timeout");
                        end
                      end else begin
                        (* Passive heartbeat monitoring for market data feeds *)
                        match conn.last_data_received with
                        | Some last_data when current_time -. last_data > 60.0 ->  (* 60s data silence threshold *)
                            Logging.warn_f ~section "[%s] No data received for %.0fs, marking connection as failed"
                              conn.name (current_time -. last_data);
                            set_state conn (Failed "data timeout")
                        | _ -> ()
                      end
                  | _ -> ()
                ) conn_list;
                (* Spawn next iteration independently to sever Forward chain. *)
          Lwt.async loop;
          Lwt.return_unit
        with exn ->
          Logging.error_f ~section "Exception in monitor loop: %s" (Printexc.to_string exn);
          Logging.error_f ~section "Monitor loop continuing after exception...";
          Lwt.async loop;
          Lwt.return_unit
      end
    end
  in
  Lwt.async loop

(** Performs the full WebSocket feed initialization sequence:
    1. Reads trading configs and partitions symbols by exchange
    2. Initializes ticker, instrument, orderbook, balance, and execution stores
    3. Registers and starts supervised WebSocket connections
    4. Waits for initial market data readiness with timeouts
    5. Fetches and caches trading fees per symbol
    Returns (configs_with_fees, auth_token). *)
let initialize_feeds () : ((Dio_engine.Config.trading_config list * string) Lwt.t) =
  Logging.info ~section "Initializing websocket feeds...";

  (* Load trading configurations *)
  let config = Dio_engine.Config.read_config () in
  let app_configs = config.trading in
  Logging.info_f ~section "Loaded %d trading configuration(s)" (List.length app_configs);

  (* Partition symbols by exchange *)
  let kraken_symbols = app_configs
                      |> List.filter (fun cfg -> cfg.Dio_engine.Config.exchange = "kraken")
                      |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in

  (* Extract Hyperliquid symbols; include base asset of spot pairs for perp hedge pricing *)
  let hyperliquid_symbols = app_configs
                           |> List.filter (fun cfg -> cfg.Dio_engine.Config.exchange = "hyperliquid")
                           |> List.fold_left (fun acc cfg ->
                                let sym = cfg.Dio_engine.Config.symbol in
                                if String.contains sym '/' then
                                  let base_asset = String.split_on_char '/' sym |> List.hd in
                                  base_asset :: sym :: acc
                                else
                                  sym :: acc
                              ) []
                           |> List.sort_uniq String.compare in
  let has_hyperliquid = List.length hyperliquid_symbols > 0 in
  let has_kraken = List.length kraken_symbols > 0 in
  let hyperliquid_testnet =
    match app_configs |> List.find_opt (fun (cfg : Dio_engine.Config.trading_config) -> cfg.exchange = "hyperliquid") with
    | Some cfg -> cfg.testnet
    | None -> false in

  (* Extract IBKR symbols *)
  let ibkr_symbols = app_configs
                     |> List.filter (fun cfg -> cfg.Dio_engine.Config.exchange = "ibkr")
                     |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in
  let has_ibkr = List.length ibkr_symbols > 0 in
  let ibkr_testnet =
    match app_configs |> List.find_opt (fun (cfg : Dio_engine.Config.trading_config) -> cfg.exchange = "ibkr") with
    | Some cfg -> cfg.testnet
    | None -> true in

  (* Extract Lighter symbols *)
  let lighter_symbols = app_configs
                        |> List.filter (fun cfg -> cfg.Dio_engine.Config.exchange = "lighter")
                        |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in
  let has_lighter = List.length lighter_symbols > 0 in

  (* Apply testnet flag to Hyperliquid module *)
  if has_hyperliquid then
    Hyperliquid.Module.Hyperliquid_impl.set_testnet hyperliquid_testnet;

  (* Apply testnet flag to IBKR module — must happen before gateway connection *)
  if has_ibkr then
    Ibkr.Module.Config.set_testnet ibkr_testnet;

  Logging.info_f ~section "Connecting to %d Kraken websockets..." (List.length kraken_symbols);
  if has_hyperliquid then
    Logging.info_f ~section "Connecting to %d Hyperliquid websockets..." (List.length hyperliquid_symbols);
  if has_ibkr then
    Logging.info_f ~section "Connecting to IBKR gateway for %d symbols..." (List.length ibkr_symbols);
  if has_lighter then
    Logging.info_f ~section "Connecting to Lighter L2 for %d symbols..." (List.length lighter_symbols);

  (* Begin sequential initialization steps *)

  let all_hyperliquid_symbols = hyperliquid_symbols |> List.sort_uniq String.compare in



  Logging.info ~section "Step 1.5: Starting Hyperliquid websocket connection early...";
  if has_hyperliquid then begin
     let hl_ws_conn = register ~name:"hyperliquid_ws" ~connect_fn:None in
     let hl_ws_connect_fn () =
       Lwt.catch (fun () ->
         let on_failure reason =
           set_state hl_ws_conn (Failed reason);
            (* Immediately schedule reconnection; avoids waiting for
               monitor loop backoff (mirrors Kraken auth WS pattern). *)           Lwt.async (fun () ->
             Lwt.catch (fun () ->
               Lwt.pause () >>= fun () ->
               start_async hl_ws_conn;
               Lwt.return_unit
             ) (fun exn ->
               Logging.warn_f ~section "[%s] Exception during emergency reconnection: %s" hl_ws_conn.name (Printexc.to_string exn);
               Lwt.return_unit
             )
           )
         in
         let on_heartbeat () = update_data_heartbeat hl_ws_conn in
         let on_connected () =
           set_state hl_ws_conn Connected;
           let wallet = Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.value ~default:"" in
           Lwt.async (fun () -> 
             Hyperliquid.Instruments_feed.wait_until_ready () >>= fun () ->
             Hyperliquid.Ws.subscribe_to_feeds ~symbols:hyperliquid_symbols ~wallet >>= fun () ->
             (* Clear stale open orders, then re-fetch from exchange.
                Sequential execution prevents the strategy from seeing
                zero orders and placing duplicates. *)
             Hyperliquid.Executions_feed.clear_all_open_orders ();
             Hyperliquid.Module.fetch_open_orders_ws ())
         in
         Hyperliquid.Ws.connect_and_monitor 
           ~testnet:hyperliquid_testnet 
           ~on_failure ~on_connected ~on_heartbeat
       ) (fun exn ->
         let error_msg = Printexc.to_string exn in
         Logging.error_f ~section "[%s] Connection failed: %s" hl_ws_conn.name error_msg;
         set_state hl_ws_conn (Failed error_msg);
         Lwt.return_unit
       )
     in
     set_connect_fn hl_ws_conn (Some hl_ws_connect_fn);
     start_async hl_ws_conn;
  end;

  (* Start Lighter WS connection and signer initialization *)
  let%lwt () = (if has_lighter then begin
    Logging.info ~section "Initializing Lighter signer and WebSocket...";
    let%lwt () = Lighter.Module.initialize_signer () in
    let lt_ws_conn = register ~name:"lighter_ws" ~connect_fn:None in
    let lt_ws_connect_fn () =
      Lwt.catch (fun () ->
        let on_failure reason =
          set_state lt_ws_conn (Failed reason);
          (* Do NOT call start_async here — connect_and_monitor has
             self-healing reconnect loops that never exit. The failure
             callback is only invoked when both sides are simultaneously
             down; the internal loops will recover automatically.
             Calling start_async would spawn a duplicate instance. *)
        in
        let on_heartbeat () = update_data_heartbeat lt_ws_conn in
        let on_connected () =
          set_state lt_ws_conn Connected;
          let account_index = match Sys.getenv_opt "LIGHTER_ACCOUNT_INDEX" |> Option.map String.trim with
            | Some s -> (try int_of_string s with _ -> 0)
            | None -> 0
          in
          let auth_token = Lighter.Signer.get_auth_token () in
          Lwt.async (fun () ->
            Lighter.Instruments_feed.wait_until_ready () >>= fun () ->
            Logging.info_f ~section "Lighter WS reconnected — resubscribing and rebuilding open-order state";
            Lighter.Ws.subscribe_to_feeds ~symbols:lighter_symbols ~account_index ~auth_token >>= fun () ->
            (* Rebuild order state when both sides come up together.
               Individual side reconnects handle their own resubscription
               internally via the per-side reconnect callbacks. *)
            Lighter.Executions_feed.clear_all_open_orders ();
            Lighter.Module.fetch_open_orders ())
        in
        Lighter.Ws.connect_and_monitor
          ~on_failure ~on_connected ~on_heartbeat
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[%s] Connection failed: %s" lt_ws_conn.name error_msg;
        set_state lt_ws_conn (Failed error_msg);
        Lwt.return_unit
      )
    in
    set_connect_fn lt_ws_conn (Some lt_ws_connect_fn);
    start_async lt_ws_conn;
    Lwt.return_unit
  end else Lwt.return_unit) in

  Logging.info ~section "Step 2: Initializing instruments feed stores...";
  let%lwt () = Kraken.Kraken_instruments_feed.initialize_symbols kraken_symbols in
  let%lwt () = 
    if has_hyperliquid then Hyperliquid.Module.initialize_instruments_ws ()
    else Lwt.return_unit
  in
  let%lwt () =
    if has_lighter then Lighter.Module.initialize_instruments ~symbols:lighter_symbols
    else Lwt.return_unit
  in

  Logging.info ~section "Step 3: Initializing orderbook feed stores...";
  let%lwt () = Kraken.Kraken_orderbook_feed.initialize kraken_symbols in
  if has_hyperliquid then Hyperliquid.Orderbook_feed.initialize all_hyperliquid_symbols;
  if has_ibkr then Ibkr.Orderbook_feed.initialize ibkr_symbols;
  if has_lighter then Lighter.Orderbook_feed.initialize lighter_symbols;

  Logging.info ~section "Step 4: Getting authentication token...";
  let%lwt auth_token = 
    if has_kraken then Kraken.Kraken_generate_auth_token.get_token ()
    else Lwt.return "temp_token_for_hyperliquid_only"
  in
  Logging.info ~section "Authentication token obtained";

  (* Store token globally for order executor reuse *)
  Token_store.set (Some auth_token);

  Logging.info ~section "Step 5: Initializing balances feed stores...";
  (* Derive unique base asset list from trading symbols *)
  let all_assets = app_configs
                  |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol)
                  |> List.map (fun symbol -> 
                      if String.contains symbol '/' then String.split_on_char '/' symbol |> List.hd
                      else symbol)
                  |> List.sort_uniq String.compare
                  |> fun assets -> "USD" :: assets in  (* Include USD as quote currency *)
  let all_assets = if has_hyperliquid then "USDC" :: all_assets else all_assets in
  let all_assets = if has_lighter then "USDC" :: all_assets else all_assets in
  let all_assets = List.sort_uniq String.compare all_assets in
  
  let () = try
    Kraken.Kraken_balances_feed.initialize all_assets;
    if has_hyperliquid then begin
      Hyperliquid.Balances.initialize ~testnet:hyperliquid_testnet all_assets;
      Lwt.async (fun () -> Hyperliquid.Module.fetch_spot_balances_ws ())
    end;
    if has_ibkr then Ibkr.Balances.initialize ();
    if has_lighter then Lighter.Balances.initialize ["USDC"; "ETH"];
    Logging.info ~section "Balances feed stores initialized";
  with exn ->
    Logging.error_f ~section "Failed to initialize balances feed stores: %s" (Printexc.to_string exn)
  in

  Logging.info ~section "Step 6: Initializing executions feed stores...";
  Kraken.Kraken_executions_feed.initialize kraken_symbols;
  if has_hyperliquid then Hyperliquid.Executions_feed.initialize hyperliquid_symbols;
  if has_ibkr then Ibkr.Executions_feed.initialize ibkr_symbols;
  if has_lighter then Lighter.Executions_feed.initialize lighter_symbols;

  (* Synchronously fetch open orders before domains start to prevent duplicate placements *)
  let%lwt () =
    if has_hyperliquid then Hyperliquid.Module.fetch_open_orders_ws ()
    else Lwt.return_unit
  in

  (* Step 7: Register and start remaining supervised WebSocket connections *)
  Logging.info ~section "Step 7: Starting Kraken websocket connections...";

  (* Kraken orderbook feed *)
  if has_kraken then begin
    let orderbook_conn = register ~name:"kraken_orderbook_ws" ~connect_fn:None in
    let orderbook_connect_fn () =
      (* Reset orderbook stores to ensure clean snapshot state *)
      Kraken.Kraken_orderbook_feed.clear_all_stores ();
      (* Exception boundary for connection establishment *)
      Lwt.catch (fun () ->
        let on_failure reason = set_state orderbook_conn (Failed reason) in
        let on_heartbeat () = update_data_heartbeat orderbook_conn in
        let on_connected () = set_state orderbook_conn Connected in
        Kraken.Kraken_orderbook_feed.connect_and_subscribe kraken_symbols ~on_failure ~on_heartbeat ~on_connected >>= fun () ->
        (* Unexpected early return from WebSocket connect_fn *)
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

    (* Unified authenticated WebSocket for trading, balances, and executions *)
    let auth_ws_conn = register ~name:"kraken_auth_ws" ~connect_fn:None in
    let auth_ws_connect_fn () =
      (* Exception boundary for connection establishment *)
      Lwt.catch (fun () ->
        let on_failure reason =
          set_state auth_ws_conn (Failed reason);
          (* Schedule immediate reconnection; bypass monitor loop backoff *)
          Lwt.async (fun () ->
            Lwt.catch (fun () ->
              Lwt.pause () >>= fun () ->  (* Cooperative yield to prevent same-turn re-entry *)
              start_async auth_ws_conn;
              Lwt.return_unit
            ) (fun exn ->
              Logging.warn_f ~section "[%s] Exception during emergency reconnection: %s" auth_ws_conn.name (Printexc.to_string exn);
              Lwt.return_unit
            )
          )
        in
        let on_heartbeat () = update_data_heartbeat auth_ws_conn in
        let on_connected () =
          set_state auth_ws_conn Connected;
          (* Subscribe balance and execution feeds on the unified connection *)
          Lwt.async (fun () ->
            Lwt.join [
              Kraken.Kraken_balances_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected:(fun () -> ());
              Kraken.Kraken_executions_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected:(fun () -> ());
            ]
          )
        in
        Kraken.Kraken_trading_client.connect_and_monitor auth_token ~on_failure ~on_connected >>= fun () ->
        (* Unexpected early return from WebSocket connect_fn *)
        Lwt.return_unit
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[%s] Connection failed during establishment: %s" auth_ws_conn.name error_msg;
        set_state auth_ws_conn (Failed error_msg);
        Lwt.return_unit
      )
    in
    set_connect_fn auth_ws_conn (Some auth_ws_connect_fn);
    start_async auth_ws_conn;
  end;

  (* IBKR Gateway TCP connection *)
  if has_ibkr then begin
    let ibkr_conn_sup = register ~name:"ibkr_gateway" ~connect_fn:None in
    (* Register feed handler hooks so they survive dispatcher reset() on
       every connect/reconnect. These closures are called from
       Ibkr.Dispatcher.initialize after core handlers are registered. *)
    Ibkr.Dispatcher.on_initialize_hooks := [
      Ibkr.Orderbook_feed.register_handlers;
      Ibkr.Executions_feed.register_handlers;
      Ibkr.Balances.register_handlers;
    ];
    let ibkr_connect_fn () =
      Lwt.catch (fun () ->
        (* Gate on US equity market hours: if the market is closed,
           sleep until the next extended-hours open instead of burning
           reconnect attempts against a gateway that will reject
           contract resolution. *)
        let%lwt () =
          if not (Ibkr.Market_hours.is_market_open ()) then begin
            let sleep_secs = Ibkr.Market_hours.seconds_until_next_open () in
            Ibkr.Market_hours.log_market_status ();
            Logging.info_f ~section "[ibkr_gateway] Sleeping %.0fs (%.1f hours) until market opens"
              sleep_secs (sleep_secs /. 3600.0);
            set_state ibkr_conn_sup (Failed "Market closed");
            Lwt_unix.sleep sleep_secs
          end else
            Lwt.return_unit
        in
        (* Clean up previous connection state to prevent leaks on reconnection.
           Old req_id mappings, handler closures, and IO channels would otherwise
           accumulate across reconnect cycles. *)
        let%lwt () = (match !(Ibkr.Module.connection) with
         | Some old_conn ->
             Logging.info ~section "[ibkr_gateway] Disconnecting old connection before reconnect";
             Ibkr.Connection.disconnect old_conn
         | None -> Lwt.return_unit) in
        Ibkr.Dispatcher.reset ();
        Ibkr.Orderbook_feed.clear_req_ids ();
        let conn = Ibkr.Connection.create
          ~host:Ibkr.Module.Config.gateway_host
          ~port:!(Ibkr.Module.Config.gateway_port)
          ~client_id:Ibkr.Module.Config.client_id
        in
        Ibkr.Module.connection := Some conn;
        Ibkr.Connection.connect_with_retry conn ~max_attempts:10 >>= fun () ->
        Ibkr.Dispatcher.initialize conn;
        (* Register callback so openOrderEnd marks execution stores as ready.
           Must be set after initialize (which clears state) and before
           request_open_orders fires — avoids dependency cycle in the lib. *)
        Ibkr.Dispatcher.on_open_orders_end := Some Ibkr.Executions_feed.mark_ready_all;
        Ibkr.Connection.start_reader conn
          ~on_message:Ibkr.Dispatcher.dispatch
          ~on_disconnect:(fun reason ->
            set_state ibkr_conn_sup (Failed reason);
            update_circuit_breaker ibkr_conn_sup false;
            Lwt.async (fun () ->
              Lwt.catch (fun () ->
                Lwt.pause () >>= fun () ->
                start_async ibkr_conn_sup;
                Lwt.return_unit
              ) (fun exn ->
                Logging.warn_f ~section "[ibkr_gateway] Reconnect exception: %s" (Printexc.to_string exn);
                Lwt.return_unit
              )
            )
          );
        (* Do NOT set Connected yet — defer until contract resolution succeeds.
           Setting Connected here would reset reconnect_attempts to 0, defeating
           the backoff and circuit breaker when contract resolution keeps failing. *)
        Logging.info ~section "IBKR Gateway TCP connected, resolving contracts...";
        update_data_heartbeat ibkr_conn_sup;
        (* Wait for nextValidId before subscribing *)
        Lwt_unix.sleep 1.0 >>= fun () ->
        (* Subscribe to account updates *)
        let account_id = match Ibkr.Module.Config.account_id with
          | Some id -> id
          | None -> Ibkr.Connection.get_account_id conn
        in
        Ibkr.Balances.subscribe conn ~account_id >>= fun () ->
        (* Request open orders snapshot *)
        Ibkr.Executions_feed.request_open_orders conn >>= fun () ->

        let is_paper = !(Ibkr.Module.Config.trading_mode) = "paper" in

        (* Phase 1: Snapshot — seed an initial price immediately.
           Paper: type 4 (delayed-frozen) — free, no live subscription needed.
           Live:  type 2 (frozen) — last close from live subscription. *)
        let snapshot_type = if is_paper then "4" else "2" in
        Logging.info_f ~section "IBKR Phase 1: Requesting %s snapshot for initial price seed"
          (if is_paper then "delayed-frozen" else "frozen");
        Ibkr.Connection.send conn [
          string_of_int Ibkr.Types.msg_req_market_data_type;
          "1";    (* version *)
          snapshot_type;
        ] >>= fun () ->
        (* Resolve contracts once; reuse for both snapshot and streaming.
           Catch contract resolution failures gracefully — IB Gateway may
           reject symbol lookups when the market data farm is disconnected.
           Return normally with Failed state instead of re-raising to avoid
           resetting backoff and circuit breaker. *)
        Lwt.catch (fun () ->
          let%lwt contracts = Lwt_list.map_s (fun symbol ->
            Ibkr.Contracts.resolve conn ~symbol >>= fun contract ->
            Lwt.return (symbol, contract)
          ) ibkr_symbols in
          let%lwt () = Lwt_list.iter_s (fun (_symbol, _contract) ->
            Lwt.return_unit
          ) contracts in
          (* Brief pause to let the gateway deliver snapshot ticks *)
          Lwt_unix.sleep 2.0 >>= fun () ->

          (* Phase 2: Streaming — ongoing market data.
             Paper: type 4 (delayed-frozen) — 15-min delayed during hours,
                    last known quote when closed. Never touches live data.
             Live:  type 1 (live) — real-time streaming. *)
          let stream_type = if is_paper then "4" else "1" in
          Logging.info_f ~section "IBKR Phase 2: Switching to %s streaming"
            (if is_paper then "delayed-frozen" else "live");
          Ibkr.Connection.send conn [
            string_of_int Ibkr.Types.msg_req_market_data_type;
            "1";    (* version *)
            stream_type;
          ] >>= fun () ->
          let%lwt () = Lwt_list.iter_s (fun (_symbol, contract) ->
            Ibkr.Orderbook_feed.subscribe conn ~contract
          ) contracts in
          Logging.info_f ~section "IBKR subscribed to %d symbols (snapshot + streaming)" (List.length ibkr_symbols);
          (* Contract resolution succeeded — NOW mark as Connected.
             This is the correct place: reconnect_attempts resets to 0,
             circuit breaker resets, and backoff is cleared. *)
          set_state ibkr_conn_sup Connected;
          update_circuit_breaker ibkr_conn_sup true;
          Logging.info ~section "✓ IBKR Gateway fully connected";
          (* Block forever — reader loop runs in background *)
          let wait_p, _wait_u = Lwt.wait () in
          wait_p
        ) (fun exn ->
          (* Contract resolution failed (e.g., error 200: no security definition).
             This typically means the IB market data farm is down or the market
             is closed. Disconnect cleanly and handle based on market hours. *)
          let error_msg = Printexc.to_string exn in
          let%lwt () = Ibkr.Connection.disconnect conn in
          Ibkr.Module.connection := None;
          if not (Ibkr.Market_hours.is_market_open ()) then begin
            (* Market is closed — don't escalate the circuit breaker.
               Schedule a deferred reconnect at the next market open. *)
            let sleep_secs = Ibkr.Market_hours.seconds_until_next_open () in
            Logging.info_f ~section "[ibkr_gateway] Contract resolution failed (market closed): %s" error_msg;
            Logging.info_f ~section "[ibkr_gateway] Sleeping %.0fs (%.1f hours) until market opens"
              sleep_secs (sleep_secs /. 3600.0);
            set_state ibkr_conn_sup (Failed "Market closed");
            (* Sleep until market open, then let the connect_fn return normally
               so start_async is triggered by the supervisor's on_disconnect handler. *)
            let%lwt () = Lwt_unix.sleep sleep_secs in
            Lwt.return_unit
          end else begin
            (* Market is open but contract resolution still failed — genuine error.
               Escalate via circuit breaker as before. *)
            Logging.error_f ~section "[ibkr_gateway] Contract resolution failed: %s" error_msg;
            update_circuit_breaker ibkr_conn_sup false;
            set_state ibkr_conn_sup (Failed error_msg);
            Lwt.return_unit
          end
        )
      ) (fun exn ->
        let error_msg = Printexc.to_string exn in
        Logging.error_f ~section "[ibkr_gateway] Connection failed: %s" error_msg;
        update_circuit_breaker ibkr_conn_sup false;
        Lwt.fail exn
      )
    in
    set_connect_fn ibkr_conn_sup (Some ibkr_connect_fn);
    start_async ibkr_conn_sup;
  end;

  (* Block until trading client WebSocket is connected to prevent
     strategies from issuing orders on a dead connection. *)
  let%lwt () = if has_kraken then begin
    Logging.info ~section "Waiting for trading client to be ready...";
    let%lwt trading_client_ready = 
      let timeout = 10.0 in
      let start_time = Unix.gettimeofday () in
      let rec wait_loop () =
        let elapsed = Unix.gettimeofday () -. start_time in
        if elapsed >= timeout then
          Lwt.return false
        else if Kraken.Kraken_trading_client.is_connected () then
          Lwt.return true
        else
          Lwt_unix.sleep 0.1 >>= fun () ->
          wait_loop ()
      in
      wait_loop ()
    in
    if not trading_client_ready then
      Logging.warn ~section "Timeout waiting for trading client connection, continuing anyway..."
    else
      Logging.info ~section "✓ Trading client connected and ready";
      
    (* Await executions feed before strategies start to avoid stale-state race *)
    Logging.info ~section "Waiting for executions feed to be ready...";
    let%lwt executions_ready = Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 in
    if not executions_ready then
      Logging.warn ~section "Timeout waiting for executions data, continuing anyway..."
    else
      Logging.info ~section "✓ Executions feed ready";
      
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Await initial data from each market data feed *)
  Logging.info ~section "Waiting for initial market data from all feeds...";



  (* Orderbook readiness gate *)
  let%lwt () = if has_kraken then begin
    let%lwt orderbook_ready = Kraken.Kraken_orderbook_feed.wait_for_orderbook_data kraken_symbols 10.0 in
    if not orderbook_ready then
      Logging.warn ~section "Timeout waiting for orderbook data, continuing anyway..."
    else
      Logging.info ~section "✓ Orderbook feed ready";
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Executions readiness gate (both exchanges) *)
  let%lwt hl_executions_ready = 
    if has_hyperliquid then Hyperliquid.Executions_feed.wait_for_execution_data all_hyperliquid_symbols 10.0
    else Lwt.return_true
  in
  let%lwt executions_ready = 
    if has_kraken then Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 
    else Lwt.return_true
  in
  if not (executions_ready && hl_executions_ready) then
    Logging.warn ~section "Timeout waiting for executions data, continuing anyway..."
  else
    Logging.info ~section "✓ Executions feed ready";

  (* Balance readiness gate — run all exchanges in parallel since
     subscriptions are already in-flight. Sequential waits would
     accumulate timeouts and delay Lighter by 10-20s unnecessarily. *)
  let%lwt balances_ready = 
    let kraken_p = 
      if has_kraken then Kraken.Kraken_balances_feed.wait_for_balance_data all_assets 10.0
      else Lwt.return_true
    in
    let hl_p = 
      if has_hyperliquid then Hyperliquid.Balances.wait_until_ready ()
      else Lwt.return_true
    in
    let lighter_p =
      if has_lighter then Lighter.Balances.wait_for_balance_data ["USDC"; "ETH"] 60.0
      else Lwt.return_true
    in
    let%lwt kraken_ready = kraken_p
    and hl_ready = hl_p
    and lighter_ready = lighter_p in
    Lwt.return (kraken_ready && hl_ready && lighter_ready)
  in
  if not balances_ready then
    Logging.warn ~section "Timeout waiting for balance data, continuing anyway..."
  else
    Logging.info ~section "✓ Balances feed ready";

  Logging.info ~section "All feeds initialized with market data!";

  (* Start Lighter TIF renewal background monitor to keep GTT orders alive *)
  if has_lighter then begin
    Logging.info ~section "Starting Lighter TIF renewal monitor...";
    Lwt.async (fun () -> Lighter.Tif_renewal.start ~symbols:lighter_symbols)
  end;

  (* Step 8: Fetch and cache trading fees per symbol *)
  Logging.info ~section "Step 8: Fetching trading fees for all assets...";

  let%lwt global_hl_fees = 
    if has_hyperliquid then begin
      Logging.info ~section "Fetching global Hyperliquid fees...";
      let%lwt fee_opt = Hyperliquid.Get_fee.get_fee_info ~testnet:hyperliquid_testnet () in
      match fee_opt with
      | Some fees -> Lwt.return_some fees
      | None ->
          Logging.error ~section "Fatal: Failed to fetch global Hyperliquid fees on startup. Exiting...";
          exit 1
    end else Lwt.return_none
  in

  (* Sequentially fetch fees per config; results enrich trading_config with fee fields *)
  let%lwt configs_with_fees = Lwt_list.map_s (fun asset ->
    try
      if asset.Dio_engine.Config.exchange = "kraken" then begin

        let%lwt fee_info_opt = Kraken.Kraken_get_fee.get_fee_info asset.Dio_engine.Config.symbol in
        let%lwt result = match fee_info_opt with
        | Some fee_info ->

            (* Populate Fee_cache for dashboard access *)
            (match fee_info.Kraken.Kraken_get_fee.maker_fee, fee_info.Kraken.Kraken_get_fee.taker_fee with
             | Some maker, Some taker ->
                 Dio_strategies.Fee_cache.store_fees 
                   ~exchange:asset.Dio_engine.Config.exchange 
                   ~symbol:asset.Dio_engine.Config.symbol 
                   ~maker_fee:maker 
                   ~taker_fee:taker 
                   ~ttl_seconds:600.0
             | Some maker, None ->
                 (* Fallback: use maker fee as taker when taker is absent *)
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
            Logging.error_f ~section "Fatal: Failed to fetch fees for %s. Exiting." asset.Dio_engine.Config.symbol;
            exit 1 in
        (* Sequential Lwt_list.map_s guarantees >10ms between HTTP requests,
           so nonce/timestamp collisions are not possible. *)
        Lwt.return result
      end else if asset.Dio_engine.Config.exchange = "hyperliquid" then begin
        let is_spot = String.contains asset.Dio_engine.Config.symbol '/' in
        let%lwt result = match global_hl_fees with
        | Some fee_info ->
            let maker = 
              if is_spot then Option.value fee_info.spot_maker_fee ~default:0.0
              else Option.value fee_info.maker_fee ~default:0.0002 
            in
            let taker = 
              if is_spot then Option.value fee_info.spot_taker_fee ~default:0.001
              else Option.value fee_info.taker_fee ~default:0.0005 
            in

            Dio_strategies.Fee_cache.store_fees ~exchange:"hyperliquid" ~symbol:asset.Dio_engine.Config.symbol ~maker_fee:maker ~taker_fee:taker ~ttl_seconds:600.0;
            Lwt.return { asset with Dio_engine.Config.maker_fee = Some maker; Dio_engine.Config.taker_fee = Some taker }
        | None ->
            Logging.error_f ~section "Fatal: No global HL fees available for %s. Exiting." asset.Dio_engine.Config.symbol;
            exit 1
        in
        Lwt.return result
      end else if asset.Dio_engine.Config.exchange = "ibkr" then begin
        (* IBKR uses fixed per-share commissions, not maker/taker %.
           US equities Fixed plan: $0.005/share all-in is a conservative estimate.
           Express as fraction of trade value for Fee_cache compatibility. *)
        let maker = 0.0005 in  (* 0.05% — conservative estimate for ETFs *)
        let taker = 0.0005 in

        Dio_strategies.Fee_cache.store_fees
          ~exchange:"ibkr"
          ~symbol:asset.Dio_engine.Config.symbol
          ~maker_fee:maker
          ~taker_fee:taker
          ~ttl_seconds:86400.0;  (* Fees don't change often for IBKR *)
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some maker;
          Dio_engine.Config.taker_fee = Some taker }
      end else if asset.Dio_engine.Config.exchange = "lighter" then begin
        (* Lighter fees are embedded in orderBookDetails and already cached
           in the instruments feed — no separate fee endpoint needed. *)
        let fees = Lighter.Instruments_feed.lookup_info asset.Dio_engine.Config.symbol in
        let maker = match fees with Some i -> i.Lighter.Types.maker_fee | None -> 0.0 in
        let taker = match fees with Some i -> i.Lighter.Types.taker_fee | None -> 0.0 in

        Dio_strategies.Fee_cache.store_fees
          ~exchange:"lighter"
          ~symbol:asset.Dio_engine.Config.symbol
          ~maker_fee:maker
          ~taker_fee:taker
          ~ttl_seconds:86400.0;
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some maker;
          Dio_engine.Config.taker_fee = Some taker }
      end else begin
        Logging.warn_f ~section "Fee fetching not implemented for exchange: %s, using defaults" asset.Dio_engine.Config.exchange;
        (* Cache default fees for unsupported exchanges *)
        Dio_strategies.Fee_cache.store_fees 
          ~exchange:asset.Dio_engine.Config.exchange 
          ~symbol:asset.Dio_engine.Config.symbol 
          ~maker_fee:0.0016 
          ~taker_fee:0.0026 
          ~ttl_seconds:600.0;
        (* Apply default fee values *)
        Lwt.return { asset with
          Dio_engine.Config.maker_fee = Some 0.0016;  (* 0.16% maker fee default *)
          Dio_engine.Config.taker_fee = Some 0.0026 } (* 0.26% taker fee default *)
      end
    with exn ->
      Logging.error_f ~section "Fatal: Exception during fee fetching for %s: %s. Exiting."
        asset.Dio_engine.Config.symbol (Printexc.to_string exn);
      exit 1
  ) app_configs in

  Lwt.return (configs_with_fees, auth_token)

(** Central order processing loop. Drains pending orders from all strategy
    ring buffers (grid, market maker, hedger) and dispatches them to the
    Order_executor via Lwt.async. Blocks on OrderSignal when idle. *)
let order_processing_loop () =
  let section = "order_processor" in
  let cycle_count = ref 0 in
  let orders_placed = Atomic.make 0 in
  let order_mutex = Mutex.create () in

  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else begin
      (* Check exchange connection liveness *)
      let kraken_connected = Kraken.Kraken_trading_client.is_connected () in
      
      let is_hyperliquid_connected =
          try
            let hl_conn = Hashtbl.find connections "hyperliquid_ws" in
            get_state hl_conn = Connected
          with Not_found -> false
      in

      let is_ibkr_connected =
          try
            let ibkr_conn = Hashtbl.find connections "ibkr_gateway" in
            get_state ibkr_conn = Connected
          with Not_found -> false
      in

      let is_lighter_connected =
          try
            let lt_conn = Hashtbl.find connections "lighter_ws" in
            get_state lt_conn = Connected
          with Not_found -> false
      in

      (* Drain ring buffers regardless of connection status to prevent backpressure *)
      let pending_grid_orders = Dio_strategies.Suicide_grid.Strategy.get_pending_orders 100 in
      let pending_mm_orders = Dio_strategies.Market_maker.Strategy.get_pending_orders 100 in
      let pending_hedge_orders = Dio_strategies.Auto_hedger.get_pending_orders 100 in

      if pending_grid_orders = [] && pending_mm_orders = [] && pending_hedge_orders = [] then
        (* No pending orders; block until signalled.
           Sever promise chain via Lwt.async to prevent Forward node accumulation. *)
        OrderSignal.wait () >>= fun () ->
        Lwt.async loop;
        Lwt.return_unit
      else begin
        incr cycle_count;
        let process_order_if_connected order process_fn reject_fn =
          let connected =
            if order.Dio_strategies.Strategy_common.exchange = "kraken" then kraken_connected
            else if order.Dio_strategies.Strategy_common.exchange = "hyperliquid" then is_hyperliquid_connected
            else if order.Dio_strategies.Strategy_common.exchange = "ibkr" then is_ibkr_connected
            else if order.Dio_strategies.Strategy_common.exchange = "lighter" then is_lighter_connected
            else true
          in
          if connected then process_fn ()
          else begin
            Logging.warn_f ~section "Exchange %s not connected, dropping order %s %s" order.Dio_strategies.Strategy_common.exchange (match order.Dio_strategies.Strategy_common.side with Buy -> "buy" | Sell -> "sell") order.Dio_strategies.Strategy_common.symbol;
            reject_fn "Exchange not connected"
          end
        in
        try
            (* Process grid strategy orders *)
            List.iter (fun order ->
              if Atomic.get shutdown_requested then () else
              Mutex.lock order_mutex;
              try
                (* Retrieve auth token *)
                let auth_token = match Token_store.get () with
                  | Some token -> token
                  | None ->
                    Logging.warn ~section "No auth token available for order operations";
                    raise (Failure "No auth token")
                in

                (* Dispatch by operation type *)
                (match order.operation with
                 | Place ->
                      process_order_if_connected order (fun () ->
                        let order_request = {
                          Dio_engine.Order_executor.order_type = order.order_type;
                          side = (match order.side with Buy -> "buy" | Sell -> "sell");
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
                          duplicate_key = order.duplicate_key;
                          exchange = order.exchange;
                        } in
                        Lwt.async (fun () ->
                          let%lwt () = Lwt.pause () in
                          Lwt.catch (fun () ->
                            Dio_engine.Order_executor.place_order ~token:auth_token ~check_duplicate:false order_request >>= function
                            | Ok result ->
                                Atomic.incr orders_placed;
                                Logging.info_f ~section "✓ Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
                                  (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                  (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                  result.order_id;
                                (match order.price with
                                 | Some price when order.exchange <> "lighter" ->
                                     Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                                       order.symbol result.order_id order.side price
                                 | Some _ -> ()
                                 | None -> ());
                                Lwt.return_unit
                            | Error err ->
                                Logging.error_f ~section "✗ Order placement failed: %s %s %.8f @ %s - %s"
                                  (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                  (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                  err;
                                Dio_strategies.Suicide_grid.Strategy.handle_order_failed order.symbol order.side err;
                                (match order.price with
                                 | Some price ->
                                     Dio_strategies.Suicide_grid.Strategy.handle_order_rejected order.symbol order.side price
                                 | None -> ());
                                Lwt.return_unit
                          ) (fun exn ->
                            Logging.error_f ~section "✗ Exception placing order %s %s: %s"
                              (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                            Dio_strategies.Suicide_grid.Strategy.handle_order_failed order.symbol order.side (Printexc.to_string exn);
                            (match order.price with
                             | Some price ->
                                 Dio_strategies.Suicide_grid.Strategy.handle_order_rejected order.symbol order.side price
                             | None -> ());
                            Lwt.return_unit
                          )
                        )
                      ) (fun err ->
                        Dio_strategies.Suicide_grid.Strategy.handle_order_failed order.symbol order.side err;
                        (match order.price with
                         | Some price ->
                             Dio_strategies.Suicide_grid.Strategy.handle_order_rejected order.symbol order.side price
                         | None -> ())
                      )

                 | Amend ->
                     process_order_if_connected order (fun () ->
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
                              exchange = order.exchange;
                            } in
  
                            (* Async dispatch; strategy state is updated by WS execution events,
                             not by the placement response. *)
                            Lwt.async (fun () ->
                               let%lwt () = Lwt.pause () in
                               Lwt.catch (fun () ->
                                 Dio_engine.Order_executor.amend_order ~token:auth_token amend_request >>= function
                                 | Ok result ->
                                     if result.Dio_exchange.Exchange_intf.Types.amend_id = Some "skipped_no_change" then begin

                                         (match order.price with
                                          | Some price ->
                                              Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_skipped
                                                order.symbol target_order_id order.side price
                                          | None -> Logging.warn_f ~section "Amendment skipped but no price available for strategy update: %s" result.Dio_exchange.Exchange_intf.Types.new_order_id
                                         );
                                         Lwt.return_unit
                                     end else begin
                                         Atomic.incr orders_placed;
                                         Logging.info_f ~section "✓ Order amended successfully: %s %s %.8f @ %s (Amend ID: %s) New Order ID: %s"
                                           (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                           (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                           (match result.Dio_exchange.Exchange_intf.Types.amend_id with Some id -> id | None -> "none")
                                           result.Dio_exchange.Exchange_intf.Types.new_order_id;
                                         (match order.price with
                                          | Some price ->
                                              Dio_strategies.Suicide_grid.Strategy.handle_order_amended
                                                order.symbol target_order_id result.Dio_exchange.Exchange_intf.Types.new_order_id order.side price
                                          | None -> Logging.warn_f ~section "Amendment acknowledged but no price available for strategy update: %s" result.Dio_exchange.Exchange_intf.Types.new_order_id
                                         );
                                         Lwt.return_unit
                                     end
                                 | Error err ->
                                     Logging.error_f ~section "✗ Order amendment failed: %s %s %.8f @ %s - %s"
                                       (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                       (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                       err;
                                     (match order.strategy with
                                      | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                                      | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                                      | Hedger -> ()
                                     );
                                     Lwt.return_unit
                               ) (fun exn ->
                                 Logging.error_f ~section "✗ Exception amending order %s %s: %s"
                                   (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                                 (match order.strategy with
                                  | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side (Printexc.to_string exn)
                                  | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side (Printexc.to_string exn)
                                  | Hedger -> ()
                                 );
                                 Lwt.return_unit
                               )
                             )
                        | None -> Logging.error_f ~section "Amendment request missing target order ID for %s %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol;
                       )
                     ) (fun err ->
                        (match order.order_id with
                         | Some target_order_id ->
                             (match order.strategy with
                              | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                              | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                              | Hedger -> ()
                             )
                         | None -> ());
                     )

                 | Cancel ->
                     process_order_if_connected order (fun () ->
                       (match order.order_id with
                        | Some target_order_id ->
                            Lwt.async (fun () ->
                              let%lwt () = Lwt.pause () in
                              Lwt.catch (fun () ->
                                let request : Dio_engine.Order_executor.cancel_request = {
                                  exchange = order.exchange;
                                  order_ids = Some [target_order_id];
                                  cl_ord_ids = None;
                                  order_userrefs = None;
                                  symbol = Some order.symbol;
                                } in
                                Dio_engine.Order_executor.cancel_orders ~token:auth_token request >>= function
                                | Ok results ->
                                    let count = List.length results in
                                    Atomic.set orders_placed (Atomic.get orders_placed + count);
                                    Logging.info_f ~section "✓ Cancelled %d order(s) successfully: %s" count target_order_id;
                                    (match order.strategy with
                                     | Grid -> Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | MM -> Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | Hedger -> ());
                                    Lwt.return_unit
                                | Error err ->
                                    Logging.error_f ~section "✗ Order cancellation failed: %s - %s" target_order_id err;
                                    (match order.strategy with
                                     | Grid -> Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | MM -> Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | Hedger -> ());
                                    Lwt.return_unit
                              ) (fun exn ->
                                Logging.error_f ~section "✗ Exception cancelling order %s: %s" target_order_id (Printexc.to_string exn);
                                (match order.strategy with
                                 | Grid -> Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                 | MM -> Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                 | Hedger -> ());
                                Lwt.return_unit
                              )
                            )
                        | None -> Logging.error_f ~section "Cancel request missing target order ID for %s" order.symbol;
                       )
                     ) (fun _err -> ())
                );
                
                (* Release mutex before Lwt.async callbacks execute *)
                Mutex.unlock order_mutex
              with exn ->
                Mutex.unlock order_mutex;
                Logging.error_f ~section "Error processing order %s %s: %s"
                  (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
            ) pending_grid_orders;

            (* Process market maker orders; abort if shutdown raised after grid batch *)
            if not (Atomic.get shutdown_requested) then
            List.iter (fun order ->
              if Atomic.get shutdown_requested then () else
              Mutex.lock order_mutex;
              try
                (* Retrieve auth token *)
                let auth_token = match Token_store.get () with
                  | Some token -> token
                  | None ->
                    Logging.warn ~section "No auth token available for order operations";
                    raise (Failure "No auth token")
                in

                (* Dispatch by operation type *)
                (match order.operation with
                 | Place ->
                      process_order_if_connected order (fun () ->
                      let order_request = {
                       Dio_engine.Order_executor.order_type = order.order_type;
                       side = (match order.side with Buy -> "buy" | Sell -> "sell");
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
                       duplicate_key = order.duplicate_key;
                       exchange = order.exchange;
                     } in

                     (* Async dispatch; strategy state is updated by WS execution events,
                      not by the placement response. *)
                     Lwt.async (fun () ->
                       let%lwt () = Lwt.pause () in
                       Lwt.catch (fun () ->
                         Dio_engine.Order_executor.place_order ~token:auth_token ~check_duplicate:false order_request >>= function
                         | Ok result ->
                             Atomic.incr orders_placed;
                             Logging.info_f ~section " Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
                               (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                               (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                               result.order_id;
                             Lwt.return_unit
                         | Error err ->
                             Logging.error_f ~section " Order placement failed: %s %s %.8f @ %s - %s"
                               (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                               (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                               err;
                             (* Notify strategy to clean up pending/in-flight state *)
                             Dio_strategies.Market_maker.Strategy.handle_order_failed order.symbol order.side err;
                             (match order.price with
                              | Some price ->
                                  Dio_strategies.Market_maker.Strategy.handle_order_rejected order.symbol order.side price
                              | None -> ());
                             Lwt.return_unit
                       ) (fun exn ->
                         Logging.error_f ~section " Exception placing order %s %s: %s"
                           (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                         (* Notify strategy to clean up pending/in-flight state *)
                         Dio_strategies.Market_maker.Strategy.handle_order_failed order.symbol order.side (Printexc.to_string exn);
                         (match order.price with
                          | Some price ->
                              Dio_strategies.Market_maker.Strategy.handle_order_rejected order.symbol order.side price
                          | None -> ());
                         Lwt.return_unit
                       )
                     )
                     ) (fun err ->
                        Dio_strategies.Market_maker.Strategy.handle_order_failed order.symbol order.side err;
                        (match order.price with
                         | Some price ->
                             Dio_strategies.Market_maker.Strategy.handle_order_rejected order.symbol order.side price
                         | None -> ())
                      )

                 | Amend ->
                     process_order_if_connected order (fun () ->
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
                              exchange = order.exchange;
                            } in
  
                            Lwt.async (fun () ->
                              let%lwt () = Lwt.pause () in
                              Lwt.catch (fun () ->
                                Dio_engine.Order_executor.amend_order ~token:auth_token amend_request >>= function
                                | Ok result ->
                                    if result.amend_id = Some "skipped_no_change" then begin

                                        (match order.price with
                                         | Some price ->
                                             (match order.strategy with
                                              | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_skipped order.symbol target_order_id order.side price
                                              | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_skipped order.symbol target_order_id order.side price
                                              | Hedger -> ()
                                             )
                                         | None -> Logging.warn_f ~section "Amendment skipped but no price available for strategy update: %s" result.new_order_id
                                        );
                                        Lwt.return_unit
                                    end else begin
                                        Atomic.incr orders_placed;
                                        Logging.info_f ~section "✓ Order amended successfully: %s %s %.8f @ %s (Amend ID: %s) New Order: %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market") (match result.amend_id with Some id -> id | None -> "none") result.new_order_id;
                                        (match order.price with
                                         | Some price ->
                                             (match order.strategy with
                                              | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amended order.symbol target_order_id result.new_order_id order.side price
                                              | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amended order.symbol target_order_id result.new_order_id order.side price
                                              | Hedger -> ()
                                             )
                                         | None -> Logging.warn_f ~section "Amendment acknowledged but no price available for strategy update: %s" result.new_order_id
                                        );
                                        Lwt.return_unit
                                    end
                                | Error err ->
                                    Logging.error_f ~section "✗ Order amendment failed: %s %s %.8f @ %s - %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market") err;
                                    (match order.strategy with
                                     | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                                     | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                                     | Hedger -> ()
                                    );
                                    Lwt.return_unit
                              ) (fun exn ->
                                Logging.error_f ~section "✗ Exception amending order %s %s: %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                                (match order.strategy with
                                 | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side (Printexc.to_string exn)
                                 | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side (Printexc.to_string exn)
                                 | Hedger -> ()
                                );
                                Lwt.return_unit
                              )
                            )
                        | None -> Logging.error_f ~section "Amendment request missing target order ID for %s %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol;
                       )
                     ) (fun err ->
                        (match order.order_id with
                         | Some target_order_id ->
                             (match order.strategy with
                              | Grid -> Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                              | MM -> Dio_strategies.Market_maker.Strategy.handle_order_amendment_failed order.symbol target_order_id order.side err
                              | Hedger -> ()
                             )
                         | None -> ());
                     )

                 | Cancel ->
                     process_order_if_connected order (fun () ->
                       (match order.order_id with
                        | Some target_order_id ->
                            Lwt.async (fun () ->
                              let%lwt () = Lwt.pause () in
                              Lwt.catch (fun () ->
                                let request : Dio_engine.Order_executor.cancel_request = { exchange = order.exchange; order_ids = Some [target_order_id]; cl_ord_ids = None; order_userrefs = None; symbol = Some order.symbol; } in
                                Dio_engine.Order_executor.cancel_orders ~token:auth_token request >>= function
                                | Ok results ->
                                    let count = List.length results in
                                    Atomic.set orders_placed (Atomic.get orders_placed + count);
                                    Logging.info_f ~section "✓ Cancelled %d order(s) successfully: %s" count target_order_id;
                                    (match order.strategy with
                                     | Grid -> Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | MM -> Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | Hedger -> ());
                                    Lwt.return_unit
                                | Error err ->
                                    Logging.error_f ~section "✗ Order cancellation failed: %s - %s" target_order_id err;
                                    (match order.strategy with
                                     | Grid -> Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | MM -> Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                     | Hedger -> ());
                                    Lwt.return_unit
                              ) (fun exn ->
                                Logging.error_f ~section "✗ Exception cancelling order %s: %s" target_order_id (Printexc.to_string exn);
                                (match order.strategy with
                                 | Grid -> Dio_strategies.Suicide_grid.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                 | MM -> Dio_strategies.Market_maker.Strategy.cleanup_pending_cancellation order.symbol target_order_id
                                 | Hedger -> ());
                                Lwt.return_unit
                              )
                            )
                        | None -> Logging.error_f ~section "Cancel request missing target order ID for %s" order.symbol;
                       )
                     ) (fun _err -> ())
                );
                
                (* Release mutex before Lwt.async callbacks execute *)
                Mutex.unlock order_mutex
              with exn ->
                Mutex.unlock order_mutex;
                Logging.error_f ~section "Error processing order %s %s: %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
            ) pending_mm_orders;

            (* Process hedger orders; abort if shutdown raised after MM batch *)
            if not (Atomic.get shutdown_requested) then
            List.iter (fun order ->
              if Atomic.get shutdown_requested then () else
              Mutex.lock order_mutex;
              try
                let auth_token = match Token_store.get () with
                  | Some token -> token
                  | None ->
                    Logging.warn ~section "No auth token available for hedge order operations";
                    raise (Failure "No auth token")
                in

                (match order.operation with
                 | Place ->
                     process_order_if_connected order (fun () ->
                       let order_request = {
                         Dio_engine.Order_executor.order_type = order.order_type;
                         side = (match order.side with Buy -> "buy" | Sell -> "sell");
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
                         duplicate_key = order.duplicate_key;
                         exchange = order.exchange;
                       } in
  
                       Lwt.async (fun () ->
                         let%lwt () = Lwt.pause () in
                         Lwt.catch (fun () ->
                           Dio_engine.Order_executor.place_order ~token:auth_token ~check_duplicate:false order_request >>= function
                           | Ok result ->
                               Atomic.incr orders_placed;
                               Logging.info_f ~section "✓ Hedger order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
                                 (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                 (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                 result.order_id;
                               Lwt.return_unit
                           | Error err ->
                               Logging.error_f ~section "✗ Hedger order placement failed: %s %s %.8f @ %s - %s"
                                 (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                 (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                 err;
                               Lwt.return_unit
                         ) (fun exn ->
                           Logging.error_f ~section "✗ Exception placing hedger order %s %s: %s"
                             (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                           Lwt.return_unit
                         )
                       )
                     ) (fun err ->
                       Logging.warn_f ~section "Hedger order rejected: %s %s - %s"
                         (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol err
                     )
                 | _ -> Logging.warn_f ~section "Auto hedger only supports Place operations, got other for %s" order.symbol
                );
                
                Mutex.unlock order_mutex
              with exn ->
                Mutex.unlock order_mutex;
                Logging.error_f ~section "Error processing hedge order %s %s: %s"
                  (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
            ) pending_hedge_orders;


            (* Sever promise chain before next drain cycle. *)
            Lwt.async loop;
            Lwt.return_unit

          with exn ->
            Logging.error_f ~section "Exception in order processing loop: %s" (Printexc.to_string exn);
            Lwt.async loop;
            Lwt.return_unit
        end
      end
  in
  Lwt.async loop

(** Periodically scans all exchanges for non-configured assets that have
    a positive balance and subscribes their orderbook feeds. Runs every 10s.
    Enables portfolio valuation for assets that are held but not actively traded. *)
let monitor_non_active_assets () =
  let subscribed_symbols = Hashtbl.create 16 in
  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else
      (* 10s polling interval *)
      Lwt_unix.sleep 10.0 >>= fun () ->
      if Atomic.get shutdown_requested then Lwt.return_unit else begin
        let config = Dio_engine.Config.read_config () in

        let configured_symbols = List.map (fun tc -> 
          (tc.Dio_engine.Config.exchange, tc.symbol)
        ) config.trading in

        let exchange_names = List.sort_uniq String.compare
          (List.map (fun (tc : Dio_engine.Config.trading_config) -> tc.exchange) config.trading) in
          
        Lwt_list.iter_s (fun exch_name ->
          match Dio_exchange.Exchange_intf.Registry.get exch_name with
          | None -> Lwt.return_unit
          | Some (module Ex) ->
              let balances = Ex.get_all_balances () in
              let symbols_to_subscribe = ref [] in
              List.iter (fun (asset, _bal) ->
                let quote = match exch_name with
                  | "hyperliquid" | "lighter" -> "USDC"
                  | _ -> "USD"
                in
                let symbol = asset ^ "/" ^ quote in
                let is_configured = List.exists (fun (ex, sym) ->
                  ex = exch_name && sym = symbol
                ) configured_symbols in
                let is_quote = (asset = "USD") || (asset = "USDC") || (asset = "ZUSD") || (asset = "USDT") || (asset = quote) || (asset = "USDe") in
                
                if not is_configured && not is_quote then begin
                  let target_key = exch_name ^ ":" ^ symbol in
                  if not (Hashtbl.mem subscribed_symbols target_key) then begin
                    Hashtbl.add subscribed_symbols target_key true;
                    symbols_to_subscribe := symbol :: !symbols_to_subscribe
                  end
                end
              ) balances;
              if !symbols_to_subscribe <> [] then begin
                Logging.info_f ~section "Dynamically subscribing non-active assets on %s: %s" 
                  exch_name (String.concat ", " !symbols_to_subscribe);
                Ex.subscribe_orderbook ~symbols:!symbols_to_subscribe
              end else
                Lwt.return_unit
        ) exchange_names >>= fun () ->
        (* Sever promise chain to prevent Forward node accumulation. *)
        Lwt.async loop;
        Lwt.return_unit
      end
  in
  Lwt.async loop

(** Entry point: starts the monitor loop, non-active asset monitor, and
    order processing loop, then runs [initialize_feeds] synchronously.
    Returns enriched trading configs with fee data. *)
let start_monitoring () =
  Logging.info ~section "Starting connection supervisor";

  (* Launch health monitor on tick event bus *)
  monitor_loop ();

  (* Launch non-active asset ticker subscription loop *)
  monitor_non_active_assets ();

  (* Launch order processing loop *)
  order_processing_loop ();

  (* Run feed initialization synchronously via Lwt_main.run *)
  let (configs_with_fees, _auth_token) = Lwt_main.run (initialize_feeds ()) in

  configs_with_fees

(** Initializes the Order_executor module. Retrieves the stored auth
    token or generates a fresh one if absent. *)
let start_order_executor () : unit Lwt.t =

  (* Retrieve or regenerate auth token *)
  let _auth_token = match Token_store.get () with
    | Some token -> token
    | None ->
        Logging.warn ~section "No stored auth token found, generating new one";
        let token = Lwt_main.run (Kraken.Kraken_generate_auth_token.get_token ()) in
        Token_store.set (Some token);
        token
  in

  Dio_engine.Order_executor.init

(** Looks up a connection by name. Returns None if unregistered. *)
let get_connection_opt name =
  Mutex.lock registry_mutex;
  let conn = Hashtbl.find_opt connections name in
  Mutex.unlock registry_mutex;
  conn

(** Looks up a connection by name. Raises [Failure] if unregistered. *)
let get_connection name =
  match get_connection_opt name with
  | Some c -> c
  | None -> failwith (Printf.sprintf "Connection '%s' not found" name)

(** Returns a snapshot list of all registered connections. *)
let get_all_connections () =
  Mutex.lock registry_mutex;
  let conns = Hashtbl.to_seq_values connections |> List.of_seq in
  Mutex.unlock registry_mutex;
  conns

(** Sets the shutdown flag and broadcasts the condition variable
    to interrupt any sleeping threads. *)
let stop_order_processing () =
  Atomic.set shutdown_requested true;
  (* Signal all waiters on the shutdown condition *)
  Mutex.lock shutdown_mutex;
  Condition.broadcast shutdown_cond;
  Mutex.unlock shutdown_mutex;
  Logging.info ~section "Order processing loop shutdown requested"

(** Stops order processing and transitions all registered connections
    to Disconnected. Includes a 500ms drain window for in-flight orders. *)
let stop_all () =
  stop_order_processing ();
  interruptible_sleep 0.5;  (* Drain window for in-flight order iterations *)
  Logging.warn ~section "Stopping all supervised connections";
  Mutex.lock registry_mutex;
  Hashtbl.iter (fun _name conn ->
    set_state conn Disconnected
  ) connections;
  Mutex.unlock registry_mutex

