
(** Connection Supervisor - Monitors and manages WebSocket connections *)

open Lwt.Infix

let section = "supervisor"

(** Open Strategy_common for shared order types *)
open Dio_strategies.Strategy_common


(** Use canonical types and connection registry from Supervisor_types *)
open Supervisor_types


(** Shutdown flag for graceful termination *)
let shutdown_requested = Atomic.make false
let shutdown_mutex = Mutex.create ()
let shutdown_cond = Condition.create ()

(** Timestamp of the last Supervisor_cache.force_update() call.
    Used to rate-limit cache refreshes so that rapid reconnect bursts
    do not trigger dozens of snapshot allocations per second. *)
let last_supervisor_cache_update : float Atomic.t = Atomic.make 0.0

(** Sleep for a specified duration, but wake up immediately if shutdown is requested *)
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
      Logging.info_f ~section "[%s] Connection established (total: %d)"
        conn.name conn.total_connections
  | Disconnected ->
      conn.last_disconnected <- Some (Unix.time ());
      conn.last_connecting <- None;  (* Clear connecting timestamp *)
      Logging.warn_f ~section "[%s] Connection lost" conn.name
  | Connecting ->
      conn.last_connecting <- Some (Unix.time ());  (* Record when we started connecting *)
      Logging.info_f ~section "[%s] Attempting connection (attempt #%d)"
        conn.name (conn.reconnect_attempts + 1)
  | Failed reason ->
      conn.last_disconnected <- Some (Unix.time ());
      conn.last_connecting <- None;  (* Clear connecting timestamp *)
      conn.reconnect_attempts <- conn.reconnect_attempts + 1;
      Logging.error_f ~section "[%s] Connection failed: %s (attempt #%d)"
        conn.name reason conn.reconnect_attempts);
  
  Mutex.unlock conn.mutex;
  
  (* Log state transitions *)
  if old_state <> new_state then begin
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
       | Failed _ -> "Failed");
       
    (* Propagate changes to the supervisor cache, but rate-limit to at most once
       per second to avoid allocation bursts during rapid reconnect cycles.
       The dashboard state_broadcaster picks up any interim delta at its next
       500ms tick regardless. *)
    let now = Unix.gettimeofday () in
    let last = Atomic.get last_supervisor_cache_update in
    if now -. last >= 1.0 then begin
      Atomic.set last_supervisor_cache_update now;
      Supervisor_cache.force_update ()
    end
  end

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
              set_state conn (Failed "connection completed unexpectedly");
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
  let subscription = Concurrency.Tick_event_bus.subscribe_ticks () in
  let last_check_time = ref 0.0 in
  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else
      Lwt_stream.get subscription.Concurrency.Tick_event_bus.stream >>= function
      | None -> Lwt.return_unit
      | Some () ->
          if Atomic.get shutdown_requested then Lwt.return_unit else begin
            let current_time = Unix.time () in
            (* Throttle full connection checks to maximum once per second to avoid excessive CPU on high tick rates *)
            if current_time -. !last_check_time < 1.0 then loop ()
            else begin
              last_check_time := current_time;
              try
                incr cycle_count;

                Mutex.lock registry_mutex;
                let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
                Mutex.unlock registry_mutex;
              
                (* Check each connection and trigger auto-restart if needed *)
                List.iter (fun conn ->
                  if Atomic.get shutdown_requested then () else
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
                          | Some t -> current_time -. t >= delay
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
                        | Some t -> current_time -. t >= 60.0
                        | None -> false
                      in

                      if should_reconnect then begin
                        Logging.warn_f ~section "[%s] Connection disconnected for >60s, restarting..." conn.name;
                        start_async conn
                      end
                  | Connecting, _ ->
                      (* Already connecting, check for stuck connections *)
                      let stuck_time = match last_connecting with
                        | Some t -> current_time -. t
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
                      (* For authenticated connections, use active ping/pong monitoring *)
                      if String.equal conn.name "kraken_auth_ws" || String.equal conn.name "hyperliquid_ws" then begin
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
                            if String.equal conn.name "kraken_auth_ws" then
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
                                    Lwt.return_unit
                                  end
                                )
                                (fun exn ->
                                  Logging.warn_f ~section "[%s] Ping exception: %s" conn.name (Printexc.to_string exn);
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit
                                )
                            else (* hyperliquid_ws *)
                              Lwt.catch
                                (fun () ->
                                  Hyperliquid.Ws.send_ping ~req_id ~timeout_ms:5000 >>= fun success ->
                                  if success then begin
                                    Logging.debug_f ~section "[%s] Ping successful (req_id: %d)" conn.name req_id;
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
                loop ()
              with exn ->
                Logging.error_f ~section "Exception in monitor loop: %s" (Printexc.to_string exn);
                Logging.error_f ~section "Monitor loop continuing after exception...";
                loop ()
            end
          end
  in
  Lwt.async (fun () -> loop ())

(** Initialize all websocket feeds and connections *)
let initialize_feeds () : ((Dio_engine.Config.trading_config list * string) Lwt.t) =
  Logging.info ~section "Initializing websocket feeds...";

  (* Get trading configurations *)
  let config = Dio_engine.Config.read_config () in
  let app_configs = config.trading in
  Logging.info_f ~section "Loaded %d trading configuration(s)" (List.length app_configs);

  (* Extract Kraken symbols for websocket connections *)
  let kraken_symbols = app_configs
                      |> List.filter (fun cfg -> cfg.Dio_engine.Config.exchange = "kraken")
                      |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol) in

  (* Extract Hyperliquid symbols (including base asset for spot pairs to enable perp hedging prices) *)
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

  (* Configure the Hyperliquid module with the testnet flag from config *)
  if has_hyperliquid then
    Hyperliquid.Module.Hyperliquid_impl.set_testnet hyperliquid_testnet;

  Logging.info_f ~section "Connecting to %d Kraken websockets..." (List.length kraken_symbols);
  if has_hyperliquid then
    Logging.info_f ~section "Connecting to %d Hyperliquid websockets..." (List.length hyperliquid_symbols);

  (* Start initialization sequence as a promise chain *)

  let all_hyperliquid_symbols = hyperliquid_symbols |> List.sort_uniq String.compare in

  (* Initialize data stores for websocket feeds *)
  Logging.info ~section "Step 1: Initializing ticker feed stores...";
  Kraken.Kraken_ticker_feed.initialize kraken_symbols;
  if has_hyperliquid then Hyperliquid.Ticker_feed.initialize all_hyperliquid_symbols;

  Logging.info ~section "Step 1.5: Starting Hyperliquid websocket connection early...";
  if has_hyperliquid then begin
     let hl_ws_conn = register ~name:"hyperliquid_ws" ~connect_fn:None in
     let hl_ws_connect_fn () =
       Lwt.catch (fun () ->
         let on_failure reason =
           set_state hl_ws_conn (Failed reason);
           (* Immediately trigger reconnection attempt — mirrors Kraken auth WS pattern
              to avoid waiting for monitor loop's backoff cycle (2s+). *)
           Lwt.async (fun () ->
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
             (* Clear stale open orders and immediately re-fetch actual state
                from exchange. Done sequentially to minimize the window where
                the strategy could see 0 orders and spam new placements. *)
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

  Logging.info ~section "Step 2: Initializing instruments feed stores...";
  let%lwt () = Kraken.Kraken_instruments_feed.initialize_symbols kraken_symbols in
  let%lwt () = 
    if has_hyperliquid then Hyperliquid.Module.initialize_instruments_ws ()
    else Lwt.return_unit
  in

  Logging.info ~section "Step 3: Initializing orderbook feed stores...";
  let%lwt () = Kraken.Kraken_orderbook_feed.initialize kraken_symbols in
  if has_hyperliquid then Hyperliquid.Orderbook_feed.initialize all_hyperliquid_symbols;

  Logging.info ~section "Step 4: Getting authentication token...";
  let%lwt auth_token = 
    if has_kraken then Kraken.Kraken_generate_auth_token.get_token ()
    else Lwt.return "temp_token_for_hyperliquid_only"
  in
  Logging.info ~section "Authentication token obtained";

  (* Store token globally for reuse by order executor *)
  Token_store.set (Some auth_token);

  Logging.info ~section "Step 5: Initializing balances feed stores...";
  (* Extract all unique assets from trading symbols *)
  let all_assets = app_configs
                  |> List.map (fun cfg -> cfg.Dio_engine.Config.symbol)
                  |> List.map (fun symbol -> 
                      if String.contains symbol '/' then String.split_on_char '/' symbol |> List.hd
                      else symbol (* For Hyperliquid which might not use / in config *))
                  |> List.sort_uniq String.compare
                  |> fun assets -> "USD" :: assets in  (* Add USD as quote currency *)
  let all_assets = if has_hyperliquid then "USDC" :: all_assets else all_assets in
  let all_assets = List.sort_uniq String.compare all_assets in
  
  let () = try
    Kraken.Kraken_balances_feed.initialize all_assets;
    if has_hyperliquid then begin
      Hyperliquid.Balances.initialize ~testnet:hyperliquid_testnet all_assets;
      Lwt.async (fun () -> Hyperliquid.Module.fetch_spot_balances_ws ())
    end;
    Logging.info ~section "Balances feed stores initialized";
  with exn ->
    Logging.error_f ~section "Failed to initialize balances feed stores: %s" (Printexc.to_string exn)
  in

  Logging.info ~section "Step 6: Initializing executions feed stores...";
  Kraken.Kraken_executions_feed.initialize kraken_symbols;
  if has_hyperliquid then Hyperliquid.Executions_feed.initialize hyperliquid_symbols;

  (* Fetch existing open orders SYNCHRONOUSLY before domains start to prevent duplicates *)
  let%lwt () =
    if has_hyperliquid then Hyperliquid.Module.fetch_open_orders_ws ()
    else Lwt.return_unit
  in

  (* Register and start remaining supervised websocket connections *)
  Logging.info ~section "Step 7: Starting Kraken websocket connections...";

  (* Ticker feed *)
  if has_kraken then begin
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

    (* Single unified authenticated WebSocket connection for trading, balances, and executions *)
    let auth_ws_conn = register ~name:"kraken_auth_ws" ~connect_fn:None in
    let auth_ws_connect_fn () =
      (* Wrap the connection in try-catch for proper error handling *)
      Lwt.catch (fun () ->
        let on_failure reason =
          set_state auth_ws_conn (Failed reason);
          (* Immediately trigger reconnection attempt to avoid waiting for monitor loop *)
          Lwt.async (fun () ->
            Lwt.catch (fun () ->
              Lwt.pause () >>= fun () ->  (* Cooperative yield to avoid same-turn re-entry *)
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
          (* Trigger individual feed subscriptions on the unified connection *)
          Lwt.async (fun () ->
            Lwt.join [
              Kraken.Kraken_balances_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected:(fun () -> ());
              Kraken.Kraken_executions_feed.connect_and_subscribe auth_token ~on_failure ~on_heartbeat ~on_connected:(fun () -> ());
            ]
          )
        in
        Kraken.Kraken_trading_client.connect_and_monitor auth_token ~on_failure ~on_connected >>= fun () ->
        (* Connection function completed - this shouldn't happen for WebSocket connections *)
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

  (* Wait for trading client connection to be fully established before proceeding *)
  (* This prevents a race condition where domains start trading before the WebSocket is ready *)
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
      
    (* Wait for executions data FIRST to avoid race condition *)
    Logging.info ~section "Waiting for executions feed to be ready...";
    let%lwt executions_ready = Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 in
    if not executions_ready then
      Logging.debug ~section "Timeout waiting for executions data, continuing anyway..."
    else
      Logging.info ~section "✓ Executions feed ready";
      
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Wait for initial market data from all feeds *)
  Logging.info ~section "Waiting for initial market data from all feeds...";

  (* Wait for ticker data *)
  let%lwt () = if has_kraken then begin
    let%lwt ticker_ready = Kraken.Kraken_ticker_feed.wait_for_price_data kraken_symbols 10.0 in
    if not ticker_ready then
      Logging.warn ~section "Timeout waiting for ticker data, continuing anyway..."
    else
      Logging.info ~section "✓ Ticker feed ready";
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Wait for orderbook data *)
  let%lwt () = if has_kraken then begin
    let%lwt orderbook_ready = Kraken.Kraken_orderbook_feed.wait_for_orderbook_data kraken_symbols 10.0 in
    if not orderbook_ready then
      Logging.warn ~section "Timeout waiting for orderbook data, continuing anyway..."
    else
      Logging.info ~section "✓ Orderbook feed ready";
    Lwt.return_unit
  end else Lwt.return_unit in

  (* Wait for executions data again *)
  let%lwt hl_executions_ready = 
    if has_hyperliquid then Hyperliquid.Executions_feed.wait_for_execution_data all_hyperliquid_symbols 10.0
    else Lwt.return_true
  in
  let%lwt executions_ready = 
    if has_kraken then Kraken.Kraken_executions_feed.wait_for_execution_data kraken_symbols 10.0 
    else Lwt.return_true
  in
  if not (executions_ready && hl_executions_ready) then
    Logging.debug ~section "Timeout waiting for executions data, continuing anyway..."
  else
    Logging.info ~section "✓ Executions feed ready";

  (* Wait for balance data *)
  let%lwt balances_ready = 
    let%lwt kraken_ready = 
      if has_kraken then Kraken.Kraken_balances_feed.wait_for_balance_data all_assets 10.0
      else Lwt.return_true
    in
    let%lwt hl_ready = 
      if has_hyperliquid then Hyperliquid.Balances.wait_until_ready ()
      else Lwt.return_true
    in
    Lwt.return (kraken_ready && hl_ready)
  in
  if not balances_ready then
    Logging.warn ~section "Timeout waiting for balance data, continuing anyway..."
  else
    Logging.info ~section "✓ Balances feed ready";

  Logging.info ~section "All feeds initialized with market data!";

  (* Fetch fees for all assets *)
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
            Logging.error_f ~section "Fatal: Failed to fetch fees for %s. Exiting." asset.Dio_engine.Config.symbol;
            exit 1 in
        (* No nonce sleep needed: requests are sequential (Lwt_list.map_s) and
           each HTTP round-trip takes >10ms, guaranteeing unique timestamps. *)
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
            Logging.debug_f ~section "Applying fees for Hyperliquid %s (spot=%b): maker=%.4f%% taker=%.4f%%" asset.Dio_engine.Config.symbol is_spot (maker *. 100.) (taker *. 100.);
            Dio_strategies.Fee_cache.store_fees ~exchange:"hyperliquid" ~symbol:asset.Dio_engine.Config.symbol ~maker_fee:maker ~taker_fee:taker ~ttl_seconds:600.0;
            Lwt.return { asset with Dio_engine.Config.maker_fee = Some maker; Dio_engine.Config.taker_fee = Some taker }
        | None ->
            Logging.error_f ~section "Fatal: No global HL fees available for %s. Exiting." asset.Dio_engine.Config.symbol;
            exit 1
        in
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
      Logging.error_f ~section "Fatal: Exception during fee fetching for %s: %s. Exiting."
        asset.Dio_engine.Config.symbol (Printexc.to_string exn);
      exit 1
  ) app_configs in

  Lwt.return (configs_with_fees, auth_token)

(** Order processing loop - consumes orders from strategy ring buffers *)
let order_processing_loop () =
  let section = "order_processor" in
  let cycle_count = ref 0 in
  let orders_placed = ref 0 in
  let order_mutex = Mutex.create () in

  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else begin
      (* Determine exchange connection statuses *)
      let kraken_connected = Kraken.Kraken_trading_client.is_connected () in
      
      let is_hyperliquid_connected =
          try
            let hl_conn = Hashtbl.find connections "hyperliquid_ws" in
            get_state hl_conn = Connected
          with Not_found -> false
      in

      (* Fetch pending orders regardless of connection status so we don't back up the ringbuffer *)
      let pending_grid_orders = Dio_strategies.Suicide_grid.Strategy.get_pending_orders 100 in
      let pending_mm_orders = Dio_strategies.Market_maker.Strategy.get_pending_orders 100 in
      let pending_hedge_orders = Dio_strategies.Auto_hedger.get_pending_orders 100 in

      if pending_grid_orders = [] && pending_mm_orders = [] && pending_hedge_orders = [] then
        (* No orders to process, wait for a signal *)
        OrderSignal.wait () >>= loop
      else begin
        incr cycle_count;
        let process_order_if_connected order process_fn reject_fn =
          let connected =
            if order.Dio_strategies.Strategy_common.exchange = "kraken" then kraken_connected
            else if order.Dio_strategies.Strategy_common.exchange = "hyperliquid" then is_hyperliquid_connected
            else true
          in
          if connected then process_fn ()
          else begin
            Logging.warn_f ~section "Exchange %s not connected, dropping order %s %s" order.Dio_strategies.Strategy_common.exchange (match order.Dio_strategies.Strategy_common.side with Buy -> "buy" | Sell -> "sell") order.Dio_strategies.Strategy_common.symbol;
            reject_fn "Exchange not connected"
          end
        in
        try
            (* Process suicide grid orders *)
            List.iter (fun order ->
              if Atomic.get shutdown_requested then () else
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
                                orders_placed := !orders_placed + 1;
                                Logging.info_f ~section "✓ Order placed successfully: %s %s %.8f @ %s (Order ID: %s)"
                                  (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty
                                  (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market")
                                  result.order_id;
                                (match order.price with
                                 | Some price ->
                                     Dio_strategies.Suicide_grid.Strategy.handle_order_acknowledged
                                       order.symbol result.order_id order.side price
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
  
                            (* Fire-and-forget: just send the amendment, don't touch strategy tracking.
                             The consumer thread handles all tracking via WS execution events. *)
                            Lwt.async (fun () ->
                               let%lwt () = Lwt.pause () in
                               Lwt.catch (fun () ->
                                 Dio_engine.Order_executor.amend_order ~token:auth_token amend_request >>= function
                                 | Ok result ->
                                     if result.Dio_exchange.Exchange_intf.Types.amend_id = Some "skipped_no_change" then begin
                                         Logging.debug_f ~section "Amendment skipped for %s %s %.8f @ %s (no price change)" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");
                                         (match order.price with
                                          | Some price ->
                                              Dio_strategies.Suicide_grid.Strategy.handle_order_amendment_skipped
                                                order.symbol target_order_id order.side price
                                          | None -> Logging.warn_f ~section "Amendment skipped but no price available for strategy update: %s" result.Dio_exchange.Exchange_intf.Types.new_order_id
                                         );
                                         Lwt.return_unit
                                     end else begin
                                         orders_placed := !orders_placed + 1;
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
                                    orders_placed := !orders_placed + count;
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
                
                (* Unlock order_mutex immediately after initiating Lwt.async requests so callbacks can run *)
                Mutex.unlock order_mutex
              with exn ->
                Mutex.unlock order_mutex;
                Logging.error_f ~section "Error processing order %s %s: %s"
                  (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
            ) pending_grid_orders;

            (* Process market maker orders — skip if shutdown requested after grid batch *)
            if not (Atomic.get shutdown_requested) then
            List.iter (fun order ->
              if Atomic.get shutdown_requested then () else
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

                     (* Fire-and-forget: just send the order, don't touch strategy tracking.
                      The consumer thread handles all tracking via WS execution events. *)
                     Lwt.async (fun () ->
                       let%lwt () = Lwt.pause () in
                       Lwt.catch (fun () ->
                         Dio_engine.Order_executor.place_order ~token:auth_token ~check_duplicate:false order_request >>= function
                         | Ok result ->
                             orders_placed := !orders_placed + 1;
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
                             (* Notify strategy so it cleans up pending/in-flight state *)
                             Dio_strategies.Market_maker.Strategy.handle_order_failed order.symbol order.side err;
                             (match order.price with
                              | Some price ->
                                  Dio_strategies.Market_maker.Strategy.handle_order_rejected order.symbol order.side price
                              | None -> ());
                             Lwt.return_unit
                       ) (fun exn ->
                         Logging.error_f ~section " Exception placing order %s %s: %s"
                           (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn);
                         (* Notify strategy so it cleans up pending/in-flight state *)
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
                                        Logging.debug_f ~section "Amendment skipped for %s %s %.8f @ %s (no price change)" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol order.qty (match order.price with Some p -> Printf.sprintf "%.2f" p | None -> "market");
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
                                        orders_placed := !orders_placed + 1;
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
                                    orders_placed := !orders_placed + count;
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
                
                (* Unlock order_mutex immediately after initiating Lwt.async requests so callbacks can run *)
                Mutex.unlock order_mutex
              with exn ->
                Mutex.unlock order_mutex;
                Logging.error_f ~section "Error processing order %s %s: %s" (match order.side with Buy -> "buy" | Sell -> "sell") order.symbol (Printexc.to_string exn)
            ) pending_mm_orders;

            (* Process auto hedger orders — skip if shutdown requested after MM batch *)
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
                               orders_placed := !orders_placed + 1;
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

            if !cycle_count mod 100 = 0 then
              Logging.debug_f ~section "Order processing: %d orders placed, %d grid + %d mm + %d hedge pending in current batch"
                !orders_placed (List.length pending_grid_orders) (List.length pending_mm_orders) (List.length pending_hedge_orders);

            (* Small pause to allow other tasks to run before checking again *)
            Lwt.pause () >>= loop

          with exn ->
            Logging.error_f ~section "Exception in order processing loop: %s" (Printexc.to_string exn);
            Lwt.pause () >>= loop
        end
      end
  in
  Lwt.async loop

(** Periodically check for non-active assets with balances and subscribe their tickers *)
let monitor_non_active_assets () =
  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else
      (* Yield and wait 10 seconds between checks *)
      Lwt_unix.sleep 10.0 >>= fun () ->
      if Atomic.get shutdown_requested then Lwt.return_unit else begin
        let config = Dio_engine.Config.read_config () in
        let configured_symbols = List.map (fun (tc : Dio_engine.Config.trading_config) ->
          (tc.exchange, tc.symbol)
        ) config.trading in
        
        let exchange_names = List.sort_uniq String.compare
          (List.map (fun (tc : Dio_engine.Config.trading_config) -> tc.exchange) config.trading) in
          
        Lwt_list.iter_s (fun exch_name ->
          match Dio_exchange.Exchange_intf.Registry.get exch_name with
          | None -> Lwt.return_unit
          | Some (module Ex) ->
              let balances = Ex.get_all_balances () in
              Lwt_list.iter_s (fun (asset, bal) ->
                if bal > 0.0 then begin
                  let quote = match exch_name with
                    | "hyperliquid" -> "USDC"
                    | _ -> "USD"
                  in
                  let symbol = asset ^ "/" ^ quote in
                  let is_configured = List.exists (fun (ex, sym) ->
                    ex = exch_name && sym = symbol
                  ) configured_symbols in
                  
                  let is_fiat_or_stable = 
                    (asset = "USD") || (asset = "USDC") || (asset = "ZUSD") || 
                    (asset = "USDT") || (asset = quote) || (asset = "USDe")
                  in
                  
                  if not is_configured && not is_fiat_or_stable then begin
                    match Ex.get_ticker ~symbol with
                    | None ->
                        (* Ticker missing for non-active asset with balance > 0, subscribe! *)
                        Logging.info_f ~section "Balance found for non-active %s on %s, subscribing to ticker" symbol exch_name;
                        Ex.subscribe_ticker ~symbol
                    | Some _ -> Lwt.return_unit
                  end else Lwt.return_unit
                end else Lwt.return_unit
              ) balances
        ) exchange_names >>= fun () ->
        loop ()
      end
  in
  Lwt.async loop

(** Start the supervisor monitoring thread and initialize all feeds *)
let start_monitoring () =
  Logging.info ~section "Starting connection supervisor";

  (* Start the monitoring thread first *)
  monitor_loop ();

  (* Start the non-active assets monitor *)
  monitor_non_active_assets ();

  (* Start the order processing thread *)
  order_processing_loop ();

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
  (* Wake up any sleeping threads *)
  Mutex.lock shutdown_mutex;
  Condition.broadcast shutdown_cond;
  Mutex.unlock shutdown_mutex;
  Logging.info ~section "Order processing loop shutdown requested"

(** Stop all connections *)
let stop_all () =
  stop_order_processing ();
  interruptible_sleep 0.5;  (* Give order processing thread time to finish current iteration *)
  Logging.warn ~section "Stopping all supervised connections";
  Mutex.lock registry_mutex;
  Hashtbl.iter (fun _name conn ->
    set_state conn Disconnected
  ) connections;
  Mutex.unlock registry_mutex


