
(** Health monitoring for supervised connections. Implements the tick-driven
    monitor loop (ping/pong, heartbeat, backoff reconnection) and the
    non-active asset subscription monitor. *)

open Lwt.Infix

open Supervisor_types
open Supervisor_connection

let section = "supervisor"

(** Tick-driven health monitor. Subscribes to the tick event bus and checks
    all registered connections at most once per second. Implements:
    - Linear backoff reconnection for Failed connections (2s..30s)
    - Stale disconnect detection (60s idle in Disconnected state)
    - Stuck-connecting timeout (120s)
    - Active ping/pong liveness for authenticated WebSockets
    - Passive data heartbeat timeout for market data feeds *)
let monitor_loop () =
  let cycle_count = ref 0 in
  let last_market_open = ref (Ibkr.Market_hours.is_market_open ()) in
  let last_market_status = ref (Ibkr.Market_hours.market_status_string ()) in
  let rec loop () =
    if Atomic.get shutdown_requested then Lwt.return_unit
    else begin
      let%lwt () = Lwt_unix.sleep 1.0 in
      if Atomic.get shutdown_requested then Lwt.return_unit else begin
        let current_time = Unix.time () in
        try
          incr cycle_count;

          let current_open = Ibkr.Market_hours.is_market_open () in
          let current_status = Ibkr.Market_hours.market_status_string () in
          (* Only force IBKR reconnect on closed→open transitions.
             Transitions between closed sub-states (weekend, pre-market,
             after-hours-ended) should NOT trigger reconnection spam. *)
          if not !last_market_open && current_open then begin
            (try
               let ibkr_conn = Hashtbl.find connections "ibkr_gateway" in
               let ibkr_state = get_state ibkr_conn in
               (match ibkr_state with
                | Connected ->
                    (* Connection was live during pre-market; tear down and
                       reconnect to get fresh market-data streams. *)
                    Logging.info_f ~section "Market status transitioned: %s. Forcing IBKR gateway reconnect to renew streams." current_status;
                    ignore (restart ibkr_conn)
                | _ ->
                    (* connect_fn is already handling reconnection (e.g. waking
                       from market-closed sleep) or the monitor loop's Failed
                       handler will pick it up.  A forced restart here would
                       spawn a duplicate connect_fn, causing two concurrent TCP
                       connections to race against the gateway on the same
                       clientId — resulting in interleaved End_of_file errors. *)
                    Logging.info_f ~section "Market status transitioned: %s. IBKR gateway is %s; existing reconnect will handle."
                      current_status
                      (match ibkr_state with
                       | Connecting -> "connecting"
                       | Failed r -> Printf.sprintf "failed (%s)" r
                       | Disconnected -> "disconnected"
                       | _ -> "unknown"))
             with Not_found ->
               Logging.info_f ~section "Market status transitioned: %s." current_status);
          end else if !last_market_status <> current_status then
            Logging.info_f ~section "Market status changed: %s" current_status;
          last_market_open := current_open;
          last_market_status := current_status;

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
                          (* IBKR market hours gate: don't restart against a
                             closed gateway — let the monitor's Failed handler
                             defer reconnection to the next market open. *)
                          if String.equal conn.name "ibkr_gateway"
                             && not (Ibkr.Market_hours.is_market_open ()) then begin
                            Logging.info_f ~section "[%s] Market closed, deferring reconnection" conn.name;
                            set_state conn (Failed "Market closed")
                          end else
                            start_async conn
                        end
                      end
                  | Connected, _ ->
                      (* Active ping/pong liveness for authenticated connections and Kraken orderbook *)
                      if String.equal conn.name "kraken_auth_ws" || String.equal conn.name "kraken_orderbook_ws" || String.equal conn.name "hyperliquid_ws" || String.equal conn.name "lighter_ws" then begin
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
                            else if String.equal conn.name "kraken_orderbook_ws" then
                              Lwt.catch
                                (fun () ->
                                  Kraken.Kraken_orderbook_feed.send_ping ~req_id ~timeout_ms:5000 >>= fun (response : Kraken.Kraken_common_types.ws_response) ->
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
                            if not (String.equal conn.name "ibkr_gateway") then begin
                              Logging.warn_f ~section "[%s] No data received for %.0fs, marking connection as failed"
                                conn.name (current_time -. last_data);
                              set_state conn (Failed "data timeout")
                            end
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

(** Periodically scans all exchanges for non-configured assets that have
    a positive balance and subscribes their orderbook feeds. Runs every 10s.
    Enables portfolio valuation for assets that are held but not actively traded. *)
let monitor_non_active_assets () =
  let subscribed_symbols : (string, float) Hashtbl.t = Hashtbl.create 16 in
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
          if exch_name = "lighter" then Lwt.return_unit else
          match Dio_exchange.Exchange_intf.Registry.get exch_name with
          | None -> Lwt.return_unit
          | Some (module Ex) ->
              let balances = Ex.get_all_balances () in
              let symbols_to_subscribe = ref [] in
              let conn_name = match Dio_exchange.Exchange_intf.Types.exchange_of_string exch_name with
                | Kraken -> "kraken_orderbook_ws"
                | Hyperliquid -> "hyperliquid_ws"
                | Ibkr -> "ibkr_gateway"
                | Alpaca -> "alpaca_trading_ws"
                | Lighter | Custom _ -> ""
              in
              let current_connected_time =
                if conn_name = "" then 0.0
                else begin
                  Mutex.lock registry_mutex;
                  let conn_opt = Hashtbl.find_opt connections conn_name in
                  Mutex.unlock registry_mutex;
                  match conn_opt with
                  | Some conn ->
                      Mutex.lock conn.mutex;
                      let t = match conn.state, conn.last_connected with
                      | Connected, Some t -> t
                      | _ -> 0.0
                      in
                      Mutex.unlock conn.mutex;
                      t
                  | None -> 0.0
                end
              in
              
              if current_connected_time > 0.0 then begin
                List.iter (fun (asset, _bal) ->
                  let quote = match Dio_exchange.Exchange_intf.Types.exchange_of_string exch_name with
                    | Hyperliquid | Lighter -> "USDC"
                    | Kraken | Ibkr | Alpaca | Custom _ -> "USD"
                  in
                  let symbol = if String.equal exch_name "alpaca" then asset else asset ^ "/" ^ quote in
                  let is_configured = List.exists (fun (ex, sym) ->
                    ex = exch_name && sym = symbol
                  ) configured_symbols in
                  let is_quote = (asset = "USD") || (asset = "USDC") || (asset = "ZUSD") || (asset = "USDT") || (asset = quote) || (asset = "USDe") in
                  
                  if not is_configured && not is_quote then begin
                    let target_key = exch_name ^ ":" ^ symbol in
                    let needs_sub = match Hashtbl.find_opt subscribed_symbols target_key with
                      | None -> true
                      | Some t -> t < current_connected_time
                    in
                    if needs_sub then begin
                      Hashtbl.replace subscribed_symbols target_key current_connected_time;
                      symbols_to_subscribe := symbol :: !symbols_to_subscribe
                    end
                  end
                ) balances
              end;
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
