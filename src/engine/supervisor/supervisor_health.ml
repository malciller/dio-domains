[@@@ocaml.text
  " Health monitoring for supervised connections: the tick-driven monitor loop (ping/pong,\n\
  \    heartbeat, backoff reconnection) and the non-active asset subscription monitor. "]

open Lwt.Infix
open Supervisor_types
open Supervisor_connection

let section = "supervisor"

(** Case-insensitive substring test, used for rate-limit classification without pulling
    the error-handling module into the supervisor. *)
let contains_substring haystack needle =
  let haystack_len = String.length haystack in
  let needle_len = String.length needle in
  let rec loop i =
    i + needle_len <= haystack_len
    && (String.sub haystack i needle_len = needle || loop (i + 1))
  in
  needle_len = 0 || loop 0
;;

(** Kraken enforces a 15-minute cooldown after a 429. Retrying on the normal 30-second
    supervisor backoff keeps resetting that timer, so use a rate-limit-specific delay. *)
let is_kraken_rate_limited name reason =
  let is_kraken = String.length name >= 7 && String.sub name 0 7 = "kraken_" in
  let reason = String.lowercase_ascii reason in
  is_kraken
  && (contains_substring reason "429"
      || contains_substring reason "too many requests"
      || contains_substring reason "rate limit")
;;

(** A pong proves the transport, not that the venue is still pushing data. Event-driven
    connections (no data timeout) have no other liveness signal, so they refresh the data
    heartbeat on a pong; streamed connections keep it honest. *)
let note_pong conn = if Option.is_none conn.data_timeout_s then update_data_heartbeat conn

(* A connection that holds this long has proven itself; its next failure starts from the
   bottom of the backoff instead of inheriting earlier flaps. *)
let reconnect_stability_s = 300.0

let monitor_loop () =
  let cycle_count = ref 0 in
  let last_market_open = ref (Ibkr.Market_hours.is_market_open ()) in
  let last_market_status = ref (Ibkr.Market_hours.market_status_string ()) in
  let rec loop () =
    if Atomic.get shutdown_requested
    then Lwt.return_unit
    else (
      let __ppx_lwt_0 = Lwt_unix.sleep 1.0 in
      Lwt.backtrace_bind
        "src/engine/supervisor/supervisor_health.ml"
        26
        (fun exn ->
          try Lwt.reraise exn with
          | exn -> exn)
        __ppx_lwt_0
        (fun () ->
          if Atomic.get shutdown_requested
          then Lwt.return_unit
          else (
            let current_time = Unix.time () in
            try
              incr cycle_count;
              let current_open = Ibkr.Market_hours.is_market_open () in
              let current_status = Ibkr.Market_hours.market_status_string () in
              if (not !last_market_open) && current_open
              then (
                try
                  let ibkr_conn =
                    match Supervisor_types.find_connection "ibkr_gateway" with
                    | Some c -> c
                    | None -> raise Not_found
                  in
                  let ibkr_state = get_state ibkr_conn in
                  match ibkr_state with
                  | Connected ->
                    Logging.info_f
                      ~section
                      "Market status transitioned: %s. Forcing IBKR gateway reconnect to \
                       renew streams."
                      current_status;
                    ignore (restart ibkr_conn)
                  | _ ->
                    Logging.info_f
                      ~section
                      "Market status transitioned: %s. IBKR gateway is %s; existing \
                       reconnect will handle."
                      current_status
                      (match ibkr_state with
                       | Connecting -> "connecting"
                       | Failed r -> Printf.sprintf "failed (%s)" r
                       | Disconnected -> "disconnected"
                       | _ -> "unknown")
                with
                | Not_found ->
                  Logging.info_f ~section "Market status transitioned: %s." current_status)
              else if !last_market_status <> current_status
              then Logging.info_f ~section "Market status changed: %s" current_status;
              last_market_open := current_open;
              last_market_status := current_status;
              Mutex.lock registry_mutex;
              let conn_list = Hashtbl.to_seq_values connections |> List.of_seq in
              Mutex.unlock registry_mutex;
              List.iter
                (fun conn ->
                  if Atomic.get shutdown_requested then () else Mutex.lock conn.mutex;
                  let state = conn.state in
                  let attempts = conn.reconnect_attempts in
                  let last_disconnected = conn.last_disconnected in
                  let last_connecting = conn.last_connecting in
                  let last_data_received = conn.last_data_received in
                  let last_connected = conn.last_connected in
                  let has_connect_fn = Option.is_some conn.connect_fn in
                  Mutex.unlock conn.mutex;
                  (match state, last_connected with
                   | Connected, Some since
                     when attempts > 0 && current_time -. since >= reconnect_stability_s
                     ->
                     Mutex.lock conn.mutex;
                     if conn.reconnect_attempts = attempts
                     then conn.reconnect_attempts <- 0;
                     Mutex.unlock conn.mutex
                   | _ -> ());
                  match state, has_connect_fn with
                  | Failed reason, true ->
                    Mutex.lock conn.mutex;
                    let current_state = conn.state in
                    Mutex.unlock conn.mutex;
                    if current_state <> Connecting
                    then
                      if (String.equal conn.name "ibkr_gateway"
                          && not (Ibkr.Market_hours.is_market_open ()))
                         || ((String.equal conn.name "alpaca_data_ws"
                              || String.equal conn.name "alpaca_trading_ws")
                             && not (Alpaca.Market_hours.is_market_open ()))
                      then ()
                      else (
                        let rate_limited = is_kraken_rate_limited conn.name reason in
                        let max_delay =
                          if String.equal conn.name "ibkr_gateway"
                             || String.equal conn.name "lighter_public_ws"
                             || String.equal conn.name "lighter_private_ws"
                          then 300.0
                          else 30.0
                        in
                        let delay =
                          if rate_limited
                          then 930.0 +. Random.float 30.0
                          else if attempts <= 1
                          then
                            if String.equal conn.name "alpaca_data_ws"
                               || String.equal conn.name "alpaca_trading_ws"
                            then 2.0
                            else 0.0
                          else min max_delay (2.0 ** Float.of_int (attempts - 1))
                        in
                        let should_reconnect =
                          match last_disconnected with
                          | Some t -> current_time -. t >= delay
                          | None -> true
                        in
                        if should_reconnect
                        then (
                          Logging.info_f
                            ~section
                            "[%s] Backup auto-reconnecting after %.1fs backoff (reason: \
                             %s)..."
                            conn.name
                            delay
                            reason;
                          start_async conn))
                  | Disconnected, true ->
                    let should_reconnect =
                      match last_disconnected with
                      | Some t -> current_time -. t >= 60.0
                      | None -> false
                    in
                    if should_reconnect
                    then (
                      Logging.warn_f
                        ~section
                        "[%s] Connection disconnected for >60s, restarting..."
                        conn.name;
                      start_async conn)
                  | Connecting, _ ->
                    let stuck_time =
                      match last_connecting with
                      | Some t -> current_time -. t
                      | None -> 0.0
                    in
                    if stuck_time > 120.0
                    then (
                      Logging.error_f
                        ~section
                        "[%s] Connection stuck in 'Connecting' state for %.0fs, \
                         restarting..."
                        conn.name
                        stuck_time;
                      Mutex.lock conn.mutex;
                      let current_state = conn.state in
                      Mutex.unlock conn.mutex;
                      if current_state = Connecting
                      then (
                        set_state conn Disconnected;
                        if (String.equal conn.name "ibkr_gateway"
                            && not (Ibkr.Market_hours.is_market_open ()))
                           || ((String.equal conn.name "alpaca_data_ws"
                                || String.equal conn.name "alpaca_trading_ws")
                               && not (Alpaca.Market_hours.is_market_open ()))
                        then (
                          Logging.info_f
                            ~section
                            "[%s] Market closed, deferring reconnection"
                            conn.name;
                          set_state conn (Failed "Market closed"))
                        else start_async conn))
                  | Connected, _ ->
                    let data_age_s =
                      match last_data_received with
                      | Some t -> Some (current_time -. t)
                      | None -> None
                    in
                    let data_fresh =
                      match data_age_s with
                      | Some age -> age <= 60.0
                      | None -> false
                    in
                    if String.equal conn.name "kraken_auth_ws"
                       || String.equal conn.name "hyperliquid_ws"
                       || String.equal conn.name "lighter_public_ws"
                       || String.equal conn.name "lighter_private_ws"
                       || String.equal conn.name "alpaca_data_ws"
                       || String.equal conn.name "alpaca_trading_ws"
                    then (
                      let should_ping =
                        match conn.last_ping_sent with
                        | None -> true
                        | Some last_ping -> current_time -. last_ping >= 15.0
                      in
                      if should_ping
                      then (
                        conn.last_ping_sent <- Some current_time;
                        Lwt.async (fun () ->
                          let req_id = next_ping_req_id () in
                          if String.equal conn.name "kraken_auth_ws"
                          then
                            Lwt.catch
                              (fun () ->
                                Kraken.Kraken_trading_client.send_ping
                                  ~req_id
                                  ~timeout_ms:10000
                                >>= fun response ->
                                if response.success
                                then (
                                  Atomic.set conn.ping_failures 0;
                                  note_pong conn;
                                  Lwt.return_unit)
                                else (
                                  Logging.warn_f
                                    ~section
                                    "[%s] Ping failed: %s"
                                    conn.name
                                    (match response.error with
                                     | Some e -> e
                                     | None -> "unknown error");
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit))
                              (fun exn ->
                                Logging.warn_f
                                  ~section
                                  "[%s] Ping exception: %s"
                                  conn.name
                                  (Printexc.to_string exn);
                                Atomic.incr conn.ping_failures;
                                Lwt.return_unit)
                          else if String.equal conn.name "hyperliquid_ws"
                          then
                            Lwt.catch
                              (fun () ->
                                Hyperliquid.Ws.send_ping ~req_id ~timeout_ms:5000
                                >>= fun success ->
                                if success
                                then (
                                  Atomic.set conn.ping_failures 0;
                                  note_pong conn;
                                  Lwt.return_unit)
                                else (
                                  Logging.warn_f
                                    ~section
                                    "[%s] Ping failed (req_id: %d)"
                                    conn.name
                                    req_id;
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit))
                              (fun exn ->
                                Logging.warn_f
                                  ~section
                                  "[%s] Ping exception: %s"
                                  conn.name
                                  (Printexc.to_string exn);
                                Atomic.incr conn.ping_failures;
                                Lwt.return_unit)
                          else if String.equal conn.name "lighter_public_ws"
                                  || String.equal conn.name "lighter_private_ws"
                          then
                            Lwt.catch
                              (fun () ->
                                let leg : Lighter.Ws.leg =
                                  if String.equal conn.name "lighter_public_ws"
                                  then Public
                                  else Private
                                in
                                Lighter.Ws.send_ping ~leg ~req_id ~timeout_ms:5000
                                >>= fun success ->
                                if success
                                then (
                                  Atomic.set conn.ping_failures 0;
                                  note_pong conn;
                                  Lwt.return_unit)
                                else (
                                  Logging.warn_f
                                    ~section
                                    "[%s] Ping failed (req_id: %d)"
                                    conn.name
                                    req_id;
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit))
                              (fun exn ->
                                Logging.warn_f
                                  ~section
                                  "[%s] Ping exception: %s"
                                  conn.name
                                  (Printexc.to_string exn);
                                Atomic.incr conn.ping_failures;
                                Lwt.return_unit)
                          else if String.equal conn.name "alpaca_data_ws"
                          then
                            Lwt.catch
                              (fun () ->
                                Alpaca.Orderbook.send_ping ~req_id ~timeout_ms:5000
                                >>= fun success ->
                                if success
                                then (
                                  Atomic.set conn.ping_failures 0;
                                  note_pong conn;
                                  Lwt.return_unit)
                                else (
                                  Logging.warn_f
                                    ~section
                                    "[%s] Ping failed (req_id: %d)"
                                    conn.name
                                    req_id;
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit))
                              (fun exn ->
                                Logging.warn_f
                                  ~section
                                  "[%s] Ping exception: %s"
                                  conn.name
                                  (Printexc.to_string exn);
                                Atomic.incr conn.ping_failures;
                                Lwt.return_unit)
                          else if String.equal conn.name "alpaca_trading_ws"
                          then
                            Lwt.catch
                              (fun () ->
                                Alpaca.Executions.send_ping ~req_id ~timeout_ms:5000
                                >>= fun success ->
                                if success
                                then (
                                  Atomic.set conn.ping_failures 0;
                                  note_pong conn;
                                  Lwt.return_unit)
                                else (
                                  Logging.warn_f
                                    ~section
                                    "[%s] Ping failed (req_id: %d)"
                                    conn.name
                                    req_id;
                                  Atomic.incr conn.ping_failures;
                                  Lwt.return_unit))
                              (fun exn ->
                                Logging.warn_f
                                  ~section
                                  "[%s] Ping exception: %s"
                                  conn.name
                                  (Printexc.to_string exn);
                                Atomic.incr conn.ping_failures;
                                Lwt.return_unit)
                          else Lwt.return_unit));
                      let ping_failures = Atomic.get conn.ping_failures in
                      if ping_failures >= 3
                      then (
                        (* Fresh data proves the probe is wrong; a pong-only connection
                           gets its freshness from [note_pong]. *)
                        let probe_tolerated = data_fresh in
                        if probe_tolerated
                        then (
                          Atomic.set conn.ping_failures 0;
                          Logging.debug_f
                            ~section
                            "[%s] %d consecutive ping failures but data is flowing; \
                             keeping the connection (broken ping probe tolerated)"
                            conn.name
                            ping_failures)
                        else (
                          Logging.error_f
                            ~section
                            "[%s] Ping failed %d times and no data received, marking \
                             connection as failed"
                            conn.name
                            ping_failures;
                          set_state conn (Failed "ping timeout"))));
                    (match conn.data_timeout_s, data_age_s with
                     | Some limit, Some age when age > limit ->
                       if not (String.equal conn.name "ibkr_gateway")
                       then (
                         Logging.warn_f
                           ~section
                           "[%s] No data received for %.0fs, marking connection as failed"
                           conn.name
                           age;
                         set_state conn (Failed "data timeout"))
                     | _ -> ())
                  | _ -> ())
                conn_list;
              Lwt.async loop;
              Lwt.return_unit
            with
            | exn ->
              Logging.error_f
                ~section
                "Exception in monitor loop: %s"
                (Printexc.to_string exn);
              Logging.error_f ~section "Monitor loop continuing after exception...";
              Lwt.async loop;
              Lwt.return_unit)))
  in
  Lwt.async loop
[@@ocaml.doc
  " Tick-driven health monitor. Subscribes to the tick event bus and checks all registered\n\
  \    connections at most once per second. Implements:\n\
  \    - Exponential backoff reconnection for [Failed] connections (0s..30s, up to 300s \
   for\n\
  \      IBKR and Lighter)\n\
  \    - Stale disconnect detection (60s idle in [Disconnected] state)\n\
  \    - Stuck-connecting timeout (120s)\n\
  \    - Active ping/pong liveness for authenticated WebSockets\n\
  \    - Passive data-heartbeat timeout for market data feeds "]
;;

let monitor_non_active_assets () =
  let subscribed_symbols : (string, float) Hashtbl.t = Hashtbl.create 16 in
  let rec loop () =
    if Atomic.get shutdown_requested
    then Lwt.return_unit
    else
      Lwt_unix.sleep 10.0
      >>= fun () ->
      if Atomic.get shutdown_requested
      then Lwt.return_unit
      else (
        let config = Dio_engine.Config.read_config () in
        let domain_assets = Dio_engine.Config.domain_assets config.trading in
        let configured_symbols =
          List.map (fun tc -> tc.Dio_engine.Config.exchange, tc.symbol) domain_assets
        in
        let exchange_names =
          List.sort_uniq
            String.compare
            (List.map
               (fun (tc : Dio_engine.Config.trading_config) -> tc.exchange)
               domain_assets)
        in
        Lwt_list.iter_s
          (fun exch_name ->
            if exch_name = "lighter"
            then Lwt.return_unit
            else (
              match Dio_exchange.Exchange_intf.Registry.get exch_name with
              | None -> Lwt.return_unit
              | Some (module Ex) ->
                let balances = Ex.get_all_balances () in
                let symbols_to_subscribe = ref [] in
                let conn_name =
                  match Dio_exchange.Exchange_intf.Types.exchange_of_string exch_name with
                  | Kraken -> "kraken_orderbook_ws"
                  | Hyperliquid -> "hyperliquid_ws"
                  | Ibkr -> "ibkr_gateway"
                  | Alpaca -> "alpaca_data_ws"
                  | Lighter | Custom _ -> ""
                in
                let current_connected_time =
                  if conn_name = ""
                  then 0.0
                  else (
                    Mutex.lock registry_mutex;
                    let conn_opt = Hashtbl.find_opt connections conn_name in
                    Mutex.unlock registry_mutex;
                    match conn_opt with
                    | Some conn ->
                      Mutex.lock conn.mutex;
                      let t =
                        match conn.state, conn.last_connected with
                        | Connected, Some t -> t
                        | _ -> 0.0
                      in
                      Mutex.unlock conn.mutex;
                      t
                    | None -> 0.0)
                in
                if current_connected_time > 0.0
                then
                  List.iter
                    (fun (asset, _bal) ->
                      let quote =
                        match
                          Dio_exchange.Exchange_intf.Types.exchange_of_string exch_name
                        with
                        | Hyperliquid | Lighter -> "USDC"
                        | Kraken | Ibkr | Alpaca | Custom _ -> "USD"
                      in
                      let symbol =
                        if String.equal exch_name "alpaca"
                        then asset
                        else asset ^ "/" ^ quote
                      in
                      let is_configured =
                        List.exists
                          (fun (ex, sym) -> ex = exch_name && sym = symbol)
                          configured_symbols
                      in
                      let is_quote =
                        asset = "USD"
                        || asset = "USDC"
                        || asset = "ZUSD"
                        || asset = "USDT"
                        || asset = quote
                        || asset = "USDe"
                      in
                      if (not is_configured) && not is_quote
                      then (
                        let target_key = exch_name ^ ":" ^ symbol in
                        let has_book = Ex.has_orderbook_data ~symbol in
                        let now = Unix.gettimeofday () in
                        let needs_sub =
                          if not has_book
                          then (
                            match Hashtbl.find_opt subscribed_symbols target_key with
                            | None -> true
                            | Some last_sub ->
                              last_sub < current_connected_time || now -. last_sub >= 15.0)
                          else (
                            match Hashtbl.find_opt subscribed_symbols target_key with
                            | None -> true
                            | Some last_sub -> last_sub < current_connected_time)
                        in
                        if needs_sub
                        then (
                          Hashtbl.replace subscribed_symbols target_key now;
                          symbols_to_subscribe := symbol :: !symbols_to_subscribe)))
                    balances;
                if !symbols_to_subscribe <> []
                then (
                  Logging.debug_f
                    ~section
                    "Dynamically subscribing non-active assets on %s: %s"
                    exch_name
                    (String.concat ", " !symbols_to_subscribe);
                  Ex.subscribe_orderbook ~symbols:!symbols_to_subscribe)
                else Lwt.return_unit))
          exchange_names
        >>= fun () ->
        Lwt.async loop;
        Lwt.return_unit)
  in
  Lwt.async loop
[@@ocaml.doc
  " Scans all exchanges every 10s for non-configured assets with a positive balance and\n\
  \    subscribes their orderbook feeds, enabling portfolio valuation for \
   held-but-not-traded\n\
  \    assets. "]
;;
