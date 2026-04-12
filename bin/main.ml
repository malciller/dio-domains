(** main.ml -- Dio Trading Engine process entry point.

    Responsibilities:
    - Parse CLI arguments and load engine configuration (config.json).
    - Initialize the logging subsystem, GC tuning, and CSPRNG backend.
    - Eagerly force the Conduit TLS context to avoid Lazy.force race conditions
      across OCaml 5 domains.
    - Register signal handlers: SIGINT/SIGTERM for graceful shutdown with hedge
      liquidation, and SIGSEGV/SIGABRT/SIGBUS/SIGFPE for crash diagnostics.
    - Start the Supervisor, which establishes websocket feeds and returns
      fee-augmented trading configs.
    - Spawn one supervised domain per asset via Domain_spawner for market data
      consumption and strategy execution.
    - Launch the Order Executor and Dashboard UDS server as background Lwt fibers.
    - Run a periodic memory reporter (600s interval) in the main Lwt scheduler.
    - Block on a shutdown condition variable; on signal, close Hyperliquid hedge
      positions, tear down feeds/domains, and force-exit after a 3s timeout. *)

open Lwt.Infix

(** Conditionally enable backtraces via DIO_BACKTRACE to avoid allocation overhead in production. *)
let () = Printexc.record_backtrace
  (Sys.getenv_opt "DIO_BACKTRACE" |> Option.is_some)



module Fear_and_greed = Cmc.Fear_and_greed

(** Register atexit handler to emit a final log entry on process termination. *)
let () =
  at_exit (fun () ->
    Logging.info ~section:"main" "Process exiting - final cleanup complete"
  )

(** Atomic flag set to true when a graceful shutdown has been requested. *)
let shutdown_requested = Atomic.make false
let shutdown_condition = Lwt_condition.create ()

(** Atomic flag for forced exit on a second SIGINT. *)
let force_exit_requested = Atomic.make false

(** Tracks fatal signal number. None until a fatal signal (SIGSEGV, etc.) is received. *)
let fatal_signal_received = Atomic.make None

(** Query Hyperliquid clearinghouse state and close all hedged perpetual positions. *)
let close_hedged_positions () =
  let config = Dio_engine.Config.read_config () in
  let hedge_configs = config.trading
    |> List.filter (fun (t: Dio_engine.Config.trading_config) -> t.hedge)
    |> List.map (fun (t: Dio_engine.Config.trading_config) ->
         let hedge_symbol = String.split_on_char '/' t.symbol |> List.hd in
         (hedge_symbol, t.testnet))
  in
  if hedge_configs = [] then Lwt.return_unit
  else begin
    Logging.info ~section:"main" "Fetching active Hyperliquid Perp positions for graceful shutdown...";
    
    let wallet = match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
      | Some w -> w
      | None ->
          Logging.warn ~section:"main" "HYPERLIQUID_WALLET_ADDRESS not set, cannot close hedges!";
          ""
    in
    if wallet = "" then Lwt.return_unit
    else begin
      (* Partition hedge configs by testnet flag to deduplicate REST calls per environment. *)
      let testnet_map = Hashtbl.create 2 in
      List.iter (fun (hedge_symbol, testnet) ->
        let existing = try Hashtbl.find testnet_map testnet with _ -> [] in
        Hashtbl.replace testnet_map testnet (hedge_symbol :: existing)
      ) hedge_configs;
      
      let testnet_envs = Hashtbl.fold (fun tnet syms acc -> (tnet, syms) :: acc) testnet_map [] in
      
      Lwt_list.iter_s (fun (testnet, active_symbols) ->
        let base_url = if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz" in
        Lwt.catch (fun () ->
          let open Cohttp_lwt_unix in
          let url = Uri.of_string (base_url ^ "/info") in
          let body_json = Printf.sprintf {|{"type":"clearinghouseState","user":"%s"}|} wallet in
          let body = Cohttp_lwt.Body.of_string body_json in
          let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
          Client.post ~headers ~body url >>= fun (_resp, resp_body) ->
          Cohttp_lwt.Body.to_string resp_body >>= fun body_str ->
          
          (* Phase 1: Cancel outstanding hedge orders. *)
          let open_orders_body = Printf.sprintf {|{"type":"openOrders","user":"%s"}|} wallet in
          let open_orders_req = Cohttp_lwt.Body.of_string open_orders_body in
          Client.post ~headers ~body:open_orders_req url >>= fun (_resp2, resp_body2) ->
          Cohttp_lwt.Body.to_string resp_body2 >>= fun orders_str ->
          (try
            let open Yojson.Safe.Util in
            let orders_json = Yojson.Safe.from_string orders_str in
            let open_orders = match orders_json with `List l -> l | _ -> [] in
            let hedge_orders = List.filter_map (fun o ->
              let coin = member "coin" o |> to_string in
              let oid = match member "oid" o with `Int i -> Some (Int64.of_int i) | `Intlit s -> Some (Int64.of_string s) | _ -> None in
              if List.mem coin active_symbols then
                Option.map (fun id -> (coin, id)) oid
              else None
            ) open_orders in
            if hedge_orders <> [] then begin
              Logging.warn_f ~section:"main" "Found %d open hedge orders to cancel on shutdown" (List.length hedge_orders);
              Lwt_list.iter_s (fun (symbol, oid) ->
                Logging.info_f ~section:"main" "Cancelling open hedge order %Ld for %s" oid symbol;
                Hyperliquid.Actions.cancel_orders ~symbol ~order_ids:[oid] ~testnet >>= function
                | Ok () -> Logging.info_f ~section:"main" "Cancelled hedge order %Ld" oid; Lwt.return_unit
                | Error err -> Logging.error_f ~section:"main" "Failed to cancel hedge order %Ld: %s" oid err; Lwt.return_unit
              ) hedge_orders
            end else
              Lwt.return_unit
          with _ -> Lwt.return_unit) >>= fun () ->

          (* Phase 2: Close open hedge positions via market orders. *)
          let open Yojson.Safe.Util in
          let json = Yojson.Safe.from_string body_str in
          let asset_positions = member "assetPositions" json |> to_list in
          
          let close_orders = List.filter_map (fun item ->
            try
              let pos = member "position" item in
              let coin = member "coin" pos |> to_string in
              let szi_str = member "szi" pos |> to_string in
              let szi = float_of_string szi_str in
              
              if szi <> 0.0 && List.mem coin active_symbols then begin
                let is_buy = szi < 0.0 in (* Negative szi = short position; buy to close. Positive = long; sell to close. *)
                let qty_abs = Float.abs szi in
                Some (coin, is_buy, qty_abs)
              end else None
            with _ -> None
          ) asset_positions in
          
          if close_orders = [] then begin
            Logging.info ~section:"main" "No open Hedger perp positions detected for closure.";
            Lwt.return_unit
          end else begin
            Logging.warn_f ~section:"main" "Executing Market Orders to liquidate %d open Hedger perp positions!" (List.length close_orders);
            Lwt_list.iter_s (fun (symbol, is_buy, qty) ->
              let auth_token = match Supervisor.Token_store.get () with
                | Some token -> token
                | None -> "temp_token"
              in
              let request = {
                Dio_engine.Order_executor.order_type = "market";
                side = if is_buy then "buy" else "sell";
                quantity = qty;
                symbol = symbol;
                limit_price = None;
                time_in_force = Some "IOC";
                post_only = Some false;
                margin = None;
                reduce_only = Some true; (* Ensure order only reduces existing hedge position. *)
                order_userref = Some 3; (* Maps to strategy_userref_hedge. *)
                cl_ord_id = None;
                trigger_price = None;
                trigger_price_type = None;
                display_qty = None;
                fee_preference = None;
                duplicate_key = "hedge_close_" ^ symbol ^ "_" ^ string_of_float (Unix.gettimeofday ());
                exchange = "hyperliquid";
              } in
              Dio_engine.Order_executor.place_order ~token:auth_token ~check_duplicate:false request >>= function
              | Ok res ->
                  Logging.info_f ~section:"main" "Hedge %s closed: %s %.8f (Order ID: %s)" 
                    symbol (if is_buy then "Buy" else "Sell") qty res.Dio_exchange.Exchange_intf.Types.order_id;
                  Lwt.return_unit
              | Error err ->
                  Logging.error_f ~section:"main" "Hedge %s closure failed: %s" symbol err;
                  Lwt.return_unit
            ) close_orders
          end
        ) (fun exn ->
          Logging.error_f ~section:"main" "Failed to retrieve open perp positions for shutdown: %s" (Printexc.to_string exn);
          Lwt.return_unit
        )
      ) testnet_envs
    end
  end

(** Install SIGINT/SIGTERM handlers for graceful and forced shutdown sequences. *)
let setup_signal_handlers () =
  let handle_force_exit _signum =
    Logging.critical ~section:"main" "Force exit signal received - terminating immediately";
    exit 1
  in
  let handle_graceful_shutdown _signum =
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;
      Logging.warn ~section:"main" "Shutdown signal received, initiating pre-flight cleanup...";

      (* Replace handler so subsequent SIGINT triggers immediate exit. *)
      Sys.set_signal Sys.sigint (Sys.Signal_handle handle_force_exit);

      Lwt.async (fun () ->
        (* Close hedged positions before tearing down network feeds. *)
        close_hedged_positions () >>= fun () ->
        
        (* Propagate shutdown signal to order executor and trading client. *)
        Dio_engine.Order_executor.signal_shutdown ();
        Kraken.Kraken_trading_client.signal_shutdown ();

        (* Terminate all supervised asset-processing domains. *)
        Dio_engine.Domain_spawner.stop_all_domains ();

        (* Tear down all supervised websocket connections. *)
        Supervisor.stop_all ();

        (* Broadcast shutdown condition to unblock the main Lwt loop. *)
        Lwt_condition.broadcast shutdown_condition ();

        Logging.info ~section:"main" "Shutdown cleanup initiated, force exit available with second Ctrl+C...";
        Lwt.return_unit
      )
    ) else if not (Atomic.get force_exit_requested) then (
      Atomic.set force_exit_requested true;
      Logging.critical ~section:"main" "Second shutdown signal received - forcing immediate exit";
      exit 1
    )
  in
  Sys.set_signal Sys.sigint (Sys.Signal_handle handle_graceful_shutdown);
  Sys.set_signal Sys.sigterm (Sys.Signal_handle handle_graceful_shutdown)

(* Force module initialization to trigger exchange-implementation side-effect registration. *)
let () = ignore (Kraken.Kraken_module.Kraken_impl.name)
let () = ignore (Hyperliquid.Module.Hyperliquid_impl.name)


(** Register handlers for fatal signals (SIGSEGV, SIGABRT, SIGBUS, SIGFPE) to capture diagnostics before exit. *)
let setup_fatal_signal_handlers () =
  let handle_fatal_signal signum =
    let signal_name = match signum with
      | 11 -> "SIGSEGV (segmentation fault)"
      | 6 -> "SIGABRT (abort)"
      | 7 -> "SIGBUS (bus error)"
      | 10 -> "SIGBUS/SIGUSR1 (bus error or user signal)"  (* Signal 10 maps to SIGBUS or SIGUSR1 depending on platform. *)
      | 8 -> "SIGFPE (floating point exception)"
      | _ -> Printf.sprintf "signal %d" signum
    in

    (* Emit diagnostic snapshot: signal identity, heap stats, and backtrace. *)
    let backtrace = Printexc.get_backtrace () in
    let gc_stats = Gc.stat () in
    Logging.critical_f ~section:"main" "=== FATAL SIGNAL DIAGNOSTICS ===";
    Logging.critical_f ~section:"main" "Signal: %s (signal number: %d)" signal_name signum;
    Logging.critical_f ~section:"main" "Memory: heap=%dMB live=%dMB free=%dMB fragments=%d compactions=%d"
      (gc_stats.heap_words * (Sys.word_size / 8) / 1048576)
      (gc_stats.live_words * (Sys.word_size / 8) / 1048576)
      ((gc_stats.heap_words - gc_stats.live_words) * (Sys.word_size / 8) / 1048576)
      gc_stats.fragments
      gc_stats.compactions;
    Logging.critical_f ~section:"main" "Backtrace:\n%s" backtrace;
    Logging.critical_f ~section:"main" "=== END DIAGNOSTICS ===";

    Logging.critical_f ~section:"main" "Fatal signal received: %s - attempting emergency shutdown" signal_name;

    (* Record the fatal signal number for the main loop to consult at exit. *)
    Atomic.set fatal_signal_received (Some signum);

    (* Attempt emergency shutdown: signal components and broadcast condition. *)
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;

      (* Minimal synchronous cleanup: signal shutdown, stop domains, stop supervisors. *)
      Dio_engine.Order_executor.signal_shutdown ();
      Dio_engine.Domain_spawner.stop_all_domains ();
      Supervisor.stop_all ();
      Lwt_condition.broadcast shutdown_condition ();

      Logging.critical ~section:"main" "Emergency shutdown initiated";
    );

    (* Brief delay to allow pending cleanup writes to flush. *)
    Thread.delay 0.1;
    Logging.critical_f ~section:"main" "Process terminating due to fatal signal: %s" signal_name;
    (* Return without exit; main loop will detect fatal_signal_received and terminate. *)
    ()
  in

  (* Register handlers for fatal signals using raw signal numbers (not defined in Sys). *)
  List.iter (fun signal ->
    Sys.set_signal signal (Sys.Signal_handle handle_fatal_signal)
  ) [11; 6; 7; 8; 10]  (* 11=SIGSEGV, 6=SIGABRT, 7=SIGBUS, 8=SIGFPE, 10=SIGBUS/SIGUSR1 *)

(** Override Lwt.async_exception_hook to log unhandled async exceptions without terminating the process. *)
let setup_lwt_exception_handler () =
  Lwt.async_exception_hook := (fun exn ->
    let backtrace = Printexc.get_backtrace () in
    Logging.critical_f ~section:"main" "Unhandled exception in Lwt.async: %s\nBacktrace:\n%s"
      (Printexc.to_string exn) backtrace;
    (* Swallow the exception; re-raising here would terminate the process. *)
    ()
  )

(** Command-line argument specification (currently empty). *)
let speclist = []

let usage_msg = "Dio Trading Engine\n\nUsage: " ^ Sys.argv.(0) ^ "\n\nStarts the trading engine."

(** Synchronous trading engine initialization: fetches market sentiment, starts supervisor monitoring, and spawns per-asset domains. *)
let init_trading_engine_sync (config : Dio_engine.Config.config) =
  (* Pre-fetch Fear & Greed index before websocket connections are established. *)
  Logging.info ~section:"main" "Step 0: Fetching Fear & Greed index...";
  let () =
    try
      let value = Fear_and_greed.fetch_and_cache_sync ~fallback:50.0 () in
      Logging.info_f ~section:"main" "Startup Fear & Greed index: %.2f" value
    with exn ->
      Logging.warn_f ~section:"main" "Failed to fetch Fear & Greed at startup, using fallbacks: %s"
        (Printexc.to_string exn)
  in

  (* Start supervisor monitoring; returns trading configs augmented with fee schedules. *)
  Logging.info ~section:"main" "Starting supervisor and initializing feeds...";
  let configs_with_fees = Supervisor.start_monitoring () in

  (* Spawn one supervised domain per asset to consume market data and execute strategies. *)
  Logging.info ~section:"main" "Initializing supervised asset domains...";
  let _supervisor_thread = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets config (fun x -> x) configs_with_fees in
  Logging.info_f ~section:"main" "%d supervised asset domains initialized!" (List.length configs_with_fees);

  configs_with_fees

(** Start the order executor as a background Lwt fiber via the supervisor. *)
let init_order_executor_async () =
  Logging.info ~section:"main" "Initializing order executor...";
  Supervisor.start_order_executor ()

(** Start the Discord fill notifier as a background Lwt fiber. *)
let init_discord_notifier () =
  Discord.Notifier.start ()

let () =
  (* Parse CLI arguments. *)
  Arg.parse speclist (fun _ -> ()) usage_msg;

  (* Initialize the logging subsystem. *)
  Logging.init ();

  (* Load engine configuration from config.json. *)
  let config = Dio_engine.Config.read_config () in

  (* Apply log level and section filters from configuration. *)
  Logging.set_level config.logging.level;
  Logging.set_enabled_sections config.logging.sections;

  (* Apply GC tuning parameters from config if specified. *)
  (match config.gc with
   | Some gc_cfg ->
       Logging.info ~section:"main" "Applying GC configuration from config.json";
       let g = Gc.get () in
       Gc.set { g with
         Gc.minor_heap_size = gc_cfg.minor_heap_size;
         Gc.space_overhead = gc_cfg.space_overhead;
         Gc.max_overhead = gc_cfg.max_overhead;
         Gc.window_size = gc_cfg.window_size;
         Gc.allocation_policy = gc_cfg.allocation_policy;
         Gc.major_heap_increment = gc_cfg.major_heap_increment;
       }
   | None -> ());

  (* Periodic memory reporter using an Lwt timer (600s interval).
     Replaces the prior Gc.create_alarm approach which had two issues:
     1. GC alarm callbacks run in GC signal context; calling Mutex.lock from
        within is unsafe on OCaml 5.x if the alarmed domain already holds
        the lock (latent deadlock on InFlightOrders/Amendments mutexes).
     2. cycle_mod=10000 with space_overhead=20 caused the alarm to fire
        thousands of times per minute without reaching the threshold.
     The Lwt timer executes in the main domain scheduler, is always safe,
     and reports on a predictable wall-clock cadence. *)
  let start_time = Unix.gettimeofday () in
  let _memory_reporter =
    Lwt.async (fun () ->
      let rec loop () =
        Lwt_unix.sleep 600.0 >>= fun () ->
        if Atomic.get shutdown_requested then Lwt.return_unit
        else begin
          let now = Unix.gettimeofday () in
          let runtime = now -. start_time in
          (* Run a full major GC so live_words reflects current allocation. *)
          Gc.full_major ();
          let s = Gc.stat () in
          let heap_mb = s.heap_words * (Sys.word_size / 8) / 1048576 in
          let live_kb = s.live_words * (Sys.word_size / 8) / 1024 in
          let free_kb = (s.heap_words - s.live_words) * (Sys.word_size / 8) / 1024 in
          Logging.info_f ~section:"memory" "heap=%dMB live=%dKB free=%dKB runtime=%.0fs gc_major=%d chunks=%d"
            heap_mb live_kb free_kb runtime s.major_collections s.heap_chunks;
          (* Spawn next cycle independently to break the Lwt.bind chain. *)
          Lwt.async loop;
          Lwt.return_unit
        end
      in
      loop ()
    )
  in

  (* Seed the CSPRNG backend (mirage-crypto-rng) for cryptographic operations. *)
  Mirage_crypto_rng_unix.use_default ();

  (* Force Conduit context initialization before domain spawning to prevent
     CamlinternalLazy.Undefined race conditions in concurrent Lazy.force calls. *)
  (try
    let _ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Logging.debug ~section:"main" "Conduit context initialized successfully"
  with exn ->
    Logging.error_f ~section:"main" "Failed to initialize Conduit context: %s" (Printexc.to_string exn);
    raise exn
  );

  (* Register SIGINT/SIGTERM handlers for graceful shutdown. *)
  setup_signal_handlers ();

  (* Register fatal signal handlers (SIGSEGV, SIGABRT, etc.) for crash diagnostics. *)
  setup_fatal_signal_handlers ();

  (* Install Lwt async exception hook. *)
  setup_lwt_exception_handler ();

  Logging.info ~section:"main" "Starting Dio Trading Engine...";

  try
    let start_time = Unix.gettimeofday () in
    Lwt.async (fun () -> Dio_dashboard.Dashboard_server.start ~start_time);

    (* Synchronous engine init: supervisor, domains, websocket feeds. *)
    Logging.info ~section:"main" "Initializing trading engine...";
    let _configs = init_trading_engine_sync config in

    (* Start order executor as a background fiber. *)
    let _order_executor_promise = init_order_executor_async () in

    (* Start Discord fill notifier as a background fiber. *)
    init_discord_notifier ();

    Logging.info ~section:"main" "Trading engine ready, waiting for shutdown signal...";

    (* Enter main Lwt loop: run dashboard UDS server and block on shutdown condition. *)
    (try
      Lwt_main.run (
        Lwt_condition.wait shutdown_condition
      )
    with e ->
      let backtrace = Printexc.get_backtrace () in
      Logging.critical_f ~section:"main" "Fatal exception in Lwt event loop: %s\nBacktrace:\n%s"
        (Printexc.to_string e) backtrace;
      exit 1
    );

    (* Remove dashboard UDS socket file. *)
    Dio_dashboard.Dashboard_server.shutdown ();

    Logging.info ~section:"main" "Shutdown signal received, shutting down...";

    (* Start force-exit timer: terminate process after 3 seconds. *)
    let shutdown_start = Unix.gettimeofday () in
    let force_exit_timeout = 3.0 in

    (* No memory reporter thread to join; Lwt timer is cancelled on shutdown. *)

    (* Block for remaining timeout then force process termination. *)
    let elapsed = Unix.gettimeofday () -. shutdown_start in
    if elapsed < force_exit_timeout then (
      let remaining = force_exit_timeout -. elapsed in
      Logging.info_f ~section:"main" "Waiting %.1fs before force exit..." remaining;
      Thread.delay remaining
    );

    (* Exit with error if a fatal signal was recorded during execution. *)
    match Atomic.get fatal_signal_received with
    | Some signal_num ->
        Logging.critical_f ~section:"main" "Process terminating due to fatal signal %d received during execution" signal_num;
        exit 1
    | None ->
        Logging.info ~section:"main" "Force exiting process to ensure clean shutdown";
        exit 0
  with e ->
    let backtrace = Printexc.get_backtrace () in
    Logging.critical_f ~section:"main" "Fatal exception during startup: %s\nBacktrace:\n%s"
      (Printexc.to_string e) backtrace;
    exit 1