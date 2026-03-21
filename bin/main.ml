
open Lwt.Infix

(** Enable backtraces for better error reporting *)
let () = Printexc.record_backtrace true



module Fear_and_greed = Cmc.Fear_and_greed

(** Set up atexit handler for final logging *)
let () =
  at_exit (fun () ->
    Logging.info ~section:"main" "Process exiting - final cleanup complete"
  )

(** Graceful shutdown flag *)
let shutdown_requested = Atomic.make false
let shutdown_condition = Lwt_condition.create ()

(** Force exit flag - for double SIGINT *)
let force_exit_requested = Atomic.make false

(** Fatal signal tracking - None if no fatal signal received, Some signal_number if fatal signal occurred *)
let fatal_signal_received = Atomic.make None

(** Fetch clearinghouse state and close perps *)
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
      (* Group by testnet environment to avoid duplicate REST calls *)
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
          
          (* Step 1: Cancel any open hedge orders *)
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
                | Ok () -> Logging.info_f ~section:"main" "✓ Cancelled hedge order %Ld" oid; Lwt.return_unit
                | Error err -> Logging.error_f ~section:"main" "✗ Failed to cancel hedge order %Ld: %s" oid err; Lwt.return_unit
              ) hedge_orders
            end else
              Lwt.return_unit
          with _ -> Lwt.return_unit) >>= fun () ->

          (* Step 2: Close any open hedge positions *)
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
                let is_buy = szi < 0.0 in (* If short (negative), buy to close. If long (positive), sell to close *)
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
                reduce_only = Some true; (* Crucially safely reduce only the hedge! *)
                order_userref = Some 3; (* strategy_userref_hedge *)
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
                  Logging.info_f ~section:"main" "✓ Hedge %s successfully closed: %s %.8f (Order ID: %s)" 
                    symbol (if is_buy then "Buy" else "Sell") qty res.Dio_exchange.Exchange_intf.Types.order_id;
                  Lwt.return_unit
              | Error err ->
                  Logging.error_f ~section:"main" "✗ Hedge %s closure failed: %s" symbol err;
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

(** Signal handler for graceful shutdown *)
let setup_signal_handlers () =
  let handle_force_exit _signum =
    Logging.critical ~section:"main" "Force exit signal received - terminating immediately";
    exit 1
  in
  let handle_graceful_shutdown _signum =
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;
      Logging.warn ~section:"main" "Shutdown signal received, initiating pre-flight cleanup...";

      (* Install force exit handler for subsequent signals *)
      Sys.set_signal Sys.sigint (Sys.Signal_handle handle_force_exit);

      Lwt.async (fun () ->
        (* Liquidate hedges synchronously inside the Lwt loop BEFORE tearing down network feeds *)
        close_hedged_positions () >>= fun () ->
        
        (* Signal shutdown to all components *)
        Dio_engine.Order_executor.signal_shutdown ();
        Kraken.Kraken_trading_client.signal_shutdown ();

        (* Stop all supervised domains *)
        Dio_engine.Domain_spawner.stop_all_domains ();

        (* Stop all websocket connections *)
        Supervisor.stop_all ();

        (* Signal shutdown condition to allow graceful shutdown *)
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

(* Forcing initialization of Kraken module to register exchange logic *)
let () = ignore (Kraken.Kraken_module.Kraken_impl.name)
let () = ignore (Hyperliquid.Module.Hyperliquid_impl.name)


(** Fatal signal handler for crashes *)
let setup_fatal_signal_handlers () =
  let handle_fatal_signal signum =
    let signal_name = match signum with
      | 11 -> "SIGSEGV (segmentation fault)"
      | 6 -> "SIGABRT (abort)"
      | 7 -> "SIGBUS (bus error)"
      | 10 -> "SIGBUS/SIGUSR1 (bus error or user signal)"  (* Signal 10 can be SIGBUS or SIGUSR1 depending on system *)
      | 8 -> "SIGFPE (floating point exception)"
      | _ -> Printf.sprintf "signal %d" signum
    in

    (* Diagnostic logging for crash investigation *)
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

    (* Track that a fatal signal was received *)
    Atomic.set fatal_signal_received (Some signum);

    (* Attempt emergency shutdown *)
    if not (Atomic.get shutdown_requested) then (
      Atomic.set shutdown_requested true;

      (* Quick emergency cleanup *)
      Dio_engine.Order_executor.signal_shutdown ();
      Dio_engine.Domain_spawner.stop_all_domains ();
      Supervisor.stop_all ();
      Lwt_condition.broadcast shutdown_condition ();

      Logging.critical ~section:"main" "Emergency shutdown initiated";
    );

    (* Give a moment for cleanup before exit *)
    Thread.delay 0.1;
    Logging.critical_f ~section:"main" "Process terminating due to fatal signal: %s" signal_name;
    (* Don't exit immediately - let main loop handle graceful shutdown *)
    ()
  in

  (* Register handlers for fatal signals - using integer values as OCaml doesn't define them in Sys *)
  List.iter (fun signal ->
    Sys.set_signal signal (Sys.Signal_handle handle_fatal_signal)
  ) [11; 6; 7; 8; 10]  (* SIGSEGV=11, SIGABRT=6, SIGBUS=7, SIGFPE=8, SIGBUS/SIGUSR1=10 *)

(** Set up Lwt unhandled exception handler *)
let setup_lwt_exception_handler () =
  Lwt.async_exception_hook := (fun exn ->
    let backtrace = Printexc.get_backtrace () in
    Logging.critical_f ~section:"main" "Unhandled exception in Lwt.async: %s\nBacktrace:\n%s"
      (Printexc.to_string exn) backtrace;
    (* Don't re-raise - this would cause the process to exit *)
    ()
  )

(** Command line argument parsing *)
let speclist = []

let usage_msg = "Dio Trading Engine\n\nUsage: " ^ Sys.argv.(0) ^ "\n\nStarts the trading engine."

(** Initialize the trading engine synchronously (for websocket setup) *)
let init_trading_engine_sync (config : Dio_engine.Config.config) =
  (* Fetch Fear & Greed once before any Kraken processes start *)
  let () =
    try
      let value = Fear_and_greed.fetch_and_cache_sync ~fallback:50.0 () in
      Logging.info_f ~section:"main" "Startup Fear & Greed index: %.2f" value
    with exn ->
      Logging.warn_f ~section:"main" "Failed to fetch Fear & Greed at startup, using fallbacks: %s"
        (Printexc.to_string exn)
  in

  (* Start supervisor monitoring for websocket connections and get configs with fees *)
  let configs_with_fees = Supervisor.start_monitoring () in

  (* Initialize supervised domains for consuming asset-specific data *)
  Logging.info ~section:"main" "Initializing supervised asset domains...";
  let _supervisor_thread = Dio_engine.Domain_spawner.spawn_supervised_domains_for_assets config (fun x -> x) configs_with_fees in
  Logging.info_f ~section:"main" "%d supervised asset domains initialized!" (List.length configs_with_fees);

  configs_with_fees

(** Initialize order executor asynchronously *)
let init_order_executor_async () =
  Logging.info ~section:"main" "Initializing order executor...";
  Supervisor.start_order_executor ()

let () =
  (* Parse command line arguments *)
  Arg.parse speclist (fun _ -> ()) usage_msg;

  (* Initialize logging system *)
  Logging.init ();


  (* Event-driven memory reporting via GC alarm *)
  let start_time = Unix.gettimeofday () in
  let mem_cycle_count = ref 0 in

  let report_memory_stats ~cycle_info_mod () =
    if Atomic.get shutdown_requested then ()
    else begin
      incr mem_cycle_count;
      if !mem_cycle_count mod cycle_info_mod = 0 then begin
        let now = Unix.gettimeofday () in
        let runtime = now -. start_time in
        let s = Gc.quick_stat () in
        let heap_mb = s.heap_words * (Sys.word_size / 8) / 1048576 in
        let live_mb = s.live_words * (Sys.word_size / 8) / 1048576 in
        let ts = let t = Unix.localtime now in
          Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
            (t.Unix.tm_year + 1900) (t.Unix.tm_mon + 1) t.Unix.tm_mday
            t.Unix.tm_hour t.Unix.tm_min t.Unix.tm_sec in
        Printf.fprintf stderr "%s INFO [memory] === MEMORY STATISTICS (Runtime: %.1fs, Cycle: %d) ===\n" ts runtime !mem_cycle_count;
        Printf.fprintf stderr "%s INFO [memory] Main Domain: heap=%dMB live=%dMB free=%dMB\n" ts heap_mb live_mb (heap_mb - live_mb);
        Printf.fprintf stderr "%s INFO [memory] GC Activity: minor=%d major=%d compactions=%d\n" ts s.minor_collections s.major_collections s.compactions;
        Printf.fprintf stderr "%s INFO [memory]\n" ts;
        Printf.fprintf stderr "%s INFO [memory] --- SUBSYSTEM COUNTS ---\n" ts;
        let ticker_stores = Hashtbl.length Kraken.Kraken_ticker_feed.stores in
        let orderbook_stores = Hashtbl.length Kraken.Kraken_orderbook_feed.stores in
        let executions_stores = Hashtbl.length Kraken.Kraken_executions_feed.symbol_stores in
        let balances_stores = Hashtbl.length Kraken.Kraken_balances_feed.balance_stores in
        Printf.fprintf stderr "%s INFO [memory] Stores: ticker=%d orderbook=%d executions=%d balances=%d\n" ts ticker_stores orderbook_stores executions_stores balances_stores;
        let domain_count = Hashtbl.length Dio_engine.Domain_spawner.domain_registry in
        Printf.fprintf stderr "%s INFO [memory] Domains: %d registered\n" ts domain_count;
        let in_flight_orders_size = Dio_strategies.Strategy_common.InFlightOrders.get_registry_size () in
        let in_flight_amendments_size = Dio_strategies.Strategy_common.InFlightAmendments.get_registry_size () in
        Printf.fprintf stderr "%s INFO [memory] In-flight: orders=%d amendments=%d\n" ts in_flight_orders_size in_flight_amendments_size;
        Printf.fprintf stderr "%s INFO [memory] === END MEMORY STATISTICS ===\n" ts;
        flush stderr
      end
    end
  in

  (* Register GC alarm — fires after every major GC cycle, no thread needed *)
  let cfg_for_alarm = Dio_engine.Config.read_config () in
  let mem_alarm_mod = cfg_for_alarm.cycle_mod in
  let _memory_alarm = Gc.create_alarm (fun () ->
    report_memory_stats ~cycle_info_mod:mem_alarm_mod ()
  ) in

  (* Read and apply logging configuration *)
  let config = Dio_engine.Config.read_config () in
  Logging.set_level config.logging.level;
  Logging.set_enabled_sections config.logging.sections;

  (* Initialize random number generator for crypto operations *)
  Mirage_crypto_rng_unix.use_default ();

  (* Initialize Conduit context early to prevent CamlinternalLazy.Undefined race conditions *)
  (* This must be done before any domains spawn or websockets connect *)
  (try
    let _ctx = Lazy.force Conduit_lwt_unix.default_ctx in
    Logging.debug ~section:"main" "Conduit context initialized successfully"
  with exn ->
    Logging.error_f ~section:"main" "Failed to initialize Conduit context: %s" (Printexc.to_string exn);
    raise exn
  );

  (* Setup signal handlers for graceful shutdown *)
  setup_signal_handlers ();

  (* Setup fatal signal handlers for crashes *)
  setup_fatal_signal_handlers ();

  (* Setup Lwt unhandled exception handler *)
  setup_lwt_exception_handler ();

  Logging.info ~section:"main" "Starting Dio Trading Engine...";

  try
    (* Initialize trading engine synchronously *)
    Logging.info ~section:"main" "Initializing trading engine...";
    let _configs = init_trading_engine_sync config in

    (* Initialize order executor asynchronously - this can run in background *)
    let _order_executor_promise = init_order_executor_async () in

    Logging.info ~section:"main" "Trading engine ready, waiting for shutdown signal...";

    (* Wait for shutdown signal *)
    (try
      Lwt_main.run (Lwt_condition.wait shutdown_condition)
    with e ->
      let backtrace = Printexc.get_backtrace () in
      Logging.critical_f ~section:"main" "Fatal exception in Lwt event loop: %s\nBacktrace:\n%s"
        (Printexc.to_string e) backtrace;
      exit 1
    );

    Logging.info ~section:"main" "Shutdown signal received, shutting down...";

    (* Start aggressive shutdown timer - force exit after 3 seconds total *)
    let shutdown_start = Unix.gettimeofday () in
    let force_exit_timeout = 3.0 in

    (* No memory reporter thread to join — GC alarm is deregistered automatically on exit *)

    (* Force exit after total timeout to ensure process terminates *)
    let elapsed = Unix.gettimeofday () -. shutdown_start in
    if elapsed < force_exit_timeout then (
      let remaining = force_exit_timeout -. elapsed in
      Logging.info_f ~section:"main" "Waiting %.1fs before force exit..." remaining;
      Thread.delay remaining
    );

    (* Check if a fatal signal was received during execution *)
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