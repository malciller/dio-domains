(** Hyperliquid Balances Feed *)



let section = "hyperliquid_balances"

let balance_cache : (string, float) Hashtbl.t = Hashtbl.create 16
let cache_mutex = Mutex.create ()
let ready_condition = Lwt_condition.create ()
let is_ready = Atomic.make false

let get_balance asset =
  Mutex.lock cache_mutex;
  let b = Hashtbl.find_opt balance_cache asset |> Option.value ~default:0.0 in
  Mutex.unlock cache_mutex;
  b

let notify_ready () =
  if not (Atomic.get is_ready) then begin
    Atomic.set is_ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let wait_for_balance_data _assets timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if Atomic.get is_ready then
      Lwt.return_true
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then
        Lwt.return_false
      else
        let open Lwt.Infix in
        Lwt.pick [
          (Lwt_condition.wait ready_condition >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (Atomic.get is_ready)
  in
  loop ()

(** Helper to parse a numeric JSON value to float *)
let parse_json_float json =
  match json with
  | `String s -> (try float_of_string s with _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> 0.0

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  match channel with
  | Some "webData2" ->
      let data = member "data" json in
      
      (* Hyperliquid webData2 often nests state under userState *)
      let user_state = member "userState" data in
      
      let clearinghouse_data = 
        let direct = member "clearinghouseState" data in
        if direct <> `Null then direct 
        else if user_state <> `Null then member "clearinghouseState" user_state
        else `Null
      in
      
      let spot_data = 
        let direct = member "spotState" data in
        if direct <> `Null then direct 
        else if user_state <> `Null then member "spotState" user_state
        else `Null
      in

      let perp_usdc = ref 0.0 in
      let spot_usdc = ref 0.0 in

      Logging.debug_f ~section "Processing webData2 data (length=%d)" (String.length (Yojson.Safe.to_string data));

      (* 1. Parse perpetuals: use withdrawable as the available USDC for trading *)
      let () = try
        if clearinghouse_data <> `Null then begin
          (* withdrawable represents actual USDC available for new orders *)
          let withdrawable = parse_json_float (member "withdrawable" clearinghouse_data) in
          (* Fallback to accountValue if withdrawable is 0 (e.g. with open positions) *)
          let account_value = 
            let margin_summary = member "marginSummary" clearinghouse_data in
            parse_json_float (member "accountValue" margin_summary)
          in
          let perp_value = if withdrawable > 0.0 then withdrawable else account_value in
          perp_usdc := perp_value;
          Logging.debug_f ~section "Parsed Hyperliquid perp USDC: %.2f (withdrawable=%.2f, accountValue=%.2f)" perp_value withdrawable account_value
        end
      with _ -> ()
      in
      
      (* 2. Parse spot state balances (if present in webData2) *)
      let () = try
        if spot_data <> `Null then begin
          let balances = member "balances" spot_data |> to_list in
          List.iter (fun item ->
            try
              let coin = member "coin" item |> to_string in
              let total = parse_json_float (member "total" item) in
              
              if coin = "USDC" then begin
                spot_usdc := total;
                Logging.debug_f ~section "Parsed Hyperliquid spot USDC wallet: %.2f" total
              end else begin
                Mutex.lock cache_mutex;
                Hashtbl.replace balance_cache coin total;
                Mutex.unlock cache_mutex;
                Logging.debug_f ~section "Updated Hyperliquid spot balance: %s = %f" coin total
              end
            with exn -> 
              Logging.warn_f ~section "Failed to parse individual spot balance: %s" (Printexc.to_string exn)
          ) balances
        end
      with _ -> ()
      in

      (* 3. Aggregate USDC and update cache *)
      let total_usdc = !perp_usdc +. !spot_usdc in
      Mutex.lock cache_mutex;
      Hashtbl.replace balance_cache "USDC" total_usdc;
      Mutex.unlock cache_mutex;

      Logging.info_f ~section "Hyperliquid Balances Synced | Total USDC: %.2f (Perp: %.2f, Spot: %.2f)" 
        total_usdc !perp_usdc !spot_usdc;
      
      notify_ready ();
      ()

  | Some "spotState" ->
      (* Dedicated spotState subscription - parse spot wallet balances *)
      let data = member "data" json in
      Logging.debug_f ~section "Processing spotState data";
      let spot_usdc = ref 0.0 in

      let () = try
        let balances = member "balances" data |> to_list in
        List.iter (fun item ->
          try
            let coin = member "coin" item |> to_string in
            let total = parse_json_float (member "total" item) in
            
            if coin = "USDC" then begin
              spot_usdc := total;
              Logging.debug_f ~section "spotState USDC balance: %.2f" total
            end;

            (* Update individual spot balances *)
            Mutex.lock cache_mutex;
            Hashtbl.replace balance_cache coin total;
            Mutex.unlock cache_mutex
          with exn ->
            Logging.warn_f ~section "Failed to parse spotState balance entry: %s" (Printexc.to_string exn)
        ) balances
      with _ -> ()
      in

      (* Re-aggregate USDC: spot + whatever perp balance we already have *)
      if !spot_usdc > 0.0 then begin
        Mutex.lock cache_mutex;
        let existing_usdc = Hashtbl.find_opt balance_cache "USDC" |> Option.value ~default:0.0 in
        (* Only update if spot changed - avoid counting perp twice *)
        let new_total = existing_usdc -. (Hashtbl.find_opt balance_cache "_spot_usdc_prev" |> Option.value ~default:0.0) +. !spot_usdc in
        Hashtbl.replace balance_cache "USDC" new_total;
        Hashtbl.replace balance_cache "_spot_usdc_prev" !spot_usdc;
        Mutex.unlock cache_mutex;

        Logging.info_f ~section "Hyperliquid Spot Balances Updated | Spot USDC: %.2f, Total USDC: %.2f" !spot_usdc new_total
      end;

      notify_ready ();
      ()
  | _ -> ()

let fetch_initial_balances ~testnet:_ () =
  Logging.info ~section "Initial balances will be populated via webData2 + spotState WebSocket subscriptions";
  Lwt.return_unit

let _processor_task =
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt.async (fun () ->
    Logging.info ~section "Starting Hyperliquid balances processor task";
    Lwt.catch (fun () ->
      Lwt_stream.iter process_market_data sub.stream
    ) (fun exn ->
      Logging.error_f ~section "Hyperliquid balances processor task crashed: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  )


let initialize () =
  Logging.info ~section "Initializing Hyperliquid balances feed"
