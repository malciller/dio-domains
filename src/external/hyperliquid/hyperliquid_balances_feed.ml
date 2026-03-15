(** Hyperliquid Balances Feed
    
    Uses:
    - REST  chouseState for initial + periodic spot balance fetch
    - WS webData2 for perp clearinghouse state (withdrawable/accountValue)
    - WS spotState for streaming spot balance updates
*)

open Lwt.Infix

let section = "hyperliquid_balances"

(* --- Internal state --- *)
let balance_cache : (string, float) Hashtbl.t = Hashtbl.create 16
let cache_mutex = Mutex.create ()
let ready_condition = Lwt_condition.create ()
let is_ready = Atomic.make false

(* Track perp vs spot separately so we can aggregate correctly *)
let perp_usdc_ref = Atomic.make 0.0 (* from clearinghouseState.withdrawable *)
let spot_usdc_ref = Atomic.make 0.0 (* from clearinghouseState or spotState WS *)

(* --- Public API --- *)

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
    if Atomic.get is_ready then Lwt.return_true
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then Lwt.return_false
      else
        Lwt.pick [
          (Lwt_condition.wait ready_condition >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (Atomic.get is_ready)
  in
  loop ()

(* --- JSON helpers --- *)

let parse_json_float json =
  match json with
  | `String s -> (try float_of_string s with _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> 0.0

(* Update the aggregated USDC total from separate perp + spot values *)
let update_usdc_total () =
  let perp = Atomic.get perp_usdc_ref in
  let spot = Atomic.get spot_usdc_ref in
  let total = perp +. spot in
  Mutex.lock cache_mutex;
  Hashtbl.replace balance_cache "USDC_TOTAL" total;
  Mutex.unlock cache_mutex;
  total

(* --- REST: clearinghouseState --- *)

let fetch_spot_balances_rest ~testnet () =
  let base_url = if testnet then "https://api.hyperliquid-testnet.xyz" else "https://api.hyperliquid.xyz" in
  let wallet = match Sys.getenv_opt "HYPERLIQUID_WALLET_ADDRESS" |> Option.map String.trim with
    | Some w -> w
    | None ->
        Logging.warn ~section "HYPERLIQUID_WALLET_ADDRESS not set, cannot fetch spot balances";
        ""
  in
  if wallet = "" then Lwt.return_unit
  else begin
    Lwt.catch (fun () ->
      let open Cohttp_lwt_unix in
      let url = Uri.of_string (base_url ^ "/info") in
      let body_json = Printf.sprintf {|{"type":"spotClearinghouseState","user":"%s"}|} wallet in
      let body = Cohttp_lwt.Body.of_string body_json in
      let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
      Client.post ~headers ~body url >>= fun (_resp, resp_body) ->
      Cohttp_lwt.Body.to_string resp_body >>= fun body_str ->

      let open Yojson.Safe.Util in
      let json = Yojson.Safe.from_string body_str in
      let balances = member "balances" json |> to_list in
      
      List.iter (fun item ->
        try
          let coin = member "coin" item |> to_string in
          let total = parse_json_float (member "total" item) in
          
          Mutex.lock cache_mutex;
          Hashtbl.replace balance_cache coin total;
          Mutex.unlock cache_mutex;

          if coin = "USDC" then begin
            Atomic.set spot_usdc_ref total;
            Logging.info_f ~section "Spot USDC balance: %.6f" total
          end else
            Logging.debug_f ~section "Spot balance: %s = %.6f" coin total
        with exn ->
          Logging.warn_f ~section "Failed to parse spot balance entry: %s" (Printexc.to_string exn)
      ) balances;
      
      let total_usdc = update_usdc_total () in
      Logging.info_f ~section "Balances synced via REST | Total USDC: %.2f (Perp: %.2f, Spot: %.2f)"
        total_usdc (Atomic.get perp_usdc_ref) (Atomic.get spot_usdc_ref);
      
      notify_ready ();
      Lwt.return_unit
    ) (fun exn ->
      Logging.error_f ~section "Failed to fetch spot balances via REST: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  end

(* --- WS message handler --- *)

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  match channel with
  | Some "webData2" ->
      let data = member "data" json in
      let user_state = member "userState" data in
      
      let clearinghouse_data = 
        let direct = member "clearinghouseState" data in
        if direct <> `Null then direct 
        else if user_state <> `Null then member "clearinghouseState" user_state
        else `Null
      in

      (* Parse perp USDC: use withdrawable (actual available) with accountValue fallback *)
      let () = try
        if clearinghouse_data <> `Null then begin
          let withdrawable = parse_json_float (member "withdrawable" clearinghouse_data) in
          let account_value = 
            let margin_summary = member "marginSummary" clearinghouse_data in
            parse_json_float (member "accountValue" margin_summary)
          in
          let perp_value = if withdrawable > 0.0 then withdrawable else account_value in
          Atomic.set perp_usdc_ref perp_value;
          let total_usdc = update_usdc_total () in
          Logging.debug_f ~section "webData2 perp USDC: %.2f (withdrawable=%.2f, accountValue=%.2f, total=%.2f)"
            perp_value withdrawable account_value total_usdc
        end
      with _ -> ()
      in
      
      notify_ready ()

  | Some "spotState" ->
      (* Streaming spot balance updates from dedicated subscription *)
      let data = member "data" json in
      let () = try
        let balances = member "spotState" data |> member "balances" |> to_list in
        List.iter (fun item ->
          try
            let coin = member "coin" item |> to_string in
            let total = parse_json_float (member "total" item) in
            
            Mutex.lock cache_mutex;
            Hashtbl.replace balance_cache coin total;
            Mutex.unlock cache_mutex;

            if coin = "USDC" then begin
              Atomic.set spot_usdc_ref total;
              let total_usdc = update_usdc_total () in
              Logging.debug_f ~section "spotState USDC: %.2f (total: %.2f)" total total_usdc
            end
          with exn ->
            Logging.warn_f ~section "Failed to parse spotState entry: %s" (Printexc.to_string exn)
        ) balances
      with _ -> ()
      in
      
      notify_ready ()

  | _ -> ()

(* --- Initialization --- *)

let fetch_initial_balances ~testnet () =
  Logging.info ~section "Fetching initial spot balances via REST API...";
  fetch_spot_balances_rest ~testnet ()

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
