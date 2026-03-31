(** Hyperliquid Balances Feed
    
    Uses:
    - REST clearinghouseState for initial + periodic spot balance fetch
    - WS webData2 for perp clearinghouse state (withdrawable/accountValue)
    - WS spotState for streaming spot balance updates
*)

open Lwt.Infix

let section = "hyperliquid_balances"

(** Balance data per asset - matches Kraken's design for downstream compatibility if needed *)
type balance_data = {
  asset: string;
  balance: float;
  wallet_type: string;
  wallet_id: string;
  last_updated: float;
}

(** Balance storage that aggregates across multiple wallets (e.g., spot vs perp) *)
module BalanceStore = struct
  type wallet_balance = {
    balance: float;
    wallet_type: string;
    wallet_id: string;
    last_updated: float;
  }

  type t = {
    wallets: (string, wallet_balance) Hashtbl.t;
    mutex: Mutex.t;
    total_balance: float Atomic.t;
    last_updated: float Atomic.t;
  }

  let create () = {
    wallets = Hashtbl.create 4;
    mutex = Mutex.create ();
    total_balance = Atomic.make 0.0;
    last_updated = Atomic.make 0.0;
  }

  let update_wallet store balance wallet_type wallet_id =
    let wallet_key = wallet_type ^ "/" ^ wallet_id in
    let now = Unix.gettimeofday () in
    let wallet_data = {
      balance;
      wallet_type;
      wallet_id;
      last_updated = now;
    } in

    Mutex.lock store.mutex;
    Hashtbl.replace store.wallets wallet_key wallet_data;

    let total = Hashtbl.fold (fun _ wallet acc -> acc +. wallet.balance) store.wallets 0.0 in
    Atomic.set store.total_balance total;
    Atomic.set store.last_updated now;
    Mutex.unlock store.mutex

  let get_balance store = Atomic.get store.total_balance
end

(* --- Internal state --- *)
let balance_stores : (string, BalanceStore.t) Hashtbl.t = Hashtbl.create 16
let balance_stores_mutex = Mutex.create ()
let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let get_balance_store asset =
  Mutex.lock balance_stores_mutex;
  let store = match Hashtbl.find_opt balance_stores asset with
  | Some store -> store
  | None ->
      let store = BalanceStore.create () in
      Hashtbl.add balance_stores asset store;
      store
  in
  Mutex.unlock balance_stores_mutex;
  store

(* --- Public API --- *)

let get_all_assets () =
  Mutex.lock balance_stores_mutex;
  let assets = Hashtbl.fold (fun asset _ acc -> asset :: acc) balance_stores [] in
  Mutex.unlock balance_stores_mutex;
  assets

let get_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_balance store

let notify_ready () =
  if not (Atomic.get is_ready) then begin
    Atomic.set is_ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let has_balance_data asset =
  let store = get_balance_store asset in
  Atomic.get store.last_updated > 0.0

let wait_until_ready () =
  if Atomic.get is_ready then Lwt.return_true
  else
    Lwt_condition.wait ready_condition >>= fun () ->
    Lwt.return_true

let wait_for_balance_data assets timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_balance_data assets then Lwt.return_true
    else
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0 then Lwt.return (List.for_all has_balance_data assets)
      else
        Lwt.pick [
          (Lwt_condition.wait ready_condition >|= fun () -> `Again);
          (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
        ] >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_balance_data assets)
  in
  loop ()

(* --- JSON helpers --- *)

let parse_json_float json =
  match json with
  | `String s -> (try float_of_string s with _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> 0.0

(** Map Hyperliquid wrapped spot token names to canonical names.
    The spot API returns e.g. "UBTC" for Bitcoin, but trading symbols
    and balance queries use "BTC".  Must match the mapping in
    hyperliquid_instruments_feed.ml process_meta_response. *)
let canonicalize_coin = function
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | other -> other


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
          let store = get_balance_store "USDC" in
          BalanceStore.update_wallet store perp_value "perp" "account";
          
          Logging.debug_f ~section "webData2 perp USDC: %.2f (withdrawable=%.2f, accountValue=%.2f, total=%.2f)"
            perp_value withdrawable account_value (BalanceStore.get_balance store)
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
            let raw_coin = member "coin" item |> to_string in
            let coin = canonicalize_coin raw_coin in
            let total = parse_json_float (member "total" item) in
            let store = get_balance_store coin in
            BalanceStore.update_wallet store total "spot" "account";

            if coin = "USDC" then
              Logging.debug_f ~section "spotState USDC: %.2f (total: %.2f)" total (BalanceStore.get_balance store)
          with exn ->
            Logging.warn_f ~section "Failed to parse spotState entry: %s" (Printexc.to_string exn)
        ) balances
      with _ -> ()
      in
      
      notify_ready ()

  | _ -> ()

(* --- Initialization --- *)

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_market_data () in
    Lwt.catch (fun () ->
      Logging.info ~section "Starting Hyperliquid balances processor task";
      let%lwt () = Concurrency.Lwt_util.consume_stream process_market_data sub.stream in
      (* Stream ended normally (disconnect pushed None) — re-subscribe *)
      sub.close ();
      Logging.info ~section "Balances stream ended (disconnect), re-subscribing in 1s...";
      Lwt_unix.sleep 1.0 >>= fun () -> run ()
    ) (fun exn ->
      sub.close ();
      Logging.error_f ~section "Hyperliquid balances processor task crashed: %s. Restarting in 5s..." (Printexc.to_string exn);
      Lwt_unix.sleep 5.0 >>= fun () ->
      run ()
    )
  in
  Lwt.async run

let initialize ~testnet assets =
  Logging.info_f ~section "Initializing Hyperliquid balances feed for %d assets (testnet=%b)" (List.length assets) testnet;
  
  (* Pre-create stores for requested assets *)
  List.iter (fun asset -> ignore (get_balance_store asset)) assets;
  
  Logging.info ~section "Hyperliquid balance stores initialized"