(** Per-asset balance aggregation from two WebSocket channels:
    - [webData2]: perpetual clearinghouse state (withdrawable, accountValue);
    - [spotState]: spot token balances. Thread-safe store with readiness signaling. *)

open Lwt.Infix

let section = "hyperliquid_balances"

(** Per-asset balance record. Fields mirror the exchange-agnostic balance representation. *)
type balance_data =
  { asset : string
  ; balance : float
  ; wallet_type : string
  ; wallet_id : string
  ; last_updated : float
  }

(** Per-asset balance store that aggregates across wallet types (spot, perp). *)
module BalanceStore = struct
  type wallet_balance =
    { balance : float
    (** Spendable balance. Spot = [total -. hold] ([hold] locked in open orders); the spot
        order book rejects anything beyond it. *)
    ; total : float (** The wallet's full balance, holds included. *)
    ; wallet_type : string
    ; wallet_id : string
    ; last_updated : float
    }

  type t =
    { wallets : (string, wallet_balance) Hashtbl.t
    ; mutex : Mutex.t
    ; total_balance : float Atomic.t
    ; trading_balance : float Atomic.t
    ; staked_balance : float Atomic.t
    ; last_updated : float Atomic.t
    }

  let create () =
    { wallets = Hashtbl.create 4
    ; mutex = Mutex.create ()
    ; total_balance = Atomic.make 0.0
    ; trading_balance = Atomic.make 0.0
    ; staked_balance = Atomic.make 0.0
    ; last_updated = Atomic.make 0.0
    }
  ;;

  (** Wallet types excluded from the tradeable figure: "staking" (delegated HYPE,
      unsellable) and "perp" (USDC margin, not spot capital). Counting either overstates
      buy capacity and lets placement pass its balance guard with an order the venue then
      rejects. *)
  let is_excluded_wallet = function
    | "staking" | "perp" -> true
    | _ -> false
  ;;

  let update_wallet store ~available ~total wallet_type wallet_id =
    let wallet_key = wallet_type ^ "/" ^ wallet_id in
    let now = Unix.gettimeofday () in
    Mutex.lock store.mutex;
    let wallet_data =
      match Hashtbl.find_opt store.wallets wallet_key with
      | Some prev when Float.equal prev.balance available && Float.equal prev.total total
        ->
        (* Unchanged balance: keep the original timestamp. Hyperliquid resends one
           whole-account spotState snapshot per fill, so bumping [last_updated] here would
           certify this asset as fresh on another asset's activity and clear its sell-hold
           guard, permitting sale of base still committed (reserved_base). Per-asset
           freshness must reflect when THIS asset moved. *)
        prev
      | _ -> { balance = available; total; wallet_type; wallet_id; last_updated = now }
    in
    Hashtbl.replace store.wallets wallet_key wallet_data;
    let total =
      Hashtbl.fold (fun _ wallet acc -> acc +. wallet.total) store.wallets 0.0
    in
    let trading =
      Hashtbl.fold
        (fun _ wallet acc ->
          if is_excluded_wallet wallet.wallet_type then acc else acc +. wallet.balance)
        store.wallets
        0.0
    in
    let staked =
      Hashtbl.fold
        (fun _ wallet acc ->
          if wallet.wallet_type = "staking" then acc +. wallet.total else acc)
        store.wallets
        0.0
    in
    Atomic.set store.total_balance total;
    Atomic.set store.trading_balance trading;
    Atomic.set store.staked_balance staked;
    Atomic.set store.last_updated now;
    Mutex.unlock store.mutex
  ;;

  let get_balance store = Atomic.get store.trading_balance
  let get_total_balance store = Atomic.get store.total_balance
  let get_staked_balance store = Atomic.get store.staked_balance

  (** Wall-clock timestamp of the last wallet update for this asset (0.0 = never updated).
      Used for balance-snapshot staleness. *)
  let get_last_updated store = Atomic.get store.last_updated

  (** Wall-clock timestamp of the newest spendable (non-excluded) wallet, or 0.0 if none.
      Store-wide [last_updated] is also bumped by the staking poller (~10s), which cannot
      change the tradeable figure, so freshness consumers must key on spendable wallets. *)
  let get_spendable_last_updated store =
    Mutex.lock store.mutex;
    let t =
      Hashtbl.fold
        (fun _ w acc ->
          if is_excluded_wallet w.wallet_type then acc else Float.max acc w.last_updated)
        store.wallets
        0.0
    in
    Mutex.unlock store.mutex;
    t
  ;;
end

(* Global mutable state: per-asset balance stores and readiness flag. *)
let balance_stores : (string, BalanceStore.t) Hashtbl.t = Hashtbl.create 16
let balance_stores_mutex = Mutex.create ()
let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()

let get_balance_store asset =
  Mutex.lock balance_stores_mutex;
  let store =
    match Hashtbl.find_opt balance_stores asset with
    | Some store -> store
    | None ->
      let store = BalanceStore.create () in
      Hashtbl.add balance_stores asset store;
      store
  in
  Mutex.unlock balance_stores_mutex;
  store
;;

(* Public query and readiness interface. *)

let get_all_assets () =
  Mutex.lock balance_stores_mutex;
  let assets = Hashtbl.fold (fun asset _ acc -> asset :: acc) balance_stores [] in
  Mutex.unlock balance_stores_mutex;
  assets
;;

let get_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_balance store
;;

let get_total_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_total_balance store
;;

let get_staked_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_staked_balance store
;;

let notify_ready () =
  if not (Atomic.get is_ready)
  then (
    Atomic.set is_ready true;
    try Lwt_condition.broadcast ready_condition () with
    | _ -> ())
;;

let has_balance_data asset =
  let store = get_balance_store asset in
  Atomic.get store.last_updated > 0.0
;;

let wait_until_ready () =
  if Atomic.get is_ready
  then Lwt.return_true
  else Lwt_condition.wait ready_condition >>= fun () -> Lwt.return_true
;;

let wait_for_balance_data assets timeout_seconds =
  let deadline = Unix.gettimeofday () +. timeout_seconds in
  let rec loop () =
    if List.for_all has_balance_data assets
    then Lwt.return_true
    else (
      let remaining = deadline -. Unix.gettimeofday () in
      if remaining <= 0.0
      then Lwt.return (List.for_all has_balance_data assets)
      else
        Lwt.pick
          [ (Lwt_condition.wait ready_condition >|= fun () -> `Again)
          ; (Lwt_unix.sleep remaining >|= fun () -> `Timeout)
          ]
        >>= function
        | `Again -> loop ()
        | `Timeout -> Lwt.return (List.for_all has_balance_data assets))
  in
  loop ()
;;

(* JSON parsing utilities for numeric fields. *)

let parse_json_float json =
  match json with
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> 0.0
;;

(** Maps wrapped spot token identifiers to canonical symbols. The spot API returns
    prefixed names (e.g. "UBTC" for BTC). This mapping must stay consistent with
    [hyperliquid_instruments_feed.ml]. *)
let canonicalize_coin = function
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | other -> other
;;

(* WebSocket message dispatcher. Routes by channel to balance update handlers. *)

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  match channel with
  | Some "webData2" ->
    let data = member "data" json in
    let user_state = member "userState" data in
    let clearinghouse_data =
      let direct = member "clearinghouseState" data in
      if direct <> `Null
      then direct
      else if user_state <> `Null
      then member "clearinghouseState" user_state
      else `Null
    in
    (* Extract perp USDC balance. Prefer withdrawable; fall back to accountValue. *)
    let () =
      try
        if clearinghouse_data <> `Null
        then (
          let withdrawable =
            parse_json_float (member "withdrawable" clearinghouse_data)
          in
          let account_value =
            let margin_summary = member "marginSummary" clearinghouse_data in
            parse_json_float (member "accountValue" margin_summary)
          in
          let perp_value = if withdrawable > 0.0 then withdrawable else account_value in
          let store = get_balance_store "USDC" in
          BalanceStore.update_wallet
            store
            ~available:perp_value
            ~total:perp_value
            "perp"
            "account";
          Logging.debug_f
            ~section
            "webData2 perp USDC: %.2f (withdrawable=%.2f, accountValue=%.2f, total=%.2f)"
            perp_value
            withdrawable
            account_value
            (BalanceStore.get_balance store))
      with
      | _ -> ()
    in
    notify_ready ()
  | Some "spotState" ->
    let data = member "data" json in
    let () =
      try
        let balances = member "spotState" data |> member "balances" |> to_list in
        List.iter
          (fun item ->
            try
              let raw_coin = member "coin" item |> to_string in
              let coin = canonicalize_coin raw_coin in
              let total = parse_json_float (member "total" item) in
              (* Spot reports [total] and [hold] (locked in open orders); only
                 [total -. hold] is spendable. Using [total] would overstate capacity and
                 pass placement with a rejected order. *)
              let hold = parse_json_float (member "hold" item) in
              let available = max 0.0 (total -. hold) in
              let store = get_balance_store coin in
              BalanceStore.update_wallet store ~available ~total "spot" "account";
              if coin = "USDC"
              then
                Logging.debug_f
                  ~section
                  "spotState USDC: %.2f avail / %.2f total (hold %.2f)"
                  available
                  total
                  hold
            with
            | exn ->
              Logging.warn_f
                ~section
                "Failed to parse spotState entry: %s"
                (Printexc.to_string exn))
          balances
      with
      | _ -> ()
    in
    notify_ready ()
  | _ -> ()
;;

(* Background processor and module initialization. *)

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_market_data () in
    Lwt.catch
      (fun () ->
        Logging.debug_f ~section "Starting Hyperliquid balances processor task";
        let%lwt () = Concurrency.Lwt_util.consume_stream process_market_data sub.stream in
        (* Disconnect pushed None. Re-subscribe; [consume_stream] blocks on the new stream
           until the WS reconnects. *)
        sub.close ();
        Logging.debug ~section "Balances stream ended (disconnect), re-subscribing...";
        Lwt.async run;
        Lwt.return_unit)
      (fun exn ->
        sub.close ();
        Logging.error_f
          ~section
          "Hyperliquid balances processor task crashed: %s. Re-subscribing..."
          (Printexc.to_string exn);
        Lwt.async run;
        Lwt.return_unit)
  in
  Lwt.async run
;;

let initialize ~testnet assets =
  Logging.debug_f
    ~section
    "Initializing Hyperliquid balances feed for %d assets (testnet=%b)"
    (List.length assets)
    testnet;
  List.iter (fun asset -> ignore (get_balance_store asset)) assets;
  Logging.debug ~section "Hyperliquid balance stores initialized"
;;
