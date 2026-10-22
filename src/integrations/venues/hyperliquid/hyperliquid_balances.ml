[@@@ocaml.text
  " Per-asset balance aggregation from two WebSocket channels:\n    - [webData2]: perpetual clearinghouse state (withdrawable, accountValue);\n    - [spotState]: spot token balances. Thread-safe store with readiness signaling. "]
open Lwt.Infix
let section = "hyperliquid_balances"
type balance_data =
  {
  asset: string ;
  balance: float ;
  wallet_type: string ;
  wallet_id: string ;
  last_updated: float }[@@ocaml.doc
                         " Per-asset balance record. Fields mirror the exchange-agnostic balance representation. "]
module BalanceStore =
  struct
    type wallet_balance =
      {
      balance: float
        [@ocaml.doc
          " Spendable balance. Spot = [total -. hold] ([hold] locked in open orders); the spot\n        order book rejects anything beyond it. "];
      total: float
        [@ocaml.doc " The wallet's full balance, holds included. "];
      wallet_type: string ;
      wallet_id: string ;
      last_updated: float }
    type t =
      {
      wallets: (string, wallet_balance) Hashtbl.t ;
      mutex: Mutex.t ;
      total_balance: float Atomic.t ;
      trading_balance: float Atomic.t ;
      staked_balance: float Atomic.t ;
      last_updated: float Atomic.t ;
      spendable_last_updated: float Atomic.t
        [@ocaml.doc
          " Newest timestamp across non-excluded (spendable) wallets, maintained by the writer\n        under [mutex] so the per-cycle freshness read is a lock-free [Atomic.get]. "]}
    let create () =
      {
        wallets = (Hashtbl.create 4);
        mutex = (Mutex.create ());
        total_balance = (Atomic.make 0.0);
        trading_balance = (Atomic.make 0.0);
        staked_balance = (Atomic.make 0.0);
        last_updated = (Atomic.make 0.0);
        spendable_last_updated = (Atomic.make 0.0)
      }
    let is_excluded_wallet =
      function | "staking" | "perp" -> true | _ -> false[@@ocaml.doc
                                                          " Wallet types excluded from the tradeable figure: \"staking\" (delegated HYPE,\n      unsellable) and \"perp\" (USDC margin, not spot capital). Counting either overstates\n      buy capacity and lets placement pass its balance guard with an order the venue then\n      rejects. "]
    let update_wallet store ~available ~total wallet_type wallet_id =
      let wallet_key = wallet_type ^ ("/" ^ wallet_id) in
      let now = Unix.gettimeofday () in
      Mutex.lock store.mutex;
      (let wallet_data =
         match Hashtbl.find_opt store.wallets wallet_key with
         | Some prev when
             (Float.equal prev.balance available) &&
               (Float.equal prev.total total)
             -> prev
         | _ ->
             {
               balance = available;
               total;
               wallet_type;
               wallet_id;
               last_updated = now
             } in
       Hashtbl.replace store.wallets wallet_key wallet_data;
       (let total =
          Hashtbl.fold (fun _ wallet acc -> acc +. wallet.total)
            store.wallets 0.0 in
        let trading =
          Hashtbl.fold
            (fun _ wallet acc ->
               if is_excluded_wallet wallet.wallet_type
               then acc
               else acc +. wallet.balance) store.wallets 0.0 in
        let staked =
          Hashtbl.fold
            (fun _ wallet acc ->
               if wallet.wallet_type = "staking"
               then acc +. wallet.total
               else acc) store.wallets 0.0 in
        let spendable_last_updated =
          Hashtbl.fold
            (fun _ w acc ->
               if is_excluded_wallet w.wallet_type
               then acc
               else Float.max acc w.last_updated) store.wallets 0.0 in
        Atomic.set store.total_balance total;
        Atomic.set store.trading_balance trading;
        Atomic.set store.staked_balance staked;
        Atomic.set store.last_updated now;
        Atomic.set store.spendable_last_updated spendable_last_updated;
        Mutex.unlock store.mutex))
    let get_balance store = Atomic.get store.trading_balance
    let get_total_balance store = Atomic.get store.total_balance
    let get_staked_balance store = Atomic.get store.staked_balance
    let get_last_updated store = Atomic.get store.last_updated[@@ocaml.doc
                                                                " Wall-clock timestamp of the last wallet update for this asset (0.0 = never updated).\n      Used for balance-snapshot staleness. "]
    let get_spendable_last_updated store =
      Atomic.get store.spendable_last_updated[@@ocaml.doc
                                               " Wall-clock timestamp of the newest spendable (non-excluded) wallet, or 0.0 if none.\n      Store-wide [last_updated] is also bumped by the staking poller (~10s), which cannot\n      change the tradeable figure, so freshness consumers must key on spendable wallets.\n      Maintained by {!update_wallet} so this is a lock-free read on the per-cycle path. "]
  end[@@ocaml.doc
       " Per-asset balance store that aggregates across wallet types (spot, perp). "]
let balance_stores : (string, BalanceStore.t) Hashtbl.t = Hashtbl.create 16
let balance_stores_mutex = Mutex.create ()
let is_ready = Atomic.make false
let ready_condition = Lwt_condition.create ()
let get_balance_store asset =
  Mutex.lock balance_stores_mutex;
  (let store =
     match Hashtbl.find_opt balance_stores asset with
     | Some store -> store
     | None ->
         let store = BalanceStore.create () in
         (Hashtbl.add balance_stores asset store; store) in
   Mutex.unlock balance_stores_mutex; store)
let get_all_assets () =
  Mutex.lock balance_stores_mutex;
  (let assets =
     Hashtbl.fold (fun asset _ acc -> asset :: acc) balance_stores [] in
   Mutex.unlock balance_stores_mutex; assets)
let get_balance asset =
  let store = get_balance_store asset in BalanceStore.get_balance store
let get_total_balance asset =
  let store = get_balance_store asset in BalanceStore.get_total_balance store
let get_staked_balance asset =
  let store = get_balance_store asset in
  BalanceStore.get_staked_balance store
(* Spot-only [(available, total)] for [asset], or [None] until the first spot snapshot.
   In a unified/portfolio account the spot USDC total is the perp collateral, so the margin
   feed reads this rather than the per-DEX perp summary. *)
let get_spot_snapshot asset =
  let store = get_balance_store asset in
  Mutex.lock store.mutex;
  (let snapshot =
     Hashtbl.fold
       (fun _ (w : BalanceStore.wallet_balance) acc ->
          if String.equal w.wallet_type "spot" then Some (w.balance, w.total) else acc)
       store.wallets None in
   Mutex.unlock store.mutex; snapshot)
(* Called after a spot snapshot is applied. The positions feed installs a callback so
   unified-account margin equity is recomputed when USDC moves without a clearinghouse
   frame (grid fills, funding, transfers). *)
let on_spot_update : (unit -> unit) ref = ref ignore
let notify_spot_update () =
  try (!on_spot_update) () with
  | exn ->
    Logging.warn_f ~section "spot-update hook failed: %s" (Printexc.to_string exn)
let notify_ready () =
  if not (Atomic.get is_ready)
  then
    (Atomic.set is_ready true;
     (try Lwt_condition.broadcast ready_condition () with | _ -> ()))
let has_balance_data asset =
  let store = get_balance_store asset in
  (Atomic.get store.last_updated) > 0.0
let spot_balances ()  : (string * float * float) list =
  Mutex.lock balance_stores_mutex;
  (let assets =
     Hashtbl.fold (fun asset _ acc -> asset :: acc) balance_stores [] in
   Mutex.unlock balance_stores_mutex;
   List.filter_map
     (fun asset ->
        let store = get_balance_store asset in
        Mutex.lock store.mutex;
        (let spot : (float * float) option =
           Hashtbl.fold
             (fun _ (w : BalanceStore.wallet_balance) acc ->
                if String.equal w.wallet_type "spot"
                then Some (w.balance, w.total)
                else acc) store.wallets None in
         Mutex.unlock store.mutex;
         Option.map (fun (available, total) -> (asset, available, total))
           spot)) assets)[@@ocaml.doc
                           " Spot-only [(asset, available, total)] triples for the capital oracle: for each asset,\n    the [\"spot/account\"] wallet exactly as the venue reports it\n    ([available = total - hold]). This is the live equivalent of the REST\n    [spotClearinghouseState] view the oracle previously had to poll \226\128\148 spot capital only,\n    with perp margin confined to a separate [\"perp/account\"] wallet that\n    {!BalanceStore.is_excluded_wallet} keeps out of the tradeable figure.\n\n    Empty until [spotState] has delivered its first snapshot. Callers must treat [] as \"no\n    live data yet\" and fall back, never as a zero balance: a fabricated zero here reads as\n    no capital and halts the buy leg. "]
let wait_until_ready () =
  if Atomic.get is_ready
  then Lwt.return_true
  else (Lwt_condition.wait ready_condition) >>= ((fun () -> Lwt.return_true))
let wait_for_balance_data assets timeout_seconds =
  let deadline = (Unix.gettimeofday ()) +. timeout_seconds in
  let rec loop () =
    if List.for_all has_balance_data assets
    then Lwt.return_true
    else
      (let remaining = deadline -. (Unix.gettimeofday ()) in
       if remaining <= 0.0
       then Lwt.return (List.for_all has_balance_data assets)
       else
         (Lwt.pick
            [(Lwt_condition.wait ready_condition) >|= ((fun () -> `Again));
            (Lwt_unix.sleep remaining) >|= ((fun () -> `Timeout))])
           >>=
           ((function
             | `Again -> loop ()
             | `Timeout -> Lwt.return (List.for_all has_balance_data assets)))) in
  loop ()
let parse_json_float json =
  match json with
  | `String s -> (try float_of_string s with | _ -> 0.0)
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> 0.0
let canonicalize_coin =
  function
  | "UBTC" -> "BTC"
  | "UETH" -> "ETH"
  | "USOL" -> "SOL"
  | other -> other[@@ocaml.doc
                    " Maps wrapped spot token identifiers to canonical symbols. The spot API returns\n    prefixed names (e.g. \"UBTC\" for BTC). This mapping must stay consistent with\n    [hyperliquid_instruments_feed.ml]. "]

(** True until the next [spotState] frame is consumed. Hyperliquid sends the full spot
    set on the first frame after a subscription, then deltas containing only changed
    coins; there is no [isSnapshot] flag. Only a full frame may zero absent assets; in a
    delta, absence means unchanged. *)
let spot_snapshot_expected = Atomic.make true

(** Mark the next [spotState] frame as the post-subscribe full snapshot; driven by the
    reconnect event, not a timer. *)
let expect_full_spot_snapshot () = Atomic.set spot_snapshot_expected true

(** Apply one spot snapshot to the per-asset stores.

    [full = true] is the authoritative full set ([spotState]'s first frame or the
    [spotClearinghouseState] resync): absent assets are zeroed, or the grid offers phantom
    base. [full = false] merges a delta: present coins update, absent coins are untouched. *)
let apply_spot_snapshot ~full entries =
  let present =
    if full then Some (Hashtbl.create (max 8 (List.length entries))) else None
  in
  List.iter
    (fun (coin, available, total) ->
       (match present with
        | Some t -> Hashtbl.replace t coin ()
        | None -> ());
       let store = get_balance_store coin in
       BalanceStore.update_wallet store ~available ~total "spot" "account")
    entries;
  match present with
  | None -> ()
  | Some present ->
    (* Full snapshot: zero every spot wallet not represented in it. Perp/staking wallets
       are left alone; only a ["spot/account"] wallet is authoritative here. *)
    List.iter
      (fun coin ->
         if not (Hashtbl.mem present coin)
         then (
           let store = get_balance_store coin in
           let has_spot =
             Mutex.lock store.mutex;
             let found =
               Hashtbl.fold
                 (fun _ (w : BalanceStore.wallet_balance) acc ->
                    acc || String.equal w.wallet_type "spot")
                 store.wallets
                 false
             in
             Mutex.unlock store.mutex;
             found
           in
           if has_spot
           then (
             BalanceStore.update_wallet
               store
               ~available:0.0
               ~total:0.0
               "spot"
               "account";
             Logging.info_f
               ~section
               "%s absent from the full spot snapshot - zeroing stale spot balance"
               coin)))
      (get_all_assets ())

let process_market_data json =
  let open Yojson.Safe.Util in
    let channel = (member "channel" json) |> to_string_option in
    match channel with
    | Some "webData2" ->
        let data = member "data" json in
        let user_state = member "userState" data in
        let clearinghouse_data =
          let direct = member "clearinghouseState" data in
          if direct <> `Null
          then direct
          else
            if user_state <> `Null
            then member "clearinghouseState" user_state
            else `Null in
        let () =
          try
            if clearinghouse_data <> `Null
            then
              let withdrawable =
                parse_json_float (member "withdrawable" clearinghouse_data) in
              let account_value =
                let margin_summary =
                  member "marginSummary" clearinghouse_data in
                parse_json_float (member "accountValue" margin_summary) in
              let perp_value =
                if withdrawable > 0.0 then withdrawable else account_value in
              let store = get_balance_store "USDC" in
              (BalanceStore.update_wallet store ~available:perp_value
                 ~total:perp_value "perp" "account";
               Logging.debug_f ~section
                 "webData2 perp USDC: %.2f (withdrawable=%.2f, accountValue=%.2f, total=%.2f)"
                 perp_value withdrawable account_value
                 (BalanceStore.get_balance store))
          with | _ -> () in
        notify_ready ()
    | Some "spotState" ->
        let data = member "data" json in
        (* The first frame after (re)subscribe is the full set; everything later is a
           delta. Consume the flag before parsing so a malformed full frame cannot cause
           a following delta to be treated as authoritative. *)
        let full = Atomic.exchange spot_snapshot_expected false in
        let () =
          try
            let balances =
              ((member "spotState" data) |> (member "balances")) |> to_list in
            let entries = ref [] in
            let parse_failed = ref false in
            List.iter
              (fun item ->
                 try
                   let raw_coin = (member "coin" item) |> to_string in
                   let coin = canonicalize_coin raw_coin in
                   let total = parse_json_float (member "total" item) in
                   let hold = parse_json_float (member "hold" item) in
                   let available = max 0.0 (total -. hold) in
                   entries := (coin, available, total) :: !entries;
                   if coin = "USDC"
                   then
                     Logging.debug_f ~section
                       "spotState USDC: %.2f avail / %.2f total (hold %.2f)"
                       available total hold
                 with
                 | exn ->
                     parse_failed := true;
                     Logging.warn_f ~section
                       "Failed to parse spotState entry: %s"
                       (Printexc.to_string exn)) balances;
            if !parse_failed && full
            then
              Logging.warn_f
                ~section
                "spotState full snapshot had parse errors; merging without the \
                 absent-asset zeroing pass (a later full resync will correct it)";
            apply_spot_snapshot
              ~full:(full && not !parse_failed)
              (List.rev !entries)
          with | _ -> () in
        notify_spot_update ();
        Concurrency.Oracle_wakeup.notify_balance ();
        notify_ready ()
    | _ -> ()
let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_user_data () in
    expect_full_spot_snapshot ();
    Lwt.catch
      (fun () ->
         Logging.debug_f ~section
           "Starting Hyperliquid balances processor task";
         (let __ppx_lwt_0 =
            Concurrency.Lwt_util.consume_stream process_market_data
              sub.stream in
          Lwt.backtrace_bind
            "src/integrations/venues/hyperliquid/hyperliquid_balances.ml" 358
            (fun exn -> try Lwt.reraise exn with | exn -> exn) __ppx_lwt_0
            (fun () ->
               sub.close ();
               Logging.debug ~section
                 "Balances stream ended (disconnect), re-subscribing...";
               Lwt.async run;
               Lwt.return_unit)))
      (fun exn ->
         sub.close ();
         Logging.error_f ~section
           "Hyperliquid balances processor task crashed: %s. Re-subscribing..."
           (Printexc.to_string exn);
         Lwt.async run;
         Lwt.return_unit) in
  Lwt.async run
let initialize ~testnet assets =
  Logging.debug_f ~section
    "Initializing Hyperliquid balances feed for %d assets (testnet=%b)"
    (List.length assets) testnet;
  List.iter (fun asset -> ignore (get_balance_store asset)) assets;
  Logging.debug ~section "Hyperliquid balance stores initialized"
