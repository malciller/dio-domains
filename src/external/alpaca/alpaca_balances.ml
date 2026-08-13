(** Account collateral and position tracking module for Alpaca.

    Lock-free reads (HFT_AUDIT.md H2): the background refresher fiber is the
    single writer. It builds a fresh immutable balance table and publishes it
    with one [Atomic.set]; readers grab the reference with [Atomic.get] and
    look up without ever taking a mutex on the read path. *)

open Lwt.Infix

let section = "alpaca_balances"

(** Published balance snapshots. Never mutated in place after publication. *)
let balances : (string, float) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 16)

let total_balances : (string, float) Hashtbl.t Atomic.t = Atomic.make (Hashtbl.create 16)
let initial_data_received = Atomic.make false
let last_update = Atomic.make 0.0 (* wall clock of the last successful poll *)

(** Age (seconds) of the balance snapshot, or [None] before the first
    successful poll. *)
let get_balance_age () =
  let lu = Atomic.get last_update in
  if lu > 0.0 then Some (Unix.gettimeofday () -. lu) else None
;;

let get_balance asset =
  let t = Atomic.get balances in
  let key = if asset = "USDC" then "USD" else asset in
  try Hashtbl.find t key with
  | _ ->
    (try Hashtbl.find t asset with
     | _ -> 0.0)
;;

let get_total_balance asset =
  let t = Atomic.get total_balances in
  let key = if asset = "USDC" then "USD" else asset in
  try Hashtbl.find t key with
  | _ ->
    (try Hashtbl.find t asset with
     | _ -> 0.0)
;;

let get_all_balances () =
  let t = Atomic.get total_balances in
  Hashtbl.fold (fun k v acc -> (k, v) :: acc) t []
;;

let update_balances () =
  Alpaca_rest.get_account ()
  >>= function
  | Ok acc ->
    Alpaca_rest.get_positions ()
    >>= fun pos_res ->
    (* Build the new snapshots entirely before publishing, so readers never
       observe a half-updated table. *)
    let new_balances = Hashtbl.create 16 in
    let new_total = Hashtbl.create 16 in
    Hashtbl.replace new_balances "USD" acc.cash;
    Hashtbl.replace new_total "USD" acc.equity;
    Hashtbl.replace new_balances "USDC" acc.cash;
    Logging.debug_f
      ~section
      "Alpaca Account updated: buying_power=%.2f, cash=%.2f, equity=%.2f, \
       portfolio_val=%.2f"
      acc.buying_power
      acc.cash
      acc.equity
      acc.portfolio_value;
    (match pos_res with
     | Ok positions ->
       if positions <> []
       then
         Logging.debug_f
           ~section
           "Alpaca loaded %d active position(s)"
           (List.length positions);
       List.iter
         (fun (p : Alpaca_types.position_record) ->
            Hashtbl.replace new_balances p.symbol p.qty;
            Hashtbl.replace new_total p.symbol p.qty;
            if p.current_price > 0.0
            then (
              let store = Alpaca_orderbook.get_or_create_store p.symbol in
              Alpaca_orderbook.SymbolStore.push
                store
                { bid_price = p.current_price
                ; bid_size = 1.0
                ; ask_price = p.current_price
                ; ask_size = 1.0
                ; timestamp = Unix.time ()
                };
              Logging.debug_f
                ~section
                "Updated [%s] price from Alpaca positions API: %.2f"
                p.symbol
                p.current_price);
            Logging.debug_f
              ~section
              "Alpaca Position [%s]: qty=%.4f, avg_entry=%.2f, current_price=%.2f, \
               mkt_val=%.2f"
              p.symbol
              p.qty
              p.avg_entry_price
              p.current_price
              p.market_value)
         positions
     | Error err ->
       Logging.warn_f ~section "Failed to fetch positions during balance poll: %s" err);
    Atomic.set balances new_balances;
    Atomic.set total_balances new_total;
    Atomic.set initial_data_received true;
    Atomic.set last_update (Unix.time ());
    Concurrency.Exchange_wakeup.signal_all ();
    Lwt.return_unit
  | Error err ->
    Logging.warn_f ~section "Failed to fetch account during balance poll: %s" err;
    Lwt.return_unit
;;

let rec poll_loop () =
  update_balances () >>= fun () -> Lwt_unix.sleep 2.0 >>= fun () -> poll_loop ()
;;

let initialize () =
  Logging.debug ~section "Initializing Alpaca balances polling background worker...";
  Lwt.async poll_loop
;;

let wait_until_ready () =
  let rec wait attempts =
    if Atomic.get initial_data_received
    then Lwt.return_true
    else if attempts <= 0
    then Lwt.return_false
    else Lwt_unix.sleep 0.5 >>= fun () -> wait (attempts - 1)
  in
  wait 20
;;
