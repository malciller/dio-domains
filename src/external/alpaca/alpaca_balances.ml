(** Account collateral and position tracking module for Alpaca. *)

open Lwt.Infix

let section = "alpaca_balances"

let balances : (string, float) Hashtbl.t = Hashtbl.create 16
let total_balances : (string, float) Hashtbl.t = Hashtbl.create 16
let balances_mutex = Mutex.create ()
let initial_data_received = ref false

let get_balance asset =
  Mutex.lock balances_mutex;
  let key = if asset = "USDC" then "USD" else asset in
  let res = try Hashtbl.find balances key with _ -> (try Hashtbl.find balances asset with _ -> 0.0) in
  Mutex.unlock balances_mutex;
  res

let get_total_balance asset =
  Mutex.lock balances_mutex;
  let key = if asset = "USDC" then "USD" else asset in
  let res = try Hashtbl.find total_balances key with _ -> (try Hashtbl.find total_balances asset with _ -> 0.0) in
  Mutex.unlock balances_mutex;
  res

let get_all_balances () =
  Mutex.lock balances_mutex;
  let res = Hashtbl.fold (fun k v acc -> (k, v) :: acc) total_balances [] in
  Mutex.unlock balances_mutex;
  res

let update_balances () =
  Alpaca_rest.get_account () >>= function
  | Ok acc ->
      Alpaca_rest.get_positions () >>= fun pos_res ->
      Mutex.lock balances_mutex;
      Hashtbl.replace balances "USD" acc.cash;
      Hashtbl.replace total_balances "USD" acc.equity;
      Hashtbl.replace balances "USDC" acc.cash;
      Hashtbl.remove total_balances "USDC";
      Logging.debug_f ~section "Alpaca Account updated: buying_power=%.2f, cash=%.2f, equity=%.2f, portfolio_val=%.2f"
        acc.buying_power acc.cash acc.equity acc.portfolio_value;
      (match pos_res with
       | Ok positions ->
           if positions <> [] then
             Logging.debug_f ~section "Alpaca loaded %d active position(s)" (List.length positions);
           let pos_symbols = List.map (fun (p : Alpaca_types.position_record) -> p.symbol) positions in
           let to_remove_b = ref [] in
           Hashtbl.iter (fun k _ ->
             if k <> "USD" && k <> "USDC" && not (List.mem k pos_symbols) then
               to_remove_b := k :: !to_remove_b
           ) balances;
           List.iter (Hashtbl.remove balances) !to_remove_b;

           let to_remove_tb = ref [] in
           Hashtbl.iter (fun k _ ->
             if k <> "USD" && k <> "USDC" && not (List.mem k pos_symbols) then
               to_remove_tb := k :: !to_remove_tb
           ) total_balances;
           List.iter (Hashtbl.remove total_balances) !to_remove_tb;

           List.iter (fun (p : Alpaca_types.position_record) ->
             Hashtbl.replace balances p.symbol p.qty;
             Hashtbl.replace total_balances p.symbol p.qty;
             if p.current_price > 0.0 then begin
               let store = Alpaca_orderbook.get_or_create_store p.symbol in
               Alpaca_orderbook.SymbolStore.push store { bid_price = p.current_price; bid_size = 1.0; ask_price = p.current_price; ask_size = 1.0; timestamp = Unix.time () };
               Logging.debug_f ~section "Updated [%s] price from Alpaca positions API: %.2f" p.symbol p.current_price
             end;
             Logging.debug_f ~section "Alpaca Position [%s]: qty=%.4f, avg_entry=%.2f, current_price=%.2f, mkt_val=%.2f"
               p.symbol p.qty p.avg_entry_price p.current_price p.market_value
           ) positions
       | Error err ->
           Logging.warn_f ~section "Failed to fetch positions during balance poll: %s" err);
      initial_data_received := true;
      Mutex.unlock balances_mutex;
      Concurrency.Exchange_wakeup.signal_all ();
      Lwt.return_unit
  | Error err ->
      Logging.warn_f ~section "Failed to fetch account during balance poll: %s" err;
      Lwt.return_unit

let rec poll_loop () =
  update_balances () >>= fun () ->
  Lwt_unix.sleep 2.0 >>= fun () ->
  poll_loop ()

let initialize () =
  Logging.info ~section "Initializing Alpaca balances polling background worker...";
  Lwt.async poll_loop

let wait_until_ready () =
  let rec wait attempts =
    if !initial_data_received then Lwt.return_true
    else if attempts <= 0 then Lwt.return_false
    else
      Lwt_unix.sleep 0.5 >>= fun () ->
      wait (attempts - 1)
  in
  wait 20
