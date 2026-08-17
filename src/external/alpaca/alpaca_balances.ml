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
       (* NOTE: the positions-API [current_price] is deliberately NOT written
          into the orderbook TOB store anymore. It is an account-API mark from
          a different reference than the market-data feeds, and pushing it
          unconditionally raced the WS quote/trade writers (a lagging or
          cross-session price clobbered a fresh quote, which was the trigger
          for the pre-market -> regular amendment loop). Real-time prices come
          from the session-aware WS feed (regular + overnight); when that feed
          is quiet the REST snapshot poll in Alpaca_orderbook takes over, so
          nothing is lost. *)
       List.iter
         (fun (p : Alpaca_types.position_record) ->
            Hashtbl.replace new_balances p.symbol p.qty;
            Hashtbl.replace new_total p.symbol p.qty;
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

(** Applies a fill event delta to the balance snapshots immediately.
    Called synchronously from the WS trade-update handler so the domain
    worker sees the updated position on its next cycle, without waiting
    for the REST balance poll round-trip (~50-200ms).

    Position delta:  buy +qty / sell -qty  (both [balances] and [total_balances]).
    Cash delta:      buy -qty*price / sell +qty*price  (approximate; ignores fees).
    Equity (total_balances["USD"]) is NOT touched: a fill moves value between
    cash and position, so equity is unchanged minus fees - the REST poll is the
    authoritative source for equity.

    Thread safety: this function is pure OCaml (no Lwt yield), so it runs
    atomically in the Lwt cooperative scheduler. The copy-on-write Atomic.set
    is safe for concurrent domain readers.

    The REST poll continues as ground-truth reconciliation: it overwrites
    these tables every 2s with authoritative data, correcting any drift from
    approximated fees or missed edge cases. The strategy's
    [anticipated_base_credit] remains as a safety net for the (now rare)
    window where even this synchronous update races the domain's balance read. *)
let apply_fill_delta ~symbol ~side ~qty ~price =
  (* Update tradeable balance: position qty and cash *)
  let old_bal = Atomic.get balances in
  let new_bal = Hashtbl.copy old_bal in
  let current_pos =
    try Hashtbl.find new_bal symbol with
    | _ -> 0.0
  in
  let current_cash =
    try Hashtbl.find new_bal "USD" with
    | _ -> 0.0
  in
  let new_pos, new_cash =
    if side = "buy"
    then current_pos +. qty, current_cash -. (qty *. price)
    else Float.max 0.0 (current_pos -. qty), current_cash +. (qty *. price)
  in
  Hashtbl.replace new_bal symbol new_pos;
  Hashtbl.replace new_bal "USD" new_cash;
  Hashtbl.replace new_bal "USDC" new_cash;
  Atomic.set balances new_bal;
  (* Update total_balances position qty. Equity (total_balances["USD"]) is
     unchanged by a fill (value moves from cash to position, minus fees) and
     is left to the REST reconciliation poll. *)
  let old_total = Atomic.get total_balances in
  let new_total = Hashtbl.copy old_total in
  let current_total_pos =
    try Hashtbl.find new_total symbol with
    | _ -> 0.0
  in
  let new_total_pos =
    if side = "buy"
    then current_total_pos +. qty
    else Float.max 0.0 (current_total_pos -. qty)
  in
  Hashtbl.replace new_total symbol new_total_pos;
  Atomic.set total_balances new_total;
  Atomic.set initial_data_received true;
  Atomic.set last_update (Unix.time ());
  Concurrency.Exchange_wakeup.signal_all ();
  Logging.info_f
    ~section
    "WS fill delta applied for %s: %s %.8f @ %.4f (position: %.8f -> %.8f, cash: %.2f -> \
     %.2f)"
    symbol
    (if side = "buy" then "BUY" else "SELL")
    qty
    price
    current_pos
    new_pos
    current_cash
    new_cash
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
