open Lwt.Infix

let section = "alpaca_fallback"

type params =
  { stale_after : float
  ; half_spread : float
  ; max_divergence : float
  }

let check_interval = 2.0
let table : (string, params) Hashtbl.t = Hashtbl.create 8
let table_mutex = Mutex.create ()

let set_params symbol params =
  Mutex.lock table_mutex;
  Hashtbl.replace table symbol params;
  Mutex.unlock table_mutex
;;

let params_of symbol =
  Mutex.lock table_mutex;
  let p = Hashtbl.find_opt table symbol in
  Mutex.unlock table_mutex;
  p
;;

let get_real_mid symbol =
  match Alpaca_orderbook.get_best_bid_ask symbol with
  | Some (bp, _, ap, _) when bp > 0.0 && ap > 0.0 -> Some ((bp +. ap) /. 2.0)
  | _ -> None
;;

let within_bounds ~mark ~mid ~max_divergence =
  if mid <= 0.0 then true else abs_float (mark -. mid) /. mid <= max_divergence
;;

let synthetic_quote ~mark ~half_spread = mark -. half_spread, mark +. half_spread

let check_symbol symbol params =
  try
    let now = Unix.gettimeofday () in
    match Alpaca_orderbook.get_quote_age symbol with
    | Some age when age < params.stale_after ->
      Alpaca_orderbook.set_fallback_active symbol false
    | _ ->
      (match Finnhub.Mark_store.get_mark symbol with
       | None -> ()
       | Some (price, _size, ts) ->
         if now -. ts > params.stale_after
         then ()
         else if
           not
             (within_bounds
                ~mark:price
                ~mid:(Option.value ~default:0.0 (get_real_mid symbol))
                ~max_divergence:params.max_divergence)
         then
           Logging.warn_f
             ~section
             "Finnhub fallback for %s: mark %.4f diverges from last real mid; skipping"
             symbol
             price
         else (
           let half_spread =
             match Alpaca_orderbook.get_best_bid_ask symbol with
             | Some (bp, _, ap, _) when bp > 0.0 && ap > 0.0 && ap > bp ->
               (ap -. bp) /. 2.0
             | _ -> params.half_spread
           in
           let bid_price, ask_price = synthetic_quote ~mark:price ~half_spread in
           Alpaca_orderbook.inject_fallback_quote symbol ~bid_price ~ask_price))
  with
  | exn ->
    Logging.warn_f
      ~section
      "Fallback check for %s failed: %s"
      symbol
      (Printexc.to_string exn)
;;

let run_loop ~symbols ~should_stop ~on_heartbeat () : unit Lwt.t =
  let rec loop () =
    if should_stop ()
    then Lwt.return_unit
    else
      Lwt_unix.sleep check_interval
      >>= fun () ->
      if should_stop ()
      then Lwt.return_unit
      else (
        List.iter
          (fun symbol ->
             match params_of symbol with
             | Some p -> check_symbol symbol p
             | None -> ())
          symbols;
        on_heartbeat ();
        loop ())
  in
  loop ()
;;
