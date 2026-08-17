open Lwt.Infix

let section = "cancel_buys"
let () = Printexc.record_backtrace true

let () =
  Logging.init ();
  Logging.set_level Logging.WARN;
  (try Dotenv.export ~path:".env" () with
   | _ -> ());
  Mirage_crypto_rng_unix.use_default ();
  let _ctx = Lazy.force Conduit_lwt_unix.default_ctx in
  let cancel () =
    Logging.info_f ~section "Fetching all open Alpaca orders...";
    Alpaca.Rest.get_open_orders ()
    >>= function
    | Error err ->
      Printf.eprintf "Failed to fetch orders: %s\n%!" err;
      Lwt.return_unit
    | Ok orders ->
      let buys =
        List.filter
          (fun (o : Alpaca.Types.order_record) -> o.side = Alpaca.Types.Buy)
          orders
      in
      let sells =
        List.filter
          (fun (o : Alpaca.Types.order_record) -> o.side = Alpaca.Types.Sell)
          orders
      in
      Printf.printf
        "Open orders: %d total  (%d buys, %d sells)\n%!"
        (List.length orders)
        (List.length buys)
        (List.length sells);
      if buys = []
      then (
        Printf.printf "No buy orders to cancel.\n%!";
        Lwt.return_unit)
      else (
        Printf.printf
          "Cancelling %d buy orders (leaving %d sells untouched)...\n%!"
          (List.length buys)
          (List.length sells);
        let cancelled = ref 0 in
        let failed = ref 0 in
        Lwt_list.iter_s
          (fun (o : Alpaca.Types.order_record) ->
             Lwt.catch
               (fun () ->
                  Alpaca.Rest.cancel_order o.id
                  >>= function
                  | Ok _ ->
                    incr cancelled;
                    Printf.printf
                      "  [OK]  %s  %s %s qty=%.4f\n%!"
                      (String.sub o.id 0 (min 8 (String.length o.id)))
                      o.side_str
                      o.symbol
                      o.qty;
                    Lwt.return_unit
                  | Error err ->
                    incr failed;
                    Printf.printf
                      "  [ERR] %s  %s: %s\n%!"
                      (String.sub o.id 0 (min 8 (String.length o.id)))
                      o.symbol
                      err;
                    Lwt.return_unit)
               (fun exn ->
                  incr failed;
                  Printf.printf
                    "  [ERR] %s  exception: %s\n%!"
                    (String.sub o.id 0 (min 8 (String.length o.id)))
                    (Printexc.to_string exn);
                  Lwt.return_unit))
          buys
        >>= fun () ->
        Printf.printf "Done: %d cancelled, %d failed.\n%!" !cancelled !failed;
        Lwt.return_unit)
  in
  Lwt_main.run (cancel ())
;;
