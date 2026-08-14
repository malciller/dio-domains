open Lwt.Infix

let section = "finnhub_poller"

(** Poll cadence in seconds: every symbol is refreshed once per interval.
    For N symbols the interval is N+1 seconds (5s floor), so the aggregate
    request rate N * (60 / interval) stays comfortably under the free-tier
    REST budget (~60 calls/min) with margin for jitter and batch time. *)
let poll_interval_seconds ~num_symbols = max 5.0 (float_of_int num_symbols +. 1.0)

let run_loop ~symbols ~should_stop ~on_heartbeat () : unit Lwt.t =
  let symbols = List.sort_uniq String.compare symbols in
  let interval = poll_interval_seconds ~num_symbols:(List.length symbols) in
  let rec loop () =
    if should_stop ()
    then Lwt.return_unit
    else
      Lwt.join
        (List.map
           (fun symbol ->
              Lwt.catch
                (fun () ->
                   Finnhub_rest.get_quote symbol
                   >|= fun price_opt ->
                   match price_opt with
                   | Some price -> Finnhub_mark_store.push symbol ~price ~size:0.0
                   | None -> ())
                (fun _ -> Lwt.return_unit))
           symbols)
      >>= fun () ->
      on_heartbeat ();
      Lwt_unix.sleep interval >>= fun () -> loop ()
  in
  loop ()
;;
