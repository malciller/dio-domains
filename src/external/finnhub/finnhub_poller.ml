open Lwt.Infix

let section = "finnhub_poller"

(** Poll cadence in seconds: every symbol is refreshed once per interval.
    For N symbols the interval is N+1 seconds (5s floor). The global REST
    rate limiter in [Finnhub_rest] is the real guard against exceeding the
    free-tier budget (~60 calls/min); this cadence only paces the loop. *)
let poll_interval_seconds ~num_symbols = max 5.0 (float_of_int num_symbols +. 1.0)

(** Generation guard against duplicate poller instances.

    The supervisor's reconnect machinery starts a fresh connect_fn on
    reconnect without cancelling the previous fiber, so a superseded poller
    would keep firing quote requests and multiply the request rate, tripping
    the Finnhub quota (the "racing" that shows up in the logs as 429 storms
    after a data-timeout reconnect). Each [run_loop] takes a generation;
    loops whose generation is no longer current exit at their next cycle
    boundary instead of racing the replacement. *)
let current_generation = ref 0

let generation_mutex = Mutex.create ()

let take_generation () =
  Mutex.lock generation_mutex;
  incr current_generation;
  let g = !current_generation in
  Mutex.unlock generation_mutex;
  g
;;

let is_current generation =
  Mutex.lock generation_mutex;
  let current = !current_generation = generation in
  Mutex.unlock generation_mutex;
  current
;;

let run_loop ~symbols ~should_stop ~on_heartbeat () : unit Lwt.t =
  let symbols = List.sort_uniq String.compare symbols in
  let interval = poll_interval_seconds ~num_symbols:(List.length symbols) in
  let generation = take_generation () in
  let rec loop () =
    if should_stop () || not (is_current generation)
    then Lwt.return_unit
    else (
      (* Heartbeat before the cycle as well as after: a cycle can legitimately
         take tens of seconds (retries, rate-limit spacing), and the
         supervisor's passive data-timeout would otherwise flag a healthy but
         slow loop as dead and spawn a duplicate poller. *)
      on_heartbeat ();
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
      Lwt_unix.sleep interval >>= fun () -> loop ())
  in
  loop ()
;;
