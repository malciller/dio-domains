open Lwt.Infix

let section = "finnhub_rest"

(** Timeout for a single Finnhub REST request, in seconds. Quote responses are
    small and fast; a request that has not produced a full response within
    this window is treated as wedged (classified as [Timeout] so the retry
    layer backs off). Without a timeout a stuck connection stalls the poller
    loop indefinitely: no heartbeat reaches the supervisor, the 60s passive
    data-timeout fires, and the reconnect machinery spawns a duplicate poller
    that races the original - the "429 storm" seen in production logs. *)
let request_timeout_seconds = 10.0

(* ------------------------------------------------------------------ *)
(* Global client-side rate limiter.                                    *)
(*                                                                     *)
(* The free Finnhub tier allows ~60 REST calls/min. We budget 30 calls/ *)
(* min (one request every 2s) for ALL quote polling, so the aggregate   *)
(* request rate can never exceed the quota even if multiple poller      *)
(* instances are ever running concurrently. On HTTP 429 the spacing     *)
(* doubles (capped at 10s) and slowly decays back to 2s on success.     *)
(* ------------------------------------------------------------------ *)

let min_request_interval = ref 2.0
let max_request_interval = 10.0
let next_allowed = ref 0.0
let last_decay = ref 0.0

(** Doubles the inter-request spacing (up to [max_request_interval]) when the
    API reports a rate limit, so we back off instead of hammering it. *)
let bump_interval () =
  let next = min max_request_interval (!min_request_interval *. 2.0) in
  if next <> !min_request_interval
  then (
    min_request_interval := next;
    Logging.warn_f ~section "Rate limit hit; spacing Finnhub requests %.1fs apart" next)
;;

(** Halves the inter-request spacing back towards 2s, at most once per minute,
    when requests are succeeding again. *)
let decay_interval () =
  let now = Unix.gettimeofday () in
  if now -. !last_decay >= 60.0 && !min_request_interval > 2.0
  then (
    last_decay := now;
    let next = max 2.0 (!min_request_interval /. 2.0) in
    min_request_interval := next;
    Logging.info_f ~section "Finnhub requests back to %.1fs spacing" next)
;;

(** Reserves the next request slot: every call is spaced at least
    [min_request_interval] apart from the previous one, globally, regardless of
    how many fibers/loops are issuing requests. The read-modify-write below is
    fully synchronous (no await point), so it is atomic with respect to other
    Lwt fibers. *)
let acquire_slot () =
  let now = Unix.gettimeofday () in
  let scheduled = max !next_allowed now in
  next_allowed := scheduled +. !min_request_interval;
  if scheduled > now then Lwt_unix.sleep (scheduled -. now) else Lwt.return_unit
;;

(** Records a 429 response: backs off the limiter immediately and pushes the
    next allowed slot out so subsequent requests wait longer. *)
let note_rate_limited () =
  bump_interval ();
  let now = Unix.gettimeofday () in
  next_allowed := max !next_allowed (now +. !min_request_interval)
;;

let retry_http_exceptions ~f =
  Error_handling.retry_with_backoff
    ~section
    ~config:Error_handling.default_retry_config
    ~f
    ~is_retriable_override:(fun e ->
      match Error_handling.classify e with
      | Error_handling.Connection | Error_handling.Timeout -> true
      | _ -> false)
    ()
;;

let json_to_float = function
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

let get_quote symbol : float option Lwt.t =
  let key = Finnhub_types.Config.api_key () in
  if key = ""
  then Lwt.return None
  else (
    let url =
      Uri.of_string
        (Printf.sprintf
           "%s/quote?symbol=%s&token=%s"
           (Finnhub_types.Config.rest_base_url ())
           symbol
           key)
    in
    retry_http_exceptions ~f:(fun () ->
      (* Every HTTP attempt - including retries - takes a rate-limit slot. *)
      acquire_slot ()
      >>= fun () ->
      Lwt.catch
        (fun () ->
           let t0 = Unix.gettimeofday () in
           (* Bounded request: a hung connection resolves as [Timeout] here
              instead of stalling the poller forever. *)
           Lwt_unix.with_timeout request_timeout_seconds (fun () ->
             Cohttp_lwt_unix.Client.get url
             >>= fun (resp, body) ->
             let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
             Cohttp_lwt.Body.to_string body
             >>= fun body_str -> Lwt.return (status, body_str))
           >>= fun (status, body_str) ->
           Network_latency.record_rest_s "finnhub" (Unix.gettimeofday () -. t0);
           if status >= 200 && status < 300
           then (
             decay_interval ();
             try
               let json = Yojson.Safe.from_string body_str in
               let c = json |> Yojson.Safe.Util.member "c" |> json_to_float in
               if c > 0.0 then Lwt.return (Ok (Some c)) else Lwt.return (Ok None)
             with
             | _ -> Lwt.return (Ok None))
           else (
             if status = 429 then note_rate_limited ();
             Logging.warn_f
               ~section
               "GET /quote for %s failed HTTP %d: %s"
               symbol
               status
               body_str;
             Lwt.return (Ok None)))
        (fun exn ->
           let exn_str = Printexc.to_string exn in
           match Error_handling.classify exn_str with
           | Error_handling.Connection | Error_handling.Timeout -> Lwt.fail exn
           | _ ->
             Logging.warn_f ~section "GET /quote for %s exception: %s" symbol exn_str;
             Lwt.return (Ok None)))
    >|= function
    | Ok v -> v
    | Error _ -> None)
;;
