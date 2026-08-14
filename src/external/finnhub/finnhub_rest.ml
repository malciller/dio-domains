open Lwt.Infix

let section = "finnhub_rest"

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
      Lwt.catch
        (fun () ->
           let t0 = Unix.gettimeofday () in
           Cohttp_lwt_unix.Client.get url
           >>= fun (resp, body) ->
           let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
           Cohttp_lwt.Body.to_string body
           >>= fun body_str ->
           Network_latency.record_rest_s "finnhub" (Unix.gettimeofday () -. t0);
           if status >= 200 && status < 300
           then (
             try
               let json = Yojson.Safe.from_string body_str in
               let c = json |> Yojson.Safe.Util.member "c" |> json_to_float in
               if c > 0.0 then Lwt.return (Ok (Some c)) else Lwt.return (Ok None)
             with
             | _ -> Lwt.return (Ok None))
           else (
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
