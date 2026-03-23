open Lwt.Infix
open Cohttp_lwt_unix

let section = "fear_and_greed"
let endpoint = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/latest"
let api_key_env = "CMC_API_KEY"

(* Cache the fetched value for the lifetime of the process *)
let cached_value : float option Atomic.t = Atomic.make None

let clear_cache () = Atomic.set cached_value None

let set_cached v =
  Atomic.set cached_value (Some v);
  v

let load_dotenv () =
  try Dotenv.export ~path:".env" () with _ -> ()

let float_of_any = function
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `String s -> (try Some (float_of_string s) with _ -> None)
  | _ -> None

let parse_value body_str =
  try
    let open Yojson.Safe.Util in
    let json = Yojson.Safe.from_string body_str in
    let data = member "data" json in
    let try_key key = float_of_any (member key data) in
    match try_key "value" with
    | Some v -> Some v
    | None -> try_key "value "
  with _ -> None

let fetch_value_lwt ?(fallback = 50.0) () =
  load_dotenv ();
  match Sys.getenv_opt api_key_env with
  | None ->
      Logging.warn_f ~section "Missing %s; using fallback value" api_key_env;
      Lwt.return (set_cached fallback)
  | Some api_key when String.trim api_key = "" ->
      Logging.warn_f ~section "Empty %s; using fallback value" api_key_env;
      Lwt.return (set_cached fallback)
  | Some api_key ->
      let headers = Cohttp.Header.of_list [ ("X-CMC_PRO_API_KEY", api_key) ] in
      Client.get ~headers (Uri.of_string endpoint) >>= fun (resp, body) ->
      Cohttp_lwt.Body.to_string body >>= fun body_str ->
      let status = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
      if status <> 200 then (
        Logging.error_f ~section "CMC fear-and-greed HTTP %d: %s" status body_str;
        Lwt.return (set_cached fallback)
      ) else
        match parse_value body_str with
        | Some value ->
            Logging.info_f ~section "Fetched fear & greed index: %.2f" value;
            Lwt.return (set_cached value)
        | None ->
            Logging.error_f ~section "Failed to parse fear-and-greed response: %s" body_str;
            Lwt.return (set_cached fallback)

let fetch_and_cache_sync ?(fallback = 50.0) () =
  match Atomic.get cached_value with
  | Some v -> v
  | None ->
      let value =
        try Lwt_main.run (fetch_value_lwt ~fallback ())
        with exn ->
          Logging.error_f ~section "Exception fetching fear-and-greed: %s" (Printexc.to_string exn);
          fallback
      in
      set_cached value

let get_cached () = Atomic.get cached_value

let fetch_value ?(fallback = 50.0) () =
  match Atomic.get cached_value with
  | Some v -> v
  | None -> set_cached fallback

let force_fetch_async ?(fallback = 50.0) () =
  ignore (Thread.create (fun () ->
    try
      ignore (Lwt_main.run (fetch_value_lwt ~fallback ()))
    with exn ->
      Logging.error_f ~section "Exception in async fetch fear-and-greed: %s" (Printexc.to_string exn)
  ) ())

(* Get the grid value based on fear_and_greed using linear interpolation (0-100 range) *)
let grid_value_for_fng ~grid_interval:(min_val, max_val) ~fear_and_greed =
  let fng = max 0.0 (min 100.0 fear_and_greed) in
  min_val +. (fng *. (max_val -. min_val) /. 100.0)

