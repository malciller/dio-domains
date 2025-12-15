open Lwt.Infix
open Cohttp_lwt_unix

let section = "fear_and_greed"
let endpoint = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/latest"
let api_key_env = "CMC_API_KEY"

(* Cache the fetched value for the lifetime of the process *)
let cached_value : float option ref = ref None

let clear_cache () = cached_value := None

let set_cached v =
  cached_value := Some v;
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
  match !cached_value with
  | Some v -> v
  | None ->
      let value =
        try Lwt_main.run (fetch_value_lwt ~fallback ())
        with exn ->
          Logging.error_f ~section "Exception fetching fear-and-greed: %s" (Printexc.to_string exn);
          fallback
      in
      set_cached value

let get_cached () = !cached_value

let fetch_value ?(fallback = 50.0) () =
  match !cached_value with
  | Some v -> v
  | None -> set_cached fallback

(* Return 0–4 based on fear_and_greed value *)
let grid_index_of_fng fng =
  match fng with
  | f when f < 20. -> 0
  | f when f < 40. -> 1
  | f when f < 60. -> 2
  | f when f < 80. -> 3
  | _ -> 4

(* Compute the 5 grid levels a, x, y, z, b *)
let compute_grid_levels (a, b) =
  let step = (b -. a) /. 4. in
  Array.init 5 (fun i -> a +. (float_of_int i *. step))

(* Get the grid value based on fear_and_greed *)
let grid_value_for_fng ~grid_interval ~fear_and_greed =
  let levels = compute_grid_levels grid_interval in
  let idx = grid_index_of_fng fear_and_greed in
  levels.(idx)

