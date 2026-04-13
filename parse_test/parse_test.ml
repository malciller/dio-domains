open Lwt.Infix

let setup_logger () = ()

let () =
  let private_key = "bc0d132f3639233e555664a26defdfdbcd34596529f2f51b3fc8e0a448cab744d19a69666737556f" in
  let base_url = "https://lighter-proxy.malciller.workers.dev" in
  let api_key_index = 5 in
  let account_index = 722154 in
  Lwt_main.run (
    match Lighter_signer.initialize ~base_url ~private_key ~key_index:api_key_index ~acct_index:account_index with
    | Error m -> Lwt_io.printf "Signer error: %s\n" m
    | Ok () ->
      let%lwt () = Lighter_signer.initialize_nonce ~base_url ~api_key_index ~account_index |> Lwt.map ignore in
      let token = Lighter_signer.get_auth_token () in
      
      let url = Printf.sprintf "%s/api/v1/accountAll?account_index=%d&auth=%s" base_url account_index token in
      let%lwt (r, b) = Cohttp_lwt_unix.Client.get (Uri.of_string url) in
      let%lwt body = Cohttp_lwt.Body.to_string b in
      Lwt_io.printf "accountAll: %s\n" body >>= fun () ->
      
      let url = Printf.sprintf "%s/api/v1/userStats?account_index=%d&auth=%s" base_url account_index token in
      let%lwt (r, b) = Cohttp_lwt_unix.Client.get (Uri.of_string url) in
      let%lwt body = Cohttp_lwt.Body.to_string b in
      Lwt_io.printf "userStats: %s\n" body
  )
