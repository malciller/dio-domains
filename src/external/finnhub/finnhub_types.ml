let section = "finnhub"

module Config = struct
  let api_key () =
    try Sys.getenv "FINNHUB_API_KEY" with
    | Not_found -> ""
  ;;

  let ws_url () =
    let key = api_key () in
    if key = "" then "" else "wss://ws.finnhub.io?token=" ^ key
  ;;

  let rest_base_url () = "https://finnhub.io/api/v1"
end
