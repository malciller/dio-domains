let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let () =
  let u = Uri.of_string "https://api.kraken.com/0/public/AssetPairs?foo=bar&x=1" in
  check "scheme" "https" (Option.value ~default:"" (Uri.scheme u));
  check "host" "api.kraken.com" (Option.value ~default:"" (Uri.host u));
  check
    "port default"
    "443"
    (Option.value ~default:"443" (Option.map string_of_int (Uri.port u)));
  check "path" "/0/public/AssetPairs" (Uri.path u);
  check
    "roundtrip"
    "https://api.kraken.com/0/public/AssetPairs?foo=bar&x=1"
    (Uri.to_string u);
  check "pct_encode" "BTC%2FUSD" (Uri.pct_encode "BTC/USD");
  check
    "encoded_of_query"
    "nonce=123&pair=XBTUSD"
    (Uri.encoded_of_query [ "nonce", [ "123" ]; "pair", [ "XBTUSD" ] ]);
  let w = Uri.of_string "wss://ws.kraken.com/v2" in
  check "ws scheme" "wss" (Option.value ~default:"" (Uri.scheme w));
  check "ws path" "/v2" (Uri.path w);
  check "ws roundtrip" "wss://ws.kraken.com/v2" (Uri.to_string w);
  let q = Uri.with_query' w [ "token", "abc" ] in
  check "with_query" "wss://ws.kraken.com/v2?token=abc" (Uri.to_string q);
  if !failures > 0 then exit 1
;;
