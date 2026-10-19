let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let () =
  let open Yojson.Safe.Util in
  let json =
    Yojson.Safe.from_string
      {|{"s":"a\u00e9\u4e2d\ud83d\ude00","i":12345678901234567890,"f":1.5,"b":true,"n":null,"a":[1,2]}|}
  in
  check "unicode string" "aé中😀" (member "s" json |> to_string);
  (match member "i" json with
   | `Intlit s -> check "large int" "12345678901234567890" s
   | _ ->
     incr failures;
     Printf.eprintf "FAIL large int kind\n");
  check "float" "1.5" (member "f" json |> to_float |> string_of_float);
  check "bool" "true" (string_of_bool (member "b" json |> to_bool));
  check
    "null option"
    "none"
    (match member "n" json |> to_string_option with
     | None -> "none"
     | Some _ -> "some");
  (match member "a" json |> to_list with
   | [ `Int 1; `Int 2 ] -> ()
   | _ ->
     incr failures;
     Printf.eprintf "FAIL array\n");
  let rendered = Yojson.Safe.to_string json in
  let reparsed = Yojson.Safe.from_string rendered in
  check "round-trip" (Yojson.Safe.to_string json) (Yojson.Safe.to_string reparsed);
  let pretty = Yojson.Safe.pretty_to_string json in
  check
    "pretty round-trip"
    (Yojson.Safe.to_string json)
    (Yojson.Safe.to_string (Yojson.Safe.from_string pretty));
  let basic = Yojson.Basic.from_string {|{"x":1.0}|} in
  (match Yojson.Basic.Util.member "x" basic |> Yojson.Basic.Util.to_float with
   | 1.0 -> ()
   | _ ->
     incr failures;
     Printf.eprintf "FAIL basic float\n");
  if !failures > 0 then exit 1
;;
