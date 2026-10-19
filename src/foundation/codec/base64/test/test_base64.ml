let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let () =
  let vectors =
    [ "", ""
    ; "f", "Zg=="
    ; "fo", "Zm8="
    ; "foo", "Zm9v"
    ; "foob", "Zm9vYg=="
    ; "fooba", "Zm9vYmE="
    ; "foobar", "Zm9vYmFy"
    ]
  in
  List.iter
    (fun (plain, encoded) ->
      check ("encode " ^ plain) encoded (Base64.encode_exn plain);
      check ("decode " ^ encoded) plain (Base64.decode_exn encoded))
    vectors;
  check "encode unpadded" "Zg" (Base64.encode_exn ~pad:false "f");
  check "decode single byte" "a" (Base64.decode_exn "YQ==");
  check "decode two bytes" "ab" (Base64.decode_exn "YWI=");
  check
    "uri-safe encode"
    "-_8="
    (Base64.encode_exn ~alphabet:Base64.uri_safe_alphabet "\xfb\xff");
  check
    "uri-safe decode"
    "\xfb\xff"
    (Base64.decode_exn ~alphabet:Base64.uri_safe_alphabet "-_8=");
  (match Base64.decode "!!!!" with
   | Error _ -> ()
   | Ok _ ->
     incr failures;
     Printf.eprintf "FAIL invalid input was accepted\n");
  if !failures > 0 then exit 1
;;
