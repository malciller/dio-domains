let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %s\n  actual   %s\n" name expected actual)
;;

let () =
  check
    "sha1 abc"
    "a9993e364706816aba3e25717850c26c9cd0d89d"
    Digestif.SHA1.(digest_string "abc" |> to_hex);
  check
    "sha256 empty"
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    Digestif.SHA256.(digest_string "" |> to_hex);
  check
    "sha256 abc"
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    Digestif.SHA256.(digest_string "abc" |> to_hex);
  check
    "sha512 abc"
    "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
    Digestif.SHA512.(digest_string "abc" |> to_hex);
  check
    "hmac-sha512 rfc4231 #1"
    "87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cdedaa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854"
    Digestif.SHA512.(hmac_string ~key:(String.make 20 '\x0b') "Hi There" |> to_hex);
  check
    "keccak256 empty"
    "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
    Digestif.KECCAK_256.(digest_string "" |> to_hex);
  check
    "keccak256 abc"
    "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"
    Digestif.KECCAK_256.(digest_string "abc" |> to_hex);
  check
    "websocket accept rfc6455"
    "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    (Base64.encode_exn
       Digestif.SHA1.(
         digest_string
           ("dGhlIHNhbXBsZSBub25jZQ==" ^ "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
         |> to_raw_string));
  if !failures > 0 then exit 1
;;
