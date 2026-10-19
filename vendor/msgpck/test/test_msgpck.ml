let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let encode msg =
  let buf = Buffer.create 64 in
  ignore (Msgpck.StringBuf.write buf msg);
  Buffer.contents buf
;;

let () =
  check
    "map a:1"
    "\x81\xa1a\x01"
    (encode (Msgpck.Map [ Msgpck.String "a", Msgpck.Int 1 ]));
  check
    "positive fixint boundary"
    "\x93\x7f\xcc\x80\xcd\x01\x00"
    (encode (Msgpck.List [ Msgpck.Int 127; Msgpck.Int 128; Msgpck.Int 256 ]));
  check
    "uint64"
    "\xd3\x00\x00\x01\x1f\x71\xfb\x04\x50"
    (encode (Msgpck.Int64 1234567890000L));
  if !failures > 0 then exit 1
;;
