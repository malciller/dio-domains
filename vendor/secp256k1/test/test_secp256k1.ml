open Bigarray

let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %s\n  actual   %s\n" name expected actual)
;;

let hex = "0123456789abcdef"

let hex_of_buffer b =
  let n = Array1.dim b in
  let s = Bytes.create (2 * n) in
  for i = 0 to n - 1 do
    let v = Char.code (Array1.get b i) in
    Bytes.set s (2 * i) hex.[v lsr 4];
    Bytes.set s ((2 * i) + 1) hex.[v land 0xf]
  done;
  Bytes.unsafe_to_string s
;;

let buffer_of_hex s =
  let n = String.length s / 2 in
  let b = Array1.create char c_layout n in
  let digit c =
    match c with
    | '0' .. '9' -> Char.code c - Char.code '0'
    | 'a' .. 'f' -> Char.code c - Char.code 'a' + 10
    | 'A' .. 'F' -> Char.code c - Char.code 'A' + 10
    | _ -> invalid_arg "bad hex"
  in
  for i = 0 to n - 1 do
    Array1.set b i (Char.chr ((digit s.[2 * i] lsl 4) lor digit s.[(2 * i) + 1]))
  done;
  b
;;

let () =
  let ctx = Secp256k1.Context.create [ Secp256k1.Context.Sign ] in
  let sk_hex = String.make 63 '0' ^ "1" in
  let sk = Secp256k1.Key.read_sk_exn ctx (buffer_of_hex sk_hex) in
  let pk = Secp256k1.Key.neuterize_exn ctx sk in
  let pub = Secp256k1.Key.to_bytes ~compress:false ctx pk in
  check
    "uncompressed public key"
    "0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8"
    (hex_of_buffer pub);
  let pub_str =
    let n = Array1.dim pub in
    let s = Bytes.create n in
    for i = 0 to n - 1 do
      Bytes.set s i (Array1.get pub i)
    done;
    Bytes.unsafe_to_string s
  in
  let digest =
    Digestif.KECCAK_256.(digest_string (String.sub pub_str 1 64) |> to_raw_string)
  in
  check
    "ethereum address"
    "7e5f4552091a69125d5dfcb7b8c2659029395bdf"
    (String.sub digest 12 20
     |> fun s ->
     let b = Bytes.of_string s in
     let out = Buffer.create 40 in
     Bytes.iter
       (fun c ->
         let v = Char.code c in
         Buffer.add_char out hex.[v lsr 4];
         Buffer.add_char out hex.[v land 0xf])
       b;
     Buffer.contents out);
  let msg = Secp256k1.Sign.msg_of_bytes_exn (buffer_of_hex (String.make 64 '0')) in
  let signature = Secp256k1.Sign.sign_recoverable_exn ctx ~sk msg in
  let r_s, recid = Secp256k1.Sign.to_bytes_recid ctx signature in
  check
    "signature r||s"
    "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce52"
    (hex_of_buffer r_s);
  check "recovery id" "1" (string_of_int recid);
  if !failures > 0 then exit 1
;;
