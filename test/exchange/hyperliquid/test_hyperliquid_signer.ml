let test_hex_conversions () =
  let str = "hello" in
  let hex = Hyperliquid.Signer.hex_of_bytes str in
  Alcotest.(check string) "hex_of_bytes" "68656c6c6f" hex;
  let back = Hyperliquid.Signer.bytes_of_hex hex in
  Alcotest.(check string) "bytes_of_hex" str back

let test_encode_uint64_be () =
  let encoded = Hyperliquid.Signer.encode_uint64_be 1L in
  Alcotest.(check string) "encode_uint64_be 1" "\x00\x00\x00\x00\x00\x00\x00\x01" encoded

(* A simple smoke test to ensure the signing logic runs without crashing.
   Verifying exact correctness of the cryptographic output requires test vectors or
   an external web3 library, which is beyond this basic unit test. *)
let test_sign_l1_action () =
  (* Example private key (do not use in prod) *)
  let pkey = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" in
  let action_msgpack = "test_msgpack_data" in
  let nonce = 1234567890000L in
  let (r, s, v) = Hyperliquid.Signer.sign_l1_action
    ~private_key_hex:pkey
    ~action_msgpack
    ~nonce
    ~is_mainnet:true
    ~vault_address:None
    ()
  in
  Alcotest.(check bool) "r is hex" true (String.length r = 64);
  Alcotest.(check bool) "s is hex" true (String.length s = 64);
  Alcotest.(check bool) "v is valid" true (v >= 27 && v <= 28)

let () =
  Alcotest.run "Hyperliquid Signer" [
    "utils", [
      Alcotest.test_case "hex_conversions" `Quick test_hex_conversions;
      Alcotest.test_case "encode_uint64_be" `Quick test_encode_uint64_be;
    ];
    "crypto", [
      Alcotest.test_case "sign_l1_action (smoke test)" `Quick test_sign_l1_action;
    ]
  ]
