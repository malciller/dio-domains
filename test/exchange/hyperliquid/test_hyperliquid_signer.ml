let test_hex_conversion () =
  let test_str = "hello" in
  let as_hex = Hyperliquid.Hyperliquid_signer.hex_of_bytes test_str in
  let back_to_bytes = Hyperliquid.Hyperliquid_signer.bytes_of_hex as_hex in
  Alcotest.(check string) "hex conversion roundtrip" test_str back_to_bytes

let test_domain_hash () =
  let h = Hyperliquid.Hyperliquid_signer.hash_domain () in
  (* Should be a 32 byte keccak256 hash (64 hex chars if it was hex, but it's raw string so 32 chars) *)
  Alcotest.(check int) "domain hash is 32 raw bytes" 32 (String.length h)

let test_agent_hash () =
  let source_str = "a" in
  let connection_id_raw = String.make 32 '\x00' in
  let h = Hyperliquid.Hyperliquid_signer.hash_agent ~source_str ~connection_id_raw in
  Alcotest.(check int) "agent hash is 32 raw bytes" 32 (String.length h)

let test_action_hash () =
  let msgpack = "\x81\xa4type\xa5order" in (* generic fake msgpack *)
  let nonce = 123456789L in
  let vault_address = None in
  let h = Hyperliquid.Hyperliquid_signer.action_hash ~action_msgpack:msgpack ~nonce ~vault_address ~expires_after:None in
  Alcotest.(check int) "action hash is 32 raw bytes" 32 (String.length h)

let test_eip712_digest () =
  let agent_hash = String.make 32 '\x01' in
  let digest = Hyperliquid.Hyperliquid_signer.eip712_digest ~hash_struct:agent_hash in
  Alcotest.(check int) "eip712 digest is 32 raw bytes" 32 (String.length digest)

let test_signature_generation () =
  (* Test with a known / dummy private key *)
  let dummy_priv_key_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" in
  let dummy_priv_key_raw = Hyperliquid.Hyperliquid_signer.bytes_of_hex dummy_priv_key_hex in
  let dummy_msg_hash_raw = String.make 32 '\x42' in

  let (r, s, v) = Hyperliquid.Hyperliquid_signer.sign_hash ~private_key_raw:dummy_priv_key_raw ~msg_hash_raw:dummy_msg_hash_raw in
  
  Alcotest.(check int) "r length is 64 hex chars" 64 (String.length r);
  Alcotest.(check int) "s length is 64 hex chars" 64 (String.length s);
  Alcotest.(check bool) "v is Eth standard (>= 27)" true (v >= 27)

let () =
  Alcotest.run "Hyperliquid Signer" [
    "utilities", [
      Alcotest.test_case "hex_conversion" `Quick test_hex_conversion;
    ];
    "hashing", [
      Alcotest.test_case "domain_hash" `Quick test_domain_hash;
      Alcotest.test_case "agent_hash" `Quick test_agent_hash;
      Alcotest.test_case "action_hash" `Quick test_action_hash;
      Alcotest.test_case "eip712_digest" `Quick test_eip712_digest;
    ];
    "signing", [
      Alcotest.test_case "signature_generation" `Quick test_signature_generation;
    ]
  ]
