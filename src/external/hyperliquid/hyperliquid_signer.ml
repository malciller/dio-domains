(** Hyperliquid EIP-712 Signing utility *)

module Int64 = Stdlib.Int64

let hex_char_val c =
  match c with
  | '0'..'9' -> Char.code c - Char.code '0'
  | 'a'..'f' -> Char.code c - Char.code 'a' + 10
  | 'A'..'F' -> Char.code c - Char.code 'A' + 10
  | _ -> failwith "invalid hex"

let bytes_of_hex s =
  let len = String.length s / 2 in
  let b = Bytes.create len in
  for i = 0 to len - 1 do
    let high = hex_char_val s.[i * 2] in
    let low = hex_char_val s.[i * 2 + 1] in
    Bytes.set b i (Char.chr (high lsl 4 lor low))
  done;
  Bytes.to_string b

let hex_of_bytes s =
  let len = String.length s in
  let b = Bytes.create (len * 2) in
  let hex_chars = "0123456789abcdef" in
  for i = 0 to len - 1 do
    let v = Char.code s.[i] in
    Bytes.set b (i * 2) hex_chars.[v lsr 4];
    Bytes.set b (i * 2 + 1) hex_chars.[v land 15]
  done;
  Bytes.to_string b

(** Convert an integer to an 8-byte big-endian string *)
let encode_uint64_be n =
  let buf = Bytes.create 8 in
  Bytes.set_int64_be buf 0 n;
  Bytes.to_string buf

(** Generate an action hash according to Hyperliquid's rules:
    msgpack(action) + nonce + [vault] + [expires_after] 
*)
let action_hash ~action_msgpack ~nonce ~vault_address ~expires_after =
  let buf = Buffer.create 128 in
  
  (* 1. msgpack(action) *)
  Buffer.add_string buf action_msgpack;
  
  (* 2. nonce (uint64, 8 bytes) *)
  Buffer.add_string buf (encode_uint64_be nonce);
  
  (* 3. vault_address *)
  (match vault_address with
  | None -> Buffer.add_char buf '\x00'
  | Some addr -> 
      Buffer.add_char buf '\x01';
      (* Convert 0x string to bytes *)
      let clean_addr = if String.starts_with ~prefix:"0x" addr then String.sub addr 2 (String.length addr - 2) else addr in
      let addr_bytes = bytes_of_hex clean_addr in
      Buffer.add_string buf addr_bytes
  );
  
  (* 4. expires_after *)
  (match expires_after with
  | None -> ()
  | Some expiration -> 
      Buffer.add_char buf '\x00';
      Buffer.add_string buf (encode_uint64_be expiration)
  );
  
  (* keccak256 *)
  let digest = Digestif.KECCAK_256.digest_string (Buffer.contents buf) in
  Digestif.KECCAK_256.to_raw_string digest

(** Helper: Keccak256 of string *)
let keccak256_str s =
  Digestif.KECCAK_256.to_raw_string (Digestif.KECCAK_256.digest_string s)

(** Domain Hash precomputation *)
let domain_type_hash = keccak256_str "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
let domain_name_hash = keccak256_str "Exchange"
let domain_version_hash = keccak256_str "1"
let domain_chain_id = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x39" (* 1337 in 32-bye BE *)
let domain_contract = String.make 32 '\x00'

let hash_domain () =
  let buf = Buffer.create 160 in
  Buffer.add_string buf domain_type_hash;
  Buffer.add_string buf domain_name_hash;
  Buffer.add_string buf domain_version_hash;
  Buffer.add_string buf domain_chain_id;
  Buffer.add_string buf domain_contract;
  keccak256_str (Buffer.contents buf)

(** Agent Struct Precomputation *)
let agent_type_hash = keccak256_str "Agent(string source,bytes32 connectionId)"

let hash_agent ~source_str ~connection_id_raw =
  let buf = Buffer.create 96 in
  Buffer.add_string buf agent_type_hash;
  Buffer.add_string buf (keccak256_str source_str);
  Buffer.add_string buf connection_id_raw;
  keccak256_str (Buffer.contents buf)

(** EIP-712 encode
    \x19\x01 || hashDomain || hashAgent 
*)
let eip712_digest ~hash_struct =
  let buf = Buffer.create 66 in
  Buffer.add_string buf "\x19\x01";
  Buffer.add_string buf (hash_domain ());
  Buffer.add_string buf hash_struct;
  keccak256_str (Buffer.contents buf)

(** Sign a payload with secp256k1 given raw bytes *)
open Bigarray

let bs_of_string s =
  let len = String.length s in
  let bs = Array1.create char c_layout len in
  for i = 0 to len - 1 do
    Array1.set bs i (String.get s i)
  done;
  bs

let bs_sub_to_string bs ofs len =
  let s = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set s i (Array1.get bs (ofs + i))
  done;
  Bytes.to_string s

let sign_hash ~private_key_raw ~msg_hash_raw =
  let ctx = Secp256k1.Context.create [Secp256k1.Context.Sign] in
  let seckey = Secp256k1.Key.read_sk_exn ctx (bs_of_string private_key_raw) in
  let msg = Secp256k1.Sign.msg_of_bytes_exn (bs_of_string msg_hash_raw) in
  let sig_rec = Secp256k1.Sign.sign_recoverable_exn ctx ~sk:seckey msg in
  let (block, v) = Secp256k1.Sign.to_bytes_recid ctx sig_rec in
  let r = bs_sub_to_string block 0 32 in
  let s = bs_sub_to_string block 32 32 in
  let hex_r = hex_of_bytes r in
  let hex_s = hex_of_bytes s in
  (* Note: standard Ethereum mapping for v is +27. But in API some just expect 27/28 or 0/1, python returns `v=27 + recovery_id`. *)
  let v_eth = v + 27 in
  (hex_r, hex_s, v_eth)

(** Final wrapper for Hyperliquid L1 Action *)
let sign_l1_action ~private_key_hex ~action_msgpack ~nonce ~is_mainnet ~vault_address =
  let pkey_clean = if String.starts_with ~prefix:"0x" private_key_hex then String.sub private_key_hex 2 (String.length private_key_hex - 2) else private_key_hex in
  let private_key_raw = bytes_of_hex pkey_clean in
  
  let action_hash_raw = action_hash ~action_msgpack ~nonce ~vault_address ~expires_after:None in
  
  let source_str = if is_mainnet then "a" else "b" in
  let agent_hash = hash_agent ~source_str ~connection_id_raw:action_hash_raw in
  
  let msg_hash = eip712_digest ~hash_struct:agent_hash in
  sign_hash ~private_key_raw ~msg_hash_raw:msg_hash
