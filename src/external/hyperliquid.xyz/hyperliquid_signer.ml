(** EIP-712 structured data signing for the Hyperliquid L1 action protocol.
    Implements domain separation, Agent struct hashing, and secp256k1 ECDSA
    signature generation with recovery ID per the Ethereum signing convention. *)

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

(** Encode a 64-bit integer as an 8-byte big-endian binary string. *)
let encode_uint64_be n =
  let buf = Bytes.create 8 in
  Bytes.set_int64_be buf 0 n;
  Bytes.to_string buf

(** Compute the Keccak-256 action hash per the Hyperliquid signing spec.
    Concatenates: msgpack(action) || nonce(u64) || vault_address? || expires_after?
    The vault and expiration fields are optional; vault uses a 0x00/0x01 presence tag. *)
let action_hash ~action_msgpack ~nonce ~vault_address ~expires_after =
  let buf = Buffer.create 128 in
  
  (* Append the msgpack-serialized action payload. *)
  Buffer.add_string buf action_msgpack;
  
  (* Append nonce as a big-endian uint64 (8 bytes). *)
  Buffer.add_string buf (encode_uint64_be nonce);
  
  (* Append vault address with presence tag: 0x00 if absent, 0x01 + 20-byte address if present. *)
  (match vault_address with
  | None -> Buffer.add_char buf '\x00'
  | Some addr_raw -> 
      Buffer.add_char buf '\x01';
      let addr = String.lowercase_ascii addr_raw in
      (* Strip optional "0x" prefix and decode hex to raw bytes. *)
      let clean_addr = if String.starts_with ~prefix:"0x" addr then String.sub addr 2 (String.length addr - 2) else addr in
      let addr_bytes = bytes_of_hex clean_addr in
      Buffer.add_string buf addr_bytes
  );
  
  (* Append optional expiration: 0x00 tag followed by big-endian uint64 timestamp. *)
  (match expires_after with
  | None -> ()
  | Some expiration -> 
      Buffer.add_char buf '\x00';
      Buffer.add_string buf (encode_uint64_be expiration)
  );
  
  (* Compute Keccak-256 digest of the concatenated buffer. *)
  let digest = Digestif.KECCAK_256.digest_string (Buffer.contents buf) in
  Digestif.KECCAK_256.to_raw_string digest

(** Compute Keccak-256 of a string, returning the raw 32-byte digest. *)
let keccak256_str s =
  Digestif.KECCAK_256.to_raw_string (Digestif.KECCAK_256.digest_string s)

(** Precomputed EIP-712 domain separator type hash and field hashes.
    Domain: name="Exchange", version="1", verifyingContract=0x0. *)
let domain_type_hash = keccak256_str "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
let domain_name_hash = keccak256_str "Exchange"
let domain_version_hash = keccak256_str "1"
let domain_contract = String.make 32 '\x00'

let hash_domain ~chain_id =
  let chain_id_bytes = 
    let b = Bytes.make 32 '\x00' in
    Bytes.set_int64_be b 24 (Int64.of_int chain_id);
    Bytes.to_string b
  in
  let buf = Buffer.create 160 in
  Buffer.add_string buf domain_type_hash;
  Buffer.add_string buf domain_name_hash;
  Buffer.add_string buf domain_version_hash;
  Buffer.add_string buf chain_id_bytes;
  Buffer.add_string buf domain_contract;
  keccak256_str (Buffer.contents buf)

(** Precomputed EIP-712 type hash for the Agent struct: Agent(string source, bytes32 connectionId). *)
let agent_type_hash = keccak256_str "Agent(string source,bytes32 connectionId)"

let hash_agent ~source_str ~connection_id_raw =
  let buf = Buffer.create 96 in
  Buffer.add_string buf agent_type_hash;
  Buffer.add_string buf (keccak256_str source_str);
  Buffer.add_string buf connection_id_raw;
  keccak256_str (Buffer.contents buf)

(** Produce the final EIP-712 signing digest: 0x1901 || domainSeparator || structHash.
    Returns a 32-byte Keccak-256 hash suitable for ECDSA signing. *)
let eip712_digest ~chain_id ~hash_struct =
  let buf = Buffer.create 66 in
  Buffer.add_string buf "\x19\x01";
  Buffer.add_string buf (hash_domain ~chain_id);
  Buffer.add_string buf hash_struct;
  keccak256_str (Buffer.contents buf)

(** Secp256k1 ECDSA signing utilities using the libsecp256k1 C bindings. *)
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
  (* Ethereum convention: v = recovery_id + 27, yielding 27 or 28. *)
  let v_eth = v + 27 in
  (hex_r, hex_s, v_eth)

(** Derive the Ethereum address (0x-prefixed, lowercase hex) from a private key.
    Computes the uncompressed public key, hashes it with Keccak-256,
    and extracts the last 20 bytes as the address. *)
let address_of_private_key_hex ~private_key_hex =
  let pkey_clean = if String.starts_with ~prefix:"0x" private_key_hex then String.sub private_key_hex 2 (String.length private_key_hex - 2) else private_key_hex in
  let private_key_raw = bytes_of_hex pkey_clean in
  let ctx = Secp256k1.Context.create [Secp256k1.Context.Sign] in
  let seckey = Secp256k1.Key.read_sk_exn ctx (bs_of_string private_key_raw) in
  let pubkey = Secp256k1.Key.neuterize_exn ctx seckey in
  (* Serialize uncompressed public key (65 bytes: 0x04 || x || y), hash with Keccak-256, take last 20 bytes. *)
  let pubkey_buf = Secp256k1.Key.to_bytes ~compress:false ctx pubkey in
  let pubkey_str = Array1.dim pubkey_buf |> fun n -> let s = Bytes.create n in for i = 0 to n - 1 do Bytes.set s i (Array1.get pubkey_buf i) done; Bytes.to_string s in
  let hash = Digestif.KECCAK_256.digest_string pubkey_str in
  let hash_raw = Digestif.KECCAK_256.to_raw_string hash in
  let addr_bytes = String.sub hash_raw 12 20 in
  "0x" ^ hex_of_bytes addr_bytes

(** Sign a Hyperliquid L1 action. Computes the action hash, constructs the EIP-712
    Agent struct, produces the typed data digest, and signs with the given private key.
    Returns (r, s, v) as hex strings and integer recovery value. *)
let sign_l1_action ?expires_after ~private_key_hex ~action_msgpack ~nonce ~is_mainnet ~vault_address () =
  let pkey_clean = if String.starts_with ~prefix:"0x" private_key_hex then String.sub private_key_hex 2 (String.length private_key_hex - 2) else private_key_hex in
  let private_key_raw = bytes_of_hex pkey_clean in
  
  let action_hash_raw = action_hash ~action_msgpack ~nonce ~vault_address ~expires_after in
  
  let source_str = if is_mainnet then "a" else "b" in
  let chain_id = 1337 in
  let agent_hash = hash_agent ~source_str ~connection_id_raw:action_hash_raw in
  
  let msg_hash = eip712_digest ~chain_id ~hash_struct:agent_hash in
  sign_hash ~private_key_raw ~msg_hash_raw:msg_hash