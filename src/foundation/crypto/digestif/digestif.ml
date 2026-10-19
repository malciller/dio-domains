(** In-tree cryptographic hashes used by the exchange adapters.

    This replaces the [digestif] package with the primitives the engine actually uses:
    SHA-1 (WebSocket handshake), SHA-256 and SHA-512 (Kraken request signing),
    HMAC-SHA-512 (Kraken), and Keccak-256 (Hyperliquid/EIP-712).

    The API mirrors the subset of digestif's interface that the call sites use, so
    [Digestif.SHA256.digest_string], [Digestif.SHA512.hmac_string] and
    [Digestif.KECCAK_256.to_raw_string] keep working unchanged.

    These are pure OCaml implementations. They are not constant-time and are used for
    message authentication and public hashing, not for secret-dependent branching in
    signing. Signing itself remains in libsecp256k1. *)

let to_raw_string x = x
let of_raw_string x = x
let equal = String.equal
let compare = String.compare

let to_hex s =
  let hex = "0123456789abcdef" in
  let n = String.length s in
  let b = Bytes.create (n * 2) in
  for i = 0 to n - 1 do
    let v = Char.code s.[i] in
    Bytes.set b (2 * i) hex.[v lsr 4];
    Bytes.set b ((2 * i) + 1) hex.[v land 0xf]
  done;
  Bytes.unsafe_to_string b
;;

let of_hex s =
  let n = String.length s in
  if n mod 2 <> 0 then invalid_arg "Digestif.of_hex: odd length";
  let digit c =
    match c with
    | '0' .. '9' -> Char.code c - Char.code '0'
    | 'a' .. 'f' -> Char.code c - Char.code 'a' + 10
    | 'A' .. 'F' -> Char.code c - Char.code 'A' + 10
    | _ -> invalid_arg "Digestif.of_hex: invalid hex digit"
  in
  let b = Bytes.create (n / 2) in
  for i = 0 to (n / 2) - 1 do
    Bytes.set b i (Char.chr ((digit s.[2 * i] lsl 4) lor digit s.[(2 * i) + 1]))
  done;
  Bytes.unsafe_to_string b
;;

let substring ?(off = 0) ?len s =
  match len with
  | Some len -> String.sub s off len
  | None -> String.sub s off (String.length s - off)
;;

let get_le64 b i =
  let open Int64 in
  let byte j = of_int (Char.code (Bytes.get b (i + j))) in
  logor
    (byte 0)
    (logor
       (shift_left (byte 1) 8)
       (logor
          (shift_left (byte 2) 16)
          (logor
             (shift_left (byte 3) 24)
             (logor
                (shift_left (byte 4) 32)
                (logor
                   (shift_left (byte 5) 40)
                   (logor (shift_left (byte 6) 48) (shift_left (byte 7) 56)))))))
;;

let le64 b i v =
  for k = 0 to 7 do
    Bytes.set
      b
      (i + k)
      (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical v (8 * k)) 0xffL)))
  done
;;

let be32 b i v =
  Bytes.set b i (Char.chr (Int32.to_int (Int32.shift_right_logical v 24) land 0xff));
  Bytes.set b (i + 1) (Char.chr (Int32.to_int (Int32.shift_right_logical v 16) land 0xff));
  Bytes.set b (i + 2) (Char.chr (Int32.to_int (Int32.shift_right_logical v 8) land 0xff));
  Bytes.set b (i + 3) (Char.chr (Int32.to_int v land 0xff))
;;

let be64 b i v =
  for k = 0 to 7 do
    let shift = 56 - (8 * k) in
    Bytes.set
      b
      (i + k)
      (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical v shift) 0xffL)))
  done
;;

let get_be32 b i =
  let byte j = Char.code (Bytes.get b (i + j)) in
  Int32.logor
    (Int32.shift_left (Int32.of_int (byte 0)) 24)
    (Int32.logor
       (Int32.shift_left (Int32.of_int (byte 1)) 16)
       (Int32.logor (Int32.shift_left (Int32.of_int (byte 2)) 8) (Int32.of_int (byte 3))))
;;

let get_be64 b i =
  let open Int64 in
  let byte j = of_int (Char.code (Bytes.get b (i + j))) in
  logor
    (shift_left (byte 0) 56)
    (logor
       (shift_left (byte 1) 48)
       (logor
          (shift_left (byte 2) 40)
          (logor
             (shift_left (byte 3) 32)
             (logor
                (shift_left (byte 4) 24)
                (logor (shift_left (byte 5) 16) (logor (shift_left (byte 6) 8) (byte 7)))))))
;;

module type HASH = sig
  type t = string

  val digest_size : int
  val block_size : int
  val digest_string : ?off:int -> ?len:int -> string -> t
  val digest_bytes : ?off:int -> ?len:int -> Bytes.t -> t
  val to_raw_string : t -> string
  val of_raw_string : string -> t
  val to_hex : t -> string
  val of_hex : string -> t
  val equal : t -> t -> bool
  val compare : t -> t -> int
  val hmac_string : key:string -> ?off:int -> ?len:int -> string -> t
end

module SHA1 = struct
  type t = string

  let digest_size = 20
  let block_size = 64
  let to_raw_string (x : t) = x
  let of_raw_string (x : string) = x
  let to_hex (x : t) = to_hex x
  let of_hex (x : string) = of_hex x
  let equal (a : t) (b : t) = equal a b
  let compare (a : t) (b : t) = compare a b
  let rotl x n = Int32.logor (Int32.shift_left x n) (Int32.shift_right_logical x (32 - n))

  let f t b c d =
    if t < 20
    then Int32.logor (Int32.logand b c) (Int32.logand (Int32.lognot b) d)
    else if t < 40
    then Int32.logxor b (Int32.logxor c d)
    else if t < 60
    then
      Int32.logor (Int32.logand b c) (Int32.logor (Int32.logand b d) (Int32.logand c d))
    else Int32.logxor b (Int32.logxor c d)
  ;;

  let k t =
    if t < 20
    then 0x5a827999l
    else if t < 40
    then 0x6ed9eba1l
    else if t < 60
    then 0x8f1bbcdcl
    else 0xca62c1d6l
  ;;

  let process state block off =
    let w = Array.make 80 0l in
    for i = 0 to 15 do
      w.(i) <- get_be32 block (off + (i * 4))
    done;
    for i = 16 to 79 do
      w.(i)
      <- rotl
           (Int32.logxor
              (Int32.logxor w.(i - 3) w.(i - 8))
              (Int32.logxor w.(i - 14) w.(i - 16)))
           1
    done;
    let a = ref state.(0) in
    let b = ref state.(1) in
    let c = ref state.(2) in
    let d = ref state.(3) in
    let e = ref state.(4) in
    for t = 0 to 79 do
      let temp =
        Int32.add
          (Int32.add (rotl !a 5) (f t !b !c !d))
          (Int32.add (Int32.add !e (k t)) w.(t))
      in
      e := !d;
      d := !c;
      c := rotl !b 30;
      b := !a;
      a := temp
    done;
    state.(0) <- Int32.add state.(0) !a;
    state.(1) <- Int32.add state.(1) !b;
    state.(2) <- Int32.add state.(2) !c;
    state.(3) <- Int32.add state.(3) !d;
    state.(4) <- Int32.add state.(4) !e
  ;;

  let digest_string ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let len = String.length msg in
    let pad = (56 - ((len + 1) mod 64) + 64) mod 64 in
    let total = len + 1 + pad + 8 in
    let b = Bytes.make total '\000' in
    Bytes.blit_string msg 0 b 0 len;
    Bytes.set b len '\x80';
    be64 b (total - 8) (Int64.mul (Int64.of_int len) 8L);
    let state = [| 0x67452301l; 0xefcdab89l; 0x98badcfel; 0x10325476l; 0xc3d2e1f0l |] in
    for i = 0 to (total / 64) - 1 do
      process state b (i * 64)
    done;
    let out = Bytes.create 20 in
    for i = 0 to 4 do
      be32 out (i * 4) state.(i)
    done;
    Bytes.unsafe_to_string out
  ;;

  let digest_bytes ?(off = 0) ?len b =
    let len =
      match len with
      | Some l -> l
      | None -> Bytes.length b - off
    in
    digest_string (Bytes.sub_string b off len)
  ;;

  let hmac_string ~key ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let key = if String.length key > block_size then digest_string key else key in
    let key = key ^ String.make (block_size - String.length key) '\000' in
    let ipad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x36)) in
    let opad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x5c)) in
    digest_string (opad ^ digest_string (ipad ^ msg))
  ;;
end

module SHA256 = struct
  type t = string

  let digest_size = 32
  let block_size = 64
  let to_raw_string (x : t) = x
  let of_raw_string (x : string) = x
  let to_hex (x : t) = to_hex x
  let of_hex (x : string) = of_hex x
  let equal (a : t) (b : t) = equal a b
  let compare (a : t) (b : t) = compare a b

  let k =
    [| 0x428a2f98l
     ; 0x71374491l
     ; 0xb5c0fbcfl
     ; 0xe9b5dba5l
     ; 0x3956c25bl
     ; 0x59f111f1l
     ; 0x923f82a4l
     ; 0xab1c5ed5l
     ; 0xd807aa98l
     ; 0x12835b01l
     ; 0x243185bel
     ; 0x550c7dc3l
     ; 0x72be5d74l
     ; 0x80deb1fel
     ; 0x9bdc06a7l
     ; 0xc19bf174l
     ; 0xe49b69c1l
     ; 0xefbe4786l
     ; 0x0fc19dc6l
     ; 0x240ca1ccl
     ; 0x2de92c6fl
     ; 0x4a7484aal
     ; 0x5cb0a9dcl
     ; 0x76f988dal
     ; 0x983e5152l
     ; 0xa831c66dl
     ; 0xb00327c8l
     ; 0xbf597fc7l
     ; 0xc6e00bf3l
     ; 0xd5a79147l
     ; 0x06ca6351l
     ; 0x14292967l
     ; 0x27b70a85l
     ; 0x2e1b2138l
     ; 0x4d2c6dfcl
     ; 0x53380d13l
     ; 0x650a7354l
     ; 0x766a0abbl
     ; 0x81c2c92el
     ; 0x92722c85l
     ; 0xa2bfe8a1l
     ; 0xa81a664bl
     ; 0xc24b8b70l
     ; 0xc76c51a3l
     ; 0xd192e819l
     ; 0xd6990624l
     ; 0xf40e3585l
     ; 0x106aa070l
     ; 0x19a4c116l
     ; 0x1e376c08l
     ; 0x2748774cl
     ; 0x34b0bcb5l
     ; 0x391c0cb3l
     ; 0x4ed8aa4al
     ; 0x5b9cca4fl
     ; 0x682e6ff3l
     ; 0x748f82eel
     ; 0x78a5636fl
     ; 0x84c87814l
     ; 0x8cc70208l
     ; 0x90befffal
     ; 0xa4506cebl
     ; 0xbef9a3f7l
     ; 0xc67178f2l
    |]
  ;;

  let rotr x n = Int32.logor (Int32.shift_right_logical x n) (Int32.shift_left x (32 - n))

  let process state block off =
    let w = Array.make 64 0l in
    for i = 0 to 15 do
      w.(i) <- get_be32 block (off + (i * 4))
    done;
    for i = 16 to 63 do
      let s0 =
        Int32.logxor
          (Int32.logxor (rotr w.(i - 15) 7) (rotr w.(i - 15) 18))
          (Int32.shift_right_logical w.(i - 15) 3)
      in
      let s1 =
        Int32.logxor
          (Int32.logxor (rotr w.(i - 2) 17) (rotr w.(i - 2) 19))
          (Int32.shift_right_logical w.(i - 2) 10)
      in
      w.(i) <- Int32.add (Int32.add w.(i - 16) s0) (Int32.add w.(i - 7) s1)
    done;
    let a = ref state.(0) in
    let b = ref state.(1) in
    let c = ref state.(2) in
    let d = ref state.(3) in
    let e = ref state.(4) in
    let f = ref state.(5) in
    let g = ref state.(6) in
    let h = ref state.(7) in
    for i = 0 to 63 do
      let s1 = Int32.logxor (Int32.logxor (rotr !e 6) (rotr !e 11)) (rotr !e 25) in
      let ch = Int32.logxor (Int32.logand !e !f) (Int32.logand (Int32.lognot !e) !g) in
      let temp1 = Int32.add (Int32.add (Int32.add !h s1) (Int32.add ch k.(i))) w.(i) in
      let s0 = Int32.logxor (Int32.logxor (rotr !a 2) (rotr !a 13)) (rotr !a 22) in
      let maj =
        Int32.logxor
          (Int32.logxor (Int32.logand !a !b) (Int32.logand !a !c))
          (Int32.logand !b !c)
      in
      let temp2 = Int32.add s0 maj in
      h := !g;
      g := !f;
      f := !e;
      e := Int32.add !d temp1;
      d := !c;
      c := !b;
      b := !a;
      a := Int32.add temp1 temp2
    done;
    state.(0) <- Int32.add state.(0) !a;
    state.(1) <- Int32.add state.(1) !b;
    state.(2) <- Int32.add state.(2) !c;
    state.(3) <- Int32.add state.(3) !d;
    state.(4) <- Int32.add state.(4) !e;
    state.(5) <- Int32.add state.(5) !f;
    state.(6) <- Int32.add state.(6) !g;
    state.(7) <- Int32.add state.(7) !h
  ;;

  let digest_string ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let len = String.length msg in
    let pad = (56 - ((len + 1) mod 64) + 64) mod 64 in
    let total = len + 1 + pad + 8 in
    let b = Bytes.make total '\000' in
    Bytes.blit_string msg 0 b 0 len;
    Bytes.set b len '\x80';
    be64 b (total - 8) (Int64.mul (Int64.of_int len) 8L);
    let state =
      [| 0x6a09e667l
       ; 0xbb67ae85l
       ; 0x3c6ef372l
       ; 0xa54ff53al
       ; 0x510e527fl
       ; 0x9b05688cl
       ; 0x1f83d9abl
       ; 0x5be0cd19l
      |]
    in
    for i = 0 to (total / 64) - 1 do
      process state b (i * 64)
    done;
    let out = Bytes.create 32 in
    for i = 0 to 7 do
      be32 out (i * 4) state.(i)
    done;
    Bytes.unsafe_to_string out
  ;;

  let digest_bytes ?(off = 0) ?len b =
    let len =
      match len with
      | Some l -> l
      | None -> Bytes.length b - off
    in
    digest_string (Bytes.sub_string b off len)
  ;;

  let hmac_string ~key ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let key = if String.length key > block_size then digest_string key else key in
    let key = key ^ String.make (block_size - String.length key) '\000' in
    let ipad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x36)) in
    let opad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x5c)) in
    digest_string (opad ^ digest_string (ipad ^ msg))
  ;;
end

module SHA512 = struct
  type t = string

  let digest_size = 64
  let block_size = 128
  let to_raw_string (x : t) = x
  let of_raw_string (x : string) = x
  let to_hex (x : t) = to_hex x
  let of_hex (x : string) = of_hex x
  let equal (a : t) (b : t) = equal a b
  let compare (a : t) (b : t) = compare a b

  let k =
    [| 0x428a2f98d728ae22L
     ; 0x7137449123ef65cdL
     ; 0xb5c0fbcfec4d3b2fL
     ; 0xe9b5dba58189dbbcL
     ; 0x3956c25bf348b538L
     ; 0x59f111f1b605d019L
     ; 0x923f82a4af194f9bL
     ; 0xab1c5ed5da6d8118L
     ; 0xd807aa98a3030242L
     ; 0x12835b0145706fbeL
     ; 0x243185be4ee4b28cL
     ; 0x550c7dc3d5ffb4e2L
     ; 0x72be5d74f27b896fL
     ; 0x80deb1fe3b1696b1L
     ; 0x9bdc06a725c71235L
     ; 0xc19bf174cf692694L
     ; 0xe49b69c19ef14ad2L
     ; 0xefbe4786384f25e3L
     ; 0x0fc19dc68b8cd5b5L
     ; 0x240ca1cc77ac9c65L
     ; 0x2de92c6f592b0275L
     ; 0x4a7484aa6ea6e483L
     ; 0x5cb0a9dcbd41fbd4L
     ; 0x76f988da831153b5L
     ; 0x983e5152ee66dfabL
     ; 0xa831c66d2db43210L
     ; 0xb00327c898fb213fL
     ; 0xbf597fc7beef0ee4L
     ; 0xc6e00bf33da88fc2L
     ; 0xd5a79147930aa725L
     ; 0x06ca6351e003826fL
     ; 0x142929670a0e6e70L
     ; 0x27b70a8546d22ffcL
     ; 0x2e1b21385c26c926L
     ; 0x4d2c6dfc5ac42aedL
     ; 0x53380d139d95b3dfL
     ; 0x650a73548baf63deL
     ; 0x766a0abb3c77b2a8L
     ; 0x81c2c92e47edaee6L
     ; 0x92722c851482353bL
     ; 0xa2bfe8a14cf10364L
     ; 0xa81a664bbc423001L
     ; 0xc24b8b70d0f89791L
     ; 0xc76c51a30654be30L
     ; 0xd192e819d6ef5218L
     ; 0xd69906245565a910L
     ; 0xf40e35855771202aL
     ; 0x106aa07032bbd1b8L
     ; 0x19a4c116b8d2d0c8L
     ; 0x1e376c085141ab53L
     ; 0x2748774cdf8eeb99L
     ; 0x34b0bcb5e19b48a8L
     ; 0x391c0cb3c5c95a63L
     ; 0x4ed8aa4ae3418acbL
     ; 0x5b9cca4f7763e373L
     ; 0x682e6ff3d6b2b8a3L
     ; 0x748f82ee5defb2fcL
     ; 0x78a5636f43172f60L
     ; 0x84c87814a1f0ab72L
     ; 0x8cc702081a6439ecL
     ; 0x90befffa23631e28L
     ; 0xa4506cebde82bde9L
     ; 0xbef9a3f7b2c67915L
     ; 0xc67178f2e372532bL
     ; 0xca273eceea26619cL
     ; 0xd186b8c721c0c207L
     ; 0xeada7dd6cde0eb1eL
     ; 0xf57d4f7fee6ed178L
     ; 0x06f067aa72176fbaL
     ; 0x0a637dc5a2c898a6L
     ; 0x113f9804bef90daeL
     ; 0x1b710b35131c471bL
     ; 0x28db77f523047d84L
     ; 0x32caab7b40c72493L
     ; 0x3c9ebe0a15c9bebcL
     ; 0x431d67c49c100d4cL
     ; 0x4cc5d4becb3e42b6L
     ; 0x597f299cfc657e2aL
     ; 0x5fcb6fab3ad6faecL
     ; 0x6c44198c4a475817L
    |]
  ;;

  let rotr x n = Int64.logor (Int64.shift_right_logical x n) (Int64.shift_left x (64 - n))

  let process state block off =
    let w = Array.make 80 0L in
    for i = 0 to 15 do
      w.(i) <- get_be64 block (off + (i * 8))
    done;
    for i = 16 to 79 do
      let s0 =
        Int64.logxor
          (Int64.logxor (rotr w.(i - 15) 1) (rotr w.(i - 15) 8))
          (Int64.shift_right_logical w.(i - 15) 7)
      in
      let s1 =
        Int64.logxor
          (Int64.logxor (rotr w.(i - 2) 19) (rotr w.(i - 2) 61))
          (Int64.shift_right_logical w.(i - 2) 6)
      in
      w.(i) <- Int64.add (Int64.add w.(i - 16) s0) (Int64.add w.(i - 7) s1)
    done;
    let a = ref state.(0) in
    let b = ref state.(1) in
    let c = ref state.(2) in
    let d = ref state.(3) in
    let e = ref state.(4) in
    let f = ref state.(5) in
    let g = ref state.(6) in
    let h = ref state.(7) in
    for i = 0 to 79 do
      let s1 = Int64.logxor (Int64.logxor (rotr !e 14) (rotr !e 18)) (rotr !e 41) in
      let ch = Int64.logxor (Int64.logand !e !f) (Int64.logand (Int64.lognot !e) !g) in
      let temp1 = Int64.add (Int64.add (Int64.add !h s1) (Int64.add ch k.(i))) w.(i) in
      let s0 = Int64.logxor (Int64.logxor (rotr !a 28) (rotr !a 34)) (rotr !a 39) in
      let maj =
        Int64.logxor
          (Int64.logxor (Int64.logand !a !b) (Int64.logand !a !c))
          (Int64.logand !b !c)
      in
      let temp2 = Int64.add s0 maj in
      h := !g;
      g := !f;
      f := !e;
      e := Int64.add !d temp1;
      d := !c;
      c := !b;
      b := !a;
      a := Int64.add temp1 temp2
    done;
    state.(0) <- Int64.add state.(0) !a;
    state.(1) <- Int64.add state.(1) !b;
    state.(2) <- Int64.add state.(2) !c;
    state.(3) <- Int64.add state.(3) !d;
    state.(4) <- Int64.add state.(4) !e;
    state.(5) <- Int64.add state.(5) !f;
    state.(6) <- Int64.add state.(6) !g;
    state.(7) <- Int64.add state.(7) !h
  ;;

  let digest_string ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let len = String.length msg in
    let pad = (112 - ((len + 1) mod 128) + 128) mod 128 in
    let total = len + 1 + pad + 16 in
    let b = Bytes.make total '\000' in
    Bytes.blit_string msg 0 b 0 len;
    Bytes.set b len '\x80';
    (* The high 64 bits of the 128-bit length are zero for any message this process can
       hold; the low 64 bits carry the bit length. *)
    be64 b (total - 8) (Int64.mul (Int64.of_int len) 8L);
    let state =
      [| 0x6a09e667f3bcc908L
       ; 0xbb67ae8584caa73bL
       ; 0x3c6ef372fe94f82bL
       ; 0xa54ff53a5f1d36f1L
       ; 0x510e527fade682d1L
       ; 0x9b05688c2b3e6c1fL
       ; 0x1f83d9abfb41bd6bL
       ; 0x5be0cd19137e2179L
      |]
    in
    for i = 0 to (total / 128) - 1 do
      process state b (i * 128)
    done;
    let out = Bytes.create 64 in
    for i = 0 to 7 do
      be64 out (i * 8) state.(i)
    done;
    Bytes.unsafe_to_string out
  ;;

  let digest_bytes ?(off = 0) ?len b =
    let len =
      match len with
      | Some l -> l
      | None -> Bytes.length b - off
    in
    digest_string (Bytes.sub_string b off len)
  ;;

  let hmac_string ~key ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let key = if String.length key > block_size then digest_string key else key in
    let key = key ^ String.make (block_size - String.length key) '\000' in
    let ipad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x36)) in
    let opad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x5c)) in
    digest_string (opad ^ digest_string (ipad ^ msg))
  ;;
end

module KECCAK_256 = struct
  type t = string

  let digest_size = 32
  let to_raw_string (x : t) = x
  let of_raw_string (x : string) = x
  let to_hex (x : t) = to_hex x
  let of_hex (x : string) = of_hex x
  let equal (a : t) (b : t) = equal a b
  let compare (a : t) (b : t) = compare a b
  let rate = 136

  let rotl x n =
    if n = 0
    then x
    else Int64.logor (Int64.shift_left x n) (Int64.shift_right_logical x (64 - n))
  ;;

  let rc =
    [| 0x0000000000000001L
     ; 0x0000000000008082L
     ; 0x800000000000808aL
     ; 0x8000000080008000L
     ; 0x000000000000808bL
     ; 0x0000000080000001L
     ; 0x8000000080008081L
     ; 0x8000000000008009L
     ; 0x000000000000008aL
     ; 0x0000000000000088L
     ; 0x0000000080008009L
     ; 0x000000008000000aL
     ; 0x000000008000808bL
     ; 0x800000000000008bL
     ; 0x8000000000008089L
     ; 0x8000000000008003L
     ; 0x8000000000008002L
     ; 0x8000000000000080L
     ; 0x000000000000800aL
     ; 0x800000008000000aL
     ; 0x8000000080008081L
     ; 0x8000000000008080L
     ; 0x0000000080000001L
     ; 0x8000000080008008L
    |]
  ;;

  let rotc =
    [| 0
     ; 1
     ; 62
     ; 28
     ; 27
     ; 36
     ; 44
     ; 6
     ; 55
     ; 20
     ; 3
     ; 10
     ; 43
     ; 25
     ; 39
     ; 41
     ; 45
     ; 15
     ; 21
     ; 8
     ; 18
     ; 2
     ; 61
     ; 56
     ; 14
    |]
  ;;

  let permute st =
    let c = Array.make 5 0L in
    let d = Array.make 5 0L in
    let b = Array.make 25 0L in
    for round = 0 to 23 do
      for x = 0 to 4 do
        c.(x)
        <- Int64.logxor
             st.(x)
             (Int64.logxor
                st.(x + 5)
                (Int64.logxor st.(x + 10) (Int64.logxor st.(x + 15) st.(x + 20))))
      done;
      for x = 0 to 4 do
        d.(x) <- Int64.logxor c.((x + 4) mod 5) (rotl c.((x + 1) mod 5) 1)
      done;
      for x = 0 to 4 do
        for y = 0 to 4 do
          st.(x + (5 * y)) <- Int64.logxor st.(x + (5 * y)) d.(x)
        done
      done;
      for x = 0 to 4 do
        for y = 0 to 4 do
          b.((y + (5 * ((2 * x) + (3 * y)))) mod 25)
          <- rotl st.(x + (5 * y)) rotc.(x + (5 * y))
        done
      done;
      for x = 0 to 4 do
        for y = 0 to 4 do
          st.(x + (5 * y))
          <- Int64.logxor
               b.(x + (5 * y))
               (Int64.logand
                  (Int64.lognot b.(((x + 1) mod 5) + (5 * y)))
                  b.(((x + 2) mod 5) + (5 * y)))
        done
      done;
      st.(0) <- Int64.logxor st.(0) rc.(round)
    done
  ;;

  let digest_string ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let len = String.length msg in
    let pad = rate - (len mod rate) in
    let total = len + pad in
    let b = Bytes.make total '\000' in
    Bytes.blit_string msg 0 b 0 len;
    Bytes.set b len '\x01';
    Bytes.set b (total - 1) (Char.chr (Char.code (Bytes.get b (total - 1)) lor 0x80));
    let st = Array.make 25 0L in
    for block = 0 to (total / rate) - 1 do
      for lane = 0 to (rate / 8) - 1 do
        st.(lane) <- Int64.logxor st.(lane) (get_le64 b ((block * rate) + (lane * 8)))
      done;
      permute st
    done;
    let out = Bytes.create 32 in
    for i = 0 to 3 do
      le64 out (i * 8) st.(i)
    done;
    Bytes.unsafe_to_string out
  ;;

  let digest_bytes ?(off = 0) ?len b =
    let len =
      match len with
      | Some l -> l
      | None -> Bytes.length b - off
    in
    digest_string (Bytes.sub_string b off len)
  ;;

  let hmac_string ~key ?(off = 0) ?len s =
    let msg = substring ~off ?len s in
    let block_size = rate in
    let key = if String.length key > block_size then digest_string key else key in
    let key = key ^ String.make (block_size - String.length key) '\000' in
    let ipad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x36)) in
    let opad = String.init block_size (fun i -> Char.chr (Char.code key.[i] lxor 0x5c)) in
    digest_string (opad ^ digest_string (ipad ^ msg))
  ;;
end
