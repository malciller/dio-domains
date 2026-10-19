(** In-tree binding to the audited libsecp256k1.

    The native library stays external; this module owns the OCaml side and exposes only
    the operations the engine uses. The API mirrors the subset of the former opam package
    that [hyperliquid_signer.ml] calls, so call sites do not change. *)

open Bigarray

type buffer = (char, int8_unsigned_elt, c_layout) Array1.t

let buffer_of_string s =
  let len = String.length s in
  let b = Array1.create char c_layout len in
  for i = 0 to len - 1 do
    Array1.set b i s.[i]
  done;
  b
;;

module Context = struct
  type flag =
    | Verify
    | Sign

  type t

  external create_raw : int -> t = "dio_secp256k1_context_create"
  external flag_verify : unit -> int = "dio_secp256k1_flag_verify"
  external flag_sign : unit -> int = "dio_secp256k1_flag_sign"
  external randomize_raw : t -> buffer -> bool = "dio_secp256k1_context_randomize"

  let create flags =
    let bits =
      List.fold_left
        (fun acc -> function
          | Verify -> acc lor flag_verify ()
          | Sign -> acc lor flag_sign ())
        0
        flags
    in
    let ctx = create_raw bits in
    let seed = buffer_of_string (Mirage_crypto_rng.generate ~len:32) in
    if not (randomize_raw ctx seed)
    then failwith "Secp256k1.Context.create: context randomize failed";
    ctx
  ;;

  let randomize ctx seed =
    if Array1.dim seed <> 32
    then invalid_arg "Secp256k1.Context.randomize: seed must be 32 bytes";
    randomize_raw ctx seed
  ;;
end

module Key = struct
  type secret
  type public

  type _ t =
    | Sk of buffer
    | Pk of buffer

  let secret_bytes = 32
  let public_bytes = 64

  external verify_sk : Context.t -> buffer -> bool = "dio_secp256k1_seckey_verify"

  external create_pk
    :  Context.t
    -> buffer
    -> buffer
    -> bool
    = "dio_secp256k1_pubkey_create"

  external serialize_pk
    :  Context.t
    -> buffer
    -> buffer
    -> bool
    -> bool
    = "dio_secp256k1_pubkey_serialize"

  let read_sk_exn ctx ?(pos = 0) buf =
    if pos < 0 || pos + secret_bytes > Array1.dim buf
    then invalid_arg "Secp256k1.Key.read_sk_exn: buffer too short";
    let sk = Array1.create char c_layout secret_bytes in
    Array1.blit (Array1.sub buf pos secret_bytes) sk;
    if not (verify_sk ctx sk)
    then invalid_arg "Secp256k1.Key.read_sk_exn: invalid secret key";
    Sk sk
  ;;

  let neuterize_exn ctx = function
    | Sk sk ->
      let pk = Array1.create char c_layout public_bytes in
      if not (create_pk ctx pk sk)
      then invalid_arg "Secp256k1.Key.neuterize_exn: public key creation failed";
      Pk pk
    | Pk _ -> invalid_arg "Secp256k1.Key.neuterize_exn: already a public key"
  ;;

  let to_bytes ?(compress = false) ctx = function
    | Sk sk -> sk
    | Pk pk ->
      let len = if compress then 33 else 65 in
      let out = Array1.create char c_layout len in
      if not (serialize_pk ctx out pk compress)
      then invalid_arg "Secp256k1.Key.to_bytes: serialization failed";
      out
  ;;
end

module Sign = struct
  type msg = buffer
  type recoverable = buffer

  external sign_recoverable_raw
    :  Context.t
    -> buffer
    -> buffer
    -> buffer
    -> bool
    = "dio_secp256k1_sign_recoverable"

  external serialize_compact
    :  Context.t
    -> buffer
    -> buffer
    -> int
    = "dio_secp256k1_recoverable_serialize"

  let msg_of_bytes_exn buf =
    if Array1.dim buf <> 32
    then invalid_arg "Secp256k1.Sign.msg_of_bytes_exn: message must be 32 bytes";
    buf
  ;;

  let sign_recoverable_exn ctx ~sk msg =
    let sk =
      match sk with
      | Key.Sk sk -> sk
      | Key.Pk _ -> invalid_arg "Secp256k1.Sign.sign_recoverable_exn: need a secret key"
    in
    let signature = Array1.create char c_layout 65 in
    if not (sign_recoverable_raw ctx signature msg sk)
    then invalid_arg "Secp256k1.Sign.sign_recoverable_exn: signing failed";
    signature
  ;;

  let to_bytes_recid ctx signature =
    let out = Array1.create char c_layout 64 in
    let recid = serialize_compact ctx out signature in
    out, recid
  ;;
end
