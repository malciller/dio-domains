(** In-tree stand-in for the mirage-crypto-rng package.

    The engine's only direct use is [Mirage_crypto_rng_unix.use_default ()] in
    [bin/main.ml]. WebSocket masking keys are produced by [Websocket.Rng], which is backed
    by OCaml's [Random] state in the vendored websocket module.

    No cryptographic primitive in this tree uses this module: secp256k1 and OpenSSL keep
    their own CSPRNGs. [generate] nevertheless reads from the OS CSPRNG so any future
    caller gets real entropy. *)

let generate ~len =
  let ic = open_in_bin "/dev/urandom" in
  let b = Bytes.create len in
  Fun.protect ~finally:(fun () -> close_in_noerr ic) (fun () -> really_input ic b 0 len);
  Bytes.unsafe_to_string b
;;
