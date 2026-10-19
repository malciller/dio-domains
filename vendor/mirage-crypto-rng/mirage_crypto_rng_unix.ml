(** In-tree stand-in for [Mirage_crypto_rng_unix]. *)

let use_default () = Random.self_init ()
