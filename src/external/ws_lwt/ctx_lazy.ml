(* Conduit >= 3 (classic-flambda toolchain): [default_ctx : ctx Lazy.t]. *)

let resolve () = Lazy.force Conduit_lwt_unix.default_ctx
