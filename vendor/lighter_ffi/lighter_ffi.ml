(** In-tree FFI to the precompiled Lighter Go signer.

    The Go shared library is the audited Lighter signing implementation; this module owns
    the OCaml side and replaces ctypes/ctypes-foreign. All calls are serialized by
    [Lighter_signer.signer_mutex] because the Go library is not thread safe. *)

external load : string -> unit = "dio_lighter_load"

external create_client
  :  string
  -> string
  -> int
  -> int
  -> int64
  -> string
  = "dio_lighter_create_client"

external check_client : int -> int64 -> string = "dio_lighter_check_client"

external create_auth_token
  :  int64
  -> int
  -> int64
  -> string
  = "dio_lighter_create_auth_token"

external sign_create_order
  :  int
  -> int64
  -> int64
  -> int
  -> int
  -> int
  -> int
  -> int
  -> int
  -> int64
  -> int64
  -> int
  -> int64
  -> string
  = "dio_lighter_sign_create_order_bytecode" "dio_lighter_sign_create_order"

external sign_cancel_order
  :  int
  -> int64
  -> int64
  -> int
  -> int64
  -> string
  = "dio_lighter_sign_cancel_order"

external sign_modify_order
  :  int
  -> int64
  -> int64
  -> int64
  -> int64
  -> int
  -> int64
  -> string
  = "dio_lighter_sign_modify_order_bytecode" "dio_lighter_sign_modify_order"

external sign_cancel_all_orders
  :  int
  -> int64
  -> int64
  -> int
  -> int64
  -> string
  = "dio_lighter_sign_cancel_all_orders"
