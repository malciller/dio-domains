open Lwt.Infix

type t = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let create n = Bigarray.Array1.create Bigarray.char Bigarray.c_layout n
let length = Bigarray.Array1.dim

(* memcpy-based range copies. The OCaml loops these replace ran one
   [unsafe_get]/[unsafe_set] per byte and dominated channel throughput. *)
external blit_to_bytes : t -> int -> Bytes.t -> int -> int -> unit = "dio_blit_to_bytes"

external blit_from_bytes
  :  Bytes.t
  -> int
  -> t
  -> int
  -> int
  -> unit
  = "dio_blit_from_bytes"

let unsafe_blit src srcoff dst dstoff len =
  for i = 0 to len - 1 do
    Bigarray.Array1.unsafe_set
      dst
      (dstoff + i)
      (Bigarray.Array1.unsafe_get src (srcoff + i))
  done
;;

(* [Unix.read]/[Unix.write] operate on [bytes], so bigarray I/O copies through a temporary
   [bytes]. *)
let read fd buffer pos len =
  let tmp = Bytes.create len in
  Lwt_unix.read fd tmp 0 len
  >|= fun n ->
  blit_from_bytes tmp 0 buffer pos n;
  n
;;

let write fd buffer pos len =
  let tmp = Bytes.create len in
  blit_to_bytes buffer pos tmp 0 len;
  Lwt_unix.write fd tmp 0 len
;;
