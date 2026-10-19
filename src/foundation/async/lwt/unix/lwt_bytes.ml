open Lwt.Infix

type t = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let create n = Bigarray.Array1.create Bigarray.char Bigarray.c_layout n
let length = Bigarray.Array1.dim

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
  for i = 0 to n - 1 do
    Bigarray.Array1.unsafe_set buffer (pos + i) (Bytes.unsafe_get tmp i)
  done;
  n
;;

let write fd buffer pos len =
  let tmp = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.unsafe_set tmp i (Bigarray.Array1.unsafe_get buffer (pos + i))
  done;
  Lwt_unix.write fd tmp 0 len
;;
