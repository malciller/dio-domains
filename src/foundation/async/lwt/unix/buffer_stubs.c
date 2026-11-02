/* Bulk copies between [Bytes.t] and the channel's [Lwt_bytes.t] bigarray.
 *
 * The channel implementation previously copied these ranges with an OCaml
 * loop (one [Bytes.unsafe_set] / [Bigarray.Array1.unsafe_get] per byte), which
 * dominates receive cost for large frames. memcpy is the same work with no
 * bounds checks and no per-byte function call. */

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/memory.h>
#include <string.h>

/* bigarray -> bytes */
CAMLprim value dio_blit_to_bytes(value v_src, value v_src_off, value v_dst, value v_dst_off,
                                 value v_len)
{
  CAMLparam5(v_src, v_src_off, v_dst, v_dst_off, v_len);
  char *src = (char *)Caml_ba_data_val(v_src);
  char *dst = (char *)Bytes_val(v_dst);
  memcpy(dst + Long_val(v_dst_off), src + Long_val(v_src_off), Long_val(v_len));
  CAMLreturn(Val_unit);
}

/* bytes -> bigarray */
CAMLprim value dio_blit_from_bytes(value v_src, value v_src_off, value v_dst,
                                   value v_dst_off, value v_len)
{
  CAMLparam5(v_src, v_src_off, v_dst, v_dst_off, v_len);
  char *src = (char *)Bytes_val(v_src);
  char *dst = (char *)Caml_ba_data_val(v_dst);
  memcpy(dst + Long_val(v_dst_off), src + Long_val(v_src_off), Long_val(v_len));
  CAMLreturn(Val_unit);
}
