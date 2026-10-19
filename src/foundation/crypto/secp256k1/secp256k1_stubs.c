/* Minimal in-tree binding to libsecp256k1 for Hyperliquid/Ethereum signing.
   Only the operations the engine uses are exposed. The native library remains
   the audited libsecp256k1; this file is our OCaml-facing wrapper. */

#include <string.h>

#include <secp256k1.h>
#include <secp256k1_recovery.h>

#include <caml/alloc.h>
#include <caml/bigarray.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

#define Context_val(v) (*((secp256k1_context **) Data_custom_val(v)))

static void context_finalize(value v) {
  secp256k1_context *ctx = Context_val(v);
  if (ctx != NULL) {
    secp256k1_context_destroy(ctx);
  }
}

static struct custom_operations context_ops = {
    .identifier = "dio.secp256k1.context",
    .finalize = context_finalize,
    .compare = custom_compare_default,
    .compare_ext = custom_compare_ext_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default};

CAMLprim value dio_secp256k1_context_create(value vflags) {
  CAMLparam1(vflags);
  CAMLlocal1(result);
  secp256k1_context *ctx =
      secp256k1_context_create((unsigned int)Int_val(vflags));
  if (ctx == NULL) {
    caml_failwith("secp256k1_context_create");
  }
  result = caml_alloc_custom(&context_ops, sizeof(secp256k1_context *), 0, 1);
  Context_val(result) = ctx;
  CAMLreturn(result);
}

CAMLprim value dio_secp256k1_flag_sign(value unit) {
  (void)unit;
  return Val_int(SECP256K1_CONTEXT_SIGN);
}

CAMLprim value dio_secp256k1_flag_verify(value unit) {
  (void)unit;
  return Val_int(SECP256K1_CONTEXT_VERIFY);
}

CAMLprim value dio_secp256k1_context_randomize(value vctx, value vseed) {
  CAMLparam2(vctx, vseed);
  int ok = secp256k1_context_randomize(
      Context_val(vctx), (const unsigned char *)Caml_ba_data_val(vseed));
  CAMLreturn(Val_bool(ok));
}

CAMLprim value dio_secp256k1_seckey_verify(value vctx, value vsk) {
  CAMLparam2(vctx, vsk);
  int ok = secp256k1_ec_seckey_verify(
      Context_val(vctx), (const unsigned char *)Caml_ba_data_val(vsk));
  CAMLreturn(Val_bool(ok));
}

CAMLprim value dio_secp256k1_pubkey_create(value vctx, value vpk, value vsk) {
  CAMLparam3(vctx, vpk, vsk);
  int ok = secp256k1_ec_pubkey_create(
      Context_val(vctx),
      (secp256k1_pubkey *)Caml_ba_data_val(vpk),
      (const unsigned char *)Caml_ba_data_val(vsk));
  CAMLreturn(Val_bool(ok));
}

CAMLprim value dio_secp256k1_pubkey_serialize(value vctx, value vout,
                                              value vpk, value vcompress) {
  CAMLparam4(vctx, vout, vpk, vcompress);
  size_t len = (size_t)Caml_ba_array_val(vout)->dim[0];
  unsigned int flags = Bool_val(vcompress) ? SECP256K1_EC_COMPRESSED
                                           : SECP256K1_EC_UNCOMPRESSED;
  int ok = secp256k1_ec_pubkey_serialize(
      Context_val(vctx),
      (unsigned char *)Caml_ba_data_val(vout),
      &len,
      (const secp256k1_pubkey *)Caml_ba_data_val(vpk),
      flags);
  CAMLreturn(Val_bool(ok));
}

CAMLprim value dio_secp256k1_sign_recoverable(value vctx, value vsig,
                                              value vmsg, value vsk) {
  CAMLparam4(vctx, vsig, vmsg, vsk);
  int ok = secp256k1_ecdsa_sign_recoverable(
      Context_val(vctx),
      (secp256k1_ecdsa_recoverable_signature *)Caml_ba_data_val(vsig),
      (const unsigned char *)Caml_ba_data_val(vmsg),
      (const unsigned char *)Caml_ba_data_val(vsk),
      NULL,
      NULL);
  CAMLreturn(Val_bool(ok));
}

CAMLprim value dio_secp256k1_recoverable_serialize(value vctx, value vout,
                                                   value vsig) {
  CAMLparam3(vctx, vout, vsig);
  int recid = 0;
  int ok = secp256k1_ecdsa_recoverable_signature_serialize_compact(
      Context_val(vctx),
      (unsigned char *)Caml_ba_data_val(vout),
      &recid,
      (const secp256k1_ecdsa_recoverable_signature *)Caml_ba_data_val(vsig));
  if (!ok) {
    caml_failwith("secp256k1_ecdsa_recoverable_signature_serialize_compact");
  }
  CAMLreturn(Val_int(recid));
}
