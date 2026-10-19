/* In-tree OCaml binding to OpenSSL for the engine's TLS client.
 *
 * OpenSSL remains the audited native TLS implementation; this file owns the
 * OCaml-facing surface used by conduit/lwt_ssl. */

#include <string.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <caml/alloc.h>
#include <caml/bigarray.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

#define Context_val(v) (*((SSL_CTX **)Data_custom_val(v)))
#define Socket_val(v) (*((SSL **)Data_custom_val(v)))

static void context_finalize(value v) {
  SSL_CTX *ctx = Context_val(v);
  if (ctx != NULL) {
    SSL_CTX_free(ctx);
  }
}

static void socket_finalize(value v) {
  SSL *ssl = Socket_val(v);
  if (ssl != NULL) {
    SSL_free(ssl);
  }
}

static struct custom_operations context_ops = {
    .identifier = "dio.ssl.context",
    .finalize = context_finalize,
    .compare = custom_compare_default,
    .compare_ext = custom_compare_ext_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default};

static struct custom_operations socket_ops = {
    .identifier = "dio.ssl.socket",
    .finalize = socket_finalize,
    .compare = custom_compare_default,
    .compare_ext = custom_compare_ext_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default};

static value alloc_context(SSL_CTX *ctx) {
  value v = caml_alloc_custom(&context_ops, sizeof(SSL_CTX *), 0, 1);
  Context_val(v) = ctx;
  return v;
}

static value alloc_socket(SSL *ssl) {
  value v = caml_alloc_custom(&socket_ops, sizeof(SSL *), 0, 1);
  Socket_val(v) = ssl;
  return v;
}

static int protocol_version(int p) {
  return p == 1 ? TLS1_3_VERSION : TLS1_2_VERSION;
}

CAMLprim value dio_ssl_init(value unit) {
  (void)unit;
  OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS |
                       OPENSSL_INIT_LOAD_CRYPTO_STRINGS,
                   NULL);
  return Val_unit;
}

CAMLprim value dio_ssl_create_context(value vprotocol, value vtype) {
  CAMLparam2(vprotocol, vtype);
  CAMLlocal1(result);
  int type = Int_val(vtype);
  const SSL_METHOD *method =
      type == 1 ? TLS_server_method() : type == 2 ? TLS_method()
                                                    : TLS_client_method();
  SSL_CTX *ctx = SSL_CTX_new(method);
  if (ctx == NULL) {
    caml_failwith("SSL_CTX_new failed");
  }
  SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY |
                           SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  int version = protocol_version(Int_val(vprotocol));
  if (!SSL_CTX_set_min_proto_version(ctx, version) ||
      !SSL_CTX_set_max_proto_version(ctx, version)) {
    SSL_CTX_free(ctx);
    caml_failwith("SSL_CTX_set_proto_version failed");
  }
  result = alloc_context(ctx);
  CAMLreturn(result);
}

CAMLprim value dio_ssl_set_min_protocol_version(value vctx, value vprotocol) {
  CAMLparam2(vctx, vprotocol);
  int ok = SSL_CTX_set_min_proto_version(Context_val(vctx),
                                         protocol_version(Int_val(vprotocol)));
  if (!ok) {
    caml_failwith("SSL_CTX_set_min_proto_version failed");
  }
  CAMLreturn(Val_unit);
}

CAMLprim value dio_ssl_set_max_protocol_version(value vctx, value vprotocol) {
  CAMLparam2(vctx, vprotocol);
  int ok = SSL_CTX_set_max_proto_version(Context_val(vctx),
                                         protocol_version(Int_val(vprotocol)));
  if (!ok) {
    caml_failwith("SSL_CTX_set_max_proto_version failed");
  }
  CAMLreturn(Val_unit);
}

CAMLprim value dio_ssl_set_default_verify_paths(value vctx) {
  CAMLparam1(vctx);
  int ok = SSL_CTX_set_default_verify_paths(Context_val(vctx));
  CAMLreturn(Val_bool(ok == 1));
}

CAMLprim value dio_ssl_set_verify(value vctx, value vmodes, value vcallback) {
  CAMLparam3(vctx, vmodes, vcallback);
  (void)vcallback;
  int flags = 0;
  for (value l = vmodes; l != Val_emptylist; l = Field(l, 1)) {
    if (Int_val(Field(l, 0)) == 1) {
      flags |= SSL_VERIFY_PEER;
    }
  }
  SSL_CTX_set_verify(Context_val(vctx), flags, NULL);
  CAMLreturn(Val_unit);
}

CAMLprim value dio_ssl_embed_socket(value vfd, value vctx) {
  CAMLparam2(vfd, vctx);
  CAMLlocal1(result);
  SSL *ssl = SSL_new(Context_val(vctx));
  if (ssl == NULL) {
    caml_failwith("SSL_new failed");
  }
  if (!SSL_set_fd(ssl, Int_val(vfd))) {
    SSL_free(ssl);
    caml_failwith("SSL_set_fd failed");
  }
  result = alloc_socket(ssl);
  CAMLreturn(result);
}

CAMLprim value dio_ssl_set_client_sni_hostname(value vssl, value vhost) {
  CAMLparam2(vssl, vhost);
  if (!SSL_set_tlsext_host_name(Socket_val(vssl), String_val(vhost))) {
    caml_failwith("SSL_set_tlsext_host_name failed");
  }
  CAMLreturn(Val_unit);
}

CAMLprim value dio_ssl_set_hostflags(value vssl, value vflags) {
  CAMLparam2(vssl, vflags);
  unsigned long flags = 0;
  for (value l = vflags; l != Val_emptylist; l = Field(l, 1)) {
    if (Int_val(Field(l, 0)) == 0) {
      flags |= X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS;
    }
  }
  X509_VERIFY_PARAM_set_hostflags(SSL_get0_param(Socket_val(vssl)), flags);
  CAMLreturn(Val_unit);
}

CAMLprim value dio_ssl_set_host(value vssl, value vhost) {
  CAMLparam2(vssl, vhost);
  if (!X509_VERIFY_PARAM_set1_host(SSL_get0_param(Socket_val(vssl)),
                                   String_val(vhost), 0)) {
    caml_failwith("X509_VERIFY_PARAM_set1_host failed");
  }
  CAMLreturn(Val_unit);
}

static int error_code(SSL *ssl, int ret) {
  int err = SSL_get_error(ssl, ret);
  switch (err) {
  case SSL_ERROR_WANT_READ:
    return 1;
  case SSL_ERROR_WANT_WRITE:
    return 2;
  case SSL_ERROR_ZERO_RETURN:
    return 3;
  default:
    return 4;
  }
}

CAMLprim value dio_ssl_connect_raw(value vssl) {
  CAMLparam1(vssl);
  SSL *ssl = Socket_val(vssl);
  int ret = SSL_connect(ssl);
  CAMLreturn(Val_int(ret == 1 ? 0 : error_code(ssl, ret)));
}

CAMLprim value dio_ssl_accept_raw(value vssl) {
  CAMLparam1(vssl);
  SSL *ssl = Socket_val(vssl);
  int ret = SSL_accept(ssl);
  CAMLreturn(Val_int(ret == 1 ? 0 : error_code(ssl, ret)));
}

static int read_raw(SSL *ssl, char *buf, int pos, int len) {
  int ret = SSL_read(ssl, buf + pos, len);
  if (ret > 0) {
    return ret;
  }
  return -error_code(ssl, ret);
}

static int write_raw(SSL *ssl, const char *buf, int pos, int len) {
  int ret = SSL_write(ssl, buf + pos, len);
  if (ret > 0) {
    return ret;
  }
  return -error_code(ssl, ret);
}

static int check_range(value vbuf_len, int pos, int len) {
  return pos >= 0 && len >= 0 && pos + len <= vbuf_len;
}

CAMLprim value dio_ssl_read(value vssl, value vbuf, value vpos, value vlen) {
  CAMLparam4(vssl, vbuf, vpos, vlen);
  int pos = Int_val(vpos);
  int len = Int_val(vlen);
  if (!check_range(Val_int(caml_string_length(vbuf)), pos, len)) {
    caml_invalid_argument("Ssl.read: invalid bounds");
  }
  CAMLreturn(Val_int(read_raw(Socket_val(vssl), (char *)Bytes_val(vbuf), pos, len)));
}

CAMLprim value dio_ssl_write(value vssl, value vbuf, value vpos, value vlen) {
  CAMLparam4(vssl, vbuf, vpos, vlen);
  int pos = Int_val(vpos);
  int len = Int_val(vlen);
  if (!check_range(Val_int(caml_string_length(vbuf)), pos, len)) {
    caml_invalid_argument("Ssl.write: invalid bounds");
  }
  CAMLreturn(Val_int(write_raw(Socket_val(vssl), (const char *)Bytes_val(vbuf), pos, len)));
}

CAMLprim value dio_ssl_read_into_bigarray(value vssl, value vbuf, value vpos,
                                          value vlen) {
  CAMLparam4(vssl, vbuf, vpos, vlen);
  int pos = Int_val(vpos);
  int len = Int_val(vlen);
  int buflen = (int)Caml_ba_array_val(vbuf)->dim[0];
  if (!check_range(Val_int(buflen), pos, len)) {
    caml_invalid_argument("Ssl.read_into_bigarray: invalid bounds");
  }
  CAMLreturn(
      Val_int(read_raw(Socket_val(vssl), Caml_ba_data_val(vbuf), pos, len)));
}

CAMLprim value dio_ssl_write_bigarray(value vssl, value vbuf, value vpos,
                                      value vlen) {
  CAMLparam4(vssl, vbuf, vpos, vlen);
  int pos = Int_val(vpos);
  int len = Int_val(vlen);
  int buflen = (int)Caml_ba_array_val(vbuf)->dim[0];
  if (!check_range(Val_int(buflen), pos, len)) {
    caml_invalid_argument("Ssl.write_bigarray: invalid bounds");
  }
  CAMLreturn(Val_int(
      write_raw(Socket_val(vssl), Caml_ba_data_val(vbuf), pos, len)));
}

CAMLprim value dio_ssl_close_notify(value vssl) {
  CAMLparam1(vssl);
  int ret = SSL_shutdown(Socket_val(vssl));
  CAMLreturn(Val_bool(ret == 1));
}

CAMLprim value dio_ssl_shutdown(value vssl) {
  CAMLparam1(vssl);
  SSL_shutdown(Socket_val(vssl));
  CAMLreturn(Val_unit);
}

CAMLprim value dio_ssl_file_descr_of_socket(value vssl) {
  CAMLparam1(vssl);
  CAMLreturn(Val_int(SSL_get_fd(Socket_val(vssl))));
}

CAMLprim value dio_ssl_get_error_string(value unit) {
  CAMLparam1(unit);
  unsigned long err = ERR_get_error();
  if (err == 0) {
    CAMLreturn(caml_copy_string(""));
  }
  char buf[256];
  ERR_error_string_n(err, buf, sizeof(buf));
  CAMLreturn(caml_copy_string(buf));
}
