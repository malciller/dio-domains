/* Minimal in-tree FFI to the precompiled Lighter Go signer.
 *
 * Replaces ctypes/ctypes-foreign. The Go shared library is loaded with
 * dlopen; the C ABI structs and functions are declared here directly. */

#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <caml/alloc.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

typedef struct {
  char *str;
  char *err;
} StrOrErr;

typedef struct {
  uint8_t txType;
  char *txInfo;
  char *txHash;
  char *messageToSign;
  char *err;
} SignedTxResponse;

typedef char *(*create_client_fn)(char *, char *, int, int, int64_t);
typedef char *(*check_client_fn)(int, int64_t);
typedef StrOrErr (*create_auth_token_fn)(int64_t, int, int64_t);
typedef SignedTxResponse (*sign_create_order_fn)(
    int, int64_t, int64_t, int, int, int, int, int, int, int64_t, int64_t,
    int, int, uint8_t, int64_t, int, int64_t);
typedef SignedTxResponse (*sign_cancel_order_fn)(
    int, int64_t, uint8_t, int64_t, int, int64_t);
typedef SignedTxResponse (*sign_modify_order_fn)(
    int, int64_t, int64_t, int64_t, int64_t, int64_t, int, int, uint8_t,
    int64_t, int, int64_t);
typedef SignedTxResponse (*sign_cancel_all_orders_fn)(
    int, int64_t, uint8_t, int64_t, int, int64_t);

static void *lib = NULL;
static void (*free_fn)(void *) = NULL;
static create_client_fn create_client = NULL;
static check_client_fn check_client = NULL;
static create_auth_token_fn create_auth_token = NULL;
static sign_create_order_fn sign_create_order = NULL;
static sign_cancel_order_fn sign_cancel_order = NULL;
static sign_modify_order_fn sign_modify_order = NULL;
static sign_cancel_all_orders_fn sign_cancel_all_orders = NULL;

static void load_symbol(const char *name, void *target) {
  void *sym = dlsym(lib, name);
  if (sym == NULL) {
    char msg[256];
    snprintf(msg, sizeof(msg), "lighter signer: missing symbol %s", name);
    caml_failwith(msg);
  }
  memcpy(target, &sym, sizeof(sym));
}

#define LOAD(name, target) load_symbol(name, (void *)&target)

CAMLprim value dio_lighter_load(value vpath) {
  CAMLparam1(vpath);
  if (lib != NULL) {
    CAMLreturn(Val_unit);
  }
  lib = dlopen(String_val(vpath), RTLD_NOW | RTLD_LOCAL);
  if (lib == NULL) {
    const char *err = dlerror();
    caml_failwith(err == NULL ? "lighter signer: dlopen failed" : err);
  }
  LOAD("Free", free_fn);
  LOAD("CreateClient", create_client);
  LOAD("CheckClient", check_client);
  LOAD("CreateAuthToken", create_auth_token);
  LOAD("SignCreateOrder", sign_create_order);
  LOAD("SignCancelOrder", sign_cancel_order);
  LOAD("SignModifyOrder", sign_modify_order);
  LOAD("SignCancelAllOrders", sign_cancel_all_orders);
  CAMLreturn(Val_unit);
}

static void free_maybe(char *p) {
  if (p != NULL) {
    free_fn(p);
  }
}

static value copy_and_free(char *p) {
  CAMLparam0();
  CAMLlocal1(v);
  v = caml_copy_string(p == NULL ? "" : p);
  free_maybe(p);
  CAMLreturn(v);
}

CAMLprim value dio_lighter_create_client(value vurl, value vkey, value vchain,
                                         value vkey_index, value vacct) {
  CAMLparam5(vurl, vkey, vchain, vkey_index, vacct);
  CAMLlocal1(v);
  char *err =
      create_client((char *)String_val(vurl), (char *)String_val(vkey),
                    Int_val(vchain), Int_val(vkey_index), Int64_val(vacct));
  v = copy_and_free(err);
  CAMLreturn(v);
}

CAMLprim value dio_lighter_check_client(value vkey_index, value vacct) {
  CAMLparam2(vkey_index, vacct);
  CAMLlocal1(v);
  char *err = check_client(Int_val(vkey_index), Int64_val(vacct));
  v = copy_and_free(err);
  CAMLreturn(v);
}

CAMLprim value dio_lighter_create_auth_token(value vdeadline, value vkey_index,
                                             value vacct) {
  CAMLparam3(vdeadline, vkey_index, vacct);
  CAMLlocal1(v);
  StrOrErr r =
      create_auth_token(Int64_val(vdeadline), Int_val(vkey_index), Int64_val(vacct));
  if (r.err != NULL && r.err[0] != '\0') {
    char msg[1024];
    snprintf(msg, sizeof(msg), "%s", r.err);
    free_maybe(r.str);
    free_maybe(r.err);
    caml_failwith(msg);
  }
  v = caml_copy_string(r.str == NULL ? "" : r.str);
  free_maybe(r.str);
  free_maybe(r.err);
  CAMLreturn(v);
}

static value signed_tx_result(SignedTxResponse r) {
  CAMLparam0();
  CAMLlocal1(v);
  if (r.err != NULL && r.err[0] != '\0') {
    char msg[1024];
    snprintf(msg, sizeof(msg), "%s", r.err);
    free_maybe(r.txInfo);
    free_maybe(r.txHash);
    free_maybe(r.messageToSign);
    free_maybe(r.err);
    caml_failwith(msg);
  }
  v = caml_copy_string(r.txInfo == NULL ? "" : r.txInfo);
  free_maybe(r.txInfo);
  free_maybe(r.txHash);
  free_maybe(r.messageToSign);
  free_maybe(r.err);
  CAMLreturn(v);
}

CAMLprim value dio_lighter_sign_create_order(
    value vmarket, value vclient_order, value vbase_amount, value vprice,
    value vis_ask, value vorder_type, value vtif, value vreduce_only,
    value vtrigger_price, value vorder_expiry, value vnonce, value vkey_index,
    value vacct) {
  CAMLparam5(vmarket, vclient_order, vbase_amount, vprice, vis_ask);
  CAMLxparam5(vorder_type, vtif, vreduce_only, vtrigger_price, vorder_expiry);
  CAMLxparam3(vnonce, vkey_index, vacct);
  SignedTxResponse r = sign_create_order(
      Int_val(vmarket), Int64_val(vclient_order), Int64_val(vbase_amount),
      Int_val(vprice), Int_val(vis_ask), Int_val(vorder_type), Int_val(vtif),
      Int_val(vreduce_only), Int_val(vtrigger_price), Int64_val(vorder_expiry),
      0L, 0, 0, 0, Int64_val(vnonce), Int_val(vkey_index), Int64_val(vacct));
  CAMLreturn(signed_tx_result(r));
}

CAMLprim value dio_lighter_sign_cancel_order(value vmarket, value vorder,
                                             value vnonce, value vkey_index,
                                             value vacct) {
  CAMLparam5(vmarket, vorder, vnonce, vkey_index, vacct);
  SignedTxResponse r = sign_cancel_order(
      Int_val(vmarket), Int64_val(vorder), 0, Int64_val(vnonce),
      Int_val(vkey_index), Int64_val(vacct));
  CAMLreturn(signed_tx_result(r));
}

CAMLprim value dio_lighter_sign_modify_order(
    value vmarket, value vorder, value vbase_amount, value vprice,
    value vnonce, value vkey_index, value vacct) {
  CAMLparam5(vmarket, vorder, vbase_amount, vprice, vnonce);
  CAMLxparam2(vkey_index, vacct);
  SignedTxResponse r = sign_modify_order(
      Int_val(vmarket), Int64_val(vorder), Int64_val(vbase_amount),
      Int64_val(vprice), 0L, 0L, 0, 0, 0, Int64_val(vnonce),
      Int_val(vkey_index), Int64_val(vacct));
  CAMLreturn(signed_tx_result(r));
}

CAMLprim value dio_lighter_sign_cancel_all_orders(value vtif, value vtime,
                                                  value vnonce,
                                                  value vkey_index,
                                                  value vacct) {
  CAMLparam5(vtif, vtime, vnonce, vkey_index, vacct);
  SignedTxResponse r = sign_cancel_all_orders(
      Int_val(vtif), Int64_val(vtime), 0, Int64_val(vnonce),
      Int_val(vkey_index), Int64_val(vacct));
  CAMLreturn(signed_tx_result(r));
}

CAMLprim value dio_lighter_sign_create_order_bytecode(value *argv, int argn) {
  (void)argn;
  return dio_lighter_sign_create_order(
      argv[0], argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7],
      argv[8], argv[9], argv[10], argv[11], argv[12]);
}

CAMLprim value dio_lighter_sign_modify_order_bytecode(value *argv, int argn) {
  (void)argn;
  return dio_lighter_sign_modify_order(argv[0], argv[1], argv[2], argv[3],
                                       argv[4], argv[5], argv[6]);
}
