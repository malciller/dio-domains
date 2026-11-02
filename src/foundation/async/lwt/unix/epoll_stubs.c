/* epoll poller for the in-tree Lwt engine.
 *
 * The engine keeps its own watcher registry; this stub is only the readiness
 * syscall. That keeps the platform-independent logic (timer heap, per-fd
 * watcher lists, dispatch) in OCaml and testable on any platform, and confines
 * the Linux-only code to four thin functions.
 *
 * On non-Linux platforms every entry point compiles to a raiser so the whole
 * tree still builds and runs on macOS against the select backend. */

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/fail.h>
#include <caml/threads.h>

#if defined(__linux__)

#include <sys/epoll.h>
#include <errno.h>
#include <string.h>
#include <caml/unixsupport.h>

/* Mirrors the masks in lwt_engine.ml. */
#define DIO_POLL_IN 1
#define DIO_POLL_OUT 2

static uint32_t eps_of_mask(int mask)
{
  uint32_t eps = 0;
  if (mask & DIO_POLL_IN) eps |= EPOLLIN;
  if (mask & DIO_POLL_OUT) eps |= EPOLLOUT;
  /* Hang-up / error must wake the reader so it observes EOF or the error. */
  eps |= EPOLLRDHUP;
  return eps;
}

CAMLprim value dio_epoll_supported(value unit)
{
  CAMLparam1(unit);
  CAMLreturn(Val_true);
}

/* The three EPOLL_CTL_* op codes, exported from the header rather than
 * restated on the OCaml side. Hardcoding them is how ADD/DEL/MOD got swapped
 * once already: MOD-with-an-empty-mask is accepted by the kernel and leaves the
 * fd registered, so the next ADD on that fd fails with EEXIST forever. */
CAMLprim value dio_epoll_ops(value unit)
{
  CAMLparam1(unit);
  CAMLlocal1(res);
  res = caml_alloc_tuple(3);
  Store_field(res, 0, Val_int(EPOLL_CTL_ADD));
  Store_field(res, 1, Val_int(EPOLL_CTL_DEL));
  Store_field(res, 2, Val_int(EPOLL_CTL_MOD));
  CAMLreturn(res);
}

CAMLprim value dio_epoll_create(value unit)
{
  CAMLparam1(unit);
  int fd = epoll_create1(EPOLL_CLOEXEC);
  if (fd == -1) uerror("epoll_create1", Nothing);
  CAMLreturn(Val_int(fd));
}

/* op: 1 = ADD, 2 = MOD, 3 = DEL (mirrors EPOLL_CTL_*). */
CAMLprim value dio_epoll_ctl(value v_op, value v_epfd, value v_fd, value v_mask)
{
  CAMLparam4(v_op, v_epfd, v_fd, v_mask);
  struct epoll_event ev;
  memset(&ev, 0, sizeof ev);
  ev.events = eps_of_mask(Int_val(v_mask));
  ev.data.fd = Int_val(v_fd);
  if (epoll_ctl(Int_val(v_epfd), Int_val(v_op), Int_val(v_fd), &ev) == -1)
    uerror("epoll_ctl", Nothing);
  CAMLreturn(Val_unit);
}

/* Returns a flat int array [fd0; mask0; fd1; mask1; ...]. One allocation per
 * wait, not per event: the loop already allocates a list of ready actions. */
CAMLprim value dio_epoll_wait(value v_epfd, value v_max, value v_timeout_ms)
{
  CAMLparam3(v_epfd, v_max, v_timeout_ms);
  CAMLlocal1(res);
  /* Copy every OCaml value out before releasing the runtime lock: reading a
   * value while another thread may run the GC is not safe. */
  int epfd = Int_val(v_epfd);
  int max = Int_val(v_max);
  int timeout = Int_val(v_timeout_ms);
  int n;
  int saved_errno;
  struct epoll_event *evs =
    (struct epoll_event *)caml_stat_alloc(sizeof(struct epoll_event) * (size_t)max);
  caml_enter_blocking_section();
  n = epoll_wait(epfd, evs, max, timeout);
  saved_errno = errno;
  caml_leave_blocking_section();
  if (n == -1) {
    caml_stat_free(evs);
    if (saved_errno == EINTR) CAMLreturn(caml_alloc(0, 0));
    errno = saved_errno;
    uerror("epoll_wait", Nothing);
  }
  res = caml_alloc(2 * n, 0);
  for (int i = 0; i < n; i++) {
    int mask = 0;
    uint32_t e = evs[i].events;
    if (e & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) mask |= DIO_POLL_IN;
    if (e & EPOLLOUT) mask |= DIO_POLL_OUT;
    Store_field(res, 2 * i, Val_int(evs[i].data.fd));
    Store_field(res, (2 * i) + 1, Val_int(mask));
  }
  caml_stat_free(evs);
  CAMLreturn(res);
}

#else /* !__linux__ */

static value dio_epoll_unavailable(void)
{
  caml_failwith("epoll backend is only available on Linux");
}

CAMLprim value dio_epoll_ops(value unit)
{
  CAMLparam1(unit);
  CAMLlocal1(res);
  /* Placeholders; never used because [supported] is false. */
  res = caml_alloc_tuple(3);
  Store_field(res, 0, Val_int(1));
  Store_field(res, 1, Val_int(2));
  Store_field(res, 2, Val_int(3));
  CAMLreturn(res);
}

CAMLprim value dio_epoll_supported(value unit)
{
  CAMLparam1(unit);
  CAMLreturn(Val_false);
}

CAMLprim value dio_epoll_create(value unit)
{
  CAMLparam1(unit);
  dio_epoll_unavailable();
  CAMLreturn(Val_int(0));
}

CAMLprim value dio_epoll_ctl(value v_op, value v_epfd, value v_fd, value v_mask)
{
  CAMLparam4(v_op, v_epfd, v_fd, v_mask);
  dio_epoll_unavailable();
  CAMLreturn(Val_unit);
}

CAMLprim value dio_epoll_wait(value v_epfd, value v_max, value v_timeout_ms)
{
  CAMLparam3(v_epfd, v_max, v_timeout_ms);
  dio_epoll_unavailable();
  CAMLreturn(caml_alloc(0, 0));
}

#endif
