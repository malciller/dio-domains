(* Thin bindings for the Linux epoll poller. See epoll_stubs.c. *)

external supported : unit -> bool = "dio_epoll_supported"
external create : unit -> int = "dio_epoll_create"

(* The EPOLL_CTL_* op codes come from the C header, never restated here. Getting them
   wrong is silent: DEL and MOD both "succeed" with the wrong op, but leave the fd
   registered in the kernel, and the next ADD on that fd returns EEXIST for the life of
   the process. *)
external ops : unit -> int * int * int = "dio_epoll_ops"

(* (add, del, mod) *)
let ctl_add, ctl_del, ctl_mod = ops ()

(* Issue [op] (one of the three above) for [fd] with the readiness [mask]. *)
external ctl : int -> int -> int -> int -> unit = "dio_epoll_ctl"

(* Returns [fd0; mask0; fd1; mask1; ...]. *)
external wait : int -> int -> int -> int array = "dio_epoll_wait"
