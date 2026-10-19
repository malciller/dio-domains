(** In-tree promise runtime.

    A compact, original implementation of the [Lwt] API surface used by dio. Promises are
    mutable cells with callback lists; resolution runs callbacks through a depth-limited
    trampoline so long chains cannot overflow the stack. The Unix event loop lives in
    [Lwt_unix]. *)

type +'a t
type 'a u

exception Canceled

val return : 'a -> 'a t
val return_unit : unit t
val return_none : 'a option t
val return_some : 'a -> 'a option t
val return_true : bool t
val return_false : bool t
val fail : exn -> 'a t
val fail_with : string -> 'a t
val bind : 'a t -> ('a -> 'b t) -> 'b t
val map : ('a -> 'b) -> 'a t -> 'b t
val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
val try_bind : (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t
val finalize : (unit -> 'a t) -> (unit -> unit t) -> 'a t
val wrap2 : ('a -> 'b -> 'c) -> 'a -> 'b -> 'c t
val apply : ('a -> 'b t) -> 'a -> 'b t
val backtrace_bind : string -> int -> (exn -> exn) -> 'a t -> ('a -> 'b t) -> 'b t

val backtrace_try_bind
  :  string
  -> int
  -> (exn -> exn)
  -> (unit -> 'a t)
  -> ('a -> 'b t)
  -> (exn -> 'b t)
  -> 'b t

val reraise : exn -> 'a
val wait : unit -> 'a t * 'a u
val task : unit -> 'a t * 'a u
val wakeup : 'a u -> 'a -> unit
val wakeup_later : 'a u -> 'a -> unit
val wakeup_exn : 'a u -> exn -> unit
val wakeup_later_exn : 'a u -> exn -> unit
val both : 'a t -> 'b t -> ('a * 'b) t
val join : unit t list -> unit t
val choose : 'a t list -> 'a t
val pick : 'a t list -> 'a t
val cancel : 'a t -> unit
val on_cancel : 'a t -> (unit -> unit) -> unit
val is_sleeping : 'a t -> bool
val async : (unit -> unit t) -> unit
val dont_wait : (unit -> unit t) -> (exn -> unit) -> unit
val on_any : 'a t -> ('a -> unit) -> (exn -> unit) -> unit
val async_exception_hook : (exn -> unit) ref
val pause : unit -> unit t
val poll : 'a t -> 'a option

(* Engine hooks, used by [Lwt_main]. *)
val wakeup_paused : unit -> unit
val paused_count : unit -> int
val abandon_paused : unit -> unit

module Infix : sig
  val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
  val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
end
