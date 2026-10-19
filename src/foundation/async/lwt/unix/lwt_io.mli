(** Buffered byte channels for the in-tree promise runtime. *)

exception Channel_closed of string

type 'a channel
type input
type output

type 'a mode =
  | Input : input mode
  | Output : output mode

val input : input mode
val output : output mode

type input_channel = input channel
type output_channel = output channel

val make
  :  ?buffer:Lwt_bytes.t
  -> ?close:(unit -> unit Lwt.t)
  -> mode:'mode mode
  -> (Lwt_bytes.t -> int -> int -> int Lwt.t)
  -> 'mode channel

val of_fd
  :  ?buffer:Lwt_bytes.t
  -> ?close:(unit -> unit Lwt.t)
  -> mode:'mode mode
  -> Lwt_unix.file_descr
  -> 'mode channel

val close : 'a channel -> unit Lwt.t
val abort : 'a channel -> unit Lwt.t
val flush : output_channel -> unit Lwt.t
val read_line_opt : input_channel -> string option Lwt.t
val read : ?count:int -> input_channel -> string Lwt.t
val read_into : input_channel -> bytes -> int -> int -> int Lwt.t
val read_into_exactly : input_channel -> bytes -> int -> int -> unit Lwt.t
val write : output_channel -> string -> unit Lwt.t
val write_from_exactly : output_channel -> bytes -> int -> int -> unit Lwt.t
