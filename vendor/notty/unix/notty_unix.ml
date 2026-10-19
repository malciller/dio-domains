(** Minimal [Notty_unix] for the dashboard.

    The dashboard only reads the terminal size; rendering goes through
    [Notty.Render.to_buffer] and is written to stdout directly. *)

external c_winsize : Unix.file_descr -> int = "caml_notty_winsize" [@@noalloc]

let winsize fd =
  match c_winsize fd with
  | 0 -> None
  | wh -> Some (wh lsr 16, (wh lsr 1) land 0x7fff)
;;
