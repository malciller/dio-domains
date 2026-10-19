module String : sig
  type 'a folder = 'a -> int -> [ `Uchar of Uchar.t | `Malformed of string ] -> 'a

  val fold_utf_8 : ?pos:int -> ?len:int -> 'a folder -> 'a -> string -> 'a
end
