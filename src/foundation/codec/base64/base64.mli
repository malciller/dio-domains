type alphabet
type sub = string * int * int

val default_alphabet : alphabet
val uri_safe_alphabet : alphabet
val make_alphabet : string -> alphabet
val length_alphabet : alphabet -> int
val alphabet : alphabet -> string

val encode
  :  ?pad:bool
  -> ?alphabet:alphabet
  -> ?off:int
  -> ?len:int
  -> string
  -> (string, [> `Msg of string ]) result

val encode_exn
  :  ?pad:bool
  -> ?alphabet:alphabet
  -> ?off:int
  -> ?len:int
  -> string
  -> string

val encode_string : ?pad:bool -> ?alphabet:alphabet -> string -> string

val decode
  :  ?pad:bool
  -> ?alphabet:alphabet
  -> ?off:int
  -> ?len:int
  -> string
  -> (string, [> `Msg of string ]) result

val decode_exn
  :  ?pad:bool
  -> ?alphabet:alphabet
  -> ?off:int
  -> ?len:int
  -> string
  -> string
