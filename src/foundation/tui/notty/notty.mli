(** In-tree terminal image library.

    This is an original, dependency-free reimplementation of the subset of the [notty] API
    used by the dashboard. Images are rectangles of styled text; [Render.to_buffer] turns
    them into terminal escape sequences. *)

(** Visual characteristics of displayed text. *)
type attr

(** Rectangles of styled characters. *)
type image

(** Attribute construction and composition. *)
module A : sig
  type color
  type style
  type t = attr

  val black : color
  val red : color
  val green : color
  val yellow : color
  val blue : color
  val magenta : color
  val cyan : color
  val white : color
  val lightblack : color
  val lightred : color
  val lightgreen : color
  val lightyellow : color
  val lightblue : color
  val lightmagenta : color
  val lightcyan : color
  val lightwhite : color
  val rgb : r:int -> g:int -> b:int -> color
  val gray : int -> color
  val rgb_888 : r:int -> g:int -> b:int -> color
  val bold : style
  val italic : style
  val underline : style
  val blink : style
  val reverse : style
  val equal : t -> t -> bool
  val empty : attr
  val ( ++ ) : attr -> attr -> attr
  val fg : color -> attr
  val bg : color -> attr
  val st : style -> attr
end

(** Image construction and composition. *)
module I : sig
  type t = image

  val width : image -> int
  val height : image -> int
  val equal : t -> t -> bool
  val empty : image
  val string : attr -> string -> image
  val uchars : attr -> Uchar.t array -> image
  val char : attr -> char -> int -> int -> image
  val uchar : attr -> Uchar.t -> int -> int -> image
  val void : int -> int -> image
  val ( <|> ) : image -> image -> image
  val ( <-> ) : image -> image -> image
  val ( </> ) : image -> image -> image
  val hcrop : int -> int -> image -> image
  val vcrop : int -> int -> image -> image
  val crop : ?l:int -> ?r:int -> ?t:int -> ?b:int -> image -> image
  val hpad : int -> int -> image -> image
  val vpad : int -> int -> image -> image
  val pad : ?l:int -> ?r:int -> ?t:int -> ?b:int -> image -> image
  val hcat : image list -> image
  val vcat : image list -> image
  val zcat : image list -> image
  val tabulate : int -> int -> (int -> int -> image) -> image
  val hsnap : ?align:[ `Left | `Middle | `Right ] -> int -> image -> image
  val vsnap : ?align:[ `Top | `Middle | `Bottom ] -> int -> image -> image
end

(** Operators, repeated. *)
module Infix : sig
  val ( <-> ) : image -> image -> image
  val ( <|> ) : image -> image -> image
  val ( </> ) : image -> image -> image
  val ( ++ ) : attr -> attr -> attr
end

(** Terminal capabilities. *)
module Cap : sig
  type t

  val ansi : t
  val dumb : t
end

(** Dump images to string buffers. *)
module Render : sig
  val to_buffer : Buffer.t -> Cap.t -> int * int -> int * int -> image -> unit
end
