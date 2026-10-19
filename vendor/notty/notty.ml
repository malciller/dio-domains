(** In-tree terminal image library.

    Original implementation of the [notty] surface used by the dashboard. It has no
    platform dependencies: images are built as trees, measured with [width]/[height], and
    rendered to escape sequences by [Render.to_buffer]. *)

type color_ =
  | Basic of int
  | Rgb of int * int * int

type style_ =
  | Bold
  | Italic
  | Underline
  | Blink
  | Reverse

type attr =
  { fg : color_ option
  ; bg : color_ option
  ; styles : style_ list
  }

type image =
  | Empty
  | Text of attr * string
  | Char of attr * string * int * int
  | Void of int * int
  | Hcat of image * image
  | Vcat of image * image
  | Overlay of image * image
  | Hcrop of image * int * int
  | Vcrop of image * int * int

let color_of_basic n = Basic n

module A = struct
  type color = color_
  type style = style_
  type t = attr

  let black = color_of_basic 0
  let red = color_of_basic 1
  let green = color_of_basic 2
  let yellow = color_of_basic 3
  let blue = color_of_basic 4
  let magenta = color_of_basic 5
  let cyan = color_of_basic 6
  let white = color_of_basic 7
  let lightblack = color_of_basic 8
  let lightred = color_of_basic 9
  let lightgreen = color_of_basic 10
  let lightyellow = color_of_basic 11
  let lightblue = color_of_basic 12
  let lightmagenta = color_of_basic 13
  let lightcyan = color_of_basic 14
  let lightwhite = color_of_basic 15

  let rgb ~r ~g ~b =
    if r < 0 || g < 0 || b < 0 || r > 5 || g > 5 || b > 5
    then invalid_arg "Notty.A.rgb: channel out of range"
    else color_of_basic (16 + ((r * 36) + (g * 6) + b))
  ;;

  let gray level =
    if level < 0 || level > 23
    then invalid_arg "Notty.A.gray: level out of range"
    else color_of_basic (232 + level)
  ;;

  let rgb_888 ~r ~g ~b =
    if r < 0 || g < 0 || b < 0 || r > 255 || g > 255 || b > 255
    then invalid_arg "Notty.A.rgb_888: channel out of range"
    else Rgb (r, g, b)
  ;;

  let bold = Bold
  let italic = Italic
  let underline = Underline
  let blink = Blink
  let reverse = Reverse
  let empty = { fg = None; bg = None; styles = [] }
  let equal (a : attr) (b : attr) = a = b
  let add_style styles s = if List.mem s styles then styles else styles @ [ s ]

  let ( ++ ) (a : attr) (b : attr) =
    if a == empty
    then b
    else if b == empty
    then a
    else
      { fg =
          (match b.fg with
           | Some _ -> b.fg
           | None -> a.fg)
      ; bg =
          (match b.bg with
           | Some _ -> b.bg
           | None -> a.bg)
      ; styles = List.fold_left add_style a.styles b.styles
      }
  ;;

  let fg c = { empty with fg = Some c }
  let bg c = { empty with bg = Some c }
  let st s = { empty with styles = [ s ] }
end

let utf8_width s =
  Uutf.String.fold_utf_8
    (fun n _ -> function
      | `Uchar _ -> n + 1
      | `Malformed _ -> n + 1)
    0
    s
;;

(* Byte offset, byte length and code point for every decoded scalar. *)
let utf8_segments s =
  let segs = ref [] in
  Uutf.String.fold_utf_8
    (fun () start -> function
      | `Uchar u ->
        let code = Uchar.to_int u in
        let len =
          if code < 0x80
          then 1
          else if code < 0x800
          then 2
          else if code < 0x10000
          then 3
          else 4
        in
        segs := (start, len, code) :: !segs
      | `Malformed m -> segs := (start, max 1 (String.length m), -1) :: !segs)
    ()
    s;
  List.rev !segs
;;

let is_control code = code >= 0 && (code < 0x20 || (code >= 0x7f && code < 0xa0))

module I = struct
  type t = image

  let rec width = function
    | Empty -> 0
    | Text (_, s) -> utf8_width s
    | Char (_, _, w, _) -> w
    | Void (w, _) -> w
    | Hcat (a, b) -> width a + width b
    | Vcat (a, b) -> max (width a) (width b)
    | Overlay (a, b) -> max (width a) (width b)
    | Hcrop (img, l, r) -> width img - l - r
    | Vcrop (img, _, _) -> width img

  and height = function
    | Empty -> 0
    | Text _ -> 1
    | Char (_, _, _, h) -> h
    | Void (_, h) -> h
    | Hcat (a, b) -> max (height a) (height b)
    | Vcat (a, b) -> height a + height b
    | Overlay (a, b) -> max (height a) (height b)
    | Hcrop (img, _, _) -> height img
    | Vcrop (img, t, b) -> height img - t - b
  ;;

  let equal (a : image) (b : image) = a = b
  let empty = Empty

  let ( <|> ) t1 t2 =
    match t1, t2 with
    | Empty, _ -> t2
    | _, Empty -> t1
    | _ -> Hcat (t1, t2)
  ;;

  let ( <-> ) t1 t2 =
    match t1, t2 with
    | Empty, _ -> t2
    | _, Empty -> t1
    | _ -> Vcat (t1, t2)
  ;;

  let ( </> ) t1 t2 =
    match t1, t2 with
    | Empty, _ -> t2
    | _, Empty -> t1
    | _ -> Overlay (t1, t2)
  ;;

  let void w h =
    let w = max 0 w
    and h = max 0 h in
    if w < 1 && h < 1 then Empty else Void (w, h)
  ;;

  let string attr s = if s = "" then void 0 1 else Text (attr, s)

  let uchars attr us =
    if Array.length us = 0
    then void 0 1
    else (
      let buf = Buffer.create 16 in
      Array.iter (Buffer.add_utf_8_uchar buf) us;
      Text (attr, Buffer.contents buf))
  ;;

  let char attr c w h =
    if w < 1 || h < 1 then void w h else Char (attr, String.make 1 c, w, h)
  ;;

  let uchar attr u w h =
    if w < 1 || h < 1
    then void w h
    else (
      let buf = Buffer.create 4 in
      Buffer.add_utf_8_uchar buf u;
      Char (attr, Buffer.contents buf, w, h))
  ;;

  let rec hcrop left right img =
    if left >= 0 && right >= 0
    then (
      let w = width img - left - right in
      if w > 0 then Hcrop (img, left, right) else void w (height img))
    else if right < 0
    then hcrop left 0 img <|> void (-right) 0
    else void (-left) 0 <|> hcrop 0 right img
  ;;

  let rec vcrop top bottom img =
    if top >= 0 && bottom >= 0
    then (
      let h = height img - top - bottom in
      if h > 0 then Vcrop (img, top, bottom) else void (width img) h)
    else if bottom < 0
    then vcrop top 0 img <-> void 0 (-bottom)
    else void 0 (-top) <-> vcrop 0 bottom img
  ;;

  let crop ?(l = 0) ?(r = 0) ?(t = 0) ?(b = 0) img =
    let img = if l <> 0 || r <> 0 then hcrop l r img else img in
    if t <> 0 || b <> 0 then vcrop t b img else img
  ;;

  let hpad left right img = hcrop (-left) (-right) img
  let vpad top bottom img = vcrop (-top) (-bottom) img

  let pad ?(l = 0) ?(r = 0) ?(t = 0) ?(b = 0) img =
    crop ~l:(-l) ~r:(-r) ~t:(-t) ~b:(-b) img
  ;;

  let hcat xs = List.fold_left ( <|> ) empty xs
  let vcat xs = List.fold_left ( <-> ) empty xs
  let zcat xs = List.fold_right ( </> ) xs empty

  let tabulate m n f =
    let m = max m 0
    and n = max n 0 in
    vcat (List.init n (fun y -> hcat (List.init m (fun x -> f x y))))
  ;;

  let hsnap ?(align = `Middle) w img =
    let off = width img - w in
    match align with
    | `Left -> hcrop 0 off img
    | `Right -> hcrop off 0 img
    | `Middle ->
      let w1 = off / 2 in
      hcrop w1 (off - w1) img
  ;;

  let vsnap ?(align = `Middle) h img =
    let off = height img - h in
    match align with
    | `Top -> vcrop 0 off img
    | `Bottom -> vcrop off 0 img
    | `Middle ->
      let h1 = off / 2 in
      vcrop h1 (off - h1) img
  ;;
end

module Infix = struct
  let ( <-> ) = I.( <-> )
  let ( <|> ) = I.( <|> )
  let ( </> ) = I.( </> )
  let ( ++ ) = A.( ++ )
end

module Cap = struct
  type t =
    | Ansi
    | Dumb

  let ansi = Ansi
  let dumb = Dumb
end

module Render = struct
  type cell =
    { glyph : string
    ; attr : attr
    }

  type grid = cell option array array

  let grid_make w h = Array.init (max 0 h) (fun _ -> Array.make (max 0 w) None)

  let blit dst x y (src : grid) =
    let dh = Array.length dst in
    Array.iteri
      (fun sy srow ->
        let dy = y + sy in
        if dy >= 0 && dy < dh
        then (
          let drow = dst.(dy) in
          let dw = Array.length drow in
          Array.iteri
            (fun sx cell ->
              match cell with
              | Some _ ->
                let dx = x + sx in
                if dx >= 0 && dx < dw then drow.(dx) <- cell
              | None -> ())
            srow))
      src
  ;;

  let rec render (img : image) : grid =
    match img with
    | Empty -> [||]
    | Text (a, s) ->
      let segs = utf8_segments s in
      let row = Array.make (List.length segs) None in
      List.iteri
        (fun i (start, len, code) ->
          let glyph = if is_control code then " " else String.sub s start len in
          row.(i) <- Some { glyph; attr = a })
        segs;
      [| row |]
    | Char (a, g, w, h) ->
      let cell = Some { glyph = g; attr = a } in
      Array.init h (fun _ -> Array.make w cell)
    | Void (w, h) -> grid_make w h
    | Hcat (a, b) ->
      let ga = render a
      and gb = render b in
      let w = I.width a + I.width b in
      let h = max (I.height a) (I.height b) in
      let g = grid_make w h in
      blit g 0 0 ga;
      blit g (I.width a) 0 gb;
      g
    | Vcat (a, b) ->
      let ga = render a
      and gb = render b in
      let w = max (I.width a) (I.width b) in
      let h = I.height a + I.height b in
      let g = grid_make w h in
      blit g 0 0 ga;
      blit g 0 (I.height a) gb;
      g
    | Overlay (a, b) ->
      let ga = render a
      and gb = render b in
      let w = max (I.width a) (I.width b) in
      let h = max (I.height a) (I.height b) in
      let g = grid_make w h in
      blit g 0 0 gb;
      blit g 0 0 ga;
      g
    | Hcrop (img, l, r) ->
      let g = render img in
      let w = I.width img - l - r in
      let h = I.height img in
      let out = grid_make w h in
      Array.iteri
        (fun y row ->
          let dst = out.(y) in
          for x = 0 to w - 1 do
            dst.(x) <- row.(l + x)
          done)
        g;
      out
    | Vcrop (img, t, b) ->
      let g = render img in
      let w = I.width img in
      let h = I.height img - t - b in
      let out = grid_make w h in
      for y = 0 to h - 1 do
        Array.blit g.(t + y) 0 out.(y) 0 w
      done;
      out
  ;;

  let add_color buf ~bg c =
    let lead = if bg then 40 else 30
    and bright = if bg then 100 else 90 in
    match c with
    | Basic n ->
      if n < 8
      then Buffer.add_string buf (Printf.sprintf ";%d" (lead + n))
      else if n < 16
      then Buffer.add_string buf (Printf.sprintf ";%d" (bright + n - 8))
      else Buffer.add_string buf (Printf.sprintf ";%s;5;%d" (if bg then "48" else "38") n)
    | Rgb (r, g, b) ->
      Buffer.add_string
        buf
        (Printf.sprintf ";%s;2;%d;%d;%d" (if bg then "48" else "38") r g b)
  ;;

  let style_code = function
    | Bold -> 1
    | Italic -> 3
    | Underline -> 4
    | Blink -> 5
    | Reverse -> 7
  ;;

  let sgr buf (a : attr) =
    Buffer.add_string buf "\x1b[0";
    (match a.fg with
     | Some c -> add_color buf ~bg:false c
     | None -> ());
    (match a.bg with
     | Some c -> add_color buf ~bg:true c
     | None -> ());
    List.iter
      (fun s -> Buffer.add_string buf (Printf.sprintf ";%d" (style_code s)))
      a.styles;
    Buffer.add_char buf 'm'
  ;;

  let to_buffer buf cap (x0, y0) (w, h) img =
    let w = max 0 w
    and h = max 0 h in
    match cap with
    | Cap.Ansi ->
      let g = render img in
      let gh = Array.length g in
      for i = 0 to h - 1 do
        if i > 0 then Buffer.add_string buf "\x1bE";
        let cy = y0 + i in
        let cur = ref A.empty in
        for j = 0 to w - 1 do
          let cx = x0 + j in
          let cell =
            if cy >= 0 && cy < gh
            then (
              let row = g.(cy) in
              if cx >= 0 && cx < Array.length row then row.(cx) else None)
            else None
          in
          let glyph, attr =
            match cell with
            | Some c -> c.glyph, c.attr
            | None -> " ", A.empty
          in
          if not (A.equal attr !cur)
          then (
            sgr buf attr;
            cur := attr);
          Buffer.add_string buf glyph
        done;
        Buffer.add_string buf "\x1b[0m\x1b[K"
      done
    | Cap.Dumb ->
      let g = render img in
      let gh = Array.length g in
      for i = 0 to h - 1 do
        if i > 0 then Buffer.add_char buf '\n';
        let cy = y0 + i in
        for j = 0 to w - 1 do
          let cx = x0 + j in
          let glyph =
            if cy >= 0 && cy < gh
            then (
              let row = g.(cy) in
              if cx >= 0 && cx < Array.length row
              then (
                match row.(cx) with
                | Some c -> c.glyph
                | None -> " ")
              else " ")
            else " "
          in
          Buffer.add_string buf glyph
        done
      done
  ;;
end
