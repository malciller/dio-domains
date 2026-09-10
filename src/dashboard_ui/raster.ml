(** RGB framebuffer with a few drawing primitives and a terminal sink.

    This is the foundation for graphics rendering: a small truecolor pixel
    buffer that can be downsampled to terminal cells with the upper half-block
    glyph (fg = top pixel, bg = bottom pixel), which doubles vertical
    resolution and works on any truecolor terminal. Later phases add Sixel and
    Kitty sinks that consume the same buffer. *)

type color = float * float * float
(* Components in [0, 1]. *)

type t =
  { w : int
  ; h : int
  ; data : float array (* w * h * 3 *)
  }

let create w h =
  let w = max 0 w
  and h = max 0 h in
  { w; h; data = Array.make (w * h * 3) 0.0 }
;;

let width t = t.w
let height t = t.h
let in_bounds t x y = x >= 0 && x < t.w && y >= 0 && y < t.h
let clamp01 x = if x < 0.0 then 0.0 else if x > 1.0 then 1.0 else x

let set t x y (r, g, b) =
  if in_bounds t x y
  then (
    let i = ((y * t.w) + x) * 3 in
    t.data.(i) <- clamp01 r;
    t.data.(i + 1) <- clamp01 g;
    t.data.(i + 2) <- clamp01 b)
;;

let get t x y =
  if in_bounds t x y
  then (
    let i = ((y * t.w) + x) * 3 in
    t.data.(i), t.data.(i + 1), t.data.(i + 2))
  else 0.0, 0.0, 0.0
;;

(** Source-over blend of [color] at [x, y] with coverage [a]. *)
let blend t x y (r, g, b) a =
  if in_bounds t x y
  then (
    let a = clamp01 a in
    let i = ((y * t.w) + x) * 3 in
    t.data.(i) <- t.data.(i) +. ((clamp01 r -. t.data.(i)) *. a);
    t.data.(i + 1) <- t.data.(i + 1) +. ((clamp01 g -. t.data.(i + 1)) *. a);
    t.data.(i + 2) <- t.data.(i + 2) +. ((clamp01 b -. t.data.(i + 2)) *. a))
;;

let fill t color =
  for y = 0 to t.h - 1 do
    for x = 0 to t.w - 1 do
      set t x y color
    done
  done
;;

let fpart x = x -. Float.floor x
let rfpart x = 1.0 -. fpart x

(** One-pixel-wide line with fractional coverage (Xiaolin Wu), so curves look
    smooth rather than stair-stepped. Endpoints are inclusive integer pixels. *)
let line t (x0i, y0i) (x1i, y1i) color =
  let x0 = float_of_int x0i
  and y0 = float_of_int y0i
  and x1 = float_of_int x1i
  and y1 = float_of_int y1i in
  let steep = abs_float (y1 -. y0) > abs_float (x1 -. x0) in
  let x0, y0, x1, y1 = if steep then y0, x0, y1, x1 else x0, y0, x1, y1 in
  let x0, y0, x1, y1 = if x0 > x1 then x1, y1, x0, y0 else x0, y0, x1, y1 in
  if x0i = x1i && y0i = y1i
  then set t x0i y0i color
  else (
    let dx = x1 -. x0
    and dy = y1 -. y0 in
    let gradient = if dx = 0.0 then 1.0 else dy /. dx in
    let plot x y cov =
      let xi = int_of_float x
      and yi = int_of_float y in
      let xx, yy = if steep then yi, xi else xi, yi in
      blend t xx yy color cov
    in
    let xpxl1 = int_of_float (x0 +. 0.5) in
    let xpxl2 = int_of_float (x1 +. 0.5) in
    let xgap1 = 1.0 -. fpart (x0 +. 0.5) in
    plot (float_of_int xpxl1) (Float.floor y0) (rfpart y0 *. xgap1);
    plot (float_of_int xpxl1) (Float.floor y0 +. 1.0) (fpart y0 *. xgap1);
    let intery = ref (y0 +. gradient) in
    for x = xpxl1 + 1 to xpxl2 - 1 do
      plot (float_of_int x) (Float.floor !intery) (rfpart !intery);
      plot (float_of_int x) (Float.floor !intery +. 1.0) (fpart !intery);
      intery := !intery +. gradient
    done;
    let xgap2 = fpart (x1 +. 0.5) in
    plot (float_of_int xpxl2) (Float.floor y1) (rfpart y1 *. xgap2);
    plot (float_of_int xpxl2) (Float.floor y1 +. 1.0) (fpart y1 *. xgap2))
;;

(** Filled rectangle, with coordinates clamped to the buffer. *)
let rect t x y w h color =
  let x1 = min t.w (x + w)
  and y1 = min t.h (y + h) in
  for yy = max 0 y to y1 - 1 do
    for xx = max 0 x to x1 - 1 do
      set t xx yy color
    done
  done
;;

(** Vertical gradient from [top] to [bottom] across the upper [h] rows. *)
let gradient t ~y ~h ~top:(tr, tg, tb) ~bottom:(br, bg, bb) =
  let h = max 1 h in
  for yy = 0 to h - 1 do
    let f = float yy /. float (max 1 (h - 1)) in
    let r = tr +. ((br -. tr) *. f)
    and g = tg +. ((bg -. tg) *. f)
    and b = tb +. ((bb -. tb) *. f) in
    rect t 0 (y + yy) t.w 1 (r, g, b)
  done
;;

(** Downsample to terminal cells using upper half-blocks. *)
let to_blocks ?(bg = 0.0, 0.0, 0.0) t =
  let u8 x = max 0 (min 255 (int_of_float ((clamp01 x *. 255.0) +. 0.5))) in
  let sample x y = if in_bounds t x y then get t x y else bg in
  let rows = (t.h + 1) / 2 in
  let row cy =
    Notty.I.hcat
      (List.init t.w (fun cx ->
         let r0, g0, b0 = sample cx (2 * cy) in
         let r1, g1, b1 = sample cx ((2 * cy) + 1) in
         let fgc = Notty.A.rgb_888 ~r:(u8 r0) ~g:(u8 g0) ~b:(u8 b0) in
         let bgc = Notty.A.rgb_888 ~r:(u8 r1) ~g:(u8 g1) ~b:(u8 b1) in
         Notty.I.string Notty.A.(fg fgc ++ bg bgc) "\u{2580}"))
  in
  Notty.I.vcat (List.init rows row)
;;
