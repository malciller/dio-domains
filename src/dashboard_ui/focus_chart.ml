open Notty
open Theme

(** Focus view: a full-screen truecolor raster chart of the selected asset's
    rolling mid history, drawn into a [Raster] buffer and downsampled to
    half-block cells so it renders on any truecolor terminal (no Kitty/Sixel
    required). The live edge glows and breathes while data is streaming. *)

let rgb01 (r, g, b) =
  float_of_int r /. 255.0, float_of_int g /. 255.0, float_of_int b /. 255.0
;;

let blend a b t = Anim.lerp a b t
let mix (r1, g1, b1) (r2, g2, b2) t = blend r1 r2 t, blend g1 g2 t, blend b1 b2 t

let render w h asset_key (snapshot : Snapshot.t) =
  let t = Theme.current () in
  let now = Anim.now () in
  let asset =
    List.find_opt
      (fun (a : Snapshot.selectable_asset) -> a.key = asset_key)
      snapshot.assets
  in
  match asset with
  | None ->
    I.string A.(fg t.c_yellow ++ bg t.c_bg) "No asset selected."
    |> I.hsnap ~align:`Left w
    |> I.vsnap ~align:`Top h
  | Some (a : Snapshot.selectable_asset) ->
    let points =
      match Hashtbl.find_opt Asset_graph.price_history a.key with
      | None -> []
      | Some q ->
        Queue.fold
          (fun acc (s : Asset_graph.price_snapshot) -> (s.timestamp, s.mid_p) :: acc)
          []
          q
    in
    let accent = rgb01 t.accent_rgb in
    let bg = rgb01 t.bg_rgb in
    let mid_line = mix accent (1.0, 1.0, 1.0) 0.35 in
    let fill = mix accent bg 0.35 in
    let up = mix accent (0.4, 1.0, 0.6) 0.5 in
    let down = mix accent (1.0, 0.45, 0.5) 0.5 in
    let header_h = 2
    and footer_h = 1 in
    let chart_rows = max 1 (h - header_h - footer_h) in
    let px_h = chart_rows * 2 in
    let title =
      I.hcat
        [ I.string A.(fg t.c_title ++ bg t.c_bg ++ st bold) (" ◆ FOCUS  " ^ a.display_name)
        ; I.string A.(fg t.c_dim ++ bg t.c_bg) "  (b: back  •  f: main)"
        ]
      |> I.hsnap ~align:`Left w
    in
    if points = []
    then
      I.vcat
        [ title
        ; I.string t.a_dim "  collecting price history…" |> I.hsnap ~align:`Left w
        ]
      |> I.vsnap ~align:`Top h
    else (
      let window = Asset_graph.window_seconds in
      let t0 = now -. window in
      let prices = List.map snd points in
      let pmin0 = List.fold_left min infinity prices in
      let pmax0 = List.fold_left max neg_infinity prices in
      let pad = ((pmax0 -. pmin0) *. 0.12) +. 1e-9 in
      let pmin = pmin0 -. pad
      and pmax = pmax0 +. pad in
      let span = max 1e-9 (pmax -. pmin) in
      let x_of ts = int_of_float ((ts -. t0) /. window *. float (max 1 (w - 1))) in
      let y_of p =
        px_h - 1 - int_of_float ((p -. pmin) /. span *. float (max 1 (px_h - 1)))
      in
      let raster = Raster.create w px_h in
      (* Background: a soft vertical gradient of the theme background. *)
      let bg_top = mix bg (0.0, 0.0, 0.0) 0.25 in
      Raster.gradient raster ~y:0 ~h:px_h ~top:bg_top ~bottom:bg;
      (* Column mid heights, forward-filled across gaps. *)
      let ycol = Array.make w None in
      List.iter
        (fun (ts, p) ->
           let x = max 0 (min (w - 1) (x_of ts)) in
           ycol.(x) <- Some (y_of p))
        points;
      let last = ref None in
      Array.iteri
        (fun x cell ->
           match cell with
           | Some y -> last := Some y
           | None -> Option.iter (fun y -> ycol.(x) <- Some y) !last)
        ycol;
      (* Illuminated area under the mid line. *)
      Array.iteri
        (fun x cell ->
           match cell with
           | None -> ()
           | Some y ->
             for yy = max 0 y to px_h - 1 do
               let depth = float (yy - y) /. float (max 1 px_h) in
               Raster.blend raster x yy fill (0.55 *. (1.0 -. depth))
             done)
        ycol;
      (* Mid line, drawn as an anti-aliased polyline across defined columns. *)
      let prev = ref None in
      Array.iteri
        (fun x cell ->
           match cell with
           | None -> ()
           | Some y ->
             (match !prev with
              | Some (px, py) ->
                let col = if y < py then up else if y > py then down else mid_line in
                Raster.line raster (px, py) (x, y) col;
                (* vertical connector so steep moves stay continuous *)
                Raster.line raster (x, py) (x, y) col
              | None -> ());
             prev := Some (x, y))
        ycol;
      (* Live edge: a breathing glow around the most recent point. *)
      let live =
        match !prev with
        | None -> None
        | Some p -> Some p
      in
      (match live with
       | None -> ()
       | Some (lx, ly) ->
         let pulse =
           if !Anim.reduced_motion then 0.5 else 0.5 +. (0.5 *. Anim.pulse ())
         in
         for r = 1 to 4 do
           let a_glow = 0.35 *. pulse /. float r in
           for dx = -r to r do
             let x = lx + dx in
             Raster.blend raster x ly mid_line a_glow;
             Raster.blend raster x (ly - 1) mid_line (a_glow *. 0.6);
             Raster.blend raster x (ly + 1) mid_line (a_glow *. 0.6)
           done;
           if not !Anim.reduced_motion then Anim.motion_pending := true
         done);
      (* Axis labels: min/max on the right edge, and a live readout. *)
      let midp = snd (List.nth points (List.length points - 1)) in
      let change =
        if List.length points < 2
        then 0.0
        else (midp -. snd (List.hd points)) /. snd (List.hd points) *. 100.0
      in
      let change_attr =
        A.(fg (if change >= 0.0 then t.c_green else t.c_red) ++ bg t.c_bg ++ st bold)
      in
      let legend =
        I.hcat
          [ I.string A.(fg t.c_label ++ bg t.c_bg) "  mid "
          ; I.string A.(fg t.c_bright ++ bg t.c_bg) (format_price midp)
          ; I.string A.(fg t.c_dim ++ bg t.c_bg) "  15m "
          ; I.string change_attr (format_pct change)
          ; I.string
              A.(fg t.c_dim ++ bg t.c_bg)
              (Printf.sprintf "  range %s–%s" (format_price pmin0) (format_price pmax0))
          ]
        |> I.hsnap ~align:`Left w
      in
      let plot =
        Raster.to_blocks ~bg raster
        |> I.vsnap ~align:`Top chart_rows
        |> I.hsnap ~align:`Left w
      in
      let footer =
        I.string A.(fg t.c_border ++ bg t.c_bg) (repeat_str "─" w)
        |> I.hsnap ~align:`Left w
      in
      I.vcat [ title; legend; plot; footer ] |> I.vsnap ~align:`Top h)
;;
