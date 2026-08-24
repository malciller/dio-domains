open Notty
open Theme

let pressure_max_len = 240
let pressure_hist = Array.make pressure_max_len 0.0
let pressure_hist_idx = ref 0
let pressure_last_time = ref 0.0
let max_seen_heap_ref = ref 0
let pressure_blocks = [| "⠀"; "⡀"; "⣀"; "⣄"; "⣤"; "⣦"; "⣶"; "⣷"; "⣿" |]

let render_memory w json =
  let t = Theme.current () in
  let mem = json |?> "memory" in
  let title = section_title w "MEMORY & GC" in
  let heap = mem |?> "heap_mb" |> to_int_d 0 in
  let live = mem |?> "live_kb" |> to_int_d 0 in
  let free = mem |?> "free_kb" |> to_int_d 0 in
  let major = mem |?> "gc_major" |> to_int_d 0 in
  let minor = mem |?> "gc_minor" |> to_int_d 0 in
  let compact = mem |?> "compactions" |> to_int_d 0 in
  let frags = mem |?> "fragments" |> to_int_d 0 in
  let total_kb = float_of_int (live + free) in
  let live_ratio = if total_kb > 0.0 then float_of_int live /. total_kb else 0.0 in
  let space_overhead = mem |?> "space_overhead" |> to_int_d 80 in
  let expected_live_ratio = 100.0 /. (100.0 +. float_of_int space_overhead) in
  let normalized_pressure =
    if expected_live_ratio > 0.0 then live_ratio /. expected_live_ratio else 0.0
  in
  let now = Unix.gettimeofday () in
  if now -. !pressure_last_time >= 1.0
  then (
    pressure_hist.(!pressure_hist_idx) <- normalized_pressure;
    pressure_hist_idx := (!pressure_hist_idx + 1) mod pressure_max_len;
    pressure_last_time := now);
  let used_width = 20 in
  let bar_len = max 10 (min pressure_max_len (w - used_width)) in
  let spark_imgs_top = ref [] in
  let spark_imgs_bot = ref [] in
  for i = bar_len - 1 downto 0 do
    let offset = !pressure_hist_idx - bar_len + i in
    let offset = if offset < 0 then offset + pressure_max_len else offset in
    let ratio = pressure_hist.(offset) in
    let v =
      if ratio <= 0.0
      then 0
      else if ratio <= 1.0
      then int_of_float (ratio /. 1.0 *. 8.0)
      else 8 + int_of_float ((min 1.5 ratio -. 1.0) /. 0.5 *. 8.0)
    in
    let v = max 0 (min 16 v) in
    let t_idx, b_idx = if v <= 8 then 0, v else v - 8, 8 in
    let s_top = pressure_blocks.(t_idx) in
    let s_bot = pressure_blocks.(b_idx) in
    let attr =
      if ratio <= 0.0
      then A.(fg t.c_dim ++ bg t.c_bg)
      else if ratio <= 1.0
      then A.(fg t.c_green ++ bg t.c_bg)
      else if ratio <= 1.25
      then A.(fg t.c_yellow ++ bg t.c_bg)
      else A.(fg t.c_red ++ bg t.c_bg)
    in
    spark_imgs_top := I.string attr s_top :: !spark_imgs_top;
    spark_imgs_bot := I.string attr s_bot :: !spark_imgs_bot
  done;
  let row3 =
    I.hcat
      ([ I.string t.a_border " │"; I.string t.a_dim "  PRESSURE "; I.string t.a_border "╭" ]
       @ !spark_imgs_top
       @ [ I.string t.a_border "╮" ])
  in
  let row4 =
    I.hcat
      ([ I.string t.a_border " │"; I.string t.a_dim "           "; I.string t.a_border "╰" ]
       @ !spark_imgs_bot
       @ [ I.string t.a_border "╯" ])
  in
  let kv lbl v =
    I.hcat
      [ I.string t.a_dim ("  " ^ lbl ^ " ")
      ; I.string t.a_text (Printf.sprintf "%-10s" (add_commas v))
      ]
  in
  let kv_bar lbl v ratio p_attr =
    I.hcat
      [ I.string t.a_dim ("  " ^ lbl ^ " ")
      ; I.string t.a_text (Printf.sprintf "%-10s" (add_commas v))
      ; I.string t.a_dim " "
      ; render_progress_bar 15 ratio p_attr
      ]
  in
  let max_seen_heap = max !max_seen_heap_ref heap in
  max_seen_heap_ref := max_seen_heap;
  let heap_ratio =
    if max_seen_heap > 0 then float_of_int heap /. float_of_int max_seen_heap else 0.0
  in
  let row1 =
    I.hcat
      [ I.string t.a_border " │"
      ; kv_bar "HEAP" (Printf.sprintf "%dMB" heap) heap_ratio t.a_yellow
      ; kv_bar "LIVE" (Printf.sprintf "%dKB" live) live_ratio t.a_green
      ; kv "FREE" (Printf.sprintf "%dKB" free)
      ]
  in
  let row2 =
    I.hcat
      [ I.string t.a_border " │"
      ; kv "MAJOR" (string_of_int major)
      ; kv "MINOR" (string_of_int minor)
      ; kv "COMPACT" (string_of_int compact)
      ; kv "FRAGS" (string_of_int frags)
      ]
  in
  I.vcat
    [ title
    ; close_row w row1
    ; close_row w row2
    ; close_row w row3
    ; close_row w row4
    ; section_footer w
    ]
;;

let render_memory_card w json =
  let t = Theme.current () in
  let mem = json |?> "memory" in
  let heap = mem |?> "heap_mb" |> to_int_d 0 in
  let live = mem |?> "live_kb" |> to_int_d 0 in
  let free = mem |?> "free_kb" |> to_int_d 0 in
  let major = mem |?> "gc_major" |> to_int_d 0 in
  let minor = mem |?> "gc_minor" |> to_int_d 0 in
  let compact = mem |?> "compactions" |> to_int_d 0 in
  let total_kb = float_of_int (live + free) in
  let live_ratio = if total_kb > 0.0 then float_of_int live /. total_kb else 0.0 in
  let max_seen_heap = max !max_seen_heap_ref heap in
  max_seen_heap_ref := max_seen_heap;
  let heap_ratio =
    if max_seen_heap > 0 then float_of_int heap /. float_of_int max_seen_heap else 0.0
  in
  let row1 =
    I.hcat
      [ col 10 t.a_dim "HEAP"
      ; col_right 10 t.a_yellow (Printf.sprintf "%dMB" heap)
      ; I.string t.a_dim " "
      ; render_progress_bar 12 heap_ratio t.a_yellow
      ]
  in
  let row2 =
    I.hcat
      [ col 10 t.a_dim "LIVE"
      ; col_right 10 t.a_green (Printf.sprintf "%dKB" live)
      ; I.string t.a_dim " "
      ; render_progress_bar 12 live_ratio t.a_green
      ]
  in
  let row3 =
    I.hcat
      [ col 10 t.a_dim "GC COUNTS"
      ; col_right 10 t.a_text (Printf.sprintf "maj:%d min:%d cmp:%d" major minor compact)
      ]
  in
  render_card w "MEMORY & GC" [ row1; row2; row3 ]
;;
