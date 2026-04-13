open Notty
open Theme

let pressure_max_len = 240
let pressure_hist = Array.make pressure_max_len 0.0
let pressure_hist_idx = ref 0
let pressure_last_time = ref 0.0
let max_seen_heap_ref = ref 0
let pressure_blocks = [| "⠀"; "⡀"; "⣀"; "⣄"; "⣤"; "⣦"; "⣶"; "⣷"; "⣿" |]

let render_memory w json =
  let mem = json |?> "memory" in
  let title = section_title w "MEMORY & GC" in
  let heap    = mem |?> "heap_mb"     |> to_int_d 0 in
  let live    = mem |?> "live_kb"     |> to_int_d 0 in
  let free    = mem |?> "free_kb"     |> to_int_d 0 in
  let major   = mem |?> "gc_major"    |> to_int_d 0 in
  let minor   = mem |?> "gc_minor"    |> to_int_d 0 in
  let compact = mem |?> "compactions" |> to_int_d 0 in
  let frags   = mem |?> "fragments"   |> to_int_d 0 in

  let total_kb = float_of_int (live + free) in
  let live_ratio = if total_kb > 0.0 then (float_of_int live) /. total_kb else 0.0 in
  
  let space_overhead = mem |?> "space_overhead" |> to_int_d 80 in
  let expected_live_ratio = 100.0 /. (100.0 +. float_of_int space_overhead) in
  let normalized_pressure = if expected_live_ratio > 0.0 then live_ratio /. expected_live_ratio else 0.0 in

  let now = Unix.gettimeofday () in
  if now -. !pressure_last_time >= 1.0 then begin
    pressure_hist.(!pressure_hist_idx) <- normalized_pressure;
    pressure_hist_idx := (!pressure_hist_idx + 1) mod pressure_max_len;
    pressure_last_time := now;
  end;

  let used_width = 20 in
  let bar_len = max 10 (min pressure_max_len (w - used_width)) in

  let spark_imgs_top = ref [] in
  let spark_imgs_bot = ref [] in

  for i = bar_len - 1 downto 0 do
    let offset = !pressure_hist_idx - bar_len + i in
    let offset = if offset < 0 then offset + pressure_max_len else offset in
    let ratio = pressure_hist.(offset) in
    
    let v = 
      if ratio <= 1.05 then
        int_of_float ((ratio /. 1.05) *. 8.0)
      else
        8 + int_of_float (((ratio -. 1.05) /. 0.3) *. 8.0)
    in
    let v = max 0 (min 16 v) in
    let t_idx, b_idx = if v <= 8 then (0, v) else (v - 8, 8) in

    let s_top = pressure_blocks.(t_idx) in
    let s_bot = pressure_blocks.(b_idx) in

    let attr = 
      if ratio <= 1.05 then A.(fg c_green ++ bg c_bg)
      else if ratio <= 1.35 then A.(fg c_yellow ++ bg c_bg)
      else A.(fg c_red ++ bg c_bg)
    in
    
    spark_imgs_top := I.string attr s_top :: !spark_imgs_top;
    spark_imgs_bot := I.string attr s_bot :: !spark_imgs_bot;
  done;

  let row3 = I.hcat (
    [ I.string a_border " │"; I.string a_dim "  PRESSURE "; I.string a_border "╭" ] @ !spark_imgs_top @ [ I.string a_border "╮" ]
  ) in
  let row4 = I.hcat (
    [ I.string a_border " │"; I.string a_dim "           "; I.string a_border "╰" ] @ !spark_imgs_bot @ [ I.string a_border "╯" ]
  ) in

  let kv lbl v =
    I.hcat [
      I.string a_dim ("  " ^ lbl ^ " ");
      I.string a_text (Printf.sprintf "%-8s" v);
    ]
  in
  let kv_bar lbl v ratio p_attr =
    I.hcat [
      I.string a_dim ("  " ^ lbl ^ " ");
      I.string a_text (Printf.sprintf "%-8s" v);
      I.string a_dim " ";
      render_progress_bar 15 ratio p_attr;
    ]
  in
  let max_seen_heap = max !max_seen_heap_ref heap in
  max_seen_heap_ref := max_seen_heap;
  let heap_ratio = if max_seen_heap > 0 then float_of_int heap /. float_of_int max_seen_heap else 0.0 in

  let row1 = I.hcat [
    I.string a_border " │";
    kv_bar "HEAP" (Printf.sprintf "%dMB" heap) heap_ratio a_yellow;
    kv_bar "LIVE" (Printf.sprintf "%dKB" live) live_ratio a_green;
    kv "FREE" (Printf.sprintf "%dKB" free);
  ] in
  let row2 = I.hcat [
    I.string a_border " │";
    kv "MAJOR" (string_of_int major);
    kv "MINOR" (string_of_int minor);
    kv "COMPACT" (string_of_int compact);
    kv "FRAGS" (string_of_int frags);
  ] in
  I.vcat [title; close_row w row1; close_row w row2; close_row w row3; close_row w row4]
