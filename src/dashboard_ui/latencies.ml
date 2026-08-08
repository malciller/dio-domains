open Notty
open Theme

let history_len = 15
let cycle_hist = Hashtbl.create 16
let cycle_hist_max = 64

let update_cycle_hist symbol p99 =
  (* Evict all entries when the table grows beyond the cap.
     This is safe — sparkline history repopulates within seconds. *)
  if Hashtbl.length cycle_hist > cycle_hist_max then Hashtbl.clear cycle_hist;
  let arr =
    try Hashtbl.find cycle_hist symbol with
    | Not_found ->
      let a = Array.make history_len 0.0 in
      Hashtbl.add cycle_hist symbol a;
      a
  in
  for i = 0 to history_len - 2 do
    arr.(i) <- arr.(i + 1)
  done;
  arr.(history_len - 1) <- p99;
  arr
;;

let render_latencies w json =
  let lats =
    match json |?> "latencies" with
    | `Assoc l -> l
    | _ -> []
  in
  (* Build symbol -> exchange lookup from strategies *)
  let sym_to_exch =
    match json |?> "strategies" with
    | `Assoc l ->
      List.map (fun (sym, data) -> sym, data |?> "exchange" |> to_string_d "") l
    | _ -> []
  in
  let exch_of_symbol sym =
    match List.assoc_opt sym sym_to_exch with
    | Some e when e <> "" -> e
    | _ -> ""
  in
  (* Filter to domains that have at least one metric with samples *)
  let active_lats =
    List.filter
      (fun (_symbol, metrics) ->
         let mlist =
           match metrics with
           | `Assoc l -> l
           | _ -> []
         in
         List.exists (fun (_label, data) -> data |?> "samples" |> to_int_d 0 > 0) mlist)
      lats
  in
  if active_lats = []
  then I.empty
  else (
    (* Per-metric latency thresholds: (yellow_us, red_us).
       These measure INTERNAL processing latency only — the code we control.
       Network round-trip to exchanges is excluded (not colocated).
       Thresholds are calibrated against achievable performance for each stage:
       - ticker/ob:  in-memory struct update from ring buffer; should be <5us
       - strategy:   grid logic + mutex + order push; target <25us p50
       - execution:  ringbuffer write + signal broadcast; short path
       - cycle:      full wakeup-to-sleep; sum of all stages *)
    let latency_thresholds label =
      match label with
      | "orderbook" -> 10.0, 30.0
      | "strategy" -> 30.0, 75.0
      | "execution" -> 50.0, 150.0
      | "cycle" -> 50.0, 100.0
      | _ -> 50.0, 100.0
    in
    let severity label f samples =
      if samples = 0
      then 3 (* dim *)
      else (
        let warn, crit = latency_thresholds label in
        if f > crit
        then 2 (* red *)
        else if f > warn
        then 1 (* yellow *)
        else 0 (* green *))
    in
    (* Metric display order *)
    let is_compact = w < 160 in
    let metric_order =
      if is_compact
      then [ "cycle"; "execution" ]
      else [ "cycle"; "orderbook"; "strategy"; "execution" ]
    in
    let metric_labels =
      if is_compact then [ "CYCLE"; "EXEC" ] else [ "CYCLE"; "OB"; "STRAT"; "EXEC" ]
    in
    (* Two-row header: metric names on row 1, p50/p99 sub-headers on row 2 *)
    let header_row1 =
      I.hcat
        ([ I.string a_border " │  "
         ; col 16 a_label ""
         ; I.string a_border " │ "
         ; col 15 a_label "     TREND     "
         ; I.string a_border " │ "
         ]
         @ List.mapi
             (fun i lbl ->
                let len = String.length lbl in
                let pad = (27 - len) / 2 in
                let s = String.make pad ' ' ^ lbl in
                let img = col 27 a_label s in
                if i = 0 then img else I.hcat [ I.string a_border " │ "; img ])
             metric_labels
         @
         if is_compact
         then []
         else [ I.string a_border " │ "; col 28 a_label "         SPIKE CAUSE        " ])
    in
    let header_row2 =
      I.hcat
        ([ I.string a_border " │  "
         ; col 16 a_label "DOMAIN"
         ; I.string a_border " │ "
         ; col 15 a_label "  (CYCLE P99)  "
         ; I.string a_border " │ "
         ]
         @ List.mapi
             (fun i _lbl ->
                let img =
                  I.hcat
                    [ col_right 9 a_dim "p50"
                    ; col_right 9 a_dim "p99"
                    ; col_right 9 a_dim "p999"
                    ]
                in
                if i = 0 then img else I.hcat [ I.string a_border " │ "; img ])
             metric_labels
         @
         if is_compact
         then []
         else [ I.string a_border " │ "; col 28 a_dim "                            " ])
    in
    let header = I.vcat [ close_row w header_row1; close_row w header_row2 ] in
    let rows =
      List.mapi
        (fun i (symbol, metrics) ->
           let bg_color = if i mod 2 = 1 then c_panel else c_bg in
           let a_text = A.(Theme.a_text ++ bg bg_color) in
           let a_green = A.(Theme.a_green ++ bg bg_color) in
           let a_red = A.(Theme.a_red ++ bg bg_color) in
           let a_yellow = A.(Theme.a_yellow ++ bg bg_color) in
           let a_dim = A.(Theme.a_dim ++ bg bg_color) in
           let a_border = A.(Theme.a_border ++ bg bg_color) in
           let a_border_outer = A.(Theme.a_border ++ bg c_bg) in
           let a_bright = A.(Theme.a_bright ++ bg bg_color) in
           let exch_sym_attr ?dim exch =
             A.(Theme.exch_sym_attr ?dim exch ++ bg bg_color)
           in
           let attr_of_sev = function
             | 2 -> a_red
             | 1 -> a_yellow
             | 0 -> a_green
             | _ -> a_dim
           in
           let col w attr s = I.string attr (Theme.pad_right w s) in
           let col_right w attr s = I.string attr (Theme.pad_left w s) in
           let close_row w img =
             let d = w - I.width img - 2 in
             I.hcat
               [ img
               ; I.string A.(bg bg_color) (String.make (max 0 d) ' ')
               ; I.string A.(bg bg_color) " "
               ; I.string a_border_outer "│"
               ]
           in
           let render_sparkline_local w data max_val attr_fn =
             let len = Array.length data in
             let start_idx = max 0 (len - w) in
             let visible_len = min w len in
             let empty_w = w - visible_len in
             let blocks =
               List.init visible_len (fun idx ->
                 let v = data.(start_idx + idx) in
                 let ratio = if max_val > 0.0 then v /. max_val else 0.0 in
                 let ratio = max 0.0 (min 1.0 ratio) in
                 let block_idx = int_of_float (ratio *. 7.0) in
                 let block_idx = max 0 (min 7 block_idx) in
                 I.string (attr_fn v) Theme.block_chars.(block_idx))
             in
             I.hcat (I.string a_dim (String.make empty_w ' ') :: blocks)
           in
           let mlist =
             match metrics with
             | `Assoc l -> l
             | _ -> []
           in
           let find_metric label =
             match List.assoc_opt label mlist with
             | Some data ->
               let p50 = data |?> "p50" |> to_float_d 0.0 in
               let p99 = data |?> "p99" |> to_float_d 0.0 in
               let p999 = data |?> "p999" |> to_float_d 0.0 in
               let samples = data |?> "samples" |> to_int_d 0 in
               p50, p99, p999, samples
             | None -> 0.0, 0.0, 0.0, 0
           in
           (* Compute worst severity across all metrics for the health dot *)
           let worst_sev =
             List.fold_left
               (fun worst label ->
                  let _, p99, _, samples = find_metric label in
                  if samples = 0 then worst else max worst (severity label p99 samples))
               0
               metric_order
           in
           let dot_attr = attr_of_sev worst_sev in
           let metric_cells =
             List.mapi
               (fun i label ->
                  let p50, p99, p999, samples = find_metric label in
                  let img =
                    if samples = 0
                    then
                      I.hcat
                        [ col_right 9 a_dim "--"
                        ; col_right 9 a_dim "--"
                        ; col_right 9 a_dim "--"
                        ]
                    else (
                      let s50 = severity label p50 samples in
                      let s99 = max s50 (severity label p99 samples) in
                      let s999 = max s99 (severity label p999 samples) in
                      I.hcat
                        [ col_right 9 (attr_of_sev s50) (format_latency_us p50)
                        ; col_right 9 (attr_of_sev s99) (format_latency_us p99)
                        ; col_right 9 (attr_of_sev s999) (format_latency_us p999)
                        ])
                  in
                  if i = 0 then img else I.hcat [ I.string a_border " │ "; img ])
               metric_order
           in
           let _, cycle_p99, _, _ = find_metric "cycle" in
           let c_arr = update_cycle_hist symbol cycle_p99 in
           let trend_spark =
             render_sparkline_local 15 c_arr 100.0 (fun v ->
               attr_of_sev (severity "cycle" v 1))
           in
           let cycle_cause =
             match List.assoc_opt "cycle" mlist with
             | Some data -> data |?> "max_cause" |> to_string_d "--"
             | None -> "--"
           in
           let contains_sub str sub =
             let len_s = String.length str in
             let len_sub = String.length sub in
             if len_sub = 0
             then true
             else if len_s < len_sub
             then false
             else (
               let found = ref false in
               let i = ref 0 in
               while !i <= len_s - len_sub && not !found do
                 if String.sub str !i len_sub = sub then found := true else incr i
               done;
               !found)
           in
           let find_sub_idx str sub =
             let len_s = String.length str in
             let len_sub = String.length sub in
             if len_sub = 0
             then Some 0
             else if len_s < len_sub
             then None
             else (
               let res = ref None in
               let i = ref 0 in
               while !i <= len_s - len_sub && !res = None do
                 if String.sub str !i len_sub = sub then res := Some !i else incr i
               done;
               !res)
           in
           let parse_first_int s =
             let len = String.length s in
             let buf = Buffer.create 8 in
             let i = ref 0 in
             while !i < len && s.[!i] >= '0' && s.[!i] <= '9' do
               Buffer.add_char buf s.[!i];
               incr i
             done;
             try int_of_string (Buffer.contents buf) with
             | _ -> 0
           in
           let get_word s =
             let len = String.length s in
             let buf = Buffer.create 16 in
             let i = ref 0 in
             while !i < len && s.[!i] <> ' ' && s.[!i] <> '(' do
               Buffer.add_char buf s.[!i];
               incr i
             done;
             Buffer.contents buf
           in
           let formatted_cause =
             if cycle_cause = "--" || cycle_cause = ""
             then col 28 a_dim "--"
             else (
               let tags = ref [] in
               if contains_sub cycle_cause "ob:true"
               then
                 tags := I.string A.(fg c_cyan ++ bg bg_color ++ st bold) "[OB]" :: !tags;
               if contains_sub cycle_cause "st:true"
               then
                 tags
                 := I.string A.(fg c_yellow ++ bg bg_color ++ st bold) "[STRAT]" :: !tags;
               (match find_sub_idx cycle_cause "ex:" with
                | Some idx ->
                  let rest =
                    String.sub cycle_cause (idx + 3) (String.length cycle_cause - idx - 3)
                  in
                  let n = parse_first_int rest in
                  if n > 0
                  then
                    tags
                    := I.string
                         A.(fg c_magenta ++ bg bg_color ++ st bold)
                         (Printf.sprintf "[EXEC:%d]" n)
                       :: !tags
                | None -> ());
               (match find_sub_idx cycle_cause "al:" with
                | Some idx ->
                  let rest =
                    String.sub cycle_cause (idx + 3) (String.length cycle_cause - idx - 3)
                  in
                  let word_str = get_word rest in
                  if word_str <> ""
                  then tags := I.string a_dim (Printf.sprintf "[%s]" word_str) :: !tags
                | None -> ());
               let has_gc_maj =
                 contains_sub cycle_cause "gc:MAJ"
                 || contains_sub cycle_cause "gc:CMP"
                 ||
                 match find_sub_idx cycle_cause "major=" with
                 | Some idx ->
                   parse_first_int
                     (String.sub
                        cycle_cause
                        (idx + 6)
                        (String.length cycle_cause - idx - 6))
                   > 0
                 | None ->
                   (match find_sub_idx cycle_cause "comp=" with
                    | Some idx ->
                      parse_first_int
                        (String.sub
                           cycle_cause
                           (idx + 5)
                           (String.length cycle_cause - idx - 5))
                      > 0
                    | None -> false)
               in
               let has_gc_min =
                 contains_sub cycle_cause "gc:MIN"
                 ||
                 match find_sub_idx cycle_cause "minor=" with
                 | Some idx ->
                   parse_first_int
                     (String.sub
                        cycle_cause
                        (idx + 6)
                        (String.length cycle_cause - idx - 6))
                   > 0
                 | None -> false
               in
               if has_gc_maj
               then
                 tags
                 := I.string A.(fg c_red ++ bg bg_color ++ st bold) "[GC:MAJ]" :: !tags
               else if has_gc_min
               then
                 tags
                 := I.string A.(fg c_yellow ++ bg bg_color ++ st bold) "[GC:MIN]" :: !tags;
               if !tags = []
               then
                 tags
                 := [ I.string
                        a_dim
                        (Printf.sprintf "[%s]" (truncate_string 24 cycle_cause))
                    ];
               let tag_img =
                 I.hcat
                   (List.rev_map
                      (fun t -> I.hcat [ t; I.string A.(bg bg_color) " " ])
                      !tags)
               in
               I.hsnap ~align:`Left 28 tag_img)
           in
           let exch = exch_of_symbol symbol in
           let sym_attr = if exch <> "" then exch_sym_attr exch else a_bright in
           close_row
             w
             (I.hcat
                ([ I.string a_border_outer " │"
                 ; I.string A.(bg bg_color) "  "
                 ; I.string dot_attr "●"
                 ; I.string a_text " "
                 ; col 14 sym_attr (truncate_string 13 symbol)
                 ; I.string a_border " │ "
                 ; trend_spark
                 ; I.string a_border " │ "
                 ]
                 @ metric_cells
                 @ if is_compact then [] else [ I.string a_border " │ "; formatted_cause ]
                )))
        active_lats
    in
    let title = section_title w "PERFORMANCE" in
    I.vcat ((title :: header :: rows) @ [ section_footer w ]))
;;
