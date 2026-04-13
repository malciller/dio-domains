open Notty
open Theme

let history_len = 15
let cycle_hist = Hashtbl.create 16

let update_cycle_hist symbol p99 =
  let arr = try Hashtbl.find cycle_hist symbol with Not_found -> 
    let a = Array.make history_len 0.0 in
    Hashtbl.add cycle_hist symbol a; a
  in
  for i = 0 to history_len - 2 do
    arr.(i) <- arr.(i + 1)
  done;
  arr.(history_len - 1) <- p99;
  arr

let render_latencies w json =
  let lats = match json |?> "latencies" with `Assoc l -> l | _ -> [] in
  (* Build symbol -> exchange lookup from strategies *)
  let sym_to_exch = match json |?> "strategies" with
    | `Assoc l -> List.map (fun (sym, data) ->
        (sym, data |?> "exchange" |> to_string_d "")
      ) l
    | _ -> []
  in
  let exch_of_symbol sym =
    match List.assoc_opt sym sym_to_exch with
    | Some e when e <> "" -> e
    | _ -> ""
  in
  (* Filter to domains that have at least one metric with samples *)
  let active_lats = List.filter (fun (_symbol, metrics) ->
    let mlist = match metrics with `Assoc l -> l | _ -> [] in
    List.exists (fun (_label, data) ->
      data |?> "samples" |> to_int_d 0 > 0
    ) mlist
  ) lats in
  if active_lats = [] then I.empty
  else
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
      | "orderbook" -> (10.0,  30.0)
      | "strategy"  -> (30.0,  75.0)
      | "execution" -> (50.0,  150.0)
      | "cycle"     -> (50.0,  100.0)
      | _           -> (50.0,  100.0)
    in
    let severity label f samples =
      if samples = 0 then 3  (* dim *)
      else
        let (warn, crit) = latency_thresholds label in
        if f > crit then 2    (* red *)
        else if f > warn then 1  (* yellow *)
        else 0                 (* green *)
    in
    let attr_of_sev = function
      | 2 -> a_red | 1 -> a_yellow | 0 -> a_green | _ -> a_dim
    in
    (* Metric display order *)
    let metric_order = ["cycle"; "orderbook"; "strategy"; "execution"] in
    let metric_labels = ["CYCLE"; "OB"; "STRAT"; "EXEC"] in
    (* Two-row header: metric names on row 1, p50/p99 sub-headers on row 2 *)
    let header_row1 = I.hcat (
      [ I.string a_border " │  ";
        col 16 a_label "";
        I.string a_border " │ ";
        col 15 a_label "     TREND     ";
        I.string a_border " │ " ]
      @ List.mapi (fun i lbl ->
          let len = String.length lbl in
          let pad = (27 - len) / 2 in
          let s = String.make pad ' ' ^ lbl in
          let img = col 27 a_label s in
          if i = 0 then img else I.hcat [ I.string a_border " │ "; img ]
        ) metric_labels
    ) in
    let header_row2 = I.hcat (
      [ I.string a_border " │  ";
        col 16 a_label "DOMAIN";
        I.string a_border " │ ";
        col 15 a_label "  (CYCLE P99)  ";
        I.string a_border " │ " ]
      @ List.mapi (fun i _lbl ->
          let img = I.hcat [ col 9 a_dim "p50"; col 9 a_dim "p99"; col 9 a_dim "p999" ] in
          if i = 0 then img else I.hcat [ I.string a_border " │ "; img ]
        ) metric_labels
    ) in
    let header = I.vcat [close_row w header_row1; close_row w header_row2] in
    let rows = List.map (fun (symbol, metrics) ->
      let mlist = match metrics with `Assoc l -> l | _ -> [] in
      let find_metric label =
        match List.assoc_opt label mlist with
        | Some data ->
            let p50 = data |?> "p50" |> to_float_d 0.0 in
            let p99 = data |?> "p99" |> to_float_d 0.0 in
            let p999 = data |?> "p999" |> to_float_d 0.0 in
            let samples = data |?> "samples" |> to_int_d 0 in
            (p50, p99, p999, samples)
        | None -> (0.0, 0.0, 0.0, 0)
      in
      (* Compute worst severity across all metrics for the health dot *)
      let worst_sev = List.fold_left (fun worst label ->
        let (_, p99, _, samples) = find_metric label in
        if samples = 0 then worst
        else max worst (severity label p99 samples)
      ) 0 metric_order in
      let dot_attr = attr_of_sev worst_sev in
      let metric_cells = List.mapi (fun i label ->
        let (p50, p99, p999, samples) = find_metric label in
        let img = if samples = 0 then
          I.hcat [col 9 a_dim "--"; col 9 a_dim "--"; col 9 a_dim "--"]
        else
          let s50 = severity label p50 samples in
          let s99 = max s50 (severity label p99 samples) in
          let s999 = max s99 (severity label p999 samples) in
          I.hcat [
            col 9 (attr_of_sev s50) (format_latency_us p50);
            col 9 (attr_of_sev s99) (format_latency_us p99);
            col 9 (attr_of_sev s999) (format_latency_us p999);
          ]
        in
        if i = 0 then img else I.hcat [ I.string a_border " │ "; img ]
      ) metric_order in
      
      let (_, cycle_p99, _, _) = find_metric "cycle" in
      let c_arr = update_cycle_hist symbol cycle_p99 in
      let trend_spark = render_sparkline 15 c_arr 100.0 (fun v -> attr_of_sev (severity "cycle" v 1)) in
      
      let exch = exch_of_symbol symbol in
      let sym_attr = if exch <> "" then exch_sym_attr exch else a_bright in
      close_row w (I.hcat (
        [ I.string a_border " │  ";
          I.string dot_attr "●";
          I.string a_text " ";
          col 14 sym_attr (truncate_string 13 symbol);
          I.string a_border " │ ";
          trend_spark;
          I.string a_border " │ " ]
        @ metric_cells
      ))
    ) active_lats in
    let title = section_title w "PERFORMANCE" in
    I.vcat (title :: header :: rows @ [section_footer w])
