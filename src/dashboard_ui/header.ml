open Notty
open Theme

let render_header w json =
  let uptime = json |?> "uptime_s" |> to_float_d 0.0 in
  let fng    = json |?> "fear_and_greed" |> to_float_d 0.0 in
  (* Per-exchange connectivity: green if any strategy has a live bid/ask
     feed on that exchange, red otherwise. Deduplicated and sorted. *)
  let exch_connected =
    let strats = match json |?> "strategies" with `Assoc l -> l | _ -> [] in
    let tbl = Hashtbl.create 4 in
    List.iter (fun (_sym, data) ->
      let exch = data |?> "exchange" |> to_string_d "" in
      if exch <> "" then begin
        let market = data |?> "market" in
        let bid = market |?> "bid" |> to_float_d 0.0 in
        let ask = market |?> "ask" |> to_float_d 0.0 in
        let live = bid > 0.0 && ask > 0.0 in
        let cur = try Hashtbl.find tbl exch with Not_found -> false in
        Hashtbl.replace tbl exch (cur || live)
      end
    ) strats;
    let pairs = Hashtbl.fold (fun k v acc -> (k, v) :: acc) tbl [] in
    List.sort (fun (a, _) (b, _) -> String.compare a b) pairs
  in
  let conn_imgs, _conn_w = List.fold_right (fun (exch, live) (imgs, w_acc) ->
    let tag = match exch with
      | "kraken"       -> "kraken"
      | "hyperliquid"  -> "hyperliquid"
      | e              -> truncate_string 10 e
    in
    let dot_attr =
      if live then A.(fg c_green ++ bg c_panel)
              else A.(fg c_red   ++ bg c_panel)
    in
    let seg = I.hcat [
      I.string A.(fg c_dim ++ bg c_panel) "  │  ";
      I.string dot_attr "◉";
      I.string A.(fg c_label ++ bg c_panel) (" " ^ tag);
    ] in
    (seg :: imgs, w_acc + 5 + 1 + 1 + String.length tag)
  ) exch_connected ([], 0) in
  let dur_str = format_duration uptime in
  let fng_str = Printf.sprintf "%.0f" fng in
  let left_space = I.string A.(bg c_bg) "  " in
  let right_space = I.string A.(bg c_bg) " " in
  let left_text = "q: quit  │  Diophant Solutions  │  " in
  let base_imgs = [
    left_space;
    I.string A.(bg c_panel) " ";
    I.string A.(fg c_dim ++ bg c_panel) left_text;
    I.string A.(fg c_label ++ bg c_panel) "up ";
    I.string A.(fg c_text ++ bg c_panel) dur_str;
    I.string A.(fg c_dim ++ bg c_panel) "  │  ";
    I.string A.(fg c_label ++ bg c_panel) "f&g ";
    I.string A.(fg (if fng >= 60.0 then c_green
                    else if fng >= 40.0 then c_yellow
                    else c_red) ++ bg c_panel ++ st bold)
             fng_str;
  ] in
  let left_seg = I.hcat base_imgs in
  let conn_seg = I.hcat conn_imgs in
  let pad_w = max 0 (w - I.width left_seg - I.width conn_seg - 1) in
  I.hcat [
    left_seg;
    conn_seg;
    I.string A.(bg c_panel) (String.make pad_w ' ');
    right_space;
  ]
