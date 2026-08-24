open Notty
open Theme

(** Reusable table for exchange connectivity deduplication. *)
let exch_tbl : (string, bool) Hashtbl.t = Hashtbl.create 4

let render_footer w json =
  let t = Theme.current () in
  let uptime = json |?> "uptime_s" |> to_float_d 0.0 in
  let fng = json |?> "fear_and_greed" |> to_float_d 0.0 in
  (* Per-exchange connectivity: shown green if any strategy has a live
     bid/ask feed on that exchange, red otherwise. Exchanges are
     deduplicated and sorted by name. *)
  let exch_connected =
    let strats =
      match json |?> "strategies" with
      | `Assoc l -> l
      | _ -> []
    in
    Hashtbl.clear exch_tbl;
    List.iter
      (fun (_sym, data) ->
         let exch = data |?> "exchange" |> to_string_d "" in
         if exch <> ""
         then (
           let market = data |?> "market" in
           let bid = market |?> "bid" |> to_float_d 0.0 in
           let ask = market |?> "ask" |> to_float_d 0.0 in
           let live = bid > 0.0 && ask > 0.0 in
           let cur =
             try Hashtbl.find exch_tbl exch with
             | Not_found -> false
           in
           Hashtbl.replace exch_tbl exch (cur || live)))
      strats;
    let pairs = Hashtbl.fold (fun k v acc -> (k, v) :: acc) exch_tbl [] in
    List.sort (fun (a, _) (b, _) -> String.compare a b) pairs
  in
  let conn_imgs, _conn_w =
    List.fold_right
      (fun (exch, live) (imgs, w_acc) ->
         let tag =
           match exch with
           | "kraken" -> "kraken"
           | "hyperliquid" -> "hyperliquid"
           | "alpaca" -> "alpaca"
           | e -> truncate_string 10 e
         in
         let dot_attr =
           if live then A.(fg t.c_green ++ bg t.c_panel) else A.(fg t.c_red ++ bg t.c_panel)
         in
         let exch_c =
           match exch with
           | "hyperliquid" -> t.c_exch_hl
           | "kraken" -> t.c_exch_kr
           | "lighter" -> t.c_exch_li
           | "ibkr" -> t.c_exch_ib
           | "alpaca" -> t.c_exch_alp
           | _ -> t.c_label
         in
         let seg =
           I.hcat
             [ I.string A.(fg t.c_dim ++ bg t.c_panel) "  │  "
             ; I.string dot_attr "◉"
             ; I.string A.(fg exch_c ++ bg t.c_panel) (" " ^ tag)
             ]
         in
         seg :: imgs, w_acc + 5 + 1 + 1 + String.length tag)
      exch_connected
      ([], 0)
  in
  let dur_str = format_duration uptime in
  let fng_str = add_commas (Printf.sprintf "%.0f" fng) in
  let left_space = I.string A.(bg t.c_bg) "  " in
  let right_space = I.string A.(bg t.c_bg) " " in
  let theme_tag = "t: " ^ t.name in
  let left_text = "q: quit  │  ←/→ latency  │  " in
  let base_imgs =
    [ left_space
    ; I.string A.(bg t.c_panel) " "
    ; I.string A.(fg t.c_dim ++ bg t.c_panel) left_text
    ; I.string A.(fg t.c_accent ++ bg t.c_panel ++ st bold) theme_tag
    ; I.string A.(fg t.c_dim ++ bg t.c_panel) "  │  "
    ; I.string A.(fg t.c_label ++ bg t.c_panel) "up "
    ; I.string A.(fg t.c_text ++ bg t.c_panel) dur_str
    ; I.string A.(fg t.c_dim ++ bg t.c_panel) "  │  "
    ; I.string A.(fg t.c_label ++ bg t.c_panel) "f&g "
    ; I.string
        A.(
          fg (if fng >= 60.0 then t.c_green else if fng >= 40.0 then t.c_yellow else t.c_red)
          ++ bg t.c_panel
          ++ st bold)
        fng_str
    ]
  in
  let left_seg = I.hcat base_imgs in
  let conn_seg = I.hcat conn_imgs in
  let pad_w = max 0 (w - I.width left_seg - I.width conn_seg - 1) in
  I.hcat
    [ left_seg; conn_seg; I.string A.(bg t.c_panel) (String.make pad_w ' '); right_space ]
;;
