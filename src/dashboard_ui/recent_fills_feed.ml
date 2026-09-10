open Notty
open Theme

(** Recent Fills Feed Component.
    Scrolls recent filled orders horizontally across the screen below the holdings.
*)

let local_fills : Snapshot.fill list ref = ref []
let capacity = 10
let initialized = ref false

(** Merge engine fills into [local_fills]. On startup the engine seeds
    historical fills into its ring buffer, so the list is non-empty from the
    first snapshot. New live fills are detected by timestamp comparison and
    prepended. *)
let merge_engine_fills (engine_fills : Snapshot.fill list) =
  if not !initialized
  then (
    if engine_fills <> []
    then (
      local_fills := [ List.hd engine_fills ];
      initialized := true))
  else (
    let latest_ts =
      match !local_fills with
      | [] -> 0.0
      | f :: _ -> f.timestamp
    in
    let new_fills =
      List.filter (fun (f : Snapshot.fill) -> f.timestamp > latest_ts) engine_fills
    in
    if new_fills <> []
    then (
      let combined = new_fills @ !local_fills in
      let rec take n l acc =
        if n <= 0
        then List.rev acc
        else (
          match l with
          | [] -> List.rev acc
          | h :: t -> take (n - 1) t (h :: acc))
      in
      local_fills := take capacity combined []))
;;

let render_fills w (snapshot : Snapshot.t) =
  let t = Theme.current () in
  merge_engine_fills snapshot.fills;
  let fills = !local_fills in
  if fills = []
  then I.empty
  else (* Build the ticker string chunks, one per fill. *)
    (
    let chunks =
      List.map
        (fun (f : Snapshot.fill) ->
           let venue = f.venue in
           let symbol = f.symbol in
           let side = String.uppercase_ascii f.side in
           let amount = f.amount in
           let price = f.fill_price in
           let timestamp = f.timestamp in
           (* Format the time elapsed since the fill as a compact
              duration. *)
           let now = Unix.gettimeofday () in
           let diff = max 0.0 (now -. timestamp) in
           let time_str =
             if diff < 60.0
             then Printf.sprintf "%.0fs" diff
             else if diff < 3600.0
             then Printf.sprintf "%.0fm" (diff /. 60.0)
             else Printf.sprintf "%.1fh" (diff /. 3600.0)
           in
           let side_attr =
             if side = "BUY"
             then A.(fg t.c_green ++ st bold)
             else A.(fg t.c_red ++ st bold)
           in
           let sym_attr = exch_sym_attr (String.lowercase_ascii venue) in
           let amount_str = format_qty amount in
           I.hcat
             [ I.string A.(fg t.c_dim) (time_str ^ " ago ")
             ; I.string sym_attr symbol
             ; I.string A.(fg t.c_dim) " "
             ; I.string side_attr side
             ; I.string A.(fg t.c_text) (" " ^ amount_str ^ " @ " ^ format_price price)
             ])
        fills
    in
    let order_separator = I.string A.(fg t.c_dim ++ bg t.c_bg) "  •  " in
    let feed_start = I.string A.(fg t.c_accent ++ bg t.c_bg) "  ◈ RECENT FILLS ◈  " in
    let max_w = w - 2 in
    let final_img =
      List.fold_left
        (fun acc chunk ->
           let candidate =
             if I.width acc = I.width feed_start
             then I.hcat [ acc; chunk ]
             else I.hcat [ acc; order_separator; chunk ]
           in
           if I.width candidate > max_w then acc else candidate)
        feed_start
        chunks
    in
    let padded = I.hsnap ~align:`Left w final_img in
    I.(padded </> I.string A.(bg t.c_bg) (String.make w ' ')))
;;

let render_fills_card w (snapshot : Snapshot.t) =
  let t = Theme.current () in
  merge_engine_fills snapshot.fills;
  let fills = !local_fills in
  if fills = []
  then render_card w "LIVE FILLS" [ I.string t.a_dim "No recent fills recorded" ]
  else (
    let now = Unix.gettimeofday () in
    let fill_rows =
      List.map
        (fun (f : Snapshot.fill) ->
           let venue = f.venue in
           let symbol = f.symbol in
           let side = String.uppercase_ascii f.side in
           let amount = f.amount in
           let price = f.fill_price in
           let timestamp = f.timestamp in
           let diff = max 0.0 (now -. timestamp) in
           let time_str =
             if diff < 60.0
             then Printf.sprintf "%.0fs" diff
             else if diff < 3600.0
             then Printf.sprintf "%.0fm" (diff /. 60.0)
             else Printf.sprintf "%.1fh" (diff /. 3600.0)
           in
           let side_attr = if side = "BUY" then t.a_green else t.a_red in
           let sym_attr = exch_sym_attr (String.lowercase_ascii venue) in
           let amount_str = format_qty amount in
           I.hcat
             [ col 6 t.a_dim (time_str ^ " ago")
             ; col 10 sym_attr (truncate_string 9 symbol)
             ; col 4 side_attr side
             ; col_right 10 t.a_text amount_str
             ; col_right 12 t.a_bright (format_price price)
             ])
        fills
    in
    render_card w "LIVE FILLS" fill_rows)
;;
